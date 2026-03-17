"""OpenAI Batch API — отправка, получение результатов, парсинг ответов."""
import asyncio
import io
import json
from typing import Any, cast

import httpx
from loguru import logger
from openai import AsyncOpenAI
from pydantic import ValidationError

from src.ai.images import resolve_profile_images
from src.ai.prompt import build_analysis_prompt
from src.ai.schemas import AIInsights
from src.config import Settings
from src.models.blog import ScrapedProfile

__all__ = [
    "TERMINAL_WITH_RESULTS",
    "BatchResult",
    "build_batch_request",
    "poll_batch",
    "submit_batch",
]

# В результатах батча: AIInsights | ("refusal", reason) | None (ошибка API)
BatchResult = AIInsights | tuple[str, str] | None

# Статусы батча, при которых могут быть результаты в файлах
TERMINAL_WITH_RESULTS = frozenset({"completed", "expired"})


def _extract_content_text(message: dict[str, Any]) -> str | None:
    """Нормализовать message.content в строку JSON для Pydantic."""
    content = message.get("content")
    if isinstance(content, str):
        normalized = content.strip()
        return normalized or None

    if isinstance(content, list):
        text_chunks: list[str] = []
        # Явно приводим к list[Any], чтобы pyright знал тип элементов
        content_list = cast(list[Any], content)
        for part in content_list:
            if not isinstance(part, dict):
                continue
            part_dict = cast(dict[str, Any], part)
            if part_dict.get("type") != "text":
                continue
            text = part_dict.get("text")
            if isinstance(text, str):
                text_chunks.append(text)
        normalized = "".join(text_chunks).strip()
        return normalized or None

    return None


def _cleanup_json_payload(raw_text: str) -> str | None:
    """Извлечь JSON-объект из ответа модели (включая markdown/code fences)."""
    text = raw_text.strip()
    if not text:
        return None

    if text.startswith("```"):
        stripped = text.strip("`").strip()
        if stripped.startswith("json"):
            stripped = stripped[4:].strip()
        text = stripped

    start_idx = text.find("{")
    end_idx = text.rfind("}")
    if start_idx == -1 or end_idx == -1 or end_idx < start_idx:
        return None

    return text[start_idx:end_idx + 1]


def _strip_null_bytes(text: str) -> str:
    """Убрать \\u0000 (null bytes) — PostgreSQL text columns их не принимают."""
    return text.replace("\x00", "")


_MAX_TAGS = 40


def _truncate_tags(data: dict[str, Any]) -> dict[str, Any]:
    """Обрезать tags до _MAX_TAGS — OpenAI strict mode не поддерживает maxItems."""
    tags = data.get("tags")
    if isinstance(tags, list):
        # Явное приведение, чтобы pyright понимал тип элементов списка
        tags_list = cast(list[Any], tags)
        if len(tags_list) > _MAX_TAGS:
            data["tags"] = tags_list[:_MAX_TAGS]
    return data


def _parse_ai_insights(content_text: str) -> AIInsights:
    """Распарсить structured output с fallback для шумных ответов.

    Стратегия:
    1. Быстрый путь: model_validate_json (без модификаций).
    2. Fallback: парсим JSON вручную → _truncate_tags → model_validate.
       Если _cleanup_json_payload вернул None, пробуем оригинальный текст.
    """
    # PostgreSQL не принимает \u0000 в text/jsonb полях
    content_text = _strip_null_bytes(content_text)
    try:
        return AIInsights.model_validate_json(content_text)
    except ValidationError as first_err:
        # Fallback: парсим JSON вручную, обрезаем tags, ревалидируем
        json_text = _cleanup_json_payload(content_text) or content_text
        try:
            parsed = json.loads(json_text)
        except json.JSONDecodeError:
            # Не удалось распарсить JSON — поднимаем оригинальную ValidationError
            raise first_err from None
        _truncate_tags(parsed)
        try:
            return AIInsights.model_validate(parsed)
        except ValidationError as second_err:
            # Логируем детали: какие поля не прошли валидацию
            for err in second_err.errors():
                logger.warning(
                    f"[parse_ai] Validation error: field={'.'.join(str(x) for x in err.get('loc', []))}, "
                    f"type={err.get('type')}, msg={err.get('msg')}"
                )
            raise second_err from first_err


_UNSUPPORTED_STRICT_KEYS = frozenset({
    "default", "minItems", "maxItems", "minLength", "maxLength",
    "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum",
    "title", "examples",
})


def _make_strict_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """
    Привести Pydantic JSON-схему к формату OpenAI strict mode.

    OpenAI strict: true требует:
    - required содержит ВСЕ ключи из properties
    - additionalProperties: false на каждом объекте
    - Рекурсивно для вложенных объектов и $defs
    - Нет unsupported ключей (default, minItems, maxItems и т.д.)
    """
    schema = schema.copy()

    # Обработать $defs (вложенные модели)
    if "$defs" in schema:
        defs_raw: dict[str, Any] = cast(dict[str, Any], schema["$defs"])
        processed_defs: dict[str, dict[str, Any]] = {}
        for def_name, def_value in defs_raw.items():
            # Явное приведение к dict[str, Any] — значения $defs всегда объекты схемы
            def_schema: dict[str, Any] = cast(dict[str, Any], def_value)
            processed_defs[def_name] = _make_strict_schema(def_schema)
        schema["$defs"] = processed_defs

    # Проставить required и additionalProperties для объектов
    if "properties" in schema:
        schema["required"] = list(schema["properties"].keys())
        schema["additionalProperties"] = False
        # Рекурсивно обработать вложенные properties
        for prop_name, prop_schema in schema["properties"].items():
            schema["properties"][prop_name] = _make_strict_schema(prop_schema)

    # $ref не допускает соседних ключей (description, default и т.д.) в strict mode
    if "$ref" in schema:
        schema = {"$ref": schema["$ref"]}
        return schema

    # Обработать items (массивы)
    if "items" in schema and isinstance(schema["items"], dict):
        # Явное приведение — schema["items"] имеет тип Any из dict[str, Any]
        items_schema: dict[str, Any] = cast(dict[str, Any], schema["items"])
        schema["items"] = _make_strict_schema(items_schema)

    # Обработать anyOf (Union типы)
    if "anyOf" in schema:
        schema["anyOf"] = [_make_strict_schema(s) for s in schema["anyOf"]]

    # Удалить ключи, не поддерживаемые OpenAI strict mode
    for key in _UNSUPPORTED_STRICT_KEYS:
        schema.pop(key, None)

    return schema


def build_batch_request(
    custom_id: str,
    profile: ScrapedProfile,
    settings: Settings,
    image_map: dict[str, str] | None = None,
    text_only: bool = False,
) -> dict[str, Any]:
    """Сформировать одну строку JSONL для Batch API."""
    # text_only: пустой dict → все URL пропускаются в _add_image (url not in image_map)
    effective_image_map = {} if text_only else image_map
    messages = build_analysis_prompt(profile, image_map=effective_image_map)
    if text_only:
        # Дополняем system prompt указанием об отсутствии изображений
        messages[0]["content"] += (
            "\n\nВАЖНО: Изображения для этого профиля недоступны. "
            "Анализируй только по текстовым данным (био, подписи к постам, хештеги, комментарии)."
        )
    return {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": settings.batch_model,
            "reasoning_effort": settings.batch_reasoning_effort,
            "messages": messages,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "ai_insights",
                    "strict": True,
                    "schema": _make_strict_schema(AIInsights.model_json_schema()),
                },
            },
        },
    }


# Количество профилей на чанк при загрузке изображений.
# Ограничивает пиковую память: 1 чанк × 10 изображений × ~400 КБ ≈ 40 МБ.
_IMAGE_CHUNK_SIZE = 10


async def submit_batch(
    client: AsyncOpenAI,
    profiles: list[tuple[str, ScrapedProfile]],
    settings: Settings,
    text_only_ids: set[str] | None = None,
) -> str:
    """
    Отправить батч профилей на анализ.
    profiles — список (blog_id, ScrapedProfile).
    text_only_ids — blog_id для которых не скачивать изображения (retry после refusal).
    Возвращает batch_id.

    Использует chunked pipeline: профили обрабатываются чанками по _IMAGE_CHUNK_SIZE,
    изображения скачиваются параллельно внутри чанка, JSONL пишется инкрементально.
    Это ограничивает пиковую память: O(chunk_size × image_size) вместо O(total × image_size × 3).
    """
    if not profiles:
        raise ValueError("Cannot submit empty batch")

    _text_only_ids = text_only_ids or set()
    download_semaphore = asyncio.Semaphore(10)
    total_images = 0

    # JSONL буфер — пишем инкрементально, не накапливая промежуточные структуры
    buffer = io.BytesIO()
    total_profiles_with_images = sum(
        1 for blog_id, _ in profiles if blog_id not in _text_only_ids
    )
    logger.info(
        f"[batch] Загрузка изображений для {total_profiles_with_images} профилей "
        f"({len(_text_only_ids)} text-only, чанки по {_IMAGE_CHUNK_SIZE})..."
    )

    try:
        async with httpx.AsyncClient() as http_client:
            for chunk_start in range(0, len(profiles), _IMAGE_CHUNK_SIZE):
                chunk = profiles[chunk_start:chunk_start + _IMAGE_CHUNK_SIZE]

                # Параллельная загрузка изображений для чанка (кроме text_only)
                chunk_for_images = [
                    (blog_id, profile) for blog_id, profile in chunk
                    if blog_id not in _text_only_ids
                ]
                chunk_image_maps: dict[str, dict[str, str]] = {}
                if chunk_for_images:
                    image_tasks = [
                        resolve_profile_images(
                            profile, client=http_client, semaphore=download_semaphore,
                        )
                        for _, profile in chunk_for_images
                    ]
                    raw = await asyncio.gather(*image_tasks, return_exceptions=True)
                    for i, r in enumerate(raw):
                        blog_id = chunk_for_images[i][0]
                        if isinstance(r, BaseException):
                            logger.warning(f"[batch] Ошибка загрузки изображений для {blog_id}: {r}")
                            chunk_image_maps[blog_id] = {}
                        else:
                            chunk_image_maps[blog_id] = r
                            total_images += len(r)

                # Формируем JSONL строки и сразу пишем в буфер
                for blog_id, profile in chunk:
                    is_text_only = blog_id in _text_only_ids
                    image_map = chunk_image_maps.get(blog_id, {})
                    request = build_batch_request(
                        blog_id, profile, settings,
                        image_map=image_map, text_only=is_text_only,
                    )
                    buffer.write(json.dumps(request, ensure_ascii=False).encode("utf-8"))
                    buffer.write(b"\n")
                    mode = "text-only" if is_text_only else f"{len(image_map)} images"
                    logger.debug(
                        f"[batch] Prepared request for blog {blog_id} "
                        f"(@{profile.username}, {len(profile.medias)} publications, {mode})"
                    )

                # Явно освобождаем данные изображений чанка
                del chunk_image_maps

        logger.info(
            f"[batch] Скачано {total_images} изображений для "
            f"{total_profiles_with_images} профилей"
        )
        logger.debug(f"[batch] JSONL size: {buffer.tell()} bytes, model={settings.batch_model}")

        # Загружаем файл в OpenAI
        buffer.seek(0)
        file_obj = await client.files.create(
            file=("batch.jsonl", buffer),
            purpose="batch",
        )
        logger.debug(f"[batch] File uploaded: {file_obj.id}")
    finally:
        buffer.close()

    # Создаём батч
    batch = await client.batches.create(
        input_file_id=file_obj.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
    )

    logger.info(f"Submitted batch {batch.id} with {len(profiles)} profiles")
    return batch.id


async def poll_batch(client: AsyncOpenAI, batch_id: str) -> dict[str, Any]:
    """
    Проверить статус батча.
    Возвращает {"status": "...", "results": {...}} или {"status": "in_progress"}.
    """
    batch = await client.batches.retrieve(batch_id)
    counts = batch.request_counts
    logger.info(
        f"[batch] Poll {batch_id}: status={batch.status}, "
        f"completed={counts.completed if counts else '?'}/"
        f"{counts.total if counts else '?'}, "
        f"failed={counts.failed if counts else '?'}, "
        f"output_file={batch.output_file_id}, "
        f"error_file={batch.error_file_id}"
    )

    if batch.status not in TERMINAL_WITH_RESULTS:
        return {"status": batch.status}

    results: dict[str, BatchResult] = {}
    output_line_count = 0
    error_line_count = 0
    # Аккумуляторы токенов per-request usage
    total_input_tokens = 0
    total_output_tokens = 0
    total_reasoning_tokens = 0
    total_cached_tokens = 0

    # Скачиваем успешные результаты
    if batch.output_file_id:
        file_content = await client.files.content(batch.output_file_id)

        for line in file_content.text.strip().split("\n"):
            if not line:
                continue
            output_line_count += 1
            try:
                data_raw = json.loads(line)
            except json.JSONDecodeError as e:
                logger.error(f"Malformed JSONL line in output file: {e}")
                continue
            if not isinstance(data_raw, dict):
                logger.error(f"Unexpected JSONL line type: {type(data_raw).__name__}, skipping")
                continue
            data: dict[str, Any] = cast(dict[str, Any], data_raw)
            custom_id = str(data.get("custom_id", ""))
            if not custom_id:
                logger.error("JSONL line missing custom_id, skipping")
                continue

            # Проверка на ошибку/refusal (response может быть null)
            response: dict[str, Any] = cast(dict[str, Any], data.get("response")) or {}
            status_code: int | None = cast(int | None, response.get("status_code"))

            # status_code=0 или None — внутренний сбой OpenAI (известный баг)
            if status_code is None or status_code == 0:
                logger.error(
                    f"[batch] Internal OpenAI failure for {custom_id}: "
                    f"status_code={status_code}, raw={json.dumps(data, ensure_ascii=False)[:1000]}"
                )
                results[custom_id] = None
                continue

            if status_code >= 400:
                response_body: dict[str, Any] = cast(dict[str, Any], response.get("body")) or {}
                logger.error(
                    f"Batch API response error for {custom_id}: "
                    f"status={status_code}, body={json.dumps(response_body, ensure_ascii=False)[:500]}"
                )
                results[custom_id] = None
                continue

            response_body = cast(dict[str, Any], response.get("body")) or {}

            # Собираем usage токенов из каждого ответа
            usage: dict[str, Any] = cast(dict[str, Any], response_body.get("usage")) or {}
            total_input_tokens += int(usage.get("prompt_tokens", 0))
            total_output_tokens += int(usage.get("completion_tokens", 0))
            # reasoning_tokens и cached_tokens — вложенные объекты
            completion_details = cast(dict[str, Any], usage.get("completion_tokens_details")) or {}
            total_reasoning_tokens += int(completion_details.get("reasoning_tokens", 0))
            prompt_details = cast(dict[str, Any], usage.get("prompt_tokens_details")) or {}
            total_cached_tokens += int(prompt_details.get("cached_tokens", 0))

            choices: list[Any] = cast(list[Any], response_body.get("choices")) or []

            if not choices:
                logger.warning(f"No choices for {custom_id}")
                results[custom_id] = None
                continue

            message: dict[str, Any] = cast(dict[str, Any], choices[0].get("message")) or {}

            # Проверка refusal (content filter)
            if message.get("refusal"):
                logger.warning(f"AI refusal for {custom_id}: {message['refusal']}")
                results[custom_id] = ("refusal", str(message["refusal"]))
                continue

            # Парсинг structured output
            try:
                content_text = _extract_content_text(message)
                if content_text is None:
                    raise ValueError("Empty or unsupported message.content")
                insights = _parse_ai_insights(content_text)
                results[custom_id] = insights
                logger.debug(f"[batch] Parsed insights for {custom_id}: "
                             f"confidence={insights.confidence}, "
                             f"summary_len={len(insights.summary)}")
            except (ValidationError, ValueError, TypeError, json.JSONDecodeError) as e:
                logger.error(f"Failed to parse AI response for {custom_id}: {e}")
                results[custom_id] = None

    # Обработка ошибок из error_file_id (запросы, провалившиеся на стороне API)
    if batch.error_file_id:
        error_content = await client.files.content(batch.error_file_id)

        for line in error_content.text.strip().split("\n"):
            if not line:
                continue
            error_line_count += 1
            try:
                err_data_raw = json.loads(line)
            except json.JSONDecodeError as e:
                logger.error(f"Malformed JSONL line in error file: {e}")
                continue
            if not isinstance(err_data_raw, dict):
                logger.error(f"Unexpected error JSONL line type: {type(err_data_raw).__name__}, skipping")
                continue
            err_data: dict[str, Any] = cast(dict[str, Any], err_data_raw)
            custom_id = str(err_data.get("custom_id", ""))
            if not custom_id:
                logger.error("Error JSONL line missing custom_id, skipping")
                continue
            error_info: dict[str, Any] = cast(dict[str, Any], err_data.get("error")) or {}
            response_info: dict[str, Any] = cast(dict[str, Any], err_data.get("response")) or {}
            logger.error(
                f"[batch] Error file entry for {custom_id}: "
                f"error_code={error_info.get('code')}, "
                f"error_message={error_info.get('message')}, "
                f"status_code={response_info.get('status_code')}, "
                f"request_id={response_info.get('request_id')}, "
                f"raw={json.dumps(err_data, ensure_ascii=False)[:1000]}"
            )
            results[custom_id] = None

    # Сверка счётчиков — request_counts OpenAI часто врут
    reported_total = counts.total if counts else 0
    reported_failed = counts.failed if counts else 0
    actual_total = output_line_count + error_line_count
    if actual_total != reported_total or error_line_count != reported_failed:
        logger.warning(
            f"[batch] Count mismatch for {batch_id}: "
            f"OpenAI reports total={reported_total}, failed={reported_failed} | "
            f"Actual: output_lines={output_line_count}, error_lines={error_line_count}, "
            f"actual_total={actual_total}"
        )

    return {
        "status": batch.status,
        "results": results,
        "usage": {
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
            "total_tokens": total_input_tokens + total_output_tokens,
            "reasoning_tokens": total_reasoning_tokens,
            "cached_tokens": total_cached_tokens,
        },
    }
