"""OpenAI Batch API — отправка, получение результатов, матчинг категорий."""
import asyncio
import io
import json
from typing import Any

import httpx
from loguru import logger
from openai import AsyncOpenAI
from pydantic import ValidationError
from supabase import Client

from src.ai.images import resolve_profile_images
from src.ai.prompt import build_analysis_prompt
from src.ai.schemas import AIInsights
from src.config import Settings
from src.database import run_in_thread
from src.models.blog import ScrapedProfile

# Статусы батча, при которых могут быть результаты в файлах
TERMINAL_WITH_RESULTS = {"completed", "expired"}


def _extract_content_text(message: dict[str, Any]) -> str | None:
    """Нормализовать message.content в строку JSON для Pydantic."""
    content = message.get("content")
    if isinstance(content, str):
        normalized = content.strip()
        return normalized or None

    if isinstance(content, list):
        text_chunks: list[str] = []
        for part in content:
            if not isinstance(part, dict):
                continue
            if part.get("type") != "text":
                continue
            text = part.get("text")
            if isinstance(text, str):
                text_chunks.append(text)
        normalized = "".join(text_chunks).strip()
        return normalized or None

    return None


def _make_strict_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """
    Привести Pydantic JSON-схему к формату OpenAI strict mode.

    OpenAI strict: true требует:
    - required содержит ВСЕ ключи из properties
    - additionalProperties: false на каждом объекте
    - Рекурсивно для вложенных объектов и $defs
    """
    schema = schema.copy()

    # Обработать $defs (вложенные модели)
    if "$defs" in schema:
        schema["$defs"] = {
            name: _make_strict_schema(defn)
            for name, defn in schema["$defs"].items()
        }

    # Проставить required и additionalProperties для объектов
    if "properties" in schema:
        schema["required"] = list(schema["properties"].keys())
        schema["additionalProperties"] = False
        # Рекурсивно обработать вложенные properties
        for prop_name, prop_schema in schema["properties"].items():
            schema["properties"][prop_name] = _make_strict_schema(prop_schema)

    # Обработать items (массивы)
    if "items" in schema and isinstance(schema["items"], dict):
        schema["items"] = _make_strict_schema(schema["items"])

    # Обработать anyOf (Union типы)
    if "anyOf" in schema:
        schema["anyOf"] = [_make_strict_schema(s) for s in schema["anyOf"]]

    return schema


def build_batch_request(
    custom_id: str,
    profile: ScrapedProfile,
    settings: Settings,
    image_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Сформировать одну строку JSONL для Batch API."""
    messages = build_analysis_prompt(profile, image_map=image_map)
    return {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": settings.batch_model,
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


async def submit_batch(
    client: AsyncOpenAI,
    profiles: list[tuple[str, ScrapedProfile]],
    settings: Settings,
) -> str:
    """
    Отправить батч профилей на анализ.
    profiles — список (blog_id, ScrapedProfile).
    Возвращает batch_id.
    """
    if not profiles:
        raise ValueError("Cannot submit empty batch")

    # Скачиваем изображения для всех профилей параллельно (один httpx-клиент на весь батч)
    logger.info(f"[batch] Скачиваем изображения для {len(profiles)} профилей...")
    async with httpx.AsyncClient() as http_client:
        tasks = [
            resolve_profile_images(profile, client=http_client)
            for _, profile in profiles
        ]
        image_maps = list(await asyncio.gather(*tasks))
    total_downloaded = sum(len(m) for m in image_maps)
    logger.info(f"[batch] Скачано {total_downloaded} изображений для {len(profiles)} профилей")

    # Формируем JSONL в памяти (без временных файлов)
    lines: list[str] = []
    for (blog_id, profile), image_map in zip(profiles, image_maps):
        request = build_batch_request(blog_id, profile, settings, image_map=image_map)
        lines.append(json.dumps(request, ensure_ascii=False))
        logger.debug(f"[batch] Prepared request for blog {blog_id} "
                     f"(@{profile.username}, {len(profile.medias)} publications, "
                     f"{len(image_map)} images as base64)")

    jsonl_bytes = "\n".join(lines).encode("utf-8")
    logger.debug(f"[batch] JSONL size: {len(jsonl_bytes)} bytes, "
                 f"model={settings.batch_model}")

    # Загружаем файл в OpenAI (async)
    file_obj = await client.files.create(
        file=("batch.jsonl", io.BytesIO(jsonl_bytes)),
        purpose="batch",
    )
    logger.debug(f"[batch] File uploaded: {file_obj.id}")

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

    results: dict[str, AIInsights | None] = {}
    output_line_count = 0
    error_line_count = 0

    # Скачиваем успешные результаты
    if batch.output_file_id:
        file_content = await client.files.content(batch.output_file_id)

        for line in file_content.text.strip().split("\n"):
            if not line:
                continue
            output_line_count += 1
            try:
                data = json.loads(line)
            except json.JSONDecodeError as e:
                logger.error(f"Malformed JSONL line in output file: {e}")
                continue
            custom_id = data.get("custom_id")
            if not custom_id:
                logger.error("JSONL line missing custom_id, skipping")
                continue

            # Проверка на ошибку/refusal (response может быть null)
            response = data.get("response") or {}
            status_code = response.get("status_code")

            # status_code=0 или None — внутренний сбой OpenAI (известный баг)
            if status_code is None or status_code == 0:
                logger.error(
                    f"[batch] Internal OpenAI failure for {custom_id}: "
                    f"status_code={status_code}, raw={json.dumps(data, ensure_ascii=False)[:1000]}"
                )
                results[custom_id] = None
                continue

            if isinstance(status_code, int) and status_code >= 400:
                response_body = response.get("body", {})
                logger.error(
                    f"Batch API response error for {custom_id}: "
                    f"status={status_code}, body={json.dumps(response_body, ensure_ascii=False)[:500]}"
                )
                results[custom_id] = None
                continue

            response_body = response.get("body", {})
            choices = response_body.get("choices", [])

            if not choices:
                logger.warning(f"No choices for {custom_id}")
                results[custom_id] = None
                continue

            message = choices[0].get("message", {})

            # Проверка refusal (content filter)
            if message.get("refusal"):
                logger.warning(f"AI refusal for {custom_id}: {message['refusal']}")
                results[custom_id] = None
                continue

            # Парсинг structured output
            try:
                content_text = _extract_content_text(message)
                if content_text is None:
                    raise ValueError("Empty or unsupported message.content")
                insights = AIInsights.model_validate_json(content_text)
                results[custom_id] = insights
                logger.debug(f"[batch] Parsed insights for {custom_id}: "
                             f"confidence={insights.confidence:.2f}, "
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
                data = json.loads(line)
            except json.JSONDecodeError as e:
                logger.error(f"Malformed JSONL line in error file: {e}")
                continue
            custom_id = data.get("custom_id")
            if not custom_id:
                logger.error("Error JSONL line missing custom_id, skipping")
                continue
            error_info = data.get("error") or {}
            response_info = data.get("response") or {}
            logger.error(
                f"[batch] Error file entry for {custom_id}: "
                f"error_code={error_info.get('code')}, "
                f"error_message={error_info.get('message')}, "
                f"status_code={response_info.get('status_code')}, "
                f"request_id={response_info.get('request_id')}, "
                f"raw={json.dumps(data, ensure_ascii=False)[:1000]}"
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

    return {"status": batch.status, "results": results}


async def load_categories(db: Client) -> dict[str, str]:
    """Загрузить все категории из БД.

    Возвращает {key: category_id} где:
    - key = code (для верхнеуровневых, AI возвращает код в primary_topic)
    - key = name_lower (для всех, подкатегории матчатся по имени)
    """
    cat_result = await run_in_thread(
        db.table("categories").select("id, code, name, parent_id").execute
    )
    categories: dict[str, str] = {}
    for c in cat_result.data:
        if not isinstance(c, dict):
            continue
        cat_id = c.get("id")
        if not isinstance(cat_id, str):
            continue
        # Верхнеуровневые категории — индексируем по code
        code = c.get("code")
        if isinstance(code, str) and code:
            categories[code] = cat_id
        # Все категории — индексируем по name_lower
        name = c.get("name")
        if isinstance(name, str):
            categories[name.lower()] = cat_id
    return categories


async def match_categories(
    db: Client,
    blog_id: str,
    insights: AIInsights,
    categories: dict[str, str] | None = None,
) -> None:
    """
    Сопоставить primary_topic и secondary_topics с таблицей categories.
    Записать в blog_categories.
    categories — кэш {name_lower: id}, если None — загружается из БД.
    """
    if not insights.content.primary_topic:
        return

    if categories is None:
        categories = await load_categories(db)

    # Собираем все записи для batch upsert
    rows: list[dict[str, str | bool]] = []
    primary = insights.content.primary_topic.lower()
    primary_category_id: str | None = categories.get(primary)
    if primary_category_id:
        rows.append({
            "blog_id": blog_id,
            "category_id": primary_category_id,
            "is_primary": True,
        })

    # Secondary topics (пропускаем по category_id, чтобы не перезаписать is_primary=True)
    seen_ids: set[str] = {primary_category_id} if primary_category_id else set()
    for topic in insights.content.secondary_topics:
        topic_lower = topic.lower()
        cat_id = categories.get(topic_lower)
        if not cat_id or cat_id in seen_ids:
            continue
        seen_ids.add(cat_id)
        rows.append({
            "blog_id": blog_id,
            "category_id": cat_id,
            "is_primary": False,
        })

    if rows:
        # Удаляем старые категории перед вставкой —
        # partial unique index blog_categories_one_primary не позволяет
        # upsert новой primary категории при существующей старой.
        # DELETE + INSERT — два HTTP-запроса (не атомарно), поэтому
        # при constraint violation ретраим один раз.
        for attempt in range(2):
            await run_in_thread(
                db.table("blog_categories")
                .delete()
                .eq("blog_id", blog_id)
                .execute
            )
            try:
                await run_in_thread(
                    db.table("blog_categories")
                    .insert(rows)
                    .execute
                )
                break
            except Exception as e:
                if attempt == 0 and "23505" in str(e):
                    logger.warning(
                        f"[match_categories] Constraint violation для blog {blog_id}, "
                        f"повторная попытка delete+insert"
                    )
                    continue
                raise


async def load_tags(db: Client) -> dict[str, str]:
    """Загрузить активные теги из БД. Возвращает {name_lower: tag_id}."""
    result = await run_in_thread(
        db.table("tags").select("id, name").eq("status", "active").execute
    )
    tags: dict[str, str] = {}
    for t in result.data:
        if not isinstance(t, dict):
            continue
        name = t.get("name")
        tag_id = t.get("id")
        if isinstance(name, str) and isinstance(tag_id, str):
            tags[name.lower()] = tag_id
    return tags


async def match_tags(
    db: Client,
    blog_id: str,
    insights: AIInsights,
    tags: dict[str, str] | None = None,
) -> None:
    """
    Сопоставить теги из AI-анализа с таблицей tags.
    Записать в blog_tags.
    tags — кэш {name_lower: id}, если None — загружается из БД.
    """
    if not insights.tags:
        return

    if tags is None:
        tags = await load_tags(db)

    rows = []
    seen_tag_ids: set[str] = set()
    for tag_name in insights.tags:
        tag_lower = tag_name.lower()
        tag_id = tags.get(tag_lower)
        if tag_id and tag_id not in seen_tag_ids:
            seen_tag_ids.add(tag_id)
            rows.append({"blog_id": blog_id, "tag_id": tag_id})

    if rows:
        # Атомарная замена: delete + insert (вместо upsert, который падает
        # с "ON CONFLICT DO UPDATE cannot affect row a second time" если
        # разные имена тегов маппятся на один tag_id)
        await run_in_thread(
            db.table("blog_tags")
            .delete()
            .eq("blog_id", blog_id)
            .execute
        )
        await run_in_thread(
            db.table("blog_tags")
            .insert(rows)
            .execute
        )
