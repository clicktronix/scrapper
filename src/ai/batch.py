"""OpenAI Batch API — отправка, получение результатов, матчинг категорий."""
import asyncio
import io
import json
import re
from difflib import get_close_matches
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

# В результатах батча: AIInsights | ("refusal", reason) | None (ошибка API)
BatchResult = AIInsights | tuple[str, str] | None

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


def _parse_ai_insights(content_text: str) -> AIInsights:
    """Распарсить structured output с fallback для слегка шумных ответов."""
    # PostgreSQL не принимает \u0000 в text/jsonb полях
    content_text = _strip_null_bytes(content_text)
    try:
        return AIInsights.model_validate_json(content_text)
    except ValidationError:
        cleaned = _cleanup_json_payload(content_text)
        if cleaned is None:
            raise
        parsed = json.loads(cleaned)
        return AIInsights.model_validate(parsed)


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

    # $ref не допускает соседних ключей (description, default и т.д.) в strict mode
    if "$ref" in schema:
        schema = {"$ref": schema["$ref"]}
        return schema

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
    text_only_ids: set[str] | None = None,
) -> str:
    """
    Отправить батч профилей на анализ.
    profiles — список (blog_id, ScrapedProfile).
    text_only_ids — blog_id для которых не скачивать изображения (retry после refusal).
    Возвращает batch_id.
    """
    if not profiles:
        raise ValueError("Cannot submit empty batch")

    _text_only_ids = text_only_ids or set()

    # Скачиваем изображения для профилей (кроме text_only)
    # Семафор ограничивает общее число конкурентных загрузок, чтобы не перегрузить
    # httpx connection pool (default 100) и Supabase Storage
    profiles_for_images = [
        (blog_id, profile) for blog_id, profile in profiles
        if blog_id not in _text_only_ids
    ]
    download_semaphore = asyncio.Semaphore(10)
    logger.info(f"[batch] Скачиваем изображения для {len(profiles_for_images)} профилей "
                f"({len(_text_only_ids)} text-only)...")
    image_maps_by_id: dict[str, dict[str, str]] = {}
    async with httpx.AsyncClient() as http_client:
        tasks = [
            resolve_profile_images(profile, client=http_client, semaphore=download_semaphore)
            for _, profile in profiles_for_images
        ]
        results = list(await asyncio.gather(*tasks))
        for (blog_id, _), img_map in zip(profiles_for_images, results):
            image_maps_by_id[blog_id] = img_map
    total_downloaded = sum(len(m) for m in image_maps_by_id.values())
    logger.info(f"[batch] Скачано {total_downloaded} изображений для {len(profiles_for_images)} профилей")

    # Формируем JSONL в памяти (без временных файлов)
    lines: list[str] = []
    for blog_id, profile in profiles:
        is_text_only = blog_id in _text_only_ids
        image_map = image_maps_by_id.get(blog_id, {})
        request = build_batch_request(
            blog_id, profile, settings,
            image_map=image_map, text_only=is_text_only,
        )
        lines.append(json.dumps(request, ensure_ascii=False))
        mode = "text-only" if is_text_only else f"{len(image_map)} images"
        logger.debug(f"[batch] Prepared request for blog {blog_id} "
                     f"(@{profile.username}, {len(profile.medias)} publications, {mode})")

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

    results: dict[str, BatchResult] = {}
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
                results[custom_id] = ("refusal", message["refusal"])
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


_TAG_ALIASES: dict[str, str] = {
    # Русские варианты / сокращения (EN→RU перевод — в _EN_TO_RU_TAGS)
    "video-контент": "видео-контент",
    "stories": "сторис-контент",
    "story": "сторис-контент",
    "reels-контент": "reels",
    "asmr": "ASMR",
}

_CATEGORY_ALIASES: dict[str, str] = {
    "стиль одежды": "Мода",
}


def normalize_lookup_key(key: str) -> str:
    """Нормализовать ключ для сопоставления со справочником."""
    normalized = key.strip().lower()
    normalized = normalized.replace("ё", "е")
    normalized = normalized.replace("—", "-").replace("–", "-")
    normalized = normalized.replace("_", " ").replace("/", " ")
    normalized = normalized.replace("&", " ")
    return " ".join(normalized.split())


def _fuzzy_lookup(key: str, cache: dict[str, str], cutoff: float = 0.8) -> str | None:
    """Поиск в кэше: exact → normalized variants → fuzzy."""
    # 1. Exact
    if key in cache:
        return cache[key]

    # 2. Normalized variants
    normalized = normalize_lookup_key(key)
    variants = {
        normalized,
        normalized.replace("-", " "),
        normalized.replace(" ", "-"),
        normalized.replace("-", ""),
        normalized.replace(" ", ""),
    }
    for variant in variants:
        if variant in cache:
            return cache[variant]

    # 3. Fuzzy (difflib)
    matches = get_close_matches(normalized, cache.keys(), n=1, cutoff=cutoff)
    if matches:
        return cache[matches[0]]
    return None


async def load_categories(db: Client) -> dict[str, str]:
    """Загрузить все категории из БД.

    Возвращает {key: category_id} где:
    - key = code (для верхнеуровневых, AI возвращает код в primary_categories)
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
            categories[normalize_lookup_key(code)] = cat_id
        # Все категории — индексируем по name_lower
        name = c.get("name")
        if isinstance(name, str):
            categories[normalize_lookup_key(name)] = cat_id
    return categories


async def match_categories(
    db: Client,
    blog_id: str,
    insights: AIInsights,
    categories: dict[str, str] | None = None,
) -> dict[str, int]:
    """
    Сопоставить primary_categories и secondary_topics с таблицей categories.
    Записать в blog_categories.
    categories — кэш {name_lower: id}, если None — загружается из БД.
    """
    total_primary = len(insights.content.primary_categories)
    total_secondary = len(insights.content.secondary_topics)
    total = total_primary + total_secondary
    if total == 0:
        return {"total": 0, "matched": 0, "unmatched": 0}

    if categories is None:
        categories = await load_categories(db)

    # Собираем все записи для batch upsert
    rows: list[dict[str, str | bool]] = []
    seen_ids: set[str] = set()

    # Primary categories (первая = is_primary=True, остальные = is_primary=False)
    unmatched_count = 0
    for i, cat_code in enumerate(insights.content.primary_categories):
        cat_lower = normalize_lookup_key(cat_code)
        cat_lookup_key = _CATEGORY_ALIASES.get(cat_lower, cat_lower)
        cat_id = _fuzzy_lookup(cat_lookup_key, categories)
        if not cat_id:
            unmatched_count += 1
            logger.warning(
                f"[match_categories] primary_category '{cat_code}' "
                f"не найден в справочнике категорий "
                f"(normalized='{cat_lookup_key}', blog={blog_id})"
            )
            continue
        if cat_id in seen_ids:
            continue
        seen_ids.add(cat_id)
        rows.append({
            "blog_id": blog_id,
            "category_id": cat_id,
            "is_primary": i == 0,
        })

    # Secondary topics
    for topic in insights.content.secondary_topics:
        topic_lower = normalize_lookup_key(topic)
        topic_lookup_key = _CATEGORY_ALIASES.get(topic_lower, topic_lower)
        cat_id = _fuzzy_lookup(topic_lookup_key, categories)
        if not cat_id:
            unmatched_count += 1
            logger.warning(
                f"[match_categories] secondary_topic '{topic}' "
                f"не найден в справочнике категорий "
                f"(normalized='{topic_lookup_key}', blog={blog_id})"
            )
            continue
        if cat_id in seen_ids:
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

    return {
        "total": total,
        "matched": len(rows),
        "unmatched": unmatched_count,
    }


_EN_TO_RU_TAGS: dict[str, str] = {
    "video content": "видео-контент",
    "video-content": "видео-контент",
    "photo content": "фото-контент",
    "photo-content": "фото-контент",
    "carousels": "карусели",
    "stories content": "сторис-контент",
    "live streams": "прямые эфиры",
    "humor": "юмор",
    "aesthetic": "эстетика",
    "aesthetics": "эстетика",
    "educational": "образовательный",
    "motivational": "мотивационный",
    "storytelling": "сторителлинг",
    "before after": "до/после",
    "reviews": "обзоры",
    "tutorials": "туториалы",
    "lifehacks": "лайфхаки",
    "life hacks": "лайфхаки",
    "challenges": "челленджи",
    "collaborations": "коллаборации",
    "professional photography": "профессиональная съёмка",
    "daily posting": "ежедневный постинг",
    "woman": "женщина",
    "man": "мужчина",
    "married": "замужем/женат",
    "mom": "мама",
    "mother": "мама",
    "dad": "папа",
    "premium lifestyle": "премиум-образ жизни",
    "healthy lifestyle": "ЗОЖ",
    "minimalist": "минималист",
    "travels frequently": "путешествует часто",
    "dog": "собака",
    "cat": "кошка",
    "expert": "эксперт в нише",
    "brand safe": "brand safe",
    "low risk": "низкий риск",
    "family values": "семейные ценности",
    "positivity": "позитив",
    "ecology": "экология",
}


_CITY_GARBAGE_PATTERN = re.compile(r"\d|%|процент", re.IGNORECASE)
_COUNTRY_NAMES = {
    "казахстан", "россия", "узбекистан", "кыргызстан",
    "беларусь", "украина", "турция",
}


def is_valid_city(city_name: str) -> bool:
    """Проверить что строка похожа на город, а не на мусор."""
    if not city_name or len(city_name) < 2:
        return False
    if _CITY_GARBAGE_PATTERN.search(city_name):
        return False
    if city_name.strip().lower() in _COUNTRY_NAMES:
        return False
    return True


_CITY_ALIASES: dict[str, str] = {
    "алмата": "алматы",
    "алма-ата": "алматы",
    "алма ата": "алматы",
    "нур-султан": "астана",
    "нурсултан": "астана",
    "караганда": "караганды",
    "шымкент": "шымкент",
    "чимкент": "шымкент",
    "усть-каменогорск": "оскемен",
    "уральск": "орал",
    "актобе": "актобе",
    "актюбинск": "актобе",
    "семипалатинск": "семей",
    "кокшетау": "кокшетау",
    "кокчетав": "кокшетау",
    "петропавловск": "петропавл",
    "павлодар": "павлодар",
    "костанай": "костанай",
    "кустанай": "костанай",
    "тараз": "тараз",
    "джамбул": "тараз",
}


def normalize_brand(name: str) -> str:
    """Нормализация названия бренда: апострофы, пробелы."""
    # Убрать типографские апострофы → ASCII
    normalized = name.replace("\u2018", "'").replace("\u2019", "'")
    return normalized.strip()


async def load_cities(db: Client) -> dict[str, str]:
    """Загрузить города из БД.

    Возвращает {normalized_name: city_id} с индексацией по:
    - name (английское, например 'almaty')
    - l10n.ru (русское, например 'алматы')
    - l10n.kk (казахское)
    """
    result = await run_in_thread(
        db.table("cities").select("id, name, l10n").execute
    )
    cities: dict[str, str] = {}
    for c in result.data:
        if not isinstance(c, dict):
            continue
        city_id = c.get("id")
        if not isinstance(city_id, str):
            continue
        # Английское название
        name = c.get("name")
        if isinstance(name, str):
            cities[normalize_lookup_key(name)] = city_id
        # Локализованные названия (ru, kk, en)
        l10n = c.get("l10n")
        if isinstance(l10n, dict):
            for lang_name in l10n.values():
                if isinstance(lang_name, str) and lang_name:
                    cities[normalize_lookup_key(lang_name)] = city_id
    return cities


async def match_city(
    db: Client,
    blog_id: str,
    city_name: str,
    cities: dict[str, str],
) -> bool:
    """Сопоставить город из AI-анализа с таблицей cities и записать в blog_cities.

    Возвращает True если город успешно сопоставлен.
    """
    key = normalize_lookup_key(city_name)
    # Попробовать алиас
    key = _CITY_ALIASES.get(key, key)
    city_id = cities.get(key)
    if not city_id:
        return False

    # Upsert в blog_cities (idempotent)
    await run_in_thread(
        db.table("blog_cities").upsert(
            {"blog_id": blog_id, "city_id": city_id},
            on_conflict="blog_id,city_id",
        ).execute
    )
    return True


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
            tags[normalize_lookup_key(name)] = tag_id
    return tags


async def match_tags(
    db: Client,
    blog_id: str,
    insights: AIInsights,
    tags: dict[str, str] | None = None,
) -> dict[str, int]:
    """
    Сопоставить теги из AI-анализа с таблицей tags.
    Записать в blog_tags.
    tags — кэш {name_lower: id}, если None — загружается из БД.
    """
    if not insights.tags:
        return {"total": 0, "matched": 0, "unmatched": 0}

    if tags is None:
        tags = await load_tags(db)

    rows = []
    seen_tag_ids: set[str] = set()
    unmatched_count = 0
    for tag_name in insights.tags:
        tag_lower = normalize_lookup_key(tag_name)
        # EN→RU перевод, затем alias для русских вариантов
        if tag_lower in _EN_TO_RU_TAGS:
            tag_lower = _EN_TO_RU_TAGS[tag_lower]
        tag_lookup_key = _TAG_ALIASES.get(tag_lower, tag_lower)
        tag_id = _fuzzy_lookup(tag_lookup_key, tags)
        if not tag_id:
            unmatched_count += 1
            logger.warning(
                f"[match_tags] тег '{tag_name}' "
                f"не найден в справочнике тегов "
                f"(normalized='{tag_lookup_key}', blog={blog_id})"
            )
            continue
        if tag_id not in seen_tag_ids:
            seen_tag_ids.add(tag_id)
            rows.append({"blog_id": blog_id, "tag_id": tag_id})

    if rows:
        # DELETE + INSERT — два HTTP-запроса (не атомарно), поэтому
        # при constraint violation ретраим один раз.
        for attempt in range(2):
            await run_in_thread(
                db.table("blog_tags")
                .delete()
                .eq("blog_id", blog_id)
                .execute
            )
            try:
                await run_in_thread(
                    db.table("blog_tags")
                    .insert(rows)
                    .execute
                )
                break
            except Exception as e:
                if attempt == 0 and "23505" in str(e):
                    logger.warning(
                        f"[match_tags] Constraint violation для blog {blog_id}, "
                        f"повторная попытка delete+insert"
                    )
                    continue
                raise

    return {
        "total": len(insights.tags),
        "matched": len(rows),
        "unmatched": unmatched_count,
    }
