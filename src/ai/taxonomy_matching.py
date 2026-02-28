"""Матчинг категорий, тегов и городов с таксономией из БД."""
import re
from difflib import get_close_matches

from loguru import logger
from supabase import Client

from src.ai.schemas import AIInsights
from src.database import run_in_thread

__all__ = [
    "normalize_lookup_key",
    "normalize_brand",
    "is_valid_city",
    "load_categories",
    "load_tags",
    "load_cities",
    "match_categories",
    "match_tags",
    "match_city",
]

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


_CITY_PREFIX_RE = re.compile(r"^(г\.|город)\s*", re.IGNORECASE)


def normalize_lookup_key(key: str) -> str:
    """Нормализовать ключ для сопоставления со справочником."""
    normalized = key.strip().lower()
    normalized = normalized.replace("ё", "е")
    normalized = normalized.replace("—", "-").replace("–", "-")
    normalized = normalized.replace("_", " ").replace("/", " ")
    normalized = normalized.replace("&", " ")
    # Убрать префикс «г.» / «город» (AI иногда возвращает «г. Алматы»)
    normalized = _CITY_PREFIX_RE.sub("", normalized).strip()
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
