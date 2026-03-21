"""
Одноразовый скрипт для исправления данных ai_insights в БД.

Фиксы:
1. country: English → русский (Kazakhstan → Казахстан)
2. posting_frequency: коррекция по фактическому posts_per_week
3. best_fit_industries: удаление префикса "подходит для"
4. not_suitable_for: удаление префикса "не подходит для"
5. tags: дедупликация
6. secondary_topics: дедупликация

Использование:
    uv run python -m src.cli.fix_insights              # все блоги
    uv run python -m src.cli.fix_insights --limit 100  # первые 100
    uv run python -m src.cli.fix_insights --dry-run    # без изменений
"""
import argparse
import asyncio
import re
from typing import Any, cast

from loguru import logger
from supabase import create_async_client

# Нормализация страны — единый источник в src.ai.normalize
from src.ai.normalize import build_city_map, normalize_city, normalize_country
from src.config import load_settings

# Паттерн для удаления префиксов индустрий
_PREFIX_RE = re.compile(r"^(?:п[oо]дх[oо]дит для |не подходит для )", re.IGNORECASE)


def _deduplicate(items: list[str]) -> list[str]:
    """Дедупликация списка с сохранением порядка."""
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _clean_industries(items: list[str]) -> list[str]:
    """Убрать префиксы 'подходит для' / 'не подходит для' и дедуплицировать."""
    cleaned: list[str] = []
    for item in items:
        cleaned.append(_PREFIX_RE.sub("", item))
    return _deduplicate(cleaned)


def _fix_posting_frequency(freq: str | None, ppw: float | None) -> str | None:
    """Переопределить posting_frequency по фактическому posts_per_week."""
    if ppw is None:
        return freq
    if ppw < 0.5:
        return "rare"
    if ppw < 1.5:
        return "weekly"
    if ppw < 5:
        return "several_per_week"
    return "daily"


def _fix_insights(
    insights: dict[str, Any],
    ppw: float | None,
    city_map: dict[str, str] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Исправить ai_insights, вернуть (fixed_insights, list_of_changes)."""
    changes: list[str] = []

    # 1. Country нормализация (используем общую функцию с fuzzy matching)
    bp = insights.get("blogger_profile", {})
    if isinstance(bp, dict):
        country = bp.get("country")
        if country and isinstance(country, str):
            normalized = normalize_country(country)
            if normalized and normalized != country:
                bp["country"] = normalized
                changes.append(f"country: '{country}' → '{normalized}'")

    # 1b. City нормализация
    if isinstance(bp, dict):
        city = bp.get("city")
        if city and isinstance(city, str):
            normalized_city = normalize_city(city, city_map)
            if normalized_city and normalized_city != city:
                bp["city"] = normalized_city
                changes.append(f"city: '{city}' → '{normalized_city}'")

    # 2. posting_frequency коррекция
    content = insights.get("content", {})
    if isinstance(content, dict):
        old_freq = content.get("posting_frequency")
        new_freq = _fix_posting_frequency(old_freq, ppw)
        if new_freq != old_freq:
            content["posting_frequency"] = new_freq
            changes.append(f"posting_frequency: '{old_freq}' → '{new_freq}'")

    # 3. Tags дедупликация
    tags = insights.get("tags")
    if isinstance(tags, list):
        deduped = _deduplicate(tags)
        if len(deduped) != len(tags):
            changes.append(f"tags: {len(tags)} → {len(deduped)} (дедупликация)")
            insights["tags"] = deduped

    # 4. secondary_topics дедупликация
    if isinstance(content, dict):
        topics = content.get("secondary_topics")
        if isinstance(topics, list):
            deduped = _deduplicate(topics)
            if len(deduped) != len(topics):
                changes.append(f"secondary_topics: {len(topics)} → {len(deduped)}")
                content["secondary_topics"] = deduped

    # 5. best_fit_industries — убрать префиксы + дедупликация
    mv = insights.get("marketing_value", {})
    if isinstance(mv, dict):
        bfi = mv.get("best_fit_industries")
        if isinstance(bfi, list):
            cleaned = _clean_industries(bfi)
            if cleaned != bfi:
                changes.append("best_fit_industries: очистка префиксов")
                mv["best_fit_industries"] = cleaned

        # 6. not_suitable_for — убрать префиксы + дедупликация
        nsf = mv.get("not_suitable_for")
        if isinstance(nsf, list):
            cleaned = _clean_industries(nsf)
            if cleaned != nsf:
                changes.append("not_suitable_for: очистка префиксов")
                mv["not_suitable_for"] = cleaned

    # 7. Дедупликация остальных списков
    ai = insights.get("audience_inference", {})
    if isinstance(ai, dict):
        for field in ("geo_mentions", "audience_interests"):
            items = ai.get(field)
            if isinstance(items, list):
                deduped = _deduplicate(items)
                if len(deduped) != len(items):
                    changes.append(f"{field}: дедупликация")
                    ai[field] = deduped

    if isinstance(content, dict):
        cl = content.get("content_language")
        if isinstance(cl, list):
            deduped = _deduplicate(cl)
            if len(deduped) != len(cl):
                changes.append("content_language: дедупликация")
                content["content_language"] = deduped

    return insights, changes


async def main(limit: int | None = None, dry_run: bool = False) -> None:
    """Основная функция: загрузить блоги, исправить insights, сохранить."""
    settings = load_settings()
    db = await create_async_client(settings.supabase_url, settings.supabase_service_key.get_secret_value())

    # Загружаем маппинг городов из БД
    cities_result = await db.table("cities").select("name, ascii_name, l10n").execute()
    city_map = build_city_map(cast(list[dict[str, object]], cities_result.data or []))
    logger.info(f"Загружено {len(city_map)} вариантов городов для нормализации")

    batch_size = 100
    offset = 0
    total_fixed = 0
    total_changes: dict[str, int] = {}

    while True:
        # Загружаем батч блогов (с retry при таймаутах)
        rows: list[Any] = []
        for attempt in range(3):
            try:
                query = (
                    db.table("blogs")
                    .select("id, ai_insights, posts_per_week")
                    .not_.is_("ai_insights", "null")
                    .range(offset, offset + batch_size - 1)
                )
                if limit:
                    remaining = limit - total_fixed
                    if remaining <= 0:
                        break
                    query = query.limit(min(batch_size, remaining))
                result = await query.execute()
                rows = result.data if isinstance(result.data, list) else []
                break
            except Exception as e:
                if attempt == 2:
                    logger.error(f"SELECT батч {offset}: ошибка после 3 попыток: {e}")
                    return
                logger.warning(f"SELECT батч {offset}: retry {attempt + 1}: {e}")
                await asyncio.sleep(2 * (attempt + 1))
        if not rows:
            break

        # Обрабатываем каждый блог
        updates: list[tuple[str, dict[str, Any]]] = []
        for row in rows:
            row_dict = cast(dict[str, Any], row)
            blog_id = row_dict["id"]
            insights = row_dict.get("ai_insights")
            ppw = row_dict.get("posts_per_week")

            if not isinstance(insights, dict):
                continue
            # Пропускаем refusal записи
            if insights.get("refusal_reason"):
                continue

            fixed, changes = _fix_insights(insights, ppw, city_map)
            if changes:
                updates.append((blog_id, fixed))
                for c in changes:
                    key = c.split(":")[0]
                    total_changes[key] = total_changes.get(key, 0) + 1

        # Сохраняем с retry при таймаутах
        if updates and not dry_run:
            for blog_id, fixed_insights in updates:
                for attempt in range(3):
                    try:
                        await db.table("blogs").update(
                            {"ai_insights": fixed_insights}
                        ).eq("id", blog_id).execute()
                        break
                    except Exception as e:
                        if attempt == 2:
                            logger.error(f"Blog {blog_id}: ошибка после 3 попыток: {e}")
                        else:
                            logger.warning(f"Blog {blog_id}: retry {attempt + 1}: {e}")
                            await asyncio.sleep(1 * (attempt + 1))

        total_fixed += len(updates)
        offset += batch_size

        if updates:
            logger.info(f"Батч {offset // batch_size}: {len(updates)} блогов исправлено")
        else:
            logger.debug(f"Батч {offset // batch_size}: без изменений")

        if limit and total_fixed >= limit:
            break

    logger.info(f"Готово. Исправлено {total_fixed} блогов. {'(dry-run)' if dry_run else ''}")
    logger.info(f"Изменения: {total_changes}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Исправление ai_insights в БД")
    parser.add_argument("--limit", type=int, default=None, help="Макс. блогов для обработки")
    parser.add_argument("--dry-run", action="store_true", help="Без изменений в БД")
    args = parser.parse_args()

    asyncio.run(main(limit=args.limit, dry_run=args.dry_run))
