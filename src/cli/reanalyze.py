"""
Одноразовый скрипт для перезапуска AI-анализа существующих блогеров.

Использование:
    uv run python -m src.cli.reanalyze              # все ai_analyzed блогеры
    uv run python -m src.cli.reanalyze --limit 50   # первые 50
    uv run python -m src.cli.reanalyze --dry-run    # без изменений, только вывод
"""
import argparse
import asyncio
import sys

from loguru import logger
from supabase import create_client

from src.config import load_settings
from src.database import create_task_if_not_exists, run_in_thread


async def reanalyze(limit: int | None = None, dry_run: bool = False) -> None:
    """Сбросить ai_insights и создать задачи переанализа."""
    settings = load_settings()
    db = create_client(settings.supabase_url, settings.supabase_service_key.get_secret_value())

    # Выбираем блогеров с завершённым AI-анализом
    query = (
        db.table("blogs")
        .select("id, username")
        .in_("scrape_status", ["ai_analyzed", "ai_refused"])
        .not_.is_("ai_insights", "null")
        .order("followers_count", desc=True)
    )
    if limit:
        query = query.limit(limit)

    result = await run_in_thread(query.execute)
    blogs = result.data

    if not blogs:
        logger.info("Нет блогеров для переанализа")
        return

    logger.info(f"Найдено {len(blogs)} блогеров для переанализа")

    if dry_run:
        for blog in blogs:
            logger.info(f"  [dry-run] @{blog.get('username', '?')} ({blog['id']})")
        logger.info(f"[dry-run] Было бы сброшено {len(blogs)} блогеров. Выход.")
        return

    # Сброс AI-полей батчами по 50
    blog_ids = [b["id"] for b in blogs]
    batch_size = 50
    for i in range(0, len(blog_ids), batch_size):
        batch = blog_ids[i:i + batch_size]
        await run_in_thread(
            db.table("blogs")
            .update({
                "ai_insights": None,
                "ai_analyzed_at": None,
                "embedding": None,
                "ai_confidence": None,
                "scrape_status": "active",
            })
            .in_("id", batch)
            .execute
        )
        logger.info(f"Сброшено {min(i + batch_size, len(blog_ids))}/{len(blog_ids)} блогеров")

    # Создание задач ai_analysis
    created = 0
    for blog in blogs:
        task_id = await create_task_if_not_exists(
            db, blog["id"], "ai_analysis", priority=5,
        )
        if task_id:
            created += 1

    logger.info(
        f"Готово: сброшено {len(blogs)} блогеров, "
        f"создано {created} задач ai_analysis. "
        f"Задачи будут обработаны воркером при следующем цикле."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Перезапуск AI-анализа блогеров")
    parser.add_argument("--limit", type=int, default=None, help="Максимум блогеров для переанализа")
    parser.add_argument("--dry-run", action="store_true", help="Только показать, не менять данные")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    asyncio.run(reanalyze(limit=args.limit, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
