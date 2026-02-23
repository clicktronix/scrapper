"""APScheduler cron-задачи для скрапера."""
from datetime import UTC, datetime, timedelta
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
from openai import AsyncOpenAI
from supabase import Client

from src.config import Settings
from src.database import (
    create_task_if_not_exists,
    mark_task_failed,
    recover_stuck_tasks,
    run_in_thread,
)
from src.image_storage import delete_blog_images
from src.worker.handlers import handle_batch_results


async def schedule_updates(db: Client, settings: Settings) -> None:
    """Создать задачи re-scrape для блогов, где scraped_at > rescrape_days дней."""
    threshold = (datetime.now(UTC) - timedelta(days=settings.rescrape_days)).isoformat()

    result = await run_in_thread(
        db.table("blogs")
        .select("id")
        .eq("scrape_status", "active")
        .or_(f"scraped_at.is.null,scraped_at.lt.{threshold}")
        .order("followers_count", desc=True)
        .limit(100)
        .execute
    )

    created = 0
    for blog in result.data:
        task_id = await create_task_if_not_exists(
            db, blog["id"], "full_scrape", priority=8
        )
        if task_id:
            created += 1

    logger.info(f"Scheduled {created} blog re-scrape tasks")


async def poll_batches(db: Client, openai_client: AsyncOpenAI) -> None:
    """Проверить статус running ai_analysis батчей."""
    logger.debug("[poll_batches] Checking running ai_analysis tasks...")
    result = await run_in_thread(
        db.table("scrape_tasks")
        .select("id, blog_id, payload, attempts, max_attempts")
        .eq("task_type", "ai_analysis")
        .eq("status", "running")
        .execute
    )

    if not result.data:
        logger.debug("[poll_batches] No running ai_analysis tasks")
        return

    # Группируем по batch_id
    # Значение:
    # {blog_id: {"id": task_id, "attempts": N, "max_attempts": M}}
    # или при коллизиях blog_id:
    # {blog_id: [ {"id": ...}, {"id": ...} ]}
    batches: dict[str, dict[str, Any]] = {}
    for task in result.data:
        batch_id = (task.get("payload") or {}).get("batch_id")
        if batch_id:
            if batch_id not in batches:
                batches[batch_id] = {}
            task_info = {
                "id": task["id"],
                "attempts": task.get("attempts", 1),
                "max_attempts": task.get("max_attempts", 3),
            }
            blog_id = task["blog_id"]
            existing = batches[batch_id].get(blog_id)
            if existing is None:
                batches[batch_id][blog_id] = task_info
            elif isinstance(existing, list):
                existing.append(task_info)
            else:
                batches[batch_id][blog_id] = [existing, task_info]

    logger.debug(f"[poll_batches] Found {len(batches)} active batches, "
                 f"{len(result.data)} running tasks")
    for batch_id, task_ids_by_blog in batches.items():
        logger.debug(f"[poll_batches] Processing batch {batch_id} "
                     f"({len(task_ids_by_blog)} blogs)")
        try:
            await handle_batch_results(db, openai_client, batch_id, task_ids_by_blog)
        except Exception as e:
            logger.error(f"Error polling batch {batch_id}: {e}")


async def retry_stale_batches(db: Client, openai_client: AsyncOpenAI, settings: Settings) -> None:
    """Пересобрать батчи, которые не завершились за 26 часов."""
    threshold = (datetime.now(UTC) - timedelta(hours=26)).isoformat()

    result = await run_in_thread(
        db.table("scrape_tasks")
        .select("id, blog_id, payload, attempts, max_attempts")
        .eq("task_type", "ai_analysis")
        .eq("status", "running")
        .lt("started_at", threshold)
        .execute
    )

    if not result.data:
        return

    for task in result.data:
        await mark_task_failed(
            db, task["id"], task["attempts"], task["max_attempts"],
            "Batch not completed in 26h", retry=True,
        )

    logger.warning(f"Retried {len(result.data)} stale AI batch tasks")


async def cleanup_old_images(db: Client, settings: Settings) -> None:
    """Удалить изображения блогов с scraped_at > rescrape_days дней назад."""
    threshold = (datetime.now(UTC) - timedelta(days=settings.rescrape_days)).isoformat()

    result = await run_in_thread(
        db.table("blogs")
        .select("id")
        .lt("scraped_at", threshold)
        .limit(100)
        .execute
    )

    if not result.data:
        logger.debug("[cleanup_images] Нет старых блогов для очистки")
        return

    total_deleted = 0
    for blog in result.data:
        deleted = await delete_blog_images(db, blog["id"])
        total_deleted += deleted

    logger.info(f"[cleanup_images] Удалено {total_deleted} изображений из {len(result.data)} блогов")


async def recover_tasks(db: Client) -> None:
    """Вернуть зависшие running задачи в pending (full_scrape, discover)."""
    await recover_stuck_tasks(db, max_running_minutes=30)


def create_scheduler(
    db: Client,
    settings: Settings,
    openai_client: AsyncOpenAI | None = None,
) -> AsyncIOScheduler:
    """Создать и настроить APScheduler."""
    scheduler = AsyncIOScheduler(
        job_defaults={
            # Дефолтный misfire_grace_time=1с слишком мало для async job'ов —
            # при задержке event loop job'ы будут тихо пропускаться.
            # None = без ограничения (job всегда выполнится при опоздании).
            "misfire_grace_time": None,
            "coalesce": True,
        }
    )

    # Ежедневно в 3:00 — обновление старых профилей
    scheduler.add_job(
        schedule_updates,
        "cron",
        hour=3,
        kwargs={"db": db, "settings": settings},
        id="schedule_updates",
    )

    # Каждые 15 минут — проверка батчей
    if openai_client:
        scheduler.add_job(
            poll_batches,
            "interval",
            minutes=15,
            kwargs={"db": db, "openai_client": openai_client},
            id="poll_batches",
        )

        # Каждые 2 часа — ретрай зависших батчей
        scheduler.add_job(
            retry_stale_batches,
            "interval",
            hours=2,
            kwargs={"db": db, "openai_client": openai_client, "settings": settings},
            id="retry_stale_batches",
        )

    # Каждые 10 минут — recovery зависших running задач
    scheduler.add_job(
        recover_tasks,
        "interval",
        minutes=10,
        kwargs={"db": db},
        id="recover_tasks",
    )

    # Еженедельно в воскресенье 4:00 — очистка старых изображений
    scheduler.add_job(
        cleanup_old_images,
        "cron",
        day_of_week="sun",
        hour=4,
        kwargs={"db": db, "settings": settings},
        id="cleanup_old_images",
    )

    return scheduler
