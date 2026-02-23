"""Основной цикл воркера — polling + обработка задач."""
import asyncio
from typing import Any

from loguru import logger
from openai import AsyncOpenAI
from supabase import Client

from src.config import Settings
from src.database import fetch_pending_tasks, mark_task_failed
from src.platforms.base import BaseScraper
from src.worker.handlers import handle_ai_analysis, handle_discover, handle_full_scrape


async def process_task(
    db: Client,
    task: dict[str, Any],
    scrapers: dict[str, BaseScraper],
    openai_client: AsyncOpenAI,
    settings: Settings,
    semaphore: asyncio.Semaphore,
) -> None:
    """Обработать одну задачу с учётом семафора."""
    async with semaphore:
        task_type = task["task_type"]
        task_id = task["id"]
        attempts = task.get("attempts", 0)
        max_attempts = task.get("max_attempts", 3)
        logger.debug(f"Processing task {task_id}: type={task_type}, "
                     f"attempts={attempts}/{max_attempts}")

        try:
            if task_type == "full_scrape":
                # Определяем платформу (по умолчанию instagram)
                scraper = scrapers.get("instagram")
                if scraper is None:
                    logger.error(f"No scraper for task {task_id}")
                    await mark_task_failed(
                        db,
                        task_id,
                        attempts,
                        max_attempts,
                        "No scraper configured for platform 'instagram'",
                        retry=False,
                    )
                    return
                await handle_full_scrape(db, task, scraper, settings)

            elif task_type == "ai_analysis":
                await handle_ai_analysis(db, task, openai_client, settings)

            elif task_type == "discover":
                scraper = scrapers.get("instagram")
                if scraper is None:
                    logger.error(f"No scraper for discover task {task_id}")
                    await mark_task_failed(
                        db,
                        task_id,
                        attempts,
                        max_attempts,
                        "No scraper configured for discover task",
                        retry=False,
                    )
                    return
                await handle_discover(db, task, scraper, settings)

            else:
                logger.warning(f"Unknown task type: {task_type}")
                await mark_task_failed(
                    db,
                    task_id,
                    attempts,
                    max_attempts,
                    f"Unknown task type: {task_type}",
                    retry=False,
                )

        except Exception as e:
            logger.exception(f"Unhandled error in task {task_id}: {e}")


async def run_worker(
    db: Client,
    scrapers: dict[str, BaseScraper],
    settings: Settings,
    shutdown_event: asyncio.Event,
    openai_client: AsyncOpenAI | None = None,
) -> None:
    """
    Основной polling-цикл воркера.
    Берёт pending задачи, запускает обработку через asyncio.create_task.
    Останавливается по shutdown_event, дожидаясь завершения активных задач.
    """
    if openai_client is None:
        openai_client = AsyncOpenAI(api_key=settings.openai_api_key.get_secret_value())

    semaphore = asyncio.Semaphore(settings.worker_max_concurrent)
    active_tasks: set[asyncio.Task[None]] = set()
    # Задачи, которые уже в обработке — не берём повторно при следующем poll
    processing_ids: set[str] = set()

    logger.info(
        f"Worker started (poll={settings.worker_poll_interval}s, "
        f"concurrent={settings.worker_max_concurrent})"
    )

    def _on_task_done(task_id: str, t: asyncio.Task[None]) -> None:
        active_tasks.discard(t)
        processing_ids.discard(task_id)

    while not shutdown_event.is_set():
        try:
            tasks = await fetch_pending_tasks(db, limit=10)

            if tasks:
                logger.info(f"Fetched {len(tasks)} pending tasks")

            for task in tasks:
                task_id = task["id"]
                if task_id in processing_ids:
                    continue
                processing_ids.add(task_id)

                t = asyncio.create_task(
                    process_task(db, task, scrapers, openai_client, settings, semaphore)
                )
                active_tasks.add(t)
                t.add_done_callback(lambda done_t, tid=task_id: _on_task_done(tid, done_t))

        except Exception as e:
            logger.exception(f"Error in worker loop: {e}")

        # Ждём poll_interval или shutdown
        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=settings.worker_poll_interval,
            )
        except TimeoutError:
            pass  # Нормальный таймаут — продолжаем цикл

    # Graceful shutdown: дождаться завершения активных задач
    if active_tasks:
        logger.info(f"Waiting for {len(active_tasks)} active tasks to finish...")
        done, pending = await asyncio.wait(active_tasks, timeout=30)
        if pending:
            logger.warning(f"Cancelling {len(pending)} tasks that didn't finish in 30s")
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    logger.info("Worker shutting down")
