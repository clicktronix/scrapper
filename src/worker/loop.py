"""Основной цикл воркера — polling + обработка задач."""
import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from typing import Any

from loguru import logger
from openai import AsyncOpenAI
from supabase import Client

from src.config import Settings
from src.database import fetch_pending_tasks, mark_task_failed
from src.models.db_types import TaskRecord
from src.platforms.base import BaseScraper
from src.worker.handlers import (
    handle_ai_analysis,
    handle_discover,
    handle_full_scrape,
)
from src.worker.pre_filter_handler import handle_pre_filter

# Тип handler-функции для type safety
type TaskHandler = Callable[..., Coroutine[Any, Any, None]]

# Зависимости task_type → какие внешние сервисы нужны для обработчика.
# "scraper" — BaseScraper (instagram), "openai" — AsyncOpenAI.
TASK_DEPS: dict[str, list[str]] = {
    "full_scrape": ["scraper"],
    "ai_analysis": ["openai"],
    "discover": ["scraper"],
    "pre_filter": ["scraper"],
}


def _resolve_handler(task_type: str) -> TaskHandler | None:
    """Dispatch table: task_type → handler-функция.

    Dict строится при каждом вызове, поэтому mock.patch на module-level
    имена (handle_full_scrape и т.д.) корректно подменяет handlers.
    """
    dispatch: dict[str, TaskHandler] = {
        "full_scrape": handle_full_scrape,
        "ai_analysis": handle_ai_analysis,
        "discover": handle_discover,
        "pre_filter": handle_pre_filter,
    }
    return dispatch.get(task_type)


async def _get_scraper(
    scrapers: dict[str, BaseScraper],
    db: Client,
    task_id: str,
    attempts: int,
    max_attempts: int,
    task_type: str,
) -> BaseScraper | None:
    """Получить скрапер для instagram; при отсутствии — пометить задачу failed."""
    scraper = scrapers.get("instagram")
    if scraper is None:
        logger.error(f"No scraper for {task_type} task {task_id}")
        await mark_task_failed(
            db,
            task_id,
            attempts,
            max_attempts,
            f"No scraper configured for {task_type} task",
            retry=False,
        )
    return scraper


async def process_task(
    db: Client,
    task: TaskRecord,
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
            # Поиск обработчика
            handler = _resolve_handler(task_type)
            if handler is None:
                logger.warning(f"Unknown task type: {task_type}")
                await mark_task_failed(
                    db,
                    task_id,
                    attempts,
                    max_attempts,
                    f"Unknown task type: {task_type}",
                    retry=False,
                )
                return

            required_deps = TASK_DEPS.get(task_type, [])

            # Резолв зависимостей для конкретного handler
            resolved: dict[str, Any] = {}
            for dep_key in required_deps:
                if dep_key == "scraper":
                    scraper = await _get_scraper(
                        scrapers, db, task_id, attempts, max_attempts, task_type,
                    )
                    if scraper is None:
                        return
                    resolved["scraper"] = scraper
                elif dep_key == "openai":
                    resolved["openai"] = openai_client

            # Вызов handler с нужными аргументами
            if "scraper" in resolved:
                await handler(db, task, resolved["scraper"], settings)
            elif "openai" in resolved:
                await handler(db, task, resolved["openai"], settings)
            else:
                await handler(db, task, settings)

        except Exception as e:
            logger.exception(f"Unhandled error in task {task_id}")
            try:
                await mark_task_failed(
                    db, task_id, attempts, max_attempts,
                    f"Unhandled error: {e}", retry=True,
                )
            except Exception as fail_err:
                logger.error(f"Failed to mark task {task_id} as failed: {fail_err}")


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

                try:
                    t = asyncio.create_task(
                        process_task(db, task, scrapers, openai_client, settings, semaphore)
                    )
                except Exception:
                    processing_ids.discard(task_id)
                    raise

                active_tasks.add(t)
                t.add_done_callback(lambda done_t, tid=task_id: _on_task_done(tid, done_t))

        except Exception:
            logger.exception("Error in worker loop")

        # Ждём poll_interval или shutdown
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=settings.worker_poll_interval,
            )

    # Graceful shutdown: дождаться завершения активных задач
    if active_tasks:
        logger.info(f"Waiting for {len(active_tasks)} active tasks to finish...")
        _done, pending = await asyncio.wait(active_tasks, timeout=30)
        if pending:
            logger.warning(f"Cancelling {len(pending)} tasks that didn't finish in 30s")
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    logger.info("Worker shutting down")
