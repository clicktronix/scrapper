"""Точка входа скрапера — инициализация и запуск API + воркера."""
import asyncio
import signal
import sys

import uvicorn
from loguru import logger
from openai import AsyncOpenAI
from supabase import create_client

from src.api.app import create_app
from src.config import load_settings
from src.log_sink import create_supabase_sink
from src.platforms.base import BaseScraper
from src.worker.loop import run_worker
from src.worker.scheduler import create_scheduler


async def main() -> None:
    """Инициализация и запуск API + воркера."""
    settings = load_settings()

    # Логирование
    logger.remove()
    logger.add(sys.stderr, level=settings.log_level)
    if settings.log_level == "DEBUG":
        logger.add("logs/scraper.log", rotation="100 MB", retention="7 days")

    logger.info("Starting scraper worker")

    # Supabase
    db = create_client(settings.supabase_url, settings.supabase_service_key.get_secret_value())

    # Персистить WARNING+ логи в Supabase
    logger.add(
        create_supabase_sink(db),
        level="WARNING",
        enqueue=True,
        serialize=False,
    )

    # OpenAI
    openai_client = AsyncOpenAI(api_key=settings.openai_api_key.get_secret_value())

    # Выбор бэкенда скрапера
    pool = None
    if settings.scraper_backend == "hikerapi" and settings.hikerapi_token:
        from src.platforms.instagram.hiker_scraper import HikerInstagramScraper

        scrapers: dict[str, BaseScraper] = {
            "instagram": HikerInstagramScraper(settings.hikerapi_token, settings),
        }
        logger.info("Using HikerAPI backend")
    else:
        from src.platforms.instagram.client import AccountPool
        from src.platforms.instagram.scraper import InstagramScraper

        pool = await AccountPool.create(db, settings)
        logger.info(f"Initialized {len(pool.accounts)} Instagram accounts")
        scrapers = {"instagram": InstagramScraper(pool, settings)}

    # FastAPI
    app = create_app(db, pool, settings)
    config = uvicorn.Config(app, host="0.0.0.0", port=settings.scraper_port, log_level="warning")
    server = uvicorn.Server(config)

    # Graceful shutdown
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    # APScheduler — крон-задачи (re-scrape, poll_batches, recover)
    scheduler = create_scheduler(db, settings, openai_client)
    scheduler.start()
    logger.info("Scheduler started")

    logger.info(f"API server starting on port {settings.scraper_port}")

    try:
        await asyncio.gather(
            server.serve(),
            run_worker(db, scrapers, settings, shutdown_event, openai_client),
        )
    finally:
        scheduler.shutdown(wait=False)
        if pool is not None:
            await pool.save_all_sessions(db)
        logger.info("Scraper stopped gracefully")


if __name__ == "__main__":
    asyncio.run(main())
