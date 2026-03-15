"""Loguru sink для записи WARNING+ логов в Supabase."""

import asyncio
import threading
import time
from collections.abc import Callable
from typing import Any

from loguru import logger as _fallback_logger
from supabase import AsyncClient

from src.database import sanitize_error

# Минимальный интервал между записями в Supabase (секунды).
# При шторме ошибок каждая WARNING генерирует ещё один
# HTTP-запрос к Supabase — rate limiting снижает нагрузку.
_MIN_WRITE_INTERVAL = 1.0


def create_supabase_sink(db: AsyncClient, loop: asyncio.AbstractEventLoop) -> Callable[[Any], None]:
    """Фабрика: sink-функция для loguru, пишет в Supabase через event loop.

    Sink вызывается из loguru-треда (enqueue=True), поэтому async запись
    планируется через asyncio.run_coroutine_threadsafe в основной event loop.
    Rate limiting: не чаще 1 записи в секунду.
    """
    last_write = 0.0
    lock = threading.Lock()

    async def _write(entry: dict[str, str]) -> None:
        try:
            await db.table("scrape_logs").insert(entry).execute()
        except Exception as e:
            _fallback_logger.opt(depth=1).trace(f"Supabase log sink error: {e}")

    def sink(message: Any) -> None:
        nonlocal last_write
        record = message.record
        if record["level"].no < 30:  # WARNING = 30
            return

        # Rate limiting — пропускаем если слишком частые записи
        now = time.monotonic()
        with lock:
            if now - last_write < _MIN_WRITE_INTERVAL:
                return
            last_write = now

        try:
            base_message = sanitize_error(str(record["message"]))
            exception = record.get("exception")
            if exception:
                safe_exception = sanitize_error(str(exception))
                sanitized_msg = f"{base_message} | exception={safe_exception}"
            else:
                sanitized_msg = base_message
            entry = {
                "level": record["level"].name,
                "module": record["name"],
                "message": sanitized_msg,
            }
            asyncio.run_coroutine_threadsafe(_write(entry), loop)
        except Exception as e:
            _fallback_logger.opt(depth=1).trace(f"Supabase log sink error: {e}")

    return sink
