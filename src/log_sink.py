"""Loguru sink для записи WARNING+ логов в Supabase."""

from collections.abc import Callable
from typing import Any

from loguru import logger as _fallback_logger
from supabase import Client

from src.database import sanitize_error


def create_supabase_sink(db: Client) -> Callable[[Any], None]:
    """Фабрика: вернуть sink-функцию, привязанную к db-клиенту."""

    def sink(message: Any) -> None:
        record = message.record
        if record["level"].no < 30:  # WARNING = 30
            return
        try:
            base_message = sanitize_error(str(record["message"]))
            exception = record.get("exception")
            if exception:
                safe_exception = sanitize_error(str(exception))
                sanitized_msg = f"{base_message} | exception={safe_exception}"
            else:
                sanitized_msg = base_message
            db.table("scrape_logs").insert({
                "level": record["level"].name,
                "module": record["name"],
                "message": sanitized_msg,
            }).execute()
        except Exception as e:
            _fallback_logger.opt(depth=1).trace(f"Supabase log sink error: {e}")

    return sink
