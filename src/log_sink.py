"""Loguru sink для записи WARNING+ логов в Supabase."""

from supabase import Client

from src.database import sanitize_error


def create_supabase_sink(db: Client):
    """Фабрика: вернуть sink-функцию, привязанную к db-клиенту."""

    def sink(message) -> None:
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
        except Exception:
            pass  # Ошибка логирования не должна ронять приложение

    return sink
