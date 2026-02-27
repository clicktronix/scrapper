"""Loguru sink для записи WARNING+ логов в Supabase."""

from supabase import Client


def create_supabase_sink(db: Client):
    """Фабрика: вернуть sink-функцию, привязанную к db-клиенту."""

    def sink(message) -> None:
        record = message.record
        try:
            db.table("scrape_logs").insert({
                "level": record["level"].name,
                "module": record["name"],
                "message": str(record["message"]),
            }).execute()
        except Exception:
            pass  # Ошибка логирования не должна ронять приложение

    return sink
