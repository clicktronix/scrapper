"""Supabase Storage для хранения Instagram-сессий."""
import asyncio
import json

from loguru import logger
from supabase import Client

BUCKET_NAME = "instagram-sessions"


async def load_session(db: Client, account_name: str) -> dict | None:
    """
    Загрузить сессию из Supabase Storage.
    Возвращает dict для cl.load_settings() или None.
    """
    try:
        data = await asyncio.to_thread(
            db.storage.from_(BUCKET_NAME).download, f"{account_name}.json"
        )
        parsed = json.loads(data)
        # Сессия должна быть dict — list/str/int/bool не валидны
        if not isinstance(parsed, dict):
            logger.debug(f"Invalid session format for {account_name}: expected dict, got {type(parsed).__name__}")
            return None
        return parsed
    except Exception as e:
        logger.debug(f"No session for {account_name}: {e}")
        return None


async def save_session(db: Client, account_name: str, settings: dict) -> None:
    """Сохранить сессию в Supabase Storage (перезаписать если есть)."""
    try:
        data = json.dumps(settings).encode()
        await asyncio.to_thread(
            db.storage.from_(BUCKET_NAME).upload,
            f"{account_name}.json",
            data,
            {"content-type": "application/json", "upsert": "true"},
        )
        logger.debug(f"Session saved for {account_name}")
    except Exception as e:
        logger.error(f"Failed to save session for {account_name}: {e}")
