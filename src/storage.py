"""Supabase Storage для хранения Instagram-сессий."""
import json
import re

from loguru import logger
from supabase import Client

from src.database import run_in_thread

BUCKET_NAME = "instagram-sessions"
MAX_SESSION_SIZE_BYTES = 500 * 1024
_ACCOUNT_NAME_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]{0,63}$")


def _build_session_file_path(account_name: str) -> str | None:
    """Build safe session file path for a storage object."""
    if not _ACCOUNT_NAME_RE.fullmatch(account_name):
        return None
    return f"{account_name}.json"


async def load_session(db: Client, account_name: str) -> dict | None:
    """
    Загрузить сессию из Supabase Storage.
    Возвращает dict для cl.load_settings() или None.
    """
    safe_path = _build_session_file_path(account_name)
    if safe_path is None:
        logger.warning(f"Unsafe account name for session load: {account_name!r}")
        return None
    try:
        data = await run_in_thread(
            db.storage.from_(BUCKET_NAME).download, safe_path
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
    safe_path = _build_session_file_path(account_name)
    if safe_path is None:
        logger.warning(f"Unsafe account name for session save: {account_name!r}")
        return
    try:
        data = json.dumps(settings).encode()
        if len(data) > MAX_SESSION_SIZE_BYTES:
            logger.error(
                f"Session payload too large for {account_name}: "
                f"{len(data)} bytes > {MAX_SESSION_SIZE_BYTES}"
            )
            return
        await run_in_thread(
            db.storage.from_(BUCKET_NAME).upload,
            safe_path,
            data,
            {"content-type": "application/json", "upsert": "true"},
        )
        logger.debug(f"Session saved for {account_name}")
    except Exception as e:
        logger.error(f"Failed to save session for {account_name}: {e}")
