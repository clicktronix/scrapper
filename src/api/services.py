"""Бизнес-логика API скрапера — вынесена из route handlers."""
import time
from typing import Any

from fastapi import Response
from loguru import logger
from postgrest.exceptions import APIError as PostgrestAPIError
from postgrest.types import CountMethod
from supabase import Client

from src.api.schemas import HealthResponse
from src.database import cleanup_orphan_person, run_in_thread
from src.platforms.instagram.client import AccountPool


async def find_blog_by_username(db: Client, username: str) -> str | None:
    """Найти блог по нормализованному username. Возвращает blog_id или None."""
    blog_result = await run_in_thread(
        db.table("blogs")
        .select("id")
        .eq("platform", "instagram")
        .eq("username", username)
        .execute
    )
    if blog_result.data:
        return blog_result.data[0]["id"]
    return None


async def find_or_create_blog(db: Client, username: str) -> str:
    """Найти существующий блог или создать person + blog. Защита от race condition."""
    normalized_username = username.strip().lstrip("@").lower()

    # Попытка найти существующий блог
    existing_id = await find_blog_by_username(db, normalized_username)
    if existing_id is not None:
        return existing_id

    # Создать person + blog
    person_result = await run_in_thread(
        db.table("persons")
        .insert({"full_name": normalized_username})
        .execute
    )
    person_id = person_result.data[0]["id"]

    try:
        blog_result = await run_in_thread(
            db.table("blogs")
            .insert({
                "person_id": person_id,
                "platform": "instagram",
                "username": normalized_username,
                "scrape_status": "pending",
            })
            .execute
        )
        return blog_result.data[0]["id"]
    except PostgrestAPIError:
        # Race condition: параллельный запрос уже создал блог (unique constraint) — ищем повторно
        retry_id = await find_blog_by_username(db, normalized_username)
        await cleanup_orphan_person(db, person_id)
        if retry_id is not None:
            return retry_id
        raise
    except Exception:
        # Любая другая ошибка — чистим orphan person перед пробросом
        await cleanup_orphan_person(db, person_id)
        raise


async def get_health_status(
    db: Client, pool: AccountPool | None, response: Response,
) -> HealthResponse:
    """Собрать данные о состоянии сервиса: аккаунты, задачи."""
    # При HikerAPI бэкенде AccountPool отсутствует
    if pool is not None:
        now = time.time()
        accounts_total = len(pool.accounts)
        accounts_available = sum(
            1 for acc in pool.accounts
            if acc.cooldown_until <= now
            and acc.requests_this_hour < pool.requests_per_hour
        )
    else:
        accounts_total = 0
        accounts_available = 0

    # Подсчёт задач из БД
    try:
        running = await run_in_thread(
            db.table("scrape_tasks")
            .select("id", count=CountMethod.exact)
            .eq("status", "running")
            .execute
        )
        pending = await run_in_thread(
            db.table("scrape_tasks")
            .select("id", count=CountMethod.exact)
            .eq("status", "pending")
            .execute
        )
        tasks_running = running.count or 0
        tasks_pending = pending.count or 0
    except Exception as e:
        logger.error(f"[health] Ошибка проверки БД: {e}")
        response.status_code = 503
        tasks_running = -1
        tasks_pending = -1
        status = "degraded"
    else:
        status = "ok"

    return HealthResponse(
        status=status,
        accounts_total=accounts_total,
        accounts_available=accounts_available,
        tasks_running=tasks_running,
        tasks_pending=tasks_pending,
    )


async def fetch_tasks_list(
    db: Client,
    status: str | None = None,
    task_type: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    """Получить список задач с фильтрами и пагинацией."""
    try:
        query = db.table("scrape_tasks").select("*", count=CountMethod.exact)
        if status:
            query = query.eq("status", status)
        if task_type:
            query = query.eq("task_type", task_type)
        query = query.order("created_at", desc=True).range(offset, offset + limit - 1)
        result = await run_in_thread(query.execute)
        return {
            "tasks": result.data,
            "total": result.count or 0,
            "limit": limit,
            "offset": offset,
        }
    except Exception as e:
        logger.error(f"[tasks_list] Ошибка получения задач: {e}")
        return {
            "tasks": [],
            "total": 0,
            "limit": limit,
            "offset": offset,
            "error": str(e),
        }
