"""Бизнес-логика API скрапера — вынесена из route handlers."""
import asyncio
import time
from typing import Any, cast

from fastapi import Response
from loguru import logger
from postgrest.exceptions import APIError as PostgrestAPIError
from postgrest.types import CountMethod
from supabase import Client

from src.api.schemas import HealthResponse
from src.database import cleanup_orphan_person, run_in_thread
from src.models.db_types import TaskListResultWithError
from src.platforms.instagram.client import AccountPool


def _is_unique_violation(error: PostgrestAPIError) -> bool:
    """Return True when PostgREST error indicates unique constraint conflict."""
    code = getattr(error, "code", None)
    if code == "23505":
        return True

    if error.args and isinstance(error.args[0], dict):
        payload = cast(dict[str, Any], error.args[0])
        return payload.get("code") == "23505"
    return False


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
        first_row = cast(dict[str, Any], blog_result.data[0]) if isinstance(blog_result.data[0], dict) else {}
        blog_id = first_row.get("id")
        if isinstance(blog_id, str):
            return blog_id
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
    first_person = (
        cast(dict[str, Any], person_result.data[0])
        if person_result.data and isinstance(person_result.data[0], dict) else {}
    )
    person_id_value = first_person.get("id")
    if not isinstance(person_id_value, str):
        raise ValueError("Invalid persons insert response: missing id")
    person_id = person_id_value

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
        first_blog = (
            cast(dict[str, Any], blog_result.data[0])
            if blog_result.data and isinstance(blog_result.data[0], dict) else {}
        )
        blog_id = first_blog.get("id")
        if not isinstance(blog_id, str):
            raise ValueError("Invalid blogs insert response: missing id")
        return blog_id
    except PostgrestAPIError as error:
        await cleanup_orphan_person(db, person_id)
        if _is_unique_violation(error):
            # Race condition: параллельный запрос уже создал блог — ищем повторно.
            retry_id = await find_blog_by_username(db, normalized_username)
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

    # Подсчёт задач из БД — параллельно
    try:
        running, pending = await asyncio.gather(
            run_in_thread(
                db.table("scrape_tasks")
                .select("id", count=CountMethod.exact)
                .eq("status", "running")
                .execute
            ),
            run_in_thread(
                db.table("scrape_tasks")
                .select("id", count=CountMethod.exact)
                .eq("status", "pending")
                .execute
            ),
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
) -> TaskListResultWithError:
    """Получить список задач с фильтрами и пагинацией."""
    try:
        query = db.table("scrape_tasks").select("*", count=CountMethod.exact)
        if status:
            query = query.eq("status", status)
        if task_type:
            query = query.eq("task_type", task_type)
        query = query.order("created_at", desc=True).range(offset, offset + limit - 1)
        result = await run_in_thread(query.execute)
        rows: list[dict[str, Any]] = []
        for raw in result.data or []:
            if isinstance(raw, dict):
                rows.append(cast(dict[str, Any], raw))
        return {
            "tasks": rows,
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
