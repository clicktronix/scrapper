"""Бизнес-логика API скрапера — вынесена из route handlers."""
import asyncio
import time
from typing import Any, cast, get_args

from apscheduler.job import Job
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Response
from loguru import logger
from postgrest.exceptions import APIError as PostgrestAPIError
from postgrest.types import CountMethod
from supabase import Client

from src.api.schemas import HealthResponse, QueueDepthItem
from src.database import cleanup_orphan_person, run_in_thread
from src.models.db_types import TaskListResultWithError, TaskType
from src.platforms.instagram.client import AccountPool
from src.worker.scheduler import get_last_run_times

# Извлекаем допустимые task_type из Literal-типа, чтобы не дублировать список
_TASK_TYPES: tuple[str, ...] = get_args(TaskType.__value__)


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

    # Глубина очереди по task_type
    queue_depth = await _get_queue_depth(db)

    return HealthResponse(
        status=status,
        accounts_total=accounts_total,
        accounts_available=accounts_available,
        tasks_running=tasks_running,
        tasks_pending=tasks_pending,
        queue_depth=queue_depth,
    )


# Маппинг job.id → (человекочитаемое имя, описание интервала)
JOB_NAMES: dict[str, tuple[str, str]] = {
    "schedule_updates": ("Schedule Updates", "daily at 03:00"),
    "poll_batches": ("Poll AI Batches", "every 15 min"),
    "retry_stale_batches": ("Retry Stale Batches", "every 2 hours (25h threshold)"),
    "cleanup_old_images": ("Cleanup Old Images", "weekly Sun 04:00"),
    "retry_missing_embeddings": ("Retry Missing Embeddings", "every 1 hour"),
    "retry_taxonomy_mappings": ("Retry Taxonomy Mappings", "every 2 hours"),
    "audit_taxonomy_drift": ("Audit Taxonomy Drift", "daily at 05:00"),
    "recover_tasks": ("Recover Stuck Tasks", "every 10 min"),
}


def get_scheduler_status(scheduler: AsyncIOScheduler) -> list[dict[str, Any]]:
    """Собрать статус каждой задачи планировщика."""
    last_runs = get_last_run_times()
    jobs: list[dict[str, Any]] = []

    raw_jobs = cast(list[Job], scheduler.get_jobs())
    for job in raw_jobs:
        job_id = str(job.id)
        name, interval = JOB_NAMES.get(job_id, (job_id, "unknown"))
        next_run = str(job.next_run_time.isoformat()) if job.next_run_time else None
        last_run = last_runs.get(job_id)
        status = "ok" if last_run else "unknown"
        jobs.append({
            "id": job_id,
            "name": name,
            "interval": interval,
            "last_run_at": last_run,
            "next_run_at": next_run,
            "status": status,
        })

    return jobs


async def _get_queue_depth(db: Client) -> dict[str, QueueDepthItem] | None:
    """Получить глубину очереди по task_type. Graceful degradation при ошибках."""
    try:
        # Пробуем RPC (быстрее — один запрос)
        rpc_result = await run_in_thread(
            db.rpc("get_queue_depth", {}).execute
        )
        rpc_rows = rpc_result.data
        if isinstance(rpc_rows, list) and rpc_rows:
            depth: dict[str, QueueDepthItem] = {}
            for raw_row in rpc_rows:
                if not isinstance(raw_row, dict):
                    continue
                row_dict = cast(dict[str, Any], raw_row)
                task_type = str(row_dict.get("task_type", ""))
                rpc_status = str(row_dict.get("status", ""))
                cnt = int(row_dict.get("cnt", 0))
                if task_type not in depth:
                    depth[task_type] = QueueDepthItem()
                if rpc_status == "pending":
                    depth[task_type].pending = cnt
                elif rpc_status == "running":
                    depth[task_type].running = cnt
            return depth
    except Exception as e:
        logger.debug(f"[health] RPC get_queue_depth недоступна, fallback к count-запросам: {e}")

    # Fallback: параллельные count-запросы (pending + running) по каждому task_type
    try:
        task_types = list(_TASK_TYPES)
        coros = []
        for tt in task_types:
            coros.append(
                run_in_thread(
                    db.table("scrape_tasks")
                    .select("id", count=CountMethod.exact)
                    .eq("task_type", tt)
                    .eq("status", "pending")
                    .execute
                )
            )
            coros.append(
                run_in_thread(
                    db.table("scrape_tasks")
                    .select("id", count=CountMethod.exact)
                    .eq("task_type", tt)
                    .eq("status", "running")
                    .execute
                )
            )

        results = await asyncio.gather(*coros, return_exceptions=True)
        depth_fb: dict[str, QueueDepthItem] = {}
        for i, tt in enumerate(task_types):
            pending_res = results[i * 2]
            running_res = results[i * 2 + 1]
            p = (pending_res.count or 0) if not isinstance(pending_res, BaseException) else 0
            r = (running_res.count or 0) if not isinstance(running_res, BaseException) else 0
            depth_fb[tt] = QueueDepthItem(pending=p, running=r)
        return depth_fb
    except Exception as e:
        logger.error(f"[health] Ошибка получения queue_depth: {e}")
        return None


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
