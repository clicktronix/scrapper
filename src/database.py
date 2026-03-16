"""CRUD-операции с Supabase для скрапера."""
import asyncio
import re
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from loguru import logger
from supabase import AsyncClient

from src.models.db_types import TaskRecord


def sanitize_error(error: str) -> str:
    """Убрать потенциальные креденшалы из сообщения об ошибке."""
    # URL credentials (user:pass@host)
    result = re.sub(r"://[^@\s]+@", "://***:***@", error)
    # Bearer tokens
    result = re.sub(r"Bearer\s+\S+", "Bearer ***", result)
    # Query parameters с ключами (token, api_key, key, secret, password)
    result = re.sub(
        r"((?:token|api_key|key|secret|password|apikey)=)[^&\s]+",
        r"\1***",
        result,
        flags=re.IGNORECASE,
    )
    return result


def get_backoff_seconds(attempts: int) -> int:
    """
    Экспоненциальный backoff для retry.
    attempt 1 → 300с (5мин), attempt 2 → 900с (15мин), attempt 3 → 2700с (45мин).
    """
    return 300 * (3 ** max(0, attempts - 1))


def _extract_rpc_scalar(data: Any) -> Any:
    """Extract scalar value from Supabase RPC response."""
    if isinstance(data, list):
        if not data:
            return None
        list_data = cast(list[Any], data)
        first_item = list_data[0]
        if isinstance(first_item, dict):
            first_dict = cast(dict[str, Any], first_item)
            if not first_dict:
                return None
            if len(first_dict) == 1:
                return next(iter(first_dict.values()))
            return first_dict
        return first_item

    if isinstance(data, dict):
        typed_data = cast(dict[str, Any], data)
        if not typed_data:
            return None
        if len(typed_data) == 1:
            return next(iter(typed_data.values()))
        return typed_data

    return data


def _as_dict_row(value: Any) -> dict[str, Any]:
    """Нормализовать элемент ответа Supabase к dict."""
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    return {}


async def is_blog_fresh(db: AsyncClient, blog_id: str, min_days: int) -> bool:
    """Проверить, скрапился ли блог менее min_days дней назад."""
    threshold = (datetime.now(UTC) - timedelta(days=min_days)).isoformat()
    result = await (
        db.table("blogs")
        .select("scraped_at")
        .eq("id", blog_id)
        .gt("scraped_at", threshold)
        .limit(1)
        .execute()
    )
    return bool(result.data)


async def mark_task_running(db: AsyncClient, task_id: str) -> bool:
    """Mark task as running; returns False when task is already claimed."""
    result = await (
        db.rpc("mark_task_running", {
            "p_task_id": task_id,
            "p_started_at": datetime.now(UTC).isoformat(),
        }).execute()
    )
    claimed_task_id = _extract_rpc_scalar(result.data)
    return claimed_task_id is not None


async def mark_task_done(db: AsyncClient, task_id: str) -> None:
    """Пометить задачу как done."""
    await (
        db.table("scrape_tasks").update({
            "status": "done",
            "completed_at": datetime.now(UTC).isoformat(),
        }).eq("id", task_id).execute()
    )


async def mark_task_failed(
    db: AsyncClient,
    task_id: str,
    attempts: int,
    max_attempts: int,
    error: str,
    retry: bool = True,
) -> None:
    """
    Пометить задачу как failed или pending (retry).
    При retry=True и attempts < max_attempts → status='pending' с backoff.
    """
    safe_error = sanitize_error(error)

    if retry and attempts < max_attempts:
        backoff = get_backoff_seconds(attempts)
        next_retry = datetime.now(UTC) + timedelta(seconds=backoff)
        await (
            db.table("scrape_tasks").update({
                "status": "pending",
                "error_message": safe_error,
                "next_retry_at": next_retry.isoformat(),
            }).eq("id", task_id).execute()
        )
        logger.info(f"Task {task_id} retry in {backoff}s (attempt {attempts}/{max_attempts})")
    else:
        await (
            db.table("scrape_tasks").update({
                "status": "failed",
                "error_message": safe_error,
            }).eq("id", task_id).execute()
        )
        logger.error(f"Task {task_id} permanently failed: {safe_error}")


async def create_task_if_not_exists(
    db: AsyncClient,
    blog_id: str | None,
    task_type: str,
    priority: int,
    payload: dict[str, Any] | None = None,
) -> str | None:
    """
    Создать задачу через атомарную RPC-функцию.
    Проверка + вставка в одной транзакции (нет race condition).
    blog_id=None допустим для discover-задач.
    """
    result = await (
        db.rpc("create_task_if_not_exists", {
            "p_blog_id": blog_id,
            "p_task_type": task_type,
            "p_priority": priority,
            "p_payload": payload or {},
        }).execute()
    )

    task_id = _extract_rpc_scalar(result.data)
    if isinstance(task_id, str) and task_id:
        logger.info(f"Created task {task_type} for blog {blog_id}: {task_id}")
        return task_id

    logger.debug(f"Task {task_type} for blog {blog_id} already exists, skipping")
    return None


async def fetch_pending_tasks(db: AsyncClient, limit: int = 10) -> list[TaskRecord]:
    """Получить pending задачи, готовые к обработке (один запрос с or-фильтром)."""
    now = datetime.now(UTC).isoformat()
    result = await (
        db.table("scrape_tasks")
        .select("*")
        .eq("status", "pending")
        .or_(f"next_retry_at.is.null,next_retry_at.lte.{now}")
        .order("priority", desc=False)
        .order("created_at", desc=False)
        .limit(limit)
        .execute()
    )
    rows: list[TaskRecord] = []
    raw_data = result.data or []
    for raw in raw_data:
        row = _as_dict_row(raw)
        if row:
            rows.append(cast(TaskRecord, row))

    if rows:
        types: dict[str, int] = {}
        for t in rows:
            # t["task_type"] может отсутствовать в mock-данных — используем get с fallback
            tt: str = str(t.get("task_type", "?"))
            types[tt] = types.get(tt, 0) + 1
        logger.debug(f"fetch_pending_tasks: {len(rows)} tasks ({types})")
    return rows


async def recover_stuck_tasks(
    db: AsyncClient,
    max_running_minutes: int = 30,
    max_ai_running_minutes: int = 1440,
) -> int:
    """
    Вернуть зависшие running задачи в pending.

    full_scrape/discover — таймаут max_running_minutes (30 мин).
    ai_analysis — таймаут max_ai_running_minutes (по умолчанию 24ч).
      AI задачи зависают если handle_batch_results упал с исключением
      (например \u0000 в PostgreSQL). retry_stale_batches — последняя линия
      обороны (25ч, после окна OpenAI Batch API), а эта функция — основная.
    """
    threshold = (
        datetime.now(UTC) - timedelta(minutes=max_running_minutes)
    ).isoformat()
    ai_threshold = (
        datetime.now(UTC) - timedelta(minutes=max_ai_running_minutes)
    ).isoformat()

    # Параллельно: короткий таймаут для scrape/discover/pre_filter, длинный для ai
    raw_results = await asyncio.gather(
        db.table("scrape_tasks")
        .select("id, task_type, attempts, max_attempts")
        .eq("status", "running")
        .in_("task_type", ["full_scrape", "discover", "pre_filter"])
        .lt("started_at", threshold)
        .execute(),
        db.table("scrape_tasks")
        .select("id, task_type, attempts, max_attempts")
        .eq("status", "running")
        .eq("task_type", "ai_analysis")
        .lt("started_at", ai_threshold)
        .execute(),
        return_exceptions=True,
    )

    # Собираем результаты, пропуская ошибочные запросы
    combined_data: list[Any] = []
    for r in raw_results:
        if isinstance(r, BaseException):
            logger.error(f"[recover] Ошибка запроса stuck tasks: {r}")
            continue
        combined_data.extend(r.data or [])

    all_tasks = [_as_dict_row(t) for t in combined_data]
    all_tasks = [t for t in all_tasks if t]
    if not all_tasks:
        return 0

    # Параллельно обновляем все зависшие задачи
    async def _recover_one(task: dict[str, Any]) -> bool:
        task_type = str(task.get("task_type", ""))
        task_id = str(task.get("id", ""))
        if not task_id:
            return False
        attempts = int(task.get("attempts", 0) or 0)
        max_attempts = int(task.get("max_attempts", 0) or 0)
        timeout = max_ai_running_minutes if task_type == "ai_analysis" else max_running_minutes
        if attempts >= max_attempts:
            await (
                db.table("scrape_tasks").update({
                    "status": "failed",
                    "error_message": f"Stuck in running for >{timeout}min, max attempts exhausted",
                }).eq("id", task_id).execute()
            )
            return False
        await (
            db.table("scrape_tasks").update({
                "status": "pending",
                "error_message": f"Recovered: stuck in running for >{timeout}min",
            }).eq("id", task_id).execute()
        )
        return True

    results = await asyncio.gather(*[_recover_one(t) for t in all_tasks], return_exceptions=True)
    recovered = 0
    for i, r in enumerate(results):
        if isinstance(r, BaseException):
            task_id = all_tasks[i].get("id", "?")
            logger.error(f"[recover] Ошибка восстановления задачи {task_id}: {r}")
        elif r:
            recovered += 1

    if recovered:
        types: dict[str, int] = {}
        for t in all_tasks:
            tt = str(t.get("task_type", "?"))
            types[tt] = types.get(tt, 0) + 1
        logger.warning(f"Recovered {recovered} stuck tasks ({types})")
    return recovered


async def cleanup_orphan_person(db: AsyncClient, person_id: str) -> None:
    """Удалить person без привязанных блогов (best-effort cleanup)."""
    try:
        blogs = await (
            db.table("blogs").select("id").eq("person_id", person_id).limit(1).execute()
        )
        if not blogs.data:
            await (
                db.table("persons").delete().eq("id", person_id).execute()
            )
    except Exception as e:
        logger.warning(f"[cleanup_orphan] Ошибка очистки person {person_id}: {e}")


async def upsert_blog(db: AsyncClient, blog_id: str, data: dict[str, Any]) -> None:
    """Обновить данные блога из скрапинга."""
    await (
        db.table("blogs").update(data).eq("id", blog_id).execute()
    )


async def upsert_posts(db: AsyncClient, blog_id: str, posts: list[dict[str, Any]]) -> None:
    """Upsert постов блогера. ON CONFLICT (blog_id, platform_id) DO UPDATE."""
    if not posts:
        return
    rows = [{**post, "blog_id": blog_id} for post in posts]
    await (
        db.table("blog_posts").upsert(
            rows, on_conflict="blog_id,platform_id"
        ).execute()
    )


async def upsert_highlights(db: AsyncClient, blog_id: str, highlights: list[dict[str, Any]]) -> None:
    """Upsert хайлайтов блогера."""
    if not highlights:
        return
    rows = [{**h, "blog_id": blog_id} for h in highlights]
    await (
        db.table("blog_highlights").upsert(
            rows, on_conflict="blog_id,platform_id"
        ).execute()
    )
