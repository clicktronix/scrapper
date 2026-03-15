"""Supabase-реализация TaskRepository."""

from datetime import UTC, datetime, timedelta
from typing import Any, cast

from loguru import logger
from supabase import AsyncClient

from src.database import _extract_rpc_scalar, get_backoff_seconds, sanitize_error
from src.models.db_types import TaskRecord


def _as_dict_row(value: Any) -> dict[str, Any]:
    """Нормализовать элемент ответа Supabase к dict."""
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    return {}


class SupabaseTaskRepository:
    """Supabase-реализация операций с задачами."""

    def __init__(self, db: AsyncClient) -> None:
        self._db = db

    async def mark_running(self, task_id: str) -> bool:
        """Mark task as running; returns False when task is already claimed."""
        result = await self._db.rpc("mark_task_running", {
            "p_task_id": task_id,
            "p_started_at": datetime.now(UTC).isoformat(),
        }).execute()
        claimed_task_id = _extract_rpc_scalar(result.data)
        return claimed_task_id is not None

    async def mark_done(self, task_id: str) -> None:
        """Пометить задачу как done."""
        await self._db.table("scrape_tasks").update({
            "status": "done",
            "completed_at": datetime.now(UTC).isoformat(),
        }).eq("id", task_id).execute()

    async def mark_failed(
        self,
        task_id: str,
        attempts: int,
        max_attempts: int,
        error: str,
        retry: bool = True,
    ) -> None:
        """Пометить задачу как failed или pending (retry)."""
        safe_error = sanitize_error(error)

        if retry and attempts < max_attempts:
            backoff = get_backoff_seconds(attempts)
            next_retry = datetime.now(UTC) + timedelta(seconds=backoff)
            await self._db.table("scrape_tasks").update({
                "status": "pending",
                "error_message": safe_error,
                "next_retry_at": next_retry.isoformat(),
            }).eq("id", task_id).execute()
            logger.info(f"Task {task_id} retry in {backoff}s (attempt {attempts}/{max_attempts})")
        else:
            await self._db.table("scrape_tasks").update({
                "status": "failed",
                "error_message": safe_error,
            }).eq("id", task_id).execute()
            logger.error(f"Task {task_id} permanently failed: {safe_error}")

    async def create_if_not_exists(
        self,
        blog_id: str | None,
        task_type: str,
        priority: int,
        payload: dict[str, Any] | None = None,
    ) -> str | None:
        """Создать задачу через атомарную RPC-функцию."""
        result = await self._db.rpc("create_task_if_not_exists", {
            "p_blog_id": blog_id,
            "p_task_type": task_type,
            "p_priority": priority,
            "p_payload": payload or {},
        }).execute()

        task_id = _extract_rpc_scalar(result.data)
        if isinstance(task_id, str) and task_id:
            logger.info(f"Created task {task_type} for blog {blog_id}: {task_id}")
            return task_id

        logger.debug(f"Task {task_type} for blog {blog_id} already exists, skipping")
        return None

    async def fetch_pending(self, limit: int = 10) -> list[TaskRecord]:
        """Получить pending задачи, готовые к обработке."""
        now = datetime.now(UTC).isoformat()
        result = await self._db.table("scrape_tasks") \
            .select("*") \
            .eq("status", "pending") \
            .or_(f"next_retry_at.is.null,next_retry_at.lte.{now}") \
            .order("priority", desc=False) \
            .order("created_at", desc=False) \
            .limit(limit) \
            .execute()
        rows: list[TaskRecord] = []
        for raw in result.data or []:
            row = _as_dict_row(raw)
            if row:
                rows.append(cast(TaskRecord, row))

        if rows:
            types: dict[str, int] = {}
            for t in rows:
                tt = t.get("task_type", "?")
                types[tt] = types.get(tt, 0) + 1
            logger.debug(f"fetch_pending_tasks: {len(rows)} tasks ({types})")
        return rows

    async def recover_stuck(
        self,
        max_running_minutes: int = 30,
        max_ai_running_minutes: int = 1440,
    ) -> int:
        """Вернуть зависшие running задачи в pending."""
        threshold = (
            datetime.now(UTC) - timedelta(minutes=max_running_minutes)
        ).isoformat()
        ai_threshold = (
            datetime.now(UTC) - timedelta(minutes=max_ai_running_minutes)
        ).isoformat()

        # full_scrape / discover / pre_filter — короткий таймаут
        result = await self._db.table("scrape_tasks") \
            .select("id, task_type, attempts, max_attempts") \
            .eq("status", "running") \
            .in_("task_type", ["full_scrape", "discover", "pre_filter"]) \
            .lt("started_at", threshold) \
            .execute()

        # ai_analysis — длинный таймаут, т.к. completion_window=24h
        ai_result = await self._db.table("scrape_tasks") \
            .select("id, task_type, attempts, max_attempts") \
            .eq("status", "running") \
            .eq("task_type", "ai_analysis") \
            .lt("started_at", ai_threshold) \
            .execute()

        all_tasks = [_as_dict_row(t) for t in (result.data or []) + (ai_result.data or [])]
        all_tasks = [t for t in all_tasks if t]
        if not all_tasks:
            return 0

        recovered = 0
        for task in all_tasks:
            task_type = str(task.get("task_type", ""))
            task_id = str(task.get("id", ""))
            if not task_id:
                continue
            attempts = int(task.get("attempts", 0) or 0)
            max_attempts = int(task.get("max_attempts", 0) or 0)
            timeout = max_ai_running_minutes if task_type == "ai_analysis" else max_running_minutes
            if attempts >= max_attempts:
                await self._db.table("scrape_tasks").update({
                    "status": "failed",
                    "error_message": f"Stuck in running for >{timeout}min, max attempts exhausted",
                }).eq("id", task_id).execute()
            else:
                await self._db.table("scrape_tasks").update({
                    "status": "pending",
                    "error_message": f"Recovered: stuck in running for >{timeout}min",
                }).eq("id", task_id).execute()
                recovered += 1

        if recovered:
            logger.warning(f"Recovered {recovered} stuck tasks")
        return recovered
