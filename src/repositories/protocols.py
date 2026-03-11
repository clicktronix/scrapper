"""Протоколы репозиториев для DI."""

from typing import Any, Protocol

from src.models.db_types import TaskRecord


class TaskRepository(Protocol):
    """Интерфейс для операций с задачами."""

    async def mark_running(self, task_id: str) -> bool: ...

    async def mark_done(self, task_id: str) -> None: ...

    async def mark_failed(
        self,
        task_id: str,
        attempts: int,
        max_attempts: int,
        error: str,
        retry: bool = True,
    ) -> None: ...

    async def create_if_not_exists(
        self,
        blog_id: str | None,
        task_type: str,
        priority: int,
        payload: dict[str, Any] | None = None,
    ) -> str | None: ...

    async def fetch_pending(self, limit: int = 10) -> list[TaskRecord]: ...

    async def recover_stuck(
        self,
        max_running_minutes: int = 30,
        max_ai_running_minutes: int = 120,
    ) -> int: ...


class BlogRepository(Protocol):
    """Интерфейс для операций с блогами."""

    async def is_fresh(self, blog_id: str, min_days: int) -> bool: ...

    async def upsert(self, blog_id: str, data: dict[str, Any]) -> None: ...

    async def upsert_posts(
        self, blog_id: str, posts: list[dict[str, Any]]
    ) -> None: ...

    async def upsert_highlights(
        self, blog_id: str, highlights: list[dict[str, Any]]
    ) -> None: ...

    async def cleanup_orphan_person(self, person_id: str) -> None: ...
