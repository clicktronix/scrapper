"""Типы для результатов БД — TypedDict вместо dict[str, Any]."""

from typing import Any, Literal, TypedDict

# Допустимые значения из DB CHECK constraints
type TaskType = Literal["full_scrape", "ai_analysis", "discover", "pre_filter"]
type TaskStatus = Literal["pending", "running", "done", "failed"]
type ScrapeStatus = Literal[
    "pending", "scraping", "analyzing", "ai_analyzed",
    "active", "failed", "private", "deleted", "deactivated",
    "ai_refused",
]
type ErTrend = Literal["growing", "stable", "declining"]


class TaskRecord(TypedDict):
    """Запись задачи из scrape_tasks."""

    id: str
    task_type: TaskType
    status: TaskStatus
    blog_id: str | None
    priority: int
    attempts: int
    max_attempts: int
    error_message: str | None
    payload: dict[str, Any]
    created_at: str
    started_at: str | None
    completed_at: str | None
    next_retry_at: str | None


class TaskListResult(TypedDict):
    """Результат fetch_tasks_list."""

    tasks: list[dict[str, Any]]
    total: int
    limit: int
    offset: int


class TaskListResultWithError(TaskListResult, total=False):
    """Результат fetch_tasks_list с опциональной ошибкой."""

    error: str
