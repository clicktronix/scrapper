"""Pydantic-модель задачи скрапинга."""
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel


class ScrapeTask(BaseModel):
    """Задача из таблицы scrape_tasks."""

    id: str
    blog_id: str | None  # None для discover-задач
    task_type: Literal["full_scrape", "ai_analysis", "discover"]
    status: Literal["pending", "running", "done", "failed"]
    priority: int
    payload: dict[str, Any] = {}
    attempts: int = 0
    max_attempts: int = 3
    error_message: str | None = None
    next_retry_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime | None = None
