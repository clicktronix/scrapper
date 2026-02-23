"""Pydantic-схемы для API скрапера."""
from typing import Any

from pydantic import BaseModel, Field, field_validator


class ScrapeRequest(BaseModel):
    """Запрос на создание full_scrape задач."""

    usernames: list[str] = Field(min_length=1, max_length=100)

    @field_validator("usernames")
    @classmethod
    def clean_usernames(cls, v: list[str]) -> list[str]:
        """Очистить и дедуплицировать username-ы."""
        cleaned: list[str] = []
        seen: set[str] = set()
        for name in v:
            name = name.strip().lstrip("@").lower()
            if name and name not in seen:
                cleaned.append(name)
                seen.add(name)
        if not cleaned:
            raise ValueError("usernames must not be empty after cleaning")
        return cleaned


class DiscoverRequest(BaseModel):
    """Запрос на создание discover задачи."""

    hashtag: str
    min_followers: int = Field(default=1000, ge=0)

    @field_validator("hashtag")
    @classmethod
    def clean_hashtag(cls, v: str) -> str:
        """Убрать # в начале, проверить что не пустой."""
        cleaned = v.strip().lstrip("#")
        if not cleaned:
            raise ValueError("hashtag must not be empty")
        return cleaned


class ScrapeTaskResult(BaseModel):
    """Результат создания одной задачи."""

    task_id: str | None
    username: str
    blog_id: str | None  # None при ошибке
    status: str  # "created" | "skipped" | "error"


class ScrapeResponse(BaseModel):
    """Ответ на POST /api/tasks/scrape."""

    created: int
    skipped: int
    tasks: list[ScrapeTaskResult]


class DiscoverResponse(BaseModel):
    """Ответ на POST /api/tasks/discover."""

    task_id: str | None
    hashtag: str


class TaskResponse(BaseModel):
    """Одна задача в ответе API."""

    id: str
    blog_id: str | None = None
    task_type: str
    status: str
    priority: int
    attempts: int = 0
    error_message: str | None = None
    payload: dict[str, Any] | None = None
    created_at: str | None = None
    started_at: str | None = None
    completed_at: str | None = None


class TaskListResponse(BaseModel):
    """Пагинированный список задач."""

    tasks: list[TaskResponse]
    total: int
    limit: int
    offset: int


class RetryResponse(BaseModel):
    """Ответ на POST /api/tasks/{id}/retry."""

    task_id: str
    status: str  # "retrying"


class HealthResponse(BaseModel):
    """Ответ healthcheck."""

    status: str
    accounts_total: int
    accounts_available: int
    tasks_running: int
    tasks_pending: int
