"""Pydantic-схемы для API скрапера."""
import re
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

# Instagram username: латинские буквы, цифры, точка и подчёркивание
_USERNAME_RE = re.compile(r"^[a-z0-9._]+$")
# Хештег: буквы (вкл. кириллицу), цифры, подчёркивание
_HASHTAG_RE = re.compile(r"^[\w\u0400-\u04FF]+$")


def _clean_usernames(v: list[str]) -> list[str]:
    """Очистить, провалидировать и дедуплицировать username-ы."""
    cleaned: list[str] = []
    seen: set[str] = set()
    for name in v:
        name = name.strip().lstrip("@").lower()
        if not name or name in seen:
            continue
        if len(name) > 30:
            raise ValueError(f"username too long (max 30): {name}")
        if not _USERNAME_RE.match(name):
            raise ValueError(f"invalid username format: {name}")
        cleaned.append(name)
        seen.add(name)
    if not cleaned:
        raise ValueError("usernames must not be empty after cleaning")
    return cleaned


class ScrapeRequest(BaseModel):
    """Запрос на создание full_scrape задач."""

    usernames: list[str] = Field(min_length=1, max_length=100)

    @field_validator("usernames")
    @classmethod
    def clean_usernames(cls, v: list[str]) -> list[str]:
        return _clean_usernames(v)


class DiscoverRequest(BaseModel):
    """Запрос на создание discover задачи."""

    hashtag: str = Field(max_length=100)
    min_followers: int = Field(default=1000, ge=0)

    @field_validator("hashtag")
    @classmethod
    def clean_hashtag(cls, v: str) -> str:
        """Убрать # в начале, проверить формат."""
        cleaned = v.strip().lstrip("#")
        if not cleaned:
            raise ValueError("hashtag must not be empty")
        if not _HASHTAG_RE.match(cleaned):
            raise ValueError(f"invalid hashtag format: {cleaned}")
        return cleaned


class ScrapeTaskResult(BaseModel):
    """Результат создания одной задачи."""

    task_id: str | None
    username: str
    blog_id: str | None  # None при ошибке
    status: Literal["created", "skipped", "error"]
    reason: str | None = None  # Причина пропуска (deleted, deactivated)


class ScrapeResponse(BaseModel):
    """Ответ на POST /api/tasks/scrape."""

    created: int
    skipped: int
    errors: int = 0
    tasks: list[ScrapeTaskResult]


class PreFilterRequest(BaseModel):
    """Запрос на создание pre_filter задач."""

    usernames: list[str] = Field(min_length=1, max_length=100)

    # Переиспользуем ту же логику очистки, что и в ScrapeRequest
    @field_validator("usernames")
    @classmethod
    def clean_usernames(cls, v: list[str]) -> list[str]:
        return _clean_usernames(v)


class PreFilterResponse(BaseModel):
    """Ответ на POST /api/tasks/pre_filter."""

    created: int
    skipped: int
    errors: int = 0
    tasks: list[ScrapeTaskResult]


class DiscoverResponse(BaseModel):
    """Ответ на POST /api/tasks/discover."""

    task_id: str | None
    hashtag: str


class TaskResponse(BaseModel):
    """Одна задача в ответе API."""

    id: str
    blog_id: str | None = None
    task_type: Literal["full_scrape", "ai_analysis", "discover", "pre_filter"]
    status: Literal["pending", "running", "done", "failed"]
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
    status: Literal["retrying"]


class HealthResponse(BaseModel):
    """Ответ healthcheck."""

    status: Literal["ok", "degraded"]
    accounts_total: int
    accounts_available: int
    tasks_running: int
    tasks_pending: int
