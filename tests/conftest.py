"""Общие фикстуры и фабрики для тестов скрапера."""
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from src.models.blog import ScrapedHighlight, ScrapedPost, ScrapedProfile


def make_settings(**overrides: Any) -> MagicMock:
    """Фабрика мок-объекта Settings."""
    settings = MagicMock()
    settings.supabase_url = "https://test.supabase.co"
    settings.batch_min_size = 5
    settings.rescrape_days = 30
    for k, v in overrides.items():
        setattr(settings, k, v)
    return settings


def make_task(
    task_type: str = "full_scrape",
    blog_id: str = "blog-1",
    **kwargs: Any,
) -> dict[str, Any]:
    """Фабрика task-словаря для воркера."""
    return {
        "id": kwargs.pop("task_id", "task-1"),
        "blog_id": blog_id,
        "task_type": task_type,
        "status": "pending",
        "priority": kwargs.pop("priority", 5),
        "payload": kwargs.pop("payload", {}),
        "attempts": kwargs.pop("attempts", 1),
        "max_attempts": kwargs.pop("max_attempts", 3),
        **kwargs,
    }


def make_db_mock() -> MagicMock:
    """Фабрика chained Supabase-мока (table→select→eq→...→execute)."""
    db = MagicMock()
    table_mock = MagicMock()
    db.table.return_value = table_mock
    table_mock.select.return_value = table_mock
    table_mock.eq.return_value = table_mock
    table_mock.neq.return_value = table_mock
    table_mock.in_.return_value = table_mock
    table_mock.not_.is_.return_value = table_mock
    table_mock.not_.in_.return_value = table_mock
    table_mock.lt.return_value = table_mock
    table_mock.gt.return_value = table_mock
    table_mock.or_.return_value = table_mock
    table_mock.is_.return_value = table_mock
    table_mock.update.return_value = table_mock
    table_mock.insert.return_value = table_mock
    table_mock.upsert.return_value = table_mock
    table_mock.delete.return_value = table_mock
    table_mock.order.return_value = table_mock
    table_mock.limit.return_value = table_mock
    table_mock.execute = AsyncMock(return_value=MagicMock(data=[]))
    rpc_mock = MagicMock()
    rpc_mock.execute = AsyncMock(return_value=MagicMock())
    db.rpc.return_value = rpc_mock
    return db


def make_scraped_profile(**overrides: Any) -> ScrapedProfile:
    """Фабрика ScrapedProfile с разумными дефолтами."""
    defaults: dict[str, Any] = {
        "platform_id": "12345",
        "username": "testblogger",
        "full_name": "Test Blogger",
        "biography": "Test bio",
        "follower_count": 50000,
        "following_count": 500,
        "media_count": 200,
        "is_verified": False,
        "is_business": True,
        "avg_er": 3.5,
        "avg_er_reels": 5.0,
        "er_trend": "stable",
        "posts_per_week": 2.5,
        "medias": [
            ScrapedPost(
                platform_id="p1",
                media_type=1,
                caption_text="Test",
                like_count=1000,
                comment_count=50,
                taken_at=datetime(2026, 1, 15, tzinfo=UTC),
            ),
        ],
        "highlights": [
            ScrapedHighlight(
                platform_id="h1",
                title="Дети",
                media_count=5,
            ),
        ],
    }
    defaults.update(overrides)
    return ScrapedProfile(**defaults)
