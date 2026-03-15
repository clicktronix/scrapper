"""Общие фикстуры и хелперы для тестов API."""
import time
from unittest.mock import AsyncMock, MagicMock


def _make_query_builder():
    """Создать мок query-builder'а Supabase.

    Все chainable-методы возвращают self, .execute() — AsyncMock.
    """
    builder = MagicMock(name="query_builder")
    builder.execute = AsyncMock(name="execute")
    # Chainable-методы возвращают builder
    for method in (
        "select", "eq", "neq", "gt", "lt", "gte", "lte",
        "insert", "update", "upsert", "delete",
        "order", "range", "limit", "in_",
    ):
        getattr(builder, method).return_value = builder
    return builder


def make_db_mock():
    """Создать мок AsyncClient с query-builder цепочкой.

    db.table(...) и db.rpc(...) возвращают отдельные query-builder'ы,
    чей .execute() — AsyncMock.
    """
    db = MagicMock(name="AsyncClient")
    db.table.return_value = _make_query_builder()
    db.rpc.return_value = _make_query_builder()
    return db


def make_settings():
    """Создать мок Settings с API-ключом."""
    settings = MagicMock()
    settings.scraper_api_key.get_secret_value.return_value = "sk-test-key"
    settings.rescrape_days = 60
    settings.rate_limit_max_requests = 60
    settings.rate_limit_window_seconds = 60
    settings.rate_limit_trust_forwarded_for = False
    settings.trusted_proxy_ip_list = []
    settings.api_docs_enabled = False
    return settings


def make_pool(total: int = 2, available: int = 1):
    """Создать мок AccountPool."""
    pool = MagicMock()
    pool.accounts = [MagicMock() for _ in range(total)]

    now = time.time()
    for i, acc in enumerate(pool.accounts):
        acc.cooldown_until = 0 if i < available else now + 9999
        acc.requests_this_hour = 0
        acc.hour_started_at = now
    pool.requests_per_hour = 30
    return pool


def make_app(pool=None, settings=None, db=None):
    """Создать FastAPI app с моками."""
    from src.api.app import create_app

    return create_app(
        db=db or make_db_mock(),
        pool=pool or make_pool(),
        settings=settings or make_settings(),
    )


# Общий заголовок авторизации
AUTH_HEADERS = {"Authorization": "Bearer sk-test-key"}
