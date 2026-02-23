"""Общие фикстуры и хелперы для тестов API."""
import time
from unittest.mock import MagicMock


def make_settings():
    """Создать мок Settings с API-ключом."""
    settings = MagicMock()
    settings.scraper_api_key.get_secret_value.return_value = "sk-test-key"
    settings.rescrape_days = 60
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


def make_app(pool=None, settings=None):
    """Создать FastAPI app с моками."""
    from src.api.app import create_app

    return create_app(
        db=MagicMock(),
        pool=pool or make_pool(),
        settings=settings or make_settings(),
    )


# Общий заголовок авторизации
AUTH_HEADERS = {"Authorization": "Bearer sk-test-key"}
