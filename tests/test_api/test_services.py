"""Тесты сервисных функций API — services.py."""
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.api.schemas import HealthResponse
from src.api.services import find_blog_by_username, get_health_status


class TestFindBlogByUsername:
    """Тесты find_blog_by_username."""

    @pytest.mark.asyncio
    async def test_returns_blog_id_when_found(self) -> None:
        """Блог найден → возвращает id."""
        db = MagicMock()
        with patch("src.api.services.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = MagicMock(data=[{"id": "blog-123"}])
            result = await find_blog_by_username(db, "testuser")

        assert result == "blog-123"

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self) -> None:
        """Блог не найден → возвращает None."""
        db = MagicMock()
        with patch("src.api.services.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = MagicMock(data=[])
            result = await find_blog_by_username(db, "nonexistent")

        assert result is None


class TestGetHealthStatus:
    """Тесты get_health_status."""

    @pytest.mark.asyncio
    async def test_ok_with_pool(self) -> None:
        """Статус ok при наличии pool и живой БД."""
        db = MagicMock()
        pool = MagicMock()
        now = time.time()
        pool.accounts = [MagicMock(), MagicMock()]
        pool.accounts[0].cooldown_until = 0
        pool.accounts[0].requests_this_hour = 0
        pool.accounts[1].cooldown_until = now + 9999
        pool.accounts[1].requests_this_hour = 0
        pool.requests_per_hour = 30
        response = MagicMock()

        with patch("src.api.services.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = MagicMock(count=3)
            result = await get_health_status(db, pool, response)

        assert isinstance(result, HealthResponse)
        assert result.status == "ok"
        assert result.accounts_total == 2
        assert result.accounts_available == 1
        assert result.tasks_running == 3
        assert result.tasks_pending == 3

    @pytest.mark.asyncio
    async def test_ok_without_pool(self) -> None:
        """Статус ok без pool (HikerAPI бэкенд)."""
        db = MagicMock()
        response = MagicMock()

        with patch("src.api.services.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = MagicMock(count=0)
            result = await get_health_status(db, None, response)

        assert result.status == "ok"
        assert result.accounts_total == 0
        assert result.accounts_available == 0

    @pytest.mark.asyncio
    async def test_degraded_on_db_error(self) -> None:
        """При ошибке БД — статус degraded, response.status_code = 503."""
        db = MagicMock()
        response = MagicMock()
        response.status_code = 200  # начальное значение

        with patch("src.api.services.run_in_thread", new_callable=AsyncMock, side_effect=Exception("DB down")):
            result = await get_health_status(db, None, response)

        assert result.status == "degraded"
        assert result.tasks_running == -1
        assert result.tasks_pending == -1
        assert response.status_code == 503
