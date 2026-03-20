"""Тесты сервисных функций API — services.py."""
import time
from unittest.mock import MagicMock

import pytest

from src.api.schemas import HealthResponse
from src.api.services import fetch_tasks_list, find_blog_by_username, get_health_status
from tests.test_api.conftest import make_db_mock


class TestFindBlogByUsername:
    """Тесты find_blog_by_username."""

    @pytest.mark.asyncio
    async def test_returns_blog_id_when_found(self) -> None:
        """Блог найден — возвращает id."""
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(data=[{"id": "blog-123"}])

        result = await find_blog_by_username(db, "testuser")
        assert result == "blog-123"

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self) -> None:
        """Блог не найден — возвращает None."""
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(data=[])

        result = await find_blog_by_username(db, "nonexistent")
        assert result is None


class TestGetHealthStatus:
    """Тесты get_health_status."""

    @pytest.mark.asyncio
    async def test_ok_with_pool(self) -> None:
        """Статус ok при наличии pool и живой БД."""
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(count=3)
        # RPC queue_depth — пустой результат, fallback к count-запросам
        db.rpc.return_value.execute.return_value = MagicMock(data=[])

        pool = MagicMock()
        now = time.time()
        pool.accounts = [MagicMock(), MagicMock()]
        pool.accounts[0].cooldown_until = 0
        pool.accounts[0].requests_this_hour = 0
        pool.accounts[1].cooldown_until = now + 9999
        pool.accounts[1].requests_this_hour = 0
        pool.requests_per_hour = 30
        response = MagicMock()

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
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(count=0)
        db.rpc.return_value.execute.return_value = MagicMock(data=[])
        response = MagicMock()

        result = await get_health_status(db, None, response)

        assert result.status == "ok"
        assert result.accounts_total == 0
        assert result.accounts_available == 0

    @pytest.mark.asyncio
    async def test_degraded_on_db_error(self) -> None:
        """При ошибке БД — статус degraded, но HTTP 200 (сервер работает)."""
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.side_effect = Exception("DB down")
        db.rpc.return_value.execute.side_effect = Exception("DB down")
        response = MagicMock()
        response.status_code = 200

        result = await get_health_status(db, None, response)

        assert result.status == "degraded"
        assert result.tasks_running == -1
        assert result.tasks_pending == -1
        # HTTP 200 — сервер работает, БД временно недоступна
        assert response.status_code == 200


class TestFetchTasksList:
    """Тесты fetch_tasks_list."""

    @pytest.mark.asyncio
    async def test_returns_tasks_and_total(self) -> None:
        """Успешное получение списка задач."""
        db = make_db_mock()
        builder = db.table.return_value
        mock_data = [{"id": "t1", "status": "pending"}, {"id": "t2", "status": "running"}]
        builder.execute.return_value = MagicMock(data=mock_data, count=5)

        result = await fetch_tasks_list(db, limit=2, offset=0)

        assert result["tasks"] == mock_data
        assert result["total"] == 5
        assert result["limit"] == 2
        assert result["offset"] == 0
        assert "error" not in result

    @pytest.mark.asyncio
    async def test_returns_empty_on_db_error(self) -> None:
        """При ошибке БД возвращает пустой список и error."""
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.side_effect = Exception("DB down")

        result = await fetch_tasks_list(db)

        assert result["tasks"] == []
        assert result["total"] == 0
        assert result["error"] == "DB down"

    @pytest.mark.asyncio
    async def test_applies_status_filter(self) -> None:
        """Фильтр по status добавляет .eq()."""
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(data=[], count=0)

        await fetch_tasks_list(db, status="pending")

        builder.eq.assert_any_call("status", "pending")
