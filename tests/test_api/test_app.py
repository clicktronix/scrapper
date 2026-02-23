"""Тесты FastAPI-приложения: auth, health."""
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from tests.test_api.conftest import AUTH_HEADERS, make_app, make_pool


class TestHealth:
    """GET /api/health — без авторизации."""

    def test_health_no_auth_required(self) -> None:
        app = make_app()
        with patch("src.api.app.run_in_thread") as mock_run:
            mock_run.return_value = MagicMock(count=5)
            client = TestClient(app)
            resp = client.get("/api/health")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["accounts_total"] == 2

    def test_health_shows_available_accounts(self) -> None:
        pool = make_pool(total=3, available=2)
        app = make_app(pool=pool)
        with patch("src.api.app.run_in_thread") as mock_run:
            mock_run.return_value = MagicMock(count=0)
            client = TestClient(app)
            resp = client.get("/api/health")

        data = resp.json()
        assert data["accounts_total"] == 3
        assert data["accounts_available"] == 2

    def test_health_db_error_returns_minus_one(self) -> None:
        """При ошибке БД — статус degraded и HTTP 503."""
        app = make_app()
        with patch("src.api.app.run_in_thread", side_effect=Exception("DB down")):
            client = TestClient(app)
            resp = client.get("/api/health")

        assert resp.status_code == 503
        data = resp.json()
        assert data["status"] == "degraded"
        assert data["tasks_running"] == -1
        assert data["tasks_pending"] == -1


class TestAuth:
    """Авторизация по API key."""

    def test_missing_auth_returns_401(self) -> None:
        app = make_app()
        client = TestClient(app)
        resp = client.get("/api/tasks")
        assert resp.status_code == 401

    def test_wrong_key_returns_401(self) -> None:
        app = make_app()
        client = TestClient(app)
        resp = client.get("/api/tasks", headers={"Authorization": "Bearer wrong"})
        assert resp.status_code == 401

    def test_valid_key_passes(self) -> None:
        app = make_app()
        client = TestClient(app)
        resp = client.get("/api/tasks", headers=AUTH_HEADERS)
        assert resp.status_code != 401
