"""Тесты FastAPI-приложения: auth, health."""
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from tests.test_api.conftest import AUTH_HEADERS, make_app, make_db_mock, make_pool


class TestHealth:
    """GET /api/health — без авторизации."""

    def test_health_no_auth_required(self) -> None:
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(count=5)
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.get("/api/health")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["accounts_total"] == 2

    def test_health_shows_available_accounts(self) -> None:
        pool = make_pool(total=3, available=2)
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(count=0)
        app = make_app(pool=pool, db=db)
        client = TestClient(app)
        resp = client.get("/api/health")

        data = resp.json()
        assert data["accounts_total"] == 3
        assert data["accounts_available"] == 2

    def test_health_db_error_returns_minus_one(self) -> None:
        """При ошибке БД — статус degraded, но HTTP 200 (сервер работает)."""
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.side_effect = Exception("DB down")
        # RPC тоже должен упасть для полной деградации
        db.rpc.return_value.execute.side_effect = Exception("DB down")
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.get("/api/health")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "degraded"
        assert data["tasks_running"] == -1
        assert data["tasks_pending"] == -1


class TestApiDocs:
    def test_docs_disabled_by_default(self) -> None:
        app = make_app()
        client = TestClient(app)
        assert client.get("/docs").status_code == 404

    def test_docs_enabled_when_flag_true(self) -> None:
        settings = MagicMock()
        settings.scraper_api_key.get_secret_value.return_value = "sk-test-key"
        settings.rescrape_days = 60
        settings.api_docs_enabled = True
        app = make_app(settings=settings)
        client = TestClient(app)
        assert client.get("/docs").status_code == 200


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
