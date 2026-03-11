"""Тесты POST /api/tasks/pre_filter — создание pre_filter задач."""
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from tests.test_api.conftest import AUTH_HEADERS, make_app


class TestPreFilterEndpoint:
    """POST /api/tasks/pre_filter."""

    def test_creates_task_for_new_username(self) -> None:
        """Новый username (нет в БД) → создать pre_filter задачу."""
        app = make_app()
        with (
            patch("src.api.app.find_blog_by_username", new_callable=AsyncMock) as mock_find,
            patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            mock_find.return_value = None
            mock_create.return_value = "task-id"
            client = TestClient(app)
            resp = client.post(
                "/api/tasks/pre_filter",
                json={"usernames": ["new_blogger"]},
                headers=AUTH_HEADERS,
            )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 1
        assert data["skipped"] == 0
        assert data["errors"] == 0
        assert data["tasks"][0]["status"] == "created"
        assert data["tasks"][0]["task_id"] == "task-id"
        assert data["tasks"][0]["blog_id"] is None

    def test_skips_existing_blog(self) -> None:
        """Username уже есть в БД → skipped с reason."""
        app = make_app()
        with patch("src.api.app.find_blog_by_username", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = "blog-id"
            client = TestClient(app)
            resp = client.post(
                "/api/tasks/pre_filter",
                json={"usernames": ["existing_blogger"]},
                headers=AUTH_HEADERS,
            )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 0
        assert data["skipped"] == 1
        assert data["tasks"][0]["status"] == "skipped"
        assert data["tasks"][0]["blog_id"] == "blog-id"
        assert data["tasks"][0]["reason"] == "blog already exists"

    def test_skips_existing_task(self) -> None:
        """Задача уже существует (create_task returns None) → skipped."""
        app = make_app()
        with (
            patch("src.api.app.find_blog_by_username", new_callable=AsyncMock) as mock_find,
            patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            mock_find.return_value = None
            mock_create.return_value = None
            client = TestClient(app)
            resp = client.post(
                "/api/tasks/pre_filter",
                json={"usernames": ["duplicate_task"]},
                headers=AUTH_HEADERS,
            )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 0
        assert data["skipped"] == 1
        assert data["tasks"][0]["status"] == "skipped"
        assert data["tasks"][0]["task_id"] is None

    def test_no_auth_returns_401(self) -> None:
        app = make_app()
        client = TestClient(app)
        resp = client.post("/api/tasks/pre_filter", json={"usernames": ["test"]})
        assert resp.status_code == 401

    def test_empty_usernames_rejected(self) -> None:
        app = make_app()
        client = TestClient(app)
        resp = client.post(
            "/api/tasks/pre_filter",
            json={"usernames": []},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 422
