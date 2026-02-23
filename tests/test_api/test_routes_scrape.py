"""Тесты POST /api/tasks/scrape — создание full_scrape задач."""
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from tests.test_api.conftest import AUTH_HEADERS, make_app


class TestScrapeEndpoint:
    """POST /api/tasks/scrape."""

    def test_create_new_blog_and_task(self) -> None:
        """Username без существующего блога → создать person + blog + task."""
        app = make_app()
        with patch("src.api.app._find_or_create_blog", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = "blog-1"
            with patch("src.api.app.is_blog_fresh", new_callable=AsyncMock, return_value=False):
                with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
                    mock_create.return_value = "task-1"
                    client = TestClient(app)
                    resp = client.post(
                        "/api/tasks/scrape",
                        json={"usernames": ["new_blogger"]},
                        headers=AUTH_HEADERS,
                    )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 1
        assert data["skipped"] == 0
        assert data["tasks"][0]["status"] == "created"
        assert data["tasks"][0]["task_id"] == "task-1"
        assert data["tasks"][0]["blog_id"] == "blog-1"

    def test_existing_blog_creates_task(self) -> None:
        """Username с существующим блогом → только task."""
        app = make_app()
        with patch("src.api.app._find_or_create_blog", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = "blog-1"
            with patch("src.api.app.is_blog_fresh", new_callable=AsyncMock, return_value=False):
                with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
                    mock_create.return_value = "task-1"
                    client = TestClient(app)
                    resp = client.post(
                        "/api/tasks/scrape",
                        json={"usernames": ["existing_blogger"]},
                        headers=AUTH_HEADERS,
                    )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 1
        assert data["tasks"][0]["blog_id"] == "blog-1"

    def test_existing_task_skipped(self) -> None:
        """Задача уже существует → skipped."""
        app = make_app()
        with patch("src.api.app._find_or_create_blog", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = "blog-1"
            with patch("src.api.app.is_blog_fresh", new_callable=AsyncMock, return_value=False):
                with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
                    mock_create.return_value = None
                    client = TestClient(app)
                    resp = client.post(
                        "/api/tasks/scrape",
                        json={"usernames": ["existing_blogger"]},
                        headers=AUTH_HEADERS,
                    )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 0
        assert data["skipped"] == 1
        assert data["tasks"][0]["status"] == "skipped"
        assert data["tasks"][0]["task_id"] is None

    def test_empty_usernames_rejected(self) -> None:
        app = make_app()
        client = TestClient(app)
        resp = client.post(
            "/api/tasks/scrape",
            json={"usernames": []},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 422

    def test_no_auth_returns_401(self) -> None:
        app = make_app()
        client = TestClient(app)
        resp = client.post("/api/tasks/scrape", json={"usernames": ["test"]})
        assert resp.status_code == 401

    def test_multiple_usernames_mixed(self) -> None:
        """Несколько username: один новый, один с existing task."""
        app = make_app()
        with patch("src.api.app._find_or_create_blog", new_callable=AsyncMock) as mock_find:
            mock_find.side_effect = ["blog-1", "blog-2"]
            with patch("src.api.app.is_blog_fresh", new_callable=AsyncMock, return_value=False):
                with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
                    mock_create.side_effect = ["task-1", None]  # first created, second skipped
                    client = TestClient(app)
                    resp = client.post(
                        "/api/tasks/scrape",
                        json={"usernames": ["blogger1", "blogger2"]},
                        headers=AUTH_HEADERS,
                    )

        data = resp.json()
        assert data["created"] == 1
        assert data["skipped"] == 1

    def test_fresh_blog_skipped(self) -> None:
        """Свежий блог (скрапился < rescrape_days назад) → skipped."""
        app = make_app()
        with patch("src.api.app._find_or_create_blog", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = "blog-1"
            with patch("src.api.app.is_blog_fresh", new_callable=AsyncMock) as mock_fresh:
                mock_fresh.return_value = True
                client = TestClient(app)
                resp = client.post(
                    "/api/tasks/scrape",
                    json={"usernames": ["fresh_blogger"]},
                    headers=AUTH_HEADERS,
                )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 0
        assert data["skipped"] == 1
        assert data["tasks"][0]["status"] == "skipped"
        assert data["tasks"][0]["task_id"] is None
        assert data["tasks"][0]["blog_id"] == "blog-1"

    def test_stale_blog_creates_task(self) -> None:
        """Устаревший блог (scraped_at > rescrape_days) → создаётся задача."""
        app = make_app()
        with patch("src.api.app._find_or_create_blog", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = "blog-1"
            with patch("src.api.app.is_blog_fresh", new_callable=AsyncMock) as mock_fresh:
                mock_fresh.return_value = False
                with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
                    mock_create.return_value = "task-1"
                    client = TestClient(app)
                    resp = client.post(
                        "/api/tasks/scrape",
                        json={"usernames": ["stale_blogger"]},
                        headers=AUTH_HEADERS,
                    )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 1
        assert data["tasks"][0]["status"] == "created"

    def test_db_error_returns_error_status(self) -> None:
        """Ошибка БД при создании блога → status=error, остальные продолжают."""
        app = make_app()
        with patch("src.api.app._find_or_create_blog", new_callable=AsyncMock) as mock_find:
            mock_find.side_effect = [Exception("DB connection lost"), "blog-2"]
            with patch("src.api.app.is_blog_fresh", new_callable=AsyncMock, return_value=False):
                with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
                    mock_create.return_value = "task-2"
                    client = TestClient(app)
                    resp = client.post(
                        "/api/tasks/scrape",
                        json={"usernames": ["failing_blog", "good_blog"]},
                        headers=AUTH_HEADERS,
                    )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 1
        # Первый — ошибка
        assert data["tasks"][0]["status"] == "error"
        assert data["tasks"][0]["blog_id"] is None
        # Второй — успех
        assert data["tasks"][1]["status"] == "created"
        assert data["tasks"][1]["blog_id"] == "blog-2"
