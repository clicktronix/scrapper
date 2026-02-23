"""Тесты POST /api/tasks/discover."""
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from tests.test_api.conftest import AUTH_HEADERS, make_app


class TestDiscoverEndpoint:
    """POST /api/tasks/discover."""

    def test_create_discover_task(self) -> None:
        app = make_app()
        with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = "task-1"
            client = TestClient(app)
            resp = client.post(
                "/api/tasks/discover",
                json={"hashtag": "алматымама"},
                headers=AUTH_HEADERS,
            )

        assert resp.status_code == 201
        data = resp.json()
        assert data["task_id"] == "task-1"
        assert data["hashtag"] == "алматымама"

        # Проверяем payload
        call_args = mock_create.call_args
        assert call_args[1]["payload"]["hashtag"] == "алматымама"
        assert call_args[1]["payload"]["min_followers"] == 1000

    def test_strips_hash_prefix(self) -> None:
        app = make_app()
        with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = "task-1"
            client = TestClient(app)
            resp = client.post(
                "/api/tasks/discover",
                json={"hashtag": "#beauty"},
                headers=AUTH_HEADERS,
            )

        data = resp.json()
        assert data["hashtag"] == "beauty"

    def test_custom_min_followers(self) -> None:
        app = make_app()
        with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = "task-1"
            client = TestClient(app)
            client.post(
                "/api/tasks/discover",
                json={"hashtag": "beauty", "min_followers": 5000},
                headers=AUTH_HEADERS,
            )

        call_args = mock_create.call_args
        assert call_args[1]["payload"]["min_followers"] == 5000

    def test_duplicate_returns_null_task_id(self) -> None:
        app = make_app()
        with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = None
            client = TestClient(app)
            resp = client.post(
                "/api/tasks/discover",
                json={"hashtag": "beauty"},
                headers=AUTH_HEADERS,
            )

        assert resp.status_code == 201
        assert resp.json()["task_id"] is None

    def test_no_auth_returns_401(self) -> None:
        app = make_app()
        client = TestClient(app)
        resp = client.post("/api/tasks/discover", json={"hashtag": "test"})
        assert resp.status_code == 401

    def test_empty_hashtag_rejected(self) -> None:
        """Пустой хештег после очистки → 422."""
        app = make_app()
        client = TestClient(app)
        resp = client.post(
            "/api/tasks/discover",
            json={"hashtag": "#"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 422

    def test_whitespace_only_hashtag_rejected(self) -> None:
        """Только пробелы в хештеге → 422."""
        app = make_app()
        client = TestClient(app)
        resp = client.post(
            "/api/tasks/discover",
            json={"hashtag": "   "},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 422
