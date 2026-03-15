"""Тесты эндпоинтов задач: GET /api/tasks, GET /api/tasks/{task_id}."""
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from tests.test_api.conftest import AUTH_HEADERS, make_app, make_db_mock

# Валидные UUID для тестов (UUID валидация в эндпоинтах)
TASK_UUID = "00000000-0000-0000-0000-000000000001"
TASK_UUID_2 = "00000000-0000-0000-0000-000000000002"


class TestListTasks:
    """GET /api/tasks — список задач с фильтрами."""

    def test_list_tasks_returns_data(self) -> None:
        db = make_db_mock()
        builder = db.table.return_value
        task_row = {
            "id": "task-1",
            "blog_id": "blog-1",
            "task_type": "full_scrape",
            "status": "pending",
            "priority": 3,
            "attempts": 0,
            "error_message": None,
            "payload": {},
            "created_at": "2026-02-20T10:00:00+00:00",
            "started_at": None,
            "completed_at": None,
        }
        builder.execute.return_value = MagicMock(data=[task_row], count=1)
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.get("/api/tasks", headers=AUTH_HEADERS)

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["tasks"]) == 1
        assert data["tasks"][0]["id"] == "task-1"
        assert data["total"] == 1

    def test_list_tasks_empty(self) -> None:
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(data=[], count=0)
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.get("/api/tasks", headers=AUTH_HEADERS)

        assert resp.status_code == 200
        data = resp.json()
        assert data["tasks"] == []
        assert data["total"] == 0

    def test_list_tasks_pagination_params(self) -> None:
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(data=[], count=50)
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.get("/api/tasks?limit=10&offset=20", headers=AUTH_HEADERS)

        data = resp.json()
        assert data["limit"] == 10
        assert data["offset"] == 20
        assert data["total"] == 50

    def test_list_tasks_requires_auth(self) -> None:
        app = make_app()
        client = TestClient(app)
        resp = client.get("/api/tasks")
        assert resp.status_code == 401

    def test_negative_limit_rejected(self) -> None:
        """limit < 1 → 422."""
        app = make_app()
        client = TestClient(app)
        resp = client.get("/api/tasks?limit=-1", headers=AUTH_HEADERS)
        assert resp.status_code == 422

    def test_limit_over_100_rejected(self) -> None:
        """limit > 100 → 422."""
        app = make_app()
        client = TestClient(app)
        resp = client.get("/api/tasks?limit=101", headers=AUTH_HEADERS)
        assert resp.status_code == 422

    def test_negative_offset_rejected(self) -> None:
        """offset < 0 → 422."""
        app = make_app()
        client = TestClient(app)
        resp = client.get("/api/tasks?offset=-5", headers=AUTH_HEADERS)
        assert resp.status_code == 422


class TestGetTask:
    """GET /api/tasks/{task_id} — одна задача."""

    def test_get_existing_task(self) -> None:
        db = make_db_mock()
        builder = db.table.return_value
        task_row = {
            "id": TASK_UUID,
            "blog_id": "blog-1",
            "task_type": "full_scrape",
            "status": "done",
            "priority": 3,
            "attempts": 1,
            "error_message": None,
            "payload": {},
            "created_at": "2026-02-20T10:00:00+00:00",
            "started_at": "2026-02-20T10:01:00+00:00",
            "completed_at": "2026-02-20T10:05:00+00:00",
        }
        builder.execute.return_value = MagicMock(data=[task_row])
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.get(f"/api/tasks/{TASK_UUID}", headers=AUTH_HEADERS)

        assert resp.status_code == 200
        assert resp.json()["id"] == TASK_UUID
        assert resp.json()["status"] == "done"

    def test_get_nonexistent_task_returns_404(self) -> None:
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(data=[])
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.get(f"/api/tasks/{TASK_UUID}", headers=AUTH_HEADERS)

        assert resp.status_code == 404

    def test_get_task_requires_auth(self) -> None:
        app = make_app()
        client = TestClient(app)
        resp = client.get("/api/tasks/some-id")
        assert resp.status_code == 401


class TestRetryTask:
    """POST /api/tasks/{task_id}/retry — повторить упавшую задачу."""

    def test_retry_failed_task(self) -> None:
        db = make_db_mock()
        builder = db.table.return_value
        # select failed -> update -> reselect pending
        builder.execute.side_effect = [
            MagicMock(data=[{"id": TASK_UUID, "status": "failed"}]),
            MagicMock(data=[]),
            MagicMock(data=[{"status": "pending"}]),
        ]
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.post(f"/api/tasks/{TASK_UUID}/retry", headers=AUTH_HEADERS)

        assert resp.status_code == 200
        data = resp.json()
        assert data["task_id"] == TASK_UUID
        assert data["status"] == "retrying"

    def test_retry_nonexistent_task_returns_404(self) -> None:
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(data=[])
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.post(f"/api/tasks/{TASK_UUID}/retry", headers=AUTH_HEADERS)

        assert resp.status_code == 404

    def test_retry_state_changed_concurrently_returns_409(self) -> None:
        db = make_db_mock()
        builder = db.table.return_value
        # Изначально failed, но после update статус не стал pending.
        builder.execute.side_effect = [
            MagicMock(data=[{"id": TASK_UUID, "status": "failed"}]),
            MagicMock(data=[]),
            MagicMock(data=[{"status": "running"}]),
        ]
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.post(f"/api/tasks/{TASK_UUID}/retry", headers=AUTH_HEADERS)
        assert resp.status_code == 409

    def test_retry_pending_task_returns_409(self) -> None:
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(data=[{"id": TASK_UUID, "status": "pending"}])
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.post(f"/api/tasks/{TASK_UUID}/retry", headers=AUTH_HEADERS)

        assert resp.status_code == 409

    def test_retry_running_task_returns_409(self) -> None:
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(data=[{"id": TASK_UUID, "status": "running"}])
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.post(f"/api/tasks/{TASK_UUID}/retry", headers=AUTH_HEADERS)

        assert resp.status_code == 409

    def test_retry_done_task_returns_409(self) -> None:
        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(data=[{"id": TASK_UUID, "status": "done"}])
        app = make_app(db=db)
        client = TestClient(app)
        resp = client.post(f"/api/tasks/{TASK_UUID}/retry", headers=AUTH_HEADERS)

        assert resp.status_code == 409

    def test_retry_requires_auth(self) -> None:
        app = make_app()
        client = TestClient(app)
        resp = client.post("/api/tasks/task-1/retry")
        assert resp.status_code == 401
