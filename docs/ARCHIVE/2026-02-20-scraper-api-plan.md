# Scraper API Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Добавить FastAPI HTTP-сервер в скрапер для создания задач, мониторинга и интеграции с платформой.

**Architecture:** FastAPI запускается параллельно с polling loop через `asyncio.gather` в одном процессе. API работает напрямую с Supabase через существующие функции `database.py`. Авторизация — API key через `Authorization: Bearer`.

**Tech Stack:** FastAPI, uvicorn, Pydantic 2.x, Supabase

**Design doc:** `docs/plans/2026-02-20-scraper-api-design.md`

---

### Task 1: Pydantic request/response схемы

**Files:**
- Create: `src/api/__init__.py`
- Create: `src/api/schemas.py`
- Create: `tests/test_api/__init__.py`
- Create: `tests/test_api/test_schemas.py`

**Step 1: Write the failing tests**

Файл `tests/test_api/test_schemas.py`:

```python
"""Тесты Pydantic-схем API."""
from datetime import datetime, timezone


class TestScrapeRequest:
    """Валидация запроса на скрейп."""

    def test_valid_request(self) -> None:
        from src.api.schemas import ScrapeRequest

        req = ScrapeRequest(usernames=["blogger1", "blogger2"])
        assert req.usernames == ["blogger1", "blogger2"]

    def test_empty_usernames_rejected(self) -> None:
        from src.api.schemas import ScrapeRequest
        import pytest

        with pytest.raises(Exception):
            ScrapeRequest(usernames=[])

    def test_over_100_usernames_rejected(self) -> None:
        from src.api.schemas import ScrapeRequest
        import pytest

        with pytest.raises(Exception):
            ScrapeRequest(usernames=[f"user{i}" for i in range(101)])

    def test_exactly_100_accepted(self) -> None:
        from src.api.schemas import ScrapeRequest

        req = ScrapeRequest(usernames=[f"user{i}" for i in range(100)])
        assert len(req.usernames) == 100

    def test_strips_whitespace(self) -> None:
        from src.api.schemas import ScrapeRequest

        req = ScrapeRequest(usernames=["  blogger1  ", "blogger2"])
        assert req.usernames == ["blogger1", "blogger2"]

    def test_removes_at_prefix(self) -> None:
        from src.api.schemas import ScrapeRequest

        req = ScrapeRequest(usernames=["@blogger1", "blogger2"])
        assert req.usernames == ["blogger1", "blogger2"]

    def test_duplicates_removed(self) -> None:
        from src.api.schemas import ScrapeRequest

        req = ScrapeRequest(usernames=["blogger1", "blogger1", "blogger2"])
        assert req.usernames == ["blogger1", "blogger2"]


class TestDiscoverRequest:
    """Валидация запроса на discover."""

    def test_valid_request(self) -> None:
        from src.api.schemas import DiscoverRequest

        req = DiscoverRequest(hashtag="алматымама")
        assert req.hashtag == "алматымама"
        assert req.min_followers == 1000  # default

    def test_custom_min_followers(self) -> None:
        from src.api.schemas import DiscoverRequest

        req = DiscoverRequest(hashtag="beauty", min_followers=5000)
        assert req.min_followers == 5000

    def test_strips_hash_prefix(self) -> None:
        from src.api.schemas import DiscoverRequest

        req = DiscoverRequest(hashtag="#алматымама")
        assert req.hashtag == "алматымама"


class TestTaskResponse:
    """Проверка формата ответа."""

    def test_task_response_from_db_row(self) -> None:
        from src.api.schemas import TaskResponse

        row = {
            "id": "abc-123",
            "blog_id": "blog-456",
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
        resp = TaskResponse(**row)
        assert resp.id == "abc-123"
        assert resp.status == "pending"


class TestTaskListResponse:
    """Проверка пагинированного ответа."""

    def test_task_list_response(self) -> None:
        from src.api.schemas import TaskListResponse

        resp = TaskListResponse(tasks=[], total=0, limit=20, offset=0)
        assert resp.total == 0


class TestHealthResponse:
    """Проверка healthcheck."""

    def test_health_response(self) -> None:
        from src.api.schemas import HealthResponse

        resp = HealthResponse(
            status="ok",
            accounts_total=2,
            accounts_available=1,
            tasks_running=2,
            tasks_pending=15,
        )
        assert resp.status == "ok"
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_api/test_schemas.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.api'`

**Step 3: Write minimal implementation**

Файл `src/api/__init__.py`:
```python
```

Файл `tests/test_api/__init__.py`:
```python
```

Файл `src/api/schemas.py`:

```python
"""Pydantic-схемы для API скрапера."""
from pydantic import BaseModel, Field, field_validator


class ScrapeRequest(BaseModel):
    """Запрос на создание full_scrape задач."""

    usernames: list[str] = Field(min_length=1, max_length=100)

    @field_validator("usernames")
    @classmethod
    def clean_usernames(cls, v: list[str]) -> list[str]:
        """Очистить и дедуплицировать username-ы."""
        cleaned: list[str] = []
        seen: set[str] = set()
        for name in v:
            name = name.strip().lstrip("@")
            if name and name not in seen:
                cleaned.append(name)
                seen.add(name)
        if not cleaned:
            raise ValueError("usernames must not be empty after cleaning")
        return cleaned


class DiscoverRequest(BaseModel):
    """Запрос на создание discover задачи."""

    hashtag: str
    min_followers: int = Field(default=1000, ge=0)

    @field_validator("hashtag")
    @classmethod
    def clean_hashtag(cls, v: str) -> str:
        """Убрать # в начале."""
        return v.strip().lstrip("#")


class ScrapeTaskResult(BaseModel):
    """Результат создания одной задачи."""

    task_id: str | None
    username: str
    blog_id: str
    status: str  # "created" | "skipped"


class ScrapeResponse(BaseModel):
    """Ответ на POST /api/tasks/scrape."""

    created: int
    skipped: int
    tasks: list[ScrapeTaskResult]


class DiscoverResponse(BaseModel):
    """Ответ на POST /api/tasks/discover."""

    task_id: str | None
    hashtag: str


class TaskResponse(BaseModel):
    """Одна задача в ответе API."""

    id: str
    blog_id: str | None = None
    task_type: str
    status: str
    priority: int
    attempts: int = 0
    error_message: str | None = None
    payload: dict | None = None
    created_at: str | None = None
    started_at: str | None = None
    completed_at: str | None = None


class TaskListResponse(BaseModel):
    """Пагинированный список задач."""

    tasks: list[TaskResponse]
    total: int
    limit: int
    offset: int


class HealthResponse(BaseModel):
    """Ответ healthcheck."""

    status: str
    accounts_total: int
    accounts_available: int
    tasks_running: int
    tasks_pending: int
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_api/test_schemas.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add src/api/ tests/test_api/
git commit -m "feat(api): add request/response Pydantic schemas"
```

---

### Task 2: Config — добавить API-ключ и порт

**Files:**
- Modify: `src/config.py:15-52`
- Modify: `tests/test_config.py`

**Step 1: Write the failing tests**

Добавить в `tests/test_config.py`:

```python
class TestSettingsApiFields:
    """Тесты новых полей для API."""

    def test_scraper_api_key_parsed(self, monkeypatch) -> None:
        from src.config import Settings

        monkeypatch.setenv("SUPABASE_URL", "https://x.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "key")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("SCRAPER_API_KEY", "sk-scraper-123")

        s = Settings()
        assert s.scraper_api_key.get_secret_value() == "sk-scraper-123"

    def test_scraper_port_default(self, monkeypatch) -> None:
        from src.config import Settings

        monkeypatch.setenv("SUPABASE_URL", "https://x.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "key")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("SCRAPER_API_KEY", "sk-scraper-123")

        s = Settings()
        assert s.scraper_port == 8001

    def test_scraper_port_custom(self, monkeypatch) -> None:
        from src.config import Settings

        monkeypatch.setenv("SUPABASE_URL", "https://x.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "key")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("SCRAPER_API_KEY", "sk-scraper-123")
        monkeypatch.setenv("SCRAPER_PORT", "9000")

        s = Settings()
        assert s.scraper_port == 9000
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_config.py::TestSettingsApiFields -v`
Expected: FAIL — `ValidationError` (field scraper_api_key required)

**Step 3: Write minimal implementation**

В `src/config.py`, добавить два поля в класс `Settings` после строки `discovery_hashtags`:

```python
    # API
    scraper_api_key: SecretStr
    scraper_port: int = 8001
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_config.py -v`
Expected: ALL PASS

**Важно:** существующие тесты конфига тоже должны проходить — они используют monkeypatch. Нужно добавить `monkeypatch.setenv("SCRAPER_API_KEY", "test")` в существующие тесты, которые создают `Settings()`. Проверить: `uv run pytest tests/test_config.py -v` — если что-то падает из-за нового обязательного поля, добавить `SCRAPER_API_KEY` в `monkeypatch.setenv`.

**Step 5: Update `.env.example`**

Добавить в конец `.env.example`:

```env
# API
SCRAPER_API_KEY=sk-scraper-change-me
SCRAPER_PORT=8001
```

**Step 6: Commit**

```bash
git add src/config.py tests/test_config.py .env.example
git commit -m "feat(config): add scraper_api_key and scraper_port settings"
```

---

### Task 3: FastAPI app с auth middleware и health

**Files:**
- Create: `src/api/app.py`
- Create: `tests/test_api/test_app.py`

**Step 1: Write the failing tests**

Файл `tests/test_api/test_app.py`:

```python
"""Тесты FastAPI-приложения: auth, health."""
from unittest.mock import MagicMock

import pytest


def _make_settings():
    """Хелпер: создать мок Settings."""
    settings = MagicMock()
    settings.scraper_api_key.get_secret_value.return_value = "sk-test-key"
    return settings


def _make_pool(total: int = 2, available: int = 1):
    """Хелпер: создать мок AccountPool."""
    pool = MagicMock()
    pool.accounts = [MagicMock() for _ in range(total)]
    pool.get_available_account.return_value = MagicMock() if available > 0 else None

    # Для подсчёта available аккаунтов
    import time
    for i, acc in enumerate(pool.accounts):
        acc.cooldown_until = 0 if i < available else time.time() + 9999
        acc.requests_this_hour = 0
        acc.hour_started_at = time.time()
    pool.requests_per_hour = 30
    return pool


def _make_app(pool=None, settings=None):
    """Хелпер: создать FastAPI app."""
    from src.api.app import create_app

    return create_app(
        db=MagicMock(),
        pool=pool or _make_pool(),
        settings=settings or _make_settings(),
    )


class TestHealth:
    """GET /api/health — без авторизации."""

    def test_health_no_auth_required(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        client = TestClient(app)
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["accounts_total"] == 2


class TestAuth:
    """Авторизация по API key."""

    def test_missing_auth_returns_401(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        client = TestClient(app)
        resp = client.get("/api/tasks")
        assert resp.status_code == 401

    def test_wrong_key_returns_401(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        client = TestClient(app)
        resp = client.get("/api/tasks", headers={"Authorization": "Bearer wrong"})
        assert resp.status_code == 401

    def test_valid_key_passes(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        client = TestClient(app)
        resp = client.get(
            "/api/tasks",
            headers={"Authorization": "Bearer sk-test-key"},
        )
        # 200 или другой код, но не 401
        assert resp.status_code != 401
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_api/test_app.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.api.app'`

**Step 3: Add fastapi dependency**

Run: `uv add fastapi uvicorn[standard]`

**Step 4: Write minimal implementation**

Файл `src/api/app.py`:

```python
"""FastAPI-приложение скрапера."""
import time

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from supabase import Client

from src.api.schemas import HealthResponse
from src.config import Settings
from src.platforms.instagram.client import AccountPool

security = HTTPBearer(auto_error=False)


def create_app(db: Client, pool: AccountPool, settings: Settings) -> FastAPI:
    """Создать FastAPI-приложение с зависимостями."""
    app = FastAPI(title="Scraper API", version="0.1.0")

    # Сохраняем зависимости в app.state
    app.state.db = db
    app.state.pool = pool
    app.state.settings = settings

    async def verify_api_key(
        credentials: HTTPAuthorizationCredentials | None = Depends(security),
    ) -> None:
        """Проверка API-ключа."""
        expected = settings.scraper_api_key.get_secret_value()
        if credentials is None or credentials.credentials != expected:
            raise HTTPException(status_code=401, detail="Invalid API key")

    @app.get("/api/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        """Healthcheck — без авторизации."""
        from src.database import _run

        now = time.time()
        available = sum(
            1 for acc in pool.accounts
            if acc.cooldown_until <= now
            and acc.requests_this_hour < pool.requests_per_hour
        )

        # Подсчёт задач из БД
        try:
            running = await _run(
                db.table("scrape_tasks")
                .select("id", count="exact")
                .eq("status", "running")
                .execute
            )
            pending = await _run(
                db.table("scrape_tasks")
                .select("id", count="exact")
                .eq("status", "pending")
                .execute
            )
            tasks_running = running.count or 0
            tasks_pending = pending.count or 0
        except Exception:
            tasks_running = -1
            tasks_pending = -1

        return HealthResponse(
            status="ok",
            accounts_total=len(pool.accounts),
            accounts_available=available,
            tasks_running=tasks_running,
            tasks_pending=tasks_pending,
        )

    @app.get("/api/tasks", dependencies=[Depends(verify_api_key)])
    async def list_tasks(
        status: str | None = None,
        task_type: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> dict:
        """Заглушка — реализация в Task 4."""
        return {"tasks": [], "total": 0, "limit": limit, "offset": offset}

    return app
```

**Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_api/test_app.py -v`
Expected: ALL PASS

**Step 6: Commit**

```bash
git add src/api/app.py tests/test_api/test_app.py pyproject.toml uv.lock
git commit -m "feat(api): add FastAPI app with auth and health endpoint"
```

---

### Task 4: Эндпоинты — GET /api/tasks и GET /api/tasks/{task_id}

**Files:**
- Modify: `src/api/app.py`
- Create: `tests/test_api/test_routes_tasks.py`

**Step 1: Write the failing tests**

Файл `tests/test_api/test_routes_tasks.py`:

```python
"""Тесты эндпоинтов задач: GET /api/tasks, GET /api/tasks/{task_id}."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


AUTH = {"Authorization": "Bearer sk-test-key"}


def _make_app():
    from src.api.app import create_app

    settings = MagicMock()
    settings.scraper_api_key.get_secret_value.return_value = "sk-test-key"
    return create_app(db=MagicMock(), pool=MagicMock(), settings=settings)


class TestListTasks:
    """GET /api/tasks — список задач с фильтрами."""

    def test_list_tasks_empty(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        with patch("src.api.app._run") as mock_run:
            # Запрос данных: пустой список
            mock_run.return_value = MagicMock(data=[], count=0)
            client = TestClient(app)
            resp = client.get("/api/tasks", headers=AUTH)

        assert resp.status_code == 200
        data = resp.json()
        assert data["tasks"] == []
        assert data["total"] == 0

    def test_list_tasks_with_filter(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
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
        with patch("src.api.app._run") as mock_run:
            mock_run.return_value = MagicMock(data=[task_row], count=1)
            client = TestClient(app)
            resp = client.get(
                "/api/tasks?status=pending&task_type=full_scrape&limit=10",
                headers=AUTH,
            )

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["tasks"]) == 1
        assert data["tasks"][0]["id"] == "task-1"
        assert data["total"] == 1

    def test_list_tasks_pagination(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        with patch("src.api.app._run") as mock_run:
            mock_run.return_value = MagicMock(data=[], count=50)
            client = TestClient(app)
            resp = client.get("/api/tasks?limit=10&offset=20", headers=AUTH)

        data = resp.json()
        assert data["limit"] == 10
        assert data["offset"] == 20


class TestGetTask:
    """GET /api/tasks/{task_id} — одна задача."""

    def test_get_existing_task(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        task_row = {
            "id": "task-1",
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
        with patch("src.api.app._run") as mock_run:
            mock_run.return_value = MagicMock(data=[task_row])
            client = TestClient(app)
            resp = client.get("/api/tasks/task-1", headers=AUTH)

        assert resp.status_code == 200
        assert resp.json()["id"] == "task-1"
        assert resp.json()["status"] == "done"

    def test_get_nonexistent_task_returns_404(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        with patch("src.api.app._run") as mock_run:
            mock_run.return_value = MagicMock(data=[])
            client = TestClient(app)
            resp = client.get("/api/tasks/nonexistent", headers=AUTH)

        assert resp.status_code == 404
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_api/test_routes_tasks.py -v`
Expected: FAIL — заглушка list_tasks не работает с patch

**Step 3: Write implementation**

Заменить заглушку `list_tasks` в `src/api/app.py` и добавить `get_task`:

```python
    @app.get("/api/tasks", dependencies=[Depends(verify_api_key)])
    async def list_tasks(
        status: str | None = None,
        task_type: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> dict:
        """Список задач с фильтрами и пагинацией."""
        query = db.table("scrape_tasks").select("*", count="exact")
        if status:
            query = query.eq("status", status)
        if task_type:
            query = query.eq("task_type", task_type)
        query = query.order("created_at", desc=True).range(offset, offset + limit - 1)
        result = await _run(query.execute)
        return {
            "tasks": result.data,
            "total": result.count or 0,
            "limit": limit,
            "offset": offset,
        }

    @app.get("/api/tasks/{task_id}", dependencies=[Depends(verify_api_key)])
    async def get_task(task_id: str) -> dict:
        """Получить задачу по ID."""
        result = await _run(
            db.table("scrape_tasks").select("*").eq("id", task_id).execute
        )
        if not result.data:
            raise HTTPException(status_code=404, detail="Task not found")
        return result.data[0]
```

Также добавить импорт `_run` в начало `create_app`:
```python
    from src.database import _run
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_api/test_routes_tasks.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add src/api/app.py tests/test_api/test_routes_tasks.py
git commit -m "feat(api): add GET /api/tasks and GET /api/tasks/{task_id}"
```

---

### Task 5: POST /api/tasks/scrape

**Files:**
- Modify: `src/api/app.py`
- Create: `tests/test_api/test_routes_scrape.py`

**Step 1: Write the failing tests**

Файл `tests/test_api/test_routes_scrape.py`:

```python
"""Тесты POST /api/tasks/scrape — создание full_scrape задач."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


AUTH = {"Authorization": "Bearer sk-test-key"}


def _make_app():
    from src.api.app import create_app

    settings = MagicMock()
    settings.scraper_api_key.get_secret_value.return_value = "sk-test-key"
    return create_app(db=MagicMock(), pool=MagicMock(), settings=settings)


class TestScrapeEndpoint:
    """POST /api/tasks/scrape."""

    def test_create_new_blog_and_task(self) -> None:
        """Username без существующего блога → создать person + blog + task."""
        from fastapi.testclient import TestClient

        app = _make_app()
        with patch("src.api.app._run") as mock_run:
            # 1) SELECT blog — не найден
            # 2) INSERT person — ok
            # 3) INSERT blog — ok
            # 4) create_task_if_not_exists — вернёт task_id
            mock_run.side_effect = [
                MagicMock(data=[]),  # blog not found
                MagicMock(data=[{"id": "person-1"}]),  # insert person
                MagicMock(data=[{"id": "blog-1"}]),  # insert blog
            ]
            with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
                mock_create.return_value = "task-1"
                client = TestClient(app)
                resp = client.post(
                    "/api/tasks/scrape",
                    json={"usernames": ["new_blogger"]},
                    headers=AUTH,
                )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 1
        assert data["skipped"] == 0
        assert data["tasks"][0]["status"] == "created"
        assert data["tasks"][0]["task_id"] == "task-1"

    def test_existing_blog_creates_task(self) -> None:
        """Username с существующим блогом → только task."""
        from fastapi.testclient import TestClient

        app = _make_app()
        with patch("src.api.app._run") as mock_run:
            mock_run.return_value = MagicMock(data=[{"id": "blog-1"}])
            with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
                mock_create.return_value = "task-1"
                client = TestClient(app)
                resp = client.post(
                    "/api/tasks/scrape",
                    json={"usernames": ["existing_blogger"]},
                    headers=AUTH,
                )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 1

    def test_existing_task_skipped(self) -> None:
        """Задача уже существует → skipped."""
        from fastapi.testclient import TestClient

        app = _make_app()
        with patch("src.api.app._run") as mock_run:
            mock_run.return_value = MagicMock(data=[{"id": "blog-1"}])
            with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
                mock_create.return_value = None  # задача уже есть
                client = TestClient(app)
                resp = client.post(
                    "/api/tasks/scrape",
                    json={"usernames": ["existing_blogger"]},
                    headers=AUTH,
                )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 0
        assert data["skipped"] == 1
        assert data["tasks"][0]["status"] == "skipped"

    def test_empty_usernames_rejected(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        client = TestClient(app)
        resp = client.post(
            "/api/tasks/scrape",
            json={"usernames": []},
            headers=AUTH,
        )
        assert resp.status_code == 422

    def test_no_auth_returns_401(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        client = TestClient(app)
        resp = client.post("/api/tasks/scrape", json={"usernames": ["test"]})
        assert resp.status_code == 401
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_api/test_routes_scrape.py -v`
Expected: FAIL — эндпоинт ещё не существует

**Step 3: Write implementation**

Добавить в `src/api/app.py` внутри `create_app`:

```python
    @app.post("/api/tasks/scrape", status_code=201, dependencies=[Depends(verify_api_key)])
    async def scrape(body: ScrapeRequest) -> dict:
        """Создать full_scrape задачи по списку username."""
        results = []
        created = 0
        skipped = 0

        for username in body.usernames:
            # Найти существующий блог
            blog_result = await _run(
                db.table("blogs")
                .select("id")
                .eq("platform", "instagram")
                .eq("username", username)
                .execute
            )

            if blog_result.data:
                blog_id = blog_result.data[0]["id"]
            else:
                # Создать person + blog
                person_result = await _run(
                    db.table("persons")
                    .insert({"full_name": username})
                    .execute
                )
                person_id = person_result.data[0]["id"]
                blog_result = await _run(
                    db.table("blogs")
                    .insert({
                        "person_id": person_id,
                        "platform": "instagram",
                        "username": username,
                        "scrape_status": "pending",
                    })
                    .execute
                )
                blog_id = blog_result.data[0]["id"]

            # Создать задачу
            task_id = await create_task_if_not_exists(
                db, blog_id, "full_scrape", priority=3,
            )

            if task_id:
                results.append({"task_id": task_id, "username": username, "blog_id": blog_id, "status": "created"})
                created += 1
            else:
                results.append({"task_id": None, "username": username, "blog_id": blog_id, "status": "skipped"})
                skipped += 1

        return {"created": created, "skipped": skipped, "tasks": results}
```

Добавить импорты в начало `create_app`:
```python
    from src.database import _run, create_task_if_not_exists
    from src.api.schemas import ScrapeRequest
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_api/test_routes_scrape.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add src/api/app.py tests/test_api/test_routes_scrape.py
git commit -m "feat(api): add POST /api/tasks/scrape endpoint"
```

---

### Task 6: POST /api/tasks/discover

**Files:**
- Modify: `src/api/app.py`
- Create: `tests/test_api/test_routes_discover.py`

**Step 1: Write the failing tests**

Файл `tests/test_api/test_routes_discover.py`:

```python
"""Тесты POST /api/tasks/discover."""
from unittest.mock import AsyncMock, MagicMock, patch


AUTH = {"Authorization": "Bearer sk-test-key"}


def _make_app():
    from src.api.app import create_app

    settings = MagicMock()
    settings.scraper_api_key.get_secret_value.return_value = "sk-test-key"
    return create_app(db=MagicMock(), pool=MagicMock(), settings=settings)


class TestDiscoverEndpoint:
    """POST /api/tasks/discover."""

    def test_create_discover_task(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = "task-1"
            client = TestClient(app)
            resp = client.post(
                "/api/tasks/discover",
                json={"hashtag": "алматымама"},
                headers=AUTH,
            )

        assert resp.status_code == 201
        data = resp.json()
        assert data["task_id"] == "task-1"
        assert data["hashtag"] == "алматымама"

        # Проверяем вызов с правильными аргументами
        mock_create.assert_called_once()
        call_kwargs = mock_create.call_args
        assert call_kwargs[0][2] == "discover"  # task_type
        assert call_kwargs[1]["payload"]["hashtag"] == "алматымама"

    def test_strips_hash_prefix(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = "task-1"
            client = TestClient(app)
            resp = client.post(
                "/api/tasks/discover",
                json={"hashtag": "#beauty"},
                headers=AUTH,
            )

        data = resp.json()
        assert data["hashtag"] == "beauty"

    def test_duplicate_discover_returns_null_task_id(self) -> None:
        from fastapi.testclient import TestClient

        app = _make_app()
        with patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = None  # уже существует
            client = TestClient(app)
            resp = client.post(
                "/api/tasks/discover",
                json={"hashtag": "beauty"},
                headers=AUTH,
            )

        assert resp.status_code == 201
        assert resp.json()["task_id"] is None
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_api/test_routes_discover.py -v`
Expected: FAIL

**Step 3: Write implementation**

Добавить в `src/api/app.py` внутри `create_app`:

```python
    @app.post("/api/tasks/discover", status_code=201, dependencies=[Depends(verify_api_key)])
    async def discover(body: DiscoverRequest) -> dict:
        """Создать discover задачу по хештегу."""
        task_id = await create_task_if_not_exists(
            db, None, "discover", priority=10,
            payload={"hashtag": body.hashtag, "min_followers": body.min_followers},
        )
        return {"task_id": task_id, "hashtag": body.hashtag}
```

Добавить импорт `DiscoverRequest` в `from src.api.schemas import ...`.

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_api/test_routes_discover.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add src/api/app.py tests/test_api/test_routes_discover.py
git commit -m "feat(api): add POST /api/tasks/discover endpoint"
```

---

### Task 7: Интеграция в main.py — uvicorn + worker

**Files:**
- Modify: `src/main.py`
- Create: `tests/test_main.py`

**Step 1: Write the failing test**

Файл `tests/test_main.py`:

```python
"""Тесты запуска: FastAPI + worker в одном процессе."""
from unittest.mock import AsyncMock, MagicMock, patch


class TestMainStartsBoth:
    """main() должен запускать uvicorn и worker параллельно."""

    async def test_main_runs_gather(self) -> None:
        """asyncio.gather вызывается с двумя корутинами."""
        with (
            patch("src.main.Settings") as mock_settings_cls,
            patch("src.main.create_client") as mock_db,
            patch("src.main.OpenAI"),
            patch("src.main.AccountPool") as mock_pool_cls,
            patch("src.main.create_scheduler") as mock_sched,
            patch("src.main.run_worker", new_callable=AsyncMock) as mock_worker,
            patch("src.main.create_app") as mock_create_app,
            patch("src.main.uvicorn") as mock_uvicorn,
        ):
            mock_settings = MagicMock()
            mock_settings.log_level = "INFO"
            mock_settings.supabase_url = "https://x.supabase.co"
            mock_settings.supabase_service_key.get_secret_value.return_value = "key"
            mock_settings.openai_api_key.get_secret_value.return_value = "sk"
            mock_settings.scraper_port = 8001
            mock_settings_cls.return_value = mock_settings

            mock_pool = AsyncMock()
            mock_pool.accounts = []
            mock_pool_cls.create = AsyncMock(return_value=mock_pool)
            mock_pool.save_all_sessions = AsyncMock()

            mock_sched.return_value = MagicMock()

            mock_server = AsyncMock()
            mock_uvicorn.Server.return_value = mock_server
            mock_server.serve = AsyncMock()

            # worker завершается сразу (shutdown_event)
            mock_worker.return_value = None

            from src.main import main

            await main()

            # uvicorn.Server.serve() и run_worker() были вызваны
            mock_server.serve.assert_called_once()
            mock_worker.assert_called_once()
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_main.py -v`
Expected: FAIL — `create_app` не импортируется

**Step 3: Write implementation**

Обновить `src/main.py`:

```python
"""Точка входа скрапера — инициализация и запуск воркера + API."""
import asyncio
import signal
import sys

import uvicorn
from loguru import logger
from openai import OpenAI
from supabase import create_client

from src.api.app import create_app
from src.config import Settings
from src.platforms.instagram.client import AccountPool
from src.platforms.instagram.scraper import InstagramScraper
from src.worker.loop import run_worker
from src.worker.scheduler import create_scheduler


async def main() -> None:
    """Инициализация и запуск API + воркера."""
    settings = Settings()

    # Логирование
    logger.remove()
    logger.add(sys.stderr, level=settings.log_level)
    if settings.log_level == "DEBUG":
        logger.add("logs/scraper.log", rotation="100 MB", retention="7 days")

    logger.info("Starting scraper worker")

    # Supabase
    db = create_client(settings.supabase_url, settings.supabase_service_key.get_secret_value())

    # OpenAI
    openai_client = OpenAI(api_key=settings.openai_api_key.get_secret_value())

    # Instagram
    pool = await AccountPool.create(db, settings)
    logger.info(f"Initialized {len(pool.accounts)} Instagram accounts")

    scrapers = {"instagram": InstagramScraper(pool, settings)}

    # Планировщик
    scheduler = create_scheduler(db, settings, openai_client)
    scheduler.start()

    # FastAPI
    app = create_app(db, pool, settings)
    config = uvicorn.Config(app, host="0.0.0.0", port=settings.scraper_port, log_level="warning")
    server = uvicorn.Server(config)

    # Graceful shutdown
    shutdown_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    try:
        await asyncio.gather(
            server.serve(),
            run_worker(db, scrapers, settings, shutdown_event, openai_client),
        )
    finally:
        await pool.save_all_sessions(db)
        scheduler.shutdown(wait=False)
        logger.info("Scraper stopped gracefully")


if __name__ == "__main__":
    asyncio.run(main())
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_main.py -v`
Expected: ALL PASS

Также запустить все тесты:
Run: `uv run pytest tests/ -v --tb=short`
Expected: ALL PASS

**Step 5: Update docker-compose.yml — expose port**

Добавить в `docker-compose.yml` в сервис `scraper`:

```yaml
    ports:
      - "8001:8001"
```

**Step 6: Commit**

```bash
git add src/main.py tests/test_main.py docker-compose.yml
git commit -m "feat: integrate FastAPI server with worker in main.py"
```

---

### Task 8: Финальная проверка

**Step 1: Run all tests**

```bash
uv run pytest tests/ -v --tb=short
```

Expected: ALL PASS (383 старых + ~25 новых)

**Step 2: Update docs**

Обновить `README.md` — добавить секцию API.
Обновить `docs/ARCHITECTURE.md` — добавить API-компонент.
Обновить `CLAUDE.md` — добавить API-роуты.

**Step 3: Commit**

```bash
git add README.md docs/ARCHITECTURE.md CLAUDE.md
git commit -m "docs: add API documentation"
```
