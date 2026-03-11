# Pre-filter Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Добавить task_type `pre_filter` для массовой фильтрации ~69K блогеров из xlsx по 3 критериям (приватность, лайки, активность) через HikerAPI.

**Architecture:** Новый эндпоинт + handler по паттерну существующего `full_scrape`. Один username = одна задача. Прошедшие фильтр записываются в `persons` + `blogs`. Отдельный скрипт читает xlsx и шлёт батчи по 100.

**Tech Stack:** Python 3.12, FastAPI, HikerAPI, Supabase PostgreSQL, pandas (скрипт)

---

### Task 1: Миграция БД — добавить `pre_filter` в CHECK constraint

**Files:**
- Create: `../platform/supabase/migrations/20260309000000_add_pre_filter_task_type.sql`

**Step 1: Создать файл миграции**

```sql
-- Добавить pre_filter в допустимые task_type
ALTER TABLE scrape_tasks DROP CONSTRAINT IF EXISTS scrape_tasks_task_type_check;
ALTER TABLE scrape_tasks ADD CONSTRAINT scrape_tasks_task_type_check
  CHECK (task_type IN ('full_scrape', 'ai_analysis', 'discover', 'pre_filter'));
```

**Step 2: Commit**

```bash
git add ../platform/supabase/migrations/20260309000000_add_pre_filter_task_type.sql
git commit -m "feat: add pre_filter to task_type CHECK constraint"
```

---

### Task 2: Конфигурация — параметры pre_filter

**Files:**
- Modify: `src/config.py:83-88`
- Test: `tests/test_config.py`

**Step 1: Написать тест**

В `tests/test_config.py` добавить:

```python
def test_pre_filter_defaults() -> None:
    """Дефолты параметров pre_filter."""
    settings = _make_settings()
    assert settings.pre_filter_min_likes == 30
    assert settings.pre_filter_max_inactive_days == 180
    assert settings.pre_filter_posts_to_check == 5
```

Нужно проверить как `_make_settings` создаётся в `test_config.py` — возможно тест использует реальный `Settings`.

**Step 2: Запустить тест — убедиться что падает**

```bash
uv run pytest tests/test_config.py::test_pre_filter_defaults -v
```

**Step 3: Добавить параметры в Settings**

В `src/config.py`, после строки 88 (`posts_with_comments`):

```python
    # Pre-filter параметры
    pre_filter_min_likes: int = 30
    pre_filter_max_inactive_days: int = 180
    pre_filter_posts_to_check: int = 5
```

**Step 4: Запустить тест**

```bash
uv run pytest tests/test_config.py::test_pre_filter_defaults -v
```

**Step 5: Commit**

```bash
git add src/config.py tests/test_config.py
git commit -m "feat: add pre_filter config parameters"
```

---

### Task 3: API schemas — PreFilterRequest/Response

**Files:**
- Modify: `src/api/schemas.py`
- Test: `tests/test_api/test_schemas.py`

**Step 1: Написать тесты**

В `tests/test_api/test_schemas.py` добавить:

```python
class TestPreFilterRequest:
    """Валидация PreFilterRequest."""

    def test_valid_usernames(self) -> None:
        req = PreFilterRequest(usernames=["blogger1", "blogger2"])
        assert req.usernames == ["blogger1", "blogger2"]

    def test_cleans_and_deduplicates(self) -> None:
        req = PreFilterRequest(usernames=["@Blogger1", "blogger1", "Blogger2"])
        assert req.usernames == ["blogger1", "blogger2"]

    def test_empty_rejected(self) -> None:
        with pytest.raises(ValidationError):
            PreFilterRequest(usernames=[])

    def test_max_100(self) -> None:
        with pytest.raises(ValidationError):
            PreFilterRequest(usernames=[f"user{i}" for i in range(101)])
```

**Step 2: Запустить — убедиться что падает**

```bash
uv run pytest tests/test_api/test_schemas.py::TestPreFilterRequest -v
```

**Step 3: Добавить схемы в `src/api/schemas.py`**

`PreFilterRequest` — идентична `ScrapeRequest` (та же валидация username). Можно использовать `ScrapeRequest` напрямую, но для семантической ясности лучше отдельный класс.

```python
class PreFilterRequest(BaseModel):
    """Запрос на создание pre_filter задач."""

    usernames: list[str] = Field(min_length=1, max_length=100)

    @field_validator("usernames")
    @classmethod
    def clean_usernames(cls, v: list[str]) -> list[str]:
        """Очистить, провалидировать и дедуплицировать username-ы."""
        cleaned: list[str] = []
        seen: set[str] = set()
        for name in v:
            name = name.strip().lstrip("@").lower()
            if not name or name in seen:
                continue
            if len(name) > 30:
                raise ValueError(f"username too long (max 30): {name}")
            if not _USERNAME_RE.match(name):
                raise ValueError(f"invalid username format: {name}")
            cleaned.append(name)
            seen.add(name)
        if not cleaned:
            raise ValueError("usernames must not be empty after cleaning")
        return cleaned


class PreFilterResponse(BaseModel):
    """Ответ на POST /api/tasks/pre_filter."""

    created: int
    skipped: int
    errors: int = 0
    tasks: list[ScrapeTaskResult]
```

Также обновить `TaskResponse.task_type`:

```python
task_type: Literal["full_scrape", "ai_analysis", "discover", "pre_filter"]
```

И `list_tasks` query parameter в `src/api/app.py`:

```python
task_type: Literal["full_scrape", "ai_analysis", "discover", "pre_filter"] | None = Query(default=None),
```

**Step 4: Запустить тесты**

```bash
uv run pytest tests/test_api/test_schemas.py -v
```

**Step 5: Commit**

```bash
git add src/api/schemas.py tests/test_api/test_schemas.py
git commit -m "feat: add PreFilterRequest/Response schemas"
```

---

### Task 4: Эндпоинт POST /api/tasks/pre_filter

**Files:**
- Modify: `src/api/app.py`
- Modify: `src/api/schemas.py` (import)
- Test: `tests/test_api/test_routes_pre_filter.py` (create)

**Step 1: Создать тесты**

Файл `tests/test_api/test_routes_pre_filter.py`:

```python
"""Тесты POST /api/tasks/pre_filter — создание pre_filter задач."""
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from tests.test_api.conftest import AUTH_HEADERS, make_app


class TestPreFilterEndpoint:
    """POST /api/tasks/pre_filter."""

    def test_creates_task_for_new_username(self) -> None:
        """Новый username → создать задачу pre_filter."""
        app = make_app()
        with (
            patch("src.api.app.find_blog_by_username", new_callable=AsyncMock, return_value=None),
            patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            mock_create.return_value = "task-1"
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
        mock_create.assert_called_once()

    def test_skips_existing_blog(self) -> None:
        """Username с существующим блогом → skip."""
        app = make_app()
        with patch("src.api.app.find_blog_by_username", new_callable=AsyncMock, return_value="blog-1"):
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

    def test_skips_existing_task(self) -> None:
        """Задача уже существует → skip."""
        app = make_app()
        with (
            patch("src.api.app.find_blog_by_username", new_callable=AsyncMock, return_value=None),
            patch("src.api.app.create_task_if_not_exists", new_callable=AsyncMock, return_value=None),
        ):
            client = TestClient(app)
            resp = client.post(
                "/api/tasks/pre_filter",
                json={"usernames": ["existing_task"]},
                headers=AUTH_HEADERS,
            )

        assert resp.status_code == 201
        data = resp.json()
        assert data["created"] == 0
        assert data["skipped"] == 1

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
```

**Step 2: Запустить — убедиться что падает**

```bash
uv run pytest tests/test_api/test_routes_pre_filter.py -v
```

**Step 3: Добавить эндпоинт в `src/api/app.py`**

Добавить импорты:

```python
from src.api.schemas import (
    ...,
    PreFilterRequest,
    PreFilterResponse,
)
from src.api.services import find_blog_by_username
```

Добавить эндпоинт (после `scrape`, перед `discover`):

```python
    @app.post(
        "/api/tasks/pre_filter",
        response_model=PreFilterResponse,
        dependencies=[Depends(check_rate_limit), Depends(verify_api_key)],
    )
    async def pre_filter(body: PreFilterRequest, response: Response) -> dict:
        """Создать pre_filter задачи для проверки блогеров."""
        results = []
        created = 0
        skipped = 0
        errors = 0

        for username in body.usernames:
            try:
                # Если блогер уже в БД — пропускаем
                existing_blog_id = await find_blog_by_username(db, username)
                if existing_blog_id is not None:
                    results.append({
                        "task_id": None, "username": username,
                        "blog_id": existing_blog_id, "status": "skipped",
                        "reason": "blog already exists",
                    })
                    skipped += 1
                    continue

                # Создать задачу без привязки к blog (blog_id=None, username в payload)
                task_id = await create_task_if_not_exists(
                    db, None, "pre_filter", priority=8,
                    payload={"username": username},
                )

                if task_id:
                    results.append({
                        "task_id": task_id, "username": username,
                        "blog_id": None, "status": "created",
                    })
                    created += 1
                else:
                    results.append({
                        "task_id": None, "username": username,
                        "blog_id": None, "status": "skipped",
                    })
                    skipped += 1
            except Exception as exc:
                logger.error(f"Ошибка при обработке pre_filter {username}: {exc}")
                results.append({
                    "task_id": None, "username": username,
                    "blog_id": None, "status": "error",
                })
                errors += 1

        response.status_code = 207 if errors > 0 else 201
        return {"created": created, "skipped": skipped, "errors": errors, "tasks": results}
```

**Важно**: `pre_filter` задача создаётся с `blog_id=None` (блог ещё не существует), username передаётся в `payload`. Это аналогично discover-задачам. Но нужно обновить unique index — сейчас дедупликация discover идёт по `payload->>'hashtag'`. Для pre_filter нужен аналогичный index по `payload->>'username'`.

Добавить в миграцию (Task 1):

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_scrape_tasks_active_pre_filter_username
  ON scrape_tasks ((payload->>'username'))
  WHERE blog_id IS NULL
    AND task_type = 'pre_filter'
    AND status IN ('pending', 'running')
    AND payload ? 'username';
```

**Step 4: Запустить тесты**

```bash
uv run pytest tests/test_api/test_routes_pre_filter.py -v
```

**Step 5: Commit**

```bash
git add src/api/app.py src/api/schemas.py tests/test_api/test_routes_pre_filter.py
git commit -m "feat: add POST /api/tasks/pre_filter endpoint"
```

---

### Task 5: pre_filter handler

**Files:**
- Create: `src/worker/pre_filter_handler.py`
- Test: `tests/test_worker/test_pre_filter_handler.py` (create)

**Step 1: Создать тесты**

Файл `tests/test_worker/test_pre_filter_handler.py`:

```python
"""Тесты pre_filter handler."""
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import make_settings, make_task


def _make_user_info(*, is_private: bool = False, pk: str = "12345") -> dict:
    """Создать ответ user_by_username_v2."""
    return {
        "user": {
            "pk": pk,
            "username": "testblogger",
            "full_name": "Test Blogger",
            "is_private": is_private,
            "follower_count": 5000,
            "following_count": 200,
            "media_count": 100,
            "is_verified": False,
            "is_business": False,
        }
    }


def _make_media(like_count: int, days_ago: int = 1) -> dict:
    """Создать media dict с like_count и taken_at."""
    taken_at = int((datetime.now(UTC) - timedelta(days=days_ago)).timestamp())
    return {
        "pk": f"media_{days_ago}",
        "like_count": like_count,
        "comment_count": 0,
        "taken_at": taken_at,
        "media_type": 1,
    }


class TestPreFilterHandler:
    """Тесты handle_pre_filter."""

    @pytest.mark.asyncio
    async def test_private_account_filtered_out(self) -> None:
        """Приватный аккаунт → filtered_out."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "private_user"})
        mock_db = MagicMock()
        mock_db.rpc.return_value.execute.return_value = MagicMock(data=[{"mark_task_running": task["id"]}])

        mock_scraper = MagicMock()
        mock_scraper.cl = MagicMock()

        with (
            patch("src.worker.pre_filter_handler.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler.mark_task_done", new_callable=AsyncMock) as mock_done,
            patch("src.worker.pre_filter_handler.run_in_thread", new_callable=AsyncMock) as mock_rit,
            patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread,
        ):
            mock_to_thread.return_value = _make_user_info(is_private=True)
            settings = make_settings(
                pre_filter_min_likes=30,
                pre_filter_max_inactive_days=180,
                pre_filter_posts_to_check=5,
            )

            await handle_pre_filter(mock_db, task, mock_scraper, settings)

            mock_done.assert_called_once()

    @pytest.mark.asyncio
    async def test_low_engagement_filtered_out(self) -> None:
        """Среднее лайков < 30 → filtered_out."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "low_er_user"})
        mock_db = MagicMock()

        with (
            patch("src.worker.pre_filter_handler.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler.mark_task_done", new_callable=AsyncMock) as mock_done,
            patch("src.worker.pre_filter_handler.run_in_thread", new_callable=AsyncMock),
            patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread,
        ):
            # Первый вызов — user_info, второй — медиа
            mock_to_thread.side_effect = [
                _make_user_info(),
                ([_make_media(10), _make_media(20), _make_media(5)], None),
            ]
            settings = make_settings(
                pre_filter_min_likes=30,
                pre_filter_max_inactive_days=180,
                pre_filter_posts_to_check=5,
            )

            await handle_pre_filter(mock_db, task, MagicMock(), settings)

            mock_done.assert_called_once()

    @pytest.mark.asyncio
    async def test_inactive_account_filtered_out(self) -> None:
        """Нет публикаций за 180 дней → filtered_out."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "inactive_user"})
        mock_db = MagicMock()

        with (
            patch("src.worker.pre_filter_handler.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler.mark_task_done", new_callable=AsyncMock) as mock_done,
            patch("src.worker.pre_filter_handler.run_in_thread", new_callable=AsyncMock),
            patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread,
        ):
            mock_to_thread.side_effect = [
                _make_user_info(),
                ([_make_media(100, days_ago=200)], None),
            ]
            settings = make_settings(
                pre_filter_min_likes=30,
                pre_filter_max_inactive_days=180,
                pre_filter_posts_to_check=5,
            )

            await handle_pre_filter(mock_db, task, MagicMock(), settings)

            mock_done.assert_called_once()

    @pytest.mark.asyncio
    async def test_passed_creates_person_and_blog(self) -> None:
        """Прошедший фильтр → создаёт person + blog."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "good_user"})
        mock_db = MagicMock()

        person_result = MagicMock()
        person_result.data = [{"id": "person-1"}]
        blog_result = MagicMock()
        blog_result.data = [{"id": "blog-1"}]

        with (
            patch("src.worker.pre_filter_handler.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler.mark_task_done", new_callable=AsyncMock) as mock_done,
            patch("src.worker.pre_filter_handler.run_in_thread", new_callable=AsyncMock) as mock_rit,
            patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread,
        ):
            mock_to_thread.side_effect = [
                _make_user_info(),
                ([_make_media(100, days_ago=5), _make_media(80, days_ago=10)], None),
            ]
            # insert person, insert blog
            mock_rit.side_effect = [person_result, blog_result]
            settings = make_settings(
                pre_filter_min_likes=30,
                pre_filter_max_inactive_days=180,
                pre_filter_posts_to_check=5,
            )

            await handle_pre_filter(mock_db, task, MagicMock(), settings)

            mock_done.assert_called_once()
            # Проверяем что person и blog были созданы
            assert mock_rit.call_count == 2

    @pytest.mark.asyncio
    async def test_no_posts_filtered_out(self) -> None:
        """Нет постов вообще → filtered_out: inactive."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "no_posts_user"})
        mock_db = MagicMock()

        with (
            patch("src.worker.pre_filter_handler.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler.mark_task_done", new_callable=AsyncMock) as mock_done,
            patch("src.worker.pre_filter_handler.run_in_thread", new_callable=AsyncMock),
            patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread,
        ):
            mock_to_thread.side_effect = [
                _make_user_info(),
                ([], None),  # пустой список медиа
            ]
            settings = make_settings(
                pre_filter_min_likes=30,
                pre_filter_max_inactive_days=180,
                pre_filter_posts_to_check=5,
            )

            await handle_pre_filter(mock_db, task, MagicMock(), settings)

            mock_done.assert_called_once()
```

**Step 2: Запустить — убедиться что падает**

```bash
uv run pytest tests/test_worker/test_pre_filter_handler.py -v
```

**Step 3: Реализовать handler**

Файл `src/worker/pre_filter_handler.py`:

```python
"""Обработчик задач pre_filter — лёгкая проверка профиля перед full_scrape."""
import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from instagrapi.exceptions import UserNotFound
from loguru import logger
from supabase import Client

from src.config import Settings
from src.database import (
    cleanup_orphan_person,
    mark_task_done,
    mark_task_failed,
    mark_task_running,
    run_in_thread,
    sanitize_error,
)
from src.platforms.instagram.exceptions import (
    AllAccountsCooldownError,
    HikerAPIError,
    InsufficientBalanceError,
    PrivateAccountError,
)


async def handle_pre_filter(
    db: Client,
    task: dict[str, Any],
    scraper: Any,
    settings: Settings,
) -> None:
    """
    Проверка профиля по 3 критериям:
    1. Приватный аккаунт → filtered_out
    2. Среднее лайков < pre_filter_min_likes → filtered_out
    3. Нет публикаций за pre_filter_max_inactive_days → filtered_out

    Прошедший → создать person + blog (scrape_status='pending').
    """
    task_id = task["id"]
    payload = task.get("payload") or {}
    username = payload.get("username", "")

    if not username:
        await mark_task_failed(db, task_id, task["attempts"], task["max_attempts"],
                               "No username in payload", retry=False)
        return

    was_claimed = await mark_task_running(db, task_id)
    if not was_claimed:
        logger.debug(f"Task {task_id} was already claimed by another worker")
        return

    current_attempts = task["attempts"] + 1

    try:
        # 1. Получить информацию о пользователе
        response = await asyncio.to_thread(scraper.cl.user_by_username_v2, username)
        user = response.get("user", {})

        if not user or not user.get("pk"):
            await _mark_filtered(db, task_id, "not_found", username)
            return

        # Приватный аккаунт
        if user.get("is_private"):
            await _mark_filtered(db, task_id, "private", username)
            return

        user_id = str(user["pk"])

        # 2. Получить последние посты
        result = await asyncio.to_thread(scraper.cl.user_medias_chunk_v1, user_id)
        raw_medias: list[dict[str, Any]] = result[0] if isinstance(result, list) and result else []

        # Берём только N последних
        medias = raw_medias[:settings.pre_filter_posts_to_check]

        # Нет публикаций
        if not medias:
            await _mark_filtered(db, task_id, "inactive", username)
            return

        # 3. Проверка активности — последний пост старше max_inactive_days
        threshold = datetime.now(UTC) - timedelta(days=settings.pre_filter_max_inactive_days)
        latest_taken_at = _parse_taken_at(medias[0].get("taken_at"))
        if latest_taken_at and latest_taken_at < threshold:
            await _mark_filtered(db, task_id, "inactive", username)
            return

        # 4. Проверка среднего количества лайков
        likes = [m.get("like_count", 0) for m in medias]
        avg_likes = sum(likes) / len(likes) if likes else 0
        if avg_likes < settings.pre_filter_min_likes:
            await _mark_filtered(db, task_id, "low_engagement", username)
            return

        # 5. Прошёл все проверки — создать person + blog
        person_id: str | None = None
        try:
            person_result = await run_in_thread(
                db.table("persons")
                .insert({"full_name": user.get("full_name") or username})
                .execute
            )
            person_id = person_result.data[0]["id"]

            blog_result = await run_in_thread(
                db.table("blogs")
                .insert({
                    "person_id": person_id,
                    "platform": "instagram",
                    "username": username,
                    "platform_id": user_id,
                    "followers_count": user.get("follower_count", 0),
                    "source": "xlsx_import",
                    "scrape_status": "pending",
                })
                .execute
            )
            blog_id = blog_result.data[0]["id"]

            await mark_task_done(db, task_id)
            logger.info(f"[pre_filter] @{username} passed → blog={blog_id}")

        except Exception as e:
            if person_id:
                await cleanup_orphan_person(db, person_id)
            await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                                   sanitize_error(str(e)), retry=True)
            return

    except PrivateAccountError:
        await _mark_filtered(db, task_id, "private", username)
    except UserNotFound:
        await _mark_filtered(db, task_id, "not_found", username)
    except InsufficientBalanceError as e:
        logger.error(f"[pre_filter] HikerAPI баланс исчерпан: {e}")
        await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                               "HikerAPI: insufficient balance", retry=False)
    except HikerAPIError as e:
        retry = e.status_code in (429, 500, 502, 503, 504)
        await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                               sanitize_error(str(e)), retry=retry)
    except AllAccountsCooldownError as e:
        await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                               sanitize_error(str(e)), retry=True)
    except Exception as e:
        await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                               sanitize_error(str(e)), retry=True)


def _parse_taken_at(value: Any) -> datetime | None:
    """Парсить taken_at — может быть int (timestamp) или str."""
    if isinstance(value, int):
        return datetime.fromtimestamp(value, tz=UTC)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


async def _mark_filtered(db: Client, task_id: str, reason: str, username: str) -> None:
    """Пометить задачу как done с результатом filtered_out."""
    await run_in_thread(
        db.table("scrape_tasks").update({
            "status": "done",
            "completed_at": datetime.now(UTC).isoformat(),
            "result": {"filtered_out": reason, "username": username},
        }).eq("id", task_id).execute
    )
    logger.info(f"[pre_filter] @{username} filtered_out: {reason}")
```

**Примечание:** в БД `scrape_tasks` нет колонки `result`. Есть два варианта:
- Использовать `payload` для записи результата (не идеально — это входные данные)
- Использовать `error_message` для записи причины отсева
- Добавить колонку `result` (миграция)

Простейший вариант — записывать причину в `error_message` при статусе `done`:

```python
async def _mark_filtered(db: Client, task_id: str, reason: str, username: str) -> None:
    """Пометить задачу как done с причиной отсева в error_message."""
    await run_in_thread(
        db.table("scrape_tasks").update({
            "status": "done",
            "completed_at": datetime.now(UTC).isoformat(),
            "error_message": f"filtered_out: {reason}",
        }).eq("id", task_id).execute
    )
    logger.info(f"[pre_filter] @{username} filtered_out: {reason}")
```

**Step 4: Запустить тесты**

```bash
uv run pytest tests/test_worker/test_pre_filter_handler.py -v
```

**Step 5: Commit**

```bash
git add src/worker/pre_filter_handler.py tests/test_worker/test_pre_filter_handler.py
git commit -m "feat: add pre_filter handler with 3 filter criteria"
```

---

### Task 6: Интеграция handler в worker loop

**Files:**
- Modify: `src/worker/loop.py:25-43`
- Modify: `src/worker/handlers.py`
- Modify: `src/database.py:239-244` (recover_stuck_tasks — добавить pre_filter)
- Test: `tests/test_worker/test_loop.py`

**Step 1: Написать тест**

В `tests/test_worker/test_loop.py` добавить (или найти существующий паттерн):

```python
def test_resolve_handler_pre_filter() -> None:
    """pre_filter task_type резолвится в handle_pre_filter."""
    from src.worker.loop import _resolve_handler
    handler = _resolve_handler("pre_filter")
    assert handler is not None
```

**Step 2: Запустить — убедиться что падает**

```bash
uv run pytest tests/test_worker/test_loop.py::test_resolve_handler_pre_filter -v
```

**Step 3: Обновить loop.py**

В `src/worker/loop.py`:

1. Добавить import:
```python
from src.worker.handlers import (
    handle_ai_analysis,
    handle_discover,
    handle_full_scrape,
    handle_pre_filter,
)
```

2. Обновить `TASK_DEPS`:
```python
TASK_DEPS: dict[str, list[str]] = {
    "full_scrape": ["scraper"],
    "ai_analysis": ["openai"],
    "discover": ["scraper"],
    "pre_filter": ["scraper"],
}
```

3. Обновить `_resolve_handler`:
```python
dispatch: dict[str, TaskHandler] = {
    "full_scrape": handle_full_scrape,
    "ai_analysis": handle_ai_analysis,
    "discover": handle_discover,
    "pre_filter": handle_pre_filter,
}
```

В `src/worker/handlers.py` добавить re-export:
```python
from src.worker.pre_filter_handler import handle_pre_filter  # noqa: F401
```

В `src/database.py`, функция `recover_stuck_tasks` (строка 244):
```python
.in_("task_type", ["full_scrape", "discover", "pre_filter"])
```

**Step 4: Запустить тесты**

```bash
uv run pytest tests/test_worker/test_loop.py -v
```

**Step 5: Commit**

```bash
git add src/worker/loop.py src/worker/handlers.py src/database.py tests/test_worker/test_loop.py
git commit -m "feat: integrate pre_filter handler into worker loop"
```

---

### Task 7: Скрипт import_xlsx

**Files:**
- Create: `src/scripts/__init__.py` (если нет)
- Create: `src/scripts/import_xlsx.py`
- Test: `tests/test_scripts/test_import_xlsx.py` (create)

**Step 1: Создать тест**

Файл `tests/test_scripts/__init__.py` (пустой).

Файл `tests/test_scripts/test_import_xlsx.py`:

```python
"""Тесты скрипта импорта xlsx."""
import pytest

from src.scripts.import_xlsx import extract_usernames


class TestExtractUsernames:
    """Извлечение и дедупликация username-ов из DataFrame."""

    def test_extracts_unique_usernames(self) -> None:
        import pandas as pd
        df = pd.DataFrame({"username": ["user1", "user2", "user1", "user3"]})
        result = extract_usernames(df)
        assert result == ["user1", "user2", "user3"]

    def test_strips_whitespace(self) -> None:
        import pandas as pd
        df = pd.DataFrame({"username": [" user1 ", "user2"]})
        result = extract_usernames(df)
        assert result == ["user1", "user2"]

    def test_lowercases(self) -> None:
        import pandas as pd
        df = pd.DataFrame({"username": ["User1", "USER2"]})
        result = extract_usernames(df)
        assert result == ["user1", "user2"]

    def test_skips_empty(self) -> None:
        import pandas as pd
        df = pd.DataFrame({"username": ["user1", "", None, "user2"]})
        result = extract_usernames(df)
        assert result == ["user1", "user2"]
```

**Step 2: Запустить — убедиться что падает**

```bash
uv run pytest tests/test_scripts/test_import_xlsx.py -v
```

**Step 3: Реализовать скрипт**

Создать `src/scripts/__init__.py` (пустой, если нет).

Файл `src/scripts/import_xlsx.py`:

```python
"""Импорт блогеров из xlsx файла через API pre_filter."""
import argparse
import asyncio
import sys
import time

import httpx
import pandas as pd
from loguru import logger


def extract_usernames(df: pd.DataFrame) -> list[str]:
    """Извлечь уникальные username-ы из DataFrame."""
    result: list[str] = []
    seen: set[str] = set()
    for raw in df["username"]:
        if not isinstance(raw, str) or not raw.strip():
            continue
        name = raw.strip().lower()
        if name not in seen:
            result.append(name)
            seen.add(name)
    return result


async def send_batch(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    usernames: list[str],
) -> dict:
    """Отправить батч username-ов на API."""
    resp = await client.post(
        f"{base_url}/api/tasks/pre_filter",
        json={"usernames": usernames},
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()


async def run(
    file_path: str,
    base_url: str,
    api_key: str,
    batch_size: int,
    delay: float,
) -> None:
    """Основной цикл: читает xlsx → шлёт батчами."""
    logger.info(f"Читаем {file_path}...")
    df = pd.read_excel(file_path)
    usernames = extract_usernames(df)
    logger.info(f"Найдено {len(usernames)} уникальных username-ов")

    total_created = 0
    total_skipped = 0
    total_errors = 0
    batches = [usernames[i:i + batch_size] for i in range(0, len(usernames), batch_size)]

    async with httpx.AsyncClient() as client:
        for i, batch in enumerate(batches, 1):
            try:
                data = await send_batch(client, base_url, api_key, batch)
                total_created += data.get("created", 0)
                total_skipped += data.get("skipped", 0)
                total_errors += data.get("errors", 0)
                logger.info(
                    f"Батч {i}/{len(batches)}: "
                    f"+{data.get('created', 0)} создано, "
                    f"~{data.get('skipped', 0)} пропущено, "
                    f"!{data.get('errors', 0)} ошибок"
                )
            except httpx.HTTPStatusError as e:
                logger.error(f"Батч {i}/{len(batches)} ошибка: {e.response.status_code} {e.response.text}")
                total_errors += len(batch)
            except Exception as e:
                logger.error(f"Батч {i}/{len(batches)} ошибка: {e}")
                total_errors += len(batch)

            if delay > 0 and i < len(batches):
                await asyncio.sleep(delay)

    logger.info(
        f"Импорт завершён: "
        f"создано={total_created}, пропущено={total_skipped}, ошибок={total_errors}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Импорт блогеров из xlsx")
    parser.add_argument("file", help="Путь к xlsx файлу")
    parser.add_argument("--base-url", default="http://localhost:8001", help="URL скрапера")
    parser.add_argument("--api-key", required=True, help="SCRAPER_API_KEY")
    parser.add_argument("--batch-size", type=int, default=100, help="Размер батча")
    parser.add_argument("--delay", type=float, default=0.1, help="Пауза между батчами (сек)")
    args = parser.parse_args()

    asyncio.run(run(args.file, args.base_url, args.api_key, args.batch_size, args.delay))


if __name__ == "__main__":
    main()
```

**Step 4: Запустить тесты**

```bash
uv run pytest tests/test_scripts/test_import_xlsx.py -v
```

**Step 5: Commit**

```bash
git add src/scripts/ tests/test_scripts/
git commit -m "feat: add import_xlsx script for batch pre_filter"
```

---

### Task 8: Финальная проверка

**Step 1: Запустить все тесты**

```bash
uv run pytest tests/ -v
```

**Step 2: Линтер и тайпчекер**

```bash
make lint && make typecheck
```

**Step 3: Исправить ошибки, если есть**

**Step 4: Финальный commit (если были исправления)**

```bash
git add -A
git commit -m "fix: address lint and type errors in pre_filter implementation"
```
