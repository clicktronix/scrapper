# Audit Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Исправить все находки аудита кодовой базы scraper service (P0→P2).

**Architecture:** Инкрементальные изменения с сохранением обратной совместимости. Repository Pattern — финальный рефакторинг, т.к. затрагивает все слои. Каждая задача — отдельный коммит.

**Tech Stack:** Python 3.13, FastAPI, Pydantic, Supabase, OpenAI, rapidfuzz (новая зависимость).

**Важно:** Тесты: `uv run pytest tests/ -v`. Линтер: `uv run ruff check src/ tests/`. Типы: `uv run pyright src/`.

---

## Task 1: RateLimiter — asyncio.Lock для thread-safety (P0)

**Files:**
- Modify: `src/api/rate_limiter.py`
- Test: `tests/test_api/test_rate_limiter.py`

**Step 1: Написать тест на конкурентный доступ**

В `tests/test_api/test_rate_limiter.py` добавить:

```python
async def test_concurrent_requests_respect_limit():
    """Конкурентные запросы не должны превышать лимит."""
    limiter = RateLimiter(max_requests=5, window_seconds=60)
    request = _make_request("10.0.0.1")

    # Запускаем 10 конкурентных запросов при лимите 5
    results = await asyncio.gather(
        *[_safe_check(limiter, request) for _ in range(10)],
    )
    passed = sum(1 for r in results if r is True)
    blocked = sum(1 for r in results if r is False)

    assert passed == 5
    assert blocked == 5


async def _safe_check(limiter: RateLimiter, request) -> bool:
    """True если запрос прошёл, False если заблокирован."""
    try:
        await limiter.check(request)
        return True
    except HTTPException:
        return False
```

Также добавить `import asyncio` в начало файла и импортировать `HTTPException` из fastapi.

**Step 2: Запустить тест — убедиться что падает**

```bash
uv run pytest tests/test_api/test_rate_limiter.py::test_concurrent_requests_respect_limit -v
```
Ожидаем: FAIL (passed > 5 из-за race condition).

**Step 3: Добавить asyncio.Lock в RateLimiter**

В `src/api/rate_limiter.py`:

```python
import asyncio
import time
from collections import defaultdict

from fastapi import HTTPException, Request


class RateLimiter:
    """Простой in-memory rate limiter на основе sliding window per IP.

    Аргументы:
        max_requests: максимальное количество запросов в окне.
        window_seconds: размер окна в секундах.
    """

    def __init__(self, max_requests: int = 60, window_seconds: int = 60) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._store: dict[str, list[float]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def check(self, request: Request) -> None:
        """Проверить rate limit для запроса. Бросает HTTPException 429 при превышении.

        Примечание: при использовании reverse proxy (nginx/traefik) убедитесь,
        что request.client.host содержит реальный IP клиента (X-Forwarded-For),
        иначе все запросы будут считаться от одного IP прокси.
        """
        # За reverse proxy (Railway, nginx) request.client.host = IP прокси
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            client_ip = forwarded.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client else "unknown"

        now = time.time()
        window_start = now - self.window_seconds

        async with self._lock:
            # Очистить устаревшие записи для этого IP
            timestamps = self._store[client_ip]
            self._store[client_ip] = [t for t in timestamps if t > window_start]

            if len(self._store[client_ip]) >= self.max_requests:
                raise HTTPException(status_code=429, detail="Rate limit exceeded")

            self._store[client_ip].append(now)

            # Периодическая очистка стухших IP (при росте store > 100 записей)
            self._cleanup_stale(window_start)

    def _cleanup_stale(self, window_start: float) -> None:
        """Удалить IP-адреса без актуальных запросов (при превышении 100 записей в store)."""
        if len(self._store) > 100:
            stale_ips = [
                ip for ip, ts in self._store.items()
                if not ts or max(ts) <= window_start
            ]
            for ip in stale_ips:
                del self._store[ip]
```

**Step 4: Запустить все тесты rate_limiter**

```bash
uv run pytest tests/test_api/test_rate_limiter.py -v
```
Ожидаем: ALL PASS.

**Step 5: Коммит**

```bash
git add src/api/rate_limiter.py tests/test_api/test_rate_limiter.py
git commit -m "fix: add asyncio.Lock to RateLimiter for thread-safety (P0)"
```

---

## Task 2: Error handling в fetch_tasks_list (P0)

**Files:**
- Modify: `src/api/services.py`
- Test: `tests/test_api/test_services.py`

**Step 1: Написать тест на ошибку БД**

В `tests/test_api/test_services.py` добавить класс:

```python
class TestFetchTasksList:
    async def test_returns_tasks_and_total(self):
        db = _mock_supabase()
        db.table.return_value.select.return_value.order.return_value.range.return_value.execute.return_value = (
            MagicMock(data=[{"id": "t1", "status": "pending"}], count=1)
        )
        result = await fetch_tasks_list(db)
        assert result["tasks"] == [{"id": "t1", "status": "pending"}]
        assert result["total"] == 1

    async def test_returns_empty_on_db_error(self):
        db = _mock_supabase()
        db.table.return_value.select.return_value.order.return_value.range.return_value.execute.side_effect = (
            Exception("connection refused")
        )
        result = await fetch_tasks_list(db)
        assert result["tasks"] == []
        assert result["total"] == 0
        assert result["error"] is not None
```

Нужно также добавить импорт `fetch_tasks_list` и вспомогательную mock-функцию, если её нет.

**Step 2: Запустить тест — убедиться что test_returns_empty_on_db_error падает**

```bash
uv run pytest tests/test_api/test_services.py::TestFetchTasksList::test_returns_empty_on_db_error -v
```

**Step 3: Добавить error handling**

В `src/api/services.py` заменить `fetch_tasks_list`:

```python
async def fetch_tasks_list(
    db: Client,
    status: str | None = None,
    task_type: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    """Получить список задач с фильтрами и пагинацией."""
    try:
        query = db.table("scrape_tasks").select("*", count=CountMethod.exact)
        if status:
            query = query.eq("status", status)
        if task_type:
            query = query.eq("task_type", task_type)
        query = query.order("created_at", desc=True).range(offset, offset + limit - 1)
        result = await run_in_thread(query.execute)
        return {
            "tasks": result.data,
            "total": result.count or 0,
            "limit": limit,
            "offset": offset,
        }
    except Exception as e:
        logger.error(f"[tasks_list] Ошибка получения задач: {e}")
        return {
            "tasks": [],
            "total": 0,
            "limit": limit,
            "offset": offset,
            "error": str(e),
        }
```

**Step 4: Запустить тесты**

```bash
uv run pytest tests/test_api/test_services.py -v
```

**Step 5: Коммит**

```bash
git add src/api/services.py tests/test_api/test_services.py
git commit -m "fix: add error handling to fetch_tasks_list (P0)"
```

---

## Task 3: Расширить правила Ruff (P1)

**Files:**
- Modify: `pyproject.toml`

**Step 1: Обновить ruff конфигурацию**

В `pyproject.toml` секцию `[tool.ruff.lint]` заменить:

```toml
[tool.ruff.lint]
select = ["E", "F", "W", "I", "UP", "B", "SIM", "RUF", "ASYNC"]
ignore = [
    "B008",    # function-call-in-default-argument (FastAPI Depends)
    "SIM108",  # use-ternary-operator (часто менее читабельно)
    "RUF012",  # mutable-class-default (Pydantic models)
]
```

**Step 2: Запустить ruff и проверить новые ошибки**

```bash
uv run ruff check src/ tests/ 2>&1 | head -50
```

**Step 3: Исправить все найденные ошибки**

Запустить автофикс:
```bash
uv run ruff check --fix src/ tests/
```

Оставшиеся ошибки исправить вручную. Типичные:
- `B006` — mutable default argument → `Field(default_factory=list)`
- `SIM` — упрощение условий
- `RUF` — Python-специфичные паттерны

**Step 4: Убедиться что все тесты проходят**

```bash
uv run ruff check src/ tests/ && uv run pytest tests/ -v --tb=short
```

**Step 5: Коммит**

```bash
git add -A
git commit -m "chore: extend ruff rules with B, SIM, RUF, ASYNC (P1)"
```

---

## Task 4: Типизация run_in_thread через ParamSpec (P1)

**Files:**
- Modify: `src/database.py`

**Step 1: Обновить сигнатуру run_in_thread**

В `src/database.py` заменить импорты и функцию:

```python
import asyncio
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

from loguru import logger
from supabase import Client

from src.utils import is_transient_network_error

_RUN_IN_THREAD_MAX_RETRIES = 3
_RUN_IN_THREAD_RETRY_DELAY = 2.0

_T = TypeVar("_T")


async def run_in_thread(
    func: Callable[..., _T], *args: Any, retry_transient: bool = False, **kwargs: Any
) -> _T:
    """Выполнить синхронный вызов Supabase в отдельном потоке.

    По умолчанию retry отключен, чтобы не дублировать неидемпотентные операции
    (например insert) при сетевых ошибках после частичного успеха.
    Для идемпотентных вызовов включайте retry_transient=True.
    """
    if not retry_transient:
        return await asyncio.to_thread(func, *args, **kwargs)

    for attempt in range(1, _RUN_IN_THREAD_MAX_RETRIES + 1):
        try:
            return await asyncio.to_thread(func, *args, **kwargs)
        except Exception as e:
            if is_transient_network_error(e) and attempt < _RUN_IN_THREAD_MAX_RETRIES:
                logger.warning(
                    f"[run_in_thread] Транзиентная ошибка (попытка {attempt}/"
                    f"{_RUN_IN_THREAD_MAX_RETRIES}): {e}"
                )
                await asyncio.sleep(_RUN_IN_THREAD_RETRY_DELAY * attempt)
                continue
            raise
    raise RuntimeError("Unreachable")  # для type checker
```

**Step 2: Проверить типы и тесты**

```bash
uv run pyright src/ && uv run pytest tests/ -v --tb=short
```

**Step 3: Коммит**

```bash
git add src/database.py
git commit -m "refactor: type run_in_thread with Callable[..., T] return type (P1)"
```

---

## Task 5: In-memory кэш для taxonomy (P1)

**Files:**
- Modify: `src/ai/taxonomy_matching.py`
- Test: `tests/test_ai/test_taxonomy.py`

**Step 1: Написать тест на кэширование**

В `tests/test_ai/test_taxonomy.py` добавить:

```python
from unittest.mock import AsyncMock, MagicMock, patch

from src.ai.taxonomy_matching import load_categories, load_tags, load_cities, invalidate_taxonomy_cache


class TestTaxonomyCache:
    async def test_load_categories_cached(self):
        """Повторный вызов load_categories не делает запрос в БД."""
        invalidate_taxonomy_cache()
        db = MagicMock()
        db.table.return_value.select.return_value.execute.return_value = MagicMock(
            data=[{"id": "cat1", "code": "beauty", "name": "Красота", "parent_id": None}]
        )
        with patch("src.ai.taxonomy_matching.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = db.table.return_value.select.return_value.execute.return_value
            result1 = await load_categories(db)
            result2 = await load_categories(db)
            # Только один вызов БД
            assert mock_run.call_count == 1
            assert result1 is result2

    async def test_invalidate_cache_forces_reload(self):
        """invalidate_taxonomy_cache сбрасывает кэш."""
        invalidate_taxonomy_cache()
        db = MagicMock()
        db.table.return_value.select.return_value.execute.return_value = MagicMock(
            data=[{"id": "cat1", "code": "beauty", "name": "Красота", "parent_id": None}]
        )
        with patch("src.ai.taxonomy_matching.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = db.table.return_value.select.return_value.execute.return_value
            await load_categories(db)
            invalidate_taxonomy_cache()
            await load_categories(db)
            assert mock_run.call_count == 2
```

**Step 2: Запустить тест — убедиться что падает**

```bash
uv run pytest tests/test_ai/test_taxonomy.py::TestTaxonomyCache -v
```

**Step 3: Добавить кэширование**

В `src/ai/taxonomy_matching.py` добавить после `__all__`:

```python
# In-memory кэш для справочников (сбрасывается при рестарте или вручную)
_categories_cache: dict[str, str] | None = None
_tags_cache: dict[str, str] | None = None
_cities_cache: dict[str, str] | None = None


def invalidate_taxonomy_cache() -> None:
    """Сбросить кэш справочников. Вызывать при обновлении таксономии."""
    global _categories_cache, _tags_cache, _cities_cache
    _categories_cache = None
    _tags_cache = None
    _cities_cache = None
```

Обновить `load_categories`:

```python
async def load_categories(db: Client) -> dict[str, str]:
    """Загрузить все категории из БД (с in-memory кэшем)."""
    global _categories_cache
    if _categories_cache is not None:
        return _categories_cache

    cat_result = await run_in_thread(
        db.table("categories").select("id, code, name, parent_id").execute
    )
    categories: dict[str, str] = {}
    for c in cat_result.data:
        if not isinstance(c, dict):
            continue
        cat_id = c.get("id")
        if not isinstance(cat_id, str):
            continue
        code = c.get("code")
        if isinstance(code, str) and code:
            categories[normalize_lookup_key(code)] = cat_id
        name = c.get("name")
        if isinstance(name, str):
            categories[normalize_lookup_key(name)] = cat_id

    _categories_cache = categories
    return categories
```

Аналогично обновить `load_tags` и `load_cities` с `_tags_cache` и `_cities_cache`.

Добавить `invalidate_taxonomy_cache` в `__all__`.

**Step 4: Запустить все тесты**

```bash
uv run pytest tests/test_ai/test_taxonomy.py -v && uv run pytest tests/ -v --tb=short
```

NB: Если другие тесты патчат `load_categories`/`load_tags`/`load_cities` через `_h`, кэш может мешать. Добавить `invalidate_taxonomy_cache()` в conftest fixture (autouse) при необходимости.

**Step 5: Коммит**

```bash
git add src/ai/taxonomy_matching.py tests/test_ai/test_taxonomy.py
git commit -m "perf: add in-memory cache for taxonomy lookups (P1)"
```

---

## Task 6: Добавить rapidfuzz для fuzzy matching (P2)

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/ai/taxonomy_matching.py`
- Test: `tests/test_ai/test_taxonomy.py`

**Step 1: Добавить зависимость**

```bash
cd /Users/clicktronix/Projects/ai/native/scraper && uv add rapidfuzz
```

**Step 2: Написать тест на fuzzy matching с rapidfuzz**

В `tests/test_ai/test_taxonomy.py` добавить:

```python
class TestFuzzyLookup:
    def test_exact_match(self):
        cache = {"красота": "id1", "мода": "id2"}
        assert _fuzzy_lookup("красота", cache) == "id1"

    def test_normalized_variant_match(self):
        cache = {"видео контент": "id1"}
        assert _fuzzy_lookup("видео-контент", cache) == "id1"

    def test_fuzzy_close_match(self):
        cache = {"профессиональная съёмка": "id1"}
        # Опечатка: "професиональная" (одна с)
        result = _fuzzy_lookup("професиональная съёмка", cache)
        assert result == "id1"

    def test_no_match_returns_none(self):
        cache = {"красота": "id1"}
        assert _fuzzy_lookup("абсолютно другое", cache) is None
```

**Step 3: Заменить difflib на rapidfuzz**

В `src/ai/taxonomy_matching.py`:

Заменить импорт:
```python
# Было: from difflib import get_close_matches
from rapidfuzz import fuzz
```

Заменить `_fuzzy_lookup`:
```python
def _fuzzy_lookup(key: str, cache: dict[str, str], cutoff: float = 80.0) -> str | None:
    """Поиск в кэше: exact → normalized variants → fuzzy (rapidfuzz)."""
    # 1. Exact
    if key in cache:
        return cache[key]

    # 2. Normalized variants
    normalized = normalize_lookup_key(key)
    variants = {
        normalized,
        normalized.replace("-", " "),
        normalized.replace(" ", "-"),
        normalized.replace("-", ""),
        normalized.replace(" ", ""),
    }
    for variant in variants:
        if variant in cache:
            return cache[variant]

    # 3. Fuzzy (rapidfuzz) — быстрее difflib в 10-100x
    best_score = 0.0
    best_key: str | None = None
    for cached_key in cache:
        score = fuzz.ratio(normalized, cached_key)
        if score > best_score:
            best_score = score
            best_key = cached_key
    if best_key is not None and best_score >= cutoff:
        return cache[best_key]
    return None
```

NB: `cutoff` меняется с 0.8 (difflib) на 80.0 (rapidfuzz score 0-100). Все вызывающие функции передают cutoff как keyword — проверить что никто не передаёт 0.8.

**Step 4: Запустить тесты**

```bash
uv run pytest tests/test_ai/test_taxonomy.py -v && uv run pytest tests/ -v --tb=short
```

**Step 5: Коммит**

```bash
git add pyproject.toml uv.lock src/ai/taxonomy_matching.py tests/test_ai/test_taxonomy.py
git commit -m "perf: replace difflib with rapidfuzz for faster fuzzy matching (P2)"
```

---

## Task 7: Расширить sanitize_error (P2)

**Files:**
- Modify: `src/database.py`
- Test: `tests/test_database.py`

**Step 1: Написать тесты**

В `tests/test_database.py` добавить:

```python
class TestSanitizeError:
    def test_masks_url_credentials(self):
        assert "***" in sanitize_error("https://user:pass@host.com/path")

    def test_masks_bearer_token(self):
        result = sanitize_error("Authorization: Bearer sk-abc123xyz")
        assert "sk-abc123xyz" not in result
        assert "Bearer ***" in result

    def test_masks_query_token(self):
        result = sanitize_error("https://api.host.com?token=secret123&other=ok")
        assert "secret123" not in result

    def test_masks_api_key_param(self):
        result = sanitize_error("url?api_key=mysecret&foo=bar")
        assert "mysecret" not in result

    def test_preserves_normal_text(self):
        msg = "Connection refused to database"
        assert sanitize_error(msg) == msg
```

**Step 2: Обновить sanitize_error**

В `src/database.py`:

```python
def sanitize_error(error: str) -> str:
    """Убрать потенциальные креденшалы из сообщения об ошибке."""
    # URL credentials (user:pass@host)
    result = re.sub(r"://[^@\s]+@", "://***:***@", error)
    # Bearer tokens
    result = re.sub(r"Bearer\s+\S+", "Bearer ***", result)
    # Query parameters с ключами (token, api_key, key, secret, password)
    result = re.sub(
        r"((?:token|api_key|key|secret|password|apikey)=)[^&\s]+",
        r"\1***",
        result,
        flags=re.IGNORECASE,
    )
    return result
```

**Step 3: Запустить тесты**

```bash
uv run pytest tests/test_database.py::TestSanitizeError -v && uv run pytest tests/ -v --tb=short
```

**Step 4: Коммит**

```bash
git add src/database.py tests/test_database.py
git commit -m "fix: extend sanitize_error to mask bearer tokens and query params (P2)"
```

---

## Task 8: MIME whitelist для изображений (P2)

**Files:**
- Modify: `src/image_storage.py`
- Test: `tests/test_image_storage.py`

**Step 1: Написать тест на MIME whitelist**

В `tests/test_image_storage.py` найти тесты на `download_image` и добавить:

```python
async def test_rejects_svg_image():
    """SVG не должен проходить проверку MIME."""
    mock_response = httpx.Response(
        200,
        content=b"<svg></svg>",
        headers={"content-type": "image/svg+xml"},
        request=httpx.Request("GET", "https://example.com/img.svg"),
    )
    client = AsyncMock(spec=httpx.AsyncClient)
    client.get = AsyncMock(return_value=mock_response)
    result = await download_image("https://example.com/img.svg", client)
    assert result is None
```

**Step 2: Добавить MIME whitelist**

В `src/image_storage.py` после констант добавить:

```python
_ALLOWED_IMAGE_MIMES = frozenset({
    "image/jpeg",
    "image/png",
    "image/webp",
    "image/gif",
})
```

В функции `download_image` заменить проверку MIME (строки 58-62):

```python
    content_type = response.headers.get("content-type", "image/jpeg")
    mime = content_type.split(";")[0].strip()
    if mime not in _ALLOWED_IMAGE_MIMES:
        logger.warning(f"[image_storage] Неподдерживаемый MIME-тип ({mime}): {url}")
        return None
```

**Step 3: Запустить тесты**

```bash
uv run pytest tests/test_image_storage.py -v && uv run pytest tests/ -v --tb=short
```

**Step 4: Коммит**

```bash
git add src/image_storage.py tests/test_image_storage.py
git commit -m "fix: add MIME whitelist for image downloads (P2)"
```

---

## Task 9: Разбить длинные функции (P2)

**Files:**
- Modify: `src/ai/prompt.py`
- Modify: `src/worker/scrape_handler.py`

### 9a: Рефакторинг build_analysis_prompt

Разбить `build_analysis_prompt()` (230 строк) на вспомогательные функции.

Извлечь из текущей функции:

```python
def _build_profile_section(profile: ScrapedProfile) -> str:
    """Секция профиля: username, bio, метрики."""
    ...

def _build_media_section(profile: ScrapedProfile) -> str:
    """Секция медиа: посты, рилсы, хэштеги, комментарии."""
    ...

def _build_highlights_section(profile: ScrapedProfile) -> str:
    """Секция хайлайтов."""
    ...

def _attach_images(
    parts: list[dict[str, Any]],
    profile: ScrapedProfile,
    image_map: dict[str, str] | None,
) -> None:
    """Добавить изображения к multimodal-запросу."""
    ...
```

Основная функция `build_analysis_prompt()` вызывает эти helper'ы.

**Важно:** НЕ менять формат промпта — только рефакторинг структуры. Тесты `test_prompt.py` должны пройти без изменений.

### 9b: Рефакторинг handle_full_scrape

Извлечь из `handle_full_scrape()` вспомогательные функции:

```python
async def _handle_scrape_error(
    db: Client, task_id: str, blog_id: str,
    error: Exception, current_attempts: int, max_attempts: int,
) -> None:
    """Обработать ошибку скрапинга — обновить статус блога и задачи."""
    ...

def _build_blog_data(profile: ScrapedProfile, avg_reels_views: int | None) -> dict[str, Any]:
    """Собрать данные блога из скрапнутого профиля."""
    ...
```

**Step: Запустить все тесты после каждого рефакторинга**

```bash
uv run pytest tests/test_ai/test_prompt.py -v
uv run pytest tests/test_worker/test_handlers.py -v
uv run pytest tests/ -v --tb=short
```

**Коммит:**

```bash
git add src/ai/prompt.py src/worker/scrape_handler.py
git commit -m "refactor: break up long functions in prompt.py and scrape_handler.py (P2)"
```

---

## Task 10: TypedDict для database результатов (P1)

**Files:**
- Create: `src/models/db_types.py`
- Modify: `src/database.py`
- Modify: `src/api/services.py`

**Step 1: Создать файл с типами**

Создать `src/models/db_types.py`:

```python
"""Типы для результатов БД — TypedDict вместо dict[str, Any]."""
from typing import Any, TypedDict


class TaskRecord(TypedDict):
    """Запись задачи из scrape_tasks."""
    id: str
    task_type: str
    status: str
    blog_id: str | None
    priority: int
    attempts: int
    max_attempts: int
    error_message: str | None
    payload: dict[str, Any]
    created_at: str
    started_at: str | None
    completed_at: str | None
    next_retry_at: str | None


class TaskListResult(TypedDict):
    """Результат fetch_tasks_list."""
    tasks: list[TaskRecord]
    total: int
    limit: int
    offset: int


class TaskListResultWithError(TypedDict, total=False):
    """Результат fetch_tasks_list с опциональной ошибкой."""
    tasks: list[TaskRecord]
    total: int
    limit: int
    offset: int
    error: str
```

**Step 2: Обновить сигнатуры в database.py**

В `src/database.py`:
```python
from src.models.db_types import TaskRecord
```

Обновить:
```python
async def fetch_pending_tasks(db: Client, limit: int = 10) -> list[TaskRecord]:
```

**Step 3: Обновить services.py**

В `src/api/services.py`:
```python
from src.models.db_types import TaskListResultWithError
```

Обновить:
```python
async def fetch_tasks_list(...) -> TaskListResultWithError:
```

**Step 4: Проверить типы и тесты**

```bash
uv run pyright src/ && uv run pytest tests/ -v --tb=short
```

**Step 5: Коммит**

```bash
git add src/models/db_types.py src/database.py src/api/services.py
git commit -m "refactor: add TypedDict for database result types (P1)"
```

---

## Task 11: Repository Pattern (P2)

Самый масштабный рефакторинг. Разделить `database.py` на репозитории с Protocol-based DI.

**Files:**
- Create: `src/repositories/__init__.py`
- Create: `src/repositories/protocols.py`
- Create: `src/repositories/task_repository.py`
- Create: `src/repositories/blog_repository.py`
- Modify: `src/database.py` (оставить `run_in_thread`, `sanitize_error`, `get_backoff_seconds`)
- Modify: `src/worker/handlers.py` (обновить re-exports)
- Modify: `src/worker/loop.py` (передавать репозитории)
- Modify: `src/worker/scrape_handler.py`
- Modify: `src/worker/ai_handler.py`
- Modify: `src/worker/discover_handler.py`
- Modify: `src/worker/scheduler.py`
- Modify: `src/api/app.py`
- Modify: `src/api/services.py`
- Modify: `src/main.py`
- Modify: `tests/conftest.py`
- Modify: Множество тестов (обновить моки)

### 11a: Создать Protocol и SupabaseTaskRepository

```python
# src/repositories/protocols.py
"""Протоколы репозиториев для DI."""
from typing import Any, Protocol

from src.models.db_types import TaskRecord


class TaskRepository(Protocol):
    async def mark_running(self, task_id: str) -> bool: ...
    async def mark_done(self, task_id: str) -> None: ...
    async def mark_failed(
        self, task_id: str, attempts: int, max_attempts: int,
        error: str, retry: bool = True,
    ) -> None: ...
    async def create_if_not_exists(
        self, blog_id: str | None, task_type: str, priority: int,
        payload: dict[str, Any] | None = None,
    ) -> str | None: ...
    async def fetch_pending(self, limit: int = 10) -> list[TaskRecord]: ...
    async def recover_stuck(
        self, max_running_minutes: int = 30, max_ai_running_minutes: int = 120,
    ) -> int: ...


class BlogRepository(Protocol):
    async def is_fresh(self, blog_id: str, min_days: int) -> bool: ...
    async def upsert(self, blog_id: str, data: dict[str, Any]) -> None: ...
    async def upsert_posts(self, blog_id: str, posts: list[dict[str, Any]]) -> None: ...
    async def upsert_highlights(self, blog_id: str, highlights: list[dict[str, Any]]) -> None: ...
    async def cleanup_orphan_person(self, person_id: str) -> None: ...
```

### 11b: Реализовать SupabaseTaskRepository

```python
# src/repositories/task_repository.py
"""Supabase-реализация TaskRepository."""
from datetime import UTC, datetime, timedelta
from typing import Any

from loguru import logger
from supabase import Client

from src.database import get_backoff_seconds, run_in_thread, sanitize_error, _extract_rpc_scalar
from src.models.db_types import TaskRecord


class SupabaseTaskRepository:
    def __init__(self, db: Client) -> None:
        self._db = db

    async def mark_running(self, task_id: str) -> bool:
        result = await run_in_thread(
            self._db.rpc("mark_task_running", {
                "p_task_id": task_id,
                "p_started_at": datetime.now(UTC).isoformat(),
            }).execute
        )
        return _extract_rpc_scalar(result.data) is not None

    # ... (перенести все методы из database.py)
```

### 11c: Реализовать SupabaseBlogRepository

Аналогично перенести `is_blog_fresh`, `upsert_blog`, `upsert_posts`, `upsert_highlights`, `cleanup_orphan_person` из `database.py`.

### 11d: Инъекция репозиториев

В `src/main.py`:
```python
task_repo = SupabaseTaskRepository(db)
blog_repo = SupabaseBlogRepository(db)
```

Передавать через `app.state`, worker kwargs, scheduler kwargs.

### 11e: Обновить handlers — заменить прямые вызовы database

В `src/worker/handlers.py` заменить re-exports database функций на репозитории.

В каждом handler вместо `_h.mark_task_running(db, task_id)` → `task_repo.mark_running(task_id)`.

### 11f: Обновить тесты

В `tests/conftest.py` добавить фикстуры:
```python
@pytest.fixture
def mock_task_repo():
    return MagicMock(spec=TaskRepository)

@pytest.fixture
def mock_blog_repo():
    return MagicMock(spec=BlogRepository)
```

Обновить все тесты handlers: вместо мокирования `_h.mark_task_running` мокировать `task_repo.mark_running`.

**Стратегия миграции:**

1. Создать репозитории (11a-11c)
2. Написать тесты для репозиториев
3. В `database.py` оставить функции как deprecated-обёртки, вызывающие репозитории
4. Постепенно обновлять handlers на прямое использование репозиториев
5. Удалить deprecated-обёртки из `database.py`

Это позволит мигрировать инкрементально, не ломая 860 тестов разом.

**Step: Запустить все тесты после каждого подшага**

```bash
uv run pytest tests/ -v --tb=short && uv run pyright src/ && uv run ruff check src/ tests/
```

**Коммиты:**

```bash
git commit -m "refactor: add repository protocols and Supabase implementations (P2)"
git commit -m "refactor: inject repositories into worker and API layers (P2)"
git commit -m "refactor: update tests for repository pattern (P2)"
git commit -m "refactor: remove deprecated database.py wrappers (P2)"
```

---

## Порядок выполнения и зависимости

```
Task 1 (RateLimiter Lock)     ─── независимый
Task 2 (fetch_tasks_list)     ─── независимый
Task 3 (Ruff rules)           ─── выполнять ПЕРВЫМ (может найти баги)
Task 4 (ParamSpec)            ─── независимый
Task 5 (Taxonomy cache)       ─── независимый
Task 6 (rapidfuzz)            ─── после Task 5
Task 7 (sanitize_error)       ─── независимый
Task 8 (MIME whitelist)        ─── независимый
Task 9 (Break functions)      ─── независимый
Task 10 (TypedDict)           ─── перед Task 11
Task 11 (Repository Pattern)  ─── ПОСЛЕДНИЙ (зависит от 4, 7, 10)
```

Задачи 1, 2, 4, 5, 7, 8, 9 можно делать параллельно.

---

## Финальная проверка

После всех задач:

```bash
uv run ruff check src/ tests/
uv run pyright src/
uv run pytest tests/ -v
```

Все три команды должны пройти без ошибок.
