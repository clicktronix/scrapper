# Backfill Tasks Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Автоматическое создание задач для 35K необработанных блогов (pending без scrape) и 85 блогов без AI insights.

**Architecture:** Две interval-задачи в APScheduler + хелпер проверки balance errors. RPC-функции в Supabase для атомарных запросов с NOT EXISTS. Конфигурация через env vars.

**Tech Stack:** Python 3.12, APScheduler, Supabase AsyncClient, Pydantic Settings, PostgreSQL RPC

**Design doc:** `docs/plans/2026-03-16-backfill-tasks-design.md`

---

### Task 1: Добавить Settings-поля для backfill

**Files:**
- Modify: `src/config.py:68-150` (class Settings)
- Test: `tests/test_config.py`

**Step 1: Написать failing тесты**

В `tests/test_config.py` добавить тест-класс `TestBackfillSettings`:

```python
class TestBackfillSettings:
    """Тесты backfill настроек."""

    def test_backfill_scrape_defaults(self) -> None:
        """Дефолтные значения backfill_scrape_*."""
        settings = make_settings()
        assert settings.backfill_scrape_enabled is True
        assert settings.backfill_scrape_batch_size == 80
        assert settings.backfill_scrape_interval_minutes == 30

    def test_backfill_ai_defaults(self) -> None:
        """Дефолтные значения backfill_ai_*."""
        settings = make_settings()
        assert settings.backfill_ai_enabled is True
        assert settings.backfill_ai_batch_size == 50
        assert settings.backfill_ai_interval_minutes == 60

    def test_backfill_settings_override(self) -> None:
        """Backfill настройки переопределяются через env."""
        settings = make_settings(
            BACKFILL_SCRAPE_ENABLED="false",
            BACKFILL_SCRAPE_BATCH_SIZE="200",
            BACKFILL_SCRAPE_INTERVAL_MINUTES="15",
            BACKFILL_AI_ENABLED="false",
            BACKFILL_AI_BATCH_SIZE="100",
            BACKFILL_AI_INTERVAL_MINUTES="120",
        )
        assert settings.backfill_scrape_enabled is False
        assert settings.backfill_scrape_batch_size == 200
        assert settings.backfill_scrape_interval_minutes == 15
        assert settings.backfill_ai_enabled is False
        assert settings.backfill_ai_batch_size == 100
        assert settings.backfill_ai_interval_minutes == 120
```

**Step 2: Запустить тесты, убедиться что падают**

Run: `uv run pytest tests/test_config.py::TestBackfillSettings -v`
Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'backfill_scrape_enabled'`

**Step 3: Реализовать Settings-поля**

В `src/config.py`, в class `Settings` после строки `rescrape_days: int = 60` добавить:

```python
    # Backfill: автоскрап pending блогов
    backfill_scrape_enabled: bool = True
    backfill_scrape_batch_size: int = 80
    backfill_scrape_interval_minutes: int = 30

    # Backfill: AI анализ для блогов без insights
    backfill_ai_enabled: bool = True
    backfill_ai_batch_size: int = 50
    backfill_ai_interval_minutes: int = 60
```

**Step 4: Запустить тесты**

Run: `uv run pytest tests/test_config.py::TestBackfillSettings -v`
Expected: PASS (3 tests)

**Step 5: Коммит**

```bash
git add src/config.py tests/test_config.py
git commit -m "feat: add backfill settings fields to config"
```

---

### Task 2: SQL миграция с RPC-функциями

**Files:**
- Create: `../platform/supabase/migrations/20260316120000_backfill_rpc.sql`

**Step 1: Создать миграцию**

```sql
-- Backfill RPC: pending блоги без задач full_scrape
CREATE OR REPLACE FUNCTION backfill_pending_blogs(p_limit int)
RETURNS TABLE(id uuid) AS $$
  SELECT b.id
  FROM blogs b
  WHERE b.scrape_status = 'pending'
    AND b.scraped_at IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM scrape_tasks t
      WHERE t.blog_id = b.id
        AND t.task_type = 'full_scrape'
        AND t.status IN ('pending', 'running')
    )
  ORDER BY b.followers_count DESC NULLS LAST
  LIMIT p_limit;
$$ LANGUAGE sql STABLE;

-- Backfill RPC: ai_analyzed блоги без insights
CREATE OR REPLACE FUNCTION backfill_unanalyzed_blogs(p_limit int)
RETURNS TABLE(id uuid) AS $$
  SELECT b.id
  FROM blogs b
  WHERE b.scrape_status = 'ai_analyzed'
    AND b.ai_insights IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM scrape_tasks t
      WHERE t.blog_id = b.id
        AND t.task_type = 'ai_analysis'
        AND t.status IN ('pending', 'running')
    )
  ORDER BY b.followers_count DESC NULLS LAST
  LIMIT p_limit;
$$ LANGUAGE sql STABLE;
```

**Step 2: Коммит**

```bash
git add ../platform/supabase/migrations/20260316120000_backfill_rpc.sql
git commit -m "feat: add backfill RPC functions (pending blogs, unanalyzed blogs)"
```

---

### Task 3: Хелпер `has_recent_balance_errors` + тесты

**Files:**
- Modify: `src/worker/scheduler.py` (добавить хелпер)
- Test: `tests/test_worker/test_scheduler.py`

**Step 1: Написать failing тесты**

В `tests/test_worker/test_scheduler.py` добавить:

```python
class TestHasRecentBalanceErrors:
    """Тесты has_recent_balance_errors."""

    @pytest.mark.asyncio
    async def test_returns_true_when_errors_found(self) -> None:
        from src.worker.scheduler import has_recent_balance_errors

        db = make_db_mock()
        result_mock = MagicMock()
        result_mock.count = 1
        result_mock.data = [{"id": "t1"}]
        db.table.return_value.execute = AsyncMock(return_value=result_mock)

        assert await has_recent_balance_errors(db, "insufficient balance") is True

    @pytest.mark.asyncio
    async def test_returns_false_when_no_errors(self) -> None:
        from src.worker.scheduler import has_recent_balance_errors

        db = make_db_mock()
        result_mock = MagicMock()
        result_mock.count = 0
        result_mock.data = []
        db.table.return_value.execute = AsyncMock(return_value=result_mock)

        assert await has_recent_balance_errors(db, "insufficient balance") is False
```

**Step 2: Запустить тесты, убедиться что падают**

Run: `uv run pytest tests/test_worker/test_scheduler.py::TestHasRecentBalanceErrors -v`
Expected: FAIL — `ImportError: cannot import name 'has_recent_balance_errors'`

**Step 3: Реализовать хелпер**

В `src/worker/scheduler.py` добавить после функции `_as_rows`:

```python
async def has_recent_balance_errors(
    db: AsyncClient,
    pattern: str,
    minutes: int = 30,
) -> bool:
    """Проверить наличие ошибок баланса API за последние N минут."""
    threshold = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
    result = await (
        db.table("scrape_tasks")
        .select("id", count="exact")
        .eq("status", "failed")
        .like("error_message", f"%{pattern}%")
        .gt("completed_at", threshold)
        .limit(1)
        .execute()
    )
    return bool(result.count and result.count > 0)
```

**Step 4: Запустить тесты**

Run: `uv run pytest tests/test_worker/test_scheduler.py::TestHasRecentBalanceErrors -v`
Expected: PASS (2 tests)

**Step 5: Коммит**

```bash
git add src/worker/scheduler.py tests/test_worker/test_scheduler.py
git commit -m "feat: add has_recent_balance_errors helper for backfill protection"
```

---

### Task 4: Функция `backfill_scrape` + тесты

**Files:**
- Modify: `src/worker/scheduler.py`
- Test: `tests/test_worker/test_scheduler.py`

**Step 1: Написать failing тесты**

```python
class TestBackfillScrape:
    """Тесты backfill_scrape."""

    @pytest.mark.asyncio
    async def test_creates_tasks_for_pending_blogs(self) -> None:
        from src.worker.scheduler import backfill_scrape

        settings = MagicMock()
        settings.backfill_scrape_batch_size = 80

        db = make_db_mock()
        rpc_result = MagicMock()
        rpc_result.data = [{"id": "blog-1"}, {"id": "blog-2"}, {"id": "blog-3"}]
        db.rpc = AsyncMock(return_value=MagicMock(execute=AsyncMock(return_value=rpc_result)))

        with (
            patch("src.worker.scheduler.has_recent_balance_errors", new_callable=AsyncMock, return_value=False),
            patch("src.worker.scheduler.create_task_if_not_exists", new_callable=AsyncMock, return_value="task-id") as mock_create,
        ):
            await backfill_scrape(db=db, settings=settings)

            assert mock_create.call_count == 3
            mock_create.assert_any_call(db, "blog-1", "full_scrape", priority=6)
            mock_create.assert_any_call(db, "blog-2", "full_scrape", priority=6)
            mock_create.assert_any_call(db, "blog-3", "full_scrape", priority=6)

    @pytest.mark.asyncio
    async def test_empty_rpc_result(self) -> None:
        from src.worker.scheduler import backfill_scrape

        settings = MagicMock()
        settings.backfill_scrape_batch_size = 80

        db = make_db_mock()
        rpc_result = MagicMock()
        rpc_result.data = []
        db.rpc = AsyncMock(return_value=MagicMock(execute=AsyncMock(return_value=rpc_result)))

        with (
            patch("src.worker.scheduler.has_recent_balance_errors", new_callable=AsyncMock, return_value=False),
            patch("src.worker.scheduler.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            await backfill_scrape(db=db, settings=settings)
            mock_create.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_on_balance_errors(self) -> None:
        from src.worker.scheduler import backfill_scrape

        settings = MagicMock()
        settings.backfill_scrape_batch_size = 80

        db = make_db_mock()

        with (
            patch("src.worker.scheduler.has_recent_balance_errors", new_callable=AsyncMock, return_value=True),
            patch("src.worker.scheduler.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            await backfill_scrape(db=db, settings=settings)
            mock_create.assert_not_called()
```

**Step 2: Запустить тесты, убедиться что падают**

Run: `uv run pytest tests/test_worker/test_scheduler.py::TestBackfillScrape -v`
Expected: FAIL — `ImportError: cannot import name 'backfill_scrape'`

**Step 3: Реализовать `backfill_scrape`**

В `src/worker/scheduler.py` добавить после `has_recent_balance_errors`:

```python
async def backfill_scrape(db: AsyncClient, settings: Settings) -> None:
    """Создать full_scrape задачи для pending блогов без скрапинга."""
    record_job_run("backfill_scrape")

    if await has_recent_balance_errors(db, "insufficient balance"):
        logger.warning("[backfill_scrape] Пропуск: недавние ошибки баланса HikerAPI")
        return

    result = await db.rpc(
        "backfill_pending_blogs",
        {"p_limit": settings.backfill_scrape_batch_size},
    ).execute()

    blog_ids = [str(row.get("id", "")) for row in _as_rows(result.data) if row.get("id")]
    if not blog_ids:
        logger.debug("[backfill_scrape] Нет pending блогов для backfill")
        return

    created = 0
    for blog_id in blog_ids:
        try:
            task_id = await create_task_if_not_exists(db, blog_id, "full_scrape", priority=6)
            if task_id:
                created += 1
        except Exception as e:
            logger.error(f"[backfill_scrape] Ошибка создания задачи для blog {blog_id}: {e}")

    logger.info(f"[backfill_scrape] Создано {created} задач из {len(blog_ids)} pending блогов")
```

**Step 4: Запустить тесты**

Run: `uv run pytest tests/test_worker/test_scheduler.py::TestBackfillScrape -v`
Expected: PASS (3 tests)

**Step 5: Коммит**

```bash
git add src/worker/scheduler.py tests/test_worker/test_scheduler.py
git commit -m "feat: add backfill_scrape cron function"
```

---

### Task 5: Функция `backfill_ai_analysis` + тесты

**Files:**
- Modify: `src/worker/scheduler.py`
- Test: `tests/test_worker/test_scheduler.py`

**Step 1: Написать failing тесты**

```python
class TestBackfillAiAnalysis:
    """Тесты backfill_ai_analysis."""

    @pytest.mark.asyncio
    async def test_creates_tasks_for_unanalyzed_blogs(self) -> None:
        from src.worker.scheduler import backfill_ai_analysis

        settings = MagicMock()
        settings.backfill_ai_batch_size = 50

        db = make_db_mock()
        rpc_result = MagicMock()
        rpc_result.data = [{"id": "blog-1"}, {"id": "blog-2"}]
        db.rpc = AsyncMock(return_value=MagicMock(execute=AsyncMock(return_value=rpc_result)))

        with (
            patch("src.worker.scheduler.has_recent_balance_errors", new_callable=AsyncMock, return_value=False),
            patch("src.worker.scheduler.create_task_if_not_exists", new_callable=AsyncMock, return_value="task-id") as mock_create,
        ):
            await backfill_ai_analysis(db=db, settings=settings)

            assert mock_create.call_count == 2
            mock_create.assert_any_call(db, "blog-1", "ai_analysis", priority=2)
            mock_create.assert_any_call(db, "blog-2", "ai_analysis", priority=2)

    @pytest.mark.asyncio
    async def test_empty_rpc_result(self) -> None:
        from src.worker.scheduler import backfill_ai_analysis

        settings = MagicMock()
        settings.backfill_ai_batch_size = 50

        db = make_db_mock()
        rpc_result = MagicMock()
        rpc_result.data = []
        db.rpc = AsyncMock(return_value=MagicMock(execute=AsyncMock(return_value=rpc_result)))

        with (
            patch("src.worker.scheduler.has_recent_balance_errors", new_callable=AsyncMock, return_value=False),
            patch("src.worker.scheduler.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            await backfill_ai_analysis(db=db, settings=settings)
            mock_create.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_on_openai_balance_errors(self) -> None:
        from src.worker.scheduler import backfill_ai_analysis

        settings = MagicMock()
        settings.backfill_ai_batch_size = 50

        db = make_db_mock()

        with (
            patch(
                "src.worker.scheduler.has_recent_balance_errors",
                new_callable=AsyncMock,
                # Первый вызов (insufficient_quota) → True
                return_value=True,
            ),
            patch("src.worker.scheduler.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            await backfill_ai_analysis(db=db, settings=settings)
            mock_create.assert_not_called()
```

**Step 2: Запустить тесты, убедиться что падают**

Run: `uv run pytest tests/test_worker/test_scheduler.py::TestBackfillAiAnalysis -v`
Expected: FAIL — `ImportError: cannot import name 'backfill_ai_analysis'`

**Step 3: Реализовать `backfill_ai_analysis`**

В `src/worker/scheduler.py` добавить после `backfill_scrape`:

```python
async def backfill_ai_analysis(db: AsyncClient, settings: Settings) -> None:
    """Создать ai_analysis задачи для блогов без insights."""
    record_job_run("backfill_ai_analysis")

    # Проверяем оба паттерна ошибок OpenAI
    if (
        await has_recent_balance_errors(db, "insufficient_quota")
        or await has_recent_balance_errors(db, "billing_hard_limit")
    ):
        logger.warning("[backfill_ai] Пропуск: недавние ошибки баланса OpenAI")
        return

    result = await db.rpc(
        "backfill_unanalyzed_blogs",
        {"p_limit": settings.backfill_ai_batch_size},
    ).execute()

    blog_ids = [str(row.get("id", "")) for row in _as_rows(result.data) if row.get("id")]
    if not blog_ids:
        logger.debug("[backfill_ai] Нет блогов без AI insights для backfill")
        return

    created = 0
    for blog_id in blog_ids:
        try:
            task_id = await create_task_if_not_exists(db, blog_id, "ai_analysis", priority=2)
            if task_id:
                created += 1
        except Exception as e:
            logger.error(f"[backfill_ai] Ошибка создания задачи для blog {blog_id}: {e}")

    logger.info(f"[backfill_ai] Создано {created} задач из {len(blog_ids)} блогов без insights")
```

**Step 4: Запустить тесты**

Run: `uv run pytest tests/test_worker/test_scheduler.py::TestBackfillAiAnalysis -v`
Expected: PASS (3 tests)

**Step 5: Коммит**

```bash
git add src/worker/scheduler.py tests/test_worker/test_scheduler.py
git commit -m "feat: add backfill_ai_analysis cron function"
```

---

### Task 6: Регистрация в `create_scheduler` + тесты

**Files:**
- Modify: `src/worker/scheduler.py:361-453` (функция `create_scheduler`)
- Test: `tests/test_worker/test_scheduler.py`

**Step 1: Написать failing тесты**

Обновить существующие тесты `TestCreateScheduler` и добавить новые:

```python
# В TestCreateScheduler.test_creates_scheduler_with_jobs добавить проверки:
assert "backfill_scrape" in job_ids
assert "backfill_ai_analysis" in job_ids

# Новый тест:
def test_backfill_disabled_not_registered(self) -> None:
    """Если backfill_*_enabled=False, задачи не регистрируются."""
    from src.worker.scheduler import create_scheduler

    mock_db = MagicMock()
    settings = MagicMock()
    settings.backfill_scrape_enabled = False
    settings.backfill_ai_enabled = False

    scheduler = create_scheduler(mock_db, settings, MagicMock())

    job_ids = [job.id for job in scheduler.get_jobs()]
    assert "backfill_scrape" not in job_ids
    assert "backfill_ai_analysis" not in job_ids
```

**Step 2: Запустить тесты, убедиться что падают**

Run: `uv run pytest tests/test_worker/test_scheduler.py::TestCreateScheduler -v`
Expected: FAIL — `"backfill_scrape" not in job_ids`

**Step 3: Добавить регистрацию в `create_scheduler`**

В `src/worker/scheduler.py`, в функции `create_scheduler`, перед `return scheduler` добавить:

```python
    # Backfill: автоскрап pending блогов
    if settings.backfill_scrape_enabled:
        sched.add_job(
            backfill_scrape,
            "interval",
            minutes=settings.backfill_scrape_interval_minutes,
            kwargs={"db": db, "settings": settings},
            id="backfill_scrape",
        )

    # Backfill: AI анализ для блогов без insights
    if settings.backfill_ai_enabled:
        sched.add_job(
            backfill_ai_analysis,
            "interval",
            minutes=settings.backfill_ai_interval_minutes,
            kwargs={"db": db, "settings": settings},
            id="backfill_ai_analysis",
        )
```

**Step 4: Запустить тесты**

Run: `uv run pytest tests/test_worker/test_scheduler.py::TestCreateScheduler -v`
Expected: PASS

**Step 5: Запустить все тесты scheduler**

Run: `uv run pytest tests/test_worker/test_scheduler.py -v`
Expected: All PASS

**Step 6: Коммит**

```bash
git add src/worker/scheduler.py tests/test_worker/test_scheduler.py
git commit -m "feat: register backfill jobs in create_scheduler"
```

---

### Task 7: Финальная верификация

**Step 1: Запустить все тесты проекта**

Run: `uv run pytest tests/ -v`
Expected: All PASS (989+ тестов)

**Step 2: Запустить линтер**

Run: `make lint`
Expected: All checks passed

**Step 3: Запустить тайпчек**

Run: `make typecheck`
Expected: 0 errors

**Step 4: Финальный коммит (если были правки)**

```bash
git add -A
git commit -m "chore: backfill tasks — lint/type fixes"
```
