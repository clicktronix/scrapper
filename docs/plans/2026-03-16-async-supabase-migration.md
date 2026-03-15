# Миграция на AsyncClient Supabase

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Заменить синхронный `supabase.Client` + `asyncio.to_thread` на нативный `AsyncClient`, устранив корневую причину Errno 11 (EAGAIN).

**Architecture:** Синхронный Supabase SDK использует `httpx.Client(http2=True)`. HTTP/2 мультиплексирует все запросы через один TCP-сокет. При concurrent доступе из нескольких тредов TCP send buffer переполняется → EAGAIN. `AsyncClient` использует `httpx.AsyncClient` в одном event loop — нет конкуренции тредов за сокет.

**Tech Stack:** supabase-py 2.28.0, postgrest-py 2.28.0, httpx 0.28.1 (AsyncClient)

---

## Механика замены

Каждый вызов трансформируется по одной схеме:

```python
# БЫЛО:
result = await run_in_thread(
    db.table("x").select("*").eq("id", val).execute
)

# СТАЛО:
result = await db.table("x").select("*").eq("id", val).execute()
```

- Убрать обёртку `run_in_thread(`...`)`
- Добавить `()` к `.execute` (был method reference, стал вызов)
- `from supabase import Client` → `from supabase import AsyncClient`
- Storage: `run_in_thread(db.storage.from_(...).upload(...))` → `await db.storage.from_(...).upload(...)`

## Особые случаи

### log_sink.py
Loguru sink — синхронная функция (вызывается из треда `enqueue=True`). Async client нельзя использовать напрямую. Решение: `asyncio.run_coroutine_threadsafe(coro, loop)` — планирует async запись в event loop из loguru-треда.

### ThreadPoolExecutor
Остаётся для HikerAPI/instagrapi (`asyncio.to_thread(scraper.cl.method, ...)`). Уменьшается до 4 тредов (только для внешних sync библиотек).

### retry_transient
`run_in_thread` имел встроенный retry для транзиентных ошибок. С async клиентом EAGAIN не возникает (нет тред-конкуренции). Для реальных сетевых ошибок — добавить простой async retry helper.

---

### Task 1: Ядро — database.py, main.py, log_sink.py

**Files:**
- Modify: `src/database.py` — удалить `run_in_thread`, семафор, retry. Все CRUD-функции → прямой `await`
- Modify: `src/main.py` — `create_async_client`, уменьшить ThreadPoolExecutor до 4
- Modify: `src/log_sink.py` — `asyncio.run_coroutine_threadsafe`

**database.py — ключевые изменения:**

```python
# Удалить: _DB_CONCURRENCY_LIMIT, _db_semaphore, _get_db_semaphore(), run_in_thread()

# Все функции: убрать run_in_thread обёртку, добавить () к .execute
# Пример:
async def mark_task_done(db: AsyncClient, task_id: str) -> None:
    await db.table("scrape_tasks").update({
        "status": "done",
        "completed_at": datetime.now(UTC).isoformat(),
    }).eq("id", task_id).execute()

# Для retry_transient случаев — простой async retry:
async def retry_transient[T](coro_factory: Callable[[], Coroutine[Any, Any, T]], max_retries: int = 3) -> T:
    for attempt in range(1, max_retries + 1):
        try:
            return await coro_factory()
        except Exception as e:
            if is_transient_network_error(e) and attempt < max_retries:
                await asyncio.sleep(2.0 * attempt)
                continue
            raise
    raise RuntimeError("Unreachable")
```

**main.py — ключевые изменения:**

```python
from supabase import AsyncClient, create_async_client

db = await create_async_client(settings.supabase_url, settings.supabase_service_key.get_secret_value())

# ThreadPoolExecutor уменьшить до 4 (только для HikerAPI/instagrapi)
executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# log_sink: передать event loop
loop = asyncio.get_running_loop()
logger.add(
    create_supabase_sink(db, loop),
    level="WARNING",
    enqueue=True,
    serialize=False,
)
```

**log_sink.py — ключевые изменения:**

```python
def create_supabase_sink(db: AsyncClient, loop: asyncio.AbstractEventLoop) -> Callable[[Any], None]:
    last_write = 0.0
    lock = threading.Lock()

    async def _write(entry: dict[str, str]) -> None:
        try:
            await db.table("scrape_logs").insert(entry).execute()
        except Exception as e:
            _fallback_logger.opt(depth=1).trace(f"Supabase log sink error: {e}")

    def sink(message: Any) -> None:
        nonlocal last_write
        record = message.record
        if record["level"].no < 30:
            return
        now = time.monotonic()
        with lock:
            if now - last_write < _MIN_WRITE_INTERVAL:
                return
            last_write = now
        entry = {
            "level": record["level"].name,
            "module": record["name"],
            "message": sanitized_msg,
        }
        asyncio.run_coroutine_threadsafe(_write(entry), loop)

    return sink
```

**Шаги:**
1. Внести изменения в database.py, main.py, log_sink.py
2. Обновить тесты для database.py (убрать моки `run_in_thread`)
3. `make test` — починить падающие тесты
4. `make lint && make typecheck`
5. Коммит

---

### Task 2: API — app.py, services.py

**Files:**
- Modify: `src/api/app.py` — `Client` → `AsyncClient`, `run_in_thread(X.execute)` → `await X.execute()`
- Modify: `src/api/services.py` — аналогично
- Modify: `src/api/schemas.py` — если есть типы Client

**Механика:** В обоих файлах ~15 вызовов `run_in_thread`. Каждый заменить по схеме.

**Шаги:**
1. Заменить импорты и типы `Client` → `AsyncClient`
2. Заменить все `run_in_thread` → прямой await
3. Обновить тесты `tests/test_api/`
4. `make test && make lint && make typecheck`
5. Коммит

---

### Task 3: Worker loop + handlers

**Files:**
- Modify: `src/worker/loop.py` — типы `Client` → `AsyncClient`
- Modify: `src/worker/handlers.py` — re-exports, типы
- Modify: `src/worker/pre_filter_handler.py` — ~5 вызовов `run_in_thread`
- Modify: `src/worker/scrape_handler.py` — ~8 вызовов `run_in_thread`
- Modify: `src/worker/ai_handler.py` — ~10 вызовов `run_in_thread`
- Modify: `src/worker/discover_handler.py` — ~3 вызова `run_in_thread`
- Modify: `src/worker/scheduler.py` — ~10 вызовов `run_in_thread`

**Механика:** Те же замены. В `loop.py` прямой вызов `db.table(...)` в transient error handler тоже заменить.

**Шаги:**
1. Заменить типы и вызовы во всех handler-файлах
2. Обновить тесты `tests/test_worker/`
3. `make test && make lint && make typecheck`
4. Коммит

---

### Task 4: Repositories, taxonomy, storage

**Files:**
- Modify: `src/repositories/task_repository.py` — ~10 вызовов
- Modify: `src/repositories/blog_repository.py` — ~5 вызовов
- Modify: `src/ai/taxonomy_matching.py` — ~6 вызовов
- Modify: `src/image_storage.py` — storage calls (async upload/download/list/remove)
- Modify: `src/storage.py` — storage calls (async upload/download)

**Шаги:**
1. Заменить типы и вызовы
2. Обновить тесты
3. `make test && make lint && make typecheck`
4. Коммит

---

### Task 5: Scripts + CLI + cleanup

**Files:**
- Modify: `src/scripts/regenerate_embeddings.py`
- Modify: `src/cli/reanalyze.py`
- Modify: `src/platforms/instagram/client.py` — AccountPool (если использует db)

**Cleanup:**
- Удалить `run_in_thread` из `database.py` (если ещё не удалён)
- Удалить семафор `_DB_CONCURRENCY_LIMIT` и связанный код
- Удалить неиспользуемые импорты `concurrent.futures.ThreadPoolExecutor` если стал ненужным
- Убрать комментарии про HTTP/2 thread contention (больше не актуально)

**Шаги:**
1. Обновить скрипты
2. Финальный cleanup
3. `make test && make lint && make typecheck` — ВСЕ 981+ тестов
4. Коммит
5. Деплой

---

## Риски

| Риск | Митигация |
|------|-----------|
| Async Storage API отличается | Проверить конкретные вызовы upload/download — API зеркальный, просто async |
| AccountPool использует sync client | Проверить и обновить если нужно |
| Тесты мокают `run_in_thread` | Заменить на моки `db.table().execute` напрямую |
| `create_async_client` требует `await` | Вызывать в `async def main()` — уже есть |
