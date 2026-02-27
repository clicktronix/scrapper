"""CRUD-операции с Supabase для скрапера."""
import asyncio
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from loguru import logger
from supabase import Client

# Supabase Python SDK использует синхронный httpx-клиент с HTTP/2.
# После простоя (sleep/network drop) соединения в пуле становятся «stale»,
# и при следующем запросе ОС возвращает EAGAIN (errno 35 на macOS),
# Connection reset (errno 54) или Broken pipe (errno 32).
# httpx оборачивает OSError в свои исключения (httpx.ReadError и т.д.),
# поэтому ловить голый OSError недостаточно — нужно проверять всю цепочку
# __cause__/__context__ и строковое представление.
# Retry на уровне run_in_thread покрывает ВСЕ операции с Supabase
# (PostgREST-запросы, RPC, Storage), а не только отдельные функции.
_RUN_IN_THREAD_MAX_RETRIES = 3
_RUN_IN_THREAD_RETRY_DELAY = 1.0
_TRANSIENT_OS_ERROR_CODES = {11, 32, 35, 54}


def _is_transient_network_error(exc: BaseException) -> bool:
    """Проверить, является ли ошибка транзиентной сетевой (EAGAIN, broken pipe и т.д.)."""
    # Проверяем всю цепочку причин — OSError может быть обёрнут в httpx.ReadError
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, OSError) and current.errno in _TRANSIENT_OS_ERROR_CODES:
            # 11/35 = EAGAIN (Linux/macOS), 54 = Connection reset, 32 = Broken pipe
            return True
        current = current.__cause__ or current.__context__
    # Fallback: строковая проверка для случаев, когда errno не доступен
    err_str = str(exc)
    return any(marker in err_str for marker in (
        "Resource temporarily unavailable",
        "Errno 11",
        "Errno 35",
        "Connection reset",
        "Broken pipe",
    ))


async def run_in_thread(
    func: Any, *args: Any, retry_transient: bool = False, **kwargs: Any
) -> Any:
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
            if _is_transient_network_error(e) and attempt < _RUN_IN_THREAD_MAX_RETRIES:
                logger.warning(
                    f"[run_in_thread] Транзиентная ошибка (попытка {attempt}/"
                    f"{_RUN_IN_THREAD_MAX_RETRIES}): {e}"
                )
                await asyncio.sleep(_RUN_IN_THREAD_RETRY_DELAY * attempt)
                continue
            raise


def sanitize_error(error: str) -> str:
    """Убрать потенциальные креденшалы из сообщения об ошибке."""
    return re.sub(r"://[^@\s]+@", "://***:***@", error)


def get_backoff_seconds(attempts: int) -> int:
    """
    Экспоненциальный backoff для retry.
    attempt 1 → 300с (5мин), attempt 2 → 900с (15мин), attempt 3 → 2700с (45мин).
    """
    return 300 * (3 ** max(0, attempts - 1))


def _extract_rpc_scalar(data: Any) -> Any:
    """Extract scalar value from Supabase RPC response."""
    if isinstance(data, list):
        if not data:
            return None
        first_item = data[0]
        if isinstance(first_item, dict):
            if not first_item:
                return None
            if len(first_item) == 1:
                return next(iter(first_item.values()))
            return first_item
        return first_item

    if isinstance(data, dict):
        if not data:
            return None
        if len(data) == 1:
            return next(iter(data.values()))
        return data

    return data


async def is_blog_fresh(db: Client, blog_id: str, min_days: int) -> bool:
    """Проверить, скрапился ли блог менее min_days дней назад."""
    threshold = (datetime.now(UTC) - timedelta(days=min_days)).isoformat()
    result = await run_in_thread(
        db.table("blogs")
        .select("scraped_at")
        .eq("id", blog_id)
        .gt("scraped_at", threshold)
        .limit(1)
        .execute
    )
    return bool(result.data)


async def mark_task_running(db: Client, task_id: str) -> bool:
    """Mark task as running; returns False when task is already claimed."""
    result = await run_in_thread(
        db.rpc("mark_task_running", {
            "p_task_id": task_id,
            "p_started_at": datetime.now(UTC).isoformat(),
        }).execute
    )
    claimed_task_id = _extract_rpc_scalar(result.data)
    return claimed_task_id is not None


async def mark_task_done(db: Client, task_id: str) -> None:
    """Пометить задачу как done."""
    await run_in_thread(
        db.table("scrape_tasks").update({
            "status": "done",
            "completed_at": datetime.now(UTC).isoformat(),
        }).eq("id", task_id).execute
    )


async def mark_task_failed(
    db: Client,
    task_id: str,
    attempts: int,
    max_attempts: int,
    error: str,
    retry: bool = True,
) -> None:
    """
    Пометить задачу как failed или pending (retry).
    При retry=True и attempts < max_attempts → status='pending' с backoff.
    """
    safe_error = sanitize_error(error)

    if retry and attempts < max_attempts:
        backoff = get_backoff_seconds(attempts)
        next_retry = datetime.now(UTC) + timedelta(seconds=backoff)
        await run_in_thread(
            db.table("scrape_tasks").update({
                "status": "pending",
                "error_message": safe_error,
                "next_retry_at": next_retry.isoformat(),
            }).eq("id", task_id).execute
        )
        logger.info(f"Task {task_id} retry in {backoff}s (attempt {attempts}/{max_attempts})")
    else:
        await run_in_thread(
            db.table("scrape_tasks").update({
                "status": "failed",
                "error_message": safe_error,
            }).eq("id", task_id).execute
        )
        logger.error(f"Task {task_id} permanently failed: {safe_error}")


async def create_task_if_not_exists(
    db: Client,
    blog_id: str | None,
    task_type: str,
    priority: int,
    payload: dict[str, Any] | None = None,
) -> str | None:
    """
    Создать задачу через атомарную RPC-функцию.
    Проверка + вставка в одной транзакции (нет race condition).
    blog_id=None допустим для discover-задач.
    """
    result = await run_in_thread(
        db.rpc("create_task_if_not_exists", {
            "p_blog_id": blog_id,
            "p_task_type": task_type,
            "p_priority": priority,
            "p_payload": payload or {},
        }).execute
    )

    task_id = result.data
    if task_id:
        logger.info(f"Created task {task_type} for blog {blog_id}: {task_id}")
        return task_id

    logger.debug(f"Task {task_type} for blog {blog_id} already exists, skipping")
    return None


async def fetch_pending_tasks(db: Client, limit: int = 10) -> list[dict]:
    """Получить pending задачи, готовые к обработке (один запрос с or-фильтром)."""
    now = datetime.now(UTC).isoformat()
    result = await run_in_thread(
        db.table("scrape_tasks")
        .select("*")
        .eq("status", "pending")
        .or_(f"next_retry_at.is.null,next_retry_at.lte.{now}")
        .order("priority", desc=False)
        .order("created_at", desc=False)
        .limit(limit)
        .execute
    )
    if result.data:
        types = {}
        for t in result.data:
            tt = t.get("task_type", "?")
            types[tt] = types.get(tt, 0) + 1
        logger.debug(f"fetch_pending_tasks: {len(result.data)} tasks ({types})")
    return result.data


async def recover_stuck_tasks(
    db: Client,
    max_running_minutes: int = 30,
    max_ai_running_minutes: int = 120,
) -> int:
    """
    Вернуть зависшие running задачи в pending.

    full_scrape/discover — таймаут max_running_minutes (30 мин).
    ai_analysis — таймаут max_ai_running_minutes (2ч).
      AI задачи зависают если handle_batch_results упал с исключением
      (например \u0000 в PostgreSQL). retry_stale_batches — последняя линия
      обороны (4ч), а эта функция — основная.
    """
    threshold = (
        datetime.now(UTC) - timedelta(minutes=max_running_minutes)
    ).isoformat()
    ai_threshold = (
        datetime.now(UTC) - timedelta(minutes=max_ai_running_minutes)
    ).isoformat()

    # full_scrape / discover — короткий таймаут
    result = await run_in_thread(
        db.table("scrape_tasks")
        .select("id, task_type, attempts, max_attempts")
        .eq("status", "running")
        .in_("task_type", ["full_scrape", "discover"])
        .lt("started_at", threshold)
        .execute
    )

    # ai_analysis — длинный таймаут (батчи обрабатываются 1-2ч)
    ai_result = await run_in_thread(
        db.table("scrape_tasks")
        .select("id, task_type, attempts, max_attempts")
        .eq("status", "running")
        .eq("task_type", "ai_analysis")
        .lt("started_at", ai_threshold)
        .execute
    )

    all_tasks = (result.data or []) + (ai_result.data or [])
    if not all_tasks:
        return 0

    recovered = 0
    for task in all_tasks:
        timeout = max_ai_running_minutes if task["task_type"] == "ai_analysis" else max_running_minutes
        if task["attempts"] >= task["max_attempts"]:
            await run_in_thread(
                db.table("scrape_tasks").update({
                    "status": "failed",
                    "error_message": f"Stuck in running for >{timeout}min, max attempts exhausted",
                }).eq("id", task["id"]).execute
            )
        else:
            await run_in_thread(
                db.table("scrape_tasks").update({
                    "status": "pending",
                    "error_message": f"Recovered: stuck in running for >{timeout}min",
                }).eq("id", task["id"]).execute
            )
            recovered += 1

    if recovered:
        logger.warning(f"Recovered {recovered} stuck tasks")
    return recovered


async def cleanup_orphan_person(db: Client, person_id: str) -> None:
    """Удалить person без привязанных блогов (best-effort cleanup)."""
    try:
        blogs = await run_in_thread(
            db.table("blogs").select("id").eq("person_id", person_id).limit(1).execute
        )
        if not blogs.data:
            await run_in_thread(
                db.table("persons").delete().eq("id", person_id).execute
            )
    except Exception:
        pass  # Не критично — orphan cleanup best-effort


async def upsert_blog(db: Client, blog_id: str, data: dict) -> None:
    """Обновить данные блога из скрапинга."""
    await run_in_thread(
        db.table("blogs").update(data).eq("id", blog_id).execute
    )


async def upsert_posts(db: Client, blog_id: str, posts: list[dict]) -> None:
    """Upsert постов блогера. ON CONFLICT (blog_id, platform_id) DO UPDATE."""
    if not posts:
        return
    rows = [{**post, "blog_id": blog_id} for post in posts]
    await run_in_thread(
        db.table("blog_posts").upsert(
            rows, on_conflict="blog_id,platform_id"
        ).execute
    )


async def upsert_highlights(db: Client, blog_id: str, highlights: list[dict]) -> None:
    """Upsert хайлайтов блогера."""
    if not highlights:
        return
    rows = [{**h, "blog_id": blog_id} for h in highlights]
    await run_in_thread(
        db.table("blog_highlights").upsert(
            rows, on_conflict="blog_id,platform_id"
        ).execute
    )
