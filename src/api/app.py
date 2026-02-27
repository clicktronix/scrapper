"""FastAPI-приложение скрапера."""
import hmac
import time
import uuid
from collections import defaultdict

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from loguru import logger
from postgrest.exceptions import APIError as PostgrestAPIError
from postgrest.types import CountMethod
from supabase import Client

from src.api.schemas import (
    DiscoverRequest,
    DiscoverResponse,
    HealthResponse,
    RetryResponse,
    ScrapeRequest,
    ScrapeResponse,
    TaskListResponse,
)
from src.config import Settings
from src.database import cleanup_orphan_person, create_task_if_not_exists, is_blog_fresh, run_in_thread
from src.platforms.instagram.client import AccountPool

security = HTTPBearer(auto_error=False)

# Rate limiting: sliding window per IP
RATE_LIMIT_MAX_REQUESTS = 60
RATE_LIMIT_WINDOW_SECONDS = 60
_rate_limit_store: dict[str, list[float]] = defaultdict(list)


def _validate_uuid(value: str) -> None:
    """Проверить что строка — валидный UUID. Бросает 422 при ошибке."""
    try:
        uuid.UUID(value)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid UUID: {value}")


def create_app(db: Client, pool: AccountPool | None, settings: Settings) -> FastAPI:
    """Создать FastAPI-приложение с зависимостями."""
    app = FastAPI(title="Scraper API", version="0.1.0")

    # Сохраняем зависимости в app.state
    app.state.db = db
    app.state.pool = pool
    app.state.settings = settings

    async def check_rate_limit(request: Request) -> None:
        """Простой in-memory rate limiter: sliding window per IP."""
        client_ip = request.client.host if request.client else "unknown"
        now = time.time()
        window_start = now - RATE_LIMIT_WINDOW_SECONDS

        # Очистить устаревшие записи
        timestamps = _rate_limit_store[client_ip]
        _rate_limit_store[client_ip] = [t for t in timestamps if t > window_start]

        if len(_rate_limit_store[client_ip]) >= RATE_LIMIT_MAX_REQUESTS:
            raise HTTPException(status_code=429, detail="Rate limit exceeded")

        _rate_limit_store[client_ip].append(now)

        # Периодическая очистка стухших IP (при росте store > 100 записей)
        if len(_rate_limit_store) > 100:
            stale_ips = [
                ip for ip, ts in _rate_limit_store.items()
                if not ts or ts[-1] <= window_start
            ]
            for ip in stale_ips:
                del _rate_limit_store[ip]

    async def verify_api_key(
        credentials: HTTPAuthorizationCredentials | None = Depends(security),
    ) -> None:
        """Проверка API-ключа."""
        expected = settings.scraper_api_key.get_secret_value()
        if credentials is None or not hmac.compare_digest(
            credentials.credentials, expected
        ):
            raise HTTPException(status_code=401, detail="Invalid API key")

    @app.get("/api/health", response_model=HealthResponse)
    async def health(response: Response) -> HealthResponse:
        """Healthcheck — без авторизации."""
        # При HikerAPI бэкенде AccountPool отсутствует
        if pool is not None:
            now = time.time()
            accounts_total = len(pool.accounts)
            accounts_available = sum(
                1 for acc in pool.accounts
                if acc.cooldown_until <= now
                and acc.requests_this_hour < pool.requests_per_hour
            )
        else:
            accounts_total = 0
            accounts_available = 0

        # Подсчёт задач из БД
        try:
            running = await run_in_thread(
                db.table("scrape_tasks")
                .select("id", count=CountMethod.exact)
                .eq("status", "running")
                .execute
            )
            pending = await run_in_thread(
                db.table("scrape_tasks")
                .select("id", count=CountMethod.exact)
                .eq("status", "pending")
                .execute
            )
            tasks_running = running.count or 0
            tasks_pending = pending.count or 0
        except Exception:
            response.status_code = 503
            tasks_running = -1
            tasks_pending = -1
            status = "degraded"
        else:
            status = "ok"

        return HealthResponse(
            status=status,
            accounts_total=accounts_total,
            accounts_available=accounts_available,
            tasks_running=tasks_running,
            tasks_pending=tasks_pending,
        )

    @app.get(
        "/api/tasks", response_model=TaskListResponse,
        dependencies=[Depends(check_rate_limit), Depends(verify_api_key)],
    )
    async def list_tasks(
        status: str | None = None,
        task_type: str | None = None,
        limit: int = Query(default=20, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        """Список задач с фильтрами и пагинацией."""
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

    @app.get("/api/tasks/{task_id}", dependencies=[Depends(check_rate_limit), Depends(verify_api_key)])
    async def get_task(task_id: str = Path(description="UUID задачи")) -> dict:
        """Получить задачу по ID."""
        _validate_uuid(task_id)
        result = await run_in_thread(
            db.table("scrape_tasks").select("*").eq("id", task_id).execute
        )
        if not result.data:
            raise HTTPException(status_code=404, detail="Task not found")
        return result.data[0]

    @app.post(
        "/api/tasks/scrape", status_code=201,
        response_model=ScrapeResponse, dependencies=[Depends(check_rate_limit), Depends(verify_api_key)],
    )
    async def scrape(body: ScrapeRequest) -> dict:
        """Создать full_scrape задачи по списку username."""
        results = []
        created = 0
        skipped = 0

        for username in body.usernames:
            try:
                blog_id = await _find_or_create_blog(db, username)

                # Не создавать задачу для deleted/deactivated блогов
                blog_row = await run_in_thread(
                    db.table("blogs").select("scrape_status")
                    .eq("id", blog_id).execute,
                    retry_transient=True,
                )
                blog_status = blog_row.data[0]["scrape_status"] if blog_row.data else None
                if blog_status in ("deleted", "deactivated"):
                    results.append({
                        "task_id": None, "username": username,
                        "blog_id": blog_id, "status": "skipped",
                        "reason": f"blog is {blog_status}",
                    })
                    skipped += 1
                    continue

                # Проверка свежести — не создавать задачу для недавно скрапленных
                if await is_blog_fresh(db, blog_id, settings.rescrape_days):
                    results.append({"task_id": None, "username": username, "blog_id": blog_id, "status": "skipped"})
                    skipped += 1
                    continue

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
            except Exception as exc:
                logger.error(f"Ошибка при обработке {username}: {exc}")
                results.append({"task_id": None, "username": username, "blog_id": None, "status": "error"})

        return {"created": created, "skipped": skipped, "tasks": results}

    @app.post(
        "/api/tasks/discover", status_code=201,
        response_model=DiscoverResponse, dependencies=[Depends(check_rate_limit), Depends(verify_api_key)],
    )
    async def discover(body: DiscoverRequest) -> dict:
        """Создать discover задачу по хештегу."""
        task_id = await create_task_if_not_exists(
            db, None, "discover", priority=10,
            payload={"hashtag": body.hashtag, "min_followers": body.min_followers},
        )
        return {"task_id": task_id, "hashtag": body.hashtag}

    @app.post(
        "/api/tasks/{task_id}/retry",
        response_model=RetryResponse,
        dependencies=[Depends(check_rate_limit), Depends(verify_api_key)],
    )
    async def retry_task(task_id: str = Path(description="UUID задачи")) -> dict:
        """Повторить упавшую задачу — сбросить в pending."""
        _validate_uuid(task_id)
        result = await run_in_thread(
            db.table("scrape_tasks").select("id, status").eq("id", task_id).execute
        )
        if not result.data:
            raise HTTPException(status_code=404, detail="Task not found")

        task = result.data[0]
        if task["status"] != "failed":
            raise HTTPException(status_code=409, detail=f"Task status is '{task['status']}', expected 'failed'")

        await run_in_thread(
            db.table("scrape_tasks").update({
                "status": "pending",
                "error_message": None,
                "next_retry_at": None,
                "attempts": 0,
            }).eq("id", task_id).execute
        )

        return {"task_id": task_id, "status": "retrying"}

    return app


async def _find_or_create_blog(db: Client, username: str) -> str:
    """Найти существующий блог или создать person + blog. Защита от race condition."""
    normalized_username = username.strip().lstrip("@").lower()

    # Попытка найти существующий блог
    blog_result = await run_in_thread(
        db.table("blogs")
        .select("id")
        .eq("platform", "instagram")
        .eq("username", normalized_username)
        .execute
    )
    if blog_result.data:
        return blog_result.data[0]["id"]

    # Создать person + blog
    person_result = await run_in_thread(
        db.table("persons")
        .insert({"full_name": normalized_username})
        .execute
    )
    person_id = person_result.data[0]["id"]

    try:
        blog_result = await run_in_thread(
            db.table("blogs")
            .insert({
                "person_id": person_id,
                "platform": "instagram",
                "username": normalized_username,
                "scrape_status": "pending",
            })
            .execute
        )
        return blog_result.data[0]["id"]
    except PostgrestAPIError:
        # Race condition: параллельный запрос уже создал блог (unique constraint) — ищем повторно
        blog_result = await run_in_thread(
            db.table("blogs")
            .select("id")
            .eq("platform", "instagram")
            .eq("username", normalized_username)
            .execute
        )
        # Всегда чистим orphan person (создан этим запросом, не привязан к блогу)
        await cleanup_orphan_person(db, person_id)
        if blog_result.data:
            return blog_result.data[0]["id"]
        raise


