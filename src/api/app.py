"""FastAPI-приложение скрапера."""
import hmac
from typing import Any, Literal, cast
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from loguru import logger
from supabase import Client

from src.api.rate_limiter import RateLimiter
from src.api.schemas import (
    DiscoverRequest,
    DiscoverResponse,
    HealthResponse,
    PreFilterRequest,
    PreFilterResponse,
    RetryResponse,
    ScrapeRequest,
    ScrapeResponse,
    TaskListResponse,
    TaskResponse,
)
from src.api.services import fetch_tasks_list, find_blog_by_username, find_or_create_blog, get_health_status
from src.config import Settings
from src.database import create_task_if_not_exists, is_blog_fresh, run_in_thread
from src.platforms.instagram.client import AccountPool

security = HTTPBearer(auto_error=False)


def _as_row_dict(value: Any) -> dict[str, Any]:
    """Преобразовать JSON-строку ответа Supabase в dict."""
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    return {}


def create_app(db: Client, pool: AccountPool | None, settings: Settings) -> FastAPI:
    """Создать FastAPI-приложение с зависимостями."""
    app = FastAPI(
        title="Scraper API",
        version="0.1.0",
        docs_url="/docs" if settings.log_level == "DEBUG" else None,
        redoc_url=None,
        openapi_url="/openapi.json" if settings.log_level == "DEBUG" else None,
    )

    @app.exception_handler(Exception)
    async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception(f"Unhandled exception on {request.method} {request.url.path}")
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})

    # Сохраняем зависимости в app.state
    app.state.db = db
    app.state.pool = pool
    app.state.settings = settings

    rate_limiter = RateLimiter()

    async def check_rate_limit(request: Request) -> None:
        """Делегирует проверку rate limit в RateLimiter."""
        await rate_limiter.check(request)

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
        return await get_health_status(db, pool, response)

    @app.get(
        "/api/tasks", response_model=TaskListResponse,
        dependencies=[Depends(check_rate_limit), Depends(verify_api_key)],
    )
    async def list_tasks(
        status: Literal["pending", "running", "done", "failed"] | None = Query(default=None),
        task_type: Literal["full_scrape", "ai_analysis", "discover", "pre_filter"] | None = Query(default=None),
        limit: int = Query(default=20, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        """Список задач с фильтрами и пагинацией."""
        return dict(await fetch_tasks_list(db, status, task_type, limit, offset))

    @app.get(
        "/api/tasks/{task_id}",
        response_model=TaskResponse,
        dependencies=[Depends(check_rate_limit), Depends(verify_api_key)],
    )
    async def get_task(task_id: UUID = Path(description="UUID задачи")) -> dict[str, Any]:
        """Получить задачу по ID."""
        result = await run_in_thread(
            db.table("scrape_tasks").select("*").eq("id", str(task_id)).execute
        )
        if not result.data:
            raise HTTPException(status_code=404, detail="Task not found")
        return _as_row_dict(result.data[0])

    @app.post(
        "/api/tasks/scrape",
        response_model=ScrapeResponse,
        dependencies=[Depends(check_rate_limit), Depends(verify_api_key)],
    )
    async def scrape(body: ScrapeRequest, response: Response) -> dict:
        """Создать full_scrape задачи по списку username."""
        results = []
        created = 0
        skipped = 0
        errors = 0

        for username in body.usernames:
            try:
                blog_id = await find_or_create_blog(db, username)

                # Не создавать задачу для deleted/deactivated блогов
                blog_row = await run_in_thread(
                    db.table("blogs").select("scrape_status")
                    .eq("id", blog_id).execute,
                    retry_transient=True,
                )
                blog_status = None
                if blog_row.data:
                    blog_status = _as_row_dict(blog_row.data[0]).get("scrape_status")
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
                errors += 1

        # 207 Multi-Status при частичных ошибках, 201 при полном успехе
        response.status_code = 207 if errors > 0 else 201
        return {"created": created, "skipped": skipped, "errors": errors, "tasks": results}

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

                # Создать задачу (blog_id=None, username в payload)
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
    async def retry_task(task_id: UUID = Path(description="UUID задачи")) -> dict[str, str]:
        """Повторить упавшую задачу — сбросить в pending."""
        tid = str(task_id)
        result = await run_in_thread(
            db.table("scrape_tasks").select("id, status").eq("id", tid).execute
        )
        if not result.data:
            raise HTTPException(status_code=404, detail="Task not found")

        task = _as_row_dict(result.data[0])
        if task["status"] != "failed":
            raise HTTPException(status_code=409, detail=f"Task status is '{task['status']}', expected 'failed'")

        await run_in_thread(
            db.table("scrape_tasks").update({
                "status": "pending",
                "error_message": None,
                "next_retry_at": None,
                "attempts": 0,
            }).eq("id", tid).execute
        )

        return {"task_id": tid, "status": "retrying"}

    return app
