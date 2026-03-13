"""FastAPI-приложение скрапера."""
import asyncio
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

        async def _process_one(username: str) -> dict[str, Any]:
            """Обработать одного блогера для full_scrape."""
            try:
                blog_id = await find_or_create_blog(db, username)

                blog_row = await run_in_thread(
                    db.table("blogs").select("scrape_status")
                    .eq("id", blog_id).execute,
                    retry_transient=True,
                )
                blog_status = None
                if blog_row.data:
                    blog_status = _as_row_dict(blog_row.data[0]).get("scrape_status")
                if blog_status in ("deleted", "deactivated"):
                    return {
                        "task_id": None, "username": username,
                        "blog_id": blog_id, "status": "skipped",
                        "reason": f"blog is {blog_status}",
                    }

                if await is_blog_fresh(db, blog_id, settings.rescrape_days):
                    return {"task_id": None, "username": username, "blog_id": blog_id, "status": "skipped"}

                task_id = await create_task_if_not_exists(
                    db, blog_id, "full_scrape", priority=3,
                )

                if task_id:
                    return {"task_id": task_id, "username": username, "blog_id": blog_id, "status": "created"}
                return {"task_id": None, "username": username, "blog_id": blog_id, "status": "skipped"}
            except Exception as exc:
                logger.error(f"Ошибка при обработке {username}: {exc}")
                return {"task_id": None, "username": username, "blog_id": None, "status": "error"}

        all_results = await asyncio.gather(*[_process_one(u) for u in body.usernames])

        created = sum(1 for r in all_results if r["status"] == "created")
        skipped = sum(1 for r in all_results if r["status"] == "skipped")
        errors = sum(1 for r in all_results if r["status"] == "error")

        response.status_code = 207 if errors > 0 else 201
        return {"created": created, "skipped": skipped, "errors": errors, "tasks": list(all_results)}

    @app.post(
        "/api/tasks/pre_filter",
        response_model=PreFilterResponse,
        dependencies=[Depends(check_rate_limit), Depends(verify_api_key)],
    )
    async def pre_filter(body: PreFilterRequest, response: Response) -> dict:
        """Создать pre_filter задачи для проверки блогеров."""
        # 1. Параллельно проверяем существование всех блогеров в БД
        check_tasks = [find_blog_by_username(db, u) for u in body.usernames]
        existing_ids = await asyncio.gather(*check_tasks, return_exceptions=True)

        # 2. Собираем юзернеймов для создания задач
        to_create: list[str] = []
        results: list[dict[str, Any]] = []
        skipped = 0
        errors = 0

        for username, existing in zip(body.usernames, existing_ids, strict=True):
            if isinstance(existing, Exception):
                logger.error(f"Ошибка при проверке pre_filter {username}: {existing}")
                results.append({
                    "task_id": None, "username": username,
                    "blog_id": None, "status": "error",
                })
                errors += 1
            elif existing is not None:
                results.append({
                    "task_id": None, "username": username,
                    "blog_id": existing, "status": "skipped",
                    "reason": "blog already exists",
                })
                skipped += 1
            else:
                to_create.append(username)

        # 3. Параллельно создаём задачи для новых блогеров
        create_tasks = [
            create_task_if_not_exists(db, None, "pre_filter", priority=8, payload={"username": u})
            for u in to_create
        ]
        create_results = await asyncio.gather(*create_tasks, return_exceptions=True)

        created = 0
        for username, result in zip(to_create, create_results, strict=True):
            if isinstance(result, Exception):
                logger.error(f"Ошибка при создании pre_filter {username}: {result}")
                results.append({
                    "task_id": None, "username": username,
                    "blog_id": None, "status": "error",
                })
                errors += 1
            elif result:
                results.append({
                    "task_id": result, "username": username,
                    "blog_id": None, "status": "created",
                })
                created += 1
            else:
                results.append({
                    "task_id": None, "username": username,
                    "blog_id": None, "status": "skipped",
                })
                skipped += 1

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
