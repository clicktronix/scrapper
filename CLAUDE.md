# Scraper Service

Instagram scraper — автономный Python-сервис для сбора данных профилей с HTTP API.

## Commands

```bash
make install                     # Install dependencies (uv sync)
make dev                         # Run (API :8001 + worker + scheduler)
make test                        # Run tests (600+)
make lint                        # Ruff linter
make lint-fix                    # Ruff auto-fix
make typecheck                   # Pyright basic mode
uv run pytest tests/ -v          # Run tests directly
```

## Architecture

Single-process: FastAPI + polling worker + APScheduler в одном asyncio event loop.
- FastAPI HTTP API на порту `SCRAPER_PORT` (default: 8001)
- `scrape_tasks` table = task queue (no Redis)
- HikerAPI (SaaS) или instagrapi (локальный) бэкенд скрапинга
- OpenAI Batch API (gpt-5-nano, structured outputs)
- AI: таксономия тегов, категоризация, embedding (text-embedding-3-small)
- APScheduler: cron-задачи (poll_batches, recover, schedule_updates, cleanup)
- Supabase Storage: изображения блогеров (аватары + thumbnails постов)

### API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/api/health` | No | Healthcheck |
| `GET` | `/api/tasks` | Yes | List tasks (filters: status, task_type, limit, offset) |
| `GET` | `/api/tasks/{id}` | Yes | Task status |
| `POST` | `/api/tasks/scrape` | Yes | Create full_scrape by usernames |
| `POST` | `/api/tasks/discover` | Yes | Create discover by hashtag |
| `POST` | `/api/tasks/{id}/retry` | Yes | Retry failed task |

Auth: `Authorization: Bearer <SCRAPER_API_KEY>`. Rate limit: 60 req/min per IP.

## Key Patterns

- Async everywhere (asyncio event loop)
- HikerAPI: `SafeHikerClient` с проверкой HTTP-статусов (базовый Client их игнорирует)
- instagrapi calls wrapped in `asyncio.to_thread()`
- Pydantic models for all validation
- Russian comments in code
- No `type: ignore` comments
- Protocol-based abstractions (`BaseScraper`)
- `InsufficientBalanceError` (402) — без ретрая; `HikerAPIError` (429/5xx) — с ретраем

## Documentation

- `docs/ARCHITECTURE.md` — Architecture, data flows, DB schema, API, diagrams
- `README.md` — Project overview, setup, API usage
- `docs/ARCHIVE/` — Completed plans and designs
