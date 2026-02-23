# Scraper Service

Instagram scraper — автономный Python-сервис для сбора данных профилей с HTTP API.

## Commands

```bash
make install                     # Install dependencies (uv sync)
make dev                         # Run (API :8001 + worker)
make test                        # Run tests (420)
make lint                        # Ruff linter
make lint-fix                    # Ruff auto-fix
make typecheck                   # Pyright basic mode
uv run pytest tests/ -v          # Run tests directly
```

## Architecture

Single-process: FastAPI + polling worker в одном asyncio event loop.
- FastAPI HTTP API на порту `SCRAPER_PORT` (default: 8001)
- `scrape_tasks` table = task queue (no Redis)
- `instagrapi` (sync) + `asyncio.to_thread()`
- OpenAI Batch API (gpt-5-nano, structured outputs)
- APScheduler for cron jobs
- Supabase Storage for Instagram sessions

### API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/api/health` | No | Healthcheck |
| `GET` | `/api/tasks` | Yes | List tasks (filters: status, task_type, limit, offset) |
| `GET` | `/api/tasks/{id}` | Yes | Task status |
| `POST` | `/api/tasks/scrape` | Yes | Create full_scrape by usernames |
| `POST` | `/api/tasks/discover` | Yes | Create discover by hashtag |

Auth: `Authorization: Bearer <SCRAPER_API_KEY>`

## Key Patterns

- Async everywhere (asyncio event loop)
- instagrapi calls wrapped in `asyncio.to_thread()`
- Pydantic models for all validation
- Russian comments in code
- No `type: ignore` comments
- Protocol-based abstractions (`BaseScraper`)

## Documentation

- `docs/ARCHITECTURE.md` — Architecture, data flows, DB schema, API, diagrams
- `README.md` — Project overview, setup, API usage
