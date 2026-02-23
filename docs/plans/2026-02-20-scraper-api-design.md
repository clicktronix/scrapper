# Scraper API — Design Doc

## Цель

Добавить HTTP API в скрапер для создания задач, мониторинга и интеграции с платформой (Next.js). Скрапер остаётся автономным сервисом — API и воркер в одном процессе.

## Архитектура

FastAPI-сервер запускается параллельно с polling loop в одном asyncio event loop:

```
main.py
  asyncio.gather(
    uvicorn.serve(app, port=8001),
    run_worker(...)
  )
```

Один процесс, один `.env`, один `docker compose up`.

```
┌──────────────────────────────────────────────┐
│               Scraper Process                │
│                                              │
│  ┌─────────────────┐  ┌──────────────────┐  │
│  │  FastAPI :8001   │  │  Polling Loop    │  │
│  │                  │  │                  │  │
│  │  POST /tasks/*   │  │  fetch_pending   │  │
│  │  GET  /tasks/*   │  │  → dispatch      │  │
│  │  GET  /health    │  │  → handlers      │  │
│  └────────┬─────────┘  └────────┬─────────┘  │
│           │                     │            │
│           └──────────┬──────────┘            │
│                      │                       │
│              ┌───────▼────────┐              │
│              │  database.py   │              │
│              │  (Supabase)    │              │
│              └────────────────┘              │
└──────────────────────────────────────────────┘
```

## Эндпоинты

### POST /api/tasks/scrape

Создать full_scrape задачи по списку username.

```json
// Request
{"usernames": ["blogger1", "blogger2"]}

// Response 201
{
  "created": 1,
  "skipped": 1,
  "tasks": [
    {"task_id": "uuid", "username": "blogger1", "blog_id": "uuid", "status": "created"},
    {"task_id": null, "username": "blogger2", "blog_id": "uuid", "status": "skipped"}
  ]
}
```

Логика:
1. Для каждого username:
   - SELECT blog по (platform='instagram', username)
   - Если нет — INSERT person + blog
   - Проверить есть ли pending/running full_scrape для blog_id
   - Если нет — create_task_if_not_exists('full_scrape', priority=3)
   - Если есть — status='skipped'
2. Валидация: usernames не пуст, max 100 штук

### POST /api/tasks/discover

Создать discover задачу по хештегу.

```json
// Request
{"hashtag": "алматымама", "min_followers": 1000}

// Response 201
{"task_id": "uuid", "hashtag": "алматымама"}
```

### GET /api/tasks

Список задач с фильтрами.

```
GET /api/tasks?status=pending&task_type=full_scrape&limit=20&offset=0
```

```json
// Response 200
{
  "tasks": [
    {
      "id": "uuid",
      "blog_id": "uuid",
      "task_type": "full_scrape",
      "status": "pending",
      "priority": 3,
      "attempts": 0,
      "error_message": null,
      "created_at": "2026-02-20T10:00:00Z",
      "started_at": null,
      "completed_at": null
    }
  ],
  "total": 42,
  "limit": 20,
  "offset": 0
}
```

### GET /api/tasks/{task_id}

Статус конкретной задачи.

```json
// Response 200
{
  "id": "uuid",
  "blog_id": "uuid",
  "task_type": "full_scrape",
  "status": "done",
  "priority": 3,
  "attempts": 1,
  "payload": {},
  "error_message": null,
  "created_at": "...",
  "started_at": "...",
  "completed_at": "..."
}
```

404 если задача не найдена.

### GET /api/health

Healthcheck воркера.

```json
// Response 200
{
  "status": "ok",
  "accounts_total": 2,
  "accounts_available": 1,
  "tasks_running": 2,
  "tasks_pending": 15
}
```

## Авторизация

API key через заголовок:

```
Authorization: Bearer <SCRAPER_API_KEY>
```

- Новая переменная `SCRAPER_API_KEY` в Settings
- FastAPI dependency проверяет заголовок на всех роутах кроме `/api/health`
- 401 при отсутствии/невалидном ключе

## Структура файлов

```
src/api/
├── __init__.py
├── app.py         # create_app(db, pool, settings) → FastAPI
├── routes.py      # Эндпоинты
└── schemas.py     # Request/Response Pydantic-модели
```

Изменения в существующих файлах:
- `src/main.py` — asyncio.gather(uvicorn, worker)
- `src/config.py` — добавить scraper_api_key, scraper_port

## Конфигурация

Новые переменные в .env:

```env
SCRAPER_API_KEY=sk-scraper-...    # API ключ (обязательный)
SCRAPER_PORT=8001                  # Порт HTTP-сервера (default: 8001)
```
