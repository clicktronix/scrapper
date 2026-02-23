# Scraper

Автономный Python-сервис для сбора и AI-анализа Instagram-профилей блогеров.

## Что делает

- **Скрейпит** Instagram-профили через HikerAPI (SaaS) или instagrapi — посты, рилсы, хайлайты, метрики
- **Анализирует** контент через OpenAI Batch API (`gpt-5-nano`, structured outputs) — тематика, аудитория, коммерция
- **Классифицирует** блогеров: категории, теги (3-уровневая таксономия), embedding для семантического поиска
- **Загружает** изображения (аватары, thumbnails постов) в Supabase Storage
- **Открывает** новых блогеров по хештегам — автоматический discovery
- **Автоматически** re-scrape устаревших блогов (>60 дней) по подписчикам DESC

## Архитектура

```
                    ┌──────────────────────────────────────┐
                    │           Supabase PostgreSQL         │
                    │                                      │
                    │  scrape_tasks  blogs  blog_posts     │
                    │  blog_highlights  categories  tags   │
                    └─────────┬──────────────┬─────────────┘
                              │              │
                         poll │              │ upsert
                              │              │
                    ┌─────────▼──────────────▼─────────────┐
                    │            Worker (main.py)           │
                    │                                      │
                    │  ┌──────────┐    ┌─────────────────┐ │
                    │  │  Polling  │    │   APScheduler   │ │
                    │  │  Loop    │    │   Cron Jobs     │ │
                    │  └────┬─────┘    └────────┬────────┘ │
                    │       │                   │          │
                    │       ▼                   ▼          │
                    │  ┌─────────────────────────────────┐ │
                    │  │         Task Handlers            │ │
                    │  │                                  │ │
                    │  │  full_scrape  │  ai_analysis     │ │
                    │  │  discover     │  batch_results   │ │
                    │  └───────┬──────────────┬──────────┘ │
                    └──────────│──────────────│────────────┘
                               │              │
                    ┌──────────▼───┐   ┌──────▼───────────┐
                    │  HikerAPI    │   │  OpenAI Batch    │
                    │  (SaaS)      │   │  API (gpt-5-nano)│
                    │      или     │   │                   │
                    │  instagrapi  │   │  Structured       │
                    │  (локальный) │   │  Outputs          │
                    └──────────────┘   └───────────────────┘
```

Подробнее: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

## Быстрый старт

### Требования

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (менеджер пакетов)
- Supabase проект с таблицами (см. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#схема-базы-данных))
- OpenAI API ключ
- HikerAPI токен **или** Instagram-аккаунт(ы) с residential-прокси

### Установка

```bash
cd scraper
uv sync

cp .env.example .env
# Заполнить .env — Supabase, OpenAI, HikerAPI/аккаунты
```

### Запуск

```bash
# Локально (API :8001 + воркер + scheduler в одном процессе)
uv run python src/main.py

# Docker
docker compose up -d
docker compose logs -f scraper
```

### Тесты

```bash
uv run pytest tests/ -v          # Все тесты (600+)
uv run pytest tests/ -v -x       # Остановиться на первом падении
uv run pytest tests/test_ai/ -v  # Только AI-модуль
uv run pytest tests/test_api/ -v # Только API-модуль
```

## API

HTTP API на порту `8001` (конфигурируемо через `SCRAPER_PORT`). Rate limit: 60 req/мин per IP.

Auth: `Authorization: Bearer <SCRAPER_API_KEY>`

| Метод | Путь | Auth | Описание |
|-------|------|------|----------|
| `GET` | `/api/health` | Нет | Healthcheck: аккаунты, задачи |
| `GET` | `/api/tasks` | Да | Список задач (фильтры: status, task_type, limit, offset) |
| `GET` | `/api/tasks/{id}` | Да | Статус конкретной задачи |
| `POST` | `/api/tasks/scrape` | Да | Создать full_scrape по списку username |
| `POST` | `/api/tasks/discover` | Да | Создать discover по хештегу |
| `POST` | `/api/tasks/{id}/retry` | Да | Повторить упавшую задачу (только failed) |

### Примеры

```bash
# Healthcheck
curl http://localhost:8001/api/health

# Добавить блогеров на скрейп
curl -X POST http://localhost:8001/api/tasks/scrape \
  -H "Authorization: Bearer $SCRAPER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"usernames": ["blogger1", "blogger2", "@blogger3"]}'

# Discover по хештегу
curl -X POST http://localhost:8001/api/tasks/discover \
  -H "Authorization: Bearer $SCRAPER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"hashtag": "алматымама", "min_followers": 1000}'

# Список упавших задач
curl "http://localhost:8001/api/tasks?status=failed" \
  -H "Authorization: Bearer $SCRAPER_API_KEY"

# Повторить упавшую задачу
curl -X POST http://localhost:8001/api/tasks/<task-id>/retry \
  -H "Authorization: Bearer $SCRAPER_API_KEY"
```

## Структура проекта

```
src/
├── main.py                  # Точка входа — API + воркер + scheduler
├── config.py                # Настройки из .env (Pydantic Settings)
├── database.py              # CRUD-операции с Supabase
├── storage.py               # Supabase Storage для Instagram-сессий
├── image_storage.py         # Загрузка изображений в Supabase Storage
│
├── api/
│   ├── app.py               # FastAPI-приложение (create_app)
│   └── schemas.py           # Request/Response Pydantic-модели
│
├── models/
│   ├── task.py              # ScrapeTask — задача в очереди
│   └── blog.py              # ScrapedProfile, ScrapedPost, ScrapedHighlight
│
├── ai/
│   ├── prompt.py            # Мультимодальный промпт (текст + изображения)
│   ├── batch.py             # OpenAI Batch API + match_categories + match_tags
│   ├── schemas.py           # AIInsights — structured output схема
│   ├── taxonomy.py          # CATEGORIES + TAGS (3-уровневая таксономия)
│   ├── embedding.py         # text-embedding-3-small (1536 dims)
│   └── images.py            # Подготовка изображений для мультимодального анализа
│
├── platforms/
│   ├── base.py              # BaseScraper — абстрактный интерфейс
│   └── instagram/
│       ├── hiker_scraper.py # HikerAPI бэкенд (SaaS, SafeHikerClient)
│       ├── scraper.py       # instagrapi бэкенд (локальный, AccountPool)
│       ├── client.py        # AccountPool — ротация аккаунтов
│       ├── metrics.py       # ER, тренд, частота публикаций
│       └── exceptions.py    # HikerAPIError, InsufficientBalanceError
│
└── worker/
    ├── loop.py              # Polling loop с graceful shutdown
    ├── handlers.py          # Обработчики задач (full_scrape, ai_analysis, discover)
    └── scheduler.py         # APScheduler — cron-задачи
```

## Типы задач

| Тип | Приоритет | Что делает |
|-----|-----------|-----------|
| `full_scrape` | 3-8 | Полный скрейп профиля: посты, рилсы, хайлайты, метрики, изображения |
| `ai_analysis` | 3 | AI-анализ через OpenAI Batch API (батчами по 10+), категоризация, теги, embedding |
| `discover` | 10 | Поиск новых блогеров по хештегу |

## Переменные окружения

Все переменные описаны в [`.env.example`](.env.example). Ключевые:

| Группа | Переменные | Описание |
|--------|-----------|----------|
| Supabase | `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` | Подключение к БД и Storage |
| OpenAI | `OPENAI_API_KEY` | API ключ для Batch API |
| HikerAPI | `SCRAPER_BACKEND`, `HIKERAPI_TOKEN` | SaaS-бэкенд скрапинга |
| Instagram | `INSTAGRAM_ACCOUNTS`, `IG_*_USERNAME/PASSWORD` | Аккаунты (instagrapi бэкенд) |
| Прокси | `PROXY_*` | Residential прокси (instagrapi) |
| Worker | `WORKER_POLL_INTERVAL`, `WORKER_MAX_CONCURRENT` | Параметры воркера |
| API | `SCRAPER_PORT`, `SCRAPER_API_KEY` | HTTP API |

## Технологии

| Компонент | Технология |
|-----------|-----------|
| Язык | Python 3.13 |
| Скрейпинг | HikerAPI (SaaS) / instagrapi 2.1+ |
| AI-анализ | OpenAI Batch API (gpt-5-nano) |
| Embedding | text-embedding-3-small (1536 dims) |
| База данных | Supabase PostgreSQL + pgvector |
| Хранилище | Supabase Storage (сессии, изображения) |
| Планировщик | APScheduler 3.x |
| Валидация | Pydantic 2.x |
| Пакеты | uv |
| Контейнер | Docker |
