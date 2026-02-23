# Scraper

Автономный Python-сервис для сбора и AI-анализа Instagram-профилей блогеров.

## Что делает

- **Скрейпит** Instagram-профили через `instagrapi` — посты, рилсы, хайлайты, метрики
- **Анализирует** контент через OpenAI Batch API (`gpt-5-nano`, structured outputs) — тематика, аудитория, коммерция
- **Открывает** новых блогеров по хештегам — автоматический discovery
- **Хранит** всё в Supabase PostgreSQL — единая база с платформой

## Архитектура

```
                    ┌──────────────────────────────────────┐
                    │           Supabase PostgreSQL         │
                    │                                      │
                    │  scrape_tasks  blogs  blog_posts     │
                    │  blog_highlights  blog_categories    │
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
                    │  Instagram   │   │  OpenAI Batch    │
                    │  (instagrapi)│   │  API (gpt-5-nano)│
                    │              │   │                   │
                    │  AccountPool │   │  Structured       │
                    │  + ротация   │   │  Outputs          │
                    └──────────────┘   └───────────────────┘
```

Подробнее: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

Текущие intentional отклонения для тестового режима запуска: [`docs/TEST_MODE_NOTES.md`](docs/TEST_MODE_NOTES.md)

## Быстрый старт

### Требования

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (менеджер пакетов)
- Supabase проект с таблицами (см. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#схема-базы-данных))
- OpenAI API ключ
- 1+ Instagram-аккаунт с residential-прокси

### Установка

```bash
# Склонировать и установить зависимости
cd scraper
uv sync

# Скопировать и заполнить переменные окружения
cp .env.example .env
# Редактировать .env — заполнить Supabase, OpenAI, аккаунты
```

### Запуск

```bash
# Локально (API :8001 + воркер в одном процессе)
uv run python src/main.py

# Docker
docker compose up -d

# Логи
docker compose logs -f scraper
```

### Тесты

```bash
uv run pytest tests/ -v          # Все тесты (420)
uv run pytest tests/ -v -x       # Остановиться на первом падении
uv run pytest tests/test_ai/ -v  # Только AI-модуль
uv run pytest tests/test_api/ -v # Только API-модуль
```

## API

HTTP API на порту `8001` (конфигурируемо через `SCRAPER_PORT`). Авторизация: `Authorization: Bearer <SCRAPER_API_KEY>`.

| Метод | Путь | Auth | Описание |
|-------|------|------|----------|
| `GET` | `/api/health` | Нет | Healthcheck: аккаунты, задачи |
| `GET` | `/api/tasks` | Да | Список задач (фильтры: status, task_type, limit, offset) |
| `GET` | `/api/tasks/{id}` | Да | Статус конкретной задачи |
| `POST` | `/api/tasks/scrape` | Да | Создать full_scrape по списку username |
| `POST` | `/api/tasks/discover` | Да | Создать discover по хештегу |

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

# Список задач
curl "http://localhost:8001/api/tasks?status=pending&limit=10" \
  -H "Authorization: Bearer $SCRAPER_API_KEY"
```

## Структура проекта

```
src/
├── main.py                  # Точка входа — API + воркер (asyncio.gather)
├── config.py                # Настройки из .env (Pydantic Settings)
├── database.py              # CRUD-операции с Supabase
├── storage.py               # Supabase Storage для Instagram-сессий
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
│   ├── batch.py             # OpenAI Batch API — отправка, поллинг, парсинг
│   └── schemas.py           # AIInsights — structured output схема
│
├── platforms/
│   ├── base.py              # BaseScraper — абстрактный интерфейс
│   └── instagram/
│       ├── client.py        # AccountPool — ротация аккаунтов
│       ├── scraper.py       # InstagramScraper — сбор данных
│       ├── metrics.py       # ER, тренд, частота публикаций
│       └── exceptions.py    # Кастомные исключения
│
└── worker/
    ├── loop.py              # Polling loop с graceful shutdown
    ├── handlers.py          # Обработчики задач
    └── scheduler.py         # APScheduler — cron-задачи
```

## Типы задач

| Тип | Приоритет | Что делает |
|-----|-----------|-----------|
| `full_scrape` | 1-5 | Полный скрейп профиля: посты, рилсы, хайлайты, метрики |
| `ai_analysis` | 3 | AI-анализ через OpenAI Batch API (батчами по 10+) |
| `discover` | 10 | Поиск новых блогеров по хештегу |

## Переменные окружения

Все переменные описаны в [`.env.example`](.env.example). Ключевые группы:

| Группа | Переменные | Описание |
|--------|-----------|----------|
| Supabase | `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` | Подключение к БД |
| OpenAI | `OPENAI_API_KEY` | API ключ для Batch API |
| Instagram | `INSTAGRAM_ACCOUNTS`, `IG_*_USERNAME/PASSWORD` | Аккаунты для скрейпинга |
| Прокси | `PROXY_*` | Residential прокси (по одному на аккаунт) |
| Rate limits | `REQUESTS_PER_HOUR`, `COOLDOWN_MINUTES` | Защита от бана |
| Worker | `WORKER_POLL_INTERVAL`, `WORKER_MAX_CONCURRENT` | Параметры воркера |

## Технологии

| Компонент | Технология |
|-----------|-----------|
| Язык | Python 3.13 |
| Instagram API | instagrapi 2.1+ |
| AI-анализ | OpenAI Batch API (gpt-5-nano) |
| База данных | Supabase PostgreSQL |
| Хранилище | Supabase Storage (сессии) |
| Планировщик | APScheduler 3.x |
| Валидация | Pydantic 2.x |
| Пакеты | uv |
| Контейнер | Docker |
