# Архитектура Scraper

## Обзор

Single-process сервис (FastAPI + polling worker), который:
1. Принимает задачи через HTTP API (создание scrape/discover)
2. Читает задачи из таблицы `scrape_tasks` (Supabase PostgreSQL)
3. Скрейпит Instagram через `instagrapi` с ротацией аккаунтов
4. Отправляет профили на AI-анализ через OpenAI Batch API
5. Записывает результаты обратно в Supabase

Без Redis, без внешних очередей — одна таблица `scrape_tasks` выполняет роль task queue. FastAPI и polling loop работают в одном asyncio event loop.

---

## Компоненты

### Общая схема

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Scraper Process                               │
│                                                                         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────────┐  │
│  │  FastAPI :8001    │  │   Polling Loop    │  │    APScheduler      │  │
│  │                  │  │                  │  │                     │  │
│  │  POST /scrape    │  │  Каждые 30с:     │  │  poll_batches  15м  │  │
│  │  POST /discover  │  │  fetch_pending   │  │  recover_tasks 10м  │  │
│  │  GET  /tasks     │  │  tasks (limit 10)│  │  retry_stale   2ч   │  │
│  │  GET  /health    │  │                  │  │  schedule   daily   │  │
│  └────────┬─────────┘  └────────┬─────────┘  │  discover weekly    │  │
│           │                     │ dispatch    └──────────┬──────────┘  │
│           │ create task         ▼                        │ trigger     │
│           │            ┌────────────────────────────────────────────┐  │
│           └───────────►│              Task Handlers                 │  │
│                        │                                            │  │
│                        │  ┌───────────┐ ┌────────────┐ ┌─────────┐ │  │
│                        │  │full_scrape│ │ai_analysis │ │discover │ │  │
│                        │  └─────┬─────┘ └─────┬──────┘ └────┬────┘ │  │
│                        └────────│─────────────│─────────────│──────┘  │
│                                 │             │             │         │
└─────────────────────────────────│─────────────│─────────────│─────────┘
                                  │             │             │
                           ┌──────▼──────┐ ┌────▼───────────┐ │
                           │  Instagram   │ │  OpenAI Batch  │ │
                           │ (instagrapi) │ │  API           │ │
                           │ AccountPool  │ │  gpt-5-nano    │ │
                           └─────────────┘ └────────────────┘ │
                           ┌─────────────┐                    │
                           │  Instagram   │◄───────────────────┘
                           │ hashtag tops │
                           └─────────────┘
```

### Слои

```
src/
│
├── config.py, main.py       ← Инфраструктура (конфигурация, запуск)
├── database.py, storage.py  ← Доступ к данным (Supabase)
├── api/                     ← HTTP API (FastAPI, эндпоинты, схемы)
├── models/                  ← Модели данных (Pydantic)
├── ai/                      ← AI-интеграция (OpenAI Batch API)
├── platforms/               ← Платформы (Instagram, расширяемо)
└── worker/                  ← Оркестрация (loop, handlers, scheduler)
```

Зависимости идут сверху вниз: `api, worker → ai, platforms → models, database, config`.

---

## Жизненный цикл задачи

### Состояния

```
                    создана
                       │
                       ▼
                  ┌─────────┐
         ┌───────│ pending  │◄──────┐
         │       └────┬─────┘       │
         │            │ claim       │ retry
         │            ▼             │ (backoff)
         │       ┌─────────┐       │
         │       │ running  ├───────┘
         │       └──┬────┬──┘
         │          │    │
         │   done   │    │ fail (max attempts)
         │          ▼    ▼
         │     ┌──────┐ ┌────────┐
         │     │ done │ │ failed │
         │     └──────┘ └────────┘
         │
         │ private/deleted
         ▼
      ┌──────┐
      │ done │  (scrape_status = 'private' | 'deleted')
      └──────┘
```

### Приоритеты

| Приоритет | Тип задачи | Источник |
|-----------|-----------|----------|
| 1-3 | `full_scrape` | Ручное создание, высокий приоритет |
| 3 | `ai_analysis` | Автоматически после full_scrape |
| 5 | `full_scrape` | Из discover (новый блогер) |
| 8 | `full_scrape` | Из schedule_updates (повторный скрейп) |
| 10 | `discover` | Из discover_weekly (еженедельный поиск) |

### Обработка ошибок и retry

```
Ошибка при выполнении задачи
       │
       ├── PrivateAccountError
       │     └─ scrape_status='private', task done (без retry)
       │
       ├── UserNotFound
       │     └─ scrape_status='deleted', task done (без retry)
       │
       ├── AllAccountsCooldownError
       │     └─ mark_task_failed(retry=True)
       │          └─ attempt 1 → retry через 5 мин
       │          └─ attempt 2 → retry через 15 мин
       │          └─ attempt 3 → status='failed' (finalized)
       │
       └── Любая другая ошибка
             └─ mark_task_failed(retry=True)
                  └─ Экспоненциальный backoff: 5м → 15м → 45м
```

---

## Instagram AccountPool

### Ротация аккаунтов

```
          ┌──────────────────────────────────────────┐
          │              AccountPool                  │
          │                                          │
          │    ┌─────────┐  ┌─────────┐  ┌────────┐ │
          │    │ Account1 │  │ Account2 │  │ Acc. N │ │
          │    │          │  │          │  │        │ │
          │    │ proxy: A │  │ proxy: B │  │ prx: N │ │
          │    │ req: 12  │  │ req: 28  │  │ req: 5 │ │
          │    │ ok       │  │ cooldown │  │ ok     │ │
          │    └─────────┘  └─────────┘  └────────┘ │
          │          ▲                        ▲      │
          │    round-robin ───────────────────┘      │
          └──────────────────────────────────────────┘

safe_request(func, *args):
  ┌──────────────────────────────────────────────────┐
  │  attempt 1                                       │
  │    acc = get_available_account()                  │
  │    ├─ ok → return result                         │
  │    ├─ RateLimit → mark cooldown, try next        │
  │    ├─ Challenge → mark 2x cooldown, try next     │
  │    ├─ LoginRequired → re-login, retry            │
  │    ├─ UserNotFound → raise (no retry)            │
  │    └─ ClientError → raise ScraperError           │
  │                                                  │
  │  attempt 2 → другой аккаунт                      │
  │  attempt 3 → другой аккаунт                      │
  │                                                  │
  │  3 попытки → AllAccountsCooldownError            │
  └──────────────────────────────────────────────────┘
```

### Anti-detection

- Один аккаунт = один sticky residential proxy (persistent IP)
- Сохранение device UUID между сессиями (Supabase Storage)
- Random delay 2-5с между запросами
- Лимит 30 запросов/час на аккаунт (конфигурируемо)
- Cooldown 45 мин при rate limit, 90 мин при challenge

---

## OpenAI Batch API Pipeline

### Отправка батча

```
                  Накопилось 10+ задач ai_analysis
                  (или самая старая > 2 часов)
                               │
                               ▼
            ┌──────────────────────────────────────┐
            │  Для каждого профиля:                │
            │                                      │
            │  blogs + blog_posts + blog_highlights │
            │           │                          │
            │           ▼                          │
            │  build_analysis_prompt(profile)       │
            │  ┌────────────────────────────────┐  │
            │  │ System: "You are an analyst..." │  │
            │  │                                │  │
            │  │ User (multimodal):             │  │
            │  │  ├── Текст: username, bio,     │  │
            │  │  │   followers, posts с ER%,   │  │
            │  │  │   reels с plays, hashtags,  │  │
            │  │  │   mentions, бренды          │  │
            │  │  │                             │  │
            │  │  └── Изображения (до 10):      │  │
            │  │      avatar + thumbnails       │  │
            │  └────────────────────────────────┘  │
            │           │                          │
            │           ▼                          │
            │  JSONL (в памяти)                    │
            │  ┌──────────────────────────────┐    │
            │  │ {"custom_id": "blog-uuid-1", │    │
            │  │  "method": "POST",           │    │
            │  │  "url": "/v1/chat/...",      │    │
            │  │  "body": {                   │    │
            │  │    "model": "gpt-5-nano",    │    │
            │  │    "response_format":        │    │
            │  │      json_schema(AIInsights) │    │
            │  │  }}                          │    │
            │  │ {"custom_id": "blog-uuid-2", │    │
            │  │  ...}                        │    │
            │  └──────────────────────────────┘    │
            └──────────────┬───────────────────────┘
                           │
                           ▼
            ┌──────────────────────────────────────┐
            │  OpenAI Files API                    │
            │  upload(batch.jsonl) → file_id       │
            │                                      │
            │  OpenAI Batches API                  │
            │  create(file_id, 24h) → batch_id     │
            └──────────────────────────────────────┘
                           │
                           ▼
            scrape_tasks: status='running',
            payload={'batch_id': 'batch_abc123'}
```

### Поллинг результатов

```
  APScheduler: каждые 15 мин
         │
         ▼
  SELECT * FROM scrape_tasks
  WHERE task_type='ai_analysis' AND status='running'
         │
         ├─ Группировка по payload.batch_id
         │
         ▼
  poll_batch(batch_id)
         │
         ├─ in_progress / validating → ждём (retry через 15м)
         │
         ├─ completed / expired
         │     │
         │     ├─ output_file → JSONL результатов
         │     │    │
         │     │    ▼
         │     │  Парсинг: custom_id → AIInsights
         │     │    ├─ Успех → upsert blog (ai_insights, confidence)
         │     │    │           match_categories()
         │     │    │           mark_task_done()
         │     │    │
         │     │    ├─ Refusal → blog.scrape_status='active' (без insights)
         │     │    │
         │     │    └─ Ошибка парсинга → log + None
         │     │
         │     └─ error_file → ошибки (логируем)
         │
         └─ expired (без результатов для части задач)
               └─ mark_task_failed(retry=True)
```

### AI Structured Output

```
AIInsights
├── life_situation
│   ├── has_children: bool?
│   ├── children_age_group: baby | toddler | school | teen
│   ├── relationship_status: married | in_relationship | single
│   └── is_young_parent: bool?
│
├── lifestyle
│   ├── has_car: bool?          car_class: budget..luxury
│   ├── travels_frequently: bool?   travel_style: budget..luxury
│   ├── has_pets: bool?         pet_types: [str]
│   ├── has_real_estate: bool?
│   └── lifestyle_level: budget | middle | premium | luxury
│
├── content
│   ├── primary_topic: str      secondary_topics: [str]
│   ├── content_language: [str]
│   ├── content_tone: positive | neutral | educational | humor | inspirational
│   └── posts_in_russian: bool?    posts_in_kazakh: bool?
│
├── commercial
│   ├── has_brand_collaborations: bool?
│   ├── detected_brand_categories: [str]
│   ├── has_affiliate_links: bool?
│   └── ad_frequency: rare | moderate | frequent
│
├── audience_inference
│   ├── estimated_audience_gender: mostly_female | mostly_male | mixed
│   ├── estimated_audience_age: 18-24 | 25-34 | 35-44 | mixed
│   ├── estimated_audience_geo: kz | ru | uz | cis_mixed
│   └── geo_mentions: [str]
│
├── confidence: float (0.0 – 1.0)
└── notes: str?
```

---

## Scheduler — периодические задачи

```
┌─────────────────────────────────────────────────────────────────┐
│                        APScheduler                              │
│                                                                 │
│  ┌───────────────────────┬───────────┬────────────────────────┐ │
│  │ Job                   │ Интервал  │ Действие               │ │
│  ├───────────────────────┼───────────┼────────────────────────┤ │
│  │ poll_batches          │ 15 мин    │ Проверка статуса       │ │
│  │                       │           │ OpenAI батчей          │ │
│  ├───────────────────────┼───────────┼────────────────────────┤ │
│  │ recover_tasks         │ 10 мин    │ Зависшие задачи        │ │
│  │                       │           │ (>30м) → pending       │ │
│  ├───────────────────────┼───────────┼────────────────────────┤ │
│  │ retry_stale_batches   │ 2 часа    │ Батчи >26ч → retry    │ │
│  ├───────────────────────┼───────────┼────────────────────────┤ │
│  │ schedule_updates      │ Daily     │ Блоги scraped >60д     │ │
│  │                       │ 03:00 UTC │ → full_scrape          │ │
│  ├───────────────────────┼───────────┼────────────────────────┤ │
│  │ discover_weekly       │ Пн        │ Хештеги из конфига     │ │
│  │                       │ 02:00 UTC │ → discover tasks       │ │
│  └───────────────────────┴───────────┴────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## HTTP API

FastAPI-сервер запускается параллельно с polling loop через `asyncio.gather()` в одном процессе.

### Эндпоинты

| Метод | Путь | Auth | Описание |
|-------|------|------|----------|
| `GET` | `/api/health` | Нет | Healthcheck: аккаунты, задачи |
| `GET` | `/api/tasks` | Да | Список задач (фильтры: status, task_type, limit, offset) |
| `GET` | `/api/tasks/{id}` | Да | Статус конкретной задачи |
| `POST` | `/api/tasks/scrape` | Да | Создать full_scrape по списку username |
| `POST` | `/api/tasks/discover` | Да | Создать discover по хештегу |

### Авторизация

Bearer token через заголовок `Authorization: Bearer <SCRAPER_API_KEY>`. Dependency `verify_api_key` проверяет все роуты кроме `/api/health`.

### Структура файлов

```
src/api/
├── __init__.py
├── app.py         # create_app(db, pool, settings) → FastAPI
└── schemas.py     # Request/Response Pydantic-модели
```

### Интеграция с main.py

```python
# main.py — один процесс, один event loop
app = create_app(db, pool, settings)
server = uvicorn.Server(uvicorn.Config(app, port=settings.scraper_port))

await asyncio.gather(
    server.serve(),
    run_worker(db, scrapers, settings, shutdown_event, openai_client),
)
```

---

## Схема базы данных

### ER-диаграмма

```
┌───────────────┐       ┌──────────────────────────────────────────┐
│   persons     │       │               blogs                      │
│               │       │                                          │
│ id (PK)       │◄──┐   │ id (PK)                                  │
│ full_name     │   └──│ person_id (FK)                            │
│ created_at    │       │ platform          username                │
└───────────────┘       │ platform_id       full_name               │
                        │ biography         avatar_url              │
                        │ follower_count    following_count          │
                        │ media_count       is_verified              │
                        │ is_business       business_category        │
                        │ external_url      er          er_reels    │
                        │ er_trend          posts_per_week           │
                        │ scrape_status     scraped_at               │
                        │ ai_insights (JSONB)                       │
                        │ ai_confidence     ai_analyzed_at           │
                        │ source            created_at  updated_at  │
                        └────┬────────────────────┬─────────────────┘
                             │                    │
                     ┌───────▼────────┐   ┌───────▼──────────┐
                     │  blog_posts    │   │ blog_highlights   │
                     │               │   │                  │
                     │ id (PK)       │   │ id (PK)          │
                     │ blog_id (FK)  │   │ blog_id (FK)     │
                     │ platform_id   │   │ platform_id      │
                     │ caption_text  │   │ title            │
                     │ hashtags []   │   │ media_count      │
                     │ mentions []   │   │ cover_url        │
                     │ like_count    │   │ story_mentions[] │
                     │ comment_count │   │ story_locations[]│
                     │ play_count    │   │ story_links []   │
                     │ media_type    │   │ created_at       │
                     │ product_type  │   └──────────────────┘
                     │ thumbnail_url │
                     │ location_*    │
                     │ taken_at      │       ┌────────────────┐
                     │ created_at    │       │  categories    │
                     └───────────────┘       │                │
                                             │ id (PK)        │
                     ┌───────────────┐       │ name (UNIQUE)  │
                     │ scrape_tasks  │       └───────┬────────┘
                     │               │               │
                     │ id (PK)       │       ┌───────▼────────┐
                     │ blog_id (FK)  │       │blog_categories │
                     │ task_type     │       │                │
                     │ status        │       │ blog_id (FK)   │
                     │ priority      │       │ category_id(FK)│
                     │ payload (JSON)│       │ is_primary     │
                     │ attempts      │       └────────────────┘
                     │ max_attempts  │
                     │ error_message │
                     │ next_retry_at │
                     │ started_at    │
                     │ completed_at  │
                     │ created_at    │
                     └───────────────┘

Уникальные ограничения:
  blogs:           (platform, username)
  blog_posts:      (blog_id, platform_id)
  blog_highlights: (blog_id, platform_id)
  blog_categories: (blog_id, category_id)
```

### Таблицы

| Таблица | Назначение | Ключевые поля |
|---------|-----------|---------------|
| `scrape_tasks` | Очередь задач | task_type, status, priority, payload |
| `blogs` | Профили блогеров | username, follower_count, ai_insights, scrape_status |
| `blog_posts` | Посты и рилсы | caption_text, like_count, play_count, media_type |
| `blog_highlights` | Хайлайты (сторис) | title, story_mentions, story_links |
| `persons` | Персоны (владельцы блогов) | full_name |
| `categories` | Категории контента | name |
| `blog_categories` | Связь блог↔категория | blog_id, category_id, is_primary |

### Supabase Storage

| Бакет | Содержимое |
|-------|-----------|
| `instagram-sessions` | `{account_name}.json` — сессии instagrapi |

---

## Потоки данных

### Full Scrape

```
full_scrape task (pending)
        │
        ▼
mark_task_running()  ←── atomic RPC (SELECT FOR UPDATE)
        │
        ▼
blogs.scrape_status = 'scraping'
        │
        ▼
scraper.scrape_profile(username)
  ├── user_info_by_username()  → bio, followers, verified
  ├── user_medias(limit=20)   → posts + reels
  ├── user_highlights()        → highlight titles + stories
  ├── calculate_er()           → median engagement rate
  ├── calculate_er_trend()     → growing / stable / declining
  └── calculate_posts_per_week()
        │
        ▼
upsert_blog()        → UPDATE blogs SET ...
upsert_posts()       → INSERT blog_posts ON CONFLICT UPDATE
upsert_highlights()  → INSERT blog_highlights ON CONFLICT UPDATE
        │
        ▼
create_task_if_not_exists('ai_analysis', priority=3)
        │
        ▼
mark_task_done()
blogs.scrape_status = 'analyzing'
```

### Discover

```
discover task (payload: {hashtag: 'алматымама'})
        │
        ▼
scraper.discover(hashtag, min_followers=1000)
  ├── hashtag_medias_top(hashtag, amount=50)
  ├── Извлечение уникальных user_pk
  ├── user_info(pk) для каждого
  └── Фильтр: not private, ≥1000 followers, ≥5 posts
        │
        ▼
Для каждого нового блогера:
  ├── INSERT persons(full_name)
  ├── INSERT blogs(username, platform_id, ...)
  └── create_task('full_scrape', priority=5)
```

---

## Деплой

### Docker

```bash
# Build и запуск
docker compose up -d

# Логи
docker compose logs -f scraper

# Остановка (30с grace period для shutdown)
docker compose stop scraper
```

### Ресурсы

- Memory limit: 512MB
- Max concurrent tasks: 2 (конфигурируемо)
- Логи: json-file, ротация 50MB x 3 файла

### Graceful Shutdown

```
SIGTERM / SIGINT
       │
       ▼
shutdown_event.set()
       │
       ├── uvicorn.Server.should_exit = True  ← API перестаёт принимать запросы
       ├── Polling loop завершается
       ├── Ожидание активных задач (до 30с)
       ├── scheduler.shutdown()
       └── pool.save_all_sessions()  ← сохранение Instagram-сессий
```
