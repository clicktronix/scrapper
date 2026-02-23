# Архитектура Scraper

## Обзор

Single-process сервис (FastAPI + polling worker + APScheduler), который:
1. Принимает задачи через HTTP API (создание scrape/discover)
2. Читает задачи из таблицы `scrape_tasks` (Supabase PostgreSQL)
3. Скрейпит Instagram через HikerAPI (SaaS) или instagrapi (локально)
4. Загружает изображения (аватар + thumbnails постов) в Supabase Storage
5. Отправляет профили на AI-анализ через OpenAI Batch API (gpt-5-nano)
6. AI-анализ: категоризация, таксономия тегов, генерация embedding
7. Записывает результаты обратно в Supabase

Без Redis, без внешних очередей — одна таблица `scrape_tasks` выполняет роль task queue. FastAPI, polling loop и APScheduler работают в одном asyncio event loop.

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
│  │  POST /retry     │  │  tasks (limit 10)│  │  retry_stale   2ч   │  │
│  │  GET  /tasks     │  │                  │  │  schedule   daily   │  │
│  │  GET  /health    │  │                  │  │  cleanup    weekly  │  │
│  └────────┬─────────┘  └────────┬─────────┘  └──────────┬──────────┘  │
│           │                     │ dispatch               │ trigger     │
│           │ create task         ▼                        ▼             │
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
                    ┌─────────────▼──┐   ┌──────▼──────────┐  │
                    │ HikerAPI (SaaS)│   │  OpenAI Batch   │  │
                    │       или      │   │  API            │  │
                    │ instagrapi     │   │  gpt-5-nano     │  │
                    │ (AccountPool)  │   └─────────────────┘  │
                    └────────────────┘                         │
                    ┌────────────────┐                         │
                    │  Supabase      │◄────────────────────────┘
                    │  Storage       │
                    │  (images)      │
                    └────────────────┘
```

### Слои

```
src/
│
├── config.py, main.py       ← Инфраструктура (конфигурация, запуск)
├── database.py, storage.py  ← Доступ к данным (Supabase)
├── image_storage.py         ← Загрузка изображений в Supabase Storage
├── api/                     ← HTTP API (FastAPI, эндпоинты, схемы)
├── models/                  ← Модели данных (Pydantic)
├── ai/                      ← AI-интеграция (OpenAI Batch, таксономия, embedding)
│   ├── batch.py             ← OpenAI Batch API, match_categories, match_tags
│   ├── embedding.py         ← text-embedding-3-small, генерация embedding
│   ├── images.py            ← Подготовка изображений для мультимодального анализа
│   ├── prompt.py            ← System/user prompt для AI-анализа
│   ├── schemas.py           ← AIInsights (structured output)
│   └── taxonomy.py          ← CATEGORIES + TAGS (3-уровневая таксономия)
├── platforms/               ← Платформы (Instagram)
│   └── instagram/
│       ├── hiker_scraper.py ← HikerAPI бэкенд (SaaS, SafeHikerClient)
│       ├── scraper.py       ← instagrapi бэкенд (локальный)
│       ├── client.py        ← AccountPool (ротация аккаунтов)
│       ├── metrics.py       ← ER, trend, posts_per_week
│       └── exceptions.py    ← HikerAPIError, InsufficientBalanceError
└── worker/                  ← Оркестрация (loop, handlers, scheduler)
```

Зависимости: `api, worker → ai, platforms → models, database, config`.

---

## Бэкенды скрапинга

### HikerAPI (основной)

SaaS-бэкенд через `SafeHikerClient` — наследник `hikerapi.Client` с проверкой HTTP-статусов:
- **402** → `InsufficientBalanceError` (ретрай бесполезен)
- **429** → `HikerAPIError` (ретрай с backoff)
- **5xx** → `HikerAPIError` (ретрай с backoff)
- **4xx** → `HikerAPIError` (без ретрая)

Настройка: `SCRAPER_BACKEND=hikerapi` + `HIKERAPI_TOKEN=...`

### instagrapi (резервный)

Локальный бэкенд с `AccountPool`:
- Ротация аккаунтов (round-robin)
- Sticky residential proxy (один IP на аккаунт)
- Сохранение device UUID в Supabase Storage
- Лимит 30 req/час, cooldown 45м при rate limit
- Random delay 2-5с между запросами

Настройка: `SCRAPER_BACKEND=instagrapi`

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
         │            │ claim       │ retry (backoff)
         │            ▼             │ или POST /retry
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
| 3 | `full_scrape` | API `/api/tasks/scrape` (ручное создание) |
| 3 | `ai_analysis` | Автоматически после full_scrape |
| 5 | `full_scrape` | Discover (новые блогеры из хештегов) |
| 8 | `full_scrape` | Scheduler `schedule_updates` (ежедневно 3:00 UTC) |
| 10 | `discover` | API `/api/tasks/discover` |

Очередь: `ORDER BY priority ASC, created_at ASC` — низкие числа = высокий приоритет.

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
       ├── InsufficientBalanceError (HTTP 402)
       │     └─ mark_task_failed(retry=False) — ретрай бесполезен
       │
       ├── HikerAPIError (429, 5xx)
       │     └─ mark_task_failed(retry=True) — backoff 5м → 15м → 45м
       │
       ├── HikerAPIError (другие 4xx)
       │     └─ mark_task_failed(retry=False)
       │
       ├── AllAccountsCooldownError (instagrapi)
       │     └─ mark_task_failed(retry=True) — backoff
       │
       └── Любая другая ошибка
             └─ mark_task_failed(retry=True)
                  └─ Экспоненциальный backoff: 5м → 15м → 45м
```

---

## Загрузка изображений

`persist_profile_images()` загружает в Supabase Storage (бакет `blog-images`):
- **Аватар**: `{blog_id}/avatar.jpg`
- **Thumbnails постов**: `{blog_id}/post_{platform_id}.jpg`

Ограничения: семафор 4 параллельных загрузки, таймаут 15с, макс. 10МБ.
CDN-URL из Instagram заменяются на постоянные Storage URL.

---

## AI Pipeline

### OpenAI Batch API

1. **Накопление** — ждёт 10+ задач `ai_analysis` или самая старая > 2ч
2. **Промпт** — мультимодальный (текст профиля + до 10 изображений)
3. **Отправка** — JSONL → OpenAI Files API → Batches API (gpt-5-nano, 24ч deadline)
4. **Поллинг** — APScheduler каждые 15 мин проверяет статус батчей
5. **Результат** — `AIInsights` (structured output) → upsert в `blogs.ai_insights`

### После получения результата

```
AIInsights (from OpenAI)
       │
       ├── match_categories()  → blog_categories (upsert)
       ├── match_tags()        → blog_tags (batch upsert)
       └── generate_embedding() → blogs.embedding (text-embedding-3-small, 1536 dims)
```

### Таксономия (src/ai/taxonomy.py)

3-уровневая иерархия:
- **~20 категорий** → **~120 подкатегорий** → **~200 тегов**
- Теги разделены по группам: `content`, `personal`, `professional`, `commercial`, `audience`, `marketing`
- AI анализирует профиль и выбирает подходящие теги из справочника

### Embedding

`text-embedding-3-small` генерирует вектор 1536 dims из:
- biography, primary_topic, secondary_topics
- hashtags, mentions, content_tone, lifestyle

Используется для семантического поиска блогеров через pgvector.

---

## Scheduler — периодические задачи

| Job | Интервал | Действие |
|-----|----------|----------|
| `poll_batches` | 15 мин | Проверка статуса OpenAI батчей |
| `recover_tasks` | 10 мин | Зависшие задачи (>30м running) → pending |
| `retry_stale_batches` | 2 часа | Батчи >26ч → retry |
| `schedule_updates` | Daily 03:00 UTC | Блоги `active` + `scraped_at > 60д` → full_scrape (по подписчикам DESC, limit 100) |
| `cleanup_old_images` | Вс 04:00 UTC | Удаление старых изображений из Storage |

---

## HTTP API

### Эндпоинты

| Метод | Путь | Auth | Описание |
|-------|------|------|----------|
| `GET` | `/api/health` | Нет | Healthcheck: аккаунты, задачи |
| `GET` | `/api/tasks` | Да | Список задач (фильтры: status, task_type, limit, offset) |
| `GET` | `/api/tasks/{id}` | Да | Статус конкретной задачи |
| `POST` | `/api/tasks/scrape` | Да | Создать full_scrape по списку username |
| `POST` | `/api/tasks/discover` | Да | Создать discover по хештегу |
| `POST` | `/api/tasks/{id}/retry` | Да | Повторить упавшую задачу (только status=failed) |

Auth: `Authorization: Bearer <SCRAPER_API_KEY>`. Rate limit: 60 req/мин per IP.

Guard: `is_blog_fresh()` — не создавать задачу если блог скрапили < 60 дней назад.

---

## Схема базы данных

### Таблицы

| Таблица | Назначение | Ключевые поля |
|---------|-----------|---------------|
| `scrape_tasks` | Очередь задач | task_type, status, priority, payload, attempts |
| `blogs` | Профили блогеров | username, follower_count, ai_insights, scrape_status, embedding |
| `blog_posts` | Посты и рилсы | caption_text, like_count, play_count, media_type |
| `blog_highlights` | Хайлайты (сторис) | title, story_mentions, story_links |
| `persons` | Персоны (владельцы блогов) | full_name |
| `categories` | Категории контента | name, code, parent_id |
| `blog_categories` | Связь блог↔категория | blog_id, category_id, is_primary |
| `tags` | Теги таксономии | name, slug, group, status |
| `blog_tags` | Связь блог↔тег | blog_id, tag_id |

### Supabase Storage

| Бакет | Содержимое |
|-------|-----------|
| `instagram-sessions` | `{account_name}.json` — сессии instagrapi |
| `blog-images` | Аватары и thumbnails постов |

---

## Потоки данных

### Full Scrape

```
full_scrape task (pending)
        │
        ▼
mark_task_running()  ←── atomic RPC (WHERE status='pending' RETURNING id)
        │
        ▼
blogs.scrape_status = 'scraping'
        │
        ▼
scraper.scrape_profile(username)
  ├── user_info_by_username()  → bio, followers, verified
  ├── user_medias()            → posts + reels
  ├── user_highlights()        → highlight titles + stories
  ├── calculate_er()           → median engagement rate
  ├── calculate_er_trend()     → growing / stable / declining
  └── calculate_posts_per_week()
        │
        ▼
persist_profile_images()  → avatar + post thumbnails → Supabase Storage
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
discover task (payload: {hashtag: '...', min_followers: 1000})
        │
        ▼
scraper.discover(hashtag, min_followers)
  ├── hashtag_medias_top(hashtag, amount=50)
  ├── Извлечение уникальных user_pk
  ├── user_info(pk) для каждого
  └── Фильтр: not private, ≥min_followers, ≥5 posts
        │
        ▼
Для каждого нового блогера:
  ├── INSERT persons(full_name)
  ├── INSERT blogs(username, platform_id, ...)
  ├── is_blog_fresh() → пропуск если < 60 дней
  └── create_task('full_scrape', priority=5)
```

---

## Деплой

### Docker

```bash
docker compose up -d        # Build и запуск
docker compose logs -f      # Логи
docker compose stop         # Остановка (30с grace period)
```

### Graceful Shutdown

```
SIGTERM / SIGINT
       │
       ▼
shutdown_event.set()
       │
       ├── uvicorn.Server: перестаёт принимать запросы
       ├── Polling loop завершается
       ├── Ожидание активных задач (до 30с)
       ├── scheduler.shutdown()
       └── pool.save_all_sessions() (instagrapi)
```

---

## Архив

Выполненные планы и дизайны: `docs/ARCHIVE/`
