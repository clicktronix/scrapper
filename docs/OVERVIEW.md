# Scraper — обзор системы

Автономный Python-сервис для сбора и AI-анализа Instagram-профилей блогеров.

---

## Общая архитектура

```mermaid
graph TB
    subgraph API["HTTP API :8001"]
        health["GET /health"]
        tasks_list["GET /tasks"]
        scrape["POST /tasks/scrape"]
        pre_filter["POST /tasks/pre_filter"]
        discover["POST /tasks/discover"]
        retry["POST /tasks/{id}/retry"]
    end

    subgraph Worker["Worker (polling loop)"]
        poll["Поллинг каждые 30с"]
        sem["Semaphore (макс. 2 задачи)"]
        dispatch["Диспетчер задач"]
    end

    subgraph Handlers["Обработчики задач"]
        h_scrape["full_scrape"]
        h_ai["ai_analysis"]
        h_discover["discover"]
        h_prefilter["pre_filter"]
    end

    subgraph Scheduler["APScheduler (cron)"]
        poll_batches["poll_batches (15м)"]
        recover["recover_tasks (10м)"]
        schedule_updates["schedule_updates (03:00)"]
        cleanup["cleanup_old_images (Вс 04:00)"]
        retry_emb["retry_missing_embeddings (1ч)"]
        retry_tax["retry_taxonomy_mappings (2ч)"]
    end

    subgraph External["Внешние сервисы"]
        hiker["HikerAPI (SaaS)"]
        openai["OpenAI Batch API"]
        storage["Supabase Storage"]
    end

    DB[(Supabase PostgreSQL)]

    API -->|"создаёт задачи"| DB
    poll -->|"SELECT pending"| DB
    poll --> sem --> dispatch
    dispatch --> h_scrape & h_ai & h_discover & h_prefilter

    h_scrape -->|"scrape_profile()"| hiker
    h_scrape -->|"upsert blog/posts"| DB
    h_scrape -->|"upload images"| storage
    h_scrape -->|"создаёт ai_analysis"| DB

    h_ai -->|"submit_batch()"| openai
    poll_batches -->|"poll_batch()"| openai
    poll_batches -->|"save insights"| DB

    h_discover -->|"discover(hashtag)"| hiker
    h_discover -->|"insert person+blog"| DB

    h_prefilter -->|"user_info + medias"| hiker
    h_prefilter -->|"insert person+blog"| DB

    recover -->|"stuck → pending"| DB
    schedule_updates -->|"stale → full_scrape"| DB
    cleanup -->|"delete old images"| storage
```

---

## Жизненный цикл задачи

```mermaid
stateDiagram-v2
    [*] --> pending: Создание задачи
    pending --> running: Worker забрал (атомарный RPC)
    running --> done: Успешно обработана
    running --> failed: Ошибка (исчерпаны попытки)
    running --> pending: Ошибка (есть попытки, backoff)

    note right of pending
        next_retry_at: 5м / 15м / 45м
        (exponential backoff)
    end note

    note right of failed
        retry=False: баланс, не найден
        retry=True: сетевые, 429/5xx
    end note
```

| Статус | Описание |
|--------|----------|
| `pending` | Ожидает обработки. Если `next_retry_at` заполнен — ждёт время ретрая |
| `running` | Забрана воркером, в процессе |
| `done` | Завершена успешно (или filtered_out для pre_filter) |
| `failed` | Ошибка, все попытки исчерпаны |

---

## Типы задач

| Тип | Приоритет | Создаётся | Что делает | Результат |
|-----|-----------|-----------|------------|-----------|
| `pre_filter` | 8 | API, xlsx-импорт | Быстрая проверка: приватный? активный? лайки? | person + blog или filtered_out в pre_filter_log |
| `full_scrape` | 3-8 | API, discover, scheduler | Полный скрейп: 25 постов, 3 хайлайта, метрики, фото | Данные в blogs/posts/highlights + ai_analysis задача |
| `ai_analysis` | 3 | full_scrape handler | AI-анализ через OpenAI Batch API | ai_insights, категории, теги, embedding |
| `discover` | 10 | API | Поиск блогеров по хештегу | Новые person + blog + full_scrape задачи |

---

## Worker — цикл обработки

```mermaid
flowchart TD
    start(["Worker запущен"]) --> poll["Поллинг БД (каждые 30с)"]
    poll --> fetch["fetch_pending_tasks(limit=10)\nnext_retry_at <= now"]
    fetch --> check{Есть задачи?}
    check -->|Нет| wait["sleep(poll_interval)"] --> poll
    check -->|Да| loop["Для каждой задачи"]
    loop --> dup{Уже в обработке?}
    dup -->|Да| loop
    dup -->|Нет| acquire["Semaphore.acquire()\n(макс. 2 параллельно)"]
    acquire --> claim["mark_task_running()\n(атомарный RPC)"]
    claim --> claimed{Забрана?}
    claimed -->|Нет, другой воркер| loop
    claimed -->|Да| handler["Вызов обработчика"]
    handler --> result{Результат?}
    result -->|OK| done["mark_task_done()"]
    result -->|Ошибка| retry{Retry?}
    retry -->|Да, есть попытки| pending["mark_task_failed()\nstatus=pending\nnext_retry_at=backoff"]
    retry -->|Нет| failed["mark_task_failed()\nstatus=failed"]
    done & pending & failed --> release["Semaphore.release()"] --> loop

    shutdown(["SIGTERM"]) --> graceful["shutdown_event.set()"]
    graceful --> wait_tasks["Ожидание активных задач (30с)"]
    wait_tasks --> stop(["Остановка"])
```

---

## Full Scrape — подробный поток

```mermaid
flowchart TD
    start["full_scrape задача"] --> claim["Атомарный claim через RPC"]
    claim --> blog["Загрузить blog из БД"]
    blog --> deleted{Блог удалён/деактивирован?}
    deleted -->|Да| skip["Пропустить, mark done"]
    deleted -->|Нет| mark_scraping["blog.scrape_status = 'scraping'"]
    mark_scraping --> scrape["scraper.scrape_profile(username)"]

    scrape -->|OK| profile["ScrapedProfile:\n25 постов, 3 хайлайта,\nметрики, комментарии"]
    scrape -->|PrivateAccountError| private["blog = 'private', done"]
    scrape -->|UserNotFound| not_found["blog = 'deleted', done"]
    scrape -->|InsufficientBalance| no_retry["failed (без retry)"]
    scrape -->|HikerAPI 429/5xx| with_retry["failed (с retry)"]
    scrape -->|AllAccountsCooldown| with_retry

    profile --> images["Загрузить аватар + thumbnails\n(до 7 постов, параллельно)"]
    images --> upload["Upload в Supabase Storage\nblog-images/{blog_id}/"]
    upload --> upsert["upsert blog + posts + highlights"]
    upsert --> create_ai["Создать ai_analysis задачу\n(приоритет 3)"]
    create_ai --> done["mark_task_done()"]
```

### Что скрейпится

| Данные | Количество | Источник |
|--------|-----------|----------|
| Профиль | 1 | `user_by_username_v2` |
| Посты + рилсы | до 25 | `user_medias_chunk_v1` |
| Рилсы | до 25 | `user_clips_chunk_v1` |
| Хайлайты | до 3 | `user_highlights`, `highlight_medias` |
| Комментарии | до 10 на 3 поста | `media_comments` |
| Аватар | 1 | URL из профиля |
| Thumbnails | до 7 | URL из постов |

---

## Pre-filter — быстрая проверка

```mermaid
flowchart TD
    start["pre_filter задача\n(payload: username)"] --> claim["Атомарный claim"]
    claim --> user_info["user_by_username_v2(username)"]

    user_info -->|UserNotFound / HikerAPI 404| nf["✗ filtered_out: not_found"]
    user_info -->|PrivateAccountError| pr["✗ filtered_out: private"]
    user_info -->|OK| check_private{is_private?}

    check_private -->|Да| pr2["✗ filtered_out: private\n+ platform_id, followers"]
    check_private -->|Нет| fetch_media["Параллельно:\nuser_medias_chunk_v1\nuser_clips_chunk_v1"]

    fetch_media -->|HikerAPI 404| nf2["✗ filtered_out: not_found"]
    fetch_media -->|OK| has_media{Есть посты/рилсы?}

    has_media -->|Нет| inactive["✗ filtered_out: inactive"]
    has_media -->|Да| check_date{Последний пост\n> 180 дней?}

    check_date -->|Да| inactive2["✗ filtered_out: inactive\n+ latest_post_at, counts"]
    check_date -->|Нет| check_likes{Лайки скрыты?\nlike_and_view_counts_disabled}

    check_likes -->|Да| pass["✓ Пропускаем фильтр\nпо engagement"]
    check_likes -->|Нет| check_avg{avg_likes < 30?}

    check_avg -->|Да| low_eng["✗ filtered_out: low_engagement\n+ avg_likes, followers"]
    check_avg -->|Нет| pass

    pass --> create["INSERT person + blog\nscrape_status='pending'"]
    create --> done["✓ mark_task_done()"]

    nf & pr & pr2 & nf2 & inactive & inactive2 & low_eng --> log["upsert pre_filter_log\n(username, reason, метрики)"]
    log --> done2["mark_task_done()\nerror_message='filtered_out: reason'"]
```

---

## AI-анализ — пайплайн

```mermaid
flowchart TD
    subgraph Сбор["1. Сбор батча"]
        trigger["full_scrape создал\nai_analysis задачу"]
        check{pending >= 10?\nили oldest > 2ч?}
        trigger --> check
        check -->|Нет| wait["Ждём ещё задач"]
        check -->|Да| load["Batch-загрузка профилей\n(3 запроса вместо N×3)"]
    end

    subgraph Submit["2. Отправка в OpenAI"]
        load --> build["Построить JSONL:\n- системный промпт (таксономия)\n- профиль + посты + хайлайты\n- аватар + thumbnails (мультимодально)"]
        build --> upload["Upload JSONL → OpenAI Files API"]
        upload --> batch["Create Batch\n(gpt-5-mini, 24ч deadline)"]
        batch --> save["Сохранить batch_id в payload задач"]
    end

    subgraph Poll["3. Поллинг (scheduler, каждые 15м)"]
        poll["poll_batch(batch_id)"]
        poll --> status{Статус?}
        status -->|in_progress| poll
        status -->|completed| download["Скачать результаты"]
        status -->|failed/cancelled| retry_all["Retry все задачи батча"]
        status -->|expired| retry_partial["Retry задачи без результата"]
    end

    subgraph Process["4. Обработка результатов"]
        download --> foreach["Для каждого блога:"]
        foreach --> parse["Парсинг AIInsights\n(structured output)"]
        parse --> save_insights["Сохранить ai_insights в blog"]
        save_insights --> categories["match_categories()\n→ blog_categories"]
        categories --> tags["match_tags()\n→ blog_tags"]
        tags --> city["match_city()\n→ blog_cities"]
        city --> embedding["generate_embedding()\ntext-embedding-3-small\n→ 1536-dim vector"]
        embedding --> done["mark_task_done()"]
    end
```

### Что анализирует AI

| Блок | Поля | Описание |
|------|------|----------|
| **Профиль** | page_type, profession, city, country, confidence | Тип страницы, профессия, геолокация |
| **Жизненная ситуация** | children, relationship, young_parent | Семейное положение |
| **Стиль жизни** | car, travel, pets, real_estate, lifestyle_level | Уровень жизни (1-5) |
| **Контент** | categories, subcategories, tags, language, tone, quality | Тематика, стиль, качество |
| **Аудитория** | gender_distribution, age_distribution, geo_distribution | Демография (%) |
| **Коммерция** | detected_brands, ambassador_brands, brand_safety | Бренды и безопасность |
| **Маркетинг** | best_fit_industries, not_suitable_for, collaboration_risk | Рекомендации для рекламодателей |

### Embedding — что кодируется в вектор

```
Краткое описание → категории → профессия → город →
теги → аудитория (пол/возраст/гео) → маркетинговая ценность →
качество engagement → brand safety → стиль жизни → риски
```

Используется для семантического поиска: `pgvector` в PostgreSQL, cosine similarity.

---

## Discover — поиск блогеров

```mermaid
flowchart LR
    start["discover задача\n(hashtag, min_followers)"] --> search["hashtag_medias_top\n(до 50 медиа)"]
    search --> extract["Извлечь уникальных авторов"]
    extract --> filter["Фильтр:\n- не приватный\n- followers >= min_followers\n- posts >= 5"]
    filter --> check["Batch-проверка:\nкакие уже есть в БД?"]
    check --> new["Для каждого нового:"]
    new --> insert["INSERT person + blog\nsource='hashtag_search'"]
    insert --> task["Создать full_scrape\n(приоритет 5)"]
```

---

## HTTP API

### Эндпоинты

| Метод | Путь | Auth | Описание | Ответ |
|-------|------|------|----------|-------|
| `GET` | `/api/health` | Нет | Healthcheck: аккаунты, счётчики задач | 200 |
| `GET` | `/api/tasks` | Да | Список задач (фильтры: status, task_type, limit, offset) | 200 |
| `GET` | `/api/tasks/{id}` | Да | Статус конкретной задачи | 200 / 404 |
| `POST` | `/api/tasks/scrape` | Да | Создать full_scrape (до 100 username) | 201 / 207 |
| `POST` | `/api/tasks/pre_filter` | Да | Создать pre_filter (до 100 username) | 201 / 207 |
| `POST` | `/api/tasks/discover` | Да | Создать discover по хештегу | 201 |
| `POST` | `/api/tasks/{id}/retry` | Да | Повторить failed задачу | 200 / 404 / 409 |

- **Auth**: `Authorization: Bearer <SCRAPER_API_KEY>` (constant-time сравнение)
- **Rate limit**: 60 req/мин per IP
- **207 Multi-Status**: Часть username обработана, часть с ошибками
- **Guard**: `is_blog_fresh()` — не создавать задачу если блог скрапили < 60 дней назад

---

## Scheduler — cron-задачи

| Задача | Расписание | Что делает |
|--------|-----------|------------|
| `poll_batches` | Каждые 15 мин | Проверяет статус OpenAI батчей, обрабатывает результаты |
| `recover_tasks` | Каждые 10 мин | Зависшие задачи (running > 30м / 2ч для AI) → pending |
| `retry_stale_batches` | Каждые 2 часа | Батчи > 4ч → retry (последняя мера после recover) |
| `retry_missing_embeddings` | Каждые 1 час | Генерация embedding для блогов без вектора |
| `retry_taxonomy_mappings` | Каждые 2 часа | Повторный матчинг категорий/тегов |
| `audit_taxonomy_drift` | Ежедневно 05:00 UTC | Аудит: промпт ↔ БД таксономия (расхождения → warning) |
| `schedule_updates` | Ежедневно 03:00 UTC | Re-scrape: блоги `active` + `scraped_at > 60д` → full_scrape (до 100, по followers DESC) |
| `cleanup_old_images` | Воскресенье 04:00 UTC | Удаление старых изображений из Storage |

---

## База данных

### Таблицы

```mermaid
erDiagram
    persons ||--o{ blogs : "has"
    blogs ||--o{ blog_posts : "has"
    blogs ||--o{ blog_highlights : "has"
    blogs ||--o{ blog_categories : "tagged"
    blogs ||--o{ blog_tags : "tagged"
    blogs ||--o{ scrape_tasks : "queued"
    categories ||--o{ blog_categories : "linked"
    tags ||--o{ blog_tags : "linked"

    scrape_tasks {
        uuid id PK
        uuid blog_id FK "nullable для pre_filter/discover"
        text task_type "full_scrape | ai_analysis | discover | pre_filter"
        text status "pending | running | done | failed"
        int priority "3-10 (ниже = важнее)"
        jsonb payload "username, hashtag, batch_id..."
        int attempts
        int max_attempts "default 3"
        timestamptz next_retry_at
    }

    blogs {
        uuid id PK
        uuid person_id FK
        text username
        text platform_id "Instagram pk"
        int follower_count
        text scrape_status "pending | scraping | analyzing | active | private | deleted"
        jsonb ai_insights "AIInsights от GPT"
        vector embedding "1536 dims, text-embedding-3-small"
        timestamptz scraped_at
    }

    blog_posts {
        uuid id PK
        uuid blog_id FK
        text platform_id "Instagram media pk"
        text caption_text
        int like_count
        int comment_count
        int play_count "для рилсов"
        int media_type "1=photo, 2=video, 8=carousel"
        timestamptz taken_at
    }

    pre_filter_log {
        uuid id PK
        text username "unique с reason"
        text reason "private | inactive | low_engagement | not_found"
        uuid task_id FK
        text platform_id
        int followers_count
        float avg_likes
        timestamptz latest_post_at
        int posts_count
        int clips_count
    }
```

### Ключевые индексы

| Индекс | Таблица | Назначение |
|--------|---------|-----------|
| `idx_scrape_tasks_active_pre_filter_username` | scrape_tasks | Дедупликация active pre_filter задач по username |
| `idx_pre_filter_log_username_reason` | pre_filter_log | Unique: один username+reason (upsert при повторном прогоне) |

---

## Обработка ошибок

### Стратегия retry

| Ошибка | Retry? | Backoff | Комментарий |
|--------|--------|---------|-------------|
| Сетевая ошибка (timeout, connection) | Да | 5м → 15м → 45м | Транзиентная, пройдёт |
| HikerAPI 429 (rate limit) | Да | 5м → 15м → 45м | Нужно подождать |
| HikerAPI 5xx (server error) | Да | 5м → 15м → 45м | Серверная проблема |
| AllAccountsCooldownError | Да | 5м → 15м → 45м | Все аккаунты в кулдауне |
| HikerAPI 402 (InsufficientBalance) | **Нет** | — | Баланс исчерпан |
| HikerAPI 404 (not found) | **Нет** | — | Аккаунт не существует |
| UserNotFound | **Нет** | — | Аккаунт не найден |
| PrivateAccountError | **Нет** | — | Приватный аккаунт |
| OpenAI batch failed | Да | Повтор батча | Весь батч ретраится |

### Graceful degradation

- Ошибка загрузки изображений → продолжить без фото
- Ошибка taxonomy matching → сохранить insights, залогировать
- Ошибка embedding → продолжить (поиск будет без этого блога)
- Ошибка одного блога в батче → не блокирует остальные

---

## Конфигурация

### Основные параметры

| Параметр | Default | Описание |
|----------|---------|----------|
| `WORKER_POLL_INTERVAL` | 30с | Интервал поллинга задач |
| `WORKER_MAX_CONCURRENT` | 2 | Максимум параллельных задач |
| `POSTS_TO_FETCH` | 25 | Постов/рилсов при скрейпе |
| `HIGHLIGHTS_TO_FETCH` | 3 | Хайлайтов при скрейпе |
| `THUMBNAILS_TO_PERSIST` | 7 | Thumbnails для сохранения |
| `BATCH_MIN_SIZE` | 10 | Мин. размер AI-батча |
| `BATCH_MODEL` | gpt-5-mini | Модель для AI-анализа |
| `PRE_FILTER_MIN_LIKES` | 30 | Порог avg likes |
| `PRE_FILTER_MAX_INACTIVE_DAYS` | 180 | Макс. дней неактивности |
| `PRE_FILTER_POSTS_TO_CHECK` | 5 | Постов для проверки engagement |
| `RESCRAPE_DAYS` | 60 | Дней до re-scrape |
| `SCRAPER_BACKEND` | instagrapi | `instagrapi` или `hikerapi` |

### HikerAPI запросы на 1 блогера

| Этап | Запросов | Endpoint |
|------|---------|----------|
| Pre-filter | 1-3 | user_info + medias + clips |
| Full scrape | 4-8 | user_info + medias + clips + highlights + comments |
| Discover | 1 + N | hashtag_medias + user_info × N |

---

## Полный путь блогера через систему

```mermaid
flowchart TD
    import["📥 Импорт из XLSX\nили API: POST /tasks/pre_filter"]
    import --> pf["🔍 Pre-filter\n(3 API-запроса)"]

    pf -->|"Приватный"| log_private["📋 pre_filter_log\nreason: private"]
    pf -->|"Не найден"| log_nf["📋 pre_filter_log\nreason: not_found"]
    pf -->|"Неактивный"| log_inactive["📋 pre_filter_log\nreason: inactive"]
    pf -->|"Мало лайков"| log_low["📋 pre_filter_log\nreason: low_engagement"]
    pf -->|"✅ Прошёл"| create["Создать person + blog\nscrape_status: pending"]

    create --> fs["📸 Full Scrape\n(4-8 API-запросов)\n25 постов, 3 хайлайта,\nфото, метрики"]
    fs --> analyze["blog.scrape_status: analyzing"]
    analyze --> ai["🤖 AI Analysis\n(OpenAI Batch API)\nПодождать батч (10+ блогов)"]
    ai --> insights["💡 Результат:\ncategories, tags, embedding,\nаудитория, бренды, маркетинг"]
    insights --> active["blog.scrape_status: active\n✅ Готов к поиску"]

    active -->|"Через 60 дней"| rescrape["♻️ Re-scrape\n(scheduler 03:00 UTC)"]
    rescrape --> fs
```
