# Pre-filter: массовая фильтрация блогеров из xlsx

## Задача

Импортировать ~69K блогеров из `blogs-base.xlsx` (TrendHERO), отфильтровать мусорные аккаунты через HikerAPI, прошедших записать в БД.

## Критерии фильтрации

1. **Приватный аккаунт** → отсеять
2. **Среднее лайков на 5 последних постов < 30** → отсеять
3. **Нет публикаций за последние 180 дней** → отсеять

## Архитектура

### Поток данных

```
xlsx → скрипт import_xlsx → POST /api/tasks/pre_filter (батчи по 100)
                                     ↓
                             scrape_tasks (task_type="pre_filter")
                                     ↓
                             worker poll → pre_filter_handler
                                     ↓
                           HikerAPI: user_info + 5 последних постов
                                     ↓
                   ┌─ прошёл → создать person + blog (scrape_status="pending")
                   └─ не прошёл → task completed, result={filtered_out: причина}
```

Один username = одна задача. ~69K задач в `scrape_tasks`.

### Эндпоинт

`POST /api/tasks/pre_filter`

```
Request:  {"usernames": ["user1", "user2", ...]}  # max 100
Response: {"created": N, "skipped": K, "errors": M, "tasks": [...]}
```

Логика аналогична `/api/tasks/scrape`:
- Валидация и дедупликация username-ов
- Проверка: блогер уже есть в `blogs` → skip
- Проверка: уже есть активная `pre_filter` задача на этот username → skip
- Создать задачу `task_type="pre_filter"`

### Handler: `pre_filter_handler.py`

```python
async def handle_pre_filter(task, scraper, ...):
    # 1. user_info = scraper.get_user_info(username)
    #    → приватный → filtered_out: "private"

    # 2. posts = scraper.get_recent_posts(user_id, count=5)
    #    → нет постов или последний > 180 дней → filtered_out: "inactive"
    #    → avg_likes < 30 → filtered_out: "low_engagement"

    # 3. Прошёл → create person + blog (как discover_handler)
    #    → task completed, result={"passed": true, "blog_id": ...}
```

Обработка ошибок:
- 402 → fail без retry
- 429/5xx → retry
- UserNotFound → filtered_out: "not_found"

### Скрипт: `src/scripts/import_xlsx.py`

```
uv run python -m src.scripts.import_xlsx blogs-base.xlsx
```

- Читает xlsx через pandas
- Дедупликация username-ов (~554 дубликата)
- Батчи по 100 → POST /api/tasks/pre_filter
- Прогресс-бар, итоговая статистика

Аргументы: `file`, `--batch-size` (100), `--base-url` (localhost:8001), `--delay` (0.1с)

## Изменения в коде

### Модифицируемые файлы

| Файл | Изменение |
|------|-----------|
| `src/config.py` | Параметры: `pre_filter_min_likes=30`, `pre_filter_max_inactive_days=180`, `pre_filter_posts_to_check=5` |
| `src/worker/scrape_handler.py` | Маршрутизация `task_type="pre_filter"` → `handle_pre_filter()` |
| `src/api/app.py` | Эндпоинт `POST /api/tasks/pre_filter` |
| `src/repositories/task_repository.py` | `pre_filter` в допустимых task_type |

### Новые файлы

| Файл | Описание |
|------|----------|
| `src/worker/pre_filter_handler.py` | Логика проверки 3 критериев |
| `src/scripts/import_xlsx.py` | Скрипт загрузки xlsx |

### Не трогаем

- HikerAPI клиент (используем существующие методы)
- Модели данных
- Существующие эндпоинты и handlers
- blog_repository (переиспользуем `find_or_create_blog()`)

## Оценка нагрузки

- ~69K профилей, 2 API-вызова на профиль = ~138K запросов к HikerAPI
- При `worker_max_concurrent=5-10` и ~1с на профиль = ~2-4 часа
- Рекомендация: поднять `worker_max_concurrent` на время импорта
