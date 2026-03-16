# Backfill Tasks — автоматическое создание задач для необработанных блогов

## Проблема

В БД 34 958 блогов с `scrape_status = 'pending'` без единой `full_scrape` задачи (34К из xlsx-импорта, 957 manual). Также 85 блогов `ai_analyzed` без `ai_insights` — задачи `ai_analysis` завершились, но результаты потерялись.

Существующий `schedule_updates` обрабатывает только **re-scrape** уже проанализированных блогов (`ai_analyzed`, `active`). Первичный скрапинг и восстановление потерянного AI-анализа — ручной процесс.

## Решение

Две независимые cron-задачи в scheduler:

- **`backfill_scrape`** — создаёт `full_scrape` задачи для pending блогов без скрапинга
- **`backfill_ai_analysis`** — создаёт `ai_analysis` задачи для блогов без insights

Обе задачи настраиваемые через env vars, с приоритизацией по followers_count и защитой от перегрузки при исчерпании баланса API.

## Конфигурация (Settings)

```python
# Backfill: автоскрап pending блогов
backfill_scrape_enabled: bool = True
backfill_scrape_batch_size: int = 80        # блогов за запуск
backfill_scrape_interval_minutes: int = 30  # интервал между запусками

# Backfill: AI анализ для блогов без insights
backfill_ai_enabled: bool = True
backfill_ai_batch_size: int = 50
backfill_ai_interval_minutes: int = 60
```

Дефолты: 80 × 48 запусков/день = **3 840 блогов/день** для скрапинга. 35К → ~9 дней.

## RPC-функции (миграция)

Файл: `../platform/supabase/migrations/YYYYMMDDHHMMSS_backfill_rpc.sql`

### `backfill_pending_blogs(p_limit int)`

```sql
CREATE OR REPLACE FUNCTION backfill_pending_blogs(p_limit int)
RETURNS TABLE(id uuid) AS $$
  SELECT b.id
  FROM blogs b
  WHERE b.scrape_status = 'pending'
    AND b.scraped_at IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM scrape_tasks t
      WHERE t.blog_id = b.id
        AND t.task_type = 'full_scrape'
        AND t.status IN ('pending', 'running')
    )
  ORDER BY b.followers_count DESC NULLS LAST
  LIMIT p_limit;
$$ LANGUAGE sql STABLE;
```

### `backfill_unanalyzed_blogs(p_limit int)`

```sql
CREATE OR REPLACE FUNCTION backfill_unanalyzed_blogs(p_limit int)
RETURNS TABLE(id uuid) AS $$
  SELECT b.id
  FROM blogs b
  WHERE b.scrape_status = 'ai_analyzed'
    AND b.ai_insights IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM scrape_tasks t
      WHERE t.blog_id = b.id
        AND t.task_type = 'ai_analysis'
        AND t.status IN ('pending', 'running')
    )
  ORDER BY b.followers_count DESC NULLS LAST
  LIMIT p_limit;
$$ LANGUAGE sql STABLE;
```

## Защита от перегрузки при исчерпании баланса

Универсальный хелпер проверяет наличие недавних ошибок баланса:

```python
async def has_recent_balance_errors(
    db: AsyncClient,
    pattern: str,
    minutes: int = 30,
) -> bool:
    """Проверить наличие ошибок баланса API за последние N минут."""
    threshold = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
    result = await db.table("scrape_tasks") \
        .select("id", count="exact") \
        .eq("status", "failed") \
        .like("error_message", f"%{pattern}%") \
        .gt("completed_at", threshold) \
        .limit(1) \
        .execute()
    return bool(result.count and result.count > 0)
```

Вызов:
- `backfill_scrape` → `has_recent_balance_errors(db, "insufficient balance")`
- `backfill_ai_analysis` → `has_recent_balance_errors(db, "insufficient_quota")` + `has_recent_balance_errors(db, "billing_hard_limit")`

При обнаружении ошибок — пропуск с `logger.warning`.

## Приоритеты задач

| Источник | Приоритет | Описание |
|----------|-----------|----------|
| AI analysis | 2 | Обработка AI-результатов |
| API (ручной scrape) | 3 | Запросы через `/api/tasks/scrape` |
| Discover (full_scrape) | 5 | Из хештег-дискавери |
| **Backfill scrape** | **6** | Автоскрап pending блогов |
| Pre-filter | 8 | Фильтрация новых блогеров |
| Re-scrape (schedule_updates) | 8 | Обновление старых данных |

## Регистрация в scheduler

```python
if settings.backfill_scrape_enabled:
    sched.add_job(
        backfill_scrape, "interval",
        minutes=settings.backfill_scrape_interval_minutes,
        kwargs={"db": db, "settings": settings},
        id="backfill_scrape",
    )

if settings.backfill_ai_enabled:
    sched.add_job(
        backfill_ai_analysis, "interval",
        minutes=settings.backfill_ai_interval_minutes,
        kwargs={"db": db, "settings": settings},
        id="backfill_ai_analysis",
    )
```

## Обработка edge cases

- **Дублирование задач**: `create_task_if_not_exists` — атомарная RPC. Повторный запуск backfill безопасен.
- **Блоги deleted/private**: `handle_full_scrape` обновит `scrape_status` → backfill не подберёт повторно.
- **HikerAPI/OpenAI баланс**: хелпер `has_recent_balance_errors` приостанавливает создание.
- **Перегрузка очереди**: приоритет 6 — ручные задачи (3) обрабатываются первыми.

## Тесты

1. `test_backfill_scrape_creates_tasks` — RPC → 3 blog_id → 3 задачи
2. `test_backfill_scrape_empty` — RPC пустой → ничего не создано
3. `test_backfill_scrape_skips_on_balance_errors` — ошибки баланса → пропуск
4. `test_backfill_scrape_disabled` — `enabled=False` → не регистрируется
5. `test_backfill_ai_creates_tasks` — аналогично для AI
6. `test_backfill_ai_empty` — пустой список
7. `test_backfill_ai_skips_on_balance_errors` — пропуск при ошибках OpenAI
8. `test_has_recent_balance_errors_true` — есть ошибки → True
9. `test_has_recent_balance_errors_false` — нет → False

## Файлы для изменения

- `src/config.py` — 6 новых полей Settings
- `src/worker/scheduler.py` — 2 функции + хелпер + регистрация
- `../platform/supabase/migrations/` — 1 миграция с 2 RPC
- `tests/test_worker/test_scheduler.py` — 9 тестов
