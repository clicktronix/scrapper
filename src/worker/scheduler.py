"""APScheduler cron-задачи для скрапера."""
import gc
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import openai
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
from openai import AsyncOpenAI
from postgrest.types import CountMethod
from supabase import AsyncClient

from src.ai.embedding import build_embedding_text, generate_embedding
from src.ai.schemas import AIInsights
from src.ai.taxonomy import CATEGORIES, TAGS
from src.ai.taxonomy_matching import (
    load_categories,
    load_tags,
    match_categories,
    match_tags,
    normalize_lookup_key,
)
from src.config import Settings
from src.database import (
    count_running_ai_tasks,
    create_task_if_not_exists,
    mark_task_failed,
    recover_stuck_tasks,
)
from src.image_storage import delete_blog_images
from src.worker.handlers import handle_batch_results

# Время последнего запуска каждой cron/interval-задачи (UTC ISO)
_last_run_at: dict[str, str] = {}


def record_job_run(job_id: str) -> None:
    """Записать текущее UTC-время как момент последнего запуска задачи."""
    _last_run_at[job_id] = datetime.now(UTC).isoformat()


def get_last_run_times() -> dict[str, str]:
    """Вернуть копию словаря последних запусков."""
    return dict(_last_run_at)


def _as_rows(data: Any) -> list[dict[str, Any]]:
    """Нормализовать result.data к списку dict-строк."""
    rows: list[dict[str, Any]] = []
    if not isinstance(data, list):
        return rows
    for item in cast(list[Any], data):
        if isinstance(item, dict):
            rows.append(cast(dict[str, Any], item))
    return rows


async def has_recent_balance_errors(
    db: AsyncClient,
    pattern: str,
    minutes: int = 30,
) -> bool:
    """Проверить наличие ошибок баланса API за последние N минут.

    Ищем по updated_at (триггер update_updated_at_column) без фильтра по status,
    т.к. HikerAPI 402 → status='failed' без completed_at,
    а OpenAI quota → status='pending' с откатом попыток.
    """
    threshold = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
    result = await (
        db.table("scrape_tasks")
        .select("id", count=CountMethod.exact)
        .like("error_message", f"%{pattern}%")
        .gt("updated_at", threshold)
        .limit(1)
        .execute()
    )
    return bool(result.count and result.count > 0)


async def backfill_scrape(db: AsyncClient, settings: Settings) -> None:
    """Создать full_scrape задачи для pending блогов без скрапинга."""
    record_job_run("backfill_scrape")

    # Throttle: каждый scrape создаёт ai_analysis задачу → не раздуваем AI-очередь
    running_ai = await count_running_ai_tasks(db)
    if running_ai >= settings.ai_queue_pause_threshold:
        logger.warning(
            f"[backfill_scrape] Пауза: AI-очередь перегружена "
            f"({running_ai} running >= {settings.ai_queue_pause_threshold})"
        )
        return

    if await has_recent_balance_errors(db, "insufficient balance"):
        logger.warning("[backfill_scrape] Пропуск: недавние ошибки баланса HikerAPI")
        return

    result = await db.rpc(
        "backfill_pending_blogs",
        {"p_limit": settings.backfill_scrape_batch_size},
    ).execute()

    blog_ids = [str(row.get("id", "")) for row in _as_rows(result.data) if row.get("id")]
    if not blog_ids:
        logger.debug("[backfill_scrape] Нет pending блогов для backfill")
        return

    created = 0
    for blog_id in blog_ids:
        try:
            task_id = await create_task_if_not_exists(db, blog_id, "full_scrape", priority=6)
            if task_id:
                created += 1
        except Exception as e:
            logger.error(f"[backfill_scrape] Ошибка создания задачи для blog {blog_id}: {e}")

    logger.info(f"[backfill_scrape] Создано {created} задач из {len(blog_ids)} pending блогов")


async def backfill_ai_analysis(db: AsyncClient, settings: Settings) -> None:
    """Создать ai_analysis задачи для блогов без insights."""
    record_job_run("backfill_ai_analysis")

    # Throttle: не создаём новые AI-задачи при перегрузке очереди
    running_ai = await count_running_ai_tasks(db)
    if running_ai >= settings.ai_queue_pause_threshold:
        logger.warning(
            f"[backfill_ai] Пауза: AI-очередь перегружена "
            f"({running_ai} running >= {settings.ai_queue_pause_threshold})"
        )
        return

    # Проверяем оба паттерна ошибок OpenAI
    if (
        await has_recent_balance_errors(db, "insufficient_quota")
        or await has_recent_balance_errors(db, "billing_hard_limit")
    ):
        logger.warning("[backfill_ai] Пропуск: недавние ошибки баланса OpenAI")
        return

    result = await db.rpc(
        "backfill_unanalyzed_blogs",
        {"p_limit": settings.backfill_ai_batch_size},
    ).execute()

    blog_ids = [str(row.get("id", "")) for row in _as_rows(result.data) if row.get("id")]
    if not blog_ids:
        logger.debug("[backfill_ai] Нет блогов без AI insights для backfill")
        return

    created = 0
    for blog_id in blog_ids:
        try:
            task_id = await create_task_if_not_exists(db, blog_id, "ai_analysis", priority=2)
            if task_id:
                created += 1
        except Exception as e:
            logger.error(f"[backfill_ai] Ошибка создания задачи для blog {blog_id}: {e}")

    logger.info(f"[backfill_ai] Создано {created} задач из {len(blog_ids)} блогов без insights")


async def schedule_updates(db: AsyncClient, settings: Settings) -> None:
    """Создать задачи re-scrape для блогов, где scraped_at > rescrape_days дней."""
    record_job_run("schedule_updates")
    threshold = (datetime.now(UTC) - timedelta(days=settings.rescrape_days)).isoformat()

    result = await db.table("blogs").select("id").in_(
        "scrape_status", ["active", "ai_analyzed"]
    ).or_(f"scraped_at.is.null,scraped_at.lt.{threshold}").order(
        "followers_count", desc=True
    ).limit(100).execute()

    created = 0
    for blog in _as_rows(result.data):
        blog_id = blog.get("id")
        if not isinstance(blog_id, str):
            continue
        try:
            task_id = await create_task_if_not_exists(
                db, blog_id, "full_scrape", priority=8
            )
            if task_id:
                created += 1
        except Exception as e:
            logger.error(f"[schedule_updates] Ошибка создания re-scrape для blog {blog_id}: {e}")

    logger.info(f"Scheduled {created} blog re-scrape tasks")


async def poll_batches(db: AsyncClient, openai_client: AsyncOpenAI) -> None:
    """Проверить статус running ai_analysis батчей."""
    record_job_run("poll_batches")
    logger.debug("[poll_batches] Checking running ai_analysis tasks...")
    result = await db.table("scrape_tasks").select(
        "id, blog_id, payload, attempts, max_attempts"
    ).eq("task_type", "ai_analysis").eq("status", "running").execute()

    if not result.data:
        logger.debug("[poll_batches] No running ai_analysis tasks")
        return

    # Группируем по batch_id
    # Значение:
    # {blog_id: {"id": task_id, "attempts": N, "max_attempts": M}}
    # или при коллизиях blog_id:
    # {blog_id: [ {"id": ...}, {"id": ...} ]}
    batches: dict[str, dict[str, Any]] = {}
    orphaned_task_ids: list[str] = []
    for task in _as_rows(result.data):
        payload = task.get("payload")
        payload_dict: dict[str, Any] = cast(dict[str, Any], payload) if isinstance(payload, dict) else {}
        batch_id_raw = payload_dict.get("batch_id")
        batch_id: str | None = batch_id_raw if isinstance(batch_id_raw, str) else None
        if not (isinstance(batch_id, str) and batch_id):
            orphaned_task_ids.append(str(task.get("id", "?")))
            continue

        # batch_id гарантированно str и непустой после проверки выше
        if batch_id not in batches:
            batches[batch_id] = {}
        task_info = {
            "id": str(task.get("id", "")),
            "attempts": int(task.get("attempts", 1) or 1),
            "max_attempts": int(task.get("max_attempts", 3) or 3),
        }
        blog_id = str(task.get("blog_id", ""))
        if not blog_id:
            continue
        existing = batches[batch_id].get(blog_id)
        if existing is None:
            batches[batch_id][blog_id] = task_info
        elif isinstance(existing, list):
            cast(list[Any], existing).append(task_info)
        else:
            batches[batch_id][blog_id] = [existing, task_info]

    if orphaned_task_ids:
        logger.warning(
            f"[poll_batches] {len(orphaned_task_ids)} running ai_analysis задач "
            f"без batch_id — сбрасываем в pending: {orphaned_task_ids[:10]}"
        )
        # Сбросить orphaned задачи в pending для повторной обработки.
        # batch_id потерян (connection error при сохранении) — задачи навсегда
        # застрянут в running, т.к. poll_batches не может найти их батч.
        for orphan_id in orphaned_task_ids:
            try:
                await db.table("scrape_tasks").update({
                    "status": "pending",
                    "error_message": "Reset: lost batch_id, no way to poll results",
                }).eq("id", orphan_id).execute()
            except Exception as e:
                logger.error(f"[poll_batches] Не удалось сбросить orphaned задачу {orphan_id}: {e}")

    logger.debug(f"[poll_batches] Found {len(batches)} active batches, "
                 f"{len(result.data)} running tasks")
    for batch_id, task_ids_by_blog in batches.items():
        logger.debug(f"[poll_batches] Processing batch {batch_id} "
                     f"({len(task_ids_by_blog)} blogs)")
        try:
            await handle_batch_results(db, openai_client, batch_id, task_ids_by_blog)
        except Exception as e:
            logger.exception(f"Error polling batch {batch_id}: {e}")
        finally:
            gc.collect()


async def retry_stale_batches(db: AsyncClient, openai_client: AsyncOpenAI, settings: Settings) -> None:
    """Пересобрать батчи, которые не завершились за 25 часов (последняя линия обороны).

    OpenAI Batch API гарантирует выполнение в пределах 24ч.
    25ч = 24ч окно + 1ч буфер на обработку результатов.
    """
    record_job_run("retry_stale_batches")
    logger.debug("[retry_stale_batches] Проверяем зависшие батчи...")
    threshold = (datetime.now(UTC) - timedelta(hours=25)).isoformat()

    result = await db.table("scrape_tasks").select(
        "id, blog_id, payload, attempts, max_attempts"
    ).eq("task_type", "ai_analysis").eq("status", "running").lt(
        "started_at", threshold
    ).execute()

    if not result.data:
        return

    retried = 0
    for task in _as_rows(result.data):
        task_id = task.get("id")
        if not isinstance(task_id, str):
            continue
        try:
            await mark_task_failed(
                db,
                task_id,
                int(task.get("attempts", 1) or 1),
                int(task.get("max_attempts", 3) or 3),
                "Batch not completed in 25h (exceeded OpenAI 24h window)", retry=True,
            )
            retried += 1
        except Exception as e:
            logger.error(f"[retry_stale_batches] Ошибка retry задачи {task_id}: {e}")

    if retried:
        logger.warning(f"Retried {retried} stale AI batch tasks")


async def cleanup_old_images(db: AsyncClient, settings: Settings) -> None:
    """Удалить изображения блогов с scraped_at > rescrape_days дней назад."""
    record_job_run("cleanup_old_images")
    threshold = (datetime.now(UTC) - timedelta(days=settings.rescrape_days)).isoformat()

    result = await db.table("blogs").select("id").lt(
        "scraped_at", threshold
    ).not_.in_("scrape_status", ["scraping", "pending"]).limit(100).execute()

    if not result.data:
        logger.debug("[cleanup_images] Нет старых блогов для очистки")
        return

    total_deleted = 0
    for blog in _as_rows(result.data):
        blog_id = blog.get("id")
        if not isinstance(blog_id, str):
            continue
        deleted = await delete_blog_images(db, blog_id)
        total_deleted += deleted

    logger.info(f"[cleanup_images] Удалено {total_deleted} изображений из {len(result.data)} блогов")


async def retry_missing_embeddings(
    db: AsyncClient, openai_client: AsyncOpenAI
) -> None:
    """Перегенерировать embedding для блогов с insights но без вектора."""
    record_job_run("retry_missing_embeddings")
    result = await db.table("blogs").select("id, ai_insights").not_.is_(
        "ai_insights", "null"
    ).is_("embedding", "null").neq("scrape_status", "ai_refused").limit(50).execute()
    if not result.data:
        return

    regenerated = 0
    failed = 0
    for blog in _as_rows(result.data):
        blog_id = blog.get("id")
        if not isinstance(blog_id, str):
            continue
        try:
            insights = AIInsights.model_validate(blog.get("ai_insights"))
            text = build_embedding_text(insights)
            if text is None:
                logger.warning(f"[retry_embedding] Blog {blog_id}: пустой текст для embedding, пропускаем")
                failed += 1
                continue
            vector = await generate_embedding(openai_client, text)
            if vector:
                await db.table("blogs").update({"embedding": vector}).eq("id", blog_id).execute()
                regenerated += 1
            else:
                failed += 1
        except openai.RateLimitError:
            logger.warning("[retry_embedding] Rate limited, stopping batch")
            break
        except Exception as e:
            failed += 1
            logger.error(f"[retry_embedding] Blog {blog_id}: {e}")

    if regenerated or failed:
        logger.info(
            f"[retry_embedding] Результат: {regenerated} успешно, {failed} ошибок "
            f"(из {len(result.data)} блогов без embedding)"
        )


async def retry_taxonomy_mappings(db: AsyncClient) -> None:
    """Повторить матчинг категорий/тегов по уже сохранённым ai_insights."""
    record_job_run("retry_taxonomy_mappings")
    result = await db.table("blogs").select("id, ai_insights").not_.is_(
        "ai_insights", "null"
    ).eq("scrape_status", "ai_analyzed").limit(50).execute()
    if not result.data:
        return

    # Исключаем блоги, у которых уже есть записи в blog_categories
    rows = _as_rows(result.data)
    blog_ids = [str(b.get("id", "")) for b in rows if str(b.get("id", ""))]
    existing_cats = await db.table("blog_categories").select("blog_id").in_(
        "blog_id", blog_ids
    ).execute()
    already_matched = {
        str(r.get("blog_id", ""))
        for r in _as_rows(existing_cats.data)
        if str(r.get("blog_id", ""))
    }

    categories_cache = await load_categories(db)
    tags_cache = await load_tags(db)

    processed = 0
    errors = 0
    for blog in rows:
        blog_id_raw = blog.get("id")
        if not isinstance(blog_id_raw, str):
            continue
        if blog_id_raw in already_matched:
            continue
        blog_id = blog_id_raw
        try:
            insights = AIInsights.model_validate(blog.get("ai_insights"))
            await match_categories(db, blog_id, insights, categories=categories_cache)
            await match_tags(db, blog_id, insights, tags=tags_cache)
            processed += 1
        except Exception as e:
            errors += 1
            logger.error(f"[retry_taxonomy] Blog {blog_id}: {e}")

    logger.info(f"[retry_taxonomy] Processed={processed}, errors={errors}")


async def audit_taxonomy_drift(db: AsyncClient) -> None:
    """Проверить расхождения taxonomy из prompt и справочников БД."""
    record_job_run("audit_taxonomy_drift")
    categories_cache = await load_categories(db)
    tags_cache = await load_tags(db)

    db_category_keys = set(categories_cache.keys())
    db_tag_keys = set(tags_cache.keys())

    expected_category_keys: set[str] = set()
    for category in CATEGORIES:
        expected_category_keys.add(normalize_lookup_key(category["code"]))
        expected_category_keys.add(normalize_lookup_key(category["name"]))
        for subcategory in category["subcategories"]:
            expected_category_keys.add(normalize_lookup_key(subcategory))

    expected_tag_keys: set[str] = set()
    for tags in TAGS.values():
        for tag in tags:
            expected_tag_keys.add(normalize_lookup_key(tag))

    missing_categories = sorted(expected_category_keys - db_category_keys)
    missing_tags = sorted(expected_tag_keys - db_tag_keys)

    if missing_categories:
        logger.warning(
            f"[taxonomy_audit] Missing categories in DB: {len(missing_categories)} "
            f"(examples={missing_categories[:10]})"
        )
    if missing_tags:
        logger.warning(
            f"[taxonomy_audit] Missing tags in DB: {len(missing_tags)} "
            f"(examples={missing_tags[:10]})"
        )
    if not missing_categories and not missing_tags:
        logger.info("[taxonomy_audit] Prompt taxonomy fully aligned with DB")


async def recover_tasks(db: AsyncClient) -> None:
    """Вернуть зависшие running задачи в pending (full_scrape, discover)."""
    record_job_run("recover_tasks")
    try:
        await recover_stuck_tasks(db, max_running_minutes=30)
    except Exception as e:
        logger.exception(f"[recover_tasks] Ошибка восстановления зависших задач: {e}")


def create_scheduler(
    db: AsyncClient,
    settings: Settings,
    openai_client: AsyncOpenAI | None = None,
) -> AsyncIOScheduler:
    """Создать и настроить APScheduler."""
    scheduler = AsyncIOScheduler(
        job_defaults={
            # Дефолтный misfire_grace_time=1с слишком мало для async job'ов —
            # при задержке event loop job'ы будут тихо пропускаться.
            # None = без ограничения (job всегда выполнится при опоздании).
            "misfire_grace_time": None,
            "coalesce": True,
        }
    )
    # APScheduler не имеет полных type stubs — add_job вызываем через Any
    sched: Any = scheduler

    # Ежедневно в 3:00 — обновление старых профилей
    sched.add_job(
        schedule_updates,
        "cron",
        hour=3,
        kwargs={"db": db, "settings": settings},
        id="schedule_updates",
    )

    # Каждые 15 минут — проверка батчей
    if openai_client:
        sched.add_job(
            poll_batches,
            "interval",
            minutes=15,
            kwargs={"db": db, "openai_client": openai_client},
            id="poll_batches",
        )

        # Каждые 2 часа — ретрай зависших батчей
        sched.add_job(
            retry_stale_batches,
            "interval",
            hours=2,
            kwargs={"db": db, "openai_client": openai_client, "settings": settings},
            id="retry_stale_batches",
        )

        # Каждый час — ретрай embedding для блогов без вектора
        sched.add_job(
            retry_missing_embeddings,
            "interval",
            hours=1,
            kwargs={"db": db, "openai_client": openai_client},
            id="retry_missing_embeddings",
        )

        # Каждые 2 часа — повторный матчинг taxonomy для ai_insights
        sched.add_job(
            retry_taxonomy_mappings,
            "interval",
            hours=2,
            kwargs={"db": db},
            id="retry_taxonomy_mappings",
        )

        # Ежедневный аудит расхождений taxonomy prompt ↔ DB
        sched.add_job(
            audit_taxonomy_drift,
            "cron",
            hour=5,
            kwargs={"db": db},
            id="audit_taxonomy_drift",
        )

    # Каждые 10 минут — recovery зависших running задач
    sched.add_job(
        recover_tasks,
        "interval",
        minutes=10,
        kwargs={"db": db},
        id="recover_tasks",
    )

    # Еженедельно в воскресенье 4:00 — очистка старых изображений
    sched.add_job(
        cleanup_old_images,
        "cron",
        day_of_week="sun",
        hour=4,
        kwargs={"db": db, "settings": settings},
        id="cleanup_old_images",
    )

    # Backfill: автоскрап pending блогов
    if settings.backfill_scrape_enabled:
        sched.add_job(
            backfill_scrape,
            "interval",
            minutes=settings.backfill_scrape_interval_minutes,
            kwargs={"db": db, "settings": settings},
            id="backfill_scrape",
        )

    # Backfill: AI анализ для блогов без insights
    if settings.backfill_ai_enabled:
        sched.add_job(
            backfill_ai_analysis,
            "interval",
            minutes=settings.backfill_ai_interval_minutes,
            kwargs={"db": db, "settings": settings},
            id="backfill_ai_analysis",
        )

    return scheduler
