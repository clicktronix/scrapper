"""APScheduler cron-задачи для скрапера."""
from datetime import UTC, datetime, timedelta
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
from openai import AsyncOpenAI
from supabase import Client

from src.ai.batch import (
    is_valid_city,
    load_categories,
    load_cities,
    load_tags,
    match_categories,
    match_city,
    match_tags,
    normalize_lookup_key,
)
from src.ai.embedding import build_embedding_text, generate_embedding
from src.ai.schemas import AIInsights
from src.ai.taxonomy import CATEGORIES, TAGS
from src.config import Settings
from src.database import (
    create_task_if_not_exists,
    mark_task_failed,
    recover_stuck_tasks,
    run_in_thread,
)
from src.image_storage import delete_blog_images
from src.worker.handlers import handle_batch_results


async def schedule_updates(db: Client, settings: Settings) -> None:
    """Создать задачи re-scrape для блогов, где scraped_at > rescrape_days дней."""
    threshold = (datetime.now(UTC) - timedelta(days=settings.rescrape_days)).isoformat()

    result = await run_in_thread(
        db.table("blogs")
        .select("id")
        .in_("scrape_status", ["active", "ai_analyzed"])
        .or_(f"scraped_at.is.null,scraped_at.lt.{threshold}")
        .order("followers_count", desc=True)
        .limit(100)
        .execute
    )

    created = 0
    for blog in result.data:
        task_id = await create_task_if_not_exists(
            db, blog["id"], "full_scrape", priority=8
        )
        if task_id:
            created += 1

    logger.info(f"Scheduled {created} blog re-scrape tasks")


async def poll_batches(db: Client, openai_client: AsyncOpenAI) -> None:
    """Проверить статус running ai_analysis батчей."""
    logger.debug("[poll_batches] Checking running ai_analysis tasks...")
    result = await run_in_thread(
        db.table("scrape_tasks")
        .select("id, blog_id, payload, attempts, max_attempts")
        .eq("task_type", "ai_analysis")
        .eq("status", "running")
        .execute
    )

    if not result.data:
        logger.debug("[poll_batches] No running ai_analysis tasks")
        return

    # Группируем по batch_id
    # Значение:
    # {blog_id: {"id": task_id, "attempts": N, "max_attempts": M}}
    # или при коллизиях blog_id:
    # {blog_id: [ {"id": ...}, {"id": ...} ]}
    batches: dict[str, dict[str, Any]] = {}
    for task in result.data:
        batch_id = (task.get("payload") or {}).get("batch_id")
        if batch_id:
            if batch_id not in batches:
                batches[batch_id] = {}
            task_info = {
                "id": task["id"],
                "attempts": task.get("attempts", 1),
                "max_attempts": task.get("max_attempts", 3),
            }
            blog_id = task["blog_id"]
            existing = batches[batch_id].get(blog_id)
            if existing is None:
                batches[batch_id][blog_id] = task_info
            elif isinstance(existing, list):
                existing.append(task_info)
            else:
                batches[batch_id][blog_id] = [existing, task_info]

    logger.debug(f"[poll_batches] Found {len(batches)} active batches, "
                 f"{len(result.data)} running tasks")
    for batch_id, task_ids_by_blog in batches.items():
        logger.debug(f"[poll_batches] Processing batch {batch_id} "
                     f"({len(task_ids_by_blog)} blogs)")
        try:
            await handle_batch_results(db, openai_client, batch_id, task_ids_by_blog)
        except Exception as e:
            logger.error(f"Error polling batch {batch_id}: {e}")


async def retry_stale_batches(db: Client, openai_client: AsyncOpenAI, settings: Settings) -> None:
    """Пересобрать батчи, которые не завершились за 4 часа (последняя линия обороны)."""
    threshold = (datetime.now(UTC) - timedelta(hours=4)).isoformat()

    result = await run_in_thread(
        db.table("scrape_tasks")
        .select("id, blog_id, payload, attempts, max_attempts")
        .eq("task_type", "ai_analysis")
        .eq("status", "running")
        .lt("started_at", threshold)
        .execute
    )

    if not result.data:
        return

    for task in result.data:
        await mark_task_failed(
            db, task["id"], task["attempts"], task["max_attempts"],
            "Batch not completed in 4h", retry=True,
        )

    logger.warning(f"Retried {len(result.data)} stale AI batch tasks")


async def cleanup_old_images(db: Client, settings: Settings) -> None:
    """Удалить изображения блогов с scraped_at > rescrape_days дней назад."""
    threshold = (datetime.now(UTC) - timedelta(days=settings.rescrape_days)).isoformat()

    result = await run_in_thread(
        db.table("blogs")
        .select("id")
        .lt("scraped_at", threshold)
        .limit(100)
        .execute
    )

    if not result.data:
        logger.debug("[cleanup_images] Нет старых блогов для очистки")
        return

    total_deleted = 0
    for blog in result.data:
        deleted = await delete_blog_images(db, blog["id"])
        total_deleted += deleted

    logger.info(f"[cleanup_images] Удалено {total_deleted} изображений из {len(result.data)} блогов")


async def retry_missing_embeddings(
    db: Client, openai_client: AsyncOpenAI
) -> None:
    """Перегенерировать embedding для блогов с insights но без вектора."""
    result = await run_in_thread(
        db.table("blogs")
        .select("id, ai_insights")
        .not_.is_("ai_insights", "null")
        .is_("embedding", "null")
        .neq("scrape_status", "ai_refused")
        .limit(50)
        .execute
    )
    if not result.data:
        return

    regenerated = 0
    for blog in result.data:
        try:
            insights = AIInsights.model_validate(blog["ai_insights"])
            text = build_embedding_text(insights)
            vector = await generate_embedding(openai_client, text)
            if vector:
                await run_in_thread(
                    db.table("blogs").update({"embedding": vector}).eq("id", blog["id"]).execute
                )
                regenerated += 1
        except Exception as e:
            logger.error(f"[retry_embedding] Blog {blog['id']}: {e}")

    if regenerated:
        logger.info(f"[retry_embedding] Перегенерировано {regenerated} embedding'ов")


async def retry_taxonomy_mappings(db: Client) -> None:
    """Повторить матчинг категорий/тегов по уже сохранённым ai_insights."""
    result = await run_in_thread(
        db.table("blogs")
        .select("id, ai_insights")
        .not_.is_("ai_insights", "null")
        .eq("scrape_status", "ai_analyzed")
        .limit(50)
        .execute
    )
    if not result.data:
        return

    categories_cache = await load_categories(db)
    tags_cache = await load_tags(db)

    processed = 0
    errors = 0
    for blog in result.data:
        blog_id = blog["id"]
        try:
            insights = AIInsights.model_validate(blog["ai_insights"])
            await match_categories(db, blog_id, insights, categories=categories_cache)
            await match_tags(db, blog_id, insights, tags=tags_cache)
            processed += 1
        except Exception as e:
            errors += 1
            logger.error(f"[retry_taxonomy] Blog {blog_id}: {e}")

    logger.info(f"[retry_taxonomy] Processed={processed}, errors={errors}")


async def audit_taxonomy_drift(db: Client) -> None:
    """Проверить расхождения taxonomy из prompt и справочников БД."""
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


async def backfill_city_matching(db: Client) -> None:
    """Сопоставить города для блогов у которых есть city но нет blog_cities."""
    # Найти блоги с AI-анализом, у которых city заполнен
    blogs = await run_in_thread(
        db.table("blogs")
        .select("id, ai_insights")
        .eq("scrape_status", "ai_analyzed")
        .not_.is_("ai_insights", "null")
        .limit(500)
        .execute
    )
    if not blogs.data:
        return

    # Проверить какие уже имеют blog_cities
    blog_ids = [b["id"] for b in blogs.data]
    existing = await run_in_thread(
        db.table("blog_cities").select("blog_id").in_("blog_id", blog_ids).execute
    )
    has_city = {r["blog_id"] for r in existing.data}

    cities_cache = await load_cities(db)
    matched = 0
    for blog in blogs.data:
        if blog["id"] in has_city:
            continue
        ai = blog.get("ai_insights") or {}
        bp = ai.get("blogger_profile") or {}
        city_name = bp.get("city")
        if not city_name or not is_valid_city(city_name):
            continue
        try:
            ok = await match_city(db, blog["id"], city_name, cities_cache)
            if ok:
                matched += 1
        except Exception as e:
            logger.error(f"[backfill_city] Blog {blog['id']}: {e}")

    if matched:
        logger.info(f"[backfill_city] Matched {matched} cities from {len(blogs.data)} blogs")


async def recover_tasks(db: Client) -> None:
    """Вернуть зависшие running задачи в pending (full_scrape, discover)."""
    await recover_stuck_tasks(db, max_running_minutes=30)


def create_scheduler(
    db: Client,
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

    # Ежедневно в 3:00 — обновление старых профилей
    scheduler.add_job(
        schedule_updates,
        "cron",
        hour=3,
        kwargs={"db": db, "settings": settings},
        id="schedule_updates",
    )

    # Каждые 15 минут — проверка батчей
    if openai_client:
        scheduler.add_job(
            poll_batches,
            "interval",
            minutes=15,
            kwargs={"db": db, "openai_client": openai_client},
            id="poll_batches",
        )

        # Каждые 2 часа — ретрай зависших батчей
        scheduler.add_job(
            retry_stale_batches,
            "interval",
            hours=2,
            kwargs={"db": db, "openai_client": openai_client, "settings": settings},
            id="retry_stale_batches",
        )

        # Каждый час — ретрай embedding для блогов без вектора
        scheduler.add_job(
            retry_missing_embeddings,
            "interval",
            hours=1,
            kwargs={"db": db, "openai_client": openai_client},
            id="retry_missing_embeddings",
        )

        # Каждые 2 часа — повторный матчинг taxonomy для ai_insights
        scheduler.add_job(
            retry_taxonomy_mappings,
            "interval",
            hours=2,
            kwargs={"db": db},
            id="retry_taxonomy_mappings",
        )

        # Ежедневный аудит расхождений taxonomy prompt ↔ DB
        scheduler.add_job(
            audit_taxonomy_drift,
            "cron",
            hour=5,
            kwargs={"db": db},
            id="audit_taxonomy_drift",
        )

    # Каждые 10 минут — recovery зависших running задач
    scheduler.add_job(
        recover_tasks,
        "interval",
        minutes=10,
        kwargs={"db": db},
        id="recover_tasks",
    )

    # Однократный запуск — бэкфилл города для существующих блогов
    scheduler.add_job(
        backfill_city_matching,
        "date",
        kwargs={"db": db},
        id="backfill_city_matching",
    )

    # Еженедельно в воскресенье 4:00 — очистка старых изображений
    scheduler.add_job(
        cleanup_old_images,
        "cron",
        day_of_week="sun",
        hour=4,
        kwargs={"db": db, "settings": settings},
        id="cleanup_old_images",
    )

    return scheduler
