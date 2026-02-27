"""Обработчики задач воркера — scrape, analyze, discover."""
import asyncio
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from instagrapi.exceptions import UserNotFound
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
    normalize_brand,
    poll_batch,
    submit_batch,
)
from src.ai.embedding import build_embedding_text, generate_embedding
from src.ai.schemas import AIInsights
from src.config import Settings
from src.database import (
    cleanup_orphan_person,
    create_task_if_not_exists,
    is_blog_fresh,
    mark_task_done,
    mark_task_failed,
    mark_task_running,
    run_in_thread,
    sanitize_error,
    upsert_blog,
    upsert_highlights,
    upsert_posts,
)
from src.image_storage import persist_profile_images
from src.models.blog import ScrapedComment, ScrapedHighlight, ScrapedPost, ScrapedProfile
from src.platforms.base import BaseScraper
from src.platforms.instagram.exceptions import (
    AllAccountsCooldownError,
    HikerAPIError,
    InsufficientBalanceError,
    PrivateAccountError,
)

# Маппинг discrete confidence (1-5) → float для БД (DECIMAL(3,2))
_CONFIDENCE_TO_FLOAT: dict[int, float] = {1: 0.20, 2: 0.40, 3: 0.60, 4: 0.80, 5: 1.00}
_ENRICHMENT_RETRY_ATTEMPTS = 3
_ENRICHMENT_RETRY_DELAY_SECONDS = 0.2


def _normalize_username(username: str) -> str:
    """Normalize Instagram username for stable deduplication."""
    return username.strip().lstrip("@").lower()


def _extract_blog_fields(insights: AIInsights) -> dict[str, Any]:
    """Извлечь поля блога из AI-анализа для заполнения основных колонок."""
    fields: dict[str, Any] = {}

    # Город (фильтруем мусор: "14% Казахстан", названия стран и т.д.)
    if insights.blogger_profile.city and is_valid_city(insights.blogger_profile.city):
        fields["city"] = insights.blogger_profile.city

    # Язык контента
    if insights.content.content_language:
        fields["content_language"] = ", ".join(insights.content.content_language)

    # Аудитория по полу (проценты)
    ai = insights.audience_inference
    if ai.audience_male_pct is not None and ai.audience_female_pct is not None:
        fields["audience_gender"] = {
            "male": ai.audience_male_pct,
            "female": ai.audience_female_pct,
            "other": ai.audience_other_pct or 0,
        }

    # Возрастное распределение аудитории
    age_data: dict[str, float] = {}
    for key, val in [
        ("13-17", ai.audience_age_13_17_pct),
        ("18-24", ai.audience_age_18_24_pct),
        ("25-34", ai.audience_age_25_34_pct),
        ("35-44", ai.audience_age_35_44_pct),
        ("45+", ai.audience_age_45_plus_pct),
    ]:
        if val is not None and val > 0:
            age_data[key] = val
    if age_data:
        fields["audience_age"] = age_data

    # Географическое распределение аудитории
    country_data: dict[str, float] = {}
    for code, val in [
        ("KZ", ai.audience_kz_pct),
        ("RU", ai.audience_ru_pct),
        ("UZ", ai.audience_uz_pct),
        ("OTHER", ai.audience_other_geo_pct),
    ]:
        if val is not None and val > 0:
            country_data[code] = val
    if country_data:
        fields["audience_countries"] = country_data

    return fields


def _parse_top_comments(raw_value: Any) -> list[ScrapedComment]:
    """Преобразовать сырой JSON из blog_posts.top_comments в типизированный список."""
    if not isinstance(raw_value, list):
        return []

    comments: list[ScrapedComment] = []
    for item in raw_value:
        if not isinstance(item, dict):
            continue
        username = item.get("username")
        text = item.get("text")
        if not isinstance(username, str) or not isinstance(text, str):
            continue
        username = username.strip()
        text = text.strip()
        if not username or not text:
            continue
        comments.append(ScrapedComment(username=username, text=text))
    return comments


async def handle_full_scrape(
    db: Client,
    task: dict[str, Any],
    scraper: BaseScraper,
    settings: Settings,
) -> None:
    """
    Полный скрапинг профиля.
    1. mark_task_running
    2. scrape_status = 'scraping'
    3. scrape_profile(username)
    4. upsert_blog, upsert_posts, upsert_highlights
    5. Создать задачу ai_analysis
    6. mark_task_done
    """
    task_id = task["id"]
    blog_id = task["blog_id"]
    logger.debug(f"[full_scrape] Starting task={task_id}, blog={blog_id}")

    was_claimed = await mark_task_running(db, task_id)
    if not was_claimed:
        logger.debug(f"Task {task_id} was already claimed by another worker")
        return
    # RPC атомарно инкрементирует attempts, используем актуальное значение
    current_attempts = task["attempts"] + 1

    # Получаем username из blogs
    blog_result = await run_in_thread(
        db.table("blogs")
        .select("username, person_id, scrape_status")
        .eq("id", blog_id).execute
    )
    if not blog_result.data:
        await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                               "Blog not found", retry=False)
        return

    username = blog_result.data[0]["username"]
    person_id = blog_result.data[0].get("person_id")
    scrape_status = blog_result.data[0].get("scrape_status")

    # Блог деактивирован/удалён — не скрапить
    if scrape_status in ("deleted", "deactivated"):
        logger.info(f"[full_scrape] Пропуск @{username}: статус {scrape_status}")
        await mark_task_done(db, task_id)
        return

    logger.debug(f"[full_scrape] Scraping @{username} (blog={blog_id})")

    # Обновить scrape_status
    await run_in_thread(
        db.table("blogs").update({"scrape_status": "scraping"}).eq("id", blog_id).execute
    )

    try:
        profile = await scraper.scrape_profile(username)
    except PrivateAccountError:
        await run_in_thread(
            db.table("blogs")
            .update({"scrape_status": "private", "needs_review": True})
            .eq("id", blog_id).execute
        )
        await mark_task_done(db, task_id)
        return
    except UserNotFound:
        # Пользователь удалён / не найден — без retry, нужна ручная проверка
        await run_in_thread(
            db.table("blogs")
            .update({"scrape_status": "deleted", "needs_review": True})
            .eq("id", blog_id).execute
        )
        await mark_task_done(db, task_id)
        return
    except InsufficientBalanceError as e:
        # Нет денег на HikerAPI — ретрай бесполезен, не трогаем scrape_status блога
        logger.error(f"[full_scrape] HikerAPI баланс исчерпан: {e}")
        await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                               "HikerAPI: insufficient balance", retry=False)
        return
    except HikerAPIError as e:
        # HTTP 429 (rate limit) / 5xx → retry, остальные 4xx → failed + needs_review
        retry = e.status_code in (429, 500, 502, 503, 504)
        update_data: dict[str, Any] = {
            "scrape_status": "pending" if retry else "failed",
        }
        if not retry:
            update_data["needs_review"] = True
        await run_in_thread(
            db.table("blogs").update(update_data).eq("id", blog_id).execute
        )
        await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                               sanitize_error(str(e)), retry=retry)
        return
    except AllAccountsCooldownError as e:
        await run_in_thread(
            db.table("blogs").update({"scrape_status": "pending"}).eq("id", blog_id).execute
        )
        await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                               str(e), retry=True)
        return
    except Exception as e:
        await run_in_thread(
            db.table("blogs").update({"scrape_status": "failed"}).eq("id", blog_id).execute
        )
        await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                               sanitize_error(str(e)), retry=True)
        return

    # Средние просмотры рилсов
    reels_views = [
        m.play_count for m in profile.medias
        if m.play_count is not None and m.product_type == "clips"
    ]
    avg_reels_views = int(sum(reels_views) / len(reels_views)) if reels_views else None

    # Сохранить данные
    logger.debug(f"[full_scrape] @{username}: scraped "
                 f"{len(profile.medias)} publications, "
                 f"{len(profile.highlights)} highlights, "
                 f"followers={profile.follower_count}")
    blog_data: dict[str, Any] = {
        "platform_id": profile.platform_id,
        "bio": profile.biography,
        "followers_count": profile.follower_count,
        "following_count": profile.following_count,
        "media_count": profile.media_count,
        "is_verified": profile.is_verified,
        "is_business": profile.is_business,
        "engagement_rate": profile.avg_er,
        "er_reels": profile.avg_er_reels,
        "er_trend": profile.er_trend,
        "posts_per_week": profile.posts_per_week,
        "avg_reels_views": avg_reels_views,
        "scrape_status": "analyzing",
        "scraped_at": datetime.now(UTC).isoformat(),
        "bio_links": profile.bio_links,
    }
    if profile.profile_pic_url:
        blog_data["avatar_url"] = profile.profile_pic_url
    if profile.business_category:
        blog_data["business_category"] = profile.business_category
    if profile.account_type is not None:
        blog_data["account_type"] = profile.account_type
    if profile.public_email:
        blog_data["public_email"] = profile.public_email
    if profile.contact_phone_number:
        blog_data["contact_phone_number"] = profile.contact_phone_number
    if profile.public_phone_country_code:
        blog_data["public_phone_country_code"] = profile.public_phone_country_code
    if profile.city_name:
        blog_data["city_name"] = profile.city_name
    if profile.address_street:
        blog_data["address_street"] = profile.address_street

    # Upsert посты и хайлайты (mode="json" для корректной сериализации datetime)
    posts_data = [p.model_dump(mode="json") for p in profile.medias]
    highlights_data = [
        h.model_dump(mode="json") for h in profile.highlights
    ]

    # Скачать CDN-изображения → загрузить в Supabase Storage → подставить постоянные URL
    try:
        avatar_storage_url, post_urls = await persist_profile_images(
            db, settings.supabase_url, blog_id,
            profile.profile_pic_url, posts_data,
        )
        if avatar_storage_url:
            blog_data["avatar_url"] = avatar_storage_url
        for post in posts_data:
            pid = post.get("platform_id", "")
            if pid in post_urls:
                post["thumbnail_url"] = post_urls[pid]
    except Exception as e:
        # Ошибка загрузки изображений не блокирует скрапинг — CDN URL останутся
        logger.warning(f"[full_scrape] @{username}: ошибка загрузки изображений в Storage: {e}")

    try:
        logger.debug(f"[full_scrape] @{username}: upserting blog data...")
        await upsert_blog(db, blog_id, blog_data)

        # Обновить full_name в persons
        if person_id and profile.full_name:
            await run_in_thread(
                db.table("persons").update({"full_name": profile.full_name}).eq("id", person_id).execute
            )

        await upsert_posts(db, blog_id, posts_data)
        logger.debug(f"[full_scrape] @{username}: upserted {len(posts_data)} posts/reels")

        await upsert_highlights(db, blog_id, highlights_data)
        logger.debug(f"[full_scrape] @{username}: upserted {len(highlights_data)} highlights")
    except Exception as e:
        await run_in_thread(
            db.table("blogs").update({"scrape_status": "failed"}).eq("id", blog_id).execute
        )
        await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                               sanitize_error(str(e)), retry=True)
        return

    # Создать задачу AI-анализа
    logger.debug(f"[full_scrape] @{username}: creating ai_analysis task...")
    await create_task_if_not_exists(db, blog_id, "ai_analysis", priority=3)

    await mark_task_done(db, task_id)
    logger.info(f"Full scrape done for @{username} (blog={blog_id})")


async def _load_profiles_for_batch(
    db: Client,
    pending_tasks: list[dict[str, Any]],
) -> tuple[list[tuple[str, ScrapedProfile]], list[str], list[str]]:
    """
    Батчевая загрузка профилей для AI-анализа.
    Возвращает (profiles, task_ids, failed_task_ids).
    """
    blog_ids = [t["blog_id"] for t in pending_tasks if t.get("blog_id")]
    if not blog_ids:
        return [], [], []

    # Батчевая загрузка всех данных (3 запроса вместо N*3)
    blogs_result = await run_in_thread(
        db.table("blogs").select("*").in_("id", blog_ids).execute
    )
    posts_result = await run_in_thread(
        db.table("blog_posts")
        .select("*")
        .in_("blog_id", blog_ids)
        .order("taken_at", desc=True)
        .execute
    )
    highlights_result = await run_in_thread(
        db.table("blog_highlights")
        .select("*")
        .in_("blog_id", blog_ids)
        .execute
    )

    # Индексация по blog_id
    blogs_by_id: dict[str, dict[str, Any]] = {b["id"]: b for b in blogs_result.data}
    posts_by_blog: dict[str, list[dict[str, Any]]] = {}
    for p in posts_result.data:
        posts_by_blog.setdefault(p["blog_id"], []).append(p)
    highlights_by_blog: dict[str, list[dict[str, Any]]] = {}
    for h in highlights_result.data:
        highlights_by_blog.setdefault(h["blog_id"], []).append(h)

    profiles: list[tuple[str, ScrapedProfile]] = []
    task_ids: list[str] = []
    failed_task_ids: list[str] = []

    for pt in pending_tasks:
        blog_id = pt["blog_id"]
        blog = blogs_by_id.get(blog_id)

        if not blog:
            await mark_task_failed(
                db, pt["id"], pt.get("attempts", 0), pt.get("max_attempts", 3),
                f"Blog {blog_id} not found in database", retry=False,
            )
            failed_task_ids.append(pt["id"])
            continue

        raw_posts = posts_by_blog.get(blog_id, [])[:25]
        raw_highlights = highlights_by_blog.get(blog_id, [])

        # Сборка ScrapedProfile — все публикации в один список
        medias: list[ScrapedPost] = []
        for p in raw_posts:
            taken_at_raw = p.get("taken_at")
            if not taken_at_raw or not p.get("platform_id"):
                continue
            try:
                taken_at = datetime.fromisoformat(taken_at_raw.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue

            post = ScrapedPost(
                platform_id=p["platform_id"],
                media_type=p.get("media_type", 1),
                product_type=p.get("product_type"),
                caption_text=p.get("caption_text", ""),
                hashtags=p.get("hashtags", []),
                mentions=p.get("mentions", []),
                like_count=p.get("like_count", 0),
                comment_count=p.get("comment_count", 0),
                play_count=p.get("play_count"),
                thumbnail_url=p.get("thumbnail_url"),
                taken_at=taken_at,
                video_duration=p.get("video_duration"),
                usertags=p.get("usertags", []),
                accessibility_caption=p.get("accessibility_caption"),
                comments_disabled=p.get("comments_disabled", False),
                top_comments=_parse_top_comments(p.get("top_comments")),
                title=p.get("title"),
                carousel_media_count=p.get("carousel_media_count"),
            )
            medias.append(post)

        highlights: list[ScrapedHighlight] = []
        for h in raw_highlights:
            if not h.get("platform_id") or not h.get("title"):
                continue
            highlights.append(ScrapedHighlight(
                platform_id=h["platform_id"],
                title=h["title"],
                media_count=h.get("media_count", 0),
                story_mentions=h.get("story_mentions", []),
                story_locations=h.get("story_locations", []),
                story_links=h.get("story_links", []),
                story_sponsor_tags=h.get("story_sponsor_tags", []),
                has_paid_partnership=h.get("has_paid_partnership", False),
                story_hashtags=h.get("story_hashtags", []),
            ))

        # Нормализация bio_links: обратная совместимость со старым форматом ["url"]
        raw_bio_links = blog.get("bio_links", []) or []
        bio_links: list[dict[str, str | None]] = []
        for item in raw_bio_links:
            if isinstance(item, str):
                bio_links.append({"url": item, "title": None, "link_type": None})
            elif isinstance(item, dict):
                bio_links.append(item)

        profile = ScrapedProfile(
            platform_id=blog.get("platform_id", ""),
            username=blog.get("username", ""),
            full_name="",
            biography=blog.get("bio") or "",
            bio_links=bio_links,
            follower_count=blog.get("followers_count", 0),
            following_count=blog.get("following_count", 0),
            media_count=blog.get("media_count", 0),
            is_verified=blog.get("is_verified", False),
            is_business=blog.get("is_business", False),
            account_type=blog.get("account_type"),
            public_email=blog.get("public_email"),
            contact_phone_number=blog.get("contact_phone_number"),
            public_phone_country_code=blog.get("public_phone_country_code"),
            city_name=blog.get("city_name"),
            address_street=blog.get("address_street"),
            profile_pic_url=blog.get("avatar_url"),
            medias=medias,
            highlights=highlights,
        )

        profiles.append((blog_id, profile))
        task_ids.append(pt["id"])

    return profiles, task_ids, failed_task_ids


async def handle_ai_analysis(
    db: Client,
    task: dict[str, Any],
    openai_client: AsyncOpenAI,
    settings: Settings,
) -> None:
    """
    AI-анализ через Batch API.
    Собирает pending ai_analysis задачи, при достижении порога отправляет батч.
    """
    # Считаем pending ai_analysis задачи
    logger.debug("[ai_analysis] Checking pending ai_analysis tasks...")
    pending_result = await run_in_thread(
        db.table("scrape_tasks")
        .select("id, blog_id, created_at, attempts, max_attempts, payload")
        .eq("task_type", "ai_analysis")
        .eq("status", "pending")
        .order("created_at", desc=False)
        .limit(100)
        .execute
    )
    pending_tasks = pending_result.data

    if not pending_tasks:
        logger.debug("[ai_analysis] No pending tasks")
        return

    # Проверяем: набрался батч или старейшая задача > 2ч?
    created_dates = [t["created_at"] for t in pending_tasks if t.get("created_at")]
    if created_dates:
        oldest_created = min(created_dates)
        oldest_dt = datetime.fromisoformat(oldest_created.replace("Z", "+00:00"))
        time_threshold = datetime.now(UTC) - timedelta(hours=2)
        time_triggered = oldest_dt < time_threshold
    else:
        time_triggered = True

    should_submit = (
        len(pending_tasks) >= settings.batch_min_size
        or time_triggered
    )

    if not should_submit:
        logger.debug(
            f"[ai_analysis] {len(pending_tasks)} pending, not enough for batch "
            f"(min={settings.batch_min_size}, time_triggered={time_triggered})"
        )
        return

    logger.debug(f"[ai_analysis] Submitting batch: {len(pending_tasks)} tasks "
                 f"(min={settings.batch_min_size}, time_triggered={time_triggered})")

    # Батчевая загрузка профилей
    profiles, task_ids, _ = await _load_profiles_for_batch(db, pending_tasks)

    if not profiles:
        return

    # Собираем text_only blog_id из payload задач (retry после refusal)
    text_only_ids: set[str] = set()
    pending_by_id = {pending_task["id"]: pending_task for pending_task in pending_tasks}
    for pt in pending_tasks:
        payload = pt.get("payload") or {}
        if payload.get("text_only") and pt.get("blog_id"):
            text_only_ids.add(pt["blog_id"])

    # Claim задачи и отправить батч
    claimed_tasks: dict[str, tuple[int, int]] = {}
    try:
        claimed_profiles: list[tuple[str, ScrapedProfile]] = []
        for profile_entry, tid in zip(profiles, task_ids, strict=True):
            was_claimed = await mark_task_running(db, tid)
            if not was_claimed:
                logger.debug(f"AI task {tid} was already claimed by another worker")
                continue
            claimed_profiles.append(profile_entry)
            original_task = pending_by_id.get(tid, {})
            current_attempts = int(original_task.get("attempts", 0)) + 1
            max_attempts = int(original_task.get("max_attempts", 3))
            claimed_tasks[tid] = (current_attempts, max_attempts)

        if not claimed_profiles:
            return

        batch_id = await submit_batch(
            openai_client, claimed_profiles, settings,
            text_only_ids=text_only_ids,
        )

        # Сохраняем batch_id в payload (мержим с существующим, чтобы не затереть text_only)
        for tid in claimed_tasks:
            existing_payload = pending_by_id.get(tid, {}).get("payload") or {}
            merged_payload = {**existing_payload, "batch_id": batch_id}
            await run_in_thread(
                db.table("scrape_tasks").update({
                    "payload": merged_payload,
                }).eq("id", tid).execute
            )

        logger.info(f"AI batch submitted: {batch_id}, {len(claimed_profiles)} profiles")
    except Exception as e:
        for tid, (attempts, max_attempts) in claimed_tasks.items():
            try:
                await mark_task_failed(
                    db=db,
                    task_id=tid,
                    attempts=attempts,
                    max_attempts=max_attempts,
                    error=sanitize_error(str(e)),
                    retry=True,
                )
            except Exception as rollback_err:
                logger.error(f"Failed to rollback task {tid}: {rollback_err}")
        logger.error(f"Failed to submit AI batch: {e}")


def _dedup_brands(brands: list[str]) -> list[str]:
    """Нормализация и дедупликация списка брендов."""
    seen: set[str] = set()
    unique: list[str] = []
    for b in brands:
        normalized = normalize_brand(b)
        key = normalized.lower()
        if key not in seen:
            seen.add(key)
            unique.append(normalized)
    return unique


async def _retry_enrichment(
    fn: Callable[[], Awaitable[dict[str, int]]],
) -> dict[str, int]:
    """Вызвать async-функцию с retry и экспоненциальным backoff."""
    for attempt in range(_ENRICHMENT_RETRY_ATTEMPTS):
        try:
            return await fn()
        except Exception:
            if attempt == _ENRICHMENT_RETRY_ATTEMPTS - 1:
                raise
            await asyncio.sleep(_ENRICHMENT_RETRY_DELAY_SECONDS * (attempt + 1))
    return {"total": 0, "matched": 0, "unmatched": 0}  # unreachable


async def _process_blog_result(
    db: Client,
    openai_client: AsyncOpenAI,
    blog_id: str,
    insights: Any,
    current_by_id: dict[str, dict[str, Any]],
    categories_cache: dict[str, str],
    tags_cache: dict[str, str],
    cities_cache: dict[str, str],
    taxonomy_metrics: dict[str, int],
) -> None:
    """Обработать результат одного блога из батча. Исключения пробрасываются наверх."""
    if isinstance(insights, tuple) and insights[0] == "refusal":
        # AI refusal — сохранить причину, попробовать text_only retry
        refusal_reason = insights[1]
        logger.warning(f"[batch_results] Blog {blog_id}: AI refusal: {refusal_reason}")

        current_status = current_by_id.get(blog_id, {}).get("scrape_status")
        already_refused = current_status == "ai_refused"

        await run_in_thread(
            db.table("blogs").update({
                "ai_insights": {"refusal_reason": refusal_reason},
                "scrape_status": "ai_analyzed" if already_refused else "ai_refused",
                "ai_analyzed_at": datetime.now(UTC).isoformat(),
            }).eq("id", blog_id).execute
        )

        if not already_refused:
            try:
                await create_task_if_not_exists(
                    db, blog_id, "ai_analysis", priority=3,
                    payload={"text_only": True},
                )
            except Exception as e:
                logger.error(f"[batch_results] Failed to create text_only retry for {blog_id}: {e}")

    elif insights is None:
        # API error (не refusal) — помечаем ai_analyzed без insights
        logger.debug(f"[batch_results] Blog {blog_id}: no insights (API error)")
        await run_in_thread(
            db.table("blogs").update({
                "scrape_status": "ai_analyzed",
                "ai_analyzed_at": datetime.now(UTC).isoformat(),
            }).eq("id", blog_id).execute
        )
    else:
        # Успешный AIInsights — экстракция полей (только пустые)
        assert isinstance(insights, AIInsights)
        extracted = _extract_blog_fields(insights)
        current = current_by_id.get(blog_id, {})
        for field in list(extracted.keys()):
            if current.get(field):  # уже заполнено — не перезаписываем
                del extracted[field]

        # Нормализация и дедупликация брендов
        if insights.commercial.detected_brands:
            insights.commercial.detected_brands = _dedup_brands(
                insights.commercial.detected_brands,
            )
        if insights.commercial.ambassador_brands:
            insights.commercial.ambassador_brands = _dedup_brands(
                insights.commercial.ambassador_brands,
            )

        # Сохраняем insights + извлечённые поля
        logger.debug(f"[batch_results] Blog {blog_id}: saving insights "
                     f"(confidence={insights.confidence}, "
                     f"page_type={insights.blogger_profile.page_type}, "
                     f"categories={insights.content.primary_categories})")
        update_data: dict[str, Any] = {
            "ai_insights": insights.model_dump(),
            "ai_confidence": _CONFIDENCE_TO_FLOAT.get(insights.confidence, 0.60),
            "ai_analyzed_at": datetime.now(UTC).isoformat(),
            "scrape_status": "ai_analyzed",
            **extracted,
        }
        await run_in_thread(
            db.table("blogs").update(update_data).eq("id", blog_id).execute
        )

        # Матчинг категорий (не блокирует mark_task_done при ошибке)
        categories_stats = {"total": 0, "matched": 0, "unmatched": 0}
        try:
            categories_stats = await _retry_enrichment(
                lambda: match_categories(db, blog_id, insights, categories=categories_cache)
            )
        except Exception as e:
            taxonomy_metrics["taxonomy_errors"] += 1
            logger.error(f"Failed to match categories for blog {blog_id}: {e}")
        taxonomy_metrics["categories_total"] += int(categories_stats["total"])
        taxonomy_metrics["categories_matched"] += int(categories_stats["matched"])
        taxonomy_metrics["categories_unmatched"] += int(categories_stats["unmatched"])

        # Матчинг тегов
        tags_stats = {"total": 0, "matched": 0, "unmatched": 0}
        try:
            tags_stats = await _retry_enrichment(
                lambda: match_tags(db, blog_id, insights, tags=tags_cache)
            )
        except Exception as e:
            taxonomy_metrics["taxonomy_errors"] += 1
            logger.error(f"Failed to match tags for blog {blog_id}: {e}")
        taxonomy_metrics["tags_total"] += int(tags_stats["total"])
        taxonomy_metrics["tags_matched"] += int(tags_stats["matched"])
        taxonomy_metrics["tags_unmatched"] += int(tags_stats["unmatched"])

        # Матчинг города (фильтруем мусор перед lookup)
        city_name = insights.blogger_profile.city
        if city_name and is_valid_city(city_name) and cities_cache:
            try:
                matched = await match_city(db, blog_id, city_name, cities_cache)
                if not matched:
                    logger.debug(f"[batch_results] Blog {blog_id}: city '{city_name}' not found in cities table")
            except Exception as e:
                logger.error(f"Failed to match city for blog {blog_id}: {e}")

        # Генерация embedding
        try:
            embedding_text = build_embedding_text(insights)
            vector = await generate_embedding(openai_client, embedding_text)
            if vector:
                await run_in_thread(
                    db.table("blogs").update({
                        "embedding": vector,
                    }).eq("id", blog_id).execute
                )
                logger.debug(f"[batch_results] Blog {blog_id}: embedding saved ({len(vector)} dim)")
        except Exception as e:
            logger.error(f"Failed to generate embedding for blog {blog_id}: {e}")


async def handle_batch_results(
    db: Client,
    openai_client: AsyncOpenAI,
    batch_id: str,
    task_ids_by_blog: Mapping[str, str | dict[str, Any] | list[str | dict[str, Any]]],
) -> None:
    """
    Обработать результаты завершённого батча.
    task_ids_by_blog:
      - {blog_id: task_id}
      - {blog_id: {"id": ..., "attempts": ..., "max_attempts": ...}}
      - {blog_id: [task_id | {"id": ..., "attempts": ..., "max_attempts": ...}, ...]}
    """
    logger.debug(f"[batch_results] Polling batch {batch_id}...")
    result = await poll_batch(openai_client, batch_id)
    logger.debug(f"[batch_results] Batch {batch_id} status={result['status']}")

    # poll_batch возвращает results для completed и expired (partial results)
    if "results" not in result:
        return

    results = result.get("results", {})
    logger.debug(f"[batch_results] Batch {batch_id}: {len(results)} results")

    # Загружаем категории, теги и города один раз для всего батча
    categories_cache = await load_categories(db)
    tags_cache = await load_tags(db)
    cities_cache = await load_cities(db)

    # Загружаем текущие значения полей для всех блогов в батче (чтобы не перезаписывать заполненные)
    blog_ids_with_results = list(results.keys())
    current_by_id: dict[str, dict[str, Any]] = {}
    if blog_ids_with_results:
        current_blogs = await run_in_thread(
            db.table("blogs").select(
                "id, city, content_language, audience_gender,"
                " audience_age, audience_countries, scrape_status"
            )
            .in_("id", blog_ids_with_results).execute
        )
        current_by_id = {b["id"]: b for b in current_blogs.data}

    processed_blog_ids: set[str] = set()
    taxonomy_metrics = {
        "categories_total": 0,
        "categories_matched": 0,
        "categories_unmatched": 0,
        "tags_total": 0,
        "tags_matched": 0,
        "tags_unmatched": 0,
        "taxonomy_errors": 0,
    }

    def _get_task_infos(blog_id: str) -> list[tuple[str, int, int]]:
        """Извлечь список (task_id, attempts, max_attempts) для blog_id."""
        val = task_ids_by_blog.get(blog_id)
        if val is None:
            return []

        items: list[str | dict[str, Any]]
        if isinstance(val, list):
            items = val
        else:
            items = [val]

        task_infos: list[tuple[str, int, int]] = []
        for item in items:
            if isinstance(item, dict):
                task_id = item.get("id")
                if not task_id:
                    continue
                attempts = int(item.get("attempts", 1))
                max_attempts = int(item.get("max_attempts", 3))
                task_infos.append((task_id, attempts, max_attempts))
            else:
                task_infos.append((item, 1, 3))
        return task_infos

    for blog_id, insights in results.items():
        task_infos = _get_task_infos(blog_id)
        if not task_infos:
            continue

        processed_blog_ids.add(blog_id)

        try:
            await _process_blog_result(
                db, openai_client, blog_id, insights,
                current_by_id, categories_cache, tags_cache, cities_cache,
                taxonomy_metrics,
            )
        except Exception as e:
            # Ошибка одного блога не должна убивать весь батч —
            # помечаем задачу как failed с retry и продолжаем
            logger.error(f"[batch_results] Blog {blog_id} failed: {e}")
            for task_id, attempts, max_attempts in task_infos:
                await mark_task_failed(
                    db, task_id, attempts, max_attempts,
                    f"Error processing batch result: {e}", retry=True,
                )
            continue

        for task_id, _, _ in task_infos:
            await mark_task_done(db, task_id)

    # Expired батч: задачи без результатов → retry (не ждать 26ч retry_stale_batches)
    if result["status"] == "expired":
        for blog_id in task_ids_by_blog:
            if blog_id not in processed_blog_ids:
                task_infos = _get_task_infos(blog_id)
                if not task_infos:
                    logger.warning(f"Skipping expired retry for {blog_id}: no task_id")
                    continue
                for task_id, attempts, max_attempts in task_infos:
                    await mark_task_failed(
                        db, task_id, attempts, max_attempts,
                        "Batch expired without result for this task", retry=True,
                    )

    logger.info(
        f"Batch {batch_id} processed: {len(results)} results | "
        f"categories: total={taxonomy_metrics['categories_total']}, "
        f"matched={taxonomy_metrics['categories_matched']}, "
        f"unmatched={taxonomy_metrics['categories_unmatched']} | "
        f"tags: total={taxonomy_metrics['tags_total']}, "
        f"matched={taxonomy_metrics['tags_matched']}, "
        f"unmatched={taxonomy_metrics['tags_unmatched']} | "
        f"taxonomy_errors={taxonomy_metrics['taxonomy_errors']}"
    )


async def handle_discover(
    db: Client,
    task: dict[str, Any],
    scraper: BaseScraper,
    settings: Settings,
) -> None:
    """
    Дискавери новых профилей по хештегу.
    1. discover() по хештегу из payload
    2. Для каждого нового профиля: insert в persons + blogs
    3. Создать full_scrape задачу
    """
    task_id = task["id"]
    payload = task.get("payload") or {}
    hashtag = payload.get("hashtag", "")
    min_followers = payload.get("min_followers", 1000)

    if not hashtag:
        await mark_task_failed(db, task_id, task["attempts"], task["max_attempts"],
                               "No hashtag in payload", retry=False)
        return

    was_claimed = await mark_task_running(db, task_id)
    if not was_claimed:
        logger.debug(f"Task {task_id} was already claimed by another worker")
        return
    # RPC атомарно инкрементирует attempts, используем актуальное значение
    current_attempts = task["attempts"] + 1

    try:
        discovered = await scraper.discover(hashtag, min_followers)
    except AllAccountsCooldownError as e:
        await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                               str(e), retry=True)
        return
    except Exception as e:
        await mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                               sanitize_error(str(e)), retry=True)
        return

    # Батчевая проверка существующих блогов (вместо N отдельных запросов)
    normalized_usernames = [_normalize_username(p.username) for p in discovered]
    existing_blogs_result = await run_in_thread(
        db.table("blogs")
        .select("id, username, scraped_at")
        .eq("platform", "instagram")
        .in_("username", normalized_usernames)
        .execute
    )
    existing_blogs_by_username: dict[str, dict[str, Any]] = {
        b["username"]: b for b in existing_blogs_result.data
    }

    new_count = 0
    for profile in discovered:
        normalized_username = _normalize_username(profile.username)

        # Проверяем, нет ли уже такого блога (из батчевого запроса)
        existing_blog = existing_blogs_by_username.get(normalized_username)
        if existing_blog:
            # Для существующих блогов: проверить свежесть
            blog_id = existing_blog["id"]
            if not await is_blog_fresh(db, blog_id, settings.rescrape_days):
                try:
                    await create_task_if_not_exists(db, blog_id, "full_scrape", priority=5)
                except Exception as e:
                    logger.error(f"Failed to create rescrape task for @{profile.username}: {e}")
            continue

        # Создаём person + blog (ошибка одного профиля не ломает весь discover)
        person_id: str | None = None
        try:
            person_result = await run_in_thread(
                db.table("persons")
                .insert({
                    "full_name": profile.full_name or normalized_username,
                })
                .execute
            )
            person_id = person_result.data[0]["id"]

            blog_insert_data: dict[str, Any] = {
                "person_id": person_id,
                "platform": "instagram",
                "username": normalized_username,
                "platform_id": profile.platform_id,
                "followers_count": profile.follower_count,
                "source": "hashtag_search",
                "scrape_status": "pending",
                "is_business": profile.is_business,
                "is_verified": profile.is_verified,
                "bio": profile.biography,
            }
            if profile.account_type is not None:
                blog_insert_data["account_type"] = profile.account_type

            blog_result = await run_in_thread(
                db.table("blogs")
                .insert(blog_insert_data)
                .execute
            )
            blog_id = blog_result.data[0]["id"]

            await create_task_if_not_exists(db, blog_id, "full_scrape", priority=5)
            new_count += 1
        except Exception as e:
            if person_id:
                await cleanup_orphan_person(db, person_id)
            logger.error(f"Failed to create profile @{profile.username}: {e}")
            continue

    await mark_task_done(db, task_id)
    logger.info(f"Discover #{hashtag}: found {len(discovered)}, new {new_count}")
