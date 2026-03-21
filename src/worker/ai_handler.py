"""Обработчики AI-анализа и батч-результатов."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from loguru import logger
from openai import AsyncOpenAI
from supabase import AsyncClient

import src.worker.handlers as _h
from src.ai.normalize import deduplicate_list, normalize_country, normalize_posting_frequency
from src.ai.schemas import AIInsights
from src.ai.taxonomy_matching import (
    is_valid_city,
    normalize_brand,
)
from src.config import Settings
from src.models.blog import BioLink, ScrapedHighlight, ScrapedPost, ScrapedProfile
from src.worker.scrape_handler import _parse_top_comments

# Маппинг discrete confidence (1-5) → float для БД (DECIMAL(3,2))
_CONFIDENCE_TO_FLOAT: dict[int, float] = {1: 0.20, 2: 0.40, 3: 0.60, 4: 0.80, 5: 1.00}
_ENRICHMENT_RETRY_ATTEMPTS = 3
_ENRICHMENT_RETRY_DELAY_SECONDS = 0.2

# Ошибки OpenAI, при которых задачи не должны терять attempts
# (проблема на стороне платформы/биллинга, не конкретной задачи)
_OPENAI_QUOTA_ERRORS = ("token_limit_exceeded", "billing_hard_limit_reached", "insufficient_quota")


async def _safe_fail_tasks(
    db: AsyncClient,
    task_infos: list[tuple[str, int, int]],
    error: str,
    *,
    retry: bool = True,
) -> None:
    """Пометить список задач как failed с обработкой ошибок каждой."""
    for task_id, attempts, max_attempts in task_infos:
        try:
            await _h.mark_task_failed(db, task_id, attempts, max_attempts, error, retry=retry)
        except Exception as fail_err:
            logger.error(f"[batch_results] Не удалось пометить задачу {task_id} как failed: {fail_err}")


def _has_successful_ai_insights(value: Any) -> bool:
    """Return True when ai_insights looks like a successful structured analysis."""
    if not isinstance(value, dict):
        return False
    typed_value = cast(dict[str, Any], value)
    if typed_value.get("refusal_reason"):
        return False
    required_sections = (
        "blogger_profile",
        "audience_inference",
        "content",
        "commercial",
        "summary",
    )
    return all(section in typed_value for section in required_sections)


@dataclass
class BatchContext:
    """Общий контекст для обработки результатов батча (shared между всеми блогами)."""

    db: AsyncClient
    openai_client: AsyncOpenAI
    current_by_id: dict[str, dict[str, Any]]
    categories_cache: dict[str, str]
    tags_cache: dict[str, str]
    cities_cache: dict[str, str]
    taxonomy_metrics: dict[str, int] = field(default_factory=lambda: {
        "categories_total": 0,
        "categories_matched": 0,
        "categories_unmatched": 0,
        "tags_total": 0,
        "tags_matched": 0,
        "tags_unmatched": 0,
        "taxonomy_errors": 0,
    })
    # Накапливаем embedding задачи для параллельного выполнения
    pending_embeddings: list[tuple[str, str]] = field(default_factory=lambda: [])


def _extract_blog_fields(insights: AIInsights) -> dict[str, Any]:
    """Извлечь поля блога из AI-анализа для заполнения основных колонок."""
    fields: dict[str, Any] = {}

    # Тип страницы (blog / public / business)
    if insights.blogger_profile.page_type:
        fields["page_type"] = insights.blogger_profile.page_type

    # Город (фильтруем мусор: "14% Казахстан", названия стран и т.д.)
    if insights.blogger_profile.city and is_valid_city(insights.blogger_profile.city):
        fields["city"] = insights.blogger_profile.city

    # Язык контента
    if insights.content.content_language:
        fields["content_language"] = ", ".join(insights.content.content_language)

    # Аудитория по полу (проценты)
    ai = insights.audience_inference
    if ai.gender and ai.gender.male_pct is not None and ai.gender.female_pct is not None:
        fields["audience_gender"] = {
            "male": ai.gender.male_pct,
            "female": ai.gender.female_pct,
            "other": ai.gender.other_pct or 0,
        }

    # Возрастное распределение аудитории
    if ai.age:
        age_data: dict[str, float] = {}
        for key, val in [
            ("13-17", ai.age.pct_13_17),
            ("18-24", ai.age.pct_18_24),
            ("25-34", ai.age.pct_25_34),
            ("35-44", ai.age.pct_35_44),
            ("45+", ai.age.pct_45_plus),
        ]:
            if val is not None and val > 0:
                age_data[key] = val
        if age_data:
            fields["audience_age"] = age_data

    # Географическое распределение аудитории
    if ai.geo:
        country_data: dict[str, float] = {}
        for code, val in [
            ("KZ", ai.geo.kz_pct),
            ("RU", ai.geo.ru_pct),
            ("UZ", ai.geo.uz_pct),
            ("OTHER", ai.geo.other_geo_pct),
        ]:
            if val is not None and val > 0:
                country_data[code] = val
        if country_data:
            fields["audience_countries"] = country_data

    return fields


async def _load_profiles_for_batch(
    db: AsyncClient,
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
    blogs_result = await db.table("blogs").select("*").in_("id", blog_ids).execute()
    posts_result = await db.table("blog_posts").select("*").in_(
        "blog_id", blog_ids
    ).order("taken_at", desc=True).execute()
    highlights_result = await db.table("blog_highlights").select("*").in_(
        "blog_id", blog_ids
    ).execute()

    # Индексация по blog_id
    blog_rows = cast(list[dict[str, Any]], blogs_result.data or [])
    post_rows = cast(list[dict[str, Any]], posts_result.data or [])
    highlight_rows = cast(list[dict[str, Any]], highlights_result.data or [])
    blogs_by_id: dict[str, dict[str, Any]] = {str(b["id"]): b for b in blog_rows}
    posts_by_blog: dict[str, list[dict[str, Any]]] = {}
    for p in post_rows:
        posts_by_blog.setdefault(str(p["blog_id"]), []).append(p)
    highlights_by_blog: dict[str, list[dict[str, Any]]] = {}
    for h in highlight_rows:
        highlights_by_blog.setdefault(str(h["blog_id"]), []).append(h)

    profiles: list[tuple[str, ScrapedProfile]] = []
    task_ids: list[str] = []
    failed_task_ids: list[str] = []

    for pt in pending_tasks:
        blog_id = pt["blog_id"]
        blog = blogs_by_id.get(blog_id)

        if not blog:
            await _h.mark_task_failed(
                db, pt["id"], pt.get("attempts", 0), pt.get("max_attempts", 3),
                f"Blog {blog_id} not found in database", retry=False,
            )
            failed_task_ids.append(pt["id"])
            continue

        raw_posts = posts_by_blog.get(blog_id, [])[:25]
        raw_highlights = highlights_by_blog.get(blog_id, [])

        # Сборка ScrapedProfile — все публикации в один список
        medias: list[ScrapedPost] = []
        skipped_posts = 0
        for p in raw_posts:
            taken_at_raw = p.get("taken_at")
            if not taken_at_raw or not p.get("platform_id"):
                skipped_posts += 1
                continue
            try:
                taken_at = datetime.fromisoformat(taken_at_raw.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                skipped_posts += 1
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

        if skipped_posts:
            logger.warning(
                f"[batch] Blog {blog_id}: пропущено {skipped_posts}/{len(raw_posts)} постов "
                f"(нет platform_id или невалидный taken_at)"
            )

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
        raw_bio_links: list[Any] = cast(list[Any], blog.get("bio_links") or [])
        bio_links: list[BioLink] = []
        for item in raw_bio_links:
            if isinstance(item, str):
                bio_links.append(BioLink(url=item))
            elif isinstance(item, dict):
                item_dict = cast(dict[str, Any], item)
                bio_links.append(BioLink(**item_dict))

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
    db: AsyncClient,
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
    pending_result = await db.table("scrape_tasks").select(
        "id, blog_id, created_at, attempts, max_attempts, payload"
    ).eq("task_type", "ai_analysis").eq("status", "pending").order(
        "created_at", desc=False
    ).limit(100).execute()
    pending_tasks = cast(list[dict[str, Any]], pending_result.data or [])

    # Гарантируем что текущая задача включена (защита от гонки с параллельным worker'ом)
    if not any(t["id"] == task["id"] for t in pending_tasks):
        pending_tasks.append(task)

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
        logger.debug("[ai_analysis] Нет профилей для батча после загрузки (все задачи failed или пустые)")
        return

    # Собираем text_only blog_id из payload задач (retry после refusal)
    text_only_ids: set[str] = set()
    pending_by_id = {pending_task["id"]: pending_task for pending_task in pending_tasks}
    for pt in pending_tasks:
        payload: dict[str, Any] = cast(dict[str, Any], pt.get("payload") or {})
        if payload.get("text_only") and pt.get("blog_id"):
            text_only_ids.add(pt["blog_id"])

    # Claim задачи и отправить батч
    claimed_tasks: dict[str, tuple[int, int]] = {}
    try:
        claimed_profiles: list[tuple[str, ScrapedProfile]] = []
        for profile_entry, tid in zip(profiles, task_ids, strict=True):
            was_claimed = await _h.mark_task_running(db, tid)
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

        batch_id = await _h.submit_batch(
            openai_client, claimed_profiles, settings,
            text_only_ids=text_only_ids,
        )

        # Сохраняем batch_id в payload (мержим с существующим, чтобы не затереть text_only)
        save_failures: list[str] = []
        for tid in claimed_tasks:
            try:
                existing_payload: dict[str, Any] = cast(
                    dict[str, Any], pending_by_id.get(tid, {}).get("payload") or {}
                )
                merged_payload: dict[str, Any] = {**existing_payload, "batch_id": batch_id}
                await db.table("scrape_tasks").update({
                    "payload": merged_payload,
                }).eq("id", tid).execute()
            except Exception as save_err:
                save_failures.append(tid)
                logger.error(
                    f"[ai_analysis] Не удалось сохранить batch_id={batch_id} "
                    f"в задачу {tid}: {save_err}"
                )

        if save_failures:
            logger.error(
                f"[ai_analysis] CRITICAL: batch_id={batch_id} потерян для задач "
                f"{save_failures}. Батч отправлен, но задачи не привязаны — "
                f"требуется ручное восстановление"
            )

        logger.info(f"AI batch submitted: {batch_id}, {len(claimed_profiles)} profiles")
    except Exception as e:
        error_str = str(e)
        is_quota_error = any(code in error_str for code in _OPENAI_QUOTA_ERRORS)

        if is_quota_error:
            # Ошибки квоты/лимитов — откатываем claim и возвращаем в pending с backoff.
            # mark_task_running уже инкрементировал attempts — восстанавливаем,
            # чтобы платформенные ошибки не сжигали попытки задачи.
            is_billing = "billing_hard_limit" in error_str or "insufficient_quota" in error_str
            backoff_seconds = 3600 if is_billing else 600  # 1ч для биллинга, 10мин для token_limit
            next_retry = datetime.now(UTC) + timedelta(seconds=backoff_seconds)
            logger.warning(
                f"[ai_analysis] OpenAI quota/limit error, returning {len(claimed_tasks)} tasks "
                f"to pending (backoff={backoff_seconds}s): {_h.sanitize_error(error_str)[:200]}"
            )
            for tid in claimed_tasks:
                attempts, _max = claimed_tasks[tid]
                try:
                    await db.table("scrape_tasks").update({
                        "status": "pending",
                        "attempts": attempts - 1,  # откат mark_task_running
                        "error_message": _h.sanitize_error(error_str)[:500],
                        "next_retry_at": next_retry.isoformat(),
                    }).eq("id", tid).execute()
                except Exception as rollback_err:
                    logger.error(f"Failed to return task {tid} to pending: {rollback_err}")
        else:
            # Обычная ошибка — считаем как attempt, ретраим стандартно
            for tid, (attempts, max_attempts) in claimed_tasks.items():
                try:
                    await _h.mark_task_failed(
                        db=db,
                        task_id=tid,
                        attempts=attempts,
                        max_attempts=max_attempts,
                        error=_h.sanitize_error(error_str),
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


def _normalize_insights(insights: AIInsights, posts_per_week: float | None) -> None:
    """Постпроцессинг AIInsights: нормализация и дедупликация полей."""
    # Нормализация страны к русскому
    if insights.blogger_profile.country:
        normalized = normalize_country(insights.blogger_profile.country)
        if normalized != insights.blogger_profile.country:
            logger.debug(
                f"[normalize] country: '{insights.blogger_profile.country}' -> '{normalized}'"
            )
            insights.blogger_profile.country = normalized

    # Переопределение posting_frequency по фактическому posts_per_week
    original_freq = insights.content.posting_frequency
    corrected_freq = normalize_posting_frequency(original_freq, posts_per_week)
    if corrected_freq != original_freq:
        logger.debug(
            f"[normalize] posting_frequency: '{original_freq}' -> '{corrected_freq}' "
            f"(posts_per_week={posts_per_week})"
        )
        insights.content.posting_frequency = corrected_freq

    # Дедупликация списков в marketing_value
    insights.marketing_value.best_fit_industries = deduplicate_list(
        insights.marketing_value.best_fit_industries
    )
    insights.marketing_value.not_suitable_for = deduplicate_list(
        insights.marketing_value.not_suitable_for
    )
    insights.marketing_value.values_and_causes = deduplicate_list(
        insights.marketing_value.values_and_causes
    )

    # Дедупликация в audience
    insights.audience_inference.geo_mentions = deduplicate_list(
        insights.audience_inference.geo_mentions
    )
    insights.audience_inference.audience_interests = deduplicate_list(
        insights.audience_inference.audience_interests
    )

    # Дедупликация в content
    insights.content.content_language = deduplicate_list(
        insights.content.content_language
    )

    # Дедупликация в commercial
    insights.commercial.detected_brand_categories = deduplicate_list(
        insights.commercial.detected_brand_categories
    )

    # Дедупликация остальных списков
    insights.blogger_profile.speaks_languages = deduplicate_list(
        insights.blogger_profile.speaks_languages
    )
    insights.lifestyle.pet_types = deduplicate_list(
        insights.lifestyle.pet_types
    )


async def _retry_enrichment(
    fn: Callable[[], Awaitable[dict[str, int]]],
) -> dict[str, int]:
    """Вызвать async-функцию с retry и экспоненциальным backoff."""
    for attempt in range(_ENRICHMENT_RETRY_ATTEMPTS):
        try:
            return await fn()
        except Exception as e:
            if attempt == _ENRICHMENT_RETRY_ATTEMPTS - 1:
                raise
            logger.warning(f"[enrichment] retry {attempt + 1}/{_ENRICHMENT_RETRY_ATTEMPTS}: {e}")
            await asyncio.sleep(_ENRICHMENT_RETRY_DELAY_SECONDS * (attempt + 1))
    return {"total": 0, "matched": 0, "unmatched": 0}  # unreachable


async def _process_blog_result(
    ctx: BatchContext,
    blog_id: str,
    insights: Any,
) -> None:
    """Обработать результат одного блога из батча. Исключения пробрасываются наверх."""
    db = ctx.db
    current_by_id = ctx.current_by_id
    categories_cache = ctx.categories_cache
    tags_cache = ctx.tags_cache
    cities_cache = ctx.cities_cache
    taxonomy_metrics = ctx.taxonomy_metrics
    if isinstance(insights, tuple) and insights[0] == "refusal":
        # AI refusal — сохранить причину, попробовать text_only retry
        refusal_reason: str = str(cast(tuple[Any, ...], insights)[1])
        logger.warning(f"[batch_results] Blog {blog_id}: AI refusal: {refusal_reason}")

        current_blog = current_by_id.get(blog_id, {})
        current_status = current_blog.get("scrape_status")
        if _has_successful_ai_insights(current_blog.get("ai_insights")):
            logger.info(
                f"[batch_results] Blog {blog_id}: refusal ignored, "
                "successful insights already stored"
            )
            return
        already_refused = current_status == "ai_refused"

        await db.table("blogs").update({
            "ai_insights": {"refusal_reason": refusal_reason},
            "scrape_status": "ai_analyzed" if already_refused else "ai_refused",
            "ai_analyzed_at": datetime.now(UTC).isoformat(),
        }).eq("id", blog_id).execute()

        if not already_refused:
            try:
                await _h.create_task_if_not_exists(
                    db, blog_id, "ai_analysis", priority=2,
                    payload={"text_only": True},
                )
            except Exception as e:
                logger.error(f"[batch_results] Failed to create text_only retry for {blog_id}: {e}")

    else:
        # Успешный AIInsights — экстракция полей (только пустые)
        if not isinstance(insights, AIInsights):
            logger.error(f"[batch_results] Unexpected insights type for {blog_id}: {type(cast(object, insights))}")
            return

        # Постпроцессинг: нормализация полей, дедупликация списков
        # posts_per_week берём из текущих данных блога в БД
        current_blog_data = current_by_id.get(blog_id, {})
        _normalize_insights(insights, current_blog_data.get("posts_per_week"))

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
        await db.table("blogs").update(update_data).eq("id", blog_id).execute()

        # Матчинг категорий (не блокирует mark_task_done при ошибке)
        categories_stats = {"total": 0, "matched": 0, "unmatched": 0}
        try:
            categories_stats = await _retry_enrichment(
                lambda: _h.match_categories(db, blog_id, insights, categories=categories_cache)
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
                lambda: _h.match_tags(db, blog_id, insights, tags=tags_cache)
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
                matched = await _h.match_city(db, blog_id, city_name, cities_cache)
                if not matched:
                    logger.debug(f"[batch_results] Blog {blog_id}: city '{city_name}' not found in cities table")
            except Exception as e:
                logger.error(f"Failed to match city for blog {blog_id}: {e}")

        # Embedding откладываем — выполним параллельно после обработки всех блогов
        try:
            embedding_text = _h.build_embedding_text(insights)
            if embedding_text is None:
                logger.warning(f"[batch_results] Blog {blog_id}: пустой текст для embedding, пропускаем")
            else:
                ctx.pending_embeddings.append((blog_id, embedding_text))
        except Exception as e:
            logger.error(f"Failed to build embedding text for blog {blog_id}: {e}")


async def handle_batch_results(
    db: AsyncClient,
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
    result = await _h.poll_batch(openai_client, batch_id)
    logger.debug(f"[batch_results] Batch {batch_id} status={result['status']}")

    # Батч упал целиком (например, token limit) — ретраим все задачи
    if result["status"] in ("failed", "cancelled"):
        logger.warning(
            f"[batch_results] Batch {batch_id} {result['status']}, retrying tasks"
        )
        all_task_infos: list[tuple[str, int, int]] = []
        for _blog_id, val in task_ids_by_blog.items():
            items = val if isinstance(val, list) else [val]
            for item in items:
                if isinstance(item, dict):
                    tid = item.get("id")
                    att = int(item.get("attempts", 1))
                    ma = int(item.get("max_attempts", 3))
                else:
                    tid, att, ma = item, 1, 3
                if tid:
                    all_task_infos.append((tid, att, ma))
        await _safe_fail_tasks(db, all_task_infos, f"Batch {result['status']}")
        return

    # poll_batch возвращает results для completed и expired (partial results)
    if "results" not in result:
        return

    results = result.get("results", {})
    logger.debug(f"[batch_results] Batch {batch_id}: {len(results)} results")

    # Загружаем категории, теги и города один раз для всего батча
    categories_cache = await _h.load_categories(db)
    tags_cache = await _h.load_tags(db)
    cities_cache = await _h.load_cities(db)

    # Загружаем текущие значения полей для всех блогов в батче (чтобы не перезаписывать заполненные)
    blog_ids_with_results = list(results.keys())
    current_by_id: dict[str, dict[str, Any]] = {}
    if blog_ids_with_results:
        current_blogs = await db.table("blogs").select(
            "id, city, content_language, audience_gender,"
            " audience_age, audience_countries, scrape_status, ai_insights,"
            " posts_per_week"
        ).in_("id", blog_ids_with_results).execute()
        current_rows = cast(list[dict[str, Any]], current_blogs.data or [])
        current_by_id = {str(b["id"]): b for b in current_rows}

    processed_blog_ids: set[str] = set()
    ctx = BatchContext(
        db=db,
        openai_client=openai_client,
        current_by_id=current_by_id,
        categories_cache=categories_cache,
        tags_cache=tags_cache,
        cities_cache=cities_cache,
    )

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

        # insights=None означает API error (не refusal) — retry задачи
        if insights is None:
            logger.warning(f"[batch_results] Blog {blog_id}: no insights (API error), retry")
            await _safe_fail_tasks(
                db, task_infos, "OpenAI API error: no insights in batch result",
            )
            continue

        try:
            await _process_blog_result(ctx, blog_id, insights)
        except Exception as e:
            # Ошибка одного блога не должна убивать весь батч
            logger.error(f"[batch_results] Blog {blog_id} failed: {e}")
            await _safe_fail_tasks(
                db, task_infos, f"Error processing batch result: {e}",
            )
            continue

        for task_id, _, _ in task_infos:
            try:
                await _h.mark_task_done(db, task_id)
            except Exception as done_err:
                logger.error(
                    f"[batch_results] Не удалось пометить задачу {task_id} как done: {done_err}"
                )

    # Expired батч: задачи без результатов → retry (не ждать 26ч retry_stale_batches)
    if result["status"] == "expired":
        for blog_id in task_ids_by_blog:
            if blog_id not in processed_blog_ids:
                task_infos = _get_task_infos(blog_id)
                if not task_infos:
                    logger.warning(f"Skipping expired retry for {blog_id}: no task_id")
                    continue
                await _safe_fail_tasks(
                    db, task_infos, "Batch expired without result for this task",
                )

    # Параллельная генерация embedding для всех блогов батча
    if ctx.pending_embeddings:
        # Семафор ограничивает параллельные DB-запросы,
        # чтобы не перегрузить connection pool Supabase (60 соединений)
        embed_semaphore = asyncio.Semaphore(20)

        async def _generate_and_save(blog_id: str, text: str) -> None:
            try:
                vector = await _h.generate_embedding(openai_client, text)
                if vector:
                    async with embed_semaphore:
                        await db.table("blogs").update({
                            "embedding": vector,
                        }).eq("id", blog_id).execute()
                    logger.debug(f"[batch_results] Blog {blog_id}: embedding saved ({len(vector)} dim)")
                else:
                    logger.warning(f"[batch_results] Blog {blog_id}: embedding не сгенерирован (rate limit?)")
            except Exception as e:
                logger.error(f"Failed to generate embedding for blog {blog_id}: {e}")

        logger.info(f"[batch_results] Generating {len(ctx.pending_embeddings)} embeddings in parallel...")
        await asyncio.gather(*[
            _generate_and_save(blog_id, text)
            for blog_id, text in ctx.pending_embeddings
        ])

    # Сохраняем usage токенов в batch_usage_log и логируем стоимость
    batch_usage: dict[str, int] = cast(dict[str, int], result.get("usage")) or {}
    input_tokens = batch_usage.get("input_tokens", 0)
    output_tokens = batch_usage.get("output_tokens", 0)
    total_tokens = batch_usage.get("total_tokens", 0)
    reasoning_tokens = batch_usage.get("reasoning_tokens", 0)
    cached_tokens = batch_usage.get("cached_tokens", 0)

    # Расчёт стоимости (Batch API = 50% от стандартных цен)
    # gpt-5-mini: input $0.15/1M, cached $0.075/1M, output $0.60/1M
    # Batch скидка 50%: input $0.075/1M, cached $0.0375/1M, output $0.30/1M
    non_cached_input = input_tokens - cached_tokens
    cost_usd = (
        non_cached_input * 0.075 / 1_000_000
        + cached_tokens * 0.0375 / 1_000_000
        + output_tokens * 0.30 / 1_000_000
    )

    if total_tokens > 0:
        try:
            await db.table("batch_usage_log").insert({
                "batch_id": batch_id,
                "model": "gpt-5-mini",
                "request_count": len(results),
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": total_tokens,
                "reasoning_tokens": reasoning_tokens,
                "cached_tokens": cached_tokens,
                "cost_usd": round(cost_usd, 6),
            }).execute()
        except Exception as usage_err:
            logger.warning(f"[batch_results] Не удалось сохранить usage для {batch_id}: {usage_err}")

    tm = ctx.taxonomy_metrics
    logger.info(
        f"Batch {batch_id} processed: {len(results)} results | "
        f"categories: total={tm['categories_total']}, "
        f"matched={tm['categories_matched']}, "
        f"unmatched={tm['categories_unmatched']} | "
        f"tags: total={tm['tags_total']}, "
        f"matched={tm['tags_matched']}, "
        f"unmatched={tm['tags_unmatched']} | "
        f"taxonomy_errors={tm['taxonomy_errors']} | "
        f"tokens: {total_tokens:,} (in={input_tokens:,}, out={output_tokens:,}, "
        f"reasoning={reasoning_tokens:,}, cached={cached_tokens:,}) | "
        f"cost=${cost_usd:.4f}"
    )
