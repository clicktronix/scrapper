"""Обработчик задач скрапинга профилей."""

from datetime import UTC, datetime
from typing import Any, cast

from instagrapi.exceptions import UserNotFound
from loguru import logger
from supabase import Client

import src.worker.handlers as _h
from src.config import Settings
from src.models.blog import ScrapedComment, ScrapedProfile
from src.platforms.base import BaseScraper
from src.platforms.instagram.exceptions import (
    AllAccountsCooldownError,
    HikerAPIError,
    InsufficientBalanceError,
    PrivateAccountError,
)


def _as_row_dict(value: Any) -> dict[str, Any]:
    """Нормализовать JSON-строку ответа Supabase к dict."""
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    return {}


def _normalize_username(username: str) -> str:
    """Normalize Instagram username for stable deduplication."""
    return username.strip().lstrip("@").lower()


def _build_blog_data(
    profile: ScrapedProfile,
    avg_reels_views: int | None,
) -> dict[str, Any]:
    """Собрать словарь blog_data из скрапленного профиля для upsert в БД."""
    blog_data: dict[str, Any] = {
        "platform_id": profile.platform_id,
        "full_name": profile.full_name,
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
        "bio_links": [bl.model_dump() for bl in profile.bio_links],
    }
    # Опциональные поля — добавляем только если заданы
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
    if profile.external_url:
        blog_data["external_url"] = profile.external_url
    return blog_data


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

    was_claimed = await _h.mark_task_running(db, task_id)
    if not was_claimed:
        logger.debug(f"Task {task_id} was already claimed by another worker")
        return
    # RPC атомарно инкрементирует attempts, используем актуальное значение
    current_attempts = task["attempts"] + 1

    # Получаем username из blogs
    blog_result = await _h.run_in_thread(
        db.table("blogs")
        .select("username, person_id, scrape_status")
        .eq("id", blog_id).execute
    )
    if not blog_result.data:
        await _h.mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                                  "Blog not found", retry=False)
        return

    blog_row = _as_row_dict(blog_result.data[0])
    username_value = blog_row.get("username")
    if not isinstance(username_value, str) or not username_value:
        await _h.mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                                  "Blog username is missing", retry=False)
        return
    username = username_value
    person_id_raw = blog_row.get("person_id")
    person_id = person_id_raw if isinstance(person_id_raw, str) else None
    scrape_status_raw = blog_row.get("scrape_status")
    scrape_status = scrape_status_raw if isinstance(scrape_status_raw, str) else None

    # Блог деактивирован/удалён — не скрапить
    if scrape_status in ("deleted", "deactivated"):
        logger.info(f"[full_scrape] Пропуск @{username}: статус {scrape_status}")
        await _h.mark_task_done(db, task_id)
        return

    logger.debug(f"[full_scrape] Scraping @{username} (blog={blog_id})")

    # Обновить scrape_status
    await _h.run_in_thread(
        db.table("blogs").update({"scrape_status": "scraping"}).eq("id", blog_id).execute
    )

    try:
        profile = await scraper.scrape_profile(username)
    except PrivateAccountError:
        await _h.run_in_thread(
            db.table("blogs")
            .update({"scrape_status": "private", "needs_review": True})
            .eq("id", blog_id).execute
        )
        await _h.mark_task_done(db, task_id)
        return
    except UserNotFound:
        # Пользователь удалён / не найден — без retry, нужна ручная проверка
        await _h.run_in_thread(
            db.table("blogs")
            .update({"scrape_status": "deleted", "needs_review": True})
            .eq("id", blog_id).execute
        )
        await _h.mark_task_done(db, task_id)
        return
    except InsufficientBalanceError as e:
        # Нет денег на HikerAPI — ретрай бесполезен, не трогаем scrape_status блога
        logger.error(f"[full_scrape] HikerAPI баланс исчерпан: {e}")
        await _h.mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
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
            update_data["scrape_error"] = str(e)[:1000]
        await _h.run_in_thread(
            db.table("blogs").update(update_data).eq("id", blog_id).execute
        )
        await _h.mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                                  _h.sanitize_error(str(e)), retry=retry)
        return
    except AllAccountsCooldownError as e:
        await _h.run_in_thread(
            db.table("blogs").update({"scrape_status": "pending"}).eq("id", blog_id).execute
        )
        await _h.mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                                  _h.sanitize_error(str(e)), retry=True)
        return
    except Exception as e:
        # scrape_status="pending" при retry, чтобы worker мог подхватить снова
        await _h.run_in_thread(
            db.table("blogs").update({
                "scrape_status": "pending",
                "needs_review": True,
                "scrape_error": _h.sanitize_error(str(e))[:1000],
            }).eq("id", blog_id).execute
        )
        await _h.mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                                  _h.sanitize_error(str(e)), retry=True)
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
    blog_data = _build_blog_data(profile, avg_reels_views)
    # avatar_url записываем только после успешной загрузки в Storage (ниже)

    # Upsert посты и хайлайты (mode="json" для корректной сериализации datetime)
    posts_data = [p.model_dump(mode="json") for p in profile.medias]
    highlights_data = [
        h.model_dump(mode="json") for h in profile.highlights
    ]

    # Скачать CDN-изображения → загрузить в Supabase Storage → подставить постоянные URL
    try:
        avatar_storage_url, post_urls = await _h.persist_profile_images(
            db, settings.supabase_url, blog_id,
            profile.profile_pic_url, posts_data,
        )
        if avatar_storage_url:
            blog_data["avatar_url"] = avatar_storage_url
        for post in posts_data:
            pid = post.get("platform_id", "")
            if pid in post_urls:
                post["thumbnail_url"] = post_urls[pid]
            else:
                # Не удалось загрузить в Storage — не сохраняем протухающий CDN URL
                post["thumbnail_url"] = None
    except Exception as e:
        logger.warning(f"[full_scrape] @{username}: ошибка загрузки изображений в Storage: {e}")
        for post in posts_data:
            post["thumbnail_url"] = None

    try:
        logger.debug(f"[full_scrape] @{username}: upserting blog data...")
        await _h.upsert_blog(db, blog_id, blog_data)

        # Обновить full_name в persons
        if person_id and profile.full_name:
            await _h.run_in_thread(
                db.table("persons").update({"full_name": profile.full_name}).eq("id", person_id).execute
            )

        await _h.upsert_posts(db, blog_id, posts_data)
        logger.debug(f"[full_scrape] @{username}: upserted {len(posts_data)} posts/reels")

        await _h.upsert_highlights(db, blog_id, highlights_data)
        logger.debug(f"[full_scrape] @{username}: upserted {len(highlights_data)} highlights")
    except Exception as e:
        await _h.run_in_thread(
            db.table("blogs").update({
                "scrape_status": "failed",
                "scrape_error": _h.sanitize_error(str(e))[:1000],
            }).eq("id", blog_id).execute
        )
        await _h.mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                                  _h.sanitize_error(str(e)), retry=True)
        return

    # Создать задачу AI-анализа
    logger.debug(f"[full_scrape] @{username}: creating ai_analysis task...")
    await _h.create_task_if_not_exists(db, blog_id, "ai_analysis", priority=3)

    await _h.mark_task_done(db, task_id)
    logger.info(f"Full scrape done for @{username} (blog={blog_id})")
