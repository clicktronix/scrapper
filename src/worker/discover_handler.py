"""Обработчик задач дискавери новых профилей."""

from typing import Any

from supabase import Client

import src.worker.handlers as _h
from src.config import Settings
from src.platforms.base import BaseScraper
from src.platforms.instagram.exceptions import AllAccountsCooldownError
from src.worker.scrape_handler import _normalize_username


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
        await _h.mark_task_failed(db, task_id, task["attempts"], task["max_attempts"],
                                  "No hashtag in payload", retry=False)
        return

    was_claimed = await _h.mark_task_running(db, task_id)
    if not was_claimed:
        _h.logger.debug(f"Task {task_id} was already claimed by another worker")
        return
    # RPC атомарно инкрементирует attempts, используем актуальное значение
    current_attempts = task["attempts"] + 1

    try:
        discovered = await scraper.discover(hashtag, min_followers)
    except AllAccountsCooldownError as e:
        await _h.mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                                  _h.sanitize_error(str(e)), retry=True)
        return
    except Exception as e:
        await _h.mark_task_failed(db, task_id, current_attempts, task["max_attempts"],
                                  _h.sanitize_error(str(e)), retry=True)
        return

    # Батчевая проверка существующих блогов (вместо N отдельных запросов)
    normalized_usernames = [_normalize_username(p.username) for p in discovered]
    existing_blogs_result = await _h.run_in_thread(
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
            if not await _h.is_blog_fresh(db, blog_id, settings.rescrape_days):
                try:
                    await _h.create_task_if_not_exists(db, blog_id, "full_scrape", priority=5)
                except Exception as e:
                    _h.logger.error(f"[discover] Failed to create rescrape task for @{profile.username}: {e}")
            continue

        # Создаём person + blog (ошибка одного профиля не ломает весь discover)
        person_id: str | None = None
        try:
            person_result = await _h.run_in_thread(
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

            blog_result = await _h.run_in_thread(
                db.table("blogs")
                .insert(blog_insert_data)
                .execute
            )
            blog_id = blog_result.data[0]["id"]

            await _h.create_task_if_not_exists(db, blog_id, "full_scrape", priority=5)
            new_count += 1
        except Exception as e:
            if person_id:
                await _h.cleanup_orphan_person(db, person_id)
            _h.logger.error(f"[discover] Failed to create profile @{profile.username}: {e}")
            continue

    await _h.mark_task_done(db, task_id)
    _h.logger.info(f"Discover #{hashtag}: found {len(discovered)}, new {new_count}")
