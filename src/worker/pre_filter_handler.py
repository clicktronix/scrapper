"""Обработчик задач pre_filter — быстрая проверка профиля перед полным скрапингом."""

import asyncio
from datetime import UTC, datetime
from typing import Any, cast

from instagrapi.exceptions import UserNotFound
from loguru import logger
from supabase import Client

import src.worker.handlers as _h
from src.config import Settings
from src.models.db_types import TaskRecord
from src.platforms.instagram.exceptions import (
    AllAccountsCooldownError,
    HikerAPIError,
    InsufficientBalanceError,
    PrivateAccountError,
)
from src.worker.scrape_handler import _normalize_username


def _extract_inserted_id(data: Any) -> str | None:
    """Достать id из ответа Supabase insert()."""
    if not isinstance(data, list) or not data:
        return None
    # cast нужен: data имеет тип Any, индексирование возвращает Unknown
    raw_first: Any = cast(list[Any], data)[0]
    if not isinstance(raw_first, dict):
        return None
    # Явное приведение после isinstance-проверки: pyright не сужает тип из Any
    first: dict[str, Any] = cast(dict[str, Any], raw_first)
    row_id: str | None = cast(str | None, first.get("id"))
    return row_id if isinstance(row_id, str) else None


async def _mark_filtered_out(
    db: Client,
    task_id: str,
    reason: str,
    *,
    username: str = "",
    platform_id: str | None = None,
    followers_count: int | None = None,
    avg_likes: float | None = None,
    latest_post_at: datetime | None = None,
    posts_count: int | None = None,
    clips_count: int | None = None,
) -> None:
    """Пометить задачу как done и записать в pre_filter_log."""
    await _h.run_in_thread(
        db.table("scrape_tasks")
        .update(
            {
                "status": "done",
                "completed_at": datetime.now(UTC).isoformat(),
                "error_message": f"filtered_out: {reason}",
            }
        )
        .eq("id", task_id)
        .execute
    )
    log_row: dict[str, Any] = {
        "username": username,
        "reason": reason,
        "task_id": task_id,
    }
    if platform_id is not None:
        log_row["platform_id"] = platform_id
    if followers_count is not None:
        log_row["followers_count"] = followers_count
    if avg_likes is not None:
        log_row["avg_likes"] = avg_likes
    if latest_post_at is not None:
        log_row["latest_post_at"] = latest_post_at.isoformat()
    if posts_count is not None:
        log_row["posts_count"] = posts_count
    if clips_count is not None:
        log_row["clips_count"] = clips_count
    try:
        await _h.run_in_thread(
            db.table("pre_filter_log")
            .upsert(log_row, on_conflict="username,reason")
            .execute
        )
    except Exception as e:
        logger.warning(f"[pre_filter] Не удалось записать лог для @{username}: {e}")


def _parse_taken_at(value: Any) -> datetime | None:
    """Преобразовать taken_at из int (unix timestamp) или str в datetime."""
    if isinstance(value, int):
        return datetime.fromtimestamp(value, tz=UTC)
    if isinstance(value, str):
        try:
            normalized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except (ValueError, TypeError):
            return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return None


async def handle_pre_filter(
    db: Client,
    task: TaskRecord,
    scraper: Any,
    settings: Settings,
) -> None:
    """
    Быстрая проверка профиля по 3 критериям:
    1. Приватный аккаунт → filtered_out: private
    2. Неактивный (нет постов или последний пост старше N дней) → filtered_out: inactive
    3. Низкая вовлечённость (avg likes < порог) → filtered_out: low_engagement

    Прошёл фильтр → создать person + blog, scrape_status="pending".
    """
    task_id = task["id"]
    payload = task.get("payload") or {}
    username = payload.get("username", "")

    if not username:
        await _h.mark_task_failed(
            db, task_id, task["attempts"], task["max_attempts"], "No username in payload", retry=False
        )
        return

    username = _normalize_username(username)

    # Проверяем, нет ли уже блога — чтобы не тратить HikerAPI запросы зря
    existing_blog = await _h.run_in_thread(
        db.table("blogs")
        .select("id")
        .eq("platform", "instagram")
        .eq("username", username)
        .execute
    )
    if existing_blog.data:
        logger.info(f"[pre_filter] @{username}: блог уже существует, пропускаем")
        await _h.mark_task_done(db, task_id)
        return

    was_claimed = await _h.mark_task_running(db, task_id)
    if not was_claimed:
        logger.debug(f"Task {task_id} was already claimed by another worker")
        return
    # RPC атомарно инкрементирует attempts, используем актуальное значение
    current_attempts = task["attempts"] + 1

    # 1. Получить информацию о пользователе
    try:
        user_info = await asyncio.to_thread(scraper.cl.user_by_username_v2, username)
    except UserNotFound:
        logger.info(f"[pre_filter] @{username}: пользователь не найден")
        await _mark_filtered_out(db, task_id, "not_found", username=username)
        return
    except PrivateAccountError:
        logger.info(f"[pre_filter] @{username}: приватный аккаунт")
        await _mark_filtered_out(db, task_id, "private", username=username)
        return
    except InsufficientBalanceError as e:
        logger.error(f"[pre_filter] HikerAPI баланс исчерпан: {e}")
        await _h.mark_task_failed(
            db, task_id, current_attempts, task["max_attempts"], "HikerAPI: insufficient balance", retry=False
        )
        return
    except HikerAPIError as e:
        # 404 = аккаунт не существует — логируем как not_found, не тратим retry
        if e.status_code == 404:
            logger.info(f"[pre_filter] @{username}: не найден (HikerAPI 404)")
            await _mark_filtered_out(db, task_id, "not_found", username=username)
            return
        retry = e.status_code in (429, 500, 502, 503, 504)
        await _h.mark_task_failed(
            db, task_id, current_attempts, task["max_attempts"], _h.sanitize_error(str(e)), retry=retry
        )
        return
    except AllAccountsCooldownError as e:
        await _h.mark_task_failed(
            db, task_id, current_attempts, task["max_attempts"], _h.sanitize_error(str(e)), retry=True
        )
        return
    except Exception as e:
        logger.exception(f"[pre_filter] @{username}: неожиданная ошибка при получении user_info")
        await _h.mark_task_failed(
            db, task_id, current_attempts, task["max_attempts"], _h.sanitize_error(str(e)), retry=True
        )
        return

    user = user_info.get("user", {})

    # Критерий 1: приватный аккаунт
    if user.get("is_private"):
        logger.info(f"[pre_filter] @{username}: приватный аккаунт")
        await _mark_filtered_out(
            db,
            task_id,
            "private",
            username=username,
            platform_id=str(user.get("pk", "")),
            followers_count=user.get("follower_count"),
        )
        return

    user_id = str(user.get("pk", ""))

    # 2. Получить последние посты и рилсы параллельно
    try:
        posts_result, clips_result = await asyncio.gather(
            asyncio.to_thread(scraper.cl.user_medias_chunk_v1, user_id),
            asyncio.to_thread(scraper.cl.user_clips_chunk_v1, user_id),
        )
    except InsufficientBalanceError as e:
        logger.error(f"[pre_filter] HikerAPI баланс исчерпан: {e}")
        await _h.mark_task_failed(
            db, task_id, current_attempts, task["max_attempts"], "HikerAPI: insufficient balance", retry=False
        )
        return
    except HikerAPIError as e:
        # 404 при запросе постов = "Entries not found" — аккаунт пустой/удалён
        if e.status_code == 404:
            logger.info(f"[pre_filter] @{username}: посты не найдены (HikerAPI 404)")
            await _mark_filtered_out(
                db,
                task_id,
                "not_found",
                username=username,
                platform_id=user_id,
                followers_count=user.get("follower_count"),
            )
            return
        retry = e.status_code in (429, 500, 502, 503, 504)
        await _h.mark_task_failed(
            db, task_id, current_attempts, task["max_attempts"], _h.sanitize_error(str(e)), retry=retry
        )
        return
    except AllAccountsCooldownError as e:
        await _h.mark_task_failed(
            db, task_id, current_attempts, task["max_attempts"], _h.sanitize_error(str(e)), retry=True
        )
        return
    except Exception as e:
        logger.exception(f"[pre_filter] @{username}: неожиданная ошибка при загрузке постов/рилсов")
        await _h.mark_task_failed(
            db, task_id, current_attempts, task["max_attempts"], _h.sanitize_error(str(e)), retry=True
        )
        return

    # Оба endpoint возвращают [medias_list, cursor]
    # cast нужен: posts_result/clips_result имеют тип Any, индексирование даёт Unknown
    raw_posts: list[dict[str, Any]] = cast(
        list[dict[str, Any]],
        posts_result[0] if isinstance(posts_result, list) and posts_result else [],
    )
    raw_clips: list[dict[str, Any]] = cast(
        list[dict[str, Any]],
        clips_result[0] if isinstance(clips_result, list) and clips_result else [],
    )
    # Объединяем посты и рилсы для проверки
    all_medias: list[dict[str, Any]] = raw_posts + raw_clips

    # Критерий 2: неактивный аккаунт (нет контента или последний пост/рилс слишком старый)
    if not all_medias:
        logger.info(f"[pre_filter] @{username}: нет постов и рилсов")
        await _mark_filtered_out(
            db,
            task_id,
            "inactive",
            username=username,
            platform_id=user_id,
            followers_count=user.get("follower_count"),
            posts_count=0,
            clips_count=0,
        )
        return

    # Находим самую свежую дату среди всех медиа
    latest_taken_at: datetime | None = None
    for media in all_medias:
        taken_at = _parse_taken_at(media.get("taken_at"))
        if taken_at and (latest_taken_at is None or taken_at > latest_taken_at):
            latest_taken_at = taken_at

    logger.debug(f"[pre_filter] @{username}: posts={len(raw_posts)}, clips={len(raw_clips)}, latest={latest_taken_at}")

    if latest_taken_at:
        days_since = (datetime.now(UTC) - latest_taken_at).days
        if days_since > settings.pre_filter_max_inactive_days:
            logger.info(f"[pre_filter] @{username}: неактивен {days_since} дней")
            await _mark_filtered_out(
                db,
                task_id,
                "inactive",
                username=username,
                platform_id=user_id,
                followers_count=user.get("follower_count"),
                latest_post_at=latest_taken_at,
                posts_count=len(raw_posts),
                clips_count=len(raw_clips),
            )
            return

    # Критерий 3: низкая вовлечённость (среднее кол-во лайков по всем медиа)
    # Instagram позволяет скрывать лайки — HikerAPI возвращает like_count=3 (фиктивное).
    # Проверяем флаг like_and_view_counts_disabled + ER-эвристику.
    likes_hidden = any(m.get("like_and_view_counts_disabled") for m in all_medias)

    posts_to_check = all_medias[: settings.pre_filter_posts_to_check]
    total_likes = sum(p.get("like_count", 0) or 0 for p in posts_to_check)
    avg_likes = total_likes / len(posts_to_check) if posts_to_check else 0

    # HikerAPI не всегда выставляет like_and_view_counts_disabled.
    # Для крупных аккаунтов (50K+) ER < 0.1% физически невозможен — лайки скрыты.
    # Для мелких (<50K) низкий ER может быть реальным (магазины, бренды с купленными подписчиками).
    followers = user.get("follower_count") or 0
    if not likes_hidden and followers >= 50_000 and avg_likes / followers < 0.001:
        likes_hidden = True
        logger.info(f"[pre_filter] @{username}: ER={avg_likes / followers:.5f} < 0.001, считаем лайки скрытыми")

    logger.debug(
        f"[pre_filter] @{username}: avg_likes={avg_likes:.0f} "
        f"(порог={settings.pre_filter_min_likes}), likes_hidden={likes_hidden}"
    )

    if avg_likes < settings.pre_filter_min_likes and not likes_hidden:
        logger.info(f"[pre_filter] @{username}: avg likes {avg_likes:.0f} < {settings.pre_filter_min_likes}")
        await _mark_filtered_out(
            db,
            task_id,
            "low_engagement",
            username=username,
            platform_id=user_id,
            followers_count=followers,
            avg_likes=avg_likes,
            latest_post_at=latest_taken_at,
            posts_count=len(raw_posts),
            clips_count=len(raw_clips),
        )
        return

    if likes_hidden:
        logger.info(f"[pre_filter] @{username}: лайки скрыты, пропускаем фильтр по engagement")

    # Профиль прошёл фильтр — создаём person + blog
    person_id: str | None = None
    try:
        full_name = user.get("full_name") or username

        person_result = await _h.run_in_thread(db.table("persons").insert({"full_name": full_name}).execute)
        person_id = _extract_inserted_id(person_result.data)
        if person_id is None:
            raise ValueError("Invalid persons insert response: missing id")

        blog_insert_data = _h.build_blog_data_from_user(
            user, person_id=person_id, username=username,
        )

        blog_result = await _h.run_in_thread(db.table("blogs").insert(blog_insert_data).execute)
        blog_id = _extract_inserted_id(blog_result.data)
        if blog_id is None:
            raise ValueError("Invalid blogs insert response: missing id")
    except Exception as e:
        if person_id:
            await _h.cleanup_orphan_person(db, person_id)
        await _h.mark_task_failed(
            db, task_id, current_attempts, task["max_attempts"], _h.sanitize_error(str(e)), retry=True
        )
        return

    await _h.mark_task_done(db, task_id)
    logger.info(f"[pre_filter] @{username}: прошёл фильтр, создан blog={blog_id}")
