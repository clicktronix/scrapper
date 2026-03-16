"""Расчёт метрик Instagram-профиля: ER, тренд, частота публикаций."""
import re
import statistics
from typing import Any

from src.models.blog import ScrapedPost
from src.models.db_types import ErTrend

# HikerAPI возвращает like_count=3 как placeholder для скрытых лайков
_HIDDEN_LIKES_PLACEHOLDER = 3
_HIDDEN_LIKES_MIN_FOLLOWERS = 50_000
_HIDDEN_LIKES_ER_THRESHOLD = 0.001  # avg_likes / followers < 0.1%


def detect_likes_hidden(
    raw_medias: list[dict[str, Any]],
    posts: list[ScrapedPost],
    follower_count: int,
) -> bool:
    """Определить, скрыты ли лайки у аккаунта.

    1) Проверяем флаг like_and_view_counts_disabled из HikerAPI.
    2) Эвристика для крупных аккаунтов (50K+):
       a) avg_likes / followers < 0.1% — невозможно для живого аккаунта
       b) > половины постов с like_count <= 3 — placeholder от HikerAPI
    """
    # Явный флаг от API
    if any(m.get("like_and_view_counts_disabled") for m in raw_medias):
        return True

    if follower_count >= _HIDDEN_LIKES_MIN_FOLLOWERS and posts:
        # Эвристика 1: слишком низкий средний ER
        avg_likes = sum(p.like_count for p in posts) / len(posts)
        if avg_likes / follower_count < _HIDDEN_LIKES_ER_THRESHOLD:
            return True

        # Эвристика 2: большинство постов с placeholder like_count=3
        # (часть постов может иметь реальные лайки, если автор скрывает лайки не на всех)
        placeholder_count = sum(
            1 for p in posts if p.like_count <= _HIDDEN_LIKES_PLACEHOLDER
        )
        if placeholder_count > len(posts) / 2:
            return True

    return False


def calculate_er(posts: list[ScrapedPost], follower_count: int) -> float | None:
    """
    ER = median(likes + comments) / followers * 100.
    Медиана вместо среднего — исключает вирусные выбросы.
    """
    if not posts or follower_count == 0:
        return None
    engagements = [p.like_count + p.comment_count for p in posts]
    median_engagement = statistics.median(engagements)
    er = round(median_engagement / follower_count * 100, 2)
    # Clamp: БД хранит er_reels как numeric(7,2), макс 99999.99; ограничиваем 999.99 как бизнес-лимит
    return min(er, 999.99)


def calculate_er_trend(
    posts: list[ScrapedPost], follower_count: int
) -> ErTrend | None:
    """
    Сравниваем ER первой половины (новые) vs второй (старые).
    Разница > 20% → 'growing' или 'declining', иначе 'stable'.
    Минимум 4 поста для анализа тренда.
    """
    if len(posts) < 4 or follower_count == 0:
        return None

    # Сортируем по дате: новые первые
    sorted_posts = sorted(posts, key=lambda p: p.taken_at, reverse=True)
    mid = len(sorted_posts) // 2
    newer = sorted_posts[:mid]
    older = sorted_posts[mid:]

    er_newer = calculate_er(newer, follower_count)
    er_older = calculate_er(older, follower_count)

    if er_newer is None or er_older is None or er_older == 0:
        return None

    change = (er_newer - er_older) / er_older
    if change > 0.2:
        return "growing"
    elif change < -0.2:
        return "declining"
    return "stable"


def calculate_posts_per_week(posts: list[ScrapedPost]) -> float | None:
    """
    Частота публикаций: кол-во постов / (период в неделях).
    Берём taken_at первого и последнего поста.
    """
    if len(posts) < 2:
        return None

    sorted_posts = sorted(posts, key=lambda p: p.taken_at)
    first = sorted_posts[0].taken_at
    last = sorted_posts[-1].taken_at
    days = (last - first).total_seconds() / 86400

    if days == 0:
        return None
    ppw = round(len(posts) / (days / 7), 2)
    # Clamp: БД хранит posts_per_week как numeric(7,2), макс 99999.99; ограничиваем 999.99 как бизнес-лимит
    return min(ppw, 999.99)


def assign_engagement_rates(posts: list[ScrapedPost], follower_count: int) -> None:
    """Рассчитать и присвоить engagement_rate каждому посту in-place."""
    if follower_count <= 0:
        return
    for p in posts:
        er = round((p.like_count + p.comment_count) / follower_count * 100, 2)
        # Clamp: blog_posts.engagement_rate — numeric(7,2); ограничиваем 999.99 как бизнес-лимит
        p.engagement_rate = min(er, 999.99)


def select_posts_for_comments(posts: list[ScrapedPost], limit: int) -> list[ScrapedPost]:
    """Выбрать первые N постов с включёнными комментариями для загрузки."""
    return [
        p for p in posts
        if not p.comments_disabled and p.comment_count > 0
    ][:limit]


def extract_hashtags(text: str) -> list[str]:
    """Извлечь хештеги из caption. Поддерживает кириллицу."""
    return re.findall(r"#[а-яА-ЯёЁa-zA-Z0-9_]+", text)


def extract_mentions(text: str) -> list[str]:
    """Извлечь упоминания (@username) из caption."""
    # Точка допускается только внутри (между word-chars), не на конце
    return re.findall(r"@[a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)*", text)
