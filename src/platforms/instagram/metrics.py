"""Расчёт метрик Instagram-профиля: ER, тренд, частота публикаций."""
import re
import statistics
from typing import Literal

from src.models.blog import ScrapedPost


def calculate_er(posts: list[ScrapedPost], follower_count: int) -> float | None:
    """
    ER = median(likes + comments) / followers * 100.
    Медиана вместо среднего — исключает вирусные выбросы.
    """
    if not posts or follower_count == 0:
        return None
    engagements = [p.like_count + p.comment_count for p in posts]
    median_engagement = statistics.median(engagements)
    return round(median_engagement / follower_count * 100, 2)


def calculate_er_trend(
    posts: list[ScrapedPost], follower_count: int
) -> Literal["growing", "stable", "declining"] | None:
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
    return round(len(posts) / (days / 7), 2)


def extract_hashtags(text: str) -> list[str]:
    """Извлечь хештеги из caption. Поддерживает кириллицу."""
    return re.findall(r"#[а-яА-ЯёЁa-zA-Z0-9_]+", text)


def extract_mentions(text: str) -> list[str]:
    """Извлечь упоминания (@username) из caption."""
    # Точка допускается только внутри (между word-chars), не на конце
    return re.findall(r"@[a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)*", text)
