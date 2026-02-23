"""Тесты расчёта метрик: ER, тренд, posts_per_week."""
from datetime import UTC, datetime, timedelta

import pytest


def _make_post(likes: int, comments: int, days_ago: float, play_count: int | None = None):
    """Хелпер для создания тестового поста."""
    from src.models.blog import ScrapedPost

    return ScrapedPost(
        platform_id=f"post_{days_ago}",
        media_type=1,
        like_count=likes,
        comment_count=comments,
        play_count=play_count,
        taken_at=datetime.now(UTC) - timedelta(days=days_ago),
    )


class TestCalculateER:
    """Тесты расчёта engagement rate."""

    def test_normal_er(self) -> None:
        """Медиана (likes+comments) / followers * 100."""
        from src.platforms.instagram.metrics import calculate_er

        posts = [
            _make_post(100, 10, 1),  # 110
            _make_post(200, 20, 2),  # 220
            _make_post(150, 15, 3),  # 165 ← медиана
        ]
        er = calculate_er(posts, follower_count=10000)
        assert er == pytest.approx(1.65, abs=0.01)

    def test_zero_followers(self) -> None:
        """followers=0 → None."""
        from src.platforms.instagram.metrics import calculate_er

        posts = [_make_post(100, 10, 1)]
        assert calculate_er(posts, follower_count=0) is None

    def test_empty_posts(self) -> None:
        """Нет постов → None."""
        from src.platforms.instagram.metrics import calculate_er

        assert calculate_er([], follower_count=10000) is None

    def test_single_post(self) -> None:
        """Один пост — медиана = сам пост."""
        from src.platforms.instagram.metrics import calculate_er

        posts = [_make_post(500, 50, 1)]
        er = calculate_er(posts, follower_count=10000)
        assert er == pytest.approx(5.5, abs=0.01)

    def test_outlier_doesnt_skew(self) -> None:
        """Вирусный пост не искажает ER благодаря медиане."""
        from src.platforms.instagram.metrics import calculate_er

        posts = [
            _make_post(100, 10, 1),     # 110
            _make_post(90, 10, 2),      # 100
            _make_post(10000, 500, 3),  # 10500 — выброс
        ]
        er = calculate_er(posts, follower_count=10000)
        assert er == pytest.approx(1.1, abs=0.01)  # median=110


class TestCalculateERTrend:
    """Тесты расчёта тренда ER."""

    def test_growing(self) -> None:
        """Новые посты ER > старых на 20%+ → 'growing'."""
        from src.platforms.instagram.metrics import calculate_er_trend

        # Новые посты (0-4 дня) — высокий ER
        new_posts = [_make_post(300, 30, i) for i in range(5)]
        # Старые посты (10-14 дней) — низкий ER
        old_posts = [_make_post(100, 10, 10 + i) for i in range(5)]
        all_posts = new_posts + old_posts
        trend = calculate_er_trend(all_posts, follower_count=10000)
        assert trend == "growing"

    def test_declining(self) -> None:
        """Новые посты ER < старых на 20%+ → 'declining'."""
        from src.platforms.instagram.metrics import calculate_er_trend

        new_posts = [_make_post(100, 10, i) for i in range(5)]
        old_posts = [_make_post(300, 30, 10 + i) for i in range(5)]
        all_posts = new_posts + old_posts
        trend = calculate_er_trend(all_posts, follower_count=10000)
        assert trend == "declining"

    def test_stable(self) -> None:
        """Разница < 20% → 'stable'."""
        from src.platforms.instagram.metrics import calculate_er_trend

        posts = [_make_post(200, 20, i) for i in range(10)]
        trend = calculate_er_trend(posts, follower_count=10000)
        assert trend == "stable"

    def test_insufficient_posts(self) -> None:
        """Меньше 4 постов → None."""
        from src.platforms.instagram.metrics import calculate_er_trend

        posts = [_make_post(200, 20, i) for i in range(3)]
        assert calculate_er_trend(posts, follower_count=10000) is None

    def test_zero_followers(self) -> None:
        """0 подписчиков → None."""
        from src.platforms.instagram.metrics import calculate_er_trend

        posts = [_make_post(200, 20, i) for i in range(5)]
        assert calculate_er_trend(posts, follower_count=0) is None

    def test_zero_older_er(self) -> None:
        """ER старых = 0 (0 лайков, 0 комментов) → None (нет деления на 0)."""
        from src.platforms.instagram.metrics import calculate_er_trend

        posts = [
            _make_post(100, 10, 1),
            _make_post(100, 10, 2),
            _make_post(0, 0, 30),
            _make_post(0, 0, 31),
        ]
        assert calculate_er_trend(posts, follower_count=10000) is None


class TestCalculatePostsPerWeek:
    """Тесты расчёта частоты публикаций."""

    def test_normal(self) -> None:
        """10 постов за 14 дней → 5 постов/неделю."""
        from src.platforms.instagram.metrics import calculate_posts_per_week

        posts = [_make_post(100, 10, i * 1.4) for i in range(10)]
        ppw = calculate_posts_per_week(posts)
        assert ppw is not None
        assert ppw > 0

    def test_single_post(self) -> None:
        """1 пост → None."""
        from src.platforms.instagram.metrics import calculate_posts_per_week

        posts = [_make_post(100, 10, 1)]
        assert calculate_posts_per_week(posts) is None

    def test_empty(self) -> None:
        """0 постов → None."""
        from src.platforms.instagram.metrics import calculate_posts_per_week

        assert calculate_posts_per_week([]) is None

    def test_same_day_posts(self) -> None:
        """Все посты в один день → None (period=0)."""
        from src.models.blog import ScrapedPost
        from src.platforms.instagram.metrics import calculate_posts_per_week

        now = datetime.now(UTC)
        posts = [
            ScrapedPost(
                platform_id=f"p{i}", media_type=1,
                like_count=10, comment_count=1, taken_at=now,
            )
            for i in range(5)
        ]
        assert calculate_posts_per_week(posts) is None

    def test_two_posts_one_week(self) -> None:
        """2 поста ровно через неделю → 2/1 = 2.0."""
        from src.platforms.instagram.metrics import calculate_posts_per_week

        posts = [_make_post(100, 10, 0), _make_post(100, 10, 7)]
        result = calculate_posts_per_week(posts)
        assert result == 2.0


class TestExtractHashtags:
    """Тесты извлечения хештегов из caption."""

    def test_cyrillic_hashtags(self) -> None:
        from src.platforms.instagram.metrics import extract_hashtags

        text = "Привет #алматы и #Казахстан!"
        assert extract_hashtags(text) == ["#алматы", "#Казахстан"]

    def test_latin_hashtags(self) -> None:
        from src.platforms.instagram.metrics import extract_hashtags

        text = "Hello #world #Python3"
        assert extract_hashtags(text) == ["#world", "#Python3"]

    def test_no_hashtags(self) -> None:
        from src.platforms.instagram.metrics import extract_hashtags

        assert extract_hashtags("Просто текст") == []

    def test_empty_string(self) -> None:
        from src.platforms.instagram.metrics import extract_hashtags

        assert extract_hashtags("") == []

    def test_hashtag_with_underscore(self) -> None:
        from src.platforms.instagram.metrics import extract_hashtags

        assert extract_hashtags("#my_tag") == ["#my_tag"]

    def test_hashtag_with_ё(self) -> None:
        """Буква ё — отдельный кейс в regex."""
        from src.platforms.instagram.metrics import extract_hashtags

        result = extract_hashtags("#ёлка #Ёлочная")
        assert result == ["#ёлка", "#Ёлочная"]

    def test_mixed_languages(self) -> None:
        """Латиница + кириллица в одном тексте."""
        from src.platforms.instagram.metrics import extract_hashtags

        result = extract_hashtags("#beauty #красота #style2026")
        assert result == ["#beauty", "#красота", "#style2026"]


class TestExtractMentions:
    """Тесты извлечения упоминаний из caption."""

    def test_mentions(self) -> None:
        from src.platforms.instagram.metrics import extract_mentions

        text = "Фото с @user.name и @user_123"
        assert extract_mentions(text) == ["@user.name", "@user_123"]

    def test_no_mentions(self) -> None:
        from src.platforms.instagram.metrics import extract_mentions

        assert extract_mentions("Просто текст") == []

    def test_empty_string(self) -> None:
        from src.platforms.instagram.metrics import extract_mentions

        assert extract_mentions("") == []


class TestCalculateERTrendEdge:
    """Дополнительные тесты тренда ER."""

    def test_odd_post_count_works(self) -> None:
        """Нечётное число постов (5) — функция не крашится, возвращает результат."""
        from src.platforms.instagram.metrics import calculate_er_trend

        # 5 постов: 2 новых с высоким ER, 3 старых с низким
        new_posts = [_make_post(300, 30, i) for i in range(2)]
        old_posts = [_make_post(100, 10, 10 + i) for i in range(3)]
        result = calculate_er_trend(new_posts + old_posts, follower_count=10000)
        assert result in ("growing", "declining", "stable")

    def test_exactly_four_posts(self) -> None:
        """Ровно 4 поста — минимально допустимое количество."""
        from src.platforms.instagram.metrics import calculate_er_trend

        posts = [_make_post(200, 20, i) for i in range(4)]
        result = calculate_er_trend(posts, follower_count=10000)
        assert result == "stable"

    def test_three_posts_returns_none(self) -> None:
        """3 поста — недостаточно, возвращает None."""
        from src.platforms.instagram.metrics import calculate_er_trend

        posts = [_make_post(200, 20, i) for i in range(3)]
        assert calculate_er_trend(posts, follower_count=10000) is None


class TestExtractHashtagsEdge:
    """Дополнительные тесты extract_hashtags."""

    def test_hashtag_with_hyphen_truncated(self) -> None:
        """Дефис не поддерживается — #алма-ата даёт только #алма."""
        from src.platforms.instagram.metrics import extract_hashtags

        result = extract_hashtags("#алма-ата")
        # Документируем текущее поведение: дефис обрезает хештег
        assert result == ["#алма"]

    def test_hashtag_with_numbers_only(self) -> None:
        """Хештег из одних цифр."""
        from src.platforms.instagram.metrics import extract_hashtags

        assert extract_hashtags("#2026") == ["#2026"]

    def test_consecutive_hashtags(self) -> None:
        """Хештеги без пробелов между ними."""
        from src.platforms.instagram.metrics import extract_hashtags

        result = extract_hashtags("#one#two#три")
        assert result == ["#one", "#two", "#три"]

    def test_hashtag_at_start_of_text(self) -> None:
        from src.platforms.instagram.metrics import extract_hashtags

        assert extract_hashtags("#first текст") == ["#first"]

    def test_bare_hash_ignored(self) -> None:
        """Одиночный # без текста не считается хештегом."""
        from src.platforms.instagram.metrics import extract_hashtags

        assert extract_hashtags("# ") == []


class TestExtractMentionsEdge:
    """Дополнительные тесты extract_mentions."""

    def test_mention_with_dots_and_underscores(self) -> None:
        from src.platforms.instagram.metrics import extract_mentions

        result = extract_mentions("@user.name_123.test")
        assert result == ["@user.name_123.test"]

    def test_mention_at_start(self) -> None:
        from src.platforms.instagram.metrics import extract_mentions

        assert extract_mentions("@first text") == ["@first"]

    def test_bare_at_sign_ignored(self) -> None:
        """Одиночный @ без текста не считается упоминанием."""
        from src.platforms.instagram.metrics import extract_mentions

        assert extract_mentions("@ ") == []


class TestExtractMentionsTrailingDot:
    """BUG-12: extract_mentions захватывает trailing dots из пунктуации."""

    def test_trailing_dot_not_included(self) -> None:
        """'Follow @user.' → '@user' (точка — пунктуация, не часть username)."""
        from src.platforms.instagram.metrics import extract_mentions

        result = extract_mentions("Follow @user.")
        assert result == ["@user"]

    def test_multiple_trailing_dots(self) -> None:
        """'@user...' → '@user' (многоточие — не часть username)."""
        from src.platforms.instagram.metrics import extract_mentions

        result = extract_mentions("cc @user...")
        assert result == ["@user"]

    def test_dot_in_middle_preserved(self) -> None:
        """'@user.name' → '@user.name' (точка внутри — часть username)."""
        from src.platforms.instagram.metrics import extract_mentions

        result = extract_mentions("@user.name is cool")
        assert result == ["@user.name"]

    def test_trailing_dot_after_internal_dots(self) -> None:
        """'@a.b.c.' → '@a.b.c' (trailing dot убирается, внутренние сохраняются)."""
        from src.platforms.instagram.metrics import extract_mentions

        result = extract_mentions("text @a.b.c. end")
        assert result == ["@a.b.c"]


class TestCalculatePostsPerWeekEdge:
    """Дополнительные тесты частоты публикаций."""

    def test_large_gap_between_posts(self) -> None:
        """2 поста за 365 дней → ~0.04 поста/неделю."""
        from src.platforms.instagram.metrics import calculate_posts_per_week

        posts = [_make_post(100, 10, 0), _make_post(100, 10, 365)]
        result = calculate_posts_per_week(posts)
        assert result is not None
        assert result < 0.1

    def test_daily_posts(self) -> None:
        """7 постов по 1 в день → 7 постов/неделю."""
        from src.platforms.instagram.metrics import calculate_posts_per_week

        posts = [_make_post(100, 10, i) for i in range(7)]
        result = calculate_posts_per_week(posts)
        assert result is not None
        # ~7 / (6/7) ≈ 8.17 (7 постов за 6-дневный период)
        assert result > 7
