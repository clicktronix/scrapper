"""Тесты Pydantic-моделей скрапинга."""
from datetime import UTC, datetime

import pytest


class TestScrapedPost:
    """Тесты модели ScrapedPost."""

    def test_minimal_post(self) -> None:
        """Минимальный пост — только обязательные поля."""
        from src.models.blog import ScrapedPost

        post = ScrapedPost(
            platform_id="123456",
            media_type=1,
            taken_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        assert post.platform_id == "123456"
        assert post.caption_text == ""
        assert post.hashtags == []
        assert post.has_sponsor_tag is False
        assert post.sponsor_brands == []
        assert post.like_count == 0

    def test_full_post(self) -> None:
        """Пост со всеми полями."""
        from src.models.blog import ScrapedPost

        post = ScrapedPost(
            platform_id="789",
            shortcode="ABC123",
            media_type=2,
            product_type="clips",
            caption_text="Тестовый пост #тест @user",
            hashtags=["#тест"],
            mentions=["@user"],
            has_sponsor_tag=True,
            sponsor_brands=["nike", "adidas"],
            like_count=1500,
            comment_count=50,
            play_count=10000,
            view_count=12000,
            thumbnail_url="https://example.com/thumb.jpg",
            location_name="Алматы",
            location_city="Алматы",
            location_lat=43.238949,
            location_lng=76.945465,
            taken_at=datetime(2026, 1, 15, tzinfo=UTC),
        )
        assert post.product_type == "clips"
        assert post.sponsor_brands == ["nike", "adidas"]
        assert post.play_count == 10000


class TestScrapedHighlight:
    """Тесты модели ScrapedHighlight."""

    def test_minimal_highlight(self) -> None:
        from src.models.blog import ScrapedHighlight

        h = ScrapedHighlight(platform_id="hl_1", title="Дети")
        assert h.media_count == 0
        assert h.story_mentions == []
        assert h.story_links == []

    def test_full_highlight(self) -> None:
        from src.models.blog import ScrapedHighlight

        h = ScrapedHighlight(
            platform_id="hl_2",
            title="Путешествия",
            media_count=15,
            cover_url="https://example.com/cover.jpg",
            story_mentions=["@hotel", "@airline"],
            story_locations=["Дубай", "Стамбул"],
            story_links=["https://booking.com/123"],
        )
        assert h.story_locations == ["Дубай", "Стамбул"]


class TestScrapedProfile:
    """Тесты модели ScrapedProfile."""

    def test_minimal_profile(self) -> None:
        from src.models.blog import ScrapedProfile

        p = ScrapedProfile(platform_id="user_1", username="testuser")
        assert p.full_name == ""
        assert p.biography == ""
        assert p.follower_count == 0
        assert p.medias == []
        assert p.highlights == []
        assert p.avg_er_posts is None

    def test_profile_with_metrics(self) -> None:
        from src.models.blog import ScrapedProfile

        p = ScrapedProfile(
            platform_id="user_2",
            username="blogger",
            follower_count=50000,
            avg_er_posts=3.5,
            avg_er_reels=5.2,
            er_trend="growing",
            posts_per_week=2.5,
        )
        assert p.er_trend == "growing"
        assert p.posts_per_week == 2.5

    def test_er_trend_validation(self) -> None:
        """er_trend допускает только growing/stable/declining/None."""
        from pydantic import ValidationError

        from src.models.blog import ScrapedProfile

        with pytest.raises(ValidationError):
            ScrapedProfile(
                platform_id="user_3",
                username="bad",
                er_trend="invalid_value",
            )

    def test_json_round_trip(self) -> None:
        """Сериализация/десериализация с mode='json' для datetime."""
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="user_rt",
            username="roundtrip",
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        data = profile.model_dump(mode="json")
        restored = ScrapedProfile.model_validate(data)
        assert restored.username == "roundtrip"
        assert len(restored.medias) == 1
        assert restored.medias[0].platform_id == "p1"

    def test_profile_with_bio_links(self) -> None:
        """Профиль с bio_links в новом формате."""
        from src.models.blog import ScrapedProfile

        p = ScrapedProfile(
            platform_id="user_bl",
            username="withlinks",
            bio_links=[
                {"url": "https://t.me/channel", "title": "Telegram", "link_type": None},
                {"url": "https://wa.me/777", "title": None, "link_type": None},
            ],
        )
        assert len(p.bio_links) == 2
        assert p.bio_links[0]["url"] == "https://t.me/channel"
        assert p.bio_links[0]["title"] == "Telegram"


class TestDiscoveredProfile:
    """Тесты DiscoveredProfile."""

    def test_creation(self) -> None:
        from src.platforms.base import DiscoveredProfile

        dp = DiscoveredProfile(
            username="newuser",
            full_name="New User",
            follower_count=5000,
            platform_id="99999",
        )
        assert dp.username == "newuser"
        assert dp.follower_count == 5000


class TestExceptionHierarchy:
    """Тесты иерархии исключений."""

    def test_private_account_is_scraper_error(self) -> None:
        from src.platforms.instagram.exceptions import (
            PrivateAccountError,
            ScraperError,
        )

        assert issubclass(PrivateAccountError, ScraperError)

    def test_all_accounts_cooldown_is_scraper_error(self) -> None:
        from src.platforms.instagram.exceptions import (
            AllAccountsCooldownError,
            ScraperError,
        )

        assert issubclass(AllAccountsCooldownError, ScraperError)

    def test_private_account_caught_by_scraper_error(self) -> None:
        from src.platforms.instagram.exceptions import (
            PrivateAccountError,
            ScraperError,
        )

        with pytest.raises(ScraperError):
            raise PrivateAccountError("private account")
