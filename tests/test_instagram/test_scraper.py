"""Тесты скрапинга Instagram-профилей."""
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest


def _mock_ig_media(pk: str, media_type: int = 1, product_type: str = "feed",
                    likes: int = 100, comments: int = 10, days_ago: int = 1):
    """Мок instagrapi Media."""
    media = MagicMock()
    media.pk = pk
    media.code = f"shortcode_{pk}"
    media.media_type = media_type
    media.product_type = product_type
    media.caption_text = f"Тестовый пост #{pk} @mention"
    media.like_count = likes
    media.comment_count = comments
    media.play_count = 5000 if media_type == 2 else None
    media.view_count = 6000 if media_type == 2 else None
    media.thumbnail_url = f"https://example.com/thumb_{pk}.jpg"
    media.sponsor_tags = []
    media.location = None
    media.taken_at = datetime(2026, 1, max(1, 28 - days_ago), tzinfo=UTC)
    # Дополнительные поля для новых маппингов
    media.video_duration = 30.5 if media_type == 2 else None
    media.usertags = []
    media.accessibility_caption = None
    media.comments_disabled = False
    media.title = None
    media.resources = []
    return media


class TestMediaToScrapedPost:
    """Тесты маппинга Media → ScrapedPost."""

    def test_photo_post(self) -> None:
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("100", media_type=1, product_type="feed")
        post = media_to_scraped_post(media)

        assert post.platform_id == "100"
        assert post.shortcode == "shortcode_100"
        assert post.media_type == 1
        assert post.product_type == "feed"
        assert "#100" in post.hashtags[0]
        assert "@mention" in post.mentions[0]
        assert post.has_sponsor_tag is False
        assert post.play_count is None

    def test_reel_with_sponsor(self) -> None:
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("200", media_type=2, product_type="clips")
        sponsor = MagicMock()
        sponsor.username = "nike"
        media.sponsor_tags = [sponsor]
        post = media_to_scraped_post(media)

        assert post.product_type == "clips"
        assert post.has_sponsor_tag is True
        assert post.sponsor_brands == ["nike"]
        assert post.play_count == 5000

    def test_post_with_location(self) -> None:
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("300")
        location = MagicMock()
        location.name = "Mega Park"
        location.city = "Алматы"
        location.lat = 43.238
        location.lng = 76.945
        media.location = location
        post = media_to_scraped_post(media)

        assert post.location_name == "Mega Park"
        assert post.location_city == "Алматы"
        assert post.location_lat == pytest.approx(43.238)



class TestMediaToScrapedPostEdgeCases:
    """Крайние случаи маппинга Media → ScrapedPost."""

    def test_none_caption(self) -> None:
        """caption_text=None → пустая строка."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("400")
        media.caption_text = None
        post = media_to_scraped_post(media)

        assert post.caption_text == ""
        assert post.hashtags == []
        assert post.mentions == []

    def test_none_thumbnail_url(self) -> None:
        """thumbnail_url=None → None."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("500")
        media.thumbnail_url = None
        post = media_to_scraped_post(media)

        assert post.thumbnail_url is None

    def test_none_sponsor_tags(self) -> None:
        """sponsor_tags=None → пустой список, has_sponsor_tag=False."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("600")
        media.sponsor_tags = None
        post = media_to_scraped_post(media)

        assert post.has_sponsor_tag is False
        assert post.sponsor_brands == []

    def test_none_like_count(self) -> None:
        """like_count=None → 0."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("700")
        media.like_count = None
        media.comment_count = None
        post = media_to_scraped_post(media)

        assert post.like_count == 0
        assert post.comment_count == 0

    def test_product_type_none(self) -> None:
        """product_type=None → None (не пустая строка)."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("800")
        media.product_type = None
        post = media_to_scraped_post(media)

        assert post.product_type is None

    def test_location_without_city(self) -> None:
        """Location без атрибута city → None."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("900")
        location = MagicMock(spec=["name"])
        location.name = "Some Place"
        media.location = location
        post = media_to_scraped_post(media)

        assert post.location_name == "Some Place"
        assert post.location_city is None


class TestHighlightToScraped:
    """Тесты маппинга Highlight → ScrapedHighlight."""

    def test_empty_highlight(self) -> None:
        """Хайлайт без items → пустые списки."""
        from src.platforms.instagram.scraper import highlight_to_scraped

        hl = MagicMock()
        hl.pk = "h1"
        hl.title = "FAQ"
        hl.media_count = 0
        hl.items = []
        hl.cover_media = {}

        result = highlight_to_scraped(hl)

        assert result.platform_id == "h1"
        assert result.title == "FAQ"
        assert result.story_mentions == []
        assert result.story_locations == []
        assert result.story_links == []

    def test_highlight_with_mentions_and_links(self) -> None:
        """Хайлайт с упоминаниями и ссылками."""
        from src.platforms.instagram.scraper import highlight_to_scraped

        mention = MagicMock()
        mention.user.username = "friend"

        loc = MagicMock()
        loc.location.name = "Almaty"

        link = MagicMock()
        link.webUri = "https://example.com"

        story = MagicMock()
        story.mentions = [mention]
        story.locations = [loc]
        story.links = [link]

        hl = MagicMock()
        hl.pk = "h2"
        hl.title = "Stories"
        hl.media_count = 5
        hl.items = [story]
        hl.cover_media = {"cropped_image_version": {"url": "https://cover.jpg"}}

        result = highlight_to_scraped(hl)

        assert "friend" in result.story_mentions
        assert "Almaty" in result.story_locations
        assert "https://example.com" in result.story_links
        assert result.cover_url == "https://cover.jpg"

    def test_highlight_deduplicates(self) -> None:
        """Повторяющиеся mentions/locations/links дедуплицируются."""
        from src.platforms.instagram.scraper import highlight_to_scraped

        mention = MagicMock()
        mention.user.username = "friend"

        story1 = MagicMock()
        story1.mentions = [mention]
        story1.locations = []
        story1.links = []

        story2 = MagicMock()
        story2.mentions = [mention]  # тот же mention
        story2.locations = []
        story2.links = []

        hl = MagicMock()
        hl.pk = "h3"
        hl.title = "Dupes"
        hl.media_count = 2
        hl.items = [story1, story2]
        hl.cover_media = {}

        result = highlight_to_scraped(hl)

        # Должен быть только один mention, не два
        assert len(result.story_mentions) == 1


def _mock_ig_user(
    pk: str = "12345",
    username: str = "testuser",
    is_private: bool = False,
    follower_count: int = 50000,
    following_count: int = 500,
    media_count: int = 200,
):
    """Мок instagrapi User."""
    user = MagicMock()
    user.pk = pk
    user.username = username
    user.full_name = "Test User"
    user.biography = "Bio"
    user.is_private = is_private
    user.follower_count = follower_count
    user.following_count = following_count
    user.media_count = media_count
    user.is_verified = False
    user.is_business = True
    user.business_category_name = "Creator"
    user.category_name = None
    user.external_url = None
    user.bio_links = []
    user.profile_pic_url = "https://example.com/pic.jpg"
    # Дополнительные поля для новых маппингов
    user.account_type = None
    user.public_email = None
    user.contact_phone_number = None
    user.public_phone_country_code = None
    user.city_name = None
    user.address_street = None
    return user


class TestInstagramScraperScrapeProfile:
    """Тесты InstagramScraper.scrape_profile."""

    async def test_private_account_raises(self) -> None:
        """Приватный аккаунт → PrivateAccountError."""
        from src.platforms.instagram.exceptions import PrivateAccountError
        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()
        settings.posts_to_fetch = 20
        settings.highlights_to_fetch = 3

        private_user = _mock_ig_user(is_private=True)
        pool.safe_request = MagicMock(return_value=private_user)
        # safe_request — async, нужен AsyncMock
        from unittest.mock import AsyncMock
        pool.safe_request = AsyncMock(return_value=private_user)

        scraper = InstagramScraper(pool, settings)

        with pytest.raises(PrivateAccountError):
            await scraper.scrape_profile("private_user")

    async def test_happy_path_returns_profile(self) -> None:
        """Успешный скрап — возвращает ScrapedProfile."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()
        settings.posts_to_fetch = 20
        settings.highlights_to_fetch = 3

        user = _mock_ig_user()
        medias = [_mock_ig_media(str(i), days_ago=i) for i in range(1, 5)]
        highlights_list = []

        pool.safe_request = AsyncMock(
            side_effect=[user, medias, highlights_list]
        )

        scraper = InstagramScraper(pool, settings)
        profile = await scraper.scrape_profile("testuser")

        assert profile.username == "testuser"
        assert profile.follower_count == 50000
        assert len(profile.medias) > 0
        assert profile.avg_er_posts is not None

    async def test_zero_followers_no_er(self) -> None:
        """0 подписчиков — ER не вычисляется."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()
        settings.posts_to_fetch = 20
        settings.highlights_to_fetch = 3

        user = _mock_ig_user(follower_count=0)
        medias = [_mock_ig_media("1")]

        pool.safe_request = AsyncMock(side_effect=[user, medias, []])

        scraper = InstagramScraper(pool, settings)
        profile = await scraper.scrape_profile("testuser")

        # ER не вычислен, все посты имеют engagement_rate=None
        for p in profile.medias:
            assert p.engagement_rate is None


class TestInstagramScraperDiscover:
    """Тесты InstagramScraper.discover."""

    async def test_filters_private_accounts(self) -> None:
        """Приватные аккаунты пропускаются."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()

        # Одна медиа от приватного пользователя
        media1 = MagicMock()
        media1.user.pk = "u1"

        private_user = _mock_ig_user(pk="u1", is_private=True, follower_count=5000)

        pool.safe_request = AsyncMock(side_effect=[[media1], private_user])

        scraper = InstagramScraper(pool, settings)
        result = await scraper.discover("beauty", min_followers=1000)

        assert len(result) == 0

    async def test_filters_low_followers(self) -> None:
        """Мало подписчиков → пропускается."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()

        media1 = MagicMock()
        media1.user.pk = "u1"

        small_user = _mock_ig_user(pk="u1", follower_count=500, media_count=10)

        pool.safe_request = AsyncMock(side_effect=[[media1], small_user])

        scraper = InstagramScraper(pool, settings)
        result = await scraper.discover("beauty", min_followers=1000)

        assert len(result) == 0

    async def test_deduplicates_users(self) -> None:
        """Пользователь из нескольких медиа → только один DiscoveredProfile."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()

        # Два медиа от одного пользователя
        media1 = MagicMock()
        media1.user.pk = "u1"
        media2 = MagicMock()
        media2.user.pk = "u1"

        user = _mock_ig_user(pk="u1", follower_count=5000, media_count=10)

        pool.safe_request = AsyncMock(side_effect=[[media1, media2], user])

        scraper = InstagramScraper(pool, settings)
        result = await scraper.discover("beauty", min_followers=1000)

        assert len(result) == 1
        assert result[0].username == "testuser"

    async def test_discover_limit_20(self) -> None:
        """Максимум 20 профилей за один запуск."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()

        # 30 медиа от 30 разных пользователей
        medias = []
        side_effects = []
        for i in range(30):
            m = MagicMock()
            m.user.pk = f"u{i}"
            medias.append(m)

        side_effects.append(medias)
        for i in range(30):
            side_effects.append(
                _mock_ig_user(pk=f"u{i}", username=f"user{i}", follower_count=5000, media_count=10)
            )

        pool.safe_request = AsyncMock(side_effect=side_effects)

        scraper = InstagramScraper(pool, settings)
        result = await scraper.discover("beauty", min_followers=1000)

        assert len(result) == 20

    async def test_discover_uses_documented_top_amount(self) -> None:
        """Discover запрашивает top media с amount=9 согласно instagrapi docs."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()
        pool.safe_request = AsyncMock(return_value=[])

        scraper = InstagramScraper(pool, settings)
        await scraper.discover("beauty", min_followers=1000)

        first_call = pool.safe_request.call_args_list[0]
        assert first_call.args[1] == "beauty"
        assert first_call.args[2] == 9


class TestMediaToScrapedPostSponsorEdge:
    """Тесты: sponsor_tags с некорректными объектами."""

    def test_sponsor_tag_without_username_crashes(self) -> None:
        """Спонсор-тег без .username → AttributeError (BUG)."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("1000")
        # Спонсор-тег без атрибута username
        broken_sponsor = MagicMock(spec=[])  # spec=[] — нет атрибутов
        media.sponsor_tags = [broken_sponsor]

        # Сейчас крашится — после фикса должен работать
        post = media_to_scraped_post(media)

        assert post.has_sponsor_tag is False
        assert post.sponsor_brands == []

    def test_sponsor_tag_with_none_username_skipped(self) -> None:
        """Спонсор-тег с username=None → пропускается."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("1001")
        sponsor1 = MagicMock()
        sponsor1.username = None
        sponsor2 = MagicMock()
        sponsor2.username = "nike"
        media.sponsor_tags = [sponsor1, sponsor2]

        post = media_to_scraped_post(media)

        assert post.sponsor_brands == ["nike"]
        assert post.has_sponsor_tag is True


class TestHighlightToScrapedNoneValues:
    """BUG-10: hasattr() True для атрибутов=None → sorted() TypeError."""

    def test_mention_user_username_none_skipped(self) -> None:
        """mention.user.username=None → не добавляется в story_mentions."""
        from src.platforms.instagram.scraper import highlight_to_scraped

        mention_none = MagicMock()
        mention_none.user.username = None

        mention_ok = MagicMock()
        mention_ok.user.username = "real_user"

        story = MagicMock()
        story.mentions = [mention_none, mention_ok]
        story.locations = []
        story.links = []

        hl = MagicMock()
        hl.pk = "h10"
        hl.title = "Test"
        hl.media_count = 1
        hl.items = [story]
        hl.cover_media = {}

        result = highlight_to_scraped(hl)

        assert result.story_mentions == ["real_user"]

    def test_location_name_none_skipped(self) -> None:
        """loc.location.name=None → не добавляется в story_locations."""
        from src.platforms.instagram.scraper import highlight_to_scraped

        loc_none = MagicMock()
        loc_none.location.name = None

        loc_ok = MagicMock()
        loc_ok.location.name = "Almaty"

        story = MagicMock()
        story.mentions = []
        story.locations = [loc_none, loc_ok]
        story.links = []

        hl = MagicMock()
        hl.pk = "h11"
        hl.title = "Test"
        hl.media_count = 1
        hl.items = [story]
        hl.cover_media = {}

        result = highlight_to_scraped(hl)

        assert result.story_locations == ["Almaty"]

    def test_link_weburi_none_skipped(self) -> None:
        """link.webUri=None → str(None)='None' — некорректные данные, пропускаем."""
        from src.platforms.instagram.scraper import highlight_to_scraped

        link_none = MagicMock()
        link_none.webUri = None

        link_ok = MagicMock()
        link_ok.webUri = "https://example.com"

        story = MagicMock()
        story.mentions = []
        story.locations = []
        story.links = [link_none, link_ok]

        hl = MagicMock()
        hl.pk = "h12"
        hl.title = "Test"
        hl.media_count = 1
        hl.items = [story]
        hl.cover_media = {}

        result = highlight_to_scraped(hl)

        assert result.story_links == ["https://example.com"]

    def test_all_none_values_produce_empty_lists(self) -> None:
        """Все значения None → пустые списки (не TypeError)."""
        from src.platforms.instagram.scraper import highlight_to_scraped

        mention = MagicMock()
        mention.user.username = None

        loc = MagicMock()
        loc.location.name = None

        link = MagicMock()
        link.webUri = None

        story = MagicMock()
        story.mentions = [mention]
        story.locations = [loc]
        story.links = [link]

        hl = MagicMock()
        hl.pk = "h13"
        hl.title = "Test"
        hl.media_count = 1
        hl.items = [story]
        hl.cover_media = {}

        result = highlight_to_scraped(hl)

        assert result.story_mentions == []
        assert result.story_locations == []
        assert result.story_links == []


class TestScrapeProfileBioLinksEdge:
    """Тесты: bio_links с некорректными объектами."""

    async def test_bio_link_without_url_attribute(self) -> None:
        """Bio link без .url → AttributeError (BUG)."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()
        settings.posts_to_fetch = 20
        settings.highlights_to_fetch = 3

        user = _mock_ig_user()
        # bio_links с объектом без .url
        broken_link = MagicMock(spec=[])  # нет атрибутов
        good_link = MagicMock()
        good_link.url = "https://example.com"
        good_link.title = None
        good_link.link_type = None
        user.bio_links = [broken_link, good_link]

        medias = [_mock_ig_media("1")]
        pool.safe_request = AsyncMock(side_effect=[user, medias, []])

        scraper = InstagramScraper(pool, settings)
        profile = await scraper.scrape_profile("testuser")

        # broken_link пропущен, good_link включён
        assert len(profile.bio_links) == 1
        assert profile.bio_links[0]["url"] == "https://example.com"


class TestDiscoverEdgeCases:
    """Тесты edge case для InstagramScraper.discover."""

    async def test_media_without_user_skipped(self) -> None:
        """media.user=None → пропускается, не крашит."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()

        # Первая медиа без user, вторая с нормальным user
        media_no_user = MagicMock()
        media_no_user.user = None

        media_ok = MagicMock()
        media_ok.user.pk = "u1"

        user = _mock_ig_user(pk="u1", follower_count=5000, media_count=10)

        pool.safe_request = AsyncMock(side_effect=[[media_no_user, media_ok], user])

        scraper = InstagramScraper(pool, settings)
        result = await scraper.discover("beauty", min_followers=1000)

        # media_no_user пропущена, media_ok обработана
        assert len(result) == 1
        assert result[0].username == "testuser"

    async def test_user_media_count_below_threshold(self) -> None:
        """media_count < 5 → пользователь отфильтровывается."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()

        media1 = MagicMock()
        media1.user.pk = "u1"

        # media_count=3 — ниже порога 5
        user = _mock_ig_user(pk="u1", follower_count=5000, media_count=3)

        pool.safe_request = AsyncMock(side_effect=[[media1], user])

        scraper = InstagramScraper(pool, settings)
        result = await scraper.discover("beauty", min_followers=1000)

        assert len(result) == 0

    async def test_user_not_found_during_user_info_skipped(self) -> None:
        """UserNotFound при получении user_info → пользователь пропускается."""
        from unittest.mock import AsyncMock

        from instagrapi.exceptions import UserNotFound

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()

        media1 = MagicMock()
        media1.user.pk = "u_deleted"
        media2 = MagicMock()
        media2.user.pk = "u_ok"

        user_ok = _mock_ig_user(pk="u_ok", follower_count=5000, media_count=10)

        pool.safe_request = AsyncMock(side_effect=[
            [media1, media2],       # hashtag_medias_top
            UserNotFound("Deleted"),  # user_info для u_deleted
            user_ok,                  # user_info для u_ok
        ])

        scraper = InstagramScraper(pool, settings)
        result = await scraper.discover("beauty", min_followers=1000)

        # u_deleted пропущен, u_ok найден
        assert len(result) == 1
        assert result[0].platform_id == "u_ok"

    async def test_generic_exception_during_user_info_skipped(self) -> None:
        """Произвольная ошибка при user_info → пользователь пропускается, остальные обрабатываются."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()

        media1 = MagicMock()
        media1.user.pk = "u_error"
        media2 = MagicMock()
        media2.user.pk = "u_ok"

        user_ok = _mock_ig_user(pk="u_ok", follower_count=5000, media_count=10)

        pool.safe_request = AsyncMock(side_effect=[
            [media1, media2],
            RuntimeError("Connection timeout"),  # ошибка для u_error
            user_ok,                              # u_ok работает
        ])

        scraper = InstagramScraper(pool, settings)
        result = await scraper.discover("beauty", min_followers=1000)

        assert len(result) == 1
        assert result[0].platform_id == "u_ok"

    async def test_empty_medias_returns_empty(self) -> None:
        """Пустой список медиа → пустой результат."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()

        pool.safe_request = AsyncMock(return_value=[])

        scraper = InstagramScraper(pool, settings)
        result = await scraper.discover("nonexistent", min_followers=1000)

        assert result == []


class TestScrapeProfileHighlightFailure:
    """Тесты: highlight_info падает → fallback на partial highlight."""

    async def test_highlight_info_failure_uses_fallback(self) -> None:
        """Ошибка highlight_info → используется highlight_to_scraped(hl) без items."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()
        settings.posts_to_fetch = 20
        settings.highlights_to_fetch = 3

        user = _mock_ig_user()

        medias = [_mock_ig_media("1", days_ago=1)]

        # Мок хайлайта без items (partial data)
        hl = MagicMock()
        hl.pk = "h1"
        hl.title = "FAQ"
        hl.media_count = 5
        hl.items = []
        hl.cover_media = {}

        pool.safe_request = AsyncMock(side_effect=[
            user,
            medias,
            [hl],                              # user_highlights
            RuntimeError("Highlight API fail"),  # highlight_info
        ])

        scraper = InstagramScraper(pool, settings)
        profile = await scraper.scrape_profile("testuser")

        # Хайлайт должен быть, несмотря на ошибку highlight_info
        assert len(profile.highlights) == 1
        assert profile.highlights[0].platform_id == "h1"
        assert profile.highlights[0].title == "FAQ"

    async def test_partial_highlight_failure_doesnt_crash(self) -> None:
        """Один хайлайт OK, другой упал → оба попадают в результат."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()
        settings.posts_to_fetch = 20
        settings.highlights_to_fetch = 3

        user = _mock_ig_user()
        medias = [_mock_ig_media("1", days_ago=1)]

        # Два хайлайта
        hl1 = MagicMock()
        hl1.pk = "h1"
        hl1.title = "FAQ"
        hl1.media_count = 5
        hl1.items = []
        hl1.cover_media = {}

        hl2 = MagicMock()
        hl2.pk = "h2"
        hl2.title = "Reviews"
        hl2.media_count = 3
        hl2.items = []
        hl2.cover_media = {}

        # highlight_info для h1 OK, для h2 упадёт
        full_hl1 = MagicMock()
        full_hl1.pk = "h1"
        full_hl1.title = "FAQ"
        full_hl1.media_count = 5
        full_hl1.items = []
        full_hl1.cover_media = {}

        pool.safe_request = AsyncMock(side_effect=[
            user,
            medias,
            [hl1, hl2],                         # user_highlights
            full_hl1,                             # highlight_info(h1) — OK
            RuntimeError("API error"),            # highlight_info(h2) — fail
        ])

        scraper = InstagramScraper(pool, settings)
        profile = await scraper.scrape_profile("testuser")

        # Оба хайлайта в результате
        assert len(profile.highlights) == 2
        assert profile.highlights[0].platform_id == "h1"
        assert profile.highlights[1].platform_id == "h2"


class TestScrapeProfileEmptyMedias:
    """Тесты: scrape_profile с пустым списком медиа."""

    async def test_no_medias_returns_empty_profile(self) -> None:
        """0 медиа → профиль с пустыми medias, ER=None."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()
        settings.posts_to_fetch = 20
        settings.highlights_to_fetch = 3

        user = _mock_ig_user(follower_count=50000)

        pool.safe_request = AsyncMock(side_effect=[user, [], []])

        scraper = InstagramScraper(pool, settings)
        profile = await scraper.scrape_profile("testuser")

        assert profile.medias == []
        assert profile.avg_er_posts is None
        assert profile.avg_er_reels is None
        assert profile.er_trend is None
        assert profile.posts_per_week is None


class TestMediaToScrapedPostNewFields:
    """Тесты новых полей маппинга Media → ScrapedPost."""

    def test_video_duration_extracted(self) -> None:
        """video_duration извлекается для видео (media_type=2)."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("v1", media_type=2, product_type="clips")
        media.video_duration = 45.3
        post = media_to_scraped_post(media)

        assert post.video_duration == 45.3

    def test_video_duration_none_for_photo(self) -> None:
        """video_duration = None для фото (media_type=1)."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("v2", media_type=1)
        media.video_duration = None
        post = media_to_scraped_post(media)

        assert post.video_duration is None

    def test_video_duration_zero_skipped(self) -> None:
        """video_duration=0 → None (бесполезное значение)."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("v3", media_type=2, product_type="clips")
        media.video_duration = 0
        post = media_to_scraped_post(media)

        assert post.video_duration is None

    def test_usertags_extracted(self) -> None:
        """usertags извлекаются из media.usertags[*].user.username."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        ut1 = MagicMock()
        ut1.user.username = "tagged_user1"
        ut2 = MagicMock()
        ut2.user.username = "tagged_user2"

        media = _mock_ig_media("u1")
        media.usertags = [ut1, ut2]
        post = media_to_scraped_post(media)

        assert post.usertags == ["tagged_user1", "tagged_user2"]

    def test_usertags_none_safe(self) -> None:
        """usertags=None → пустой список."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("u2")
        media.usertags = None
        post = media_to_scraped_post(media)

        assert post.usertags == []

    def test_accessibility_caption(self) -> None:
        """accessibility_caption извлекается."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("a1")
        media.accessibility_caption = "Photo of a cat"
        post = media_to_scraped_post(media)

        assert post.accessibility_caption == "Photo of a cat"

    def test_comments_disabled(self) -> None:
        """comments_disabled передаётся."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("c1")
        media.comments_disabled = True
        post = media_to_scraped_post(media)

        assert post.comments_disabled is True

    def test_title_extracted(self) -> None:
        """title извлекается для Reels/IGTV."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("t1", media_type=2, product_type="clips")
        media.title = "My Reel Title"
        post = media_to_scraped_post(media)

        assert post.title == "My Reel Title"

    def test_empty_title_becomes_none(self) -> None:
        """Пустая строка title → None."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("t2")
        media.title = ""
        post = media_to_scraped_post(media)

        assert post.title is None

    def test_carousel_media_count(self) -> None:
        """carousel_media_count = len(resources) для альбомов (media_type=8)."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("c1", media_type=8)
        media.resources = [MagicMock(), MagicMock(), MagicMock()]
        post = media_to_scraped_post(media)

        assert post.carousel_media_count == 3

    def test_carousel_media_count_none_for_photo(self) -> None:
        """carousel_media_count=None для фото (media_type=1)."""
        from src.platforms.instagram.scraper import media_to_scraped_post

        media = _mock_ig_media("c2", media_type=1)
        post = media_to_scraped_post(media)

        assert post.carousel_media_count is None


class TestHighlightToScrapedNewFields:
    """Тесты новых полей маппинга Highlight → ScrapedHighlight."""

    def test_story_sponsor_tags(self) -> None:
        """story_sponsor_tags агрегируются из stories."""
        from src.platforms.instagram.scraper import highlight_to_scraped

        sponsor1 = MagicMock()
        sponsor1.username = "brand_a"
        sponsor2 = MagicMock()
        sponsor2.username = "brand_b"

        story1 = MagicMock()
        story1.mentions = []
        story1.locations = []
        story1.links = []
        story1.sponsor_tags = [sponsor1]
        story1.is_paid_partnership = False
        story1.story_hashtags = []

        story2 = MagicMock()
        story2.mentions = []
        story2.locations = []
        story2.links = []
        story2.sponsor_tags = [sponsor2, sponsor1]  # дубликат brand_a
        story2.is_paid_partnership = False
        story2.story_hashtags = []

        hl = MagicMock()
        hl.pk = "hs1"
        hl.title = "Sponsors"
        hl.media_count = 2
        hl.items = [story1, story2]
        hl.cover_media = {}

        result = highlight_to_scraped(hl)

        assert result.story_sponsor_tags == ["brand_a", "brand_b"]

    def test_has_paid_partnership(self) -> None:
        """has_paid_partnership=True если любая story имеет is_paid_partnership."""
        from src.platforms.instagram.scraper import highlight_to_scraped

        story1 = MagicMock()
        story1.mentions = []
        story1.locations = []
        story1.links = []
        story1.sponsor_tags = []
        story1.is_paid_partnership = False
        story1.story_hashtags = []

        story2 = MagicMock()
        story2.mentions = []
        story2.locations = []
        story2.links = []
        story2.sponsor_tags = []
        story2.is_paid_partnership = True
        story2.story_hashtags = []

        hl = MagicMock()
        hl.pk = "hp1"
        hl.title = "Paid"
        hl.media_count = 2
        hl.items = [story1, story2]
        hl.cover_media = {}

        result = highlight_to_scraped(hl)

        assert result.has_paid_partnership is True

    def test_story_hashtags(self) -> None:
        """story_hashtags агрегируются из stories."""
        from src.platforms.instagram.scraper import highlight_to_scraped

        ht1 = MagicMock()
        ht1.hashtag.name = "beauty"
        ht2 = MagicMock()
        ht2.hashtag.name = "fashion"

        story = MagicMock()
        story.mentions = []
        story.locations = []
        story.links = []
        story.sponsor_tags = []
        story.is_paid_partnership = False
        story.story_hashtags = [ht1, ht2]

        hl = MagicMock()
        hl.pk = "hh1"
        hl.title = "Tags"
        hl.media_count = 1
        hl.items = [story]
        hl.cover_media = {}

        result = highlight_to_scraped(hl)

        assert result.story_hashtags == ["beauty", "fashion"]


class TestScrapeProfileNewFields:
    """Тесты новых полей scrape_profile."""

    async def test_profile_contact_fields(self) -> None:
        """Контактные данные профиля извлекаются."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()
        settings.posts_to_fetch = 20
        settings.highlights_to_fetch = 3

        user = _mock_ig_user()
        user.account_type = 2
        user.public_email = "test@example.com"
        user.contact_phone_number = "7001234567"
        user.public_phone_country_code = "7"
        user.city_name = "Алматы"
        user.address_street = "Абая 1"

        pool.safe_request = AsyncMock(side_effect=[user, [], []])

        scraper = InstagramScraper(pool, settings)
        profile = await scraper.scrape_profile("testuser")

        assert profile.account_type == 2
        assert profile.public_email == "test@example.com"
        assert profile.contact_phone_number == "7001234567"
        assert profile.public_phone_country_code == "7"
        assert profile.city_name == "Алматы"
        assert profile.address_street == "Абая 1"

    async def test_bio_links_new_format(self) -> None:
        """bio_links извлекаются в новом формате {url, title, link_type}."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()
        settings.posts_to_fetch = 20
        settings.highlights_to_fetch = 3

        user = _mock_ig_user()
        link1 = MagicMock()
        link1.url = "https://t.me/channel"
        link1.title = "Telegram"
        link1.link_type = None
        link2 = MagicMock()
        link2.url = "https://wa.me/777"
        link2.title = None
        link2.link_type = "external"
        user.bio_links = [link1, link2]

        pool.safe_request = AsyncMock(side_effect=[user, [], []])

        scraper = InstagramScraper(pool, settings)
        profile = await scraper.scrape_profile("testuser")

        assert len(profile.bio_links) == 2
        assert profile.bio_links[0] == {"url": "https://t.me/channel", "title": "Telegram", "link_type": None}
        assert profile.bio_links[1] == {"url": "https://wa.me/777", "title": None, "link_type": "external"}


class TestDiscoverNewFields:
    """Тесты новых полей discover."""

    async def test_discover_includes_new_fields(self) -> None:
        """discover возвращает DiscoveredProfile с новыми полями."""
        from unittest.mock import AsyncMock

        from src.platforms.instagram.scraper import InstagramScraper

        pool = MagicMock()
        settings = MagicMock()

        media1 = MagicMock()
        media1.user.pk = "u1"

        user = _mock_ig_user(pk="u1", follower_count=5000, media_count=10)
        user.is_business = True
        user.is_verified = True
        user.biography = "My bio"
        user.account_type = 3

        pool.safe_request = AsyncMock(side_effect=[[media1], user])

        scraper = InstagramScraper(pool, settings)
        result = await scraper.discover("beauty", min_followers=1000)

        assert len(result) == 1
        assert result[0].is_business is True
        assert result[0].is_verified is True
        assert result[0].biography == "My bio"
        assert result[0].account_type == 3
