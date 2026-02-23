"""Тесты HikerAPI скрапера."""
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.platforms.instagram.hiker_scraper import (
    HikerInstagramScraper,
    _hiker_highlight_to_scraped,
    _hiker_media_to_post,
)


def _mock_hiker_media(
    pk: str = "100",
    media_type: int = 1,
    product_type: str = "feed",
    likes: int = 100,
    comments: int = 10,
    taken_at: int = 1706400000,
) -> dict[str, Any]:
    """Мок HikerAPI media dict."""
    return {
        "pk": pk,
        "code": f"shortcode_{pk}",
        "media_type": media_type,
        "product_type": product_type,
        "caption_text": f"Тестовый пост #{pk} @mention",
        "like_count": likes,
        "comment_count": comments,
        "play_count": 5000 if media_type == 2 else None,
        "view_count": 6000 if media_type == 2 else None,
        "thumbnail_url": f"https://example.com/thumb_{pk}.jpg",
        "sponsor_tags": [],
        "location": None,
        "taken_at": taken_at,
        "video_duration": 30.5 if media_type == 2 else None,
        "usertags": [],
        "accessibility_caption": None,
        "comments_disabled": False,
        "title": None,
        "resources": [],
    }


def _mock_hiker_user(
    pk: str = "12345",
    username: str = "testuser",
    follower_count: int = 10000,
    is_private: bool = False,
) -> dict[str, Any]:
    """Мок HikerAPI user dict."""
    return {
        "pk": pk,
        "username": username,
        "full_name": "Test User",
        "biography": "Тестовая биография",
        "follower_count": follower_count,
        "following_count": 500,
        "media_count": 100,
        "is_verified": False,
        "is_business": True,
        "is_private": is_private,
        "business_category_name": "Blogger",
        "category_name": None,
        "account_type": 2,
        "external_url": "https://example.com",
        "bio_links": [{"url": "https://example.com", "title": "Site", "link_type": "external"}],
        "profile_pic_url": "https://example.com/pic.jpg",
        "public_email": "test@example.com",
        "contact_phone_number": None,
        "public_phone_country_code": None,
        "city_name": "Алматы",
        "address_street": None,
    }


class TestHikerMediaToPost:
    """Тесты маппинга HikerAPI media dict → ScrapedPost."""

    def test_photo_post(self) -> None:
        media = _mock_hiker_media(pk="100", media_type=1, product_type="feed")
        post = _hiker_media_to_post(media)

        assert post.platform_id == "100"
        assert post.shortcode == "shortcode_100"
        assert post.media_type == 1
        assert post.product_type == "feed"
        assert "#100" in post.hashtags[0]
        assert "@mention" in post.mentions[0]
        assert post.has_sponsor_tag is False
        assert post.play_count is None

    def test_reel_with_sponsor(self) -> None:
        media = _mock_hiker_media(pk="200", media_type=2, product_type="clips")
        media["sponsor_tags"] = [{"username": "nike"}]
        post = _hiker_media_to_post(media)

        assert post.product_type == "clips"
        assert post.has_sponsor_tag is True
        assert post.sponsor_brands == ["nike"]
        assert post.play_count == 5000

    def test_post_with_location(self) -> None:
        media = _mock_hiker_media(pk="300")
        media["location"] = {
            "name": "Mega Park",
            "city": "Алматы",
            "lat": 43.238,
            "lng": 76.945,
        }
        post = _hiker_media_to_post(media)

        assert post.location_name == "Mega Park"
        assert post.location_city == "Алматы"
        assert post.location_lat == pytest.approx(43.238)

    def test_video_duration(self) -> None:
        media = _mock_hiker_media(pk="400", media_type=2)
        post = _hiker_media_to_post(media)

        assert post.video_duration == 30.5

    def test_no_video_duration_for_photo(self) -> None:
        media = _mock_hiker_media(pk="500", media_type=1)
        post = _hiker_media_to_post(media)

        assert post.video_duration is None

    def test_usertags(self) -> None:
        media = _mock_hiker_media(pk="600")
        media["usertags"] = [
            {"user": {"username": "alice"}},
            {"user": {"username": "bob"}},
        ]
        post = _hiker_media_to_post(media)

        assert post.usertags == ["alice", "bob"]

    def test_carousel(self) -> None:
        media = _mock_hiker_media(pk="700", media_type=8)
        media["resources"] = [{"pk": "1"}, {"pk": "2"}, {"pk": "3"}]
        post = _hiker_media_to_post(media)

        assert post.carousel_media_count == 3

    def test_taken_at_int_timestamp(self) -> None:
        media = _mock_hiker_media(pk="800", taken_at=1706400000)
        post = _hiker_media_to_post(media)

        assert post.taken_at.year == 2024

    def test_empty_caption(self) -> None:
        media = _mock_hiker_media(pk="900")
        media["caption_text"] = ""
        post = _hiker_media_to_post(media)

        assert post.caption_text == ""
        assert post.hashtags == []
        assert post.mentions == []

    def test_none_caption(self) -> None:
        media = _mock_hiker_media(pk="1000")
        media["caption_text"] = None
        post = _hiker_media_to_post(media)

        assert post.caption_text == ""

    def test_empty_title_becomes_none(self) -> None:
        media = _mock_hiker_media(pk="1100")
        media["title"] = ""
        post = _hiker_media_to_post(media)

        assert post.title is None

    def test_comments_disabled(self) -> None:
        media = _mock_hiker_media(pk="1200")
        media["comments_disabled"] = True
        post = _hiker_media_to_post(media)

        assert post.comments_disabled is True

    def test_accessibility_caption(self) -> None:
        media = _mock_hiker_media(pk="1300")
        media["accessibility_caption"] = "Photo of a sunset"
        post = _hiker_media_to_post(media)

        assert post.accessibility_caption == "Photo of a sunset"

    def test_thumbnail_fallback_from_image_versions2(self) -> None:
        """Если thumbnail_url пустой, берём URL из image_versions2.candidates."""
        media = _mock_hiker_media(pk="1400")
        media["thumbnail_url"] = None
        media["image_versions2"] = {
            "candidates": [
                {"width": 320, "height": 320, "url": "https://example.com/cand_320.jpg"},
                {"width": 640, "height": 640, "url": "https://example.com/cand_640.jpg"},
            ]
        }

        post = _hiker_media_to_post(media)

        assert post.thumbnail_url == "https://example.com/cand_320.jpg"

    def test_thumbnail_fallback_from_video_versions_preview(self) -> None:
        """Если нет image_versions2, используем preview из video_versions."""
        media = _mock_hiker_media(pk="1500", media_type=2, product_type="clips")
        media["thumbnail_url"] = None
        media["image_versions2"] = None
        media["video_versions"] = [
            {"type": 101, "url": "https://example.com/video.mp4"},
            {"type": 102, "preview_url": "https://example.com/video_preview.jpg"},
        ]

        post = _hiker_media_to_post(media)

        assert post.thumbnail_url == "https://example.com/video_preview.jpg"


class TestHikerHighlightToScraped:
    """Тесты маппинга HikerAPI highlight dict → ScrapedHighlight."""

    def test_basic_highlight(self) -> None:
        highlight = {
            "pk": "18022505437841494",
            "title": "Зодиаки",
            "media_count": 13,
            "cover_media": {
                "cropped_image_version": {"url": "https://example.com/cover.jpg"},
            },
        }
        result = _hiker_highlight_to_scraped(highlight)

        assert result.platform_id == "18022505437841494"
        assert result.title == "Зодиаки"
        assert result.media_count == 13
        assert result.cover_url == "https://example.com/cover.jpg"

    def test_highlight_with_story_items(self) -> None:
        highlight = {
            "pk": "111",
            "title": "Test",
            "media_count": 2,
            "cover_media": {},
        }
        items = [
            {
                "mentions": [{"user": {"username": "alice"}}],
                "locations": [{"location": {"name": "Алматы"}}],
                "links": [{"url": "https://example.com"}],
                "sponsor_tags": [{"username": "brandx"}],
                "is_paid_partnership": True,
                "hashtags": [{"hashtag": {"name": "travel"}}],
            },
            {
                "mentions": [{"user": {"username": "bob"}}],
                "locations": [],
                "links": [],
                "sponsor_tags": [],
                "hashtags": [{"hashtag": {"name": "food"}}],
            },
        ]
        result = _hiker_highlight_to_scraped(highlight, items)

        assert result.story_mentions == ["alice", "bob"]
        assert result.story_locations == ["Алматы"]
        assert result.story_links == ["https://example.com"]
        assert result.story_sponsor_tags == ["brandx"]
        assert result.has_paid_partnership is True
        assert result.story_hashtags == ["food", "travel"]

    def test_highlight_no_cover(self) -> None:
        highlight = {
            "pk": "222",
            "title": "NoCover",
            "media_count": 0,
            "cover_media": None,
        }
        result = _hiker_highlight_to_scraped(highlight)

        assert result.cover_url is None

    def test_highlight_empty_items(self) -> None:
        highlight = {
            "pk": "333",
            "title": "Empty",
            "media_count": 0,
            "cover_media": {},
        }
        result = _hiker_highlight_to_scraped(highlight, [])

        assert result.story_mentions == []
        assert result.story_locations == []
        assert result.has_paid_partnership is False

    def test_highlight_with_weburi_links(self) -> None:
        """HikerAPI может вернуть webUri вместо url."""
        highlight = {
            "pk": "444",
            "title": "Links",
            "media_count": 1,
            "cover_media": {},
        }
        items = [
            {"mentions": [], "locations": [], "links": [{"webUri": "https://example.com/link"}],
             "sponsor_tags": [], "hashtags": []},
        ]
        result = _hiker_highlight_to_scraped(highlight, items)

        assert result.story_links == ["https://example.com/link"]


class TestHikerInstagramScraper:
    """Тесты HikerInstagramScraper.scrape_profile()."""

    @pytest.fixture
    def settings(self) -> MagicMock:
        s = MagicMock()
        s.posts_to_fetch = 20
        s.highlights_to_fetch = 2
        return s

    @pytest.fixture
    def mock_client(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def scraper(self, settings: MagicMock, mock_client: MagicMock) -> HikerInstagramScraper:
        with patch("src.platforms.instagram.hiker_scraper.SafeHikerClient", return_value=mock_client):
            return HikerInstagramScraper(token="test-token", settings=settings)

    async def test_scrape_profile_basic(
        self, scraper: HikerInstagramScraper, mock_client: MagicMock
    ) -> None:
        """Базовый тест scrape_profile — все данные корректно маппятся."""
        user = _mock_hiker_user()
        mock_client.user_by_username_v2.return_value = {"user": user, "status": "ok"}

        # 6 медиа: 4 поста + 2 рилса
        medias = [
            _mock_hiker_media("1", media_type=1, taken_at=1706400000 + i * 86400)
            for i in range(4)
        ] + [
            _mock_hiker_media(f"r{i}", media_type=2, product_type="clips", taken_at=1706400000 + i * 86400)
            for i in range(2)
        ]
        mock_client.user_medias_chunk_v1.return_value = [medias, "cursor123"]

        # Хайлайты
        mock_client.user_highlights.return_value = [
            {"pk": "h1", "title": "HL1", "media_count": 5, "cover_media": {}},
            {"pk": "h2", "title": "HL2", "media_count": 3, "cover_media": {}},
        ]
        mock_client.highlight_by_id_v2.return_value = {
            "response": {"reels": {"highlight:h1": {"items": []}}}
        }

        profile = await scraper.scrape_profile("testuser")

        assert profile.platform_id == "12345"
        assert profile.username == "testuser"
        assert profile.follower_count == 10000
        assert profile.is_business is True
        assert len(profile.medias) <= 6  # все медиа (посты + рилсы)
        assert len(profile.highlights) == 2
        assert profile.biography == "Тестовая биография"
        assert profile.business_category == "Blogger"

    async def test_scrape_profile_private_account(
        self, scraper: HikerInstagramScraper, mock_client: MagicMock
    ) -> None:
        """Приватный аккаунт должен бросать PrivateAccountError."""
        from src.platforms.instagram.exceptions import PrivateAccountError

        user = _mock_hiker_user(is_private=True)
        mock_client.user_by_username_v2.return_value = {"user": user}

        with pytest.raises(PrivateAccountError, match="is private"):
            await scraper.scrape_profile("private_user")

    async def test_scrape_profile_user_not_found(
        self, scraper: HikerInstagramScraper, mock_client: MagicMock
    ) -> None:
        """Пользователь не найден — ValueError."""
        mock_client.user_by_username_v2.return_value = {"user": {}}

        with pytest.raises(ValueError, match="not found"):
            await scraper.scrape_profile("nonexistent")

    async def test_scrape_profile_bio_links(
        self, scraper: HikerInstagramScraper, mock_client: MagicMock
    ) -> None:
        """Bio links корректно маппятся."""
        user = _mock_hiker_user()
        user["bio_links"] = [
            {"url": "https://example.com", "title": "My Site", "link_type": "external"},
            {"url": "https://t.me/test", "title": None, "link_type": None},
        ]
        mock_client.user_by_username_v2.return_value = {"user": user}
        mock_client.user_medias_chunk_v1.return_value = [[], ""]
        mock_client.user_highlights.return_value = []

        profile = await scraper.scrape_profile("testuser")

        assert len(profile.bio_links) == 2
        assert profile.bio_links[0]["url"] == "https://example.com"
        assert profile.bio_links[0]["title"] == "My Site"

    async def test_scrape_profile_engagement_rate(
        self, scraper: HikerInstagramScraper, mock_client: MagicMock
    ) -> None:
        """ER вычисляется корректно для постов."""
        user = _mock_hiker_user(follower_count=1000)
        mock_client.user_by_username_v2.return_value = {"user": user}

        # 4 поста с разными like/comment
        medias = [
            _mock_hiker_media(f"{i}", likes=50, comments=10, taken_at=1706400000 + i * 86400)
            for i in range(4)
        ]
        mock_client.user_medias_chunk_v1.return_value = [medias, ""]
        mock_client.user_highlights.return_value = []

        profile = await scraper.scrape_profile("testuser")

        # ER = median(60) / 1000 * 100 = 6.0
        assert profile.avg_er_posts == 6.0

    async def test_scrape_profile_highlight_detail_failure(
        self, scraper: HikerInstagramScraper, mock_client: MagicMock
    ) -> None:
        """При ошибке загрузки деталей хайлайта — fallback на базовый маппинг."""
        user = _mock_hiker_user()
        mock_client.user_by_username_v2.return_value = {"user": user}
        mock_client.user_medias_chunk_v1.return_value = [[], ""]
        mock_client.user_highlights.return_value = [
            {"pk": "h1", "title": "Fail", "media_count": 5, "cover_media": {}},
        ]
        mock_client.highlight_by_id_v2.side_effect = Exception("API error")

        profile = await scraper.scrape_profile("testuser")

        assert len(profile.highlights) == 1
        assert profile.highlights[0].title == "Fail"

    async def test_scrape_profile_empty_medias(
        self, scraper: HikerInstagramScraper, mock_client: MagicMock
    ) -> None:
        """Пустой список медиа — профиль без постов."""
        user = _mock_hiker_user()
        mock_client.user_by_username_v2.return_value = {"user": user}
        mock_client.user_medias_chunk_v1.return_value = [[], ""]
        mock_client.user_highlights.return_value = []

        profile = await scraper.scrape_profile("testuser")

        assert profile.medias == []
        assert profile.avg_er_posts is None

    async def test_discover_raises_not_implemented(
        self, scraper: HikerInstagramScraper
    ) -> None:
        """Discover должен бросать NotImplementedError."""
        with pytest.raises(NotImplementedError, match="не поддерживает discover"):
            await scraper.discover("travel", 1000)


class TestSafeHikerClient:
    """Тесты SafeHikerClient — проверка HTTP-статусов."""

    def test_raises_insufficient_balance_on_402(self) -> None:
        """HTTP 402 → InsufficientBalanceError."""
        from src.platforms.instagram.exceptions import InsufficientBalanceError
        from src.platforms.instagram.hiker_scraper import SafeHikerClient

        client = MagicMock(spec=SafeHikerClient)
        client._headers = {"x-access-key": "test"}
        client._timeout = 10

        mock_resp = MagicMock()
        mock_resp.status_code = 402
        mock_resp.text = "Insufficient balance"
        mock_resp.json.return_value = {"detail": "Insufficient balance"}
        mock_resp.headers = {"content-type": "application/json"}

        mock_http = MagicMock()
        mock_http.request.return_value = mock_resp
        client._client = mock_http

        with pytest.raises(InsufficientBalanceError, match="402"):
            SafeHikerClient._request(client, "GET", "/v2/user/by/username")

    def test_raises_hiker_api_error_on_429(self) -> None:
        """HTTP 429 → HikerAPIError."""
        from src.platforms.instagram.exceptions import HikerAPIError
        from src.platforms.instagram.hiker_scraper import SafeHikerClient

        client = MagicMock(spec=SafeHikerClient)
        client._headers = {"x-access-key": "test"}
        client._timeout = 10

        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_resp.text = "Rate limit"
        mock_resp.headers = {"content-type": "application/json"}

        mock_http = MagicMock()
        mock_http.request.return_value = mock_resp
        client._client = mock_http

        with pytest.raises(HikerAPIError, match="429"):
            SafeHikerClient._request(client, "GET", "/v2/user/by/username")

    def test_raises_hiker_api_error_on_500(self) -> None:
        """HTTP 500 → HikerAPIError."""
        from src.platforms.instagram.exceptions import HikerAPIError
        from src.platforms.instagram.hiker_scraper import SafeHikerClient

        client = MagicMock(spec=SafeHikerClient)
        client._headers = {"x-access-key": "test"}
        client._timeout = 10

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal server error"
        mock_resp.json.return_value = {"detail": "Internal server error"}
        mock_resp.headers = {"content-type": "application/json"}

        mock_http = MagicMock()
        mock_http.request.return_value = mock_resp
        client._client = mock_http

        with pytest.raises(HikerAPIError, match="500"):
            SafeHikerClient._request(client, "GET", "/v2/user/by/username")

    def test_returns_json_on_200(self) -> None:
        """HTTP 200 → возвращает JSON."""
        from src.platforms.instagram.hiker_scraper import SafeHikerClient

        client = MagicMock(spec=SafeHikerClient)
        client._headers = {"x-access-key": "test"}
        client._timeout = 10

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"user": {"pk": "123"}}
        mock_resp.headers = {"content-type": "application/json"}

        mock_http = MagicMock()
        mock_http.request.return_value = mock_resp
        client._client = mock_http

        result = SafeHikerClient._request(client, "GET", "/v2/user/by/username")
        assert result == {"user": {"pk": "123"}}
