"""Тесты общих helper-функций маппинга из mappers.py."""
from datetime import UTC, datetime

from src.platforms.instagram.mappers import (
    aggregate_story_data_from_dicts,
    extract_carousel_count,
    extract_cover_url,
    extract_video_duration,
    normalize_title,
    parse_taken_at,
)


class TestExtractVideoDuration:
    """Тесты extract_video_duration."""

    def test_video_with_positive_duration(self) -> None:
        assert extract_video_duration(2, 45.3) == 45.3

    def test_video_with_zero_duration(self) -> None:
        assert extract_video_duration(2, 0) is None

    def test_video_with_none_duration(self) -> None:
        assert extract_video_duration(2, None) is None

    def test_photo_ignores_duration(self) -> None:
        assert extract_video_duration(1, 30.0) is None

    def test_carousel_ignores_duration(self) -> None:
        assert extract_video_duration(8, 10.0) is None

    def test_video_with_negative_duration(self) -> None:
        assert extract_video_duration(2, -5.0) is None


class TestNormalizeTitle:
    """Тесты normalize_title."""

    def test_normal_title(self) -> None:
        assert normalize_title("My Reel Title") == "My Reel Title"

    def test_empty_string_becomes_none(self) -> None:
        assert normalize_title("") is None

    def test_none_becomes_none(self) -> None:
        assert normalize_title(None) is None

    def test_zero_becomes_none(self) -> None:
        assert normalize_title(0) is None

    def test_whitespace_title_kept(self) -> None:
        # Пробелы — это не пустая строка, сохраняем как есть
        assert normalize_title("  ") == "  "


class TestExtractCarouselCount:
    """Тесты extract_carousel_count."""

    def test_carousel_with_resources(self) -> None:
        assert extract_carousel_count(8, [1, 2, 3]) == 3

    def test_carousel_empty_resources(self) -> None:
        assert extract_carousel_count(8, []) is None

    def test_photo_ignores_resources(self) -> None:
        assert extract_carousel_count(1, [1, 2]) is None

    def test_video_ignores_resources(self) -> None:
        assert extract_carousel_count(2, [1]) is None

    def test_carousel_none_resources(self) -> None:
        assert extract_carousel_count(8, None) is None


class TestExtractCoverUrl:
    """Тесты extract_cover_url."""

    def test_valid_cover_media(self) -> None:
        cover_media = {"cropped_image_version": {"url": "https://example.com/cover.jpg"}}
        assert extract_cover_url(cover_media) == "https://example.com/cover.jpg"

    def test_empty_dict(self) -> None:
        assert extract_cover_url({}) is None

    def test_none_cover_media(self) -> None:
        assert extract_cover_url(None) is None

    def test_not_a_dict(self) -> None:
        assert extract_cover_url("string") is None

    def test_missing_cropped_image_version(self) -> None:
        assert extract_cover_url({"other_key": "value"}) is None

    def test_cropped_image_version_none(self) -> None:
        assert extract_cover_url({"cropped_image_version": None}) is None

    def test_cropped_image_version_empty(self) -> None:
        assert extract_cover_url({"cropped_image_version": {}}) is None


class TestParseTakenAt:
    """Тесты parse_taken_at."""

    def test_int_timestamp(self) -> None:
        result = parse_taken_at(1706400000)
        assert result.year == 2024
        assert result.tzinfo is not None

    def test_iso_string(self) -> None:
        result = parse_taken_at("2026-01-15T12:00:00+00:00")
        assert result.year == 2026
        assert result.month == 1
        assert result.day == 15

    def test_datetime_passthrough(self) -> None:
        dt = datetime(2026, 2, 1, tzinfo=UTC)
        result = parse_taken_at(dt)
        assert result is dt

    def test_none_returns_now(self) -> None:
        before = datetime.now(tz=UTC)
        result = parse_taken_at(None)
        after = datetime.now(tz=UTC)
        assert before <= result <= after

    def test_invalid_type_returns_now(self) -> None:
        before = datetime.now(tz=UTC)
        result = parse_taken_at({"invalid": True})
        after = datetime.now(tz=UTC)
        assert before <= result <= after


class TestAggregateStoryDataFromDicts:
    """Тесты aggregate_story_data_from_dicts."""

    def test_empty_items(self) -> None:
        result = aggregate_story_data_from_dicts([])
        assert result["story_mentions"] == []
        assert result["story_locations"] == []
        assert result["story_links"] == []
        assert result["story_sponsor_tags"] == []
        assert result["story_hashtags"] == []
        assert result["has_paid_partnership"] is False

    def test_mentions_extracted(self) -> None:
        items = [
            {"mentions": [{"user": {"username": "alice"}}, {"user": {"username": "bob"}}]},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["story_mentions"] == ["alice", "bob"]

    def test_mentions_deduplicated(self) -> None:
        items = [
            {"mentions": [{"user": {"username": "alice"}}]},
            {"mentions": [{"user": {"username": "alice"}}]},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["story_mentions"] == ["alice"]

    def test_locations_extracted(self) -> None:
        items = [
            {"locations": [{"location": {"name": "Алматы"}}]},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["story_locations"] == ["Алматы"]

    def test_links_from_url_key(self) -> None:
        items = [
            {"links": [{"url": "https://example.com"}]},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["story_links"] == ["https://example.com"]

    def test_links_from_weburi_key(self) -> None:
        items = [
            {"links": [{"webUri": "https://example.com/link"}]},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["story_links"] == ["https://example.com/link"]

    def test_sponsor_tags(self) -> None:
        items = [
            {"sponsor_tags": [{"username": "brandx"}, {"username": "brandy"}]},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["story_sponsor_tags"] == ["brandx", "brandy"]

    def test_paid_partnership_true(self) -> None:
        items = [
            {"is_paid_partnership": True},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["has_paid_partnership"] is True

    def test_paid_partnership_false_by_default(self) -> None:
        items = [
            {"mentions": []},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["has_paid_partnership"] is False

    def test_hashtags_from_dict(self) -> None:
        items = [
            {"hashtags": [{"hashtag": {"name": "travel"}}, {"hashtag": {"name": "food"}}]},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["story_hashtags"] == ["food", "travel"]

    def test_hashtags_from_string(self) -> None:
        items = [
            {"hashtags": [{"hashtag": "beauty"}]},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["story_hashtags"] == ["beauty"]

    def test_full_story_data(self) -> None:
        """Полный набор данных из нескольких stories."""
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
        result = aggregate_story_data_from_dicts(items)

        assert result["story_mentions"] == ["alice", "bob"]
        assert result["story_locations"] == ["Алматы"]
        assert result["story_links"] == ["https://example.com"]
        assert result["story_sponsor_tags"] == ["brandx"]
        assert result["has_paid_partnership"] is True
        assert result["story_hashtags"] == ["food", "travel"]

    def test_invalid_mention_no_user(self) -> None:
        """Mention без user → пропускается."""
        items = [
            {"mentions": [{"not_user": "val"}]},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["story_mentions"] == []

    def test_invalid_mention_not_dict(self) -> None:
        """Mention не dict → пропускается."""
        items = [
            {"mentions": ["string_mention"]},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["story_mentions"] == []

    def test_none_fields_handled(self) -> None:
        """None в полях → пропускается."""
        items = [
            {"mentions": None, "locations": None, "links": None, "sponsor_tags": None, "hashtags": None},
        ]
        result = aggregate_story_data_from_dicts(items)
        assert result["story_mentions"] == []
        assert result["story_locations"] == []
        assert result["story_links"] == []
        assert result["story_sponsor_tags"] == []
        assert result["story_hashtags"] == []
