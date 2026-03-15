"""Общие helper-функции маппинга для Instagram-скраперов (instagrapi и HikerAPI)."""
from datetime import UTC, datetime
from typing import Any, cast


def extract_video_duration(media_type: int, raw_duration: Any) -> float | None:
    """Извлечь video_duration: только для видео (media_type=2) и если > 0."""
    if media_type != 2:
        return None
    if raw_duration and raw_duration > 0:
        return float(raw_duration)
    return None


def normalize_title(raw_title: Any) -> str | None:
    """Нормализовать title: пустая строка и None → None."""
    return raw_title or None


def extract_carousel_count(media_type: int, resources: Any) -> int | None:
    """Извлечь количество элементов карусели (media_type=8)."""
    if media_type != 8:
        return None
    if resources:
        return len(resources)
    return None


def extract_cover_url(cover_media: Any) -> str | None:
    """Извлечь cover_url из cover_media dict."""
    if not isinstance(cover_media, dict):
        return None
    # Явный cast устраняет Unknown-тип после isinstance-проверки
    cover_dict = cast(dict[str, Any], cover_media)
    cropped = cast(dict[str, Any], cover_dict.get("cropped_image_version") or {})
    url = cropped.get("url")
    return str(url) if url is not None else None


def parse_taken_at(raw_value: Any) -> datetime:
    """Парсинг taken_at: int (unix timestamp), str (ISO), datetime — или fallback на now."""
    if isinstance(raw_value, datetime):
        return raw_value
    if isinstance(raw_value, int):
        return datetime.fromtimestamp(raw_value, tz=UTC)
    if isinstance(raw_value, str):
        dt = datetime.fromisoformat(raw_value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    return datetime.now(tz=UTC)


def aggregate_story_data_from_dicts(
    story_items: list[dict[str, Any]],
) -> dict[str, Any]:
    """Агрегировать данные из story items (dict-формат HikerAPI).

    Возвращает dict с ключами:
    - story_mentions: sorted list[str]
    - story_locations: sorted list[str]
    - story_links: sorted list[str]
    - story_sponsor_tags: sorted list[str]
    - story_hashtags: sorted list[str]
    - has_paid_partnership: bool
    """
    mentions: set[str] = set()
    locations: set[str] = set()
    links: set[str] = set()
    sponsor_tags: set[str] = set()
    hashtags: set[str] = set()
    has_paid_partnership = False

    for story in story_items:
        # Mentions
        for mention in cast(list[Any], story.get("mentions") or []):
            if isinstance(mention, dict):
                mention_d = cast(dict[str, Any], mention)
                user_raw = mention_d.get("user")
                if isinstance(user_raw, dict):
                    user: dict[str, Any] = cast(dict[str, Any], user_raw)
                    if user.get("username"):
                        mentions.add(str(user["username"]))

        # Locations
        for loc in cast(list[Any], story.get("locations") or []):
            if isinstance(loc, dict):
                loc_d = cast(dict[str, Any], loc)
                location_raw = loc_d.get("location")
                if isinstance(location_raw, dict):
                    location: dict[str, Any] = cast(dict[str, Any], location_raw)
                    if location.get("name"):
                        locations.add(str(location["name"]))

        # Links
        for link in cast(list[Any], story.get("links") or []):
            if isinstance(link, dict):
                link_dict = cast(dict[str, Any], link)
                raw_url = link_dict.get("webUri") or link_dict.get("url")
                if raw_url:
                    links.add(str(raw_url))

        # Sponsors
        for sponsor in cast(list[Any], story.get("sponsor_tags") or []):
            if isinstance(sponsor, dict):
                sponsor_dict = cast(dict[str, Any], sponsor)
                if sponsor_dict.get("username"):
                    sponsor_tags.add(str(sponsor_dict["username"]))

        # Paid partnership
        if story.get("is_paid_partnership"):
            has_paid_partnership = True

        # Hashtags
        for ht in cast(list[Any], story.get("hashtags") or []):
            if isinstance(ht, dict):
                ht_dict = cast(dict[str, Any], ht)
                hashtag_raw = ht_dict.get("hashtag")
                if isinstance(hashtag_raw, dict):
                    hashtag_dict = cast(dict[str, Any], hashtag_raw)
                    if hashtag_dict.get("name"):
                        hashtags.add(str(hashtag_dict["name"]))
                elif isinstance(hashtag_raw, str):
                    hashtags.add(hashtag_raw)

    return {
        "story_mentions": sorted(mentions),
        "story_locations": sorted(locations),
        "story_links": sorted(links),
        "story_sponsor_tags": sorted(sponsor_tags),
        "story_hashtags": sorted(hashtags),
        "has_paid_partnership": has_paid_partnership,
    }
