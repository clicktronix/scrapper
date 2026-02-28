"""Скрапинг Instagram-профилей через HikerAPI (SaaS-бэкенд)."""
import asyncio
from typing import Any

import httpx
from hikerapi import Client
from loguru import logger

from src.config import Settings
from src.models.blog import BioLink, ScrapedComment, ScrapedHighlight, ScrapedPost, ScrapedProfile
from src.platforms.base import DiscoveredProfile
from src.platforms.instagram.exceptions import (
    HikerAPIError,
    InsufficientBalanceError,
    PrivateAccountError,
)
from src.platforms.instagram.mappers import (
    aggregate_story_data_from_dicts,
    extract_carousel_count,
    extract_cover_url,
    extract_video_duration,
    normalize_title,
    parse_taken_at,
)
from src.platforms.instagram.metrics import (
    assign_engagement_rates,
    calculate_er,
    calculate_er_trend,
    calculate_posts_per_week,
    extract_hashtags,
    extract_mentions,
    select_posts_for_comments,
)


def _pick_image_url(candidate: Any) -> str | None:
    """Извлечь URL изображения из структуры кандидата."""
    if isinstance(candidate, str):
        return candidate
    if not isinstance(candidate, dict):
        return None

    for key in ("url", "thumbnail_url", "display_url", "src"):
        value = candidate.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _extract_thumbnail_url(media: dict[str, Any]) -> str | None:
    """Выбрать thumbnail URL c fallback на image_versions2/video_versions preview."""
    direct = media.get("thumbnail_url")
    if isinstance(direct, str) and direct:
        return direct

    image_versions2 = media.get("image_versions2")
    if isinstance(image_versions2, dict):
        candidates = image_versions2.get("candidates")
        if isinstance(candidates, list):
            for candidate in candidates:
                url = _pick_image_url(candidate)
                if url:
                    return url

    video_versions = media.get("video_versions")
    if isinstance(video_versions, list):
        for version in video_versions:
            if not isinstance(version, dict):
                continue
            # Не используем video url (обычно mp4), только явный image-preview.
            for key in ("thumbnail_url", "poster_url", "image_url", "preview_url"):
                value = version.get(key)
                if isinstance(value, str) and value:
                    return value

    return None


def _hiker_media_to_post(media: dict[str, Any]) -> ScrapedPost:
    """Маппинг HikerAPI media dict → ScrapedPost."""
    caption = media.get("caption_text") or ""

    # Спонсоры
    sponsor_tags = media.get("sponsor_tags") or []
    sponsor_usernames: list[str] = [
        s["username"] for s in sponsor_tags
        if isinstance(s, dict) and isinstance(s.get("username"), str)
    ]

    # Локация
    location = media.get("location")
    location_name = None
    location_city = None
    location_lat = None
    location_lng = None
    if isinstance(location, dict):
        location_name = location.get("name")
        location_city = location.get("city")
        location_lat = location.get("lat")
        location_lng = location.get("lng")

    media_type = media.get("media_type", 1)

    # Длительность видео
    video_duration = extract_video_duration(media_type, media.get("video_duration"))

    # Usertags
    usertags_list: list[str] = []
    for ut in media.get("usertags") or []:
        if isinstance(ut, dict):
            user = ut.get("user")
            if isinstance(user, dict) and user.get("username"):
                usertags_list.append(user["username"])

    # Accessibility caption
    accessibility_caption = media.get("accessibility_caption") or None

    # Comments disabled
    comments_disabled = bool(media.get("comments_disabled", False))

    # Title
    title = normalize_title(media.get("title"))

    # Carousel
    carousel_media_count = extract_carousel_count(
        media_type, media.get("resources") or []
    )

    # taken_at — может быть int (timestamp) или str
    taken_at = parse_taken_at(media.get("taken_at"))

    return ScrapedPost(
        platform_id=str(media.get("pk", "")),
        shortcode=media.get("code"),
        media_type=media_type,
        product_type=media.get("product_type") or None,
        caption_text=caption,
        hashtags=extract_hashtags(caption),
        mentions=extract_mentions(caption),
        has_sponsor_tag=bool(sponsor_usernames),
        sponsor_brands=sponsor_usernames,
        like_count=media.get("like_count") or 0,
        comment_count=media.get("comment_count") or 0,
        play_count=media.get("play_count"),
        view_count=media.get("view_count"),
        thumbnail_url=_extract_thumbnail_url(media),
        location_name=location_name,
        location_city=location_city,
        location_lat=location_lat,
        location_lng=location_lng,
        taken_at=taken_at,
        video_duration=video_duration,
        usertags=usertags_list,
        accessibility_caption=accessibility_caption,
        comments_disabled=comments_disabled,
        title=title,
        carousel_media_count=carousel_media_count,
    )


def _hiker_highlight_to_scraped(
    highlight: dict[str, Any],
    items: list[dict[str, Any]] | None = None,
) -> ScrapedHighlight:
    """Маппинг HikerAPI highlight dict → ScrapedHighlight."""
    # items могут быть в самом highlight или переданы отдельно (из highlight_by_id_v2)
    story_items = items if items is not None else (highlight.get("items") or [])

    # Агрегация данных из story items через общий helper
    story_data = aggregate_story_data_from_dicts(story_items)

    # Cover URL
    cover_url = extract_cover_url(highlight.get("cover_media"))

    return ScrapedHighlight(
        platform_id=str(highlight.get("pk", "")),
        title=highlight.get("title", ""),
        media_count=highlight.get("media_count", 0),
        cover_url=cover_url,
        story_mentions=story_data["story_mentions"],
        story_locations=story_data["story_locations"],
        story_links=story_data["story_links"],
        story_sponsor_tags=story_data["story_sponsor_tags"],
        has_paid_partnership=story_data["has_paid_partnership"],
        story_hashtags=story_data["story_hashtags"],
    )


class SafeHikerClient(Client):
    """Client с проверкой HTTP-статусов (базовый Client их игнорирует)."""

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> Any:
        if params:
            params = {k: v for k, v in params.items() if v is not None}
        resp: httpx.Response = self._client.request(
            method,
            path,
            headers=self._headers | (headers or {}),
            params=params,
            data=data,
            json=json,
            timeout=self._timeout,
        )
        # Проверяем HTTP-статус до парсинга JSON
        if resp.status_code == 402:
            detail = ""
            try:
                detail = resp.json().get("detail", resp.text)
            except Exception:
                detail = resp.text
            raise InsufficientBalanceError(
                f"HikerAPI: недостаточно средств (HTTP 402). {detail}"
            )
        if resp.status_code == 429:
            raise HikerAPIError(429, "rate limit exceeded")
        if resp.status_code >= 400:
            detail = ""
            try:
                detail = resp.json().get("detail", resp.text)
            except Exception:
                detail = resp.text
            raise HikerAPIError(resp.status_code, detail)

        if "json" in resp.headers.get("content-type", "").lower():
            return resp.json()
        return resp.content


class HikerInstagramScraper:
    """Скрапер Instagram через HikerAPI (SaaS от subzeroid)."""

    def __init__(self, token: str, settings: Settings) -> None:
        self.cl = SafeHikerClient(token=token)
        self.settings = settings

    async def scrape_profile(self, username: str) -> ScrapedProfile:
        """Полный скрапинг Instagram-профиля через HikerAPI."""
        logger.info(f"[HikerAPI] Scraping profile @{username}")

        # 1. Информация о пользователе (sync → thread)
        response = await asyncio.to_thread(self.cl.user_by_username_v2, username)
        user = response.get("user", {})

        if not user or not user.get("pk"):
            msg = f"@{username} not found via HikerAPI"
            raise ValueError(msg)

        if user.get("is_private"):
            raise PrivateAccountError(f"@{username} is private")

        user_id = str(user["pk"])

        # 2+3. Медиа и хайлайты параллельно (sync → thread)
        result, raw_highlights = await asyncio.gather(
            asyncio.to_thread(self.cl.user_medias_chunk_v1, user_id),
            asyncio.to_thread(
                self.cl.user_highlights, user_id, amount=self.settings.highlights_to_fetch
            ),
        )
        raw_medias: list[dict[str, Any]] = result[0] if isinstance(result, list) and result else []
        highlights: list[ScrapedHighlight] = []
        for hl in raw_highlights[: self.settings.highlights_to_fetch]:
            try:
                hl_pk = str(hl.get("pk", ""))
                # highlight_by_id_v2 принимает pk без prefix 'highlight:'
                hl_pk_clean = hl_pk.replace("highlight:", "")
                detail = await asyncio.to_thread(self.cl.highlight_by_id_v2, hl_pk_clean)
                # Структура: response.reels.{highlight:pk}.items
                reels_data = detail.get("response", {}).get("reels", {})
                hl_items: list[dict[str, Any]] = []
                for reel_data in reels_data.values():
                    hl_items = reel_data.get("items", [])
                    break  # берём первый (единственный) reel
                highlights.append(_hiker_highlight_to_scraped(hl, hl_items))
            except Exception as e:
                logger.warning(f"[HikerAPI] Failed to fetch highlight {hl.get('pk')}: {e}")
                highlights.append(_hiker_highlight_to_scraped(hl))

        # 4. Маппинг — все медиа в один список (без разделения на посты/рилсы)
        medias_mapped = [_hiker_media_to_post(m) for m in raw_medias]

        # 5. Комментарии для первых N постов с включёнными комментариями
        posts_for_comments = select_posts_for_comments(medias_mapped, self.settings.posts_with_comments)

        for post in posts_for_comments:
            try:
                raw_comments = await asyncio.to_thread(
                    self.cl.media_comments_chunk_v1, post.platform_id
                )
                comments: list[ScrapedComment] = []
                for c in (raw_comments or [])[:self.settings.comments_to_fetch]:
                    text = c.get("text", "").strip()
                    comment_user = c.get("user") or {}
                    uname = comment_user.get("username", "")
                    if text and uname:
                        comments.append(ScrapedComment(username=uname, text=text))
                post.top_comments = comments
            except Exception as e:
                logger.warning(f"[HikerAPI] Failed to fetch comments for {post.platform_id}: {e}")

        # Рилсы отдельно для avg_er_reels
        reels_for_er = [p for p in medias_mapped if p.media_type == 2 and p.product_type == "clips"]

        # Вычислить engagement_rate для всех медиа
        follower_count = user.get("follower_count", 0)
        assign_engagement_rates(medias_mapped, follower_count)

        # Bio links
        bio_links: list[BioLink] = []
        for link in user.get("bio_links") or []:
            if isinstance(link, dict):
                url = link.get("url")
                if url:
                    bio_links.append(BioLink(
                        url=str(url),
                        title=link.get("title") or None,
                        link_type=link.get("link_type") or None,
                    ))

        profile = ScrapedProfile(
            platform_id=user_id,
            username=user.get("username", username),
            full_name=user.get("full_name") or "",
            biography=user.get("biography") or "",
            external_url=user.get("external_url") or None,
            bio_links=bio_links,
            follower_count=follower_count,
            following_count=user.get("following_count", 0),
            media_count=user.get("media_count", 0),
            is_verified=user.get("is_verified", False),
            is_business=user.get("is_business", False),
            business_category=user.get("business_category_name") or user.get("category_name"),
            account_type=user.get("account_type"),
            public_email=user.get("public_email") or None,
            contact_phone_number=user.get("contact_phone_number") or None,
            public_phone_country_code=user.get("public_phone_country_code") or None,
            city_name=user.get("city_name") or None,
            address_street=user.get("address_street") or None,
            profile_pic_url=user.get("profile_pic_url"),
            medias=medias_mapped,
            highlights=highlights,
            avg_er=calculate_er(medias_mapped, follower_count),
            avg_er_reels=calculate_er(reels_for_er, follower_count),
            er_trend=calculate_er_trend(medias_mapped, follower_count),
            posts_per_week=calculate_posts_per_week(medias_mapped),
        )

        logger.info(
            f"[HikerAPI] Scraped @{username}: {len(medias_mapped)} publications, "
            f"{len(highlights)} highlights, ER={profile.avg_er}"
        )
        return profile

    async def discover(
        self, query: str, min_followers: int
    ) -> list[DiscoveredProfile]:
        """Discover через HikerAPI — не поддерживается полноценно."""
        raise NotImplementedError(
            "HikerAPI бэкенд не поддерживает discover. "
            "Используйте instagrapi бэкенд для discover-задач."
        )
