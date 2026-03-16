"""Скрапинг Instagram-профилей через HikerAPI (SaaS-бэкенд)."""
import asyncio
from typing import Any, cast

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
    detect_likes_hidden,
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

    # Приводим к dict[str, Any] для корректной типизации
    candidate_dict = cast(dict[str, Any], candidate)
    for key in ("url", "thumbnail_url", "display_url", "src"):
        value: Any = candidate_dict.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _extract_thumbnail_url(media: dict[str, Any]) -> str | None:
    """Выбрать thumbnail URL c fallback на image_versions2/video_versions preview."""
    direct = media.get("thumbnail_url")
    if isinstance(direct, str) and direct:
        return direct

    image_versions2: Any = media.get("image_versions2")
    if isinstance(image_versions2, dict):
        image_versions2_dict = cast(dict[str, Any], image_versions2)
        candidates: Any = image_versions2_dict.get("candidates")
        if isinstance(candidates, list):
            candidates_list = cast(list[Any], candidates)
            for candidate in candidates_list:
                url = _pick_image_url(candidate)
                if url:
                    return url

    video_versions: Any = media.get("video_versions")
    if isinstance(video_versions, list):
        video_versions_list = cast(list[Any], video_versions)
        for version in video_versions_list:
            if not isinstance(version, dict):
                continue
            version_dict = cast(dict[str, Any], version)
            # Не используем video url (обычно mp4), только явный image-preview.
            for key in ("thumbnail_url", "poster_url", "image_url", "preview_url"):
                value: Any = version_dict.get(key)
                if isinstance(value, str) and value:
                    return value

    return None


def _hiker_media_to_post(media: dict[str, Any]) -> ScrapedPost:
    """Маппинг HikerAPI media dict → ScrapedPost."""
    caption = media.get("caption_text") or ""

    # Спонсоры
    sponsor_tags_raw: Any = media.get("sponsor_tags") or []
    sponsor_tags = cast(list[Any], sponsor_tags_raw)
    sponsor_usernames: list[str] = [
        cast(dict[str, Any], s)["username"] for s in sponsor_tags
        if isinstance(s, dict) and isinstance(cast(dict[str, Any], s).get("username"), str)
    ]

    # Локация
    location_raw: Any = media.get("location")
    location_name: str | None = None
    location_city: str | None = None
    location_lat: float | None = None
    location_lng: float | None = None
    if isinstance(location_raw, dict):
        location = cast(dict[str, Any], location_raw)
        location_name_val: Any = location.get("name")
        location_city_val: Any = location.get("city")
        location_lat_val: Any = location.get("lat")
        location_lng_val: Any = location.get("lng")
        location_name = str(location_name_val) if location_name_val is not None else None
        location_city = str(location_city_val) if location_city_val is not None else None
        location_lat = float(location_lat_val) if location_lat_val is not None else None
        location_lng = float(location_lng_val) if location_lng_val is not None else None

    media_type = media.get("media_type", 1)

    # Длительность видео
    video_duration = extract_video_duration(media_type, media.get("video_duration"))

    # Usertags
    usertags_list: list[str] = []
    usertags_raw: Any = media.get("usertags") or []
    for ut in cast(list[Any], usertags_raw):
        if isinstance(ut, dict):
            ut_dict = cast(dict[str, Any], ut)
            user_val: Any = ut_dict.get("user")
            if isinstance(user_val, dict):
                user_dict = cast(dict[str, Any], user_val)
                username_val: Any = user_dict.get("username")
                if username_val:
                    usertags_list.append(str(username_val))

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

    # --- Sync-обёртки для HikerAPI методов с явными типами ---
    # Необходимы, так как hikerapi не имеет полных type stubs.
    # cast(Any, self.cl) скрывает нетипизированные члены от pyright.

    def _call_user_by_username_v2(self, username: str) -> dict[str, Any]:
        """Sync-вызов HikerAPI: получить данные пользователя по username."""
        cl: Any = self.cl
        return cast(dict[str, Any], cl.user_by_username_v2(username))

    def _call_user_medias_chunk_v1(self, user_id: str) -> list[Any]:
        """Sync-вызов HikerAPI: получить chunk медиа пользователя."""
        cl: Any = self.cl
        return cast(list[Any], cl.user_medias_chunk_v1(user_id))

    def _call_user_highlights(self, user_id: str, amount: int) -> list[dict[str, Any]]:
        """Sync-вызов HikerAPI: получить хайлайты пользователя."""
        cl: Any = self.cl
        return cast(list[dict[str, Any]], cl.user_highlights(user_id, amount=amount))

    def _call_highlight_by_id_v2(self, pk: str) -> dict[str, Any]:
        """Sync-вызов HikerAPI: получить детали хайлайта по pk."""
        cl: Any = self.cl
        return cast(dict[str, Any], cl.highlight_by_id_v2(pk))

    def _call_media_comments_chunk_v1(self, media_id: str) -> list[Any]:
        """Sync-вызов HikerAPI: получить chunk комментариев к медиа."""
        cl: Any = self.cl
        return cast(list[Any], cl.media_comments_chunk_v1(media_id))

    async def scrape_profile(self, username: str) -> ScrapedProfile:
        """Полный скрапинг Instagram-профиля через HikerAPI."""
        logger.info(f"[HikerAPI] Scraping profile @{username}")

        # 1. Информация о пользователе (sync → thread)
        response_raw: dict[str, Any] = await asyncio.to_thread(
            self._call_user_by_username_v2, username
        )
        user: dict[str, Any] = cast(dict[str, Any], response_raw.get("user", {}))

        if not user or not user.get("pk"):
            msg = f"@{username} not found via HikerAPI"
            raise ValueError(msg)

        if user.get("is_private"):
            raise PrivateAccountError(f"@{username} is private")

        user_id = str(user["pk"])

        # 2+3. Медиа и хайлайты параллельно (sync → thread)
        medias_result_raw: list[Any]
        raw_highlights: list[dict[str, Any]]
        medias_result_raw, raw_highlights = await asyncio.gather(
            asyncio.to_thread(self._call_user_medias_chunk_v1, user_id),
            asyncio.to_thread(
                self._call_user_highlights, user_id, self.settings.highlights_to_fetch
            ),
        )
        # user_medias_chunk_v1 возвращает [medias_list, next_cursor]
        raw_medias: list[dict[str, Any]] = (
            cast(list[dict[str, Any]], medias_result_raw[0])
            if medias_result_raw
            else []
        )

        highlights: list[ScrapedHighlight] = []
        for hl in raw_highlights[: self.settings.highlights_to_fetch]:
            try:
                hl_pk = str(hl.get("pk", ""))
                # highlight_by_id_v2 принимает pk без prefix 'highlight:'
                hl_pk_clean = hl_pk.replace("highlight:", "")
                detail_raw: dict[str, Any] = await asyncio.to_thread(
                    self._call_highlight_by_id_v2, hl_pk_clean
                )
                # Структура: response.reels.{highlight:pk}.items
                response_part = cast(dict[str, Any], detail_raw.get("response", {}))
                reels_data = cast(dict[str, Any], response_part.get("reels", {}))
                hl_items: list[dict[str, Any]] = []
                for reel_data_raw in reels_data.values():
                    reel_data = cast(dict[str, Any], reel_data_raw)
                    hl_items = cast(list[dict[str, Any]], reel_data.get("items", []))
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
                raw_comments_raw: list[Any] = await asyncio.to_thread(
                    self._call_media_comments_chunk_v1, post.platform_id
                )
                # media_comments_chunk_v1 возвращает [comments_list, max_id, can_support_threading]
                comment_items: list[Any]
                if raw_comments_raw and isinstance(raw_comments_raw[0], list):
                    comment_items = cast(list[Any], raw_comments_raw[0])
                else:
                    comment_items = list(raw_comments_raw) if raw_comments_raw else []
                comments: list[ScrapedComment] = []
                for c in comment_items[:self.settings.comments_to_fetch]:
                    if not isinstance(c, dict):
                        continue
                    c_dict = cast(dict[str, Any], c)
                    text: str = str(c_dict.get("text", "")).strip()
                    comment_user_raw: Any = c_dict.get("user") or {}
                    comment_user = cast(dict[str, Any], comment_user_raw)
                    uname: str = str(comment_user.get("username", ""))
                    if text and uname:
                        comments.append(ScrapedComment(username=uname, text=text))
                post.top_comments = comments
            except Exception as e:
                logger.warning(f"[HikerAPI] Failed to fetch comments for {post.platform_id}: {e}")

        # Рилсы отдельно для avg_er_reels
        reels_for_er = [p for p in medias_mapped if p.media_type == 2 and p.product_type == "clips"]

        # Вычислить engagement_rate для всех медиа
        follower_count: int = int(user.get("follower_count") or 0)

        # Детекция скрытых лайков (HikerAPI возвращает like_count=3 как placeholder)
        likes_hidden = detect_likes_hidden(raw_medias, medias_mapped, follower_count)
        if likes_hidden:
            logger.info(f"[HikerAPI] @{username}: лайки скрыты, ER будет NULL")

        assign_engagement_rates(medias_mapped, follower_count)

        # Bio links
        bio_links: list[BioLink] = []
        bio_links_raw: Any = user.get("bio_links") or []
        for link_raw in cast(list[Any], bio_links_raw):
            if isinstance(link_raw, dict):
                link = cast(dict[str, Any], link_raw)
                url_val: Any = link.get("url")
                if url_val:
                    title_val: Any = link.get("title")
                    link_type_val: Any = link.get("link_type")
                    bio_links.append(BioLink(
                        url=str(url_val),
                        title=str(title_val) if title_val is not None else None,
                        link_type=str(link_type_val) if link_type_val is not None else None,
                    ))

        # Явно типизируем значения из user dict для ScrapedProfile
        username_val: Any = user.get("username", username)
        full_name_val: Any = user.get("full_name") or ""
        biography_val: Any = user.get("biography") or ""
        external_url_val: Any = user.get("external_url") or None
        following_count_val: Any = user.get("following_count", 0)
        media_count_val: Any = user.get("media_count", 0)
        is_verified_val: Any = user.get("is_verified", False)
        is_business_val: Any = user.get("is_business", False)
        business_category_val: Any = user.get("business_category_name") or user.get("category_name")
        account_type_val: Any = user.get("account_type")
        public_email_val: Any = user.get("public_email") or None
        contact_phone_val: Any = user.get("contact_phone_number") or None
        phone_country_code_val: Any = user.get("public_phone_country_code") or None
        city_name_val: Any = user.get("city_name") or None
        address_street_val: Any = user.get("address_street") or None
        profile_pic_url_val: Any = user.get("profile_pic_url")

        profile = ScrapedProfile(
            platform_id=user_id,
            username=str(username_val),
            full_name=str(full_name_val),
            biography=str(biography_val),
            external_url=str(external_url_val) if external_url_val is not None else None,
            bio_links=bio_links,
            follower_count=follower_count,
            following_count=int(following_count_val),
            media_count=int(media_count_val),
            is_verified=bool(is_verified_val),
            is_business=bool(is_business_val),
            business_category=str(business_category_val) if business_category_val is not None else None,
            account_type=int(account_type_val) if account_type_val is not None else None,
            public_email=str(public_email_val) if public_email_val is not None else None,
            contact_phone_number=str(contact_phone_val) if contact_phone_val is not None else None,
            public_phone_country_code=str(phone_country_code_val) if phone_country_code_val is not None else None,
            city_name=str(city_name_val) if city_name_val is not None else None,
            address_street=str(address_street_val) if address_street_val is not None else None,
            profile_pic_url=str(profile_pic_url_val) if profile_pic_url_val is not None else None,
            medias=medias_mapped,
            highlights=highlights,
            avg_er=None if likes_hidden else calculate_er(medias_mapped, follower_count),
            avg_er_reels=None if likes_hidden else calculate_er(reels_for_er, follower_count),
            er_trend=None if likes_hidden else calculate_er_trend(medias_mapped, follower_count),
            posts_per_week=calculate_posts_per_week(medias_mapped),
            likes_hidden=likes_hidden,
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
