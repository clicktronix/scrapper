"""Скрапинг Instagram-профилей через instagrapi."""
from typing import Any

from instagrapi.exceptions import UserNotFound
from loguru import logger

from src.config import Settings
from src.models.blog import ScrapedComment, ScrapedHighlight, ScrapedPost, ScrapedProfile
from src.platforms.base import DiscoveredProfile
from src.platforms.instagram.client import AccountPool
from src.platforms.instagram.exceptions import PrivateAccountError
from src.platforms.instagram.metrics import (
    calculate_er,
    calculate_er_trend,
    calculate_posts_per_week,
    extract_hashtags,
    extract_mentions,
)

TOP_HASHTAG_MEDIAS_AMOUNT = 9


def media_to_scraped_post(media: Any) -> ScrapedPost:
    """Маппинг instagrapi Media → ScrapedPost."""
    caption = media.caption_text or ""
    sponsor_usernames = [
        name for s in (media.sponsor_tags or [])
        if (name := getattr(s, "username", None))
    ]

    location_name = None
    location_city = None
    location_lat = None
    location_lng = None
    if media.location:
        location_name = media.location.name
        location_city = getattr(media.location, "city", None)
        location_lat = getattr(media.location, "lat", None)
        location_lng = getattr(media.location, "lng", None)

    # Извлечение дополнительных полей
    video_duration = None
    if media.media_type == 2:
        dur = getattr(media, "video_duration", None)
        if dur and dur > 0:
            video_duration = float(dur)

    usertags_list = []
    for ut in getattr(media, "usertags", []) or []:
        username = getattr(getattr(ut, "user", None), "username", None)
        if username:
            usertags_list.append(username)

    accessibility_caption = getattr(media, "accessibility_caption", None) or None

    comments_disabled = bool(getattr(media, "comments_disabled", False))

    title = getattr(media, "title", None) or None
    if title == "":
        title = None

    carousel_media_count = None
    if media.media_type == 8:
        resources = getattr(media, "resources", []) or []
        if resources:
            carousel_media_count = len(resources)

    return ScrapedPost(
        platform_id=str(media.pk),
        shortcode=getattr(media, "code", None),
        media_type=media.media_type,
        product_type=media.product_type or None,
        caption_text=caption,
        hashtags=extract_hashtags(caption),
        mentions=extract_mentions(caption),
        has_sponsor_tag=bool(sponsor_usernames),
        sponsor_brands=sponsor_usernames,
        like_count=media.like_count or 0,
        comment_count=media.comment_count or 0,
        play_count=media.play_count,
        view_count=media.view_count,
        thumbnail_url=str(media.thumbnail_url) if media.thumbnail_url else None,
        location_name=location_name,
        location_city=location_city,
        location_lat=location_lat,
        location_lng=location_lng,
        taken_at=media.taken_at,
        video_duration=video_duration,
        usertags=usertags_list,
        accessibility_caption=accessibility_caption,
        comments_disabled=comments_disabled,
        title=title,
        carousel_media_count=carousel_media_count,
    )


def highlight_to_scraped(highlight: Any) -> ScrapedHighlight:
    """Маппинг instagrapi Highlight → ScrapedHighlight."""
    story_mentions: set[str] = set()
    story_locations: set[str] = set()
    story_links: set[str] = set()
    story_sponsor_tags: set[str] = set()
    story_hashtags: set[str] = set()
    has_paid_partnership = False

    for story in getattr(highlight, "items", []):
        for mention in getattr(story, "mentions", []):
            if hasattr(mention, "user") and getattr(mention.user, "username", None):
                story_mentions.add(mention.user.username)
        for loc in getattr(story, "locations", []):
            if hasattr(loc, "location") and getattr(loc.location, "name", None):
                story_locations.add(loc.location.name)
        for link in getattr(story, "links", []):
            if getattr(link, "webUri", None):
                story_links.add(str(link.webUri))
        # Спонсоры из stories
        for sponsor in getattr(story, "sponsor_tags", []) or []:
            username = getattr(sponsor, "username", None)
            if username:
                story_sponsor_tags.add(username)
        # Paid partnership
        if getattr(story, "is_paid_partnership", False):
            has_paid_partnership = True
        # Хештеги из stories
        for ht in getattr(story, "story_hashtags", []) or []:
            hashtag = getattr(ht, "hashtag", None)
            if hashtag:
                name = getattr(hashtag, "name", None)
                if name:
                    story_hashtags.add(name)

    # Извлечение cover_url из cover_media dict
    cover_url = None
    cover_media = getattr(highlight, "cover_media", {})
    if isinstance(cover_media, dict):
        cropped = cover_media.get("cropped_image_version") or {}
        cover_url = cropped.get("url")

    return ScrapedHighlight(
        platform_id=str(highlight.pk),
        title=highlight.title,
        media_count=getattr(highlight, "media_count", 0),
        cover_url=cover_url,
        story_mentions=sorted(story_mentions),
        story_locations=sorted(story_locations),
        story_links=sorted(story_links),
        story_sponsor_tags=sorted(story_sponsor_tags),
        has_paid_partnership=has_paid_partnership,
        story_hashtags=sorted(story_hashtags),
    )


# --- Функции-обёртки для safe_request ---
# safe_request вызывает func(client, *args), поэтому первый аргумент — client

def _user_info_by_username(client: Any, username: str) -> Any:
    return client.user_info_by_username(username)


def _user_medias(client: Any, user_id: str, amount: int) -> Any:
    return client.user_medias(user_id, amount)


def _user_highlights(client: Any, user_id: str) -> Any:
    return client.user_highlights(user_id)


def _highlight_info(client: Any, pk: int) -> Any:
    return client.highlight_info(pk)


def _media_comments(client: Any, media_id: str, amount: int) -> Any:
    return client.media_comments(media_id, amount=amount)


def _hashtag_medias_top(client: Any, hashtag: str, amount: int) -> Any:
    return client.hashtag_medias_top(hashtag, amount=amount)


def _user_info(client: Any, user_pk: str) -> Any:
    return client.user_info(user_pk)


class InstagramScraper:
    """Скрапер Instagram через instagrapi + safe_request."""

    def __init__(self, pool: AccountPool, settings: Settings) -> None:
        self.pool = pool
        self.settings = settings

    async def scrape_profile(self, username: str) -> ScrapedProfile:
        """Полный скрапинг Instagram-профиля через safe_request."""
        logger.info(f"Scraping profile @{username}")

        # 1. Получить информацию о пользователе
        user = await self.pool.safe_request(_user_info_by_username, username)

        # Проверка приватности
        if user.is_private:
            raise PrivateAccountError(f"@{username} is private")

        # 2. Получить медиа
        medias = await self.pool.safe_request(
            _user_medias, str(user.pk), self.settings.posts_to_fetch
        )

        # 3. Получить хайлайты
        raw_highlights = await self.pool.safe_request(
            _user_highlights, str(user.pk)
        )

        # Загрузить детали первых N хайлайтов
        highlights: list[ScrapedHighlight] = []
        for hl in raw_highlights[: self.settings.highlights_to_fetch]:
            try:
                full_hl = await self.pool.safe_request(_highlight_info, int(hl.pk))
                highlights.append(highlight_to_scraped(full_hl))
            except Exception as e:
                logger.warning(f"Failed to fetch highlight {hl.pk}: {e}")
                highlights.append(highlight_to_scraped(hl))

        # 4. Маппинг — все медиа в один список (без разделения на посты/рилсы)
        medias_mapped = [media_to_scraped_post(m) for m in medias]

        # 5. Комментарии для первых N постов с включёнными комментариями
        posts_for_comments = [
            p for p in medias_mapped
            if not p.comments_disabled and p.comment_count > 0
        ][:self.settings.posts_with_comments]

        for post in posts_for_comments:
            try:
                raw_comments = await self.pool.safe_request(
                    _media_comments, post.platform_id, self.settings.comments_to_fetch
                )
                comments: list[ScrapedComment] = []
                for c in raw_comments[:self.settings.comments_to_fetch]:
                    text = (c.text or "").strip()
                    uname = c.user.username if c.user else ""
                    if text and uname:
                        comments.append(ScrapedComment(username=uname, text=text))
                post.top_comments = comments
            except Exception as e:
                logger.warning(f"Failed to fetch comments for {post.platform_id}: {e}")

        # Рилсы отдельно для avg_er_reels
        reels_for_er = [p for p in medias_mapped if p.media_type == 2 and p.product_type == "clips"]

        # Вычислить engagement_rate для всех медиа
        if user.follower_count > 0:
            for p in medias_mapped:
                p.engagement_rate = round(
                    (p.like_count + p.comment_count) / user.follower_count * 100, 2
                )

        bio_links: list[dict[str, str | None]] = []
        if user.bio_links:
            for link in user.bio_links:
                url = getattr(link, "url", None)
                if url:
                    bio_links.append({
                        "url": str(url),
                        "title": getattr(link, "title", None) or None,
                        "link_type": getattr(link, "link_type", None) or None,
                    })

        # Извлечение контактных данных профиля
        account_type = getattr(user, "account_type", None) or None
        public_email = getattr(user, "public_email", None) or None
        contact_phone_number = getattr(user, "contact_phone_number", None) or None
        public_phone_country_code = getattr(user, "public_phone_country_code", None) or None
        city_name = getattr(user, "city_name", None) or None
        address_street = getattr(user, "address_street", None) or None

        profile = ScrapedProfile(
            platform_id=str(user.pk),
            username=user.username,
            full_name=user.full_name or "",
            biography=user.biography or "",
            external_url=str(user.external_url) if user.external_url else None,
            bio_links=bio_links,
            follower_count=user.follower_count,
            following_count=user.following_count,
            media_count=user.media_count,
            is_verified=user.is_verified,
            is_business=user.is_business,
            business_category=user.business_category_name or user.category_name,
            account_type=account_type,
            public_email=public_email,
            contact_phone_number=contact_phone_number,
            public_phone_country_code=public_phone_country_code,
            city_name=city_name,
            address_street=address_street,
            profile_pic_url=str(user.profile_pic_url) if user.profile_pic_url else None,
            medias=medias_mapped,
            highlights=highlights,
            avg_er=calculate_er(medias_mapped, user.follower_count),
            avg_er_reels=calculate_er(reels_for_er, user.follower_count),
            er_trend=calculate_er_trend(medias_mapped, user.follower_count),
            posts_per_week=calculate_posts_per_week(medias_mapped),
        )

        logger.info(
            f"Scraped @{username}: {len(medias_mapped)} publications, "
            f"{len(highlights)} highlights, ER={profile.avg_er}"
        )
        return profile

    async def discover(
        self, query: str, min_followers: int
    ) -> list[DiscoveredProfile]:
        """Discover новых профилей по хештегу."""
        logger.info(f"Discovering by hashtag #{query}, min_followers={min_followers}")

        medias = await self.pool.safe_request(
            _hashtag_medias_top, query, TOP_HASHTAG_MEDIAS_AMOUNT
        )

        # Уникальные пользователи
        seen_pks: set[str] = set()
        discovered: list[DiscoveredProfile] = []

        for media in medias:
            if not media.user:
                continue
            user_pk = str(media.user.pk)
            if user_pk in seen_pks:
                continue
            seen_pks.add(user_pk)

            if len(discovered) >= 20:  # лимит за один запуск
                break

            try:
                user = await self.pool.safe_request(_user_info, user_pk)
            except UserNotFound:
                continue
            except Exception as e:
                logger.warning(f"Failed to get user info for {user_pk}: {e}")
                continue

            # Фильтры
            if user.is_private:
                continue
            if user.follower_count < min_followers:
                continue
            if user.media_count < 5:
                continue

            discovered.append(DiscoveredProfile(
                username=user.username,
                full_name=user.full_name or "",
                follower_count=user.follower_count,
                platform_id=str(user.pk),
                is_business=user.is_business,
                is_verified=user.is_verified,
                biography=user.biography or "",
                account_type=getattr(user, "account_type", None) or None,
            ))

        logger.info(f"Discovered {len(discovered)} new profiles for #{query}")
        return discovered
