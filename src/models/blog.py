"""Pydantic-модели результатов скрапинга."""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class ScrapedComment(BaseModel):
    """Комментарий к посту."""

    username: str
    text: str


class ScrapedPost(BaseModel):
    """Пост или рилс из Instagram."""

    platform_id: str
    shortcode: str | None = None
    media_type: int  # 1=фото, 2=видео, 8=альбом
    product_type: str | None = None  # feed, clips, igtv
    caption_text: str = ""
    hashtags: list[str] = []
    mentions: list[str] = []
    has_sponsor_tag: bool = False
    sponsor_brands: list[str] = []  # username брендов-спонсоров
    like_count: int = 0
    comment_count: int = 0
    play_count: int | None = None
    view_count: int | None = None
    engagement_rate: float | None = None  # ER поста (likes+comments)/followers*100
    thumbnail_url: str | None = None
    location_name: str | None = None
    location_city: str | None = None
    location_lat: float | None = None
    location_lng: float | None = None
    taken_at: datetime

    # Дополнительные поля из instagrapi
    video_duration: float | None = None  # длительность видео (только media_type=2)
    usertags: list[str] = []  # отмеченные на фото пользователи
    accessibility_caption: str | None = None  # автоописание от Instagram
    comments_disabled: bool = False  # комменты отключены
    top_comments: list[ScrapedComment] = []  # топ комментариев для AI-анализа
    title: str | None = None  # заголовок Reels/IGTV
    carousel_media_count: int | None = None  # количество слайдов карусели


class ScrapedHighlight(BaseModel):
    """Хайлайт из Instagram."""

    platform_id: str
    title: str
    media_count: int = 0
    cover_url: str | None = None
    story_mentions: list[str] = []  # username из StoryMention
    story_locations: list[str] = []  # Location.name из StoryLocation
    story_links: list[str] = []  # StoryLink.webUri

    # Дополнительные поля из instagrapi stories
    story_sponsor_tags: list[str] = []  # бренды-спонсоры из stories
    has_paid_partnership: bool = False  # есть paid partnership в stories
    story_hashtags: list[str] = []  # хештеги из stories


class ScrapedProfile(BaseModel):
    """Полный результат скрапинга профиля."""

    # Идентификаторы
    platform_id: str
    username: str
    full_name: str = ""

    # Профиль
    biography: str = ""
    external_url: str | None = None
    bio_links: list[dict[str, str | None]] = []  # [{url, title, link_type}]

    # Метрики
    follower_count: int = 0
    following_count: int = 0
    media_count: int = 0

    # Тип аккаунта
    is_verified: bool = False
    is_business: bool = False
    business_category: str | None = None
    account_type: int | None = None  # 1=personal, 2=business, 3=creator

    # Контактные данные
    public_email: str | None = None
    contact_phone_number: str | None = None
    public_phone_country_code: str | None = None
    city_name: str | None = None
    address_street: str | None = None

    # Аватар
    profile_pic_url: str | None = None

    # Контент
    medias: list[ScrapedPost] = []
    highlights: list[ScrapedHighlight] = []

    # Вычисленные метрики
    avg_er: float | None = None
    avg_er_reels: float | None = None
    er_trend: Literal["growing", "stable", "declining"] | None = None
    posts_per_week: float | None = None
