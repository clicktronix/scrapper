"""Pydantic-схемы для AI-анализа профилей. Используются как structured output."""
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class BloggerProfile(BaseModel):
    """Демография и характеристики самого блогера."""
    model_config = ConfigDict(extra="forbid")

    estimated_age: Literal["18-24", "25-34", "35-44", "45+"] | None = None
    gender: Literal["female", "male"] | None = None
    city: str | None = None  # на русском: "Алматы", "Астана", "Москва"
    profession: str | None = None  # на русском: "визажист", "фитнес-тренер"
    education: Literal["school", "university", "mba"] | None = None
    speaks_languages: list[str] = Field(default_factory=list)  # на русском: ["русский", "казахский"]
    page_type: Literal["blog", "public", "business"] | None = None
    has_manager: bool | None = None
    manager_contact: str | None = None  # контакт менеджера: "@username" или ссылка
    country: str | None = None  # на русском: "Казахстан", "Россия"


class LifeSituation(BaseModel):
    """Жизненная ситуация блогера — семья, дети, отношения."""
    model_config = ConfigDict(extra="forbid")

    has_children: bool | None = None
    children_age_group: Literal["baby", "toddler", "school", "teen"] | None = None
    relationship_status: Literal["married", "in_relationship", "single"] | None = None
    is_young_parent: bool | None = None


class Lifestyle(BaseModel):
    """Образ жизни — авто, путешествия, недвижимость, уровень."""
    model_config = ConfigDict(extra="forbid")

    has_car: bool | None = None
    car_class: Literal["budget", "middle", "premium", "luxury"] | None = None
    travels_frequently: bool | None = None
    travel_style: Literal["budget", "comfort", "luxury"] | None = None
    has_pets: bool | None = None
    pet_types: list[str] = Field(default_factory=list)  # на русском: ["собака", "кошка"]
    has_real_estate: bool | None = None
    lifestyle_level: Literal["budget", "middle", "premium", "luxury"] | None = None


class ContentProfile(BaseModel):
    """Контент-профиль — тематика, язык, тон, формат."""
    model_config = ConfigDict(extra="forbid")

    primary_topic: str | None = None  # код категории: "beauty", "fitness", "family"
    secondary_topics: list[str] = Field(default_factory=list)  # названия подкатегорий: ["Макияж", "Уход за кожей"]
    content_language: list[str] = Field(default_factory=list)  # на русском: ["русский", "казахский"]
    content_tone: Literal[
        "positive", "neutral", "educational", "humor", "inspirational"
    ] | None = None
    posts_in_russian: bool | None = None
    posts_in_kazakh: bool | None = None
    preferred_format: Literal["photo", "video", "reels", "carousel", "mixed"] | None = None
    content_quality: Literal["low", "medium", "high", "professional"] | None = None
    uses_professional_photo: bool | None = None
    has_consistent_visual_style: bool | None = None
    posting_frequency: Literal["rare", "weekly", "several_per_week", "daily"] | None = None
    audience_interaction: Literal["low", "medium", "high"] | None = None
    call_to_action_style: str | None = None  # на русском: "вопросы к аудитории", "конкурсы"


class CommercialActivity(BaseModel):
    """Коммерческая активность — реклама, бренды, партнёрки."""
    model_config = ConfigDict(extra="forbid")

    has_brand_collaborations: bool | None = None
    detected_brand_categories: list[str] = Field(default_factory=list)  # на русском: ["косметика", "детские товары"]
    detected_brands: list[str] = Field(default_factory=list)  # названия брендов: ["Zara", "L'Oreal", "Kaspi"]
    has_affiliate_links: bool | None = None
    is_active_advertiser: bool | None = None
    ad_frequency: Literal["rare", "moderate", "frequent"] | None = None
    ad_format: list[Literal[
        "integration", "dedicated_post", "stories", "reels", "unboxing", "review"
    ]] = Field(default_factory=list)
    has_price_list: bool | None = None
    estimated_price_tier: Literal["nano", "micro", "mid", "macro"] | None = None
    open_to_barter: bool | None = None
    has_own_product: bool | None = None
    own_product_type: str | None = None  # на русском: "курс по макияжу", "магазин одежды"
    ambassador_brands: list[str] = Field(default_factory=list)  # бренды, у которых блогер амбассадор


class AudienceInference(BaseModel):
    """Предположения об аудитории на основе контента."""
    model_config = ConfigDict(extra="forbid")

    estimated_audience_gender: Literal[
        "mostly_female", "mostly_male", "mixed"
    ] | None = None
    estimated_audience_age: Literal["18-24", "25-34", "35-44", "mixed"] | None = None
    estimated_audience_geo: Literal["kz", "ru", "uz", "cis_mixed"] | None = None
    geo_mentions: list[str] = Field(default_factory=list)  # на русском: ["Алматы", "Астана", "Турция"]
    estimated_audience_income: Literal["low", "medium", "high"] | None = None
    audience_interests: list[str] = Field(default_factory=list)  # на русском: ["красота", "материнство", "фитнес"]
    engagement_quality: Literal["organic", "mixed", "suspicious"] | None = None
    comments_sentiment: Literal["positive", "mixed", "negative"] | None = None


class MarketingValue(BaseModel):
    """Оценка ценности для рекламодателя."""
    model_config = ConfigDict(extra="forbid")

    best_fit_industries: list[str] = Field(default_factory=list)  # на русском: ["красота", "детские товары"]
    not_suitable_for: list[str] = Field(default_factory=list)  # на русском: ["алкоголь", "азартные игры"]
    collaboration_risk: Literal["low", "medium", "high"] | None = None
    brand_safety_score: float | None = Field(ge=0.0, le=1.0, default=None)
    values_and_causes: list[str] = Field(default_factory=list)  # на русском: ["экология", "ЗОЖ", "феминизм"]


class AIInsights(BaseModel):
    """Полный AI-анализ профиля блогера."""
    model_config = ConfigDict(extra="forbid")

    short_label: str = ""  # на русском: 2-3 слова ("фуд-блогер", "мама двоих")
    short_summary: str = ""  # на русском: 2-3 строки краткое описание
    tags: list[str] = Field(default_factory=list)  # теги из справочника
    summary: str = ""  # на русском: 2-3 абзаца описание блогера для маркетолога
    blogger_profile: BloggerProfile = Field(default_factory=BloggerProfile)
    life_situation: LifeSituation = Field(default_factory=LifeSituation)
    lifestyle: Lifestyle = Field(default_factory=Lifestyle)
    content: ContentProfile = Field(default_factory=ContentProfile)
    commercial: CommercialActivity = Field(default_factory=CommercialActivity)
    audience_inference: AudienceInference = Field(default_factory=AudienceInference)
    marketing_value: MarketingValue = Field(default_factory=MarketingValue)
    confidence: float = Field(ge=0.0, le=1.0, default=0.5)
