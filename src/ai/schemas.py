"""Pydantic-схемы для AI-анализа профилей. Используются как structured output."""
from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field

from src.ai.taxonomy import ALL_CATEGORY_CODES, ALL_SUBCATEGORY_NAMES, ALL_TAG_NAMES

# Runtime + JSON-schema enum ограничения:
# - ограничивают structured output в OpenAI strict mode
# - и валидируются Pydantic при обычном model_validate/model_validate_json
# TYPE_CHECKING guard: pyright не разрешает Literal[*runtime_list],
# но runtime Python 3.12 поддерживает корректно.
if TYPE_CHECKING:
    CategoryCode = str
    SubcategoryName = str
    TagName = str
else:
    CategoryCode = Literal[*ALL_CATEGORY_CODES]
    SubcategoryName = Literal[*ALL_SUBCATEGORY_NAMES]
    TagName = Literal[*ALL_TAG_NAMES]


class BloggerProfile(BaseModel):
    """Демография и характеристики самого блогера."""
    model_config = ConfigDict(extra="forbid")

    estimated_age: Literal["18-24", "25-34", "35-44", "45+"] | None = Field(
        default=None,
        description="Возрастная группа блогера. Определяй по фото, упоминаниям "
        "возраста в постах, жизненным событиям (окончание вуза, свадьба, дети).",
    )
    gender: Literal["female", "male"] | None = Field(
        default=None,
        description="Пол блогера. Определяй по фото, имени, местоимениям в текстах.",
    )
    city: str | None = Field(
        default=None,
        description="Город блогера на русском ('Алматы', 'Москва', 'Ташкент'). "
        "Определяй по геотегам постов, упоминаниям в био, адресу профиля.",
    )
    profession: str | None = Field(
        default=None,
        description="Профессия блогера на русском ('визажист', 'фитнес-тренер', 'фотограф'). "
        "Определяй по био, хайлайтам, контенту постов. Если не очевидна — null.",
    )
    education: Literal["school", "university", "mba"] | None = Field(
        default=None,
        description="Уровень образования. 'school'=среднее, 'university'=высшее, "
        "'mba'=MBA/второе высшее. Определяй по упоминаниям в био и постах.",
    )
    speaks_languages: list[str] = Field(
        default_factory=list,
        description="Языки блогера на русском (['русский', 'казахский', 'английский']). "
        "Определяй по языку постов, био, хайлайтов.",
    )
    page_type: Literal["blog", "public", "business"] | None = Field(
        default=None,
        description="Тип страницы. 'blog'=личный блог от первого лица. "
        "'public'=тематический паблик без привязки к личности. "
        "'business'=страница компании/магазина/бренда.",
    )
    has_manager: bool | None = Field(
        default=None,
        description="true если в био, контактах или контенте указан менеджер/агентство.",
    )
    manager_contact: str | None = Field(
        default=None,
        description="Контакт менеджера ('@username', email, ссылка). null если нет менеджера.",
    )
    country: str | None = Field(
        default=None,
        description="Страна блогера на русском ('Казахстан', 'Россия', 'Узбекистан').",
    )


class LifeSituation(BaseModel):
    """Жизненная ситуация блогера — семья, дети, отношения."""
    model_config = ConfigDict(extra="forbid")

    has_children: bool | None = Field(
        default=None,
        description="Есть ли дети. Определяй по постам, хайлайтам, упоминаниям в био.",
    )
    children_age_group: Literal["baby", "toddler", "school", "teen"] | None = Field(
        default=None,
        description="Возрастная группа детей. 'baby'=до 1 года, 'toddler'=1-6 лет, "
        "'school'=7-12 лет, 'teen'=13-17 лет. null если нет детей.",
    )
    relationship_status: Literal["married", "in_relationship", "single"] | None = Field(
        default=None,
        description="Семейное положение. Определяй по постам, кольцам на фото, "
        "упоминаниям партнёра.",
    )
    is_young_parent: bool | None = Field(
        default=None,
        description="Молодой родитель (до 30 лет с маленькими детьми).",
    )


class Lifestyle(BaseModel):
    """Образ жизни — авто, путешествия, недвижимость, уровень."""
    model_config = ConfigDict(extra="forbid")

    has_car: bool | None = Field(
        default=None,
        description="Есть ли автомобиль. Определяй по фото, упоминаниям в постах.",
    )
    car_class: Literal["budget", "middle", "premium", "luxury"] | None = Field(
        default=None,
        description="Класс автомобиля. 'budget'=эконом (Lada, Hyundai), "
        "'middle'=средний (Toyota, Kia), 'premium'=премиум (BMW, Mercedes), "
        "'luxury'=люкс (Porsche, Bentley). null если нет авто.",
    )
    travels_frequently: bool | None = Field(
        default=None,
        description="Часто путешествует (3+ поездок в год по постам).",
    )
    travel_style: Literal["budget", "comfort", "luxury"] | None = Field(
        default=None,
        description="Стиль путешествий. 'budget'=хостелы/эконом, 'comfort'=отели 3-4*, "
        "'luxury'=5* отели, бизнес-класс.",
    )
    has_pets: bool | None = Field(
        default=None,
        description="Есть ли домашние питомцы. Определяй по фото и постам.",
    )
    pet_types: list[str] = Field(
        default_factory=list,
        description="Типы питомцев на русском (['собака', 'кошка', 'попугай']).",
    )
    has_real_estate: bool | None = Field(
        default=None,
        description="Упоминается ли собственная недвижимость (квартира, дом).",
    )
    lifestyle_level: Literal["budget", "middle", "premium", "luxury"] | None = Field(
        default=None,
        description="Общий уровень жизни по контенту. 'budget'=эконом-сегмент, "
        "'middle'=средний класс, 'premium'=выше среднего, 'luxury'=люкс.",
    )


class ContentProfile(BaseModel):
    """Контент-профиль — тематика, язык, тон, формат."""
    model_config = ConfigDict(extra="forbid")

    primary_categories: list[CategoryCode] = Field(
        default_factory=list,
        max_length=3,
        description="До 3 кодов основных категорий из списка в промпте (английский): "
        "'beauty', 'fitness', 'family'. Первый элемент = основная категория, "
        "остальные = дополнительные. НЕ русские названия.",
    )
    secondary_topics: list[SubcategoryName] = Field(
        default_factory=list,
        max_length=5,
        description="До 5 подкатегорий из списка в промпте (русские названия): "
        "['Макияж', 'Уход за кожей'].",
    )
    content_language: list[str] = Field(
        default_factory=list,
        description="Языки контента на русском (['русский', 'казахский', 'английский']).",
    )
    content_tone: Literal[
        "positive", "neutral", "educational", "humor", "inspirational"
    ] | None = Field(
        default=None,
        description="Тон контента. 'positive'=позитивный, 'neutral'=нейтральный, "
        "'educational'=обучающий, 'humor'=юмор, 'inspirational'=мотивационный.",
    )
    posts_in_russian: bool | None = Field(
        default=None,
        description="Блогер пишет посты на русском языке.",
    )
    posts_in_kazakh: bool | None = Field(
        default=None,
        description="Блогер пишет посты на казахском языке.",
    )
    preferred_format: Literal["photo", "video", "reels", "carousel", "mixed"] | None = Field(
        default=None,
        description="Преобладающий формат контента. 'mixed' если нет явного преобладания.",
    )
    content_quality: Literal["low", "medium", "high", "professional"] | None = Field(
        default=None,
        description="Качество контента. 'low'=нечёткие фото, плохое освещение. "
        "'medium'=нормальные фото, базовая обработка. 'high'=качественные фото, "
        "единый стиль. 'professional'=студийное качество, цветокоррекция.",
    )
    uses_professional_photo: bool | None = Field(
        default=None,
        description="Используется ли профессиональная фотосъёмка (студия, фотограф).",
    )
    has_consistent_visual_style: bool | None = Field(
        default=None,
        description="Посты выдержаны в едином визуальном стиле "
        "(цветовая гамма, фильтры, композиция). Определяй по изображениям.",
    )
    posting_frequency: Literal["rare", "weekly", "several_per_week", "daily"] | None = Field(
        default=None,
        description="Частота публикаций. 'rare'=реже 1/нед, 'weekly'=~1/нед, "
        "'several_per_week'=2-5/нед, 'daily'=каждый день. Используй posts_per_week.",
    )
    audience_interaction: Literal["low", "medium", "high"] | None = Field(
        default=None,
        description="Уровень взаимодействия с аудиторией. 'low'=мало комментариев, "
        "блогер не отвечает. 'medium'=есть комментарии, иногда отвечает. "
        "'high'=активная дискуссия, регулярно общается.",
    )
    call_to_action_style: str | None = Field(
        default=None,
        description="Стиль CTA на русском: 'вопросы к аудитории', 'конкурсы и розыгрыши', "
        "'ссылки на товары', 'промокоды'. null если нет CTA.",
    )


class CommercialActivity(BaseModel):
    """Коммерческая активность — реклама, бренды, партнёрки."""
    model_config = ConfigDict(extra="forbid")

    has_brand_collaborations: bool | None = Field(
        default=None,
        description="Есть ли рекламные интеграции с брендами в постах/сторис.",
    )
    detected_brand_categories: list[str] = Field(
        default_factory=list,
        description="Категории рекламируемых брендов на русском: "
        "['косметика', 'детские товары', 'одежда'].",
    )
    detected_brands: list[str] = Field(
        default_factory=list,
        description="Названия рекламируемых брендов: Zara, L'Oreal, Kaspi.",
    )
    has_affiliate_links: bool | None = Field(
        default=None,
        description="Есть ли партнёрские/реферальные ссылки в постах или bio.",
    )
    is_active_advertiser: bool | None = Field(
        default=None,
        description="Активно ли размещает рекламу (регулярные рекламные посты).",
    )
    ad_frequency: Literal["rare", "moderate", "frequent"] | None = Field(
        default=None,
        description="Частота рекламных постов. 'rare'=редко (1-2/мес), "
        "'moderate'=умеренно (3-5/мес), 'frequent'=часто (6+/мес).",
    )
    ad_format: list[Literal[
        "integration", "dedicated_post", "stories", "reels", "unboxing", "review"
    ]] = Field(
        default_factory=list,
        description="Форматы рекламы: 'integration'=нативная, 'dedicated_post'=рекламный пост, "
        "'stories'=в сторис, 'reels'=рилс, 'unboxing'=распаковка, 'review'=обзор.",
    )
    has_price_list: bool | None = Field(
        default=None,
        description="Упоминается ли прайс-лист на рекламу (в био, хайлайтах, постах).",
    )
    estimated_price_tier: Literal["nano", "micro", "mid", "macro"] | None = Field(
        default=None,
        description="Ценовой сегмент по числу подписчиков. 'nano'=до 10К, "
        "'micro'=10К-50К, 'mid'=50К-300К, 'macro'=300К+.",
    )
    open_to_barter: bool | None = Field(
        default=None,
        description="Открыт ли к бартерному сотрудничеству (упоминания в био/постах).",
    )
    has_own_product: bool | None = Field(
        default=None,
        description="Есть ли собственный продукт/услуга (курсы, магазин, салон).",
    )
    own_product_type: str | None = Field(
        default=None,
        description="Тип продукта на русском: 'курс по макияжу', 'магазин одежды', "
        "'салон красоты'. null если нет продукта.",
    )
    ambassador_brands: list[str] = Field(
        default_factory=list,
        description="Бренды, у которых блогер является амбассадором "
        "(долгосрочное сотрудничество, не разовая реклама).",
    )


class AudienceInference(BaseModel):
    """Предположения об аудитории на основе контента."""
    model_config = ConfigDict(extra="forbid")

    audience_male_pct: int | None = Field(
        ge=0, le=100, default=None,
        description="Процент мужской аудитории (0-100). Сумма male+female+other=100.",
    )
    audience_female_pct: int | None = Field(
        ge=0, le=100, default=None,
        description="Процент женской аудитории (0-100). Сумма male+female+other=100.",
    )
    audience_other_pct: int | None = Field(
        ge=0, le=100, default=None,
        description="Процент неопределённого пола (0-100). Обычно 0-5%.",
    )
    estimated_audience_age: Literal["18-24", "25-34", "35-44", "mixed"] | None = Field(
        default=None,
        description="Возраст основной аудитории. '18-24'=молодёжь, тренды. "
        "'25-34'=карьера, семья. '35-44'=зрелый контент. 'mixed'=разнородная.",
    )
    audience_age_13_17_pct: int | None = Field(
        ge=0, le=100, default=None,
        description="Процент аудитории 13-17 лет (0-100). "
        "Сумма всех age-групп = 100. Определяй по контенту, "
        "стилю комментариев, тематике (школьный контент, тренды TikTok).",
    )
    audience_age_18_24_pct: int | None = Field(
        ge=0, le=100, default=None,
        description="Процент аудитории 18-24 лет (0-100). "
        "Молодёжь: студенты, начало карьеры, тренды, мемы.",
    )
    audience_age_25_34_pct: int | None = Field(
        ge=0, le=100, default=None,
        description="Процент аудитории 25-34 лет (0-100). "
        "Молодые специалисты, молодые родители, карьерный рост.",
    )
    audience_age_35_44_pct: int | None = Field(
        ge=0, le=100, default=None,
        description="Процент аудитории 35-44 лет (0-100). "
        "Зрелая аудитория: бизнес, семья, образование детей.",
    )
    audience_age_45_plus_pct: int | None = Field(
        ge=0, le=100, default=None,
        description="Процент аудитории 45+ лет (0-100). "
        "Старшая аудитория: здоровье, путешествия, внуки.",
    )
    estimated_audience_geo: Literal["kz", "ru", "uz", "cis_mixed"] | None = Field(
        default=None,
        description="География аудитории. 'kz'=Казахстан, 'ru'=Россия, "
        "'uz'=Узбекистан, 'cis_mixed'=смешанная из СНГ.",
    )
    audience_kz_pct: int | None = Field(
        ge=0, le=100, default=None,
        description="Процент аудитории из Казахстана (0-100). "
        "Сумма всех geo-процентов = 100. Определяй по языку, "
        "геотегам, упоминаниям городов КЗ, комментариям.",
    )
    audience_ru_pct: int | None = Field(
        ge=0, le=100, default=None,
        description="Процент аудитории из России (0-100).",
    )
    audience_uz_pct: int | None = Field(
        ge=0, le=100, default=None,
        description="Процент аудитории из Узбекистана (0-100).",
    )
    audience_other_geo_pct: int | None = Field(
        ge=0, le=100, default=None,
        description="Процент аудитории из других стран (0-100).",
    )
    geo_mentions: list[str] = Field(
        default_factory=list,
        description="Упоминаемые города/страны на русском: ['Алматы', 'Астана', 'Турция'].",
    )
    estimated_audience_income: Literal["low", "medium", "high"] | None = Field(
        default=None,
        description="Доход аудитории. 'low'=бюджетные товары, скидки. "
        "'medium'=средний сегмент. 'high'=люкс, премиум-бренды.",
    )
    audience_interests: list[str] = Field(
        default_factory=list,
        description="Интересы аудитории на русском: ['красота', 'материнство', 'фитнес'].",
    )
    engagement_quality: Literal["organic", "mixed", "suspicious"] | None = Field(
        default=None,
        description="Качество вовлечённости. 'organic'=осмысленные комментарии. "
        "'suspicious'=однотипные/эмодзи-комментарии, подозрение на накрутку. "
        "'mixed'=что-то среднее.",
    )
    comments_sentiment: Literal["positive", "mixed", "negative"] | None = Field(
        default=None,
        description="Тональность комментариев. Оценивай по реальным комментариям к постам. "
        "'positive'=похвала, поддержка. 'mixed'=и позитив и негатив. "
        "'negative'=критика, недовольство.",
    )


class MarketingValue(BaseModel):
    """Оценка ценности для рекламодателя."""
    model_config = ConfigDict(extra="forbid")

    best_fit_industries: list[str] = Field(
        default_factory=list,
        description="Подходящие индустрии для рекламы на русском: "
        "['красота', 'детские товары', 'фитнес'].",
    )
    not_suitable_for: list[str] = Field(
        default_factory=list,
        description="Неподходящие индустрии на русском: ['алкоголь', 'азартные игры'].",
    )
    collaboration_risk: Literal["low", "medium", "high"] | None = Field(
        default=None,
        description="Риск сотрудничества. 'low'=стабильный контент, нет скандалов. "
        "'medium'=есть спорные темы. 'high'=скандальный контент, хейт, 18+, политика.",
    )
    brand_safety_score: Literal[1, 2, 3, 4, 5] | None = Field(
        default=None,
        description="Безопасность для рекламодателя по шкале 1-5. "
        "1=высокий риск (скандалы, хейт-спич, 18+). "
        "2=есть спорный контент (провокации, нецензурная лексика). "
        "3=нейтрально (нет явных рисков, но и не семейный контент). "
        "4=безопасно (позитивный контент без спорных тем). "
        "5=идеально безопасный семейный контент.",
    )
    values_and_causes: list[str] = Field(
        default_factory=list,
        description="Ценности и социальные темы на русском: ['экология', 'ЗОЖ', 'феминизм'].",
    )


class AIInsights(BaseModel):
    """Полный AI-анализ профиля блогера."""
    model_config = ConfigDict(extra="forbid")

    reasoning: str = Field(
        default="",
        description="ЗАПОЛНЯЙ ЭТО ПОЛЕ ПЕРВЫМ. Напиши 3-5 предложений свободного анализа: "
        "кто этот блогер, о чём пишет, какой стиль контента, какая предполагаемая аудитория, "
        "какие коммерческие возможности и риски. Этот анализ поможет точнее заполнить "
        "остальные поля.",
    )
    short_label: str = Field(
        default="",
        description="2-3 слова на русском, характеризующие блогера: "
        "'фуд-блогер', 'мама двоих', 'фитнес-тренер', 'бьюти-мастер'.",
    )
    short_summary: str = Field(
        default="",
        description="2-3 строки на русском: краткое описание блогера для быстрого понимания.",
    )
    tags: list[TagName] = Field(
        default_factory=list,
        min_length=3,
        max_length=40,
        description="Теги из справочника (7-40 штук, русские). Выбирай СТРОГО из списка в промпте.",
    )
    summary: str = Field(
        default="",
        description="2-3 абзаца на русском: кто этот блогер, о чём пишет, какая аудитория, "
        "чем полезен для рекламодателя.",
    )
    blogger_profile: BloggerProfile = Field(
        default_factory=BloggerProfile,
        description="Демография и характеристики блогера.",
    )
    life_situation: LifeSituation = Field(
        default_factory=LifeSituation,
        description="Жизненная ситуация — семья, дети, отношения.",
    )
    lifestyle: Lifestyle = Field(
        default_factory=Lifestyle,
        description="Образ жизни — авто, путешествия, недвижимость.",
    )
    content: ContentProfile = Field(
        default_factory=ContentProfile,
        description="Контент-профиль блогера: тематика, язык, тон, формат, частота публикаций.",
    )
    commercial: CommercialActivity = Field(
        default_factory=CommercialActivity,
        description="Коммерческая активность блогера: реклама, бренды, партнёрки, собственные продукты.",
    )
    audience_inference: AudienceInference = Field(
        default_factory=AudienceInference,
        description="Предположения об аудитории блогера на основе контента и комментариев.",
    )
    marketing_value: MarketingValue = Field(
        default_factory=MarketingValue,
        description="Оценка маркетинговой ценности блогера для рекламодателя.",
    )
    confidence: Literal[1, 2, 3, 4, 5] = Field(
        default=3,
        description="Уверенность в анализе по шкале 1-5. "
        "1=крайне мало данных (пустой профиль, 1-2 поста без текста). "
        "2=мало данных (несколько постов, скудное био). "
        "3=достаточно данных для базового анализа. "
        "4=хорошая база (много постов, подробное био, хайлайты). "
        "5=отличная полнота данных (много постов с текстом, комментарии, хайлайты).",
    )
