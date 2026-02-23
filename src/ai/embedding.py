"""Генерация embedding для семантического поиска блогеров."""
from loguru import logger
from openai import AsyncOpenAI

from src.ai.schemas import AIInsights

EMBEDDING_MODEL = "text-embedding-3-small"


def build_embedding_text(insights: AIInsights) -> str:
    """Построить структурированный русскоязычный текст для embedding."""
    parts: list[str] = []

    # Краткое описание
    if insights.short_label:
        label = insights.short_label
        if insights.short_summary:
            parts.append(f"{label}. {insights.short_summary}")
        else:
            parts.append(label)
    elif insights.short_summary:
        parts.append(insights.short_summary)

    # Категория и подкатегории
    profile_parts: list[str] = []
    if insights.content.primary_topic:
        profile_parts.append(f"Категория: {insights.content.primary_topic}")
    if insights.content.secondary_topics:
        profile_parts.append(f"Подкатегории: {', '.join(insights.content.secondary_topics)}")
    bp = insights.blogger_profile
    if bp.profession:
        profile_parts.append(f"Профессия: {bp.profession}")
    if bp.city:
        city = bp.city
        if bp.country:
            city += f", {bp.country}"
        profile_parts.append(f"Город: {city}")
    elif bp.country:
        profile_parts.append(f"Страна: {bp.country}")
    if bp.speaks_languages:
        profile_parts.append(f"Языки: {', '.join(bp.speaks_languages)}")
    if bp.page_type:
        type_map = {"blog": "личный блог", "public": "паблик", "business": "бизнес"}
        profile_parts.append(f"Тип: {type_map.get(bp.page_type, bp.page_type)}")
    if profile_parts:
        parts.append(". ".join(profile_parts) + ".")

    # Теги
    if insights.tags:
        parts.append(f"Теги: {', '.join(insights.tags)}.")

    # Аудитория
    aud = insights.audience_inference
    aud_parts: list[str] = []
    if aud.estimated_audience_gender:
        aud_parts.append(aud.estimated_audience_gender)
    if aud.estimated_audience_age:
        aud_parts.append(aud.estimated_audience_age)
    if aud.estimated_audience_geo:
        aud_parts.append(aud.estimated_audience_geo)
    if aud_parts:
        parts.append(f"Аудитория: {', '.join(aud_parts)}.")
    if aud.audience_interests:
        parts.append(f"Интересы аудитории: {', '.join(aud.audience_interests)}.")

    # Маркетинг
    mv = insights.marketing_value
    if mv.best_fit_industries:
        parts.append(f"Подходит для рекламы: {', '.join(mv.best_fit_industries)}.")
    if mv.not_suitable_for:
        parts.append(f"Не подходит: {', '.join(mv.not_suitable_for)}.")
    if insights.commercial.detected_brand_categories:
        parts.append(f"Рекламирует: {', '.join(insights.commercial.detected_brand_categories)}.")

    return "\n".join(parts) if parts else "блогер"


async def generate_embedding(
    client: AsyncOpenAI,
    text: str,
) -> list[float] | None:
    """Сгенерировать embedding-вектор через OpenAI API. None при ошибке."""
    try:
        response = await client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=text,
        )
        return response.data[0].embedding
    except Exception as e:
        logger.error(f"[embedding] Ошибка генерации embedding: {e}")
        return None
