"""Тесты генерации embedding для блогеров."""
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.ai.schemas import AIInsights


class TestBuildEmbeddingText:
    """Тесты построения текста для embedding."""

    def test_basic_embedding_text(self) -> None:
        """Embedding текст содержит ключевые данные."""
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(
            short_label="фуд-блогер",
            short_summary="Готовит казахскую кухню.",
            tags=["видео-контент", "reels", "юмор", "мама", "ЗОЖ", "эстетика", "сторителлинг"],
            blogger_profile={
                "profession": "повар",
                "city": "Алматы",
                "country": "Казахстан",
                "page_type": "blog",
                "speaks_languages": ["русский"],
            },
            content={
                "primary_categories": ["food"],
                "secondary_topics": ["Рецепты", "ПП и диеты"],
            },
            audience_inference={
                "audience_male_pct": 20,
                "audience_female_pct": 75,
                "audience_other_pct": 5,
                "estimated_audience_age": "25-34",
                "estimated_audience_geo": "kz",
                "audience_interests": ["кулинария", "ЗОЖ"],
            },
            marketing_value={
                "best_fit_industries": ["food", "HoReCa"],
                "not_suitable_for": ["алкоголь"],
            },
            commercial={
                "detected_brand_categories": ["еда", "рестораны"],
            },
        )

        text = build_embedding_text(insights)

        assert "Готовит казахскую кухню" in text
        assert "food" in text
        assert "повар" in text
        assert "Алматы" in text
        assert "видео-контент" in text
        assert "reels" in text

    def test_empty_insights_embedding_text(self) -> None:
        """Пустой AIInsights -> минимальный текст без ошибок."""
        from src.ai.embedding import build_embedding_text

        insights = AIInsights()
        text = build_embedding_text(insights)

        assert isinstance(text, str)
        assert len(text) > 0

    def test_embedding_text_includes_audience(self) -> None:
        """Текст включает данные об аудитории."""
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(
            audience_inference={
                "audience_male_pct": 15,
                "audience_female_pct": 80,
                "audience_other_pct": 5,
                "audience_interests": ["красота", "мода"],
            },
        )

        text = build_embedding_text(insights)
        assert "муж. 15%" in text
        assert "жен. 80%" in text
        assert "красота" in text

    def test_embedding_text_includes_marketing(self) -> None:
        """Текст включает маркетинговую ценность."""
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(
            marketing_value={
                "best_fit_industries": ["beauty", "fashion"],
                "not_suitable_for": ["алкоголь"],
            },
        )

        text = build_embedding_text(insights)
        assert "beauty" in text
        assert "алкоголь" in text


class TestEmbeddingQualityFields:
    """Тесты quality-полей в embedding тексте."""

    def test_includes_engagement_quality(self) -> None:
        """engagement_quality=organic → 'органическая' в тексте."""
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(audience_inference={"engagement_quality": "organic"})
        text = build_embedding_text(insights)
        assert "органическая" in text

    def test_includes_brand_safety_score(self) -> None:
        """brand_safety_score → 'безопасность бренда: N/5' в тексте."""
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(marketing_value={"brand_safety_score": 4})
        text = build_embedding_text(insights)
        assert "безопасность бренда: 4/5" in text

    def test_includes_lifestyle_level(self) -> None:
        """lifestyle_level → в тексте."""
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(lifestyle={"lifestyle_level": "premium"})
        text = build_embedding_text(insights)
        assert "premium" in text

    def test_includes_content_quality(self) -> None:
        """content_quality → 'качество контента: X' в тексте."""
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(content={"content_quality": "high"})
        text = build_embedding_text(insights)
        assert "качество контента: high" in text

    def test_includes_collaboration_risk(self) -> None:
        """collaboration_risk → 'риск коллаборации: X' в тексте."""
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(marketing_value={"collaboration_risk": "low"})
        text = build_embedding_text(insights)
        assert "риск коллаборации: low" in text

    def test_short_summary_without_label(self) -> None:
        """short_summary используется без short_label."""
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(
            short_summary="Готовит казахскую кухню, снимает рецепты для YouTube.",
        )
        text = build_embedding_text(insights)
        assert "Готовит казахскую кухню" in text

    def test_empty_quality_fields_no_section(self) -> None:
        """Без quality-полей секция 'Характеристики' не добавляется."""
        from src.ai.embedding import build_embedding_text

        insights = AIInsights()
        text = build_embedding_text(insights)
        assert "Характеристики" not in text


class TestGenerateEmbedding:
    """Тесты вызова OpenAI Embeddings API."""

    @pytest.mark.asyncio
    async def test_generate_embedding_returns_vector(self) -> None:
        """Успешная генерация -> вектор 1536 dim."""
        from src.ai.embedding import generate_embedding

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_embedding = MagicMock()
        mock_embedding.embedding = [0.1] * 1536
        mock_response.data = [mock_embedding]
        mock_client.embeddings.create.return_value = mock_response

        vector = await generate_embedding(mock_client, "тестовый текст")

        assert vector is not None
        assert len(vector) == 1536
        mock_client.embeddings.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_generate_embedding_uses_correct_model(self) -> None:
        """Использует text-embedding-3-small."""
        from src.ai.embedding import generate_embedding

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_embedding = MagicMock()
        mock_embedding.embedding = [0.1] * 1536
        mock_response.data = [mock_embedding]
        mock_client.embeddings.create.return_value = mock_response

        await generate_embedding(mock_client, "текст")

        mock_client.embeddings.create.assert_called_once_with(
            model="text-embedding-3-small",
            input="текст",
        )

    @pytest.mark.asyncio
    async def test_generate_embedding_handles_error(self) -> None:
        """Ошибка API -> None (не крашит пайплайн)."""
        from src.ai.embedding import generate_embedding

        mock_client = AsyncMock()
        mock_client.embeddings.create.side_effect = Exception("API error")

        vector = await generate_embedding(mock_client, "текст")

        assert vector is None
