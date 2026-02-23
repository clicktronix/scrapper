"""Тесты AI-схем для structured output."""
import pytest


class TestAIInsights:
    """Тесты валидации AIInsights."""

    def test_empty_insights(self) -> None:
        """Все поля по умолчанию — валидно."""
        from src.ai.schemas import AIInsights

        insights = AIInsights()
        assert insights.confidence == 0.5
        assert insights.summary == ""
        assert insights.life_situation.has_children is None
        assert insights.content.primary_topic is None
        assert insights.blogger_profile.page_type is None
        assert insights.marketing_value.best_fit_industries == []

    def test_full_insights(self) -> None:
        """Полностью заполненный ответ AI."""
        from src.ai.schemas import AIInsights

        data = {
            "summary": "Мама двоих детей из Алматы, ведёт блог о материнстве.",
            "blogger_profile": {
                "estimated_age": "25-34",
                "gender": "female",
                "city": "Алматы",
                "profession": "визажист",
                "education": "university",
                "speaks_languages": ["русский", "казахский"],
                "page_type": "blog",
            },
            "life_situation": {
                "has_children": True,
                "children_age_group": "toddler",
                "relationship_status": "married",
                "is_young_parent": True,
            },
            "lifestyle": {
                "has_car": True,
                "car_class": "premium",
                "travels_frequently": True,
                "travel_style": "luxury",
                "has_pets": False,
                "pet_types": [],
                "has_real_estate": True,
                "lifestyle_level": "premium",
            },
            "content": {
                "primary_topic": "материнство",
                "secondary_topics": ["путешествия", "красота"],
                "content_language": ["русский", "казахский"],
                "content_tone": "positive",
                "posts_in_russian": True,
                "posts_in_kazakh": True,
                "preferred_format": "photo",
                "content_quality": "high",
                "uses_professional_photo": True,
                "has_consistent_visual_style": True,
                "posting_frequency": "several_per_week",
                "audience_interaction": "high",
                "call_to_action_style": "вопросы к аудитории",
            },
            "commercial": {
                "has_brand_collaborations": True,
                "detected_brand_categories": ["косметика", "детские товары"],
                "detected_brands": ["L'Oreal", "Chicco"],
                "has_affiliate_links": False,
                "is_active_advertiser": True,
                "ad_frequency": "moderate",
                "ad_format": ["integration", "stories"],
                "has_price_list": True,
                "estimated_price_tier": "mid",
                "open_to_barter": False,
                "has_own_product": True,
                "own_product_type": "курс по макияжу",
            },
            "audience_inference": {
                "estimated_audience_gender": "mostly_female",
                "estimated_audience_age": "25-34",
                "estimated_audience_geo": "kz",
                "geo_mentions": ["Алматы", "Астана"],
                "estimated_audience_income": "medium",
                "audience_interests": ["красота", "материнство"],
                "engagement_quality": "organic",
                "comments_sentiment": "positive",
            },
            "marketing_value": {
                "best_fit_industries": ["красота", "детские товары"],
                "not_suitable_for": ["алкоголь"],
                "collaboration_risk": "low",
                "brand_safety_score": 0.9,
                "values_and_causes": ["экология", "ЗОЖ"],
            },
            "confidence": 0.85,
        }
        insights = AIInsights.model_validate(data)
        assert insights.summary == "Мама двоих детей из Алматы, ведёт блог о материнстве."
        assert insights.blogger_profile.page_type == "blog"
        assert insights.blogger_profile.profession == "визажист"
        assert insights.life_situation.has_children is True
        assert insights.lifestyle.car_class == "premium"
        assert insights.content.primary_topic == "материнство"
        assert insights.content.preferred_format == "photo"
        assert insights.commercial.detected_brands == ["L'Oreal", "Chicco"]
        assert insights.commercial.ad_format == ["integration", "stories"]
        assert insights.audience_inference.engagement_quality == "organic"
        assert insights.marketing_value.brand_safety_score == 0.9
        assert insights.confidence == 0.85

    def test_confidence_bounds(self) -> None:
        """confidence должен быть 0.0-1.0."""
        from pydantic import ValidationError

        from src.ai.schemas import AIInsights

        with pytest.raises(ValidationError):
            AIInsights(confidence=1.5)

        with pytest.raises(ValidationError):
            AIInsights(confidence=-0.1)

    def test_brand_safety_score_bounds(self) -> None:
        """brand_safety_score должен быть 0.0-1.0."""
        from pydantic import ValidationError

        from src.ai.schemas import MarketingValue

        with pytest.raises(ValidationError):
            MarketingValue(brand_safety_score=1.5)

        with pytest.raises(ValidationError):
            MarketingValue(brand_safety_score=-0.1)

    def test_json_schema_generation(self) -> None:
        """model_json_schema() генерирует валидный JSON Schema."""
        from src.ai.schemas import AIInsights

        schema = AIInsights.model_json_schema()
        assert "properties" in schema
        assert "summary" in schema["properties"]
        assert "blogger_profile" in schema["properties"]
        assert "marketing_value" in schema["properties"]
        assert "confidence" in schema["properties"]

    def test_from_json_string(self) -> None:
        """Парсинг из JSON-строки (как от OpenAI)."""
        from src.ai.schemas import AIInsights

        raw = (
            '{"summary": "Тест", "blogger_profile": {}, "life_situation": {},'
            ' "lifestyle": {}, "content": {}, "commercial": {},'
            ' "audience_inference": {}, "marketing_value": {},'
            ' "confidence": 0.7}'
        )
        insights = AIInsights.model_validate_json(raw)
        assert insights.confidence == 0.7
        assert insights.summary == "Тест"

    def test_boundary_confidence_zero(self) -> None:
        """confidence=0.0 — валидно (минимальная граница)."""
        from src.ai.schemas import AIInsights

        insights = AIInsights(confidence=0.0)
        assert insights.confidence == 0.0

    def test_boundary_confidence_one(self) -> None:
        """confidence=1.0 — валидно (максимальная граница)."""
        from src.ai.schemas import AIInsights

        insights = AIInsights(confidence=1.0)
        assert insights.confidence == 1.0

    def test_invalid_literal_raises(self) -> None:
        """Невалидные значения Literal полей → ValidationError."""
        from pydantic import ValidationError

        from src.ai.schemas import AIInsights

        with pytest.raises(ValidationError):
            AIInsights.model_validate({
                "lifestyle": {"car_class": "tank"},
            })

        with pytest.raises(ValidationError):
            AIInsights.model_validate({
                "content": {"content_tone": "aggressive"},
            })

        with pytest.raises(ValidationError):
            AIInsights.model_validate({
                "blogger_profile": {"page_type": "forum"},
            })

        with pytest.raises(ValidationError):
            AIInsights.model_validate({
                "commercial": {"ad_format": ["tiktok_dance"]},
            })

    def test_extra_fields_forbidden(self) -> None:
        """Неизвестные поля от AI запрещены (strict structured output)."""
        from pydantic import ValidationError

        from src.ai.schemas import AIInsights

        data = {
            "confidence": 0.8,
            "unknown_field": "something",
            "life_situation": {
                "has_children": True,
                "some_extra": 42,
            },
        }
        with pytest.raises(ValidationError):
            AIInsights.model_validate(data)

    def test_model_dump_round_trip(self) -> None:
        """model_dump → model_validate round trip."""
        from src.ai.schemas import AIInsights

        original = AIInsights(
            summary="Тестовый блогер из Алматы.",
            confidence=0.9,
        )
        original.life_situation.has_children = True
        original.blogger_profile.page_type = "blog"
        dumped = original.model_dump()
        restored = AIInsights.model_validate(dumped)

        assert restored.confidence == 0.9
        assert restored.summary == "Тестовый блогер из Алматы."
        assert restored.life_situation.has_children is True
        assert restored.blogger_profile.page_type == "blog"

    def test_partial_sub_models(self) -> None:
        """Частично заполненные sub-models — остальные поля None."""
        from src.ai.schemas import AIInsights

        insights = AIInsights.model_validate({
            "life_situation": {"has_children": True},
            "content": {"primary_topic": "красота"},
            "blogger_profile": {"page_type": "business"},
        })
        assert insights.life_situation.has_children is True
        assert insights.life_situation.relationship_status is None
        assert insights.content.primary_topic == "красота"
        assert insights.content.content_tone is None
        assert insights.blogger_profile.page_type == "business"
        assert insights.blogger_profile.profession is None
        # Незаданные sub-models — дефолтные пустые
        assert insights.lifestyle.has_car is None
        assert insights.commercial.has_brand_collaborations is None
        assert insights.marketing_value.best_fit_industries == []

    def test_page_type_literals(self) -> None:
        """Все варианты page_type валидны."""
        from src.ai.schemas import BloggerProfile

        for pt in ("blog", "public", "business"):
            bp = BloggerProfile(page_type=pt)
            assert bp.page_type == pt

    def test_ad_format_list(self) -> None:
        """ad_format принимает список Literal-значений."""
        from src.ai.schemas import CommercialActivity

        ca = CommercialActivity(
            ad_format=["integration", "dedicated_post", "stories", "reels", "unboxing", "review"]
        )
        assert len(ca.ad_format) == 6

    def test_new_fields_defaults(self) -> None:
        """Новые поля имеют дефолтные значения."""
        from src.ai.schemas import AIInsights

        insights = AIInsights()
        assert insights.short_label == ""
        assert insights.short_summary == ""
        assert insights.tags == []
        assert insights.blogger_profile.has_manager is None
        assert insights.blogger_profile.manager_contact is None
        assert insights.blogger_profile.country is None
        assert insights.commercial.ambassador_brands == []

    def test_new_fields_filled(self) -> None:
        """Новые поля заполняются корректно."""
        from src.ai.schemas import AIInsights

        insights = AIInsights(
            short_label="фуд-блогер",
            short_summary="Готовит казахскую кухню, снимает рецепты.",
            tags=["видео-контент", "reels", "рецепты"],
            blogger_profile={
                "has_manager": True,
                "manager_contact": "@manager_account",
                "country": "Казахстан",
            },
            commercial={
                "ambassador_brands": ["Kaspi", "Magnum"],
            },
        )
        assert insights.short_label == "фуд-блогер"
        assert insights.short_summary == "Готовит казахскую кухню, снимает рецепты."
        assert len(insights.tags) == 3
        assert insights.blogger_profile.has_manager is True
        assert insights.blogger_profile.manager_contact == "@manager_account"
        assert insights.blogger_profile.country == "Казахстан"
        assert insights.commercial.ambassador_brands == ["Kaspi", "Magnum"]
