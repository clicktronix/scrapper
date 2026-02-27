"""Тесты AI-схем для structured output."""
import pytest


class TestAIInsights:
    """Тесты валидации AIInsights."""

    def test_empty_insights(self) -> None:
        """Все поля по умолчанию — валидно."""
        from src.ai.schemas import AIInsights

        insights = AIInsights()
        assert insights.confidence == 3
        assert insights.reasoning == ""
        assert insights.summary == ""
        assert insights.life_situation.has_children is None
        assert insights.content.primary_categories == []
        assert insights.blogger_profile.page_type is None
        assert insights.marketing_value.best_fit_industries == []

    def test_full_insights(self) -> None:
        """Полностью заполненный ответ AI."""
        from src.ai.schemas import AIInsights

        data = {
            "reasoning": "Тестовый анализ блогера.",
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
                "primary_categories": ["family"],
                "secondary_topics": ["Материнство", "Повседневная жизнь"],
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
                "audience_male_pct": 20,
                "audience_female_pct": 75,
                "audience_other_pct": 5,
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
                "brand_safety_score": 4,
                "values_and_causes": ["экология", "ЗОЖ"],
            },
            "confidence": 4,
        }
        insights = AIInsights.model_validate(data)
        assert insights.summary == "Мама двоих детей из Алматы, ведёт блог о материнстве."
        assert insights.blogger_profile.page_type == "blog"
        assert insights.blogger_profile.profession == "визажист"
        assert insights.life_situation.has_children is True
        assert insights.lifestyle.car_class == "premium"
        assert insights.content.primary_categories == ["family"]
        assert insights.content.preferred_format == "photo"
        assert insights.commercial.detected_brands == ["L'Oreal", "Chicco"]
        assert insights.commercial.ad_format == ["integration", "stories"]
        assert insights.audience_inference.engagement_quality == "organic"
        assert insights.marketing_value.brand_safety_score == 4
        assert insights.confidence == 4

    def test_confidence_bounds(self) -> None:
        """confidence принимает только Literal[1, 2, 3, 4, 5]."""
        from pydantic import ValidationError

        from src.ai.schemas import AIInsights

        with pytest.raises(ValidationError):
            AIInsights(confidence=0)

        with pytest.raises(ValidationError):
            AIInsights(confidence=6)

    def test_brand_safety_score_bounds(self) -> None:
        """brand_safety_score принимает только Literal[1, 2, 3, 4, 5]."""
        from pydantic import ValidationError

        from src.ai.schemas import MarketingValue

        with pytest.raises(ValidationError):
            MarketingValue(brand_safety_score=0)

        with pytest.raises(ValidationError):
            MarketingValue(brand_safety_score=6)

    def test_json_schema_generation(self) -> None:
        """model_json_schema() генерирует валидный JSON Schema."""
        from src.ai.schemas import AIInsights

        schema = AIInsights.model_json_schema()
        assert "properties" in schema
        assert "reasoning" in schema["properties"]
        assert "summary" in schema["properties"]
        assert "blogger_profile" in schema["properties"]
        assert "marketing_value" in schema["properties"]
        assert "confidence" in schema["properties"]
        # Проверяем наличие description хотя бы у некоторых полей
        assert "description" in schema["properties"]["reasoning"]
        assert "description" in schema["properties"]["confidence"]

    def test_from_json_string(self) -> None:
        """Парсинг из JSON-строки (как от OpenAI)."""
        from src.ai.schemas import AIInsights

        raw = (
            '{"reasoning": "", "summary": "Тест", "blogger_profile": {}, "life_situation": {},'
            ' "lifestyle": {}, "content": {}, "commercial": {},'
            ' "audience_inference": {}, "marketing_value": {},'
            ' "tags": ["видео-контент", "reels", "юмор"],'
            ' "confidence": 4}'
        )
        insights = AIInsights.model_validate_json(raw)
        assert insights.confidence == 4
        assert insights.summary == "Тест"

    def test_boundary_confidence_one(self) -> None:
        """confidence=1 — валидно (минимальная граница Literal)."""
        from src.ai.schemas import AIInsights

        insights = AIInsights(confidence=1)
        assert insights.confidence == 1

    def test_boundary_confidence_five(self) -> None:
        """confidence=5 — валидно (максимальная граница Literal)."""
        from src.ai.schemas import AIInsights

        insights = AIInsights(confidence=5)
        assert insights.confidence == 5

    def test_invalid_literal_raises(self) -> None:
        """Невалидные значения Literal полей -> ValidationError."""
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
            "confidence": 4,
            "unknown_field": "something",
            "life_situation": {
                "has_children": True,
                "some_extra": 42,
            },
        }
        with pytest.raises(ValidationError):
            AIInsights.model_validate(data)

    def test_model_dump_round_trip(self) -> None:
        """model_dump -> model_validate round trip."""
        from src.ai.schemas import AIInsights

        original = AIInsights(
            summary="Тестовый блогер из Алматы.",
            confidence=4,
            tags=["видео-контент", "reels", "юмор"],
        )
        original.life_situation.has_children = True
        original.blogger_profile.page_type = "blog"
        dumped = original.model_dump()
        restored = AIInsights.model_validate(dumped)

        assert restored.confidence == 4
        assert restored.summary == "Тестовый блогер из Алматы."
        assert restored.life_situation.has_children is True
        assert restored.blogger_profile.page_type == "blog"

    def test_partial_sub_models(self) -> None:
        """Частично заполненные sub-models — остальные поля None."""
        from src.ai.schemas import AIInsights

        insights = AIInsights.model_validate({
            "life_situation": {"has_children": True},
            "content": {"primary_categories": ["beauty"]},
            "blogger_profile": {"page_type": "business"},
        })
        assert insights.life_situation.has_children is True
        assert insights.life_situation.relationship_status is None
        assert insights.content.primary_categories == ["beauty"]
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
            tags=["видео-контент", "reels", "юмор", "мама", "ЗОЖ", "эстетика", "сторителлинг"],
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
        assert len(insights.tags) == 7
        assert insights.blogger_profile.has_manager is True
        assert insights.blogger_profile.manager_contact == "@manager_account"
        assert insights.blogger_profile.country == "Казахстан"
        assert insights.commercial.ambassador_brands == ["Kaspi", "Magnum"]

    def test_all_fields_have_description(self) -> None:
        """Все поля AIInsights и вложенных моделей имеют description в schema."""
        from src.ai.schemas import AIInsights

        schema = AIInsights.model_json_schema()

        # Проверяем top-level properties (включая sub-model поля с $ref)
        for name, prop in schema["properties"].items():
            assert "description" in prop, f"Field '{name}' missing description"

        # Проверяем $defs (вложенные модели)
        for model_name, model_schema in schema.get("$defs", {}).items():
            for name, prop in model_schema.get("properties", {}).items():
                assert "description" in prop, (
                    f"Field '{model_name}.{name}' missing description"
                )

    def test_reasoning_is_first_property(self) -> None:
        """reasoning — первое поле в AIInsights."""
        from src.ai.schemas import AIInsights

        schema = AIInsights.model_json_schema()
        first_prop = list(schema["properties"].keys())[0]
        assert first_prop == "reasoning"

    def test_confidence_literal_values(self) -> None:
        """confidence принимает значения 1-5."""
        from src.ai.schemas import AIInsights

        for val in (1, 2, 3, 4, 5):
            insights = AIInsights(confidence=val)
            assert insights.confidence == val

    def test_brand_safety_literal_values(self) -> None:
        """brand_safety_score принимает значения 1-5."""
        from src.ai.schemas import MarketingValue

        for val in (1, 2, 3, 4, 5):
            mv = MarketingValue(brand_safety_score=val)
            assert mv.brand_safety_score == val


class TestPrimaryCategories:
    """Тесты primary_categories (замена primary_topic)."""

    def test_primary_categories_default_empty(self) -> None:
        from src.ai.schemas import AIInsights

        insights = AIInsights()
        assert insights.content.primary_categories == []

    def test_primary_categories_accepts_list(self) -> None:
        from src.ai.schemas import AIInsights

        insights = AIInsights(content={"primary_categories": ["beauty", "fashion", "lifestyle"]})
        assert insights.content.primary_categories == ["beauty", "fashion", "lifestyle"]

    def test_primary_categories_max_3(self) -> None:
        """primary_categories принимает до 3 элементов."""
        from src.ai.schemas import AIInsights

        insights = AIInsights(content={"primary_categories": ["beauty", "fashion", "lifestyle"]})
        assert len(insights.content.primary_categories) == 3

    def test_primary_categories_rejects_more_than_3(self) -> None:
        """primary_categories > 3 элементов → ValidationError."""
        from src.ai.schemas import AIInsights

        with pytest.raises(Exception):
            AIInsights(content={"primary_categories": ["a", "b", "c", "d"]})

    def test_primary_categories_rejects_unknown_code(self) -> None:
        """primary_categories принимает только коды из справочника."""
        from pydantic import ValidationError

        from src.ai.schemas import AIInsights

        with pytest.raises(ValidationError):
            AIInsights(content={"primary_categories": ["unknown-category-code"]})

    def test_primary_topic_removed(self) -> None:
        """Поле primary_topic удалено из ContentProfile."""
        from src.ai.schemas import ContentProfile

        assert "primary_topic" not in ContentProfile.model_fields


class TestTagsMinLength:
    """Тесты min_length=3 для tags."""

    def test_tags_min_length_3_in_json_schema(self) -> None:
        """JSON-схема содержит minItems=3 для tags."""
        from src.ai.schemas import AIInsights

        schema = AIInsights.model_json_schema()
        tags_schema = schema["properties"]["tags"]
        assert tags_schema.get("minItems") == 3
        assert tags_schema.get("maxItems") == 40

    def test_secondary_topics_max_length_5_in_json_schema(self) -> None:
        """JSON-схема ограничивает secondary_topics до 5 элементов."""
        from src.ai.schemas import AIInsights

        schema = AIInsights.model_json_schema()
        content_schema = schema["$defs"]["ContentProfile"]["properties"]["secondary_topics"]
        assert content_schema.get("maxItems") == 5

    def test_tags_default_factory_still_works(self) -> None:
        """default_factory=list по-прежнему создаёт пустой список (min_length не влияет на default)."""
        from src.ai.schemas import AIInsights

        # min_length в Pydantic валидирует только явно переданные значения,
        # default_factory=list проходит т.к. это default
        insights = AIInsights()
        assert insights.tags == []

    def test_tags_reject_unknown_values(self) -> None:
        """tags валидируются по enum-списку."""
        from pydantic import ValidationError

        from src.ai.schemas import AIInsights

        with pytest.raises(ValidationError):
            AIInsights(tags=["foo", "bar", "baz"])

    def test_secondary_topics_reject_unknown_values(self) -> None:
        """secondary_topics валидируются по enum-списку."""
        from pydantic import ValidationError

        from src.ai.schemas import AIInsights

        with pytest.raises(ValidationError):
            AIInsights(content={"secondary_topics": ["Неизвестная подкатегория"]})


class TestEnumConstraints:
    """Тесты enum ограничений для OpenAI structured outputs."""

    def test_tags_enum_in_json_schema(self) -> None:
        """JSON-схема tags содержит enum из ALL_TAG_NAMES."""
        from src.ai.schemas import AIInsights
        from src.ai.taxonomy import ALL_TAG_NAMES

        schema = AIInsights.model_json_schema()
        tags_items = schema["properties"]["tags"]["items"]
        assert "enum" in tags_items
        assert tags_items["enum"] == ALL_TAG_NAMES

    def test_primary_categories_enum_in_json_schema(self) -> None:
        """JSON-схема primary_categories содержит enum из ALL_CATEGORY_CODES."""
        from src.ai.schemas import AIInsights
        from src.ai.taxonomy import ALL_CATEGORY_CODES

        schema = AIInsights.model_json_schema()
        content_ref = schema["properties"]["content"]["$ref"]
        content_def = content_ref.split("/")[-1]
        content_schema = schema["$defs"][content_def]
        cat_items = content_schema["properties"]["primary_categories"]["items"]
        assert "enum" in cat_items
        assert cat_items["enum"] == ALL_CATEGORY_CODES

    def test_secondary_topics_enum_in_json_schema(self) -> None:
        """JSON-схема secondary_topics содержит enum из ALL_SUBCATEGORY_NAMES."""
        from src.ai.schemas import AIInsights
        from src.ai.taxonomy import ALL_SUBCATEGORY_NAMES

        schema = AIInsights.model_json_schema()
        content_ref = schema["properties"]["content"]["$ref"]
        content_def = content_ref.split("/")[-1]
        content_schema = schema["$defs"][content_def]
        sub_items = content_schema["properties"]["secondary_topics"]["items"]
        assert "enum" in sub_items
        assert sub_items["enum"] == ALL_SUBCATEGORY_NAMES

    def test_secondary_topics_max_length_5(self) -> None:
        """JSON-схема secondary_topics имеет maxItems=5."""
        from src.ai.schemas import AIInsights

        schema = AIInsights.model_json_schema()
        content_ref = schema["properties"]["content"]["$ref"]
        content_def = content_ref.split("/")[-1]
        content_schema = schema["$defs"][content_def]
        assert content_schema["properties"]["secondary_topics"]["maxItems"] == 5

    def test_enum_survives_strict_schema(self) -> None:
        """enum ограничения сохраняются после _make_strict_schema."""
        from src.ai.batch import _make_strict_schema
        from src.ai.schemas import AIInsights

        schema = _make_strict_schema(AIInsights.model_json_schema())
        tags_items = schema["properties"]["tags"]["items"]
        assert "enum" in tags_items
        assert len(tags_items["enum"]) > 0
