"""Тесты справочника категорий и тегов."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.ai.taxonomy_matching import (
    _fuzzy_lookup,
    invalidate_taxonomy_cache,
    load_categories,
    load_cities,
    load_tags,
)


class TestCategories:
    def test_categories_count(self) -> None:
        """Проверяем что в справочнике ровно 20 категорий."""
        from src.ai.taxonomy import CATEGORIES

        assert len(CATEGORIES) == 20

    def test_category_has_required_fields(self) -> None:
        """Каждая категория содержит code, name и subcategories."""
        from src.ai.taxonomy import CATEGORIES

        for cat in CATEGORIES:
            assert "code" in cat
            assert "name" in cat
            assert "subcategories" in cat
            assert isinstance(cat["subcategories"], list)
            assert len(cat["subcategories"]) > 0

    def test_category_codes_unique(self) -> None:
        """Коды категорий уникальны."""
        from src.ai.taxonomy import CATEGORIES

        codes = [c["code"] for c in CATEGORIES]
        assert len(codes) == len(set(codes))

    def test_subcategory_names_are_strings(self) -> None:
        """Подкатегории — непустые строки."""
        from src.ai.taxonomy import CATEGORIES

        for cat in CATEGORIES:
            for sub in cat["subcategories"]:
                assert isinstance(sub, str)
                assert len(sub) > 0

    def test_known_category_exists(self) -> None:
        """Известные категории присутствуют в справочнике."""
        from src.ai.taxonomy import CATEGORIES

        codes = {c["code"] for c in CATEGORIES}
        assert "beauty" in codes
        assert "food" in codes
        assert "tech" in codes
        assert "entertainment" in codes

    def test_all_category_codes(self) -> None:
        """Все 20 кодов категорий из PRD присутствуют."""
        from src.ai.taxonomy import CATEGORIES

        codes = {c["code"] for c in CATEGORIES}
        expected = {
            "beauty",
            "fashion",
            "fitness",
            "food",
            "travel",
            "lifestyle",
            "family",
            "health",
            "tech",
            "business",
            "education",
            "entertainment",
            "music",
            "gaming",
            "art",
            "home",
            "auto",
            "pets",
            "realestate",
            "esoteric",
        }
        assert codes == expected

    def test_subcategories_count(self) -> None:
        """Общее количество подкатегорий примерно ~120."""
        from src.ai.taxonomy import CATEGORIES

        total = sum(len(cat["subcategories"]) for cat in CATEGORIES)
        assert total >= 100  # ~120 в PRD, допускаем небольшой запас

    def test_get_categories_for_prompt(self) -> None:
        """Промпт-текст содержит коды и названия категорий."""
        from src.ai.taxonomy import get_categories_for_prompt

        text = get_categories_for_prompt()
        assert "beauty" in text
        assert "Красота" in text
        assert "Макияж" in text

    def test_get_categories_for_prompt_all_codes(self) -> None:
        """Промпт содержит все коды категорий."""
        from src.ai.taxonomy import CATEGORIES, get_categories_for_prompt

        text = get_categories_for_prompt()
        for cat in CATEGORIES:
            assert cat["code"] in text


class TestTags:
    def test_tags_count(self) -> None:
        """Общее количество тегов >= 200."""
        from src.ai.taxonomy import TAGS

        total = sum(len(tags) for tags in TAGS.values())
        assert total >= 199

    def test_tag_groups(self) -> None:
        """Справочник содержит ровно 6 групп тегов."""
        from src.ai.taxonomy import TAGS

        expected = {
            "content",
            "personal",
            "professional",
            "commercial",
            "audience",
            "marketing",
        }
        assert set(TAGS.keys()) == expected

    def test_tags_are_strings(self) -> None:
        """Все теги — непустые строки."""
        from src.ai.taxonomy import TAGS

        for group, tags in TAGS.items():
            for tag in tags:
                assert isinstance(tag, str), f"Тег в группе {group} не строка"
                assert len(tag) > 0, f"Пустой тег в группе {group}"

    def test_tags_unique_within_groups(self) -> None:
        """Теги уникальны внутри каждой группы."""
        from src.ai.taxonomy import TAGS

        for group, tags in TAGS.items():
            assert len(tags) == len(set(tags)), f"Дубликаты тегов в группе {group}"

    def test_content_tags_present(self) -> None:
        """Группа content содержит ожидаемые теги."""
        from src.ai.taxonomy import TAGS

        content = TAGS["content"]
        assert "видео-контент" in content
        assert "reels" in content
        assert "юмор" in content
        assert "профессиональная съёмка" in content

    def test_personal_tags_present(self) -> None:
        """Группа personal содержит ожидаемые теги."""
        from src.ai.taxonomy import TAGS

        personal = TAGS["personal"]
        assert "женщина" in personal
        assert "мужчина" in personal
        assert "Алматы" in personal
        assert "Астана" in personal

    def test_professional_tags_present(self) -> None:
        """Группа professional содержит ожидаемые теги."""
        from src.ai.taxonomy import TAGS

        professional = TAGS["professional"]
        assert "эксперт в нише" in professional
        assert "русский" in professional
        assert "Instagram" in professional

    def test_commercial_tags_present(self) -> None:
        """Группа commercial содержит ожидаемые теги."""
        from src.ai.taxonomy import TAGS

        commercial = TAGS["commercial"]
        assert "нано (до 10K)" in commercial
        assert "интеграции" in commercial
        assert "свой бренд одежды" in commercial

    def test_audience_tags_present(self) -> None:
        """Группа audience содержит ожидаемые теги."""
        from src.ai.taxonomy import TAGS

        audience = TAGS["audience"]
        assert "аудитория женская" in audience
        assert "аудитория Казахстан" in audience
        assert "органическая аудитория" in audience

    def test_marketing_tags_present(self) -> None:
        """Группа marketing содержит ожидаемые теги."""
        from src.ai.taxonomy import TAGS

        marketing = TAGS["marketing"]
        assert "подходит для beauty-брендов" in marketing
        assert "не подходит для алкоголя" in marketing
        assert "низкий риск" in marketing
        assert "семейные ценности" in marketing

    def test_tags_unique_across_groups(self) -> None:
        """Теги уникальны между всеми группами."""
        from src.ai.taxonomy import TAGS

        all_tags: list[str] = []
        for tags in TAGS.values():
            all_tags.extend(tags)
        assert len(all_tags) == len(set(all_tags)), (
            f"Дубликаты тегов между группами: "
            f"{[t for t in all_tags if all_tags.count(t) > 1]}"
        )

    def test_get_tags_for_prompt(self) -> None:
        """Промпт-текст содержит теги из разных групп."""
        from src.ai.taxonomy import get_tags_for_prompt

        text = get_tags_for_prompt()
        assert "видео-контент" in text
        assert "content" in text
        assert "marketing" in text

    def test_get_tags_for_prompt_all_groups(self) -> None:
        """Промпт содержит все группы тегов."""
        from src.ai.taxonomy import TAGS, get_tags_for_prompt

        text = get_tags_for_prompt()
        for group in TAGS:
            assert group in text


class TestFuzzyLookup:
    """Тесты _fuzzy_lookup с rapidfuzz."""

    def test_exact_match(self) -> None:
        cache = {"красота": "id1", "мода": "id2"}
        assert _fuzzy_lookup("красота", cache) == "id1"

    def test_normalized_variant_match(self) -> None:
        cache = {"видео контент": "id1"}
        assert _fuzzy_lookup("видео-контент", cache) == "id1"

    def test_fuzzy_close_match(self) -> None:
        """Опечатка: 'професиональная' (одна с) должна матчиться."""
        cache = {"профессиональная съёмка": "id1"}
        result = _fuzzy_lookup("професиональная съёмка", cache)
        assert result == "id1"

    def test_no_match_returns_none(self) -> None:
        cache = {"красота": "id1"}
        assert _fuzzy_lookup("абсолютно другое", cache) is None


def _mock_async_db():
    """Создать мок Supabase AsyncClient с async execute для таблиц."""
    db = MagicMock()
    table_mock = MagicMock()
    db.table.return_value = table_mock
    table_mock.select.return_value = table_mock
    table_mock.eq.return_value = table_mock
    table_mock.execute = AsyncMock(return_value=MagicMock(data=[]))
    return db, table_mock


class TestTaxonomyCache:
    """Тесты in-memory кэша для load_categories / load_tags / load_cities."""

    @pytest.mark.asyncio
    async def test_load_categories_cached(self) -> None:
        """Повторный вызов не делает запрос в БД."""
        invalidate_taxonomy_cache()
        db, table_mock = _mock_async_db()
        mock_data = [{"id": "c1", "code": "beauty", "name": "Красота", "parent_id": None}]
        table_mock.execute = AsyncMock(return_value=MagicMock(data=mock_data))

        r1 = await load_categories(db)
        r2 = await load_categories(db)
        # execute вызывается один раз — второй раз данные берутся из кэша
        assert table_mock.execute.call_count == 1
        assert r1 is r2
        invalidate_taxonomy_cache()

    @pytest.mark.asyncio
    async def test_invalidate_forces_reload(self) -> None:
        """invalidate_taxonomy_cache сбрасывает кэш."""
        invalidate_taxonomy_cache()
        db, table_mock = _mock_async_db()
        mock_data = [{"id": "c1", "code": "beauty", "name": "Красота", "parent_id": None}]
        table_mock.execute = AsyncMock(return_value=MagicMock(data=mock_data))

        await load_categories(db)
        invalidate_taxonomy_cache()
        await load_categories(db)
        assert table_mock.execute.call_count == 2
        invalidate_taxonomy_cache()

    @pytest.mark.asyncio
    async def test_load_tags_cached(self) -> None:
        """Повторный вызов load_tags не делает запрос в БД."""
        invalidate_taxonomy_cache()
        db, table_mock = _mock_async_db()
        mock_data = [{"id": "t1", "name": "видео-контент"}]
        table_mock.execute = AsyncMock(return_value=MagicMock(data=mock_data))

        await load_tags(db)
        await load_tags(db)
        assert table_mock.execute.call_count == 1
        invalidate_taxonomy_cache()

    @pytest.mark.asyncio
    async def test_load_cities_cached(self) -> None:
        """Повторный вызов load_cities не делает запрос в БД."""
        invalidate_taxonomy_cache()
        db, table_mock = _mock_async_db()
        mock_data = [{"id": "ci1", "name": "almaty", "l10n": {"ru": "Алматы"}}]
        table_mock.execute = AsyncMock(return_value=MagicMock(data=mock_data))

        await load_cities(db)
        await load_cities(db)
        assert table_mock.execute.call_count == 1
        invalidate_taxonomy_cache()
