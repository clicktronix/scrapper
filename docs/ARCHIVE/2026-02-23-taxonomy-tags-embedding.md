# Таксономия, теги и Embedding — План реализации

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Внедрить трёхуровневую таксономию (категория → подкатегория → теги), обновить AI-анализ для выбора из фиксированных справочников, генерировать embedding после анализа.

**Architecture:** AI получает фиксированные списки категорий/тегов в промпте и возвращает коды (не свободный текст). После AI-анализа: match categories по коду, upsert теги, генерировать embedding-вектор из structured text. Embedding сохраняется в существующее поле `blogs.embedding VECTOR(1536)`.

**Tech Stack:** Python 3.12, Pydantic, OpenAI Batch API (gpt-5-nano), OpenAI Embeddings API (text-embedding-3-small), Supabase PostgreSQL, asyncio.

**Design doc:** `scraper/docs/PRD.md`

---

### Task 1: Обновить AIInsights — новые поля

**Files:**
- Modify: `src/ai/schemas.py:7-17` (BloggerProfile), `src/ai/schemas.py:65-82` (CommercialActivity), `src/ai/schemas.py:112-125` (AIInsights)
- Test: `tests/test_ai/test_schemas.py`

**Step 1: Написать тесты для новых полей**

В `tests/test_ai/test_schemas.py` добавить тесты:

```python
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
```

**Step 2: Запустить тесты — убедиться что падают**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_ai/test_schemas.py::TestAIInsights::test_new_fields_defaults tests/test_ai/test_schemas.py::TestAIInsights::test_new_fields_filled -v`
Expected: FAIL — поля не существуют

**Step 3: Добавить поля в схемы**

В `src/ai/schemas.py`:

1. В `BloggerProfile` (после строки 17) добавить:
```python
    has_manager: bool | None = None
    manager_contact: str | None = None  # контакт менеджера: "@username" или ссылка
    country: str | None = None  # на русском: "Казахстан", "Россия"
```

2. В `CommercialActivity` (после строки 82) добавить:
```python
    ambassador_brands: list[str] = Field(default_factory=list)  # бренды, у которых блогер амбассадор
```

3. В `AIInsights` (после строки 116, перед `blogger_profile`) добавить:
```python
    short_label: str = ""  # на русском: 2-3 слова ("фуд-блогер", "мама двоих")
    short_summary: str = ""  # на русском: 2-3 строки краткое описание
    tags: list[str] = Field(default_factory=list)  # теги из справочника
```

**Step 4: Запустить тесты — убедиться что проходят**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_ai/test_schemas.py -v`
Expected: ALL PASS

**Step 5: Запустить все тесты — проверить что ничего не сломалось**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/ -v --tb=short`
Expected: ALL PASS (existing tests handle `extra="forbid"` — новые optional поля не ломают)

**Step 6: Коммит**

```bash
cd /Users/clicktronix/Projects/ai/native/scraper
git add src/ai/schemas.py tests/test_ai/test_schemas.py
git commit -m "feat: add short_label, short_summary, tags, has_manager, country, ambassador_brands to AIInsights"
```

---

### Task 2: Создать модуль таксономии — категории и теги

**Files:**
- Create: `src/ai/taxonomy.py`
- Test: `tests/test_ai/test_taxonomy.py`

**Step 1: Написать тесты для модуля таксономии**

Создать `tests/test_ai/test_taxonomy.py`:

```python
"""Тесты справочника категорий и тегов."""


class TestCategories:
    """Тесты справочника категорий."""

    def test_categories_count(self) -> None:
        """Ровно 20 верхнеуровневых категорий."""
        from src.ai.taxonomy import CATEGORIES

        assert len(CATEGORIES) == 20

    def test_category_has_required_fields(self) -> None:
        """Каждая категория имеет code, name, subcategories."""
        from src.ai.taxonomy import CATEGORIES

        for cat in CATEGORIES:
            assert "code" in cat, f"Missing code: {cat}"
            assert "name" in cat, f"Missing name: {cat}"
            assert "subcategories" in cat, f"Missing subcategories: {cat}"
            assert isinstance(cat["subcategories"], list)
            assert len(cat["subcategories"]) > 0, f"Empty subcategories: {cat['code']}"

    def test_category_codes_unique(self) -> None:
        """Коды категорий уникальны."""
        from src.ai.taxonomy import CATEGORIES

        codes = [c["code"] for c in CATEGORIES]
        assert len(codes) == len(set(codes))

    def test_subcategory_names_are_strings(self) -> None:
        """Подкатегории — строки на русском."""
        from src.ai.taxonomy import CATEGORIES

        for cat in CATEGORIES:
            for sub in cat["subcategories"]:
                assert isinstance(sub, str), f"Non-string subcategory in {cat['code']}: {sub}"
                assert len(sub) > 0

    def test_known_category_exists(self) -> None:
        """Проверяем наличие конкретных категорий."""
        from src.ai.taxonomy import CATEGORIES

        codes = {c["code"] for c in CATEGORIES}
        assert "beauty" in codes
        assert "food" in codes
        assert "tech" in codes
        assert "entertainment" in codes

    def test_get_categories_for_prompt(self) -> None:
        """Генерация текста категорий для промпта."""
        from src.ai.taxonomy import get_categories_for_prompt

        text = get_categories_for_prompt()
        assert "beauty" in text
        assert "Красота" in text
        assert "Макияж" in text


class TestTags:
    """Тесты справочника тегов."""

    def test_tags_count(self) -> None:
        """Минимум 200 тегов."""
        from src.ai.taxonomy import TAGS

        total = sum(len(tags) for tags in TAGS.values())
        assert total >= 200, f"Only {total} tags, expected >= 200"

    def test_tag_groups(self) -> None:
        """Ровно 6 групп тегов."""
        from src.ai.taxonomy import TAGS

        expected_groups = {"content", "personal", "professional", "commercial", "audience", "marketing"}
        assert set(TAGS.keys()) == expected_groups

    def test_tags_are_strings(self) -> None:
        """Все теги — непустые строки."""
        from src.ai.taxonomy import TAGS

        for group, tags in TAGS.items():
            for tag in tags:
                assert isinstance(tag, str), f"Non-string tag in {group}: {tag}"
                assert len(tag) > 0, f"Empty tag in {group}"

    def test_tags_unique_within_groups(self) -> None:
        """Теги уникальны внутри каждой группы."""
        from src.ai.taxonomy import TAGS

        for group, tags in TAGS.items():
            assert len(tags) == len(set(tags)), f"Duplicate tags in {group}"

    def test_get_tags_for_prompt(self) -> None:
        """Генерация текста тегов для промпта."""
        from src.ai.taxonomy import get_tags_for_prompt

        text = get_tags_for_prompt()
        assert "content" in text.lower() or "Контент" in text
        assert "видео-контент" in text
```

**Step 2: Запустить тесты — убедиться что падают**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_ai/test_taxonomy.py -v`
Expected: FAIL — модуль не существует

**Step 3: Создать модуль таксономии**

Создать `src/ai/taxonomy.py` с данными из PRD (20 категорий, ~120 подкатегорий, 200+ тегов, функции для промпта).

Структура:

```python
"""Справочник категорий, подкатегорий и тегов для AI-анализа блогеров."""

# 20 верхнеуровневых категорий + подкатегории
CATEGORIES: list[dict[str, str | list[str]]] = [
    {
        "code": "beauty",
        "name": "Красота",
        "subcategories": [
            "Макияж", "Уход за кожей", "Волосы", "Ногти",
            "Парфюмерия", "Барберинг", "Визажисты", "Стилисты", "Косметологи",
        ],
    },
    # ... все 20 категорий из PRD раздел 2.2
]

# 200+ тегов по 6 группам
TAGS: dict[str, list[str]] = {
    "content": [
        "видео-контент", "фото-контент", "reels", "карусели", "сторис-контент",
        "прямые эфиры", "подкасты", "лонгриды", "shorts",
        "юмор", "эстетика", "образовательный", "мотивационный", "провокационный",
        # ... все теги из PRD разделы 3.3-3.8
    ],
    "personal": [...],
    "professional": [...],
    "commercial": [...],
    "audience": [...],
    "marketing": [...],
}


def get_categories_for_prompt() -> str:
    """Сформировать текст категорий для system prompt."""
    lines: list[str] = []
    for cat in CATEGORIES:
        subs = ", ".join(cat["subcategories"])
        lines.append(f"- {cat['code']} ({cat['name']}): {subs}")
    return "\n".join(lines)


def get_tags_for_prompt() -> str:
    """Сформировать текст тегов для system prompt."""
    lines: list[str] = []
    for group, tags in TAGS.items():
        lines.append(f"\n{group}: {', '.join(tags)}")
    return "\n".join(lines)
```

Заполнить ВСЕ данные из PRD раздел 2.2 (категории) и разделы 3.3-3.8 (теги).

**Step 4: Запустить тесты — убедиться что проходят**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_ai/test_taxonomy.py -v`
Expected: ALL PASS

**Step 5: Коммит**

```bash
cd /Users/clicktronix/Projects/ai/native/scraper
git add src/ai/taxonomy.py tests/test_ai/test_taxonomy.py
git commit -m "feat: add taxonomy module with 20 categories, 120 subcategories, 200+ tags"
```

---

### Task 3: Обновить system prompt — категории и теги из справочника

**Files:**
- Modify: `src/ai/prompt.py:8-43` (SYSTEM_PROMPT)
- Test: `tests/test_ai/test_prompt.py`

**Step 1: Написать тесты для обновлённого промпта**

В `tests/test_ai/test_prompt.py` добавить новый тестовый класс:

```python
class TestPromptIncludesTaxonomy:
    """Тесты: промпт содержит справочник категорий и тегов."""

    def test_system_prompt_contains_categories(self) -> None:
        """System prompt содержит список категорий."""
        from src.ai.prompt import SYSTEM_PROMPT

        assert "beauty" in SYSTEM_PROMPT
        assert "Красота" in SYSTEM_PROMPT
        assert "entertainment" in SYSTEM_PROMPT
        assert "Развлечения" in SYSTEM_PROMPT

    def test_system_prompt_contains_tags(self) -> None:
        """System prompt содержит список тегов."""
        from src.ai.prompt import SYSTEM_PROMPT

        assert "видео-контент" in SYSTEM_PROMPT
        assert "reels" in SYSTEM_PROMPT
        assert "brand safe" in SYSTEM_PROMPT

    def test_system_prompt_has_category_instructions(self) -> None:
        """System prompt содержит инструкции по выбору категорий."""
        from src.ai.prompt import SYSTEM_PROMPT

        assert "primary_topic" in SYSTEM_PROMPT
        assert "secondary_topics" in SYSTEM_PROMPT
        # AI должен выбирать ИЗ СПИСКА
        assert "код категории" in SYSTEM_PROMPT.lower() or "из списка" in SYSTEM_PROMPT.lower()

    def test_system_prompt_has_tag_instructions(self) -> None:
        """System prompt содержит инструкции по выбору тегов."""
        from src.ai.prompt import SYSTEM_PROMPT

        assert "tags" in SYSTEM_PROMPT
        assert "short_label" in SYSTEM_PROMPT
        assert "short_summary" in SYSTEM_PROMPT
```

**Step 2: Запустить тесты — убедиться что падают**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_ai/test_prompt.py::TestPromptIncludesTaxonomy -v`
Expected: FAIL — текущий промпт не содержит таксономию

**Step 3: Обновить SYSTEM_PROMPT**

В `src/ai/prompt.py`:

1. Добавить импорт: `from src.ai.taxonomy import get_categories_for_prompt, get_tags_for_prompt`
2. Добавить в конец SYSTEM_PROMPT блоки:

```python
SYSTEM_PROMPT = """\
Ты — аналитик инфлюенс-маркетинга. ...
[существующий текст без изменений]
...

ОПРЕДЕЛЕНИЕ short_label:
- 2-3 слова на русском, характеризующие блогера: "фуд-блогер", "мама двоих", "фитнес-тренер".

ОПРЕДЕЛЕНИЕ short_summary:
- 2-3 строки на русском: краткое описание блогера для быстрого понимания.

ОПРЕДЕЛЕНИЕ primary_topic:
- Выбери ОДИН код категории из списка ниже. Используй СТРОГО код (английский), не русское название.
- Пример: "beauty", не "Красота".

ОПРЕДЕЛЕНИЕ secondary_topics:
- Выбери до 5 подкатегорий из списка ниже (русские названия подкатегорий).
- Пример: ["Макияж", "Уход за кожей"].

ОПРЕДЕЛЕНИЕ tags:
- Выбери 15-40 тегов из списка ниже (русские).
- Если подходящего тега нет в списке, можешь предложить свой (он попадёт на модерацию).

ОПРЕДЕЛЕНИЕ has_manager:
- true если в био, контактах или контенте указан менеджер/агентство.
- manager_contact — контакт менеджера если есть.

ОПРЕДЕЛЕНИЕ country:
- Страна блогера на русском: "Казахстан", "Россия", "Узбекистан".

ОПРЕДЕЛЕНИЕ ambassador_brands:
- Бренды, у которых блогер является амбассадором (долгосрочное сотрудничество, а не разовая реклама).

КАТЕГОРИИ И ПОДКАТЕГОРИИ:
""" + get_categories_for_prompt() + """

ТЕГИ (выбирай из этого списка):
""" + get_tags_for_prompt()
```

**Step 4: Запустить тесты — убедиться что проходят**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_ai/test_prompt.py -v`
Expected: ALL PASS

**Step 5: Запустить все тесты**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/ -v --tb=short`
Expected: ALL PASS

**Step 6: Коммит**

```bash
cd /Users/clicktronix/Projects/ai/native/scraper
git add src/ai/prompt.py tests/test_ai/test_prompt.py
git commit -m "feat: include taxonomy (categories + tags) in AI analysis prompt"
```

---

### Task 4: Рефакторинг match_categories — матчинг по коду

**Files:**
- Modify: `src/ai/batch.py:302-359` (load_categories, match_categories)
- Test: `tests/test_ai/test_batch.py:792-870`

**Step 1: Обновить тесты match_categories**

Текущий `match_categories` использует `name.lower()`. Нужно перейти на `code`. Обновить существующие тесты в `tests/test_ai/test_batch.py`:

```python
class TestMatchCategories:
    """Тесты сопоставления тем с категориями по коду."""

    @pytest.mark.asyncio
    async def test_matches_primary_by_code(self) -> None:
        """Primary topic = код категории → upsert с is_primary=True."""
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_topic = "beauty"
        insights.content.secondary_topics = ["Макияж", "Уход за кожей"]

        mock_db = MagicMock()
        cat_mock = MagicMock()
        cat_mock.data = [
            {"id": "cat-1", "code": "beauty", "name": "Красота", "parent_id": None},
            {"id": "sub-1", "code": None, "name": "Макияж", "parent_id": "cat-1"},
            {"id": "sub-2", "code": None, "name": "Уход за кожей", "parent_id": "cat-1"},
        ]
        mock_db.table.return_value.select.return_value.execute.return_value = cat_mock

        upsert_mock = MagicMock()
        upsert_mock.execute.return_value = MagicMock()
        mock_db.table.return_value.upsert.return_value = upsert_mock

        await match_categories(mock_db, "blog-1", insights)

        # 1 primary (code) + 2 secondary (name) = 3 upserts
        assert mock_db.table.return_value.upsert.call_count == 3

    @pytest.mark.asyncio
    async def test_primary_by_code_secondary_by_name(self) -> None:
        """Primary матчится по code, secondary — по name (подкатегории)."""
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_topic = "food"
        insights.content.secondary_topics = ["Рецепты"]

        categories = {
            "food": "cat-food",  # code → id для primary
            "рецепты": "sub-recipes",  # name_lower → id для secondary
        }

        mock_db = MagicMock()
        upsert_mock = MagicMock()
        upsert_mock.execute.return_value = MagicMock()
        mock_db.table.return_value.upsert.return_value = upsert_mock

        await match_categories(mock_db, "blog-1", insights, categories=categories)

        assert mock_db.table.return_value.upsert.call_count == 2
```

**Step 2: Запустить тесты — убедиться что падают**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_ai/test_batch.py::TestMatchCategories -v`
Expected: FAIL — текущий load_categories не выбирает code

**Step 3: Обновить load_categories и match_categories**

В `src/ai/batch.py`:

```python
async def load_categories(db: Client) -> dict[str, str]:
    """Загрузить все категории из БД.

    Возвращает {key: category_id} где:
    - key = code (для верхнеуровневых, используется AI в primary_topic)
    - key = name_lower (для подкатегорий, используется AI в secondary_topics)
    """
    cat_result = await asyncio.to_thread(
        db.table("categories").select("id, code, name, parent_id").execute
    )
    categories: dict[str, str] = {}
    for c in cat_result.data:
        if not isinstance(c, dict):
            continue
        cat_id = c.get("id")
        if not isinstance(cat_id, str):
            continue
        # Верхнеуровневые категории — индексируем по code
        code = c.get("code")
        if isinstance(code, str) and code:
            categories[code] = cat_id
        # Все категории — индексируем по name_lower (для подкатегорий)
        name = c.get("name")
        if isinstance(name, str):
            categories[name.lower()] = cat_id
    return categories
```

`match_categories` остаётся без изменений — `primary_topic` теперь содержит код ("beauty"), который матчится с `categories["beauty"]`, а `secondary_topics` содержат русские названия подкатегорий ("Макияж"), которые матчатся с `categories["макияж"]`.

**Step 4: Запустить тесты**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_ai/test_batch.py -v --tb=short`
Expected: ALL PASS

**Step 5: Коммит**

```bash
cd /Users/clicktronix/Projects/ai/native/scraper
git add src/ai/batch.py tests/test_ai/test_batch.py
git commit -m "refactor: match_categories uses code for primary, name for subcategories"
```

---

### Task 5: Добавить match_tags — upsert тегов в blog_tags

**Files:**
- Modify: `src/ai/batch.py` (добавить load_tags, match_tags)
- Test: `tests/test_ai/test_batch.py`

**Step 1: Написать тесты**

В `tests/test_ai/test_batch.py` добавить:

```python
class TestMatchTags:
    """Тесты присвоения тегов блогеру."""

    @pytest.mark.asyncio
    async def test_matches_known_tags(self) -> None:
        """Теги из справочника → upsert в blog_tags."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео-контент", "reels", "юмор"]

        tags_cache = {
            "видео-контент": "tag-1",
            "reels": "tag-2",
            "юмор": "tag-3",
            "фото-контент": "tag-4",
        }

        mock_db = MagicMock()
        upsert_mock = MagicMock()
        upsert_mock.execute.return_value = MagicMock()
        mock_db.table.return_value.upsert.return_value = upsert_mock

        await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        assert mock_db.table.return_value.upsert.call_count == 3

    @pytest.mark.asyncio
    async def test_skips_unknown_tags(self) -> None:
        """Теги, отсутствующие в справочнике, пропускаются."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео-контент", "новый-неизвестный-тег"]

        tags_cache = {"видео-контент": "tag-1"}

        mock_db = MagicMock()
        upsert_mock = MagicMock()
        upsert_mock.execute.return_value = MagicMock()
        mock_db.table.return_value.upsert.return_value = upsert_mock

        await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        # Только 1 upsert — известный тег
        assert mock_db.table.return_value.upsert.call_count == 1

    @pytest.mark.asyncio
    async def test_empty_tags(self) -> None:
        """Пустой список тегов — нет upserts."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = []

        mock_db = MagicMock()
        await match_tags(mock_db, "blog-1", insights, tags={})

        mock_db.table.assert_not_called()

    @pytest.mark.asyncio
    async def test_loads_tags_from_db_when_cache_is_none(self) -> None:
        """Если tags=None — загрузить из БД."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео-контент"]

        mock_db = MagicMock()
        # load_tags query
        tags_mock = MagicMock()
        tags_mock.data = [{"id": "tag-1", "name": "видео-контент"}]
        mock_db.table.return_value.select.return_value.execute.return_value = tags_mock
        # upsert
        upsert_mock = MagicMock()
        upsert_mock.execute.return_value = MagicMock()
        mock_db.table.return_value.upsert.return_value = upsert_mock

        await match_tags(mock_db, "blog-1", insights, tags=None)

        # Должен был вызвать select для загрузки тегов
        mock_db.table.assert_any_call("tags")
```

**Step 2: Запустить тесты — убедиться что падают**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_ai/test_batch.py::TestMatchTags -v`
Expected: FAIL — match_tags не существует

**Step 3: Реализовать load_tags и match_tags**

В `src/ai/batch.py` после `match_categories`:

```python
async def load_tags(db: Client) -> dict[str, str]:
    """Загрузить все теги из БД. Возвращает {name_lower: tag_id}."""
    result = await asyncio.to_thread(
        db.table("tags").select("id, name").execute
    )
    tags: dict[str, str] = {}
    for t in result.data:
        if not isinstance(t, dict):
            continue
        name = t.get("name")
        tag_id = t.get("id")
        if isinstance(name, str) and isinstance(tag_id, str):
            tags[name.lower()] = tag_id
    return tags


async def match_tags(
    db: Client,
    blog_id: str,
    insights: AIInsights,
    tags: dict[str, str] | None = None,
) -> None:
    """
    Сопоставить теги из AI-анализа с таблицей tags.
    Записать в blog_tags.
    tags — кэш {name_lower: id}, если None — загружается из БД.
    """
    if not insights.tags:
        return

    if tags is None:
        tags = await load_tags(db)

    for tag_name in insights.tags:
        tag_lower = tag_name.lower()
        if tag_lower in tags:
            await asyncio.to_thread(
                db.table("blog_tags").upsert({
                    "blog_id": blog_id,
                    "tag_id": tags[tag_lower],
                }, on_conflict="blog_id,tag_id").execute
            )
```

**Step 4: Запустить тесты**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_ai/test_batch.py -v --tb=short`
Expected: ALL PASS

**Step 5: Коммит**

```bash
cd /Users/clicktronix/Projects/ai/native/scraper
git add src/ai/batch.py tests/test_ai/test_batch.py
git commit -m "feat: add match_tags for upsert blog_tags after AI analysis"
```

---

### Task 6: Генерация embedding после AI-анализа

**Files:**
- Create: `src/ai/embedding.py`
- Test: `tests/test_ai/test_embedding.py`

**Step 1: Написать тесты**

Создать `tests/test_ai/test_embedding.py`:

```python
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
            tags=["видео-контент", "reels", "рецепты"],
            blogger_profile={
                "profession": "повар",
                "city": "Алматы",
                "country": "Казахстан",
                "page_type": "blog",
                "speaks_languages": ["русский"],
            },
            content={
                "primary_topic": "food",
                "secondary_topics": ["Рецепты", "ПП и диеты"],
            },
            audience_inference={
                "estimated_audience_gender": "mostly_female",
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

        assert "фуд-блогер" in text
        assert "Готовит казахскую кухню" in text
        assert "food" in text
        assert "повар" in text
        assert "Алматы" in text
        assert "видео-контент" in text
        assert "reels" in text

    def test_empty_insights_embedding_text(self) -> None:
        """Пустой AIInsights → минимальный текст без ошибок."""
        from src.ai.embedding import build_embedding_text

        insights = AIInsights()
        text = build_embedding_text(insights)

        assert isinstance(text, str)
        assert len(text) > 0


class TestGenerateEmbedding:
    """Тесты вызова OpenAI Embeddings API."""

    @pytest.mark.asyncio
    async def test_generate_embedding_returns_vector(self) -> None:
        """Успешная генерация → вектор 1536 dim."""
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

        call_kwargs = mock_client.embeddings.create.call_args
        assert call_kwargs.kwargs.get("model") == "text-embedding-3-small" or \
               call_kwargs[1].get("model") == "text-embedding-3-small"

    @pytest.mark.asyncio
    async def test_generate_embedding_handles_error(self) -> None:
        """Ошибка API → None (не крашит пайплайн)."""
        from src.ai.embedding import generate_embedding

        mock_client = AsyncMock()
        mock_client.embeddings.create.side_effect = Exception("API error")

        vector = await generate_embedding(mock_client, "текст")

        assert vector is None
```

**Step 2: Запустить тесты — убедиться что падают**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_ai/test_embedding.py -v`
Expected: FAIL — модуль не существует

**Step 3: Реализовать модуль embedding**

Создать `src/ai/embedding.py`:

```python
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
```

**Step 4: Запустить тесты**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_ai/test_embedding.py -v`
Expected: ALL PASS

**Step 5: Коммит**

```bash
cd /Users/clicktronix/Projects/ai/native/scraper
git add src/ai/embedding.py tests/test_ai/test_embedding.py
git commit -m "feat: add embedding generation from AI insights"
```

---

### Task 7: Подключить match_tags и embedding в handle_batch_results

**Files:**
- Modify: `src/worker/handlers.py:538-557` (handle_batch_results, блок сохранения insights)
- Test: `tests/test_worker/test_handlers.py`

**Step 1: Написать тесты**

В `tests/test_worker/test_handlers.py` добавить тесты в существующий класс тестов `handle_batch_results`:

```python
@pytest.mark.asyncio
async def test_batch_results_calls_match_tags(self) -> None:
    """handle_batch_results вызывает match_tags после сохранения insights."""
    # ... setup mock_db, openai_client, batch results с insights ...
    # Проверить что mock_db.table("blog_tags") был вызван

@pytest.mark.asyncio
async def test_batch_results_generates_embedding(self) -> None:
    """handle_batch_results генерирует и сохраняет embedding."""
    # ... setup mock_db, openai_client, batch results с insights ...
    # Проверить что openai_client.embeddings.create был вызван
    # Проверить что blogs.update включает embedding vector

@pytest.mark.asyncio
async def test_batch_results_embedding_failure_does_not_block(self) -> None:
    """Ошибка генерации embedding не блокирует mark_task_done."""
    # ... setup с embedding error ...
    # mark_task_done всё равно должен быть вызван
```

Точный код тестов зависит от существующих фикстур в `test_handlers.py`. Нужно прочитать файл и адаптировать.

**Step 2: Обновить handle_batch_results**

В `src/worker/handlers.py`, в блоке `else:` (строка 538-551), после `match_categories`:

```python
        else:
            # Сохраняем insights
            logger.debug(f"[batch_results] Blog {blog_id}: saving insights ...")
            await run_in_thread(
                db.table("blogs").update({
                    "ai_insights": insights.model_dump(),
                    "ai_confidence": insights.confidence,
                    "ai_analyzed_at": datetime.now(UTC).isoformat(),
                    "scrape_status": "active",
                }).eq("id", blog_id).execute
            )

            # Матчинг категорий
            try:
                await match_categories(db, blog_id, insights, categories=categories_cache)
            except Exception as e:
                logger.error(f"Failed to match categories for blog {blog_id}: {e}")

            # Матчинг тегов
            try:
                await match_tags(db, blog_id, insights, tags=tags_cache)
            except Exception as e:
                logger.error(f"Failed to match tags for blog {blog_id}: {e}")

            # Генерация embedding
            try:
                from src.ai.embedding import build_embedding_text, generate_embedding
                embedding_text = build_embedding_text(insights)
                vector = await generate_embedding(openai_client, embedding_text)
                if vector:
                    await run_in_thread(
                        db.table("blogs").update({
                            "embedding": vector,
                        }).eq("id", blog_id).execute
                    )
                    logger.debug(f"[batch_results] Blog {blog_id}: embedding saved ({len(vector)} dim)")
            except Exception as e:
                logger.error(f"Failed to generate embedding for blog {blog_id}: {e}")
```

Также: добавить `tags_cache = await load_tags(db)` рядом с `categories_cache = await load_categories(db)` в начале `handle_batch_results`.

Добавить импорт: `from src.ai.batch import load_categories, match_categories, load_tags, match_tags`

**Step 3: Запустить тесты**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/test_worker/test_handlers.py -v --tb=short`
Expected: ALL PASS

**Step 4: Запустить все тесты**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/ -v --tb=short`
Expected: ALL PASS

**Step 5: Коммит**

```bash
cd /Users/clicktronix/Projects/ai/native/scraper
git add src/worker/handlers.py src/ai/batch.py tests/test_worker/test_handlers.py
git commit -m "feat: wire match_tags and embedding generation into batch results handler"
```

---

### Task 8: Добавить avg_reels_views — расчёт при скрапинге

**Files:**
- Modify: `src/worker/handlers.py` (handle_full_scrape, блок upsert_blog)
- Test: `tests/test_worker/test_handlers.py`

**Step 1: Написать тест**

В `tests/test_worker/test_handlers.py` в тестах `handle_full_scrape`:

```python
@pytest.mark.asyncio
async def test_full_scrape_saves_avg_reels_views(self) -> None:
    """avg_reels_views рассчитывается и сохраняется при скрапинге."""
    # ... setup с профилем, содержащим reels с play_count ...
    # Проверить что blog update включает avg_reels_views
```

**Step 2: Обновить handle_full_scrape**

В `src/worker/handlers.py`, в блоке upsert_blog данных, добавить расчёт:

```python
# Средние просмотры рилсов
reels_views = [
    m.play_count for m in profile_data.medias
    if m.play_count is not None and m.product_type == "clips"
]
avg_reels_views = int(sum(reels_views) / len(reels_views)) if reels_views else None
```

Добавить `"avg_reels_views": avg_reels_views` в dict, передаваемый в `upsert_blog()`.

**Step 3: Запустить тесты**

Run: `cd /Users/clicktronix/Projects/ai/native/scraper && uv run pytest tests/ -v --tb=short`
Expected: ALL PASS

**Step 4: Коммит**

```bash
cd /Users/clicktronix/Projects/ai/native/scraper
git add src/worker/handlers.py tests/test_worker/test_handlers.py
git commit -m "feat: calculate and store avg_reels_views during scrape"
```

---

### Task 9: SQL миграции — parent_id, group, status, seed data

**Files:**
- Create: SQL миграции через Supabase MCP
- Dependencies: Tasks 1-8 должны быть завершены (код готов)

**Step 1: Миграция — добавить parent_id в categories**

```sql
ALTER TABLE categories ADD COLUMN parent_id UUID REFERENCES categories(id);
```

**Step 2: Миграция — добавить group и status в tags**

```sql
ALTER TABLE tags
    ADD COLUMN "group" TEXT NOT NULL DEFAULT 'content'
        CHECK ("group" IN ('content', 'personal', 'professional', 'commercial', 'audience', 'marketing')),
    ADD COLUMN status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'unconfirmed'));
```

**Step 3: Миграция — добавить avg_reels_views в blogs**

```sql
ALTER TABLE blogs ADD COLUMN avg_reels_views INTEGER;
```

**Step 4: Seed — 20 категорий + ~120 подкатегорий**

SQL INSERT для всех категорий из PRD раздел 2.2. Использовать данные из `src/ai/taxonomy.py`:

```sql
-- Верхнеуровневые категории
INSERT INTO categories (code, name) VALUES
    ('beauty', 'Красота'),
    ('fashion', 'Мода'),
    -- ... все 20
ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name;

-- Подкатегории (parent_id через subquery)
INSERT INTO categories (name, parent_id)
    SELECT 'Макияж', id FROM categories WHERE code = 'beauty';
-- ... все ~120 подкатегорий
```

**Step 5: Seed — 200+ тегов**

SQL INSERT для всех тегов из PRD разделы 3.3-3.8:

```sql
INSERT INTO tags (name, "group", status) VALUES
    ('видео-контент', 'content', 'active'),
    ('фото-контент', 'content', 'active'),
    -- ... все 200+ тегов
ON CONFLICT (name) DO UPDATE SET "group" = EXCLUDED."group";
```

**Step 6: Проверка**

```sql
SELECT COUNT(*) FROM categories WHERE parent_id IS NULL;  -- 20
SELECT COUNT(*) FROM categories WHERE parent_id IS NOT NULL;  -- ~120
SELECT COUNT(*) FROM tags;  -- >= 200
SELECT COUNT(DISTINCT "group") FROM tags;  -- 6
```

---

## Порядок выполнения

```
Task 1 (AIInsights schema)
    ↓
Task 2 (taxonomy module) ←── можно параллельно с Task 1
    ↓
Task 3 (prompt update) ←── зависит от Task 2
    ↓
Task 4 (match_categories refactor)
    ↓
Task 5 (match_tags) ←── зависит от Task 4
    ↓
Task 6 (embedding) ←── зависит от Task 1
    ↓
Task 7 (wire into handler) ←── зависит от Task 4, 5, 6
    ↓
Task 8 (avg_reels_views) ←── независимый
    ↓
Task 9 (SQL migrations) ←── после всего кода
```

## Верификация

После всех задач:

```bash
cd /Users/clicktronix/Projects/ai/native/scraper
uv run pytest tests/ -v --tb=short
uv run ruff check src/ tests/
uv run pyright src/
```
