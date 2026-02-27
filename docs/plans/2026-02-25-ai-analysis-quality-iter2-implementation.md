# AI Analysis Quality Iteration 2 — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Улучшить pipeline AI-анализа: embedding, fuzzy matching, logging, prompt, multi-category, retry, refusal fallback.

**Architecture:** 7 блоков улучшений из дизайн-документа `docs/plans/2026-02-25-ai-analysis-quality-iter2-design.md`. Изменения затрагивают: schemas, embedding, batch matching, prompt, scheduler, handlers. Без изменений БД-схемы.

**Tech Stack:** Python 3.12, Pydantic v2, OpenAI Batch API, difflib, APScheduler, loguru, pytest.

---

### Task 1: Fix confidence format bug in batch.py

**Контекст:** В Iteration 1 confidence стал `Literal[1,2,3,4,5]` (int), но batch.py:256 всё ещё использует `:.2f` (float format). Падает при `confidence=3` → `"3.00"` вместо `"3"`.

**Files:**
- Modify: `src/ai/batch.py:255-257`
- Test: `tests/test_ai/test_batch.py`

**Step 1: Write the failing test**

В `tests/test_ai/test_batch.py`, добавить тест в новый класс:

```python
class TestParseResultLineLogging:
    """Тесты логирования при парсинге результатов."""

    @pytest.mark.asyncio
    async def test_confidence_logged_as_int(self) -> None:
        """confidence логируется как int, не float."""
        from src.ai.batch import poll_batch

        insights = AIInsights(confidence=3)
        output_line = json.dumps({
            "custom_id": "blog-conf",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{"message": {"content": insights.model_dump_json()}}]
                }
            },
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock()
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)
        mock_content = MagicMock()
        mock_content.text = output_line
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-conf")
        assert result["results"]["blog-conf"].confidence == 3
```

**Step 2: Run test to verify current state**

Run: `uv run pytest tests/test_ai/test_batch.py::TestParseResultLineLogging::test_confidence_logged_as_int -v`

**Step 3: Fix the format string**

В `src/ai/batch.py`, строка 255-257, заменить:

```python
# БЫЛО:
logger.debug(f"[batch] Parsed insights for {custom_id}: "
             f"confidence={insights.confidence:.2f}, "
             f"summary_len={len(insights.summary)}")

# СТАЛО:
logger.debug(f"[batch] Parsed insights for {custom_id}: "
             f"confidence={insights.confidence}, "
             f"summary_len={len(insights.summary)}")
```

**Step 4: Run test to verify fix**

Run: `uv run pytest tests/test_ai/test_batch.py::TestParseResultLineLogging -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/ai/batch.py tests/test_ai/test_batch.py
git commit -m "fix: remove .2f format from int confidence in batch logging"
```

---

### Task 2: Embedding — short_summary + quality fields

**Контекст:** Embedding текст не включает quality-поля (engagement_quality, brand_safety_score, lifestyle_level, content_quality, collaboration_risk). Также short_label (2-3 слова) менее информативен, чем short_summary (2-3 строки).

**Files:**
- Modify: `src/ai/embedding.py:14-22, 66-75`
- Test: `tests/test_ai/test_embedding.py`

**Step 1: Write failing tests**

В `tests/test_ai/test_embedding.py`, добавить тесты:

```python
class TestEmbeddingQualityFields:
    """Тесты quality-полей в embedding тексте."""

    def test_includes_engagement_quality(self) -> None:
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(
            audience_inference={"engagement_quality": "organic"},
        )
        text = build_embedding_text(insights)
        assert "органическая" in text

    def test_includes_brand_safety_score(self) -> None:
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(
            marketing_value={"brand_safety_score": 4},
        )
        text = build_embedding_text(insights)
        assert "безопасность бренда: 4/5" in text

    def test_includes_lifestyle_level(self) -> None:
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(
            lifestyle={"lifestyle_level": "premium"},
        )
        text = build_embedding_text(insights)
        assert "premium" in text

    def test_includes_content_quality(self) -> None:
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(
            content={"content_quality": "high"},
        )
        text = build_embedding_text(insights)
        assert "качество контента: high" in text

    def test_includes_collaboration_risk(self) -> None:
        from src.ai.embedding import build_embedding_text

        insights = AIInsights(
            marketing_value={"collaboration_risk": "low"},
        )
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
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ai/test_embedding.py::TestEmbeddingQualityFields -v`
Expected: FAIL (quality fields not in text)

**Step 3: Implement changes in embedding.py**

В `src/ai/embedding.py`, изменить `build_embedding_text()`:

1. Строки 14-22 — заменить short_label логику на только short_summary:

```python
# БЫЛО:
if insights.short_label:
    label = insights.short_label
    if insights.short_summary:
        parts.append(f"{label}. {insights.short_summary}")
    else:
        parts.append(label)
elif insights.short_summary:
    parts.append(insights.short_summary)

# СТАЛО:
if insights.short_summary:
    parts.append(insights.short_summary)
```

2. После строки 73 (маркетинг секция), перед `return`, добавить:

```python
# Качественные характеристики
quality_parts: list[str] = []
if insights.audience_inference.engagement_quality:
    eq_map = {"organic": "органическая", "mixed": "смешанная", "suspicious": "подозрительная"}
    quality_parts.append(f"вовлечённость: {eq_map.get(insights.audience_inference.engagement_quality, insights.audience_inference.engagement_quality)}")
if insights.marketing_value.brand_safety_score:
    quality_parts.append(f"безопасность бренда: {insights.marketing_value.brand_safety_score}/5")
if insights.lifestyle.lifestyle_level:
    quality_parts.append(f"уровень жизни: {insights.lifestyle.lifestyle_level}")
if insights.content.content_quality:
    quality_parts.append(f"качество контента: {insights.content.content_quality}")
if insights.marketing_value.collaboration_risk:
    quality_parts.append(f"риск коллаборации: {insights.marketing_value.collaboration_risk}")
if quality_parts:
    parts.append(f"Характеристики: {', '.join(quality_parts)}.")
```

**Step 4: Update existing test**

В `tests/test_ai/test_embedding.py`, класс `TestBuildEmbeddingText`, тест `test_basic_embedding_text`:
- Убрать `assert "фуд-блогер" in text` (short_label больше не в embedding)
- Оставить `assert "Готовит казахскую кухню" in text` (short_summary)

**Step 5: Run all embedding tests**

Run: `uv run pytest tests/test_ai/test_embedding.py -v`
Expected: ALL PASS

**Step 6: Commit**

```bash
git add src/ai/embedding.py tests/test_ai/test_embedding.py
git commit -m "feat: enrich embedding text with quality fields, use short_summary"
```

---

### Task 3: Logging missed tags and categories

**Контекст:** Когда AI возвращает тег/категорию не из справочника, она молча пропускается. Нужно логировать через `logger.warning` — попадёт в `scrape_logs` через loguru sink.

**Files:**
- Modify: `src/ai/batch.py:340-370, 440-446`
- Test: `tests/test_ai/test_batch.py`

**Step 1: Write failing tests**

В `tests/test_ai/test_batch.py`:

```python
class TestMatchCategoriesLogging:
    """Тесты логирования пропущенных категорий."""

    @pytest.mark.asyncio
    async def test_logs_missing_primary_category(self, caplog) -> None:
        """Пропущенная primary категория логируется как warning."""
        import logging

        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_topic = "nonexistent_category"

        mock_db = MagicMock()
        categories = {"beauty": "cat-1"}

        with caplog.at_level(logging.WARNING):
            await match_categories(mock_db, "blog-1", insights, categories=categories)

        assert any("nonexistent_category" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_missing_secondary_category(self, caplog) -> None:
        import logging

        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_topic = "beauty"
        insights.content.secondary_topics = ["Несуществующая"]

        mock_db = MagicMock()
        categories = {"beauty": "cat-1"}

        with caplog.at_level(logging.WARNING):
            await match_categories(mock_db, "blog-1", insights, categories=categories)

        assert any("Несуществующая" in r.message for r in caplog.records)


class TestMatchTagsLogging:
    """Тесты логирования пропущенных тегов."""

    @pytest.mark.asyncio
    async def test_logs_missing_tag(self, caplog) -> None:
        import logging

        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео-контент", "неизвестный-тег"]

        tags_cache = {"видео-контент": "tag-1"}
        mock_db = MagicMock()

        with caplog.at_level(logging.WARNING):
            await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        assert any("неизвестный-тег" in r.message for r in caplog.records)
```

**Примечание:** loguru по умолчанию не пишет в стандартный Python logging. Нужно использовать `loguru.logger` пропагацию или проверять иначе. Вместо `caplog` используем `patch("src.ai.batch.logger")`:

```python
class TestMatchCategoriesLogging:

    @pytest.mark.asyncio
    async def test_logs_missing_primary_category(self) -> None:
        from unittest.mock import patch

        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_topic = "nonexistent_category"

        mock_db = MagicMock()
        categories = {"beauty": "cat-1"}

        with patch("src.ai.batch.logger") as mock_logger:
            await match_categories(mock_db, "blog-1", insights, categories=categories)
            mock_logger.warning.assert_called()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "nonexistent_category" in warning_msg


class TestMatchTagsLogging:

    @pytest.mark.asyncio
    async def test_logs_missing_tag(self) -> None:
        from unittest.mock import patch

        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео-контент", "неизвестный-тег"]

        tags_cache = {"видео-контент": "tag-1"}
        mock_db = MagicMock()

        with patch("src.ai.batch.logger") as mock_logger:
            await match_tags(mock_db, "blog-1", insights, tags=tags_cache)
            # Хотя бы один warning вызван для неизвестного тега
            warnings = [c for c in mock_logger.warning.call_args_list
                        if "неизвестный-тег" in str(c)]
            assert len(warnings) >= 1
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ai/test_batch.py::TestMatchCategoriesLogging -v`
Run: `uv run pytest tests/test_ai/test_batch.py::TestMatchTagsLogging -v`
Expected: FAIL

**Step 3: Add logging to match_categories**

В `src/ai/batch.py`, в `match_categories()`:

После строки 354 (`primary_category_id: str | None = categories.get(primary)`):
```python
if primary_category_id is None:
    logger.warning(f"[match_categories] Blog {blog_id}: категория не найдена: '{insights.content.primary_topic}'")
```

В цикле secondary topics (строка 366), после `cat_id = categories.get(topic_lower)`:
```python
if not cat_id:
    logger.warning(f"[match_categories] Blog {blog_id}: подкатегория не найдена: '{topic}'")
    continue
```

**Step 4: Add logging to match_tags**

В `src/ai/batch.py`, в `match_tags()`, строка 443:

```python
# БЫЛО:
tag_id = tags.get(tag_lower)
if tag_id and tag_id not in seen_tag_ids:

# СТАЛО:
tag_id = tags.get(tag_lower)
if not tag_id:
    logger.warning(f"[match_tags] Blog {blog_id}: тег не найден в справочнике: '{tag_name}'")
    continue
if tag_id not in seen_tag_ids:
```

**Step 5: Run tests**

Run: `uv run pytest tests/test_ai/test_batch.py::TestMatchCategoriesLogging tests/test_ai/test_batch.py::TestMatchTagsLogging -v`
Expected: PASS

**Step 6: Run all batch tests**

Run: `uv run pytest tests/test_ai/test_batch.py -v`
Expected: ALL PASS

**Step 7: Commit**

```bash
git add src/ai/batch.py tests/test_ai/test_batch.py
git commit -m "feat: log missing tags and categories as warnings to scrape_logs"
```

---

### Task 4: Fuzzy matching for tags and categories

**Контекст:** Exact match пропускает варианты: "Beauty & Makeup" → "beauty", "Путешествия и приключения" → "путешествия". Используем `difflib.get_close_matches` (stdlib, без зависимостей).

**Files:**
- Modify: `src/ai/batch.py`
- Test: `tests/test_ai/test_batch.py`

**Step 1: Write tests for _fuzzy_lookup helper**

```python
class TestFuzzyLookup:
    """Тесты fuzzy matching."""

    def test_exact_match(self) -> None:
        from src.ai.batch import _fuzzy_lookup

        cache = {"beauty": "cat-1", "fitness": "cat-2"}
        assert _fuzzy_lookup("beauty", cache) == "cat-1"

    def test_normalized_match_ampersand(self) -> None:
        from src.ai.batch import _fuzzy_lookup

        cache = {"beauty makeup": "cat-1"}
        assert _fuzzy_lookup("beauty & makeup", cache) == "cat-1"

    def test_normalized_match_hyphen(self) -> None:
        from src.ai.batch import _fuzzy_lookup

        cache = {"видео контент": "tag-1"}
        assert _fuzzy_lookup("видео-контент", cache) == "tag-1"

    def test_fuzzy_match_close(self) -> None:
        from src.ai.batch import _fuzzy_lookup

        cache = {"путешествия": "cat-1"}
        # "путешествие" (единственное число) достаточно близко
        result = _fuzzy_lookup("путешествие", cache)
        assert result == "cat-1"

    def test_no_match(self) -> None:
        from src.ai.batch import _fuzzy_lookup

        cache = {"beauty": "cat-1"}
        assert _fuzzy_lookup("программирование", cache) is None

    def test_empty_cache(self) -> None:
        from src.ai.batch import _fuzzy_lookup

        assert _fuzzy_lookup("beauty", {}) is None
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ai/test_batch.py::TestFuzzyLookup -v`
Expected: FAIL (ImportError — _fuzzy_lookup not defined)

**Step 3: Implement _fuzzy_lookup**

В `src/ai/batch.py`, добавить импорт и функцию после imports:

```python
from difflib import get_close_matches
```

Функция (перед `load_categories`):

```python
def _fuzzy_lookup(key: str, cache: dict[str, str], cutoff: float = 0.8) -> str | None:
    """Поиск в кэше: exact → normalized → fuzzy."""
    # 1. Exact
    if key in cache:
        return cache[key]
    # 2. Normalized: убрать &, -, лишние пробелы
    normalized = key.replace("&", "").replace("-", " ").strip()
    normalized = " ".join(normalized.split())
    if normalized in cache:
        return cache[normalized]
    # 3. Fuzzy (difflib)
    matches = get_close_matches(key, cache.keys(), n=1, cutoff=cutoff)
    if matches:
        return cache[matches[0]]
    return None
```

**Step 4: Run _fuzzy_lookup tests**

Run: `uv run pytest tests/test_ai/test_batch.py::TestFuzzyLookup -v`
Expected: PASS

**Step 5: Integrate _fuzzy_lookup into match_categories and match_tags**

В `match_categories()` (строка ~354):
```python
# БЫЛО:
primary_category_id: str | None = categories.get(primary)

# СТАЛО:
primary_category_id: str | None = _fuzzy_lookup(primary, categories)
```

В цикле secondary (строка ~366):
```python
# БЫЛО:
cat_id = categories.get(topic_lower)

# СТАЛО:
cat_id = _fuzzy_lookup(topic_lower, categories)
```

В `match_tags()` (строка ~443):
```python
# БЫЛО:
tag_id = tags.get(tag_lower)

# СТАЛО:
tag_id = _fuzzy_lookup(tag_lower, tags)
```

**Step 6: Write integration test for fuzzy matching in categories**

```python
class TestMatchCategoriesFuzzy:
    """Тесты fuzzy matching в match_categories."""

    @pytest.mark.asyncio
    async def test_fuzzy_primary_match(self) -> None:
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_topic = "beauty & makeup"

        mock_db = MagicMock()
        categories = {"beauty makeup": "cat-1"}

        await match_categories(mock_db, "blog-1", insights, categories=categories)

        assert mock_db.table.return_value.insert.call_count == 1
        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert rows[0]["category_id"] == "cat-1"

    @pytest.mark.asyncio
    async def test_fuzzy_tag_match(self) -> None:
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео контент"]  # пробел вместо дефиса

        tags_cache = {"видео-контент": "tag-1"}
        mock_db = MagicMock()

        await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        assert mock_db.table.return_value.insert.call_count == 1
```

**Step 7: Run all batch tests**

Run: `uv run pytest tests/test_ai/test_batch.py -v`
Expected: ALL PASS

**Step 8: Commit**

```bash
git add src/ai/batch.py tests/test_ai/test_batch.py
git commit -m "feat: add fuzzy matching for tags and categories (difflib)"
```

---

### Task 5: Data quality hint in prompt

**Контекст:** AI не знает объём доступных данных. `er_trend` уже есть в промпте (строка 223-224). Нужно только добавить data quality hint.

**Files:**
- Modify: `src/ai/prompt.py:225-227`
- Test: `tests/test_ai/test_prompt.py`

**Step 1: Write failing test**

В `tests/test_ai/test_prompt.py`:

```python
class TestDataQualityHint:
    """Тесты data quality hint в промпте."""

    def _make_profile_with_data(self):
        from src.models.blog import ScrapedComment, ScrapedPost, ScrapedProfile

        return ScrapedProfile(
            platform_id="12345",
            username="testblogger",
            biography="Мама двоих детей из Алматы",
            follower_count=50000,
            highlights=[],
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    caption_text="Длинный текст поста о красоте и здоровье" * 2,
                    like_count=500,
                    comment_count=10,
                    top_comments=[
                        ScrapedComment(username="fan1", text="Классно!"),
                    ],
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
                ScrapedPost(
                    platform_id="p2",
                    media_type=1,
                    caption_text="Ещё один пост с текстом для анализа",
                    like_count=300,
                    comment_count=5,
                    taken_at=datetime(2026, 1, 20, tzinfo=UTC),
                ),
            ],
        )

    def test_data_quality_hint_present(self) -> None:
        from src.ai.prompt import build_analysis_prompt

        profile = self._make_profile_with_data()
        messages = build_analysis_prompt(profile)
        user_msg = messages[1]
        text = user_msg["content"] if isinstance(user_msg["content"], str) else \
            " ".join(p["text"] for p in user_msg["content"] if p.get("type") == "text")

        assert "Объём данных:" in text
        assert "2 постов" in text
        assert "с текстом" in text
        assert "био заполнено" in text

    def test_data_quality_hint_with_comments(self) -> None:
        from src.ai.prompt import build_analysis_prompt

        profile = self._make_profile_with_data()
        messages = build_analysis_prompt(profile)
        user_msg = messages[1]
        text = user_msg["content"] if isinstance(user_msg["content"], str) else \
            " ".join(p["text"] for p in user_msg["content"] if p.get("type") == "text")

        assert "1 с комментариями" in text

    def test_data_quality_hint_with_highlights(self) -> None:
        from src.models.blog import ScrapedHighlight, ScrapedPost, ScrapedProfile
        from src.ai.prompt import build_analysis_prompt

        profile = ScrapedProfile(
            platform_id="12345",
            username="test",
            follower_count=1000,
            highlights=[
                ScrapedHighlight(platform_id="h1", title="About", media_count=5),
                ScrapedHighlight(platform_id="h2", title="Travel", media_count=3),
            ],
            medias=[],
        )
        messages = build_analysis_prompt(profile)
        user_msg = messages[1]
        text = user_msg["content"] if isinstance(user_msg["content"], str) else \
            " ".join(p["text"] for p in user_msg["content"] if p.get("type") == "text")

        assert "2 хайлайтов" in text
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ai/test_prompt.py::TestDataQualityHint -v`
Expected: FAIL

**Step 3: Implement data quality hint**

В `src/ai/prompt.py`, после строки 226 (`text_parts.append(f"Posts per week: ...")`), добавить:

```python
# Data quality hint — объём доступных данных для AI
posts_with_text = sum(1 for m in profile.medias if m.caption_text and len(m.caption_text) > 20)
posts_with_comments = sum(1 for m in profile.medias if m.top_comments)
hint_parts = [f"{len(profile.medias)} постов"]
if posts_with_text:
    hint_parts.append(f"{posts_with_text} с текстом")
if profile.biography:
    hint_parts.append("био заполнено")
if profile.highlights:
    hint_parts.append(f"{len(profile.highlights)} хайлайтов")
if posts_with_comments:
    hint_parts.append(f"{posts_with_comments} с комментариями")
text_parts.append(f"Объём данных: {', '.join(hint_parts)}.")
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_ai/test_prompt.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add src/ai/prompt.py tests/test_ai/test_prompt.py
git commit -m "feat: add data quality hint to AI prompt"
```

---

### Task 6: Multi-category support — schema change

**Контекст:** `primary_topic: str | None` → `primary_categories: list[str]` (до 3 кодов). Первый = основная категория.

**Files:**
- Modify: `src/ai/schemas.py:130-134`
- Test: `tests/test_ai/test_schemas.py`

**Step 1: Write failing tests**

В `tests/test_ai/test_schemas.py`:

```python
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

    def test_primary_topic_removed(self) -> None:
        """primary_topic больше не существует."""
        from pydantic import ValidationError

        from src.ai.schemas import AIInsights

        with pytest.raises(ValidationError):
            AIInsights(content={"primary_topic": "beauty"})
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ai/test_schemas.py::TestPrimaryCategories -v`
Expected: FAIL

**Step 3: Modify ContentProfile in schemas.py**

В `src/ai/schemas.py`, в классе `ContentProfile`, заменить строки 130-134:

```python
# БЫЛО:
primary_topic: str | None = Field(
    default=None,
    description="ОДИН код категории из списка в промпте (английский): "
    "'beauty', 'fitness', 'family', 'food' и т.д. НЕ русское название.",
)

# СТАЛО:
primary_categories: list[str] = Field(
    default_factory=list,
    description="До 3 кодов основных категорий из списка в промпте (английский): "
    "'beauty', 'fitness', 'family'. Первый элемент = основная категория, "
    "остальные = дополнительные. НЕ русские названия.",
)
```

**Step 4: Update existing tests referencing primary_topic**

В `tests/test_ai/test_schemas.py`:

1. `test_empty_insights`: `assert insights.content.primary_topic is None` → `assert insights.content.primary_categories == []`
2. `test_full_insights`: `"primary_topic": "материнство"` → `"primary_categories": ["материнство"]`, `assert insights.content.primary_topic == "материнство"` → `assert insights.content.primary_categories == ["материнство"]`
3. `test_partial_sub_models`: `"primary_topic": "красота"` → `"primary_categories": ["красота"]`, `assert insights.content.primary_topic == "красота"` → `assert insights.content.primary_categories == ["красота"]`
4. `test_invalid_literal_raises`: удалить тест с `primary_topic` если есть (его нет — проверить)

**Step 5: Run schema tests**

Run: `uv run pytest tests/test_ai/test_schemas.py -v`
Expected: ALL PASS

**Step 6: Commit**

```bash
git add src/ai/schemas.py tests/test_ai/test_schemas.py
git commit -m "feat: replace primary_topic with primary_categories list"
```

---

### Task 7: Multi-category — update prompt, embedding, batch, handlers

**Контекст:** После schema change нужно обновить все места, которые используют `primary_topic`.

**Files:**
- Modify: `src/ai/prompt.py:53-55` (_BASE_PROMPT)
- Modify: `src/ai/embedding.py:26-27`
- Modify: `src/ai/batch.py:345-360` (match_categories)
- Modify: `src/worker/handlers.py:625`
- Test: `tests/test_ai/test_batch.py`, `tests/test_ai/test_embedding.py`

**Step 1: Update _BASE_PROMPT in prompt.py**

В `src/ai/prompt.py`, строки 53-55:

```python
# БЫЛО:
ОПРЕДЕЛЕНИЕ primary_topic:
- Выбери ОДИН код категории из списка ниже. Используй СТРОГО код (английский), не русское название.
- Пример: "beauty", не "Красота".

# СТАЛО:
ОПРЕДЕЛЕНИЕ primary_categories:
- Выбери до 3 кодов основных категорий из списка ниже. Используй СТРОГО коды (английский).
- Первый элемент = основная категория, остальные = дополнительные.
- Пример: ["beauty", "lifestyle"] или ["fitness"].
```

**Step 2: Update embedding.py**

В `src/ai/embedding.py`, строка 26-27:

```python
# БЫЛО:
if insights.content.primary_topic:
    profile_parts.append(f"Категория: {insights.content.primary_topic}")

# СТАЛО:
if insights.content.primary_categories:
    profile_parts.append(f"Категория: {', '.join(insights.content.primary_categories)}")
```

**Step 3: Update match_categories in batch.py**

В `src/ai/batch.py`, `match_categories()`:

```python
# БЫЛО (строка 345):
if not insights.content.primary_topic:
    return

# СТАЛО:
if not insights.content.primary_categories:
    return
```

```python
# БЫЛО (строка 353-360):
primary = insights.content.primary_topic.lower()
primary_category_id: str | None = _fuzzy_lookup(primary, categories)
if primary_category_id is None:
    logger.warning(f"[match_categories] Blog {blog_id}: категория не найдена: '{insights.content.primary_topic}'")
if primary_category_id:
    rows.append({...})
seen_ids: set[str] = {primary_category_id} if primary_category_id else set()

# СТАЛО:
# Primary categories (первая = основная, остальные = дополнительные)
seen_ids: set[str] = set()
for idx, cat_code in enumerate(insights.content.primary_categories):
    cat_lower = cat_code.lower()
    cat_id = _fuzzy_lookup(cat_lower, categories)
    if cat_id is None:
        logger.warning(f"[match_categories] Blog {blog_id}: категория не найдена: '{cat_code}'")
        continue
    if cat_id in seen_ids:
        continue
    seen_ids.add(cat_id)
    rows.append({
        "blog_id": blog_id,
        "category_id": cat_id,
        "is_primary": idx == 0,
    })
```

Secondary topics цикл остаётся как есть (строки 364-374).

**Step 4: Update handlers.py logging**

В `src/worker/handlers.py`, строка 625:

```python
# БЫЛО:
f"topic={insights.content.primary_topic})")

# СТАЛО:
f"topics={insights.content.primary_categories})")
```

**Step 5: Update existing tests**

В `tests/test_ai/test_batch.py`, все тесты `TestMatchCategories`:
- `insights.content.primary_topic = "beauty"` → `insights.content.primary_categories = ["beauty"]`
- `insights.content.primary_topic = "BEAUTY"` → `insights.content.primary_categories = ["BEAUTY"]`
- `insights.content.primary_topic = "cooking"` → `insights.content.primary_categories = ["cooking"]`
- `insights.content.primary_topic = None` → `insights.content.primary_categories = []`
- `insights.content.primary_topic = ""` → `insights.content.primary_categories = []`

В `tests/test_ai/test_batch.py`, `TestMatchCategoriesEdge`:
- `insights.content.primary_topic = "beauty"` → `insights.content.primary_categories = ["beauty"]`
- `insights.content.primary_topic = ""` → `insights.content.primary_categories = []`

В `tests/test_ai/test_embedding.py`, `test_basic_embedding_text`:
- `"primary_topic": "food"` → `"primary_categories": ["food"]`

В `tests/test_ai/test_prompt.py` — проверить есть ли ссылки на `primary_topic` и обновить.

**Step 6: Add new test for multi-primary categories**

В `tests/test_ai/test_batch.py`:

```python
class TestMatchCategoriesMulti:
    """Тесты multi-category matching."""

    @pytest.mark.asyncio
    async def test_multiple_primary_categories(self) -> None:
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["beauty", "lifestyle", "fashion"]
        insights.content.secondary_topics = []

        mock_db = MagicMock()
        categories = {
            "beauty": "cat-1",
            "lifestyle": "cat-2",
            "fashion": "cat-3",
        }

        await match_categories(mock_db, "blog-1", insights, categories=categories)

        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert len(rows) == 3
        assert rows[0]["is_primary"] is True
        assert rows[0]["category_id"] == "cat-1"
        assert rows[1]["is_primary"] is False
        assert rows[1]["category_id"] == "cat-2"
        assert rows[2]["is_primary"] is False
        assert rows[2]["category_id"] == "cat-3"

    @pytest.mark.asyncio
    async def test_primary_overlaps_secondary(self) -> None:
        """primary_categories дублирует secondary → не дублируется в rows."""
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["beauty"]
        insights.content.secondary_topics = ["beauty", "Путешествия"]

        mock_db = MagicMock()
        categories = {
            "beauty": "cat-1",
            "путешествия": "cat-3",
        }

        await match_categories(mock_db, "blog-1", insights, categories=categories)

        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert len(rows) == 2
        cat_ids = [r["category_id"] for r in rows]
        assert cat_ids.count("cat-1") == 1
```

**Step 7: Run all affected tests**

Run: `uv run pytest tests/test_ai/ -v`
Expected: ALL PASS

**Step 8: Commit**

```bash
git add src/ai/prompt.py src/ai/embedding.py src/ai/batch.py src/worker/handlers.py \
        tests/test_ai/test_batch.py tests/test_ai/test_embedding.py tests/test_ai/test_schemas.py
git commit -m "feat: multi-category support — update prompt, embedding, batch, handlers"
```

---

### Task 8: Retry missing embeddings cron job

**Контекст:** Если embedding генерация упала при обработке батча — вектор = null навсегда. Нужна cron-задача для ретрая.

**Files:**
- Modify: `src/worker/scheduler.py`
- Test: `tests/test_worker/test_scheduler.py`

**Step 1: Write failing tests**

В `tests/test_worker/test_scheduler.py`:

```python
class TestRetryMissingEmbeddings:
    """Тесты retry_missing_embeddings."""

    @pytest.mark.asyncio
    async def test_regenerates_embedding_for_blog_without_vector(self) -> None:
        from src.worker.scheduler import retry_missing_embeddings

        mock_db = MagicMock()
        mock_openai = MagicMock()

        insights_data = AIInsights(
            short_summary="Тестовый блогер",
        ).model_dump()

        with (
            patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.scheduler.generate_embedding", new_callable=AsyncMock) as mock_embed,
        ):
            mock_run.side_effect = [
                # Первый вызов — запрос блогов без embedding
                MagicMock(data=[{"id": "blog-1", "ai_insights": insights_data}]),
                # Второй вызов — update embedding
                MagicMock(),
            ]
            mock_embed.return_value = [0.1] * 1536

            await retry_missing_embeddings(mock_db, mock_openai)

            mock_embed.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_blogs_without_embedding(self) -> None:
        from src.worker.scheduler import retry_missing_embeddings

        mock_db = MagicMock()
        mock_openai = MagicMock()

        with patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = MagicMock(data=[])

            await retry_missing_embeddings(mock_db, mock_openai)
            # Только один вызов (запрос блогов), без update
            mock_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_in_one_blog_does_not_crash(self) -> None:
        from src.worker.scheduler import retry_missing_embeddings

        mock_db = MagicMock()
        mock_openai = MagicMock()

        with (
            patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.scheduler.generate_embedding", new_callable=AsyncMock) as mock_embed,
        ):
            mock_run.return_value = MagicMock(data=[
                {"id": "blog-1", "ai_insights": {"invalid": True}},
                {"id": "blog-2", "ai_insights": AIInsights(short_summary="OK").model_dump()},
            ])
            mock_embed.return_value = [0.1] * 1536

            # Не должно падать
            await retry_missing_embeddings(mock_db, mock_openai)
```

Также тест на наличие job'а в scheduler:

```python
# В классе TestCreateScheduler, тест test_creates_scheduler_with_jobs:
# Добавить: assert "retry_missing_embeddings" in job_ids
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_worker/test_scheduler.py::TestRetryMissingEmbeddings -v`
Expected: FAIL (ImportError)

**Step 3: Implement retry_missing_embeddings**

В `src/worker/scheduler.py`, добавить импорты:

```python
from src.ai.embedding import build_embedding_text, generate_embedding
from src.ai.schemas import AIInsights
```

Добавить функцию перед `recover_tasks`:

```python
async def retry_missing_embeddings(
    db: Client, openai_client: AsyncOpenAI
) -> None:
    """Перегенерировать embedding для блогов с insights но без вектора."""
    result = await run_in_thread(
        db.table("blogs")
        .select("id, ai_insights")
        .not_.is_("ai_insights", "null")
        .is_("embedding", "null")
        .limit(50)
        .execute
    )
    if not result.data:
        return

    regenerated = 0
    for blog in result.data:
        try:
            insights = AIInsights.model_validate(blog["ai_insights"])
            text = build_embedding_text(insights)
            vector = await generate_embedding(openai_client, text)
            if vector:
                await run_in_thread(
                    db.table("blogs").update({"embedding": vector}).eq("id", blog["id"]).execute
                )
                regenerated += 1
        except Exception as e:
            logger.error(f"[retry_embedding] Blog {blog['id']}: {e}")

    if regenerated:
        logger.info(f"[retry_embedding] Перегенерировано {regenerated} embedding'ов")
```

**Step 4: Register in create_scheduler**

В `create_scheduler()`, внутри блока `if openai_client:` (после retry_stale_batches job), добавить:

```python
# Каждый час — ретрай embedding для блогов без вектора
scheduler.add_job(
    retry_missing_embeddings,
    "interval",
    hours=1,
    kwargs={"db": db, "openai_client": openai_client},
    id="retry_missing_embeddings",
)
```

**Step 5: Update existing scheduler test**

В `tests/test_worker/test_scheduler.py`, `TestCreateScheduler.test_creates_scheduler_with_jobs`:
```python
assert "retry_missing_embeddings" in job_ids
```

**Step 6: Run all scheduler tests**

Run: `uv run pytest tests/test_worker/test_scheduler.py -v`
Expected: ALL PASS

**Step 7: Commit**

```bash
git add src/worker/scheduler.py tests/test_worker/test_scheduler.py
git commit -m "feat: add retry_missing_embeddings cron job (hourly, limit 50)"
```

---

### Task 9: Refusal fallback — batch.py (return refusal tuple)

**Контекст:** При refusal `poll_batch` возвращает `None`. Нужно вернуть `("refusal", reason)` tuple чтобы handlers мог различить refusal от ошибки.

**Files:**
- Modify: `src/ai/batch.py:242-246`
- Test: `tests/test_ai/test_batch.py`

**Step 1: Write tests**

В `tests/test_ai/test_batch.py`, обновить `test_refusal_returns_none`:

```python
@pytest.mark.asyncio
async def test_refusal_returns_tuple(self) -> None:
    """Refusal возвращает tuple ('refusal', reason), не None."""
    from src.ai.batch import poll_batch

    output_line = json.dumps({
        "custom_id": "blog-2",
        "response": {
            "status_code": 200,
            "body": {
                "choices": [{
                    "message": {"refusal": "Content filtered"}
                }]
            }
        },
    })

    mock_client = MagicMock()
    mock_batch = _make_batch_mock()
    mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

    mock_content = MagicMock()
    mock_content.text = output_line
    mock_client.files.content = AsyncMock(return_value=mock_content)

    result = await poll_batch(mock_client, "batch-123")

    assert result["results"]["blog-2"] == ("refusal", "Content filtered")
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_ai/test_batch.py::TestPollBatch::test_refusal_returns_tuple -v`
Expected: FAIL (returns None, not tuple)

**Step 3: Modify _parse_result_line in batch.py**

В `src/ai/batch.py`, строки 242-246:

```python
# БЫЛО:
if message.get("refusal"):
    logger.warning(f"AI refusal for {custom_id}: {message['refusal']}")
    results[custom_id] = None
    continue

# СТАЛО:
if message.get("refusal"):
    logger.warning(f"AI refusal for {custom_id}: {message['refusal']}")
    results[custom_id] = ("refusal", message["refusal"])
    continue
```

**Step 4: Remove old test_refusal_returns_none**

Удалить тест `test_refusal_returns_none` (заменён на `test_refusal_returns_tuple`).

**Step 5: Run tests**

Run: `uv run pytest tests/test_ai/test_batch.py -v`
Expected: ALL PASS

**Step 6: Commit**

```bash
git add src/ai/batch.py tests/test_ai/test_batch.py
git commit -m "feat: return ('refusal', reason) tuple from poll_batch"
```

---

### Task 10: Refusal fallback — handlers.py (ai_refused status + text_only retry)

**Контекст:** handlers.py должен обрабатывать refusal tuple: сохранить причину, установить `ai_refused`, создать retry-задачу с `text_only=True`.

**Files:**
- Modify: `src/worker/handlers.py:597-612`
- Test: `tests/test_worker/test_handlers.py`

**Step 1: Write tests**

В `tests/test_worker/test_handlers.py` найти или создать тесты для refusal:

```python
class TestHandleBatchResultsRefusal:
    """Тесты обработки AI refusal в handle_batch_results."""

    @pytest.mark.asyncio
    async def test_refusal_saves_reason_and_status(self) -> None:
        """Refusal сохраняет ai_insights с refusal_reason и scrape_status=ai_refused."""
        from src.worker.handlers import handle_batch_results

        mock_db = MagicMock()
        mock_openai = MagicMock()

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.load_categories", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.load_tags", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock) as mock_create_task,
        ):
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-1": ("refusal", "Content policy violation")},
            }
            mock_run.return_value = MagicMock(data=[])  # current_by_id

            await handle_batch_results(
                mock_db, mock_openai, "batch-123",
                {"blog-1": {"id": "task-1", "attempts": 1, "max_attempts": 3}},
            )

            # Проверяем update: ai_insights содержит refusal_reason, scrape_status=ai_refused
            update_calls = [c for c in mock_run.call_args_list
                           if "update" in str(c)]
            # Должна быть создана retry задача с text_only=True
            mock_create_task.assert_called_once()
            create_args = mock_create_task.call_args
            assert create_args[1].get("payload", {}).get("text_only") is True \
                or (len(create_args[0]) > 3 and create_args[0][3].get("text_only") is True)

    @pytest.mark.asyncio
    async def test_double_refusal_finalizes(self) -> None:
        """Повторный refusal (уже ai_refused) — финализировать без retry."""
        # Блог уже имеет scrape_status=ai_refused → не создавать retry
        ...
```

**Примечание:** Точная структура тестов зависит от текущей test infrastructure в test_handlers.py. Проверить паттерны перед написанием.

**Step 2: Implement refusal handling in handlers.py**

В `src/worker/handlers.py`, добавить импорт:

```python
from src.worker.tasks import create_task_if_not_exists
```

В `handle_batch_results()`, строки 597-612, заменить блок:

```python
# БЫЛО:
if insights is None:
    # Refusal — ставим ai_analyzed без insights
    logger.debug(f"[batch_results] Blog {blog_id}: AI refusal, no insights")
    await run_in_thread(
        db.table("blogs").update({
            "scrape_status": "ai_analyzed",
            "ai_analyzed_at": datetime.now(UTC).isoformat(),
        }).eq("id", blog_id).execute
    )

# СТАЛО:
if insights is None:
    # API error (не refusal) — помечаем как ai_analyzed без insights
    logger.debug(f"[batch_results] Blog {blog_id}: no insights (API error)")
    await run_in_thread(
        db.table("blogs").update({
            "scrape_status": "ai_analyzed",
            "ai_analyzed_at": datetime.now(UTC).isoformat(),
        }).eq("id", blog_id).execute
    )
elif isinstance(insights, tuple) and insights[0] == "refusal":
    # AI refusal — сохранить причину, попробовать text_only retry
    refusal_reason = insights[1]
    logger.warning(f"[batch_results] Blog {blog_id}: AI refusal: {refusal_reason}")

    # Проверяем, не было ли уже refusal (double refusal → финализировать)
    current_blog = await run_in_thread(
        db.table("blogs").select("scrape_status").eq("id", blog_id).execute
    )
    already_refused = (
        current_blog.data
        and current_blog.data[0].get("scrape_status") == "ai_refused"
    )

    await run_in_thread(
        db.table("blogs").update({
            "ai_insights": {"refusal_reason": refusal_reason},
            "scrape_status": "ai_refused" if not already_refused else "ai_analyzed",
            "ai_analyzed_at": datetime.now(UTC).isoformat(),
        }).eq("id", blog_id).execute
    )

    if not already_refused:
        # Создать retry задачу с text_only=True
        try:
            await create_task_if_not_exists(
                db, blog_id, "ai_analysis",
                payload={"text_only": True},
            )
        except Exception as e:
            logger.error(f"[batch_results] Failed to create text_only retry for {blog_id}: {e}")
else:
    # Нормальный результат — AIInsights
    ...  # (existing code block for insights processing)
```

**Важно:** Нужно проверить, что `create_task_if_not_exists` принимает `payload` kwarg. Если нет — передать через другой механизм.

**Step 3: Update batch.py build_batch_request for text_only mode**

В `src/ai/batch.py`, `build_batch_request()`:

```python
def build_batch_request(
    custom_id: str,
    profile: ScrapedProfile,
    settings: Settings,
    image_map: dict[str, str] | None = None,
    text_only: bool = False,
) -> dict[str, Any]:
```

Если `text_only=True`:
- Передать `image_map=None` в `build_analysis_prompt` (не включать изображения)
- Добавить к system prompt: `"\n\nАнализируй только по текстовым данным. Изображения недоступны."`

**Step 4: Run tests**

Run: `uv run pytest tests/test_worker/test_handlers.py -v`
Run: `uv run pytest tests/test_ai/test_batch.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add src/worker/handlers.py src/ai/batch.py \
        tests/test_worker/test_handlers.py tests/test_ai/test_batch.py
git commit -m "feat: refusal fallback — ai_refused status, text_only retry, double refusal detection"
```

---

### Task 11: Update type annotations for refusal tuple

**Контекст:** `poll_batch` теперь возвращает `dict[str, AIInsights | tuple[str, str] | None]` в results. Нужно обновить type hints.

**Files:**
- Modify: `src/ai/batch.py` (return type hint of poll_batch)
- Modify: `src/worker/handlers.py` (type narrowing в handle_batch_results)

**Step 1: Update poll_batch return type**

В `src/ai/batch.py`, в документации/типизации `poll_batch`:

Добавить комментарий или type alias:

```python
# В результатах: AIInsights | ("refusal", reason) | None
BatchResult = AIInsights | tuple[str, str] | None
```

**Step 2: Run typecheck**

Run: `make typecheck`
Expected: 0 errors

**Step 3: Commit**

```bash
git add src/ai/batch.py src/worker/handlers.py
git commit -m "chore: update type annotations for refusal tuple in batch results"
```

---

### Task 12: Final verification

**Step 1: Run all tests**

Run: `make test`
Expected: 655+ tests PASS

**Step 2: Run linter**

Run: `make lint`
Expected: 0 errors. Если есть — `make lint-fix` и проверить.

**Step 3: Run typecheck**

Run: `make typecheck`
Expected: 0 errors.

**Step 4: Final commit (if lint-fix needed)**

```bash
git add -A
git commit -m "chore: lint fixes after iter2 changes"
```

---

## Порядок задач и зависимости

```
Task 1 (confidence bug) — независимый
Task 2 (embedding quality) — независимый
Task 3 (logging missed) — независимый
Task 4 (fuzzy matching) — после Task 3 (использует те же функции)
Task 5 (data quality hint) — независимый
Task 6 (multi-category schema) — независимый
Task 7 (multi-category integration) — после Task 6 (зависит от schema change)
Task 8 (retry embedding) — после Task 2 (использует обновлённый build_embedding_text)
Task 9 (refusal tuple) — независимый
Task 10 (refusal handlers) — после Task 9 (зависит от tuple в poll_batch)
Task 11 (type annotations) — после Task 9 + 10
Task 12 (final verification) — после всех
```

## Файлы для изменения (summary)

| Файл | Задачи |
|------|--------|
| `src/ai/schemas.py` | Task 6 |
| `src/ai/embedding.py` | Task 2, 7 |
| `src/ai/batch.py` | Task 1, 3, 4, 7, 9, 10, 11 |
| `src/ai/prompt.py` | Task 5, 7 |
| `src/worker/handlers.py` | Task 7, 10, 11 |
| `src/worker/scheduler.py` | Task 8 |
| `tests/test_ai/test_embedding.py` | Task 2, 7 |
| `tests/test_ai/test_batch.py` | Task 1, 3, 4, 7, 9 |
| `tests/test_ai/test_schemas.py` | Task 6 |
| `tests/test_ai/test_prompt.py` | Task 5 |
| `tests/test_worker/test_scheduler.py` | Task 8 |
| `tests/test_worker/test_handlers.py` | Task 10 |
