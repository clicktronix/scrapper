# Memory Optimization: Chunked Streaming Pipeline

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Снизить пиковое потребление памяти `submit_batch` с ~1.2GB до ~50MB и обеспечить возврат освобождённой памяти ОС.

**Architecture:** Заменить batch-accumulate паттерн (все изображения → все JSON → один encode) на chunked streaming pipeline: профили обрабатываются чанками по 10, изображения скачиваются параллельно внутри чанка, JSONL пишется инкрементально в `BytesIO`, промежуточные данные освобождаются после каждого чанка. Принудительный `gc.collect()` после тяжёлых операций + `PYTHONMALLOC=malloc` для возврата памяти ОС.

**Tech Stack:** Python stdlib (`io`, `gc`, `tempfile`), OpenAI SDK, httpx, pytest

---

## Корневые причины

1. **3× дублирование данных в `submit_batch`** — `image_maps_by_id` (все base64), `lines` (JSON строки с base64 внутри), `jsonl_bytes` (encode всех строк). Все три живут одновременно: ~400MB × 3 = ~1.2GB пик при 100 профилях.

2. **Нет backpressure** — `.limit(100)` загружает до 100 профилей, но нет контроля памяти. Семафор ограничивает _конкурентность_ загрузок, но не _накопление_ результатов.

3. **CPython pymalloc не возвращает память ОС** — после GC арены остаются в процессе. RSS не падает даже после полной очистки.

## Обзор изменений

| Файл | Что делаем | Зачем |
|------|-----------|-------|
| `src/ai/batch_api.py` | Refactor `submit_batch` → chunked pipeline | Пик с 1.2GB до ~50MB |
| `src/worker/loop.py` | `gc.collect()` в `process_task` | Принудительный GC после тяжёлых задач |
| `src/worker/scheduler.py` | `gc.collect()` в `poll_batches` | GC после обработки батч-результатов |
| `tests/test_ai/test_batch.py` | Новые тесты chunked поведения | Покрытие нового кода |
| Railway env | `PYTHONMALLOC=malloc` | Возврат памяти ОС |

---

### Task 1: Refactor `submit_batch` — chunked streaming pipeline

**Files:**
- Modify: `src/ai/batch_api.py:186-267`

**Step 1: Run existing tests to verify green baseline**

Run: `uv run pytest tests/test_ai/test_batch.py::TestSubmitBatch -v`
Expected: all 4 tests PASS

**Step 2: Refactor `submit_batch`**

Заменить всё тело функции `submit_batch` (строки 186-267) на:

```python
# Количество профилей на чанк при загрузке изображений.
# Ограничивает пиковую память: 1 чанк × 10 изображений × ~400 КБ ≈ 40 МБ.
_IMAGE_CHUNK_SIZE = 10


async def submit_batch(
    client: AsyncOpenAI,
    profiles: list[tuple[str, ScrapedProfile]],
    settings: Settings,
    text_only_ids: set[str] | None = None,
) -> str:
    """
    Отправить батч профилей на анализ.
    profiles — список (blog_id, ScrapedProfile).
    text_only_ids — blog_id для которых не скачивать изображения (retry после refusal).
    Возвращает batch_id.

    Использует chunked pipeline: профили обрабатываются чанками по _IMAGE_CHUNK_SIZE,
    изображения скачиваются параллельно внутри чанка, JSONL пишется инкрементально.
    Это ограничивает пиковую память: O(chunk_size × image_size) вместо O(total × image_size × 3).
    """
    if not profiles:
        raise ValueError("Cannot submit empty batch")

    _text_only_ids = text_only_ids or set()
    download_semaphore = asyncio.Semaphore(10)
    total_images = 0

    # JSONL буфер — пишем инкрементально, не накапливая промежуточные структуры
    buffer = io.BytesIO()

    async with httpx.AsyncClient() as http_client:
        for chunk_start in range(0, len(profiles), _IMAGE_CHUNK_SIZE):
            chunk = profiles[chunk_start:chunk_start + _IMAGE_CHUNK_SIZE]

            # Параллельная загрузка изображений для чанка (кроме text_only)
            chunk_for_images = [
                (blog_id, profile) for blog_id, profile in chunk
                if blog_id not in _text_only_ids
            ]
            chunk_image_maps: dict[str, dict[str, str]] = {}
            if chunk_for_images:
                image_tasks = [
                    resolve_profile_images(
                        profile, client=http_client, semaphore=download_semaphore,
                    )
                    for _, profile in chunk_for_images
                ]
                raw = await asyncio.gather(*image_tasks, return_exceptions=True)
                for i, r in enumerate(raw):
                    blog_id = chunk_for_images[i][0]
                    if isinstance(r, BaseException):
                        logger.warning(f"[batch] Ошибка загрузки изображений для {blog_id}: {r}")
                        chunk_image_maps[blog_id] = {}
                    else:
                        chunk_image_maps[blog_id] = r
                        total_images += len(r)

            # Формируем JSONL строки и сразу пишем в буфер
            for blog_id, profile in chunk:
                is_text_only = blog_id in _text_only_ids
                image_map = chunk_image_maps.get(blog_id, {})
                request = build_batch_request(
                    blog_id, profile, settings,
                    image_map=image_map, text_only=is_text_only,
                )
                buffer.write(json.dumps(request, ensure_ascii=False).encode("utf-8"))
                buffer.write(b"\n")
                mode = "text-only" if is_text_only else f"{len(image_map)} images"
                logger.debug(
                    f"[batch] Prepared request for blog {blog_id} "
                    f"(@{profile.username}, {len(profile.medias)} publications, {mode})"
                )

            # Явно освобождаем данные изображений чанка
            del chunk_image_maps

    total_profiles_with_images = sum(
        1 for blog_id, _ in profiles if blog_id not in _text_only_ids
    )
    logger.info(
        f"[batch] Скачано {total_images} изображений для "
        f"{total_profiles_with_images} профилей "
        f"({len(_text_only_ids)} text-only)"
    )
    logger.debug(f"[batch] JSONL size: {buffer.tell()} bytes, model={settings.batch_model}")

    # Загружаем файл в OpenAI
    buffer.seek(0)
    try:
        file_obj = await client.files.create(
            file=("batch.jsonl", buffer),
            purpose="batch",
        )
    finally:
        buffer.close()
    logger.debug(f"[batch] File uploaded: {file_obj.id}")

    # Создаём батч
    batch = await client.batches.create(
        input_file_id=file_obj.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
    )

    logger.info(f"Submitted batch {batch.id} with {len(profiles)} profiles")
    return batch.id
```

Ключевые архитектурные решения:
- **Chunked processing**: профили обрабатываются по `_IMAGE_CHUNK_SIZE` (10). Параллельность загрузки изображений внутри чанка сохраняется (семафор=10 = один полный чанк). Скорость идентична текущей.
- **Incremental write**: JSONL пишется построчно в `BytesIO`. Нет промежуточных `lines: list[str]` и `jsonl_bytes`.
- **Eager cleanup**: `del chunk_image_maps` после каждого чанка. `buffer.close()` в `finally` после upload.
- **Без tempfile**: `BytesIO` вместо `SpooledTemporaryFile`, т.к. OpenAI SDK (через HTTPX) всё равно читает весь файл в память при multipart encoding. Диск не даёт выигрыша.

**Step 3: Run existing tests to verify refactoring is correct**

Run: `uv run pytest tests/test_ai/test_batch.py::TestSubmitBatch -v`
Expected: all 4 tests PASS (интерфейс не изменился, моки работают)

**Step 4: Commit**

```bash
git add src/ai/batch_api.py
git commit -m "refactor(batch_api): chunked streaming pipeline для submit_batch

Заменяет batch-accumulate паттерн на chunked pipeline:
- Профили обрабатываются чанками по 10
- JSONL пишется инкрементально в BytesIO
- Изображения освобождаются после каждого чанка

Пиковая память: ~50MB (чанк) вместо ~1.2GB (3 копии всех данных)."
```

---

### Task 2: `gc.collect()` после тяжёлых операций

**Files:**
- Modify: `src/worker/loop.py:73-140` (`process_task`)
- Modify: `src/worker/scheduler.py:117-123` (`poll_batches`)

**Step 1: Добавить gc.collect() в process_task**

В `src/worker/loop.py`, добавить `import gc` в начало файла и finally-блок в `process_task`:

```python
import gc

# В process_task, обернуть try/except в try/finally:
async def process_task(...) -> None:
    """Обработать одну задачу с учётом семафора."""
    async with semaphore:
        task_type = task["task_type"]
        task_id = task["id"]
        attempts = task.get("attempts", 0)
        max_attempts = task.get("max_attempts", 3)
        logger.debug(f"Processing task {task_id}: type={task_type}, "
                     f"attempts={attempts}/{max_attempts}")

        try:
            # ... существующий код обработки (handler dispatch) ...
        except Exception as e:
            # ... существующий код обработки ошибок ...
        finally:
            # AI-задачи и full_scrape могут выделять сотни МБ (изображения, JSONL).
            # Принудительный GC гарантирует освобождение циклических ссылок
            # до следующего поллинга. С PYTHONMALLOC=malloc память возвращается ОС.
            if task_type in ("ai_analysis", "full_scrape"):
                gc.collect()
```

**Step 2: Добавить gc.collect() в poll_batches**

В `src/worker/scheduler.py`, добавить `import gc` и gc.collect после каждого батча:

```python
import gc

# В poll_batches, после handle_batch_results:
    for batch_id, task_ids_by_blog in batches.items():
        logger.debug(f"[poll_batches] Processing batch {batch_id} "
                     f"({len(task_ids_by_blog)} blogs)")
        try:
            await handle_batch_results(db, openai_client, batch_id, task_ids_by_blog)
        except Exception as e:
            logger.error(f"Error polling batch {batch_id}: {e}")
        finally:
            gc.collect()
```

**Step 3: Run tests**

Run: `uv run pytest tests/test_worker/test_loop.py tests/test_worker/test_handlers.py -v`
Expected: PASS (gc.collect() не влияет на поведение)

**Step 4: Commit**

```bash
git add src/worker/loop.py src/worker/scheduler.py
git commit -m "perf: gc.collect() после задач с высоким потреблением памяти

AI-анализ и full_scrape могут выделять сотни МБ для изображений и JSONL.
Принудительный GC после этих задач гарантирует своевременное освобождение."
```

---

### Task 3: Тесты chunked pipeline

**Files:**
- Modify: `tests/test_ai/test_batch.py`

**Step 1: Добавить тест chunked обработки**

```python
class TestSubmitBatchChunked:
    """Тесты chunked streaming pipeline в submit_batch."""

    @pytest.mark.asyncio
    async def test_large_batch_processes_in_chunks(self) -> None:
        """15 профилей обрабатываются чанками, все попадают в один JSONL."""
        from unittest.mock import patch

        from src.ai.batch_api import submit_batch

        settings = _make_settings()
        profiles = [(f"blog-{i}", _make_profile()) for i in range(15)]

        mock_client = MagicMock()
        mock_file = MagicMock()
        mock_file.id = "file-chunked"
        mock_client.files.create = AsyncMock(return_value=mock_file)

        mock_batch = MagicMock()
        mock_batch.id = "batch-chunked"
        mock_client.batches.create = AsyncMock(return_value=mock_batch)

        with patch(
            "src.ai.batch_api.resolve_profile_images",
            new_callable=AsyncMock, return_value={},
        ) as mock_resolve:
            batch_id = await submit_batch(mock_client, profiles, settings)

        assert batch_id == "batch-chunked"
        # resolve вызван для каждого профиля
        assert mock_resolve.call_count == 15
        # Один файл загружен в OpenAI (не по чанкам)
        mock_client.files.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_chunked_text_only_mixed(self) -> None:
        """text_only профили пропускают загрузку изображений в каждом чанке."""
        from unittest.mock import patch

        from src.ai.batch_api import submit_batch

        settings = _make_settings()
        # 12 профилей, 3 из них text_only (разбросаны по чанкам)
        profiles = [(f"blog-{i}", _make_profile()) for i in range(12)]
        text_only_ids = {"blog-0", "blog-5", "blog-11"}

        mock_client = MagicMock()
        mock_file = MagicMock()
        mock_file.id = "file-mixed"
        mock_client.files.create = AsyncMock(return_value=mock_file)

        mock_batch = MagicMock()
        mock_batch.id = "batch-mixed"
        mock_client.batches.create = AsyncMock(return_value=mock_batch)

        with patch(
            "src.ai.batch_api.resolve_profile_images",
            new_callable=AsyncMock, return_value={},
        ) as mock_resolve:
            await submit_batch(
                mock_client, profiles, settings,
                text_only_ids=text_only_ids,
            )

        # resolve вызван только для non-text-only (12 - 3 = 9)
        assert mock_resolve.call_count == 9

    @pytest.mark.asyncio
    async def test_jsonl_content_valid(self) -> None:
        """Каждая строка буфера — валидный JSON с правильным custom_id."""
        from unittest.mock import patch

        from src.ai.batch_api import submit_batch

        settings = _make_settings()
        profiles = [
            ("blog-a", _make_profile()),
            ("blog-b", _make_profile()),
        ]

        mock_client = MagicMock()
        mock_file = MagicMock()
        mock_file.id = "file-valid"
        mock_client.files.create = AsyncMock(return_value=mock_file)
        mock_batch = MagicMock()
        mock_batch.id = "batch-valid"
        mock_client.batches.create = AsyncMock(return_value=mock_batch)

        captured_buffer = None

        async def capture_file(**kwargs):
            nonlocal captured_buffer
            _, buf = kwargs.get("file") or (None, None)
            if buf is not None:
                captured_buffer = buf.read()
            return mock_file

        mock_client.files.create = capture_file

        with patch(
            "src.ai.batch_api.resolve_profile_images",
            new_callable=AsyncMock, return_value={},
        ):
            await submit_batch(mock_client, profiles, settings)

        assert captured_buffer is not None
        lines = captured_buffer.decode("utf-8").strip().split("\n")
        assert len(lines) == 2
        ids = {json.loads(line)["custom_id"] for line in lines}
        assert ids == {"blog-a", "blog-b"}
```

**Step 2: Run all submit_batch tests**

Run: `uv run pytest tests/test_ai/test_batch.py::TestSubmitBatch tests/test_ai/test_batch.py::TestSubmitBatchChunked -v`
Expected: all PASS

**Step 3: Commit**

```bash
git add tests/test_ai/test_batch.py
git commit -m "test: тесты chunked pipeline для submit_batch"
```

---

### Task 4: Фикс мёртвого кода в scheduler.py

**Files:**
- Modify: `src/worker/scheduler.py:96-113`

В текущем staged diff есть мёртвый код — после `continue` на orphaned tasks, второй `if isinstance(batch_id, str) and batch_id:` всегда `True`:

**Step 1: Убрать лишнюю проверку**

```python
    for task in _as_rows(result.data):
        payload = task.get("payload")
        payload_dict = payload if isinstance(payload, dict) else {}
        batch_id = payload_dict.get("batch_id")
        if not (isinstance(batch_id, str) and batch_id):
            orphaned_task_ids.append(str(task.get("id", "?")))
            continue

        # batch_id гарантированно str и непустой после проверки выше
        if batch_id not in batches:
            batches[batch_id] = {}
        task_info = {
            "id": str(task.get("id", "")),
            "attempts": int(task.get("attempts", 1) or 1),
            "max_attempts": int(task.get("max_attempts", 3) or 3),
        }
        blog_id = str(task.get("blog_id", ""))
        if not blog_id:
            continue
        existing = batches[batch_id].get(blog_id)
        if existing is None:
            batches[batch_id][blog_id] = task_info
        elif isinstance(existing, list):
            existing.append(task_info)
        else:
            batches[batch_id][blog_id] = [existing, task_info]
```

**Step 2: Run scheduler tests**

Run: `uv run pytest tests/test_worker/ -v -k scheduler`
Expected: PASS

**Step 3: Commit**

```bash
git add src/worker/scheduler.py
git commit -m "fix(scheduler): убрать мёртвый код после orphaned check"
```

---

### Task 5: PYTHONMALLOC=malloc в Railway

Не код — настройка окружения.

**Step 1:** В Railway dashboard → сервис scraper → Variables, добавить:
```
PYTHONMALLOC=malloc
```

**Зачем:** CPython pymalloc использует пул арен и **не возвращает** освобождённую память ОС. Системный malloc (`glibc malloc` на Linux) использует `mmap` для крупных аллокаций и возвращает их через `munmap`. Это бесплатно для I/O-bound сервиса (разница < 5%).

**Ожидаемый эффект после деплоя:**
- Пик при batch submit: ~50MB (чанк) + ~400MB (буфер JSONL) = ~450MB
- После gc.collect() + malloc: возврат к ~150-200MB base
- Вместо текущих: 1GB RSS которые не падают

---

## Верификация

После деплоя всех изменений проверить в Railway:

1. **Функциональность**: AI batch submit → poll → results обрабатываются корректно
2. **Память**: при обработке батча пик < 500MB, после — снижение до ~200MB
3. **Скорость**: время обработки батча не увеличилось (семафор=10 = chunk_size=10, параллельность та же)
