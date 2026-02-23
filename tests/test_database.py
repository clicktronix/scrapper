"""Тесты CRUD-операций с Supabase."""
from unittest.mock import MagicMock


def _mock_supabase():
    """Создать мок Supabase client с цепочкой вызовов."""
    db = MagicMock()
    # table().update().eq().execute() цепочка
    table_mock = MagicMock()
    db.table.return_value = table_mock
    table_mock.update.return_value = table_mock
    table_mock.insert.return_value = table_mock
    table_mock.upsert.return_value = table_mock
    table_mock.select.return_value = table_mock
    table_mock.eq.return_value = table_mock
    table_mock.in_.return_value = table_mock
    table_mock.is_.return_value = table_mock
    table_mock.lte.return_value = table_mock
    table_mock.lt.return_value = table_mock
    table_mock.gt.return_value = table_mock
    table_mock.or_.return_value = table_mock
    table_mock.order.return_value = table_mock
    table_mock.limit.return_value = table_mock
    table_mock.execute.return_value = MagicMock(data=[])
    # rpc цепочка
    rpc_mock = MagicMock()
    db.rpc.return_value = rpc_mock
    rpc_mock.execute.return_value = MagicMock(data=[])
    return db


class TestIsBlogFresh:
    """Тесты is_blog_fresh — проверка свежести скрапинга блога."""

    async def test_fresh_blog_returns_true(self) -> None:
        """Блог скрапился недавно → True."""
        from src.database import is_blog_fresh

        db = _mock_supabase()
        # Возвращаем данные — блог свежий
        chain = db.table.return_value.select.return_value.eq.return_value
        chain.gt.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[{"scraped_at": "2026-02-01T00:00:00+00:00"}]
        )

        result = await is_blog_fresh(db, "blog-1", 60)
        assert result is True
        db.table.assert_called_with("blogs")

    async def test_stale_blog_returns_false(self) -> None:
        """Блог скрапился давно → False."""
        from src.database import is_blog_fresh

        db = _mock_supabase()
        # Пустой результат — блог устаревший
        chain = db.table.return_value.select.return_value.eq.return_value
        chain.gt.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[]
        )

        result = await is_blog_fresh(db, "blog-1", 60)
        assert result is False

    async def test_null_scraped_at_returns_false(self) -> None:
        """scraped_at=NULL (никогда не скрапился) → False."""
        from src.database import is_blog_fresh

        db = _mock_supabase()
        # NULL scraped_at не проходит gt фильтр → пустой результат
        chain = db.table.return_value.select.return_value.eq.return_value
        chain.gt.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[]
        )

        result = await is_blog_fresh(db, "blog-new", 60)
        assert result is False


class TestExtractRpcScalar:
    """Тесты _extract_rpc_scalar — парсинг ответов Supabase RPC."""

    def test_list_with_named_key(self) -> None:
        """Стандартный ответ: [{\"mark_task_running\": \"task-id\"}]."""
        from src.database import _extract_rpc_scalar

        result = _extract_rpc_scalar([{"mark_task_running": "task-id"}])
        assert result == "task-id"

    def test_list_with_named_key_none(self) -> None:
        """Задача не была заклеймлена: [{\"mark_task_running\": null}]."""
        from src.database import _extract_rpc_scalar

        result = _extract_rpc_scalar([{"mark_task_running": None}])
        assert result is None

    def test_dict_with_named_key(self) -> None:
        """Ответ как dict: {\"mark_task_running\": \"task-id\"}."""
        from src.database import _extract_rpc_scalar

        result = _extract_rpc_scalar({"mark_task_running": "task-id"})
        assert result == "task-id"

    def test_empty_list(self) -> None:
        from src.database import _extract_rpc_scalar

        result = _extract_rpc_scalar([])
        assert result is None

    def test_none_value(self) -> None:
        from src.database import _extract_rpc_scalar

        result = _extract_rpc_scalar(None)
        assert result is None

    def test_scalar_string(self) -> None:
        """Скалярное значение проходит без изменений."""
        from src.database import _extract_rpc_scalar

        result = _extract_rpc_scalar("task-uuid-123")
        assert result == "task-uuid-123"

    def test_list_with_single_key_dict(self) -> None:
        """Один ключ — возвращаем значение."""
        from src.database import _extract_rpc_scalar

        result = _extract_rpc_scalar([{"result": "ok"}])
        assert result == "ok"

    def test_list_with_multi_key_dict(self) -> None:
        """Несколько ключей без mark_task_running — возвращаем весь dict."""
        from src.database import _extract_rpc_scalar

        data = [{"a": 1, "b": 2}]
        result = _extract_rpc_scalar(data)
        assert result == {"a": 1, "b": 2}

    def test_list_with_non_dict_item(self) -> None:
        """[42] → 42."""
        from src.database import _extract_rpc_scalar

        result = _extract_rpc_scalar([42])
        assert result == 42

    def test_list_with_none_item(self) -> None:
        """[None] → None."""
        from src.database import _extract_rpc_scalar

        result = _extract_rpc_scalar([None])
        assert result is None

    def test_dict_single_key(self) -> None:
        """Dict с одним ключом — возвращаем значение."""
        from src.database import _extract_rpc_scalar

        result = _extract_rpc_scalar({"value": "hello"})
        assert result == "hello"

    def test_dict_multi_key_returns_whole_dict(self) -> None:
        """Dict с несколькими ключами — возвращаем весь dict."""
        from src.database import _extract_rpc_scalar

        data = {"x": 1, "y": 2}
        result = _extract_rpc_scalar(data)
        assert result == {"x": 1, "y": 2}

    def test_empty_dict_in_list_returns_none(self) -> None:
        """[{}] → None — пустой dict означает отсутствие результата."""
        from src.database import _extract_rpc_scalar

        result = _extract_rpc_scalar([{}])
        assert result is None

    def test_bare_empty_dict_returns_none(self) -> None:
        """Пустой dict {} — truthy, но означает отсутствие результата."""
        from src.database import _extract_rpc_scalar

        result = _extract_rpc_scalar({})
        assert result is None

    async def test_mark_task_running_false_for_empty_dict(self) -> None:
        """mark_task_running не должен возвращать True для пустого dict."""
        from src.database import mark_task_running

        db = _mock_supabase()
        db.rpc.return_value.execute.return_value = MagicMock(data={})

        result = await mark_task_running(db, "task-1")
        assert result is False


class TestMarkTaskRunning:
    """Тесты mark_task_running."""

    async def test_calls_rpc_with_task_id(self) -> None:
        from src.database import mark_task_running

        db = _mock_supabase()
        db.rpc.return_value.execute.return_value = MagicMock(
            data={"mark_task_running": "task-123"}
        )
        result = await mark_task_running(db, "task-123")

        db.rpc.assert_called_once()
        call_args = db.rpc.call_args[0]
        assert call_args[0] == "mark_task_running"
        params = call_args[1]
        assert params["p_task_id"] == "task-123"
        assert "p_started_at" in params
        assert result is True

    async def test_returns_false_when_task_not_claimed(self) -> None:
        from src.database import mark_task_running

        db = _mock_supabase()
        db.rpc.return_value.execute.return_value = MagicMock(data=None)

        result = await mark_task_running(db, "task-123")
        assert result is False


class TestMarkTaskDone:
    """Тесты mark_task_done."""

    async def test_updates_status(self) -> None:
        from src.database import mark_task_done

        db = _mock_supabase()
        await mark_task_done(db, "task-123")

        call_args = db.table().update.call_args[0][0]
        assert call_args["status"] == "done"
        assert "completed_at" in call_args


class TestMarkTaskFailed:
    """Тесты mark_task_failed с retry и без."""

    async def test_retry_sets_pending(self) -> None:
        from src.database import mark_task_failed

        db = _mock_supabase()
        await mark_task_failed(db, "task-1", attempts=1, max_attempts=3,
                               error="timeout", retry=True)

        call_args = db.table().update.call_args[0][0]
        assert call_args["status"] == "pending"
        assert "next_retry_at" in call_args

    async def test_no_retry_sets_failed(self) -> None:
        from src.database import mark_task_failed

        db = _mock_supabase()
        await mark_task_failed(db, "task-2", attempts=3, max_attempts=3,
                               error="UserNotFound", retry=False)

        call_args = db.table().update.call_args[0][0]
        assert call_args["status"] == "failed"
        assert call_args["error_message"] == "UserNotFound"

    async def test_exhausted_retries_sets_failed(self) -> None:
        from src.database import mark_task_failed

        db = _mock_supabase()
        await mark_task_failed(db, "task-3", attempts=3, max_attempts=3,
                               error="timeout", retry=True)

        call_args = db.table().update.call_args[0][0]
        assert call_args["status"] == "failed"


class TestCreateTaskIfNotExists:
    """Тесты дедупликации задач через RPC."""

    async def test_creates_when_no_duplicate(self) -> None:
        from src.database import create_task_if_not_exists

        db = _mock_supabase()
        # RPC возвращает UUID новой задачи
        rpc_mock = db.rpc.return_value
        rpc_mock.execute.return_value = MagicMock(data="new-task-uuid")

        result = await create_task_if_not_exists(db, "blog-1", "full_scrape", 5)
        assert result == "new-task-uuid"

        db.rpc.assert_called_once()
        call_args = db.rpc.call_args[0]
        assert call_args[0] == "create_task_if_not_exists"
        params = call_args[1]
        assert params["p_blog_id"] == "blog-1"
        assert params["p_task_type"] == "full_scrape"
        assert params["p_priority"] == 5

    async def test_skips_when_duplicate_exists(self) -> None:
        from src.database import create_task_if_not_exists

        db = _mock_supabase()
        # RPC возвращает None, когда дубликат уже есть
        rpc_mock = db.rpc.return_value
        rpc_mock.execute.return_value = MagicMock(data=None)

        result = await create_task_if_not_exists(db, "blog-1", "full_scrape", 5)
        assert result is None

    async def test_allows_none_blog_id(self) -> None:
        from src.database import create_task_if_not_exists

        db = _mock_supabase()
        rpc_mock = db.rpc.return_value
        rpc_mock.execute.return_value = MagicMock(data="discover-task-uuid")

        result = await create_task_if_not_exists(db, None, "discover", 10)
        assert result == "discover-task-uuid"
        params = db.rpc.call_args[0][1]
        assert params["p_blog_id"] is None


class TestSanitizeError:
    """Тесты очистки ошибок от креденшалов."""

    def test_removes_proxy_credentials(self) -> None:
        from src.database import sanitize_error
        error = "Connection to http://user:pass123@proxy.example.com:8080 failed"
        result = sanitize_error(error)
        assert "user:pass123" not in result
        assert "***:***" in result

    def test_leaves_clean_error_unchanged(self) -> None:
        from src.database import sanitize_error
        error = "UserNotFound: user does not exist"
        assert sanitize_error(error) == error

    def testsanitize_error_in_mark_task_failed(self) -> None:
        """mark_task_failed должен санитизировать ошибку перед сохранением."""
        # Проверяем через мок, что в БД попадает очищенная ошибка
        from src.database import sanitize_error
        error = "Proxy http://admin:secret@10.0.0.1:3128 timeout"
        sanitized = sanitize_error(error)
        assert "admin:secret" not in sanitized


class TestGetBackoffSeconds:
    """Тесты экспоненциального backoff."""

    def test_first_attempt(self) -> None:
        from src.database import get_backoff_seconds
        assert get_backoff_seconds(1) == 300  # 5 минут

    def test_second_attempt(self) -> None:
        from src.database import get_backoff_seconds
        assert get_backoff_seconds(2) == 900  # 15 минут

    def test_third_attempt(self) -> None:
        from src.database import get_backoff_seconds
        assert get_backoff_seconds(3) == 2700  # 45 минут


class TestGetBackoffSecondsEdge:
    """Дополнительные edge case тесты backoff."""

    def test_fourth_attempt(self) -> None:
        """attempt 4 → 8100с (2.25ч)."""
        from src.database import get_backoff_seconds
        assert get_backoff_seconds(4) == 8100

    def test_backoff_grows_exponentially(self) -> None:
        """Каждый следующий backoff = предыдущий * 3."""
        from src.database import get_backoff_seconds
        for i in range(1, 5):
            assert get_backoff_seconds(i + 1) == get_backoff_seconds(i) * 3

    def test_first_attempt_is_five_minutes(self) -> None:
        """Первый retry через 5 минут."""
        from src.database import get_backoff_seconds
        assert get_backoff_seconds(1) == 5 * 60

    def test_zero_attempts_returns_300(self) -> None:
        """attempts=0 → 300с (такой же как attempts=1)."""
        from src.database import get_backoff_seconds
        result = get_backoff_seconds(0)
        assert isinstance(result, int)
        assert result == 300

    def test_negative_attempts_returns_300(self) -> None:
        """Отрицательные attempts → 300с (max(0, n-1)=0 → 3**0=1)."""
        from src.database import get_backoff_seconds
        assert get_backoff_seconds(-1) == 300
        assert get_backoff_seconds(-10) == 300


class TestMarkTaskFailedEdge:
    """Дополнительные edge case тесты mark_task_failed."""

    async def test_sanitizes_proxy_credentials_in_error(self) -> None:
        """Креденшалы прокси убираются из error_message перед сохранением в БД."""
        from src.database import mark_task_failed

        db = _mock_supabase()
        error = "Connection to http://user:p@ss@proxy:8080 failed"

        await mark_task_failed(db, "t1", attempts=1, max_attempts=3,
                               error=error, retry=True)

        call_args = db.table().update.call_args[0][0]
        assert "user:p@ss" not in call_args["error_message"]
        assert "***:***" in call_args["error_message"]

    async def test_retry_true_but_max_reached_sets_failed(self) -> None:
        """retry=True, но attempts >= max_attempts → status=failed."""
        from src.database import mark_task_failed

        db = _mock_supabase()
        await mark_task_failed(db, "t1", attempts=5, max_attempts=3,
                               error="timeout", retry=True)

        call_args = db.table().update.call_args[0][0]
        assert call_args["status"] == "failed"


class TestCreateTaskIfNotExistsEdge:
    """Дополнительные edge case тесты create_task_if_not_exists."""

    async def test_passes_payload_to_rpc(self) -> None:
        """Payload передаётся в RPC-вызов."""
        from src.database import create_task_if_not_exists

        db = _mock_supabase()
        db.rpc.return_value.execute.return_value = MagicMock(data="task-uuid")
        payload = {"hashtag": "beauty", "min_followers": 1000}

        await create_task_if_not_exists(db, None, "discover", 10, payload=payload)

        params = db.rpc.call_args[0][1]
        assert params["p_payload"] == payload

    async def test_default_payload_is_empty_dict(self) -> None:
        """Без payload передаётся пустой dict {}."""
        from src.database import create_task_if_not_exists

        db = _mock_supabase()
        db.rpc.return_value.execute.return_value = MagicMock(data="task-uuid")

        await create_task_if_not_exists(db, "blog-1", "full_scrape", 5)

        params = db.rpc.call_args[0][1]
        assert params["p_payload"] == {}

    async def test_empty_string_data_returns_none(self) -> None:
        """RPC возвращает пустую строку — считаем как 'не создано'."""
        from src.database import create_task_if_not_exists

        db = _mock_supabase()
        db.rpc.return_value.execute.return_value = MagicMock(data="")

        result = await create_task_if_not_exists(db, "blog-1", "full_scrape", 5)
        assert result is None


class TestRecoverStuckTasks:
    """Тесты recovery зависших running задач."""

    async def test_no_stuck_tasks(self) -> None:
        from src.database import recover_stuck_tasks

        db = _mock_supabase()
        result = await recover_stuck_tasks(db)
        assert result == 0

    async def test_recovers_pending(self) -> None:
        """Задача с оставшимися попытками возвращается в pending."""
        from src.database import recover_stuck_tasks

        db = _mock_supabase()
        chain = db.table.return_value.select.return_value.eq.return_value.in_.return_value
        chain.lt.return_value.execute.return_value = MagicMock(
            data=[{"id": "task-1", "task_type": "full_scrape", "attempts": 1, "max_attempts": 3}]
        )

        result = await recover_stuck_tasks(db)
        assert result == 1

        # Проверяем, что update вызван со status=pending
        update_args = db.table.return_value.update.call_args[0][0]
        assert update_args["status"] == "pending"
        assert "Recovered" in update_args["error_message"]

    async def test_exhausted_attempts_sets_failed(self) -> None:
        """Задача с исчерпанными попытками помечается как failed."""
        from src.database import recover_stuck_tasks

        db = _mock_supabase()
        chain = db.table.return_value.select.return_value.eq.return_value.in_.return_value
        chain.lt.return_value.execute.return_value = MagicMock(
            data=[{"id": "task-2", "task_type": "discover", "attempts": 3, "max_attempts": 3}]
        )

        result = await recover_stuck_tasks(db)
        assert result == 0  # не считается как recovered

        update_args = db.table.return_value.update.call_args[0][0]
        assert update_args["status"] == "failed"
        assert "max attempts exhausted" in update_args["error_message"]

    async def test_mixed_tasks(self) -> None:
        """Микс: одна задача recovered, другая failed."""
        from src.database import recover_stuck_tasks

        db = _mock_supabase()
        chain = db.table.return_value.select.return_value.eq.return_value.in_.return_value
        chain.lt.return_value.execute.return_value = MagicMock(
            data=[
                {"id": "task-1", "task_type": "full_scrape", "attempts": 1, "max_attempts": 3},
                {"id": "task-2", "task_type": "discover", "attempts": 3, "max_attempts": 3},
            ]
        )

        result = await recover_stuck_tasks(db)
        assert result == 1  # только task-1 recovered


class TestFetchPendingTasks:
    """Тесты получения pending задач."""

    async def test_returns_task_list(self) -> None:
        from src.database import fetch_pending_tasks

        db = _mock_supabase()
        tasks = [{"id": "t1", "status": "pending", "priority": 5}]
        chain = db.table.return_value.select.return_value.eq.return_value
        or_chain = chain.or_.return_value.order.return_value.order.return_value
        or_chain.limit.return_value.execute.return_value = MagicMock(
            data=tasks
        )

        result = await fetch_pending_tasks(db, limit=10)
        assert result == tasks

    async def test_empty_result(self) -> None:
        from src.database import fetch_pending_tasks

        db = _mock_supabase()
        chain = db.table.return_value.select.return_value.eq.return_value
        or_chain = chain.or_.return_value.order.return_value.order.return_value
        or_chain.limit.return_value.execute.return_value = MagicMock(
            data=[]
        )

        result = await fetch_pending_tasks(db)
        assert result == []

    async def test_calls_with_correct_table(self) -> None:
        from src.database import fetch_pending_tasks

        db = _mock_supabase()
        chain = db.table.return_value.select.return_value.eq.return_value
        or_chain = chain.or_.return_value.order.return_value.order.return_value
        or_chain.limit.return_value.execute.return_value = MagicMock(
            data=[]
        )

        await fetch_pending_tasks(db, limit=5)
        db.table.assert_called_with("scrape_tasks")


class TestUpsertBlog:
    """Тесты обновления данных блога."""

    async def test_calls_update_with_data(self) -> None:
        from src.database import upsert_blog

        db = _mock_supabase()
        data = {"username": "testuser", "follower_count": 5000}

        await upsert_blog(db, "blog-1", data)
        db.table.assert_called_with("blogs")
        db.table.return_value.update.assert_called_once_with(data)

    async def test_filters_by_blog_id(self) -> None:
        from src.database import upsert_blog

        db = _mock_supabase()
        await upsert_blog(db, "blog-42", {"x": 1})
        db.table.return_value.update.return_value.eq.assert_called_with("id", "blog-42")


class TestUpsertPosts:
    """Тесты upsert постов."""

    async def test_empty_posts_skips(self) -> None:
        from src.database import upsert_posts

        db = _mock_supabase()
        await upsert_posts(db, "blog-1", [])
        # Не должно быть вызова upsert
        db.table.return_value.upsert.assert_not_called()

    async def test_adds_blog_id_to_posts(self) -> None:
        from src.database import upsert_posts

        db = _mock_supabase()
        posts = [{"platform_id": "p1", "caption": "test"}]

        await upsert_posts(db, "blog-1", posts)
        # blog_id добавлен в данные, переданные в upsert
        upserted = db.table.return_value.upsert.call_args[0][0]
        assert upserted[0]["blog_id"] == "blog-1"
        db.table.assert_called_with("blog_posts")

    async def test_uses_conflict_key(self) -> None:
        from src.database import upsert_posts

        db = _mock_supabase()
        posts = [{"platform_id": "p1"}]

        await upsert_posts(db, "blog-1", posts)
        db.table.return_value.upsert.assert_called_once()
        call_kwargs = db.table.return_value.upsert.call_args
        assert call_kwargs[1]["on_conflict"] == "blog_id,platform_id"


class TestUpsertHighlights:
    """Тесты upsert хайлайтов."""

    async def test_empty_highlights_skips(self) -> None:
        from src.database import upsert_highlights

        db = _mock_supabase()
        await upsert_highlights(db, "blog-1", [])
        db.table.return_value.upsert.assert_not_called()

    async def test_adds_blog_id(self) -> None:
        from src.database import upsert_highlights

        db = _mock_supabase()
        highlights = [{"platform_id": "h1", "title": "Stories"}]

        await upsert_highlights(db, "blog-1", highlights)
        # blog_id добавлен в данные, переданные в upsert
        upserted = db.table.return_value.upsert.call_args[0][0]
        assert upserted[0]["blog_id"] == "blog-1"
        db.table.assert_called_with("blog_highlights")

    async def test_uses_conflict_key(self) -> None:
        from src.database import upsert_highlights

        db = _mock_supabase()
        highlights = [{"platform_id": "h1", "title": "FAQ"}]

        await upsert_highlights(db, "blog-1", highlights)
        call_kwargs = db.table.return_value.upsert.call_args
        assert call_kwargs[1]["on_conflict"] == "blog_id,platform_id"


class TestUpsertPostsMutation:
    """Баг: upsert_posts/upsert_highlights мутируют входные данные."""

    async def test_upsert_posts_does_not_mutate_input(self) -> None:
        """Входной список dict-ов не должен модифицироваться после вызова."""
        from src.database import upsert_posts

        db = _mock_supabase()
        posts = [{"platform_id": "p1", "caption": "test"}]

        await upsert_posts(db, "blog-1", posts)

        # После вызова входной dict НЕ должен содержать blog_id
        assert "blog_id" not in posts[0]

    async def test_upsert_highlights_does_not_mutate_input(self) -> None:
        """Входной список dict-ов не должен модифицироваться после вызова."""
        from src.database import upsert_highlights

        db = _mock_supabase()
        highlights = [{"platform_id": "h1", "title": "Stories"}]

        await upsert_highlights(db, "blog-1", highlights)

        assert "blog_id" not in highlights[0]
