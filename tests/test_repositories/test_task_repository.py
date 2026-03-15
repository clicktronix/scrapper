"""Тесты SupabaseTaskRepository."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.repositories.task_repository import SupabaseTaskRepository, _extract_rpc_scalar


def _mock_supabase():
    """Создать мок Supabase AsyncClient с цепочкой вызовов."""
    db = MagicMock()
    table_mock = MagicMock()
    db.table.return_value = table_mock
    table_mock.update.return_value = table_mock
    table_mock.select.return_value = table_mock
    table_mock.eq.return_value = table_mock
    table_mock.in_.return_value = table_mock
    table_mock.lt.return_value = table_mock
    table_mock.gt.return_value = table_mock
    table_mock.or_.return_value = table_mock
    table_mock.order.return_value = table_mock
    table_mock.limit.return_value = table_mock
    table_mock.execute = AsyncMock(return_value=MagicMock(data=[]))

    rpc_mock = MagicMock()
    db.rpc.return_value = rpc_mock
    rpc_mock.execute = AsyncMock(return_value=MagicMock(data=[]))

    return db, table_mock, rpc_mock


# --- _extract_rpc_scalar ---


class TestExtractRpcScalar:
    def test_empty_list(self):
        assert _extract_rpc_scalar([]) is None

    def test_list_with_single_key_dict(self):
        assert _extract_rpc_scalar([{"id": "abc"}]) == "abc"

    def test_list_with_multi_key_dict(self):
        result = _extract_rpc_scalar([{"id": "abc", "name": "test"}])
        assert result == {"id": "abc", "name": "test"}

    def test_list_with_empty_dict(self):
        assert _extract_rpc_scalar([{}]) is None

    def test_list_with_scalar(self):
        assert _extract_rpc_scalar(["value"]) == "value"

    def test_empty_dict(self):
        assert _extract_rpc_scalar({}) is None

    def test_single_key_dict(self):
        assert _extract_rpc_scalar({"key": 42}) == 42

    def test_multi_key_dict(self):
        result = _extract_rpc_scalar({"a": 1, "b": 2})
        assert result == {"a": 1, "b": 2}

    def test_scalar_passthrough(self):
        assert _extract_rpc_scalar("hello") == "hello"
        assert _extract_rpc_scalar(42) == 42
        assert _extract_rpc_scalar(None) is None


# --- SupabaseTaskRepository ---


class TestMarkRunning:
    @pytest.mark.asyncio
    async def test_mark_running_success(self):
        db, _, rpc_mock = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        rpc_mock.execute = AsyncMock(
            return_value=MagicMock(data=[{"mark_task_running": "task-1"}])
        )
        result = await repo.mark_running("task-1")
        assert result is True

    @pytest.mark.asyncio
    async def test_mark_running_already_claimed(self):
        db, _, rpc_mock = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        rpc_mock.execute = AsyncMock(return_value=MagicMock(data=[]))
        result = await repo.mark_running("task-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_mark_running_calls_rpc(self):
        db, _, rpc_mock = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        rpc_mock.execute = AsyncMock(
            return_value=MagicMock(data=[{"mark_task_running": "t1"}])
        )
        await repo.mark_running("t1")
        db.rpc.assert_called_once()


class TestMarkDone:
    @pytest.mark.asyncio
    async def test_mark_done_calls_update(self):
        db, _table_mock, _ = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        await repo.mark_done("task-1")
        db.table.assert_called_with("scrape_tasks")

    @pytest.mark.asyncio
    async def test_mark_done_sets_status(self):
        db, table_mock, _ = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        await repo.mark_done("task-1")
        update_call = table_mock.update.call_args
        assert update_call[0][0]["status"] == "done"
        assert "completed_at" in update_call[0][0]


class TestMarkFailed:
    @pytest.mark.asyncio
    async def test_mark_failed_with_retry(self):
        db, table_mock, _ = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        await repo.mark_failed("task-1", attempts=1, max_attempts=3, error="timeout")
        update_call = table_mock.update.call_args
        assert update_call[0][0]["status"] == "pending"
        assert "next_retry_at" in update_call[0][0]

    @pytest.mark.asyncio
    async def test_mark_failed_permanently(self):
        db, table_mock, _ = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        await repo.mark_failed("task-1", attempts=3, max_attempts=3, error="fatal")
        update_call = table_mock.update.call_args
        assert update_call[0][0]["status"] == "failed"

    @pytest.mark.asyncio
    async def test_mark_failed_no_retry_flag(self):
        db, table_mock, _ = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        await repo.mark_failed(
            "task-1", attempts=1, max_attempts=3, error="bad", retry=False
        )
        update_call = table_mock.update.call_args
        assert update_call[0][0]["status"] == "failed"

    @pytest.mark.asyncio
    async def test_mark_failed_sanitizes_error(self):
        db, table_mock, _ = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        await repo.mark_failed(
            "task-1", attempts=3, max_attempts=3,
            error="Bearer sk-12345 failed",
        )
        update_call = table_mock.update.call_args
        assert "sk-12345" not in update_call[0][0]["error_message"]
        assert "***" in update_call[0][0]["error_message"]


class TestCreateIfNotExists:
    @pytest.mark.asyncio
    async def test_create_returns_task_id(self):
        db, _, rpc_mock = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        rpc_mock.execute = AsyncMock(return_value=MagicMock(data="new-task-id"))
        result = await repo.create_if_not_exists("blog-1", "full_scrape", 1)
        assert result == "new-task-id"

    @pytest.mark.asyncio
    async def test_create_returns_none_if_exists(self):
        db, _, rpc_mock = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        rpc_mock.execute = AsyncMock(return_value=MagicMock(data=None))
        result = await repo.create_if_not_exists("blog-1", "full_scrape", 1)
        assert result is None

    @pytest.mark.asyncio
    async def test_create_with_payload(self):
        db, _, rpc_mock = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        rpc_mock.execute = AsyncMock(return_value=MagicMock(data="tid"))
        await repo.create_if_not_exists(
            None, "discover", 2, payload={"hashtag": "test"}
        )
        db.rpc.assert_called_once_with(
            "create_task_if_not_exists",
            {
                "p_blog_id": None,
                "p_task_type": "discover",
                "p_priority": 2,
                "p_payload": {"hashtag": "test"},
            },
        )


class TestFetchPending:
    @pytest.mark.asyncio
    async def test_fetch_pending_returns_tasks(self):
        db, table_mock, _ = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        tasks = [{"id": "t1", "task_type": "full_scrape", "status": "pending"}]
        table_mock.execute = AsyncMock(return_value=MagicMock(data=tasks))
        result = await repo.fetch_pending(limit=5)
        assert result == tasks

    @pytest.mark.asyncio
    async def test_fetch_pending_empty(self):
        db, table_mock, _ = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        table_mock.execute = AsyncMock(return_value=MagicMock(data=[]))
        result = await repo.fetch_pending()
        assert result == []


class TestRecoverStuck:
    @pytest.mark.asyncio
    async def test_recover_no_stuck_tasks(self):
        db, table_mock, _ = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        table_mock.execute = AsyncMock(return_value=MagicMock(data=[]))
        result = await repo.recover_stuck()
        assert result == 0

    @pytest.mark.asyncio
    async def test_recover_stuck_retryable(self):
        db, table_mock, _ = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        stuck_tasks = [
            {"id": "t1", "task_type": "full_scrape", "attempts": 1, "max_attempts": 3}
        ]
        # Первый вызов execute — select full_scrape/discover/pre_filter
        # Второй вызов execute — select ai_analysis (пустой)
        # Третий вызов execute — update (recover)
        table_mock.execute = AsyncMock(
            side_effect=[
                MagicMock(data=stuck_tasks),
                MagicMock(data=[]),
                MagicMock(data=[]),
            ]
        )
        result = await repo.recover_stuck()
        assert result == 1

    @pytest.mark.asyncio
    async def test_recover_includes_pre_filter_tasks(self):
        db, table_mock, _ = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        stuck_tasks = [
            {"id": "pf-1", "task_type": "pre_filter", "attempts": 1, "max_attempts": 3}
        ]
        table_mock.execute = AsyncMock(
            side_effect=[
                MagicMock(data=stuck_tasks),
                MagicMock(data=[]),
                MagicMock(data=[]),
            ]
        )
        result = await repo.recover_stuck()
        assert result == 1
        assert "pre_filter" in table_mock.in_.call_args[0][1]

    @pytest.mark.asyncio
    async def test_recover_stuck_exhausted(self):
        db, table_mock, _ = _mock_supabase()
        repo = SupabaseTaskRepository(db)
        stuck_tasks = [
            {"id": "t1", "task_type": "full_scrape", "attempts": 3, "max_attempts": 3}
        ]
        table_mock.execute = AsyncMock(
            side_effect=[
                MagicMock(data=stuck_tasks),
                MagicMock(data=[]),
                MagicMock(data=[]),
            ]
        )
        result = await repo.recover_stuck()
        # Задача с исчерпанными попытками помечается failed, не recovered
        assert result == 0
