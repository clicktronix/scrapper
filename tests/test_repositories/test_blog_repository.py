"""Тесты SupabaseBlogRepository."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.repositories.blog_repository import SupabaseBlogRepository


def _mock_supabase():
    """Создать мок Supabase AsyncClient с цепочкой вызовов."""
    db = MagicMock()
    table_mock = MagicMock()
    db.table.return_value = table_mock
    table_mock.update.return_value = table_mock
    table_mock.upsert.return_value = table_mock
    table_mock.select.return_value = table_mock
    table_mock.delete.return_value = table_mock
    table_mock.eq.return_value = table_mock
    table_mock.gt.return_value = table_mock
    table_mock.limit.return_value = table_mock
    table_mock.execute = AsyncMock(return_value=MagicMock(data=[]))
    return db, table_mock


class TestIsFresh:
    @pytest.mark.asyncio
    async def test_blog_is_fresh(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        table_mock.execute = AsyncMock(
            return_value=MagicMock(data=[{"scraped_at": "2026-03-08T00:00:00+00:00"}])
        )
        result = await repo.is_fresh("blog-1", min_days=7)
        assert result is True

    @pytest.mark.asyncio
    async def test_blog_is_not_fresh(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        table_mock.execute = AsyncMock(return_value=MagicMock(data=[]))
        result = await repo.is_fresh("blog-1", min_days=7)
        assert result is False

    @pytest.mark.asyncio
    async def test_is_fresh_queries_correct_table(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        table_mock.execute = AsyncMock(return_value=MagicMock(data=[]))
        await repo.is_fresh("blog-1", min_days=7)
        db.table.assert_called_with("blogs")


class TestUpsert:
    @pytest.mark.asyncio
    async def test_upsert_blog(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        await repo.upsert("blog-1", {"username": "test", "followers_count": 100})
        db.table.assert_called_with("blogs")
        table_mock.update.assert_called_once_with(
            {"username": "test", "followers_count": 100}
        )

    @pytest.mark.asyncio
    async def test_upsert_blog_with_eq(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        await repo.upsert("blog-1", {"username": "test"})
        table_mock.eq.assert_called_with("id", "blog-1")


class TestUpsertPosts:
    @pytest.mark.asyncio
    async def test_upsert_posts_empty(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        await repo.upsert_posts("blog-1", [])
        table_mock.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_upsert_posts_with_data(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        posts = [{"platform_id": "p1", "caption": "hello"}]
        await repo.upsert_posts("blog-1", posts)
        db.table.assert_called_with("blog_posts")
        upsert_call = table_mock.upsert.call_args
        assert upsert_call[0][0] == [
            {"platform_id": "p1", "caption": "hello", "blog_id": "blog-1"}
        ]

    @pytest.mark.asyncio
    async def test_upsert_posts_conflict_key(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        await repo.upsert_posts("blog-1", [{"platform_id": "p1"}])
        upsert_call = table_mock.upsert.call_args
        assert upsert_call[1]["on_conflict"] == "blog_id,platform_id"


class TestUpsertHighlights:
    @pytest.mark.asyncio
    async def test_upsert_highlights_empty(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        await repo.upsert_highlights("blog-1", [])
        table_mock.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_upsert_highlights_with_data(self):
        db, _table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        highlights = [{"platform_id": "h1", "title": "Story"}]
        await repo.upsert_highlights("blog-1", highlights)
        db.table.assert_called_with("blog_highlights")

    @pytest.mark.asyncio
    async def test_upsert_highlights_adds_blog_id(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        await repo.upsert_highlights("blog-1", [{"platform_id": "h1"}])
        upsert_call = table_mock.upsert.call_args
        assert upsert_call[0][0] == [
            {"platform_id": "h1", "blog_id": "blog-1"}
        ]


class TestCleanupOrphanPerson:
    @pytest.mark.asyncio
    async def test_cleanup_no_blogs_deletes(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        # Первый execute — select (нет блогов), второй — delete
        table_mock.execute = AsyncMock(
            side_effect=[
                MagicMock(data=[]),
                MagicMock(data=[]),
            ]
        )
        await repo.cleanup_orphan_person("person-1")
        assert table_mock.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_cleanup_has_blogs_no_delete(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        table_mock.execute = AsyncMock(
            return_value=MagicMock(data=[{"id": "blog-1"}])
        )
        await repo.cleanup_orphan_person("person-1")
        # Только select, без delete
        assert table_mock.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_cleanup_exception_swallowed(self):
        db, table_mock = _mock_supabase()
        repo = SupabaseBlogRepository(db)
        table_mock.execute = AsyncMock(side_effect=Exception("DB error"))
        # Не должно бросить исключение
        await repo.cleanup_orphan_person("person-1")
