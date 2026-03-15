"""Тесты find_or_create_blog — поиск/создание блога с защитой от race condition."""
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from postgrest.exceptions import APIError as PostgrestAPIError

from tests.test_api.conftest import make_db_mock


class TestFindOrCreateBlog:
    """Тесты для find_or_create_blog."""

    @pytest.mark.asyncio
    async def test_returns_existing_blog(self) -> None:
        """Блог уже существует — возвращает его id."""
        from src.api.services import find_or_create_blog

        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(data=[{"id": "blog-existing"}])

        result = await find_or_create_blog(db, "test_user")
        assert result == "blog-existing"

    @pytest.mark.asyncio
    async def test_creates_new_person_and_blog(self) -> None:
        """Блог не существует — создаёт person + blog."""
        from src.api.services import find_or_create_blog

        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.side_effect = [
            MagicMock(data=[]),  # blog не найден
            MagicMock(data=[{"id": "person-1"}]),  # insert person
            MagicMock(data=[{"id": "blog-new"}]),  # insert blog
        ]

        result = await find_or_create_blog(db, "new_user")
        assert result == "blog-new"

    @pytest.mark.asyncio
    async def test_race_condition_retry(self) -> None:
        """Race condition: INSERT blog падает — повторный SELECT находит блог."""
        from src.api.services import find_or_create_blog

        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.side_effect = [
            MagicMock(data=[]),  # blog не найден
            MagicMock(data=[{"id": "person-1"}]),  # insert person
            PostgrestAPIError({  # insert blog — конфликт
                "message": "duplicate key", "code": "23505",
                "details": "", "hint": "",
            }),
            MagicMock(data=[{"id": "blog-concurrent"}]),  # повторный SELECT
        ]

        with patch("src.api.services.cleanup_orphan_person", new_callable=AsyncMock):
            result = await find_or_create_blog(db, "concurrent_user")

        assert result == "blog-concurrent"

    @pytest.mark.asyncio
    async def test_race_condition_reraise_if_not_found(self) -> None:
        """Race condition: INSERT падает (PostgrestAPIError), повторный SELECT тоже пуст — reraise."""
        from src.api.services import find_or_create_blog

        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.side_effect = [
            MagicMock(data=[]),  # blog не найден
            MagicMock(data=[{"id": "person-1"}]),  # insert person
            PostgrestAPIError({  # insert blog — ошибка
                "message": "unknown error", "code": "23505",
                "details": "", "hint": "",
            }),
            MagicMock(data=[]),  # повторный SELECT — тоже пусто
        ]

        with patch("src.api.services.cleanup_orphan_person", new_callable=AsyncMock), \
             pytest.raises(PostgrestAPIError):
            await find_or_create_blog(db, "broken_user")

    @pytest.mark.asyncio
    async def test_non_unique_postgrest_error_not_treated_as_race(self) -> None:
        """Не-unique PostgREST ошибка пробрасывается без retry SELECT."""
        from src.api.services import find_or_create_blog

        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.side_effect = [
            MagicMock(data=[]),  # blog не найден
            MagicMock(data=[{"id": "person-1"}]),  # insert person
            PostgrestAPIError({
                "message": "permission denied", "code": "42501",
                "details": "", "hint": "",
            }),
        ]

        with patch("src.api.services.cleanup_orphan_person", new_callable=AsyncMock):
            with pytest.raises(PostgrestAPIError):
                await find_or_create_blog(db, "broken_permissions")
            assert builder.execute.await_count == 3

    @pytest.mark.asyncio
    async def test_normalizes_username_to_lowercase(self) -> None:
        """Username нормализуется: trim + lstrip('@') + lowercase."""
        from src.api.services import find_or_create_blog

        db = make_db_mock()
        builder = db.table.return_value
        builder.execute.return_value = MagicMock(data=[{"id": "blog-existing"}])

        result = await find_or_create_blog(db, "  @TeSt_User  ")

        assert result == "blog-existing"
        assert call("username", "test_user") in builder.eq.call_args_list
