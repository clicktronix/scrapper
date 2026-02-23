"""Тесты _find_or_create_blog — поиск/создание блога с защитой от race condition."""
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from postgrest.exceptions import APIError as PostgrestAPIError


class TestFindOrCreateBlog:
    """Тесты для _find_or_create_blog."""

    @pytest.mark.asyncio
    async def test_returns_existing_blog(self) -> None:
        """Блог уже существует → возвращает его id."""
        from src.api.app import _find_or_create_blog

        db = MagicMock()
        with patch("src.api.app.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = MagicMock(data=[{"id": "blog-existing"}])

            result = await _find_or_create_blog(db, "test_user")

        assert result == "blog-existing"

    @pytest.mark.asyncio
    async def test_creates_new_person_and_blog(self) -> None:
        """Блог не существует → создаёт person + blog."""
        from src.api.app import _find_or_create_blog

        db = MagicMock()
        with patch("src.api.app.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = [
                MagicMock(data=[]),  # blog не найден
                MagicMock(data=[{"id": "person-1"}]),  # insert person
                MagicMock(data=[{"id": "blog-new"}]),  # insert blog
            ]

            result = await _find_or_create_blog(db, "new_user")

        assert result == "blog-new"

    @pytest.mark.asyncio
    async def test_race_condition_retry(self) -> None:
        """Race condition: INSERT blog падает → повторный SELECT находит блог."""
        from src.api.app import _find_or_create_blog

        db = MagicMock()
        with patch("src.api.app.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = [
                MagicMock(data=[]),  # blog не найден
                MagicMock(data=[{"id": "person-1"}]),  # insert person
                PostgrestAPIError({  # insert blog — конфликт
                    "message": "duplicate key", "code": "23505",
                    "details": "", "hint": "",
                }),
                MagicMock(data=[{"id": "blog-concurrent"}]),  # повторный SELECT
            ]

            result = await _find_or_create_blog(db, "concurrent_user")

        assert result == "blog-concurrent"

    @pytest.mark.asyncio
    async def test_race_condition_reraise_if_not_found(self) -> None:
        """Race condition: INSERT падает (PostgrestAPIError), и повторный SELECT тоже пуст → reraise."""
        from src.api.app import _find_or_create_blog

        db = MagicMock()
        with patch("src.api.app.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = [
                MagicMock(data=[]),  # blog не найден
                MagicMock(data=[{"id": "person-1"}]),  # insert person
                PostgrestAPIError({  # insert blog — ошибка
                    "message": "unknown error", "code": "23505",
                    "details": "", "hint": "",
                }),
                MagicMock(data=[]),  # повторный SELECT — тоже пусто
            ]

            with pytest.raises(PostgrestAPIError):
                await _find_or_create_blog(db, "broken_user")

    @pytest.mark.asyncio
    async def test_normalizes_username_to_lowercase(self) -> None:
        """Username нормализуется: trim + lstrip('@') + lowercase."""
        from src.api.app import _find_or_create_blog

        db = MagicMock()
        table_mock = MagicMock()
        table_mock.select.return_value = table_mock
        table_mock.eq.return_value = table_mock
        table_mock.execute.return_value = MagicMock(data=[{"id": "blog-existing"}])
        db.table.return_value = table_mock

        async def passthrough(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch("src.api.app.run_in_thread", side_effect=passthrough):
            result = await _find_or_create_blog(db, "  @TeSt_User  ")

        assert result == "blog-existing"
        assert call("username", "test_user") in table_mock.eq.call_args_list
