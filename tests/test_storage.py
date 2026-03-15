"""Тесты Supabase Storage для Instagram-сессий."""
import json
from unittest.mock import AsyncMock, MagicMock


class TestLoadSession:
    """Тесты загрузки сессии."""

    async def test_load_existing_session(self) -> None:
        from src.storage import load_session

        db = MagicMock()
        session_data = {"uuids": {"phone_id": "abc"}, "cookies": {}}
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.download = AsyncMock(return_value=json.dumps(session_data).encode())

        result = await load_session(db, "account1")
        assert result == session_data
        db.storage.from_.assert_called_with("instagram-sessions")

    async def test_load_missing_session(self) -> None:
        from src.storage import load_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.download = AsyncMock(side_effect=Exception("Not found"))

        result = await load_session(db, "missing")
        assert result is None

    async def test_load_corrupted_json(self) -> None:
        """Повреждённый JSON -> None (не краш)."""
        from src.storage import load_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.download = AsyncMock(return_value=b"not valid json {")

        result = await load_session(db, "corrupted")
        assert result is None

    async def test_load_empty_file(self) -> None:
        """Пустой файл -> None."""
        from src.storage import load_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.download = AsyncMock(return_value=b"")

        result = await load_session(db, "empty")
        assert result is None

    async def test_correct_filename_used(self) -> None:
        """Имя файла = account_name.json."""
        from src.storage import load_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.download = AsyncMock(return_value=b'{"ok": true}')

        await load_session(db, "my_account")
        mock_bucket.download.assert_called_with("my_account.json")

    async def test_rejects_unsafe_account_name(self) -> None:
        from src.storage import load_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.download = AsyncMock()

        result = await load_session(db, "../evil")
        assert result is None
        mock_bucket.download.assert_not_called()


class TestLoadSessionNonDictJson:
    """BUG-11: load_session должен возвращать None для не-dict JSON."""

    async def test_json_list_returns_none(self) -> None:
        """JSON массив [1,2,3] -> None (не list)."""
        from src.storage import load_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.download = AsyncMock(return_value=b'[1, 2, 3]')

        result = await load_session(db, "bad_session")
        assert result is None

    async def test_json_string_returns_none(self) -> None:
        """JSON строка "hello" -> None (не str)."""
        from src.storage import load_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.download = AsyncMock(return_value=b'"hello"')

        result = await load_session(db, "bad_session")
        assert result is None

    async def test_json_number_returns_none(self) -> None:
        """JSON число 42 -> None (не int)."""
        from src.storage import load_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.download = AsyncMock(return_value=b'42')

        result = await load_session(db, "bad_session")
        assert result is None

    async def test_json_true_returns_none(self) -> None:
        """JSON true -> None (не bool)."""
        from src.storage import load_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.download = AsyncMock(return_value=b'true')

        result = await load_session(db, "bad_session")
        assert result is None

    async def test_json_null_returns_none(self) -> None:
        """JSON null -> None."""
        from src.storage import load_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.download = AsyncMock(return_value=b'null')

        result = await load_session(db, "bad_session")
        assert result is None

    async def test_valid_dict_still_works(self) -> None:
        """Валидный JSON dict по-прежнему работает."""
        from src.storage import load_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.download = AsyncMock(return_value=b'{"key": "value"}')

        result = await load_session(db, "good_session")
        assert result == {"key": "value"}


class TestSaveSession:
    """Тесты сохранения сессии."""

    async def test_save_session(self) -> None:
        from src.storage import save_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.upload = AsyncMock()
        settings = {"uuids": {"phone_id": "abc"}, "cookies": {}}

        await save_session(db, "account1", settings)
        db.storage.from_.assert_called_with("instagram-sessions")
        mock_bucket.upload.assert_called_once()

    async def test_save_correct_filename(self) -> None:
        """Имя файла = account_name.json."""
        from src.storage import save_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.upload = AsyncMock()
        settings = {"session": "data"}

        await save_session(db, "my_account", settings)
        call_args = mock_bucket.upload.call_args
        assert call_args[0][0] == "my_account.json"

    async def test_save_upsert_option(self) -> None:
        """Передаётся upsert=true для перезаписи."""
        from src.storage import save_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.upload = AsyncMock()
        settings = {"session": "data"}

        await save_session(db, "account1", settings)
        call_args = mock_bucket.upload.call_args
        # Третий позиционный аргумент — опции
        options = call_args[0][2]
        assert options["upsert"] == "true"

    async def test_save_error_doesnt_raise(self) -> None:
        """Ошибка сохранения логируется, не пробрасывается."""
        from src.storage import save_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.upload = AsyncMock(side_effect=RuntimeError("Upload failed"))

        # Не должен бросить исключение
        await save_session(db, "account1", {"session": "data"})

    async def test_save_rejects_unsafe_account_name(self) -> None:
        from src.storage import save_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.upload = AsyncMock()

        await save_session(db, "../evil", {"session": "data"})
        mock_bucket.upload.assert_not_called()

    async def test_save_skips_oversized_payload(self) -> None:
        from src.storage import MAX_SESSION_SIZE_BYTES, save_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.upload = AsyncMock()

        huge_value = "x" * (MAX_SESSION_SIZE_BYTES + 1024)
        await save_session(db, "account1", {"payload": huge_value})
        mock_bucket.upload.assert_not_called()

    async def test_save_non_serializable_doesnt_raise(self) -> None:
        """Несериализуемые данные (set, datetime) не роняют save_session."""
        from datetime import datetime

        from src.storage import save_session

        db = MagicMock()
        mock_bucket = MagicMock()
        db.storage.from_.return_value = mock_bucket
        mock_bucket.upload = AsyncMock()
        # set и datetime не сериализуются json.dumps -> TypeError
        settings = {"data": {1, 2, 3}, "ts": datetime.now()}

        # Не должен бросить исключение
        await save_session(db, "account1", settings)
        # upload не должен быть вызван (json.dumps упал раньше)
        mock_bucket.upload.assert_not_called()
