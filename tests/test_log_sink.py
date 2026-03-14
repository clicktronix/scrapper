"""Тесты Supabase sink для loguru."""

from unittest.mock import MagicMock

from src.log_sink import create_supabase_sink


def _mock_supabase():
    """Создать мок Supabase client с цепочкой table().insert().execute()."""
    db = MagicMock()
    table_mock = MagicMock()
    db.table.return_value = table_mock
    table_mock.insert.return_value = table_mock
    table_mock.execute.return_value = MagicMock(data=[])
    return db


_LEVEL_NO = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}


def _make_message(level_name: str, module: str = "src.worker", text: str = "test msg"):
    """Создать мок loguru message с record."""
    message = MagicMock()
    level_mock = MagicMock()
    level_mock.name = level_name
    level_mock.no = _LEVEL_NO.get(level_name, 0)
    message.record = {
        "level": level_mock,
        "name": module,
        "message": text,
        "exception": None,
    }
    return message


class TestSupabaseSink:
    """Тесты create_supabase_sink."""

    def test_sink_writes_error_log(self) -> None:
        """Sink записывает ERROR лог в scrape_logs."""
        db = _mock_supabase()
        sink = create_supabase_sink(db)

        sink(_make_message("ERROR", "src.worker.handlers", "Connection failed"))

        db.table.assert_called_with("scrape_logs")
        db.table.return_value.insert.assert_called_once_with({
            "level": "ERROR",
            "module": "src.worker.handlers",
            "message": "Connection failed",
        })
        db.table.return_value.execute.assert_called_once()

    def test_sink_writes_warning_log(self) -> None:
        """Sink записывает WARNING лог."""
        db = _mock_supabase()
        sink = create_supabase_sink(db)

        sink(_make_message("WARNING", "src.database", "Slow query"))

        db.table.return_value.insert.assert_called_once_with({
            "level": "WARNING",
            "module": "src.database",
            "message": "Slow query",
        })

    def test_sink_does_not_crash_on_db_error(self) -> None:
        """Sink не падает при ошибке записи в БД."""
        db = _mock_supabase()
        db.table.return_value.insert.return_value.execute.side_effect = Exception(
            "connection refused"
        )
        sink = create_supabase_sink(db)

        # Не должно бросить исключение
        sink(_make_message("ERROR", "src.main", "some error"))

    def test_sink_passes_correct_fields(self) -> None:
        """Sink передаёт правильные поля level, module, message."""
        db = _mock_supabase()
        sink = create_supabase_sink(db)

        sink(_make_message("WARNING", "src.ai.batch_api", "Batch timeout exceeded"))

        call_args = db.table.return_value.insert.call_args[0][0]
        assert call_args["level"] == "WARNING"
        assert call_args["module"] == "src.ai.batch_api"
        assert call_args["message"] == "Batch timeout exceeded"

    def test_sink_skips_debug_and_info(self) -> None:
        """Sink пропускает уровни ниже WARNING (DEBUG, INFO)."""
        db = _mock_supabase()
        sink = create_supabase_sink(db)

        sink(_make_message("DEBUG", "src.worker", "debug msg"))
        sink(_make_message("INFO", "src.worker", "info msg"))

        db.table.return_value.insert.assert_not_called()

    def test_sink_appends_sanitized_exception(self) -> None:
        db = _mock_supabase()
        sink = create_supabase_sink(db)
        message = _make_message(
            "ERROR",
            "src.worker",
            "Request failed",
        )
        message.record["exception"] = "Authorization: Bearer sk-secret-token"

        sink(message)

        payload = db.table.return_value.insert.call_args[0][0]
        assert "sk-secret-token" not in payload["message"]
        assert "Bearer ***" in payload["message"]
