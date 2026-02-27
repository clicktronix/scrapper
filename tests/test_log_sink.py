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


def _make_message(level_name: str, module: str = "src.worker", text: str = "test msg"):
    """Создать мок loguru message с record."""
    message = MagicMock()
    message.record = {
        "level": MagicMock(name=level_name),
        "name": module,
        "message": text,
    }
    # loguru level.name — property, MagicMock(name=...) не работает как .name
    message.record["level"].name = level_name
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

        sink(_make_message("WARNING", "src.ai.batch", "Batch timeout exceeded"))

        call_args = db.table.return_value.insert.call_args[0][0]
        assert call_args["level"] == "WARNING"
        assert call_args["module"] == "src.ai.batch"
        assert call_args["message"] == "Batch timeout exceeded"
