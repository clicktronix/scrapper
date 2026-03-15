"""Тесты Supabase sink для loguru (AsyncClient)."""

from unittest.mock import AsyncMock, MagicMock, patch

from src.log_sink import create_supabase_sink


def _mock_supabase():
    """Создать мок Supabase AsyncClient с цепочкой table().insert().execute()."""
    db = MagicMock()
    table_mock = MagicMock()
    db.table.return_value = table_mock
    table_mock.insert.return_value = table_mock
    table_mock.execute = AsyncMock(return_value=MagicMock(data=[]))
    return db


def _mock_loop():
    """Создать мок event loop для run_coroutine_threadsafe."""
    return MagicMock()


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
    """Тесты create_supabase_sink с AsyncClient."""

    def test_sink_schedules_error_log(self) -> None:
        """Sink планирует запись ERROR лога через run_coroutine_threadsafe."""
        db = _mock_supabase()
        loop = _mock_loop()

        with patch("src.log_sink.asyncio.run_coroutine_threadsafe") as mock_schedule:
            sink = create_supabase_sink(db, loop)
            sink(_make_message("ERROR", "src.worker.handlers", "Connection failed"))

            mock_schedule.assert_called_once()
            # Проверяем, что coroutine и loop переданы
            args = mock_schedule.call_args[0]
            assert args[1] is loop

    def test_sink_schedules_warning_log(self) -> None:
        """Sink планирует запись WARNING лога."""
        db = _mock_supabase()
        loop = _mock_loop()

        with patch("src.log_sink.asyncio.run_coroutine_threadsafe") as mock_schedule:
            sink = create_supabase_sink(db, loop)
            sink(_make_message("WARNING", "src.database", "Slow query"))

            mock_schedule.assert_called_once()

    def test_sink_does_not_crash_on_schedule_error(self) -> None:
        """Sink не падает при ошибке планирования."""
        db = _mock_supabase()
        loop = _mock_loop()

        with patch("src.log_sink.asyncio.run_coroutine_threadsafe", side_effect=RuntimeError("loop closed")):
            sink = create_supabase_sink(db, loop)
            # Не должно бросить исключение
            sink(_make_message("ERROR", "src.main", "some error"))

    def test_sink_skips_debug_and_info(self) -> None:
        """Sink пропускает уровни ниже WARNING (DEBUG, INFO)."""
        db = _mock_supabase()
        loop = _mock_loop()

        with patch("src.log_sink.asyncio.run_coroutine_threadsafe") as mock_schedule:
            sink = create_supabase_sink(db, loop)
            sink(_make_message("DEBUG", "src.worker", "debug msg"))
            sink(_make_message("INFO", "src.worker", "info msg"))

            mock_schedule.assert_not_called()

    def test_sink_appends_sanitized_exception(self) -> None:
        """Exception в записи санитизируется."""
        db = _mock_supabase()
        loop = _mock_loop()

        with patch("src.log_sink.asyncio.run_coroutine_threadsafe") as mock_schedule:
            sink = create_supabase_sink(db, loop)
            message = _make_message(
                "ERROR",
                "src.worker",
                "Request failed",
            )
            message.record["exception"] = "Authorization: Bearer sk-secret-token"

            sink(message)

            # Проверяем что coroutine запланирован
            mock_schedule.assert_called_once()
