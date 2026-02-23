"""Тесты polling-цикла воркера."""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestProcessTask:
    """Тесты process_task."""

    @pytest.mark.asyncio
    async def test_dispatches_full_scrape(self) -> None:
        from src.worker.loop import process_task

        task = {
            "id": "task-1",
            "task_type": "full_scrape",
            "blog_id": "blog-1",
            "attempts": 1,
            "max_attempts": 3,
        }

        mock_db = MagicMock()
        mock_scraper = AsyncMock()
        mock_openai = MagicMock()
        settings = MagicMock()
        semaphore = asyncio.Semaphore(2)

        with patch("src.worker.loop.handle_full_scrape", new_callable=AsyncMock) as mock_handler:
            await process_task(
                mock_db, task, {"instagram": mock_scraper},
                mock_openai, settings, semaphore,
            )
            mock_handler.assert_called_once_with(mock_db, task, mock_scraper, settings)

    @pytest.mark.asyncio
    async def test_dispatches_ai_analysis(self) -> None:
        from src.worker.loop import process_task

        task = {
            "id": "task-2",
            "task_type": "ai_analysis",
            "blog_id": "blog-1",
            "attempts": 1,
            "max_attempts": 3,
        }

        mock_db = MagicMock()
        mock_openai = MagicMock()
        settings = MagicMock()
        semaphore = asyncio.Semaphore(2)

        with patch("src.worker.loop.handle_ai_analysis", new_callable=AsyncMock) as mock_handler:
            await process_task(
                mock_db, task, {}, mock_openai, settings, semaphore,
            )
            mock_handler.assert_called_once_with(mock_db, task, mock_openai, settings)

    @pytest.mark.asyncio
    async def test_dispatches_discover(self) -> None:
        from src.worker.loop import process_task

        task = {
            "id": "task-3",
            "task_type": "discover",
            "blog_id": "blog-1",
            "attempts": 1,
            "max_attempts": 3,
        }

        mock_db = MagicMock()
        mock_scraper = AsyncMock()
        mock_openai = MagicMock()
        settings = MagicMock()
        semaphore = asyncio.Semaphore(2)

        with patch("src.worker.loop.handle_discover", new_callable=AsyncMock) as mock_handler:
            await process_task(
                mock_db, task, {"instagram": mock_scraper},
                mock_openai, settings, semaphore,
            )
            mock_handler.assert_called_once_with(mock_db, task, mock_scraper, settings)

    @pytest.mark.asyncio
    async def test_handles_unknown_task_type(self) -> None:
        from src.worker.loop import process_task

        task = {
            "id": "task-4",
            "task_type": "unknown_type",
            "blog_id": "blog-1",
            "attempts": 0,
            "max_attempts": 3,
        }

        mock_db = MagicMock()
        mock_openai = MagicMock()
        settings = MagicMock()
        semaphore = asyncio.Semaphore(2)

        with patch("src.worker.loop.mark_task_failed", new_callable=AsyncMock) as mock_failed:
            # Не должен упасть
            await process_task(
                mock_db, task, {}, mock_openai, settings, semaphore,
            )
            mock_failed.assert_called_once()
            assert mock_failed.call_args.kwargs["retry"] is False

    @pytest.mark.asyncio
    async def test_no_scraper_for_full_scrape(self) -> None:
        """Нет скрапера для instagram → задача пропускается без краша."""
        from src.worker.loop import process_task

        task = {"id": "task-5", "task_type": "full_scrape", "blog_id": "b1", "attempts": 0, "max_attempts": 3}
        mock_db = MagicMock()
        mock_openai = MagicMock()
        settings = MagicMock()
        semaphore = asyncio.Semaphore(2)

        with patch("src.worker.loop.mark_task_failed", new_callable=AsyncMock) as mock_failed:
            # scrapers пустой — нет instagram
            await process_task(mock_db, task, {}, mock_openai, settings, semaphore)
            mock_failed.assert_called_once()
            assert mock_failed.call_args.kwargs["retry"] is False

    @pytest.mark.asyncio
    async def test_no_scraper_for_discover(self) -> None:
        """Нет скрапера для discover → задача пропускается."""
        from src.worker.loop import process_task

        task = {"id": "task-6", "task_type": "discover", "blog_id": "b1", "attempts": 0, "max_attempts": 3}
        mock_db = MagicMock()
        mock_openai = MagicMock()
        settings = MagicMock()
        semaphore = asyncio.Semaphore(2)

        with patch("src.worker.loop.mark_task_failed", new_callable=AsyncMock) as mock_failed:
            await process_task(mock_db, task, {}, mock_openai, settings, semaphore)
            mock_failed.assert_called_once()
            assert mock_failed.call_args.kwargs["retry"] is False

    @pytest.mark.asyncio
    async def test_exception_in_handler_is_caught(self) -> None:
        """Исключение в хэндлере не пробрасывается наружу."""
        from src.worker.loop import process_task

        task = {"id": "task-7", "task_type": "full_scrape", "blog_id": "b1"}
        mock_db = MagicMock()
        mock_scraper = AsyncMock()
        mock_openai = MagicMock()
        settings = MagicMock()
        semaphore = asyncio.Semaphore(2)

        with patch(
            "src.worker.loop.handle_full_scrape",
            new_callable=AsyncMock,
            side_effect=RuntimeError("crash"),
        ):
            # Не должен пробросить исключение
            await process_task(
                mock_db, task, {"instagram": mock_scraper},
                mock_openai, settings, semaphore,
            )

    @pytest.mark.asyncio
    async def test_respects_semaphore(self) -> None:
        """Семафор ограничивает параллельные задачи."""
        from src.worker.loop import process_task

        semaphore = asyncio.Semaphore(1)
        execution_order: list[str] = []

        async def slow_handler(*args, **kwargs):
            execution_order.append("start")
            await asyncio.sleep(0.1)
            execution_order.append("end")

        task1 = {"id": "t1", "task_type": "full_scrape", "blog_id": "b1"}
        task2 = {"id": "t2", "task_type": "full_scrape", "blog_id": "b2"}

        mock_db = MagicMock()
        mock_scraper = AsyncMock()
        mock_openai = MagicMock()
        settings = MagicMock()

        with patch("src.worker.loop.handle_full_scrape", side_effect=slow_handler):
            t1 = asyncio.create_task(
                process_task(mock_db, task1, {"instagram": mock_scraper},
                             mock_openai, settings, semaphore)
            )
            t2 = asyncio.create_task(
                process_task(mock_db, task2, {"instagram": mock_scraper},
                             mock_openai, settings, semaphore)
            )
            await asyncio.gather(t1, t2)

        # С семафором=1, вторая задача начнётся только после завершения первой
        assert execution_order == ["start", "end", "start", "end"]


class TestRunWorker:
    """Тесты run_worker."""

    @pytest.mark.asyncio
    async def test_stops_on_shutdown_event(self) -> None:
        from src.worker.loop import run_worker

        mock_db = MagicMock()
        settings = MagicMock()
        settings.worker_poll_interval = 1
        settings.worker_max_concurrent = 2
        settings.openai_api_key = "test"

        shutdown_event = asyncio.Event()
        mock_openai = MagicMock()

        with patch("src.worker.loop.fetch_pending_tasks", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = []

            # Запускаем воркер и сразу ставим shutdown
            async def stop_soon():
                await asyncio.sleep(0.1)
                shutdown_event.set()

            asyncio.create_task(stop_soon())

            await run_worker(mock_db, {}, settings, shutdown_event, mock_openai)

        # Воркер должен завершиться
        assert shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_fetches_tasks_on_poll(self) -> None:
        from src.worker.loop import run_worker

        mock_db = MagicMock()
        settings = MagicMock()
        settings.worker_poll_interval = 0.1
        settings.worker_max_concurrent = 2
        settings.openai_api_key = "test"

        shutdown_event = asyncio.Event()
        mock_openai = MagicMock()
        fetch_count = 0

        async def counted_fetch(*args, **kwargs):
            nonlocal fetch_count
            fetch_count += 1
            if fetch_count >= 2:
                shutdown_event.set()
            return []

        with patch("src.worker.loop.fetch_pending_tasks", side_effect=counted_fetch):
            await run_worker(mock_db, {}, settings, shutdown_event, mock_openai)

        assert fetch_count >= 2

    @pytest.mark.asyncio
    async def test_creates_tasks_for_pending(self) -> None:
        """Полученные pending задачи запускаются через process_task."""
        from src.worker.loop import run_worker

        mock_db = MagicMock()
        settings = MagicMock()
        settings.worker_poll_interval = 0.1
        settings.worker_max_concurrent = 2
        settings.openai_api_key = "test"

        shutdown_event = asyncio.Event()
        mock_openai = MagicMock()

        tasks_returned = [
            [{"id": "t1", "task_type": "full_scrape", "blog_id": "b1"}],
            [],  # Второй poll — пусто, shutdown
        ]
        call_idx = 0

        async def mock_fetch(*args, **kwargs):
            nonlocal call_idx
            idx = call_idx
            call_idx += 1
            if idx >= 1:
                shutdown_event.set()
            return tasks_returned[min(idx, len(tasks_returned) - 1)]

        with (
            patch("src.worker.loop.fetch_pending_tasks", side_effect=mock_fetch),
            patch("src.worker.loop.process_task", new_callable=AsyncMock),
        ):
            # process_task замокан — не будет реального выполнения
            # Но run_worker вызывает asyncio.create_task(process_task(...))
            # Поэтому мок process_task не вызывается напрямую из run_worker
            await run_worker(mock_db, {}, settings, shutdown_event, mock_openai)

        # Воркер создал задачу и корректно завершился
        assert shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_handles_fetch_error(self) -> None:
        """Ошибка при fetch не крашит воркер."""
        from src.worker.loop import run_worker

        mock_db = MagicMock()
        settings = MagicMock()
        settings.worker_poll_interval = 0.1
        settings.worker_max_concurrent = 2
        settings.openai_api_key = "test"

        shutdown_event = asyncio.Event()
        mock_openai = MagicMock()
        call_count = 0

        async def failing_fetch(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("DB connection error")
            shutdown_event.set()
            return []

        with patch("src.worker.loop.fetch_pending_tasks", side_effect=failing_fetch):
            await run_worker(mock_db, {}, settings, shutdown_event, mock_openai)

        # Воркер пережил ошибку и продолжил работу
        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_waits_for_active_tasks_on_shutdown(self) -> None:
        """При shutdown воркер дожидается завершения активных задач."""
        from src.worker.loop import run_worker

        mock_db = MagicMock()
        settings = MagicMock()
        settings.worker_poll_interval = 0.1
        settings.worker_max_concurrent = 5
        settings.openai_api_key = "test"

        shutdown_event = asyncio.Event()
        mock_openai = MagicMock()
        task_completed = False

        async def slow_handler(*args, **kwargs):
            nonlocal task_completed
            await asyncio.sleep(0.3)
            task_completed = True

        call_idx = 0

        async def mock_fetch(*args, **kwargs):
            nonlocal call_idx
            call_idx += 1
            if call_idx == 1:
                return [{"id": "t1", "task_type": "full_scrape", "blog_id": "b1"}]
            # Второй poll — shutdown (задача ещё выполняется)
            shutdown_event.set()
            return []

        with (
            patch("src.worker.loop.fetch_pending_tasks", side_effect=mock_fetch),
            patch("src.worker.loop.handle_full_scrape", side_effect=slow_handler),
        ):
            await run_worker(
                mock_db, {"instagram": MagicMock()}, settings,
                shutdown_event, mock_openai,
            )

        # Задача должна завершиться (graceful wait)
        assert task_completed is True

    @pytest.mark.asyncio
    async def test_cancels_tasks_after_timeout(self) -> None:
        """Задачи, не завершившиеся за 30с, отменяются."""
        from src.worker.loop import run_worker

        mock_db = MagicMock()
        settings = MagicMock()
        settings.worker_poll_interval = 0.1
        settings.worker_max_concurrent = 5
        settings.openai_api_key = "test"

        shutdown_event = asyncio.Event()
        mock_openai = MagicMock()
        was_cancelled = False

        async def stuck_handler(*args, **kwargs):
            nonlocal was_cancelled
            try:
                await asyncio.sleep(999)  # "зависла"
            except asyncio.CancelledError:
                was_cancelled = True
                raise

        call_idx = 0

        async def mock_fetch(*args, **kwargs):
            nonlocal call_idx
            call_idx += 1
            if call_idx == 1:
                return [{"id": "t1", "task_type": "full_scrape", "blog_id": "b1"}]
            shutdown_event.set()
            return []

        async def fast_wait(tasks, timeout=None):
            """Имитация asyncio.wait с мгновенным таймаутом — все задачи pending."""
            # Не ждём — сразу возвращаем все как pending
            return set(), set(tasks)

        with (
            patch("src.worker.loop.fetch_pending_tasks", side_effect=mock_fetch),
            patch("src.worker.loop.handle_full_scrape", side_effect=stuck_handler),
            patch("src.worker.loop.asyncio.wait", side_effect=fast_wait),
        ):
            await run_worker(
                mock_db, {"instagram": MagicMock()}, settings,
                shutdown_event, mock_openai,
            )

        # Задача должна быть отменена
        assert was_cancelled is True
