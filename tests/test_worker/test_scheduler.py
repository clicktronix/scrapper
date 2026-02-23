"""Тесты APScheduler cron-задач."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestScheduleUpdates:
    """Тесты schedule_updates."""

    @pytest.mark.asyncio
    async def test_creates_tasks_for_stale_blogs(self) -> None:
        from src.worker.scheduler import schedule_updates

        mock_db = MagicMock()
        settings = MagicMock()
        settings.rescrape_days = 60

        # Вернуть 2 устаревших блога
        result_mock = MagicMock()
        result_mock.data = [{"id": "blog-1"}, {"id": "blog-2"}]
        chain = mock_db.table.return_value.select.return_value.eq.return_value
        chain.or_.return_value.order.return_value.limit.return_value.execute.return_value = result_mock

        # Мок create_task_if_not_exists
        with patch(
            "src.worker.scheduler.create_task_if_not_exists",
            new_callable=AsyncMock,
            return_value="task-id",
        ) as mock_create:
            await schedule_updates(mock_db, settings)

            assert mock_create.call_count == 2
            # Проверяем priority=8 для re-scrape
            mock_create.assert_any_call(mock_db, "blog-1", "full_scrape", priority=8)
            mock_create.assert_any_call(mock_db, "blog-2", "full_scrape", priority=8)

    @pytest.mark.asyncio
    async def test_no_stale_blogs(self) -> None:
        from src.worker.scheduler import schedule_updates

        mock_db = MagicMock()
        settings = MagicMock()
        settings.rescrape_days = 60

        result_mock = MagicMock()
        result_mock.data = []
        chain = mock_db.table.return_value.select.return_value.eq.return_value
        chain.or_.return_value.order.return_value.limit.return_value.execute.return_value = result_mock

        with patch(
            "src.worker.scheduler.create_task_if_not_exists",
            new_callable=AsyncMock,
        ) as mock_create:
            await schedule_updates(mock_db, settings)

            mock_create.assert_not_called()


class TestScheduleUpdatesRescrape:
    """Тесты schedule_updates — rescrape_days из settings + сортировка."""

    @pytest.mark.asyncio
    async def test_uses_rescrape_days_from_settings(self) -> None:
        """Используется settings.rescrape_days вместо хардкода 60."""
        from src.worker.scheduler import schedule_updates

        mock_db = MagicMock()
        settings = MagicMock()
        settings.rescrape_days = 90

        result_mock = MagicMock()
        result_mock.data = []
        chain = mock_db.table.return_value.select.return_value.eq.return_value
        chain.or_.return_value.order.return_value.limit.return_value.execute.return_value = result_mock

        with patch(
            "src.worker.scheduler.create_task_if_not_exists",
            new_callable=AsyncMock,
        ):
            await schedule_updates(mock_db, settings)

        # Проверяем, что фильтр включает и stale, и null scraped_at
        chain.or_.assert_called_once()

    @pytest.mark.asyncio
    async def test_orders_by_followers_count_desc(self) -> None:
        """Результаты отсортированы по подписчикам (desc)."""
        from src.worker.scheduler import schedule_updates

        mock_db = MagicMock()
        settings = MagicMock()
        settings.rescrape_days = 60

        result_mock = MagicMock()
        result_mock.data = []
        chain = mock_db.table.return_value.select.return_value.eq.return_value
        chain.or_.return_value.order.return_value.limit.return_value.execute.return_value = result_mock

        with patch(
            "src.worker.scheduler.create_task_if_not_exists",
            new_callable=AsyncMock,
        ):
            await schedule_updates(mock_db, settings)

        # Проверяем вызов .order("followers_count", desc=True)
        chain.or_.return_value.order.assert_called_once_with("followers_count", desc=True)


class TestScheduleUpdatesEdge:
    """Дополнительные тесты schedule_updates."""

    @pytest.mark.asyncio
    async def test_duplicate_tasks_not_counted(self) -> None:
        """create_task_if_not_exists возвращает None для дубликатов — счётчик не растёт."""
        from src.worker.scheduler import schedule_updates

        mock_db = MagicMock()
        settings = MagicMock()
        settings.rescrape_days = 60

        result_mock = MagicMock()
        result_mock.data = [{"id": "blog-1"}, {"id": "blog-2"}]
        chain = mock_db.table.return_value.select.return_value.eq.return_value
        chain.or_.return_value.order.return_value.limit.return_value.execute.return_value = result_mock

        with patch(
            "src.worker.scheduler.create_task_if_not_exists",
            new_callable=AsyncMock,
            # Первый — новая задача, второй — уже существует
            side_effect=["task-new", None],
        ) as mock_create:
            await schedule_updates(mock_db, settings)
            assert mock_create.call_count == 2

    @pytest.mark.asyncio
    async def test_includes_null_scraped_at_in_filter(self) -> None:
        """Фильтр учитывает блоги без scraped_at (NULL)."""
        from src.worker.scheduler import schedule_updates

        mock_db = MagicMock()
        settings = MagicMock()
        settings.rescrape_days = 60

        result_mock = MagicMock()
        result_mock.data = []
        chain = mock_db.table.return_value.select.return_value.eq.return_value
        chain.or_.return_value.order.return_value.limit.return_value.execute.return_value = result_mock

        with patch(
            "src.worker.scheduler.create_task_if_not_exists",
            new_callable=AsyncMock,
        ):
            await schedule_updates(mock_db, settings)

        filter_arg = chain.or_.call_args.args[0]
        assert "scraped_at.is.null" in filter_arg
        assert "scraped_at.lt." in filter_arg


class TestRetryStaleBatchesEdge:
    """Дополнительные тесты retry_stale_batches."""

    @pytest.mark.asyncio
    async def test_passes_correct_attempts_from_task(self) -> None:
        """mark_task_failed получает attempts/max_attempts из задачи, не дефолты."""
        from src.worker.scheduler import retry_stale_batches

        mock_db = MagicMock()
        mock_openai = MagicMock()
        settings = MagicMock()

        stale_tasks = [
            {"id": "t1", "blog_id": "b1", "payload": {}, "attempts": 3, "max_attempts": 5},
        ]

        with (
            patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.scheduler.mark_task_failed", new_callable=AsyncMock) as mock_fail,
        ):
            mock_run.return_value = MagicMock(data=stale_tasks)
            await retry_stale_batches(mock_db, mock_openai, settings)

            mock_fail.assert_called_once_with(
                mock_db, "t1", 3, 5,
                "Batch not completed in 26h", retry=True,
            )


class TestCreateScheduler:
    """Тесты create_scheduler."""

    def test_creates_scheduler_with_jobs(self) -> None:
        from src.worker.scheduler import create_scheduler

        mock_db = MagicMock()
        settings = MagicMock()
        settings.discovery_hashtags_list = ["test"]
        mock_openai = MagicMock()

        scheduler = create_scheduler(mock_db, settings, mock_openai)

        job_ids = [job.id for job in scheduler.get_jobs()]
        assert "schedule_updates" in job_ids
        assert "poll_batches" in job_ids
        assert "retry_stale_batches" in job_ids
        assert "cleanup_old_images" in job_ids
    def test_no_poll_jobs_without_openai(self) -> None:
        from src.worker.scheduler import create_scheduler

        mock_db = MagicMock()
        settings = MagicMock()

        scheduler = create_scheduler(mock_db, settings, openai_client=None)

        job_ids = [job.id for job in scheduler.get_jobs()]
        assert "schedule_updates" in job_ids
        assert "poll_batches" not in job_ids
        assert "retry_stale_batches" not in job_ids

    def test_misfire_grace_time_is_none(self) -> None:
        """misfire_grace_time=None — job'ы не пропускаются при задержке."""
        from src.worker.scheduler import create_scheduler

        mock_db = MagicMock()
        settings = MagicMock()
        settings.discovery_hashtags_list = []

        scheduler = create_scheduler(mock_db, settings)

        # APScheduler 3.x: job_defaults хранятся в _job_defaults
        assert scheduler._job_defaults["misfire_grace_time"] is None

    def test_coalesce_enabled(self) -> None:
        """coalesce=True — пропущенные запуски объединяются в один."""
        from src.worker.scheduler import create_scheduler

        mock_db = MagicMock()
        settings = MagicMock()
        settings.discovery_hashtags_list = []

        scheduler = create_scheduler(mock_db, settings)

        assert scheduler._job_defaults["coalesce"] is True

    def test_recover_tasks_job_exists(self) -> None:
        from src.worker.scheduler import create_scheduler

        mock_db = MagicMock()
        settings = MagicMock()
        settings.discovery_hashtags_list = []

        scheduler = create_scheduler(mock_db, settings)

        job_ids = [job.id for job in scheduler.get_jobs()]
        assert "recover_tasks" in job_ids


class TestPollBatches:
    """Тесты poll_batches."""

    @pytest.mark.asyncio
    async def test_no_running_tasks(self) -> None:
        from src.worker.scheduler import poll_batches

        mock_db = MagicMock()
        mock_openai = MagicMock()

        with patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = MagicMock(data=[])
            await poll_batches(mock_db, mock_openai)
            mock_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_groups_by_batch_id(self) -> None:
        from src.worker.scheduler import poll_batches

        mock_db = MagicMock()
        mock_openai = MagicMock()

        tasks = [
            {"id": "t1", "blog_id": "b1", "payload": {"batch_id": "batch-A"},
             "attempts": 1, "max_attempts": 3},
            {"id": "t2", "blog_id": "b2", "payload": {"batch_id": "batch-A"},
             "attempts": 1, "max_attempts": 3},
            {"id": "t3", "blog_id": "b3", "payload": {"batch_id": "batch-B"},
             "attempts": 2, "max_attempts": 3},
        ]

        with (
            patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.scheduler.handle_batch_results", new_callable=AsyncMock) as mock_handle,
        ):
            mock_run.return_value = MagicMock(data=tasks)
            await poll_batches(mock_db, mock_openai)

            assert mock_handle.call_count == 2
            # Проверяем batch_id аргументы
            batch_ids = {c.args[2] for c in mock_handle.call_args_list}
            assert batch_ids == {"batch-A", "batch-B"}

            # Проверяем формат task_ids_by_blog с attempts/max_attempts
            for call in mock_handle.call_args_list:
                task_ids_map = call.args[3]
                for blog_id, task_info in task_ids_map.items():
                    assert "id" in task_info
                    assert "attempts" in task_info
                    assert "max_attempts" in task_info

    @pytest.mark.asyncio
    async def test_skips_tasks_without_batch_id(self) -> None:
        from src.worker.scheduler import poll_batches

        mock_db = MagicMock()
        mock_openai = MagicMock()

        tasks = [
            {"id": "t1", "blog_id": "b1", "payload": {}},
            {"id": "t2", "blog_id": "b2", "payload": None},
        ]

        with (
            patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.scheduler.handle_batch_results", new_callable=AsyncMock) as mock_handle,
        ):
            mock_run.return_value = MagicMock(data=tasks)
            await poll_batches(mock_db, mock_openai)

            mock_handle.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_exception_in_batch(self) -> None:
        """Ошибка в одном батче не должна мешать другим."""
        from src.worker.scheduler import poll_batches

        mock_db = MagicMock()
        mock_openai = MagicMock()

        tasks = [
            {"id": "t1", "blog_id": "b1", "payload": {"batch_id": "batch-fail"}},
            {"id": "t2", "blog_id": "b2", "payload": {"batch_id": "batch-ok"}},
        ]

        with (
            patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.scheduler.handle_batch_results", new_callable=AsyncMock) as mock_handle,
        ):
            mock_run.return_value = MagicMock(data=tasks)
            # Первый батч падает, второй проходит
            mock_handle.side_effect = [RuntimeError("fail"), None]
            await poll_batches(mock_db, mock_openai)

            assert mock_handle.call_count == 2

    @pytest.mark.asyncio
    async def test_preserves_duplicate_blog_tasks_in_same_batch(self) -> None:
        """Если в батче несколько задач на один blog_id, передаются обе."""
        from src.worker.scheduler import poll_batches

        mock_db = MagicMock()
        mock_openai = MagicMock()

        tasks = [
            {"id": "t1", "blog_id": "b1", "payload": {"batch_id": "batch-A"}, "attempts": 1, "max_attempts": 3},
            {"id": "t2", "blog_id": "b1", "payload": {"batch_id": "batch-A"}, "attempts": 2, "max_attempts": 3},
        ]

        with (
            patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.scheduler.handle_batch_results", new_callable=AsyncMock) as mock_handle,
        ):
            mock_run.return_value = MagicMock(data=tasks)
            await poll_batches(mock_db, mock_openai)

            mock_handle.assert_called_once()
            task_map = mock_handle.call_args.args[3]
            assert isinstance(task_map["b1"], list)
            assert len(task_map["b1"]) == 2


class TestRetryStaleBatches:
    """Тесты retry_stale_batches."""

    @pytest.mark.asyncio
    async def test_no_stale_tasks(self) -> None:
        from src.worker.scheduler import retry_stale_batches

        mock_db = MagicMock()
        mock_openai = MagicMock()
        settings = MagicMock()

        with patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = MagicMock(data=[])
            await retry_stale_batches(mock_db, mock_openai, settings)
            mock_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_retries_stale_tasks(self) -> None:
        from src.worker.scheduler import retry_stale_batches

        mock_db = MagicMock()
        mock_openai = MagicMock()
        settings = MagicMock()

        stale_tasks = [
            {"id": "t1", "blog_id": "b1", "payload": {}, "attempts": 1, "max_attempts": 3},
            {"id": "t2", "blog_id": "b2", "payload": {}, "attempts": 2, "max_attempts": 3},
        ]

        with (
            patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.scheduler.mark_task_failed", new_callable=AsyncMock) as mock_fail,
        ):
            mock_run.return_value = MagicMock(data=stale_tasks)
            await retry_stale_batches(mock_db, mock_openai, settings)

            assert mock_fail.call_count == 2
            # Проверяем, что передаётся retry=True и сообщение о 26ч
            for call_obj in mock_fail.call_args_list:
                assert call_obj.kwargs.get("retry") is True
                # Позиционные аргументы: db, task_id, attempts, max_attempts, error
                assert "26h" in call_obj.args[4]


class TestRecoverTasks:
    """Тесты recover_tasks."""

    @pytest.mark.asyncio
    async def test_calls_recover_stuck(self) -> None:
        from src.worker.scheduler import recover_tasks

        mock_db = MagicMock()

        with patch("src.worker.scheduler.recover_stuck_tasks", new_callable=AsyncMock) as mock_recover:
            mock_recover.return_value = 3
            await recover_tasks(mock_db)
            mock_recover.assert_called_once_with(mock_db, max_running_minutes=30)


class TestCleanupOldImages:
    """Тесты cleanup_old_images."""

    @pytest.mark.asyncio
    async def test_deletes_images_for_stale_blogs(self) -> None:
        from src.worker.scheduler import cleanup_old_images

        mock_db = MagicMock()
        settings = MagicMock()
        settings.rescrape_days = 60

        with (
            patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.scheduler.delete_blog_images", new_callable=AsyncMock) as mock_delete,
        ):
            mock_run.return_value = MagicMock(data=[{"id": "blog-1"}, {"id": "blog-2"}])
            mock_delete.side_effect = [3, 2]

            await cleanup_old_images(mock_db, settings)

            assert mock_delete.call_count == 2
            mock_delete.assert_any_call(mock_db, "blog-1")
            mock_delete.assert_any_call(mock_db, "blog-2")

    @pytest.mark.asyncio
    async def test_no_stale_blogs(self) -> None:
        from src.worker.scheduler import cleanup_old_images

        mock_db = MagicMock()
        settings = MagicMock()
        settings.rescrape_days = 60

        with (
            patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.scheduler.delete_blog_images", new_callable=AsyncMock) as mock_delete,
        ):
            mock_run.return_value = MagicMock(data=[])

            await cleanup_old_images(mock_db, settings)

            mock_delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_error_does_not_crash(self) -> None:
        """Ошибка delete_blog_images для одного блога не мешает другим."""
        from src.worker.scheduler import cleanup_old_images

        mock_db = MagicMock()
        settings = MagicMock()
        settings.rescrape_days = 60

        with (
            patch("src.worker.scheduler.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.scheduler.delete_blog_images", new_callable=AsyncMock) as mock_delete,
        ):
            mock_run.return_value = MagicMock(data=[{"id": "blog-1"}, {"id": "blog-2"}])
            # Первый блог — ошибка, второй — OK
            mock_delete.side_effect = [0, 5]

            await cleanup_old_images(mock_db, settings)

            assert mock_delete.call_count == 2
