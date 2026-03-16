"""Тесты APScheduler cron-задач."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import make_db_mock


def _make_async_db(*execute_results: MagicMock) -> MagicMock:
    """Создать db mock с последовательными результатами execute()."""
    db = make_db_mock()
    if execute_results:
        db.table.return_value.execute = AsyncMock(side_effect=list(execute_results))
    return db


class TestScheduleUpdates:
    """Тесты schedule_updates."""

    @pytest.mark.asyncio
    async def test_creates_tasks_for_stale_blogs(self) -> None:
        from src.worker.scheduler import schedule_updates

        settings = MagicMock()
        settings.rescrape_days = 60

        # Вернуть 2 устаревших блога
        result_mock = MagicMock()
        result_mock.data = [{"id": "blog-1"}, {"id": "blog-2"}]
        db = _make_async_db(result_mock)

        # Мок create_task_if_not_exists
        with patch(
            "src.worker.scheduler.create_task_if_not_exists",
            new_callable=AsyncMock,
            return_value="task-id",
        ) as mock_create:
            await schedule_updates(db, settings)

            assert mock_create.call_count == 2
            # Проверяем priority=8 для re-scrape
            mock_create.assert_any_call(db, "blog-1", "full_scrape", priority=8)
            mock_create.assert_any_call(db, "blog-2", "full_scrape", priority=8)

    @pytest.mark.asyncio
    async def test_no_stale_blogs(self) -> None:
        from src.worker.scheduler import schedule_updates

        settings = MagicMock()
        settings.rescrape_days = 60

        result_mock = MagicMock()
        result_mock.data = []
        db = _make_async_db(result_mock)

        with patch(
            "src.worker.scheduler.create_task_if_not_exists",
            new_callable=AsyncMock,
        ) as mock_create:
            await schedule_updates(db, settings)

            mock_create.assert_not_called()


class TestScheduleUpdatesRescrape:
    """Тесты schedule_updates — rescrape_days из settings + сортировка."""

    @pytest.mark.asyncio
    async def test_uses_rescrape_days_from_settings(self) -> None:
        """Используется settings.rescrape_days вместо хардкода 60."""
        from src.worker.scheduler import schedule_updates

        settings = MagicMock()
        settings.rescrape_days = 90

        result_mock = MagicMock()
        result_mock.data = []
        db = _make_async_db(result_mock)

        with patch(
            "src.worker.scheduler.create_task_if_not_exists",
            new_callable=AsyncMock,
        ):
            await schedule_updates(db, settings)

        # Проверяем, что фильтр включает и stale, и null scraped_at
        chain = db.table.return_value
        chain.or_.assert_called_once()

    @pytest.mark.asyncio
    async def test_orders_by_followers_count_desc(self) -> None:
        """Результаты отсортированы по подписчикам (desc)."""
        from src.worker.scheduler import schedule_updates

        settings = MagicMock()
        settings.rescrape_days = 60

        result_mock = MagicMock()
        result_mock.data = []
        db = _make_async_db(result_mock)

        with patch(
            "src.worker.scheduler.create_task_if_not_exists",
            new_callable=AsyncMock,
        ):
            await schedule_updates(db, settings)

        # Проверяем вызов .order("followers_count", desc=True)
        chain = db.table.return_value
        chain.or_.return_value.order.assert_called_once_with("followers_count", desc=True)


class TestScheduleUpdatesEdge:
    """Дополнительные тесты schedule_updates."""

    @pytest.mark.asyncio
    async def test_duplicate_tasks_not_counted(self) -> None:
        """create_task_if_not_exists возвращает None для дубликатов — счётчик не растёт."""
        from src.worker.scheduler import schedule_updates

        settings = MagicMock()
        settings.rescrape_days = 60

        result_mock = MagicMock()
        result_mock.data = [{"id": "blog-1"}, {"id": "blog-2"}]
        db = _make_async_db(result_mock)

        with patch(
            "src.worker.scheduler.create_task_if_not_exists",
            new_callable=AsyncMock,
            # Первый — новая задача, второй — уже существует
            side_effect=["task-new", None],
        ) as mock_create:
            await schedule_updates(db, settings)
            assert mock_create.call_count == 2

    @pytest.mark.asyncio
    async def test_includes_null_scraped_at_in_filter(self) -> None:
        """Фильтр учитывает блоги без scraped_at (NULL)."""
        from src.worker.scheduler import schedule_updates

        settings = MagicMock()
        settings.rescrape_days = 60

        result_mock = MagicMock()
        result_mock.data = []
        db = _make_async_db(result_mock)

        with patch(
            "src.worker.scheduler.create_task_if_not_exists",
            new_callable=AsyncMock,
        ):
            await schedule_updates(db, settings)

        chain = db.table.return_value
        filter_arg = chain.or_.call_args.args[0]
        assert "scraped_at.is.null" in filter_arg
        assert "scraped_at.lt." in filter_arg


class TestRetryStaleBatchesEdge:
    """Дополнительные тесты retry_stale_batches."""

    @pytest.mark.asyncio
    async def test_passes_correct_attempts_from_task(self) -> None:
        """mark_task_failed получает attempts/max_attempts из задачи, не дефолты."""
        from src.worker.scheduler import retry_stale_batches

        mock_openai = MagicMock()
        settings = MagicMock()

        stale_tasks = [
            {"id": "t1", "blog_id": "b1", "payload": {}, "attempts": 3, "max_attempts": 5},
        ]

        result_mock = MagicMock(data=stale_tasks)
        db = _make_async_db(result_mock)

        with patch("src.worker.scheduler.mark_task_failed", new_callable=AsyncMock) as mock_fail:
            await retry_stale_batches(db, mock_openai, settings)

            mock_fail.assert_called_once_with(
                db, "t1", 3, 5,
                "Batch not completed in 25h (exceeded OpenAI 24h window)", retry=True,
            )


class TestCreateScheduler:
    """Тесты create_scheduler."""

    def test_creates_scheduler_with_jobs(self) -> None:
        from src.worker.scheduler import create_scheduler

        mock_db = MagicMock()
        settings = MagicMock()
        settings.discovery_hashtags_list = ["test"]
        settings.backfill_scrape_interval_minutes = 30
        settings.backfill_ai_interval_minutes = 60
        mock_openai = MagicMock()

        scheduler = create_scheduler(mock_db, settings, mock_openai)

        job_ids = [job.id for job in scheduler.get_jobs()]
        assert "schedule_updates" in job_ids
        assert "poll_batches" in job_ids
        assert "retry_stale_batches" in job_ids
        assert "retry_missing_embeddings" in job_ids
        assert "retry_taxonomy_mappings" in job_ids
        assert "audit_taxonomy_drift" in job_ids
        assert "cleanup_old_images" in job_ids
        assert "backfill_scrape" in job_ids
        assert "backfill_ai_analysis" in job_ids

    def test_backfill_disabled_not_registered(self) -> None:
        """Если backfill_*_enabled=False, задачи не регистрируются."""
        from src.worker.scheduler import create_scheduler

        mock_db = MagicMock()
        settings = MagicMock()
        settings.backfill_scrape_enabled = False
        settings.backfill_ai_enabled = False

        scheduler = create_scheduler(mock_db, settings, MagicMock())

        job_ids = [job.id for job in scheduler.get_jobs()]
        assert "backfill_scrape" not in job_ids
        assert "backfill_ai_analysis" not in job_ids

    def test_no_poll_jobs_without_openai(self) -> None:
        from src.worker.scheduler import create_scheduler

        mock_db = MagicMock()
        settings = MagicMock()
        settings.backfill_scrape_interval_minutes = 30
        settings.backfill_ai_interval_minutes = 60

        scheduler = create_scheduler(mock_db, settings, openai_client=None)

        job_ids = [job.id for job in scheduler.get_jobs()]
        assert "schedule_updates" in job_ids
        assert "poll_batches" not in job_ids
        assert "retry_stale_batches" not in job_ids
        assert "retry_missing_embeddings" not in job_ids
        assert "retry_taxonomy_mappings" not in job_ids
        assert "audit_taxonomy_drift" not in job_ids
        # Backfill задачи НЕ зависят от openai_client
        assert "backfill_scrape" in job_ids
        assert "backfill_ai_analysis" in job_ids

    def test_misfire_grace_time_is_none(self) -> None:
        """misfire_grace_time=None — job'ы не пропускаются при задержке."""
        from src.worker.scheduler import create_scheduler

        mock_db = MagicMock()
        settings = MagicMock()
        settings.discovery_hashtags_list = []
        settings.backfill_scrape_interval_minutes = 30
        settings.backfill_ai_interval_minutes = 60

        scheduler = create_scheduler(mock_db, settings)

        # APScheduler 3.x: job_defaults хранятся в _job_defaults
        assert scheduler._job_defaults["misfire_grace_time"] is None

    def test_coalesce_enabled(self) -> None:
        """coalesce=True — пропущенные запуски объединяются в один."""
        from src.worker.scheduler import create_scheduler

        mock_db = MagicMock()
        settings = MagicMock()
        settings.discovery_hashtags_list = []
        settings.backfill_scrape_interval_minutes = 30
        settings.backfill_ai_interval_minutes = 60

        scheduler = create_scheduler(mock_db, settings)

        assert scheduler._job_defaults["coalesce"] is True

    def test_recover_tasks_job_exists(self) -> None:
        from src.worker.scheduler import create_scheduler

        mock_db = MagicMock()
        settings = MagicMock()
        settings.discovery_hashtags_list = []
        settings.backfill_scrape_interval_minutes = 30
        settings.backfill_ai_interval_minutes = 60

        scheduler = create_scheduler(mock_db, settings)

        job_ids = [job.id for job in scheduler.get_jobs()]
        assert "recover_tasks" in job_ids


class TestPollBatches:
    """Тесты poll_batches."""

    @pytest.mark.asyncio
    async def test_no_running_tasks(self) -> None:
        from src.worker.scheduler import poll_batches

        mock_openai = MagicMock()

        result_mock = MagicMock(data=[])
        db = _make_async_db(result_mock)

        await poll_batches(db, mock_openai)
        db.table.return_value.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_groups_by_batch_id(self) -> None:
        from src.worker.scheduler import poll_batches

        mock_openai = MagicMock()

        tasks = [
            {"id": "t1", "blog_id": "b1", "payload": {"batch_id": "batch-A"},
             "attempts": 1, "max_attempts": 3},
            {"id": "t2", "blog_id": "b2", "payload": {"batch_id": "batch-A"},
             "attempts": 1, "max_attempts": 3},
            {"id": "t3", "blog_id": "b3", "payload": {"batch_id": "batch-B"},
             "attempts": 2, "max_attempts": 3},
        ]

        result_mock = MagicMock(data=tasks)
        db = _make_async_db(result_mock)

        with patch("src.worker.scheduler.handle_batch_results", new_callable=AsyncMock) as mock_handle:
            await poll_batches(db, mock_openai)

            assert mock_handle.call_count == 2
            # Проверяем batch_id аргументы
            batch_ids = {c.args[2] for c in mock_handle.call_args_list}
            assert batch_ids == {"batch-A", "batch-B"}

            # Проверяем формат task_ids_by_blog с attempts/max_attempts
            for call in mock_handle.call_args_list:
                task_ids_map = call.args[3]
                for _blog_id, task_info in task_ids_map.items():
                    assert "id" in task_info
                    assert "attempts" in task_info
                    assert "max_attempts" in task_info

    @pytest.mark.asyncio
    async def test_skips_tasks_without_batch_id(self) -> None:
        from src.worker.scheduler import poll_batches

        mock_openai = MagicMock()

        tasks = [
            {"id": "t1", "blog_id": "b1", "payload": {}},
            {"id": "t2", "blog_id": "b2", "payload": None},
        ]

        # 1 вызов execute для select + 2 вызова для orphaned задач (update)
        result_mock = MagicMock(data=tasks)
        db = _make_async_db(result_mock, MagicMock(), MagicMock())

        with patch("src.worker.scheduler.handle_batch_results", new_callable=AsyncMock) as mock_handle:
            await poll_batches(db, mock_openai)

            mock_handle.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_exception_in_batch(self) -> None:
        """Ошибка в одном батче не должна мешать другим."""
        from src.worker.scheduler import poll_batches

        mock_openai = MagicMock()

        tasks = [
            {"id": "t1", "blog_id": "b1", "payload": {"batch_id": "batch-fail"}},
            {"id": "t2", "blog_id": "b2", "payload": {"batch_id": "batch-ok"}},
        ]

        result_mock = MagicMock(data=tasks)
        db = _make_async_db(result_mock)

        with patch("src.worker.scheduler.handle_batch_results", new_callable=AsyncMock) as mock_handle:
            # Первый батч падает, второй проходит
            mock_handle.side_effect = [RuntimeError("fail"), None]
            await poll_batches(db, mock_openai)

            assert mock_handle.call_count == 2

    @pytest.mark.asyncio
    async def test_preserves_duplicate_blog_tasks_in_same_batch(self) -> None:
        """Если в батче несколько задач на один blog_id, передаются обе."""
        from src.worker.scheduler import poll_batches

        mock_openai = MagicMock()

        tasks = [
            {"id": "t1", "blog_id": "b1", "payload": {"batch_id": "batch-A"}, "attempts": 1, "max_attempts": 3},
            {"id": "t2", "blog_id": "b1", "payload": {"batch_id": "batch-A"}, "attempts": 2, "max_attempts": 3},
        ]

        result_mock = MagicMock(data=tasks)
        db = _make_async_db(result_mock)

        with patch("src.worker.scheduler.handle_batch_results", new_callable=AsyncMock) as mock_handle:
            await poll_batches(db, mock_openai)

            mock_handle.assert_called_once()
            task_map = mock_handle.call_args.args[3]
            assert isinstance(task_map["b1"], list)
            assert len(task_map["b1"]) == 2


class TestRetryStaleBatches:
    """Тесты retry_stale_batches."""

    @pytest.mark.asyncio
    async def test_no_stale_tasks(self) -> None:
        from src.worker.scheduler import retry_stale_batches

        mock_openai = MagicMock()
        settings = MagicMock()

        result_mock = MagicMock(data=[])
        db = _make_async_db(result_mock)

        await retry_stale_batches(db, mock_openai, settings)
        db.table.return_value.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_retries_stale_tasks(self) -> None:
        from src.worker.scheduler import retry_stale_batches

        mock_openai = MagicMock()
        settings = MagicMock()

        stale_tasks = [
            {"id": "t1", "blog_id": "b1", "payload": {}, "attempts": 1, "max_attempts": 3},
            {"id": "t2", "blog_id": "b2", "payload": {}, "attempts": 2, "max_attempts": 3},
        ]

        result_mock = MagicMock(data=stale_tasks)
        db = _make_async_db(result_mock)

        with patch("src.worker.scheduler.mark_task_failed", new_callable=AsyncMock) as mock_fail:
            await retry_stale_batches(db, mock_openai, settings)

            assert mock_fail.call_count == 2
            # Проверяем, что передаётся retry=True и сообщение о 25ч
            for call_obj in mock_fail.call_args_list:
                assert call_obj.kwargs.get("retry") is True
                # Позиционные аргументы: db, task_id, attempts, max_attempts, error
                assert "25h" in call_obj.args[4]


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

        settings = MagicMock()
        settings.rescrape_days = 60

        result_mock = MagicMock(data=[{"id": "blog-1"}, {"id": "blog-2"}])
        db = _make_async_db(result_mock)

        with patch("src.worker.scheduler.delete_blog_images", new_callable=AsyncMock) as mock_delete:
            mock_delete.side_effect = [3, 2]

            await cleanup_old_images(db, settings)

            assert mock_delete.call_count == 2
            mock_delete.assert_any_call(db, "blog-1")
            mock_delete.assert_any_call(db, "blog-2")

    @pytest.mark.asyncio
    async def test_no_stale_blogs(self) -> None:
        from src.worker.scheduler import cleanup_old_images

        settings = MagicMock()
        settings.rescrape_days = 60

        result_mock = MagicMock(data=[])
        db = _make_async_db(result_mock)

        with patch("src.worker.scheduler.delete_blog_images", new_callable=AsyncMock) as mock_delete:
            await cleanup_old_images(db, settings)

            mock_delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_error_does_not_crash(self) -> None:
        """Ошибка delete_blog_images для одного блога не мешает другим."""
        from src.worker.scheduler import cleanup_old_images

        settings = MagicMock()
        settings.rescrape_days = 60

        result_mock = MagicMock(data=[{"id": "blog-1"}, {"id": "blog-2"}])
        db = _make_async_db(result_mock)

        with patch("src.worker.scheduler.delete_blog_images", new_callable=AsyncMock) as mock_delete:
            # Первый блог — ошибка, второй — OK
            mock_delete.side_effect = [0, 5]

            await cleanup_old_images(db, settings)

            assert mock_delete.call_count == 2


class TestRetryMissingEmbeddings:
    """Тесты retry_missing_embeddings."""

    @pytest.mark.asyncio
    async def test_regenerates_embedding_for_blog_without_vector(self) -> None:
        from src.ai.schemas import AIInsights
        from src.worker.scheduler import retry_missing_embeddings

        mock_openai = MagicMock()

        insights_data = AIInsights(
            short_summary="Тестовый блогер",
            tags=["видео-контент", "reels", "юмор"],
        ).model_dump()

        # Первый вызов — запрос блогов без embedding, второй — update embedding
        db = _make_async_db(
            MagicMock(data=[{"id": "blog-1", "ai_insights": insights_data}]),
            MagicMock(),
        )

        with patch("src.worker.scheduler.generate_embedding", new_callable=AsyncMock) as mock_embed:
            mock_embed.return_value = [0.1] * 1536

            await retry_missing_embeddings(db, mock_openai)

            mock_embed.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_blogs_without_embedding(self) -> None:
        from src.worker.scheduler import retry_missing_embeddings

        mock_openai = MagicMock()

        result_mock = MagicMock(data=[])
        db = _make_async_db(result_mock)

        await retry_missing_embeddings(db, mock_openai)
        # Только один вызов (запрос блогов), без update
        db.table.return_value.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_in_one_blog_does_not_crash(self) -> None:
        from src.ai.schemas import AIInsights
        from src.worker.scheduler import retry_missing_embeddings

        mock_openai = MagicMock()

        result_mock = MagicMock(data=[
            {"id": "blog-1", "ai_insights": {"invalid": True}},
            {"id": "blog-2", "ai_insights": AIInsights(short_summary="OK").model_dump()},
        ])
        # Для blog-2: update embedding
        db = _make_async_db(result_mock, MagicMock())

        with patch("src.worker.scheduler.generate_embedding", new_callable=AsyncMock) as mock_embed:
            mock_embed.return_value = [0.1] * 1536

            # Не должно падать
            await retry_missing_embeddings(db, mock_openai)


class TestRetryTaxonomyMappings:
    """Тесты retry_taxonomy_mappings."""

    @pytest.mark.asyncio
    async def test_retry_taxonomy_calls_matchers(self) -> None:
        from src.worker.scheduler import retry_taxonomy_mappings

        mock_db = make_db_mock()

        blogs_result = MagicMock(data=[{
            "id": "blog-1",
            "ai_insights": {"tags": ["видео-контент", "reels", "юмор"]},
        }])
        cats_result = MagicMock(data=[])

        # 2 вызова execute(): blogs query + blog_categories query
        mock_db.table.return_value.execute = AsyncMock(side_effect=[blogs_result, cats_result])

        with (
            patch("src.worker.scheduler.load_categories", new_callable=AsyncMock, return_value={}),
            patch("src.worker.scheduler.load_tags", new_callable=AsyncMock, return_value={}),
            patch("src.worker.scheduler.match_categories", new_callable=AsyncMock) as mock_match_categories,
            patch("src.worker.scheduler.match_tags", new_callable=AsyncMock) as mock_match_tags,
        ):
            await retry_taxonomy_mappings(mock_db)

            mock_match_categories.assert_called_once()
            mock_match_tags.assert_called_once()


class TestAuditTaxonomyDrift:
    """Тесты audit_taxonomy_drift."""

    @pytest.mark.asyncio
    async def test_audit_logs_when_mismatch_found(self) -> None:
        from src.worker.scheduler import audit_taxonomy_drift

        mock_db = MagicMock()
        with (
            patch("src.worker.scheduler.load_categories", new_callable=AsyncMock, return_value={"beauty": "cat-1"}),
            patch("src.worker.scheduler.load_tags", new_callable=AsyncMock, return_value={"видео-контент": "tag-1"}),
            patch("src.worker.scheduler.logger") as mock_logger,
        ):
            await audit_taxonomy_drift(mock_db)
            assert mock_logger.warning.call_count >= 1


class TestHasRecentBalanceErrors:
    """Тесты has_recent_balance_errors."""

    @pytest.mark.asyncio
    async def test_returns_true_when_errors_found(self) -> None:
        from src.worker.scheduler import has_recent_balance_errors

        db = make_db_mock()
        result_mock = MagicMock()
        result_mock.count = 1
        result_mock.data = [{"id": "t1"}]
        db.table.return_value.execute = AsyncMock(return_value=result_mock)

        assert await has_recent_balance_errors(db, "insufficient balance") is True

    @pytest.mark.asyncio
    async def test_returns_false_when_no_errors(self) -> None:
        from src.worker.scheduler import has_recent_balance_errors

        db = make_db_mock()
        result_mock = MagicMock()
        result_mock.count = 0
        result_mock.data = []
        db.table.return_value.execute = AsyncMock(return_value=result_mock)

        assert await has_recent_balance_errors(db, "insufficient balance") is False

    @pytest.mark.asyncio
    async def test_returns_false_when_count_is_none(self) -> None:
        """Supabase может вернуть count=None — не должен падать."""
        from src.worker.scheduler import has_recent_balance_errors

        db = make_db_mock()
        result_mock = MagicMock()
        result_mock.count = None
        result_mock.data = []
        db.table.return_value.execute = AsyncMock(return_value=result_mock)

        assert await has_recent_balance_errors(db, "insufficient balance") is False


class TestBackfillScrape:
    """Тесты backfill_scrape."""

    @pytest.mark.asyncio
    async def test_creates_tasks_for_pending_blogs(self) -> None:
        from src.worker.scheduler import backfill_scrape

        settings = MagicMock()
        settings.backfill_scrape_batch_size = 80

        db = make_db_mock()
        rpc_result = MagicMock()
        rpc_result.data = [{"id": "blog-1"}, {"id": "blog-2"}, {"id": "blog-3"}]
        rpc_mock = MagicMock()
        rpc_mock.execute = AsyncMock(return_value=rpc_result)
        db.rpc.return_value = rpc_mock

        with (
            patch("src.worker.scheduler.has_recent_balance_errors", new_callable=AsyncMock, return_value=False),
            patch(
                "src.worker.scheduler.create_task_if_not_exists",
                new_callable=AsyncMock, return_value="task-id",
            ) as mock_create,
        ):
            await backfill_scrape(db=db, settings=settings)

            assert mock_create.call_count == 3
            mock_create.assert_any_call(db, "blog-1", "full_scrape", priority=6)
            mock_create.assert_any_call(db, "blog-2", "full_scrape", priority=6)
            mock_create.assert_any_call(db, "blog-3", "full_scrape", priority=6)

    @pytest.mark.asyncio
    async def test_empty_rpc_result(self) -> None:
        from src.worker.scheduler import backfill_scrape

        settings = MagicMock()
        settings.backfill_scrape_batch_size = 80

        db = make_db_mock()
        rpc_result = MagicMock()
        rpc_result.data = []
        rpc_mock = MagicMock()
        rpc_mock.execute = AsyncMock(return_value=rpc_result)
        db.rpc.return_value = rpc_mock

        with (
            patch("src.worker.scheduler.has_recent_balance_errors", new_callable=AsyncMock, return_value=False),
            patch("src.worker.scheduler.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            await backfill_scrape(db=db, settings=settings)
            mock_create.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_on_balance_errors(self) -> None:
        from src.worker.scheduler import backfill_scrape

        settings = MagicMock()
        settings.backfill_scrape_batch_size = 80

        db = make_db_mock()

        with (
            patch("src.worker.scheduler.has_recent_balance_errors", new_callable=AsyncMock, return_value=True),
            patch("src.worker.scheduler.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            await backfill_scrape(db=db, settings=settings)
            mock_create.assert_not_called()


class TestBackfillAiAnalysis:
    """Тесты backfill_ai_analysis."""

    @pytest.mark.asyncio
    async def test_creates_tasks_for_unanalyzed_blogs(self) -> None:
        from src.worker.scheduler import backfill_ai_analysis

        settings = MagicMock()
        settings.backfill_ai_batch_size = 50

        db = make_db_mock()
        rpc_result = MagicMock()
        rpc_result.data = [{"id": "blog-1"}, {"id": "blog-2"}]
        rpc_mock = MagicMock()
        rpc_mock.execute = AsyncMock(return_value=rpc_result)
        db.rpc.return_value = rpc_mock

        with (
            patch("src.worker.scheduler.has_recent_balance_errors", new_callable=AsyncMock, return_value=False),
            patch(
                "src.worker.scheduler.create_task_if_not_exists",
                new_callable=AsyncMock, return_value="task-id",
            ) as mock_create,
        ):
            await backfill_ai_analysis(db=db, settings=settings)

            assert mock_create.call_count == 2
            mock_create.assert_any_call(db, "blog-1", "ai_analysis", priority=2)
            mock_create.assert_any_call(db, "blog-2", "ai_analysis", priority=2)

    @pytest.mark.asyncio
    async def test_empty_rpc_result(self) -> None:
        from src.worker.scheduler import backfill_ai_analysis

        settings = MagicMock()
        settings.backfill_ai_batch_size = 50

        db = make_db_mock()
        rpc_result = MagicMock()
        rpc_result.data = []
        rpc_mock = MagicMock()
        rpc_mock.execute = AsyncMock(return_value=rpc_result)
        db.rpc.return_value = rpc_mock

        with (
            patch("src.worker.scheduler.has_recent_balance_errors", new_callable=AsyncMock, return_value=False),
            patch("src.worker.scheduler.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            await backfill_ai_analysis(db=db, settings=settings)
            mock_create.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_on_openai_balance_errors(self) -> None:
        from src.worker.scheduler import backfill_ai_analysis

        settings = MagicMock()
        settings.backfill_ai_batch_size = 50

        db = make_db_mock()

        with (
            patch(
                "src.worker.scheduler.has_recent_balance_errors",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch("src.worker.scheduler.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            await backfill_ai_analysis(db=db, settings=settings)
            mock_create.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_on_billing_hard_limit(self) -> None:
        """Пропуск при ошибке billing_hard_limit (второй паттерн)."""
        from src.worker.scheduler import backfill_ai_analysis

        settings = MagicMock()
        settings.backfill_ai_batch_size = 50

        db = make_db_mock()

        with (
            patch(
                "src.worker.scheduler.has_recent_balance_errors",
                new_callable=AsyncMock,
                # insufficient_quota=False, billing_hard_limit=True
                side_effect=[False, True],
            ),
            patch(
                "src.worker.scheduler.create_task_if_not_exists",
                new_callable=AsyncMock,
            ) as mock_create,
        ):
            await backfill_ai_analysis(db=db, settings=settings)
            mock_create.assert_not_called()
