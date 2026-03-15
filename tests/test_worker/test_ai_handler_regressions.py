"""Регрессионные тесты для edge-cases в ai_handler."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import make_db_mock


def _current_blog_row(*, status: str, ai_insights: dict) -> dict:
    return {
        "id": "blog-1",
        "city": None,
        "content_language": None,
        "audience_gender": None,
        "audience_age": None,
        "audience_countries": None,
        "scrape_status": status,
        "ai_insights": ai_insights,
    }


class TestHandleBatchResultsRegressions:
    @pytest.mark.asyncio
    async def test_refusal_does_not_override_successful_insights(self) -> None:
        """Refusal из другого батча не должен перетирать успешные insights."""
        from src.worker.handlers import handle_batch_results

        db = make_db_mock()
        openai_client = MagicMock()
        successful_insights = {
            "blogger_profile": {"page_type": "personal"},
            "audience_inference": {},
            "content": {},
            "commercial": {},
            "summary": "ok",
        }

        # execute() для загрузки current blogs
        db.table.return_value.execute = AsyncMock(
            return_value=MagicMock(
                data=[_current_blog_row(status="ai_analyzed", ai_insights=successful_insights)]
            )
        )

        with (
            patch(
                "src.worker.handlers.poll_batch",
                new_callable=AsyncMock,
                return_value={
                    "status": "completed",
                    "results": {"blog-1": ("refusal", "safety policy")},
                },
            ),
            patch("src.worker.handlers.load_categories", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.load_tags", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.load_cities", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock) as mock_retry_task,
        ):
            await handle_batch_results(db, openai_client, "batch-1", {"blog-1": "task-1"})

        # refusal не перетирает успешный ai_insights и не создаёт text-only retry
        db.table.return_value.update.assert_not_called()
        mock_retry_task.assert_not_called()
        mock_done.assert_called_once_with(db, "task-1")

    @pytest.mark.asyncio
    async def test_failed_mark_task_failed_for_one_task_does_not_block_others(self) -> None:
        """Ошибка mark_task_failed для одной задачи не блокирует остальные задачи блога."""
        from src.worker.handlers import handle_batch_results

        db = make_db_mock()
        openai_client = MagicMock()

        # execute() для загрузки current blogs
        db.table.return_value.execute = AsyncMock(
            return_value=MagicMock(
                data=[_current_blog_row(status="running", ai_insights={})]
            )
        )

        with (
            patch(
                "src.worker.handlers.poll_batch",
                new_callable=AsyncMock,
                return_value={"status": "completed", "results": {"blog-1": None}},
            ),
            patch("src.worker.handlers.load_categories", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.load_tags", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.load_cities", new_callable=AsyncMock, return_value={}),
            patch("src.worker.ai_handler._process_blog_result", new_callable=AsyncMock, side_effect=Exception("boom")),
            patch(
                "src.worker.handlers.mark_task_failed",
                new_callable=AsyncMock,
                side_effect=[Exception("db down"), None],
            ) as mock_failed,
        ):
            await handle_batch_results(
                db,
                openai_client,
                "batch-2",
                {
                    "blog-1": [
                        {"id": "task-1", "attempts": 1, "max_attempts": 3},
                        {"id": "task-2", "attempts": 1, "max_attempts": 3},
                    ]
                },
            )

        assert mock_failed.await_count == 2
