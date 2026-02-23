"""Тесты модели ScrapeTask."""
import pytest


class TestScrapeTask:
    """Тесты модели задачи скрапинга."""

    def test_minimal_task(self) -> None:
        from src.models.task import ScrapeTask

        t = ScrapeTask(
            id="task-1",
            blog_id="blog-1",
            task_type="full_scrape",
            status="pending",
            priority=5,
        )
        assert t.attempts == 0
        assert t.max_attempts == 3
        assert t.payload == {}

    def test_task_type_validation(self) -> None:
        from pydantic import ValidationError

        from src.models.task import ScrapeTask

        with pytest.raises(ValidationError):
            ScrapeTask(
                id="task-2",
                blog_id="blog-2",
                task_type="invalid_type",
                status="pending",
                priority=5,
            )

    def test_status_validation(self) -> None:
        from pydantic import ValidationError

        from src.models.task import ScrapeTask

        with pytest.raises(ValidationError):
            ScrapeTask(
                id="task-3",
                blog_id="blog-3",
                task_type="full_scrape",
                status="bad_status",
                priority=5,
            )

    def test_discover_task_with_payload(self) -> None:
        from src.models.task import ScrapeTask

        t = ScrapeTask(
            id="task-4",
            blog_id=None,
            task_type="discover",
            status="pending",
            priority=6,
            payload={"hashtag": "алматымама", "min_followers": 3000},
        )
        assert t.payload["hashtag"] == "алматымама"
        assert t.blog_id is None
