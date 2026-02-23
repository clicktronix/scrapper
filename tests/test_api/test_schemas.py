"""Тесты Pydantic-схем API."""


class TestScrapeRequest:
    """Валидация запроса на скрейп."""

    def test_valid_request(self) -> None:
        from src.api.schemas import ScrapeRequest

        req = ScrapeRequest(usernames=["blogger1", "blogger2"])
        assert req.usernames == ["blogger1", "blogger2"]

    def test_empty_usernames_rejected(self) -> None:
        import pytest

        from src.api.schemas import ScrapeRequest

        with pytest.raises(Exception):
            ScrapeRequest(usernames=[])

    def test_over_100_usernames_rejected(self) -> None:
        import pytest

        from src.api.schemas import ScrapeRequest

        with pytest.raises(Exception):
            ScrapeRequest(usernames=[f"user{i}" for i in range(101)])

    def test_exactly_100_accepted(self) -> None:
        from src.api.schemas import ScrapeRequest

        req = ScrapeRequest(usernames=[f"user{i}" for i in range(100)])
        assert len(req.usernames) == 100

    def test_strips_whitespace(self) -> None:
        from src.api.schemas import ScrapeRequest

        req = ScrapeRequest(usernames=["  blogger1  ", "blogger2"])
        assert req.usernames == ["blogger1", "blogger2"]

    def test_removes_at_prefix(self) -> None:
        from src.api.schemas import ScrapeRequest

        req = ScrapeRequest(usernames=["@blogger1", "blogger2"])
        assert req.usernames == ["blogger1", "blogger2"]

    def test_duplicates_removed(self) -> None:
        from src.api.schemas import ScrapeRequest

        req = ScrapeRequest(usernames=["blogger1", "blogger1", "blogger2"])
        assert req.usernames == ["blogger1", "blogger2"]

    def test_usernames_normalized_to_lowercase(self) -> None:
        from src.api.schemas import ScrapeRequest

        req = ScrapeRequest(usernames=["BlogGer1", "@BLOGGER2"])
        assert req.usernames == ["blogger1", "blogger2"]


class TestDiscoverRequest:
    """Валидация запроса на discover."""

    def test_valid_request(self) -> None:
        from src.api.schemas import DiscoverRequest

        req = DiscoverRequest(hashtag="алматымама")
        assert req.hashtag == "алматымама"
        assert req.min_followers == 1000

    def test_custom_min_followers(self) -> None:
        from src.api.schemas import DiscoverRequest

        req = DiscoverRequest(hashtag="beauty", min_followers=5000)
        assert req.min_followers == 5000

    def test_strips_hash_prefix(self) -> None:
        from src.api.schemas import DiscoverRequest

        req = DiscoverRequest(hashtag="#алматымама")
        assert req.hashtag == "алматымама"

    def test_empty_hashtag_rejected(self) -> None:
        import pytest

        from src.api.schemas import DiscoverRequest

        with pytest.raises(Exception):
            DiscoverRequest(hashtag="")

    def test_only_hash_rejected(self) -> None:
        import pytest

        from src.api.schemas import DiscoverRequest

        with pytest.raises(Exception):
            DiscoverRequest(hashtag="#")

    def test_whitespace_only_rejected(self) -> None:
        import pytest

        from src.api.schemas import DiscoverRequest

        with pytest.raises(Exception):
            DiscoverRequest(hashtag="   ")


class TestTaskResponse:
    """Проверка формата ответа."""

    def test_task_response_from_db_row(self) -> None:
        from src.api.schemas import TaskResponse

        row = {
            "id": "abc-123",
            "blog_id": "blog-456",
            "task_type": "full_scrape",
            "status": "pending",
            "priority": 3,
            "attempts": 0,
            "error_message": None,
            "payload": {},
            "created_at": "2026-02-20T10:00:00+00:00",
            "started_at": None,
            "completed_at": None,
        }
        resp = TaskResponse(**row)
        assert resp.id == "abc-123"
        assert resp.status == "pending"


class TestTaskListResponse:
    """Проверка пагинированного ответа."""

    def test_task_list_response(self) -> None:
        from src.api.schemas import TaskListResponse

        resp = TaskListResponse(tasks=[], total=0, limit=20, offset=0)
        assert resp.total == 0


class TestHealthResponse:
    """Проверка healthcheck."""

    def test_health_response(self) -> None:
        from src.api.schemas import HealthResponse

        resp = HealthResponse(
            status="ok",
            accounts_total=2,
            accounts_available=1,
            tasks_running=2,
            tasks_pending=15,
        )
        assert resp.status == "ok"
