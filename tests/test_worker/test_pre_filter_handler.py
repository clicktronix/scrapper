"""Тесты обработчика pre_filter."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from instagrapi.exceptions import UserNotFound

from src.platforms.instagram.exceptions import HikerAPIError, InsufficientBalanceError
from tests.conftest import make_db_mock, make_settings, make_task


def _make_user_info(*, is_private: bool = False, pk: int = 12345,
                    full_name: str = "Test User", follower_count: int = 10000) -> dict:
    """Фабрика ответа user_by_username_v2."""
    return {
        "user": {
            "pk": pk,
            "is_private": is_private,
            "full_name": full_name,
            "follower_count": follower_count,
        }
    }


def _make_medias(count: int = 5, like_count: int = 100,
                 taken_at: datetime | None = None) -> list:
    """Фабрика ответа user_medias_chunk_v1 / user_clips_chunk_v1 — [medias_list, cursor]."""
    if taken_at is None:
        taken_at = datetime.now(UTC) - timedelta(days=1)
    medias = [
        {
            "like_count": like_count,
            "taken_at": int(taken_at.timestamp()),
        }
        for _ in range(count)
    ]
    return [medias, "next_cursor"]


def _empty_medias() -> list:
    """Пустой ответ медиа."""
    return [[], None]


def _mock_to_thread_factory(
    user_info: dict,
    posts: list | None = None,
    clips: list | None = None,
):
    """Фабрика mock для asyncio.to_thread с user_info + posts + clips.

    Порядок вызовов:
    1. user_by_username_v2 → user_info
    2. user_medias_chunk_v1 → posts (через gather)
    3. user_clips_chunk_v1 → clips (через gather)
    """
    if posts is None:
        posts = _make_medias()
    if clips is None:
        clips = _empty_medias()

    call_count = 0

    async def mock_to_thread(func, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return user_info
        if call_count == 2:
            return posts
        return clips

    return mock_to_thread


class TestHandlePreFilter:
    """Тесты handle_pre_filter."""

    @pytest.mark.asyncio
    async def test_private_account_filtered_out(self) -> None:
        """Приватный аккаунт → task done с filtered_out: private."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "private_user"})
        db = make_db_mock()
        settings = make_settings(
            pre_filter_min_likes=30,
            pre_filter_max_inactive_days=180,
            pre_filter_posts_to_check=5,
        )
        scraper = MagicMock()
        mock_fn = _mock_to_thread_factory(_make_user_info(is_private=True))

        with (
            patch("src.worker.pre_filter_handler.asyncio.to_thread", side_effect=mock_fn),
            patch("src.worker.pre_filter_handler._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler._h.run_in_thread", new_callable=AsyncMock) as mock_run,
        ):
            await handle_pre_filter(db, task, scraper, settings)

            # 2 вызова run_in_thread: update задачи + insert в pre_filter_log
            assert mock_run.call_count == 2
            update_calls = db.table.return_value.update.call_args_list
            assert len(update_calls) == 1
            update_data = update_calls[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: private"
            # Проверяем запись в pre_filter_log
            insert_calls = db.table.return_value.insert.call_args_list
            assert len(insert_calls) == 1
            log_data = insert_calls[0][0][0]
            assert log_data["username"] == "private_user"
            assert log_data["reason"] == "private"

    @pytest.mark.asyncio
    async def test_low_engagement_filtered_out(self) -> None:
        """Средние лайки ниже порога → filtered_out: low_engagement."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "low_eng_user"})
        db = make_db_mock()
        settings = make_settings(
            pre_filter_min_likes=30,
            pre_filter_max_inactive_days=180,
            pre_filter_posts_to_check=5,
        )
        mock_fn = _mock_to_thread_factory(
            _make_user_info(),
            posts=_make_medias(count=3, like_count=10),
            clips=_make_medias(count=2, like_count=5),
        )

        with (
            patch("src.worker.pre_filter_handler.asyncio.to_thread", side_effect=mock_fn),
            patch("src.worker.pre_filter_handler._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler._h.run_in_thread", new_callable=AsyncMock),
        ):
            await handle_pre_filter(db, task, MagicMock(), settings)

            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: low_engagement"

    @pytest.mark.asyncio
    async def test_inactive_account_filtered_out(self) -> None:
        """Последний пост старше 180 дней → filtered_out: inactive."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "inactive_user"})
        db = make_db_mock()
        settings = make_settings(
            pre_filter_min_likes=30,
            pre_filter_max_inactive_days=180,
            pre_filter_posts_to_check=5,
        )
        old_date = datetime.now(UTC) - timedelta(days=200)
        mock_fn = _mock_to_thread_factory(
            _make_user_info(),
            posts=_make_medias(count=3, like_count=100, taken_at=old_date),
            clips=_empty_medias(),
        )

        with (
            patch("src.worker.pre_filter_handler.asyncio.to_thread", side_effect=mock_fn),
            patch("src.worker.pre_filter_handler._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler._h.run_in_thread", new_callable=AsyncMock),
        ):
            await handle_pre_filter(db, task, MagicMock(), settings)

            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: inactive"

    @pytest.mark.asyncio
    async def test_inactive_account_with_naive_iso_datetime(self) -> None:
        """Naive ISO datetime в taken_at корректно обрабатывается как UTC."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "inactive_iso_user"})
        db = make_db_mock()
        settings = make_settings(
            pre_filter_min_likes=30,
            pre_filter_max_inactive_days=180,
            pre_filter_posts_to_check=5,
        )
        old_date_iso = (datetime.now(UTC) - timedelta(days=200)).replace(tzinfo=None).isoformat()
        posts = [[{"like_count": 100, "taken_at": old_date_iso}], None]
        mock_fn = _mock_to_thread_factory(
            _make_user_info(),
            posts=posts,
            clips=_empty_medias(),
        )

        with (
            patch("src.worker.pre_filter_handler.asyncio.to_thread", side_effect=mock_fn),
            patch("src.worker.pre_filter_handler._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler._h.run_in_thread", new_callable=AsyncMock),
        ):
            await handle_pre_filter(db, task, MagicMock(), settings)

            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: inactive"

    @pytest.mark.asyncio
    async def test_inactive_posts_but_active_clips_passes(self) -> None:
        """Посты старые, но есть свежие рилсы → не inactive."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "reels_user"})
        db = make_db_mock()
        settings = make_settings(
            pre_filter_min_likes=30,
            pre_filter_max_inactive_days=180,
            pre_filter_posts_to_check=5,
        )
        old_date = datetime.now(UTC) - timedelta(days=200)
        recent_date = datetime.now(UTC) - timedelta(days=5)
        mock_fn = _mock_to_thread_factory(
            _make_user_info(),
            posts=_make_medias(count=2, like_count=100, taken_at=old_date),
            clips=_make_medias(count=3, like_count=100, taken_at=recent_date),
        )

        person_result = MagicMock()
        person_result.data = [{"id": "person-1"}]
        blog_result = MagicMock()
        blog_result.data = [{"id": "blog-1"}]
        insert_count = 0

        async def mock_rit(func, *args, **kwargs):
            nonlocal insert_count
            insert_count += 1
            return person_result if insert_count == 1 else blog_result

        with (
            patch("src.worker.pre_filter_handler.asyncio.to_thread", side_effect=mock_fn),
            patch("src.worker.pre_filter_handler._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler._h.run_in_thread", new_callable=AsyncMock, side_effect=mock_rit),
            patch("src.worker.pre_filter_handler._h.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            await handle_pre_filter(db, task, MagicMock(), settings)
            # Прошёл фильтр — не отфильтрован как inactive
            mock_done.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_posts_filtered_out(self) -> None:
        """Нет постов и рилсов → filtered_out: inactive."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "empty_user"})
        db = make_db_mock()
        settings = make_settings(
            pre_filter_min_likes=30,
            pre_filter_max_inactive_days=180,
            pre_filter_posts_to_check=5,
        )
        mock_fn = _mock_to_thread_factory(
            _make_user_info(),
            posts=_empty_medias(),
            clips=_empty_medias(),
        )

        with (
            patch("src.worker.pre_filter_handler.asyncio.to_thread", side_effect=mock_fn),
            patch("src.worker.pre_filter_handler._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler._h.run_in_thread", new_callable=AsyncMock),
        ):
            await handle_pre_filter(db, task, MagicMock(), settings)

            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: inactive"

    @pytest.mark.asyncio
    async def test_passed_creates_person_and_blog(self) -> None:
        """Профиль прошёл фильтр → создаётся person + blog."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "good_user"})
        db = make_db_mock()
        settings = make_settings(
            pre_filter_min_likes=30,
            pre_filter_max_inactive_days=180,
            pre_filter_posts_to_check=5,
        )
        mock_fn = _mock_to_thread_factory(
            _make_user_info(full_name="Good User", follower_count=50000),
            posts=_make_medias(count=5, like_count=100),
        )

        person_result = MagicMock()
        person_result.data = [{"id": "person-1"}]
        blog_result = MagicMock()
        blog_result.data = [{"id": "blog-1"}]
        insert_count = 0

        async def mock_rit(func, *args, **kwargs):
            nonlocal insert_count
            insert_count += 1
            return person_result if insert_count == 1 else blog_result

        with (
            patch("src.worker.pre_filter_handler.asyncio.to_thread", side_effect=mock_fn),
            patch("src.worker.pre_filter_handler._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler._h.run_in_thread", new_callable=AsyncMock, side_effect=mock_rit),
            patch("src.worker.pre_filter_handler._h.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            await handle_pre_filter(db, task, MagicMock(), settings)

            blog_insert_data = db.table.return_value.insert.call_args_list[-1][0][0]
            assert blog_insert_data["source"] == "xlsx_import"
            assert blog_insert_data["scrape_status"] == "pending"
            assert blog_insert_data["platform"] == "instagram"
            assert blog_insert_data["username"] == "good_user"
            mock_done.assert_called_once_with(db, task["id"])

    @pytest.mark.asyncio
    async def test_user_not_found_filtered_out(self) -> None:
        """UserNotFound → task done с filtered_out: not_found."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "ghost_user"})
        db = make_db_mock()
        settings = make_settings(
            pre_filter_min_likes=30,
            pre_filter_max_inactive_days=180,
            pre_filter_posts_to_check=5,
        )

        with (
            patch("src.worker.pre_filter_handler.asyncio.to_thread",
                  new_callable=AsyncMock, side_effect=UserNotFound()),
            patch("src.worker.pre_filter_handler._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler._h.run_in_thread", new_callable=AsyncMock),
        ):
            await handle_pre_filter(db, task, MagicMock(), settings)

            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: not_found"

    @pytest.mark.asyncio
    async def test_insufficient_balance_fails_no_retry(self) -> None:
        """InsufficientBalanceError → task failed, retry=False."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "test_user"})
        db = make_db_mock()
        settings = make_settings(
            pre_filter_min_likes=30,
            pre_filter_max_inactive_days=180,
            pre_filter_posts_to_check=5,
        )

        with (
            patch("src.worker.pre_filter_handler.asyncio.to_thread",
                  new_callable=AsyncMock, side_effect=InsufficientBalanceError("No balance")),
            patch("src.worker.pre_filter_handler._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler._h.mark_task_failed", new_callable=AsyncMock) as mock_failed,
        ):
            await handle_pre_filter(db, task, MagicMock(), settings)

            mock_failed.assert_called_once()
            call_kwargs = mock_failed.call_args
            assert call_kwargs[1].get("retry") is False or call_kwargs[0][-1] is False

    @pytest.mark.asyncio
    async def test_hiker_api_404_logged_as_not_found(self) -> None:
        """HikerAPIError 404 при user_by_username → done + pre_filter_log: not_found."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "deleted_user"})
        db = make_db_mock()
        settings = make_settings(
            pre_filter_min_likes=30,
            pre_filter_max_inactive_days=180,
            pre_filter_posts_to_check=5,
        )

        with (
            patch("src.worker.pre_filter_handler.asyncio.to_thread",
                  new_callable=AsyncMock, side_effect=HikerAPIError(404, "Target user not found")),
            patch("src.worker.pre_filter_handler._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler._h.run_in_thread", new_callable=AsyncMock) as mock_run,
        ):
            await handle_pre_filter(db, task, MagicMock(), settings)

            # 2 вызова run_in_thread: update задачи + insert в pre_filter_log
            assert mock_run.call_count == 2
            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: not_found"
            # Проверяем запись в pre_filter_log
            insert_calls = db.table.return_value.insert.call_args_list
            assert len(insert_calls) == 1
            log_data = insert_calls[0][0][0]
            assert log_data["username"] == "deleted_user"
            assert log_data["reason"] == "not_found"

    @pytest.mark.asyncio
    async def test_hiker_api_404_on_medias_logged_as_not_found(self) -> None:
        """HikerAPIError 404 при запросе постов → done + pre_filter_log: not_found."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "empty_acct"})
        db = make_db_mock()
        settings = make_settings(
            pre_filter_min_likes=30,
            pre_filter_max_inactive_days=180,
            pre_filter_posts_to_check=5,
        )

        call_count = 0

        async def mock_to_thread(func, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _make_user_info(follower_count=5000)
            raise HikerAPIError(404, "Entries not found")

        with (
            patch("src.worker.pre_filter_handler.asyncio.to_thread", side_effect=mock_to_thread),
            patch("src.worker.pre_filter_handler._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.pre_filter_handler._h.run_in_thread", new_callable=AsyncMock) as mock_run,
        ):
            await handle_pre_filter(db, task, MagicMock(), settings)

            assert mock_run.call_count == 2
            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: not_found"
            log_data = db.table.return_value.insert.call_args_list[0][0][0]
            assert log_data["reason"] == "not_found"
            assert log_data["platform_id"] == "12345"
            assert log_data["followers_count"] == 5000
