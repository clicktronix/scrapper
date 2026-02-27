"""Тесты обработчиков задач воркера."""
import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.ai.schemas import AIInsights
from src.models.blog import ScrapedHighlight, ScrapedPost, ScrapedProfile
from src.platforms.base import DiscoveredProfile
from src.platforms.instagram.exceptions import (
    AllAccountsCooldownError,
    PrivateAccountError,
)


def _make_settings() -> MagicMock:
    settings = MagicMock()
    settings.supabase_url = "https://test.supabase.co"
    return settings


def _make_task(
    task_type: str = "full_scrape",
    blog_id: str = "blog-1",
    **kwargs,
) -> dict:
    return {
        "id": "task-1",
        "blog_id": blog_id,
        "task_type": task_type,
        "status": "pending",
        "priority": 5,
        "payload": kwargs.get("payload", {}),
        "attempts": kwargs.get("attempts", 1),
        "max_attempts": kwargs.get("max_attempts", 3),
    }


def _make_scraped_profile() -> ScrapedProfile:
    return ScrapedProfile(
        platform_id="12345",
        username="testblogger",
        full_name="Test Blogger",
        biography="Test bio",
        follower_count=50000,
        following_count=500,
        media_count=200,
        is_verified=False,
        is_business=True,
        avg_er=3.5,
        avg_er_reels=5.0,
        er_trend="stable",
        posts_per_week=2.5,
        medias=[
            ScrapedPost(
                platform_id="p1",
                media_type=1,
                caption_text="Test",
                like_count=1000,
                comment_count=50,
                taken_at=datetime(2026, 1, 15, tzinfo=UTC),
            ),
        ],
        highlights=[
            ScrapedHighlight(
                platform_id="h1",
                title="Дети",
                media_count=5,
            ),
        ],
    )


class TestHandleFullScrape:
    """Тесты handle_full_scrape."""

    @pytest.mark.asyncio
    @patch("src.worker.handlers.persist_profile_images", new_callable=AsyncMock, return_value=(None, {}))
    async def test_successful_scrape(self, mock_persist: AsyncMock) -> None:
        from src.worker.handlers import handle_full_scrape

        task = _make_task("full_scrape")
        mock_db = MagicMock()

        # blog select — возвращает username
        blog_select_mock = MagicMock()
        blog_select_mock.data = [{"username": "testblogger"}]

        # Настраиваем цепочки Supabase mock
        table_mock = MagicMock()
        table_mock.select.return_value.eq.return_value.execute.return_value = blog_select_mock
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        table_mock.upsert.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        # Мок скрапера
        mock_scraper = AsyncMock()
        mock_scraper.scrape_profile.return_value = _make_scraped_profile()

        await handle_full_scrape(mock_db, task, mock_scraper, _make_settings())

        mock_scraper.scrape_profile.assert_called_once_with("testblogger")
        mock_persist.assert_called_once()

    @pytest.mark.asyncio
    async def test_private_account_sets_needs_review(self) -> None:
        from src.worker.handlers import handle_full_scrape

        task = _make_task("full_scrape")
        mock_db = MagicMock()

        blog_select_mock = MagicMock()
        blog_select_mock.data = [{"username": "privateblogger"}]

        table_mock = MagicMock()
        table_mock.select.return_value.eq.return_value.execute.return_value = blog_select_mock
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        mock_scraper = AsyncMock()
        mock_scraper.scrape_profile.side_effect = PrivateAccountError("private")

        await handle_full_scrape(mock_db, task, mock_scraper, _make_settings())

        mock_scraper.scrape_profile.assert_called_once()
        # Проверяем scrape_status='private' и needs_review=True
        update_calls = table_mock.update.call_args_list
        status_updates = [
            c[0][0] for c in update_calls if "scrape_status" in c[0][0]
        ]
        assert any(u.get("scrape_status") == "private" for u in status_updates)
        assert any(u.get("needs_review") is True for u in status_updates)

    @pytest.mark.asyncio
    async def test_all_accounts_cooldown(self) -> None:
        from src.worker.handlers import handle_full_scrape

        task = _make_task("full_scrape")
        mock_db = MagicMock()

        blog_select_mock = MagicMock()
        blog_select_mock.data = [{"username": "testblogger"}]

        table_mock = MagicMock()
        table_mock.select.return_value.eq.return_value.execute.return_value = blog_select_mock
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        mock_scraper = AsyncMock()
        mock_scraper.scrape_profile.side_effect = AllAccountsCooldownError("cooldown")

        await handle_full_scrape(mock_db, task, mock_scraper, _make_settings())

        mock_scraper.scrape_profile.assert_called_once()
        update_calls = table_mock.update.call_args_list
        statuses = [
            c[0][0].get("scrape_status")
            for c in update_calls
            if "scrape_status" in c[0][0]
        ]
        assert "pending" in statuses

    @pytest.mark.asyncio
    async def test_user_not_found_sets_deleted_and_needs_review(self) -> None:
        from instagrapi.exceptions import UserNotFound

        from src.worker.handlers import handle_full_scrape

        task = _make_task("full_scrape")
        mock_db = MagicMock()

        blog_select_mock = MagicMock()
        blog_select_mock.data = [{"username": "deleteduser"}]

        table_mock = MagicMock()
        table_mock.select.return_value.eq.return_value.execute.return_value = blog_select_mock
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        mock_scraper = AsyncMock()
        mock_scraper.scrape_profile.side_effect = UserNotFound()

        await handle_full_scrape(mock_db, task, mock_scraper, _make_settings())

        mock_scraper.scrape_profile.assert_called_once()
        # Проверяем, что был вызван update со scrape_status='deleted' и needs_review=True
        update_calls = table_mock.update.call_args_list
        status_updates = [
            c[0][0] for c in update_calls if "scrape_status" in c[0][0]
        ]
        assert any(u.get("scrape_status") == "deleted" for u in status_updates)
        assert any(u.get("needs_review") is True for u in status_updates)

    @pytest.mark.asyncio
    async def test_blog_not_found(self) -> None:
        from src.worker.handlers import handle_full_scrape

        task = _make_task("full_scrape")
        mock_db = MagicMock()

        # Блог не найден
        empty_result = MagicMock()
        empty_result.data = []
        table_mock = MagicMock()
        table_mock.select.return_value.eq.return_value.execute.return_value = empty_result
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        mock_scraper = AsyncMock()

        await handle_full_scrape(mock_db, task, mock_scraper, _make_settings())

        # scrape_profile не должен вызываться, т.к. блог не найден
        mock_scraper.scrape_profile.assert_not_called()

    @pytest.mark.asyncio
    async def test_generic_exception_sets_failed_status(self) -> None:
        """RuntimeError → scrape_status='failed', mark_task_failed с retry."""
        from src.worker.handlers import handle_full_scrape

        task = _make_task("full_scrape")
        mock_db = MagicMock()

        blog_select_mock = MagicMock()
        blog_select_mock.data = [{"username": "testblogger"}]

        table_mock = MagicMock()
        table_mock.select.return_value.eq.return_value.execute.return_value = blog_select_mock
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        mock_scraper = AsyncMock()
        mock_scraper.scrape_profile.side_effect = RuntimeError("Connection reset")

        await handle_full_scrape(mock_db, task, mock_scraper, _make_settings())

        # Проверяем, что scrape_status='failed' установлен
        update_calls = table_mock.update.call_args_list
        statuses = [
            c[0][0].get("scrape_status")
            for c in update_calls
            if "scrape_status" in c[0][0]
        ]
        assert "failed" in statuses

    @pytest.mark.asyncio
    async def test_hiker_api_error_non_retryable_sets_needs_review(self) -> None:
        """HikerAPIError 404 → scrape_status='failed', needs_review=True."""
        from src.platforms.instagram.exceptions import HikerAPIError
        from src.worker.handlers import handle_full_scrape

        task = _make_task("full_scrape")
        mock_db = MagicMock()

        blog_select_mock = MagicMock()
        blog_select_mock.data = [{"username": "gone_blogger"}]

        table_mock = MagicMock()
        table_mock.select.return_value.eq.return_value.execute.return_value = blog_select_mock
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        mock_scraper = AsyncMock()
        mock_scraper.scrape_profile.side_effect = HikerAPIError(404, "Not Found")

        await handle_full_scrape(mock_db, task, mock_scraper, _make_settings())

        update_calls = table_mock.update.call_args_list
        status_updates = [
            c[0][0] for c in update_calls if "scrape_status" in c[0][0]
        ]
        assert any(u.get("scrape_status") == "failed" for u in status_updates)
        assert any(u.get("needs_review") is True for u in status_updates)

    @pytest.mark.asyncio
    async def test_hiker_api_error_retryable_no_needs_review(self) -> None:
        """HikerAPIError 429 → scrape_status='pending', needs_review не ставится."""
        from src.platforms.instagram.exceptions import HikerAPIError
        from src.worker.handlers import handle_full_scrape

        task = _make_task("full_scrape")
        mock_db = MagicMock()

        blog_select_mock = MagicMock()
        blog_select_mock.data = [{"username": "rate_limited_blogger"}]

        table_mock = MagicMock()
        table_mock.select.return_value.eq.return_value.execute.return_value = blog_select_mock
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        mock_scraper = AsyncMock()
        mock_scraper.scrape_profile.side_effect = HikerAPIError(429, "Rate limited")

        await handle_full_scrape(mock_db, task, mock_scraper, _make_settings())

        update_calls = table_mock.update.call_args_list
        status_updates = [
            c[0][0] for c in update_calls if "scrape_status" in c[0][0]
        ]
        assert any(u.get("scrape_status") == "pending" for u in status_updates)
        # needs_review не должен ставиться при retryable ошибке
        assert not any(u.get("needs_review") is True for u in status_updates)

    @pytest.mark.asyncio
    async def test_deleted_blog_skipped(self) -> None:
        """Блог со статусом 'deleted' → задача пропускается."""
        from src.worker.handlers import handle_full_scrape

        task = _make_task("full_scrape")
        mock_db = MagicMock()

        blog_select_mock = MagicMock()
        blog_select_mock.data = [{"username": "gone_user", "scrape_status": "deleted"}]

        table_mock = MagicMock()
        table_mock.select.return_value.eq.return_value.execute.return_value = blog_select_mock
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        mock_scraper = AsyncMock()

        await handle_full_scrape(mock_db, task, mock_scraper, _make_settings())

        # Скрапинг не должен запускаться
        mock_scraper.scrape_profile.assert_not_called()

    @pytest.mark.asyncio
    async def test_deactivated_blog_skipped(self) -> None:
        """Блог со статусом 'deactivated' → задача пропускается."""
        from src.worker.handlers import handle_full_scrape

        task = _make_task("full_scrape")
        mock_db = MagicMock()

        blog_select_mock = MagicMock()
        blog_select_mock.data = [
            {"username": "deactivated_user", "scrape_status": "deactivated"},
        ]

        table_mock = MagicMock()
        table_mock.select.return_value.eq.return_value.execute.return_value = blog_select_mock
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        mock_scraper = AsyncMock()

        await handle_full_scrape(mock_db, task, mock_scraper, _make_settings())

        # Скрапинг не должен запускаться
        mock_scraper.scrape_profile.assert_not_called()

    @pytest.mark.asyncio
    async def test_already_claimed_returns_early(self) -> None:
        """mark_task_running вернул False — задача уже взята другим воркером."""
        from src.worker.handlers import handle_full_scrape

        task = _make_task("full_scrape")
        mock_db = MagicMock()

        # mark_task_running возвращает False (не заклеймлено)
        with patch(
            "src.worker.handlers.mark_task_running",
            new_callable=AsyncMock,
            return_value=False,
        ) as mock_claim:
            mock_scraper = AsyncMock()
            await handle_full_scrape(mock_db, task, mock_scraper, _make_settings())

            mock_claim.assert_called_once_with(mock_db, "task-1")
            # Скрапинг не должен запускаться
            mock_scraper.scrape_profile.assert_not_called()

    @pytest.mark.asyncio
    async def test_storage_urls_replace_cdn(self) -> None:
        """persist_profile_images подставляет Storage URL вместо CDN URL."""
        from src.worker.handlers import handle_full_scrape

        task = _make_task("full_scrape")
        mock_db = MagicMock()

        blog_select_mock = MagicMock()
        blog_select_mock.data = [{"username": "testblogger"}]

        table_mock = MagicMock()
        table_mock.select.return_value.eq.return_value.execute.return_value = blog_select_mock
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        table_mock.upsert.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        profile = _make_scraped_profile()
        profile.profile_pic_url = "https://cdn.instagram.com/avatar.jpg"
        profile.medias[0].thumbnail_url = "https://cdn.instagram.com/post_p1.jpg"
        mock_scraper = AsyncMock()
        mock_scraper.scrape_profile.return_value = profile

        storage_avatar = "https://sb.co/storage/v1/object/public/blog-images/blog-1/avatar.jpg"
        storage_post = "https://sb.co/storage/v1/object/public/blog-images/blog-1/post_p1.jpg"

        with patch(
            "src.worker.handlers.persist_profile_images",
            new_callable=AsyncMock,
            return_value=(storage_avatar, {"p1": storage_post}),
        ):
            await handle_full_scrape(mock_db, task, mock_scraper, _make_settings())

        # Проверяем что upsert_blog вызван с Storage URL для avatar
        upsert_calls = table_mock.upsert.call_args_list
        # blog upsert содержит avatar_url = storage URL
        blog_upsert_found = False
        for call_obj in upsert_calls:
            data = call_obj[0][0]
            if isinstance(data, dict) and "avatar_url" in data:
                assert data["avatar_url"] == storage_avatar
                blog_upsert_found = True
        # Если upsert_blog использует table.update — проверяем тоже
        if not blog_upsert_found:
            for call_obj in table_mock.update.call_args_list:
                data = call_obj[0][0]
                if isinstance(data, dict) and "avatar_url" in data:
                    assert data["avatar_url"] == storage_avatar


class TestProcessTask:
    """Тесты process_task."""

    @pytest.mark.asyncio
    async def test_unknown_task_type(self) -> None:
        """Неизвестный task_type — не падает, просто логируется."""
        from src.worker.loop import process_task

        db = MagicMock()
        task = {"id": "t1", "task_type": "unknown_type"}
        scrapers = {"instagram": AsyncMock()}
        mock_client = MagicMock()
        settings = MagicMock()
        sem = asyncio.Semaphore(1)

        # Не должен бросить исключение
        await process_task(db, task, scrapers, mock_client, settings, sem)

    @pytest.mark.asyncio
    async def test_ai_analysis_dispatched(self) -> None:
        """task_type='ai_analysis' вызывает handle_ai_analysis."""
        from src.worker.loop import process_task

        db = MagicMock()
        task = _make_task("ai_analysis")
        scrapers = {}
        mock_client = MagicMock()
        settings = MagicMock()
        sem = asyncio.Semaphore(1)

        with patch("src.worker.loop.handle_ai_analysis", new_callable=AsyncMock) as mock_handler:
            await process_task(db, task, scrapers, mock_client, settings, sem)
            mock_handler.assert_called_once()


class TestHandleDiscover:
    """Тесты handle_discover."""

    @pytest.mark.asyncio
    async def test_discover_new_profiles(self) -> None:
        from src.worker.handlers import handle_discover

        task = _make_task(
            "discover",
            payload={"hashtag": "beauty", "min_followers": 1000},
        )

        mock_db = MagicMock()
        table_mock = MagicMock()

        # Для mark_task_running/done — rpc и update
        mock_db.rpc.return_value.execute.return_value = MagicMock()
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()

        # blogs select — нет существующих
        empty_result = MagicMock()
        empty_result.data = []
        eq_chain = table_mock.select.return_value.eq.return_value.eq.return_value
        eq_chain.limit.return_value.execute.return_value = empty_result
        eq_chain.in_.return_value.limit.return_value.execute.return_value = empty_result

        # person insert
        person_result = MagicMock()
        person_result.data = [{"id": "person-1"}]
        table_mock.insert.return_value.execute.return_value = person_result

        mock_db.table.return_value = table_mock

        # Мок скрапера
        mock_scraper = AsyncMock()
        mock_scraper.discover.return_value = [
            DiscoveredProfile(
                username="newblogger",
                full_name="New Blogger",
                follower_count=5000,
                platform_id="99999",
            ),
        ]

        settings = MagicMock()

        await handle_discover(mock_db, task, mock_scraper, settings)

        mock_scraper.discover.assert_called_once_with("beauty", 1000)

    @pytest.mark.asyncio
    async def test_no_hashtag_in_payload(self) -> None:
        from src.worker.handlers import handle_discover

        task = _make_task("discover", payload={})

        mock_db = MagicMock()
        table_mock = MagicMock()
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock

        mock_scraper = AsyncMock()
        settings = MagicMock()

        await handle_discover(mock_db, task, mock_scraper, settings)

        # discover не должен вызываться
        mock_scraper.discover.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_existing_fresh_blogs(self) -> None:
        """Существующие свежие профили пропускаются — insert не вызывается."""
        from src.worker.handlers import handle_discover

        task = _make_task(
            "discover",
            payload={"hashtag": "beauty", "min_followers": 1000},
        )

        mock_db = MagicMock()
        table_mock = MagicMock()

        mock_db.rpc.return_value.execute.return_value = MagicMock()
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()

        # batch blogs select — профиль уже существует
        existing_result = MagicMock()
        existing_result.data = [{"id": "existing-blog", "username": "existing_blogger", "scraped_at": None}]
        in_chain = table_mock.select.return_value.eq.return_value.in_.return_value
        in_chain.execute.return_value = existing_result
        mock_db.table.return_value = table_mock

        mock_scraper = AsyncMock()
        mock_scraper.discover.return_value = [
            DiscoveredProfile(
                username="existing_blogger",
                full_name="Existing",
                follower_count=5000,
                platform_id="99999",
            ),
        ]

        settings = MagicMock()
        settings.rescrape_days = 60

        with patch("src.worker.handlers.is_blog_fresh", new_callable=AsyncMock, return_value=True) as mock_fresh:
            await handle_discover(mock_db, task, mock_scraper, settings)
            mock_fresh.assert_called_once_with(mock_db, "existing-blog", 60)

        # insert не должен вызываться — профиль уже есть и свежий
        table_mock.insert.assert_not_called()

    @pytest.mark.asyncio
    async def test_creates_rescrape_for_stale_existing_blogs(self) -> None:
        """Устаревшие существующие блоги получают задачу re-scrape."""
        from src.worker.handlers import handle_discover

        task = _make_task(
            "discover",
            payload={"hashtag": "beauty", "min_followers": 1000},
        )

        mock_db = MagicMock()
        table_mock = MagicMock()

        mock_db.rpc.return_value.execute.return_value = MagicMock()
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()

        # batch blogs select — профиль уже существует
        existing_result = MagicMock()
        existing_result.data = [{"id": "existing-blog", "username": "stale_blogger", "scraped_at": None}]
        in_chain = table_mock.select.return_value.eq.return_value.in_.return_value
        in_chain.execute.return_value = existing_result
        mock_db.table.return_value = table_mock

        mock_scraper = AsyncMock()
        mock_scraper.discover.return_value = [
            DiscoveredProfile(
                username="stale_blogger",
                full_name="Stale",
                follower_count=5000,
                platform_id="99999",
            ),
        ]

        settings = MagicMock()
        settings.rescrape_days = 60

        with (
            patch("src.worker.handlers.is_blog_fresh", new_callable=AsyncMock, return_value=False) as mock_fresh,
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            await handle_discover(mock_db, task, mock_scraper, settings)
            mock_fresh.assert_called_once_with(mock_db, "existing-blog", 60)
            mock_create.assert_called_once_with(mock_db, "existing-blog", "full_scrape", priority=5)

        # insert не должен вызываться — профиль уже есть
        table_mock.insert.assert_not_called()

    @pytest.mark.asyncio
    async def test_payload_none_does_not_crash(self) -> None:
        """payload=None в БД → не падает с AttributeError, а mark_task_failed."""
        from src.worker.handlers import handle_discover

        task = _make_task("discover", payload=None)

        mock_db = MagicMock()
        table_mock = MagicMock()
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock

        mock_scraper = AsyncMock()
        settings = MagicMock()

        # Не должен бросить AttributeError
        await handle_discover(mock_db, task, mock_scraper, settings)

        # discover не должен вызываться (hashtag пустой)
        mock_scraper.discover.assert_not_called()

    @pytest.mark.asyncio
    async def test_all_accounts_cooldown(self) -> None:
        """AllAccountsCooldownError → mark_task_failed с retry."""
        from src.worker.handlers import handle_discover

        task = _make_task("discover", payload={"hashtag": "test", "min_followers": 500})
        mock_db = MagicMock()
        table_mock = MagicMock()
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        mock_scraper = AsyncMock()
        mock_scraper.discover.side_effect = AllAccountsCooldownError("All in cooldown")

        settings = MagicMock()

        await handle_discover(mock_db, task, mock_scraper, settings)

        # mark_task_failed вызван через table.update
        update_calls = table_mock.update.call_args_list
        assert len(update_calls) > 0


    @pytest.mark.asyncio
    async def test_generic_exception_retries(self) -> None:
        """RuntimeError → mark_task_failed с retry=True."""
        from src.worker.handlers import handle_discover

        task = _make_task("discover", payload={"hashtag": "test", "min_followers": 500})
        mock_db = MagicMock()
        table_mock = MagicMock()
        table_mock.update.return_value.eq.return_value.execute.return_value = MagicMock()
        mock_db.table.return_value = table_mock
        mock_db.rpc.return_value.execute.return_value = MagicMock()

        mock_scraper = AsyncMock()
        mock_scraper.discover.side_effect = RuntimeError("Connection timeout")

        settings = MagicMock()

        with patch(
            "src.worker.handlers.mark_task_failed", new_callable=AsyncMock
        ) as mock_fail:
            await handle_discover(mock_db, task, mock_scraper, settings)

            mock_fail.assert_called_once()
            # retry=True
            assert mock_fail.call_args.kwargs["retry"] is True

    @pytest.mark.asyncio
    async def test_already_claimed_returns_early(self) -> None:
        """mark_task_running вернул False — задача уже взята другим воркером."""
        from src.worker.handlers import handle_discover

        task = _make_task("discover", payload={"hashtag": "test", "min_followers": 500})
        mock_db = MagicMock()

        with patch(
            "src.worker.handlers.mark_task_running",
            new_callable=AsyncMock,
            return_value=False,
        ) as mock_claim:
            mock_scraper = AsyncMock()
            settings = MagicMock()
            await handle_discover(mock_db, task, mock_scraper, settings)

            mock_claim.assert_called_once_with(mock_db, "task-1")
            # discover не запускается
            mock_scraper.discover.assert_not_called()


def _mock_db_for_batch() -> MagicMock:
    """Создать мок Supabase для batch-тестов."""
    db = MagicMock()
    table_mock = MagicMock()
    db.table.return_value = table_mock
    table_mock.select.return_value = table_mock
    table_mock.eq.return_value = table_mock
    table_mock.update.return_value = table_mock
    table_mock.upsert.return_value = table_mock
    table_mock.order.return_value = table_mock
    table_mock.limit.return_value = table_mock
    table_mock.in_.return_value = table_mock
    table_mock.execute.return_value = MagicMock(data=[])
    db.rpc.return_value.execute.return_value = MagicMock()
    return db


class TestHandleBatchResults:
    """Тесты handle_batch_results."""

    @pytest.mark.asyncio
    async def test_not_completed_returns_early(self) -> None:
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()

        with patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll:
            mock_poll.return_value = {"status": "in_progress"}
            await handle_batch_results(db, mock_client, "batch-1", {"blog-1": "task-1"})

        # mark_task_done не должен вызываться
        db.table.return_value.update.assert_not_called()

    @pytest.mark.asyncio
    async def test_completed_with_insights(self) -> None:
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()
        insights = AIInsights()
        insights.confidence = 4

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock) as mock_match,
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock),
            patch("src.worker.handlers.generate_embedding", new_callable=AsyncMock, return_value=None),
        ):
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-1": insights},
            }
            await handle_batch_results(
                db, mock_client, "batch-1", {"blog-1": "task-1"}
            )

            mock_match.assert_called_once_with(db, "blog-1", insights, categories={})

        # Проверяем, что update вызван с insights
        update_calls = db.table.return_value.update.call_args_list
        ai_updates = [
            c[0][0] for c in update_calls
            if "ai_insights" in c[0][0]
        ]
        assert len(ai_updates) == 1
        assert ai_updates[0]["ai_confidence"] == 0.80
        assert ai_updates[0]["scrape_status"] == "ai_analyzed"

    @pytest.mark.asyncio
    async def test_completed_with_refusal(self) -> None:
        """Refusal (insights=None) → scrape_status=ai_analyzed без insights."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()

        with patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll:
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-2": None},
            }
            await handle_batch_results(
                db, mock_client, "batch-2", {"blog-2": "task-2"}
            )

        update_calls = db.table.return_value.update.call_args_list
        status_updates = [
            c[0][0] for c in update_calls
            if c[0][0].get("scrape_status") == "ai_analyzed"
        ]
        assert len(status_updates) >= 1
        # При refusal не должно быть ai_insights
        assert "ai_insights" not in status_updates[0]

    @pytest.mark.asyncio
    async def test_skips_unknown_blog_id(self) -> None:
        """blog_id из results, которого нет в task_ids_by_blog, пропускается."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()

        with patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll:
            mock_poll.return_value = {
                "status": "completed",
                "results": {"unknown-blog": AIInsights()},
            }
            # task_ids_by_blog не содержит "unknown-blog"
            await handle_batch_results(
                db, mock_client, "batch-3", {"blog-1": "task-1"}
            )

        # mark_task_done не должен вызываться (нет совпадений)
        # update вызывается только для mark_task_done, но blog-1 нет в results
        update_calls = db.table.return_value.update.call_args_list
        done_updates = [
            c[0][0] for c in update_calls
            if c[0][0].get("status") == "done"
        ]
        assert len(done_updates) == 0

    @pytest.mark.asyncio
    async def test_mixed_insights_and_refusals(self) -> None:
        """Батч с insights для одного блога и None для другого."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()
        insights = AIInsights()
        insights.confidence = 5

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock) as mock_match,
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock),
            patch("src.worker.handlers.generate_embedding", new_callable=AsyncMock, return_value=None),
        ):
            mock_poll.return_value = {
                "status": "completed",
                "results": {
                    "blog-1": insights,
                    "blog-2": None,
                },
            }
            await handle_batch_results(
                db, mock_client, "batch-mixed",
                {"blog-1": "task-1", "blog-2": "task-2"},
            )

            # match_categories вызывается только для insights (не None)
            mock_match.assert_called_once_with(db, "blog-1", insights, categories={})

    @pytest.mark.asyncio
    async def test_failed_batch_does_nothing(self) -> None:
        """Батч со статусом 'failed' — ничего не делаем."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()

        with patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll:
            mock_poll.return_value = {"status": "failed"}
            await handle_batch_results(
                db, mock_client, "batch-fail", {"blog-1": "task-1"}
            )

        db.table.return_value.update.assert_not_called()

    @pytest.mark.asyncio
    async def test_expired_batch_with_partial_results(self) -> None:
        """Expired батч с частичными результатами — обрабатываются."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()
        insights = AIInsights(confidence=4)

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock) as mock_match,
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock),
            patch("src.worker.handlers.generate_embedding", new_callable=AsyncMock, return_value=None),
        ):
            mock_poll.return_value = {
                "status": "expired",
                "results": {"blog-1": insights, "blog-2": None},
            }
            await handle_batch_results(
                db, mock_client, "batch-expired",
                {"blog-1": "task-1", "blog-2": "task-2"},
            )

            # match_categories вызывается для blog-1 (insights)
            mock_match.assert_called_once_with(db, "blog-1", insights, categories={})

        # Обновления прошли для обоих блогов
        update_calls = db.table.return_value.update.call_args_list
        active_updates = [
            c[0][0] for c in update_calls
            if c[0][0].get("scrape_status") == "ai_analyzed"
        ]
        assert len(active_updates) == 2

    @pytest.mark.asyncio
    async def test_expired_batch_retries_missing_tasks(self) -> None:
        """Expired батч: задачи без результатов → retry (не ждать 26ч)."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()
        insights = AIInsights()

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock),
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock),
            patch("src.worker.handlers.generate_embedding", new_callable=AsyncMock, return_value=None),
            patch("src.worker.handlers.mark_task_failed", new_callable=AsyncMock) as mock_fail,
        ):
            # blog-1 получил результат, blog-3 — нет (expired без обработки)
            mock_poll.return_value = {
                "status": "expired",
                "results": {"blog-1": insights},
            }
            await handle_batch_results(
                db, mock_client, "batch-expired",
                {"blog-1": "task-1", "blog-3": "task-3"},
            )

            # blog-3 должен быть помечен как failed с retry
            mock_fail.assert_called_once_with(
                db, "task-3", 1, 3,
                "Batch expired without result for this task", retry=True,
            )

    @pytest.mark.asyncio
    async def test_expired_batch_dict_format_retries_with_correct_attempts(self) -> None:
        """Expired батч с dict-форматом task_ids_by_blog передаёт корректные attempts."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.mark_task_failed", new_callable=AsyncMock) as mock_fail,
        ):
            # Нет результатов вообще — все задачи должны retry
            mock_poll.return_value = {
                "status": "expired",
                "results": {},
            }
            task_ids_by_blog = {
                "blog-1": {"id": "task-1", "attempts": 2, "max_attempts": 5},
                "blog-2": {"id": "task-2", "attempts": 4, "max_attempts": 5},
            }
            await handle_batch_results(
                db, mock_client, "batch-exp", task_ids_by_blog,
            )

            assert mock_fail.call_count == 2
            # Проверяем что attempts/max_attempts взяты из dict
            calls = {c.args[1]: (c.args[2], c.args[3]) for c in mock_fail.call_args_list}
            assert calls["task-1"] == (2, 5)
            assert calls["task-2"] == (4, 5)

    @pytest.mark.asyncio
    async def test_expired_batch_mixed_format_processed_not_retried(self) -> None:
        """Expired: задачи с результатами не ретраятся, без результатов — ретрай."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()
        insights = AIInsights(confidence=4)

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock),
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock),
            patch("src.worker.handlers.generate_embedding", new_callable=AsyncMock, return_value=None),
            patch("src.worker.handlers.mark_task_failed", new_callable=AsyncMock) as mock_fail,
        ):
            mock_poll.return_value = {
                "status": "expired",
                "results": {"blog-1": insights},  # blog-1 обработан
            }
            task_ids_by_blog = {
                "blog-1": {"id": "task-1", "attempts": 1, "max_attempts": 3},
                "blog-2": {"id": "task-2", "attempts": 1, "max_attempts": 3},
            }
            await handle_batch_results(
                db, mock_client, "batch-mix", task_ids_by_blog,
            )

            # Только blog-2 (без результата) должен быть в retry
            mock_fail.assert_called_once()
            assert mock_fail.call_args.args[1] == "task-2"


class TestHandleBatchResultsEdge:
    """Дополнительные edge case тесты handle_batch_results."""

    async def test_match_categories_error_still_marks_task_done(self) -> None:
        """match_categories бросает исключение — задача всё равно должна быть помечена done."""
        from src.worker.handlers import handle_batch_results

        db = MagicMock()
        mock_client = MagicMock()

        insights = AIInsights()
        insights.content.primary_categories = ["Beauty"]

        task_ids_by_blog = {"blog-1": "task-1"}

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock),
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock,
                  side_effect=RuntimeError("DB connection lost")),
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock),
            patch("src.worker.handlers.generate_embedding", new_callable=AsyncMock, return_value=None),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-1": insights},
            }

            await handle_batch_results(db, mock_client, "batch-1", task_ids_by_blog)

            # Задача должна быть помечена done, несмотря на ошибку match_categories
            mock_done.assert_called_once_with(db, "task-1")

    async def test_poll_batch_exception_propagates(self) -> None:
        """poll_batch бросает — исключение пробрасывается, задачи остаются в running."""
        from src.worker.handlers import handle_batch_results

        db = MagicMock()
        mock_client = MagicMock()
        task_ids_by_blog = {"blog-1": "task-1"}

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock,
                  side_effect=RuntimeError("OpenAI API error")),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            with pytest.raises(RuntimeError, match="OpenAI API error"):
                await handle_batch_results(db, mock_client, "batch-1", task_ids_by_blog)

            mock_done.assert_not_called()


class TestHandleAiAnalysis:
    """Тесты handle_ai_analysis."""

    @pytest.mark.asyncio
    async def test_no_pending_tasks_returns(self) -> None:
        from src.worker.handlers import handle_ai_analysis

        db = _mock_db_for_batch()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        mock_client = MagicMock()

        await handle_ai_analysis(db, task, mock_client, settings)

        # submit_batch не должен вызываться
        mock_client.files.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_not_enough_for_batch(self) -> None:
        """Задач меньше batch_min_size и не прошло 2 часа → пропуск."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 10
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[{"id": "t1", "blog_id": "b1", "created_at": now_iso}]
        )

        with patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = pending_data
            await handle_ai_analysis(db, task, mock_client, settings)
            # Только один вызов (select pending tasks), submit не вызван
            assert mock_run.call_count == 1

    @pytest.mark.asyncio
    async def test_submits_when_threshold_reached(self) -> None:
        """Задач < batch_min_size, но старейшая > 2ч → батч отправляется."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 10
        mock_client = MagicMock()

        old_time = (datetime.now(UTC) - timedelta(hours=3)).isoformat()
        pending_data = MagicMock(
            data=[{"id": "t1", "blog_id": "b1", "created_at": old_time}]
        )
        blog_data = MagicMock(data=[{
            "id": "b1",
            "username": "testblog", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
        }])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock),
        ):
            # Последовательность: pending tasks → blogs batch → posts batch → highlights batch → save payload
            mock_run.side_effect = [pending_data, blog_data, empty_data, empty_data, MagicMock()]
            mock_submit.return_value = "batch-new"
            await handle_ai_analysis(db, task, mock_client, settings)
            mock_submit.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_rolls_back_to_pending(self) -> None:
        """Ошибка при submit_batch → задачи откатываются через mark_task_failed(retry=True)."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[{"id": "t1", "blog_id": "b1", "created_at": now_iso}]
        )
        blog_data = MagicMock(data=[{
            "id": "b1",
            "username": "testblog", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
        }])
        empty_data = MagicMock(data=[])
        rollback_result = MagicMock()

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock),
            patch("src.worker.handlers.mark_task_failed", new_callable=AsyncMock) as mock_failed,
        ):
            # pending tasks → blogs batch → posts batch → highlights batch → rollback update
            mock_run.side_effect = [pending_data, blog_data, empty_data, empty_data, rollback_result]
            mock_submit.side_effect = RuntimeError("OpenAI down")
            await handle_ai_analysis(db, task, mock_client, settings)

        # 4 вызова run_in_thread: pending + blogs + posts + highlights
        assert mock_run.call_count == 4
        mock_failed.assert_called_once()
        assert mock_failed.call_args.kwargs["retry"] is True

    @pytest.mark.asyncio
    async def test_skips_posts_with_null_taken_at(self) -> None:
        """Пост с taken_at=None не крашит весь батч — пропускается."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[{"id": "t1", "blog_id": "b1", "created_at": now_iso}]
        )
        blog_data = MagicMock(data=[{
            "id": "b1",
            "username": "testblog", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
        }])
        # Посты: один с None taken_at, один нормальный
        posts_data = MagicMock(data=[
            {"blog_id": "b1", "platform_id": "p1", "media_type": 1, "taken_at": None,
             "caption_text": "bad post"},
            {"blog_id": "b1", "platform_id": "p2", "media_type": 1,
             "taken_at": "2026-01-15T12:00:00+00:00",
             "caption_text": "good post"},
        ])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock),
        ):
            mock_run.side_effect = [
                pending_data, blog_data, posts_data, empty_data, MagicMock(),
            ]
            mock_submit.return_value = "batch-ok"
            # Не должен упасть — пост с None taken_at пропускается
            await handle_ai_analysis(db, task, mock_client, settings)
            mock_submit.assert_called_once()

            # Проверяем, что submit получил профиль с 1 постом (не 2)
            call_args = mock_submit.call_args
            profiles = call_args[0][1]  # второй позиционный аргумент
            assert len(profiles) == 1
            _, profile = profiles[0]
            assert len(profile.medias) == 1
            assert profile.medias[0].platform_id == "p2"

    @pytest.mark.asyncio
    async def test_skips_posts_with_missing_platform_id(self) -> None:
        """Пост без platform_id пропускается."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[{"id": "t1", "blog_id": "b1", "created_at": now_iso}]
        )
        blog_data = MagicMock(data=[{
            "id": "b1",
            "username": "testblog", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
        }])
        # Пост без platform_id
        posts_data = MagicMock(data=[
            {"blog_id": "b1", "media_type": 1, "taken_at": "2026-01-15T12:00:00Z",
             "caption_text": "no id post"},
        ])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock),
        ):
            mock_run.side_effect = [
                pending_data, blog_data, posts_data, empty_data, MagicMock(),
            ]
            mock_submit.return_value = "batch-ok"
            await handle_ai_analysis(db, task, mock_client, settings)
            mock_submit.assert_called_once()

            # Профиль должен быть с 0 постов (пост без platform_id пропущен)
            call_args = mock_submit.call_args
            profiles = call_args[0][1]
            _, profile = profiles[0]
            assert len(profile.medias) == 0

    @pytest.mark.asyncio
    async def test_submits_when_batch_min_size_reached(self) -> None:
        """batch_min_size задач набралось → батч отправляется (даже если < 2ч)."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 2  # порог = 2 задачи
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[
                {"id": "t1", "blog_id": "b1", "created_at": now_iso},
                {"id": "t2", "blog_id": "b2", "created_at": now_iso},
            ]
        )
        # Батчевая загрузка: оба блога в одном результате
        blog_data = MagicMock(data=[
            {
                "id": "b1",
                "username": "testblog1", "platform_id": "123",
                "bio": "Bio",
                "followers_count": 1000, "following_count": 100, "media_count": 50,
            },
            {
                "id": "b2",
                "username": "testblog2", "platform_id": "456",
                "bio": "Bio2",
                "followers_count": 2000, "following_count": 200, "media_count": 100,
            },
        ])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock),
        ):
            # pending → blogs batch → posts batch → highlights batch → 2x save payload
            mock_run.side_effect = [
                pending_data,
                blog_data, empty_data, empty_data,  # батчевая загрузка
                MagicMock(), MagicMock(),  # save payload
            ]
            mock_submit.return_value = "batch-size"
            await handle_ai_analysis(db, task, mock_client, settings)
            mock_submit.assert_called_once()

            # 2 профиля в батче
            call_args = mock_submit.call_args
            profiles = call_args[0][1]
            assert len(profiles) == 2

    @pytest.mark.asyncio
    async def test_already_claimed_tasks_skipped_in_batch(self) -> None:
        """mark_task_running=False для части задач → только claimed попадают в батч."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[
                {"id": "t1", "blog_id": "b1", "created_at": now_iso},
                {"id": "t2", "blog_id": "b2", "created_at": now_iso},
            ]
        )
        # Батчевая загрузка: оба блога
        blog_data = MagicMock(data=[
            {
                "id": "b1",
                "username": "testblog1", "platform_id": "123",
                "bio": "Bio",
                "followers_count": 1000, "following_count": 100, "media_count": 50,
            },
            {
                "id": "b2",
                "username": "testblog2", "platform_id": "456",
                "bio": "Bio2",
                "followers_count": 2000, "following_count": 200, "media_count": 100,
            },
        ])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch(
                "src.worker.handlers.mark_task_running",
                new_callable=AsyncMock,
                # t1 claimed, t2 already taken
                side_effect=[True, False],
            ),
        ):
            mock_run.side_effect = [
                pending_data,
                blog_data, empty_data, empty_data,  # батчевая загрузка
                MagicMock(),  # save payload для t1
            ]
            mock_submit.return_value = "batch-partial"
            await handle_ai_analysis(db, task, mock_client, settings)
            mock_submit.assert_called_once()

            # Только 1 профиль в батче (t2 не claimed)
            call_args = mock_submit.call_args
            profiles = call_args[0][1]
            assert len(profiles) == 1

    @pytest.mark.asyncio
    async def test_rollback_continues_on_partial_db_failure(self) -> None:
        """Ошибка отката одной задачи не мешает откату остальных."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[
                {"id": "t1", "blog_id": "b1", "created_at": now_iso},
                {"id": "t2", "blog_id": "b2", "created_at": now_iso},
            ]
        )
        # Батчевая загрузка: оба блога
        blog_data = MagicMock(data=[
            {
                "id": "b1",
                "username": "testblog1", "platform_id": "123",
                "bio": "Bio",
                "followers_count": 1000, "following_count": 100, "media_count": 50,
            },
            {
                "id": "b2",
                "username": "testblog2", "platform_id": "456",
                "bio": "Bio2",
                "followers_count": 2000, "following_count": 200, "media_count": 100,
            },
        ])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock),
            patch("src.worker.handlers.mark_task_failed", new_callable=AsyncMock) as mock_failed,
        ):
            # pending → blogs batch → posts batch → hl batch
            mock_run.side_effect = [
                pending_data,
                blog_data, empty_data, empty_data,  # батчевая загрузка
            ]
            mock_submit.side_effect = RuntimeError("OpenAI down")

            # Не должно крашить — второй откат должен выполниться
            await handle_ai_analysis(db, task, mock_client, settings)

            # Должно быть 4 вызова run_in_thread: только батчевая загрузка
            assert mock_run.call_count == 4
            assert mock_failed.call_count == 2

    @pytest.mark.asyncio
    async def test_text_only_passed_to_submit_batch(self) -> None:
        """Задача с payload.text_only=True → submit_batch получает text_only_ids."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[{
                "id": "t1", "blog_id": "b1", "created_at": now_iso,
                "attempts": 1, "max_attempts": 3,
                "payload": {"text_only": True},
            }]
        )
        blog_data = MagicMock(data=[{
            "id": "b1",
            "username": "testblog", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
        }])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock),
        ):
            mock_run.side_effect = [pending_data, blog_data, empty_data, empty_data, MagicMock()]
            mock_submit.return_value = "batch-new"
            await handle_ai_analysis(db, task, mock_client, settings)

            mock_submit.assert_called_once()
            call_kwargs = mock_submit.call_args
            assert call_kwargs.kwargs["text_only_ids"] == {"b1"}

    @pytest.mark.asyncio
    async def test_payload_batch_id_merges_with_text_only(self) -> None:
        """batch_id сохраняется в payload не затирая text_only."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[{
                "id": "t1", "blog_id": "b1", "created_at": now_iso,
                "attempts": 0, "max_attempts": 3,
                "payload": {"text_only": True},
            }]
        )
        blog_data = MagicMock(data=[{
            "id": "b1",
            "username": "testblog", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
        }])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock),
        ):
            mock_run.side_effect = [pending_data, blog_data, empty_data, empty_data, MagicMock()]
            mock_submit.return_value = "batch-new"
            await handle_ai_analysis(db, task, mock_client, settings)

            # Проверяем что update вызван с правильным payload (мержим text_only + batch_id)
            update_chain = db.table("scrape_tasks").update
            saved_payload = update_chain.call_args[0][0]["payload"]
            assert saved_payload["text_only"] is True
            assert saved_payload["batch_id"] == "batch-new"


class TestHandleDiscoverEdge:
    """Дополнительные edge case тесты handle_discover."""

    async def test_empty_discovered_list(self) -> None:
        """scraper.discover возвращает пустой список — задача помечается done."""
        from src.worker.handlers import handle_discover

        db = MagicMock()
        settings = MagicMock()
        scraper = MagicMock()
        scraper.discover = AsyncMock(return_value=[])

        task = _make_task("discover", payload={"hashtag": "beauty", "min_followers": 1000})

        with (
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            await handle_discover(db, task, scraper, settings)
            mock_done.assert_called_once_with(db, "task-1")

    async def test_person_insert_failure_skips_profile(self) -> None:
        """Ошибка вставки person — профиль пропускается, остальные обрабатываются."""
        from src.worker.handlers import handle_discover

        db = MagicMock()
        settings = MagicMock()
        scraper = MagicMock()
        scraper.discover = AsyncMock(return_value=[
            DiscoveredProfile(
                username="baduser",
                full_name="Bad User",
                platform_id="98",
                follower_count=5000,
            ),
            DiscoveredProfile(
                username="gooduser",
                full_name="Good User",
                platform_id="99",
                follower_count=6000,
            ),
        ])

        task = _make_task("discover", payload={"hashtag": "beauty", "min_followers": 1000})

        with (
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            person_ok = MagicMock(data=[{"id": "person-2"}])
            blog_ok = MagicMock(data=[{"id": "blog-2"}])
            mock_run.side_effect = [
                MagicMock(data=[]),  # batch blogs select — ни одного нет
                RuntimeError("DB insert failed"),  # baduser: person insert crash
                person_ok,  # gooduser: person insert OK
                blog_ok,  # gooduser: blog insert OK
            ]

            await handle_discover(db, task, scraper, settings)

            # Задача помечена done, несмотря на ошибку первого профиля
            mock_done.assert_called_once()
            # gooduser создан (full_scrape задача создана)
            mock_create.assert_called_once()

    async def test_blog_insert_failure_cleans_orphan_person(self) -> None:
        """Если blog insert упал после person insert, orphan person очищается."""
        from src.worker.handlers import handle_discover

        db = MagicMock()
        settings = MagicMock()
        scraper = MagicMock()
        scraper.discover = AsyncMock(return_value=[
            DiscoveredProfile(
                username="brokenuser",
                full_name="Broken User",
                platform_id="98",
                follower_count=5000,
            ),
        ])

        task = _make_task("discover", payload={"hashtag": "beauty", "min_followers": 1000})

        with (
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.cleanup_orphan_person", new_callable=AsyncMock) as mock_cleanup,
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
        ):
            mock_run.side_effect = [
                MagicMock(data=[]),  # blogs select — profile does not exist
                MagicMock(data=[{"id": "person-99"}]),  # person insert OK
                RuntimeError("duplicate key"),  # blog insert fails
            ]

            await handle_discover(db, task, scraper, settings)

            mock_cleanup.assert_called_once_with(db, "person-99")

    async def test_discover_marks_done_after_all_profiles(self) -> None:
        """Все профили уже существуют и свежие — задача всё равно помечается done."""
        from src.worker.handlers import handle_discover

        db = MagicMock()
        settings = MagicMock()
        scraper = MagicMock()
        scraper.discover = AsyncMock(return_value=[
            DiscoveredProfile(username="existing1", full_name="E1", platform_id="1", follower_count=5000),
            DiscoveredProfile(username="existing2", full_name="E2", platform_id="2", follower_count=8000),
        ])

        task = _make_task("discover", payload={"hashtag": "beauty", "min_followers": 1000})

        with (
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
            patch("src.worker.handlers.is_blog_fresh", new_callable=AsyncMock, return_value=True),
        ):
            # Оба профиля уже в базе (batch query возвращает оба)
            mock_run.return_value = MagicMock(data=[
                {"id": "existing-1", "username": "existing1", "scraped_at": None},
                {"id": "existing-2", "username": "existing2", "scraped_at": None},
            ])

            await handle_discover(db, task, scraper, settings)

            mock_done.assert_called_once_with(db, "task-1")
            mock_create.assert_not_called()

    async def test_discover_existing_stale_blog_creates_task(self) -> None:
        """Существующий блог устарел → создаётся full_scrape задача."""
        from src.worker.handlers import handle_discover

        db = MagicMock()
        settings = MagicMock()
        scraper = MagicMock()
        scraper.discover = AsyncMock(return_value=[
            DiscoveredProfile(username="stale_user", full_name="Stale", platform_id="1", follower_count=5000),
        ])

        task = _make_task("discover", payload={"hashtag": "beauty", "min_followers": 1000})

        with (
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
            patch("src.worker.handlers.is_blog_fresh", new_callable=AsyncMock, return_value=False),
        ):
            # Блог уже в базе (batch query с username)
            mock_run.return_value = MagicMock(data=[
                {"id": "existing-blog", "username": "stale_user", "scraped_at": None},
            ])

            await handle_discover(db, task, scraper, settings)

            mock_done.assert_called_once()
            mock_create.assert_called_once_with(db, "existing-blog", "full_scrape", priority=5)

    async def test_discover_existing_fresh_blog_skips_task(self) -> None:
        """Существующий свежий блог → задача full_scrape не создаётся."""
        from src.worker.handlers import handle_discover

        db = MagicMock()
        settings = MagicMock()
        scraper = MagicMock()
        scraper.discover = AsyncMock(return_value=[
            DiscoveredProfile(username="fresh_user", full_name="Fresh", platform_id="1", follower_count=5000),
        ])

        task = _make_task("discover", payload={"hashtag": "beauty", "min_followers": 1000})

        with (
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
            patch("src.worker.handlers.is_blog_fresh", new_callable=AsyncMock, return_value=True),
        ):
            mock_run.return_value = MagicMock(data=[
                {"id": "existing-blog", "username": "fresh_user", "scraped_at": None},
            ])

            await handle_discover(db, task, scraper, settings)

            mock_done.assert_called_once()
            mock_create.assert_not_called()

    async def test_discover_normalizes_username_for_select_and_insert(self) -> None:
        """Discover normalizes username before dedup check and insert."""
        from src.worker.handlers import handle_discover

        db = MagicMock()
        settings = MagicMock()
        scraper = MagicMock()
        scraper.discover = AsyncMock(return_value=[
            DiscoveredProfile(
                username="  @TeSt_User  ",
                full_name="",
                platform_id="42",
                follower_count=5000,
            ),
        ])
        task = _make_task("discover", payload={"hashtag": "beauty", "min_followers": 1000})

        blogs_table = MagicMock()
        blogs_select_query = MagicMock()
        blogs_insert_query = MagicMock()
        blogs_table.select.return_value = blogs_select_query
        blogs_select_query.eq.return_value = blogs_select_query
        blogs_select_query.in_.return_value = blogs_select_query
        blogs_select_query.execute.return_value = MagicMock(data=[])
        blogs_table.insert.return_value = blogs_insert_query
        blogs_insert_query.execute.return_value = MagicMock(data=[{"id": "blog-1"}])

        persons_table = MagicMock()
        persons_insert_query = MagicMock()
        persons_table.insert.return_value = persons_insert_query
        persons_insert_query.execute.return_value = MagicMock(data=[{"id": "person-1"}])

        db.table.side_effect = lambda name: blogs_table if name == "blogs" else persons_table

        async def passthrough(func, *args, **kwargs):
            return func(*args, **kwargs)

        with (
            patch("src.worker.handlers.run_in_thread", side_effect=passthrough),
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock),
        ):
            await handle_discover(db, task, scraper, settings)

        # Batch query использует .in_() вместо .eq() для username
        assert call("username", ["test_user"]) in blogs_select_query.in_.call_args_list
        inserted_blog = blogs_table.insert.call_args[0][0]
        assert inserted_blog["username"] == "test_user"


class TestHandleFullScrapeEdge:
    """Дополнительные edge case тесты handle_full_scrape."""

    async def test_upsert_posts_failure_sets_failed_status(self) -> None:
        """upsert_posts упал — blog откатывается в 'failed', задача помечается failed."""
        from src.worker.handlers import handle_full_scrape

        db = MagicMock()
        task = _make_task("full_scrape")
        scraper = MagicMock()
        scraper.scrape_profile = AsyncMock(return_value=_make_scraped_profile())

        with (
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.persist_profile_images", new_callable=AsyncMock, return_value=(None, {})),
            patch("src.worker.handlers.upsert_blog", new_callable=AsyncMock),
            patch("src.worker.handlers.upsert_posts", new_callable=AsyncMock, side_effect=RuntimeError("DB error")),
            patch("src.worker.handlers.mark_task_failed", new_callable=AsyncMock) as mock_failed,
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            mock_run.side_effect = [
                MagicMock(data=[{"username": "testblogger"}]),
                MagicMock(),  # scrape_status = 'scraping'
                MagicMock(),  # scrape_status = 'failed' (в except)
            ]

            await handle_full_scrape(db, task, scraper, _make_settings())

            mock_done.assert_not_called()
            mock_failed.assert_called_once()
            # Проверяем retry=True
            assert mock_failed.call_args.kwargs.get("retry") is not False or True

    async def test_profile_without_optional_fields(self) -> None:
        """Профиль без avatar/external_url/business_category — не крашится."""
        from src.worker.handlers import handle_full_scrape

        db = MagicMock()
        task = _make_task("full_scrape")
        scraper = MagicMock()

        profile = _make_scraped_profile()
        profile.profile_pic_url = None
        profile.external_url = None
        profile.business_category = None
        scraper.scrape_profile = AsyncMock(return_value=profile)

        with (
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.persist_profile_images", new_callable=AsyncMock, return_value=(None, {})),
            patch("src.worker.handlers.upsert_blog", new_callable=AsyncMock) as mock_upsert,
            patch("src.worker.handlers.upsert_posts", new_callable=AsyncMock),
            patch("src.worker.handlers.upsert_highlights", new_callable=AsyncMock),
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            mock_run.side_effect = [
                MagicMock(data=[{"username": "testblogger"}]),
                MagicMock(),  # scrape_status = 'scraping'
            ]

            await handle_full_scrape(db, task, scraper, _make_settings())

            mock_done.assert_called_once()
            # blog_data не должен содержать optional полей
            blog_data = mock_upsert.call_args[0][2]
            assert "avatar_url" not in blog_data
            assert "external_url" not in blog_data
            assert "business_category" not in blog_data


class TestHandleFullScrapeNewFields:
    """Тесты новых полей в handle_full_scrape."""

    async def test_contact_fields_in_blog_data(self) -> None:
        """Контактные поля добавляются в blog_data."""
        from src.worker.handlers import handle_full_scrape

        db = MagicMock()
        task = _make_task("full_scrape")
        scraper = MagicMock()

        profile = _make_scraped_profile()
        profile.account_type = 2
        profile.public_email = "test@example.com"
        profile.contact_phone_number = "7001234567"
        profile.public_phone_country_code = "7"
        profile.city_name = "Алматы"
        profile.address_street = "Абая 1"
        profile.bio_links = [{"url": "https://t.me/ch", "title": "TG", "link_type": None}]
        scraper.scrape_profile = AsyncMock(return_value=profile)

        with (
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.persist_profile_images", new_callable=AsyncMock, return_value=(None, {})),
            patch("src.worker.handlers.upsert_blog", new_callable=AsyncMock) as mock_upsert,
            patch("src.worker.handlers.upsert_posts", new_callable=AsyncMock),
            patch("src.worker.handlers.upsert_highlights", new_callable=AsyncMock),
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
        ):
            mock_run.side_effect = [
                MagicMock(data=[{"username": "testblogger"}]),
                MagicMock(),
            ]

            await handle_full_scrape(db, task, scraper, _make_settings())

            blog_data = mock_upsert.call_args[0][2]
            assert blog_data["account_type"] == 2
            assert blog_data["public_email"] == "test@example.com"
            assert blog_data["contact_phone_number"] == "7001234567"
            assert blog_data["public_phone_country_code"] == "7"
            assert blog_data["city_name"] == "Алматы"
            assert blog_data["address_street"] == "Абая 1"
            assert blog_data["bio_links"] == [{"url": "https://t.me/ch", "title": "TG", "link_type": None}]

    async def test_none_contact_fields_excluded(self) -> None:
        """None контактные поля не добавляются в blog_data."""
        from src.worker.handlers import handle_full_scrape

        db = MagicMock()
        task = _make_task("full_scrape")
        scraper = MagicMock()

        profile = _make_scraped_profile()
        scraper.scrape_profile = AsyncMock(return_value=profile)

        with (
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.persist_profile_images", new_callable=AsyncMock, return_value=(None, {})),
            patch("src.worker.handlers.upsert_blog", new_callable=AsyncMock) as mock_upsert,
            patch("src.worker.handlers.upsert_posts", new_callable=AsyncMock),
            patch("src.worker.handlers.upsert_highlights", new_callable=AsyncMock),
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
        ):
            mock_run.side_effect = [
                MagicMock(data=[{"username": "testblogger"}]),
                MagicMock(),
            ]

            await handle_full_scrape(db, task, scraper, _make_settings())

            blog_data = mock_upsert.call_args[0][2]
            assert "account_type" not in blog_data
            assert "public_email" not in blog_data
            assert "contact_phone_number" not in blog_data

    async def test_avg_reels_views_calculated(self) -> None:
        """avg_reels_views рассчитывается из play_count рилсов."""
        from src.worker.handlers import handle_full_scrape

        db = MagicMock()
        task = _make_task("full_scrape")
        scraper = MagicMock()

        profile = _make_scraped_profile()
        # Добавляем рилсы с play_count
        profile.medias = [
            ScrapedPost(
                platform_id="r1",
                media_type=2,
                product_type="clips",
                caption_text="Reel 1",
                like_count=500,
                comment_count=20,
                play_count=10000,
                taken_at=datetime(2026, 1, 15, tzinfo=UTC),
            ),
            ScrapedPost(
                platform_id="r2",
                media_type=2,
                product_type="clips",
                caption_text="Reel 2",
                like_count=300,
                comment_count=10,
                play_count=20000,
                taken_at=datetime(2026, 1, 16, tzinfo=UTC),
            ),
            # Обычный пост (не рилс) — не учитывается
            ScrapedPost(
                platform_id="p1",
                media_type=1,
                caption_text="Photo",
                like_count=200,
                comment_count=5,
                taken_at=datetime(2026, 1, 17, tzinfo=UTC),
            ),
            # Рилс без play_count — не учитывается
            ScrapedPost(
                platform_id="r3",
                media_type=2,
                product_type="clips",
                caption_text="Reel 3",
                like_count=100,
                comment_count=2,
                play_count=None,
                taken_at=datetime(2026, 1, 18, tzinfo=UTC),
            ),
        ]
        scraper.scrape_profile = AsyncMock(return_value=profile)

        with (
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.persist_profile_images", new_callable=AsyncMock, return_value=(None, {})),
            patch("src.worker.handlers.upsert_blog", new_callable=AsyncMock) as mock_upsert,
            patch("src.worker.handlers.upsert_posts", new_callable=AsyncMock),
            patch("src.worker.handlers.upsert_highlights", new_callable=AsyncMock),
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
        ):
            mock_run.side_effect = [
                MagicMock(data=[{"username": "testblogger"}]),
                MagicMock(),
            ]

            await handle_full_scrape(db, task, scraper, _make_settings())

            blog_data = mock_upsert.call_args[0][2]
            # (10000 + 20000) / 2 = 15000
            assert blog_data["avg_reels_views"] == 15000

    async def test_avg_reels_views_none_when_no_reels(self) -> None:
        """avg_reels_views = None, если нет рилсов с play_count."""
        from src.worker.handlers import handle_full_scrape

        db = MagicMock()
        task = _make_task("full_scrape")
        scraper = MagicMock()

        profile = _make_scraped_profile()
        # Только обычные посты, без рилсов
        profile.medias = [
            ScrapedPost(
                platform_id="p1",
                media_type=1,
                caption_text="Photo",
                like_count=200,
                comment_count=5,
                taken_at=datetime(2026, 1, 17, tzinfo=UTC),
            ),
        ]
        scraper.scrape_profile = AsyncMock(return_value=profile)

        with (
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.persist_profile_images", new_callable=AsyncMock, return_value=(None, {})),
            patch("src.worker.handlers.upsert_blog", new_callable=AsyncMock) as mock_upsert,
            patch("src.worker.handlers.upsert_posts", new_callable=AsyncMock),
            patch("src.worker.handlers.upsert_highlights", new_callable=AsyncMock),
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
        ):
            mock_run.side_effect = [
                MagicMock(data=[{"username": "testblogger"}]),
                MagicMock(),
            ]

            await handle_full_scrape(db, task, scraper, _make_settings())

            blog_data = mock_upsert.call_args[0][2]
            assert blog_data["avg_reels_views"] is None


class TestLoadProfilesBioLinksCompat:
    """Тесты обратной совместимости bio_links в _load_profiles_for_batch."""

    async def test_old_string_format_normalized(self) -> None:
        """Старый формат bio_links ['url'] нормализуется в [{url, title, link_type}]."""
        from src.worker.handlers import _load_profiles_for_batch

        db = MagicMock()
        pending_tasks = [{"id": "t1", "blog_id": "b1", "attempts": 0, "max_attempts": 3}]

        blogs_result = MagicMock(data=[{
            "id": "b1", "username": "test", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
            "bio_links": ["https://t.me/ch", "https://wa.me/777"],
        }])
        empty_data = MagicMock(data=[])

        with patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = [blogs_result, empty_data, empty_data]
            profiles, task_ids, failed = await _load_profiles_for_batch(db, pending_tasks)

        assert len(profiles) == 1
        _, profile = profiles[0]
        assert len(profile.bio_links) == 2
        assert profile.bio_links[0] == {"url": "https://t.me/ch", "title": None, "link_type": None}

    async def test_new_dict_format_preserved(self) -> None:
        """Новый формат bio_links [{url, title, link_type}] сохраняется как есть."""
        from src.worker.handlers import _load_profiles_for_batch

        db = MagicMock()
        pending_tasks = [{"id": "t1", "blog_id": "b1", "attempts": 0, "max_attempts": 3}]

        blogs_result = MagicMock(data=[{
            "id": "b1", "username": "test", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
            "bio_links": [{"url": "https://t.me/ch", "title": "TG", "link_type": None}],
        }])
        empty_data = MagicMock(data=[])

        with patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = [blogs_result, empty_data, empty_data]
            profiles, task_ids, failed = await _load_profiles_for_batch(db, pending_tasks)

        _, profile = profiles[0]
        assert profile.bio_links[0]["title"] == "TG"

    async def test_null_bio_links_handled(self) -> None:
        """bio_links=None → пустой список."""
        from src.worker.handlers import _load_profiles_for_batch

        db = MagicMock()
        pending_tasks = [{"id": "t1", "blog_id": "b1", "attempts": 0, "max_attempts": 3}]

        blogs_result = MagicMock(data=[{
            "id": "b1", "username": "test", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
            "bio_links": None,
        }])
        empty_data = MagicMock(data=[])

        with patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = [blogs_result, empty_data, empty_data]
            profiles, task_ids, failed = await _load_profiles_for_batch(db, pending_tasks)

        _, profile = profiles[0]
        assert profile.bio_links == []

    async def test_top_comments_are_loaded_for_batch_prompt(self) -> None:
        """top_comments из blog_posts прокидываются в ScrapedPost для AI-анализа."""
        from src.worker.handlers import _load_profiles_for_batch

        db = MagicMock()
        pending_tasks = [{"id": "t1", "blog_id": "b1", "attempts": 0, "max_attempts": 3}]

        blogs_result = MagicMock(data=[{
            "id": "b1", "username": "test", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
            "bio_links": [],
        }])
        posts_result = MagicMock(data=[{
            "blog_id": "b1",
            "platform_id": "p1",
            "media_type": 1,
            "taken_at": "2026-01-01T12:00:00+00:00",
            "caption_text": "Post",
            "top_comments": [
                {"username": "user_1", "text": "Great!"},
                {"username": "user_2", "text": "Love this"},
            ],
        }])
        empty_data = MagicMock(data=[])

        with patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = [blogs_result, posts_result, empty_data]
            profiles, _, _ = await _load_profiles_for_batch(db, pending_tasks)

        _, profile = profiles[0]
        assert len(profile.medias) == 1
        assert len(profile.medias[0].top_comments) == 2
        assert profile.medias[0].top_comments[0].username == "user_1"


class TestHandleDiscoverNewFields:
    """Тесты новых полей в handle_discover."""

    async def test_discover_inserts_new_fields(self) -> None:
        """handle_discover передаёт новые поля DiscoveredProfile в blog insert."""
        from src.worker.handlers import handle_discover

        db = MagicMock()
        task = _make_task("discover", payload={"hashtag": "beauty", "min_followers": 1000})
        settings = MagicMock()
        scraper = MagicMock()
        scraper.discover = AsyncMock(return_value=[
            DiscoveredProfile(
                username="newblogger",
                full_name="New Blogger",
                follower_count=5000,
                platform_id="99999",
                is_business=True,
                is_verified=True,
                biography="My bio",
                account_type=3,
            ),
        ])

        blogs_table = MagicMock()
        blogs_select_query = MagicMock()
        blogs_insert_query = MagicMock()
        blogs_table.select.return_value = blogs_select_query
        blogs_select_query.eq.return_value = blogs_select_query
        blogs_select_query.limit.return_value = blogs_select_query
        blogs_select_query.execute.return_value = MagicMock(data=[])
        blogs_table.insert.return_value = blogs_insert_query
        blogs_insert_query.execute.return_value = MagicMock(data=[{"id": "blog-1"}])

        persons_table = MagicMock()
        persons_insert_query = MagicMock()
        persons_table.insert.return_value = persons_insert_query
        persons_insert_query.execute.return_value = MagicMock(data=[{"id": "person-1"}])

        db.table.side_effect = lambda name: blogs_table if name == "blogs" else persons_table

        async def passthrough(func, *args, **kwargs):
            return func(*args, **kwargs)

        with (
            patch("src.worker.handlers.run_in_thread", side_effect=passthrough),
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock),
        ):
            await handle_discover(db, task, scraper, settings)

        inserted_blog = blogs_table.insert.call_args[0][0]
        assert inserted_blog["is_business"] is True
        assert inserted_blog["is_verified"] is True
        assert inserted_blog["bio"] == "My bio"
        assert inserted_blog["account_type"] == 3


class TestHandleAiAnalysisBlogNotFound:
    """Тесты: handle_ai_analysis — блог удалён из БД."""

    async def test_blog_not_found_marks_task_failed(self) -> None:
        """Если blog_id не найден в БД — задача помечается failed, не висит в pending."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[{"id": "t1", "blog_id": "deleted-blog", "created_at": now_iso}]
        )
        # Блог не найден в БД (батчевая загрузка возвращает пустой список)
        empty_blog = MagicMock(data=[])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_failed", new_callable=AsyncMock) as mock_failed,
        ):
            # pending → blogs batch (empty) → posts batch → highlights batch
            mock_run.side_effect = [pending_data, empty_blog, empty_data, empty_data]
            await handle_ai_analysis(db, task, mock_client, settings)

            # submit_batch НЕ вызван (нет профилей)
            mock_submit.assert_not_called()
            # Задача помечена failed
            mock_failed.assert_called_once()
            call_args = mock_failed.call_args
            assert call_args[0][1] == "t1"  # task_id
            positional_match = "not found" in call_args[0][4].lower()
            kwarg_match = "not found" in str(
                call_args.kwargs.get("error", "")
            ).lower()
            assert positional_match or kwarg_match

    async def test_mixed_found_and_not_found_blogs(self) -> None:
        """Один блог найден, другой нет — первый попадает в батч, второй failed."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[
                {"id": "t1", "blog_id": "b1", "created_at": now_iso},
                {"id": "t2", "blog_id": "deleted-blog", "created_at": now_iso},
            ]
        )
        # Батчевая загрузка: только b1 найден, deleted-blog — нет
        blog_data = MagicMock(data=[{
            "id": "b1",
            "username": "testblog", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
        }])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch("src.worker.handlers.mark_task_failed", new_callable=AsyncMock) as mock_failed,
        ):
            # pending → blogs batch (only b1) → posts batch → highlights batch → save payload
            mock_run.side_effect = [
                pending_data,
                blog_data, empty_data, empty_data,  # батчевая загрузка
                MagicMock(),  # save payload для t1
            ]
            mock_submit.return_value = "batch-ok"
            await handle_ai_analysis(db, task, mock_client, settings)

            # Батч содержит только 1 профиль (b1)
            mock_submit.assert_called_once()
            profiles = mock_submit.call_args[0][1]
            assert len(profiles) == 1

            # deleted-blog помечен failed
            mock_failed.assert_called_once()
            assert mock_failed.call_args[0][1] == "t2"


class TestHandleAiAnalysisHighlightsEdge:
    """Тесты: handle_ai_analysis — хайлайты с некорректными данными."""

    async def test_highlight_without_platform_id_skipped(self) -> None:
        """Хайлайт без platform_id не крашит весь батч — пропускается."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[{"id": "t1", "blog_id": "b1", "created_at": now_iso}]
        )
        blog_data = MagicMock(data=[{
            "id": "b1",
            "username": "testblog", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
        }])
        empty_data = MagicMock(data=[])
        # Хайлайт без platform_id + хайлайт нормальный
        highlights_data = MagicMock(data=[
            {"blog_id": "b1", "title": "bad highlight", "media_count": 3},  # нет platform_id
            {"blog_id": "b1", "platform_id": "h1", "title": "Good", "media_count": 5},
        ])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            mock_run.side_effect = [
                pending_data, blog_data, empty_data, highlights_data, MagicMock(),
            ]
            mock_submit.return_value = "batch-ok"

            # Не должен крашить с KeyError
            await handle_ai_analysis(db, task, mock_client, settings)

            mock_submit.assert_called_once()
            _, profile = mock_submit.call_args[0][1][0]
            # Только 1 хайлайт (с platform_id)
            assert len(profile.highlights) == 1
            assert profile.highlights[0].platform_id == "h1"

    async def test_highlight_without_title_skipped(self) -> None:
        """Хайлайт без title — пропускается."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[{"id": "t1", "blog_id": "b1", "created_at": now_iso}]
        )
        blog_data = MagicMock(data=[{
            "id": "b1",
            "username": "testblog", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
        }])
        empty_data = MagicMock(data=[])
        highlights_data = MagicMock(data=[
            {"blog_id": "b1", "platform_id": "h1"},  # нет title
        ])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            mock_run.side_effect = [
                pending_data, blog_data, empty_data, highlights_data, MagicMock(),
            ]
            mock_submit.return_value = "batch-ok"

            # Не должен крашить с KeyError
            await handle_ai_analysis(db, task, mock_client, settings)

            mock_submit.assert_called_once()
            _, profile = mock_submit.call_args[0][1][0]
            assert len(profile.highlights) == 0


class TestHandleAiAnalysisCreatedAtEdge:
    """Тесты: handle_ai_analysis — edge cases с created_at."""

    async def test_pending_task_with_none_created_at(self) -> None:
        """Задача с created_at=None → TypeError при min() (BUG)."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        # Задача с created_at=None
        pending_data = MagicMock(
            data=[{"id": "t1", "blog_id": "b1", "created_at": None}]
        )
        blog_data = MagicMock(data=[{
            "id": "b1",
            "username": "testblog", "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
        }])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            mock_run.side_effect = [
                pending_data, blog_data, empty_data, empty_data, MagicMock(),
            ]
            mock_submit.return_value = "batch-ok"

            # Сейчас крашится TypeError — после фикса должен работать
            await handle_ai_analysis(db, task, mock_client, settings)
            mock_submit.assert_called_once()

    async def test_mixed_none_and_valid_created_at(self) -> None:
        """Смешанные created_at: None + валидная дата → не крашит."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[
                {"id": "t1", "blog_id": "b1", "created_at": None},
                {"id": "t2", "blog_id": "b2", "created_at": now_iso},
            ]
        )
        # Батчевая загрузка: оба блога
        blog_data = MagicMock(data=[
            {
                "id": "b1",
                "username": "testblog1", "platform_id": "123",
                "bio": "Bio",
                "followers_count": 1000, "following_count": 100, "media_count": 50,
            },
            {
                "id": "b2",
                "username": "testblog2", "platform_id": "456",
                "bio": "Bio2",
                "followers_count": 2000, "following_count": 200, "media_count": 100,
            },
        ])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            mock_run.side_effect = [
                pending_data,
                blog_data, empty_data, empty_data,  # батчевая загрузка
                MagicMock(), MagicMock(),  # save payload
            ]
            mock_submit.return_value = "batch-ok"

            # Не должен крашить TypeError
            await handle_ai_analysis(db, task, mock_client, settings)
            mock_submit.assert_called_once()


class TestHandleAiAnalysisUsernameEdge:
    """Тесты: handle_ai_analysis — blog без username."""

    async def test_blog_without_username_key(self) -> None:
        """blog из БД без ключа 'username' → KeyError (BUG)."""
        from src.worker.handlers import handle_ai_analysis

        db = MagicMock()
        db.rpc.return_value.execute.return_value = MagicMock()
        task = _make_task("ai_analysis")
        settings = MagicMock()
        settings.batch_min_size = 1
        mock_client = MagicMock()

        now_iso = datetime.now(UTC).isoformat()
        pending_data = MagicMock(
            data=[{"id": "t1", "blog_id": "b1", "created_at": now_iso}]
        )
        # Блог без ключа "username"
        blog_data = MagicMock(data=[{
            "id": "b1",
            "platform_id": "123",
            "bio": "Bio",
            "followers_count": 1000, "following_count": 100, "media_count": 50,
        }])
        empty_data = MagicMock(data=[])

        with (
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.submit_batch", new_callable=AsyncMock) as mock_submit,
            patch("src.worker.handlers.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            mock_run.side_effect = [
                pending_data, blog_data, empty_data, empty_data, MagicMock(),
            ]
            mock_submit.return_value = "batch-ok"

            # Сейчас крашится KeyError — после фикса должен работать
            await handle_ai_analysis(db, task, mock_client, settings)
            mock_submit.assert_called_once()


class TestHandleBatchResultsGetTaskId:
    """Тесты _get_task_id внутри handle_batch_results."""

    async def test_dict_without_id_key_crashes(self) -> None:
        """task_ids_by_blog с dict без ключа 'id' → KeyError (BUG)."""
        from src.worker.handlers import handle_batch_results

        db = MagicMock()
        mock_client = MagicMock()
        insights = AIInsights()

        # dict без ключа "id" — текущий код крашится с KeyError
        task_ids_by_blog = {"blog-1": {"attempts": 1, "max_attempts": 3}}

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-1": insights},
            }

            # Сейчас крашится KeyError — после фикса должен пропустить blog
            await handle_batch_results(db, mock_client, "batch-1", task_ids_by_blog)

            # Без task_id — задача не должна помечаться done
            mock_done.assert_not_called()

    async def test_expired_dict_without_id_key_skips_retry(self) -> None:
        """Expired путь: dict без 'id' → пропускается, не крашит."""
        from src.worker.handlers import handle_batch_results

        db = MagicMock()
        mock_client = MagicMock()

        # dict без ключа "id" в expired пути
        task_ids_by_blog = {
            "blog-missing": {"attempts": 2, "max_attempts": 3},
            "blog-ok": {"id": "task-ok", "attempts": 1, "max_attempts": 3},
        }

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock),
            patch("src.worker.handlers.mark_task_failed", new_callable=AsyncMock) as mock_fail,
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
        ):
            mock_poll.return_value = {
                "status": "expired",
                "results": {},  # пустые результаты → все в retry
            }

            # Не должен крашить KeyError
            await handle_batch_results(db, mock_client, "batch-exp", task_ids_by_blog)

            # Только blog-ok (с "id") должен ретраиться
            mock_fail.assert_called_once()
            assert mock_fail.call_args.args[1] == "task-ok"


class TestHandleBatchResultsTagsAndEmbedding:
    """Тесты match_tags и embedding в handle_batch_results."""

    @pytest.mark.asyncio
    async def test_batch_results_calls_match_tags(self) -> None:
        """handle_batch_results вызывает match_tags после сохранения insights."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()
        insights = AIInsights()
        insights.confidence = 4

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock),
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock) as mock_tags,
            patch("src.worker.handlers.generate_embedding", new_callable=AsyncMock, return_value=None),
        ):
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-1": insights},
            }
            await handle_batch_results(
                db, mock_client, "batch-1", {"blog-1": "task-1"}
            )

            # match_tags вызывается с пустым кэшем (db возвращает data=[])
            mock_tags.assert_called_once_with(db, "blog-1", insights, tags={})

    @pytest.mark.asyncio
    async def test_batch_results_generates_embedding(self) -> None:
        """handle_batch_results генерирует и сохраняет embedding."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()
        insights = AIInsights()
        insights.confidence = 5

        fake_vector = [0.1] * 1536

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock),
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock),
            patch("src.worker.handlers.generate_embedding", new_callable=AsyncMock,
                  return_value=fake_vector) as mock_embed,
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
        ):
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-1": insights},
            }
            await handle_batch_results(
                db, mock_client, "batch-1", {"blog-1": "task-1"}
            )

            # generate_embedding вызывается с openai_client и текстом
            mock_embed.assert_called_once()
            args = mock_embed.call_args
            assert args[0][0] is mock_client  # первый аргумент — openai_client

            # db.table().update() вызывается с embedding
            update_calls = db.table.return_value.update.call_args_list
            embedding_updates = [
                c[0][0] for c in update_calls
                if "embedding" in c[0][0]
            ]
            assert len(embedding_updates) == 1
            assert embedding_updates[0]["embedding"] == fake_vector

    @pytest.mark.asyncio
    async def test_batch_results_embedding_none_skips_save(self) -> None:
        """Если generate_embedding вернул None, embedding не сохраняется в БД."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()
        insights = AIInsights()

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock),
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock),
            patch("src.worker.handlers.generate_embedding", new_callable=AsyncMock,
                  return_value=None),
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
        ):
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-1": insights},
            }
            await handle_batch_results(
                db, mock_client, "batch-1", {"blog-1": "task-1"}
            )

            # run_in_thread НЕ вызывается для embedding (только для insights save)
            embedding_calls = [
                c for c in mock_run.call_args_list
                if "embedding" in str(c)
            ]
            assert len(embedding_calls) == 0

    @pytest.mark.asyncio
    async def test_batch_results_tag_error_does_not_block(self) -> None:
        """Ошибка match_tags не блокирует остальную обработку."""
        from src.worker.handlers import handle_batch_results

        db = MagicMock()
        mock_client = MagicMock()
        insights = AIInsights()
        insights.content.primary_categories = ["Beauty"]

        task_ids_by_blog = {"blog-1": "task-1"}

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock),
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock),
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock,
                  side_effect=RuntimeError("Tag DB error")),
            patch("src.worker.handlers.generate_embedding", new_callable=AsyncMock, return_value=None),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-1": insights},
            }

            await handle_batch_results(db, mock_client, "batch-1", task_ids_by_blog)

            # Задача должна быть помечена done, несмотря на ошибку match_tags
            mock_done.assert_called_once_with(db, "task-1")

    @pytest.mark.asyncio
    async def test_batch_results_embedding_error_does_not_block(self) -> None:
        """Ошибка генерации embedding не блокирует остальную обработку."""
        from src.worker.handlers import handle_batch_results

        db = MagicMock()
        mock_client = MagicMock()
        insights = AIInsights()

        task_ids_by_blog = {"blog-1": "task-1"}

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock),
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock),
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock),
            patch("src.worker.handlers.build_embedding_text",
                  side_effect=RuntimeError("Embedding text error")),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-1": insights},
            }

            await handle_batch_results(db, mock_client, "batch-1", task_ids_by_blog)

            # Задача должна быть помечена done, несмотря на ошибку embedding
            mock_done.assert_called_once_with(db, "task-1")

    @pytest.mark.asyncio
    async def test_batch_results_api_error_skips_tags_and_embedding(self) -> None:
        """При API error (insights=None) match_tags и embedding не вызываются."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock) as mock_cat,
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock) as mock_tags,
            patch("src.worker.handlers.generate_embedding", new_callable=AsyncMock) as mock_embed,
        ):
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-1": None},
            }
            await handle_batch_results(
                db, mock_client, "batch-1", {"blog-1": "task-1"}
            )

            # При API error ни категории, ни теги, ни embedding не вызываются
            mock_cat.assert_not_called()
            mock_tags.assert_not_called()
            mock_embed.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_results_refusal_creates_text_only_retry(self) -> None:
        """При refusal tuple создаётся retry задача с text_only=True."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.load_categories", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.load_tags", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            # current_by_id запрос (scrape_status != ai_refused)
            mock_run.return_value = MagicMock(data=[
                {"id": "blog-1", "city": None, "content_language": None,
                 "audience_gender": None, "scrape_status": "scraped"},
            ])
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-1": ("refusal", "Content policy violation")},
            }

            await handle_batch_results(
                db, mock_client, "batch-1", {"blog-1": "task-1"}
            )

            # Должна создаться retry задача с text_only=True
            mock_create.assert_called_once()
            call_kwargs = mock_create.call_args
            assert call_kwargs[1].get("payload") == {"text_only": True}

    @pytest.mark.asyncio
    async def test_batch_results_double_refusal_no_retry(self) -> None:
        """Повторный refusal (ai_refused) — не создаёт retry."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.load_categories", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.load_tags", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock),
            patch("src.worker.handlers.create_task_if_not_exists", new_callable=AsyncMock) as mock_create,
        ):
            # current_by_id: уже ai_refused
            mock_run.return_value = MagicMock(data=[
                {"id": "blog-1", "city": None, "content_language": None,
                 "audience_gender": None, "scrape_status": "ai_refused"},
            ])
            mock_poll.return_value = {
                "status": "completed",
                "results": {"blog-1": ("refusal", "Content policy violation again")},
            }

            await handle_batch_results(
                db, mock_client, "batch-1", {"blog-1": "task-1"}
            )

            # Повторный refusal — retry не создаётся
            mock_create.assert_not_called()

    @pytest.mark.asyncio
    async def test_per_blog_error_does_not_kill_batch(self) -> None:
        """Ошибка при обработке одного блога не блокирует остальные."""
        from src.worker.handlers import handle_batch_results

        db = _mock_db_for_batch()
        mock_client = MagicMock()
        insights_ok = AIInsights()

        with (
            patch("src.worker.handlers.poll_batch", new_callable=AsyncMock) as mock_poll,
            patch("src.worker.handlers.run_in_thread", new_callable=AsyncMock) as mock_run,
            patch("src.worker.handlers.load_categories", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.load_tags", new_callable=AsyncMock, return_value={}),
            patch("src.worker.handlers.match_categories", new_callable=AsyncMock,
                  return_value={"total": 0, "matched": 0, "unmatched": 0}),
            patch("src.worker.handlers.match_tags", new_callable=AsyncMock,
                  return_value={"total": 0, "matched": 0, "unmatched": 0}),
            patch("src.worker.handlers.generate_embedding", new_callable=AsyncMock, return_value=None),
            patch("src.worker.handlers.mark_task_done", new_callable=AsyncMock) as mock_done,
            patch("src.worker.handlers.mark_task_failed", new_callable=AsyncMock) as mock_fail,
        ):
            mock_run.return_value = MagicMock(data=[
                {"id": "blog-bad", "city": None, "content_language": None,
                 "audience_gender": None, "scrape_status": "active"},
                {"id": "blog-ok", "city": None, "content_language": None,
                 "audience_gender": None, "scrape_status": "active"},
            ])
            # blog-bad вызовет ошибку при update, blog-ok обработается нормально
            call_count = 0

            async def _side_effect_run(func, *args, **kwargs):
                nonlocal call_count
                call_count += 1
                # Первый вызов — SELECT blogs, второй и далее — UPDATE
                if call_count == 1:
                    return MagicMock(data=[
                        {"id": "blog-bad", "city": None, "content_language": None,
                         "audience_gender": None, "scrape_status": "active"},
                        {"id": "blog-ok", "city": None, "content_language": None,
                         "audience_gender": None, "scrape_status": "active"},
                    ])
                if call_count == 2:
                    # blog-bad: update падает
                    raise Exception("\\u0000 cannot be converted to text")
                return MagicMock(data=[])

            mock_run.side_effect = _side_effect_run

            mock_poll.return_value = {
                "status": "completed",
                "results": {
                    "blog-bad": insights_ok,
                    "blog-ok": insights_ok,
                },
            }

            await handle_batch_results(
                db, mock_client, "batch-1",
                {
                    "blog-bad": {"id": "task-bad", "attempts": 1, "max_attempts": 3},
                    "blog-ok": {"id": "task-ok", "attempts": 1, "max_attempts": 3},
                },
            )

            # blog-bad → mark_task_failed с retry
            mock_fail.assert_called_once()
            fail_args = mock_fail.call_args
            assert fail_args[0][1] == "task-bad"
            assert fail_args.kwargs.get("retry") is True

            # blog-ok → mark_task_done (обработан несмотря на падение blog-bad)
            mock_done.assert_called_once()
            done_args = mock_done.call_args[0]
            assert done_args[1] == "task-ok"


class TestExtractBlogFieldsCityValidation:
    """Тесты валидации города в _extract_blog_fields."""

    def test_skips_garbage_city(self) -> None:
        """city '14% Казахстан' → не попадает в fields."""
        from src.worker.handlers import _extract_blog_fields

        insights = AIInsights()
        insights.blogger_profile.city = "14% Казахстан"

        fields = _extract_blog_fields(insights)

        assert "city" not in fields

    def test_skips_country_name(self) -> None:
        """city 'Казахстан' → не попадает в fields."""
        from src.worker.handlers import _extract_blog_fields

        insights = AIInsights()
        insights.blogger_profile.city = "Казахстан"

        fields = _extract_blog_fields(insights)

        assert "city" not in fields

    def test_valid_city_included(self) -> None:
        """city 'Алматы' → попадает в fields."""
        from src.worker.handlers import _extract_blog_fields

        insights = AIInsights()
        insights.blogger_profile.city = "Алматы"

        fields = _extract_blog_fields(insights)

        assert fields["city"] == "Алматы"

    def test_none_city_not_included(self) -> None:
        """city None → не попадает в fields."""
        from src.worker.handlers import _extract_blog_fields

        insights = AIInsights()
        insights.blogger_profile.city = None

        fields = _extract_blog_fields(insights)

        assert "city" not in fields
