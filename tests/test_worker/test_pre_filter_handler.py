"""Тесты обработчика pre_filter."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from instagrapi.exceptions import UserNotFound

from src.platforms.instagram.exceptions import (
    AllAccountsCooldownError,
    HikerAPIError,
    InsufficientBalanceError,
    PrivateAccountError,
)
from tests.conftest import make_db_mock, make_settings, make_task

_MOD = "src.worker.pre_filter_handler"


def _make_user_info(
    *,
    is_private: bool = False,
    pk: int = 12345,
    full_name: str = "Test User",
    follower_count: int = 10000,
    following_count: int = 500,
    media_count: int = 100,
    biography: str = "Test bio",
    is_verified: bool = False,
    is_business: bool = False,
    **extra: object,
) -> dict:
    """Фабрика ответа user_by_username_v2."""
    user: dict = {
        "pk": pk,
        "is_private": is_private,
        "full_name": full_name,
        "follower_count": follower_count,
        "following_count": following_count,
        "media_count": media_count,
        "biography": biography,
        "is_verified": is_verified,
        "is_business": is_business,
    }
    user.update(extra)
    return {"user": user}


def _make_medias(
    count: int = 5, like_count: int = 100, taken_at: datetime | None = None, likes_hidden: bool = False
) -> list:
    """Фабрика ответа user_medias_chunk_v1 / user_clips_chunk_v1 — [medias_list, cursor]."""
    if taken_at is None:
        taken_at = datetime.now(UTC) - timedelta(days=1)
    medias = [
        {
            "like_count": like_count,
            "taken_at": int(taken_at.timestamp()),
            "like_and_view_counts_disabled": likes_hidden,
        }
        for _ in range(count)
    ]
    return [medias, "next_cursor"]


def _empty_medias() -> list:
    """Пустой ответ медиа."""
    return [[], None]


def _make_scraper(
    user_info: dict | BaseException,
    posts: list | BaseException | None = None,
    clips: list | BaseException | None = None,
) -> MagicMock:
    """Фабрика scraper-мока — маршрутизирует по return_value/side_effect метода."""
    scraper = MagicMock()
    if isinstance(user_info, BaseException):
        scraper.cl.user_by_username_v2.side_effect = user_info
    else:
        scraper.cl.user_by_username_v2.return_value = user_info

    if posts is None:
        posts = _make_medias()
    if isinstance(posts, BaseException):
        scraper.cl.user_medias_chunk_v1.side_effect = posts
    else:
        scraper.cl.user_medias_chunk_v1.return_value = posts

    if clips is None:
        clips = _empty_medias()
    if isinstance(clips, BaseException):
        scraper.cl.user_clips_chunk_v1.side_effect = clips
    else:
        scraper.cl.user_clips_chunk_v1.return_value = clips

    return scraper


async def _mock_to_thread(func, *args, **kwargs):
    """Mock asyncio.to_thread — делегирует вызов в настроенный scraper mock."""
    return func(*args, **kwargs)


def _no_blog_result() -> MagicMock:
    """Результат проверки блога — блог не найден."""
    result = MagicMock()
    result.data = []
    return result


def _setup_db_execute(*results: MagicMock) -> MagicMock:
    """Создать db mock с последовательными результатами execute().

    Каждый вызов await db.table(...).execute() возвращает следующий результат.
    """
    db = make_db_mock()
    if results:
        db.table.return_value.execute = AsyncMock(side_effect=list(results))
    return db


def _pf_settings(**overrides) -> MagicMock:
    """Настройки с дефолтами для pre_filter тестов."""
    defaults = {
        "pre_filter_min_likes": 30,
        "pre_filter_max_inactive_days": 180,
        "pre_filter_posts_to_check": 5,
    }
    defaults.update(overrides)
    return make_settings(**defaults)


class TestHandlePreFilter:
    """Тесты handle_pre_filter."""

    @pytest.mark.asyncio
    async def test_private_account_filtered_out(self) -> None:
        """Приватный аккаунт → task done с filtered_out: private."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "private_user"})
        # 3 вызова execute(): проверка блога + update задачи + upsert в pre_filter_log
        db = _setup_db_execute(_no_blog_result(), MagicMock(), MagicMock())
        scraper = _make_scraper(_make_user_info(is_private=True))

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            assert db.table.return_value.execute.call_count == 3
            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: private"
            # Проверяем запись в pre_filter_log (upsert)
            upsert_calls = db.table.return_value.upsert.call_args_list
            assert len(upsert_calls) == 1
            log_data = upsert_calls[0][0][0]
            assert log_data["username"] == "private_user"
            assert log_data["reason"] == "private"

    @pytest.mark.asyncio
    async def test_low_engagement_filtered_out(self) -> None:
        """Средние лайки ниже порога → filtered_out: low_engagement."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "low_eng_user"})
        db = _setup_db_execute(_no_blog_result(), MagicMock(), MagicMock())
        scraper = _make_scraper(
            _make_user_info(),
            posts=_make_medias(count=3, like_count=10),
            clips=_make_medias(count=2, like_count=5),
        )

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: low_engagement"

    @pytest.mark.asyncio
    async def test_inactive_account_filtered_out(self) -> None:
        """Последний пост старше 180 дней → filtered_out: inactive."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "inactive_user"})
        db = _setup_db_execute(_no_blog_result(), MagicMock(), MagicMock())
        old_date = datetime.now(UTC) - timedelta(days=200)
        scraper = _make_scraper(
            _make_user_info(),
            posts=_make_medias(count=3, like_count=100, taken_at=old_date),
            clips=_empty_medias(),
        )

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: inactive"

    @pytest.mark.asyncio
    async def test_inactive_account_with_naive_iso_datetime(self) -> None:
        """Naive ISO datetime в taken_at корректно обрабатывается как UTC."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "inactive_iso_user"})
        db = _setup_db_execute(_no_blog_result(), MagicMock(), MagicMock())
        old_date_iso = (datetime.now(UTC) - timedelta(days=200)).replace(tzinfo=None).isoformat()
        posts = [[{"like_count": 100, "taken_at": old_date_iso}], None]
        scraper = _make_scraper(_make_user_info(), posts=posts, clips=_empty_medias())

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: inactive"

    @pytest.mark.asyncio
    async def test_inactive_posts_but_active_clips_passes(self) -> None:
        """Посты старые, но есть свежие рилсы → не inactive."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "reels_user"})
        old_date = datetime.now(UTC) - timedelta(days=200)
        recent_date = datetime.now(UTC) - timedelta(days=5)
        scraper = _make_scraper(
            _make_user_info(),
            posts=_make_medias(count=2, like_count=100, taken_at=old_date),
            clips=_make_medias(count=3, like_count=100, taken_at=recent_date),
        )

        person_result = MagicMock()
        person_result.data = [{"id": "person-1"}]
        blog_result = MagicMock()
        blog_result.data = [{"id": "blog-1"}]

        # execute(): проверка блога (пусто) + insert person + insert blog
        db = _setup_db_execute(_no_blog_result(), person_result, blog_result)

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch(f"{_MOD}._h.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())
            # Прошёл фильтр — не отфильтрован как inactive
            mock_done.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_posts_filtered_out(self) -> None:
        """Нет постов и рилсов → filtered_out: inactive."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "empty_user"})
        db = _setup_db_execute(_no_blog_result(), MagicMock(), MagicMock())
        scraper = _make_scraper(_make_user_info(), posts=_empty_medias(), clips=_empty_medias())

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: inactive"

    @pytest.mark.asyncio
    async def test_passed_creates_person_and_blog(self) -> None:
        """Профиль прошёл фильтр → создаётся person + blog со всеми данными из HikerAPI."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "good_user"})
        scraper = _make_scraper(
            _make_user_info(
                full_name="Good User",
                follower_count=50000,
                following_count=1200,
                media_count=250,
                biography="Travel blogger",
                is_verified=True,
                is_business=True,
                business_category_name="Travel",
                account_type=3,
                public_email="good@example.com",
                city_name="Moscow",
                external_url="https://example.com",
                bio_links=[{"url": "https://link.com", "title": "My site", "link_type": "external"}],
            ),
            posts=_make_medias(count=5, like_count=100),
        )

        person_result = MagicMock()
        person_result.data = [{"id": "person-1"}]
        blog_result = MagicMock()
        blog_result.data = [{"id": "blog-1"}]

        db = _setup_db_execute(_no_blog_result(), person_result, blog_result)

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch(f"{_MOD}._h.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            blog_insert_data = db.table.return_value.insert.call_args_list[-1][0][0]
            # Базовые поля
            assert blog_insert_data["source"] == "xlsx_import"
            assert blog_insert_data["scrape_status"] == "pending"
            assert blog_insert_data["platform"] == "instagram"
            assert blog_insert_data["username"] == "good_user"
            # Данные профиля из HikerAPI
            assert blog_insert_data["followers_count"] == 50000
            assert blog_insert_data["following_count"] == 1200
            assert blog_insert_data["media_count"] == 250
            assert blog_insert_data["bio"] == "Travel blogger"
            assert blog_insert_data["is_verified"] is True
            assert blog_insert_data["is_business"] is True
            assert blog_insert_data["business_category"] == "Travel"
            assert blog_insert_data["account_type"] == 3
            assert blog_insert_data["public_email"] == "good@example.com"
            assert blog_insert_data["city_name"] == "Moscow"
            assert blog_insert_data["external_url"] == "https://example.com"
            assert blog_insert_data["bio_links"] == [
                {"url": "https://link.com", "title": "My site", "link_type": "external"}
            ]
            mock_done.assert_called_once_with(db, task["id"])

    @pytest.mark.asyncio
    async def test_user_not_found_filtered_out(self) -> None:
        """UserNotFound → task done с filtered_out: not_found."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "ghost_user"})
        db = _setup_db_execute(_no_blog_result(), MagicMock(), MagicMock())
        scraper = _make_scraper(UserNotFound())

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: not_found"

    @pytest.mark.asyncio
    async def test_insufficient_balance_fails_no_retry(self) -> None:
        """InsufficientBalanceError → task failed, retry=False."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "test_user"})
        # execute() для проверки блога
        db = _setup_db_execute(_no_blog_result())
        scraper = _make_scraper(InsufficientBalanceError("No balance"))

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch(f"{_MOD}._h.mark_task_failed", new_callable=AsyncMock) as mock_failed,
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            mock_failed.assert_called_once()
            call_kwargs = mock_failed.call_args
            assert call_kwargs[1].get("retry") is False or call_kwargs[0][-1] is False

    @pytest.mark.asyncio
    async def test_hiker_api_404_logged_as_not_found(self) -> None:
        """HikerAPIError 404 при user_by_username → done + pre_filter_log: not_found."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "deleted_user"})
        # 3 вызова execute(): проверка блога + update задачи + upsert в pre_filter_log
        db = _setup_db_execute(_no_blog_result(), MagicMock(), MagicMock())
        scraper = _make_scraper(HikerAPIError(404, "Target user not found"))

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            assert db.table.return_value.execute.call_count == 3
            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: not_found"
            # Проверяем запись в pre_filter_log (upsert)
            upsert_calls = db.table.return_value.upsert.call_args_list
            assert len(upsert_calls) == 1
            log_data = upsert_calls[0][0][0]
            assert log_data["username"] == "deleted_user"
            assert log_data["reason"] == "not_found"

    @pytest.mark.asyncio
    async def test_hiker_api_404_on_medias_logged_as_not_found(self) -> None:
        """HikerAPIError 404 при запросе постов → done + pre_filter_log: not_found."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "empty_acct"})
        # 3 вызова execute(): проверка блога + update задачи + upsert в pre_filter_log
        db = _setup_db_execute(_no_blog_result(), MagicMock(), MagicMock())
        scraper = _make_scraper(
            _make_user_info(follower_count=5000),
            posts=HikerAPIError(404, "Entries not found"),
        )

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            assert db.table.return_value.execute.call_count == 3
            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: not_found"
            log_data = db.table.return_value.upsert.call_args_list[0][0][0]
            assert log_data["reason"] == "not_found"
            assert log_data["platform_id"] == "12345"
            assert log_data["followers_count"] == 5000

    @pytest.mark.asyncio
    async def test_hidden_likes_bypasses_engagement_filter(self) -> None:
        """Скрытые лайки (like_and_view_counts_disabled) → пропускаем фильтр по engagement."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "big_blogger"})
        # like_count=3 фиктивный, но like_and_view_counts_disabled=True
        scraper = _make_scraper(
            _make_user_info(follower_count=1_000_000),
            posts=_make_medias(count=5, like_count=3, likes_hidden=True),
            clips=_make_medias(count=5, like_count=0, likes_hidden=True),
        )

        person_result = MagicMock()
        person_result.data = [{"id": "person-1"}]
        blog_result = MagicMock()
        blog_result.data = [{"id": "blog-1"}]

        db = _setup_db_execute(_no_blog_result(), person_result, blog_result)

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch(f"{_MOD}._h.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())
            # Не отфильтрован — прошёл на создание blog
            mock_done.assert_called_once()

    @pytest.mark.asyncio
    async def test_genuine_low_engagement_still_filtered(self) -> None:
        """Аккаунт с 15k подписчиков и avg_likes=10 → реально низкий ER, фильтруем."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "small_blogger"})
        db = _setup_db_execute(_no_blog_result(), MagicMock(), MagicMock())
        # 15k подписчиков, avg_likes=10 → ER = 0.067% > 0.01%, данные надёжные
        scraper = _make_scraper(
            _make_user_info(follower_count=15_000),
            posts=_make_medias(count=3, like_count=10),
            clips=_make_medias(count=2, like_count=10),
        )

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: low_engagement"

    @pytest.mark.asyncio
    async def test_er_heuristic_detects_hidden_likes_for_large_accounts(self) -> None:
        """50K+ подписчиков и ER < 0.1% без флага → определяем как скрытые лайки, не фильтруем."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "hidden_likes_user"})
        # 200K подписчиков, like_count=5 (фейковый от HikerAPI), флаг НЕ выставлен
        scraper = _make_scraper(
            _make_user_info(follower_count=200_000),
            posts=_make_medias(count=5, like_count=5, likes_hidden=False),
            clips=_empty_medias(),
        )

        person_result = MagicMock()
        person_result.data = [{"id": "person-1"}]
        blog_result = MagicMock()
        blog_result.data = [{"id": "blog-1"}]

        db = _setup_db_execute(_no_blog_result(), person_result, blog_result)

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch(f"{_MOD}._h.mark_task_done", new_callable=AsyncMock) as mock_done,
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())
            # ER = 5/200000 = 0.0025% → hidden likes → НЕ фильтруем → создаём blog
            mock_done.assert_called_once()

    @pytest.mark.asyncio
    async def test_er_heuristic_skips_small_accounts(self) -> None:
        """<50K подписчиков — ER-эвристика не применяется, low_engagement фильтруется."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "small_shop"})
        db = _setup_db_execute(_no_blog_result(), MagicMock(), MagicMock())
        # 40K подписчиков, like_count=5 → ER = 0.0125%, но <50K → не hidden likes
        scraper = _make_scraper(
            _make_user_info(follower_count=40_000),
            posts=_make_medias(count=5, like_count=5, likes_hidden=False),
            clips=_empty_medias(),
        )

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["error_message"] == "filtered_out: low_engagement"

    # --- Новые тесты ---

    @pytest.mark.asyncio
    async def test_empty_username_fails_no_retry(self) -> None:
        """Пустой username в payload → mark_task_failed без retry."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={})
        db = make_db_mock()

        with patch(f"{_MOD}._h.mark_task_failed", new_callable=AsyncMock) as mock_failed:
            await handle_pre_filter(db, task, MagicMock(), _pf_settings())

            mock_failed.assert_called_once()
            args = mock_failed.call_args[0]
            assert "No username in payload" in args[4]
            assert mock_failed.call_args[1]["retry"] is False

    @pytest.mark.asyncio
    async def test_private_account_error_exception_filtered(self) -> None:
        """PrivateAccountError (исключение HikerAPI) → filtered_out: private."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "private_hiker"})
        db = _setup_db_execute(_no_blog_result(), MagicMock(), MagicMock())
        scraper = _make_scraper(PrivateAccountError("This account is private"))

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            assert db.table.return_value.execute.call_count == 3
            update_data = db.table.return_value.update.call_args_list[0][0][0]
            assert update_data["status"] == "done"
            assert update_data["error_message"] == "filtered_out: private"
            log_data = db.table.return_value.upsert.call_args_list[0][0][0]
            assert log_data["username"] == "private_hiker"
            assert log_data["reason"] == "private"

    @pytest.mark.asyncio
    async def test_all_accounts_cooldown_retries(self) -> None:
        """AllAccountsCooldownError → mark_task_failed с retry=True."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "cooldown_user"})
        db = _setup_db_execute(_no_blog_result())
        scraper = _make_scraper(AllAccountsCooldownError("All accounts on cooldown"))

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch(f"{_MOD}._h.mark_task_failed", new_callable=AsyncMock) as mock_failed,
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            mock_failed.assert_called_once()
            assert mock_failed.call_args[1]["retry"] is True

    @pytest.mark.asyncio
    async def test_blog_creation_error_cleans_up_person(self) -> None:
        """Ошибка при создании blog → cleanup_orphan_person + mark_task_failed."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "error_user"})
        scraper = _make_scraper(
            _make_user_info(follower_count=50000),
            posts=_make_medias(count=5, like_count=100),
        )

        person_result = MagicMock()
        person_result.data = [{"id": "person-orphan"}]

        # execute(): проверка блога + insert person + insert blog (ошибка)
        db = _setup_db_execute(
            _no_blog_result(),
            person_result,
            RuntimeError("DB connection lost"),
        )

        with (
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread),
            patch(f"{_MOD}._h.mark_task_running", new_callable=AsyncMock, return_value=True),
            patch(f"{_MOD}._h.cleanup_orphan_person", new_callable=AsyncMock) as mock_cleanup,
            patch(f"{_MOD}._h.mark_task_failed", new_callable=AsyncMock) as mock_failed,
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            mock_cleanup.assert_called_once_with(db, "person-orphan")
            mock_failed.assert_called_once()
            assert mock_failed.call_args[1]["retry"] is True

    @pytest.mark.asyncio
    async def test_existing_blog_skips_without_api_calls(self) -> None:
        """Если блог уже существует → mark_task_done, без HikerAPI запросов."""
        from src.worker.pre_filter_handler import handle_pre_filter

        task = make_task("pre_filter", blog_id=None, payload={"username": "existing_user"})
        scraper = _make_scraper(_make_user_info())

        # execute() возвращает результат с данными (блог найден)
        existing_result = MagicMock()
        existing_result.data = [{"id": "blog-existing"}]
        db = _setup_db_execute(existing_result)

        with (
            patch(f"{_MOD}._h.mark_task_done", new_callable=AsyncMock) as mock_done,
            patch(f"{_MOD}.asyncio.to_thread", side_effect=_mock_to_thread) as mock_api,
        ):
            await handle_pre_filter(db, task, scraper, _pf_settings())

            # Задача завершена без ошибок
            mock_done.assert_called_once_with(db, task["id"])
            # HikerAPI вызовы НЕ делались
            mock_api.assert_not_called()
