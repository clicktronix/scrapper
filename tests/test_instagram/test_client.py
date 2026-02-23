"""Тесты пула Instagram-аккаунтов."""
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from instagrapi.exceptions import (
    ChallengeRequired,
    ClientConnectionError,
    ClientError,
    ClientJSONDecodeError,
    ClientThrottledError,
    LoginRequired,
    PleaseWaitFewMinutes,
    RateLimitError,
    UserNotFound,
)

from src.platforms.instagram.client import AccountPool, AccountState
from src.platforms.instagram.exceptions import AllAccountsCooldownError, ScraperError


def _make_pool(num_accounts: int = 3) -> AccountPool:
    """Создать пул с заданным числом аккаунтов."""
    pool = AccountPool.__new__(AccountPool)
    pool.accounts = []
    pool.current_index = 0
    pool.requests_per_hour = 30
    pool.cooldown_minutes = 45
    pool._lock = asyncio.Lock()

    for i in range(num_accounts):
        state = AccountState(
            name=f"acc{i}",
            client=MagicMock(),
            proxy=f"http://proxy:{7000+i}",
            username=f"user{i}",
            password=f"pass{i}",
            is_available=True,
            cooldown_until=0,
            requests_this_hour=0,
            hour_started_at=time.time(),
        )
        pool.accounts.append(state)
    return pool


class TestAccountPool:
    """Тесты ротации и cooldown аккаунтов."""

    def _make_pool(self, num_accounts: int = 3):
        return _make_pool(num_accounts)

    def test_get_available_round_robin(self) -> None:
        """Ротация round-robin между аккаунтами."""
        pool = self._make_pool(3)

        acc1 = pool.get_available_account()
        assert acc1 is not None
        assert acc1.name == "acc0"

        acc2 = pool.get_available_account()
        assert acc2.name == "acc1"

        acc3 = pool.get_available_account()
        assert acc3.name == "acc2"

        # Цикл заново
        acc4 = pool.get_available_account()
        assert acc4.name == "acc0"

    def test_skip_cooldown_account(self) -> None:
        """Аккаунт в cooldown пропускается."""
        pool = self._make_pool(3)
        pool.accounts[0].cooldown_until = time.time() + 3600  # через час

        acc = pool.get_available_account()
        assert acc is not None
        assert acc.name == "acc1"

    def test_all_cooldown_returns_none(self) -> None:
        """Все в cooldown → None."""
        pool = self._make_pool(2)
        future = time.time() + 3600
        pool.accounts[0].cooldown_until = future
        pool.accounts[1].cooldown_until = future

        assert pool.get_available_account() is None

    def test_skip_hourly_limit(self) -> None:
        """Аккаунт достиг лимита запросов/час — пропускается."""
        pool = self._make_pool(2)
        pool.accounts[0].requests_this_hour = 30  # лимит

        acc = pool.get_available_account()
        assert acc is not None
        assert acc.name == "acc1"

    def test_hourly_counter_resets(self) -> None:
        """Счётчик сбрасывается через час."""
        pool = self._make_pool(1)
        pool.accounts[0].requests_this_hour = 30
        pool.accounts[0].hour_started_at = time.time() - 3601  # больше часа назад

        acc = pool.get_available_account()
        assert acc is not None
        assert acc.requests_this_hour == 0

    def test_mark_rate_limited(self) -> None:
        """mark_rate_limited устанавливает cooldown."""
        pool = self._make_pool(1)
        acc = pool.accounts[0]
        pool.mark_rate_limited(acc)
        assert acc.cooldown_until > time.time()

    def test_mark_challenge(self) -> None:
        """mark_challenge устанавливает двойной cooldown."""
        pool = self._make_pool(1)
        acc = pool.accounts[0]
        pool.mark_challenge(acc)
        # Двойной cooldown
        expected_min = time.time() + (pool.cooldown_minutes * 2 * 60) - 5
        assert acc.cooldown_until > expected_min

    def test_increment_requests(self) -> None:
        """increment_requests увеличивает счётчик."""
        pool = self._make_pool(1)
        acc = pool.accounts[0]
        assert acc.requests_this_hour == 0
        pool.increment_requests(acc)
        assert acc.requests_this_hour == 1
        pool.increment_requests(acc)
        assert acc.requests_this_hour == 2


class TestSafeRequest:
    """Тесты safe_request — ротация аккаунтов при ошибках."""

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_successful_request(self, _sleep) -> None:
        """Успешный запрос — результат возвращается, счётчик инкрементирован."""
        pool = _make_pool(1)

        def func(client, arg):
            return f"result-{arg}"

        result = await pool.safe_request(func, "test")
        assert result == "result-test"
        assert pool.accounts[0].requests_this_hour == 1

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_rate_limit_rotates_account(self, _sleep) -> None:
        """RateLimitError → cooldown первого, запрос ко второму."""
        pool = _make_pool(2)
        call_count = 0

        def func(client, arg):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RateLimitError()
            return "ok"

        result = await pool.safe_request(func, "test")
        assert result == "ok"
        # Первый аккаунт в cooldown
        assert pool.accounts[0].cooldown_until > time.time()

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_please_wait_rotates_account(self, _sleep) -> None:
        """PleaseWaitFewMinutes → cooldown, ротация."""
        pool = _make_pool(2)
        call_count = 0

        def func(client, arg):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise PleaseWaitFewMinutes()
            return "ok"

        result = await pool.safe_request(func, "test")
        assert result == "ok"

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_challenge_rotates_account(self, _sleep) -> None:
        """ChallengeRequired → двойной cooldown, ротация."""
        pool = _make_pool(2)
        call_count = 0

        def func(client, arg):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ChallengeRequired()
            return "ok"

        result = await pool.safe_request(func, "test")
        assert result == "ok"
        # Двойной cooldown
        expected = time.time() + (pool.cooldown_minutes * 2 * 60) - 5
        assert pool.accounts[0].cooldown_until > expected

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_user_not_found_raises_immediately(self, _sleep) -> None:
        """UserNotFound → raise без retry."""
        pool = _make_pool(2)

        def func(client, arg):
            raise UserNotFound()

        with pytest.raises(UserNotFound):
            await pool.safe_request(func, "test")

        # Счётчик увеличен (запрос был сделан)
        assert pool.accounts[0].requests_this_hour == 1

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_client_error_raises_scraper_error(self, _sleep) -> None:
        """ClientError → ScraperError без retry."""
        pool = _make_pool(2)

        def func(client, arg):
            raise ClientError("some error")

        with pytest.raises(ScraperError, match="Instagram client error"):
            await pool.safe_request(func, "test")

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_all_accounts_exhausted_raises(self, _sleep) -> None:
        """Все аккаунты в cooldown после MAX_RETRIES → AllAccountsCooldownError."""
        pool = _make_pool(1)

        def func(client, arg):
            raise RateLimitError()

        with pytest.raises(AllAccountsCooldownError):
            await pool.safe_request(func, "test")

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_login_required_triggers_relogin(self, _sleep) -> None:
        """LoginRequired → re-login, повторный запрос."""
        pool = _make_pool(1)
        call_count = 0

        def func(client, arg):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise LoginRequired()
            return "after-relogin"

        # Mock login для re-login
        pool.accounts[0].client.login = MagicMock()

        result = await pool.safe_request(func, "test")
        assert result == "after-relogin"
        pool.accounts[0].client.login.assert_called_once_with("user0", "pass0")


class TestTryRelogin:
    """Тесты _try_relogin."""

    @pytest.mark.asyncio
    async def test_successful_relogin(self) -> None:
        """Успешный re-login возвращает True."""
        pool = _make_pool(1)
        acc = pool.accounts[0]
        acc.client.login = MagicMock()

        result = await pool._try_relogin(acc)
        assert result is True
        acc.client.login.assert_called_once_with("user0", "pass0")

    @pytest.mark.asyncio
    async def test_failed_relogin(self) -> None:
        """Неудачный re-login → cooldown + возвращает False."""
        pool = _make_pool(1)
        acc = pool.accounts[0]
        acc.client.login = MagicMock(side_effect=Exception("login failed"))

        result = await pool._try_relogin(acc)
        assert result is False
        assert acc.cooldown_until > time.time()


class TestSaveAllSessions:
    """Тесты save_all_sessions."""

    @pytest.mark.asyncio
    async def test_saves_all_accounts(self) -> None:
        """Сессии всех аккаунтов сохраняются."""
        pool = _make_pool(3)
        mock_db = MagicMock()

        for acc in pool.accounts:
            acc.client.get_settings.return_value = {"session": "data"}

        with patch("src.platforms.instagram.client.save_session", new_callable=AsyncMock) as mock_save:
            await pool.save_all_sessions(mock_db)
            assert mock_save.call_count == 3

    @pytest.mark.asyncio
    async def test_continues_on_error(self) -> None:
        """Ошибка в одном аккаунте не мешает сохранению остальных."""
        pool = _make_pool(3)
        mock_db = MagicMock()

        pool.accounts[0].client.get_settings.side_effect = RuntimeError("fail")
        pool.accounts[1].client.get_settings.return_value = {"session": "ok"}
        pool.accounts[2].client.get_settings.return_value = {"session": "ok"}

        with patch("src.platforms.instagram.client.save_session", new_callable=AsyncMock) as mock_save:
            await pool.save_all_sessions(mock_db)
            # Только 2 успешных сохранения (первый аккаунт упал на get_settings)
            assert mock_save.call_count == 2


class TestAccountPoolEdgeCases:
    """Крайние случаи AccountPool."""

    def test_empty_pool_returns_none(self) -> None:
        """Пустой пул → get_available_account() возвращает None."""
        pool = _make_pool(0)
        assert pool.get_available_account() is None

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_empty_pool_safe_request_raises(self, _sleep) -> None:
        """safe_request с пустым пулом → AllAccountsCooldownError."""
        pool = _make_pool(0)

        def func(client):
            return "ok"

        with pytest.raises(AllAccountsCooldownError):
            await pool.safe_request(func)

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_login_required_failed_relogin_retries_other(self, _sleep) -> None:
        """LoginRequired → re-login fail → cooldown → пробует другой аккаунт."""
        pool = _make_pool(2)
        call_count = 0

        def func(client, arg):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise LoginRequired()
            return "ok-from-second-retry"

        # Первый аккаунт: LoginRequired → re-login fail → cooldown
        pool.accounts[0].client.login = MagicMock(side_effect=Exception("login fail"))
        # Второй аккаунт: LoginRequired → re-login ok → запрос ok
        pool.accounts[1].client.login = MagicMock()

        result = await pool.safe_request(func, "test")
        assert result == "ok-from-second-retry"

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_max_retries_exhausted_with_rate_limits(self, _sleep) -> None:
        """Все MAX_RETRIES попыток исчерпаны → AllAccountsCooldownError."""
        pool = _make_pool(3)

        def func(client, arg):
            raise RateLimitError()

        with pytest.raises(AllAccountsCooldownError, match="Failed after"):
            await pool.safe_request(func, "test")

    def test_single_account_at_hourly_limit_returns_none(self) -> None:
        """Единственный аккаунт достиг лимита → None."""
        pool = _make_pool(1)
        pool.accounts[0].requests_this_hour = pool.requests_per_hour

        assert pool.get_available_account() is None

    def test_cooldown_expired_account_available(self) -> None:
        """Аккаунт с истёкшим cooldown снова доступен."""
        pool = _make_pool(1)
        pool.accounts[0].cooldown_until = time.time() - 1  # в прошлом

        acc = pool.get_available_account()
        assert acc is not None
        assert acc.name == "acc0"

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_login_required_relogin_ok_but_retry_fails(self, _sleep) -> None:
        """LoginRequired → re-login ok → повторный запрос тоже падает → cooldown."""
        pool = _make_pool(2)
        call_count = 0

        def func(client, arg):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                # 1й вызов: LoginRequired, 2й (после re-login): тоже ошибка
                if call_count == 1:
                    raise LoginRequired()
                raise RuntimeError("still broken")
            return "ok"

        pool.accounts[0].client.login = MagicMock()

        result = await pool.safe_request(func, "test")
        assert result == "ok"
        # Первый аккаунт должен быть в cooldown
        assert pool.accounts[0].cooldown_until > time.time()


class TestTransientErrorHandling:
    """Тесты обработки транзиентных ошибок (ClientConnectionError, ClientJSONDecodeError, ClientThrottledError)."""

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_client_connection_error_retries(self, _sleep) -> None:
        """ClientConnectionError → cooldown + retry с другим аккаунтом."""
        pool = _make_pool(2)
        call_count = 0

        def func(client, arg):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ClientConnectionError("Connection reset")
            return "ok"

        result = await pool.safe_request(func, "test")
        assert result == "ok"
        # Первый аккаунт в cooldown
        assert pool.accounts[0].cooldown_until > time.time()

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_client_json_decode_error_retries(self, _sleep) -> None:
        """ClientJSONDecodeError → cooldown + retry."""
        pool = _make_pool(2)
        call_count = 0

        def func(client, arg):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ClientJSONDecodeError("Invalid JSON from Instagram")
            return "ok"

        result = await pool.safe_request(func, "test")
        assert result == "ok"
        assert pool.accounts[0].cooldown_until > time.time()

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_client_throttled_error_retries(self, _sleep) -> None:
        """ClientThrottledError (HTTP 429) → cooldown + retry."""
        pool = _make_pool(2)
        call_count = 0

        def func(client, arg):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ClientThrottledError("429 Too Many Requests")
            return "ok"

        result = await pool.safe_request(func, "test")
        assert result == "ok"
        assert pool.accounts[0].cooldown_until > time.time()

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_all_transient_errors_exhaust_retries(self, _sleep) -> None:
        """Все аккаунты дают ClientConnectionError → AllAccountsCooldownError."""
        pool = _make_pool(2)

        def func(client, arg):
            raise ClientConnectionError("Network down")

        with pytest.raises(AllAccountsCooldownError):
            await pool.safe_request(func, "test")

    @pytest.mark.asyncio
    @patch("src.platforms.instagram.client.asyncio.sleep", return_value=None)
    async def test_client_login_required_relogin_flow(self, _sleep) -> None:
        """ClientLoginRequired (ClientError subclass) обрабатывается как re-login path."""
        pool = _make_pool(1)
        call_count = 0

        class ClientLoginRequired(ClientError):
            pass

        def func(client, arg):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ClientLoginRequired("login required")
            return "ok-after-client-login-required"

        pool.accounts[0].client.login = MagicMock()
        result = await pool.safe_request(func, "test")

        assert result == "ok-after-client-login-required"
        pool.accounts[0].client.login.assert_called_once_with("user0", "pass0")
