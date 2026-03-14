"""Тесты RateLimiter — in-memory rate limiter."""
import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from src.api.rate_limiter import RateLimiter


def _make_request(ip: str = "127.0.0.1", forwarded_for: str | None = None) -> MagicMock:
    """Создать мок Request с указанным IP."""
    request = MagicMock()
    request.client.host = ip
    request.headers.get.return_value = forwarded_for
    return request


class TestRateLimiter:
    """Тесты класса RateLimiter."""

    @pytest.mark.asyncio
    async def test_allows_requests_within_limit(self) -> None:
        """Запросы в пределах лимита проходят без ошибок."""
        limiter = RateLimiter(max_requests=5, window_seconds=60)
        request = _make_request()

        for _ in range(5):
            await limiter.check(request)

    @pytest.mark.asyncio
    async def test_blocks_requests_over_limit(self) -> None:
        """Запросы сверх лимита → HTTPException 429."""
        limiter = RateLimiter(max_requests=3, window_seconds=60)
        request = _make_request()

        for _ in range(3):
            await limiter.check(request)

        with pytest.raises(HTTPException) as exc_info:
            await limiter.check(request)
        assert exc_info.value.status_code == 429

    @pytest.mark.asyncio
    async def test_different_ips_have_separate_limits(self) -> None:
        """Разные IP имеют раздельные лимиты."""
        limiter = RateLimiter(max_requests=2, window_seconds=60)
        req_a = _make_request("10.0.0.1")
        req_b = _make_request("10.0.0.2")

        # IP A использует весь лимит
        for _ in range(2):
            await limiter.check(req_a)
        with pytest.raises(HTTPException):
            await limiter.check(req_a)

        # IP B всё ещё свободен
        await limiter.check(req_b)

    @pytest.mark.asyncio
    async def test_expired_timestamps_cleaned(self) -> None:
        """Устаревшие timestamp-ы очищаются, лимит сбрасывается."""
        limiter = RateLimiter(max_requests=2, window_seconds=1)
        request = _make_request()

        # Заполняем лимит вручную старыми записями
        base_time = 1_700_000_000.0
        old_time = base_time - 10
        limiter._store["127.0.0.1"] = [old_time, old_time]

        # Новый запрос проходит — старые записи очищены.
        with patch("src.api.rate_limiter.time.time", return_value=base_time):
            await limiter.check(request)

    @pytest.mark.asyncio
    async def test_unknown_client_ip(self) -> None:
        """Запрос без client и без X-Forwarded-For → IP 'unknown'."""
        limiter = RateLimiter(max_requests=5, window_seconds=60)
        request = MagicMock()
        request.client = None
        request.headers.get.return_value = None

        await limiter.check(request)
        assert "unknown" in limiter._store

    @pytest.mark.asyncio
    async def test_x_forwarded_for_used(self) -> None:
        """X-Forwarded-For используется только для доверенного прокси."""
        limiter = RateLimiter(
            max_requests=2,
            window_seconds=60,
            trust_forwarded_for=True,
            trusted_proxy_ips=["10.0.0.1"],
        )
        request = _make_request(ip="10.0.0.1", forwarded_for="192.168.1.100, 10.0.0.1")

        await limiter.check(request)
        # Используется первый IP из X-Forwarded-For.
        assert "192.168.1.100" in limiter._store
        assert "10.0.0.1" not in limiter._store

    @pytest.mark.asyncio
    async def test_untrusted_proxy_ignores_x_forwarded_for(self) -> None:
        """X-Forwarded-For игнорируется для недоверенного источника."""
        limiter = RateLimiter(
            max_requests=2,
            window_seconds=60,
            trust_forwarded_for=True,
            trusted_proxy_ips=["10.0.0.2"],
        )
        request = _make_request(ip="10.0.0.1", forwarded_for="192.168.1.100, 10.0.0.1")

        await limiter.check(request)
        assert "10.0.0.1" in limiter._store
        assert "192.168.1.100" not in limiter._store

    @pytest.mark.asyncio
    async def test_empty_trusted_proxy_list_ignores_forwarded_for(self) -> None:
        """При trust_forwarded_for=True без trusted proxies XFF не используется."""
        limiter = RateLimiter(
            max_requests=2,
            window_seconds=60,
            trust_forwarded_for=True,
            trusted_proxy_ips=[],
        )
        request = _make_request(ip="10.0.0.1", forwarded_for="192.168.1.100, 10.0.0.1")

        await limiter.check(request)
        assert "10.0.0.1" in limiter._store
        assert "192.168.1.100" not in limiter._store

    def test_cleanup_stale_removes_old_ips(self) -> None:
        """_cleanup_stale удаляет IP без актуальных запросов при > 100 записях."""
        limiter = RateLimiter()
        now = time.time()
        window_start = now - limiter.window_seconds

        # Создаём > 100 IP с просроченными записями
        for i in range(110):
            limiter._store[f"10.0.0.{i}"] = [window_start - 100]

        # Добавляем один «живой» IP
        limiter._store["10.0.1.1"] = [now]

        limiter._cleanup_stale(window_start)

        # Живой IP остался, все просроченные удалены
        assert "10.0.1.1" in limiter._store
        assert len(limiter._store) == 1

    def test_cleanup_stale_noop_under_100(self) -> None:
        """_cleanup_stale ничего не делает при <= 100 записях."""
        limiter = RateLimiter()
        now = time.time()
        window_start = now - limiter.window_seconds

        for i in range(50):
            limiter._store[f"10.0.0.{i}"] = [window_start - 100]

        limiter._cleanup_stale(window_start)
        # Ничего не удалено — меньше порога
        assert len(limiter._store) == 50

    @pytest.mark.asyncio
    async def test_default_params(self) -> None:
        """Параметры по умолчанию: 60 запросов, 60 секунд."""
        limiter = RateLimiter()
        assert limiter.max_requests == 60
        assert limiter.window_seconds == 60

    @pytest.mark.asyncio
    async def test_concurrent_requests_respect_limit(self) -> None:
        """Конкурентные запросы не должны превышать лимит."""
        limiter = RateLimiter(max_requests=5, window_seconds=60)
        request = _make_request("10.0.0.1")
        results = await asyncio.gather(
            *[_safe_check(limiter, request) for _ in range(10)],
        )
        passed = sum(1 for r in results if r is True)
        blocked = sum(1 for r in results if r is False)
        assert passed == 5
        assert blocked == 5


async def _safe_check(limiter: RateLimiter, request: MagicMock) -> bool:
    """Проверить rate limit, вернуть True если прошёл, False если заблокирован."""
    try:
        await limiter.check(request)
        return True
    except HTTPException:
        return False
