"""In-memory rate limiter: sliding window per IP."""
import asyncio
import ipaddress
import time
from collections import defaultdict

from fastapi import HTTPException, Request
from loguru import logger


class RateLimiter:
    """Простой in-memory rate limiter на основе sliding window per IP.

    Аргументы:
        max_requests: максимальное количество запросов в окне.
        window_seconds: размер окна в секундах.
    """

    def __init__(
        self,
        max_requests: int = 60,
        window_seconds: int = 60,
        *,
        trust_forwarded_for: bool = False,
        trusted_proxy_ips: list[str] | None = None,
    ) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.trust_forwarded_for = trust_forwarded_for
        self.trusted_proxy_ips = set(trusted_proxy_ips or [])
        self._store: dict[str, list[float]] = defaultdict(list)
        self._lock = asyncio.Lock()

    def _extract_first_forwarded_ip(self, request: Request) -> str | None:
        """Extract first valid IP from X-Forwarded-For header."""
        header = request.headers.get("x-forwarded-for")
        if not header:
            return None
        candidate = header.split(",")[0].strip()
        if not candidate:
            return None
        try:
            return str(ipaddress.ip_address(candidate))
        except ValueError:
            return None

    def _resolve_client_ip(self, request: Request) -> str:
        """Resolve client IP with optional trusted-proxy support."""
        direct_ip = request.client.host if request.client else "unknown"

        if not self.trust_forwarded_for:
            return direct_ip

        if not self.trusted_proxy_ips:
            # Secure-by-default: ignore XFF unless trusted proxies are configured.
            return direct_ip

        # If trusted proxies are configured, only trust forwarded headers from them.
        if direct_ip not in self.trusted_proxy_ips:
            return direct_ip

        forwarded_ip = self._extract_first_forwarded_ip(request)
        return forwarded_ip or direct_ip

    async def check(self, request: Request) -> None:
        """Проверить rate limit для запроса. Бросает HTTPException 429 при превышении.

        Примечание: при использовании reverse proxy (nginx/traefik) убедитесь,
        что request.client.host содержит реальный IP клиента (X-Forwarded-For),
        иначе все запросы будут считаться от одного IP прокси.
        """
        client_ip = self._resolve_client_ip(request)
        async with self._lock:
            now = time.time()
            window_start = now - self.window_seconds

            # Очистить устаревшие записи для этого IP
            timestamps = self._store[client_ip]
            self._store[client_ip] = [t for t in timestamps if t > window_start]

            if len(self._store[client_ip]) >= self.max_requests:
                logger.warning(
                    f"[rate_limit] 429 для {client_ip} "
                    f"({len(self._store[client_ip])}/{self.max_requests} за {self.window_seconds}с)"
                )
                raise HTTPException(status_code=429, detail="Rate limit exceeded")

            self._store[client_ip].append(now)

            # Периодическая очистка стухших IP (при росте store > 100 записей)
            self._cleanup_stale(window_start)

    def _cleanup_stale(self, window_start: float) -> None:
        """Удалить IP-адреса без актуальных запросов (при превышении 100 записей в store)."""
        if len(self._store) > 100:
            stale_ips = [
                ip for ip, ts in self._store.items()
                if not ts or max(ts) <= window_start
            ]
            for ip in stale_ips:
                del self._store[ip]
