"""Пул Instagram-аккаунтов с ротацией и cooldown."""
import asyncio
import hashlib
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from instagrapi import Client
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
from loguru import logger
from supabase import Client as SupabaseClient

from src.config import Settings
from src.platforms.instagram.exceptions import (
    AllAccountsCooldownError,
    ScraperError,
)
from src.storage import load_session, save_session

# Реалистичные устройства для уникальных fingerprints (популярные Android-модели)
_DEVICE_POOL: list[dict[str, str | int]] = [
    {
        "manufacturer": "Samsung",
        "model": "SM-G991B",
        "device": "o1s",
        "cpu": "exynos",
        "android_version": 33,
        "android_release": "13.0",
        "dpi": "480dpi",
        "resolution": "1080x2400",
    },
    {
        "manufacturer": "Google",
        "model": "Pixel 7",
        "device": "panther",
        "cpu": "qcom",
        "android_version": 34,
        "android_release": "14.0",
        "dpi": "420dpi",
        "resolution": "1080x2400",
    },
    {
        "manufacturer": "Xiaomi",
        "model": "2201117TG",
        "device": "vili",
        "cpu": "qcom",
        "android_version": 33,
        "android_release": "13.0",
        "dpi": "440dpi",
        "resolution": "1080x2400",
    },
    {
        "manufacturer": "OnePlus",
        "model": "LE2125",
        "device": "lemonadep",
        "cpu": "qcom",
        "android_version": 34,
        "android_release": "14.0",
        "dpi": "480dpi",
        "resolution": "1440x3216",
    },
    {
        "manufacturer": "Samsung",
        "model": "SM-A536B",
        "device": "a53x",
        "cpu": "exynos",
        "android_version": 34,
        "android_release": "14.0",
        "dpi": "480dpi",
        "resolution": "1080x2400",
    },
    {
        "manufacturer": "Huawei",
        "model": "NOH-NX9",
        "device": "noah",
        "cpu": "kirin",
        "android_version": 31,
        "android_release": "12.0",
        "dpi": "480dpi",
        "resolution": "1344x2772",
    },
]


def _generate_device_for_account(account_name: str) -> dict[str, str | int]:
    """Детерминированно выбирает устройство по имени аккаунта.

    Один аккаунт = одно устройство навсегда (хеш имени → индекс).
    """
    idx = int(hashlib.md5(account_name.encode()).hexdigest(), 16) % len(_DEVICE_POOL)
    device = dict(_DEVICE_POOL[idx])
    device.setdefault("app_version", "269.0.0.18.75")
    device.setdefault("version_code", "314665256")
    return device


@dataclass
class AccountState:
    """Состояние одного Instagram-аккаунта."""

    name: str
    client: Client
    proxy: str
    username: str = ""
    password: str = ""
    totp_seed: str = ""
    is_available: bool = True
    cooldown_until: float = 0
    requests_this_hour: int = 0
    hour_started_at: float = field(default_factory=time.time)


class AccountPool:
    """
    Управляет пулом Instagram-аккаунтов.
    Один аккаунт = один sticky IP. Ротация round-robin.
    """

    MAX_RETRIES = 3  # Максимум переключений аккаунтов на один запрос

    def __init__(
        self,
        accounts: list[AccountState],
        requests_per_hour: int = 30,
        cooldown_minutes: int = 45,
    ) -> None:
        self.accounts = accounts
        self.current_index = 0
        self.requests_per_hour = requests_per_hour
        self.cooldown_minutes = cooldown_minutes
        self._lock = asyncio.Lock()

    @staticmethod
    def _login_with_totp(
        cl: Client, username: str, password: str, totp_seed: str,
    ) -> None:
        """Логин с опциональным TOTP-кодом для 2FA."""
        if totp_seed:
            code = cl.totp_generate_code(totp_seed)
            cl.login(username, password, verification_code=code)
        else:
            cl.login(username, password)

    @classmethod
    async def create(cls, db: SupabaseClient, settings: Settings) -> "AccountPool":
        """Инициализировать пул: загрузить сессии, залогиниться."""
        accounts: list[AccountState] = []

        credentials = settings.account_credentials
        logger.debug(f"Initializing account pool with {len(credentials)} accounts")
        for cred in credentials:
            logger.debug(f"Account {cred.name}: credentials loaded, "
                         f"proxy={'set' if cred.proxy else 'MISSING'}, "
                         f"totp={'set' if cred.totp_seed else 'no'}")

            try:
                cl = Client()
                if cred.proxy:
                    cl.set_proxy(cred.proxy)
                    logger.debug(f"Account {cred.name}: proxy configured")
                else:
                    logger.warning(f"Account {cred.name}: no proxy configured, using direct IP")
                cl.delay_range = [settings.scrape_delay_min, settings.scrape_delay_max]

                # Загрузить или создать сессию
                session = await load_session(db, cred.name)
                logger.debug(f"Account {cred.name}: saved session {'found' if session else 'not found'}")
                if session:
                    cl.set_settings(session)
                    try:
                        # Валидация сессии через get_timeline_feed
                        logger.debug(f"Account {cred.name}: validating session via timeline feed...")
                        await asyncio.to_thread(cl.get_timeline_feed)
                        logger.info(f"Account {cred.name}: session restored via timeline check")
                    except Exception as e:
                        logger.warning(f"Account {cred.name}: session expired ({type(e).__name__}), fresh login")
                        # Сохраняем device settings и UUID для консистентности
                        old_settings = cl.get_settings()
                        old_uuids = old_settings.get("uuids", {})
                        old_device = old_settings.get("device_settings", {})
                        cl = Client()
                        if old_device:
                            cl.set_device(old_device)
                        if old_uuids:
                            cl.set_uuids(old_uuids)
                        if cred.proxy:
                            cl.set_proxy(cred.proxy)
                        cl.delay_range = [settings.scrape_delay_min, settings.scrape_delay_max]
                        await asyncio.to_thread(
                            cls._login_with_totp, cl,
                            cred.username, cred.password, cred.totp_seed,
                        )
                else:
                    # Первый логин — уникальный device fingerprint
                    device = _generate_device_for_account(cred.name)
                    cl.set_device(device, reset=True)
                    logger.debug(f"Account {cred.name}: unique device set "
                                 f"({device['manufacturer']} {device['model']})")
                    await asyncio.to_thread(
                        cls._login_with_totp, cl,
                        cred.username, cred.password, cred.totp_seed,
                    )
                    logger.info(f"Account {cred.name}: fresh login")

                # Сохранить сессию
                await save_session(db, cred.name, cl.get_settings())
                logger.debug(f"Account {cred.name}: session saved to storage")

                accounts.append(AccountState(
                    name=cred.name, client=cl, proxy=cred.proxy,
                    username=cred.username, password=cred.password,
                    totp_seed=cred.totp_seed,
                ))
                logger.info(f"Account {cred.name}: initialized successfully")
            except Exception as e:
                logger.error(f"Account {cred.name}: login failed ({type(e).__name__}: {e}), skipping")

        if not accounts:
            logger.warning("No Instagram accounts initialized")

        logger.debug(f"Account pool ready: {len(accounts)} accounts")
        return cls(
            accounts=accounts,
            requests_per_hour=settings.requests_per_hour,
            cooldown_minutes=settings.cooldown_minutes,
        )

    def get_available_account(self) -> AccountState | None:
        """Вернуть доступный аккаунт (round-robin) или None."""
        now = time.time()
        checked = 0

        while checked < len(self.accounts):
            acc = self.accounts[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.accounts)
            checked += 1

            # Проверить cooldown
            if acc.cooldown_until > now:
                remaining = int(acc.cooldown_until - now)
                logger.debug(f"Account {acc.name}: in cooldown ({remaining}s remaining)")
                continue

            # Проверить/сбросить часовой счётчик
            if now - acc.hour_started_at >= 3600:
                logger.debug(f"Account {acc.name}: hourly counter reset "
                             f"(was {acc.requests_this_hour})")
                acc.requests_this_hour = 0
                acc.hour_started_at = now

            # Проверить лимит запросов
            if acc.requests_this_hour >= self.requests_per_hour:
                logger.debug(f"Account {acc.name}: hourly limit reached "
                             f"({acc.requests_this_hour}/{self.requests_per_hour})")
                continue

            logger.debug(f"Selected account {acc.name} "
                         f"(requests: {acc.requests_this_hour}/{self.requests_per_hour})")
            return acc

        logger.debug("No available accounts (all in cooldown or at limit)")
        return None

    def mark_rate_limited(self, account: AccountState) -> None:
        """Аккаунт получил rate limit → cooldown."""
        account.cooldown_until = time.time() + (self.cooldown_minutes * 60)
        logger.warning(f"Account {account.name} rate-limited, cooldown {self.cooldown_minutes}min")

    def mark_challenge(self, account: AccountState) -> None:
        """Аккаунт получил challenge → двойной cooldown."""
        account.cooldown_until = time.time() + (self.cooldown_minutes * 2 * 60)
        logger.warning(
            f"Account {account.name} challenge required, cooldown {self.cooldown_minutes * 2}min"
        )

    def increment_requests(self, account: AccountState) -> None:
        """Инкрементировать счётчик запросов."""
        account.requests_this_hour += 1

    async def _try_relogin(self, account: AccountState) -> bool:
        """Попробовать re-login. Возвращает True при успехе."""
        try:
            await asyncio.to_thread(
                self._login_with_totp, account.client,
                account.username, account.password, account.totp_seed,
            )
            logger.info(f"Account {account.name}: re-login successful")
            return True
        except Exception as e:
            logger.error(f"Account {account.name}: re-login failed: {e}")
            async with self._lock:
                self.mark_rate_limited(account)
            return False

    async def safe_request(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Выполнить запрос к Instagram с обработкой ошибок и ротацией аккаунтов.
        - RateLimitError / PleaseWaitFewMinutes → cooldown, retry с другим аккаунтом
        - ChallengeRequired → двойной cooldown, retry с другим аккаунтом
        - LoginRequired → re-login, если не получилось → cooldown
        - UserNotFound → raise (без retry)
        - ClientError → raise ScraperError

        Лок удерживается только при выборе аккаунта и инкременте счётчика,
        чтобы concurrent-корутины не получали один и тот же аккаунт.
        """
        func_name = getattr(func, "__name__", str(func))
        for attempt in range(self.MAX_RETRIES):
            async with self._lock:
                acc = self.get_available_account()
                if acc is None:
                    raise AllAccountsCooldownError("All accounts in cooldown")
                self.increment_requests(acc)

            logger.debug(f"safe_request: {func_name} via {acc.name} "
                         f"(attempt {attempt + 1}/{self.MAX_RETRIES})")

            try:
                result = await asyncio.to_thread(func, acc.client, *args, **kwargs)

                # Случайная пауза между запросами (anti-detection)
                delay = random.uniform(2, 5)
                logger.debug(f"safe_request: {func_name} OK, sleeping {delay:.1f}s")
                await asyncio.sleep(delay)
                return result

            except UserNotFound:
                # Не повторяем — пользователь удалён/не найден
                raise

            except (RateLimitError, PleaseWaitFewMinutes, ClientThrottledError):
                async with self._lock:
                    self.mark_rate_limited(acc)
                logger.warning(f"Rate limit on {acc.name}, trying another account")
                continue

            except (ClientConnectionError, ClientJSONDecodeError) as e:
                # Транзиентные ошибки — cooldown + retry с другим аккаунтом
                async with self._lock:
                    self.mark_rate_limited(acc)
                logger.warning(f"Transient error on {acc.name}: {type(e).__name__}, trying another")
                continue

            except ChallengeRequired:
                async with self._lock:
                    self.mark_challenge(acc)
                logger.warning(f"Challenge on {acc.name}, trying another account")
                continue

            except LoginRequired:
                success = await self._try_relogin(acc)
                if not success:
                    continue
                # После re-login повторяем тот же запрос
                try:
                    result = await asyncio.to_thread(func, acc.client, *args, **kwargs)
                    await asyncio.sleep(random.uniform(2, 5))
                    return result
                except Exception:
                    async with self._lock:
                        self.mark_rate_limited(acc)
                    continue

            except ClientError as e:
                if e.__class__.__name__ == "ClientLoginRequired":
                    success = await self._try_relogin(acc)
                    if not success:
                        continue
                    try:
                        result = await asyncio.to_thread(func, acc.client, *args, **kwargs)
                        await asyncio.sleep(random.uniform(2, 5))
                        return result
                    except Exception:
                        async with self._lock:
                            self.mark_rate_limited(acc)
                        continue
                raise ScraperError(f"Instagram client error: {e}") from e

        raise AllAccountsCooldownError(
            f"Failed after {self.MAX_RETRIES} account retries"
        )

    async def save_all_sessions(self, db: SupabaseClient) -> None:
        """Сохранить сессии всех аккаунтов (при shutdown)."""
        for acc in self.accounts:
            try:
                settings = acc.client.get_settings()
                await save_session(db, acc.name, settings)
            except Exception as e:
                logger.error(f"Failed to save session for {acc.name}: {e}")
