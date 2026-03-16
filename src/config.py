"""Конфигурация скрапера из переменных окружения."""
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Literal

from dotenv import dotenv_values
from pydantic import AliasChoices, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _split_comma(value: str) -> list[str]:
    """Парсит строку 'a,b,c' → ['a', 'b', 'c']."""
    if not value or not value.strip():
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class AccountCredentials:
    """Креды одного Instagram-аккаунта."""

    name: str
    username: str
    password: SecretStr
    proxy: str
    totp_seed: SecretStr = field(default_factory=lambda: SecretStr(""))

    @property
    def has_totp_seed(self) -> bool:
        """Есть ли TOTP seed у аккаунта."""
        return bool(self.totp_seed.get_secret_value().strip())

    def __repr__(self) -> str:
        return f"AccountCredentials(name={self.name!r}, username={self.username!r}, password='***', totp_seed='***')"


def _parse_account_credentials(env_file: str = ".env") -> list[AccountCredentials]:
    """Парсит IG_*_USERNAME/PASSWORD и PROXY_* из .env файла.

    Pydantic Settings игнорирует динамические переменные (extra='ignore'),
    поэтому читаем .env напрямую через dotenv_values.
    """
    path = Path(env_file)
    if not path.exists():
        return []

    env = dotenv_values(path)
    accounts_str = env.get("INSTAGRAM_ACCOUNTS", "")
    names = _split_comma(accounts_str) if accounts_str else []

    result: list[AccountCredentials] = []
    for name in names:
        upper = name.upper()
        username = env.get(f"IG_{upper}_USERNAME", "")
        password = env.get(f"IG_{upper}_PASSWORD", "")
        proxy = env.get(f"PROXY_{upper}", "") or ""
        totp_seed = env.get(f"IG_{upper}_TOTP_SEED", "") or ""
        if username and password:
            result.append(AccountCredentials(
                name=name, username=username, password=SecretStr(password),
                proxy=proxy, totp_seed=SecretStr(totp_seed),
            ))

    return result


class Settings(BaseSettings):
    """Настройки скрапера — парсятся из env или .env файла."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Supabase
    supabase_url: str
    supabase_service_key: SecretStr

    # OpenAI
    openai_api_key: SecretStr

    # Instagram — список аккаунтов через запятую
    instagram_accounts: str = ""

    # Rate limiting
    scrape_delay_min: float = 1.5
    scrape_delay_max: float = 4.0
    requests_per_hour: int = 30
    cooldown_minutes: int = 45

    # Параметры сбора контента
    posts_to_fetch: int = 25          # Количество медиа из API
    thumbnails_to_persist: int = 7    # Рандомных миниатюр для загрузки в Storage
    highlights_to_fetch: int = 3
    comments_to_fetch: int = 10       # Комментариев на пост
    posts_with_comments: int = 3      # Постов с комментариями

    # Pre-filter параметры
    pre_filter_min_likes: int = 30
    pre_filter_max_inactive_days: int = 180
    pre_filter_posts_to_check: int = 5

    # AI Batch
    batch_min_size: int = 10
    batch_model: str = "gpt-5-mini"
    batch_reasoning_effort: Literal["low", "medium", "high"] = "low"

    # AI
    embedding_model: str = "text-embedding-3-small"

    # Воркер
    worker_poll_interval: int = 30
    worker_max_concurrent: int = 5
    upload_max_concurrent: int = 5  # глобальный лимит параллельных загрузок в Storage
    log_level: str = "INFO"

    # API
    scraper_api_key: SecretStr
    scraper_port: int = Field(
        default=8001,
        validation_alias=AliasChoices("SCRAPER_PORT", "PORT"),
    )
    rate_limit_max_requests: int = 60
    rate_limit_window_seconds: int = 60
    rate_limit_trust_forwarded_for: bool = False
    trusted_proxy_ips: str = ""
    api_docs_enabled: bool = False

    # Фильтрация свежести
    rescrape_days: int = 60  # Минимальный интервал между скрапами (дни)

    # Backfill: автоскрап pending блогов
    backfill_scrape_enabled: bool = True
    backfill_scrape_batch_size: int = 80
    backfill_scrape_interval_minutes: int = 30

    # Backfill: AI анализ для блогов без insights
    backfill_ai_enabled: bool = True
    backfill_ai_batch_size: int = 50
    backfill_ai_interval_minutes: int = 60

    # HikerAPI (альтернативный бэкенд)
    hikerapi_token: SecretStr = SecretStr("")
    scraper_backend: Literal["instagrapi", "hikerapi"] = "instagrapi"

    @field_validator("scraper_api_key")
    @classmethod
    def _check_api_key_not_default(cls, v: SecretStr) -> SecretStr:
        if v.get_secret_value() == "sk-scraper-change-me":
            raise ValueError("SCRAPER_API_KEY must be changed from default value")
        return v

    @cached_property
    def account_credentials(self) -> list[AccountCredentials]:
        """Креды всех Instagram-аккаунтов из .env файла."""
        return _parse_account_credentials()

    @cached_property
    def trusted_proxy_ip_list(self) -> list[str]:
        """Список доверенных proxy IP из trusted_proxy_ips."""
        return _split_comma(self.trusted_proxy_ips)


def load_settings() -> Settings:
    """Создать Settings из переменных окружения (.env файла).

    Фабричная функция — обходит ограничение pyright, который не знает,
    что pydantic-settings заполняет обязательные поля из окружения.
    """
    return Settings.model_validate({})
