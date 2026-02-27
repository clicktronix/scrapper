"""Конфигурация скрапера из переменных окружения."""
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

from dotenv import dotenv_values
from pydantic import AliasChoices, Field, SecretStr
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
    password: str
    proxy: str
    totp_seed: str = ""


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
                name=name, username=username, password=password,
                proxy=proxy, totp_seed=totp_seed,
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

    # AI Batch
    batch_min_size: int = 10
    batch_model: str = "gpt-5-mini"

    # Воркер
    worker_poll_interval: int = 30
    worker_max_concurrent: int = 2
    log_level: str = "INFO"

    # API
    scraper_api_key: SecretStr
    scraper_port: int = Field(
        default=8001,
        validation_alias=AliasChoices("SCRAPER_PORT", "PORT"),
    )

    # Фильтрация свежести
    rescrape_days: int = 60  # Минимальный интервал между скрапами (дни)

    # HikerAPI (альтернативный бэкенд)
    hikerapi_token: str = ""
    scraper_backend: str = "instagrapi"  # "instagrapi" | "hikerapi"

    @cached_property
    def instagram_accounts_list(self) -> list[str]:
        """Парсит INSTAGRAM_ACCOUNTS='a,b,c' → ['a', 'b', 'c']."""
        return _split_comma(self.instagram_accounts)

    @cached_property
    def account_credentials(self) -> list[AccountCredentials]:
        """Креды всех Instagram-аккаунтов из .env файла."""
        return _parse_account_credentials()


def load_settings() -> Settings:
    """Создать Settings из переменных окружения (.env файла).

    Фабричная функция — обходит ограничение pyright, который не знает,
    что pydantic-settings заполняет обязательные поля из окружения.
    """
    return Settings.model_validate({})
