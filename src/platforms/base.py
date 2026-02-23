"""Базовый интерфейс скрапера для любой платформы."""
from dataclasses import dataclass
from typing import Protocol

from src.models.blog import ScrapedProfile


@dataclass
class DiscoveredProfile:
    """Найденный профиль при discover-поиске."""

    username: str
    full_name: str
    follower_count: int
    platform_id: str
    is_business: bool = False
    is_verified: bool = False
    biography: str = ""
    account_type: int | None = None


class BaseScraper(Protocol):
    """Общий интерфейс скрапера."""

    async def scrape_profile(self, username: str) -> ScrapedProfile:
        """Полный скрап профиля."""
        ...

    async def discover(
        self, query: str, min_followers: int
    ) -> list[DiscoveredProfile]:
        """Поиск новых профилей."""
        ...
