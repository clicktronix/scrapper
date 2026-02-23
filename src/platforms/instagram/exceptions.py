"""Кастомные исключения скрапера."""


class ScraperError(Exception):
    """Общая ошибка скрапинга."""


class PrivateAccountError(ScraperError):
    """Аккаунт приватный — скрапинг невозможен."""


class AllAccountsCooldownError(ScraperError):
    """Все аккаунты в cooldown — задача откладывается."""


class InsufficientBalanceError(ScraperError):
    """Недостаточно средств на HikerAPI — ретрай бесполезен."""


class HikerAPIError(ScraperError):
    """Ошибка HTTP от HikerAPI (4xx/5xx)."""

    def __init__(self, status_code: int, detail: str = "") -> None:
        self.status_code = status_code
        super().__init__(f"HikerAPI HTTP {status_code}: {detail}")
