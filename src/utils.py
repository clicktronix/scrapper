"""Общие утилиты для скрапера."""
import ipaddress
from urllib.parse import urlparse

# Коды errno для транзиентных сетевых ошибок:
# 11 = EAGAIN (Linux), 32 = Broken pipe, 35 = EAGAIN/ENOBUFS (macOS), 54 = Connection reset
_TRANSIENT_OS_ERROR_CODES = {11, 32, 35, 54}

# Строковые маркеры для fallback-проверки, когда errno недоступен
_TRANSIENT_ERROR_MARKERS = (
    "Resource temporarily unavailable",
    "Errno 11",
    "Errno 35",
    "Connection reset",
    "Broken pipe",
)


def is_transient_network_error(exc: BaseException) -> bool:
    """Проверить, является ли ошибка транзиентной сетевой (EAGAIN, broken pipe и т.д.).

    Supabase Python SDK использует синхронный httpx-клиент с HTTP/2.
    После простоя соединения в пуле становятся «stale», и ОС возвращает
    EAGAIN (errno 11 на Linux, 35 на macOS), Connection reset (54) или
    Broken pipe (32). httpx оборачивает OSError в свои исключения
    (httpx.ReadError и т.д.), поэтому нужно проверять всю цепочку
    __cause__/__context__ и строковое представление.
    """
    # Проверяем саму ошибку и всю цепочку причин — OSError может быть обёрнут
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, OSError) and current.errno in _TRANSIENT_OS_ERROR_CODES:
            return True
        current = current.__cause__ or current.__context__
    # Fallback: строковая проверка для случаев, когда errno недоступен
    err_str = str(exc)
    return any(marker in err_str for marker in _TRANSIENT_ERROR_MARKERS)


def is_safe_url(url: str) -> bool:
    """Проверить URL на безопасность (не private IP, корректная схема).

    Защита от SSRF: блокирует прямые IP-адреса из приватных диапазонов.
    NB: Не защищает от DNS rebinding — допустимо, т.к. URL приходят
    от Instagram CDN и Supabase Storage, а не от пользовательского ввода.
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    if not parsed.hostname:
        return False
    try:
        ip = ipaddress.ip_address(parsed.hostname)
        if ip.is_private or ip.is_loopback or ip.is_link_local:
            return False
    except ValueError:
        pass  # hostname, не IP — OK
    return True
