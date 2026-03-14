"""Скачивание изображений и конвертация в base64 data URI для Batch API."""
import asyncio
import base64
import io

import httpx
from loguru import logger
from PIL import Image

from src.models.blog import ScrapedProfile
from src.utils import is_safe_url

# Защита от decompression bomb (по умолчанию ~178M пикселей — слишком много)
Image.MAX_IMAGE_PIXELS = 25_000_000

# Максимальное количество изображений на профиль (как в prompt.py)
MAX_IMAGES = 10

# Таймаут на скачивание одного изображения
DOWNLOAD_TIMEOUT = 30.0

# Количество повторных попыток при таймауте
MAX_RETRIES = 3

# Базовая задержка между ретраями (секунды), умножается на номер попытки
RETRY_BASE_DELAY = 2.0

# Максимальный размер изображения (5 МБ)
MAX_IMAGE_SIZE = 5 * 1024 * 1024

# Ограничение длинной стороны изображения для LLM
MAX_IMAGE_DIMENSION = 512

# Целевой верхний предел размера после оптимизации (400 КБ)
MAX_OPTIMIZED_IMAGE_SIZE = 400 * 1024
_ALLOWED_IMAGE_MIMES = frozenset({
    "image/jpeg",
    "image/png",
    "image/webp",
    "image/gif",
})


class _ImageTooLargeError(Exception):
    """Raised when downloaded image exceeds hard byte limit."""


def _optimize_image_for_llm(raw_image: bytes, source_url: str) -> tuple[bytes, str] | None:
    """Сжать/уменьшить изображение для более компактного base64 payload."""
    try:
        image = Image.open(io.BytesIO(raw_image))
        image.load()
    except Exception as e:
        logger.warning(f"[images] Не удалось декодировать изображение, пропускаем: {source_url} ({e})")
        return None

    image.thumbnail((MAX_IMAGE_DIMENSION, MAX_IMAGE_DIMENSION), Image.Resampling.LANCZOS)

    has_alpha = "A" in image.getbands()

    # Для изображений с прозрачностью сохраняем PNG.
    if has_alpha:
        png_buffer = io.BytesIO()
        image.save(png_buffer, format="PNG", optimize=True)
        png_bytes = png_buffer.getvalue()
        if len(png_bytes) <= MAX_OPTIMIZED_IMAGE_SIZE:
            return png_bytes, "image/png"
        # Если PNG слишком большой, fallback на JPEG без alpha.
        image = image.convert("RGB")

    if image.mode not in ("RGB", "L"):
        image = image.convert("RGB")

    best_jpeg = raw_image
    for quality in (82, 72, 62, 52):
        jpeg_buffer = io.BytesIO()
        image.save(jpeg_buffer, format="JPEG", quality=quality, optimize=True)
        jpeg_bytes = jpeg_buffer.getvalue()
        best_jpeg = jpeg_bytes
        if len(jpeg_bytes) <= MAX_OPTIMIZED_IMAGE_SIZE:
            break

    return best_jpeg, "image/jpeg"


async def _do_download(
    url: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore | None = None,
) -> tuple[bytes, str, str]:
    """Скачать изображение с лимитом размера и вернуть (bytes, mime, final_url)."""
    async def _download() -> tuple[bytes, str, str]:
        async with client.stream(
            "GET",
            url,
            timeout=DOWNLOAD_TIMEOUT,
            follow_redirects=True,
        ) as response:
            response.raise_for_status()

            final_url = str(response.url)
            content_type = response.headers.get("content-type", "").split(";", maxsplit=1)[0].strip().lower()
            if content_type not in _ALLOWED_IMAGE_MIMES:
                raise ValueError(f"Unsupported content type: {content_type or 'unknown'}")

            content_length = response.headers.get("content-length")
            if content_length and content_length.isdigit() and int(content_length) > MAX_IMAGE_SIZE:
                raise _ImageTooLargeError(f"Content-Length={content_length}")

            chunks: list[bytes] = []
            downloaded = 0
            async for chunk in response.aiter_bytes():
                downloaded += len(chunk)
                if downloaded > MAX_IMAGE_SIZE:
                    raise _ImageTooLargeError(f"Downloaded={downloaded}")
                chunks.append(chunk)

            return b"".join(chunks), content_type, final_url

    if semaphore is not None:
        async with semaphore:
            return await _download()
    return await _download()


async def download_image_as_base64(
    url: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore | None = None,
) -> str | None:
    """Скачать изображение и вернуть data URI. None при ошибке."""
    if not is_safe_url(url):
        logger.warning(f"[images] Небезопасный URL, пропускаем: {url}")
        return None

    downloaded_bytes: bytes | None = None
    mime: str | None = None
    final_url: str | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            downloaded_bytes, mime, final_url = await _do_download(url, client, semaphore)
            break
        except _ImageTooLargeError as e:
            logger.warning(f"[images] Слишком большое изображение ({e}): {url}")
            return None
        except ValueError as e:
            logger.warning(f"[images] Неподдерживаемый тип контента: {url} ({e})")
            return None
        except httpx.TimeoutException:
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (attempt + 1)
                logger.debug(f"[images] Таймаут (попытка {attempt + 1}), ретрай через {delay}с: {url}")
                await asyncio.sleep(delay)
                continue
            logger.warning(f"[images] Таймаут при скачивании ({MAX_RETRIES + 1} попыток): {url}")
            return None
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # Retry при 429 (rate limit) и 5xx (серверные ошибки)
            if status in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (attempt + 1)
                logger.debug(f"[images] HTTP {status} (попытка {attempt + 1}), ретрай через {delay}с: {url}")
                await asyncio.sleep(delay)
                continue
            logger.warning(f"[images] HTTP {status} при скачивании: {url}")
            return None
        except httpx.HTTPError as e:
            logger.warning(f"[images] Ошибка при скачивании {url}: {e}")
            return None

    if downloaded_bytes is None or mime is None or final_url is None:
        return None

    if not is_safe_url(final_url):
        logger.warning(f"[images] Небезопасный redirect URL, пропускаем: {final_url}")
        return None

    optimized = _optimize_image_for_llm(downloaded_bytes, url)
    if optimized is None:
        return None
    optimized_bytes, optimized_mime = optimized

    encoded = base64.b64encode(optimized_bytes).decode("ascii")
    return f"data:{optimized_mime};base64,{encoded}"


def _collect_image_urls(profile: ScrapedProfile) -> list[str]:
    """Собрать URL изображений из профиля (avatar + posts, max MAX_IMAGES)."""
    urls: list[str] = []
    seen: set[str] = set()

    def _append_unique(url: str | None) -> None:
        if not url or url in seen or len(urls) >= MAX_IMAGES:
            return
        seen.add(url)
        urls.append(url)

    _append_unique(profile.profile_pic_url)

    for post in profile.medias:
        _append_unique(post.thumbnail_url)

    return urls


async def resolve_profile_images(
    profile: ScrapedProfile,
    client: httpx.AsyncClient | None = None,
    semaphore: asyncio.Semaphore | None = None,
) -> dict[str, str]:
    """
    Скачать все изображения профиля параллельно.
    Возвращает {original_url: data_uri} для успешных скачиваний.
    semaphore — ограничивает общее число конкурентных загрузок (при батче).
    """
    urls = _collect_image_urls(profile)
    if not urls:
        return {}

    # Если клиент не передан — создаём временный
    own_client = client is None
    if client is None:
        client = httpx.AsyncClient()

    try:
        tasks = [download_image_as_base64(url, client, semaphore=semaphore) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        if own_client:
            await client.aclose()

    # Заменяем exceptions на None
    processed: list[str | None] = []
    for r in results:
        if isinstance(r, BaseException):
            logger.warning(f"[images] Ошибка загрузки изображения: {r}")
            processed.append(None)
        else:
            processed.append(r)

    # Собираем только успешные
    image_map: dict[str, str] = {}
    for url, data_uri in zip(urls, processed, strict=True):
        if data_uri is not None:
            image_map[url] = data_uri

    return image_map
