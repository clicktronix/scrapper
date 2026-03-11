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


def _optimize_image_for_llm(raw_image: bytes, original_mime: str, source_url: str) -> tuple[bytes, str]:
    """Сжать/уменьшить изображение для более компактного base64 payload."""
    try:
        image = Image.open(io.BytesIO(raw_image))
        image.load()
    except Exception as e:
        logger.warning(f"[images] Не удалось декодировать изображение, используем оригинал: {source_url} ({e})")
        return raw_image, original_mime

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
) -> httpx.Response:
    """Выполнить HTTP GET с учётом семафора."""
    if semaphore is not None:
        async with semaphore:
            response = await client.get(url, timeout=DOWNLOAD_TIMEOUT)
            response.raise_for_status()
            return response
    response = await client.get(url, timeout=DOWNLOAD_TIMEOUT)
    response.raise_for_status()
    return response


async def download_image_as_base64(
    url: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore | None = None,
) -> str | None:
    """Скачать изображение и вернуть data URI. None при ошибке."""
    if not is_safe_url(url):
        logger.warning(f"[images] Небезопасный URL, пропускаем: {url}")
        return None

    response: httpx.Response | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = await _do_download(url, client, semaphore)
            break
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

    if response is None:
        return None

    # Проверяем размер
    if len(response.content) > MAX_IMAGE_SIZE:
        logger.warning(
            f"[images] Слишком большое изображение "
            f"({len(response.content)} байт): {url}"
        )
        return None

    # Определяем MIME-тип из Content-Type (fallback: image/jpeg)
    content_type = response.headers.get("content-type", "image/jpeg")
    # Убираем параметры типа charset
    mime = content_type.split(";")[0].strip()
    if not mime.startswith("image/"):
        mime = "image/jpeg"

    optimized_bytes, optimized_mime = _optimize_image_for_llm(response.content, mime, url)

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
