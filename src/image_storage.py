"""Загрузка изображений Instagram в Supabase Storage для постоянного хранения."""
import asyncio

import httpx
from loguru import logger
from supabase import Client

from src.database import run_in_thread

IMAGES_BUCKET = "blog-images"
DOWNLOAD_TIMEOUT = 15.0
MAX_DOWNLOAD_SIZE = 10 * 1024 * 1024  # 10 МБ
MAX_CONCURRENT_UPLOADS = 2  # Ограничение параллельных загрузок в Storage

# Глобальный семафор — разделяется между всеми конкурентными задачами worker'а,
# чтобы суммарное число одновременных загрузок не превышало лимит (Errno 35 EAGAIN)
_upload_semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)


def build_public_url(supabase_url: str, path: str) -> str:
    """Постоянный публичный URL для файла в Storage."""
    base = supabase_url.rstrip("/")
    return f"{base}/storage/v1/object/public/{IMAGES_BUCKET}/{path}"


async def download_image(url: str, client: httpx.AsyncClient) -> tuple[bytes, str] | None:
    """Скачать изображение по URL. Вернуть (bytes, content_type) или None при ошибке."""
    try:
        response = await client.get(url, timeout=DOWNLOAD_TIMEOUT, follow_redirects=True)
        response.raise_for_status()
    except httpx.TimeoutException:
        logger.warning(f"[image_storage] Таймаут при скачивании: {url}")
        return None
    except httpx.HTTPStatusError as e:
        logger.warning(f"[image_storage] HTTP {e.response.status_code}: {url}")
        return None
    except httpx.HTTPError as e:
        logger.warning(f"[image_storage] Ошибка скачивания {url}: {e}")
        return None

    if len(response.content) > MAX_DOWNLOAD_SIZE:
        logger.warning(
            f"[image_storage] Слишком большое изображение "
            f"({len(response.content)} байт): {url}"
        )
        return None

    content_type = response.headers.get("content-type", "image/jpeg")
    mime = content_type.split(";")[0].strip()
    if not mime.startswith("image/"):
        logger.warning(f"[image_storage] Не изображение ({mime}): {url}")
        return None

    return response.content, mime


UPLOAD_MAX_RETRIES = 3
UPLOAD_RETRY_DELAY = 1.0  # секунды между попытками


def _is_eagain(exc: BaseException) -> bool:
    """Проверить, содержит ли исключение EAGAIN (Errno 11/35).

    Supabase Storage SDK оборачивает OSError в httpx/storage3 исключения,
    поэтому проверяем всю цепочку __cause__/__context__ и строку.
    """
    # Проверяем саму ошибку и всю цепочку причин
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, OSError) and current.errno in (11, 35):
            return True
        current = current.__cause__ or current.__context__
    # Fallback: проверка строкового представления
    return (
        "Errno 11" in str(exc)
        or "Errno 35" in str(exc)
        or "Resource temporarily unavailable" in str(exc)
    )


async def upload_image(db: Client, path: str, data: bytes, content_type: str) -> bool:
    """Загрузить файл в Supabase Storage (upsert) с retry при EAGAIN."""
    for attempt in range(1, UPLOAD_MAX_RETRIES + 1):
        try:
            await run_in_thread(
                db.storage.from_(IMAGES_BUCKET).upload,
                path,
                data,
                {"content-type": content_type, "upsert": "true"},
            )
            return True
        except Exception as e:
            if _is_eagain(e) and attempt < UPLOAD_MAX_RETRIES:
                logger.warning(
                    f"[image_storage] EAGAIN при загрузке ({path}), "
                    f"попытка {attempt}/{UPLOAD_MAX_RETRIES}, жду {UPLOAD_RETRY_DELAY}с..."
                )
                await asyncio.sleep(UPLOAD_RETRY_DELAY * attempt)
                continue
            logger.error(f"[image_storage] Ошибка загрузки в Storage ({path}): {e}")
            return False
    return False


async def download_and_upload_image(
    db: Client,
    http_client: httpx.AsyncClient,
    cdn_url: str,
    storage_path: str,
    supabase_url: str,
) -> str | None:
    """Скачать из CDN и загрузить в Storage. Вернуть постоянный URL или None."""
    result = await download_image(cdn_url, http_client)
    if result is None:
        return None

    data, content_type = result
    ok = await upload_image(db, storage_path, data, content_type)
    if not ok:
        return None

    return build_public_url(supabase_url, storage_path)


async def persist_profile_images(
    db: Client,
    supabase_url: str,
    blog_id: str,
    avatar_cdn_url: str | None,
    posts: list[dict],
) -> tuple[str | None, dict[str, str]]:
    """
    Скачать и загрузить изображения профиля параллельно (аватар + посты).

    Возвращает:
        (avatar_url, {post_platform_id: url})
    """
    # Метаинформация для сборки результата
    task_keys: list[tuple[str, str]] = []  # (type, platform_id)

    async def _throttled_upload(
        client: httpx.AsyncClient, cdn_url: str, storage_path: str,
    ) -> str | None:
        async with _upload_semaphore:
            return await download_and_upload_image(db, client, cdn_url, storage_path, supabase_url)

    tasks: list[asyncio.Task] = []

    async with httpx.AsyncClient() as client:
        # Аватар
        if avatar_cdn_url:
            path = f"{blog_id}/avatar.jpg"
            task = asyncio.create_task(_throttled_upload(client, avatar_cdn_url, path))
            tasks.append(task)
            task_keys.append(("avatar", ""))

        # Посты
        for post in posts:
            cdn_url = post.get("thumbnail_url")
            platform_id = post.get("platform_id", "")
            if cdn_url and platform_id:
                path = f"{blog_id}/post_{platform_id}.jpg"
                task = asyncio.create_task(_throttled_upload(client, cdn_url, path))
                tasks.append(task)
                task_keys.append(("post", platform_id))

        if not tasks:
            return None, {}

        results = await asyncio.gather(*tasks, return_exceptions=True)

    avatar_url: str | None = None
    post_urls: dict[str, str] = {}

    for (kind, platform_id), result in zip(task_keys, results):
        if isinstance(result, BaseException):
            logger.error(f"[image_storage] Ошибка при обработке {kind} {platform_id}: {result}")
            continue
        if result is None:
            continue

        if kind == "avatar":
            avatar_url = result
        elif kind == "post":
            post_urls[platform_id] = result

    total = (1 if avatar_url else 0) + len(post_urls)
    logger.info(f"[image_storage] blog={blog_id}: загружено {total} изображений в Storage")

    return avatar_url, post_urls


async def delete_blog_images(db: Client, blog_id: str) -> int:
    """Удалить все изображения блога из Storage. Вернуть количество удалённых."""
    try:
        files = await run_in_thread(
            db.storage.from_(IMAGES_BUCKET).list, blog_id
        )
    except Exception as e:
        logger.error(f"[image_storage] Ошибка листинга файлов для blog={blog_id}: {e}")
        return 0

    if not files:
        return 0

    paths = [f"{blog_id}/{f['name']}" for f in files]

    try:
        await run_in_thread(
            db.storage.from_(IMAGES_BUCKET).remove, paths
        )
        logger.debug(f"[image_storage] Удалено {len(paths)} файлов для blog={blog_id}")
        return len(paths)
    except Exception as e:
        logger.error(f"[image_storage] Ошибка удаления файлов для blog={blog_id}: {e}")
        return 0
