"""Тесты скачивания изображений и конвертации в base64."""
import base64
import io
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from PIL import Image

from src.ai.images import (
    MAX_IMAGES,
    download_image_as_base64,
    resolve_profile_images,
)
from src.models.blog import ScrapedPost, ScrapedProfile


def _make_valid_image_bytes(fmt: str = "PNG") -> bytes:
    image = Image.new("RGB", (16, 16), color="blue")
    buffer = io.BytesIO()
    image.save(buffer, format=fmt)
    return buffer.getvalue()


class TestDownloadImageAsBase64:
    """Тесты download_image_as_base64."""

    @pytest.mark.asyncio
    async def test_download_success(self) -> None:
        """Успешное скачивание — возвращает data URI."""
        client = AsyncMock(spec=httpx.AsyncClient)
        with patch("src.ai.images._do_download", new_callable=AsyncMock) as mock_dl:
            mock_dl.return_value = (
                _make_valid_image_bytes("PNG"),
                "image/png",
                "https://example.com/img.png",
            )
            result = await download_image_as_base64("https://example.com/img.png", client)

        assert result is not None
        assert result.startswith("data:image/")

    @pytest.mark.asyncio
    async def test_download_jpeg_default(self) -> None:
        """Content-Type image/jpeg — корректный MIME."""
        client = AsyncMock(spec=httpx.AsyncClient)
        with patch("src.ai.images._do_download", new_callable=AsyncMock) as mock_dl:
            mock_dl.return_value = (
                _make_valid_image_bytes("JPEG"),
                "image/jpeg",
                "https://example.com/img.jpg",
            )
            result = await download_image_as_base64("https://example.com/img.jpg", client)

        assert result is not None
        assert result.startswith("data:image/jpeg;base64,")

    @pytest.mark.asyncio
    async def test_unsupported_mime_returns_none(self) -> None:
        """Неподдерживаемый MIME отклоняется."""
        client = AsyncMock(spec=httpx.AsyncClient)
        with patch("src.ai.images._do_download", new_callable=AsyncMock) as mock_dl:
            mock_dl.side_effect = ValueError("Unsupported content type")
            result = await download_image_as_base64("https://example.com/img", client)
        assert result is None

    @pytest.mark.asyncio
    async def test_download_timeout(self) -> None:
        """Таймаут после всех ретраев — возвращает None."""
        client = AsyncMock(spec=httpx.AsyncClient)
        with patch("src.ai.images._do_download", new_callable=AsyncMock) as mock_dl:
            mock_dl.side_effect = httpx.ReadTimeout("timeout")
            result = await download_image_as_base64("https://example.com/slow.jpg", client)
            assert result is None
            from src.ai.images import MAX_RETRIES
            assert mock_dl.await_count == MAX_RETRIES + 1

    @pytest.mark.asyncio
    async def test_download_timeout_retry_success(self) -> None:
        """Таймаут на первой попытке, успех на второй — возвращает data URI."""
        client = AsyncMock(spec=httpx.AsyncClient)
        with patch("src.ai.images._do_download", new_callable=AsyncMock) as mock_dl:
            mock_dl.side_effect = [
                httpx.ReadTimeout("timeout"),
                (
                    _make_valid_image_bytes("PNG"),
                    "image/png",
                    "https://example.com/img.png",
                ),
            ]
            result = await download_image_as_base64("https://example.com/img.png", client)

        assert result is not None
        assert result.startswith("data:image/")
        assert mock_dl.await_count == 2

    @pytest.mark.asyncio
    async def test_redirect_to_unsafe_url_returns_none(self) -> None:
        """Небезопасный финальный URL после редиректа отклоняется."""
        client = AsyncMock(spec=httpx.AsyncClient)
        with patch("src.ai.images._do_download", new_callable=AsyncMock) as mock_dl:
            mock_dl.return_value = (b"\x89PNG\r\n\x1a\n", "image/png", "http://127.0.0.1/internal.png")
            result = await download_image_as_base64("https://example.com/img.png", client)
        assert result is None

    @pytest.mark.asyncio
    async def test_download_too_large_returns_none(self) -> None:
        """Слишком большое изображение отклоняется."""
        from src.ai.images import _ImageTooLargeError

        client = AsyncMock(spec=httpx.AsyncClient)
        with patch("src.ai.images._do_download", new_callable=AsyncMock) as mock_dl:
            mock_dl.side_effect = _ImageTooLargeError("Downloaded=99999999")
            result = await download_image_as_base64("https://example.com/img.jpg", client)
        assert result is None

    @pytest.mark.asyncio
    async def test_download_resizes_large_image_to_512(self) -> None:
        """Большое изображение ужимается до max 512 по длинной стороне."""
        image = Image.new("RGB", (2048, 1024), color="red")
        buffer = io.BytesIO()
        image.save(buffer, format="JPEG", quality=95)
        large_jpeg = buffer.getvalue()

        client = AsyncMock(spec=httpx.AsyncClient)
        with patch("src.ai.images._do_download", new_callable=AsyncMock) as mock_dl:
            mock_dl.return_value = (large_jpeg, "image/jpeg", "https://example.com/large.jpg")
            result = await download_image_as_base64("https://example.com/large.jpg", client)

        assert result is not None
        assert result.startswith("data:image/jpeg;base64,")
        encoded = result.split(",", maxsplit=1)[1]
        decoded = base64.b64decode(encoded)
        processed = Image.open(io.BytesIO(decoded))
        assert max(processed.size) <= 512


class TestResolveProfileImages:
    """Тесты resolve_profile_images."""

    @pytest.mark.asyncio
    async def test_resolve_profile_images(self) -> None:
        """Успешное скачивание нескольких изображений."""
        profile = ScrapedProfile(
            platform_id="12345",
            username="test",
            profile_pic_url="https://example.com/avatar.jpg",
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    thumbnail_url="https://example.com/post1.jpg",
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )

        with patch("src.ai.images.download_image_as_base64") as mock_dl:
            mock_dl.side_effect = [
                "data:image/jpeg;base64,avatar",
                "data:image/jpeg;base64,post1",
            ]
            client = AsyncMock(spec=httpx.AsyncClient)
            result = await resolve_profile_images(profile, client=client)

        assert len(result) == 2
        assert result["https://example.com/avatar.jpg"] == "data:image/jpeg;base64,avatar"
        assert result["https://example.com/post1.jpg"] == "data:image/jpeg;base64,post1"

    @pytest.mark.asyncio
    async def test_resolve_skips_failed_downloads(self) -> None:
        """Неудачные скачивания (None) не попадают в результат."""
        profile = ScrapedProfile(
            platform_id="12345",
            username="test",
            profile_pic_url="https://example.com/avatar.jpg",
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    thumbnail_url="https://example.com/post1.jpg",
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )

        with patch("src.ai.images.download_image_as_base64") as mock_dl:
            mock_dl.side_effect = [
                "data:image/jpeg;base64,avatar",
                None,  # скачивание post1 не удалось
            ]
            client = AsyncMock(spec=httpx.AsyncClient)
            result = await resolve_profile_images(profile, client=client)

        assert len(result) == 1
        assert "https://example.com/avatar.jpg" in result
        assert "https://example.com/post1.jpg" not in result

    @pytest.mark.asyncio
    async def test_resolve_empty_profile(self) -> None:
        """Профиль без изображений — пустой словарь."""
        profile = ScrapedProfile(
            platform_id="12345",
            username="empty",
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        result = await resolve_profile_images(profile, client=client)

        assert result == {}

    @pytest.mark.asyncio
    async def test_resolve_skips_none_urls(self) -> None:
        """Посты без thumbnail_url не попадают в URLs."""
        profile = ScrapedProfile(
            platform_id="12345",
            username="test",
            profile_pic_url=None,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    thumbnail_url=None,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        result = await resolve_profile_images(profile, client=client)

        assert result == {}

    @pytest.mark.asyncio
    async def test_resolve_max_10_images(self) -> None:
        """Не больше MAX_IMAGES изображений."""
        posts = [
            ScrapedPost(
                platform_id=f"p{i}",
                media_type=1,
                thumbnail_url=f"https://example.com/p{i}.jpg",
                taken_at=datetime(2026, 1, i + 1, tzinfo=UTC),
            )
            for i in range(15)
        ]
        profile = ScrapedProfile(
            platform_id="12345",
            username="many",
            profile_pic_url="https://example.com/avatar.jpg",
            medias=posts,
        )

        with patch("src.ai.images.download_image_as_base64") as mock_dl:
            mock_dl.return_value = "data:image/jpeg;base64,img"
            client = AsyncMock(spec=httpx.AsyncClient)
            result = await resolve_profile_images(profile, client=client)

        # 1 avatar + 9 posts = 10 (MAX_IMAGES)
        assert len(result) == MAX_IMAGES
        assert mock_dl.call_count == MAX_IMAGES

    @pytest.mark.asyncio
    async def test_resolve_includes_reels(self) -> None:
        """Рилсы тоже включаются в URL-ы."""
        profile = ScrapedProfile(
            platform_id="12345",
            username="test",
            medias=[
                ScrapedPost(
                    platform_id="r1",
                    media_type=2,
                    product_type="clips",
                    thumbnail_url="https://example.com/reel1.jpg",
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )

        with patch("src.ai.images.download_image_as_base64") as mock_dl:
            mock_dl.return_value = "data:image/jpeg;base64,reel"
            client = AsyncMock(spec=httpx.AsyncClient)
            result = await resolve_profile_images(profile, client=client)

        assert result["https://example.com/reel1.jpg"] == "data:image/jpeg;base64,reel"

    @pytest.mark.asyncio
    async def test_resolve_deduplicates_same_urls(self) -> None:
        """Одинаковые URL (avatar/post/reel) скачиваются только один раз."""
        shared_url = "https://example.com/same.jpg"
        profile = ScrapedProfile(
            platform_id="12345",
            username="dups",
            profile_pic_url=shared_url,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    thumbnail_url=shared_url,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
                ScrapedPost(
                    platform_id="r1",
                    media_type=2,
                    product_type="clips",
                    thumbnail_url=shared_url,
                    taken_at=datetime(2026, 1, 16, tzinfo=UTC),
                ),
            ],
        )

        with patch("src.ai.images.download_image_as_base64") as mock_dl:
            mock_dl.return_value = "data:image/jpeg;base64,shared"
            client = AsyncMock(spec=httpx.AsyncClient)
            result = await resolve_profile_images(profile, client=client)

        assert len(result) == 1
        assert result[shared_url] == "data:image/jpeg;base64,shared"
        assert mock_dl.call_count == 1
