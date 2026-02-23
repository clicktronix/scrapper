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


class TestDownloadImageAsBase64:
    """Тесты download_image_as_base64."""

    @pytest.mark.asyncio
    async def test_download_success(self) -> None:
        """Успешное скачивание — возвращает data URI."""
        mock_response = httpx.Response(
            200,
            content=b"\x89PNG\r\n\x1a\n",
            headers={"content-type": "image/png"},
            request=httpx.Request("GET", "https://example.com/img.png"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)

        result = await download_image_as_base64("https://example.com/img.png", client)

        assert result is not None
        assert result.startswith("data:image/png;base64,")
        client.get.assert_called_once_with(
            "https://example.com/img.png", timeout=10.0,
        )

    @pytest.mark.asyncio
    async def test_download_jpeg_default(self) -> None:
        """Content-Type image/jpeg — корректный MIME."""
        mock_response = httpx.Response(
            200,
            content=b"\xff\xd8\xff\xe0",
            headers={"content-type": "image/jpeg"},
            request=httpx.Request("GET", "https://example.com/img.jpg"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)

        result = await download_image_as_base64("https://example.com/img.jpg", client)

        assert result is not None
        assert result.startswith("data:image/jpeg;base64,")

    @pytest.mark.asyncio
    async def test_content_type_with_charset(self) -> None:
        """Content-Type с charset — параметры отсекаются."""
        mock_response = httpx.Response(
            200,
            content=b"imagedata",
            headers={"content-type": "image/webp; charset=utf-8"},
            request=httpx.Request("GET", "https://example.com/img.webp"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)

        result = await download_image_as_base64("https://example.com/img.webp", client)

        assert result is not None
        assert result.startswith("data:image/webp;base64,")

    @pytest.mark.asyncio
    async def test_non_image_content_type_fallback(self) -> None:
        """Не-image Content-Type → fallback на image/jpeg."""
        mock_response = httpx.Response(
            200,
            content=b"imagedata",
            headers={"content-type": "application/octet-stream"},
            request=httpx.Request("GET", "https://example.com/img"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)

        result = await download_image_as_base64("https://example.com/img", client)

        assert result is not None
        assert result.startswith("data:image/jpeg;base64,")

    @pytest.mark.asyncio
    async def test_download_timeout(self) -> None:
        """Таймаут — возвращает None."""
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(side_effect=httpx.ReadTimeout("timeout"))

        result = await download_image_as_base64("https://example.com/slow.jpg", client)

        assert result is None

    @pytest.mark.asyncio
    async def test_download_http_404(self) -> None:
        """HTTP 404 — возвращает None."""
        mock_response = httpx.Response(
            404,
            request=httpx.Request("GET", "https://example.com/gone.jpg"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)

        result = await download_image_as_base64("https://example.com/gone.jpg", client)

        assert result is None

    @pytest.mark.asyncio
    async def test_download_http_500(self) -> None:
        """HTTP 500 — возвращает None."""
        mock_response = httpx.Response(
            500,
            request=httpx.Request("GET", "https://example.com/error.jpg"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)

        result = await download_image_as_base64("https://example.com/error.jpg", client)

        assert result is None

    @pytest.mark.asyncio
    async def test_download_connection_error(self) -> None:
        """Ошибка соединения — возвращает None."""
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(side_effect=httpx.ConnectError("connection refused"))

        result = await download_image_as_base64("https://example.com/img.jpg", client)

        assert result is None

    @pytest.mark.asyncio
    async def test_download_resizes_large_image_to_512(self) -> None:
        """Большое изображение ужимается до max 512 по длинной стороне."""
        image = Image.new("RGB", (2048, 1024), color="red")
        buffer = io.BytesIO()
        image.save(buffer, format="JPEG", quality=95)
        large_jpeg = buffer.getvalue()

        mock_response = httpx.Response(
            200,
            content=large_jpeg,
            headers={"content-type": "image/jpeg"},
            request=httpx.Request("GET", "https://example.com/large.jpg"),
        )
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=mock_response)

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
