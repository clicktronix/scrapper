"""Тесты модуля image_storage — загрузка изображений в Supabase Storage."""
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest


class TestBuildPublicUrl:
    """Тесты build_public_url."""

    def test_correct_format(self) -> None:
        from src.image_storage import build_public_url

        url = build_public_url("https://example.supabase.co", "blog-1/avatar.jpg")
        assert url == "https://example.supabase.co/storage/v1/object/public/blog-images/blog-1/avatar.jpg"

    def test_strips_trailing_slash(self) -> None:
        from src.image_storage import build_public_url

        url = build_public_url("https://example.supabase.co/", "blog-1/avatar.jpg")
        assert url == "https://example.supabase.co/storage/v1/object/public/blog-images/blog-1/avatar.jpg"

    def test_rejects_unsafe_path(self) -> None:
        from src.image_storage import build_public_url

        with pytest.raises(ValueError, match="Unsafe storage path"):
            build_public_url("https://example.supabase.co", "../avatar.jpg")


class TestDownloadImage:
    """Тесты download_image."""

    @pytest.mark.asyncio
    async def test_success(self) -> None:
        from src.image_storage import download_image

        mock_response = MagicMock()
        mock_response.content = b"\xff\xd8\xff" * 100
        mock_response.headers = {"content-type": "image/jpeg"}
        mock_response.raise_for_status = MagicMock()
        mock_response.url = "https://cdn.instagram.com/photo.jpg"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        result = await download_image("https://cdn.instagram.com/photo.jpg", mock_client)
        assert result is not None
        data, content_type = result
        assert data == mock_response.content
        assert content_type == "image/jpeg"

    @pytest.mark.asyncio
    async def test_timeout(self) -> None:
        from src.image_storage import download_image

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.side_effect = httpx.TimeoutException("timeout")

        result = await download_image("https://cdn.instagram.com/photo.jpg", mock_client)
        assert result is None

    @pytest.mark.asyncio
    async def test_http_error(self) -> None:
        from src.image_storage import download_image

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found", request=MagicMock(), response=mock_response
        )

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        result = await download_image("https://cdn.instagram.com/photo.jpg", mock_client)
        assert result is None

    @pytest.mark.asyncio
    async def test_too_large(self) -> None:
        from src.image_storage import MAX_DOWNLOAD_SIZE, download_image

        mock_response = MagicMock()
        mock_response.content = b"\x00" * (MAX_DOWNLOAD_SIZE + 1)
        mock_response.headers = {"content-type": "image/jpeg"}
        mock_response.raise_for_status = MagicMock()
        mock_response.url = "https://cdn.instagram.com/photo.jpg"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        result = await download_image("https://cdn.instagram.com/photo.jpg", mock_client)
        assert result is None

    @pytest.mark.asyncio
    async def test_non_image_content_type(self) -> None:
        from src.image_storage import download_image

        mock_response = MagicMock()
        mock_response.content = b"<html>Not an image</html>"
        mock_response.headers = {"content-type": "text/html"}
        mock_response.raise_for_status = MagicMock()
        mock_response.url = "https://cdn.instagram.com/photo.jpg"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        result = await download_image("https://cdn.instagram.com/photo.jpg", mock_client)
        assert result is None


    @pytest.mark.asyncio
    async def test_rejects_svg_mime_type(self) -> None:
        """SVG не должен проходить проверку MIME."""
        from src.image_storage import download_image

        mock_response = MagicMock()
        mock_response.content = b"<svg></svg>"
        mock_response.headers = {"content-type": "image/svg+xml"}
        mock_response.raise_for_status = MagicMock()
        mock_response.url = "https://cdn.instagram.com/photo.svg"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        result = await download_image("https://cdn.instagram.com/photo.svg", mock_client)
        assert result is None

    @pytest.mark.asyncio
    async def test_accepts_webp_mime_type(self) -> None:
        """WebP должен проходить проверку MIME."""
        from src.image_storage import download_image

        mock_response = MagicMock()
        mock_response.content = b"\x00webp" * 10
        mock_response.headers = {"content-type": "image/webp"}
        mock_response.raise_for_status = MagicMock()
        mock_response.url = "https://cdn.instagram.com/photo.webp"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        result = await download_image("https://cdn.instagram.com/photo.webp", mock_client)
        assert result is not None
        data, content_type = result
        assert data == mock_response.content
        assert content_type == "image/webp"

    @pytest.mark.asyncio
    async def test_rejects_unsafe_redirect_url(self) -> None:
        from src.image_storage import download_image

        mock_response = MagicMock()
        mock_response.content = b"\xff\xd8\xff" * 10
        mock_response.headers = {"content-type": "image/jpeg"}
        mock_response.raise_for_status = MagicMock()
        mock_response.url = "http://127.0.0.1/internal.jpg"

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.get.return_value = mock_response

        result = await download_image("https://cdn.instagram.com/photo.jpg", mock_client)
        assert result is None


class TestUploadImage:
    """Тесты upload_image."""

    @pytest.mark.asyncio
    async def test_success(self) -> None:
        from src.image_storage import upload_image

        mock_db = MagicMock()
        mock_storage_bucket = MagicMock()
        mock_db.storage.from_.return_value = mock_storage_bucket
        mock_storage_bucket.upload = AsyncMock()

        result = await upload_image(mock_db, "blog-1/avatar.jpg", b"image-data", "image/jpeg")
        assert result is True
        mock_storage_bucket.upload.assert_called_once()

    @pytest.mark.asyncio
    async def test_failure(self) -> None:
        from src.image_storage import upload_image

        mock_db = MagicMock()
        mock_storage_bucket = MagicMock()
        mock_db.storage.from_.return_value = mock_storage_bucket
        mock_storage_bucket.upload = AsyncMock(side_effect=Exception("Storage error"))

        result = await upload_image(mock_db, "blog-1/avatar.jpg", b"image-data", "image/jpeg")
        assert result is False

    @pytest.mark.asyncio
    async def test_correct_bucket_and_path(self) -> None:
        from src.image_storage import upload_image

        mock_db = MagicMock()
        mock_storage_bucket = MagicMock()
        mock_db.storage.from_.return_value = mock_storage_bucket
        mock_storage_bucket.upload = AsyncMock()

        await upload_image(mock_db, "blog-1/post_p1.jpg", b"data", "image/jpeg")

        mock_db.storage.from_.assert_called_with("blog-images")
        mock_storage_bucket.upload.assert_called_once_with(
            "blog-1/post_p1.jpg",
            b"data",
            {"content-type": "image/jpeg", "upsert": "true"},
        )


class TestEagainDetection:
    """Тесты распознавания EAGAIN-ошибок."""

    def test_linux_errno_11_is_recognized(self) -> None:
        from src.utils import is_transient_network_error

        assert is_transient_network_error(OSError(11, "Resource temporarily unavailable"))


class TestDownloadAndUploadImage:
    """Тесты download_and_upload_image."""

    @pytest.mark.asyncio
    async def test_success(self) -> None:
        from src.image_storage import download_and_upload_image

        mock_db = MagicMock()
        mock_client = AsyncMock(spec=httpx.AsyncClient)

        with (
            patch("src.image_storage.download_image", new_callable=AsyncMock) as mock_download,
            patch("src.image_storage.upload_image", new_callable=AsyncMock) as mock_upload,
        ):
            mock_download.return_value = (b"image-data", "image/jpeg")
            mock_upload.return_value = True

            result = await download_and_upload_image(
                mock_db, mock_client, "https://cdn.instagram.com/photo.jpg",
                "blog-1/avatar.jpg", "https://example.supabase.co",
            )

            assert result == "https://example.supabase.co/storage/v1/object/public/blog-images/blog-1/avatar.jpg"
            mock_download.assert_called_once_with("https://cdn.instagram.com/photo.jpg", mock_client)
            mock_upload.assert_called_once_with(mock_db, "blog-1/avatar.jpg", b"image-data", "image/jpeg")

    @pytest.mark.asyncio
    async def test_download_fails(self) -> None:
        from src.image_storage import download_and_upload_image

        mock_db = MagicMock()
        mock_client = AsyncMock(spec=httpx.AsyncClient)

        with (
            patch("src.image_storage.download_image", new_callable=AsyncMock) as mock_download,
            patch("src.image_storage.upload_image", new_callable=AsyncMock) as mock_upload,
        ):
            mock_download.return_value = None

            result = await download_and_upload_image(
                mock_db, mock_client, "https://cdn.instagram.com/photo.jpg",
                "blog-1/avatar.jpg", "https://example.supabase.co",
            )

            assert result is None
            mock_upload.assert_not_called()

    @pytest.mark.asyncio
    async def test_upload_fails(self) -> None:
        from src.image_storage import download_and_upload_image

        mock_db = MagicMock()
        mock_client = AsyncMock(spec=httpx.AsyncClient)

        with (
            patch("src.image_storage.download_image", new_callable=AsyncMock) as mock_download,
            patch("src.image_storage.upload_image", new_callable=AsyncMock) as mock_upload,
        ):
            mock_download.return_value = (b"image-data", "image/jpeg")
            mock_upload.return_value = False

            result = await download_and_upload_image(
                mock_db, mock_client, "https://cdn.instagram.com/photo.jpg",
                "blog-1/avatar.jpg", "https://example.supabase.co",
            )

            assert result is None


class TestPersistProfileImages:
    """Тесты persist_profile_images."""

    @pytest.mark.asyncio
    async def test_all_images(self) -> None:
        from src.image_storage import persist_profile_images

        mock_db = MagicMock()
        posts = [
            {"platform_id": "p1", "thumbnail_url": "https://cdn/p1.jpg"},
            {"platform_id": "p2", "thumbnail_url": "https://cdn/p2.jpg"},
        ]

        with patch("src.image_storage.download_and_upload_image", new_callable=AsyncMock) as mock_fn:
            # Возвращает постоянные URL
            mock_fn.side_effect = [
                "https://sb.co/storage/v1/object/public/blog-images/blog-1/avatar.jpg",
                "https://sb.co/storage/v1/object/public/blog-images/blog-1/post_p1.jpg",
                "https://sb.co/storage/v1/object/public/blog-images/blog-1/post_p2.jpg",
            ]

            avatar_url, post_urls = await persist_profile_images(
                mock_db, "https://sb.co", "blog-1",
                "https://cdn/avatar.jpg", posts,
            )

            assert avatar_url == "https://sb.co/storage/v1/object/public/blog-images/blog-1/avatar.jpg"
            assert post_urls == {
                "p1": "https://sb.co/storage/v1/object/public/blog-images/blog-1/post_p1.jpg",
                "p2": "https://sb.co/storage/v1/object/public/blog-images/blog-1/post_p2.jpg",
            }
            assert mock_fn.call_count == 3

    @pytest.mark.asyncio
    async def test_partial_failure(self) -> None:
        from src.image_storage import persist_profile_images

        mock_db = MagicMock()
        posts = [
            {"platform_id": "p1", "thumbnail_url": "https://cdn/p1.jpg"},
        ]

        with patch("src.image_storage.download_and_upload_image", new_callable=AsyncMock) as mock_fn:
            # Аватар OK, пост — None (скачивание не удалось)
            mock_fn.side_effect = [
                "https://sb.co/storage/v1/object/public/blog-images/blog-1/avatar.jpg",
                None,
            ]

            avatar_url, post_urls = await persist_profile_images(
                mock_db, "https://sb.co", "blog-1",
                "https://cdn/avatar.jpg", posts,
            )

            assert avatar_url is not None
            assert post_urls == {}

    @pytest.mark.asyncio
    async def test_no_images(self) -> None:
        from src.image_storage import persist_profile_images

        mock_db = MagicMock()

        avatar_url, post_urls = await persist_profile_images(
            mock_db, "https://sb.co", "blog-1", None, [],
        )

        assert avatar_url is None
        assert post_urls == {}

    @pytest.mark.asyncio
    async def test_skips_posts_without_thumbnail(self) -> None:
        from src.image_storage import persist_profile_images

        mock_db = MagicMock()
        posts = [
            {"platform_id": "p1", "thumbnail_url": None},
            {"platform_id": "p2"},  # нет thumbnail_url
        ]

        with patch("src.image_storage.download_and_upload_image", new_callable=AsyncMock) as mock_fn:
            _avatar_url, post_urls = await persist_profile_images(
                mock_db, "https://sb.co", "blog-1", None, posts,
            )

            mock_fn.assert_not_called()
            assert post_urls == {}


class TestDeleteBlogImages:
    """Тесты delete_blog_images."""

    @pytest.mark.asyncio
    async def test_success(self) -> None:
        from src.image_storage import delete_blog_images

        mock_db = MagicMock()
        mock_storage_bucket = MagicMock()
        mock_db.storage.from_.return_value = mock_storage_bucket
        # avatar.jpg пропускается — удаляется только post_p1.jpg
        files = [{"name": "avatar.jpg"}, {"name": "post_p1.jpg"}]
        mock_storage_bucket.list = AsyncMock(return_value=files)
        mock_storage_bucket.remove = AsyncMock()

        # Мок для db.table("blog_posts").update(...).eq(...).execute()
        table_mock = MagicMock()
        mock_db.table.return_value = table_mock
        table_mock.update.return_value = table_mock
        table_mock.eq.return_value = table_mock
        table_mock.execute = AsyncMock()

        result = await delete_blog_images(mock_db, "blog-1")
        assert result == 1  # только post, без avatar
        mock_storage_bucket.remove.assert_called_once()
        table_mock.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_folder(self) -> None:
        from src.image_storage import delete_blog_images

        mock_db = MagicMock()
        mock_storage_bucket = MagicMock()
        mock_db.storage.from_.return_value = mock_storage_bucket
        mock_storage_bucket.list = AsyncMock(return_value=[])

        result = await delete_blog_images(mock_db, "blog-1")
        assert result == 0
        mock_storage_bucket.list.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_error(self) -> None:
        from src.image_storage import delete_blog_images

        mock_db = MagicMock()
        mock_storage_bucket = MagicMock()
        mock_db.storage.from_.return_value = mock_storage_bucket
        mock_storage_bucket.list = AsyncMock(side_effect=Exception("Storage error"))

        result = await delete_blog_images(mock_db, "blog-1")
        assert result == 0

    @pytest.mark.asyncio
    async def test_remove_error(self) -> None:
        from src.image_storage import delete_blog_images

        mock_db = MagicMock()
        mock_storage_bucket = MagicMock()
        mock_db.storage.from_.return_value = mock_storage_bucket
        files = [{"name": "avatar.jpg"}]
        mock_storage_bucket.list = AsyncMock(return_value=files)

        result = await delete_blog_images(mock_db, "blog-1")
        assert result == 0

    @pytest.mark.asyncio
    async def test_skips_unsafe_file_names(self) -> None:
        from src.image_storage import delete_blog_images

        mock_db = MagicMock()
        mock_storage_bucket = MagicMock()
        mock_db.storage.from_.return_value = mock_storage_bucket
        files = [
            {"name": "avatar.jpg"},
            {"name": "../outside.jpg"},
            {"name": "safe_post.jpg"},
        ]
        mock_storage_bucket.list = AsyncMock(return_value=files)
        mock_storage_bucket.remove = AsyncMock()

        table_mock = MagicMock()
        mock_db.table.return_value = table_mock
        table_mock.update.return_value = table_mock
        table_mock.eq.return_value = table_mock
        table_mock.execute = AsyncMock()

        result = await delete_blog_images(mock_db, "blog-1")
        assert result == 1

    @pytest.mark.asyncio
    async def test_rejects_unsafe_blog_id(self) -> None:
        from src.image_storage import delete_blog_images

        mock_db = MagicMock()
        mock_storage_bucket = MagicMock()
        mock_db.storage.from_.return_value = mock_storage_bucket
        mock_storage_bucket.list = AsyncMock()

        result = await delete_blog_images(mock_db, "../evil")
        assert result == 0
        mock_storage_bucket.list.assert_not_called()

    @pytest.mark.asyncio
    async def test_db_update_fails_after_storage_remove(self) -> None:
        """Файлы удалены из Storage, но обнуление thumbnail_url в БД упало.

        Regression: раньше оба вызова были в одном try — если storage.remove
        проходил, но db.table().update() падал, возвращался 0 (как будто файлы не удалены).
        Сейчас storage deletion и DB update — отдельные try-блоки.
        """
        from src.image_storage import delete_blog_images

        mock_db = MagicMock()
        mock_storage_bucket = MagicMock()
        mock_db.storage.from_.return_value = mock_storage_bucket
        files = [
            {"name": "avatar.jpg"},
            {"name": "post_abc.jpg"},
        ]
        mock_storage_bucket.list = AsyncMock(return_value=files)
        mock_storage_bucket.remove = AsyncMock()  # Storage remove OK

        # DB update fails
        table_mock = MagicMock()
        mock_db.table.return_value = table_mock
        table_mock.update.return_value = table_mock
        table_mock.eq.return_value = table_mock
        table_mock.execute = AsyncMock(side_effect=Exception("DB connection lost"))

        result = await delete_blog_images(mock_db, "blog-1")
        # Файлы удалены (1 пост, avatar сохранён), возвращаем count
        assert result == 1
        mock_storage_bucket.remove.assert_called_once()

    @pytest.mark.asyncio
    async def test_storage_remove_fails_db_untouched(self) -> None:
        """Если storage.remove упал, DB update НЕ вызывается."""
        from src.image_storage import delete_blog_images

        mock_db = MagicMock()
        mock_storage_bucket = MagicMock()
        mock_db.storage.from_.return_value = mock_storage_bucket
        files = [{"name": "post_abc.jpg"}]
        mock_storage_bucket.list = AsyncMock(return_value=files)
        mock_storage_bucket.remove = AsyncMock(side_effect=Exception("Storage API error"))

        result = await delete_blog_images(mock_db, "blog-1")
        assert result == 0
        # DB update не должен вызываться если файлы не удалены
        mock_db.table.assert_not_called()
