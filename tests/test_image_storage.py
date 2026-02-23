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


class TestDownloadImage:
    """Тесты download_image."""

    @pytest.mark.asyncio
    async def test_success(self) -> None:
        from src.image_storage import download_image

        mock_response = MagicMock()
        mock_response.content = b"\xff\xd8\xff" * 100
        mock_response.headers = {"content-type": "image/jpeg"}
        mock_response.raise_for_status = MagicMock()

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

        with patch("src.image_storage.run_in_thread", new_callable=AsyncMock) as mock_run:
            result = await upload_image(mock_db, "blog-1/avatar.jpg", b"image-data", "image/jpeg")
            assert result is True
            mock_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_failure(self) -> None:
        from src.image_storage import upload_image

        mock_db = MagicMock()

        with patch("src.image_storage.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = Exception("Storage error")
            result = await upload_image(mock_db, "blog-1/avatar.jpg", b"image-data", "image/jpeg")
            assert result is False

    @pytest.mark.asyncio
    async def test_correct_bucket_and_path(self) -> None:
        from src.image_storage import upload_image

        mock_db = MagicMock()
        mock_storage = MagicMock()
        mock_db.storage.from_.return_value = mock_storage

        with patch("src.image_storage.run_in_thread", new_callable=AsyncMock) as mock_run:
            await upload_image(mock_db, "blog-1/post_p1.jpg", b"data", "image/jpeg")

            # Проверяем аргументы run_in_thread: первый — callable upload
            call_args = mock_run.call_args
            # run_in_thread(db.storage.from_(BUCKET).upload, path, data, opts)
            assert call_args.args[1] == "blog-1/post_p1.jpg"
            assert call_args.args[2] == b"data"
            assert call_args.args[3] == {"content-type": "image/jpeg", "upsert": "true"}


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
            avatar_url, post_urls = await persist_profile_images(
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
        files = [{"name": "avatar.jpg"}, {"name": "post_p1.jpg"}]

        with patch("src.image_storage.run_in_thread", new_callable=AsyncMock) as mock_run:
            # list → files, remove → OK
            mock_run.side_effect = [files, None]

            result = await delete_blog_images(mock_db, "blog-1")
            assert result == 2
            assert mock_run.call_count == 2

    @pytest.mark.asyncio
    async def test_empty_folder(self) -> None:
        from src.image_storage import delete_blog_images

        mock_db = MagicMock()

        with patch("src.image_storage.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = []

            result = await delete_blog_images(mock_db, "blog-1")
            assert result == 0
            # Только вызов list, remove не должен вызываться
            mock_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_error(self) -> None:
        from src.image_storage import delete_blog_images

        mock_db = MagicMock()

        with patch("src.image_storage.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = Exception("Storage error")

            result = await delete_blog_images(mock_db, "blog-1")
            assert result == 0

    @pytest.mark.asyncio
    async def test_remove_error(self) -> None:
        from src.image_storage import delete_blog_images

        mock_db = MagicMock()
        files = [{"name": "avatar.jpg"}]

        with patch("src.image_storage.run_in_thread", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = [files, Exception("Remove error")]

            result = await delete_blog_images(mock_db, "blog-1")
            assert result == 0
