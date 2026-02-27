"""Тесты AI Batch API операций."""
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.ai.schemas import AIInsights
from src.config import Settings
from src.models.blog import ScrapedPost, ScrapedProfile

_DUMMY_TAGS = [
    "видео-контент",
    "reels",
    "юмор",
    "эстетика",
    "сторителлинг",
    "лайфхаки",
    "влог",
]


def _valid_insights(**kwargs: object) -> AIInsights:
    """AIInsights с минимально валидными tags (для round-trip сериализации)."""
    kwargs.setdefault("tags", _DUMMY_TAGS)
    return AIInsights(**kwargs)


def _make_settings() -> Settings:
    return Settings(
        supabase_url="https://test.supabase.co",
        supabase_service_key="test-key",
        openai_api_key="test-openai",
        batch_model="gpt-5-nano",
        scraper_api_key="test-key",
    )


def _make_batch_mock(
    status: str = "completed",
    output_file_id: str | None = "file-out",
    error_file_id: str | None = None,
    total: int = 1,
    completed: int = 1,
    failed: int = 0,
) -> MagicMock:
    """Создать мок batch объекта с числовыми request_counts."""
    mock = MagicMock()
    mock.status = status
    mock.output_file_id = output_file_id
    mock.error_file_id = error_file_id
    mock.request_counts.total = total
    mock.request_counts.completed = completed
    mock.request_counts.failed = failed
    return mock


def _make_profile() -> ScrapedProfile:
    return ScrapedProfile(
        platform_id="12345",
        username="testblogger",
        biography="Test bio",
        follower_count=50000,
        medias=[
            ScrapedPost(
                platform_id="p1",
                media_type=1,
                caption_text="Test post",
                like_count=1000,
                comment_count=50,
                taken_at=datetime(2026, 1, 15, tzinfo=UTC),
            ),
        ],
    )


class TestBuildBatchRequest:
    """Тесты формирования строки запроса для Batch API."""

    def test_structure(self) -> None:
        from src.ai.batch import build_batch_request

        settings = _make_settings()
        profile = _make_profile()
        request = build_batch_request("blog-123", profile, settings)

        assert request["custom_id"] == "blog-123"
        assert request["method"] == "POST"
        assert request["url"] == "/v1/chat/completions"
        assert request["body"]["model"] == "gpt-5-nano"

    def test_has_structured_output(self) -> None:
        from src.ai.batch import build_batch_request

        settings = _make_settings()
        profile = _make_profile()
        request = build_batch_request("blog-123", profile, settings)

        rf = request["body"]["response_format"]
        assert rf["type"] == "json_schema"
        assert rf["json_schema"]["strict"] is True
        assert rf["json_schema"]["name"] == "ai_insights"

    def test_messages_included(self) -> None:
        from src.ai.batch import build_batch_request

        settings = _make_settings()
        profile = _make_profile()
        request = build_batch_request("blog-123", profile, settings)

        messages = request["body"]["messages"]
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"

    def test_image_map_passed_to_prompt(self) -> None:
        """image_map передаётся в build_analysis_prompt через build_batch_request."""
        from src.ai.batch import build_batch_request

        settings = _make_settings()
        profile = ScrapedProfile(
            platform_id="12345",
            username="imgtest",
            profile_pic_url="https://example.com/avatar.jpg",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    thumbnail_url="https://example.com/post1.jpg",
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        image_map = {
            "https://example.com/avatar.jpg": "data:image/jpeg;base64,abc",
            "https://example.com/post1.jpg": "data:image/jpeg;base64,def",
        }

        request = build_batch_request("blog-123", profile, settings, image_map=image_map)

        messages = request["body"]["messages"]
        content = messages[1]["content"]
        image_parts = [p for p in content if p["type"] == "image_url"]
        urls = [p["image_url"]["url"] for p in image_parts]
        assert "data:image/jpeg;base64,abc" in urls
        assert "data:image/jpeg;base64,def" in urls

    def test_text_only_no_images(self) -> None:
        """text_only=True → изображения не включаются в запрос."""
        from src.ai.batch import build_batch_request

        settings = _make_settings()
        profile = ScrapedProfile(
            platform_id="12345",
            username="textonly",
            profile_pic_url="https://example.com/avatar.jpg",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    thumbnail_url="https://example.com/post1.jpg",
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        image_map = {
            "https://example.com/avatar.jpg": "data:image/jpeg;base64,abc",
            "https://example.com/post1.jpg": "data:image/jpeg;base64,def",
        }

        request = build_batch_request(
            "blog-123", profile, settings, image_map=image_map, text_only=True,
        )

        messages = request["body"]["messages"]
        # System prompt содержит указание о text-only
        assert "Изображения для этого профиля недоступны" in messages[0]["content"]
        # User content не содержит изображений
        content = messages[1]["content"]
        image_parts = [p for p in content if p["type"] == "image_url"]
        assert image_parts == []

    def test_text_only_false_includes_images(self) -> None:
        """text_only=False (default) → изображения включены."""
        from src.ai.batch import build_batch_request

        settings = _make_settings()
        profile = ScrapedProfile(
            platform_id="12345",
            username="withimages",
            profile_pic_url="https://example.com/avatar.jpg",
            follower_count=10000,
        )
        image_map = {"https://example.com/avatar.jpg": "data:image/jpeg;base64,abc"}

        request = build_batch_request("blog-123", profile, settings, image_map=image_map)

        messages = request["body"]["messages"]
        assert "Изображения для этого профиля недоступны" not in messages[0]["content"]
        content = messages[1]["content"]
        image_parts = [p for p in content if p["type"] == "image_url"]
        assert len(image_parts) == 1


class TestSubmitBatch:
    """Тесты отправки батча в OpenAI."""

    @pytest.mark.asyncio
    async def test_submit_creates_batch(self) -> None:
        from unittest.mock import patch

        from src.ai.batch import submit_batch

        settings = _make_settings()
        profile = _make_profile()

        mock_client = MagicMock()
        mock_file = MagicMock()
        mock_file.id = "file-abc"
        mock_client.files.create = AsyncMock(return_value=mock_file)

        mock_batch = MagicMock()
        mock_batch.id = "batch-xyz"
        mock_client.batches.create = AsyncMock(return_value=mock_batch)

        with patch("src.ai.batch.resolve_profile_images", new_callable=AsyncMock, return_value={}):
            batch_id = await submit_batch(
                mock_client, [("blog-123", profile)], settings
            )

        assert batch_id == "batch-xyz"
        mock_client.files.create.assert_called_once()
        mock_client.batches.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_submit_empty_profiles_raises(self) -> None:
        from src.ai.batch import submit_batch

        settings = _make_settings()
        mock_client = MagicMock()

        with pytest.raises(ValueError, match="Cannot submit empty batch"):
            await submit_batch(mock_client, [], settings)

    @pytest.mark.asyncio
    async def test_submit_multiple_profiles(self) -> None:
        from unittest.mock import patch

        from src.ai.batch import submit_batch

        settings = _make_settings()
        profiles = [
            ("blog-1", _make_profile()),
            ("blog-2", _make_profile()),
            ("blog-3", _make_profile()),
        ]

        mock_client = MagicMock()
        mock_file = MagicMock()
        mock_file.id = "file-abc"
        mock_client.files.create = AsyncMock(return_value=mock_file)

        mock_batch = MagicMock()
        mock_batch.id = "batch-multi"
        mock_client.batches.create = AsyncMock(return_value=mock_batch)

        with patch("src.ai.batch.resolve_profile_images", new_callable=AsyncMock, return_value={}):
            batch_id = await submit_batch(mock_client, profiles, settings)
        assert batch_id == "batch-multi"

    @pytest.mark.asyncio
    async def test_submit_calls_resolve_for_each_profile(self) -> None:
        """resolve_profile_images вызывается для каждого профиля."""
        from unittest.mock import patch

        from src.ai.batch import submit_batch

        settings = _make_settings()
        profile1 = _make_profile()
        profile2 = _make_profile()

        mock_client = MagicMock()
        mock_file = MagicMock()
        mock_file.id = "file-abc"
        mock_client.files.create = AsyncMock(return_value=mock_file)

        mock_batch = MagicMock()
        mock_batch.id = "batch-resolve"
        mock_client.batches.create = AsyncMock(return_value=mock_batch)

        with patch("src.ai.batch.resolve_profile_images", new_callable=AsyncMock, return_value={}) as mock_resolve:
            await submit_batch(mock_client, [("b1", profile1), ("b2", profile2)], settings)

        assert mock_resolve.call_count == 2
        # Проверяем что передавались правильные профили
        assert mock_resolve.call_args_list[0][0][0] is profile1
        assert mock_resolve.call_args_list[1][0][0] is profile2

    @pytest.mark.asyncio
    async def test_text_only_skips_image_download(self) -> None:
        """text_only профили не участвуют в загрузке изображений."""
        from unittest.mock import patch

        from src.ai.batch import submit_batch

        settings = _make_settings()
        profile1 = _make_profile()
        profile2 = _make_profile()

        mock_client = MagicMock()
        mock_file = MagicMock()
        mock_file.id = "file-abc"
        mock_client.files.create = AsyncMock(return_value=mock_file)

        mock_batch = MagicMock()
        mock_batch.id = "batch-textonly"
        mock_client.batches.create = AsyncMock(return_value=mock_batch)

        with patch(
            "src.ai.batch.resolve_profile_images",
            new_callable=AsyncMock, return_value={},
        ) as mock_resolve:
            await submit_batch(
                mock_client,
                [("b1", profile1), ("b2", profile2)],
                settings,
                text_only_ids={"b1"},
            )

        # Только 1 вызов resolve (для b2, b1 — text-only)
        assert mock_resolve.call_count == 1
        assert mock_resolve.call_args_list[0][0][0] is profile2


class TestPollBatch:
    """Тесты проверки статуса батча."""

    @pytest.mark.asyncio
    async def test_in_progress(self) -> None:
        from src.ai.batch import poll_batch

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(status="in_progress", output_file_id=None)
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        result = await poll_batch(mock_client, "batch-123")
        assert result["status"] == "in_progress"
        assert "results" not in result

    @pytest.mark.asyncio
    async def test_completed_with_results(self) -> None:
        from src.ai.batch import poll_batch

        insights_json = _valid_insights().model_dump_json()
        output_line = json.dumps({
            "custom_id": "blog-1",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{
                        "message": {"content": insights_json}
                    }]
                }
            },
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock()
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_content = MagicMock()
        mock_content.text = output_line
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-123")

        assert result["status"] == "completed"
        assert "blog-1" in result["results"]
        assert isinstance(result["results"]["blog-1"], AIInsights)

    @pytest.mark.asyncio
    async def test_completed_no_output_file(self) -> None:
        from src.ai.batch import poll_batch

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(output_file_id=None, total=0, completed=0)
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        result = await poll_batch(mock_client, "batch-456")
        assert result["status"] == "completed"
        assert result["results"] == {}

    @pytest.mark.asyncio
    async def test_completed_with_error_file(self) -> None:
        """error_file_id содержит провалившиеся запросы — они попадают в results как None."""
        from src.ai.batch import poll_batch

        # Успешный результат
        insights_json = _valid_insights().model_dump_json()
        output_line = json.dumps({
            "custom_id": "blog-1",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{"message": {"content": insights_json}}]
                }
            },
        })

        # Ошибочный результат
        error_line = json.dumps({
            "custom_id": "blog-2",
            "error": {"code": "server_error", "message": "Internal error"},
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(
            error_file_id="file-err", total=2, completed=1, failed=1,
        )
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_output = MagicMock()
        mock_output.text = output_line
        mock_error = MagicMock()
        mock_error.text = error_line

        async def _files_content(fid: str) -> MagicMock:
            return mock_output if fid == "file-out" else mock_error

        mock_client.files.content = _files_content

        result = await poll_batch(mock_client, "batch-mixed")

        assert result["status"] == "completed"
        assert isinstance(result["results"]["blog-1"], AIInsights)
        assert result["results"]["blog-2"] is None

    @pytest.mark.asyncio
    async def test_completed_only_errors(self) -> None:
        """Батч без output_file_id, только error_file_id."""
        from src.ai.batch import poll_batch

        error_line = json.dumps({
            "custom_id": "blog-3",
            "error": {"code": "rate_limit", "message": "Rate limited"},
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(
            output_file_id=None, error_file_id="file-err",
            total=1, completed=0, failed=1,
        )
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_error = MagicMock()
        mock_error.text = error_line
        mock_client.files.content = AsyncMock(return_value=mock_error)

        result = await poll_batch(mock_client, "batch-errors")

        assert result["status"] == "completed"
        assert result["results"]["blog-3"] is None

    @pytest.mark.asyncio
    async def test_refusal_returns_tuple(self) -> None:
        """Refusal возвращает tuple ('refusal', reason), не None."""
        from src.ai.batch import poll_batch

        output_line = json.dumps({
            "custom_id": "blog-2",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{
                        "message": {"refusal": "Content filtered"}
                    }]
                }
            },
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock()
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_content = MagicMock()
        mock_content.text = output_line
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-123")

        assert result["results"]["blog-2"] == ("refusal", "Content filtered")

    @pytest.mark.asyncio
    async def test_content_as_text_parts_parsed(self) -> None:
        """message.content как list[text-part] корректно парсится."""
        from src.ai.batch import poll_batch

        insights_json = _valid_insights().model_dump_json()
        output_line = json.dumps({
            "custom_id": "blog-3",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{
                        "message": {
                            "content": [{"type": "text", "text": insights_json}]
                        },
                    }],
                },
            },
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock()
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_content = MagicMock()
        mock_content.text = output_line
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-content-parts")

        assert isinstance(result["results"]["blog-3"], AIInsights)

    @pytest.mark.asyncio
    async def test_status_code_error_returns_none(self) -> None:
        """response.status_code >= 400 помечается как None."""
        from src.ai.batch import poll_batch

        output_line = json.dumps({
            "custom_id": "blog-http-error",
            "response": {
                "status_code": 429,
                "body": {},
            },
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock()
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_content = MagicMock()
        mock_content.text = output_line
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-http-err")

        assert result["results"]["blog-http-error"] is None

    @pytest.mark.asyncio
    async def test_status_code_zero_returns_none(self) -> None:
        """status_code=0 — внутренний сбой OpenAI → None."""
        from src.ai.batch import poll_batch

        output_line = json.dumps({
            "custom_id": "blog-zero",
            "response": {
                "status_code": 0,
                "request_id": "",
                "body": {},
            },
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock()
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_content = MagicMock()
        mock_content.text = output_line
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-zero")

        assert result["results"]["blog-zero"] is None

    @pytest.mark.asyncio
    async def test_failed_status(self) -> None:
        """Батч со статусом 'failed' — нет файлов, просто статус."""
        from src.ai.batch import poll_batch

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(status="failed", output_file_id=None)
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        result = await poll_batch(mock_client, "batch-fail")
        assert result["status"] == "failed"
        assert "results" not in result

    @pytest.mark.asyncio
    async def test_expired_status_no_files(self) -> None:
        """Expired без файлов — пустые results."""
        from src.ai.batch import poll_batch

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(
            status="expired", output_file_id=None, total=0, completed=0,
        )
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        result = await poll_batch(mock_client, "batch-expired")
        assert result["status"] == "expired"
        assert result["results"] == {}

    @pytest.mark.asyncio
    async def test_cancelled_status(self) -> None:
        """Батч со статусом 'cancelled' — не completed."""
        from src.ai.batch import poll_batch

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(status="cancelled", output_file_id=None)
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        result = await poll_batch(mock_client, "batch-cancel")
        assert result["status"] == "cancelled"
        assert "results" not in result

    @pytest.mark.asyncio
    async def test_cancelling_status(self) -> None:
        """'cancelling' — промежуточный статус, не терминальный."""
        from src.ai.batch import poll_batch

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(status="cancelling", output_file_id=None)
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        result = await poll_batch(mock_client, "batch-cancelling")
        assert result["status"] == "cancelling"
        assert "results" not in result

    @pytest.mark.asyncio
    async def test_malformed_jsonl_line_skipped(self) -> None:
        """Битая строка JSONL не валит весь батч — пропускается."""
        from src.ai.batch import poll_batch

        insights_json = _valid_insights().model_dump_json()
        good_line = json.dumps({
            "custom_id": "blog-1",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{"message": {"content": insights_json}}]
                }
            },
        })
        bad_line = "this is not valid json {"

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(total=2, completed=2)
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_content = MagicMock()
        mock_content.text = f"{bad_line}\n{good_line}"
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-malformed")

        assert result["status"] == "completed"
        # Хорошая строка обработана, битая пропущена
        assert isinstance(result["results"]["blog-1"], AIInsights)
        assert len(result["results"]) == 1

    @pytest.mark.asyncio
    async def test_malformed_error_jsonl_skipped(self) -> None:
        """Битая строка в error file пропускается."""
        from src.ai.batch import poll_batch

        good_error = json.dumps({
            "custom_id": "blog-2",
            "error": {"code": "server_error", "message": "fail"},
        })
        bad_line = "{invalid json"

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(
            output_file_id=None, error_file_id="file-err",
            total=2, completed=0, failed=2,
        )
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_error = MagicMock()
        mock_error.text = f"{bad_line}\n{good_error}"
        mock_client.files.content = AsyncMock(return_value=mock_error)

        result = await poll_batch(mock_client, "batch-err-malformed")

        assert result["status"] == "completed"
        assert result["results"]["blog-2"] is None
        assert len(result["results"]) == 1

    @pytest.mark.asyncio
    async def test_missing_custom_id_skipped(self) -> None:
        """Строка без custom_id пропускается."""
        from src.ai.batch import poll_batch

        # Строка без custom_id
        no_id_line = json.dumps({
            "response": {
                "status_code": 200,
                "body": {"choices": [{"message": {"content": "{}"}}]},
            }
        })
        good_line = json.dumps({
            "custom_id": "blog-ok",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{"message": {"content": _valid_insights().model_dump_json()}}]
                }
            },
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(total=2, completed=2)
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_content = MagicMock()
        mock_content.text = f"{no_id_line}\n{good_line}"
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-noid")

        assert "blog-ok" in result["results"]
        assert len(result["results"]) == 1

    @pytest.mark.asyncio
    async def test_expired_with_partial_results(self) -> None:
        """Expired батч МОЖЕТ иметь partial output_file_id — результаты должны обрабатываться."""
        from src.ai.batch import poll_batch

        insights_json = _valid_insights().model_dump_json()
        output_line = json.dumps({
            "custom_id": "blog-1",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{"message": {"content": insights_json}}]
                }
            },
        })

        error_line = json.dumps({
            "custom_id": "blog-2",
            "error": {"code": "batch_expired", "message": "Batch expired"},
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(
            status="expired", output_file_id="file-partial",
            error_file_id="file-err", total=2, completed=1, failed=1,
        )
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_output = MagicMock()
        mock_output.text = output_line
        mock_error = MagicMock()
        mock_error.text = error_line

        async def _files_content_expired(fid: str) -> MagicMock:
            return mock_output if fid == "file-partial" else mock_error

        mock_client.files.content = _files_content_expired

        result = await poll_batch(mock_client, "batch-expired-partial")

        # Expired с файлами — результаты должны быть обработаны
        assert result["status"] == "expired"
        assert "results" in result
        assert isinstance(result["results"]["blog-1"], AIInsights)
        assert result["results"]["blog-2"] is None

    @pytest.mark.asyncio
    async def test_response_null_in_output_line(self) -> None:
        """Строка с response: null в output file → не крашит парсинг."""
        from src.ai.batch import poll_batch

        # response: null — такое возможно при batch API edge cases
        null_response_line = json.dumps({
            "custom_id": "blog-null",
            "response": None,
        })
        good_line = json.dumps({
            "custom_id": "blog-ok",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{"message": {"content": _valid_insights().model_dump_json()}}]
                }
            },
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(total=2, completed=2)
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_content = MagicMock()
        mock_content.text = f"{null_response_line}\n{good_line}"
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-null-resp")

        # Строка с response=null → None, хорошая строка → AIInsights
        assert result["results"]["blog-null"] is None
        assert isinstance(result["results"]["blog-ok"], AIInsights)

    @pytest.mark.asyncio
    async def test_validating_status(self) -> None:
        """'validating' — промежуточный статус, не терминальный."""
        from src.ai.batch import poll_batch

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(status="validating", output_file_id=None)
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        result = await poll_batch(mock_client, "batch-validating")
        assert result["status"] == "validating"
        assert "results" not in result

    @pytest.mark.asyncio
    async def test_finalizing_status(self) -> None:
        """'finalizing' — промежуточный статус, не терминальный."""
        from src.ai.batch import poll_batch

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(status="finalizing", output_file_id=None)
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        result = await poll_batch(mock_client, "batch-finalizing")
        assert result["status"] == "finalizing"
        assert "results" not in result

    @pytest.mark.asyncio
    async def test_empty_content_returns_none(self) -> None:
        """Пустой content от AI → results[id] = None (ValidationError перехватывается)."""
        from src.ai.batch import poll_batch

        empty_content_line = json.dumps({
            "custom_id": "blog-empty",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{"message": {"content": ""}}]
                }
            },
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock()
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_content = MagicMock()
        mock_content.text = empty_content_line
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-empty-content")

        assert result["results"]["blog-empty"] is None

    @pytest.mark.asyncio
    async def test_json_in_code_fence_parsed(self) -> None:
        """JSON внутри markdown code fence корректно парсится fallback-ом."""
        from src.ai.batch import poll_batch

        wrapped_json = f"```json\n{_valid_insights().model_dump_json()}\n```"
        output_line = json.dumps({
            "custom_id": "blog-fenced",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{"message": {"content": wrapped_json}}],
                },
            },
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock()
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_content = MagicMock()
        mock_content.text = output_line
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-fenced-json")
        assert isinstance(result["results"]["blog-fenced"], AIInsights)

    @pytest.mark.asyncio
    async def test_duplicate_custom_id_last_wins(self) -> None:
        """Две строки с одинаковым custom_id — последняя перезаписывает."""
        from src.ai.batch import poll_batch

        insights1 = _valid_insights(confidence=1)
        insights2 = _valid_insights(confidence=5)

        line1 = json.dumps({
            "custom_id": "blog-dup",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{"message": {"content": insights1.model_dump_json()}}]
                }
            },
        })
        line2 = json.dumps({
            "custom_id": "blog-dup",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{"message": {"content": insights2.model_dump_json()}}]
                }
            },
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock(total=2, completed=2)
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)

        mock_content = MagicMock()
        mock_content.text = f"{line1}\n{line2}"
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-dup")

        # Последняя строка перезаписала
        assert result["results"]["blog-dup"].confidence == 5


class TestMatchCategories:
    """Тесты сопоставления тем с категориями."""

    @pytest.mark.asyncio
    async def test_matches_primary_and_secondary(self) -> None:
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["beauty"]  # код категории
        insights.content.secondary_topics = ["Fashion", "Travel"]  # русские/англ. названия

        mock_db = MagicMock()
        cat_mock = MagicMock()
        cat_mock.data = [
            {"id": "cat-1", "code": "beauty", "name": "Красота", "parent_id": None},
            {"id": "cat-2", "code": "fashion", "name": "fashion", "parent_id": None},
            {"id": "cat-3", "code": "travel", "name": "travel", "parent_id": None},
        ]
        mock_db.table.return_value.select.return_value.execute.return_value = cat_mock

        stats = await match_categories(mock_db, "blog-1", insights)

        # delete старых + insert новых: 3 записи (1 primary + 2 secondary)
        assert mock_db.table.return_value.delete.call_count == 1
        assert mock_db.table.return_value.insert.call_count == 1
        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert len(rows) == 3
        assert stats == {"total": 3, "matched": 3, "unmatched": 0}

    @pytest.mark.asyncio
    async def test_skips_when_no_categories(self) -> None:
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = []

        mock_db = MagicMock()

        await match_categories(mock_db, "blog-1", insights)

        mock_db.table.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_matching_categories_in_db(self) -> None:
        """Тема есть, но в categories таблице совпадений нет → upsert не вызывается."""
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["cooking"]  # код, которого нет в БД
        insights.content.secondary_topics = ["Gardening"]

        mock_db = MagicMock()
        cat_mock = MagicMock()
        cat_mock.data = [
            {"id": "cat-1", "code": "beauty", "name": "Красота", "parent_id": None},
            {"id": "cat-2", "code": "fashion", "name": "Мода", "parent_id": None},
        ]
        mock_db.table.return_value.select.return_value.execute.return_value = cat_mock

        await match_categories(mock_db, "blog-1", insights)

        # insert не должен вызываться — нет совпадений
        mock_db.table.return_value.insert.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_secondary_topics(self) -> None:
        """Только primary_categories, без secondary → delete + insert."""
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["beauty"]  # код категории
        insights.content.secondary_topics = []

        mock_db = MagicMock()
        cat_mock = MagicMock()
        cat_mock.data = [
            {"id": "cat-1", "code": "beauty", "name": "Красота", "parent_id": None},
        ]
        mock_db.table.return_value.select.return_value.execute.return_value = cat_mock

        await match_categories(mock_db, "blog-1", insights)

        # delete старых категорий + insert новых
        assert mock_db.table.return_value.delete.call_count == 1
        assert mock_db.table.return_value.insert.call_count == 1
        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert len(rows) == 1
        assert rows[0]["is_primary"] is True

    @pytest.mark.asyncio
    async def test_case_insensitive_matching(self) -> None:
        """Сопоставление нечувствительно к регистру."""
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["BEAUTY"]  # код в верхнем регистре
        insights.content.secondary_topics = ["Макияж"]

        mock_db = MagicMock()
        cat_mock = MagicMock()
        cat_mock.data = [
            {"id": "cat-1", "code": "beauty", "name": "Красота", "parent_id": None},
            {"id": "sub-1", "code": None, "name": "Макияж", "parent_id": "cat-1"},
        ]
        mock_db.table.return_value.select.return_value.execute.return_value = cat_mock

        await match_categories(mock_db, "blog-1", insights)

        # "BEAUTY".lower() == "beauty" (code) → совпадение
        # "Макияж".lower() == "макияж" (name_lower) → совпадение
        assert mock_db.table.return_value.insert.call_count == 1
        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_primary_is_secondary_upserts_correctly(self) -> None:
        """is_primary=False для secondary topics."""
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["beauty"]  # код категории
        insights.content.secondary_topics = ["Путешествия"]  # русское название подкатегории

        mock_db = MagicMock()
        cat_mock = MagicMock()
        cat_mock.data = [
            {"id": "cat-1", "code": "beauty", "name": "Красота", "parent_id": None},
            {"id": "cat-3", "code": "travel", "name": "Путешествия", "parent_id": None},
        ]
        mock_db.table.return_value.select.return_value.execute.return_value = cat_mock

        await match_categories(mock_db, "blog-1", insights)

        # delete + insert: 1 вызов каждый, 2 записи
        assert mock_db.table.return_value.insert.call_count == 1
        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert len(rows) == 2
        # Первый — primary (is_primary=True)
        assert rows[0]["is_primary"] is True
        assert rows[0]["category_id"] == "cat-1"
        # Второй — secondary (is_primary=False)
        assert rows[1]["is_primary"] is False
        assert rows[1]["category_id"] == "cat-3"

    @pytest.mark.asyncio
    async def test_primary_category_in_secondary_keeps_is_primary_true(self) -> None:
        """primary_categories[0] совпадает с элементом secondary_topics — is_primary=True сохраняется."""
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["beauty"]  # код категории
        # "beauty" как secondary должна быть пропущена (дублирует primary)
        insights.content.secondary_topics = ["beauty", "Путешествия"]

        mock_db = MagicMock()
        cat_mock = MagicMock()
        cat_mock.data = [
            {"id": "cat-1", "code": "beauty", "name": "Красота", "parent_id": None},
            {"id": "cat-3", "code": "travel", "name": "Путешествия", "parent_id": None},
        ]
        mock_db.table.return_value.select.return_value.execute.return_value = cat_mock

        await match_categories(mock_db, "blog-1", insights)

        # delete + insert: 2 записи (beauty дубль пропущен)
        assert mock_db.table.return_value.insert.call_count == 1
        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert len(rows) == 2

        # Проверяем, что "beauty" (cat-1) записана как is_primary=True
        beauty_rows = [r for r in rows if r["category_id"] == "cat-1"]
        assert len(beauty_rows) == 1
        assert beauty_rows[0]["is_primary"] is True

        # Путешествия — secondary
        travel_rows = [r for r in rows if r["category_id"] == "cat-3"]
        assert len(travel_rows) == 1
        assert travel_rows[0]["is_primary"] is False

    @pytest.mark.asyncio
    async def test_primary_matches_by_code(self) -> None:
        """Primary topic = код категории → матчится по code."""
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["beauty"]  # код, не название
        insights.content.secondary_topics = ["Макияж"]  # русское название подкатегории

        categories = {
            "beauty": "cat-1",       # code → id
            "макияж": "sub-1",       # name_lower → id
        }

        mock_db = MagicMock()

        await match_categories(mock_db, "blog-1", insights, categories=categories)

        # delete + insert: 2 записи
        assert mock_db.table.return_value.insert.call_count == 1
        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert len(rows) == 2
        # Первый — primary
        assert rows[0]["category_id"] == "cat-1"
        assert rows[0]["is_primary"] is True
        # Второй — secondary
        assert rows[1]["category_id"] == "sub-1"
        assert rows[1]["is_primary"] is False


class TestMatchCategoriesEdge:
    """Edge case тесты match_categories."""

    @pytest.mark.asyncio
    async def test_category_with_none_name_skipped(self) -> None:
        """Категория с name=None в БД → не крашит dict comprehension."""
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["beauty"]  # код категории
        insights.content.secondary_topics = []

        mock_db = MagicMock()
        cat_mock = MagicMock()
        cat_mock.data = [
            {"id": "cat-1", "code": "beauty", "name": "Красота", "parent_id": None},
            {"id": "cat-bad", "code": None, "name": None, "parent_id": None},
        ]
        mock_db.table.return_value.select.return_value.execute.return_value = cat_mock

        await match_categories(mock_db, "blog-1", insights)

        # beauty найдена по code, insert вызван один раз
        assert mock_db.table.return_value.insert.call_count == 1

    @pytest.mark.asyncio
    async def test_empty_categories_list(self) -> None:
        """primary_categories=[] (пустой список) → пропуск (falsy)."""
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = []

        mock_db = MagicMock()

        await match_categories(mock_db, "blog-1", insights)

        mock_db.table.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_categories_table(self) -> None:
        """Таблица categories пуста → upsert не вызывается."""
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["beauty"]  # код категории

        mock_db = MagicMock()
        cat_mock = MagicMock()
        cat_mock.data = []
        mock_db.table.return_value.select.return_value.execute.return_value = cat_mock

        await match_categories(mock_db, "blog-1", insights)

        mock_db.table.return_value.insert.assert_not_called()


class TestMatchTags:
    """Тесты присвоения тегов блогеру."""

    @pytest.mark.asyncio
    async def test_matches_known_tags(self) -> None:
        """Теги из справочника -> delete + insert в blog_tags."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео-контент", "reels", "юмор"]

        tags_cache = {
            "видео-контент": "tag-1",
            "reels": "tag-2",
            "юмор": "tag-3",
            "фото-контент": "tag-4",
        }

        mock_db = MagicMock()

        stats = await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        # delete старых + insert новых: 3 записи
        assert mock_db.table.return_value.delete.call_count == 1
        assert mock_db.table.return_value.insert.call_count == 1
        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert len(rows) == 3
        assert stats == {"total": 3, "matched": 3, "unmatched": 0}

    @pytest.mark.asyncio
    async def test_skips_unknown_tags(self) -> None:
        """Теги, отсутствующие в справочнике, пропускаются."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео-контент", "новый-неизвестный-тег"]

        tags_cache = {"видео-контент": "tag-1"}

        mock_db = MagicMock()

        await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        # Только 1 известный тег → delete + insert
        assert mock_db.table.return_value.delete.call_count == 1
        assert mock_db.table.return_value.insert.call_count == 1

    @pytest.mark.asyncio
    async def test_empty_tags(self) -> None:
        """Пустой список тегов — нет upserts."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = []

        mock_db = MagicMock()
        await match_tags(mock_db, "blog-1", insights, tags={})

        mock_db.table.assert_not_called()

    @pytest.mark.asyncio
    async def test_loads_tags_from_db_when_cache_is_none(self) -> None:
        """Если tags=None — загрузить из БД."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео-контент"]

        mock_db = MagicMock()
        # load_tags query
        tags_mock = MagicMock()
        tags_mock.data = [{"id": "tag-1", "name": "видео-контент"}]
        mock_db.table.return_value.select.return_value.execute.return_value = tags_mock

        await match_tags(mock_db, "blog-1", insights, tags=None)

        # Должен загрузить теги из БД (table("tags").select)
        mock_db.table.assert_any_call("tags")

    @pytest.mark.asyncio
    async def test_case_insensitive_matching(self) -> None:
        """Матчинг тегов регистронезависимый."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["Видео-контент"]  # с большой буквы

        tags_cache = {"видео-контент": "tag-1"}

        mock_db = MagicMock()

        await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        # delete + insert
        assert mock_db.table.return_value.delete.call_count == 1
        assert mock_db.table.return_value.insert.call_count == 1


class TestParseResultLineLogging:
    """Тесты логирования при парсинге результатов."""

    @pytest.mark.asyncio
    async def test_confidence_logged_as_int(self) -> None:
        """confidence логируется как int, не float."""
        from src.ai.batch import poll_batch

        insights = _valid_insights(confidence=3)
        output_line = json.dumps({
            "custom_id": "blog-conf",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{"message": {"content": insights.model_dump_json()}}]
                }
            },
        })

        mock_client = MagicMock()
        mock_batch = _make_batch_mock()
        mock_client.batches.retrieve = AsyncMock(return_value=mock_batch)
        mock_content = MagicMock()
        mock_content.text = output_line
        mock_client.files.content = AsyncMock(return_value=mock_content)

        result = await poll_batch(mock_client, "batch-conf")
        assert result["results"]["blog-conf"].confidence == 3


class TestMatchCategoriesLogging:
    """Тесты логирования пропущенных категорий."""

    @pytest.mark.asyncio
    async def test_logs_missing_primary_category(self) -> None:
        """Предупреждение если primary_category не найден в справочнике."""
        from unittest.mock import patch

        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["cooking"]

        categories_cache = {"beauty": "cat-1", "fashion": "cat-2"}

        mock_db = MagicMock()

        with patch("src.ai.batch.logger") as mock_logger:
            await match_categories(mock_db, "blog-1", insights, categories=categories_cache)

        mock_logger.warning.assert_called_once()
        call_msg = mock_logger.warning.call_args[0][0]
        assert "cooking" in call_msg
        assert "blog-1" in call_msg

    @pytest.mark.asyncio
    async def test_logs_missing_secondary_topic(self) -> None:
        """Предупреждение для каждого secondary_topic, не найденного в справочнике."""
        from unittest.mock import patch

        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["beauty"]
        insights.content.secondary_topics = ["Fashion", "Gardening"]

        categories_cache = {"beauty": "cat-1", "fashion": "cat-2"}

        mock_db = MagicMock()

        with patch("src.ai.batch.logger") as mock_logger:
            await match_categories(mock_db, "blog-1", insights, categories=categories_cache)

        # 1 warning за Gardening (beauty найден, fashion найден)
        assert mock_logger.warning.call_count == 1
        call_msg = mock_logger.warning.call_args[0][0]
        assert "Gardening" in call_msg

    @pytest.mark.asyncio
    async def test_no_log_when_all_categories_found(self) -> None:
        """Нет предупреждений когда все категории найдены."""
        from unittest.mock import patch

        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["beauty"]
        insights.content.secondary_topics = ["Fashion"]

        categories_cache = {"beauty": "cat-1", "fashion": "cat-2"}

        mock_db = MagicMock()

        with patch("src.ai.batch.logger") as mock_logger:
            await match_categories(mock_db, "blog-1", insights, categories=categories_cache)

        mock_logger.warning.assert_not_called()


class TestMatchTagsLogging:
    """Тесты логирования пропущенных тегов."""

    @pytest.mark.asyncio
    async def test_logs_missing_tag(self) -> None:
        """Предупреждение для тега, не найденного в справочнике."""
        from unittest.mock import patch

        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео-контент", "новый-тег"]

        tags_cache = {"видео-контент": "tag-1"}

        mock_db = MagicMock()

        with patch("src.ai.batch.logger") as mock_logger:
            await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        assert mock_logger.warning.call_count == 1
        call_msg = mock_logger.warning.call_args[0][0]
        assert "новый-тег" in call_msg
        assert "blog-1" in call_msg

    @pytest.mark.asyncio
    async def test_no_log_when_all_tags_found(self) -> None:
        """Нет предупреждений когда все теги найдены."""
        from unittest.mock import patch

        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео-контент", "reels"]

        tags_cache = {"видео-контент": "tag-1", "reels": "tag-2"}

        mock_db = MagicMock()

        with patch("src.ai.batch.logger") as mock_logger:
            await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        mock_logger.warning.assert_not_called()

    @pytest.mark.asyncio
    async def test_logs_multiple_missing_tags(self) -> None:
        """Предупреждение для каждого пропущенного тега."""
        from unittest.mock import patch

        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео-контент", "новый-тег-1", "новый-тег-2"]

        tags_cache = {"видео-контент": "tag-1"}

        mock_db = MagicMock()

        with patch("src.ai.batch.logger") as mock_logger:
            await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        assert mock_logger.warning.call_count == 2


class TestFuzzyLookup:
    """Тесты fuzzy matching."""

    def test_exact_match(self) -> None:
        from src.ai.batch import _fuzzy_lookup

        cache = {"beauty": "cat-1", "fitness": "cat-2"}
        assert _fuzzy_lookup("beauty", cache) == "cat-1"

    def test_normalized_match_ampersand(self) -> None:
        from src.ai.batch import _fuzzy_lookup

        cache = {"beauty makeup": "cat-1"}
        assert _fuzzy_lookup("beauty & makeup", cache) == "cat-1"

    def test_normalized_match_hyphen(self) -> None:
        from src.ai.batch import _fuzzy_lookup

        cache = {"видео контент": "tag-1"}
        assert _fuzzy_lookup("видео-контент", cache) == "tag-1"

    def test_fuzzy_match_close(self) -> None:
        from src.ai.batch import _fuzzy_lookup

        cache = {"путешествия": "cat-1"}
        # "путешествие" (единственное число) достаточно близко
        result = _fuzzy_lookup("путешествие", cache)
        assert result == "cat-1"

    def test_no_match(self) -> None:
        from src.ai.batch import _fuzzy_lookup

        cache = {"beauty": "cat-1"}
        assert _fuzzy_lookup("программирование", cache) is None

    def test_empty_cache(self) -> None:
        from src.ai.batch import _fuzzy_lookup

        assert _fuzzy_lookup("beauty", {}) is None


class TestMatchCategoriesFuzzy:
    """Тесты fuzzy matching в match_categories."""

    @pytest.mark.asyncio
    async def test_fuzzy_primary_match(self) -> None:
        from src.ai.batch import match_categories

        insights = AIInsights()
        insights.content.primary_categories = ["beauty & makeup"]

        mock_db = MagicMock()
        categories = {"beauty makeup": "cat-1"}

        await match_categories(mock_db, "blog-1", insights, categories=categories)

        assert mock_db.table.return_value.insert.call_count == 1
        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert rows[0]["category_id"] == "cat-1"

    @pytest.mark.asyncio
    async def test_fuzzy_tag_match(self) -> None:
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["видео контент"]  # пробел вместо дефиса

        tags_cache = {"видео-контент": "tag-1"}
        mock_db = MagicMock()

        await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        assert mock_db.table.return_value.insert.call_count == 1


class TestMatchTagsENtoRU:
    """Тесты EN→RU перевода тегов."""

    @pytest.mark.asyncio
    async def test_en_tag_translated_to_ru(self) -> None:
        """'video-content' → матчится как 'видео-контент'."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["video-content"]

        tags_cache = {"видео-контент": "tag-1"}
        mock_db = MagicMock()

        await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        assert mock_db.table.return_value.insert.call_count == 1
        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert rows[0]["tag_id"] == "tag-1"

    @pytest.mark.asyncio
    async def test_en_tag_case_insensitive(self) -> None:
        """'Video-Content' (с большой буквы) → тоже матчится."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["Video-Content"]

        tags_cache = {"видео-контент": "tag-1"}
        mock_db = MagicMock()

        await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        assert mock_db.table.return_value.insert.call_count == 1

    @pytest.mark.asyncio
    async def test_unknown_en_tag_not_translated(self) -> None:
        """Неизвестный EN тег → warning, не матчится."""
        from unittest.mock import patch

        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["unknown-english-tag"]

        tags_cache = {"видео-контент": "tag-1"}
        mock_db = MagicMock()

        with patch("src.ai.batch.logger") as mock_logger:
            await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        mock_logger.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_ru_tag_not_affected(self) -> None:
        """Русский тег не проходит через словарь EN→RU."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["юмор"]

        tags_cache = {"юмор": "tag-1"}
        mock_db = MagicMock()

        await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        assert mock_db.table.return_value.insert.call_count == 1
        rows = mock_db.table.return_value.insert.call_args[0][0]
        assert rows[0]["tag_id"] == "tag-1"

    @pytest.mark.asyncio
    async def test_alias_video_with_latin_v_maps_to_ru(self) -> None:
        """'video-контент' (латинская v) матчится через alias."""
        from src.ai.batch import match_tags

        insights = AIInsights()
        insights.tags = ["video-контент"]

        tags_cache = {"видео-контент": "tag-1"}
        mock_db = MagicMock()

        await match_tags(mock_db, "blog-1", insights, tags=tags_cache)

        assert mock_db.table.return_value.insert.call_count == 1


class TestIsValidCity:
    """Тесты валидации строки города."""

    def test_valid_city(self) -> None:
        from src.ai.batch import is_valid_city
        assert is_valid_city("Алматы") is True

    def test_garbage_with_percent(self) -> None:
        from src.ai.batch import is_valid_city
        assert is_valid_city("14% Казахстан") is False

    def test_country_name(self) -> None:
        from src.ai.batch import is_valid_city
        assert is_valid_city("Казахстан") is False

    def test_empty(self) -> None:
        from src.ai.batch import is_valid_city
        assert is_valid_city("") is False

    def test_digit_in_city(self) -> None:
        from src.ai.batch import is_valid_city
        assert is_valid_city("город 123") is False

    def test_short_string(self) -> None:
        from src.ai.batch import is_valid_city
        assert is_valid_city("A") is False

    def test_country_case_insensitive(self) -> None:
        from src.ai.batch import is_valid_city
        assert is_valid_city("РОССИЯ") is False

    def test_country_with_spaces(self) -> None:
        from src.ai.batch import is_valid_city
        assert is_valid_city("  Казахстан  ") is False

    def test_valid_russian_city(self) -> None:
        from src.ai.batch import is_valid_city
        assert is_valid_city("Астана") is True

    def test_percent_word(self) -> None:
        from src.ai.batch import is_valid_city
        assert is_valid_city("50 процент") is False


class TestCityAliases:
    """Тесты алиасов городов."""

    @pytest.mark.asyncio
    async def test_alias_match(self) -> None:
        """'Алмата' → матчится как 'Алматы'."""
        from src.ai.batch import match_city

        cities_cache = {"алматы": "city-1"}
        mock_db = MagicMock()

        result = await match_city(mock_db, "blog-1", "Алмата", cities_cache)

        assert result is True

    @pytest.mark.asyncio
    async def test_normalized_alias(self) -> None:
        """'алма-ата' → матчится через алиас."""
        from src.ai.batch import match_city

        cities_cache = {"алматы": "city-1"}
        mock_db = MagicMock()

        result = await match_city(mock_db, "blog-1", "Алма-Ата", cities_cache)

        assert result is True

    @pytest.mark.asyncio
    async def test_no_alias_direct(self) -> None:
        """'Алматы' → работает напрямую без алиаса."""
        from src.ai.batch import match_city

        cities_cache = {"алматы": "city-1"}
        mock_db = MagicMock()

        result = await match_city(mock_db, "blog-1", "Алматы", cities_cache)

        assert result is True


class TestMatchCityWithAliases:
    """Тесты match_city с алиасами."""

    @pytest.mark.asyncio
    async def test_match_with_alias_upserts(self) -> None:
        """'Алмата' → blog_cities upsert вызван."""
        from src.ai.batch import match_city

        cities_cache = {"алматы": "city-almaty"}
        mock_db = MagicMock()

        result = await match_city(mock_db, "blog-1", "Алмата", cities_cache)

        assert result is True
        # Проверяем upsert в blog_cities
        mock_db.table.assert_called_with("blog_cities")
        upsert_call = mock_db.table.return_value.upsert
        upsert_call.assert_called_once()
        upsert_data = upsert_call.call_args[0][0]
        assert upsert_data["blog_id"] == "blog-1"
        assert upsert_data["city_id"] == "city-almaty"

    @pytest.mark.asyncio
    async def test_no_match_returns_false(self) -> None:
        """Город без алиаса и не в cache → False."""
        from src.ai.batch import match_city

        cities_cache = {"алматы": "city-1"}
        mock_db = MagicMock()

        result = await match_city(mock_db, "blog-1", "Москва", cities_cache)

        assert result is False
        mock_db.table.assert_not_called()

    @pytest.mark.asyncio
    async def test_aktobe_alias(self) -> None:
        """'Актюбинск' → 'Актобе'."""
        from src.ai.batch import match_city

        cities_cache = {"актобе": "city-aktobe"}
        mock_db = MagicMock()

        result = await match_city(mock_db, "blog-1", "Актюбинск", cities_cache)

        assert result is True


class TestNormalizeBrand:
    """Тесты нормализации названий брендов."""

    def test_typographic_apostrophe(self) -> None:
        """L\u2019Oreal → L'Oreal."""
        from src.ai.batch import normalize_brand

        assert normalize_brand("L\u2019Oreal") == "L'Oreal"

    def test_left_single_quote(self) -> None:
        """L\u2018Oreal → L'Oreal."""
        from src.ai.batch import normalize_brand

        assert normalize_brand("L\u2018Oreal") == "L'Oreal"

    def test_strip_whitespace(self) -> None:
        """ Zara  → Zara."""
        from src.ai.batch import normalize_brand

        assert normalize_brand("  Zara  ") == "Zara"

    def test_dedup_brands(self) -> None:
        """Два варианта L'Oreal → один после нормализации."""
        from src.ai.batch import normalize_brand

        brands = ["L\u2019Oreal", "L'Oreal", "l'oreal"]
        seen: set[str] = set()
        unique: list[str] = []
        for b in brands:
            key = normalize_brand(b).lower()
            if key not in seen:
                seen.add(key)
                unique.append(normalize_brand(b))

        assert len(unique) == 1
        assert unique[0] == "L'Oreal"
