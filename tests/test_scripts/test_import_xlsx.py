"""Тесты скрипта импорта xlsx."""
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pandas as pd
import pytest

from src.scripts.import_xlsx import _resolve_api_key, extract_usernames, run, send_batch


class TestExtractUsernames:
    """Извлечение и дедупликация username-ов из DataFrame."""

    def test_extracts_unique_usernames(self) -> None:
        df = pd.DataFrame({"username": ["user1", "user2", "user1", "user3"]})
        result = extract_usernames(df)
        assert result == ["user1", "user2", "user3"]

    def test_strips_whitespace(self) -> None:
        df = pd.DataFrame({"username": [" user1 ", "user2"]})
        result = extract_usernames(df)
        assert result == ["user1", "user2"]

    def test_lowercases(self) -> None:
        df = pd.DataFrame({"username": ["User1", "USER2"]})
        result = extract_usernames(df)
        assert result == ["user1", "user2"]

    def test_skips_empty_and_none(self) -> None:
        df = pd.DataFrame({"username": ["user1", "", None, "user2"]})
        result = extract_usernames(df)
        assert result == ["user1", "user2"]


class TestResolveApiKey:
    def test_prefers_cli_value(self) -> None:
        assert _resolve_api_key("cli-token") == "cli-token"

    def test_uses_env_when_cli_missing(self) -> None:
        with patch("src.scripts.import_xlsx.os.getenv", return_value="env-token"):
            assert _resolve_api_key(None) == "env-token"

    def test_raises_when_missing(self) -> None:
        with (
            patch("src.scripts.import_xlsx.os.getenv", return_value=""),
            pytest.raises(ValueError, match="SCRAPER_API_KEY is required"),
        ):
            _resolve_api_key(None)


class TestSendBatch:
    @pytest.mark.asyncio
    async def test_sends_auth_header_and_payload(self) -> None:
        client = AsyncMock()
        response = MagicMock()
        response.json.return_value = {"created": 1}
        response.raise_for_status.return_value = None
        client.post.return_value = response

        result = await send_batch(
            client=client,
            base_url="http://localhost:8001",
            api_key="secret",
            usernames=["user1", "user2"],
        )

        assert result == {"created": 1}
        client.post.assert_awaited_once()
        _, kwargs = client.post.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer secret"
        assert kwargs["json"] == {"usernames": ["user1", "user2"]}


class TestRunLogging:
    @pytest.mark.asyncio
    async def test_http_error_logs_sanitized_message(self) -> None:
        request = httpx.Request("POST", "http://localhost:8001/api/tasks/pre_filter")
        response = httpx.Response(
            status_code=401,
            request=request,
            text="Authorization: Bearer sk-secret-token",
        )
        status_error = httpx.HTTPStatusError("bad status", request=request, response=response)

        with (
            patch("src.scripts.import_xlsx.pd.read_excel", return_value=pd.DataFrame({"username": ["user1"]})),
            patch("src.scripts.import_xlsx.send_batch", new_callable=AsyncMock, side_effect=status_error),
            patch("src.scripts.import_xlsx.logger.error") as mock_error,
        ):
            await run("dummy.xlsx", "http://localhost:8001", "api-key", batch_size=100, delay=0)

        logged_message = mock_error.call_args[0][0]
        assert "sk-secret-token" not in logged_message
        assert "Bearer ***" in logged_message
