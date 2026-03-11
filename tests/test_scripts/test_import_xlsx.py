"""Тесты скрипта импорта xlsx."""
import pandas as pd

from src.scripts.import_xlsx import extract_usernames


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
