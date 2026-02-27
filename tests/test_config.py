"""Тесты конфигурации скрапера."""
import pytest

from src.config import _split_comma


class TestSplitComma:
    """Тесты парсера строки через запятую."""

    def test_normal_split(self) -> None:
        assert _split_comma("a,b,c") == ["a", "b", "c"]

    def test_strips_whitespace(self) -> None:
        assert _split_comma(" a , b , c ") == ["a", "b", "c"]

    def test_empty_string(self) -> None:
        assert _split_comma("") == []

    def test_whitespace_only(self) -> None:
        assert _split_comma("   ") == []

    def test_only_commas(self) -> None:
        """Строка из одних запятых → пустой список."""
        assert _split_comma(",,,") == []

    def test_empty_items(self) -> None:
        """Пустые элементы фильтруются."""
        assert _split_comma("a,,b") == ["a", "b"]

    def test_single_value(self) -> None:
        assert _split_comma("single") == ["single"]

    def test_cyrillic(self) -> None:
        assert _split_comma("мама,бизнес") == ["мама", "бизнес"]


class TestSettings:
    """Тесты парсинга Settings из env."""

    def test_minimal_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Минимальный набор обязательных переменных."""
        monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-key")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("SCRAPER_API_KEY", "test-key")

        from src.config import Settings

        s = Settings()
        assert s.supabase_url == "https://test.supabase.co"
        assert s.supabase_service_key.get_secret_value() == "test-key"
        assert s.openai_api_key.get_secret_value() == "sk-test"

    def test_instagram_accounts_parsing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """INSTAGRAM_ACCOUNTS='a,b,c' → ['a', 'b', 'c']."""
        monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-key")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("SCRAPER_API_KEY", "test-key")
        monkeypatch.setenv("INSTAGRAM_ACCOUNTS", "acc1,acc2,acc3")

        from src.config import Settings

        s = Settings()
        assert s.instagram_accounts_list == ["acc1", "acc2", "acc3"]

    def test_default_values(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Дефолтные значения для необязательных полей."""
        monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-key")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("SCRAPER_API_KEY", "test-key")
        # Очистить, чтобы .env файл не влиял
        monkeypatch.setenv("INSTAGRAM_ACCOUNTS", "")

        from src.config import Settings

        s = Settings(_env_file=None)
        assert s.scrape_delay_min == 1.5
        assert s.scrape_delay_max == 4.0
        assert s.requests_per_hour == 30
        assert s.cooldown_minutes == 45
        assert s.posts_to_fetch == 25
        assert s.highlights_to_fetch == 3
        assert s.rescrape_days == 60
        assert s.batch_min_size == 10
        assert s.batch_model == "gpt-5-mini"
        assert s.worker_poll_interval == 30
        assert s.worker_max_concurrent == 2
        assert s.log_level == "INFO"
        assert s.instagram_accounts_list == []

    def test_extra_env_ignored(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Неизвестные переменные не ломают Settings (extra='ignore')."""
        monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-key")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("SCRAPER_API_KEY", "test-key")
        monkeypatch.setenv("UNKNOWN_VAR", "unknown")

        from src.config import Settings

        s = Settings()
        assert not hasattr(s, "unknown_var")

    def test_secret_str_not_exposed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """SecretStr не раскрывается при str()/repr()."""
        monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "supersecret")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-secret")
        monkeypatch.setenv("SCRAPER_API_KEY", "test-key")

        from src.config import Settings

        s = Settings()
        # repr не должен содержать секрет
        assert "supersecret" not in repr(s.supabase_service_key)
        assert "sk-secret" not in repr(s.openai_api_key)
        # Но get_secret_value() работает
        assert s.supabase_service_key.get_secret_value() == "supersecret"

    def test_secret_str_not_in_model_repr(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """repr(Settings) не содержит секретных значений."""
        monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "my_super_secret_key")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-top-secret-key")
        monkeypatch.setenv("SCRAPER_API_KEY", "test-key")

        from src.config import Settings

        s = Settings()
        model_repr = repr(s)
        assert "my_super_secret_key" not in model_repr
        assert "sk-top-secret-key" not in model_repr

    def test_secret_str_not_in_model_dump_json(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """model_dump_json() маскирует секреты."""
        monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "leaked_key_123")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-leaked")
        monkeypatch.setenv("SCRAPER_API_KEY", "test-key")

        from src.config import Settings

        s = Settings()
        json_str = s.model_dump_json()
        assert "leaked_key_123" not in json_str
        assert "sk-leaked" not in json_str

    def test_secret_str_in_model_dump_is_object(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """model_dump() возвращает SecretStr объект, а не строку."""
        monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-key")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("SCRAPER_API_KEY", "test-key")

        from pydantic import SecretStr

        from src.config import Settings

        s = Settings()
        dumped = s.model_dump()
        assert isinstance(dumped["supabase_service_key"], SecretStr)
        assert isinstance(dumped["openai_api_key"], SecretStr)

    def test_str_of_secret_is_masked(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """str() на SecretStr возвращает маску."""
        monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "actual_secret")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-actual")
        monkeypatch.setenv("SCRAPER_API_KEY", "test-key")

        from src.config import Settings

        s = Settings()
        assert "actual_secret" not in str(s.supabase_service_key)
        assert "sk-actual" not in str(s.openai_api_key)
        assert "**" in str(s.supabase_service_key)


class TestSettingsApiFields:
    """Тесты новых полей для API."""

    def test_scraper_api_key_parsed(self, monkeypatch) -> None:
        from src.config import Settings

        monkeypatch.setenv("SUPABASE_URL", "https://x.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "key")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("SCRAPER_API_KEY", "sk-scraper-123")

        s = Settings()
        assert s.scraper_api_key.get_secret_value() == "sk-scraper-123"

    def test_scraper_port_default(self, monkeypatch) -> None:
        from src.config import Settings

        monkeypatch.setenv("SUPABASE_URL", "https://x.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "key")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("SCRAPER_API_KEY", "sk-scraper-123")

        s = Settings()
        assert s.scraper_port == 8001

    def test_scraper_port_custom(self, monkeypatch) -> None:
        from src.config import Settings

        monkeypatch.setenv("SUPABASE_URL", "https://x.supabase.co")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", "key")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("SCRAPER_API_KEY", "sk-scraper-123")
        monkeypatch.setenv("SCRAPER_PORT", "9000")

        s = Settings()
        assert s.scraper_port == 9000
