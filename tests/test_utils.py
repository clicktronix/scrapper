"""Тесты общих утилит скрапера."""
from src.utils import is_safe_url


class TestIsSafeUrl:
    """Тесты is_safe_url — SSRF-защита URL."""

    def test_https_url_safe(self) -> None:
        assert is_safe_url("https://example.com/image.jpg")

    def test_http_url_safe(self) -> None:
        assert is_safe_url("http://cdn.instagram.com/photo.jpg")

    def test_private_ip_blocked(self) -> None:
        assert not is_safe_url("http://192.168.1.1/secret")

    def test_loopback_blocked(self) -> None:
        assert not is_safe_url("http://127.0.0.1/admin")

    def test_link_local_blocked(self) -> None:
        assert not is_safe_url("http://169.254.169.254/metadata")

    def test_ftp_scheme_blocked(self) -> None:
        assert not is_safe_url("ftp://files.example.com/data")

    def test_empty_string_blocked(self) -> None:
        assert not is_safe_url("")

    def test_no_hostname_blocked(self) -> None:
        assert not is_safe_url("https://")

    def test_hostname_not_ip_allowed(self) -> None:
        assert is_safe_url("https://scontent-arn2-1.cdninstagram.com/v/photo.jpg")


class TestIsTransientNetworkError:
    """Тесты is_transient_network_error — определение транзиентных сетевых ошибок."""

    def test_linux_eagain_errno_11(self) -> None:
        """Linux errno=11 (EAGAIN) — транзиентная ошибка."""
        from src.utils import is_transient_network_error

        assert is_transient_network_error(OSError(11, "Resource temporarily unavailable"))

    def test_macos_eagain_errno_35(self) -> None:
        """macOS errno=35 (EAGAIN/ENOBUFS) — транзиентная ошибка."""
        from src.utils import is_transient_network_error

        assert is_transient_network_error(OSError(35, "Resource temporarily unavailable"))

    def test_connection_reset_errno_54(self) -> None:
        """Connection reset errno=54 — транзиентная ошибка."""
        from src.utils import is_transient_network_error

        assert is_transient_network_error(OSError(54, "Connection reset by peer"))

    def test_broken_pipe_errno_32(self) -> None:
        """Broken pipe errno=32 — транзиентная ошибка."""
        from src.utils import is_transient_network_error

        assert is_transient_network_error(OSError(32, "Broken pipe"))

    def test_chained_cause_oserror(self) -> None:
        """OSError в цепочке __cause__ — должен быть распознан."""
        from src.utils import is_transient_network_error

        inner = OSError(11, "Resource temporarily unavailable")
        outer = RuntimeError("httpx.ReadError")
        outer.__cause__ = inner

        assert is_transient_network_error(outer)

    def test_chained_context_oserror(self) -> None:
        """OSError в цепочке __context__ — должен быть распознан."""
        from src.utils import is_transient_network_error

        inner = OSError(35, "Resource temporarily unavailable")
        outer = RuntimeError("Something went wrong")
        outer.__context__ = inner

        assert is_transient_network_error(outer)

    def test_deeply_chained_oserror(self) -> None:
        """OSError глубоко в цепочке причин — должен быть распознан."""
        from src.utils import is_transient_network_error

        root = OSError(11, "EAGAIN")
        mid = ValueError("mid-level")
        mid.__cause__ = root
        top = RuntimeError("top-level")
        top.__cause__ = mid

        assert is_transient_network_error(top)

    def test_fallback_string_errno_11(self) -> None:
        """Строковый fallback: 'Errno 11' в сообщении — транзиентная ошибка."""
        from src.utils import is_transient_network_error

        assert is_transient_network_error(Exception("Errno 11 in connection"))

    def test_fallback_string_errno_35(self) -> None:
        """Строковый fallback: 'Errno 35' в сообщении — транзиентная ошибка."""
        from src.utils import is_transient_network_error

        assert is_transient_network_error(Exception("Errno 35: Resource temporarily unavailable"))

    def test_fallback_string_resource_unavailable(self) -> None:
        """Строковый fallback: 'Resource temporarily unavailable' — транзиентная ошибка."""
        from src.utils import is_transient_network_error

        assert is_transient_network_error(Exception("Resource temporarily unavailable"))

    def test_fallback_string_connection_reset(self) -> None:
        """Строковый fallback: 'Connection reset' — транзиентная ошибка."""
        from src.utils import is_transient_network_error

        assert is_transient_network_error(Exception("Connection reset by peer"))

    def test_fallback_string_broken_pipe(self) -> None:
        """Строковый fallback: 'Broken pipe' — транзиентная ошибка."""
        from src.utils import is_transient_network_error

        assert is_transient_network_error(Exception("Broken pipe"))

    def test_non_transient_oserror(self) -> None:
        """OSError с другим errno — НЕ транзиентная."""
        from src.utils import is_transient_network_error

        assert not is_transient_network_error(OSError(2, "No such file or directory"))

    def test_non_transient_generic_exception(self) -> None:
        """Обычная Exception — НЕ транзиентная."""
        from src.utils import is_transient_network_error

        assert not is_transient_network_error(ValueError("invalid data"))

    def test_non_transient_runtime_error(self) -> None:
        """RuntimeError без транзиентных маркеров — НЕ транзиентная."""
        from src.utils import is_transient_network_error

        assert not is_transient_network_error(RuntimeError("unexpected error"))

    def test_connection_error_without_errno(self) -> None:
        """ConnectionError без errno и без маркеров — НЕ транзиентная."""
        from src.utils import is_transient_network_error

        assert not is_transient_network_error(ConnectionError("some connection issue"))

    def test_oserror_without_errno(self) -> None:
        """OSError без errno (None) и без маркеров — НЕ транзиентная."""
        from src.utils import is_transient_network_error

        err = OSError("generic OS error")
        assert not is_transient_network_error(err)

    def test_cause_takes_priority_over_context(self) -> None:
        """__cause__ проверяется раньше __context__."""
        from src.utils import is_transient_network_error

        # __cause__ — транзиентная, __context__ — нет
        cause = OSError(11, "EAGAIN")
        context = ValueError("not transient")
        outer = RuntimeError("wrapped")
        outer.__cause__ = cause
        outer.__context__ = context

        assert is_transient_network_error(outer)
