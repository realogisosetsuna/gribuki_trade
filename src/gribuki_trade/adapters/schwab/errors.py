"""Errors raised by the Schwab adapter.

The exceptions intentionally avoid putting response bodies, credentials, or
tokens in their messages.  Callers can branch on the concrete type without
accidentally copying sensitive broker data into a log.
"""

from __future__ import annotations

from datetime import datetime


class SchwabError(Exception):
    """Base class for all adapter errors."""


class SchwabLiveAccessError(SchwabError, PermissionError):
    """Raised when production endpoints were not explicitly enabled."""


class SchwabOAuthError(SchwabError):
    """OAuth configuration, callback, or token exchange failure."""


class SchwabTransportError(SchwabError, ConnectionError):
    """An ambiguous failure while an HTTP request was in flight."""


class SchwabHttpError(SchwabError):
    """A non-success HTTP response."""

    def __init__(self, status_code: int, method: str, url: str) -> None:
        self.status_code = status_code
        self.method = method
        # Account hashes and order IDs occur in Trader API paths. Keep the
        # original URL out of exception state as well as its message.
        self.url = "<redacted>"
        super().__init__(f"Schwab API returned HTTP {status_code} for {method}")


class SchwabAuthenticationError(SchwabHttpError):
    """Authentication still failed after the one permitted refresh."""


class SchwabPermissionError(SchwabHttpError, PermissionError):
    """The token or linked account is not permitted to perform an operation."""


class SchwabRateLimitError(SchwabHttpError):
    """A 429 response with the server's Retry-After semantics preserved."""

    def __init__(
        self,
        method: str,
        url: str,
        *,
        retry_after: float | None,
        retry_at: datetime | None,
        raw_retry_after: str | None,
    ) -> None:
        self.retry_after = retry_after
        self.retry_at = retry_at
        self.raw_retry_after = raw_retry_after
        super().__init__(429, method, url)


class SchwabServerError(SchwabHttpError):
    """A 5xx response whose outcome may be ambiguous for a write request."""


class SchwabProtocolError(SchwabError):
    """A successful response did not have the documented minimum shape."""


class SchwabOrderValidationError(SchwabError, ValueError):
    """An OrderIntent cannot be represented safely as a Schwab order."""
