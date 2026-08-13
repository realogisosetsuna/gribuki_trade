"""Small asynchronous HTTP boundary used by the Binance adapter.

The production implementation deliberately uses only the standard library.
Tests can inject any object implementing :class:`AsyncHttpTransport`, which
keeps order and signing tests completely offline.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class HttpTransportError(ConnectionError):
    """A transport failed without exposing a potentially signed URL."""


@dataclass(frozen=True, slots=True)
class HttpRequest:
    """A transport-neutral HTTP request with a deliberately redacted repr."""

    method: str
    url: str = field(repr=False)
    headers: dict[str, str] = field(default_factory=dict, repr=False)
    body: bytes | None = field(default=None, repr=False)
    timeout_seconds: float = 10.0

    def __repr__(self) -> str:
        # Query strings may contain a signature. Header values may contain an
        # API key. Neither belongs in tracebacks, logs, or assertion output.
        safe_url = self.url.partition("?")[0]
        header_names = sorted(self.headers)
        body_description = "none" if self.body is None else f"{len(self.body)} bytes"
        return (
            f"HttpRequest(method={self.method!r}, url={safe_url!r}, "
            f"header_names={header_names!r}, body={body_description!r}, "
            f"timeout_seconds={self.timeout_seconds!r})"
        )


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """A minimal HTTP response whose body is never included in its repr."""

    status_code: int
    body: bytes = field(repr=False)
    headers: dict[str, str] = field(default_factory=dict, repr=False)

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8"))

    def __repr__(self) -> str:
        return f"HttpResponse(status_code={self.status_code!r}, body=<{len(self.body)} bytes>)"


class AsyncHttpTransport(Protocol):
    """Injectable async transport contract used by :class:`BinanceSpotGateway`."""

    async def request(self, request: HttpRequest) -> HttpResponse: ...


class UrllibAsyncHttpTransport:
    """Standard-library transport that moves blocking I/O to a worker thread."""

    async def request(self, request: HttpRequest) -> HttpResponse:
        return await asyncio.to_thread(self._request_sync, request)

    @staticmethod
    def _request_sync(request: HttpRequest) -> HttpResponse:
        raw_request = Request(
            url=request.url,
            data=request.body,
            headers=request.headers,
            method=request.method,
        )
        try:
            with urlopen(raw_request, timeout=request.timeout_seconds) as response:  # noqa: S310
                return HttpResponse(
                    status_code=response.status,
                    body=response.read(),
                    headers=dict(response.headers.items()),
                )
        except HTTPError as exc:
            # HTTP errors still carry the Binance JSON response body and must
            # be interpreted by the gateway (especially -1007).
            return HttpResponse(
                status_code=exc.code,
                body=exc.read(),
                headers=dict(exc.headers.items()) if exc.headers is not None else {},
            )
        except (TimeoutError, URLError, OSError):
            # urllib exceptions often embed the full URL. Do not chain them.
            raise HttpTransportError("HTTP request failed") from None
