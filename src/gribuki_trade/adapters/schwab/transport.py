"""HTTP boundary and production ``httpx`` transport for the Schwab adapter."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Protocol

import httpx

from .errors import SchwabTransportError


@dataclass(frozen=True, slots=True)
class HttpRequest:
    """A transport-neutral HTTP request.

    Headers and bodies are excluded from ``repr`` because both can contain
    bearer tokens, OAuth codes, account data, or order details.
    """

    method: str
    url: str = field(repr=False)
    headers: dict[str, str] = field(default_factory=dict, repr=False)
    body: bytes | None = field(default=None, repr=False)
    timeout: float | None = None

    def __repr__(self) -> str:
        body_description = "none" if self.body is None else f"{len(self.body)} bytes"
        return (
            f"HttpRequest(method={self.method!r}, url=<redacted>, "
            f"header_names={sorted(self.headers)!r}, body={body_description!r}, "
            f"timeout={self.timeout!r})"
        )


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """The minimum response surface required from an injected transport."""

    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = field(default=b"", repr=False)

    def header(self, name: str) -> str | None:
        wanted = name.casefold()
        return next(
            (value for key, value in self.headers.items() if key.casefold() == wanted),
            None,
        )

    def json(self) -> object:
        if not self.body:
            return None
        return json.loads(self.body.decode("utf-8"))


class AsyncHttpTransport(Protocol):
    """Injectable asynchronous HTTP transport."""

    async def send(self, request: HttpRequest) -> HttpResponse: ...


class HttpxAsyncHttpTransport:
    """Reusable production transport with sanitized network failures.

    An ``httpx.AsyncClient`` may be injected for deterministic tests or shared
    connection pooling. Redirects remain disabled because silently following
    an OAuth or order redirect can cross an authentication boundary.
    """

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._client = client or httpx.AsyncClient(follow_redirects=False)
        self._owns_client = client is None
        self._closed = False

    async def send(self, request: HttpRequest) -> HttpResponse:
        if self._closed:
            raise SchwabTransportError("Schwab HTTP transport is closed")
        try:
            response = await self._client.request(
                request.method,
                request.url,
                headers=request.headers,
                content=request.body,
                timeout=request.timeout,
                follow_redirects=False,
            )
        except (httpx.HTTPError, OSError):
            # httpx errors may include a URL or request object. Never retain
            # them as exception context around an authenticated request.
            raise SchwabTransportError("Schwab HTTP request failed") from None
        return HttpResponse(
            status_code=response.status_code,
            headers=dict(response.headers.items()),
            body=response.content,
        )

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> HttpxAsyncHttpTransport:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
