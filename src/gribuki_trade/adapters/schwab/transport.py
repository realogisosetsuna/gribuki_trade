"""Schwab 适配器的 HTTP 边界与生产级 ``httpx`` 传输实现。"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Protocol

import httpx

from .errors import SchwabTransportError


@dataclass(frozen=True, slots=True)
class HttpRequest:
    """与具体传输实现无关的 HTTP 请求。

    请求头和正文均不进入 ``repr``，因为其中可能含有不记名令牌、OAuth
    授权码、账户数据或订单详情。
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
    """注入式传输实现必须提供的最小响应接口。"""

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
    """可注入的异步 HTTP 传输协议。"""

    async def send(self, request: HttpRequest) -> HttpResponse: ...


class HttpxAsyncHttpTransport:
    """可复用且会净化网络异常的生产传输实现。

    可以注入 ``httpx.AsyncClient``，用于确定性测试或共享连接池。重定向保持
    禁用，因为静默跟随 OAuth 或订单重定向可能跨越身份验证边界。
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
            # httpx 异常可能包含 URL 或请求对象；处理已认证请求时，绝不能
            # 把它们保留为异常上下文。
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
