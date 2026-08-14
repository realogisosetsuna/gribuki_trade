"""Binance 适配器使用的小型异步 HTTP 边界。

生产实现特意只使用标准库。测试可以注入任何实现
:class:`AsyncHttpTransport` 的对象，使订单与签名测试能够完全离线运行。
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class HttpTransportError(ConnectionError):
    """传输失败，且不会暴露可能带签名的 URL。"""


@dataclass(frozen=True, slots=True)
class HttpRequest:
    """与传输实现无关、且特意对 repr 脱敏的 HTTP 请求。"""

    method: str
    url: str = field(repr=False)
    headers: dict[str, str] = field(default_factory=dict, repr=False)
    body: bytes | None = field(default=None, repr=False)
    timeout_seconds: float = 10.0

    def __repr__(self) -> str:
        # 查询字符串可能含签名，请求头值可能含 API 密钥；二者都不得出现在
        # 回溯、日志或断言输出中。
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
    """响应体永不出现在 repr 中的最小 HTTP 响应。"""

    status_code: int
    body: bytes = field(repr=False)
    headers: dict[str, str] = field(default_factory=dict, repr=False)

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8"))

    def __repr__(self) -> str:
        return f"HttpResponse(status_code={self.status_code!r}, body=<{len(self.body)} bytes>)"


class AsyncHttpTransport(Protocol):
    """:class:`BinanceSpotGateway` 使用的可注入异步传输契约。"""

    async def request(self, request: HttpRequest) -> HttpResponse: ...


class UrllibAsyncHttpTransport:
    """把阻塞 I/O 移到工作线程的标准库传输实现。"""

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
            # HTTP 错误仍携带 Binance JSON 响应体，必须由网关解释，尤其是 -1007。
            return HttpResponse(
                status_code=exc.code,
                body=exc.read(),
                headers=dict(exc.headers.items()) if exc.headers is not None else {},
            )
        except (TimeoutError, URLError, OSError):
            # urllib 异常往往嵌入完整 URL，因此不要保留异常链。
            raise HttpTransportError("HTTP request failed") from None
