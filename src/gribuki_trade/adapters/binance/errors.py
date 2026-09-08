"""Binance 适配器的错误类型。

错误对象只携带安全的状态、错误码和已净化消息，不依赖具体 REST 网关。这样
Spot、合约和用户数据流可以共享同一错误层级，而不会互相导入网关实现。
"""

from __future__ import annotations


class BinanceError(RuntimeError):
    """可安全记录日志的 Binance 适配器失败基类。"""


class BinanceConfigurationError(BinanceError):
    """所选环境或签名端点未得到安全配置。"""


class BinanceProtocolError(BinanceError):
    """Binance 或传输层返回了格式错误的响应。"""


class BinanceTransportError(BinanceError):
    """HTTP 请求失败；不保留请求 URL 或凭据。"""


class BinanceAPIError(BinanceError):
    """消息已净化的已知 Binance API 拒绝。"""

    def __init__(self, *, status_code: int, code: int | None, message: str) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        code_text = "unknown" if code is None else str(code)
        super().__init__(f"Binance API error (HTTP {status_code}, code {code_text}): {message}")


class BinanceUncertainResultError(BinanceAPIError):
    """交易所可能已经执行请求，必须进行对账。"""


__all__ = [
    "BinanceAPIError",
    "BinanceConfigurationError",
    "BinanceError",
    "BinanceProtocolError",
    "BinanceTransportError",
    "BinanceUncertainResultError",
]
