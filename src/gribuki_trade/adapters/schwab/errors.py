"""Schwab 适配器抛出的异常。

这些异常刻意不在消息中写入响应正文、凭据或令牌。调用方可以按具体类型
分支处理，而不会意外地把敏感经纪商数据复制到日志中。
"""

from __future__ import annotations

from datetime import datetime


class SchwabError(Exception):
    """所有适配器异常的基类。"""


class SchwabLiveAccessError(SchwabError, PermissionError):
    """未明确启用生产端点时抛出。"""


class SchwabOAuthError(SchwabError):
    """OAuth 配置、回调或令牌交换失败。"""


class SchwabTransportError(SchwabError, ConnectionError):
    """HTTP 请求进行期间发生结果不明确的失败。"""


class SchwabHttpError(SchwabError):
    """HTTP 响应不是成功状态。"""

    def __init__(self, status_code: int, method: str, url: str) -> None:
        self.status_code = status_code
        self.method = method
        # 交易者 API 路径中会出现账户哈希和订单编号，因此异常状态及其消息
        # 都不得保留原始 URL。
        self.url = "<redacted>"
        super().__init__(f"Schwab API returned HTTP {status_code} for {method}")


class SchwabAuthenticationError(SchwabHttpError):
    """执行唯一一次允许的刷新后，身份验证仍然失败。"""


class SchwabPermissionError(SchwabHttpError, PermissionError):
    """令牌或关联账户无权执行该操作。"""


class SchwabRateLimitError(SchwabHttpError):
    """保留服务器 Retry-After 语义的 429 响应。"""

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
    """对写请求而言，执行结果可能不明确的 5xx 响应。"""


class SchwabProtocolError(SchwabError):
    """成功响应不具备文档规定的最小结构。"""


class SchwabOrderValidationError(SchwabError, ValueError):
    """无法把 OrderIntent 安全地表示为 Schwab 订单。"""
