"""与提供方无关、只读的全球风险市场数据契约。

本模块契约刻意与可执行市场数据分离。它们描述时点研究观测与调用方持有的 HTTP 缓存
令牌；绝不授权订单，也不持久化远程内容。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol, runtime_checkable


class GlobalRiskDataError(RuntimeError):
    """官方全球风险数据适配器抛出的基础失败。"""


class GlobalRiskTimeoutError(TimeoutError, GlobalRiskDataError):
    """官方端点未在已配置期限内响应。"""


class GlobalRiskTransportError(ConnectionError, GlobalRiskDataError):
    """无法访问官方端点。"""


class GlobalRiskHTTPStatusError(GlobalRiskDataError):
    """官方端点返回了不可接受的 HTTP 状态。"""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"official global-risk source returned HTTP {status_code}")


class GlobalRiskSchemaError(GlobalRiskDataError):
    """无法无歧义地解析官方载荷。"""


class GlobalRiskNoDataError(GlobalRiskDataError):
    """请求时点没有可见的已完成观测。"""


class GlobalRiskCacheError(GlobalRiskDataError):
    """无法从调用方持有的缓存解析条件响应。"""


@dataclass(frozen=True, slots=True)
class GlobalRiskCacheEntry:
    """不透明响应字节及调度器可持久化的验证信息。

    适配器只消费并返回此值，自身不写入缓存，从而保持市场数据边界的确定性与只读性。
    """

    source_url: str
    body: bytes = field(repr=False)
    fetched_at: datetime
    content_sha256: str
    etag: str | None = None
    last_modified: str | None = None

    def __post_init__(self) -> None:
        if not self.source_url.strip():
            raise ValueError("cache source_url cannot be blank")
        if not self.body:
            raise ValueError("cache body cannot be empty")
        _require_aware(self.fetched_at, "cache fetched_at")
        expected_digest = hashlib.sha256(self.body).hexdigest()
        if self.content_sha256 != expected_digest:
            raise ValueError("cache content_sha256 does not match body")
        for field_name in ("etag", "last_modified"):
            value = getattr(self, field_name)
            if value is not None and not value.strip():
                raise ValueError(f"cache {field_name} cannot be blank")


@dataclass(frozen=True, slots=True)
class GlobalRiskSourceMeta:
    """来源、可见性、新鲜度与 HTTP 缓存元数据。"""

    source_id: str
    source_url: str
    available_at: datetime
    fetched_at: datetime
    stale: bool
    content_sha256: str
    etag: str | None = None
    last_modified: str | None = None
    cache_revalidated: bool = False
    warnings: tuple[str, ...] = ()
    skipped_invalid_ohlc_rows: int = 0

    def __post_init__(self) -> None:
        if not self.source_id.strip() or not self.source_url.strip():
            raise ValueError("global-risk source identifiers cannot be blank")
        _require_aware(self.available_at, "available_at")
        _require_aware(self.fetched_at, "fetched_at")
        if self.available_at > self.fetched_at:
            raise ValueError("available_at cannot follow fetched_at")
        if len(self.content_sha256) != 64:
            raise ValueError("content_sha256 must be a SHA-256 hex digest")
        try:
            int(self.content_sha256, 16)
        except ValueError as exc:
            raise ValueError("content_sha256 must be a SHA-256 hex digest") from exc
        if self.skipped_invalid_ohlc_rows < 0:
            raise ValueError("skipped_invalid_ohlc_rows cannot be negative")
        if any(not warning.strip() for warning in self.warnings):
            raise ValueError("global-risk warnings cannot be blank")


@dataclass(frozen=True, slots=True)
class VIXDailyBar:
    """一条 Cboe 官方 VIX 日频 OHLC 记录。"""

    session_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    observed_at: datetime
    available_at: datetime

    def __post_init__(self) -> None:
        _require_aware(self.observed_at, "VIX observed_at")
        _require_aware(self.available_at, "VIX available_at")
        if self.observed_at > self.available_at:
            raise ValueError("VIX observed_at cannot follow available_at")
        if self.session_date != self.observed_at.date():
            raise ValueError("VIX session_date must match observed_at local date")
        values = (self.open, self.high, self.low, self.close)
        if any(not value.is_finite() or value <= 0 for value in values):
            raise ValueError("VIX OHLC values must be positive and finite")
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("VIX high is inconsistent with OHLC")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("VIX low is inconsistent with OHLC")


@dataclass(frozen=True, slots=True)
class VIXDailyHistory:
    """在一个时点可见的严格有序 VIX K 线。"""

    as_of: datetime
    bars: tuple[VIXDailyBar, ...]
    meta: GlobalRiskSourceMeta
    cache_entry: GlobalRiskCacheEntry = field(repr=False)

    def __post_init__(self) -> None:
        _require_aware(self.as_of, "as_of")
        if not self.bars:
            raise ValueError("VIX daily history cannot be empty")
        session_dates = tuple(bar.session_date for bar in self.bars)
        if session_dates != tuple(sorted(session_dates)):
            raise ValueError("VIX session dates must be strictly ordered")
        if len(session_dates) != len(set(session_dates)):
            raise ValueError("VIX session dates must be unique")
        if any(bar.available_at > self.as_of for bar in self.bars):
            raise ValueError("VIX history cannot include a session unavailable at as_of")
        if self.meta.available_at != self.bars[-1].available_at:
            raise ValueError("VIX metadata must describe the latest visible bar")
        if self.meta.source_url != self.cache_entry.source_url:
            raise ValueError("VIX metadata and cache source URLs must match")
        if self.meta.content_sha256 != self.cache_entry.content_sha256:
            raise ValueError("VIX metadata and cache digests must match")
        if self.meta.fetched_at != self.cache_entry.fetched_at:
            raise ValueError("VIX metadata and cache fetched_at values must match")


@runtime_checkable
class AsyncVIXDailyData(Protocol):
    async def fetch_vix_daily_history(
        self,
        *,
        as_of: datetime,
        cache: GlobalRiskCacheEntry | None = None,
    ) -> VIXDailyHistory: ...


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
