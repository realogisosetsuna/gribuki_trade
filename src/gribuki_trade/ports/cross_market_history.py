"""独立来源跨市场历史数据的时点契约。

这些观测刻意只包含价格与来源信息。它们可支持滚动相关性或相对收益特征，但本身不能
证明一个市场导致另一个市场发生波动。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, runtime_checkable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from gribuki_trade.ports.market_data import (
    MarketDataTimeoutError,
    MarketDataUnavailableError,
)

MINIMUM_CROSS_MARKET_HISTORY = 130


class CrossMarketHistoryDataError(MarketDataUnavailableError):
    """收集单个历史市场序列时抛出的基础失败。"""


class CrossMarketHistoryTimeoutError(MarketDataTimeoutError, CrossMarketHistoryDataError):
    """一个独立设限的上游调用超过期限。"""


class CrossMarketHistoryEndpointError(CrossMarketHistoryDataError):
    """已安装的提供方未暴露经审计的精确端点。"""


class CrossMarketHistoryPayloadError(CrossMarketHistoryDataError):
    """某个历史数据端点返回了不兼容的载荷。"""


class CrossMarketHistoryFailureCode(StrEnum):
    """历史序列显式缺失时使用的稳定原因码。"""

    ENDPOINT_UNAVAILABLE = "ENDPOINT_UNAVAILABLE"
    TIMEOUT = "TIMEOUT"
    UPSTREAM_ERROR = "UPSTREAM_ERROR"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"


@dataclass(frozen=True, slots=True)
class CrossMarketHistoryObservation:
    """在已配置最早可消费时点上的一个日收盘观测。"""

    market_id: str
    session_date: date
    close: Decimal
    available_at: datetime
    source: str
    local_timezone: str

    def __post_init__(self) -> None:
        if not self.market_id.strip():
            raise ValueError("market_id cannot be blank")
        if self.close <= 0 or not self.close.is_finite():
            raise ValueError("close must be positive and finite")
        if not self.source.strip():
            raise ValueError("source cannot be blank")
        if self.available_at.tzinfo is None or self.available_at.utcoffset() is None:
            raise ValueError("available_at must be timezone-aware")
        try:
            local_zone = ZoneInfo(self.local_timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown local_timezone: {self.local_timezone}") from exc
        if self.available_at.astimezone(local_zone).date() != self.session_date:
            raise ValueError("available_at local date must equal session_date")


@dataclass(frozen=True, slots=True)
class CrossMarketHistorySeries:
    """严格有序、来源单一的历史市场序列。"""

    market_id: str
    display_name: str
    observations: tuple[CrossMarketHistoryObservation, ...]

    def __post_init__(self) -> None:
        if not self.market_id.strip():
            raise ValueError("market_id cannot be blank")
        if not self.display_name.strip():
            raise ValueError("display_name cannot be blank")
        if not self.observations:
            raise ValueError("observations cannot be empty")
        if any(item.market_id != self.market_id for item in self.observations):
            raise ValueError("every observation must belong to the series market_id")
        dates = tuple(item.session_date for item in self.observations)
        if dates != tuple(sorted(dates)):
            raise ValueError("observations must be ordered by session_date")
        if len(dates) != len(set(dates)):
            raise ValueError("observations cannot contain duplicate session dates")
        if len({item.source for item in self.observations}) != 1:
            raise ValueError("one series cannot mix independent sources")

    @property
    def source(self) -> str:
        return self.observations[0].source


@dataclass(frozen=True, slots=True)
class CrossMarketHistoryMissingSeries:
    """无法安全提供的请求精确序列。"""

    market_id: str
    display_name: str
    expected_source: str
    failure_code: CrossMarketHistoryFailureCode
    reason: str

    def __post_init__(self) -> None:
        if not self.market_id.strip():
            raise ValueError("market_id cannot be blank")
        if not self.display_name.strip():
            raise ValueError("display_name cannot be blank")
        if not self.expected_source.strip():
            raise ValueError("expected_source cannot be blank")
        if not self.reason.strip():
            raise ValueError("reason cannot be blank")


@dataclass(frozen=True, slots=True)
class CrossMarketHistorySnapshot:
    """经过 PIT 过滤且显式保留覆盖失败的历史数据。"""

    as_of: datetime
    fetched_at: datetime
    minimum_observations: int
    series: tuple[CrossMarketHistorySeries, ...]
    missing: tuple[CrossMarketHistoryMissingSeries, ...]
    degraded: bool
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_aware(self.as_of, "as_of")
        _require_aware(self.fetched_at, "fetched_at")
        if self.minimum_observations < MINIMUM_CROSS_MARKET_HISTORY:
            raise ValueError(
                "minimum_observations must be at least "
                f"{MINIMUM_CROSS_MARKET_HISTORY}"
            )
        series_ids = [item.market_id for item in self.series]
        missing_ids = [item.market_id for item in self.missing]
        if len(series_ids) != len(set(series_ids)):
            raise ValueError("snapshot contains duplicate series market_id values")
        if len(missing_ids) != len(set(missing_ids)):
            raise ValueError("snapshot contains duplicate missing market_id values")
        overlap = set(series_ids).intersection(missing_ids)
        if overlap:
            raise ValueError(f"market IDs cannot be both present and missing: {sorted(overlap)}")
        for item in self.series:
            if len(item.observations) < self.minimum_observations:
                raise ValueError("present series cannot have insufficient observations")
            if any(observation.available_at > self.as_of for observation in item.observations):
                raise ValueError("snapshot cannot contain observations unavailable at as_of")
        if self.degraded != bool(self.missing):
            raise ValueError("degraded must equal whether requested series are missing")


@runtime_checkable
class CrossMarketHistoryData(Protocol):
    def fetch_cross_market_history(
        self,
        *,
        as_of: datetime,
        minimum_observations: int = MINIMUM_CROSS_MARKET_HISTORY,
    ) -> CrossMarketHistorySnapshot: ...


@runtime_checkable
class AsyncCrossMarketHistoryData(Protocol):
    async def fetch_cross_market_history_async(
        self,
        *,
        as_of: datetime,
        minimum_observations: int = MINIMUM_CROSS_MARKET_HISTORY,
    ) -> CrossMarketHistorySnapshot: ...


def _require_aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
