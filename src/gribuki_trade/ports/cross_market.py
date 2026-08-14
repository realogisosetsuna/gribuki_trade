"""与提供方无关的观察性跨市场快照契约。

本模块中的类型刻意只描述行情及其来源，而不描述因果关系。两个市场同步波动只能证明
共同运动；研究层必须提供独立来源的证据，才能提出因果主张。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from typing import Protocol, runtime_checkable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from gribuki_trade.ports.market_data import (
    MarketDataTimeoutError,
    MarketDataUnavailableError,
)


class CrossMarketDataError(MarketDataUnavailableError):
    """跨市场数据适配器抛出的基础失败。"""


class CrossMarketDataTimeoutError(MarketDataTimeoutError, CrossMarketDataError):
    """全表快照未在调用方期限内完成。"""


class CrossMarketPayloadError(CrossMarketDataError):
    """提供方返回了不兼容或内部无效的载荷。"""


class CrossMarketSegment(StrEnum):
    """用于报告编排的宽泛市场分组，绝不用于因果推断。"""

    A_SHARE = "A_SHARE"
    HONG_KONG = "HONG_KONG"
    ASIA_PACIFIC = "ASIA_PACIFIC"
    UNITED_STATES = "UNITED_STATES"
    DOLLAR_COMMODITY_RISK = "DOLLAR_COMMODITY_RISK"


@dataclass(frozen=True, slots=True)
class CrossMarketInstrumentSpec:
    """一个目标序列的精确提供方别名与本地时间语义。"""

    instrument_id: str
    display_name: str
    segment: CrossMarketSegment
    code_aliases: tuple[str, ...]
    name_aliases: tuple[str, ...]
    local_timezone: str
    stale_after: timedelta = timedelta(hours=36)

    def __post_init__(self) -> None:
        if not self.instrument_id.strip():
            raise ValueError("instrument_id cannot be blank")
        if not self.display_name.strip():
            raise ValueError("display_name cannot be blank")
        if not self.code_aliases and not self.name_aliases:
            raise ValueError("at least one exact provider alias is required")
        if any(not alias.strip() for alias in (*self.code_aliases, *self.name_aliases)):
            raise ValueError("provider aliases cannot be blank")
        if self.stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        try:
            ZoneInfo(self.local_timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown local_timezone: {self.local_timezone}") from exc


@dataclass(frozen=True, slots=True)
class CrossMarketQuote:
    """从单个全市场提供方快照中严格解析的一条行情。"""

    instrument_id: str
    display_name: str
    segment: CrossMarketSegment
    provider_code: str
    provider_name: str
    last: Decimal
    change_percent: Decimal
    change_amount: Decimal | None
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    previous_close: Decimal | None
    amplitude_percent: Decimal | None
    local_quote_time: datetime
    local_timezone: str
    fetched_at: datetime
    provider: str
    stale: bool
    degraded: bool
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_aware(self.local_quote_time, "local_quote_time")
        _require_aware(self.fetched_at, "fetched_at")
        if self.last <= 0:
            raise ValueError("last must be positive")


@dataclass(frozen=True, slots=True)
class CrossMarketMissingItem:
    """提供方表中缺失的请求序列；不会虚构任何数值。"""

    instrument_id: str
    display_name: str
    segment: CrossMarketSegment
    expected_codes: tuple[str, ...]
    expected_names: tuple[str, ...]
    reason: str


@dataclass(frozen=True, slots=True)
class CrossMarketSnapshot:
    """时点跨市场观测及显式覆盖缺口。"""

    fetched_at: datetime
    provider: str
    quotes: tuple[CrossMarketQuote, ...]
    missing: tuple[CrossMarketMissingItem, ...]
    degraded: bool
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_aware(self.fetched_at, "fetched_at")
        quoted_ids = [quote.instrument_id for quote in self.quotes]
        missing_ids = [item.instrument_id for item in self.missing]
        if len(quoted_ids) != len(set(quoted_ids)):
            raise ValueError("snapshot contains duplicate quote instrument_id values")
        if len(missing_ids) != len(set(missing_ids)):
            raise ValueError("snapshot contains duplicate missing instrument_id values")
        overlap = set(quoted_ids).intersection(missing_ids)
        if overlap:
            raise ValueError(f"snapshot IDs cannot be both quoted and missing: {sorted(overlap)}")


@dataclass(frozen=True, slots=True)
class CrossMarketSummaryItem:
    """已可展示且恰有三位小数的数值。"""

    instrument_id: str
    display_name: str
    segment: CrossMarketSegment
    last: str
    change_percent: str
    local_quote_time: datetime
    stale: bool
    degraded: bool


@dataclass(frozen=True, slots=True)
class CrossMarketThreeDecimalSummary:
    """供通知与报告层使用的紧凑无损状态投影。"""

    fetched_at: datetime
    provider: str
    items: tuple[CrossMarketSummaryItem, ...]
    missing: tuple[CrossMarketMissingItem, ...]
    degraded: bool
    interpretation_note: str = (
        "跨市场同步涨跌仅表示共振或相关性，不构成因果关系证据。"
    )


def build_three_decimal_summary(
    snapshot: CrossMarketSnapshot,
) -> CrossMarketThreeDecimalSummary:
    """将数值行情投影为稳定的三位小数展示字符串。"""

    quantum = Decimal("0.001")
    return CrossMarketThreeDecimalSummary(
        fetched_at=snapshot.fetched_at,
        provider=snapshot.provider,
        items=tuple(
            CrossMarketSummaryItem(
                instrument_id=quote.instrument_id,
                display_name=quote.display_name,
                segment=quote.segment,
                last=format(quote.last.quantize(quantum, rounding=ROUND_HALF_UP), "f"),
                change_percent=format(
                    quote.change_percent.quantize(quantum, rounding=ROUND_HALF_UP),
                    "f",
                ),
                local_quote_time=quote.local_quote_time,
                stale=quote.stale,
                degraded=quote.degraded,
            )
            for quote in snapshot.quotes
        ),
        missing=snapshot.missing,
        degraded=snapshot.degraded,
    )


@runtime_checkable
class CrossMarketData(Protocol):
    def fetch_cross_market_snapshot(self) -> CrossMarketSnapshot: ...


@runtime_checkable
class AsyncCrossMarketData(Protocol):
    async def fetch_cross_market_snapshot_async(self) -> CrossMarketSnapshot: ...


def _require_aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
