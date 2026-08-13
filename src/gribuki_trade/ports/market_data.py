"""Stable ports and transport-neutral types for market data.

The source semantics are deliberately part of the contract.  A public web
``time-and-sales`` endpoint is useful for research, but it must not be exposed
to a strategy as an exchange sequenced tick feed.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, runtime_checkable

from gribuki_trade.domain.market import DailyBar, PriceAdjustment


class MarketDataUnavailableError(RuntimeError):
    """The provider could not supply market data for this request."""


class MarketDataTimeoutError(MarketDataUnavailableError):
    """The market-data request exceeded its configured time limit."""


class SourceSemantics(StrEnum):
    """What an upstream record actually represents."""

    PUBLIC_WEB_QUOTE_SNAPSHOT = "PUBLIC_WEB_QUOTE_SNAPSHOT"
    PUBLIC_WEB_TIME_AND_SALES = "PUBLIC_WEB_TIME_AND_SALES"
    AGGREGATED_MINUTE_BAR = "AGGREGATED_MINUTE_BAR"
    PROVIDER_HISTORICAL_DAILY_BAR = "PROVIDER_HISTORICAL_DAILY_BAR"


class FreshnessStatus(StrEnum):
    """Freshness assessed when a record is collected.

    ``UNKNOWN`` is materially different from ``CURRENT``.  For example,
    AKShare's all-A-share snapshot contains no authoritative quote timestamp,
    so a recent HTTP fetch cannot prove that the underlying quote is current.
    """

    CURRENT = "CURRENT"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"


class MinuteInterval(StrEnum):
    ONE_MINUTE = "1m"
    FIVE_MINUTES = "5m"

    @property
    def minutes(self) -> int:
        return 1 if self is MinuteInterval.ONE_MINUTE else 5


class TradeDirection(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    NEUTRAL = "NEUTRAL"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class MarketDataMeta:
    """Provenance and point-in-time metadata attached to a provider record."""

    provider: str
    semantics: SourceSemantics
    fetched_at: datetime
    provider_timestamp: datetime | None
    freshness: FreshnessStatus
    degraded: bool = False
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.fetched_at.tzinfo is None or self.fetched_at.utcoffset() is None:
            raise ValueError("fetched_at must be timezone-aware")
        timestamp = self.provider_timestamp
        if timestamp is not None and (
            timestamp.tzinfo is None or timestamp.utcoffset() is None
        ):
            raise ValueError("provider_timestamp must be timezone-aware")


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    """Best-effort public-web quote snapshot, not an exchange tick."""

    symbol: str
    name: str | None
    last: Decimal | None
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    previous_close: Decimal | None
    volume_lots: int
    amount: Decimal
    turnover_percent: Decimal | None
    meta: MarketDataMeta


@dataclass(frozen=True, slots=True)
class TradePrint:
    """A public-web time-and-sales row.

    AKShare/Eastmoney does not expose an exchange sequence number through this
    endpoint, so these rows cannot be used for order-book reconstruction.
    ``volume_lots`` preserves the provider's reported unit (A-share lots).
    """

    symbol: str
    occurred_at: datetime
    price: Decimal
    volume_lots: int
    direction: TradeDirection
    exchange_sequence: int | None
    meta: MarketDataMeta


@dataclass(frozen=True, slots=True)
class IntradayBar:
    """A completed, provider-aggregated minute bar."""

    symbol: str
    start_at: datetime
    end_at: datetime
    interval: MinuteInterval
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume_lots: int
    amount: Decimal
    vwap: Decimal | None
    is_closed: bool
    meta: MarketDataMeta


@dataclass(frozen=True, slots=True)
class TradeCalendarDay:
    """One natural day from a provider's official trading calendar.

    Calendar adapters must return every natural day in the requested inclusive
    range.  This makes weekends and exchange holidays explicit instead of
    forcing callers to infer them from missing rows.
    """

    calendar_date: date
    is_trading_day: bool


@runtime_checkable
class HistoricalDailyData(Protocol):
    def fetch_daily_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> Sequence[DailyBar]: ...


@runtime_checkable
class AsyncHistoricalDailyData(Protocol):
    async def fetch_daily_bars_async(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> Sequence[DailyBar]: ...


@runtime_checkable
class AsyncTradingCalendar(Protocol):
    async def fetch_trade_calendar_async(
        self,
        start: date,
        end: date,
    ) -> Sequence[TradeCalendarDay]: ...


@runtime_checkable
class SpotMarketData(Protocol):
    def fetch_spot_snapshot(self, symbol: str) -> MarketSnapshot: ...


@runtime_checkable
class IntradayMarketData(Protocol):
    def fetch_trade_prints(self, symbol: str) -> Sequence[TradePrint]: ...

    def fetch_intraday_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        interval: MinuteInterval = MinuteInterval.ONE_MINUTE,
        completed_only: bool = True,
    ) -> Sequence[IntradayBar]: ...


@runtime_checkable
class AsyncSpotMarketData(Protocol):
    async def fetch_spot_snapshot_async(self, symbol: str) -> MarketSnapshot: ...


@runtime_checkable
class AsyncIntradayMarketData(Protocol):
    async def fetch_trade_prints_async(self, symbol: str) -> Sequence[TradePrint]: ...

    async def fetch_intraday_bars_async(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        interval: MinuteInterval = MinuteInterval.ONE_MINUTE,
        completed_only: bool = True,
    ) -> Sequence[IntradayBar]: ...
