"""稳定的市场数据端口与传输无关类型。

来源语义刻意作为契约的一部分。公开网页的 ``time-and-sales`` 端点可用于研究，
但不得作为交易所排序的逐笔数据源暴露给策略。
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
    """提供方无法为此请求提供市场数据。"""


class MarketDataTimeoutError(MarketDataUnavailableError):
    """市场数据请求超过已配置的时限。"""


class SourceSemantics(StrEnum):
    """上游记录实际表示的内容。"""

    PUBLIC_WEB_QUOTE_SNAPSHOT = "PUBLIC_WEB_QUOTE_SNAPSHOT"
    PUBLIC_WEB_TIME_AND_SALES = "PUBLIC_WEB_TIME_AND_SALES"
    AGGREGATED_MINUTE_BAR = "AGGREGATED_MINUTE_BAR"
    PROVIDER_HISTORICAL_DAILY_BAR = "PROVIDER_HISTORICAL_DAILY_BAR"


class FreshnessStatus(StrEnum):
    """收集记录时评估的新鲜度。

    ``UNKNOWN`` 与 ``CURRENT`` 有实质差异。例如，AKShare 全 A 股快照不含权威行情
    时间戳，因此近期的 HTTP 获取不能证明底层行情为当前数据。
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
    """附加到提供方记录的来源与时点元数据。"""

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
    """尽力而为的公开网页行情快照，并非交易所逐笔数据。"""

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
    """一条公开网页逐笔成交记录。

    AKShare/Eastmoney 不通过此端点暴露交易所序列号，因此这些记录不能用于重建订单簿。
    ``volume_lots`` 保留提供方报告的单位（A 股手数）。
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
    """一根由提供方聚合且已完成的分钟 K 线。"""

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
    """提供方官方交易日历中的一个自然日。

    日历适配器必须返回请求闭区间内的每个自然日。这样周末与交易所休市日会被显式
    表达，而无需调用方根据缺失记录进行推断。
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
