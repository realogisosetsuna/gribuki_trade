"""A 股研究使用的供应商无关时点上下文。

这些契约将上下文观测与可执行市场数据分开。公共 ETF 报价、定盘利率、收益率曲线或
期货日线记录可以丰富研究证据，但都不能授权订单或证明因果关系。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, runtime_checkable

from gribuki_trade.ports.market_data import (
    MarketDataTimeoutError,
    MarketDataUnavailableError,
)


class AShareContextDataError(MarketDataUnavailableError):
    """A 股上下文数据适配器抛出的基础失败。"""


class AShareContextTimeoutError(MarketDataTimeoutError, AShareContextDataError):
    """一次独立有界的上游调用超过截止时间。"""


class AShareContextEndpointError(AShareContextDataError):
    """已安装供应商未公开经审计的端点。"""


class AShareContextPayloadError(AShareContextDataError):
    """上游响应无法无歧义地解析。"""


class AShareContextNoDataError(AShareContextDataError):
    """端点未返回合格观测。"""


class AShareContextFailureCode(StrEnum):
    ENDPOINT_UNAVAILABLE = "ENDPOINT_UNAVAILABLE"
    TIMEOUT = "TIMEOUT"
    UPSTREAM_ERROR = "UPSTREAM_ERROR"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"
    NO_DATA = "NO_DATA"


@dataclass(frozen=True, slots=True)
class AShareContextMeta:
    """一条上下文观测的来源与首次可见时间。"""

    source_id: str
    source_url: str
    observed_at: datetime
    available_at: datetime
    fetched_at: datetime
    stale: bool
    degraded: bool
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.source_id.strip() or not self.source_url.strip():
            raise ValueError("context source identifiers cannot be blank")
        for field_name in ("observed_at", "available_at", "fetched_at"):
            _require_aware(getattr(self, field_name), field_name)
        if self.observed_at > self.available_at:
            raise ValueError("observed_at cannot follow available_at")
        if self.available_at > self.fetched_at:
            raise ValueError("available_at cannot follow fetched_at")
        if self.stale and not self.degraded:
            raise ValueError("a stale context observation must be degraded")
        if any(not warning.strip() for warning in self.warnings):
            raise ValueError("context warnings cannot be blank")


@dataclass(frozen=True, slots=True)
class ETFContextSnapshot:
    """来自公共全市场快照的一条精确 ETF 记录。

    百分比字段保留供应商的百分点单位。例如，``discount_rate_percent=-0.05``
    表示负 0.05%，而不是负 5%。供应商分类的订单规模流量只用于观测，绝不视为
    交易所参与者身份。
    """

    symbol: str
    name: str
    data_date: date
    last: Decimal
    iopv: Decimal | None
    discount_rate_percent: Decimal | None
    turnover_percent: Decimal | None
    shares_outstanding: Decimal | None
    amount_cny: Decimal | None
    main_net_inflow_cny: Decimal | None
    main_net_inflow_percent: Decimal | None
    bid1: Decimal | None
    ask1: Decimal | None
    meta: AShareContextMeta

    def __post_init__(self) -> None:
        if not self.symbol.strip() or not self.name.strip():
            raise ValueError("ETF symbol and name cannot be blank")
        if self.last <= 0 or not self.last.is_finite():
            raise ValueError("ETF last must be positive and finite")
        for field_name in ("iopv", "bid1", "ask1"):
            value = getattr(self, field_name)
            if value is not None and (not value.is_finite() or value <= 0):
                raise ValueError(f"ETF {field_name} must be positive and finite")
        for field_name in (
            "turnover_percent",
            "shares_outstanding",
            "amount_cny",
        ):
            value = getattr(self, field_name)
            if value is not None and (not value.is_finite() or value < 0):
                raise ValueError(f"ETF {field_name} must be non-negative and finite")
        for field_name in (
            "discount_rate_percent",
            "main_net_inflow_cny",
            "main_net_inflow_percent",
        ):
            value = getattr(self, field_name)
            if value is not None and not value.is_finite():
                raise ValueError(f"ETF {field_name} must be finite")
        if self.bid1 is not None and self.ask1 is not None and self.bid1 > self.ask1:
            raise ValueError("ETF bid1 cannot exceed ask1")


class RepoFixingFamily(StrEnum):
    """精确的中国货币网定盘品种；两个值都不代表 DR007。"""

    FR = "FR"
    FDR = "FDR"


@dataclass(frozen=True, slots=True)
class RepoFixingObservation:
    """一条 FR 或 FDR 每日定盘记录，利率单位为百分比。"""

    family: RepoFixingFamily
    session_date: date
    overnight_percent: Decimal
    seven_day_percent: Decimal
    fourteen_day_percent: Decimal
    meta: AShareContextMeta

    def __post_init__(self) -> None:
        for field_name in (
            "overnight_percent",
            "seven_day_percent",
            "fourteen_day_percent",
        ):
            value = getattr(self, field_name)
            if not value.is_finite() or value < 0:
                raise ValueError(f"repo fixing {field_name} must be non-negative and finite")


@dataclass(frozen=True, slots=True)
class GovernmentBondYieldPoint:
    """中债国债收益率曲线上的一个期限，单位为百分比。"""

    tenor_years: Decimal
    yield_percent: Decimal

    def __post_init__(self) -> None:
        if self.tenor_years <= 0 or not self.tenor_years.is_finite():
            raise ValueError("yield tenor must be positive and finite")
        if not self.yield_percent.is_finite():
            raise ValueError("yield must be finite")


@dataclass(frozen=True, slots=True)
class GovernmentBondYieldCurve:
    curve_name: str
    session_date: date
    points: tuple[GovernmentBondYieldPoint, ...]
    meta: AShareContextMeta

    def __post_init__(self) -> None:
        if not self.curve_name.strip() or not self.points:
            raise ValueError("government-bond curve name and points cannot be empty")
        tenors = tuple(point.tenor_years for point in self.points)
        if tenors != tuple(sorted(tenors)) or len(tenors) != len(set(tenors)):
            raise ValueError("government-bond curve tenors must be ordered and unique")


@dataclass(frozen=True, slots=True)
class AShareContextMissingSource:
    context_id: str
    display_name: str
    expected_source: str
    failure_code: AShareContextFailureCode
    reason: str

    def __post_init__(self) -> None:
        if any(
            not value.strip()
            for value in (
                self.context_id,
                self.display_name,
                self.expected_source,
                self.reason,
            )
        ):
            raise ValueError("missing-source fields cannot be blank")


@dataclass(frozen=True, slots=True)
class LiquidityContextSnapshot:
    """对两个定盘品种与一条主权曲线执行软失败采集。"""

    fetched_at: datetime
    cutoff_date: date
    repo_fixings: tuple[RepoFixingObservation, ...]
    government_curve: GovernmentBondYieldCurve | None
    missing: tuple[AShareContextMissingSource, ...]
    degraded: bool
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_aware(self.fetched_at, "fetched_at")
        families = tuple(item.family for item in self.repo_fixings)
        if len(families) != len(set(families)):
            raise ValueError("liquidity snapshot contains duplicate fixing families")
        missing_ids = tuple(item.context_id for item in self.missing)
        if len(missing_ids) != len(set(missing_ids)):
            raise ValueError("liquidity snapshot contains duplicate missing sources")
        expected_degraded = (
            bool(self.missing)
            or any(item.meta.degraded for item in self.repo_fixings)
            or (self.government_curve is not None and self.government_curve.meta.degraded)
        )
        if self.degraded != expected_degraded:
            raise ValueError("liquidity degraded flag does not match source status")


@dataclass(frozen=True, slots=True)
class IFContractDailyObservation:
    """一条中金所 IF 合约官方日线记录；不推断现货基差。"""

    symbol: str
    session_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    settle: Decimal
    previous_settle: Decimal
    volume: int
    open_interest: int
    turnover_reported: Decimal
    meta: AShareContextMeta

    def __post_init__(self) -> None:
        if not self.symbol.startswith("IF"):
            raise ValueError("IF contract symbol must start with IF")
        prices = (self.open, self.high, self.low, self.close, self.settle, self.previous_settle)
        if any(not value.is_finite() or value <= 0 for value in prices):
            raise ValueError("IF contract prices must be positive and finite")
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("IF contract high is inconsistent with OHLC")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("IF contract low is inconsistent with OHLC")
        if self.volume < 0 or self.open_interest < 0:
            raise ValueError("IF volume and open interest cannot be negative")
        if not self.turnover_reported.is_finite() or self.turnover_reported < 0:
            raise ValueError("IF reported turnover must be non-negative and finite")


@dataclass(frozen=True, slots=True)
class IFDailyContextSnapshot:
    """官方 IF 日合约；后续必须提供已对齐的沪深 300 现货。"""

    session_date: date
    fetched_at: datetime
    contracts: tuple[IFContractDailyObservation, ...]
    degraded: bool
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_aware(self.fetched_at, "fetched_at")
        if not self.contracts:
            raise ValueError("IF daily snapshot cannot be empty")
        symbols = tuple(item.symbol for item in self.contracts)
        if symbols != tuple(sorted(symbols)) or len(symbols) != len(set(symbols)):
            raise ValueError("IF contract symbols must be ordered and unique")
        if any(item.session_date != self.session_date for item in self.contracts):
            raise ValueError("IF contracts must match snapshot session_date")
        if self.degraded != any(item.meta.degraded for item in self.contracts):
            raise ValueError("IF degraded flag does not match contract status")


@runtime_checkable
class ETFContextData(Protocol):
    def fetch_etf_context(self, symbol: str) -> ETFContextSnapshot: ...


@runtime_checkable
class AsyncETFContextData(Protocol):
    async def fetch_etf_context_async(self, symbol: str) -> ETFContextSnapshot: ...


@runtime_checkable
class LiquidityContextData(Protocol):
    def fetch_liquidity_context(
        self,
        *,
        start_date: date,
        end_date: date,
    ) -> LiquidityContextSnapshot: ...


@runtime_checkable
class AsyncLiquidityContextData(Protocol):
    async def fetch_liquidity_context_async(
        self,
        *,
        start_date: date,
        end_date: date,
    ) -> LiquidityContextSnapshot: ...


@runtime_checkable
class IFDailyContextData(Protocol):
    def fetch_if_daily_context(self, session_date: date) -> IFDailyContextSnapshot: ...


@runtime_checkable
class AsyncIFDailyContextData(Protocol):
    async def fetch_if_daily_context_async(
        self,
        session_date: date,
    ) -> IFDailyContextSnapshot: ...


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
