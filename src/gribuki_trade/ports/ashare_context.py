"""Provider-neutral point-in-time context used by A-share research.

These contracts keep contextual observations separate from executable market
data.  A public ETF quote, a fixing rate, a yield curve, or a futures daily row
may enrich research evidence, but none of them authorizes an order or proves a
causal relationship.
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
    """Base failure raised by an A-share contextual-data adapter."""


class AShareContextTimeoutError(MarketDataTimeoutError, AShareContextDataError):
    """One independently bounded upstream call exceeded its deadline."""


class AShareContextEndpointError(AShareContextDataError):
    """The installed provider does not expose the audited endpoint."""


class AShareContextPayloadError(AShareContextDataError):
    """An upstream response cannot be parsed without ambiguity."""


class AShareContextNoDataError(AShareContextDataError):
    """The endpoint returned no eligible observation."""


class AShareContextFailureCode(StrEnum):
    ENDPOINT_UNAVAILABLE = "ENDPOINT_UNAVAILABLE"
    TIMEOUT = "TIMEOUT"
    UPSTREAM_ERROR = "UPSTREAM_ERROR"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"
    NO_DATA = "NO_DATA"


@dataclass(frozen=True, slots=True)
class AShareContextMeta:
    """Provenance and first-seen timing for one contextual observation."""

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
    """One exact ETF row from a public full-market snapshot.

    Percent fields preserve the provider's percentage-point unit.  For
    example, ``discount_rate_percent=-0.05`` means minus 0.05%, not -5%.
    Vendor-classified order-size flows are observational and are never treated
    as exchange participant identities.
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
    """Exact ChinaMoney fixing families; neither value means DR007."""

    FR = "FR"
    FDR = "FDR"


@dataclass(frozen=True, slots=True)
class RepoFixingObservation:
    """One daily FR or FDR fixing row, with rates in percent."""

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
    """One tenor on the ChinaBond government-bond curve, in percent."""

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
    """Fail-soft collection of two fixing families and one sovereign curve."""

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
    """One official CFFEX IF contract daily row; no spot basis is inferred."""

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
    """Official IF daily contracts; an aligned CSI 300 spot must be supplied later."""

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
