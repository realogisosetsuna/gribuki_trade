"""Provider-neutral contracts for observational cross-market snapshots.

The types in this module deliberately describe quotes and their provenance,
not causal relationships.  A simultaneous move in two markets is evidence of
co-movement only; a research layer must supply separately sourced evidence
before making a causal claim.
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
    """Base failure raised by a cross-market data adapter."""


class CrossMarketDataTimeoutError(MarketDataTimeoutError, CrossMarketDataError):
    """The full-table snapshot did not finish within the caller's deadline."""


class CrossMarketPayloadError(CrossMarketDataError):
    """The provider returned an incompatible or internally invalid payload."""


class CrossMarketSegment(StrEnum):
    """Broad market bucket used for report composition, never causal inference."""

    A_SHARE = "A_SHARE"
    HONG_KONG = "HONG_KONG"
    ASIA_PACIFIC = "ASIA_PACIFIC"
    UNITED_STATES = "UNITED_STATES"
    DOLLAR_COMMODITY_RISK = "DOLLAR_COMMODITY_RISK"


@dataclass(frozen=True, slots=True)
class CrossMarketInstrumentSpec:
    """Exact provider aliases and local-time semantics for one desired series."""

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
    """One strictly parsed quote from a single full-market provider snapshot."""

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
    """A requested series absent from the provider table; no value is invented."""

    instrument_id: str
    display_name: str
    segment: CrossMarketSegment
    expected_codes: tuple[str, ...]
    expected_names: tuple[str, ...]
    reason: str


@dataclass(frozen=True, slots=True)
class CrossMarketSnapshot:
    """Point-in-time cross-market observations plus explicit coverage gaps."""

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
    """Presentation-ready values with exactly three fractional digits."""

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
    """Compact lossless-status projection for notification/report layers."""

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
    """Project numeric quote values to stable three-decimal display strings."""

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
