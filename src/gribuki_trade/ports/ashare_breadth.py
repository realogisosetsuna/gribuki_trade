"""Provider-neutral contracts for an A-share close breadth snapshot.

Breadth in this module is contextual research data, not an exchange feed.  A
snapshot is accepted only when a sufficiently large, explicitly沪深京 universe
can be parsed.  An independently sourced listing count is deliberately not
invented when the public quote providers do not publish one with the payload.
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


class AShareBreadthDataError(MarketDataUnavailableError):
    """Base failure raised by a close-breadth adapter."""


class AShareBreadthTimeoutError(MarketDataTimeoutError, AShareBreadthDataError):
    """One independently bounded public snapshot call timed out."""


class AShareBreadthEndpointError(AShareBreadthDataError):
    """The installed AKShare client does not expose a required endpoint."""


class AShareBreadthPayloadError(AShareBreadthDataError):
    """A provider payload cannot be parsed without ambiguity."""


class AShareBreadthCoverageError(AShareBreadthDataError):
    """A parsed payload failed the minimum-universe completeness gate."""


@dataclass(frozen=True, slots=True)
class AShareBreadthSourceFailure:
    """Stable, non-secret diagnostic for one failed provider attempt."""

    source_id: str
    failure_code: str
    reason: str

    def __post_init__(self) -> None:
        if not self.source_id.strip() or not self.failure_code.strip() or not self.reason.strip():
            raise ValueError("breadth source failure fields cannot be blank")


class AShareBreadthSourcesExhaustedError(AShareBreadthDataError):
    """All configured breadth sources failed independently."""

    def __init__(self, failures: tuple[AShareBreadthSourceFailure, ...]) -> None:
        if not failures:
            raise ValueError("at least one breadth source failure is required")
        self.failures = failures
        super().__init__("all A-share breadth snapshot sources failed")


class AShareExchange(StrEnum):
    SHANGHAI = "SHANGHAI"
    SHENZHEN = "SHENZHEN"
    BEIJING = "BEIJING"


@dataclass(frozen=True, slots=True)
class AShareExchangeCount:
    exchange: AShareExchange
    eligible_count: int

    def __post_init__(self) -> None:
        if self.eligible_count < 0:
            raise ValueError("exchange eligible_count cannot be negative")


@dataclass(frozen=True, slots=True)
class AShareBreadthMeta:
    """Provenance and first-seen state for a secondary public-web snapshot."""

    source_id: str
    source_url: str
    observed_at: datetime
    available_at: datetime
    fetched_at: datetime
    stale: bool
    degraded: bool
    fallback_used: bool
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.source_id.strip() or not self.source_url.strip():
            raise ValueError("breadth source identifiers cannot be blank")
        for field_name in ("observed_at", "available_at", "fetched_at"):
            _require_aware(getattr(self, field_name), field_name)
        if self.observed_at > self.available_at:
            raise ValueError("observed_at cannot follow available_at")
        if self.available_at > self.fetched_at:
            raise ValueError("available_at cannot follow fetched_at")
        if self.stale and not self.degraded:
            raise ValueError("a stale breadth snapshot must be degraded")
        if self.fallback_used and not self.degraded:
            raise ValueError("a fallback breadth snapshot must be degraded")
        if any(not warning.strip() for warning in self.warnings):
            raise ValueError("breadth warnings cannot be blank")


@dataclass(frozen=True, slots=True)
class AShareBreadthSnapshot:
    """Aggregate close breadth for eligible沪深京 A-share equities.

    ``received_count`` is the raw number of provider rows.  ``eligible_count``
    is the number of unique, trading A-share rows used in the aggregates.
    ``expected_count`` and ``coverage_percent`` remain ``None`` unless a truly
    independent universe count becomes available; the minimum-count gate is
    not mislabeled as precise coverage.
    """

    session_date: date
    received_count: int
    eligible_count: int
    minimum_eligible_count: int
    expected_count: int | None
    coverage_percent: Decimal | None
    duplicate_count: int
    excluded_non_equity_count: int
    non_trading_count: int
    included_exchanges: tuple[AShareExchangeCount, ...]
    advancing_count: int
    declining_count: int
    flat_count: int
    advance_decline_ratio: Decimal | None
    advancing_amount_share_percent: Decimal | None
    equal_weight_mean_change_percent: Decimal
    median_change_percent: Decimal
    total_amount_cny: Decimal
    limit_up_count: int | None
    limit_down_count: int | None
    meta: AShareBreadthMeta

    def __post_init__(self) -> None:
        count_fields = (
            "received_count",
            "eligible_count",
            "minimum_eligible_count",
            "duplicate_count",
            "excluded_non_equity_count",
            "non_trading_count",
            "advancing_count",
            "declining_count",
            "flat_count",
        )
        if any(getattr(self, name) < 0 for name in count_fields):
            raise ValueError("breadth counts cannot be negative")
        if self.minimum_eligible_count < 1:
            raise ValueError("minimum_eligible_count must be positive")
        if self.eligible_count < self.minimum_eligible_count:
            raise ValueError("eligible_count failed minimum completeness gate")
        if self.advancing_count + self.declining_count + self.flat_count != self.eligible_count:
            raise ValueError("direction counts must sum to eligible_count")
        exchange_counts = tuple(item.exchange for item in self.included_exchanges)
        if exchange_counts != tuple(AShareExchange):
            raise ValueError("included_exchanges must explicitly cover Shanghai, Shenzhen, Beijing")
        if sum(item.eligible_count for item in self.included_exchanges) != self.eligible_count:
            raise ValueError("exchange counts must sum to eligible_count")
        if self.expected_count is None:
            if self.coverage_percent is not None:
                raise ValueError("coverage_percent requires an independent expected_count")
        else:
            if self.expected_count < self.eligible_count or self.expected_count < 1:
                raise ValueError("expected_count cannot be below eligible_count")
            if self.coverage_percent is None:
                raise ValueError("expected_count requires coverage_percent")
        for name in (
            "advance_decline_ratio",
            "advancing_amount_share_percent",
            "equal_weight_mean_change_percent",
            "median_change_percent",
            "total_amount_cny",
            "coverage_percent",
        ):
            value = getattr(self, name)
            if value is not None and not value.is_finite():
                raise ValueError(f"{name} must be finite")
        if self.advance_decline_ratio is not None and self.advance_decline_ratio < 0:
            raise ValueError("advance_decline_ratio cannot be negative")
        if self.advancing_amount_share_percent is not None and not (
            Decimal("0") <= self.advancing_amount_share_percent <= Decimal("100")
        ):
            raise ValueError("advancing_amount_share_percent must be in [0, 100]")
        if self.total_amount_cny < 0:
            raise ValueError("total_amount_cny cannot be negative")
        if self.limit_up_count is not None or self.limit_down_count is not None:
            raise ValueError("limit counts require a separately audited board-rule engine")


@runtime_checkable
class AShareCloseBreadthData(Protocol):
    def fetch_close_breadth(self, session_date: date) -> AShareBreadthSnapshot: ...


@runtime_checkable
class AsyncAShareCloseBreadthData(Protocol):
    async def fetch_close_breadth_async(
        self,
        session_date: date,
    ) -> AShareBreadthSnapshot: ...


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
