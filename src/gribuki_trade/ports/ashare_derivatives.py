"""Point-in-time contracts for official SSE ETF and option research data.

The exchange pages expose end-of-day observations, not executable quotes.  In
particular, an ETF ``STAT_DATE`` is the date of the post-settlement share count
and option risk numbers are calculated from that session's closing data.  The
official endpoints do not publish a machine-readable release timestamp, so
``available_at`` deliberately means the collector's first-seen time.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol, runtime_checkable


class SSEOfficialDataError(RuntimeError):
    """Base error for the two read-only SSE official sources."""


class SSEOptionRiskDataError(SSEOfficialDataError):
    """Base error for the SSE option-risk source."""


class SSEOptionRiskTimeoutError(TimeoutError, SSEOptionRiskDataError):
    """The SSE option-risk request exceeded its deadline."""


class SSEOptionRiskTransportError(ConnectionError, SSEOptionRiskDataError):
    """The SSE option-risk endpoint could not be reached."""


class SSEOptionRiskHTTPStatusError(SSEOptionRiskDataError):
    """The SSE option-risk endpoint returned an unacceptable HTTP status."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"official SSE option-risk source returned HTTP {status_code}")


class SSEOptionRiskSchemaError(SSEOptionRiskDataError):
    """The option-risk payload cannot be interpreted without ambiguity."""


class SSEOptionRiskNoDataError(SSEOptionRiskDataError):
    """No eligible completed option-risk session was available."""


class SSEOptionRiskNotVisibleError(SSEOptionRiskDataError):
    """The fetched option-risk document was first seen after ``as_of``."""


class SSEETFShareDataError(SSEOfficialDataError):
    """Base error for the SSE post-settlement ETF-share source."""


class SSEETFShareTimeoutError(TimeoutError, SSEETFShareDataError):
    """The SSE ETF-share request exceeded its deadline."""


class SSEETFShareTransportError(ConnectionError, SSEETFShareDataError):
    """The SSE ETF-share endpoint could not be reached."""


class SSEETFShareHTTPStatusError(SSEETFShareDataError):
    """The SSE ETF-share endpoint returned an unacceptable HTTP status."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"official SSE ETF-share source returned HTTP {status_code}")


class SSEETFShareSchemaError(SSEETFShareDataError):
    """The ETF-share payload cannot be interpreted without ambiguity."""


class SSEETFShareNoDataError(SSEETFShareDataError):
    """No eligible post-settlement ETF-share observation was available."""


class SSEETFShareNotVisibleError(SSEETFShareDataError):
    """The fetched ETF-share document was first seen after ``as_of``."""


@dataclass(frozen=True, slots=True)
class SSEOfficialSourceMeta:
    """Provenance and first-seen timing for one official SSE observation."""

    source_id: str
    source_url: str
    requested_date: date
    observed_date: date
    available_at: datetime
    fetched_at: datetime
    exact_date_match: bool
    latest_available_fallback: bool
    content_sha256: str
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.source_id.strip() or not self.source_url.strip():
            raise ValueError("official SSE source identifiers cannot be blank")
        _require_aware(self.available_at, "available_at")
        _require_aware(self.fetched_at, "fetched_at")
        if self.available_at > self.fetched_at:
            raise ValueError("available_at cannot follow fetched_at")
        if self.observed_date > self.requested_date:
            raise ValueError("observed_date cannot follow requested_date")
        if self.exact_date_match != (self.observed_date == self.requested_date):
            raise ValueError("exact_date_match does not match observed_date")
        if self.latest_available_fallback == self.exact_date_match:
            raise ValueError("exact and latest-available flags must be mutually exclusive")
        if len(self.content_sha256) != 64:
            raise ValueError("content_sha256 must be a SHA-256 hex digest")
        try:
            int(self.content_sha256, 16)
        except ValueError as exc:
            raise ValueError("content_sha256 must be a SHA-256 hex digest") from exc
        if any(not warning.strip() for warning in self.warnings):
            raise ValueError("official SSE source warnings cannot be blank")


@dataclass(frozen=True, slots=True)
class SSEOptionRiskContract:
    """One official SSE closing-risk row with its source decimal precision."""

    security_id: str
    contract_id: str
    contract_symbol: str
    contract_type: str
    delta: Decimal
    theta: Decimal
    gamma: Decimal
    vega: Decimal
    rho: Decimal
    implied_volatility: Decimal
    raw_fields: tuple[tuple[str, str], ...] = field(repr=False)

    def __post_init__(self) -> None:
        if any(
            not value.strip()
            for value in (
                self.security_id,
                self.contract_id,
                self.contract_symbol,
                self.contract_type,
            )
        ):
            raise ValueError("option contract identifiers cannot be blank")
        values = (
            self.delta,
            self.theta,
            self.gamma,
            self.vega,
            self.rho,
            self.implied_volatility,
        )
        if any(not value.is_finite() for value in values):
            raise ValueError("option risk values must be finite")
        if self.gamma < 0 or self.vega < 0 or self.implied_volatility < 0:
            raise ValueError("Gamma, Vega, and implied volatility cannot be negative")
        if self.contract_type == "认购" and not Decimal("0") <= self.delta <= Decimal("1"):
            raise ValueError("call Delta must be between zero and one")
        if self.contract_type == "认沽" and not Decimal("-1") <= self.delta <= Decimal("0"):
            raise ValueError("put Delta must be between minus one and zero")
        if self.contract_type not in {"认购", "认沽"}:
            raise ValueError("contract_type must be 认购 or 认沽")
        names = tuple(name for name, _ in self.raw_fields)
        empty_raw_field = any(
            not name or not value for name, value in self.raw_fields
        )
        if len(names) != len(set(names)) or empty_raw_field:
            raise ValueError("raw option fields must be non-empty and unique")


@dataclass(frozen=True, slots=True)
class SSEOptionRiskSnapshot:
    """All official closing-risk rows for one SSE ETF underlying and date."""

    underlying_symbol: str
    underlying_name: str
    contracts: tuple[SSEOptionRiskContract, ...]
    meta: SSEOfficialSourceMeta

    def __post_init__(self) -> None:
        if not self.underlying_symbol.strip() or not self.underlying_name.strip():
            raise ValueError("option underlying identifiers cannot be blank")
        if not self.contracts:
            raise ValueError("option-risk snapshot cannot be empty")
        security_ids = tuple(item.security_id for item in self.contracts)
        contract_ids = tuple(item.contract_id for item in self.contracts)
        if len(security_ids) != len(set(security_ids)):
            raise ValueError("option-risk snapshot has duplicate security IDs")
        if len(contract_ids) != len(set(contract_ids)):
            raise ValueError("option-risk snapshot has duplicate contract IDs")


@dataclass(frozen=True, slots=True)
class SSEETFShareObservation:
    """One official SSE post-settlement ETF total-share observation.

    ``total_shares_ten_thousands`` is the field exactly as published by SSE;
    ``total_shares`` is an exact unit conversion, not an estimate of NAV or
    assets under management.
    """

    symbol: str
    name: str
    expanded_name: str | None
    etf_type: str | None
    total_shares_ten_thousands: Decimal
    total_shares: Decimal
    raw_total_shares: str
    meta: SSEOfficialSourceMeta

    def __post_init__(self) -> None:
        if not self.symbol.strip() or not self.name.strip() or not self.raw_total_shares.strip():
            raise ValueError("ETF share identifiers and raw value cannot be blank")
        if self.expanded_name is not None and not self.expanded_name.strip():
            raise ValueError("expanded ETF name cannot be blank")
        if self.etf_type is not None and not self.etf_type.strip():
            raise ValueError("ETF type cannot be blank")
        if not self.total_shares_ten_thousands.is_finite() or self.total_shares_ten_thousands < 0:
            raise ValueError("ETF total shares in ten-thousands must be non-negative and finite")
        if self.total_shares != self.total_shares_ten_thousands * Decimal("10000"):
            raise ValueError("ETF total_shares unit conversion is inconsistent")


@runtime_checkable
class AsyncSSEOptionRiskData(Protocol):
    async def fetch_option_risk(
        self,
        underlying_symbol: str,
        requested_date: date,
        *,
        as_of: datetime | None = None,
        allow_latest_available: bool = True,
    ) -> SSEOptionRiskSnapshot: ...


@runtime_checkable
class AsyncSSEETFShareData(Protocol):
    async def fetch_etf_shares(
        self,
        symbol: str,
        requested_date: date,
        *,
        as_of: datetime | None = None,
        allow_latest_available: bool = True,
    ) -> SSEETFShareObservation: ...


def digest_official_payload(body: bytes) -> str:
    """Return the exact document digest used by source metadata."""

    return hashlib.sha256(body).hexdigest()


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
