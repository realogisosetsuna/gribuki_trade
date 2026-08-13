"""Point-in-time ports for an all-A-share screening funnel.

The contracts split the cheap universe snapshot from the more expensive
historical-factor enrichment.  A screening service can therefore hard-filter
the whole market before requesting histories for the surviving symbols.

``available_at`` is part of both batch contracts.  Providers must return the
revision that was knowable at ``known_at``; a recent download timestamp is not
proof that a historical value was available at an earlier decision time.
"""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, runtime_checkable


class AShareBoard(StrEnum):
    """A-share listing boards supported by the screening domain."""

    SSE_MAIN = "SSE_MAIN"
    SZSE_MAIN = "SZSE_MAIN"
    CHINEXT = "CHINEXT"
    STAR = "STAR"
    BSE = "BSE"


class ScreeningSourceQuality(StrEnum):
    """Whether a source met its preferred semantics without fallback."""

    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"


class ScreeningHistoryPolicy(StrEnum):
    """Corporate-action policy used to construct historical factors.

    ``CURRENTLY_ADJUSTED`` is intentionally represented so adapters can report
    what an upstream endpoint returned.  The service rejects that policy for a
    historical point-in-time run because future corporate actions can rewrite
    past values.
    """

    UNADJUSTED_WITH_CORPORATE_ACTION_GUARD = (
        "UNADJUSTED_WITH_CORPORATE_ACTION_GUARD"
    )
    POINT_IN_TIME_ADJUSTED = "POINT_IN_TIME_ADJUSTED"
    CURRENTLY_ADJUSTED = "CURRENTLY_ADJUSTED"


class ScreeningFactorId(StrEnum):
    """Stable raw factors accepted by the v1 cross-sectional scorer."""

    MOMENTUM_20 = "MOMENTUM_20"
    MOMENTUM_60 = "MOMENTUM_60"
    MOMENTUM_120_SKIP_5 = "MOMENTUM_120_SKIP_5"
    TREND_MA20_OVER_MA60 = "TREND_MA20_OVER_MA60"
    BREAKOUT_20_POSITION = "BREAKOUT_20_POSITION"
    VOLUME_RATIO_20 = "VOLUME_RATIO_20"
    ANNUALIZED_VOLATILITY_60 = "ANNUALIZED_VOLATILITY_60"
    MAX_DRAWDOWN_60_MAGNITUDE = "MAX_DRAWDOWN_60_MAGNITUDE"
    AMIHUD_ILLIQUIDITY_20 = "AMIHUD_ILLIQUIDITY_20"
    AVERAGE_AMOUNT_20_CNY = "AVERAGE_AMOUNT_20_CNY"


@dataclass(frozen=True, slots=True)
class AShareUniverseRecord:
    """Cheap end-of-session snapshot fields used by the hard-filter layer.

    Nullable status fields are deliberate.  ``False`` and "provider did not
    say" are different states; the hard filter fails closed on the latter.
    Numeric range checks belong to the filter so exclusions retain a readable
    reason instead of disappearing during transport parsing.
    """

    symbol: str
    name: str
    board: AShareBoard
    industry: str | None
    listing_days: int | None
    is_tradable: bool | None
    is_st: bool | None
    is_suspended: bool | None
    last_price: Decimal | None
    session_amount_cny: Decimal | None
    market_cap_cny: Decimal | None

    def __post_init__(self) -> None:
        _validate_symbol_board(self.symbol, self.board)
        if not self.name.strip():
            raise ValueError("name must not be empty")
        if self.industry is not None and not self.industry.strip():
            raise ValueError("industry must be non-empty when present")
        if self.listing_days is not None and self.listing_days < 0:
            raise ValueError("listing_days must not be negative")
        for field_name in (
            "last_price",
            "session_amount_cny",
            "market_cap_cny",
        ):
            value = getattr(self, field_name)
            if value is not None and not value.is_finite():
                raise ValueError(f"{field_name} must be finite when present")


@dataclass(frozen=True, slots=True)
class AShareUniverseSnapshot:
    """One complete-market snapshot revision known at a precise time."""

    as_of: date
    available_at: datetime
    observed_at: datetime
    source_id: str
    source_revision: str
    records: tuple[AShareUniverseRecord, ...]
    quality: ScreeningSourceQuality = ScreeningSourceQuality.COMPLETE
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _validate_batch_metadata(
            available_at=self.available_at,
            observed_at=self.observed_at,
            source_id=self.source_id,
            source_revision=self.source_revision,
            warnings=self.warnings,
        )


@dataclass(frozen=True, slots=True)
class AShareFactorValue:
    """One raw factor; ``None`` means absent and is never neutral-filled."""

    factor_id: ScreeningFactorId
    value: float | None

    def __post_init__(self) -> None:
        # Non-finite provider values remain representable so the feature layer
        # can surface an auditable INVALID_FACTOR degradation instead of
        # crashing or silently deleting the symbol.
        if self.value is not None and not isinstance(self.value, (int, float)):
            raise TypeError("factor value must be numeric or None")


@dataclass(frozen=True, slots=True)
class AShareFactorRecord:
    """Historical factors for one symbol, calculated only through ``as_of``."""

    symbol: str
    values: tuple[AShareFactorValue, ...]
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)
        identifiers = tuple(item.factor_id for item in self.values)
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("factor IDs must be unique within a symbol")
        _validate_warnings(self.warnings)

    def value_for(self, factor_id: ScreeningFactorId) -> float | None:
        """Return the raw value without manufacturing a default."""

        return next(
            (item.value for item in self.values if item.factor_id is factor_id),
            None,
        )


@dataclass(frozen=True, slots=True)
class AShareFactorSnapshot:
    """Expensive factor batch for hard-filter survivors only."""

    as_of: date
    available_at: datetime
    observed_at: datetime
    source_id: str
    source_revision: str
    feature_version: str
    history_policy: ScreeningHistoryPolicy
    records: tuple[AShareFactorRecord, ...]
    quality: ScreeningSourceQuality = ScreeningSourceQuality.COMPLETE
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _validate_batch_metadata(
            available_at=self.available_at,
            observed_at=self.observed_at,
            source_id=self.source_id,
            source_revision=self.source_revision,
            warnings=self.warnings,
        )
        if not self.feature_version.strip():
            raise ValueError("feature_version must not be empty")


@runtime_checkable
class AsyncAShareScreeningData(Protocol):
    """Two-stage point-in-time input for the all-market funnel."""

    async def fetch_universe_snapshot(
        self,
        *,
        as_of: date,
        known_at: datetime,
    ) -> AShareUniverseSnapshot: ...

    async def fetch_factor_snapshot(
        self,
        symbols: Sequence[str],
        *,
        as_of: date,
        known_at: datetime,
    ) -> AShareFactorSnapshot: ...


_SYMBOL_PATTERN = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")


def _validate_symbol(symbol: str) -> None:
    if _SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise ValueError("symbol must look like 600000.SH, 000001.SZ, or 430047.BJ")


def _validate_symbol_board(symbol: str, board: AShareBoard) -> None:
    _validate_symbol(symbol)
    exchange = symbol[-2:]
    expected_exchange = {
        AShareBoard.SSE_MAIN: "SH",
        AShareBoard.STAR: "SH",
        AShareBoard.SZSE_MAIN: "SZ",
        AShareBoard.CHINEXT: "SZ",
        AShareBoard.BSE: "BJ",
    }[board]
    if exchange != expected_exchange:
        raise ValueError("symbol exchange is inconsistent with board")


def _validate_batch_metadata(
    *,
    available_at: datetime,
    observed_at: datetime,
    source_id: str,
    source_revision: str,
    warnings: tuple[str, ...],
) -> None:
    if available_at.tzinfo is None or available_at.utcoffset() is None:
        raise ValueError("available_at must be timezone-aware")
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    if observed_at < available_at:
        raise ValueError("observed_at must not precede available_at")
    if not source_id.strip() or not source_revision.strip():
        raise ValueError("source_id and source_revision must not be empty")
    _validate_warnings(warnings)


def _validate_warnings(warnings: tuple[str, ...]) -> None:
    if any(not item.strip() for item in warnings):
        raise ValueError("warnings must contain non-empty text")
    if len(warnings) != len(set(warnings)):
        raise ValueError("warnings must be unique")


def is_finite_factor_value(value: float | None) -> bool:
    """Public helper for adapters that want the scorer's finite-value rule."""

    return value is not None and math.isfinite(value)
