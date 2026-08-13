"""Three-layer all-A-share screening orchestration.

The service performs no LLM analysis and has no broker dependency:

1. fetch a cheap, point-in-time full-market snapshot and hard-filter it;
2. fetch historical factors only for survivors and rank the cross-section;
3. expose the deterministic Top-N as inputs to a later deep-research service.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from zoneinfo import ZoneInfo

from gribuki_trade.features.ashare_screening import (
    AShareFactorRanking,
    AShareScreeningConfig,
    HardFilterResult,
    RankedAShareCandidate,
    ScreeningCandidateDataStatus,
    ScreeningDeferralReason,
    ScreeningDeferredRecord,
    hard_filter_ashare_universe,
    rank_ashare_factor_cross_section,
)
from gribuki_trade.ports.ashare_screening import (
    AShareFactorSnapshot,
    AShareUniverseRecord,
    AShareUniverseSnapshot,
    AsyncAShareScreeningData,
    ScreeningHistoryPolicy,
    ScreeningSourceQuality,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


class AShareScreeningRunStatus(StrEnum):
    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"
    NO_ELIGIBLE_UNIVERSE = "NO_ELIGIBLE_UNIVERSE"
    NO_RANKABLE_CANDIDATES = "NO_RANKABLE_CANDIDATES"


class AShareScreeningPointInTimeError(RuntimeError):
    """The source batch cannot be used at the requested decision time."""

    def __init__(self, code: str) -> None:
        super().__init__(f"A-share screening point-in-time check failed ({code})")
        self.code = code


@dataclass(frozen=True, slots=True)
class AShareScreeningRun:
    """Auditable funnel output; ``top_candidates`` are research inputs only."""

    as_of: date
    decision_at: datetime
    status: AShareScreeningRunStatus
    strategy_version: str
    universe_source_id: str
    universe_source_revision: str
    factor_source_id: str | None
    factor_source_revision: str | None
    feature_version: str | None
    universe_count: int
    hard_filter_eligible_count: int
    factor_requested_count: int
    eligible_count: int
    ranked_count: int
    hard_filter: HardFilterResult
    factor_budget_deferred: tuple[ScreeningDeferredRecord, ...]
    factor_ranking: AShareFactorRanking
    top_candidates: tuple[RankedAShareCandidate, ...]
    warnings: tuple[str, ...]


class AShareScreeningService:
    """Coordinate a bounded all-market screen without creating a trade."""

    def __init__(
        self,
        data_source: AsyncAShareScreeningData,
        *,
        config: AShareScreeningConfig | None = None,
    ) -> None:
        self._data_source = data_source
        self._config = config or AShareScreeningConfig()

    @property
    def config(self) -> AShareScreeningConfig:
        return self._config

    async def run(
        self,
        *,
        as_of: date,
        decision_at: datetime,
    ) -> AShareScreeningRun:
        """Run all three layers using only data knowable at ``decision_at``."""

        _validate_decision_time(as_of=as_of, decision_at=decision_at)
        universe = await self._data_source.fetch_universe_snapshot(
            as_of=as_of,
            known_at=decision_at,
        )
        _validate_universe_snapshot(
            universe,
            as_of=as_of,
            decision_at=decision_at,
        )
        hard_filter = hard_filter_ashare_universe(
            universe.records,
            config=self._config,
        )
        universe_warnings = list(universe.warnings)
        if universe.quality is ScreeningSourceQuality.DEGRADED:
            universe_warnings.append("UNIVERSE_SOURCE_DEGRADED")

        if not hard_filter.eligible:
            return AShareScreeningRun(
                as_of=as_of,
                decision_at=decision_at,
                status=AShareScreeningRunStatus.NO_ELIGIBLE_UNIVERSE,
                strategy_version=self._config.strategy_version,
                universe_source_id=universe.source_id,
                universe_source_revision=universe.source_revision,
                factor_source_id=None,
                factor_source_revision=None,
                feature_version=None,
                universe_count=len(universe.records),
                hard_filter_eligible_count=0,
                factor_requested_count=0,
                eligible_count=0,
                ranked_count=0,
                hard_filter=hard_filter,
                factor_budget_deferred=(),
                factor_ranking=AShareFactorRanking((), (), (), ()),
                top_candidates=(),
                warnings=tuple(dict.fromkeys(universe_warnings)),
            )

        factor_budget_ordered = sorted(
            hard_filter.eligible,
            key=lambda item: (-_required_session_amount(item), item.symbol),
        )
        factor_requested_records = tuple(
            factor_budget_ordered[: self._config.max_factor_candidates]
        )
        factor_budget_deferred = tuple(
            ScreeningDeferredRecord(
                symbol=item.symbol,
                name=item.name,
                reason=ScreeningDeferralReason.FACTOR_BUDGET_DEFERRED,
            )
            for item in factor_budget_ordered[self._config.max_factor_candidates :]
        )
        eligible_symbols = tuple(item.symbol for item in factor_requested_records)
        factor_snapshot = await self._data_source.fetch_factor_snapshot(
            eligible_symbols,
            as_of=as_of,
            known_at=decision_at,
        )
        _validate_factor_snapshot(
            factor_snapshot,
            requested_symbols=frozenset(eligible_symbols),
            as_of=as_of,
            decision_at=decision_at,
        )
        source_degraded = (
            universe.quality is ScreeningSourceQuality.DEGRADED
            or factor_snapshot.quality is ScreeningSourceQuality.DEGRADED
        )
        ranking = rank_ashare_factor_cross_section(
            factor_requested_records,
            factor_snapshot.records,
            config=self._config,
            source_degraded=source_degraded,
        )
        top_candidates = ranking.ranked_candidates[: self._config.top_n]

        warnings = universe_warnings + list(factor_snapshot.warnings)
        if factor_snapshot.quality is ScreeningSourceQuality.DEGRADED:
            warnings.append("FACTOR_SOURCE_DEGRADED")
        warnings.extend(
            f"GLOBAL_FACTOR_UNAVAILABLE:{factor_id.value}"
            for factor_id in ranking.globally_unavailable_factors
        )
        if len(top_candidates) < self._config.top_n:
            warnings.append("TOP_N_SHORTFALL")
        if factor_budget_deferred:
            warnings.append("FACTOR_BUDGET_DEFERRED")

        if not ranking.ranked_candidates:
            status = AShareScreeningRunStatus.NO_RANKABLE_CANDIDATES
        elif source_degraded or ranking.globally_unavailable_factors or any(
            item.data_status is not ScreeningCandidateDataStatus.COMPLETE
            for item in top_candidates
        ):
            status = AShareScreeningRunStatus.DEGRADED
        else:
            status = AShareScreeningRunStatus.COMPLETE

        return AShareScreeningRun(
            as_of=as_of,
            decision_at=decision_at,
            status=status,
            strategy_version=self._config.strategy_version,
            universe_source_id=universe.source_id,
            universe_source_revision=universe.source_revision,
            factor_source_id=factor_snapshot.source_id,
            factor_source_revision=factor_snapshot.source_revision,
            feature_version=factor_snapshot.feature_version,
            universe_count=len(universe.records),
            hard_filter_eligible_count=len(hard_filter.eligible),
            factor_requested_count=len(factor_requested_records),
            eligible_count=(
                len(factor_requested_records)
                - len(ranking.factor_eligibility_exclusions)
            ),
            ranked_count=len(ranking.ranked_candidates),
            hard_filter=hard_filter,
            factor_budget_deferred=factor_budget_deferred,
            factor_ranking=ranking,
            top_candidates=top_candidates,
            warnings=tuple(dict.fromkeys(warnings)),
        )


def _validate_decision_time(*, as_of: date, decision_at: datetime) -> None:
    if decision_at.tzinfo is None or decision_at.utcoffset() is None:
        raise ValueError("decision_at must be timezone-aware")
    if as_of > decision_at.astimezone(SHANGHAI).date():
        raise AShareScreeningPointInTimeError("FUTURE_AS_OF_DATE")


def _validate_universe_snapshot(
    snapshot: AShareUniverseSnapshot,
    *,
    as_of: date,
    decision_at: datetime,
) -> None:
    _validate_snapshot_clock(
        kind="UNIVERSE",
        snapshot_as_of=snapshot.as_of,
        available_at=snapshot.available_at,
        observed_at=snapshot.observed_at,
        requested_as_of=as_of,
        decision_at=decision_at,
    )


def _validate_factor_snapshot(
    snapshot: AShareFactorSnapshot,
    *,
    requested_symbols: frozenset[str],
    as_of: date,
    decision_at: datetime,
) -> None:
    _validate_snapshot_clock(
        kind="FACTOR",
        snapshot_as_of=snapshot.as_of,
        available_at=snapshot.available_at,
        observed_at=snapshot.observed_at,
        requested_as_of=as_of,
        decision_at=decision_at,
    )
    if snapshot.history_policy is ScreeningHistoryPolicy.CURRENTLY_ADJUSTED:
        raise AShareScreeningPointInTimeError("UNSAFE_CURRENTLY_ADJUSTED_HISTORY")
    returned_symbols = tuple(item.symbol for item in snapshot.records)
    if len(returned_symbols) != len(set(returned_symbols)):
        raise AShareScreeningPointInTimeError("DUPLICATE_FACTOR_SYMBOL")
    if not set(returned_symbols).issubset(requested_symbols):
        raise AShareScreeningPointInTimeError("UNREQUESTED_FACTOR_SYMBOL")


def _validate_snapshot_clock(
    *,
    kind: str,
    snapshot_as_of: date,
    available_at: datetime,
    observed_at: datetime,
    requested_as_of: date,
    decision_at: datetime,
) -> None:
    if snapshot_as_of != requested_as_of:
        raise AShareScreeningPointInTimeError(f"{kind}_AS_OF_MISMATCH")
    if available_at > decision_at:
        raise AShareScreeningPointInTimeError(f"{kind}_AVAILABLE_IN_FUTURE")
    if observed_at < available_at:
        raise AShareScreeningPointInTimeError(f"{kind}_OBSERVED_BEFORE_AVAILABLE")


def _required_session_amount(record: AShareUniverseRecord) -> int:
    """Return an exact sortable amount after the hard filter proved presence."""

    if record.session_amount_cny is None:
        raise ValueError("hard-filter survivor must have a session amount")
    return int(record.session_amount_cny)
