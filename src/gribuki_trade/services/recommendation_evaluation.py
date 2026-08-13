"""Orchestrate append-only, ex-post recommendation outcome evaluation.

The daily bars consumed here are outcome observations, never strategy inputs.
The service explicitly requests unadjusted prices and records when an outcome
was evaluated; it does not claim that a revisable provider history was known
at the original recommendation time.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Literal

from gribuki_trade.backtest.recommendation_outcomes import (
    OutcomeStatus,
    RecommendationEvaluationConfig,
    RecommendationOutcome,
    evaluate_recommendation,
)
from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.domain.recommendations import (
    RecommendationDecision,
    ResearchRecommendation,
)
from gribuki_trade.ports.market_data import AsyncHistoricalDailyData
from gribuki_trade.storage.research_store import SQLiteResearchStore

EvaluationPhase = Literal["load", "fetch", "evaluate", "persist"]


class HistoricalOutcomeDataError(ValueError):
    """A provider violated the unadjusted, bounded outcome-data contract."""


@dataclass(frozen=True, slots=True)
class RecommendationEvaluationFailure:
    """Sanitized per-item failure; provider messages are deliberately omitted."""

    recommendation_id: str
    phase: EvaluationPhase
    error_type: str


@dataclass(frozen=True, slots=True)
class RecommendationEvaluationRun:
    """Results and mutually exclusive status counts for one finite run."""

    selected: int
    pending: int
    evaluated: int
    unevaluable: int
    errors: int
    observations_written: int
    observations_reused: int
    outcomes: tuple[RecommendationOutcome, ...]
    failures: tuple[RecommendationEvaluationFailure, ...]

    def __post_init__(self) -> None:
        if self.pending + self.evaluated + self.unevaluable + self.errors != self.selected:
            raise ValueError("evaluation status counts must cover all selected items")


class RecommendationEvaluationService:
    """Evaluate retained recommendations independently against future EOD bars.

    A terminal outcome for the configured horizon is reused, making repeated
    runs idempotent.  A pending outcome is retried because later sessions may
    complete its horizon; an unchanged pending payload is not appended again.
    Every provider request uses ``PriceAdjustment.NONE`` and ends before the
    evaluation date so an unfinished current-day row cannot enter the result.
    """

    def __init__(
        self,
        store: SQLiteResearchStore,
        historical_data: AsyncHistoricalDailyData,
        *,
        config: RecommendationEvaluationConfig | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._store = store
        self._historical_data = historical_data
        self._config = config or RecommendationEvaluationConfig()
        self._clock = clock

    async def run_once(
        self,
        recommendations: Sequence[ResearchRecommendation] | None = None,
        *,
        symbol: str | None = None,
        as_of: datetime | None = None,
        limit: int = 200,
        evaluated_at: datetime | None = None,
        data_through: date | None = None,
    ) -> RecommendationEvaluationRun:
        """Evaluate an explicit set, or select retained records by symbol/as-of.

        ``as_of`` is an exact timestamp filter.  Explicit recommendations and
        store filters are mutually exclusive.  ``data_through`` is the latest
        fully closed daily session that the caller permits; it must precede
        ``evaluated_at``.  The conservative default is the previous calendar
        day.
        """

        observation_time = evaluated_at or self._clock()
        _require_aware(observation_time, "evaluated_at")
        if limit < 1:
            raise ValueError("limit must be positive")
        if as_of is not None:
            _require_aware(as_of, "as_of")
        if recommendations is not None and (symbol is not None or as_of is not None):
            raise ValueError(
                "explicit recommendations cannot be combined with symbol/as_of filters"
            )

        last_closed_date = data_through or observation_time.date() - timedelta(days=1)
        if last_closed_date >= observation_time.date():
            raise ValueError("data_through must precede evaluated_at's calendar date")

        selected = self._select(
            recommendations,
            symbol=symbol,
            as_of=as_of,
            limit=limit,
        )
        outcomes: list[RecommendationOutcome] = []
        failures: list[RecommendationEvaluationFailure] = []
        written = 0
        reused = 0

        for recommendation in selected:
            phase: EvaluationPhase = "load"
            try:
                _require_aware(recommendation.as_of, "recommendation as_of")
                prior = self._matching_prior_outcome(recommendation)
                if prior is not None and prior.status is not OutcomeStatus.PENDING:
                    outcomes.append(prior)
                    reused += 1
                    continue

                if recommendation.decision in {
                    RecommendationDecision.WATCH,
                    RecommendationDecision.ABSTAIN,
                }:
                    future_bars: tuple[DailyBar, ...] = ()
                else:
                    phase = "fetch"
                    future_bars = await self._future_bars(
                        recommendation,
                        data_through=last_closed_date,
                    )
                phase = "evaluate"
                outcome = evaluate_recommendation(
                    recommendation,
                    future_bars,
                    config=self._config,
                )

                phase = "persist"
                if prior is not None and prior == outcome:
                    reused += 1
                elif self._store.append_outcome(
                    outcome,
                    evaluated_at=observation_time,
                ):
                    written += 1
                else:
                    reused += 1
                outcomes.append(outcome)
            except Exception as exc:
                failures.append(
                    RecommendationEvaluationFailure(
                        recommendation_id=recommendation.recommendation_id,
                        phase=phase,
                        error_type=type(exc).__name__,
                    )
                )

        pending = sum(item.status is OutcomeStatus.PENDING for item in outcomes)
        evaluated = sum(item.status is OutcomeStatus.COMPLETE for item in outcomes)
        unevaluable = sum(item.status is OutcomeStatus.UNEVALUABLE for item in outcomes)
        return RecommendationEvaluationRun(
            selected=len(selected),
            pending=pending,
            evaluated=evaluated,
            unevaluable=unevaluable,
            errors=len(failures),
            observations_written=written,
            observations_reused=reused,
            outcomes=tuple(outcomes),
            failures=tuple(failures),
        )

    def _select(
        self,
        recommendations: Sequence[ResearchRecommendation] | None,
        *,
        symbol: str | None,
        as_of: datetime | None,
        limit: int,
    ) -> tuple[ResearchRecommendation, ...]:
        if recommendations is None:
            resolved_symbol = None if symbol is None else symbol.strip().upper()
            candidates = self._store.latest_recommendations(
                limit=limit,
                symbol=resolved_symbol,
            )
            if as_of is not None:
                candidates = tuple(item for item in candidates if item.as_of == as_of)
            return candidates

        candidates = tuple(recommendations)
        identifiers: dict[str, ResearchRecommendation] = {}
        for item in candidates:
            existing = identifiers.get(item.recommendation_id)
            if existing is not None and existing != item:
                raise ValueError("recommendation ID was supplied with conflicting content")
            identifiers[item.recommendation_id] = item
        return tuple(identifiers.values())

    def _matching_prior_outcome(
        self,
        recommendation: ResearchRecommendation,
    ) -> RecommendationOutcome | None:
        outcomes = self._store.latest_outcomes(recommendation.recommendation_id)
        return next(
            (
                item
                for item in outcomes
                if item.horizon_sessions == self._config.horizon_sessions
            ),
            None,
        )

    async def _future_bars(
        self,
        recommendation: ResearchRecommendation,
        *,
        data_through: date,
    ) -> tuple[DailyBar, ...]:
        start = recommendation.as_of.date() + timedelta(days=1)
        if start > data_through:
            return ()
        bars = tuple(
            await self._historical_data.fetch_daily_bars_async(
                recommendation.symbol,
                start,
                data_through,
                adjustment=PriceAdjustment.NONE,
            )
        )
        _validate_provider_bars(
            bars,
            symbol=recommendation.symbol,
            start=start,
            end=data_through,
        )
        return bars


def _validate_provider_bars(
    bars: tuple[DailyBar, ...],
    *,
    symbol: str,
    start: date,
    end: date,
) -> None:
    if any(item.symbol != symbol for item in bars):
        raise HistoricalOutcomeDataError("historical provider returned another symbol")
    if any(item.trade_date < start or item.trade_date > end for item in bars):
        raise HistoricalOutcomeDataError("historical provider returned an out-of-window row")
    if any(item.adjustment is not PriceAdjustment.NONE for item in bars):
        raise HistoricalOutcomeDataError("outcome history must be unadjusted")
    dates = tuple(item.trade_date for item in bars)
    if dates != tuple(sorted(dates)) or len(dates) != len(set(dates)):
        raise HistoricalOutcomeDataError(
            "historical provider rows must be strictly ordered and unique"
        )


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
