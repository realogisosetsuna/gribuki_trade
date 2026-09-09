"""编排仅追加的事后推荐结果评估。

这里使用的日线柱是结果观测，绝不是策略输入。服务明确请求未复权价格并记录结果评估
时间；它不会声称可修订的供应商历史在原始推荐时点已经可知。
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
from gribuki_trade.storage.research.research_store import SQLiteResearchStore

EvaluationPhase = Literal["load", "fetch", "evaluate", "persist"]


class HistoricalOutcomeDataError(ValueError):
    """供应商违反未复权且有界的结果数据契约。"""


@dataclass(frozen=True, slots=True)
class RecommendationEvaluationFailure:
    """已脱敏的单项失败；刻意省略供应商消息。"""

    recommendation_id: str
    phase: EvaluationPhase
    error_type: str


@dataclass(frozen=True, slots=True)
class RecommendationEvaluationRun:
    """一次有限运行的结果与互斥状态计数。"""

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
    """使用未来日终行情柱独立评估已保留推荐。

    配置期限内的终态结果会被复用，使重复运行具有幂等性。待定结果会重试，因为后续
    交易日可能补足期限；未变化的待定载荷不会再次追加。每次供应商请求都使用
    ``PriceAdjustment.NONE``，且截止于评估日期之前，避免未结束的当日记录进入结果。
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
        """评估显式集合，或按标的/截止时点选择已保留记录。

        ``as_of`` 是精确时间戳过滤器。显式推荐与存储过滤器互斥。``data_through``
        是调用方允许的最近一个已完整收盘交易日，必须早于 ``evaluated_at``；
        保守默认值为前一自然日。
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
