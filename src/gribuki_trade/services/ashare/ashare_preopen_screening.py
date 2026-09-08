"""在下一 A 股交易日开盘前执行保守的三层筛选。

常规收盘筛选器会刻意为当时已可用的数据批次使用一个固定的 ``decision_at``。
实时盘前采集则不同：全市场页面和高成本历史因子只有在各自 HTTP 调用完成后才可知。
本服务明确保留这两个可见性边界，不会把任一批次的时间倒填。

结果只是研究观察列表的种子，始终标记为降级，不能单独授权盘中 PAPER 订单；
PAPER 日内运行器必须在开盘后使用当前交易日的新鲜监控快照与已完成分钟柱交叉确认。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal

from gribuki_trade.features.ashare_screening import (
    AShareFactorRanking,
    AShareScreeningConfig,
    HardFilterResult,
    RankedAShareCandidate,
    ScreeningDeferralReason,
    ScreeningDeferredRecord,
    hard_filter_ashare_universe,
    rank_ashare_factor_cross_section,
)
from gribuki_trade.ports.ashare_screening import (
    AShareBoard,
    AsyncAShareScreeningData,
    ScreeningHistoryPolicy,
    ScreeningSourceQuality,
)


class ASharePreopenScreeningError(RuntimeError):
    """盘前筛选中稳定的时点或覆盖率失败。"""

    def __init__(self, code: str) -> None:
        normalized = code.strip()
        if not normalized:
            raise ValueError("pre-open screening error code must not be empty")
        self.code = normalized
        super().__init__(f"A-share pre-open screening failed ({normalized})")


@dataclass(frozen=True, slots=True)
class ASharePreopenScreeningRun:
    """可审计的盘前漏斗输出，绝不是可执行信号。"""

    as_of: date
    requested_at: datetime
    decision_at: datetime
    strategy_version: str
    universe_source_id: str
    universe_source_revision: str
    factor_source_id: str | None
    factor_source_revision: str | None
    universe_count: int
    hard_filter_eligible_count: int
    factor_requested_count: int
    ranked_count: int
    hard_filter: HardFilterResult
    factor_budget_deferred: tuple[ScreeningDeferredRecord, ...]
    factor_ranking: AShareFactorRanking
    top_candidates: tuple[RankedAShareCandidate, ...]
    warnings: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in ("requested_at", "decision_at"):
            value = getattr(self, name)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{name} must be timezone-aware")
        if self.decision_at < self.requested_at:
            raise ValueError("decision_at must not precede requested_at")


class ASharePreopenScreeningService:
    """开盘前采集并排序经核验的上一交易日标的全集。"""

    def __init__(
        self,
        data_source: AsyncAShareScreeningData,
        *,
        config: AShareScreeningConfig | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._source = data_source
        self._config = config or AShareScreeningConfig(
            allowed_boards=(
                AShareBoard.SSE_MAIN,
                AShareBoard.SZSE_MAIN,
                AShareBoard.CHINEXT,
                AShareBoard.STAR,
            )
        )
        if AShareBoard.BSE in self._config.allowed_boards:
            raise ValueError("pre-open minute-tracked screen must exclude BSE")
        self._clock = clock

    @property
    def config(self) -> AShareScreeningConfig:
        return self._config

    async def run(
        self,
        *,
        as_of: date,
        requested_at: datetime,
    ) -> ASharePreopenScreeningRun:
        requested_at = _aware_utc(requested_at, "requested_at")
        universe = await self._source.fetch_universe_snapshot(
            as_of=as_of,
            known_at=requested_at,
        )
        universe_known_at = _aware_utc(self._clock(), "clock")
        _validate_batch(
            kind="UNIVERSE",
            expected_as_of=as_of,
            batch_as_of=universe.as_of,
            available_at=universe.available_at,
            observed_at=universe.observed_at,
            decision_at=universe_known_at,
        )
        if universe.quality is not ScreeningSourceQuality.DEGRADED:
            raise ASharePreopenScreeningError("UNIVERSE_NOT_MARKED_DEGRADED")

        hard_filter = hard_filter_ashare_universe(
            universe.records,
            config=self._config,
        )
        ordered = sorted(
            hard_filter.eligible,
            key=lambda item: (-_required_amount(item.session_amount_cny), item.symbol),
        )
        selected = tuple(ordered[: self._config.max_factor_candidates])
        deferred = tuple(
            ScreeningDeferredRecord(
                symbol=item.symbol,
                name=item.name,
                reason=ScreeningDeferralReason.FACTOR_BUDGET_DEFERRED,
            )
            for item in ordered[self._config.max_factor_candidates :]
        )
        if not selected:
            decision_at = _aware_utc(self._clock(), "clock")
            return ASharePreopenScreeningRun(
                as_of=as_of,
                requested_at=requested_at,
                decision_at=decision_at,
                strategy_version=self._config.strategy_version,
                universe_source_id=universe.source_id,
                universe_source_revision=universe.source_revision,
                factor_source_id=None,
                factor_source_revision=None,
                universe_count=len(universe.records),
                hard_filter_eligible_count=0,
                factor_requested_count=0,
                ranked_count=0,
                hard_filter=hard_filter,
                factor_budget_deferred=(),
                factor_ranking=AShareFactorRanking((), (), (), ()),
                top_candidates=(),
                warnings=tuple(
                    dict.fromkeys((*universe.warnings, "NO_HARD_FILTER_SURVIVORS"))
                ),
            )

        factors = await self._source.fetch_factor_snapshot(
            tuple(item.symbol for item in selected),
            as_of=as_of,
            known_at=universe_known_at,
        )
        decision_at = _aware_utc(self._clock(), "clock")
        _validate_batch(
            kind="FACTOR",
            expected_as_of=as_of,
            batch_as_of=factors.as_of,
            available_at=factors.available_at,
            observed_at=factors.observed_at,
            decision_at=decision_at,
        )
        if factors.quality is not ScreeningSourceQuality.DEGRADED:
            raise ASharePreopenScreeningError("FACTORS_NOT_MARKED_DEGRADED")
        if factors.history_policy is ScreeningHistoryPolicy.CURRENTLY_ADJUSTED:
            raise ASharePreopenScreeningError("UNSAFE_CURRENTLY_ADJUSTED_HISTORY")
        requested_symbols = {item.symbol for item in selected}
        supplied_symbols = tuple(item.symbol for item in factors.records)
        if len(supplied_symbols) != len(set(supplied_symbols)):
            raise ASharePreopenScreeningError("DUPLICATE_FACTOR_SYMBOL")
        if not set(supplied_symbols).issubset(requested_symbols):
            raise ASharePreopenScreeningError("UNREQUESTED_FACTOR_SYMBOL")

        ranking = rank_ashare_factor_cross_section(
            selected,
            factors.records,
            config=self._config,
            source_degraded=True,
        )
        top = ranking.ranked_candidates[: self._config.top_n]
        warnings = [
            *universe.warnings,
            *factors.warnings,
            "PREOPEN_RESEARCH_SEED_ONLY",
            "CURRENT_SESSION_CORROBORATION_REQUIRED_BEFORE_PAPER_ENTRY",
        ]
        warnings.extend(
            f"GLOBAL_FACTOR_UNAVAILABLE:{item.value}"
            for item in ranking.globally_unavailable_factors
        )
        if deferred:
            warnings.append("FACTOR_BUDGET_DEFERRED")
        if len(top) < self._config.top_n:
            warnings.append("TOP_N_SHORTFALL")
        return ASharePreopenScreeningRun(
            as_of=as_of,
            requested_at=requested_at,
            decision_at=decision_at,
            strategy_version=self._config.strategy_version,
            universe_source_id=universe.source_id,
            universe_source_revision=universe.source_revision,
            factor_source_id=factors.source_id,
            factor_source_revision=factors.source_revision,
            universe_count=len(universe.records),
            hard_filter_eligible_count=len(hard_filter.eligible),
            factor_requested_count=len(selected),
            ranked_count=len(ranking.ranked_candidates),
            hard_filter=hard_filter,
            factor_budget_deferred=deferred,
            factor_ranking=ranking,
            top_candidates=top,
            warnings=tuple(dict.fromkeys(warnings)),
        )


def _validate_batch(
    *,
    kind: str,
    expected_as_of: date,
    batch_as_of: date,
    available_at: datetime,
    observed_at: datetime,
    decision_at: datetime,
) -> None:
    if batch_as_of != expected_as_of:
        raise ASharePreopenScreeningError(f"{kind}_AS_OF_MISMATCH")
    if available_at > decision_at:
        raise ASharePreopenScreeningError(f"{kind}_AVAILABLE_IN_FUTURE")
    if observed_at < available_at:
        raise ASharePreopenScreeningError(f"{kind}_OBSERVED_BEFORE_AVAILABLE")


def _required_amount(value: Decimal | None) -> int:
    if value is None:
        raise ValueError("hard-filter survivor must have a session amount")
    return int(value)


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)
