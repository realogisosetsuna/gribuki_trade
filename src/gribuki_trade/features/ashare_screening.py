"""确定性的 A 股硬过滤与横截面排序。

本模块刻意保持纯粹：不依赖供应商、时钟、LLM、账户或券商。缺失因子在审计轨迹中
保持 ``None``，绝不会获得虚构的中位百分位或中性评分。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from gribuki_trade.ports.ashare_screening import (
    AShareBoard,
    AShareFactorRecord,
    AShareUniverseRecord,
    ScreeningFactorId,
)


class ScreeningFactorDirection(StrEnum):
    HIGHER_IS_BETTER = "HIGHER_IS_BETTER"
    LOWER_IS_BETTER = "LOWER_IS_BETTER"


class HardFilterReason(StrEnum):
    UNSUPPORTED_BOARD = "UNSUPPORTED_BOARD"
    UNKNOWN_TRADABILITY = "UNKNOWN_TRADABILITY"
    NOT_TRADABLE = "NOT_TRADABLE"
    UNKNOWN_ST_STATUS = "UNKNOWN_ST_STATUS"
    ST_SECURITY = "ST_SECURITY"
    UNKNOWN_SUSPENSION_STATUS = "UNKNOWN_SUSPENSION_STATUS"
    SUSPENDED = "SUSPENDED"
    MISSING_LISTING_AGE = "MISSING_LISTING_AGE"
    INSUFFICIENT_LISTING_AGE = "INSUFFICIENT_LISTING_AGE"
    MISSING_PRICE = "MISSING_PRICE"
    INVALID_PRICE = "INVALID_PRICE"
    PRICE_BELOW_MINIMUM = "PRICE_BELOW_MINIMUM"
    PRICE_ABOVE_MAXIMUM = "PRICE_ABOVE_MAXIMUM"
    MISSING_SESSION_AMOUNT = "MISSING_SESSION_AMOUNT"
    INVALID_SESSION_AMOUNT = "INVALID_SESSION_AMOUNT"
    LOW_SESSION_AMOUNT = "LOW_SESSION_AMOUNT"
    MISSING_MARKET_CAP = "MISSING_MARKET_CAP"
    INVALID_MARKET_CAP = "INVALID_MARKET_CAP"
    LOW_MARKET_CAP = "LOW_MARKET_CAP"


class FactorObservationStatus(StrEnum):
    AVAILABLE = "AVAILABLE"
    MISSING = "MISSING"
    INVALID = "INVALID"
    CROSS_SECTION_UNAVAILABLE = "CROSS_SECTION_UNAVAILABLE"


class ScreeningCandidateDataStatus(StrEnum):
    """已评分候选标的的完整度，而非交易建议。"""

    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"
    INSUFFICIENT = "INSUFFICIENT"


class FactorEligibilityReason(StrEnum):
    """第二层历史资格检查失败。"""

    FACTOR_RECORD_MISSING = "FACTOR_RECORD_MISSING"
    MISSING_AVERAGE_AMOUNT_20 = "MISSING_AVERAGE_AMOUNT_20"
    INVALID_AVERAGE_AMOUNT_20 = "INVALID_AVERAGE_AMOUNT_20"
    LOW_AVERAGE_AMOUNT_20 = "LOW_AVERAGE_AMOUNT_20"


class ScreeningDeferralReason(StrEnum):
    """第一层通过者未送往高成本因子增强的原因。"""

    FACTOR_BUDGET_DEFERRED = "FACTOR_BUDGET_DEFERRED"


@dataclass(frozen=True, slots=True)
class ScreeningFactorSpec:
    factor_id: ScreeningFactorId
    weight: float
    direction: ScreeningFactorDirection

    def __post_init__(self) -> None:
        if not math.isfinite(self.weight) or self.weight <= 0:
            raise ValueError("factor weight must be finite and positive")


DEFAULT_FACTOR_SPECS = (
    ScreeningFactorSpec(
        ScreeningFactorId.MOMENTUM_20,
        0.10,
        ScreeningFactorDirection.HIGHER_IS_BETTER,
    ),
    ScreeningFactorSpec(
        ScreeningFactorId.MOMENTUM_60,
        0.15,
        ScreeningFactorDirection.HIGHER_IS_BETTER,
    ),
    ScreeningFactorSpec(
        ScreeningFactorId.MOMENTUM_120_SKIP_5,
        0.20,
        ScreeningFactorDirection.HIGHER_IS_BETTER,
    ),
    ScreeningFactorSpec(
        ScreeningFactorId.TREND_MA20_OVER_MA60,
        0.15,
        ScreeningFactorDirection.HIGHER_IS_BETTER,
    ),
    ScreeningFactorSpec(
        ScreeningFactorId.BREAKOUT_20_POSITION,
        0.10,
        ScreeningFactorDirection.HIGHER_IS_BETTER,
    ),
    ScreeningFactorSpec(
        ScreeningFactorId.VOLUME_RATIO_20,
        0.10,
        ScreeningFactorDirection.HIGHER_IS_BETTER,
    ),
    ScreeningFactorSpec(
        ScreeningFactorId.ANNUALIZED_VOLATILITY_60,
        0.08,
        ScreeningFactorDirection.LOWER_IS_BETTER,
    ),
    ScreeningFactorSpec(
        ScreeningFactorId.MAX_DRAWDOWN_60_MAGNITUDE,
        0.07,
        ScreeningFactorDirection.LOWER_IS_BETTER,
    ),
    ScreeningFactorSpec(
        ScreeningFactorId.AMIHUD_ILLIQUIDITY_20,
        0.05,
        ScreeningFactorDirection.LOWER_IS_BETTER,
    ),
)


@dataclass(frozen=True, slots=True)
class AShareScreeningConfig:
    """第一版全市场漏斗的透明默认值。"""

    min_listing_days: int = 250
    strict_listing_age: bool = True
    min_session_amount_cny: Decimal = Decimal("20000000")
    min_average_amount_20_cny: Decimal = Decimal("50000000")
    min_market_cap_cny: Decimal | None = Decimal("2000000000")
    min_price: Decimal = Decimal("1")
    max_price: Decimal | None = None
    allowed_boards: tuple[AShareBoard, ...] = tuple(AShareBoard)
    lower_winsor_quantile: float = 0.025
    upper_winsor_quantile: float = 0.975
    min_cross_section_observations: int = 20
    min_factor_weight_coverage: float = 0.80
    max_factor_candidates: int = 300
    top_n: int = 30
    factor_specs: tuple[ScreeningFactorSpec, ...] = DEFAULT_FACTOR_SPECS
    strategy_version: str = "ashare-cross-section@1"

    def __post_init__(self) -> None:
        if self.min_listing_days < 0:
            raise ValueError("min_listing_days must not be negative")
        if not self.min_session_amount_cny.is_finite() or self.min_session_amount_cny < 0:
            raise ValueError("minimum session amount must be finite and non-negative")
        if (
            not self.min_average_amount_20_cny.is_finite()
            or self.min_average_amount_20_cny < 0
        ):
            raise ValueError("minimum average amount must be finite and non-negative")
        if self.min_market_cap_cny is not None and (
            not self.min_market_cap_cny.is_finite() or self.min_market_cap_cny < 0
        ):
            raise ValueError("minimum market cap must be finite and non-negative")
        if not self.min_price.is_finite() or self.min_price <= 0:
            raise ValueError("min_price must be finite and positive")
        if self.max_price is not None and (
            not self.max_price.is_finite() or self.max_price < self.min_price
        ):
            raise ValueError("max_price must be finite and at least min_price")
        if not self.allowed_boards or len(self.allowed_boards) != len(
            set(self.allowed_boards)
        ):
            raise ValueError("allowed_boards must be non-empty and unique")
        if not 0 <= self.lower_winsor_quantile < self.upper_winsor_quantile <= 1:
            raise ValueError("winsor quantiles must satisfy 0 <= lower < upper <= 1")
        if self.min_cross_section_observations < 2:
            raise ValueError("min_cross_section_observations must be at least two")
        if not 0 < self.min_factor_weight_coverage <= 1:
            raise ValueError("min_factor_weight_coverage must be in (0, 1]")
        if self.max_factor_candidates <= 0:
            raise ValueError("max_factor_candidates must be positive")
        if self.top_n <= 0:
            raise ValueError("top_n must be positive")
        if not self.factor_specs:
            raise ValueError("at least one factor is required")
        factor_ids = tuple(item.factor_id for item in self.factor_specs)
        if len(factor_ids) != len(set(factor_ids)):
            raise ValueError("factor IDs must be unique")
        if not math.isclose(
            math.fsum(item.weight for item in self.factor_specs),
            1.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError("factor weights must sum to one")
        if not self.strategy_version.strip():
            raise ValueError("strategy_version must not be empty")


@dataclass(frozen=True, slots=True)
class HardFilterExclusion:
    symbol: str
    name: str
    reasons: tuple[HardFilterReason, ...]


@dataclass(frozen=True, slots=True)
class HardFilterResult:
    eligible: tuple[AShareUniverseRecord, ...]
    excluded: tuple[HardFilterExclusion, ...]


@dataclass(frozen=True, slots=True)
class FactorEligibilityExclusion:
    symbol: str
    name: str
    reasons: tuple[FactorEligibilityReason, ...]


@dataclass(frozen=True, slots=True)
class ScreeningDeferredRecord:
    symbol: str
    name: str
    reason: ScreeningDeferralReason


@dataclass(frozen=True, slots=True)
class ScreeningFactorContribution:
    """一个标的-因子组合的完整审计行。"""

    factor_id: ScreeningFactorId
    status: FactorObservationStatus
    raw_value: float | None
    winsorized_value: float | None
    percentile_rank: float | None
    directional_score: float | None
    configured_weight: float
    contribution: float | None
    cross_section_observations: int


@dataclass(frozen=True, slots=True)
class RankedAShareCandidate:
    """适合后续深度研究阶段的横截面结果。"""

    symbol: str
    name: str
    board: AShareBoard
    industry: str | None
    rank: int | None
    composite_score: float | None
    factor_weight_coverage: float
    data_status: ScreeningCandidateDataStatus
    degradation_reasons: tuple[str, ...]
    factor_contributions: tuple[ScreeningFactorContribution, ...]


@dataclass(frozen=True, slots=True)
class AShareFactorRanking:
    ranked_candidates: tuple[RankedAShareCandidate, ...]
    insufficient_candidates: tuple[RankedAShareCandidate, ...]
    factor_eligibility_exclusions: tuple[FactorEligibilityExclusion, ...]
    globally_unavailable_factors: tuple[ScreeningFactorId, ...]


def hard_filter_ashare_universe(
    records: tuple[AShareUniverseRecord, ...],
    *,
    config: AShareScreeningConfig | None = None,
) -> HardFilterResult:
    """资格字段未知时按失败关闭处理，并保留每项原因。"""

    resolved = config or AShareScreeningConfig()
    symbols = tuple(item.symbol for item in records)
    if len(symbols) != len(set(symbols)):
        raise ValueError("universe records must contain unique symbols")

    eligible: list[AShareUniverseRecord] = []
    excluded: list[HardFilterExclusion] = []
    for record in sorted(records, key=lambda item: item.symbol):
        reasons = _hard_filter_reasons(record, resolved)
        if reasons:
            excluded.append(
                HardFilterExclusion(
                    symbol=record.symbol,
                    name=record.name,
                    reasons=tuple(reasons),
                )
            )
        else:
            eligible.append(record)
    return HardFilterResult(eligible=tuple(eligible), excluded=tuple(excluded))


def rank_ashare_factor_cross_section(
    eligible: tuple[AShareUniverseRecord, ...],
    factor_records: tuple[AShareFactorRecord, ...],
    *,
    config: AShareScreeningConfig | None = None,
    source_degraded: bool = False,
) -> AShareFactorRanking:
    """对因子执行缩尾、百分位排序、方向调整与聚合。

    每个可用因子获得 ``[-1, 1]`` 内的带符号百分位分数，配置贡献为
    ``score * weight``。缺失因子的权重不会计入综合分，而非赋予百分位 ``0.5``；
    覆盖率与稳定降级原因会明确体现该惩罚。
    """

    resolved = config or AShareScreeningConfig()
    eligible_symbols = tuple(item.symbol for item in eligible)
    if len(eligible_symbols) != len(set(eligible_symbols)):
        raise ValueError("eligible records must contain unique symbols")

    factor_map = _index_factor_records(factor_records)
    factor_eligible: list[AShareUniverseRecord] = []
    eligibility_exclusions: list[FactorEligibilityExclusion] = []
    for universe_record in sorted(eligible, key=lambda item: item.symbol):
        record = factor_map.get(universe_record.symbol)
        eligibility_reasons = _factor_eligibility_reasons(record, resolved)
        if eligibility_reasons:
            eligibility_exclusions.append(
                FactorEligibilityExclusion(
                    symbol=universe_record.symbol,
                    name=universe_record.name,
                    reasons=tuple(eligibility_reasons),
                )
            )
        else:
            factor_eligible.append(universe_record)

    eligible = tuple(factor_eligible)
    eligible_symbols = tuple(item.symbol for item in eligible)
    raw_by_factor: dict[ScreeningFactorId, dict[str, float]] = {}
    invalid_pairs: set[tuple[str, ScreeningFactorId]] = set()
    for spec in resolved.factor_specs:
        values: dict[str, float] = {}
        for symbol in eligible_symbols:
            record = factor_map.get(symbol)
            raw = None if record is None else record.value_for(spec.factor_id)
            if raw is None:
                continue
            if not math.isfinite(raw):
                invalid_pairs.add((symbol, spec.factor_id))
                continue
            values[symbol] = float(raw)
        raw_by_factor[spec.factor_id] = values

    transformed: dict[
        ScreeningFactorId,
        dict[str, tuple[float, float, float, float]],
    ] = {}
    globally_unavailable: list[ScreeningFactorId] = []
    for spec in resolved.factor_specs:
        observed = raw_by_factor[spec.factor_id]
        if len(observed) < resolved.min_cross_section_observations:
            globally_unavailable.append(spec.factor_id)
            transformed[spec.factor_id] = {}
            continue
        lower = _linear_quantile(
            tuple(observed.values()), resolved.lower_winsor_quantile
        )
        upper = _linear_quantile(
            tuple(observed.values()), resolved.upper_winsor_quantile
        )
        winsorized = {
            symbol: min(max(value, lower), upper)
            for symbol, value in observed.items()
        }
        ordered_values = tuple(winsorized.values())
        factor_results: dict[str, tuple[float, float, float, float]] = {}
        for symbol, raw in observed.items():
            clipped = winsorized[symbol]
            percentile = _tie_aware_percentile_rank(clipped, ordered_values)
            directional_percentile = (
                percentile
                if spec.direction is ScreeningFactorDirection.HIGHER_IS_BETTER
                else 1.0 - percentile
            )
            score = directional_percentile * 2.0 - 1.0
            factor_results[symbol] = (
                raw,
                clipped,
                directional_percentile,
                score,
            )
        transformed[spec.factor_id] = factor_results

    rankable: list[RankedAShareCandidate] = []
    insufficient: list[RankedAShareCandidate] = []
    for universe_record in sorted(eligible, key=lambda item: item.symbol):
        symbol = universe_record.symbol
        factor_record = factor_map.get(symbol)
        reasons: list[str] = []
        if factor_record is None:
            reasons.append("FACTOR_RECORD_MISSING")
        elif factor_record.warnings:
            reasons.append("FACTOR_RECORD_WARNING")
        if source_degraded:
            reasons.append("SOURCE_DEGRADED")
        if universe_record.industry is None:
            reasons.append("MISSING_INDUSTRY_METADATA")

        contributions: list[ScreeningFactorContribution] = []
        available_weight = 0.0
        numeric_contributions: list[float] = []
        for spec in resolved.factor_specs:
            raw = raw_by_factor[spec.factor_id].get(symbol)
            observation_count = len(raw_by_factor[spec.factor_id])
            result = transformed[spec.factor_id].get(symbol)
            if (symbol, spec.factor_id) in invalid_pairs:
                observation_status = FactorObservationStatus.INVALID
                reasons.append(f"INVALID_FACTOR:{spec.factor_id.value}")
                contribution = ScreeningFactorContribution(
                    factor_id=spec.factor_id,
                    status=observation_status,
                    raw_value=None,
                    winsorized_value=None,
                    percentile_rank=None,
                    directional_score=None,
                    configured_weight=spec.weight,
                    contribution=None,
                    cross_section_observations=observation_count,
                )
            elif spec.factor_id in globally_unavailable:
                observation_status = FactorObservationStatus.CROSS_SECTION_UNAVAILABLE
                reasons.append(
                    f"INSUFFICIENT_CROSS_SECTION:{spec.factor_id.value}"
                )
                contribution = ScreeningFactorContribution(
                    factor_id=spec.factor_id,
                    status=observation_status,
                    raw_value=raw,
                    winsorized_value=None,
                    percentile_rank=None,
                    directional_score=None,
                    configured_weight=spec.weight,
                    contribution=None,
                    cross_section_observations=observation_count,
                )
            elif result is None:
                observation_status = FactorObservationStatus.MISSING
                reasons.append(f"MISSING_FACTOR:{spec.factor_id.value}")
                contribution = ScreeningFactorContribution(
                    factor_id=spec.factor_id,
                    status=observation_status,
                    raw_value=None,
                    winsorized_value=None,
                    percentile_rank=None,
                    directional_score=None,
                    configured_weight=spec.weight,
                    contribution=None,
                    cross_section_observations=observation_count,
                )
            else:
                raw_value, clipped, percentile, score = result
                value_contribution = score * spec.weight
                available_weight += spec.weight
                numeric_contributions.append(value_contribution)
                contribution = ScreeningFactorContribution(
                    factor_id=spec.factor_id,
                    status=FactorObservationStatus.AVAILABLE,
                    raw_value=raw_value,
                    winsorized_value=clipped,
                    percentile_rank=percentile,
                    directional_score=score,
                    configured_weight=spec.weight,
                    contribution=value_contribution,
                    cross_section_observations=observation_count,
                )
            contributions.append(contribution)

        coverage = min(1.0, max(0.0, available_weight))
        enough_data = coverage + 1e-12 >= resolved.min_factor_weight_coverage
        if not enough_data:
            reasons.append("INSUFFICIENT_FACTOR_WEIGHT_COVERAGE")
            candidate = RankedAShareCandidate(
                symbol=symbol,
                name=universe_record.name,
                board=universe_record.board,
                industry=universe_record.industry,
                rank=None,
                composite_score=None,
                factor_weight_coverage=coverage,
                data_status=ScreeningCandidateDataStatus.INSUFFICIENT,
                degradation_reasons=tuple(dict.fromkeys(reasons)),
                factor_contributions=tuple(contributions),
            )
            insufficient.append(candidate)
            continue

        composite_score = math.fsum(numeric_contributions)
        candidate_status = (
            ScreeningCandidateDataStatus.COMPLETE
            if math.isclose(coverage, 1.0, rel_tol=0.0, abs_tol=1e-12)
            and not reasons
            else ScreeningCandidateDataStatus.DEGRADED
        )
        rankable.append(
            RankedAShareCandidate(
                symbol=symbol,
                name=universe_record.name,
                board=universe_record.board,
                industry=universe_record.industry,
                rank=None,
                composite_score=composite_score,
                factor_weight_coverage=coverage,
                data_status=candidate_status,
                degradation_reasons=tuple(dict.fromkeys(reasons)),
                factor_contributions=tuple(contributions),
            )
        )

    ordered = sorted(
        rankable,
        key=lambda item: (
            -_required_score(item),
            -item.factor_weight_coverage,
            item.data_status is ScreeningCandidateDataStatus.DEGRADED,
            item.symbol,
        ),
    )
    ranked = tuple(_with_rank(item, rank) for rank, item in enumerate(ordered, start=1))
    return AShareFactorRanking(
        ranked_candidates=ranked,
        insufficient_candidates=tuple(sorted(insufficient, key=lambda item: item.symbol)),
        factor_eligibility_exclusions=tuple(eligibility_exclusions),
        globally_unavailable_factors=tuple(globally_unavailable),
    )


def _hard_filter_reasons(
    record: AShareUniverseRecord,
    config: AShareScreeningConfig,
) -> list[HardFilterReason]:
    reasons: list[HardFilterReason] = []
    if record.board not in config.allowed_boards:
        reasons.append(HardFilterReason.UNSUPPORTED_BOARD)
    if record.is_tradable is None:
        reasons.append(HardFilterReason.UNKNOWN_TRADABILITY)
    elif not record.is_tradable:
        reasons.append(HardFilterReason.NOT_TRADABLE)
    if record.is_st is None:
        reasons.append(HardFilterReason.UNKNOWN_ST_STATUS)
    elif record.is_st:
        reasons.append(HardFilterReason.ST_SECURITY)
    if record.is_suspended is None:
        reasons.append(HardFilterReason.UNKNOWN_SUSPENSION_STATUS)
    elif record.is_suspended:
        reasons.append(HardFilterReason.SUSPENDED)
    if record.listing_days is None:
        if config.strict_listing_age:
            reasons.append(HardFilterReason.MISSING_LISTING_AGE)
    elif record.listing_days < config.min_listing_days:
        reasons.append(HardFilterReason.INSUFFICIENT_LISTING_AGE)

    if record.last_price is None:
        reasons.append(HardFilterReason.MISSING_PRICE)
    elif record.last_price <= 0:
        reasons.append(HardFilterReason.INVALID_PRICE)
    elif record.last_price < config.min_price:
        reasons.append(HardFilterReason.PRICE_BELOW_MINIMUM)
    elif config.max_price is not None and record.last_price > config.max_price:
        reasons.append(HardFilterReason.PRICE_ABOVE_MAXIMUM)

    if record.session_amount_cny is None:
        reasons.append(HardFilterReason.MISSING_SESSION_AMOUNT)
    elif record.session_amount_cny < 0:
        reasons.append(HardFilterReason.INVALID_SESSION_AMOUNT)
    elif record.session_amount_cny < config.min_session_amount_cny:
        reasons.append(HardFilterReason.LOW_SESSION_AMOUNT)

    if config.min_market_cap_cny is not None:
        if record.market_cap_cny is None:
            reasons.append(HardFilterReason.MISSING_MARKET_CAP)
        elif record.market_cap_cny < 0:
            reasons.append(HardFilterReason.INVALID_MARKET_CAP)
        elif record.market_cap_cny < config.min_market_cap_cny:
            reasons.append(HardFilterReason.LOW_MARKET_CAP)
    return reasons


def _factor_eligibility_reasons(
    record: AShareFactorRecord | None,
    config: AShareScreeningConfig,
) -> list[FactorEligibilityReason]:
    if record is None:
        return [FactorEligibilityReason.FACTOR_RECORD_MISSING]
    average_amount = record.value_for(ScreeningFactorId.AVERAGE_AMOUNT_20_CNY)
    if average_amount is None:
        return [FactorEligibilityReason.MISSING_AVERAGE_AMOUNT_20]
    if not math.isfinite(average_amount) or average_amount < 0:
        return [FactorEligibilityReason.INVALID_AVERAGE_AMOUNT_20]
    if average_amount < float(config.min_average_amount_20_cny):
        return [FactorEligibilityReason.LOW_AVERAGE_AMOUNT_20]
    return []


def _index_factor_records(
    records: tuple[AShareFactorRecord, ...],
) -> dict[str, AShareFactorRecord]:
    indexed: dict[str, AShareFactorRecord] = {}
    for record in records:
        if record.symbol in indexed:
            raise ValueError("factor records must contain unique symbols")
        indexed[record.symbol] = record
    return indexed


def _linear_quantile(values: tuple[float, ...], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("quantile requires at least one value")
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return ordered[lower_index]
    fraction = position - lower_index
    return ordered[lower_index] + (
        ordered[upper_index] - ordered[lower_index]
    ) * fraction


def _tie_aware_percentile_rank(value: float, values: tuple[float, ...]) -> float:
    if len(values) == 1:
        return 0.5
    less = sum(item < value for item in values)
    equal = sum(item == value for item in values)
    return (less + (equal - 1) / 2.0) / (len(values) - 1)


def _required_score(candidate: RankedAShareCandidate) -> float:
    if candidate.composite_score is None:
        raise ValueError("rankable candidate must have a composite score")
    return candidate.composite_score


def _with_rank(candidate: RankedAShareCandidate, rank: int) -> RankedAShareCandidate:
    return RankedAShareCandidate(
        symbol=candidate.symbol,
        name=candidate.name,
        board=candidate.board,
        industry=candidate.industry,
        rank=rank,
        composite_score=candidate.composite_score,
        factor_weight_coverage=candidate.factor_weight_coverage,
        data_status=candidate.data_status,
        degradation_reasons=candidate.degradation_reasons,
        factor_contributions=candidate.factor_contributions,
    )
