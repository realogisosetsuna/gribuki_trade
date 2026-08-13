"""Pure cross-sectional anomaly scoring for an intraday A-share snapshot."""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from gribuki_trade.ports.ashare_surveillance import AShareIntradayUniverseRecord


class IntradayAnomalyFactor(StrEnum):
    PRICE_STRENGTH = "PRICE_STRENGTH"
    SESSION_AMOUNT = "SESSION_AMOUNT"
    RANGE_POSITION = "RANGE_POSITION"
    OPEN_FOLLOW_THROUGH = "OPEN_FOLLOW_THROUGH"
    VOLUME_RATIO = "VOLUME_RATIO"
    TURNOVER_RATE = "TURNOVER_RATE"


class IntradayCandidateClass(StrEnum):
    MOMENTUM_EXPANSION = "MOMENTUM_EXPANSION"
    ACTIVE_STRENGTH = "ACTIVE_STRENGTH"
    OBSERVATION_ONLY = "OBSERVATION_ONLY"


class IntradayExclusionReason(StrEnum):
    UNKNOWN_ST_STATUS = "UNKNOWN_ST_STATUS"
    ST_SECURITY = "ST_SECURITY"
    UNKNOWN_SUSPENSION_STATUS = "UNKNOWN_SUSPENSION_STATUS"
    SUSPENDED = "SUSPENDED"
    MISSING_PRICE = "MISSING_PRICE"
    INVALID_PRICE = "INVALID_PRICE"
    MISSING_PREVIOUS_CLOSE = "MISSING_PREVIOUS_CLOSE"
    MISSING_CHANGE_PERCENT = "MISSING_CHANGE_PERCENT"
    MISSING_SESSION_AMOUNT = "MISSING_SESSION_AMOUNT"
    LOW_SESSION_AMOUNT = "LOW_SESSION_AMOUNT"
    INSUFFICIENT_FACTOR_COVERAGE = "INSUFFICIENT_FACTOR_COVERAGE"


@dataclass(frozen=True, slots=True)
class IntradayFactorSpec:
    factor_id: IntradayAnomalyFactor
    weight: float

    def __post_init__(self) -> None:
        if not math.isfinite(self.weight) or self.weight <= 0:
            raise ValueError("factor weight must be finite and positive")


DEFAULT_INTRADAY_FACTORS = (
    IntradayFactorSpec(IntradayAnomalyFactor.PRICE_STRENGTH, 0.25),
    IntradayFactorSpec(IntradayAnomalyFactor.SESSION_AMOUNT, 0.20),
    IntradayFactorSpec(IntradayAnomalyFactor.RANGE_POSITION, 0.15),
    IntradayFactorSpec(IntradayAnomalyFactor.OPEN_FOLLOW_THROUGH, 0.15),
    IntradayFactorSpec(IntradayAnomalyFactor.VOLUME_RATIO, 0.15),
    IntradayFactorSpec(IntradayAnomalyFactor.TURNOVER_RATE, 0.10),
)


@dataclass(frozen=True, slots=True)
class AShareIntradaySurveillanceConfig:
    min_price: Decimal = Decimal("1")
    min_session_amount_cny: Decimal = Decimal("2000000")
    min_cross_section_observations: int = 100
    min_factor_weight_coverage: float = 0.55
    candidate_score_threshold: float = 0.35
    momentum_score_threshold: float = 0.55
    top_n: int = 30
    factor_specs: tuple[IntradayFactorSpec, ...] = DEFAULT_INTRADAY_FACTORS
    strategy_version: str = "ashare-intraday-anomaly@1"

    def __post_init__(self) -> None:
        if not self.min_price.is_finite() or self.min_price <= 0:
            raise ValueError("min_price must be finite and positive")
        if (
            not self.min_session_amount_cny.is_finite()
            or self.min_session_amount_cny < 0
        ):
            raise ValueError("minimum session amount must be finite and non-negative")
        if self.min_cross_section_observations < 20:
            raise ValueError("minimum cross-section must be at least 20")
        if not 0 < self.min_factor_weight_coverage <= 1:
            raise ValueError("factor coverage must be in (0, 1]")
        if not -1 <= self.candidate_score_threshold <= 1:
            raise ValueError("candidate threshold must be in [-1, 1]")
        if not self.candidate_score_threshold <= self.momentum_score_threshold <= 1:
            raise ValueError("momentum threshold must be at least candidate threshold")
        if self.top_n < 1:
            raise ValueError("top_n must be positive")
        if not self.factor_specs:
            raise ValueError("at least one factor is required")
        identifiers = tuple(item.factor_id for item in self.factor_specs)
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("factor IDs must be unique")
        if not math.isclose(
            math.fsum(item.weight for item in self.factor_specs),
            1.0,
            rel_tol=0,
            abs_tol=1e-12,
        ):
            raise ValueError("factor weights must sum to one")
        if not self.strategy_version.strip():
            raise ValueError("strategy_version must not be empty")


@dataclass(frozen=True, slots=True)
class IntradayFactorContribution:
    factor_id: IntradayAnomalyFactor
    raw_value: float | None
    winsorized_value: float | None
    percentile_rank: float | None
    directional_score: float | None
    configured_weight: float
    contribution: float | None
    cross_section_observations: int


@dataclass(frozen=True, slots=True)
class IntradayCandidate:
    symbol: str
    name: str
    rank: int
    candidate_class: IntradayCandidateClass
    anomaly_score: float
    factor_weight_coverage: float
    last_price: Decimal
    change_percent: Decimal
    session_amount_cny: Decimal
    factors: tuple[IntradayFactorContribution, ...]
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class IntradayExcludedRecord:
    symbol: str
    name: str
    reasons: tuple[IntradayExclusionReason, ...]


@dataclass(frozen=True, slots=True)
class AShareIntradayRanking:
    candidates: tuple[IntradayCandidate, ...]
    excluded: tuple[IntradayExcludedRecord, ...]
    globally_unavailable_factors: tuple[IntradayAnomalyFactor, ...]
    eligible_count: int


def rank_intraday_anomalies(
    records: tuple[AShareIntradayUniverseRecord, ...],
    *,
    config: AShareIntradaySurveillanceConfig | None = None,
) -> AShareIntradayRanking:
    """Rank current-session anomalies without producing a trading action."""

    resolved = config or AShareIntradaySurveillanceConfig()
    if len(records) != len({item.symbol for item in records}):
        raise ValueError("intraday universe must have unique symbols")

    eligible: list[AShareIntradayUniverseRecord] = []
    excluded: list[IntradayExcludedRecord] = []
    for record in sorted(records, key=lambda item: item.symbol):
        reasons = _hard_filter_reasons(record, resolved)
        if reasons:
            excluded.append(IntradayExcludedRecord(record.symbol, record.name, reasons))
        else:
            eligible.append(record)

    raw_by_factor: dict[IntradayAnomalyFactor, dict[str, float]] = {
        spec.factor_id: {} for spec in resolved.factor_specs
    }
    for record in eligible:
        for factor_id, value in _raw_factors(record).items():
            if value is not None and math.isfinite(value):
                raw_by_factor[factor_id][record.symbol] = value

    transformed: dict[
        IntradayAnomalyFactor, dict[str, tuple[float, float, float]]
    ] = {}
    unavailable: list[IntradayAnomalyFactor] = []
    for spec in resolved.factor_specs:
        observed = raw_by_factor[spec.factor_id]
        if len(observed) < resolved.min_cross_section_observations:
            unavailable.append(spec.factor_id)
            transformed[spec.factor_id] = {}
            continue
        values = tuple(observed.values())
        lower = _quantile(values, 0.025)
        upper = _quantile(values, 0.975)
        clipped_values = {
            symbol: min(max(value, lower), upper) for symbol, value in observed.items()
        }
        ranks = _tie_aware_percentiles(clipped_values)
        transformed[spec.factor_id] = {
            symbol: (observed[symbol], value, ranks[symbol] * 2 - 1)
            for symbol, value in clipped_values.items()
        }

    provisional: list[
        tuple[
            AShareIntradayUniverseRecord,
            float,
            float,
            tuple[IntradayFactorContribution, ...],
        ]
    ] = []
    for record in eligible:
        contributions: list[IntradayFactorContribution] = []
        weighted = 0.0
        coverage = 0.0
        for spec in resolved.factor_specs:
            raw = raw_by_factor[spec.factor_id].get(record.symbol)
            transformed_value = transformed[spec.factor_id].get(record.symbol)
            if transformed_value is None:
                contributions.append(
                    IntradayFactorContribution(
                        factor_id=spec.factor_id,
                        raw_value=raw,
                        winsorized_value=None,
                        percentile_rank=None,
                        directional_score=None,
                        configured_weight=spec.weight,
                        contribution=None,
                        cross_section_observations=len(raw_by_factor[spec.factor_id]),
                    )
                )
                continue
            original, clipped, score = transformed_value
            contribution = score * spec.weight
            weighted += contribution
            coverage += spec.weight
            contributions.append(
                IntradayFactorContribution(
                    factor_id=spec.factor_id,
                    raw_value=original,
                    winsorized_value=clipped,
                    percentile_rank=(score + 1) / 2,
                    directional_score=score,
                    configured_weight=spec.weight,
                    contribution=contribution,
                    cross_section_observations=len(raw_by_factor[spec.factor_id]),
                )
            )
        if coverage < resolved.min_factor_weight_coverage:
            excluded.append(
                IntradayExcludedRecord(
                    record.symbol,
                    record.name,
                    (IntradayExclusionReason.INSUFFICIENT_FACTOR_COVERAGE,),
                )
            )
            continue
        score = max(-1.0, min(1.0, weighted / coverage))
        if score >= resolved.candidate_score_threshold:
            provisional.append((record, score, coverage, tuple(contributions)))

    provisional.sort(key=lambda item: (-item[1], item[0].symbol))
    candidates = tuple(
        _candidate(item, rank=index + 1, config=resolved)
        for index, item in enumerate(provisional[: resolved.top_n])
    )
    return AShareIntradayRanking(
        candidates=candidates,
        excluded=tuple(sorted(excluded, key=lambda item: item.symbol)),
        globally_unavailable_factors=tuple(unavailable),
        eligible_count=len(eligible),
    )


def _candidate(
    item: tuple[
        AShareIntradayUniverseRecord,
        float,
        float,
        tuple[IntradayFactorContribution, ...],
    ],
    *,
    rank: int,
    config: AShareIntradaySurveillanceConfig,
) -> IntradayCandidate:
    record, score, coverage, factors = item
    assert record.last_price is not None
    assert record.change_percent is not None
    assert record.session_amount_cny is not None
    factor_map = {factor.factor_id: factor for factor in factors}
    range_raw = factor_map[IntradayAnomalyFactor.RANGE_POSITION].raw_value
    volume_raw = factor_map[IntradayAnomalyFactor.VOLUME_RATIO].raw_value
    if (
        score >= config.momentum_score_threshold
        and record.change_percent >= Decimal("1")
        and range_raw is not None
        and range_raw >= 0.6
        and volume_raw is not None
        and volume_raw >= 1.2
    ):
        candidate_class = IntradayCandidateClass.MOMENTUM_EXPANSION
    elif record.change_percent > 0:
        candidate_class = IntradayCandidateClass.ACTIVE_STRENGTH
    else:
        candidate_class = IntradayCandidateClass.OBSERVATION_ONLY
    reasons = [candidate_class.value, "CURRENT_SESSION_SNAPSHOT_ONLY"]
    if coverage < 1:
        reasons.append("PARTIAL_FACTOR_COVERAGE")
    return IntradayCandidate(
        symbol=record.symbol,
        name=record.name,
        rank=rank,
        candidate_class=candidate_class,
        anomaly_score=score,
        factor_weight_coverage=coverage,
        last_price=record.last_price,
        change_percent=record.change_percent,
        session_amount_cny=record.session_amount_cny,
        factors=factors,
        reason_codes=tuple(reasons),
    )


def _hard_filter_reasons(
    record: AShareIntradayUniverseRecord,
    config: AShareIntradaySurveillanceConfig,
) -> tuple[IntradayExclusionReason, ...]:
    reasons: list[IntradayExclusionReason] = []
    if record.is_st is None:
        reasons.append(IntradayExclusionReason.UNKNOWN_ST_STATUS)
    elif record.is_st:
        reasons.append(IntradayExclusionReason.ST_SECURITY)
    if record.is_suspended is None:
        reasons.append(IntradayExclusionReason.UNKNOWN_SUSPENSION_STATUS)
    elif record.is_suspended:
        reasons.append(IntradayExclusionReason.SUSPENDED)
    if record.last_price is None:
        reasons.append(IntradayExclusionReason.MISSING_PRICE)
    elif record.last_price <= 0 or record.last_price < config.min_price:
        reasons.append(IntradayExclusionReason.INVALID_PRICE)
    if record.previous_close is None or record.previous_close <= 0:
        reasons.append(IntradayExclusionReason.MISSING_PREVIOUS_CLOSE)
    if record.change_percent is None:
        reasons.append(IntradayExclusionReason.MISSING_CHANGE_PERCENT)
    if record.session_amount_cny is None:
        reasons.append(IntradayExclusionReason.MISSING_SESSION_AMOUNT)
    elif record.session_amount_cny < config.min_session_amount_cny:
        reasons.append(IntradayExclusionReason.LOW_SESSION_AMOUNT)
    return tuple(dict.fromkeys(reasons))


def _raw_factors(
    record: AShareIntradayUniverseRecord,
) -> dict[IntradayAnomalyFactor, float | None]:
    range_position: float | None = None
    if (
        record.last_price is not None
        and record.high_price is not None
        and record.low_price is not None
        and record.high_price > record.low_price
    ):
        range_position = float(
            (record.last_price - record.low_price)
            / (record.high_price - record.low_price)
        )
    follow_through: float | None = None
    if (
        record.last_price is not None
        and record.open_price is not None
        and record.open_price > 0
    ):
        follow_through = float(record.last_price / record.open_price - 1)
    return {
        IntradayAnomalyFactor.PRICE_STRENGTH: _float(record.change_percent),
        IntradayAnomalyFactor.SESSION_AMOUNT: (
            None
            if record.session_amount_cny is None or record.session_amount_cny <= 0
            else math.log10(float(record.session_amount_cny))
        ),
        IntradayAnomalyFactor.RANGE_POSITION: range_position,
        IntradayAnomalyFactor.OPEN_FOLLOW_THROUGH: follow_through,
        IntradayAnomalyFactor.VOLUME_RATIO: _float(record.volume_ratio),
        IntradayAnomalyFactor.TURNOVER_RATE: _float(record.turnover_rate_percent),
    }


def _float(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _quantile(values: tuple[float, ...], quantile: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * quantile
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _tie_aware_percentiles(values: dict[str, float]) -> dict[str, float]:
    ordered = sorted(values.items(), key=lambda item: (item[1], item[0]))
    result: dict[str, float] = {}
    denominator = max(len(ordered) - 1, 1)
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][1] == ordered[index][1]:
            end += 1
        average_position = (index + end - 1) / 2
        percentile = average_position / denominator
        for symbol, _ in ordered[index:end]:
            result[symbol] = percentile
        index = end
    return result
