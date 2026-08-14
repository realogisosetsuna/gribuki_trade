"""基于多时间框架、时点一致证据生成确定性的 DEEP 退出计划。

DEEP 计划不是让大模型直接报一个止损价。价格门槛只由已完成且当时可知的
K 线映射得到；LLM 结论仅能选择预登记的收益/风险档位或缩短持有期限，不能
把硬止损下移，也不能延长原计划期限。这样既允许强趋势保留超额收益空间，
又不会让语言模型绕过风险边界。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from gribuki_trade.domain.exit_plans import (
    ExitPlan,
    ExitPlanDepth,
    ExitPlanState,
    exit_plan_id,
)
from gribuki_trade.features.technical import TechnicalBar


@dataclass(frozen=True, slots=True)
class DeepSemanticAssessment:
    """一个可审计的单分析器或对抗分析系统结论。"""

    assessment_id: str
    system: str
    score: Decimal
    confidence: Decimal
    market_data_as_of: datetime
    evidence_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("assessment_id", "system"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must not be empty")
        for name in ("score", "confidence"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ValueError(f"{name} must be a finite Decimal")
        if not Decimal("-1") <= self.score <= Decimal("1"):
            raise ValueError("score must be in [-1, 1]")
        if not Decimal("0") <= self.confidence <= Decimal("1"):
            raise ValueError("confidence must be in [0, 1]")
        object.__setattr__(
            self,
            "market_data_as_of",
            _aware_utc(self.market_data_as_of, "market_data_as_of"),
        )
        evidence = tuple(item.strip() for item in self.evidence_ids)
        if any(not item for item in evidence) or len(evidence) != len(set(evidence)):
            raise ValueError("evidence_ids must be unique and non-empty")
        object.__setattr__(self, "evidence_ids", evidence)


@dataclass(frozen=True, slots=True)
class DeepExitTimeframe:
    """一个时间框架的冻结行情与预登记计算参数。"""

    timeframe_id: str
    bars: tuple[TechnicalBar, ...]
    weight: Decimal
    maximum_age: timedelta
    atr_bars: int = 14
    structure_bars: int = 20
    atr_stop_multiple: Decimal = Decimal("2.25")
    structure_buffer_atr: Decimal = Decimal("0.10")

    def __post_init__(self) -> None:
        if not isinstance(self.timeframe_id, str) or not self.timeframe_id.strip():
            raise ValueError("timeframe_id must not be empty")
        if not isinstance(self.weight, Decimal) or not self.weight.is_finite() or self.weight <= 0:
            raise ValueError("weight must be a positive finite Decimal")
        if self.maximum_age <= timedelta(0):
            raise ValueError("maximum_age must be positive")
        for name in ("atr_bars", "structure_bars"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 2:
                raise ValueError(f"{name} must be an integer of at least two")
        for name in ("atr_stop_multiple", "structure_buffer_atr"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be a positive finite Decimal")

    @property
    def minimum_history(self) -> int:
        return max(self.atr_bars + 1, self.structure_bars)


@dataclass(frozen=True, slots=True)
class DeepExitPlanConfig:
    """DEEP 价格映射和语义分档的冻结策略。"""

    price_tick: Decimal = Decimal("0.01")
    reward_score_grid: tuple[tuple[Decimal, Decimal], ...] = (
        (Decimal("-1.0"), Decimal("1.0")),
        (Decimal("-0.25"), Decimal("1.5")),
        (Decimal("0.20"), Decimal("2.0")),
        (Decimal("0.50"), Decimal("2.5")),
        (Decimal("0.75"), Decimal("3.0")),
    )
    policy_version: str = "deep-exit-multiframe-semantic@1"
    calibration_id: str = "DEEP_MULTIFRAME_REGISTERED_GRID_UNCALIBRATED@1"

    def __post_init__(self) -> None:
        if (
            not isinstance(self.price_tick, Decimal)
            or not self.price_tick.is_finite()
            or self.price_tick <= 0
        ):
            raise ValueError("price_tick must be a positive finite Decimal")
        if not self.reward_score_grid:
            raise ValueError("reward_score_grid must not be empty")
        thresholds = tuple(item[0] for item in self.reward_score_grid)
        if thresholds != tuple(sorted(thresholds)) or len(thresholds) != len(set(thresholds)):
            raise ValueError("reward_score_grid thresholds must be unique and ordered")
        if any(reward <= 0 for _, reward in self.reward_score_grid):
            raise ValueError("reward multiples must be positive")
        for name in ("policy_version", "calibration_id"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(f"{name} must not be empty")


@dataclass(frozen=True, slots=True)
class DeepTimeframeCalculation:
    timeframe_id: str
    robust_atr: Decimal
    volatility_stop: Decimal
    structure_stop: Decimal
    protective_level: Decimal
    weight: Decimal


@dataclass(frozen=True, slots=True)
class DeepExitPlanResult:
    plan: ExitPlan
    timeframe_calculations: tuple[DeepTimeframeCalculation, ...]
    weighted_consensus_stop: Decimal
    selected_semantic_system: str
    selected_semantic_score: Decimal
    baseline_score: Decimal | None
    adversarial_score: Decimal | None


def build_deep_exit_plan(
    previous: ExitPlan,
    *,
    timeframes: tuple[DeepExitTimeframe, ...],
    decision_at: datetime,
    baseline_assessment: DeepSemanticAssessment | None = None,
    adversarial_assessment: DeepSemanticAssessment | None = None,
    config: DeepExitPlanConfig | None = None,
) -> DeepExitPlanResult:
    """融合多时间框架并生成只能收紧硬风险的下一版本计划。"""

    resolved = config or DeepExitPlanConfig(price_tick=previous.price_tick)
    decision_at = _aware_utc(decision_at, "decision_at")
    if resolved.price_tick != previous.price_tick:
        raise ValueError("DEEP price tick must match the active plan")
    if not timeframes:
        raise ValueError("at least one timeframe is required")
    ids = tuple(item.timeframe_id for item in timeframes)
    if len(ids) != len(set(ids)):
        raise ValueError("timeframe_id values must be unique")

    calculations = tuple(
        _calculate_timeframe(item, previous.entry_basis_price, decision_at) for item in timeframes
    )
    consensus = _weighted_median(
        tuple((item.protective_level, item.weight) for item in calculations)
    )
    mapped_stop = _round_to_tick(consensus, resolved.price_tick, ROUND_FLOOR)
    # 硬约束：DEEP 可以维持或上移止损，绝不能把 QUICK/上一版止损下移。
    stop = max(previous.stop_price, mapped_stop)

    assessments = tuple(
        item for item in (baseline_assessment, adversarial_assessment) if item is not None
    )
    for item in assessments:
        if item.market_data_as_of > decision_at:
            raise ValueError("semantic assessment contains future market data")
    selected = adversarial_assessment or baseline_assessment
    selected_system = "DETERMINISTIC_ONLY" if selected is None else selected.system
    selected_score = Decimal("0") if selected is None else selected.score * selected.confidence
    reward_to_risk = _reward_multiple(selected_score, resolved.reward_score_grid)
    take_profit = _round_to_tick(
        previous.entry_basis_price + previous.initial_risk_per_share * reward_to_risk,
        resolved.price_tick,
        ROUND_CEILING,
    )
    if stop >= take_profit:
        # 已经保护了大量浮盈时，目标至少保留一个 tick，且同步登记实际 R 倍数。
        take_profit = stop + resolved.price_tick
        reward_to_risk = (
            take_profit - previous.entry_basis_price
        ) / previous.initial_risk_per_share

    time_exit_at = previous.time_exit_at
    if selected_score <= Decimal("-0.25"):
        shortened = decision_at + timedelta(days=1)
        if shortened < time_exit_at:
            time_exit_at = shortened
    if time_exit_at <= decision_at:
        # 已到原时间门槛时不伪造一个新计划，由调用方直接处理 TIME barrier。
        raise ValueError("active time barrier has already expired")

    feature_sha = _snapshot_sha256(
        previous=previous,
        timeframes=timeframes,
        calculations=calculations,
        decision_at=decision_at,
        baseline=baseline_assessment,
        adversarial=adversarial_assessment,
        config=resolved,
    )
    evidence_ids = tuple(
        dict.fromkeys(
            item
            for assessment in assessments
            for item in (assessment.assessment_id, *assessment.evidence_ids)
        )
    )
    state = (
        ExitPlanState.CONFIRMED
        if baseline_assessment is not None and adversarial_assessment is not None
        else ExitPlanState.DEGRADED
    )
    reasons = [
        "MULTIFRAME_PIT_PRICE_FUSION",
        "WEIGHTED_MEDIAN_STRUCTURE_VOLATILITY_STOP",
        "HARD_RISK_NEVER_LOOSENED",
        "REGISTERED_REWARD_RISK_TARGET",
        "ADVERSARIAL_RESULT_PREFERRED"
        if adversarial_assessment is not None
        else "SEMANTIC_DEGRADED_FALLBACK",
    ]
    if reward_to_risk >= Decimal("2.5"):
        reasons.append("STRONG_REGIME_ALLOWS_ALPHA_RUNWAY")
    metrics: list[tuple[str, Decimal]] = [
        ("weighted_consensus_stop", consensus),
        ("selected_semantic_score", selected_score),
    ]
    if baseline_assessment is not None:
        metrics.append(
            ("baseline_semantic_score", baseline_assessment.score * baseline_assessment.confidence)
        )
    if adversarial_assessment is not None:
        metrics.append(
            (
                "adversarial_semantic_score",
                adversarial_assessment.score * adversarial_assessment.confidence,
            )
        )
    for calculation in calculations:
        prefix = calculation.timeframe_id.replace("-", "_")
        metrics.extend(
            (
                (f"{prefix}_robust_atr", calculation.robust_atr),
                (f"{prefix}_structure_stop", calculation.structure_stop),
                (f"{prefix}_volatility_stop", calculation.volatility_stop),
                (f"{prefix}_protective_level", calculation.protective_level),
            )
        )
    market_data_as_of = max(frame.bars[-1].end_time for frame in timeframes)
    plan = ExitPlan(
        plan_id=exit_plan_id(
            protection_id=previous.protection_id,
            version=previous.version + 1,
            feature_snapshot_sha256=feature_sha,
            policy_version=resolved.policy_version,
        ),
        protection_id=previous.protection_id,
        account_id=previous.account_id,
        symbol=previous.symbol,
        version=previous.version + 1,
        depth=ExitPlanDepth.DEEP,
        state=state,
        decision_at=decision_at,
        market_data_as_of=market_data_as_of,
        time_exit_at=time_exit_at,
        entry_basis_price=previous.entry_basis_price,
        stop_price=stop,
        take_profit_price=take_profit,
        initial_risk_per_share=previous.initial_risk_per_share,
        reward_to_risk=reward_to_risk,
        price_tick=previous.price_tick,
        technical_invalidation_price=previous.technical_invalidation_price,
        feature_snapshot_sha256=feature_sha,
        policy_version=resolved.policy_version,
        strategy_version=previous.strategy_version,
        calibration_id=resolved.calibration_id,
        reason_codes=tuple(reasons),
        metrics=tuple(metrics),
        evidence_ids=evidence_ids,
        supersedes_plan_id=previous.plan_id,
    )
    return DeepExitPlanResult(
        plan=plan,
        timeframe_calculations=calculations,
        weighted_consensus_stop=consensus,
        selected_semantic_system=selected_system,
        selected_semantic_score=selected_score,
        baseline_score=None if baseline_assessment is None else baseline_assessment.score,
        adversarial_score=None if adversarial_assessment is None else adversarial_assessment.score,
    )


def aggregate_completed_bars(
    bars: tuple[TechnicalBar, ...],
    *,
    interval_minutes: int,
) -> tuple[TechnicalBar, ...]:
    """把连续的一分钟完成线确定性聚合；缺口或残段直接丢弃。"""

    if (
        isinstance(interval_minutes, bool)
        or not isinstance(interval_minutes, int)
        or interval_minutes < 1
    ):
        raise ValueError("interval_minutes must be a positive integer")
    if interval_minutes == 1:
        return bars
    ordered = tuple(sorted(bars, key=lambda item: item.end_time))
    groups: dict[int, list[TechnicalBar]] = {}
    seconds = interval_minutes * 60
    for bar in ordered:
        end = _aware_utc(bar.end_time, "bar.end_time")
        bucket = int((end.timestamp() - 1) // seconds)
        groups.setdefault(bucket, []).append(bar)
    output: list[TechnicalBar] = []
    for bucket in sorted(groups):
        group = groups[bucket]
        if len(group) != interval_minutes or any(not item.complete for item in group):
            continue
        ends = tuple(_aware_utc(item.end_time, "bar.end_time") for item in group)
        if any(
            right - left != timedelta(minutes=1)
            for left, right in zip(ends, ends[1:], strict=False)
        ):
            continue
        output.append(
            TechnicalBar(
                end_time=group[-1].end_time,
                available_at=max(item.available_at for item in group),
                open=group[0].open,
                high=max(item.high for item in group),
                low=min(item.low for item in group),
                close=group[-1].close,
                volume=sum(item.volume for item in group),
                complete=True,
            )
        )
    return tuple(output)


def _calculate_timeframe(
    frame: DeepExitTimeframe,
    entry: Decimal,
    decision_at: datetime,
) -> DeepTimeframeCalculation:
    bars = frame.bars
    if len(bars) < frame.minimum_history:
        raise ValueError(f"insufficient completed history for {frame.timeframe_id}")
    if any(not item.complete for item in bars):
        raise ValueError("DEEP planning requires completed bars")
    ends = tuple(_aware_utc(item.end_time, "bar.end_time") for item in bars)
    if ends != tuple(sorted(ends)) or len(ends) != len(set(ends)):
        raise ValueError("bars must be strictly ordered and unique")
    if any(_aware_utc(item.available_at, "bar.available_at") > decision_at for item in bars):
        raise ValueError("a DEEP bar was not available at decision_at")
    age = decision_at - ends[-1]
    if age < timedelta(0):
        raise ValueError("latest DEEP bar is from the future")
    if age > frame.maximum_age:
        raise ValueError(f"stale DEEP timeframe: {frame.timeframe_id}")
    window = bars[-(frame.atr_bars + 1) :]
    true_ranges = tuple(
        max(
            current.high - current.low,
            abs(current.high - previous.close),
            abs(current.low - previous.close),
        )
        for previous, current in zip(window, window[1:], strict=False)
    )
    median = _median(true_ranges)
    mad = _median(tuple(abs(item - median) for item in true_ranges))
    ewma = _ewma(true_ranges)
    atr = max(median + Decimal("1.4826") * mad, ewma)
    if atr <= 0:
        raise ValueError("DEEP robust ATR is not positive")
    volatility = entry - atr * frame.atr_stop_multiple
    structure = (
        min(item.low for item in bars[-frame.structure_bars :]) - atr * frame.structure_buffer_atr
    )
    if volatility <= 0 or structure <= 0:
        raise ValueError("DEEP stop candidate is non-positive")
    # 每个框架先取较贴近价格、仍有证据支持的保护位，再跨框架做稳健中位融合。
    protective = max(volatility, structure)
    return DeepTimeframeCalculation(
        timeframe_id=frame.timeframe_id,
        robust_atr=atr,
        volatility_stop=volatility,
        structure_stop=structure,
        protective_level=protective,
        weight=frame.weight,
    )


def _weighted_median(values: tuple[tuple[Decimal, Decimal], ...]) -> Decimal:
    ordered = tuple(sorted(values, key=lambda item: item[0]))
    half = sum((weight for _, weight in ordered), Decimal("0")) / Decimal("2")
    cumulative = Decimal("0")
    for value, weight in ordered:
        cumulative += weight
        if cumulative >= half:
            return value
    raise RuntimeError("weighted median is unavailable")  # pragma: no cover


def _reward_multiple(score: Decimal, grid: tuple[tuple[Decimal, Decimal], ...]) -> Decimal:
    selected = grid[0][1]
    for threshold, reward in grid:
        if score >= threshold:
            selected = reward
    return selected


def _snapshot_sha256(
    *,
    previous: ExitPlan,
    timeframes: tuple[DeepExitTimeframe, ...],
    calculations: tuple[DeepTimeframeCalculation, ...],
    decision_at: datetime,
    baseline: DeepSemanticAssessment | None,
    adversarial: DeepSemanticAssessment | None,
    config: DeepExitPlanConfig,
) -> str:
    document = {
        "previous_plan_id": previous.plan_id,
        "decision_at": decision_at.isoformat(),
        "policy_version": config.policy_version,
        "calibration_id": config.calibration_id,
        "reward_score_grid": [[str(a), str(b)] for a, b in config.reward_score_grid],
        "timeframes": [
            {
                "timeframe_id": frame.timeframe_id,
                "weight": str(frame.weight),
                "maximum_age_seconds": int(frame.maximum_age.total_seconds()),
                "atr_bars": frame.atr_bars,
                "structure_bars": frame.structure_bars,
                "atr_stop_multiple": str(frame.atr_stop_multiple),
                "structure_buffer_atr": str(frame.structure_buffer_atr),
                "bars": [_bar_document(bar) for bar in frame.bars],
            }
            for frame in timeframes
        ],
        "calculations": [
            {
                "timeframe_id": item.timeframe_id,
                "robust_atr": str(item.robust_atr),
                "volatility_stop": str(item.volatility_stop),
                "structure_stop": str(item.structure_stop),
                "protective_level": str(item.protective_level),
                "weight": str(item.weight),
            }
            for item in calculations
        ],
        "baseline": _assessment_document(baseline),
        "adversarial": _assessment_document(adversarial),
    }
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()


def _bar_document(bar: TechnicalBar) -> dict[str, object]:
    return {
        "end_time": _aware_utc(bar.end_time, "bar.end_time").isoformat(),
        "available_at": _aware_utc(bar.available_at, "bar.available_at").isoformat(),
        "open": str(bar.open),
        "high": str(bar.high),
        "low": str(bar.low),
        "close": str(bar.close),
        "volume": bar.volume,
        "complete": bar.complete,
    }


def _assessment_document(value: DeepSemanticAssessment | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "assessment_id": value.assessment_id,
        "system": value.system,
        "score": str(value.score),
        "confidence": str(value.confidence),
        "market_data_as_of": value.market_data_as_of.isoformat(),
        "evidence_ids": list(value.evidence_ids),
    }


def _median(values: tuple[Decimal, ...]) -> Decimal:
    ordered = tuple(sorted(values))
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def _ewma(values: tuple[Decimal, ...]) -> Decimal:
    alpha = Decimal("2") / Decimal(len(values) + 1)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (Decimal("1") - alpha) * result
    return result


def _round_to_tick(value: Decimal, tick: Decimal, rounding: str) -> Decimal:
    return (value / tick).to_integral_value(rounding=rounding) * tick


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


__all__ = [
    "DeepExitPlanConfig",
    "DeepExitPlanResult",
    "DeepExitTimeframe",
    "DeepSemanticAssessment",
    "DeepTimeframeCalculation",
    "aggregate_completed_bars",
    "build_deep_exit_plan",
]
