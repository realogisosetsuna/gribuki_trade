"""符合时点安全要求的描述性跨市场关系计算。

调用方负责来源市场日历转换与时点对齐。每个因子观测都关联到一个 A 股决策日期，且必须
不晚于该日 ``decision_at`` 时间戳可用。本模块在进行任何计算前校验该契约。

``lag_0`` 估计 ``target_return[t] ~ factor_return[t]``；``lag_1`` 估计
``target_return[t] ~ factor_return[t-1]``，其中 ``t-1`` 是所提供的前一个 A 股交易日，
而不是前一自然日。

所有相关、回归与风险标签都只是描述性关联，不能确立因果性、可预测性或可执行交易优势。

窗口相关与 OLS 使用最近 20/60/120 个共同观测。EWMA 相关使用全部共同观测，年龄按所提供
A 股交易日计量，半衰期为 20 与 60 个交易日。OLS 是单变量模型
``target = alpha + beta * factor + error``；其 beta t 统计量采用经典独立同分布/同方差
标准误，未作 HAC/Newey-West 调整。
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Literal, NoReturn

from gribuki_trade.features.cross_market_models import (
    METHODOLOGY_VERSION,
    MINIMUM_COMMON_SAMPLES,
    NON_CAUSALITY_NOTICE,
    AlignedFactorReturn,
    CrossMarketCorrelationSign,
    CrossMarketFactorRelation,
    CrossMarketFactorSeries,
    CrossMarketLagRelation,
    CrossMarketRelationFailureReason,
    CrossMarketRelationInputError,
    CrossMarketRelationsReport,
    CrossMarketRiskAlignment,
    CrossMarketRiskDirection,
    CrossMarketSignRegime,
    Numeric,
    TargetCloseObservation,
    _Pair,
    _TargetReturn,
)


def build_cross_market_relations(
    target_symbol: str,
    target_closes: Sequence[TargetCloseObservation],
    factors: Sequence[CrossMarketFactorSeries],
) -> CrossMarketRelationsReport:
    """强制执行时点输入契约后构建当期与滞后一期关系。

    每个滞后都独立要求至少 120 个共同观测。缺失因子日期会降低 ``coverage``，因此可以
    只使一个滞后不可用，而不令其他因子的结果失效。
    """

    symbol = target_symbol.strip().upper()
    if not symbol:
        _raise_input(
            CrossMarketRelationFailureReason.EMPTY_TARGET_SYMBOL,
            "target_symbol must not be empty",
        )
    if not target_closes:
        _raise_input(
            CrossMarketRelationFailureReason.EMPTY_TARGET_SERIES,
            "target_closes must not be empty",
        )

    closes, decisions = _validate_target_closes(target_closes)
    target_returns = _build_target_returns(closes)
    factor_ids: set[str] = set()
    relations: list[CrossMarketFactorRelation] = []

    for factor in factors:
        factor_id = factor.factor_id.strip()
        if not factor_id:
            _raise_input(
                CrossMarketRelationFailureReason.EMPTY_FACTOR_ID,
                "factor_id must not be empty",
            )
        if factor_id in factor_ids:
            _raise_input(
                CrossMarketRelationFailureReason.DUPLICATE_FACTOR_ID,
                f"duplicate factor_id: {factor_id}",
            )
        factor_ids.add(factor_id)
        if not isinstance(factor.risk_direction, CrossMarketRiskDirection):
            _raise_input(
                CrossMarketRelationFailureReason.INVALID_RISK_DIRECTION,
                f"factor {factor_id} has an invalid risk_direction",
            )

        factor_values = _validate_factor_observations(
            factor_id=factor_id,
            observations=factor.observations,
            decisions=decisions,
        )
        lag_0_pairs = _pair_returns(target_returns, factor_values, lag=0)
        lag_1_pairs = _pair_returns(target_returns, factor_values, lag=1)
        eligible = len(target_returns)
        relations.append(
            CrossMarketFactorRelation(
                factor_id=factor_id,
                risk_direction=factor.risk_direction,
                lag_0=_calculate_lag_relation(
                    lag=0,
                    eligible_target_samples=eligible,
                    pairs=lag_0_pairs,
                    risk_direction=factor.risk_direction,
                ),
                lag_1=_calculate_lag_relation(
                    lag=1,
                    eligible_target_samples=eligible,
                    pairs=lag_1_pairs,
                    risk_direction=factor.risk_direction,
                ),
            )
        )

    return CrossMarketRelationsReport(
        target_symbol=symbol,
        as_of_trade_date=closes[-1][0],
        target_close_samples=len(closes),
        minimum_common_samples=MINIMUM_COMMON_SAMPLES,
        factors=tuple(relations),
        methodology_version=METHODOLOGY_VERSION,
        non_causality_notice=NON_CAUSALITY_NOTICE,
    )


def _validate_target_closes(
    observations: Sequence[TargetCloseObservation],
) -> tuple[tuple[tuple[date, float], ...], dict[date, datetime]]:
    output: list[tuple[date, float]] = []
    decisions: dict[date, datetime] = {}
    previous_date: date | None = None
    previous_decision: datetime | None = None

    for point in observations:
        if previous_date is not None and point.trade_date <= previous_date:
            _raise_input(
                CrossMarketRelationFailureReason.TARGET_DATES_NOT_STRICTLY_ORDERED,
                "target trade dates must be strictly increasing and unique",
            )
        _require_aware(point.decision_at, "target decision_at")
        if previous_decision is not None and point.decision_at <= previous_decision:
            _raise_input(
                CrossMarketRelationFailureReason.TARGET_DECISIONS_NOT_STRICTLY_ORDERED,
                "target decision_at timestamps must be strictly increasing",
            )
        close = _finite_float(point.close, "target close")
        if close <= 0.0:
            _raise_input(
                CrossMarketRelationFailureReason.NON_POSITIVE_CLOSE,
                "target closes must be positive",
            )
        output.append((point.trade_date, close))
        decisions[point.trade_date] = point.decision_at
        previous_date = point.trade_date
        previous_decision = point.decision_at
    return tuple(output), decisions


def _validate_factor_observations(
    *,
    factor_id: str,
    observations: Sequence[AlignedFactorReturn],
    decisions: dict[date, datetime],
) -> dict[date, float]:
    output: dict[date, float] = {}
    previous_date: date | None = None
    for point in observations:
        if previous_date is not None and point.target_trade_date <= previous_date:
            _raise_input(
                CrossMarketRelationFailureReason.FACTOR_DATES_NOT_STRICTLY_ORDERED,
                f"factor {factor_id} dates must be strictly increasing and unique",
            )
        decision_at = decisions.get(point.target_trade_date)
        if decision_at is None:
            _raise_input(
                CrossMarketRelationFailureReason.UNKNOWN_TARGET_DATE,
                f"factor {factor_id} references an unknown target trade date",
            )
        _require_aware(point.available_at, f"factor {factor_id} available_at")
        if point.available_at > decision_at:
            _raise_input(
                CrossMarketRelationFailureReason.FUTURE_AVAILABLE_AT,
                f"factor {factor_id} was not available at its target decision point",
            )
        output[point.target_trade_date] = _finite_float(
            point.value,
            f"factor {factor_id} return",
        )
        previous_date = point.target_trade_date
    return output


def _build_target_returns(
    closes: Sequence[tuple[date, float]],
) -> tuple[_TargetReturn, ...]:
    output: list[_TargetReturn] = []
    for index in range(1, len(closes)):
        previous_date, previous_close = closes[index - 1]
        trade_date, close = closes[index]
        value = close / previous_close - 1.0
        if not math.isfinite(value):
            _raise_input(
                CrossMarketRelationFailureReason.NON_FINITE_VALUE,
                "a derived target return is not finite",
            )
        output.append(
            _TargetReturn(
                target_index=index,
                trade_date=trade_date,
                previous_trade_date=previous_date,
                value=value,
            )
        )
    return tuple(output)


def _pair_returns(
    target_returns: Sequence[_TargetReturn],
    factor_values: dict[date, float],
    *,
    lag: Literal[0, 1],
) -> tuple[_Pair, ...]:
    output: list[_Pair] = []
    for target in target_returns:
        factor_date = target.trade_date if lag == 0 else target.previous_trade_date
        factor_return = factor_values.get(factor_date)
        if factor_return is None:
            continue
        output.append(
            _Pair(
                target_index=target.target_index,
                trade_date=target.trade_date,
                target_return=target.value,
                factor_return=factor_return,
            )
        )
    return tuple(output)


def _calculate_lag_relation(
    *,
    lag: Literal[0, 1],
    eligible_target_samples: int,
    pairs: Sequence[_Pair],
    risk_direction: CrossMarketRiskDirection,
) -> CrossMarketLagRelation:
    common_samples = len(pairs)
    coverage = (
        common_samples / eligible_target_samples if eligible_target_samples > 0 else 0.0
    )
    first_date = pairs[0].trade_date if pairs else None
    last_date = pairs[-1].trade_date if pairs else None
    if common_samples < MINIMUM_COMMON_SAMPLES:
        return _empty_lag_relation(
            lag=lag,
            eligible_target_samples=eligible_target_samples,
            common_samples=common_samples,
            coverage=coverage,
            first_common_date=first_date,
            last_common_date=last_date,
            failure_reason=CrossMarketRelationFailureReason.INSUFFICIENT_COMMON_SAMPLES,
        )

    latest_120 = pairs[-MINIMUM_COMMON_SAMPLES:]
    target_values = [pair.target_return for pair in latest_120]
    factor_values = [pair.factor_return for pair in latest_120]
    target_sum_squares = _centered_sum_squares(target_values)
    factor_sum_squares = _centered_sum_squares(factor_values)

    beta: float | None = None
    alpha: float | None = None
    beta_t_stat: float | None = None
    failure_reason: CrossMarketRelationFailureReason | None = None
    if factor_sum_squares == 0.0:
        failure_reason = CrossMarketRelationFailureReason.ZERO_FACTOR_VARIANCE
    elif target_sum_squares == 0.0:
        failure_reason = CrossMarketRelationFailureReason.ZERO_TARGET_VARIANCE
    else:
        alpha, beta, beta_t_stat = _ols(target_values, factor_values)
        if beta_t_stat is None:
            failure_reason = (
                CrossMarketRelationFailureReason.BETA_STANDARD_ERROR_UNAVAILABLE
            )

    correlation_20 = _pearson(pairs[-20:])
    correlation_60 = _pearson(pairs[-60:])
    correlation_120 = _pearson(latest_120)
    ewma_20 = _ewma_correlation(pairs, half_life=20)
    ewma_60 = _ewma_correlation(pairs, half_life=60)
    correlations = (correlation_20, correlation_60, correlation_120)
    if failure_reason is None and (
        any(value is None for value in correlations) or ewma_20 is None or ewma_60 is None
    ):
        failure_reason = CrossMarketRelationFailureReason.UNDEFINED_CORRELATION

    signs = (
        _correlation_sign(correlation_20),
        _correlation_sign(correlation_60),
        _correlation_sign(correlation_120),
    )
    sign_regime, stability = _sign_stability(signs)
    risk_alignment = _risk_alignment(sign_regime, risk_direction)
    return CrossMarketLagRelation(
        lag=lag,
        eligible_target_samples=eligible_target_samples,
        common_samples=common_samples,
        coverage=coverage,
        first_common_date=first_date,
        last_common_date=last_date,
        ewma_correlation_half_life_20=ewma_20,
        ewma_correlation_half_life_60=ewma_60,
        correlation_20=correlation_20,
        correlation_60=correlation_60,
        correlation_120=correlation_120,
        correlation_sign_20=signs[0],
        correlation_sign_60=signs[1],
        correlation_sign_120=signs[2],
        correlation_sign_stability=stability,
        sign_regime=sign_regime,
        alpha_120=alpha,
        beta_120=beta,
        beta_t_stat_120=beta_t_stat,
        risk_alignment=risk_alignment,
        failure_reason=failure_reason,
    )


def _empty_lag_relation(
    *,
    lag: Literal[0, 1],
    eligible_target_samples: int,
    common_samples: int,
    coverage: float,
    first_common_date: date | None,
    last_common_date: date | None,
    failure_reason: CrossMarketRelationFailureReason,
) -> CrossMarketLagRelation:
    return CrossMarketLagRelation(
        lag=lag,
        eligible_target_samples=eligible_target_samples,
        common_samples=common_samples,
        coverage=coverage,
        first_common_date=first_common_date,
        last_common_date=last_common_date,
        ewma_correlation_half_life_20=None,
        ewma_correlation_half_life_60=None,
        correlation_20=None,
        correlation_60=None,
        correlation_120=None,
        correlation_sign_20=CrossMarketCorrelationSign.UNAVAILABLE,
        correlation_sign_60=CrossMarketCorrelationSign.UNAVAILABLE,
        correlation_sign_120=CrossMarketCorrelationSign.UNAVAILABLE,
        correlation_sign_stability=None,
        sign_regime=CrossMarketSignRegime.UNAVAILABLE,
        alpha_120=None,
        beta_120=None,
        beta_t_stat_120=None,
        risk_alignment=CrossMarketRiskAlignment.UNSTABLE_OR_NEUTRAL,
        failure_reason=failure_reason,
    )


def _pearson(pairs: Sequence[_Pair]) -> float | None:
    if len(pairs) < 2:
        return None
    target_values = [pair.target_return for pair in pairs]
    factor_values = [pair.factor_return for pair in pairs]
    target_mean = math.fsum(target_values) / len(target_values)
    factor_mean = math.fsum(factor_values) / len(factor_values)
    target_centered = [value - target_mean for value in target_values]
    factor_centered = [value - factor_mean for value in factor_values]
    target_ss = math.fsum(value * value for value in target_centered)
    factor_ss = math.fsum(value * value for value in factor_centered)
    if target_ss == 0.0 or factor_ss == 0.0:
        return None
    covariance = math.fsum(
        target * factor
        for target, factor in zip(target_centered, factor_centered, strict=True)
    )
    correlation = covariance / math.sqrt(target_ss * factor_ss)
    return _finite_clamped_correlation(correlation)


def _ewma_correlation(
    pairs: Sequence[_Pair],
    *,
    half_life: int,
) -> float | None:
    if len(pairs) < 2:
        return None
    newest_target_index = pairs[-1].target_index
    log_decay = math.log(0.5) / half_life
    weights = [
        math.exp(log_decay * (newest_target_index - pair.target_index)) for pair in pairs
    ]
    weight_sum = math.fsum(weights)
    if weight_sum == 0.0:
        return None
    target_mean = math.fsum(
        weight * pair.target_return for weight, pair in zip(weights, pairs, strict=True)
    ) / weight_sum
    factor_mean = math.fsum(
        weight * pair.factor_return for weight, pair in zip(weights, pairs, strict=True)
    ) / weight_sum
    covariance = math.fsum(
        weight
        * (pair.target_return - target_mean)
        * (pair.factor_return - factor_mean)
        for weight, pair in zip(weights, pairs, strict=True)
    )
    target_variance = math.fsum(
        weight * (pair.target_return - target_mean) ** 2
        for weight, pair in zip(weights, pairs, strict=True)
    )
    factor_variance = math.fsum(
        weight * (pair.factor_return - factor_mean) ** 2
        for weight, pair in zip(weights, pairs, strict=True)
    )
    if target_variance == 0.0 or factor_variance == 0.0:
        return None
    correlation = covariance / math.sqrt(target_variance * factor_variance)
    return _finite_clamped_correlation(correlation)


def _ols(
    target_values: Sequence[float],
    factor_values: Sequence[float],
) -> tuple[float, float, float | None]:
    sample_count = len(target_values)
    target_mean = math.fsum(target_values) / sample_count
    factor_mean = math.fsum(factor_values) / sample_count
    target_centered = [value - target_mean for value in target_values]
    factor_centered = [value - factor_mean for value in factor_values]
    factor_ss = math.fsum(value * value for value in factor_centered)
    covariance = math.fsum(
        target * factor
        for target, factor in zip(target_centered, factor_centered, strict=True)
    )
    beta = covariance / factor_ss
    alpha = target_mean - beta * factor_mean
    residual_sum_squares = math.fsum(
        (target - alpha - beta * factor) ** 2
        for target, factor in zip(target_values, factor_values, strict=True)
    )
    beta_standard_error = math.sqrt(
        (residual_sum_squares / (sample_count - 2)) / factor_ss
    )
    if beta_standard_error == 0.0:
        return alpha, beta, None
    beta_t_stat = beta / beta_standard_error
    if not math.isfinite(beta_t_stat):
        return alpha, beta, None
    return alpha, beta, beta_t_stat


def _centered_sum_squares(values: Sequence[float]) -> float:
    mean = math.fsum(values) / len(values)
    return math.fsum((value - mean) ** 2 for value in values)


def _correlation_sign(value: float | None) -> CrossMarketCorrelationSign:
    if value is None:
        return CrossMarketCorrelationSign.UNAVAILABLE
    if value > 0.0:
        return CrossMarketCorrelationSign.POSITIVE
    if value < 0.0:
        return CrossMarketCorrelationSign.NEGATIVE
    return CrossMarketCorrelationSign.NEUTRAL


def _sign_stability(
    signs: tuple[
        CrossMarketCorrelationSign,
        CrossMarketCorrelationSign,
        CrossMarketCorrelationSign,
    ],
) -> tuple[CrossMarketSignRegime, float | None]:
    if CrossMarketCorrelationSign.UNAVAILABLE in signs:
        return CrossMarketSignRegime.UNAVAILABLE, None
    counts = Counter(signs)
    stability = max(counts.values()) / len(signs)
    if len(counts) > 1:
        return CrossMarketSignRegime.MIXED, stability
    only_sign = signs[0]
    if only_sign is CrossMarketCorrelationSign.POSITIVE:
        return CrossMarketSignRegime.STABLE_POSITIVE, stability
    if only_sign is CrossMarketCorrelationSign.NEGATIVE:
        return CrossMarketSignRegime.STABLE_NEGATIVE, stability
    return CrossMarketSignRegime.STABLE_NEUTRAL, stability


def _risk_alignment(
    sign_regime: CrossMarketSignRegime,
    risk_direction: CrossMarketRiskDirection,
) -> CrossMarketRiskAlignment:
    positive_relation = sign_regime is CrossMarketSignRegime.STABLE_POSITIVE
    negative_relation = sign_regime is CrossMarketSignRegime.STABLE_NEGATIVE
    if not positive_relation and not negative_relation:
        return CrossMarketRiskAlignment.UNSTABLE_OR_NEUTRAL
    if risk_direction is CrossMarketRiskDirection.POSITIVE_IS_RISK_ON:
        return (
            CrossMarketRiskAlignment.RISK_ON_SENSITIVE
            if positive_relation
            else CrossMarketRiskAlignment.RISK_OFF_SENSITIVE
        )
    return (
        CrossMarketRiskAlignment.RISK_OFF_SENSITIVE
        if positive_relation
        else CrossMarketRiskAlignment.RISK_ON_SENSITIVE
    )


def _finite_float(value: Numeric, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (Decimal, float)):
        _raise_input(
            CrossMarketRelationFailureReason.INVALID_NUMERIC_VALUE,
            f"{field_name} must be Decimal or float",
        )
    if isinstance(value, Decimal) and not value.is_finite():
        _raise_input(
            CrossMarketRelationFailureReason.NON_FINITE_VALUE,
            f"{field_name} must be finite",
        )
    converted = float(value)
    if not math.isfinite(converted):
        _raise_input(
            CrossMarketRelationFailureReason.NON_FINITE_VALUE,
            f"{field_name} must be finite after float conversion",
        )
    return converted


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        _raise_input(
            CrossMarketRelationFailureReason.NAIVE_TIMESTAMP,
            f"{field_name} must be timezone-aware",
        )


def _finite_clamped_correlation(value: float) -> float | None:
    if not math.isfinite(value):
        return None
    return min(1.0, max(-1.0, value))


def _raise_input(
    reason: CrossMarketRelationFailureReason,
    message: str,
) -> NoReturn:
    raise CrossMarketRelationInputError(reason, message)


__all__ = [
    "MINIMUM_COMMON_SAMPLES",
    "METHODOLOGY_VERSION",
    "NON_CAUSALITY_NOTICE",
    "Numeric",
    "AlignedFactorReturn",
    "CrossMarketCorrelationSign",
    "CrossMarketFactorRelation",
    "CrossMarketFactorSeries",
    "CrossMarketLagRelation",
    "CrossMarketRelationFailureReason",
    "CrossMarketRelationInputError",
    "CrossMarketRelationsReport",
    "CrossMarketRiskAlignment",
    "CrossMarketRiskDirection",
    "CrossMarketSignRegime",
    "TargetCloseObservation",
    "build_cross_market_relations",
]
