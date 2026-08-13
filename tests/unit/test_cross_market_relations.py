from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.features import (
    AlignedFactorReturn,
    CrossMarketCorrelationSign,
    CrossMarketFactorSeries,
    CrossMarketRelationFailureReason,
    CrossMarketRelationInputError,
    CrossMarketRiskAlignment,
    CrossMarketRiskDirection,
    CrossMarketSignRegime,
    TargetCloseObservation,
    build_cross_market_relations,
)


def synthetic_returns(count: int = 180) -> tuple[Decimal, ...]:
    state = 481_516_234
    values: list[Decimal] = []
    for _ in range(count):
        state = (1_103_515_245 * state + 12_345) % (2**31)
        centered = Decimal(state) / Decimal(2**31) - Decimal("0.5")
        values.append(centered / Decimal("50"))
    return tuple(values)


def target_closes(
    returns: tuple[Decimal, ...],
) -> tuple[TargetCloseObservation, ...]:
    first_date = date(2025, 1, 2)
    close = Decimal("100")
    output = [
        TargetCloseObservation(
            trade_date=first_date,
            close=close,
            decision_at=datetime.combine(first_date, time(15, 30), tzinfo=UTC),
        )
    ]
    for index, value in enumerate(returns, start=1):
        close *= Decimal("1") + value
        trade_date = first_date + timedelta(days=index)
        output.append(
            TargetCloseObservation(
                trade_date=trade_date,
                close=close,
                decision_at=datetime.combine(
                    trade_date,
                    time(15, 30),
                    tzinfo=UTC,
                ),
            )
        )
    return tuple(output)


def factor_series(
    factor_id: str,
    target: tuple[TargetCloseObservation, ...],
    values: tuple[Decimal | float, ...],
    *,
    risk_direction: CrossMarketRiskDirection = (
        CrossMarketRiskDirection.POSITIVE_IS_RISK_ON
    ),
) -> CrossMarketFactorSeries:
    assert len(values) == len(target)
    return CrossMarketFactorSeries(
        factor_id=factor_id,
        risk_direction=risk_direction,
        observations=tuple(
            AlignedFactorReturn(
                target_trade_date=point.trade_date,
                value=value,
                available_at=point.decision_at - timedelta(minutes=1),
            )
            for point, value in zip(target, values, strict=True)
        ),
    )


def same_date_values(
    returns: tuple[Decimal, ...],
    *,
    multiplier: Decimal,
    change_sign_for_last: int = 0,
) -> tuple[Decimal, ...]:
    output = [Decimal("0")]
    for index, value in enumerate(returns, start=1):
        direction = Decimal("-1") if index > len(returns) - change_sign_for_last else Decimal("1")
        noise = Decimal(((index * 19) % 17) - 8) / Decimal("1000000")
        output.append(direction * multiplier * value + noise)
    return tuple(output)


def test_positive_negative_and_changing_sign_relations_are_explicit() -> None:
    returns = synthetic_returns()
    target = target_closes(returns)
    positive = factor_series(
        "SPX",
        target,
        same_date_values(returns, multiplier=Decimal("0.8")),
    )
    negative = factor_series(
        "VIX",
        target,
        same_date_values(returns, multiplier=Decimal("-1")),
        risk_direction=CrossMarketRiskDirection.POSITIVE_IS_RISK_OFF,
    )
    changing = factor_series(
        "REGIME_CHANGE",
        target,
        same_date_values(
            returns,
            multiplier=Decimal("1"),
            change_sign_for_last=20,
        ),
    )

    report = build_cross_market_relations(
        "510300.sh",
        target,
        (positive, negative, changing),
    )

    positive_lag = report.factors[0].lag_0
    assert report.target_symbol == "510300.SH"
    assert report.minimum_common_samples == 120
    assert "not evidence of causation" in report.non_causality_notice
    assert positive_lag.failure_reason is None
    assert positive_lag.common_samples == len(returns)
    assert positive_lag.coverage == 1.0
    assert positive_lag.ewma_correlation_half_life_20 is not None
    assert positive_lag.ewma_correlation_half_life_20 > 0.99
    assert positive_lag.ewma_correlation_half_life_60 is not None
    assert positive_lag.ewma_correlation_half_life_60 > 0.99
    assert positive_lag.beta_120 is not None
    assert positive_lag.beta_120 > 1.2
    assert positive_lag.beta_t_stat_120 is not None
    assert positive_lag.correlation_sign_stability == 1.0
    assert positive_lag.sign_regime is CrossMarketSignRegime.STABLE_POSITIVE
    assert positive_lag.risk_alignment is CrossMarketRiskAlignment.RISK_ON_SENSITIVE

    negative_lag = report.factors[1].lag_0
    assert negative_lag.correlation_sign_20 is CrossMarketCorrelationSign.NEGATIVE
    assert negative_lag.correlation_sign_60 is CrossMarketCorrelationSign.NEGATIVE
    assert negative_lag.correlation_sign_120 is CrossMarketCorrelationSign.NEGATIVE
    assert negative_lag.sign_regime is CrossMarketSignRegime.STABLE_NEGATIVE
    assert negative_lag.risk_alignment is CrossMarketRiskAlignment.RISK_ON_SENSITIVE

    changing_lag = report.factors[2].lag_0
    assert changing_lag.correlation_sign_20 is CrossMarketCorrelationSign.NEGATIVE
    assert changing_lag.correlation_sign_60 is CrossMarketCorrelationSign.POSITIVE
    assert changing_lag.correlation_sign_120 is CrossMarketCorrelationSign.POSITIVE
    assert changing_lag.sign_regime is CrossMarketSignRegime.MIXED
    assert changing_lag.correlation_sign_stability == pytest.approx(2 / 3)
    assert (
        changing_lag.risk_alignment
        is CrossMarketRiskAlignment.UNSTABLE_OR_NEUTRAL
    )


def test_lag_one_uses_previous_supplied_ashare_trade_date() -> None:
    returns = synthetic_returns()
    target = target_closes(returns)
    factor_values: list[Decimal] = []
    for index in range(len(target)):
        if index < len(returns):
            noise = Decimal(((index * 23) % 13) - 6) / Decimal("10000000")
            factor_values.append(returns[index] + noise)
        else:
            factor_values.append(Decimal("0"))
    leading_factor = factor_series("LEADING", target, tuple(factor_values))

    relation = build_cross_market_relations("510300.SH", target, (leading_factor,)).factors[0]

    assert relation.lag_0.lag == 0
    assert relation.lag_1.lag == 1
    assert relation.lag_1.correlation_120 is not None
    assert relation.lag_1.correlation_120 > 0.999
    assert relation.lag_0.correlation_120 is not None
    assert abs(relation.lag_0.correlation_120) < 0.8


def test_missing_dates_reduce_coverage_and_below_120_returns_stable_failure() -> None:
    returns = synthetic_returns(count=160)
    target = target_closes(returns)
    complete = factor_series(
        "PARTIAL",
        target,
        same_date_values(returns, multiplier=Decimal("1")),
    )
    retained = tuple(
        point
        for index, point in enumerate(complete.observations)
        if index == 0 or index % 8 != 0
    )
    partial = replace(complete, observations=retained)
    sparse = replace(
        complete,
        factor_id="SPARSE",
        observations=complete.observations[1:120],
    )

    report = build_cross_market_relations("510300.SH", target, (partial, sparse))

    partial_lag = report.factors[0].lag_0
    assert 120 <= partial_lag.common_samples < partial_lag.eligible_target_samples
    assert 0.0 < partial_lag.coverage < 1.0
    assert partial_lag.failure_reason is None
    sparse_lag = report.factors[1].lag_0
    assert sparse_lag.common_samples == 119
    assert sparse_lag.failure_reason is (
        CrossMarketRelationFailureReason.INSUFFICIENT_COMMON_SAMPLES
    )
    assert sparse_lag.beta_120 is None
    assert sparse_lag.correlation_120 is None


def test_future_available_at_is_rejected_before_calculation() -> None:
    returns = synthetic_returns(count=120)
    target = target_closes(returns)
    factor = factor_series(
        "FUTURE",
        target,
        same_date_values(returns, multiplier=Decimal("1")),
    )
    future_point = replace(
        factor.observations[50],
        available_at=target[50].decision_at + timedelta(microseconds=1),
    )
    observations = list(factor.observations)
    observations[50] = future_point

    with pytest.raises(CrossMarketRelationInputError) as caught:
        build_cross_market_relations(
            "510300.SH",
            target,
            (replace(factor, observations=tuple(observations)),),
        )

    assert caught.value.failure_reason is (
        CrossMarketRelationFailureReason.FUTURE_AVAILABLE_AT
    )


@pytest.mark.parametrize("invalid", [float("inf"), float("nan"), Decimal("NaN")])
def test_non_finite_decimal_or_float_is_rejected(invalid: Decimal | float) -> None:
    returns = synthetic_returns(count=120)
    target = target_closes(returns)
    values = list(same_date_values(returns, multiplier=Decimal("1")))
    values[10] = invalid
    factor = factor_series("INVALID", target, tuple(values))

    with pytest.raises(CrossMarketRelationInputError) as caught:
        build_cross_market_relations("510300.SH", target, (factor,))

    assert caught.value.failure_reason is CrossMarketRelationFailureReason.NON_FINITE_VALUE


def test_unordered_or_duplicate_factor_dates_are_rejected() -> None:
    returns = synthetic_returns(count=120)
    target = target_closes(returns)
    factor = factor_series(
        "UNORDERED",
        target,
        same_date_values(returns, multiplier=Decimal("1")),
    )
    observations = list(factor.observations)
    observations[20], observations[21] = observations[21], observations[20]

    with pytest.raises(CrossMarketRelationInputError) as caught:
        build_cross_market_relations(
            "510300.SH",
            target,
            (replace(factor, observations=tuple(observations)),),
        )

    assert caught.value.failure_reason is (
        CrossMarketRelationFailureReason.FACTOR_DATES_NOT_STRICTLY_ORDERED
    )
