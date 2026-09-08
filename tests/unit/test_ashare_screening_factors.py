from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from gribuki_trade.adapters.ashare.screening_factors import (
    HistoryBar,
    average_amount_20,
    calculate_factors,
    corporate_action_guard,
    empty_factor_values,
    factor_values_with_average_amount,
)
from gribuki_trade.ports.ashare_screening import ScreeningFactorId


def _bars(count: int = 201) -> tuple[HistoryBar, ...]:
    start = date(2026, 1, 1)
    return tuple(
        HistoryBar(
            trade_date=start + timedelta(days=index),
            open=Decimal("10") + Decimal(index) / Decimal("100"),
            high=Decimal("10.2") + Decimal(index) / Decimal("100"),
            low=Decimal("9.8") + Decimal(index) / Decimal("100"),
            close=Decimal("10") + Decimal(index) / Decimal("100"),
            previous_close=(
                None
                if index == 0
                else Decimal("10") + Decimal(index - 1) / Decimal("100")
            ),
            volume=Decimal("1000") + Decimal(index),
            amount=Decimal("100000") + Decimal(index) * Decimal("100"),
        )
        for index in range(count)
    )


def test_factor_calculator_is_deterministic_and_keeps_all_factor_ids() -> None:
    bars = _bars()
    values = {item.factor_id: item.value for item in calculate_factors(bars)}

    assert set(values) == set(ScreeningFactorId)
    assert all(value is not None for value in values.values())
    assert values[ScreeningFactorId.MOMENTUM_20] is not None
    assert values[ScreeningFactorId.MOMENTUM_20] > 0
    assert average_amount_20(bars) == sum(
        (item.amount for item in bars[-20:]), Decimal(0)
    ) / Decimal(20)


def test_corporate_action_guard_reports_discontinuity_and_incomplete_coverage() -> None:
    bars = list(_bars())
    bars[150] = HistoryBar(
        trade_date=bars[150].trade_date,
        open=bars[150].open,
        high=bars[150].high,
        low=bars[150].low,
        close=bars[150].close,
        previous_close=Decimal("5"),
        volume=bars[150].volume,
        amount=bars[150].amount,
    )
    assert corporate_action_guard(tuple(bars)) == (
        f"CORPORATE_ACTION_DISCONTINUITY:{bars[150].trade_date.isoformat()}"
    )

    incomplete = tuple(
        item if index == len(bars) - 1 else HistoryBar(
            trade_date=item.trade_date,
            open=item.open,
            high=item.high,
            low=item.low,
            close=item.close,
            previous_close=None,
            volume=item.volume,
            amount=item.amount,
        )
        for index, item in enumerate(_bars())
    )
    assert corporate_action_guard(incomplete) == "CORPORATE_ACTION_GUARD_INCOMPLETE:0.005"


def test_degraded_vectors_are_explicit_and_preserve_liquidity_only() -> None:
    empty = empty_factor_values()
    assert len(empty) == len(ScreeningFactorId)
    assert all(item.value is None for item in empty)

    preserved = factor_values_with_average_amount(Decimal("123.45"))
    by_id = {item.factor_id: item.value for item in preserved}
    assert by_id[ScreeningFactorId.AVERAGE_AMOUNT_20_CNY] == 123.45
    assert sum(value is not None for value in by_id.values()) == 1
