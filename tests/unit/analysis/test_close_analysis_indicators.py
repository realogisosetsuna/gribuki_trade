from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.features import close_analysis as facade
from gribuki_trade.features import close_analysis_indicators as indicators


def _bars(count: int = 30) -> tuple[DailyBar, ...]:
    output: list[DailyBar] = []
    previous = Decimal("10")
    for index in range(count):
        close = Decimal("10") + Decimal(index) * Decimal("0.1")
        output.append(
            DailyBar(
                symbol="600000.SH",
                trade_date=date(2026, 1, 1) + timedelta(days=index),
                open=close - Decimal("0.02"),
                high=close + Decimal("0.05"),
                low=close - Decimal("0.05"),
                close=close,
                previous_close=previous,
                volume=100_000,
                amount=close * Decimal("100000"),
                turnover_percent=Decimal("1"),
                is_trading=True,
                is_st=False,
                adjustment=PriceAdjustment.NONE,
            )
        )
        previous = close
    return tuple(output)


def test_facade_keeps_historical_indicator_private_names() -> None:
    assert facade._wilder_adx is indicators._wilder_adx
    assert facade._relative_strength_index is indicators._relative_strength_index
    assert facade._amihud_illiquidity is indicators._amihud_illiquidity
    assert facade._turnover_state is indicators._turnover_state


def test_indicator_module_calculates_pure_decimal_metrics() -> None:
    bars = _bars()
    closes = tuple(item.close for item in bars if item.close is not None)

    assert indicators._mean((Decimal("1"), Decimal("3"))) == Decimal("2")
    assert indicators._relative_strength_index(closes, 14) == Decimal("100")
    assert indicators._average_true_range(bars, 14) > Decimal("0")
    adx, positive_di, negative_di = indicators._wilder_adx(bars, 14)
    assert adx >= Decimal("0")
    assert positive_di > negative_di
    value, coverage = indicators._turnover_state_with_coverage(bars, 20)
    assert value == Decimal("1")
    assert coverage == Decimal("1")
