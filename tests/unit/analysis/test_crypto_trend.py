from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.backtest import BacktestBarEvent, CryptoBar, PortfolioSnapshot
from gribuki_trade.domain.orders import Side
from gribuki_trade.strategy import (
    CryptoTrendConfig,
    CryptoTrendRegime,
    MovingAverageCryptoTrendStrategy,
)

START = datetime(2026, 1, 1, tzinfo=UTC)


def bar(index: int, close: str) -> CryptoBar:
    open_time = START + timedelta(hours=index)
    available_at = open_time + timedelta(hours=1)
    price = Decimal(close)
    return CryptoBar(
        symbol="BTCUSDT",
        open_time=open_time,
        close_time=available_at,
        available_at=available_at,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("100"),
    )


def event(
    closes: tuple[str, ...],
    *,
    btc: str = "0",
    usdt: str = "1000",
) -> BacktestBarEvent:
    history = tuple(bar(index, close) for index, close in enumerate(closes))
    return BacktestBarEvent(
        decision_time=history[-1].available_at,
        bar=history[-1],
        history=history,
        portfolio=PortfolioSnapshot(
            (("BTC", Decimal(btc)), ("USDT", Decimal(usdt)))
        ),
    )


def strategy() -> MovingAverageCryptoTrendStrategy:
    return MovingAverageCryptoTrendStrategy(
        symbol="btcusdt",
        base_asset="btc",
        quote_asset="usdt",
        config=CryptoTrendConfig(
            fast_window=2,
            slow_window=3,
            minimum_history=3,
            target_position_fraction=Decimal("0.5"),
            quantity_step=Decimal("0.01"),
            minimum_order_quantity=Decimal("0.01"),
        ),
    )


def test_strategy_warms_up_then_emits_decimal_market_target_order() -> None:
    selected = strategy()

    warmup = selected.evaluate(event(("10", "11")))
    decision = selected.evaluate(event(("10", "11", "12")))

    assert warmup.regime is CryptoTrendRegime.WARMUP
    assert warmup.order is None
    assert decision.regime is CryptoTrendRegime.LONG
    assert decision.fast_average == Decimal("11.5")
    assert decision.slow_average == Decimal("11")
    assert decision.target_base_quantity == Decimal("41.66")
    assert decision.order is not None
    assert decision.order.side is Side.BUY
    assert decision.order.quantity == Decimal("41.66")
    assert decision.order.limit_price is None


def test_target_position_avoids_duplicate_rebalance_and_id_is_stable() -> None:
    selected = strategy()
    source_event = event(("10", "11", "12"))

    first = tuple(selected(source_event))
    repeated = tuple(selected(source_event))
    delayed = tuple(
        selected(
            replace(
                source_event,
                decision_time=source_event.decision_time + timedelta(minutes=1),
            )
        )
    )
    at_target = selected.evaluate(
        event(("10", "11", "12"), btc="41.66", usdt="500.08")
    )

    assert len(first) == 1
    assert repeated == first
    assert delayed == first
    assert len(first[0].order_id) <= 36
    assert at_target.order is None
    assert at_target.reason == "WITHIN_REBALANCE_TOLERANCE"


def test_flat_regime_sells_existing_spot_position() -> None:
    decision = strategy().evaluate(event(("12", "11", "10"), btc="2", usdt="0"))

    assert decision.regime is CryptoTrendRegime.FLAT
    assert decision.target_base_quantity == 0
    assert decision.order is not None
    assert decision.order.side is Side.SELL
    assert decision.order.quantity == Decimal("2")


def test_fractional_rebalance_band_avoids_small_churn_but_not_regime_exit() -> None:
    selected = MovingAverageCryptoTrendStrategy(
        symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        config=CryptoTrendConfig(
            fast_window=2,
            slow_window=3,
            minimum_history=3,
            target_position_fraction=Decimal("0.5"),
            rebalance_tolerance_fraction=Decimal("0.02"),
            quantity_step=Decimal("0.01"),
            minimum_order_quantity=Decimal("0.01"),
        ),
    )

    within_band = selected.evaluate(
        event(("10", "11", "12"), btc="41", usdt="508")
    )
    flat = selected.evaluate(event(("12", "11", "10"), btc="41", usdt="508"))

    assert within_band.order is None
    assert within_band.reason == "WITHIN_REBALANCE_TOLERANCE"
    assert flat.order is not None
    assert flat.order.side is Side.SELL


def test_strategy_rejects_incomplete_or_not_yet_available_history() -> None:
    selected = strategy()
    valid = event(("10", "11", "12"))
    incomplete = replace(valid.bar, complete=False)
    incomplete_event = replace(valid, bar=incomplete, history=(*valid.history[:-1], incomplete))
    with pytest.raises(ValueError, match="completed bars"):
        selected(incomplete_event)

    unavailable = replace(
        valid.bar,
        available_at=valid.decision_time + timedelta(seconds=1),
    )
    unavailable_event = replace(
        valid,
        bar=unavailable,
        history=(*valid.history[:-1], unavailable),
    )
    with pytest.raises(ValueError, match="precedes current bar availability"):
        selected(unavailable_event)


def test_strategy_decision_cannot_observe_a_future_crash_bar() -> None:
    selected = strategy()
    point_in_time = event(("10", "11", "12"))

    earlier = selected.evaluate(point_in_time)
    after_crash = selected.evaluate(event(("10", "11", "12", "1")))
    repeated_earlier = selected.evaluate(point_in_time)

    assert earlier.regime is CryptoTrendRegime.LONG
    assert after_crash.regime is CryptoTrendRegime.FLAT
    assert repeated_earlier == earlier


def test_crypto_trend_config_rejects_unsafe_parameters() -> None:
    with pytest.raises(ValueError, match="less than slow_window"):
        CryptoTrendConfig(fast_window=3, slow_window=3)
    with pytest.raises(ValueError, match="at least slow_window"):
        CryptoTrendConfig(fast_window=2, slow_window=3, minimum_history=2)
    with pytest.raises(TypeError, match="must be a Decimal"):
        CryptoTrendConfig(target_position_fraction=0.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="rebalance_tolerance_fraction"):
        CryptoTrendConfig(rebalance_tolerance_fraction=Decimal("1.1"))
