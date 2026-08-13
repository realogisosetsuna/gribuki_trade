from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.backtest import (
    BacktestBarEvent,
    CryptoBacktestConfig,
    CryptoBacktestEngine,
    CryptoBar,
    CryptoFeeConfig,
    CryptoOrderRequest,
    CryptoOrderType,
    InsufficientBalanceError,
    LiquidityRole,
    SimulatedOrderStatus,
    SpotLedger,
)
from gribuki_trade.domain.orders import Side

START = datetime(2026, 1, 1, tzinfo=UTC)


def bar(
    index: int,
    *,
    open_price: str,
    high: str,
    low: str,
    close: str,
    volume: str = "100",
) -> CryptoBar:
    open_time = START + timedelta(hours=index)
    close_time = open_time + timedelta(hours=1)
    return CryptoBar(
        symbol="BTCUSDT",
        open_time=open_time,
        close_time=close_time,
        available_at=close_time,
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(volume),
    )


def replay_bars() -> tuple[CryptoBar, ...]:
    return (
        bar(0, open_price="100", high="101", low="99", close="100"),
        bar(1, open_price="101", high="106", low="100", close="105"),
        bar(2, open_price="108", high="112", low="107", close="109"),
    )


def test_decimal_spot_ledger_applies_quote_fees_atomically() -> None:
    ledger = SpotLedger({"USDT": Decimal("1000"), "BTC": Decimal("0")})

    ledger.apply_fill(
        side=Side.BUY,
        base_asset="BTC",
        quote_asset="USDT",
        quantity=Decimal("2.5"),
        price=Decimal("100"),
        fee_quote=Decimal("0.25"),
    )
    ledger.apply_fill(
        side=Side.SELL,
        base_asset="BTC",
        quote_asset="USDT",
        quantity=Decimal("1.25"),
        price=Decimal("120"),
        fee_quote=Decimal("0.15"),
    )

    assert ledger.balance("BTC") == Decimal("1.25")
    assert ledger.balance("USDT") == Decimal("899.60")
    with pytest.raises(InsufficientBalanceError, match="insufficient BTC"):
        ledger.apply_fill(
            side=Side.SELL,
            base_asset="BTC",
            quote_asset="USDT",
            quantity=Decimal("2"),
            price=Decimal("120"),
            fee_quote=Decimal("0.24"),
        )
    assert ledger.balance("BTC") == Decimal("1.25")
    assert ledger.balance("USDT") == Decimal("899.60")


def test_fee_schedule_applies_maker_taker_rates_and_optional_discount() -> None:
    fees = CryptoFeeConfig(
        maker_rate=Decimal("0.001"),
        taker_rate=Decimal("0.002"),
        discount_fraction=Decimal("0.25"),
    )

    assert fees.effective_rate(LiquidityRole.MAKER) == Decimal("0.00075")
    assert fees.effective_rate(LiquidityRole.TAKER) == Decimal("0.00150")


def test_replay_delays_orders_one_bar_and_accounts_for_fees_and_slippage() -> None:
    config = CryptoBacktestConfig(
        symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        fees=CryptoFeeConfig(),
        market_slippage_rate=Decimal("0.01"),
    )
    observed_history_lengths: list[int] = []

    def strategy(event: BacktestBarEvent) -> tuple[CryptoOrderRequest, ...]:
        observed_history_lengths.append(len(event.history))
        if len(event.history) == 1:
            return (
                CryptoOrderRequest(
                    order_id="buy-next-open",
                    side=Side.BUY,
                    order_type=CryptoOrderType.MARKET,
                    quantity=Decimal("1"),
                ),
            )
        if len(event.history) == 2:
            assert event.portfolio.balance("BTC") == Decimal("1")
            return (
                CryptoOrderRequest(
                    order_id="sell-on-touch",
                    side=Side.SELL,
                    order_type=CryptoOrderType.LIMIT,
                    quantity=Decimal("1"),
                    limit_price=Decimal("110"),
                ),
            )
        return ()

    report = CryptoBacktestEngine(config).run(
        replay_bars(),
        strategy,
        initial_balances={"USDT": Decimal("1000"), "BTC": Decimal("0")},
    )

    assert observed_history_lengths == [1, 2, 3]
    assert [fill.executed_at for fill in report.fills] == [
        replay_bars()[1].open_time,
        replay_bars()[2].close_time,
    ]
    assert report.fills[0].price == Decimal("102.01")
    assert report.fills[0].liquidity_role is LiquidityRole.TAKER
    assert report.fills[1].price == Decimal("110")
    assert report.fills[1].liquidity_role is LiquidityRole.MAKER
    assert report.total_fees_quote == Decimal("0.21201")
    assert report.final_equity_quote == Decimal("1007.77799")
    assert report.net_profit_quote == Decimal("7.77799")
    assert report.total_return == Decimal("0.00777799")
    assert report.trade_count == 2
    assert report.pending_order_ids == ()
    assert report.final_portfolio.balance("BTC") == 0


def test_limit_fill_uses_limit_not_optimistic_gap_price() -> None:
    bars = (
        bar(0, open_price="105", high="106", low="104", close="105"),
        bar(1, open_price="90", high="95", low="89", close="92"),
    )

    def strategy(event: BacktestBarEvent) -> tuple[CryptoOrderRequest, ...]:
        if len(event.history) != 1:
            return ()
        return (
            CryptoOrderRequest(
                order_id="gap-buy",
                side=Side.BUY,
                order_type=CryptoOrderType.LIMIT,
                quantity=Decimal("1"),
                limit_price=Decimal("100"),
            ),
        )

    report = CryptoBacktestEngine(CryptoBacktestConfig("BTCUSDT", "BTC", "USDT")).run(
        bars,
        strategy,
        initial_balances={"USDT": Decimal("1000")},
    )

    assert report.fills[0].price == Decimal("100")
    assert report.fills[0].liquidity_role is LiquidityRole.TAKER
    assert report.final_equity_quote == Decimal("991.900")


def test_volume_cap_partially_fills_limit_and_shares_bar_liquidity() -> None:
    bars = (
        bar(0, open_price="100", high="101", low="99", close="100"),
        bar(1, open_price="101", high="102", low="99", close="100", volume="2"),
        bar(2, open_price="101", high="102", low="99", close="100", volume="2"),
    )

    def strategy(event: BacktestBarEvent) -> tuple[CryptoOrderRequest, ...]:
        if len(event.history) != 1:
            return ()
        return (
            CryptoOrderRequest(
                order_id="large-limit",
                side=Side.BUY,
                order_type=CryptoOrderType.LIMIT,
                quantity=Decimal("3"),
                limit_price=Decimal("100"),
            ),
        )

    report = CryptoBacktestEngine(
        CryptoBacktestConfig(
            "BTCUSDT",
            "BTC",
            "USDT",
            max_bar_volume_fraction=Decimal("0.5"),
        )
    ).run(
        bars,
        strategy,
        initial_balances={"USDT": Decimal("1000")},
    )

    assert [fill.quantity for fill in report.fills] == [Decimal("1"), Decimal("1")]
    assert report.orders[0].status is SimulatedOrderStatus.PARTIALLY_FILLED
    assert report.orders[0].remaining_quantity == Decimal("1")
    assert report.pending_order_ids == ("large-limit",)


def test_report_calculates_mark_to_market_drawdown() -> None:
    bars = (
        bar(0, open_price="100", high="101", low="99", close="100"),
        bar(1, open_price="100", high="101", low="79", close="80"),
    )

    def buy(event: BacktestBarEvent) -> tuple[CryptoOrderRequest, ...]:
        if len(event.history) == 1:
            return (
                CryptoOrderRequest(
                    order_id="buy",
                    side=Side.BUY,
                    order_type=CryptoOrderType.MARKET,
                    quantity=Decimal("1"),
                ),
            )
        return ()

    report = CryptoBacktestEngine(
        CryptoBacktestConfig(
            "BTCUSDT",
            "BTC",
            "USDT",
            fees=CryptoFeeConfig(maker_rate=Decimal("0"), taker_rate=Decimal("0")),
        )
    ).run(bars, buy, initial_balances={"USDT": Decimal("100")})

    assert report.final_equity_quote == Decimal("80")
    assert report.max_drawdown == Decimal("0.2")


def test_point_in_time_validation_rejects_unsafe_history() -> None:
    engine = CryptoBacktestEngine(CryptoBacktestConfig("BTCUSDT", "BTC", "USDT"))

    def idle(_: BacktestBarEvent) -> tuple[CryptoOrderRequest, ...]:
        return ()

    incomplete = replace(replay_bars()[0], complete=False)
    with pytest.raises(ValueError, match="completed bars"):
        engine.run((incomplete,), idle, initial_balances={"USDT": Decimal("1")})

    delayed = replace(
        replay_bars()[0], available_at=replay_bars()[1].open_time + timedelta(seconds=1)
    )
    with pytest.raises(ValueError, match="before the following bar opens"):
        engine.run(
            (delayed, replay_bars()[1]),
            idle,
            initial_balances={"USDT": Decimal("1")},
        )

    with pytest.raises(ValueError, match="strictly ordered and unique"):
        engine.run(
            (replay_bars()[0], replay_bars()[0]),
            idle,
            initial_balances={"USDT": Decimal("1")},
        )


def test_replay_is_deterministic_and_does_not_retain_engine_state() -> None:
    engine = CryptoBacktestEngine(
        CryptoBacktestConfig(
            "BTCUSDT",
            "BTC",
            "USDT",
            fees=CryptoFeeConfig(maker_rate=Decimal("0"), taker_rate=Decimal("0")),
        )
    )

    def strategy(event: BacktestBarEvent) -> tuple[CryptoOrderRequest, ...]:
        if len(event.history) == 1:
            return (
                CryptoOrderRequest(
                    order_id="deterministic-buy",
                    side=Side.BUY,
                    order_type=CryptoOrderType.MARKET,
                    quantity=Decimal("1"),
                ),
            )
        return ()

    first = engine.run(replay_bars(), strategy, initial_balances={"USDT": Decimal("1000")})
    second = engine.run(replay_bars(), strategy, initial_balances={"USDT": Decimal("1000")})

    assert first == second
