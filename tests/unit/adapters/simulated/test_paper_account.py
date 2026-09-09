from datetime import UTC, datetime
from decimal import Decimal

import pytest

from gribuki_trade.adapters.simulated.paper_account import (
    InsufficientPaperBalance,
    PaperFeeSchedule,
    PaperLiquidityRole,
    PaperReservationStatus,
    PaperSpotAccount,
    SpotSymbolAssets,
)
from gribuki_trade.domain.orders import OrderIntent, Side

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)


def order(
    order_id: str,
    *,
    side: Side,
    quantity: str,
    price: str,
) -> OrderIntent:
    return OrderIntent(
        client_order_id=order_id,
        account_id="paper-spot",
        strategy_id="unit",
        symbol="BTCUSDT",
        side=side,
        quantity=Decimal(quantity),
        limit_price=Decimal(price),
        created_at=NOW,
    )


def account(*, usdt: str = "1000", btc: str = "1") -> PaperSpotAccount:
    return PaperSpotAccount(
        account_id="paper-spot",
        initial_balances={"USDT": usdt, "BTC": btc},
        symbol_assets={"BTCUSDT": SpotSymbolAssets("BTC", "USDT")},
        fees=PaperFeeSchedule(
            maker_rate=Decimal("0.0005"),
            taker_rate=Decimal("0.001"),
        ),
    )


def test_buy_reserves_limit_notional_and_fee_buffer_then_partially_fills() -> None:
    ledger = account()
    intent = order("buy-1", side=Side.BUY, quantity="1", price="100")

    reserved = ledger.reserve_order(intent)

    assert reserved.locked_asset == "USDT"
    assert reserved.locked_amount == Decimal("100.100")
    assert ledger.balance("USDT").free == Decimal("899.900")
    assert ledger.balance("USDT").locked == Decimal("100.100")

    fill = ledger.apply_fill(
        "buy-1",
        quantity="0.4",
        price="99",
        fill_id="fill-buy-1",
        liquidity_role=PaperLiquidityRole.TAKER,
    )

    assert fill.notional_quote == Decimal("39.6")
    assert fill.fee_asset == "USDT"
    assert fill.fee_amount == Decimal("0.0396")
    assert fill.released_amount == Decimal("0.4004")
    assert ledger.balance("BTC").free == Decimal("1.4")
    assert ledger.balance("USDT").free == Decimal("900.3004")
    assert ledger.balance("USDT").locked == Decimal("60.0600")
    assert ledger.reservation("buy-1").remaining_quantity == Decimal("0.6")


def test_final_buy_fill_uses_maker_rate_and_clears_locked_balance() -> None:
    ledger = account()
    ledger.reserve_order(order("buy-1", side=Side.BUY, quantity="1", price="100"))
    ledger.apply_fill(
        "buy-1",
        quantity="0.4",
        price="99",
        fill_id="fill-buy-1",
        liquidity_role=PaperLiquidityRole.TAKER,
    )

    receipt = ledger.apply_fill(
        "buy-1",
        quantity="0.6",
        price="100",
        fill_id="fill-buy-2",
        liquidity_role=PaperLiquidityRole.MAKER,
    )

    assert receipt.fee_amount == Decimal("0.03000")
    assert receipt.released_amount == Decimal("0.03000")
    assert ledger.balance("USDT").locked == 0
    assert ledger.balance("USDT").free == Decimal("900.3304")
    assert ledger.balance("BTC").free == Decimal("2.0")
    reservation = ledger.reservation("buy-1")
    assert reservation.status is PaperReservationStatus.FILLED
    assert reservation.locked_amount == 0


def test_sell_reserves_base_charges_quote_fee_and_cancel_releases_remainder() -> None:
    ledger = account()
    ledger.reserve_order(order("sell-1", side=Side.SELL, quantity="0.5", price="100"))

    assert ledger.balance("BTC").free == Decimal("0.5")
    assert ledger.balance("BTC").locked == Decimal("0.5")

    receipt = ledger.apply_fill(
        "sell-1",
        quantity="0.2",
        price="101",
        fill_id="fill-sell-1",
        liquidity_role=PaperLiquidityRole.MAKER,
    )
    canceled = ledger.cancel_order("sell-1")

    assert receipt.fee_amount == Decimal("0.01010")
    assert ledger.balance("USDT").free == Decimal("1020.18990")
    assert ledger.balance("BTC").free == Decimal("0.8")
    assert ledger.balance("BTC").locked == 0
    assert canceled.status is PaperReservationStatus.CANCELED
    assert canceled.remaining_quantity == Decimal("0.3")


def test_insufficient_balance_is_atomic_and_never_goes_negative() -> None:
    ledger = account(usdt="100")
    before = ledger.snapshot()

    with pytest.raises(InsufficientPaperBalance, match="insufficient free USDT"):
        ledger.reserve_order(order("too-large", side=Side.BUY, quantity="1", price="100"))

    assert ledger.snapshot() == before
    assert ledger.reservation("too-large") is None
    assert all(balance.free >= 0 and balance.locked >= 0 for balance in ledger.balances())


def test_duplicate_order_and_fill_are_idempotent_but_conflicts_are_rejected() -> None:
    ledger = account()
    intent = order("buy-1", side=Side.BUY, quantity="1", price="100")
    first_reservation = ledger.reserve_order(intent)
    assert ledger.reserve_order(intent) == first_reservation

    first = ledger.apply_fill(
        "buy-1", quantity="0.5", price="100", fill_id="same-fill"
    )
    snapshot = ledger.snapshot()
    assert (
        ledger.apply_fill("buy-1", quantity="0.5", price="100", fill_id="same-fill")
        == first
    )
    assert ledger.snapshot() == snapshot

    with pytest.raises(ValueError, match="conflicting contents"):
        ledger.apply_fill("buy-1", quantity="0.4", price="100", fill_id="same-fill")
    with pytest.raises(ValueError, match="different order"):
        ledger.reserve_order(order("buy-1", side=Side.BUY, quantity="2", price="100"))


def test_invalid_fill_price_overfill_and_undersized_fee_buffer_are_rejected() -> None:
    ledger = account()
    ledger.reserve_order(order("buy-1", side=Side.BUY, quantity="1", price="100"))

    with pytest.raises(ValueError, match="must not exceed"):
        ledger.apply_fill("buy-1", quantity="0.1", price="101", fill_id="bad-price")
    with pytest.raises(ValueError, match="overfill"):
        ledger.apply_fill("buy-1", quantity="2", price="100", fill_id="overfill")
    assert ledger.balance("USDT").locked == Decimal("100.100")

    with pytest.raises(ValueError, match="must cover"):
        PaperFeeSchedule(
            maker_rate=Decimal("0.001"),
            taker_rate=Decimal("0.002"),
            buy_fee_buffer_rate=Decimal("0.001"),
        )


def test_snapshot_is_sorted_and_unknown_assets_are_zero() -> None:
    ledger = account()
    snapshot = ledger.snapshot()

    assert [balance.asset for balance in snapshot.balances] == ["BTC", "USDT"]
    assert snapshot.balance("ETH").total == 0
