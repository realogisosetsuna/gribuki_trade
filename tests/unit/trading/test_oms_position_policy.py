from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from gribuki_trade.domain.orders import Side
from gribuki_trade.trading.core.models import ExecutionFill
from gribuki_trade.trading.core.oms_position_policy import project_position_fill


def _fill(side: Side, quantity: str, price: str) -> ExecutionFill:
    return ExecutionFill(
        fill_id=f"fill-{side.value}-{quantity}-{price}",
        client_order_id="order-1",
        account_id="account-1",
        symbol="BTCUSDT",
        side=side,
        quantity=Decimal(quantity),
        price=Decimal(price),
        occurred_at=datetime(2026, 8, 13, 1, 0, tzinfo=UTC),
    )


def test_buy_then_partial_sell_updates_average_and_realized_pnl() -> None:
    opened = project_position_fill(_fill(Side.BUY, "2", "100"))
    result = project_position_fill(
        _fill(Side.SELL, "1", "120"),
        old_quantity=opened.quantity,
        old_average_entry_price=opened.average_entry_price,
        old_realized_pnl=opened.realized_pnl,
    )

    assert result.quantity == Decimal("1")
    assert result.average_entry_price == Decimal("100")
    assert result.realized_pnl == Decimal("20")


def test_reversal_uses_fill_price_for_new_side_average() -> None:
    result = project_position_fill(
        _fill(Side.SELL, "3", "90"),
        old_quantity=Decimal("2"),
        old_average_entry_price=Decimal("100"),
    )

    assert result.quantity == Decimal("-1")
    assert result.average_entry_price == Decimal("90")
    assert result.realized_pnl == Decimal("-20")
