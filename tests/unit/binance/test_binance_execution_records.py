from datetime import UTC, datetime
from decimal import Decimal

from gribuki_trade.adapters.binance import BinanceOrderSnapshot
from gribuki_trade.domain.orders import OrderStatus
from gribuki_trade.services.binance.binance_execution_records import (
    average_price,
    balance_digest,
    datetime_from_ms,
    exchange_order_rank,
)
from gribuki_trade.trading import BalanceValue


def snapshot(*, status: OrderStatus = OrderStatus.PARTIALLY_FILLED) -> BinanceOrderSnapshot:
    return BinanceOrderSnapshot(
        symbol="BTCUSDT",
        client_order_id="order-1",
        order_id=42,
        status=status,
        exchange_status=status.value,
        side=None,
        price=Decimal("100"),
        original_quantity=Decimal("2"),
        executed_quantity=Decimal("1"),
        transact_time_ms=1_000,
        cumulative_quote_quantity=Decimal("99"),
    )


def test_snapshot_projection_helpers_are_deterministic() -> None:
    assert average_price(snapshot()) == Decimal("99")
    assert exchange_order_rank(snapshot())[0] == 1_000
    assert exchange_order_rank(snapshot(status=OrderStatus.FILLED))[1] == 1
    assert datetime_from_ms(1_000, fallback=datetime.now(UTC)) == datetime.fromtimestamp(
        1, tz=UTC
    )


def test_balance_digest_is_order_independent() -> None:
    first = (
        BalanceValue(asset="USDT", free=Decimal("10"), locked=Decimal("1")),
        BalanceValue(asset="BTC", free=Decimal("2"), locked=Decimal("0")),
    )
    assert balance_digest(first) == balance_digest(tuple(reversed(first)))
