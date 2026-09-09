from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from gribuki_trade.adapters.binance import BinanceEnvironment, BinanceOrderSnapshot
from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.services.binance.binance_execution_policy import (
    merge_exchange_orders,
    normalize_now,
    validate_order,
)


@dataclass
class Component:
    environment: BinanceEnvironment


def snapshot(client_id: str, status: OrderStatus) -> BinanceOrderSnapshot:
    return BinanceOrderSnapshot(
        symbol="BTCUSDT",
        client_order_id=client_id,
        order_id=1,
        status=status,
        exchange_status=status.value,
        side=Side.BUY,
        price=Decimal("100"),
        original_quantity=Decimal("1"),
        executed_quantity=Decimal("0"),
    )


def test_policy_normalizes_clock_and_keeps_latest_order_snapshot() -> None:
    value = normalize_now(datetime(2026, 9, 9, 8, 0, tzinfo=UTC))
    assert value.tzinfo is UTC
    merged = merge_exchange_orders(
        [snapshot("client-1", OrderStatus.ACCEPTED), snapshot("client-1", OrderStatus.FILLED)]
    )
    assert merged["client-1"].status is OrderStatus.FILLED


def test_policy_rejects_account_or_symbol_mismatch() -> None:
    order = OrderIntent(
        client_order_id="client-1",
        account_id="account-a",
        strategy_id="strategy",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("1"),
        limit_price=Decimal("100"),
        created_at=datetime(2026, 9, 9, tzinfo=UTC),
    )
    validate_order(order, account_id="account-a", symbols=frozenset({"BTCUSDT"}))
    with pytest.raises(ValueError):
        validate_order(order, account_id="account-b", symbols=frozenset({"BTCUSDT"}))
    with pytest.raises(ValueError):
        validate_order(order, account_id="account-a", symbols=frozenset({"ETHUSDT"}))
