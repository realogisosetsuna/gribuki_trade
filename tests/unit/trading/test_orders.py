from datetime import UTC, datetime
from decimal import Decimal
from unittest import TestCase

from gribuki_trade.domain.orders import OrderIntent, Side


class OrderIntentTests(TestCase):
    def test_valid_limit_order(self) -> None:
        order = OrderIntent(
            client_order_id="test-1",
            account_id="paper",
            strategy_id="smoke",
            symbol="600000.SH",
            side=Side.BUY,
            quantity=100,
            limit_price=Decimal("10.01"),
            created_at=datetime.now(UTC),
        )

        self.assertEqual(order.quantity, 100)

    def test_rejects_non_positive_quantity(self) -> None:
        with self.assertRaisesRegex(ValueError, "quantity must be positive"):
            OrderIntent(
                client_order_id="test-2",
                account_id="paper",
                strategy_id="smoke",
                symbol="600000.SH",
                side=Side.BUY,
                quantity=0,
                limit_price=Decimal("10.01"),
                created_at=datetime.now(UTC),
            )

    def test_normalizes_fractional_quantity_to_decimal(self) -> None:
        order = OrderIntent(
            client_order_id="crypto-1",
            account_id="paper",
            strategy_id="smoke",
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity="0.00125000",
            limit_price="65000.10",
            created_at=datetime.now(UTC),
        )

        self.assertEqual(order.quantity, Decimal("0.00125000"))
        self.assertEqual(order.limit_price, Decimal("65000.10"))
