from decimal import Decimal
from unittest import TestCase

from gribuki_trade.adapters.binance.spot.parsing import (
    map_order_status,
    normalize_symbol,
    parameter_text,
    parse_levels,
    parse_order_snapshot,
    sanitize_message,
)
from gribuki_trade.domain.orders import OrderStatus


class BinanceSpotParsingTests(TestCase):
    def test_scalar_helpers_are_transport_independent(self) -> None:
        self.assertEqual(normalize_symbol(" btcusdt "), "BTCUSDT")
        self.assertEqual(parameter_text(Decimal("1.2300")), "1.2300")
        self.assertEqual(parameter_text(False), "false")
        self.assertEqual(sanitize_message("secret=abc", ["abc"]), "secret=<redacted>")

    def test_order_book_levels_parse_without_gateway_state(self) -> None:
        levels = parse_levels([["100.00", "0.25"], ["99.50", "1.0"]], "bids")
        self.assertEqual(levels[0].price, Decimal("100.00"))
        self.assertEqual(levels[1].quantity, Decimal("1.0"))

    def test_order_snapshot_and_status_are_transport_independent(self) -> None:
        snapshot = parse_order_snapshot(
            {
                "symbol": "BTCUSDT",
                "clientOrderId": "client-1",
                "orderId": 42,
                "status": "PARTIALLY_FILLED",
                "side": "BUY",
                "price": "100.00",
                "origQty": "0.25",
                "executedQty": "0.10",
                "cummulativeQuoteQty": "10.000",
                "transactTime": 123,
            },
            fallback_symbol="ETHUSDT",
        )
        self.assertEqual(snapshot.status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(snapshot.executed_quantity, Decimal("0.10"))
        self.assertEqual(map_order_status("FILLED"), OrderStatus.FILLED)
