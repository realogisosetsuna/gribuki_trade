from decimal import Decimal
from unittest import TestCase

from gribuki_trade.adapters.binance.spot_parsing import (
    normalize_symbol,
    parameter_text,
    parse_levels,
    sanitize_message,
)


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
