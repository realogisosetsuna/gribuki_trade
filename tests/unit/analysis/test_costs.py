from decimal import Decimal
from unittest import TestCase

from gribuki_trade.backtest.costs import (
    InstrumentType,
    TradingCostConfig,
    calculate_trade_cost,
)
from gribuki_trade.domain.orders import Side


class TradeCostTests(TestCase):
    def test_small_stock_order_uses_minimum_commission(self) -> None:
        cost = calculate_trade_cost(
            side=Side.BUY,
            price=Decimal("10"),
            quantity=100,
            instrument_type=InstrumentType.STOCK,
        )

        self.assertEqual(cost.trade_value, Decimal("1000.00"))
        self.assertEqual(cost.commission, Decimal("5.00"))
        self.assertEqual(cost.transfer_fee, Decimal("0.00"))
        self.assertEqual(cost.stamp_tax, Decimal("0.00"))
        self.assertEqual(cost.slippage_cost, Decimal("0.50"))
        self.assertEqual(cost.total_cost, Decimal("5.50"))
        self.assertEqual(cost.cash_change, Decimal("-1005.50"))

    def test_large_stock_order_uses_commission_rate(self) -> None:
        cost = calculate_trade_cost(
            side=Side.BUY,
            price=Decimal("100"),
            quantity=1_000,
            instrument_type=InstrumentType.STOCK,
        )

        self.assertEqual(cost.commission, Decimal("30.00"))

    def test_stock_stamp_tax_is_charged_only_on_sell(self) -> None:
        sell = calculate_trade_cost(
            side=Side.SELL,
            price=Decimal("10"),
            quantity=1_000,
            instrument_type=InstrumentType.STOCK,
        )
        buy = calculate_trade_cost(
            side=Side.BUY,
            price=Decimal("10"),
            quantity=1_000,
            instrument_type=InstrumentType.STOCK,
        )

        self.assertEqual(sell.stamp_tax, Decimal("5.00"))
        self.assertEqual(buy.stamp_tax, Decimal("0.00"))
        self.assertEqual(sell.total_cost, Decimal("15.00"))

    def test_configured_transfer_fee_is_charged_on_both_sides(self) -> None:
        config = TradingCostConfig(
            stock_transfer_fee_rate=Decimal("0.00001"),
            stock_slippage_rate=Decimal("0"),
        )

        buy = calculate_trade_cost(
            side=Side.BUY,
            price=Decimal("10"),
            quantity=10_000,
            instrument_type=InstrumentType.STOCK,
            config=config,
        )
        sell = calculate_trade_cost(
            side=Side.SELL,
            price=Decimal("10"),
            quantity=10_000,
            instrument_type=InstrumentType.STOCK,
            config=config,
        )

        self.assertEqual(buy.transfer_fee, Decimal("1.00"))
        self.assertEqual(sell.transfer_fee, Decimal("1.00"))

    def test_etf_has_no_stamp_tax_and_uses_two_basis_point_slippage(self) -> None:
        cost = calculate_trade_cost(
            side=Side.SELL,
            price=Decimal("10"),
            quantity=1_000,
            instrument_type=InstrumentType.ETF,
        )

        self.assertEqual(cost.trade_value, Decimal("10000.00"))
        self.assertEqual(cost.commission, Decimal("5.00"))
        self.assertEqual(cost.stamp_tax, Decimal("0.00"))
        self.assertEqual(cost.slippage_cost, Decimal("2.00"))
        self.assertEqual(cost.total_cost, Decimal("7.00"))
        self.assertEqual(cost.cash_change, Decimal("9993.00"))

    def test_buy_and_sell_cash_changes_have_opposite_signs(self) -> None:
        buy = calculate_trade_cost(
            side="BUY",
            price=Decimal("20"),
            quantity=500,
            instrument_type="ETF",
        )
        sell = calculate_trade_cost(
            side="SELL",
            price=Decimal("20"),
            quantity=500,
            instrument_type="ETF",
        )

        self.assertLess(buy.cash_change, 0)
        self.assertGreater(sell.cash_change, 0)
        self.assertEqual(buy.cash_change, Decimal("-10007.00"))
        self.assertEqual(sell.cash_change, Decimal("9993.00"))

    def test_rounds_each_cash_component_to_fen_with_decimal_half_up(self) -> None:
        cost = calculate_trade_cost(
            side=Side.SELL,
            price=Decimal("10.005"),
            quantity=1,
            instrument_type=InstrumentType.STOCK,
            config=TradingCostConfig(minimum_commission_cny=Decimal("0")),
        )

        self.assertEqual(cost.trade_value, Decimal("10.01"))
        self.assertEqual(cost.commission, Decimal("0.00"))
        self.assertEqual(cost.stamp_tax, Decimal("0.01"))
        self.assertEqual(cost.slippage_cost, Decimal("0.01"))
        self.assertEqual(cost.cash_change, Decimal("9.99"))

    def test_rejects_invalid_price_and_quantity(self) -> None:
        invalid_prices = (Decimal("0"), Decimal("-1"), Decimal("NaN"))
        for price in invalid_prices:
            with self.subTest(price=price), self.assertRaises(ValueError):
                calculate_trade_cost(
                    side=Side.BUY,
                    price=price,
                    quantity=100,
                    instrument_type=InstrumentType.STOCK,
                )

        for quantity in (0, -1, True):
            with self.subTest(quantity=quantity), self.assertRaises(ValueError):
                calculate_trade_cost(
                    side=Side.BUY,
                    price=Decimal("10"),
                    quantity=quantity,
                    instrument_type=InstrumentType.STOCK,
                )

    def test_requires_decimal_price(self) -> None:
        with self.assertRaisesRegex(TypeError, "price must be a Decimal"):
            calculate_trade_cost(
                side=Side.BUY,
                price=10.0,  # type: ignore[arg-type]
                quantity=100,
                instrument_type=InstrumentType.STOCK,
            )
