"""Deterministic A-share and ETF trading-cost calculations."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum

from gribuki_trade.domain.orders import Side


class InstrumentType(StrEnum):
    """Instrument categories with distinct tax and slippage assumptions."""

    STOCK = "STOCK"
    ETF = "ETF"


@dataclass(frozen=True, slots=True)
class TradingCostConfig:
    """A simplified, configurable personal-account cost schedule.

    The defaults model 0.03% commission with a CNY 5 minimum per order, 0.05%
    sell-side stamp tax for stocks, and symmetric one-way slippage of five basis
    points for stocks or two basis points for ETFs.  ETF trades have no stamp tax.
    """

    commission_rate: Decimal = Decimal("0.0003")
    minimum_commission_cny: Decimal = Decimal("5")
    stock_sell_stamp_tax_rate: Decimal = Decimal("0.0005")
    stock_transfer_fee_rate: Decimal = Decimal("0")
    stock_slippage_rate: Decimal = Decimal("0.0005")
    etf_sell_stamp_tax_rate: Decimal = Decimal("0")
    etf_transfer_fee_rate: Decimal = Decimal("0")
    etf_slippage_rate: Decimal = Decimal("0.0002")
    currency_quantum: Decimal = Decimal("0.01")

    def __post_init__(self) -> None:
        non_negative = {
            "commission_rate": self.commission_rate,
            "minimum_commission_cny": self.minimum_commission_cny,
            "stock_sell_stamp_tax_rate": self.stock_sell_stamp_tax_rate,
            "stock_transfer_fee_rate": self.stock_transfer_fee_rate,
            "stock_slippage_rate": self.stock_slippage_rate,
            "etf_sell_stamp_tax_rate": self.etf_sell_stamp_tax_rate,
            "etf_transfer_fee_rate": self.etf_transfer_fee_rate,
            "etf_slippage_rate": self.etf_slippage_rate,
        }
        for name, value in non_negative.items():
            _require_decimal(name, value)
            if value < 0:
                raise ValueError(f"{name} must be non-negative")

        _require_decimal("currency_quantum", self.currency_quantum)
        if self.currency_quantum <= 0:
            raise ValueError("currency_quantum must be positive")


@dataclass(frozen=True, slots=True)
class TradeCost:
    """Rounded cash effects for one complete fill."""

    trade_value: Decimal
    commission: Decimal
    transfer_fee: Decimal
    stamp_tax: Decimal
    slippage_cost: Decimal
    total_cost: Decimal
    cash_change: Decimal


def calculate_trade_cost(
    *,
    side: Side | str,
    price: Decimal,
    quantity: int,
    instrument_type: InstrumentType | str,
    config: TradingCostConfig | None = None,
) -> TradeCost:
    """Calculate explicit costs and signed cash movement for a completed fill.

    ``price`` is the reference fill price before the configured slippage penalty.
    Slippage is returned as an explicit cost rather than changing that price.  A buy
    therefore produces a negative cash change, while a sell produces a positive one.
    """

    resolved_side = _resolve_side(side)
    resolved_instrument_type = _resolve_instrument_type(instrument_type)
    resolved_config = config or TradingCostConfig()

    _require_decimal("price", price)
    if price <= 0:
        raise ValueError("price must be positive")
    if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
        raise ValueError("quantity must be a positive integer")

    raw_trade_value = price * quantity
    trade_value = _money(raw_trade_value, resolved_config)
    commission = _money(
        max(
            raw_trade_value * resolved_config.commission_rate,
            resolved_config.minimum_commission_cny,
        ),
        resolved_config,
    )

    if resolved_instrument_type is InstrumentType.STOCK:
        stamp_tax_rate = resolved_config.stock_sell_stamp_tax_rate
        transfer_fee_rate = resolved_config.stock_transfer_fee_rate
        slippage_rate = resolved_config.stock_slippage_rate
    else:
        stamp_tax_rate = resolved_config.etf_sell_stamp_tax_rate
        transfer_fee_rate = resolved_config.etf_transfer_fee_rate
        slippage_rate = resolved_config.etf_slippage_rate

    stamp_tax = (
        _money(raw_trade_value * stamp_tax_rate, resolved_config)
        if resolved_side is Side.SELL
        else _money(Decimal(0), resolved_config)
    )
    transfer_fee = _money(raw_trade_value * transfer_fee_rate, resolved_config)
    slippage_cost = _money(raw_trade_value * slippage_rate, resolved_config)
    total_cost = commission + transfer_fee + stamp_tax + slippage_cost
    cash_change = (
        -(trade_value + total_cost)
        if resolved_side is Side.BUY
        else trade_value - total_cost
    )

    return TradeCost(
        trade_value=trade_value,
        commission=commission,
        transfer_fee=transfer_fee,
        stamp_tax=stamp_tax,
        slippage_cost=slippage_cost,
        total_cost=total_cost,
        cash_change=cash_change,
    )


def _money(value: Decimal, config: TradingCostConfig) -> Decimal:
    return value.quantize(config.currency_quantum, rounding=ROUND_HALF_UP)


def _require_decimal(name: str, value: object) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")


def _resolve_side(side: Side | str) -> Side:
    try:
        return Side(side)
    except ValueError as error:
        raise ValueError(f"unsupported side: {side!r}") from error


def _resolve_instrument_type(instrument_type: InstrumentType | str) -> InstrumentType:
    try:
        return InstrumentType(instrument_type)
    except ValueError as error:
        raise ValueError(f"unsupported instrument_type: {instrument_type!r}") from error
