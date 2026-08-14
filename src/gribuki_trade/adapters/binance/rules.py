"""对 Binance ``exchangeInfo`` 规则做解析与精确 Decimal 校验。"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any


class BinanceValidationError(ValueError):
    """订单违反了本地缓存的 Binance 标的过滤规则。"""


def decimal_from_api(value: object, field_name: str) -> Decimal:
    """解析有限 Decimal，且绝不经过二进制浮点。"""

    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise BinanceValidationError(f"invalid decimal in {field_name}") from None
    if not result.is_finite():
        raise BinanceValidationError(f"non-finite decimal in {field_name}")
    return result


def decimal_to_fixed(value: Decimal) -> str:
    """返回适合 Binance 请求的非科学计数法十进制字符串。"""

    if not isinstance(value, Decimal):
        raise TypeError("Binance numeric order values must be Decimal")
    if not value.is_finite():
        raise BinanceValidationError("order numeric values must be finite")
    return format(value, "f")


@dataclass(frozen=True, slots=True)
class SymbolRules:
    symbol: str
    status: str
    supports_limit: bool
    spot_trading_allowed: bool
    min_price: Decimal
    max_price: Decimal
    tick_size: Decimal
    min_quantity: Decimal
    max_quantity: Decimal
    step_size: Decimal
    min_notional: Decimal | None
    max_notional: Decimal | None
    percent_multiplier_up: Decimal | None = None
    percent_multiplier_down: Decimal | None = None
    bid_multiplier_up: Decimal | None = None
    bid_multiplier_down: Decimal | None = None
    ask_multiplier_up: Decimal | None = None
    ask_multiplier_down: Decimal | None = None
    average_price_minutes: int | None = None
    maximum_open_orders: int | None = None
    maximum_algo_orders: int | None = None
    maximum_position: Decimal | None = None

    @classmethod
    def from_exchange_info(cls, symbol_info: dict[str, Any]) -> SymbolRules:
        symbol = str(symbol_info.get("symbol", "")).strip().upper()
        if not symbol:
            raise BinanceValidationError("exchangeInfo symbol is missing")

        filters_value = symbol_info.get("filters")
        if not isinstance(filters_value, list):
            raise BinanceValidationError(f"exchangeInfo filters missing for {symbol}")
        filters = {
            str(item.get("filterType")): item
            for item in filters_value
            if isinstance(item, dict) and item.get("filterType") is not None
        }
        price_filter = filters.get("PRICE_FILTER")
        lot_filter = filters.get("LOT_SIZE")
        if not isinstance(price_filter, dict) or not isinstance(lot_filter, dict):
            raise BinanceValidationError(
                f"exchangeInfo PRICE_FILTER/LOT_SIZE missing for {symbol}"
            )

        minimums: list[Decimal] = []
        maximums: list[Decimal] = []
        min_notional_filter = filters.get("MIN_NOTIONAL")
        if isinstance(min_notional_filter, dict):
            minimums.append(
                decimal_from_api(min_notional_filter.get("minNotional", "0"), "minNotional")
            )
        notional_filter = filters.get("NOTIONAL")
        if isinstance(notional_filter, dict):
            minimums.append(
                decimal_from_api(notional_filter.get("minNotional", "0"), "minNotional")
            )
            maximums.append(
                decimal_from_api(notional_filter.get("maxNotional", "0"), "maxNotional")
            )

        order_types = symbol_info.get("orderTypes", [])
        supports_limit = isinstance(order_types, list) and "LIMIT" in order_types
        percent_filter = filters.get("PERCENT_PRICE")
        percent_by_side_filter = filters.get("PERCENT_PRICE_BY_SIDE")
        maximum_orders_filter = filters.get("MAX_NUM_ORDERS")
        maximum_algo_filter = filters.get("MAX_NUM_ALGO_ORDERS")
        maximum_position_filter = filters.get("MAX_POSITION")

        def optional_decimal(filter_value: object, key: str) -> Decimal | None:
            if not isinstance(filter_value, dict) or key not in filter_value:
                return None
            result = decimal_from_api(filter_value[key], key)
            return result if result > 0 else None

        def optional_integer(filter_value: object, key: str) -> int | None:
            if not isinstance(filter_value, dict) or key not in filter_value:
                return None
            value = filter_value[key]
            if isinstance(value, bool):
                raise BinanceValidationError(f"invalid integer in {key}")
            try:
                result = int(value)
            except (TypeError, ValueError):
                raise BinanceValidationError(f"invalid integer in {key}") from None
            return result if result > 0 else None

        average_price_minutes = None
        for candidate in (percent_by_side_filter, percent_filter, min_notional_filter):
            parsed = optional_integer(candidate, "avgPriceMins")
            if parsed is not None:
                average_price_minutes = parsed
                break
        return cls(
            symbol=symbol,
            status=str(symbol_info.get("status", "")),
            supports_limit=supports_limit,
            spot_trading_allowed=bool(symbol_info.get("isSpotTradingAllowed", True)),
            min_price=decimal_from_api(price_filter.get("minPrice", "0"), "minPrice"),
            max_price=decimal_from_api(price_filter.get("maxPrice", "0"), "maxPrice"),
            tick_size=decimal_from_api(price_filter.get("tickSize", "0"), "tickSize"),
            min_quantity=decimal_from_api(lot_filter.get("minQty", "0"), "minQty"),
            max_quantity=decimal_from_api(lot_filter.get("maxQty", "0"), "maxQty"),
            step_size=decimal_from_api(lot_filter.get("stepSize", "0"), "stepSize"),
            min_notional=max(minimums) if minimums else None,
            max_notional=min((item for item in maximums if item > 0), default=None),
            percent_multiplier_up=optional_decimal(percent_filter, "multiplierUp"),
            percent_multiplier_down=optional_decimal(percent_filter, "multiplierDown"),
            bid_multiplier_up=optional_decimal(percent_by_side_filter, "bidMultiplierUp"),
            bid_multiplier_down=optional_decimal(
                percent_by_side_filter, "bidMultiplierDown"
            ),
            ask_multiplier_up=optional_decimal(percent_by_side_filter, "askMultiplierUp"),
            ask_multiplier_down=optional_decimal(
                percent_by_side_filter, "askMultiplierDown"
            ),
            average_price_minutes=average_price_minutes,
            maximum_open_orders=optional_integer(
                maximum_orders_filter, "maxNumOrders"
            ),
            maximum_algo_orders=optional_integer(
                maximum_algo_filter, "maxNumAlgoOrders"
            ),
            maximum_position=optional_decimal(maximum_position_filter, "maxPosition"),
        )

    def validate_limit_order(
        self,
        *,
        quantity: Decimal,
        price: Decimal,
        side: str | None = None,
        weighted_average_price: Decimal | None = None,
        current_position: Decimal | None = None,
        open_order_count: int | None = None,
    ) -> None:
        """仅使用 Decimal 校验精确精度、范围与名义金额。"""

        decimal_to_fixed(quantity)
        decimal_to_fixed(price)
        if self.status != "TRADING":
            raise BinanceValidationError(f"{self.symbol} is not trading")
        if not self.spot_trading_allowed:
            raise BinanceValidationError(f"spot trading is disabled for {self.symbol}")
        if not self.supports_limit:
            raise BinanceValidationError(f"LIMIT orders are not supported for {self.symbol}")
        self._validate_increment(
            value=price,
            minimum=self.min_price,
            maximum=self.max_price,
            increment=self.tick_size,
            label="price",
        )
        self._validate_increment(
            value=quantity,
            minimum=self.min_quantity,
            maximum=self.max_quantity,
            increment=self.step_size,
            label="quantity",
        )
        normalized_side = side.upper() if side is not None else None
        if normalized_side not in {None, "BUY", "SELL"}:
            raise BinanceValidationError("side must be BUY or SELL")
        if weighted_average_price is not None:
            decimal_to_fixed(weighted_average_price)
            if weighted_average_price <= 0:
                raise BinanceValidationError("weighted_average_price must be positive")
            lower, upper = self._price_band(normalized_side, weighted_average_price)
            if lower is not None and price < lower:
                raise BinanceValidationError(
                    f"price {decimal_to_fixed(price)} is below dynamic minimum "
                    f"{decimal_to_fixed(lower)}"
                )
            if upper is not None and price > upper:
                raise BinanceValidationError(
                    f"price {decimal_to_fixed(price)} is above dynamic maximum "
                    f"{decimal_to_fixed(upper)}"
                )
        notional = price * quantity
        if self.min_notional is not None and self.min_notional > 0 and notional < self.min_notional:
            raise BinanceValidationError(
                f"notional {decimal_to_fixed(notional)} is below minimum "
                f"{decimal_to_fixed(self.min_notional)}"
            )
        if self.max_notional is not None and notional > self.max_notional:
            raise BinanceValidationError(
                f"notional {decimal_to_fixed(notional)} is above maximum "
                f"{decimal_to_fixed(self.max_notional)}"
            )
        if open_order_count is not None:
            if open_order_count < 0:
                raise BinanceValidationError("open_order_count must be non-negative")
            if (
                self.maximum_open_orders is not None
                and open_order_count >= self.maximum_open_orders
            ):
                raise BinanceValidationError(
                    f"open-order limit {self.maximum_open_orders} is exhausted"
                )
        if current_position is not None:
            decimal_to_fixed(current_position)
            if current_position < 0:
                raise BinanceValidationError("current_position must be non-negative")
            if (
                normalized_side == "BUY"
                and self.maximum_position is not None
                and current_position + quantity > self.maximum_position
            ):
                raise BinanceValidationError(
                    f"position would exceed maximum {decimal_to_fixed(self.maximum_position)}"
                )

    @property
    def requires_reference_price(self) -> bool:
        return any(
            value is not None
            for value in (
                self.percent_multiplier_up,
                self.percent_multiplier_down,
                self.bid_multiplier_up,
                self.bid_multiplier_down,
                self.ask_multiplier_up,
                self.ask_multiplier_down,
            )
        )

    def _price_band(
        self,
        side: str | None,
        reference: Decimal,
    ) -> tuple[Decimal | None, Decimal | None]:
        lower_multiplier = self.percent_multiplier_down
        upper_multiplier = self.percent_multiplier_up
        if side == "BUY" and (
            self.bid_multiplier_down is not None or self.bid_multiplier_up is not None
        ):
            lower_multiplier = self.bid_multiplier_down
            upper_multiplier = self.bid_multiplier_up
        elif side == "SELL" and (
            self.ask_multiplier_down is not None or self.ask_multiplier_up is not None
        ):
            lower_multiplier = self.ask_multiplier_down
            upper_multiplier = self.ask_multiplier_up
        return (
            None if lower_multiplier is None else reference * lower_multiplier,
            None if upper_multiplier is None else reference * upper_multiplier,
        )

    @staticmethod
    def _validate_increment(
        *,
        value: Decimal,
        minimum: Decimal,
        maximum: Decimal,
        increment: Decimal,
        label: str,
    ) -> None:
        if value <= 0:
            raise BinanceValidationError(f"{label} must be positive")
        if minimum > 0 and value < minimum:
            raise BinanceValidationError(
                f"{label} {decimal_to_fixed(value)} is below minimum "
                f"{decimal_to_fixed(minimum)}"
            )
        if maximum > 0 and value > maximum:
            raise BinanceValidationError(
                f"{label} {decimal_to_fixed(value)} is above maximum "
                f"{decimal_to_fixed(maximum)}"
            )
        if increment > 0 and value % increment != 0:
            raise BinanceValidationError(
                f"{label} {decimal_to_fixed(value)} is not aligned to increment "
                f"{decimal_to_fixed(increment)}"
            )
