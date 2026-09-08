"""Binance Spot 订单请求的纯参数校验和表单编码。

这些函数只负责把结构化订单模型转换为 Binance REST 参数，不访问网络、凭据或
网关状态。将它们独立出来后，单笔订单与 OCO/OTO/OTOCO/cancel-replace 共用同一
套校验规则，策略和适配器测试也可以在不构造网关的情况下验证协议边界。
"""

from __future__ import annotations

import re
from decimal import Decimal

from gribuki_trade.domain.orders import Side

from .models import BinanceSpotOrderLeg
from .rules import decimal_to_fixed

CLIENT_ORDER_ID = re.compile(r"^[A-Za-z0-9._:/-]{1,36}$")
SPOT_ORDER_TYPES = frozenset(
    {
        "MARKET",
        "LIMIT",
        "STOP_LOSS",
        "STOP_LOSS_LIMIT",
        "TAKE_PROFIT",
        "TAKE_PROFIT_LIMIT",
        "LIMIT_MAKER",
    }
)
ORDER_RESPONSE_TYPES = frozenset({"ACK", "RESULT", "FULL"})


def positive_decimal(value: Decimal, label: str) -> Decimal:
    """验证正的有限十进制定点数。"""

    number = Decimal(str(value))
    if not number.is_finite() or number <= 0:
        raise ValueError(f"{label} must be positive and finite")
    return number


def order_type(value: str) -> str:
    """规范化并验证 Binance Spot 订单类型。"""

    normalized = str(value).upper()
    if normalized not in SPOT_ORDER_TYPES:
        raise ValueError(f"unsupported Spot order type: {value!r}")
    return normalized


def validate_list_response_type(value: str) -> None:
    """验证订单列表接口支持的响应类型。"""

    if str(value).upper() not in ORDER_RESPONSE_TYPES:
        raise ValueError("response_type must be ACK, RESULT, or FULL")


def append_optional(params: list[tuple[str, object]], key: str, value: object | None) -> None:
    """在值存在时追加一个 Binance 参数。"""

    if value is not None:
        params.append((key, value))


def require_client_order_id(value: str | None) -> None:
    """验证 Binance 客户端订单标识符。"""

    if value is None or not CLIENT_ORDER_ID.fullmatch(value):
        raise ValueError("client order id must be 1-36 Binance-safe characters")


def optional_text(value: object) -> str | None:
    """把可选响应字段转换为文本。"""

    return None if value is None else str(value)


def validate_leg(leg: BinanceSpotOrderLeg, *, require_quantity: bool) -> None:
    """验证订单列表的一条订单腿。"""

    order_type(leg.order_type)
    if not isinstance(leg.side, Side):
        raise ValueError("order leg side must be BUY or SELL")
    if require_quantity:
        positive_decimal(leg.quantity, "quantity")
    for name, value in (
        ("price", leg.price),
        ("stop_price", leg.stop_price),
        ("quote_order_quantity", leg.quote_order_quantity),
        ("iceberg_quantity", leg.iceberg_quantity),
    ):
        if value is not None:
            positive_decimal(value, name)
    if leg.trailing_delta is not None and (
        isinstance(leg.trailing_delta, bool) or leg.trailing_delta <= 0
    ):
        raise ValueError("trailing_delta must be a positive integer BIPS")
    if leg.client_order_id is not None:
        require_client_order_id(leg.client_order_id)


def spot_order_params(
    *,
    symbol: str,
    side: Side,
    order_type_value: str,
    quantity: Decimal | None,
    quote_order_quantity: Decimal | None,
    price: Decimal | None,
    stop_price: Decimal | None,
    trailing_delta: int | None,
    time_in_force: str | None,
    client_order_id: str | None,
    iceberg_quantity: Decimal | None,
    strategy_id: int | None,
    strategy_type: int | None,
    self_trade_prevention_mode: str | None,
    response_type: str,
) -> list[tuple[str, object]]:
    """把单笔 Spot 订单转换为签名请求参数。"""

    kind = order_type(order_type_value)
    if not isinstance(side, Side):
        raise ValueError("side must be BUY or SELL")
    if (quantity is None) == (quote_order_quantity is None):
        raise ValueError("provide exactly one of quantity or quote_order_quantity")
    if quantity is not None:
        quantity = positive_decimal(quantity, "quantity")
    if quote_order_quantity is not None:
        quote_order_quantity = positive_decimal(quote_order_quantity, "quote_order_quantity")
    if price is not None:
        price = positive_decimal(price, "price")
    if stop_price is not None:
        stop_price = positive_decimal(stop_price, "stop_price")
    if trailing_delta is not None and (isinstance(trailing_delta, bool) or trailing_delta <= 0):
        raise ValueError("trailing_delta must be a positive integer BIPS")
    if kind in {"LIMIT", "LIMIT_MAKER", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"} and price is None:
        raise ValueError(f"price is required for {kind}")
    if kind in {"LIMIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"} and not time_in_force:
        raise ValueError(f"time_in_force is required for {kind}")
    if (
        kind in {"STOP_LOSS", "STOP_LOSS_LIMIT", "TAKE_PROFIT", "TAKE_PROFIT_LIMIT"}
        and stop_price is None
        and trailing_delta is None
    ):
        raise ValueError(f"stop_price or trailing_delta is required for {kind}")
    validate_list_response_type(response_type)
    if client_order_id is not None:
        require_client_order_id(client_order_id)
    params: list[tuple[str, object]] = [
        ("symbol", symbol),
        ("side", side.value),
        ("type", kind),
    ]
    append_optional(
        params, "timeInForce", None if time_in_force is None else str(time_in_force).upper()
    )
    append_optional(params, "quantity", None if quantity is None else decimal_to_fixed(quantity))
    append_optional(
        params,
        "quoteOrderQty",
        None if quote_order_quantity is None else decimal_to_fixed(quote_order_quantity),
    )
    append_optional(params, "price", None if price is None else decimal_to_fixed(price))
    append_optional(
        params, "stopPrice", None if stop_price is None else decimal_to_fixed(stop_price)
    )
    append_optional(params, "trailingDelta", trailing_delta)
    append_optional(params, "newClientOrderId", client_order_id)
    append_optional(
        params,
        "icebergQty",
        None
        if iceberg_quantity is None
        else decimal_to_fixed(positive_decimal(iceberg_quantity, "iceberg_quantity")),
    )
    append_optional(params, "strategyId", strategy_id)
    append_optional(params, "strategyType", strategy_type)
    append_optional(params, "selfTradePreventionMode", self_trade_prevention_mode)
    params.append(("newOrderRespType", str(response_type).upper()))
    return params


def list_leg_params(
    prefix: str, leg: BinanceSpotOrderLeg, *, quantity: Decimal
) -> list[tuple[str, object]]:
    """编码 OCO/OTOCO 订单腿的可选字段。"""

    del quantity  # 数量由列表请求本身编码；保留参数以兼容原网关辅助方法。
    validate_leg(leg, require_quantity=False)
    params: list[tuple[str, object]] = []
    for key, value in (
        (f"{prefix}Price", leg.price),
        (f"{prefix}StopPrice", leg.stop_price),
        (f"{prefix}TrailingDelta", leg.trailing_delta),
        (
            f"{prefix}TimeInForce",
            None if leg.time_in_force is None else str(leg.time_in_force).upper(),
        ),
        (
            f"{prefix}IcebergQty",
            None if leg.iceberg_quantity is None else decimal_to_fixed(leg.iceberg_quantity),
        ),
        (f"{prefix}ClientOrderId", leg.client_order_id),
    ):
        if value is not None:
            params.append((key, decimal_to_fixed(value) if isinstance(value, Decimal) else value))
    return params


def prefixed_leg_params(
    prefix: str,
    leg: BinanceSpotOrderLeg,
    *,
    required: bool,
    include_type: bool = True,
    include_side: bool = True,
    include_quantity: bool = True,
) -> list[tuple[str, object]]:
    """编码 OTO/OTOCO 中带前缀的订单腿。"""

    validate_leg(leg, require_quantity=required and include_quantity)
    params: list[tuple[str, object]] = []
    if include_type:
        params.append((f"{prefix}Type", order_type(leg.order_type)))
    if include_side:
        params.append((f"{prefix}Side", leg.side.value))
    if include_quantity:
        params.append((f"{prefix}Quantity", decimal_to_fixed(leg.quantity)))
    params.extend(list_leg_params(prefix, leg, quantity=leg.quantity))
    return params


__all__ = [
    "CLIENT_ORDER_ID",
    "ORDER_RESPONSE_TYPES",
    "SPOT_ORDER_TYPES",
    "append_optional",
    "list_leg_params",
    "optional_text",
    "order_type",
    "positive_decimal",
    "prefixed_leg_params",
    "require_client_order_id",
    "spot_order_params",
    "validate_leg",
    "validate_list_response_type",
]
