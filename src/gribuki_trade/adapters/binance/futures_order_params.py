"""Binance 合约订单请求的纯校验与参数编码。

REST 客户端负责传输、凭据和账户模式检查。本模块只把已经过边界校验的订单值
转换为 Binance 协议字段，使策略和适配器测试无需网络客户端即可验证协议行为。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

EnumValue = Callable[[str, str], str]
NormalizeSymbol = Callable[[str], str]


def _identity_symbol(value: str) -> str:
    return value


@dataclass(frozen=True, slots=True)
class BinanceFuturesProtectionOrder:
    """供 REST 外观层使用的结构化保护单请求。"""

    kind: Literal["stop_loss", "take_profit", "trailing_stop"]
    symbol: str
    side: str
    position_side: str | None = None
    quantity: Decimal | str | None = None
    stop_price: Decimal | str | None = None
    callback_rate: Decimal | str | None = None
    activation_price: Decimal | str | None = None
    close_position: bool = True
    working_type: str = "MARK_PRICE"
    price_protect: bool | None = None
    client_order_id: str | None = None

    def validate(self) -> None:
        if self.kind in {"stop_loss", "take_profit"} and self.stop_price is None:
            raise ValueError("stop_price is required for fixed protection orders")
        if self.kind == "trailing_stop" and self.callback_rate is None:
            raise ValueError("callback_rate is required for trailing_stop")
        if self.kind == "trailing_stop" and self.close_position:
            raise ValueError("trailing_stop requires quantity and cannot use close_position")


def order_params(
    values: Mapping[str, object],
    *,
    normalize_symbol: NormalizeSymbol = _identity_symbol,
    enum_value: EnumValue,
) -> list[tuple[str, object]]:
    """使用 REST 字段名编码普通合约订单。"""

    order_type = values.get("type", values.get("order_type"))
    if "symbol" not in values or "side" not in values or order_type is None:
        raise ValueError("submit_order requires symbol, side and type")
    params: list[tuple[str, object]] = [
        ("symbol", normalize_symbol(str(values["symbol"]))),
        ("side", enum_value(str(values["side"]), "side")),
        ("type", enum_value(str(order_type), "order_type")),
    ]
    aliases = {
        "quantity": "quantity",
        "price": "price",
        "time_in_force": "timeInForce",
        "position_side": "positionSide",
        "reduce_only": "reduceOnly",
        "stop_price": "stopPrice",
        "close_position": "closePosition",
        "client_order_id": "newClientOrderId",
        "new_client_order_id": "newClientOrderId",
        "working_type": "workingType",
        "price_protect": "priceProtect",
        "activation_price": "activationPrice",
        "callback_rate": "callbackRate",
        "price_match": "priceMatch",
        "self_trade_prevention_mode": "selfTradePreventionMode",
    }
    for key, api_name in aliases.items():
        value = values.get(key)
        if value is None:
            continue
        if key in {"time_in_force", "position_side", "working_type"}:
            value = enum_value(str(value), key)
        elif key in {"reduce_only", "close_position", "price_protect"}:
            value = str(value).lower()
        params.append((api_name, value))
    return params


def algo_order_params(
    values: Mapping[str, object],
    *,
    enum_value: EnumValue,
    normalize_symbol: NormalizeSymbol | None = None,
) -> list[tuple[str, object]]:
    """使用文档定义的字段别名编码 USDⓈ-M 算法订单。"""

    if "symbol" not in values or "side" not in values:
        raise ValueError("submit_algo_order requires symbol and side")
    if values.get("trigger_price") is not None and values.get("stop_price") is not None:
        raise ValueError("provide trigger_price or stop_price, not both")
    params: list[tuple[str, object]] = []
    aliases = {
        "algo_type": "algoType",
        "symbol": "symbol",
        "side": "side",
        "type": "type",
        "quantity": "quantity",
        "price": "price",
        "trigger_price": "triggerPrice",
        "stop_price": "triggerPrice",
        "position_side": "positionSide",
        "close_position": "closePosition",
        "working_type": "workingType",
        "price_protect": "priceProtect",
        "callback_rate": "callbackRate",
        "activation_price": "activatePrice",
        "activate_price": "activatePrice",
        "client_algo_id": "clientAlgoId",
        "reduce_only": "reduceOnly",
    }
    for key, api_name in aliases.items():
        value = values.get(key)
        if value is None:
            continue
        if key == "symbol" and normalize_symbol is not None:
            value = normalize_symbol(str(value))
        if key in {"side", "type", "position_side", "working_type", "algo_type"}:
            value = enum_value(str(value), key)
        elif key in {"close_position", "price_protect", "reduce_only"}:
            value = str(value).lower()
        params.append((api_name, value))
    if not any(name == "algoType" for name, _ in params):
        params.append(("algoType", "CONDITIONAL"))
    return params


__all__ = [
    "BinanceFuturesProtectionOrder",
    "algo_order_params",
    "order_params",
]
