"""Binance 合约 REST 响应解析和基础字段校验。

本模块只处理确定性的值转换，不创建 HTTP 请求，也不读取客户端凭据或运行时
交易守卫。合约网关因此可以继续拥有签名、传输和 LIVE 权限，而订单簿、Ticker
响应和通用字段校验可以被独立测试或由其他合约入口复用。
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .errors import BinanceProtocolError
from .models import OrderBookLevel, OrderBookSnapshot

_SYMBOL = re.compile(r"^[A-Z0-9_]{1,30}$")


@dataclass(frozen=True, slots=True)
class BinanceFuturesTicker:
    """合约单交易对 Ticker 的规范化值。"""

    symbol: str
    price: Decimal
    time_ms: int | None = None


def require_mapping(payload: Any, description: str) -> Mapping[str, Any]:
    """确保交易所响应是对象，并用统一的协议错误描述失败。"""

    if not isinstance(payload, Mapping):
        raise BinanceProtocolError(f"Binance Futures {description} response must be an object")
    return payload


def normalize_symbol(symbol: str) -> str:
    """规范化合约交易对，同时拒绝空值和路径中不安全的字符。"""

    normalized = symbol.strip().upper()
    if not _SYMBOL.fullmatch(normalized):
        raise ValueError(f"invalid Binance Futures symbol: {symbol!r}")
    return normalized


def enum_value(value: str, name: str) -> str:
    """把合约枚举参数规范化为大写安全文本。"""

    normalized = value.strip().upper()
    if not normalized or not normalized.replace("_", "").isalnum():
        raise ValueError(f"invalid Binance Futures {name}: {value!r}")
    return normalized


def validate_listen_key(listen_key: str) -> str:
    """校验用户数据流密钥，使其不能注入额外的路径或查询参数。"""

    if not isinstance(listen_key, str) or not listen_key.strip():
        raise ValueError("listen_key must not be blank")
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    if any(char not in allowed for char in listen_key):
        raise ValueError("listen_key contains invalid characters")
    return listen_key


def parse_futures_ticker(payload: Any) -> BinanceFuturesTicker:
    """解析 USD-M 对象或 COIN-M 单元素数组形式的 Ticker 响应。"""

    # 当前 COIN-M 即使提供 ``symbol`` 仍返回单元素数组，而 USD-M 返回对象；
    # 在纯解析边界保留这一有文档依据的产品差异。
    if isinstance(payload, list):
        if len(payload) != 1 or not isinstance(payload[0], Mapping):
            raise BinanceProtocolError(
                "Binance Futures ticker response must contain exactly one symbol"
            )
        mapping = payload[0]
    else:
        mapping = require_mapping(payload, "ticker price")
    try:
        response_symbol = normalize_symbol(str(mapping["symbol"]))
        price = Decimal(str(mapping["price"]))
        time_value = mapping.get("time")
        time_ms = None if time_value is None else int(time_value)
    except (KeyError, InvalidOperation, TypeError, ValueError):
        raise BinanceProtocolError("Binance Futures ticker response is malformed") from None
    return BinanceFuturesTicker(symbol=response_symbol, price=price, time_ms=time_ms)


def parse_order_book_levels(value: object) -> tuple[OrderBookLevel, ...]:
    """把价格和数量数组解析为不可变订单簿档位。"""

    if not isinstance(value, list):
        raise TypeError("order book levels must be a list")
    levels: list[OrderBookLevel] = []
    for row in value:
        if not isinstance(row, (list, tuple)) or len(row) != 2:
            raise ValueError("order book level must contain price and quantity")
        price = Decimal(str(row[0]))
        quantity = Decimal(str(row[1]))
        if price <= 0 or quantity < 0:
            raise ValueError("order book level values are invalid")
        levels.append(OrderBookLevel(price=price, quantity=quantity))
    return tuple(levels)


def parse_order_book_snapshot(payload: Any, *, symbol: str) -> OrderBookSnapshot:
    """解析合约深度快照，供本地订单簿恢复使用。"""

    mapping = require_mapping(payload, "Futures order book")
    normalized = normalize_symbol(symbol)
    try:
        update_id = int(mapping["lastUpdateId"])
        bids = parse_order_book_levels(mapping["bids"])
        asks = parse_order_book_levels(mapping["asks"])
    except (KeyError, TypeError, ValueError, InvalidOperation):
        raise BinanceProtocolError("Binance Futures order book response is malformed") from None
    return OrderBookSnapshot(
        symbol=normalized,
        last_update_id=update_id,
        bids=bids,
        asks=asks,
    )


__all__ = [
    "BinanceFuturesTicker",
    "enum_value",
    "normalize_symbol",
    "parse_futures_ticker",
    "parse_order_book_levels",
    "parse_order_book_snapshot",
    "require_mapping",
    "validate_listen_key",
]
