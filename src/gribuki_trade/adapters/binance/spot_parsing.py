"""Binance 现货 REST 网关使用的纯解析与标量校验函数。

网关负责传输、认证和状态；本模块不访问网络，也不保存网关状态，便于独立测试和供
其他现货入口复用协议转换逻辑。
"""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Mapping, Sequence
from decimal import Decimal

from .models import (
    BinanceCommissionComponent,
    BinanceTrade,
    Kline,
    OrderBookLevel,
)
from .rules import decimal_from_api, decimal_to_fixed

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
_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)\b(api[-_ ]?key|secret(?:[-_ ]?key)?|signature)\b\s*[:=]\s*[^\s,;&]+"
)


def sign_hmac_sha256(secret_key: str, payload: str) -> str:
    """返回 Binance 使用的小写 HMAC-SHA256 签名。"""

    return hmac.new(secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def sanitize_message(message: object, secrets: Sequence[str]) -> str:
    """从交易所消息中移除凭据和控制字符。"""

    text = str(message).replace("\r", " ").replace("\n", " ")
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    text = _SENSITIVE_ASSIGNMENT.sub(lambda match: f"{match.group(1)}=<redacted>", text)
    return text[:500] or "request rejected"


def parameter_text(value: object) -> str:
    """把支持的值编码为 Binance 表单或查询参数文本。"""

    if isinstance(value, Decimal):
        return decimal_to_fixed(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int)):
        return str(value)
    if value is None:
        raise TypeError("Binance request parameters cannot be None")
    raise TypeError(f"unsupported Binance request parameter type: {type(value).__name__}")


def normalize_symbol(symbol: str) -> str:
    """在不访问网关状态的前提下规范化并校验 Binance 交易对。"""

    normalized = symbol.strip().upper()
    if not normalized or not normalized.isascii() or not normalized.isalnum():
        raise ValueError(f"invalid Binance symbol: {symbol!r}")
    return normalized


def api_code(payload: Mapping[object, object]) -> int | None:
    """读取 Binance 响应中可选的数字错误码。"""

    try:
        value = payload.get("code")
        if isinstance(value, bool) or not isinstance(value, (str, bytes, int)):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def optional_integer(value: object, label: str) -> int | None:
    """解析可以省略或以字符串表示的非负整数字段。"""

    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError(label)
    if isinstance(value, int):
        result = value
    elif isinstance(value, str):
        result = int(value)
    else:
        raise TypeError(label)
    if result < 0:
        raise ValueError(label)
    return result


def parse_levels(value: object, label: str) -> tuple[OrderBookLevel, ...]:
    if not isinstance(value, list):
        raise TypeError(label)
    return tuple(
        OrderBookLevel(
            price=decimal_from_api(item[0], f"{label}.price"),
            quantity=decimal_from_api(item[1], f"{label}.quantity"),
        )
        for item in value
        if isinstance(item, list) and len(item) >= 2
    )


def parse_kline(item: object) -> Kline:
    if not isinstance(item, list) or len(item) < 11:
        raise TypeError("kline")
    return Kline(
        open_time_ms=int(item[0]),
        open=decimal_from_api(item[1], "open"),
        high=decimal_from_api(item[2], "high"),
        low=decimal_from_api(item[3], "low"),
        close=decimal_from_api(item[4], "close"),
        volume=decimal_from_api(item[5], "volume"),
        close_time_ms=int(item[6]),
        quote_volume=decimal_from_api(item[7], "quoteVolume"),
        trade_count=int(item[8]),
        taker_buy_base_volume=decimal_from_api(item[9], "takerBuyBaseVolume"),
        taker_buy_quote_volume=decimal_from_api(item[10], "takerBuyQuoteVolume"),
    )


def parse_trade(item: object, *, fallback_symbol: str) -> BinanceTrade:
    if not isinstance(item, Mapping):
        raise TypeError("trade")
    return BinanceTrade(
        symbol=str(item.get("symbol", fallback_symbol)).upper(),
        trade_id=int(item["id"]),
        order_id=int(item["orderId"]),
        price=decimal_from_api(item["price"], "price"),
        quantity=decimal_from_api(item["qty"], "qty"),
        quote_quantity=decimal_from_api(item["quoteQty"], "quoteQty"),
        commission=decimal_from_api(item["commission"], "commission"),
        commission_asset=str(item["commissionAsset"]),
        time_ms=int(item["time"]),
        is_buyer=bool(item.get("isBuyer", False)),
        is_maker=bool(item.get("isMaker", False)),
        is_best_match=bool(item.get("isBestMatch", False)),
    )


def parse_commission_component(value: object) -> BinanceCommissionComponent:
    if not isinstance(value, Mapping):
        raise TypeError("commission component")
    return BinanceCommissionComponent(
        maker=decimal_from_api(value["maker"], "maker"),
        taker=decimal_from_api(value["taker"], "taker"),
        buyer=decimal_from_api(value["buyer"], "buyer"),
        seller=decimal_from_api(value["seller"], "seller"),
    )
