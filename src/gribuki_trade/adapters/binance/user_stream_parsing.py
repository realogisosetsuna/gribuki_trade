"""Binance 现货用户数据事件模型、签名和严格 payload 解析。

本模块不建立 WebSocket 连接；连接生命周期位于 ``user_stream.py``，这里只负责
把 Binance 用户数据帧转换为不可变事件并校验字段。
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, TypeAlias

from gribuki_trade.domain.orders import OrderStatus, Side

from .gateway import BinanceConfigurationError, BinanceProtocolError
from .models import BinanceEnvironment

LIVE_USER_WS_API_URL = "wss://ws-api.binance.com:443/ws-api/v3"
TESTNET_USER_WS_API_URL = "wss://ws-api.testnet.binance.vision/ws-api/v3"

_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)\b(api[-_ ]?key|secret(?:[-_ ]?key)?|signature)\b\s*[:=]\s*[^\s,;&]+"
)

@dataclass(frozen=True, slots=True)
class BinanceExecutionReport:
    """来自现货账户的类型化 ``executionReport`` 事件。"""

    subscription_id: int
    event_time_ms: int
    transaction_time_ms: int
    symbol: str
    client_order_id: str
    original_client_order_id: str | None
    side: Side
    order_type: str
    time_in_force: str
    original_quantity: Decimal
    order_price: Decimal
    stop_price: Decimal
    iceberg_quantity: Decimal
    order_list_id: int
    execution_type: str
    status: OrderStatus
    exchange_order_status: str
    reject_reason: str
    order_id: int
    last_executed_quantity: Decimal
    cumulative_filled_quantity: Decimal
    last_executed_price: Decimal
    commission_amount: Decimal
    commission_asset: str | None
    trade_id: int
    prevented_match_id: int | None
    execution_id: int
    is_order_on_book: bool
    is_maker_side: bool
    order_creation_time_ms: int
    cumulative_quote_quantity: Decimal
    last_quote_quantity: Decimal
    quote_order_quantity: Decimal
    working_time_ms: int | None
    self_trade_prevention_mode: str | None


@dataclass(frozen=True, slots=True)
class BinanceListStatus:
    """现货 OCO、OTO、OTOCO 订单列表状态事件。"""

    subscription_id: int
    event_time_ms: int
    transaction_time_ms: int
    order_list_id: int
    list_client_order_id: str
    contingency_type: str
    list_status_type: str
    list_order_status: str
    reject_reason: str
    symbol: str
    order_ids: tuple[int, ...]
    client_order_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BinanceUserBalance:
    asset: str
    free: Decimal
    locked: Decimal


@dataclass(frozen=True, slots=True)
class BinanceOutboundAccountPosition:
    subscription_id: int
    event_time_ms: int
    last_account_update_ms: int
    balances: tuple[BinanceUserBalance, ...]


@dataclass(frozen=True, slots=True)
class BinanceBalanceUpdate:
    subscription_id: int
    event_time_ms: int
    asset: str
    balance_delta: Decimal
    clear_time_ms: int


@dataclass(frozen=True, slots=True)
class BinanceEventStreamTerminated:
    subscription_id: int
    event_time_ms: int


BinanceUserDataEvent: TypeAlias = (
    BinanceExecutionReport
    | BinanceListStatus
    | BinanceOutboundAccountPosition
    | BinanceBalanceUpdate
    | BinanceEventStreamTerminated
)

def user_websocket_api_url(environment: BinanceEnvironment | str) -> str:
    """返回指定环境的私有 WebSocket API 端点。"""

    try:
        selected = (
            environment
            if isinstance(environment, BinanceEnvironment)
            else BinanceEnvironment(str(environment).upper())
        )
    except ValueError:
        raise BinanceConfigurationError(
            f"unknown Binance environment: {environment!r}"
        ) from None
    if selected is BinanceEnvironment.LIVE:
        return LIVE_USER_WS_API_URL
    return TESTNET_USER_WS_API_URL


def signature_payload(params: Mapping[str, object]) -> str:
    """生成采用 ASCII 键顺序的规范 Binance WebSocket-API 载荷。

    始终排除 ``signature``。调用方用 HMAC-SHA256 对返回字符串签名，再把十六进制
    结果追加到请求参数。
    """

    keys: list[str] = []
    for key in params:
        if not isinstance(key, str):
            raise TypeError("Binance signing parameter names must be strings")
        try:
            key.encode("ascii")
        except UnicodeEncodeError:
            raise ValueError("Binance signing parameter names must be ASCII") from None
        keys.append(key)

    pairs: list[tuple[str, str]] = []
    for key in sorted(keys, key=lambda value: value.encode("ascii")):
        if key == "signature":
            continue
        value = params[key]
        if isinstance(value, bool):
            text = "true" if value else "false"
        elif isinstance(value, (str, int, Decimal)) and not isinstance(value, bool):
            text = str(value)
        else:
            raise TypeError(
                f"unsupported Binance signing parameter type: {type(value).__name__}"
            )
        pairs.append((key, text))
    return "&".join(f"{key}={value}" for key, value in pairs)


def parse_user_data_event(message: str | bytes) -> BinanceUserDataEvent:
    """解析一个文档规定的 ``{subscriptionId,event}`` 通知。"""

    decoded = _decode_frame(message)
    if "id" in decoded:
        raise BinanceProtocolError("Binance response frame is not a user-data event")
    subscription_id = _integer(decoded, "subscriptionId", minimum=0)
    event = decoded.get("event")
    if not isinstance(event, Mapping):
        raise BinanceProtocolError("Binance user-data event envelope is malformed")
    event_type = event.get("e")
    if event_type == "executionReport":
        return _parse_execution_report(subscription_id, event)
    if event_type == "listStatus":
        return _parse_list_status(subscription_id, event)
    if event_type == "outboundAccountPosition":
        return _parse_outbound_account_position(subscription_id, event)
    if event_type == "balanceUpdate":
        return _parse_balance_update(subscription_id, event)
    if event_type == "eventStreamTerminated":
        return BinanceEventStreamTerminated(
            subscription_id=subscription_id,
            event_time_ms=_integer(event, "E", minimum=0),
        )
    raise BinanceProtocolError("unsupported Binance user-data event type")


def _decode_frame(message: str | bytes) -> Mapping[str, Any]:
    if isinstance(message, bytes):
        try:
            text = message.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            raise BinanceProtocolError("Binance WebSocket frame is not UTF-8") from None
    elif isinstance(message, str):
        text = message
    else:
        raise BinanceProtocolError("Binance WebSocket frame must be text or bytes")
    try:
        decoded = json.loads(text)
    except (json.JSONDecodeError, UnicodeError):
        raise BinanceProtocolError("Binance WebSocket frame is not valid JSON") from None
    if not isinstance(decoded, Mapping):
        raise BinanceProtocolError("Binance WebSocket payload must be an object")
    return decoded


def _parse_decoded_user_event(decoded: Mapping[str, Any]) -> BinanceUserDataEvent:
    # 避免重新编码，使 Decimal 来源字符串与发送值完全一致。
    if "id" in decoded:
        raise BinanceProtocolError("Binance response frame is not a user-data event")
    subscription_id = _integer(decoded, "subscriptionId", minimum=0)
    event = decoded.get("event")
    if not isinstance(event, Mapping):
        raise BinanceProtocolError("Binance user-data event envelope is malformed")
    event_type = event.get("e")
    if event_type == "executionReport":
        return _parse_execution_report(subscription_id, event)
    if event_type == "listStatus":
        return _parse_list_status(subscription_id, event)
    if event_type == "outboundAccountPosition":
        return _parse_outbound_account_position(subscription_id, event)
    if event_type == "balanceUpdate":
        return _parse_balance_update(subscription_id, event)
    if event_type == "eventStreamTerminated":
        return BinanceEventStreamTerminated(
            subscription_id=subscription_id,
            event_time_ms=_integer(event, "E", minimum=0),
        )
    raise BinanceProtocolError("unsupported Binance user-data event type")


def _parse_list_status(
    subscription_id: int,
    event: Mapping[str, Any],
) -> BinanceListStatus:
    orders_value = event.get("O")
    if not isinstance(orders_value, list):
        raise BinanceProtocolError("Binance listStatus orders are malformed")
    order_ids: list[int] = []
    client_order_ids: list[str] = []
    for item in orders_value:
        if not isinstance(item, Mapping):
            raise BinanceProtocolError("Binance listStatus order is malformed")
        order_ids.append(_integer(item, "i", minimum=0))
        client_order_ids.append(_required_string(item, "c"))
    return BinanceListStatus(
        subscription_id=subscription_id,
        event_time_ms=_integer(event, "E", minimum=0),
        transaction_time_ms=_integer(event, "T", minimum=0),
        order_list_id=_integer(event, "g", minimum=-1),
        list_client_order_id=_required_string(event, "c"),
        contingency_type=_required_string(event, "l"),
        list_status_type=_required_string(event, "L"),
        list_order_status=_required_string(event, "J"),
        reject_reason=_required_string(event, "r"),
        symbol=_symbol(event, "s"),
        order_ids=tuple(order_ids),
        client_order_ids=tuple(client_order_ids),
    )


def _parse_execution_report(
    subscription_id: int,
    event: Mapping[str, Any],
) -> BinanceExecutionReport:
    try:
        side = Side(_required_string(event, "S"))
    except ValueError:
        raise BinanceProtocolError("Binance executionReport side is malformed") from None
    exchange_status = _required_string(event, "X")
    return BinanceExecutionReport(
        subscription_id=subscription_id,
        event_time_ms=_integer(event, "E", minimum=0),
        transaction_time_ms=_integer(event, "T", minimum=0),
        symbol=_symbol(event, "s"),
        client_order_id=_required_string(event, "c"),
        original_client_order_id=_optional_string(event, "C", blank_as_none=True),
        side=side,
        order_type=_required_string(event, "o"),
        time_in_force=_required_string(event, "f"),
        original_quantity=_decimal(event, "q", minimum=Decimal("0")),
        order_price=_decimal(event, "p", minimum=Decimal("0")),
        stop_price=_decimal(event, "P", minimum=Decimal("0")),
        iceberg_quantity=_decimal(event, "F", minimum=Decimal("0")),
        order_list_id=_integer(event, "g", minimum=-1),
        execution_type=_required_string(event, "x"),
        status=_map_order_status(exchange_status),
        exchange_order_status=exchange_status,
        reject_reason=_required_string(event, "r"),
        order_id=_integer(event, "i", minimum=0),
        last_executed_quantity=_decimal(event, "l", minimum=Decimal("0")),
        cumulative_filled_quantity=_decimal(event, "z", minimum=Decimal("0")),
        last_executed_price=_decimal(event, "L", minimum=Decimal("0")),
        commission_amount=_decimal(event, "n", minimum=Decimal("0")),
        commission_asset=_nullable_string(event, "N"),
        trade_id=_integer(event, "t", minimum=-1),
        prevented_match_id=_optional_integer(event, "v", minimum=0),
        execution_id=_integer(event, "I", minimum=0),
        is_order_on_book=_boolean(event, "w"),
        is_maker_side=_boolean(event, "m"),
        order_creation_time_ms=_integer(event, "O", minimum=0),
        cumulative_quote_quantity=_decimal(event, "Z", minimum=Decimal("0")),
        last_quote_quantity=_decimal(event, "Y", minimum=Decimal("0")),
        quote_order_quantity=_decimal(event, "Q", minimum=Decimal("0")),
        working_time_ms=_optional_integer(event, "W", minimum=0),
        self_trade_prevention_mode=_optional_string(event, "V"),
    )


def _parse_outbound_account_position(
    subscription_id: int,
    event: Mapping[str, Any],
) -> BinanceOutboundAccountPosition:
    raw_balances = event.get("B")
    if not isinstance(raw_balances, list):
        raise BinanceProtocolError("Binance outboundAccountPosition balances are malformed")
    balances: list[BinanceUserBalance] = []
    for raw in raw_balances:
        if not isinstance(raw, Mapping):
            raise BinanceProtocolError(
                "Binance outboundAccountPosition balance is malformed"
            )
        balances.append(
            BinanceUserBalance(
                asset=_asset(raw, "a"),
                free=_decimal(raw, "f", minimum=Decimal("0")),
                locked=_decimal(raw, "l", minimum=Decimal("0")),
            )
        )
    return BinanceOutboundAccountPosition(
        subscription_id=subscription_id,
        event_time_ms=_integer(event, "E", minimum=0),
        last_account_update_ms=_integer(event, "u", minimum=0),
        balances=tuple(balances),
    )


def _parse_balance_update(
    subscription_id: int,
    event: Mapping[str, Any],
) -> BinanceBalanceUpdate:
    return BinanceBalanceUpdate(
        subscription_id=subscription_id,
        event_time_ms=_integer(event, "E", minimum=0),
        asset=_asset(event, "a"),
        balance_delta=_decimal(event, "d", minimum=None),
        clear_time_ms=_integer(event, "T", minimum=0),
    )


def _map_order_status(value: str) -> OrderStatus:
    return {
        "NEW": OrderStatus.ACCEPTED,
        "PENDING_NEW": OrderStatus.SUBMITTING,
        "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
        "FILLED": OrderStatus.FILLED,
        "PENDING_CANCEL": OrderStatus.CANCEL_PENDING,
        "CANCELED": OrderStatus.CANCELED,
        "REJECTED": OrderStatus.BROKER_REJECTED,
        "EXPIRED": OrderStatus.EXPIRED,
        "EXPIRED_IN_MATCH": OrderStatus.EXPIRED,
    }.get(value, OrderStatus.UNKNOWN)


def _integer(mapping: Mapping[str, Any], key: str, *, minimum: int) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _optional_integer(
    mapping: Mapping[str, Any],
    key: str,
    *,
    minimum: int,
) -> int | None:
    if key not in mapping:
        return None
    return _integer(mapping, key, minimum=minimum)


def _decimal(
    mapping: Mapping[str, Any],
    key: str,
    *,
    minimum: Decimal | None,
) -> Decimal:
    value = mapping.get(key)
    if not isinstance(value, str):
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed") from None
    if not number.is_finite() or (minimum is not None and number < minimum):
        raise BinanceProtocolError(f"Binance user-data field {key!r} is out of range")
    return number


def _boolean(mapping: Mapping[str, Any], key: str) -> bool:
    value = mapping.get(key)
    if not isinstance(value, bool):
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _required_string(mapping: Mapping[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _optional_string(
    mapping: Mapping[str, Any],
    key: str,
    *,
    blank_as_none: bool = False,
) -> str | None:
    if key not in mapping:
        return None
    value = mapping.get(key)
    if not isinstance(value, str):
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    if not value and blank_as_none:
        return None
    if not value:
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _nullable_string(mapping: Mapping[str, Any], key: str) -> str | None:
    if key not in mapping:
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    value = mapping.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _symbol(mapping: Mapping[str, Any], key: str) -> str:
    value = _required_string(mapping, key)
    if not value.isascii() or not value.isalnum() or value != value.upper():
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _asset(mapping: Mapping[str, Any], key: str) -> str:
    value = _required_string(mapping, key)
    if not value.isascii() or not value.isalnum() or value != value.upper():
        raise BinanceProtocolError(f"Binance user-data field {key!r} is malformed")
    return value


def _sanitize_message(message: object, secrets: Sequence[str]) -> str:
    text = str(message).replace("\r", " ").replace("\n", " ")
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    text = _SENSITIVE_ASSIGNMENT.sub(
        lambda match: f"{match.group(1)}=<redacted>",
        text,
    )
    return text[:500] or "subscription rejected"
