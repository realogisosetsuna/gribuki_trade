"""券商中立 OMS 的 SQLite 行编解码与纯校验辅助函数。

本模块不打开数据库连接，也不执行事务；它只负责把 SQLite 基础值与领域
模型互相转换，并集中维护时间、Decimal、JSON、标识符和状态单调性规则。
持久化事务仍由 :mod:`gribuki_trade.trading.oms` 管理。
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

from gribuki_trade.domain.orders import OrderIntent, OrderStatus, OrderType, Side
from gribuki_trade.trading.models import (
    AssetBalance,
    ExecutionFill,
    OrderEventRecord,
    OrderSnapshot,
    PositionSnapshot,
    TradingCommand,
    TradingCommandStatus,
    TradingCommandType,
)

_OPEN_STATUSES = frozenset(
    {
        OrderStatus.CREATED,
        OrderStatus.VALIDATED,
        OrderStatus.SUBMITTING,
        OrderStatus.ACCEPTED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.CANCEL_PENDING,
        OrderStatus.UNKNOWN,
    }
)
_TERMINAL_STATUSES = frozenset(
    {
        OrderStatus.LOCAL_REJECTED,
        OrderStatus.FILLED,
        OrderStatus.CANCELED,
        OrderStatus.BROKER_REJECTED,
        OrderStatus.EXPIRED,
    }
)
_STATUS_RANK = {
    OrderStatus.CREATED: 0,
    OrderStatus.VALIDATED: 1,
    OrderStatus.SUBMITTING: 2,
    OrderStatus.ACCEPTED: 3,
    OrderStatus.CANCEL_PENDING: 4,
    OrderStatus.PARTIALLY_FILLED: 5,
}
_SAFE_ERROR_CODE = re.compile(r"^[a-z0-9_.-]{1,64}$")


def row_to_order(row: sqlite3.Row) -> OrderSnapshot:
    order = OrderIntent(
        client_order_id=str(row["client_order_id"]),
        account_id=str(row["account_id"]),
        strategy_id=str(row["strategy_id"]),
        symbol=str(row["symbol"]),
        side=Side(str(row["side"])),
        order_type=OrderType(str(row["order_type"])),
        quantity=Decimal(str(row["quantity"])),
        limit_price=Decimal(str(row["limit_price"])),
        created_at=parse_time(str(row["created_at"])),
    )
    average = row["average_fill_price"]
    return OrderSnapshot(
        order=order,
        status=OrderStatus(str(row["status"])),
        filled_quantity=Decimal(str(row["filled_quantity"])),
        average_fill_price=None if average is None else Decimal(str(average)),
        exchange_order_id=None
        if row["exchange_order_id"] is None
        else str(row["exchange_order_id"]),
        reason=None if row["reason"] is None else str(row["reason"]),
        updated_at=parse_time(str(row["updated_at"])),
        broker_error_code=(
            None if row["broker_error_code"] is None else int(row["broker_error_code"])
        ),
    )


def row_to_order_event(row: sqlite3.Row) -> OrderEventRecord:
    return OrderEventRecord(
        sequence=int(row["sequence"]),
        event_id=str(row["event_id"]),
        client_order_id=str(row["client_order_id"]),
        event_type=str(row["event_type"]),
        status=None if row["status"] is None else OrderStatus(str(row["status"])),
        occurred_at=parse_time(str(row["occurred_at"])),
        payload_json=str(row["payload_json"]),
        applied=bool(row["applied"]),
    )


def row_to_command(row: sqlite3.Row) -> TradingCommand:
    return TradingCommand(
        id=int(row["id"]),
        command_id=str(row["command_id"]),
        command_type=TradingCommandType(str(row["command_type"])),
        client_order_id=str(row["client_order_id"]),
        payload_json=str(row["payload_json"]),
        created_at=parse_time(str(row["created_at"])),
        status=TradingCommandStatus(str(row["status"])),
        attempt_count=int(row["attempt_count"]),
        lease_until=optional_time(row["lease_until"]),
        last_error_code=None
        if row["last_error_code"] is None
        else str(row["last_error_code"]),
        dispatched_at=optional_time(row["dispatched_at"]),
    )


def row_to_fill(row: sqlite3.Row) -> ExecutionFill:
    return ExecutionFill(
        fill_id=str(row["fill_id"]),
        client_order_id=str(row["client_order_id"]),
        account_id=str(row["account_id"]),
        symbol=str(row["symbol"]),
        side=Side(str(row["side"])),
        quantity=Decimal(str(row["quantity"])),
        price=Decimal(str(row["price"])),
        occurred_at=parse_time(str(row["occurred_at"])),
        fee_asset=None if row["fee_asset"] is None else str(row["fee_asset"]),
        fee_amount=Decimal(str(row["fee_amount"])),
        exchange_order_id=None
        if row["exchange_order_id"] is None
        else str(row["exchange_order_id"]),
    )


def row_to_balance(row: sqlite3.Row) -> AssetBalance:
    return AssetBalance(
        account_id=str(row["account_id"]),
        asset=str(row["asset"]),
        free=Decimal(str(row["free"])),
        locked=Decimal(str(row["locked"])),
        updated_at=parse_time(str(row["updated_at"])),
    )


def row_to_position(row: sqlite3.Row) -> PositionSnapshot:
    return PositionSnapshot(
        account_id=str(row["account_id"]),
        symbol=str(row["symbol"]),
        quantity=Decimal(str(row["quantity"])),
        average_entry_price=Decimal(str(row["average_entry_price"])),
        realized_pnl=Decimal(str(row["realized_pnl"])),
        updated_at=parse_time(str(row["updated_at"])),
    )


def order_payload(order: OrderIntent) -> dict[str, object]:
    return {
        "account_id": order.account_id,
        "client_order_id": order.client_order_id,
        "created_at": time_text(order.created_at),
        "limit_price": str(order.limit_price),
        "order_type": order.order_type.value,
        "quantity": str(order.quantity),
        "side": order.side.value,
        "strategy_id": order.strategy_id,
        "symbol": order.symbol,
    }


def fill_payload(fill: ExecutionFill) -> dict[str, object]:
    return {
        "account_id": fill.account_id,
        "client_order_id": fill.client_order_id,
        "exchange_order_id": fill.exchange_order_id,
        "fee_amount": str(fill.fee_amount),
        "fee_asset": fill.fee_asset,
        "fill_id": fill.fill_id,
        "occurred_at": time_text(fill.occurred_at),
        "price": str(fill.price),
        "quantity": str(fill.quantity),
        "side": fill.side.value,
        "symbol": fill.symbol,
    }


def should_apply(
    current: OrderSnapshot, status: OrderStatus, filled_quantity: Decimal
) -> bool:
    if filled_quantity < current.filled_quantity:
        return False
    if current.status in _TERMINAL_STATUSES:
        return False
    if current.status is OrderStatus.UNKNOWN or status is OrderStatus.UNKNOWN:
        return current.status not in _TERMINAL_STATUSES
    current_rank = _STATUS_RANK.get(current.status)
    new_rank = _STATUS_RANK.get(status)
    return not (
        current_rank is not None
        and new_rank is not None
        and new_rank < current_rank
        and filled_quantity == current.filled_quantity
    )


def same_sign(left: Decimal, right: Decimal) -> bool:
    return (left > 0 and right > 0) or (left < 0 and right < 0)


def validate_order_time(order: OrderIntent) -> None:
    utc(order.created_at, "order.created_at")


def positive_decimal(value: object, name: str) -> Decimal:
    normalized = non_negative_decimal(value, name)
    if normalized <= 0:
        raise ValueError(f"{name} must be positive")
    return normalized


def non_negative_decimal(value: object, name: str) -> Decimal:
    try:
        normalized = Decimal(str(value))
    except Exception as error:
        raise ValueError(f"{name} must be a decimal number") from error
    if not normalized.is_finite() or normalized < 0:
        raise ValueError(f"{name} must be non-negative")
    return normalized


def identifier(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    if len(normalized) > 256:
        raise ValueError(f"{name} is too long")
    return normalized


def error_code(value: str) -> str:
    return value if _SAFE_ERROR_CODE.fullmatch(value) else "unclassified_error"


def json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def time_text(value: datetime) -> str:
    return utc(value, "datetime").isoformat(timespec="microseconds")


def parse_time(value: str) -> datetime:
    return utc(datetime.fromisoformat(value), "stored datetime")


def optional_time(value: object) -> datetime | None:
    return None if value is None else parse_time(str(value))


def attribute(value: object, name: str) -> object:
    sentinel = object()
    result = getattr(value, name, sentinel)
    if result is sentinel:
        raise TypeError(f"broker event payload is missing {name!r}")
    return result


def optional_attribute(value: object, name: str) -> object | None:
    return getattr(value, name, None)


def first_attribute(value: object, *names: str) -> object | None:
    sentinel = object()
    for name in names:
        result = getattr(value, name, sentinel)
        if result is not sentinel:
            return result
    return None


def require_row(row: sqlite3.Row | None) -> sqlite3.Row:
    if row is None:
        raise RuntimeError("SQLite OMS row disappeared")
    return row


# 历史兼容别名：oms.py 及外部调用者仍可使用原私有名称。
_row_to_order = row_to_order
_row_to_order_event = row_to_order_event
_row_to_command = row_to_command
_row_to_fill = row_to_fill
_row_to_balance = row_to_balance
_row_to_position = row_to_position
_order_payload = order_payload
_fill_payload = fill_payload
_should_apply = should_apply
_same_sign = same_sign
_validate_order_time = validate_order_time
_positive_decimal = positive_decimal
_non_negative_decimal = non_negative_decimal
_identifier = identifier
_error_code = error_code
_json = json_text
_decimal_text = decimal_text
_utc = utc
_time = time_text
_parse_time = parse_time
_optional_time = optional_time
_attribute = attribute
_optional_attribute = optional_attribute
_first_attribute = first_attribute
_require_row = require_row

__all__ = [
    "attribute",
    "decimal_text",
    "error_code",
    "fill_payload",
    "first_attribute",
    "identifier",
    "json_text",
    "non_negative_decimal",
    "optional_attribute",
    "optional_time",
    "order_payload",
    "parse_time",
    "positive_decimal",
    "require_row",
    "row_to_balance",
    "row_to_command",
    "row_to_fill",
    "row_to_order",
    "row_to_order_event",
    "row_to_position",
    "same_sign",
    "should_apply",
    "time_text",
    "utc",
    "validate_order_time",
]
