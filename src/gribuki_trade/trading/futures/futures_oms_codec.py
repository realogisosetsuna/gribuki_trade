"""券商中立合约 OMS 的持久化编解码函数。

SQLite 存储负责事务和状态单调性；本模块负责 SQLite 基础值与类型化合约快照之间的
转换。把这层边界单独放置后，OMS 主类更容易审查，同时不改变公开 API。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from .futures_models import (
    FuturesBalanceSnapshot,
    FuturesCommand,
    FuturesCommandStatus,
    FuturesFill,
    FuturesOrderKind,
    FuturesOrderSnapshot,
    FuturesOrderStatus,
    FuturesPositionSnapshot,
    FuturesProtectionPlan,
    FuturesUserEvent,
)


def scope(account_id: str, environment: str, product: str) -> tuple[str, str, str]:
    values = tuple(str(value).strip() for value in (account_id, environment, product))
    if any(not value for value in values):
        raise ValueError("account_id, environment and product must not be blank")
    return values  # type: ignore[return-value]


def timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def parse_timestamp(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value).astimezone(UTC)


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return timestamp(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def json_payload(value: Any) -> str:
    return json.dumps(_json_value(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def mapping_payload(value: str) -> dict[str, Any]:
    loaded = json.loads(value)
    if not isinstance(loaded, dict):
        raise ValueError("stored JSON payload must be an object")
    return loaded


def utc_or_now(value: datetime | None) -> datetime:
    current = datetime.now(UTC) if value is None else value
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return current.astimezone(UTC)


def event_identity(event: FuturesUserEvent) -> str:
    payload = json_payload(event.payload)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"{event.event_type}:{event.event_time_ms}:{event.transaction_time_ms}:{digest}"


def order_values(value: FuturesOrderSnapshot) -> dict[str, Any]:
    return {
        "account_id": value.account_id,
        "environment": value.environment,
        "product": value.product,
        "order_key": value.order_key,
        "symbol": value.symbol,
        "side": value.side,
        "position_side": value.position_side,
        "kind": FuturesOrderKind(str(value.kind)).value,
        "status": FuturesOrderStatus(str(value.status)).value,
        "client_order_id": value.client_order_id,
        "exchange_order_id": value.exchange_order_id,
        "algo_id": value.algo_id,
        "client_algo_id": value.client_algo_id,
        "parent_order_key": value.parent_order_key,
        "protection_plan_id": value.protection_plan_id,
        "order_type": value.order_type,
        "execution_type": value.execution_type,
        "quantity": str(value.quantity),
        "filled_quantity": str(value.filled_quantity),
        "average_price": None if value.average_price is None else str(value.average_price),
        "trigger_price": None if value.trigger_price is None else str(value.trigger_price),
        "activate_price": None if value.activate_price is None else str(value.activate_price),
        "callback_rate": None if value.callback_rate is None else str(value.callback_rate),
        "reduce_only": int(value.reduce_only),
        "close_position": int(value.close_position),
        "working_type": value.working_type,
        "realized_pnl": str(value.realized_pnl),
        "status_time_ms": value.status_time_ms,
        "updated_at": timestamp(value.updated_at),
        "extra_json": json_payload(value.extra),
    }


def merge_order_optional_fields(
    value: FuturesOrderSnapshot, old: sqlite3.Row
) -> FuturesOrderSnapshot:
    fields = {
        "average_price": value.average_price,
        "trigger_price": value.trigger_price,
        "activate_price": value.activate_price,
        "callback_rate": value.callback_rate,
        "working_type": value.working_type,
    }
    for name in tuple(fields):
        if fields[name] is None and old[name] is not None:
            fields[name] = Decimal(str(old[name])) if name != "working_type" else old[name]
    return FuturesOrderSnapshot(
        account_id=value.account_id,
        environment=value.environment,
        product=value.product,
        order_key=value.order_key,
        symbol=value.symbol,
        side=value.side,
        position_side=value.position_side,
        kind=value.kind,
        status=value.status,
        client_order_id=value.client_order_id or old["client_order_id"],
        exchange_order_id=value.exchange_order_id or old["exchange_order_id"],
        algo_id=value.algo_id or old["algo_id"],
        client_algo_id=value.client_algo_id or old["client_algo_id"],
        parent_order_key=value.parent_order_key or old["parent_order_key"],
        protection_plan_id=value.protection_plan_id or old["protection_plan_id"],
        order_type=value.order_type or old["order_type"],
        execution_type=value.execution_type or old["execution_type"],
        quantity=value.quantity,
        filled_quantity=max(value.filled_quantity, Decimal(old["filled_quantity"])),
        average_price=cast(Decimal | None, fields["average_price"]),
        trigger_price=cast(Decimal | None, fields["trigger_price"]),
        activate_price=cast(Decimal | None, fields["activate_price"]),
        callback_rate=cast(Decimal | None, fields["callback_rate"]),
        reduce_only=value.reduce_only,
        close_position=value.close_position,
        working_type=cast(str | None, fields["working_type"]),
        realized_pnl=value.realized_pnl,
        status_time_ms=value.status_time_ms,
        updated_at=value.updated_at,
        extra=value.extra or mapping_payload(old["extra_json"]),
    )


def order(row: sqlite3.Row) -> FuturesOrderSnapshot:
    return FuturesOrderSnapshot(
        account_id=row["account_id"], environment=row["environment"], product=row["product"],
        order_key=row["order_key"], symbol=row["symbol"], side=row["side"],
        position_side=row["position_side"], kind=row["kind"], status=row["status"],
        client_order_id=row["client_order_id"], exchange_order_id=row["exchange_order_id"],
        algo_id=row["algo_id"], client_algo_id=row["client_algo_id"],
        parent_order_key=row["parent_order_key"], protection_plan_id=row["protection_plan_id"],
        order_type=row["order_type"], execution_type=row["execution_type"],
        quantity=Decimal(row["quantity"]), filled_quantity=Decimal(row["filled_quantity"]),
        average_price=None if row["average_price"] is None else Decimal(row["average_price"]),
        trigger_price=None if row["trigger_price"] is None else Decimal(row["trigger_price"]),
        activate_price=None if row["activate_price"] is None else Decimal(row["activate_price"]),
        callback_rate=None if row["callback_rate"] is None else Decimal(row["callback_rate"]),
        reduce_only=bool(row["reduce_only"]), close_position=bool(row["close_position"]),
        working_type=row["working_type"], realized_pnl=Decimal(row["realized_pnl"]),
        status_time_ms=int(row["status_time_ms"]),
        updated_at=parse_timestamp(row["updated_at"]) or datetime.now(UTC),
        extra=mapping_payload(row["extra_json"]),
    )


def fill(row: sqlite3.Row) -> FuturesFill:
    return FuturesFill(
        account_id=row["account_id"], environment=row["environment"], product=row["product"],
        fill_id=row["fill_id"], trade_id=row["trade_id"], symbol=row["symbol"], side=row["side"],
        position_side=row["position_side"],
        quantity=Decimal(row["quantity"]),
        price=Decimal(row["price"]),
        order_key=row["order_key"], exchange_order_id=row["exchange_order_id"],
        fee_asset=row["fee_asset"], fee_amount=Decimal(row["fee_amount"]),
        realized_pnl=Decimal(row["realized_pnl"]),
        occurred_at=parse_timestamp(row["occurred_at"]) or datetime.now(UTC),
        extra=mapping_payload(row["extra_json"]),
    )


def position(row: sqlite3.Row) -> FuturesPositionSnapshot:
    return FuturesPositionSnapshot(
        account_id=row["account_id"], environment=row["environment"], product=row["product"],
        symbol=row["symbol"], position_side=row["position_side"], quantity=Decimal(row["quantity"]),
        entry_price=Decimal(row["entry_price"]), break_even_price=Decimal(row["break_even_price"]),
        realized_pnl=Decimal(row["realized_pnl"]), unrealized_pnl=Decimal(row["unrealized_pnl"]),
        margin_type=row["margin_type"], isolated_wallet=Decimal(row["isolated_wallet"]),
        leverage=row["leverage"],
        updated_at=parse_timestamp(row["updated_at"]) or datetime.now(UTC),
        extra=mapping_payload(row["extra_json"]),
    )


def balance(row: sqlite3.Row) -> FuturesBalanceSnapshot:
    return FuturesBalanceSnapshot(
        account_id=row["account_id"], environment=row["environment"], product=row["product"],
        asset=row["asset"], wallet_balance=Decimal(row["wallet_balance"]),
        available_balance=Decimal(row["available_balance"]),
        cross_wallet_balance=Decimal(row["cross_wallet_balance"]),
        updated_at=parse_timestamp(row["updated_at"]) or datetime.now(UTC),
        extra=mapping_payload(row["extra_json"]),
    )


def command(row: sqlite3.Row) -> FuturesCommand:
    return FuturesCommand(
        account_id=row["account_id"], environment=row["environment"], product=row["product"],
        command_id=row["command_id"], command_type=row["command_type"], order_key=row["order_key"],
        payload=mapping_payload(row["payload_json"]), status=FuturesCommandStatus(row["status"]),
        attempt_count=int(row["attempt_count"]), owner_id=row["owner_id"],
        fencing_token=row["fencing_token"], lease_until=parse_timestamp(row["lease_until"]),
        created_at=parse_timestamp(row["created_at"]) or datetime.now(UTC),
        updated_at=parse_timestamp(row["updated_at"]) or datetime.now(UTC),
        error_code=row["error_code"],
    )


def plan(row: sqlite3.Row) -> FuturesProtectionPlan:
    return FuturesProtectionPlan(
        account_id=row["account_id"], environment=row["environment"], product=row["product"],
        plan_id=row["plan_id"], revision=int(row["revision"]), symbol=row["symbol"],
        position_side=row["position_side"], desired_state=row["desired_state"],
        coverage_state=row["coverage_state"], entry_order_key=row["entry_order_key"],
        stop_algo_key=row["stop_algo_key"], take_profit_algo_key=row["take_profit_algo_key"],
        trailing_algo_key=row["trailing_algo_key"],
        updated_at=parse_timestamp(row["updated_at"]) or datetime.now(UTC),
        extra=mapping_payload(row["extra_json"]),
    )
