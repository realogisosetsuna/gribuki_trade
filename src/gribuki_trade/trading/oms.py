"""SQLite WAL 订单管理、持久化命令与最小账本。"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from os import PathLike
from typing import cast
from uuid import uuid4

from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.ports.broker import BrokerEvent
from gribuki_trade.trading.models import (
    AssetBalance,
    BalanceValue,
    ExecutionFill,
    OrderEventRecord,
    OrderSnapshot,
    PositionSnapshot,
    TradingCommand,
    TradingCommandStatus,
    TradingCommandType,
)

from . import oms_codec as _oms_codec
from .oms_codec import (
    _OPEN_STATUSES,
    _TERMINAL_STATUSES,
    _attribute,
    _decimal_text,
    _error_code,
    _fill_payload,
    _first_attribute,
    _identifier,
    _json,
    _non_negative_decimal,
    _optional_attribute,
    _order_payload,
    _positive_decimal,
    _require_row,
    _row_to_balance,
    _row_to_command,
    _row_to_fill,
    _row_to_order,
    _row_to_order_event,
    _row_to_position,
    _same_sign,
    _should_apply,
    _time,
    _utc,
    _validate_order_time,
)
from .oms_schema import initialize_oms_schema

# 保留历史模块级常量，避免下游私有导入在拆分后失效。
_SAFE_ERROR_CODE = _oms_codec._SAFE_ERROR_CODE
_STATUS_RANK = _oms_codec._STATUS_RANK

ORDER_CREATED_EVENT = "ORDER_CREATED"
ORDER_STATUS_EVENT = "ORDER_STATUS"
ORDER_FILL_EVENT = "ORDER_FILL"
COMMAND_UNKNOWN_EVENT = "COMMAND_UNKNOWN"
ORDER_RECONCILED_EVENT = "ORDER_RECONCILED"



class SQLiteOrderManagementStore:
    """供 PAPER、Testnet 与实时模式共用的单节点持久化 OMS。

    每个订单均与其提交命令在同一 SQLite 事务内写入。已认领、且可能已经跨越
    进程边界的命令，在崩溃或超时后绝不自动重试：它会变为 ``UNKNOWN``，必须
    按 ``client_order_id`` 对账。
    """

    def __init__(self, path: str | PathLike[str]) -> None:
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            path,
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._initialize()

    def _initialize(self) -> None:
        with self._transaction() as connection:
            initialize_oms_schema(connection)

    def create_order(
        self,
        order: OrderIntent,
        *,
        command_id: str | None = None,
        event_id: str | None = None,
    ) -> OrderSnapshot:
        """原子化持久化新订单及其尚未发送的提交命令。"""

        _validate_order_time(order)
        resolved_command_id = _identifier(
            command_id or f"submit:{order.client_order_id}", "command_id"
        )
        resolved_event_id = _identifier(
            event_id or f"created:{order.client_order_id}", "event_id"
        )
        payload_json = _json(_order_payload(order))
        with self._transaction() as connection:
            existing_row = connection.execute(
                "SELECT * FROM oms_orders WHERE client_order_id = ?",
                (order.client_order_id,),
            ).fetchone()
            if existing_row is not None:
                existing = _row_to_order(existing_row)
                if existing.order != order:
                    raise ValueError(
                        f"client_order_id {order.client_order_id!r} is already used "
                        "for a different order"
                    )
                command_row = connection.execute(
                    "SELECT * FROM oms_command_outbox WHERE command_id = ?",
                    (resolved_command_id,),
                ).fetchone()
                if command_row is None or (
                    command_row["client_order_id"] != order.client_order_id
                    or command_row["payload_json"] != payload_json
                ):
                    raise ValueError("idempotent order exists without the matching command")
                return existing

            timestamp = _time(order.created_at)
            connection.execute(
                """
                INSERT INTO oms_orders (
                    client_order_id, account_id, strategy_id, symbol, side,
                    order_type, quantity, limit_price, created_at, status,
                    filled_quantity, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order.client_order_id,
                    order.account_id,
                    order.strategy_id,
                    order.symbol,
                    order.side.value,
                    order.order_type.value,
                    str(order.quantity),
                    str(order.limit_price),
                    timestamp,
                    OrderStatus.CREATED.value,
                    "0",
                    timestamp,
                ),
            )
            self._insert_order_event(
                connection,
                event_id=resolved_event_id,
                client_order_id=order.client_order_id,
                event_type=ORDER_CREATED_EVENT,
                status=OrderStatus.CREATED,
                occurred_at=order.created_at,
                payload_json=_json(
                    {"command_id": resolved_command_id, "order": _order_payload(order)}
                ),
                applied=True,
            )
            connection.execute(
                """
                INSERT INTO oms_command_outbox (
                    command_id, command_type, client_order_id, payload_json,
                    created_at, status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    resolved_command_id,
                    TradingCommandType.SUBMIT_ORDER.value,
                    order.client_order_id,
                    payload_json,
                    timestamp,
                    TradingCommandStatus.PENDING.value,
                ),
            )
            row = connection.execute(
                "SELECT * FROM oms_orders WHERE client_order_id = ?",
                (order.client_order_id,),
            ).fetchone()
        return _row_to_order(_require_row(row))

    def enqueue_cancel(
        self,
        client_order_id: str,
        *,
        occurred_at: datetime,
        command_id: str | None = None,
        event_id: str | None = None,
    ) -> TradingCommand:
        """原子化地将开放订单转为待撤销，并只入队一次。"""

        client_order_id = _identifier(client_order_id, "client_order_id")
        occurred_at = _utc(occurred_at, "occurred_at")
        resolved_command_id = _identifier(
            command_id or f"cancel:{client_order_id}", "command_id"
        )
        resolved_event_id = _identifier(
            event_id or f"cancel-pending:{client_order_id}", "event_id"
        )
        payload_json = _json({"client_order_id": client_order_id})
        with self._transaction() as connection:
            order_row = connection.execute(
                "SELECT * FROM oms_orders WHERE client_order_id = ?", (client_order_id,)
            ).fetchone()
            if order_row is None:
                raise KeyError(f"unknown client_order_id: {client_order_id!r}")
            order = _row_to_order(order_row)
            existing = connection.execute(
                "SELECT * FROM oms_command_outbox WHERE command_id = ?",
                (resolved_command_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["command_type"] != TradingCommandType.CANCEL_ORDER.value
                    or existing["client_order_id"] != client_order_id
                    or existing["payload_json"] != payload_json
                ):
                    raise ValueError("command_id is already used for a different command")
                return _row_to_command(existing)
            if order.status not in {
                OrderStatus.ACCEPTED,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.UNKNOWN,
            }:
                raise ValueError(
                    f"order {client_order_id!r} cannot be canceled from {order.status.value}"
                )
            connection.execute(
                """
                INSERT INTO oms_command_outbox (
                    command_id, command_type, client_order_id, payload_json,
                    created_at, status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    resolved_command_id,
                    TradingCommandType.CANCEL_ORDER.value,
                    client_order_id,
                    payload_json,
                    _time(occurred_at),
                    TradingCommandStatus.PENDING.value,
                ),
            )
            self._insert_order_event(
                connection,
                event_id=resolved_event_id,
                client_order_id=client_order_id,
                event_type=ORDER_STATUS_EVENT,
                status=OrderStatus.CANCEL_PENDING,
                occurred_at=occurred_at,
                payload_json=_json(
                    {"command_id": resolved_command_id, "status": "CANCEL_PENDING"}
                ),
                applied=True,
            )
            connection.execute(
                """
                UPDATE oms_orders SET status = ?, updated_at = ?
                WHERE client_order_id = ?
                """,
                (OrderStatus.CANCEL_PENDING.value, _time(occurred_at), client_order_id),
            )
            row = connection.execute(
                "SELECT * FROM oms_command_outbox WHERE command_id = ?",
                (resolved_command_id,),
            ).fetchone()
        return _row_to_command(_require_row(row))

    def claim_commands(
        self,
        *,
        now: datetime,
        limit: int = 50,
        lease_for: timedelta = timedelta(seconds=30),
        account_id: str | None = None,
        symbols: Iterable[str] | None = None,
    ) -> tuple[TradingCommand, ...]:
        """租赁未发送命令，可选限制在一个账户内。"""

        now = _utc(now, "now")
        normalized_account = (
            None if account_id is None else _identifier(account_id, "account_id")
        )
        normalized_symbols = (
            None
            if symbols is None
            else tuple(dict.fromkeys(_identifier(value, "symbol") for value in symbols))
        )
        if limit < 1:
            raise ValueError("limit must be positive")
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        with self._transaction() as connection:
            self._recover_claims(
                connection,
                now=now,
                include_active=False,
                account_id=normalized_account,
                symbols=normalized_symbols,
            )
            conditions = ["commands.status = ?"]
            parameters: list[object] = [TradingCommandStatus.PENDING.value]
            if normalized_account is not None:
                conditions.append("orders.account_id = ?")
                parameters.append(normalized_account)
            if normalized_symbols is not None:
                if not normalized_symbols:
                    return ()
                placeholders = ",".join("?" for _ in normalized_symbols)
                conditions.append(f"orders.symbol IN ({placeholders})")
                parameters.extend(normalized_symbols)
            parameters.append(limit)
            rows = connection.execute(
                f"""
                SELECT commands.id
                FROM oms_command_outbox AS commands
                JOIN oms_orders AS orders
                  ON orders.client_order_id = commands.client_order_id
                WHERE {' AND '.join(conditions)}
                ORDER BY commands.id LIMIT ?
                """,
                parameters,
            ).fetchall()
            ids = [int(row["id"]) for row in rows]
            if not ids:
                return ()
            placeholders = ",".join("?" for _ in ids)
            connection.execute(
                f"""
                UPDATE oms_command_outbox
                SET status = ?, attempt_count = attempt_count + 1, lease_until = ?
                WHERE id IN ({placeholders})
                """,
                (
                    TradingCommandStatus.IN_FLIGHT.value,
                    _time(now + lease_for),
                    *ids,
                ),
            )
            claimed = connection.execute(
                f"SELECT * FROM oms_command_outbox WHERE id IN ({placeholders}) ORDER BY id",
                ids,
            ).fetchall()
        return tuple(_row_to_command(row) for row in claimed)

    def claim_command(
        self,
        command_id: str,
        *,
        now: datetime,
        lease_for: timedelta = timedelta(seconds=30),
    ) -> TradingCommand:
        """租赁一条精确命令，不认领无关账户的工作。"""

        command_id = _identifier(command_id, "command_id")
        now = _utc(now, "now")
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        with self._transaction() as connection:
            self._recover_claims(connection, now=now, include_active=False)
            row = connection.execute(
                "SELECT * FROM oms_command_outbox WHERE command_id = ?",
                (command_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown command_id: {command_id!r}")
            if row["status"] != TradingCommandStatus.PENDING.value:
                raise ValueError("command is not pending")
            connection.execute(
                """
                UPDATE oms_command_outbox
                SET status = ?, attempt_count = attempt_count + 1, lease_until = ?
                WHERE command_id = ?
                """,
                (
                    TradingCommandStatus.IN_FLIGHT.value,
                    _time(now + lease_for),
                    command_id,
                ),
            )
            updated = connection.execute(
                "SELECT * FROM oms_command_outbox WHERE command_id = ?",
                (command_id,),
            ).fetchone()
        return _row_to_command(_require_row(updated))

    def mark_command_sent(
        self, command_id: str, *, occurred_at: datetime
    ) -> TradingCommand:
        """记录适配器调用返回，且未发生不明确的失败。"""

        return self._set_command_status(
            command_id,
            required=TradingCommandStatus.IN_FLIGHT,
            status=TradingCommandStatus.SENT,
            occurred_at=occurred_at,
            error_code=None,
        )

    def mark_command_unknown(
        self,
        command_id: str,
        *,
        occurred_at: datetime,
        error_code: str = "ambiguous_delivery",
    ) -> TradingCommand:
        """隔离一次不明确的发送，直至 REST 对账将其解决。"""

        command_id = _identifier(command_id, "command_id")
        occurred_at = _utc(occurred_at, "occurred_at")
        safe_error = _error_code(error_code)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM oms_command_outbox WHERE command_id = ?", (command_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown command_id: {command_id!r}")
            if row["status"] == TradingCommandStatus.UNKNOWN.value:
                return _row_to_command(row)
            if row["status"] != TradingCommandStatus.IN_FLIGHT.value:
                raise ValueError("command is not in flight")
            self._make_command_unknown(connection, row, occurred_at, safe_error)
            updated = connection.execute(
                "SELECT * FROM oms_command_outbox WHERE command_id = ?", (command_id,)
            ).fetchone()
        return _row_to_command(_require_row(updated))

    def recover_after_restart(
        self,
        *,
        now: datetime,
        account_id: str | None = None,
        symbols: Iterable[str] | None = None,
    ) -> tuple[TradingCommand, ...]:
        """隔离被遗弃的发送，可选限制在一个账户内。"""

        now = _utc(now, "now")
        normalized_account = (
            None if account_id is None else _identifier(account_id, "account_id")
        )
        normalized_symbols = (
            None
            if symbols is None
            else tuple(dict.fromkeys(_identifier(value, "symbol") for value in symbols))
        )
        with self._transaction() as connection:
            recovered_ids = self._recover_claims(
                connection,
                now=now,
                include_active=True,
                account_id=normalized_account,
                symbols=normalized_symbols,
            )
            if not recovered_ids:
                return ()
            placeholders = ",".join("?" for _ in recovered_ids)
            rows = connection.execute(
                f"SELECT * FROM oms_command_outbox WHERE id IN ({placeholders}) ORDER BY id",
                recovered_ids,
            ).fetchall()
        return tuple(_row_to_command(row) for row in rows)

    def record_order_update(
        self,
        client_order_id: str,
        *,
        event_id: str,
        status: OrderStatus,
        occurred_at: datetime,
        filled_quantity: Decimal | str | int | None = None,
        average_fill_price: Decimal | str | int | None = None,
        exchange_order_id: str | int | None = None,
        reason: str | None = None,
        broker_error_code: int | None = None,
        event_type: str = ORDER_STATUS_EVENT,
    ) -> OrderSnapshot:
        """追加幂等状态事件，并更新其物化订单。"""

        client_order_id = _identifier(client_order_id, "client_order_id")
        event_id = _identifier(event_id, "event_id")
        event_type = _identifier(event_type, "event_type")
        occurred_at = _utc(occurred_at, "occurred_at")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM oms_orders WHERE client_order_id = ?", (client_order_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown client_order_id: {client_order_id!r}")
            current = _row_to_order(row)
            normalized_filled = (
                current.filled_quantity
                if filled_quantity is None
                else _non_negative_decimal(filled_quantity, "filled_quantity")
            )
            if normalized_filled > current.order.quantity:
                raise ValueError("filled_quantity exceeds original order quantity")
            if status is OrderStatus.FILLED and normalized_filled != current.order.quantity:
                raise ValueError("FILLED status requires the complete order quantity")
            if status is OrderStatus.PARTIALLY_FILLED and not (
                Decimal("0") < normalized_filled < current.order.quantity
            ):
                raise ValueError(
                    "PARTIALLY_FILLED status requires a quantity between zero "
                    "and the order quantity"
                )
            normalized_average = (
                current.average_fill_price
                if average_fill_price is None
                else _positive_decimal(average_fill_price, "average_fill_price")
            )
            normalized_exchange_id = (
                current.exchange_order_id
                if exchange_order_id is None
                else _identifier(str(exchange_order_id), "exchange_order_id")
            )
            normalized_reason = None if reason is None else reason.strip()[:256] or None
            normalized_error_code = (
                current.broker_error_code
                if broker_error_code is None
                else int(broker_error_code)
            )
            payload_json = _json(
                {
                    "average_fill_price": _decimal_text(normalized_average),
                    "exchange_order_id": normalized_exchange_id,
                    "filled_quantity": str(normalized_filled),
                    "reason": normalized_reason,
                    "broker_error_code": normalized_error_code,
                    "status": status.value,
                }
            )
            duplicate = self._duplicate_order_event(
                connection,
                event_id=event_id,
                client_order_id=client_order_id,
                event_type=event_type,
                status=status,
                occurred_at=occurred_at,
                payload_json=payload_json,
            )
            if duplicate:
                return current

            applied = _should_apply(current, status, normalized_filled)
            self._insert_order_event(
                connection,
                event_id=event_id,
                client_order_id=client_order_id,
                event_type=event_type,
                status=status,
                occurred_at=occurred_at,
                payload_json=payload_json,
                applied=applied,
            )
            if applied:
                connection.execute(
                    """
                    UPDATE oms_orders
                    SET status = ?, filled_quantity = ?, average_fill_price = ?,
                        exchange_order_id = ?, reason = ?, broker_error_code = ?, updated_at = ?
                    WHERE client_order_id = ?
                    """,
                    (
                        status.value,
                        str(normalized_filled),
                        _decimal_text(normalized_average),
                        normalized_exchange_id,
                        normalized_reason,
                        normalized_error_code,
                        _time(max(occurred_at, current.updated_at)),
                        client_order_id,
                    ),
                )
            updated = connection.execute(
                "SELECT * FROM oms_orders WHERE client_order_id = ?", (client_order_id,)
            ).fetchone()
        return _row_to_order(_require_row(updated))

    def reconcile_order(
        self,
        client_order_id: str,
        *,
        status: OrderStatus,
        occurred_at: datetime,
        filled_quantity: Decimal | str | int | None = None,
        average_fill_price: Decimal | str | int | None = None,
        exchange_order_id: str | int | None = None,
        reason: str | None = None,
        broker_error_code: int | None = None,
        event_id: str | None = None,
    ) -> OrderSnapshot:
        """应用权威 REST 快照，并解决未知命令。"""

        resolved_event_id = event_id or f"reconcile:{client_order_id}:{uuid4()}"
        snapshot = self.record_order_update(
            client_order_id,
            event_id=resolved_event_id,
            status=status,
            occurred_at=occurred_at,
            filled_quantity=filled_quantity,
            average_fill_price=average_fill_price,
            exchange_order_id=exchange_order_id,
            reason=reason,
            broker_error_code=broker_error_code,
            event_type=ORDER_RECONCILED_EVENT,
        )
        with self._transaction() as connection:
            connection.execute(
                """
                UPDATE oms_command_outbox
                SET status = ?, lease_until = NULL, last_error_code = NULL
                WHERE client_order_id = ? AND status = ?
                """,
                (
                    TradingCommandStatus.RESOLVED.value,
                    client_order_id,
                    TradingCommandStatus.UNKNOWN.value,
                ),
            )
        return snapshot

    def record_fill(self, fill: ExecutionFill, *, event_id: str | None = None) -> ExecutionFill:
        """仅插入一次成交记录，并原子化更新订单与净持仓。"""

        resolved_event_id = _identifier(event_id or f"fill:{fill.fill_id}", "event_id")
        with self._transaction() as connection:
            duplicate = connection.execute(
                "SELECT * FROM oms_fills WHERE fill_id = ?", (fill.fill_id,)
            ).fetchone()
            if duplicate is not None:
                existing = _row_to_fill(duplicate)
                if existing != fill:
                    raise ValueError("fill_id is already used for a different fill")
                return existing
            order_row = connection.execute(
                "SELECT * FROM oms_orders WHERE client_order_id = ?",
                (fill.client_order_id,),
            ).fetchone()
            if order_row is None:
                raise KeyError(f"unknown client_order_id: {fill.client_order_id!r}")
            order = _row_to_order(order_row)
            if (
                fill.account_id != order.order.account_id
                or fill.symbol != order.order.symbol
                or fill.side is not order.order.side
            ):
                raise ValueError("fill identity does not match its order")

            known_rows = connection.execute(
                "SELECT quantity, price FROM oms_fills WHERE client_order_id = ?",
                (fill.client_order_id,),
            ).fetchall()
            known_quantity = sum(
                (Decimal(str(row["quantity"])) for row in known_rows), Decimal("0")
            ) + fill.quantity
            if known_quantity > order.order.quantity:
                raise ValueError("fill would exceed original order quantity")
            known_notional = sum(
                (
                    Decimal(str(row["quantity"])) * Decimal(str(row["price"]))
                    for row in known_rows
                ),
                Decimal("0"),
            ) + fill.quantity * fill.price
            projected_quantity = max(order.filled_quantity, known_quantity)
            projected_average = order.average_fill_price
            if known_quantity >= order.filled_quantity:
                projected_average = known_notional / known_quantity
            projected_status = order.status
            if projected_quantity == order.order.quantity:
                projected_status = OrderStatus.FILLED
            elif order.status not in _TERMINAL_STATUSES:
                projected_status = OrderStatus.PARTIALLY_FILLED

            connection.execute(
                """
                INSERT INTO oms_fills (
                    fill_id, client_order_id, account_id, symbol, side,
                    quantity, price, occurred_at, fee_asset, fee_amount,
                    exchange_order_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fill.fill_id,
                    fill.client_order_id,
                    fill.account_id,
                    fill.symbol,
                    fill.side.value,
                    str(fill.quantity),
                    str(fill.price),
                    _time(fill.occurred_at),
                    fill.fee_asset,
                    str(fill.fee_amount),
                    fill.exchange_order_id,
                ),
            )
            payload_json = _json(_fill_payload(fill))
            self._insert_order_event(
                connection,
                event_id=resolved_event_id,
                client_order_id=fill.client_order_id,
                event_type=ORDER_FILL_EVENT,
                status=projected_status,
                occurred_at=fill.occurred_at,
                payload_json=payload_json,
                applied=True,
            )
            connection.execute(
                """
                UPDATE oms_orders
                SET status = ?, filled_quantity = ?, average_fill_price = ?,
                    exchange_order_id = COALESCE(?, exchange_order_id), updated_at = ?
                WHERE client_order_id = ?
                """,
                (
                    projected_status.value,
                    str(projected_quantity),
                    _decimal_text(projected_average),
                    fill.exchange_order_id,
                    _time(max(fill.occurred_at, order.updated_at)),
                    fill.client_order_id,
                ),
            )
            self._apply_position_fill(connection, fill)
        return fill

    def record_broker_event(self, event: BrokerEvent) -> OrderSnapshot | ExecutionFill:
        """持久化既有券商端口的状态和 PAPER 成交载荷。"""

        if event.event_type == ORDER_STATUS_EVENT:
            payload = event.payload
            order_object = _attribute(payload, "order")
            if not isinstance(order_object, OrderIntent):
                raise TypeError("ORDER_STATUS payload.order must be an OrderIntent")
            status_object = _attribute(payload, "status")
            status = (
                status_object
                if isinstance(status_object, OrderStatus)
                else OrderStatus(str(status_object))
            )
            filled = _first_attribute(payload, "filled_quantity", "executed_quantity")
            average = _optional_attribute(payload, "average_fill_price")
            exchange_order_id = _optional_attribute(payload, "exchange_order_id")
            reason = _optional_attribute(payload, "reason")
            return self.record_order_update(
                order_object.client_order_id,
                event_id=event.event_id,
                status=status,
                occurred_at=event.occurred_at,
                filled_quantity=cast(Decimal | str | int | None, filled),
                average_fill_price=cast(Decimal | str | int | None, average),
                exchange_order_id=cast(str | int | None, exchange_order_id),
                reason=None if reason is None else str(reason),
            )
        if event.event_type == ORDER_FILL_EVENT:
            payload = event.payload
            client_order_id = str(_attribute(payload, "client_order_id"))
            order = self.require_order(client_order_id).order
            occurred = _optional_attribute(payload, "occurred_at")
            fill = ExecutionFill(
                fill_id=str(_attribute(payload, "fill_id")),
                client_order_id=client_order_id,
                account_id=order.account_id,
                symbol=str(_attribute(payload, "symbol")),
                side=cast(Side, _attribute(payload, "side")),
                quantity=cast(Decimal, _attribute(payload, "quantity")),
                price=cast(Decimal, _attribute(payload, "price")),
                occurred_at=event.occurred_at if occurred is None else cast(datetime, occurred),
            )
            return self.record_fill(fill, event_id=event.event_id)
        raise ValueError(f"unsupported broker event type: {event.event_type!r}")

    def record_balance_snapshot(
        self,
        account_id: str,
        balances: Iterable[BalanceValue],
        *,
        event_id: str,
        occurred_at: datetime,
        full_snapshot: bool = True,
    ) -> tuple[AssetBalance, ...]:
        """记录幂等的完整或部分权威余额快照。"""

        account_id = _identifier(account_id, "account_id")
        event_id = _identifier(event_id, "event_id")
        occurred_at = _utc(occurred_at, "occurred_at")
        materialized = tuple(balances)
        assets = [balance.asset for balance in materialized]
        if len(assets) != len(set(assets)):
            raise ValueError("balance snapshot contains duplicate assets")
        payload_json = _json(
            {
                "balances": [
                    {
                        "asset": item.asset,
                        "free": str(item.free),
                        "locked": str(item.locked),
                    }
                    for item in sorted(materialized, key=lambda item: item.asset)
                ],
                "full_snapshot": full_snapshot,
            }
        )
        with self._transaction() as connection:
            duplicate = connection.execute(
                "SELECT * FROM oms_account_events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if duplicate is not None:
                if (
                    duplicate["account_id"] != account_id
                    or duplicate["event_type"] != "BALANCE_SNAPSHOT"
                    or duplicate["occurred_at"] != _time(occurred_at)
                    or duplicate["payload_json"] != payload_json
                ):
                    raise ValueError("event_id is already used for a different account event")
                return self._balances(connection, account_id)
            connection.execute(
                """
                INSERT INTO oms_account_events (
                    event_id, account_id, event_type, occurred_at, payload_json
                ) VALUES (?, ?, 'BALANCE_SNAPSHOT', ?, ?)
                """,
                (event_id, account_id, _time(occurred_at), payload_json),
            )
            if full_snapshot:
                connection.execute(
                    "DELETE FROM oms_balances WHERE account_id = ?", (account_id,)
                )
            for item in materialized:
                connection.execute(
                    """
                    INSERT INTO oms_balances (
                        account_id, asset, free, locked, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(account_id, asset) DO UPDATE SET
                        free = excluded.free,
                        locked = excluded.locked,
                        updated_at = excluded.updated_at
                    """,
                    (
                        account_id,
                        item.asset,
                        str(item.free),
                        str(item.locked),
                        _time(occurred_at),
                    ),
                )
            return self._balances(connection, account_id)

    def order(self, client_order_id: str) -> OrderSnapshot | None:
        with self._read_lock():
            row = self._connection.execute(
                "SELECT * FROM oms_orders WHERE client_order_id = ?", (client_order_id,)
            ).fetchone()
        return None if row is None else _row_to_order(row)

    def require_order(self, client_order_id: str) -> OrderSnapshot:
        order = self.order(client_order_id)
        if order is None:
            raise KeyError(f"unknown client_order_id: {client_order_id!r}")
        return order

    def open_orders(self, *, account_id: str | None = None) -> tuple[OrderSnapshot, ...]:
        placeholders = ",".join("?" for _ in _OPEN_STATUSES)
        values: list[object] = [status.value for status in _OPEN_STATUSES]
        query = f"SELECT * FROM oms_orders WHERE status IN ({placeholders})"
        if account_id is not None:
            query += " AND account_id = ?"
            values.append(account_id)
        query += " ORDER BY created_at, client_order_id"
        with self._read_lock():
            rows = self._connection.execute(query, values).fetchall()
        return tuple(_row_to_order(row) for row in rows)

    def orders_requiring_reconciliation(
        self,
        *,
        account_id: str | None = None,
        symbols: Iterable[str] | None = None,
    ) -> tuple[OrderSnapshot, ...]:
        normalized_account = (
            None if account_id is None else _identifier(account_id, "account_id")
        )
        normalized_symbols = (
            None
            if symbols is None
            else tuple(dict.fromkeys(_identifier(value, "symbol") for value in symbols))
        )
        with self._read_lock():
            query = """
                SELECT DISTINCT orders.* FROM oms_orders AS orders
                LEFT JOIN oms_command_outbox AS commands
                  ON commands.client_order_id = orders.client_order_id
                WHERE (orders.status = ? OR commands.status = ?)
            """
            values: list[object] = [
                OrderStatus.UNKNOWN.value,
                TradingCommandStatus.UNKNOWN.value,
            ]
            if normalized_account is not None:
                query += " AND orders.account_id = ?"
                values.append(normalized_account)
            if normalized_symbols is not None:
                if not normalized_symbols:
                    return ()
                placeholders = ",".join("?" for _ in normalized_symbols)
                query += f" AND orders.symbol IN ({placeholders})"
                values.extend(normalized_symbols)
            query += " ORDER BY orders.created_at, orders.client_order_id"
            rows = self._connection.execute(query, values).fetchall()
        return tuple(_row_to_order(row) for row in rows)

    def order_events(self, client_order_id: str) -> tuple[OrderEventRecord, ...]:
        with self._read_lock():
            rows = self._connection.execute(
                """
                SELECT * FROM oms_order_events
                WHERE client_order_id = ? ORDER BY sequence
                """,
                (client_order_id,),
            ).fetchall()
        return tuple(_row_to_order_event(row) for row in rows)

    def commands(
        self, *, status: TradingCommandStatus | None = None
    ) -> tuple[TradingCommand, ...]:
        with self._read_lock():
            if status is None:
                rows = self._connection.execute(
                    "SELECT * FROM oms_command_outbox ORDER BY id"
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT * FROM oms_command_outbox WHERE status = ? ORDER BY id",
                    (status.value,),
                ).fetchall()
        return tuple(_row_to_command(row) for row in rows)

    def fills(self, *, client_order_id: str | None = None) -> tuple[ExecutionFill, ...]:
        with self._read_lock():
            if client_order_id is None:
                rows = self._connection.execute(
                    "SELECT * FROM oms_fills ORDER BY occurred_at, fill_id"
                ).fetchall()
            else:
                rows = self._connection.execute(
                    """
                    SELECT * FROM oms_fills WHERE client_order_id = ?
                    ORDER BY occurred_at, fill_id
                    """,
                    (client_order_id,),
                ).fetchall()
        return tuple(_row_to_fill(row) for row in rows)

    def balances(self, account_id: str) -> tuple[AssetBalance, ...]:
        with self._read_lock():
            return self._balances(self._connection, account_id)

    def positions(self, *, account_id: str | None = None) -> tuple[PositionSnapshot, ...]:
        with self._read_lock():
            if account_id is None:
                rows = self._connection.execute(
                    "SELECT * FROM oms_positions ORDER BY account_id, symbol"
                ).fetchall()
            else:
                rows = self._connection.execute(
                    """
                    SELECT * FROM oms_positions WHERE account_id = ? ORDER BY symbol
                    """,
                    (account_id,),
                ).fetchall()
        return tuple(_row_to_position(row) for row in rows)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()

    def __enter__(self) -> SQLiteOrderManagementStore:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _set_command_status(
        self,
        command_id: str,
        *,
        required: TradingCommandStatus,
        status: TradingCommandStatus,
        occurred_at: datetime,
        error_code: str | None,
    ) -> TradingCommand:
        command_id = _identifier(command_id, "command_id")
        occurred_at = _utc(occurred_at, "occurred_at")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM oms_command_outbox WHERE command_id = ?", (command_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown command_id: {command_id!r}")
            if row["status"] == status.value:
                return _row_to_command(row)
            if row["status"] != required.value:
                raise ValueError(f"command is not {required.value.lower()}")
            connection.execute(
                """
                UPDATE oms_command_outbox
                SET status = ?, lease_until = NULL, last_error_code = ?, dispatched_at = ?
                WHERE command_id = ?
                """,
                (status.value, error_code, _time(occurred_at), command_id),
            )
            updated = connection.execute(
                "SELECT * FROM oms_command_outbox WHERE command_id = ?", (command_id,)
            ).fetchone()
        return _row_to_command(_require_row(updated))

    def _recover_claims(
        self,
        connection: sqlite3.Connection,
        *,
        now: datetime,
        include_active: bool,
        account_id: str | None = None,
        symbols: tuple[str, ...] | None = None,
    ) -> tuple[int, ...]:
        conditions = ["commands.status = ?"]
        parameters: list[object] = [TradingCommandStatus.IN_FLIGHT.value]
        if not include_active:
            conditions.append("commands.lease_until <= ?")
            parameters.append(_time(now))
        if account_id is not None:
            conditions.append("orders.account_id = ?")
            parameters.append(account_id)
        if symbols is not None:
            if not symbols:
                return ()
            placeholders = ",".join("?" for _ in symbols)
            conditions.append(f"orders.symbol IN ({placeholders})")
            parameters.extend(symbols)
        rows = connection.execute(
            f"""
            SELECT commands.* FROM oms_command_outbox AS commands
            JOIN oms_orders AS orders
              ON orders.client_order_id = commands.client_order_id
            WHERE {' AND '.join(conditions)}
            ORDER BY commands.id
            """,
            parameters,
        ).fetchall()
        recovered: list[int] = []
        for row in rows:
            self._make_command_unknown(connection, row, now, "restart_recovery")
            recovered.append(int(row["id"]))
        return tuple(recovered)

    def _make_command_unknown(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        occurred_at: datetime,
        error_code: str,
    ) -> None:
        client_order_id = str(row["client_order_id"])
        order_row = connection.execute(
            "SELECT * FROM oms_orders WHERE client_order_id = ?", (client_order_id,)
        ).fetchone()
        order = _row_to_order(_require_row(order_row))
        status = (
            TradingCommandStatus.RESOLVED
            if order.status in _TERMINAL_STATUSES
            else TradingCommandStatus.UNKNOWN
        )
        connection.execute(
            """
            UPDATE oms_command_outbox
            SET status = ?, lease_until = NULL, last_error_code = ?, dispatched_at = ?
            WHERE id = ?
            """,
            (status.value, error_code, _time(occurred_at), int(row["id"])),
        )
        event_id = f"command-unknown:{row['command_id']}:{row['attempt_count']}"
        order_status = None if status is TradingCommandStatus.RESOLVED else OrderStatus.UNKNOWN
        self._insert_order_event(
            connection,
            event_id=event_id,
            client_order_id=client_order_id,
            event_type=COMMAND_UNKNOWN_EVENT,
            status=order_status,
            occurred_at=occurred_at,
            payload_json=_json(
                {
                    "command_id": row["command_id"],
                    "error_code": error_code,
                    "status": status.value,
                }
            ),
            applied=order_status is not None,
        )
        if order_status is not None:
            connection.execute(
                """
                UPDATE oms_orders SET status = ?, reason = ?, updated_at = ?
                WHERE client_order_id = ?
                """,
                (
                    OrderStatus.UNKNOWN.value,
                    error_code,
                    _time(max(occurred_at, order.updated_at)),
                    client_order_id,
                ),
            )

    def _apply_position_fill(
        self, connection: sqlite3.Connection, fill: ExecutionFill
    ) -> None:
        row = connection.execute(
            "SELECT * FROM oms_positions WHERE account_id = ? AND symbol = ?",
            (fill.account_id, fill.symbol),
        ).fetchone()
        if row is None:
            old_quantity = old_average = realized = Decimal("0")
        else:
            old_quantity = Decimal(str(row["quantity"]))
            old_average = Decimal(str(row["average_entry_price"]))
            realized = Decimal(str(row["realized_pnl"]))
        delta = fill.quantity if fill.side is Side.BUY else -fill.quantity
        new_quantity = old_quantity + delta
        if old_quantity == 0 or _same_sign(old_quantity, delta):
            total = abs(old_quantity) + abs(delta)
            new_average = (
                abs(old_quantity) * old_average + abs(delta) * fill.price
            ) / total
        else:
            closed = min(abs(old_quantity), abs(delta))
            if old_quantity > 0:
                realized += (fill.price - old_average) * closed
            else:
                realized += (old_average - fill.price) * closed
            if new_quantity == 0:
                new_average = Decimal("0")
            elif _same_sign(new_quantity, old_quantity):
                new_average = old_average
            else:
                new_average = fill.price
        connection.execute(
            """
            INSERT INTO oms_positions (
                account_id, symbol, quantity, average_entry_price,
                realized_pnl, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, symbol) DO UPDATE SET
                quantity = excluded.quantity,
                average_entry_price = excluded.average_entry_price,
                realized_pnl = excluded.realized_pnl,
                updated_at = excluded.updated_at
            """,
            (
                fill.account_id,
                fill.symbol,
                str(new_quantity),
                str(new_average),
                str(realized),
                _time(fill.occurred_at),
            ),
        )

    def _duplicate_order_event(
        self,
        connection: sqlite3.Connection,
        *,
        event_id: str,
        client_order_id: str,
        event_type: str,
        status: OrderStatus,
        occurred_at: datetime,
        payload_json: str,
    ) -> bool:
        row = connection.execute(
            "SELECT * FROM oms_order_events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if row is None:
            return False
        if (
            row["client_order_id"] != client_order_id
            or row["event_type"] != event_type
            or row["status"] != status.value
            or row["occurred_at"] != _time(occurred_at)
            or row["payload_json"] != payload_json
        ):
            raise ValueError("event_id is already used for a different order event")
        return True

    @staticmethod
    def _insert_order_event(
        connection: sqlite3.Connection,
        *,
        event_id: str,
        client_order_id: str,
        event_type: str,
        status: OrderStatus | None,
        occurred_at: datetime,
        payload_json: str,
        applied: bool,
    ) -> None:
        connection.execute(
            """
            INSERT INTO oms_order_events (
                event_id, client_order_id, event_type, status, occurred_at,
                payload_json, applied
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                client_order_id,
                event_type,
                None if status is None else status.value,
                _time(occurred_at),
                payload_json,
                int(applied),
            ),
        )

    @staticmethod
    def _balances(
        connection: sqlite3.Connection, account_id: str
    ) -> tuple[AssetBalance, ...]:
        rows = connection.execute(
            "SELECT * FROM oms_balances WHERE account_id = ? ORDER BY asset",
            (account_id,),
        ).fetchall()
        return tuple(_row_to_balance(row) for row in rows)

    @contextmanager
    def _read_lock(self) -> Iterator[None]:
        with self._lock:
            self._ensure_open()
            yield

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._ensure_open()
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except BaseException:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("SQLite OMS is closed")
