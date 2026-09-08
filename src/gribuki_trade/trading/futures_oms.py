"""合约普通单与算法单的持久化、券商无关 OMS。

该存储不认识 Binance 原始字段；服务层传入适配器规范化后的快照和事件，并通过
发件箱保存可能跨越进程或网络边界的命令。
"""
# ruff: noqa: E501

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from os import PathLike
from typing import Any, cast

from .futures_models import (
    FuturesBalanceSnapshot,
    FuturesCommand,
    FuturesCommandStatus,
    FuturesConfigSnapshot,
    FuturesFill,
    FuturesOrderKind,
    FuturesOrderSnapshot,
    FuturesOrderStatus,
    FuturesPositionSnapshot,
    FuturesProtectionPlan,
    FuturesStreamHealth,
    FuturesUserEvent,
)

_STATUS_RANK = {
    FuturesOrderStatus.UNKNOWN: 0,
    FuturesOrderStatus.NEW: 10,
    FuturesOrderStatus.PARTIALLY_FILLED: 20,
    FuturesOrderStatus.TRIGGERING: 22,
    FuturesOrderStatus.TRIGGERED: 25,
    FuturesOrderStatus.CANCELED: 30,
    FuturesOrderStatus.EXPIRED: 30,
    FuturesOrderStatus.REJECTED: 30,
    FuturesOrderStatus.FILLED: 40,
    FuturesOrderStatus.FINISHED: 40,
    FuturesOrderStatus.EXPIRED_IN_MATCH: 30,
}
_TERMINAL = frozenset(
    {
        FuturesOrderStatus.CANCELED,
        FuturesOrderStatus.EXPIRED,
        FuturesOrderStatus.REJECTED,
        FuturesOrderStatus.FILLED,
        FuturesOrderStatus.FINISHED,
        FuturesOrderStatus.EXPIRED_IN_MATCH,
    }
)


def _scope(account_id: str, environment: str, product: str) -> tuple[str, str, str]:
    values = tuple(str(value).strip() for value in (account_id, environment, product))
    if any(not value for value in values):
        raise ValueError("account_id, environment and product must not be blank")
    return values  # type: ignore[return-value]


def _time(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _parse_time(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value).astimezone(UTC)


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return _time(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def _json(value: Any) -> str:
    return json.dumps(_json_value(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _mapping(value: str) -> dict[str, Any]:
    loaded = json.loads(value)
    if not isinstance(loaded, dict):
        raise ValueError("stored JSON payload must be an object")
    return loaded


class FuturesOrderManagementStore:
    """使用 SQLite WAL/FULL、作用域隔离和崩溃 fencing 的持久化存储。"""

    def __init__(self, path: str | PathLike[str]) -> None:
        self._lock = threading.RLock()
        self._closed = False
        self._db = sqlite3.connect(path, timeout=5, isolation_level=None, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._initialize()

    def _initialize(self) -> None:
        with self._transaction() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS futures_orders (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  order_key TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
                  position_side TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL,
                  client_order_id TEXT, exchange_order_id TEXT, algo_id TEXT, client_algo_id TEXT,
                  parent_order_key TEXT, protection_plan_id TEXT, order_type TEXT,
                  execution_type TEXT, quantity TEXT NOT NULL, filled_quantity TEXT NOT NULL,
                  average_price TEXT, trigger_price TEXT, activate_price TEXT, callback_rate TEXT,
                  reduce_only INTEGER NOT NULL, close_position INTEGER NOT NULL, working_type TEXT,
                  realized_pnl TEXT NOT NULL, status_time_ms INTEGER NOT NULL, updated_at TEXT NOT NULL,
                  extra_json TEXT NOT NULL, PRIMARY KEY(account_id, environment, product, order_key)
                );
                CREATE TABLE IF NOT EXISTS futures_events (
                  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  event_id TEXT NOT NULL, event_type TEXT NOT NULL, event_time_ms INTEGER NOT NULL,
                  transaction_time_ms INTEGER, received_time_ms INTEGER NOT NULL,
                  connection_epoch INTEGER NOT NULL, payload_json TEXT NOT NULL, applied INTEGER NOT NULL,
                  UNIQUE(account_id, environment, product, event_id)
                );
                CREATE TABLE IF NOT EXISTS futures_fills (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  fill_id TEXT NOT NULL, trade_id TEXT, symbol TEXT NOT NULL, side TEXT NOT NULL,
                  position_side TEXT NOT NULL, quantity TEXT NOT NULL, price TEXT NOT NULL,
                  order_key TEXT, exchange_order_id TEXT, fee_asset TEXT, fee_amount TEXT NOT NULL,
                  realized_pnl TEXT NOT NULL, occurred_at TEXT NOT NULL, extra_json TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, fill_id),
                  UNIQUE(account_id, environment, product, symbol, trade_id)
                );
                CREATE TABLE IF NOT EXISTS futures_positions (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  symbol TEXT NOT NULL, position_side TEXT NOT NULL, quantity TEXT NOT NULL,
                  entry_price TEXT NOT NULL, break_even_price TEXT NOT NULL, realized_pnl TEXT NOT NULL,
                  unrealized_pnl TEXT NOT NULL, margin_type TEXT, isolated_wallet TEXT NOT NULL,
                  leverage INTEGER, updated_at TEXT NOT NULL, extra_json TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, symbol, position_side)
                );
                CREATE TABLE IF NOT EXISTS futures_balances (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  asset TEXT NOT NULL, wallet_balance TEXT NOT NULL, available_balance TEXT NOT NULL,
                  cross_wallet_balance TEXT NOT NULL, updated_at TEXT NOT NULL, extra_json TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, asset)
                );
                CREATE TABLE IF NOT EXISTS futures_configs (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  symbol TEXT NOT NULL, leverage INTEGER, margin_type TEXT, position_mode TEXT,
                  multi_assets_mode INTEGER, updated_at TEXT NOT NULL, extra_json TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, symbol)
                );
                CREATE TABLE IF NOT EXISTS futures_snapshot_watermarks (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  snapshot_kind TEXT NOT NULL, cutoff_at TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, snapshot_kind)
                );
                CREATE TABLE IF NOT EXISTS futures_commands (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  command_id TEXT NOT NULL, command_type TEXT NOT NULL, order_key TEXT,
                  payload_json TEXT NOT NULL, status TEXT NOT NULL, attempt_count INTEGER NOT NULL,
                  owner_id TEXT, fencing_token INTEGER, lease_until TEXT, error_code TEXT,
                  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, command_id)
                );
                CREATE TABLE IF NOT EXISTS futures_leases (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  owner_id TEXT NOT NULL, fencing_token INTEGER NOT NULL, lease_until TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product)
                );
                CREATE TABLE IF NOT EXISTS futures_stream_health (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  state TEXT NOT NULL, connection_epoch INTEGER NOT NULL, last_event_time_ms INTEGER,
                  last_received_time_ms INTEGER, gap_count INTEGER NOT NULL, reason TEXT,
                  updated_at TEXT NOT NULL, PRIMARY KEY(account_id, environment, product)
                );
                CREATE TABLE IF NOT EXISTS futures_protection_plans (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  plan_id TEXT NOT NULL, revision INTEGER NOT NULL, symbol TEXT NOT NULL,
                  position_side TEXT NOT NULL, desired_state TEXT NOT NULL, coverage_state TEXT NOT NULL,
                  entry_order_key TEXT, stop_algo_key TEXT, take_profit_algo_key TEXT,
                  trailing_algo_key TEXT, updated_at TEXT NOT NULL, extra_json TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, plan_id, revision)
                );
                CREATE INDEX IF NOT EXISTS ix_futures_commands_due
                  ON futures_commands(account_id, environment, product, status, created_at);
                """
            )

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            if self._closed:
                raise RuntimeError("Futures OMS is closed")
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
            except BaseException:
                self._db.rollback()
                raise
            else:
                self._db.commit()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                self._db.close()

    def __enter__(self) -> FuturesOrderManagementStore:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def enqueue_command(
        self,
        *,
        account_id: str,
        environment: str,
        product: str,
        command_id: str,
        command_type: str,
        payload: Mapping[str, Any],
        order: FuturesOrderSnapshot | None = None,
        order_key: str | None = None,
        occurred_at: datetime | None = None,
    ) -> FuturesCommand:
        scope = _scope(account_id, environment, product)
        if not command_id.strip() or not command_type.strip():
            raise ValueError("command_id and command_type must not be blank")
        now = _utc_or_now(occurred_at)
        payload_json = _json(payload)
        if (
            order is not None
            and _scope(order.account_id, order.environment, order.product) != scope
        ):
            raise ValueError("order intent must share the command scope")
        with self._transaction() as db:
            existing = db.execute(
                "SELECT * FROM futures_commands WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                (*scope, command_id),
            ).fetchone()
            if existing is not None:
                if (
                    existing["payload_json"] != payload_json
                    or existing["command_type"] != command_type
                ):
                    raise ValueError("command_id is already used for different command")
                return _command(existing)
            if order is not None:
                self._upsert_order(db, order, apply_stale=True)
                order_key = order.order_key
            db.execute(
                "INSERT INTO futures_commands(account_id,environment,product,command_id,command_type,order_key,payload_json,status,attempt_count,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    *scope,
                    command_id,
                    command_type,
                    order_key,
                    payload_json,
                    FuturesCommandStatus.PENDING.value,
                    0,
                    _time(now),
                    _time(now),
                ),
            )
            return _command(
                db.execute(
                    "SELECT * FROM futures_commands WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                    (*scope, command_id),
                ).fetchone()
            )

    def acquire_owner(
        self,
        *,
        account_id: str,
        environment: str,
        product: str,
        owner_id: str,
        now: datetime | None = None,
        lease_for: timedelta = timedelta(seconds=30),
    ) -> int:
        scope = _scope(account_id, environment, product)
        if lease_for.total_seconds() <= 0 or not owner_id.strip():
            raise ValueError("owner_id and lease_for are required")
        current = _utc_or_now(now)
        with self._transaction() as db:
            row = db.execute(
                "SELECT * FROM futures_leases WHERE account_id=? AND environment=? AND product=?",
                scope,
            ).fetchone()
            if (
                row is not None
                and row["owner_id"] != owner_id
                and (_parse_time(row["lease_until"]) or datetime.min.replace(tzinfo=UTC)) > current
            ):
                raise RuntimeError("Futures OMS scope is owned by another live process")
            token = 1 if row is None else int(row["fencing_token"]) + 1
            db.execute(
                "INSERT INTO futures_leases(account_id,environment,product,owner_id,fencing_token,lease_until) VALUES(?,?,?,?,?,?) ON CONFLICT(account_id,environment,product) DO UPDATE SET owner_id=excluded.owner_id,fencing_token=excluded.fencing_token,lease_until=excluded.lease_until",
                (*scope, owner_id, token, _time(current + lease_for)),
            )
            return token

    def claim_command(
        self,
        command_id: str,
        *,
        account_id: str,
        environment: str,
        product: str,
        owner_id: str,
        fencing_token: int,
        now: datetime | None = None,
        lease_for: timedelta = timedelta(seconds=30),
    ) -> FuturesCommand | None:
        scope = _scope(account_id, environment, product)
        current = _utc_or_now(now)
        if lease_for.total_seconds() <= 0:
            raise ValueError("lease_for must be positive")
        with self._transaction() as db:
            lease = db.execute(
                "SELECT * FROM futures_leases WHERE account_id=? AND environment=? AND product=?",
                scope,
            ).fetchone()
            if (
                lease is None
                or lease["owner_id"] != owner_id
                or int(lease["fencing_token"]) != fencing_token
                or (_parse_time(lease["lease_until"]) or datetime.min.replace(tzinfo=UTC))
                <= current
            ):
                raise RuntimeError("invalid or expired Futures OMS owner lease")
            row = db.execute(
                "SELECT * FROM futures_commands WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                (*scope, command_id),
            ).fetchone()
            if row is None:
                raise KeyError(command_id)
            if row["status"] != FuturesCommandStatus.PENDING.value:
                return None
            db.execute(
                "UPDATE futures_commands SET status=?,attempt_count=attempt_count+1,owner_id=?,fencing_token=?,lease_until=?,updated_at=? WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                (
                    FuturesCommandStatus.IN_FLIGHT.value,
                    owner_id,
                    fencing_token,
                    _time(current + lease_for),
                    _time(current),
                    *scope,
                    command_id,
                ),
            )
            return _command(
                db.execute(
                    "SELECT * FROM futures_commands WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                    (*scope, command_id),
                ).fetchone()
            )

    def finish_command(
        self,
        command_id: str,
        *,
        account_id: str,
        environment: str,
        product: str,
        owner_id: str,
        fencing_token: int,
        status: FuturesCommandStatus | str,
        now: datetime | None = None,
        error_code: str | None = None,
    ) -> FuturesCommand:
        scope = _scope(account_id, environment, product)
        current = _utc_or_now(now)
        desired = FuturesCommandStatus(str(status))
        if desired not in {
            FuturesCommandStatus.SENT,
            FuturesCommandStatus.UNKNOWN,
            FuturesCommandStatus.RESOLVED,
            FuturesCommandStatus.FAILED,
        }:
            raise ValueError("finished commands cannot return to dispatchable states")
        with self._transaction() as db:
            self._assert_owner(db, scope, owner_id, fencing_token, current)
            row = db.execute(
                "SELECT * FROM futures_commands WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                (*scope, command_id),
            ).fetchone()
            if row is None:
                raise KeyError(command_id)
            if (
                row["status"] != FuturesCommandStatus.IN_FLIGHT.value
                or row["owner_id"] != owner_id
                or int(row["fencing_token"]) != fencing_token
            ):
                raise RuntimeError("command fencing token or state does not match")
            db.execute(
                "UPDATE futures_commands SET status=?,lease_until=NULL,error_code=?,updated_at=? WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                (desired.value, error_code, _time(current), *scope, command_id),
            )
            return _command(
                db.execute(
                    "SELECT * FROM futures_commands WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                    (*scope, command_id),
                ).fetchone()
            )

    def _assert_owner(
        self,
        db: sqlite3.Connection,
        scope: tuple[str, str, str],
        owner_id: str,
        fencing_token: int,
        now: datetime,
    ) -> None:
        row = db.execute(
            "SELECT * FROM futures_leases WHERE account_id=? AND environment=? AND product=?",
            scope,
        ).fetchone()
        if (
            row is None
            or row["owner_id"] != owner_id
            or int(row["fencing_token"]) != fencing_token
            or (_parse_time(row["lease_until"]) or datetime.min.replace(tzinfo=UTC)) <= now
        ):
            raise RuntimeError("invalid or expired Futures OMS owner lease")

    def renew_owner(
        self,
        *,
        account_id: str,
        environment: str,
        product: str,
        owner_id: str,
        fencing_token: int,
        now: datetime | None = None,
        lease_for: timedelta = timedelta(seconds=30),
    ) -> None:
        """续租保留 fencing 代数；过期进程必须重新取得所有权。"""

        scope = _scope(account_id, environment, product)
        current = _utc_or_now(now)
        if lease_for.total_seconds() <= 0:
            raise ValueError("lease_for must be positive")
        with self._transaction() as db:
            self._assert_owner(db, scope, owner_id, fencing_token, current)
            db.execute(
                "UPDATE futures_leases SET lease_until=? WHERE account_id=? AND environment=? AND product=?",
                (_time(current + lease_for), *scope),
            )

    def release_owner(
        self,
        *,
        account_id: str,
        environment: str,
        product: str,
        owner_id: str,
        fencing_token: int,
    ) -> bool:
        """保留 fencing 历史并结束当前租约，旧持有者不能释放新租约。"""

        scope = _scope(account_id, environment, product)
        with self._transaction() as db:
            result = db.execute(
                "UPDATE futures_leases SET lease_until=? WHERE account_id=? AND environment=? AND product=? AND owner_id=? AND fencing_token=?",
                (_time(datetime.min.replace(tzinfo=UTC)), *scope, owner_id, fencing_token),
            )
            return result.rowcount == 1

    def resolve_command(
        self,
        command_id: str,
        *,
        account_id: str,
        environment: str,
        product: str,
        status: FuturesCommandStatus | str = FuturesCommandStatus.RESOLVED,
        evidence: Mapping[str, Any],
        now: datetime | None = None,
        error_code: str | None = None,
    ) -> FuturesCommand:
        """仅凭对账证据终结 UNKNOWN/SENT，无法确定结果时保持 UNKNOWN。"""

        scope = _scope(account_id, environment, product)
        desired = FuturesCommandStatus(str(status))
        if desired not in {FuturesCommandStatus.RESOLVED, FuturesCommandStatus.FAILED}:
            raise ValueError("reconciliation can only resolve or fail commands")
        if not evidence:
            raise ValueError("reconciliation evidence must not be empty")
        with self._transaction() as db:
            row = db.execute(
                "SELECT * FROM futures_commands WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                (*scope, command_id),
            ).fetchone()
            if row is None:
                raise KeyError(command_id)
            if row["status"] == desired.value:
                return _command(row)
            if row["status"] not in {
                FuturesCommandStatus.UNKNOWN.value,
                FuturesCommandStatus.SENT.value,
            }:
                raise RuntimeError("only UNKNOWN or SENT commands can be reconciled")
            payload = _mapping(row["payload_json"])
            payload["reconciliation_evidence"] = dict(evidence)
            db.execute(
                "UPDATE futures_commands SET status=?,payload_json=?,error_code=?,updated_at=? WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                (
                    desired.value,
                    _json(payload),
                    error_code,
                    _time(_utc_or_now(now)),
                    *scope,
                    command_id,
                ),
            )
            return _command(
                db.execute(
                    "SELECT * FROM futures_commands WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                    (*scope, command_id),
                ).fetchone()
            )

    def recover_inflight(
        self,
        *,
        account_id: str,
        environment: str,
        product: str,
        now: datetime | None = None,
        include_active: bool = True,
    ) -> tuple[FuturesCommand, ...]:
        scope = _scope(account_id, environment, product)
        current = _utc_or_now(now)
        with self._transaction() as db:
            where = (
                "status=?"
                if include_active
                else "status=? AND (lease_until IS NULL OR lease_until<=?)"
            )
            args: tuple[Any, ...] = (
                (*scope, FuturesCommandStatus.IN_FLIGHT.value)
                if include_active
                else (*scope, FuturesCommandStatus.IN_FLIGHT.value, _time(current))
            )
            rows = db.execute(
                f"SELECT * FROM futures_commands WHERE account_id=? AND environment=? AND product=? AND {where} ORDER BY created_at",
                args,
            ).fetchall()
            result: list[FuturesCommand] = []
            for row in rows:
                db.execute(
                    "UPDATE futures_commands SET status=?,owner_id=NULL,fencing_token=NULL,lease_until=NULL,error_code=?,updated_at=? WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                    (
                        FuturesCommandStatus.UNKNOWN.value,
                        "restart_recovery",
                        _time(current),
                        *scope,
                        row["command_id"],
                    ),
                )
                updated = db.execute(
                    "SELECT * FROM futures_commands WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                    (*scope, row["command_id"]),
                ).fetchone()
                result.append(_command(updated))
            return tuple(result)

    def command(
        self, command_id: str, *, account_id: str, environment: str, product: str
    ) -> FuturesCommand | None:
        scope = _scope(account_id, environment, product)
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM futures_commands WHERE account_id=? AND environment=? AND product=? AND command_id=?",
                (*scope, command_id),
            ).fetchone()
        return None if row is None else _command(row)

    def commands(
        self,
        *,
        account_id: str,
        environment: str,
        product: str,
        status: FuturesCommandStatus | str | None = None,
    ) -> tuple[FuturesCommand, ...]:
        scope = _scope(account_id, environment, product)
        query = "SELECT * FROM futures_commands WHERE account_id=? AND environment=? AND product=?"
        args: list[Any] = list(scope)
        if status is not None:
            query += " AND status=?"
            args.append(FuturesCommandStatus(str(status)).value)
        query += " ORDER BY created_at"
        with self._lock:
            rows = self._db.execute(query, args).fetchall()
        return tuple(_command(row) for row in rows)

    def upsert_order(self, snapshot: FuturesOrderSnapshot) -> FuturesOrderSnapshot:
        with self._transaction() as db:
            self._upsert_order(db, snapshot, apply_stale=True)
            row = db.execute(
                "SELECT * FROM futures_orders WHERE account_id=? AND environment=? AND product=? AND order_key=?",
                (snapshot.account_id, snapshot.environment, snapshot.product, snapshot.order_key),
            ).fetchone()
        return _order(row)

    def _upsert_order(
        self, db: sqlite3.Connection, snapshot: FuturesOrderSnapshot, *, apply_stale: bool
    ) -> None:
        old = db.execute(
            "SELECT * FROM futures_orders WHERE account_id=? AND environment=? AND product=? AND order_key=?",
            (snapshot.account_id, snapshot.environment, snapshot.product, snapshot.order_key),
        ).fetchone()
        if old is not None and apply_stale and not _should_apply(old, snapshot):
            return
        if old is not None:
            # 部分 WebSocket 事件不会重复发送保护参数；同一状态的更新不能擦掉
            # 之前已知的激活价、回调率和触发价。
            snapshot = _merge_order_optional_fields(snapshot, old)
        values = _order_values(snapshot)
        names = ",".join(values)
        marks = ",".join("?" for _ in values)
        updates = ",".join(
            f"{name}=excluded.{name}"
            for name in values
            if name not in {"account_id", "environment", "product", "order_key"}
        )
        db.execute(
            f"INSERT INTO futures_orders({names}) VALUES({marks}) ON CONFLICT(account_id,environment,product,order_key) DO UPDATE SET {updates}",
            tuple(values.values()),
        )

    def order(
        self, order_key: str, *, account_id: str, environment: str, product: str
    ) -> FuturesOrderSnapshot | None:
        scope = _scope(account_id, environment, product)
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM futures_orders WHERE account_id=? AND environment=? AND product=? AND order_key=?",
                (*scope, order_key),
            ).fetchone()
        return None if row is None else _order(row)

    def orders(
        self,
        *,
        account_id: str,
        environment: str,
        product: str,
        kind: FuturesOrderKind | str | None = None,
    ) -> tuple[FuturesOrderSnapshot, ...]:
        scope = _scope(account_id, environment, product)
        query = "SELECT * FROM futures_orders WHERE account_id=? AND environment=? AND product=?"
        args: list[Any] = list(scope)
        if kind is not None:
            query += " AND kind=?"
            args.append(FuturesOrderKind(str(kind)).value)
        query += " ORDER BY updated_at, order_key"
        with self._lock:
            rows = self._db.execute(query, args).fetchall()
        return tuple(_order(row) for row in rows)

    def record_fill(self, fill: FuturesFill) -> FuturesFill:
        scope = _scope(fill.account_id, fill.environment, fill.product)
        with self._transaction() as db:
            duplicate = db.execute(
                "SELECT * FROM futures_fills WHERE account_id=? AND environment=? AND product=? AND fill_id=?",
                (*scope, fill.fill_id),
            ).fetchone()
            if duplicate is not None:
                return _fill(duplicate)
            if fill.trade_id is not None:
                trade = db.execute(
                    "SELECT * FROM futures_fills WHERE account_id=? AND environment=? AND product=? AND symbol=? AND trade_id=?",
                    (*scope, fill.symbol, fill.trade_id),
                ).fetchone()
                if trade is not None:
                    return _fill(trade)
            db.execute(
                "INSERT INTO futures_fills(account_id,environment,product,fill_id,trade_id,symbol,side,position_side,quantity,price,order_key,exchange_order_id,fee_asset,fee_amount,realized_pnl,occurred_at,extra_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    *scope,
                    fill.fill_id,
                    fill.trade_id,
                    fill.symbol,
                    fill.side,
                    fill.position_side,
                    str(fill.quantity),
                    str(fill.price),
                    fill.order_key,
                    fill.exchange_order_id,
                    fill.fee_asset,
                    str(fill.fee_amount),
                    str(fill.realized_pnl),
                    _time(fill.occurred_at),
                    _json(fill.extra),
                ),
            )
            return fill

    def fills(
        self, *, account_id: str, environment: str, product: str, symbol: str | None = None
    ) -> tuple[FuturesFill, ...]:
        scope = _scope(account_id, environment, product)
        query = "SELECT * FROM futures_fills WHERE account_id=? AND environment=? AND product=?"
        args: list[Any] = list(scope)
        if symbol is not None:
            query += " AND symbol=?"
            args.append(symbol)
        query += " ORDER BY occurred_at, fill_id"
        with self._lock:
            rows = self._db.execute(query, args).fetchall()
        return tuple(_fill(row) for row in rows)

    def upsert_position(self, snapshot: FuturesPositionSnapshot) -> FuturesPositionSnapshot:
        with self._transaction() as db:
            self._upsert_position_row(db, snapshot, respect_watermark=True)
        return snapshot

    def _upsert_position_row(
        self, db: sqlite3.Connection, snapshot: FuturesPositionSnapshot, *, respect_watermark: bool
    ) -> None:
        scope = _scope(snapshot.account_id, snapshot.environment, snapshot.product)
        if respect_watermark and self._is_before_watermark(
            db, scope, "positions", snapshot.updated_at
        ):
            return
        db.execute(
            "INSERT INTO futures_positions(account_id,environment,product,symbol,position_side,quantity,entry_price,break_even_price,realized_pnl,unrealized_pnl,margin_type,isolated_wallet,leverage,updated_at,extra_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,environment,product,symbol,position_side) DO UPDATE SET quantity=excluded.quantity,entry_price=excluded.entry_price,break_even_price=excluded.break_even_price,realized_pnl=excluded.realized_pnl,unrealized_pnl=excluded.unrealized_pnl,margin_type=excluded.margin_type,isolated_wallet=excluded.isolated_wallet,leverage=excluded.leverage,updated_at=excluded.updated_at,extra_json=excluded.extra_json",
            (
                *scope,
                snapshot.symbol,
                snapshot.position_side,
                str(snapshot.quantity),
                str(snapshot.entry_price),
                str(snapshot.break_even_price),
                str(snapshot.realized_pnl),
                str(snapshot.unrealized_pnl),
                snapshot.margin_type,
                str(snapshot.isolated_wallet),
                snapshot.leverage,
                _time(snapshot.updated_at),
                _json(snapshot.extra),
            ),
        )

    @staticmethod
    def _is_before_watermark(
        db: sqlite3.Connection,
        scope: tuple[str, str, str],
        snapshot_kind: str,
        updated_at: datetime,
    ) -> bool:
        row = db.execute(
            "SELECT cutoff_at FROM futures_snapshot_watermarks WHERE account_id=? AND environment=? AND product=? AND snapshot_kind=?",
            (*scope, snapshot_kind),
        ).fetchone()
        return row is not None and _time(updated_at) <= row["cutoff_at"]

    @staticmethod
    def _set_watermark(
        db: sqlite3.Connection,
        scope: tuple[str, str, str],
        snapshot_kind: str,
        cutoff_at: datetime,
    ) -> None:
        db.execute(
            "INSERT INTO futures_snapshot_watermarks(account_id,environment,product,snapshot_kind,cutoff_at) VALUES(?,?,?,?,?) ON CONFLICT(account_id,environment,product,snapshot_kind) DO UPDATE SET cutoff_at=excluded.cutoff_at",
            (*scope, snapshot_kind, _time(cutoff_at)),
        )

    def replace_positions(
        self,
        snapshots: Iterable[FuturesPositionSnapshot],
        *,
        account_id: str,
        environment: str,
        product: str,
        cutoff_at: datetime | None = None,
    ) -> tuple[FuturesPositionSnapshot, ...]:
        """用权威全量快照替换一个作用域，空快照会清空旧仓位。"""

        scope = _scope(account_id, environment, product)
        values = tuple(snapshots)
        if any(_scope(item.account_id, item.environment, item.product) != scope for item in values):
            raise ValueError("position snapshots must share the requested scope")
        with self._transaction() as db:
            if cutoff_at is None:
                db.execute(
                    "DELETE FROM futures_positions WHERE account_id=? AND environment=? AND product=?",
                    scope,
                )
            else:
                cutoff = _utc_or_now(cutoff_at)
                db.execute(
                    "DELETE FROM futures_positions WHERE account_id=? AND environment=? AND product=? AND updated_at<=?",
                    (*scope, _time(cutoff)),
                )
                self._set_watermark(db, scope, "positions", cutoff)
            for item in values:
                self._upsert_position_row(db, item, respect_watermark=False)
        return values

    def positions(
        self, *, account_id: str, environment: str, product: str, include_empty: bool = False
    ) -> tuple[FuturesPositionSnapshot, ...]:
        scope = _scope(account_id, environment, product)
        query = "SELECT * FROM futures_positions WHERE account_id=? AND environment=? AND product=?"
        if not include_empty:
            query += " AND quantity <> '0'"
        query += " ORDER BY symbol, position_side"
        with self._lock:
            rows = self._db.execute(query, scope).fetchall()
        return tuple(_position(row) for row in rows)

    def record_balances(
        self,
        snapshots: Iterable[FuturesBalanceSnapshot],
        *,
        full_snapshot: bool = False,
        cutoff_at: datetime | None = None,
    ) -> tuple[FuturesBalanceSnapshot, ...]:
        values = tuple(snapshots)
        if not values:
            if not full_snapshot:
                return ()
            raise ValueError("an empty balance snapshot requires account_id/environment/product")
        scope = _scope(values[0].account_id, values[0].environment, values[0].product)
        if any(_scope(item.account_id, item.environment, item.product) != scope for item in values):
            raise ValueError("balance snapshots must share one scope")
        with self._transaction() as db:
            if full_snapshot:
                if cutoff_at is None:
                    db.execute(
                        "DELETE FROM futures_balances WHERE account_id=? AND environment=? AND product=?",
                        scope,
                    )
                else:
                    cutoff = _utc_or_now(cutoff_at)
                    db.execute(
                        "DELETE FROM futures_balances WHERE account_id=? AND environment=? AND product=? AND updated_at<=?",
                        (*scope, _time(cutoff)),
                    )
                    self._set_watermark(db, scope, "balances", cutoff)
            for item in values:
                if not full_snapshot and self._is_before_watermark(
                    db, scope, "balances", item.updated_at
                ):
                    continue
                db.execute(
                    "INSERT INTO futures_balances(account_id,environment,product,asset,wallet_balance,available_balance,cross_wallet_balance,updated_at,extra_json) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,environment,product,asset) DO UPDATE SET wallet_balance=excluded.wallet_balance,available_balance=excluded.available_balance,cross_wallet_balance=excluded.cross_wallet_balance,updated_at=excluded.updated_at,extra_json=excluded.extra_json",
                    (
                        *scope,
                        item.asset,
                        str(item.wallet_balance),
                        str(item.available_balance),
                        str(item.cross_wallet_balance),
                        _time(item.updated_at),
                        _json(item.extra),
                    ),
                )
        return values

    def balances(
        self, *, account_id: str, environment: str, product: str
    ) -> tuple[FuturesBalanceSnapshot, ...]:
        scope = _scope(account_id, environment, product)
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM futures_balances WHERE account_id=? AND environment=? AND product=? ORDER BY asset",
                scope,
            ).fetchall()
        return tuple(_balance(row) for row in rows)

    def replace_balances(
        self,
        snapshots: Iterable[FuturesBalanceSnapshot],
        *,
        account_id: str,
        environment: str,
        product: str,
        cutoff_at: datetime | None = None,
    ) -> tuple[FuturesBalanceSnapshot, ...]:
        """以全量账户余额替换作用域；空响应同样会清理旧资产。"""

        scope = _scope(account_id, environment, product)
        values = tuple(snapshots)
        if any(_scope(item.account_id, item.environment, item.product) != scope for item in values):
            raise ValueError("balance snapshots must share the requested scope")
        with self._transaction() as db:
            if cutoff_at is None:
                db.execute(
                    "DELETE FROM futures_balances WHERE account_id=? AND environment=? AND product=?",
                    scope,
                )
            else:
                cutoff = _utc_or_now(cutoff_at)
                db.execute(
                    "DELETE FROM futures_balances WHERE account_id=? AND environment=? AND product=? AND updated_at<=?",
                    (*scope, _time(cutoff)),
                )
                self._set_watermark(db, scope, "balances", cutoff)
            for item in values:
                db.execute(
                    "INSERT INTO futures_balances(account_id,environment,product,asset,wallet_balance,available_balance,cross_wallet_balance,updated_at,extra_json) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,environment,product,asset) DO UPDATE SET wallet_balance=excluded.wallet_balance,available_balance=excluded.available_balance,cross_wallet_balance=excluded.cross_wallet_balance,updated_at=excluded.updated_at,extra_json=excluded.extra_json",
                    (
                        *scope,
                        item.asset,
                        str(item.wallet_balance),
                        str(item.available_balance),
                        str(item.cross_wallet_balance),
                        _time(item.updated_at),
                        _json(item.extra),
                    ),
                )
        return values

    def upsert_config(self, snapshot: FuturesConfigSnapshot) -> FuturesConfigSnapshot:
        scope = _scope(snapshot.account_id, snapshot.environment, snapshot.product)
        with self._transaction() as db:
            db.execute(
                "INSERT INTO futures_configs(account_id,environment,product,symbol,leverage,margin_type,position_mode,multi_assets_mode,updated_at,extra_json) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,environment,product,symbol) DO UPDATE SET leverage=excluded.leverage,margin_type=excluded.margin_type,position_mode=excluded.position_mode,multi_assets_mode=excluded.multi_assets_mode,updated_at=excluded.updated_at,extra_json=excluded.extra_json",
                (
                    *scope,
                    snapshot.symbol,
                    snapshot.leverage,
                    snapshot.margin_type,
                    snapshot.position_mode,
                    None if snapshot.multi_assets_mode is None else int(snapshot.multi_assets_mode),
                    _time(_utc_or_now(snapshot.updated_at)),
                    _json(snapshot.extra),
                ),
            )
        return snapshot

    def set_stream_health(self, health: FuturesStreamHealth) -> FuturesStreamHealth:
        scope = _scope(health.account_id, health.environment, health.product)
        with self._transaction() as db:
            db.execute(
                "INSERT INTO futures_stream_health(account_id,environment,product,state,connection_epoch,last_event_time_ms,last_received_time_ms,gap_count,reason,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,environment,product) DO UPDATE SET state=excluded.state,connection_epoch=excluded.connection_epoch,last_event_time_ms=excluded.last_event_time_ms,last_received_time_ms=excluded.last_received_time_ms,gap_count=excluded.gap_count,reason=excluded.reason,updated_at=excluded.updated_at",
                (
                    *scope,
                    health.state,
                    health.connection_epoch,
                    health.last_event_time_ms,
                    health.last_received_time_ms,
                    health.gap_count,
                    health.reason,
                    _time(_utc_or_now(health.updated_at)),
                ),
            )
        return health

    def health(
        self, *, account_id: str, environment: str, product: str
    ) -> FuturesStreamHealth | None:
        scope = _scope(account_id, environment, product)
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM futures_stream_health WHERE account_id=? AND environment=? AND product=?",
                scope,
            ).fetchone()
        if row is None:
            return None
        return FuturesStreamHealth(
            account_id=row["account_id"],
            environment=row["environment"],
            product=row["product"],
            state=row["state"],
            connection_epoch=int(row["connection_epoch"]),
            last_event_time_ms=row["last_event_time_ms"],
            last_received_time_ms=row["last_received_time_ms"],
            gap_count=int(row["gap_count"]),
            reason=row["reason"],
            updated_at=_parse_time(row["updated_at"]) or datetime.now(UTC),
        )

    def append_event(
        self,
        event: FuturesUserEvent,
        *,
        account_id: str,
        environment: str,
        product: str,
        event_id: str | None = None,
    ) -> bool:
        scope = _scope(account_id, environment, product)
        identity = event_id or _event_identity(event)
        payload_json = _json(event.payload)
        with self._transaction() as db:
            existing = db.execute(
                "SELECT * FROM futures_events WHERE account_id=? AND environment=? AND product=? AND event_id=?",
                (*scope, identity),
            ).fetchone()
            if existing is not None:
                if existing["payload_json"] != payload_json:
                    raise ValueError("event_id is already used for different payload")
                return False
            db.execute(
                "INSERT INTO futures_events(account_id,environment,product,event_id,event_type,event_time_ms,transaction_time_ms,received_time_ms,connection_epoch,payload_json,applied) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    *scope,
                    identity,
                    event.event_type,
                    event.event_time_ms,
                    event.transaction_time_ms,
                    event.received_time_ms,
                    event.connection_epoch,
                    payload_json,
                    0,
                ),
            )
            return True

    @staticmethod
    def event_id_for(event: FuturesUserEvent) -> str:
        """返回与 append_event 一致的确定性事件标识。"""

        return _event_identity(event)

    def events(
        self, *, account_id: str, environment: str, product: str, limit: int = 500
    ) -> tuple[dict[str, Any], ...]:
        scope = _scope(account_id, environment, product)
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM futures_events WHERE account_id=? AND environment=? AND product=? ORDER BY sequence DESC LIMIT ?",
                (*scope, limit),
            ).fetchall()
        return tuple(dict(row) for row in reversed(rows))

    def mark_event_applied(
        self,
        event_id: str,
        *,
        account_id: str,
        environment: str,
        product: str,
    ) -> bool:
        """在规范快照与填充成功落库后标记事件，供重启审计使用。"""

        scope = _scope(account_id, environment, product)
        with self._transaction() as db:
            result = db.execute(
                "UPDATE futures_events SET applied=1 WHERE account_id=? AND environment=? AND product=? AND event_id=?",
                (*scope, event_id),
            )
            return result.rowcount == 1

    def upsert_protection_plan(self, plan: FuturesProtectionPlan) -> FuturesProtectionPlan:
        scope = _scope(plan.account_id, plan.environment, plan.product)
        with self._transaction() as db:
            current = db.execute(
                "SELECT * FROM futures_protection_plans WHERE account_id=? AND environment=? AND product=? AND plan_id=? ORDER BY revision DESC LIMIT 1",
                (*scope, plan.plan_id),
            ).fetchone()
            if current is not None and plan.revision < int(current["revision"]):
                return _plan(current)
            if current is not None and plan.revision == int(current["revision"]):
                current_time = _parse_time(current["updated_at"])
                if current_time is not None and plan.updated_at < current_time:
                    return _plan(current)
            db.execute(
                "INSERT INTO futures_protection_plans(account_id,environment,product,plan_id,revision,symbol,position_side,desired_state,coverage_state,entry_order_key,stop_algo_key,take_profit_algo_key,trailing_algo_key,updated_at,extra_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,environment,product,plan_id,revision) DO UPDATE SET desired_state=excluded.desired_state,coverage_state=excluded.coverage_state,entry_order_key=excluded.entry_order_key,stop_algo_key=excluded.stop_algo_key,take_profit_algo_key=excluded.take_profit_algo_key,trailing_algo_key=excluded.trailing_algo_key,updated_at=excluded.updated_at,extra_json=excluded.extra_json",
                (
                    *scope,
                    plan.plan_id,
                    plan.revision,
                    plan.symbol,
                    plan.position_side,
                    plan.desired_state,
                    plan.coverage_state,
                    plan.entry_order_key,
                    plan.stop_algo_key,
                    plan.take_profit_algo_key,
                    plan.trailing_algo_key,
                    _time(_utc_or_now(plan.updated_at)),
                    _json(plan.extra),
                ),
            )
        return plan

    def protection_plans(
        self, *, account_id: str, environment: str, product: str, plan_id: str | None = None
    ) -> tuple[FuturesProtectionPlan, ...]:
        scope = _scope(account_id, environment, product)
        query = "SELECT * FROM futures_protection_plans WHERE account_id=? AND environment=? AND product=?"
        args: list[Any] = list(scope)
        if plan_id is not None:
            query += " AND plan_id=?"
            args.append(plan_id)
        query += " ORDER BY plan_id, revision"
        with self._lock:
            rows = self._db.execute(query, args).fetchall()
        return tuple(_plan(row) for row in rows)


def _utc_or_now(value: datetime | None) -> datetime:
    current = datetime.now(UTC) if value is None else value
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return current.astimezone(UTC)


def _event_identity(event: FuturesUserEvent) -> str:
    payload = _json(event.payload)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"{event.event_type}:{event.event_time_ms}:{event.transaction_time_ms}:{digest}"


def _order_values(value: FuturesOrderSnapshot) -> dict[str, Any]:
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
        "updated_at": _time(value.updated_at),
        "extra_json": _json(value.extra),
    }


def _should_apply(row: sqlite3.Row, value: FuturesOrderSnapshot) -> bool:
    old_time = int(row["status_time_ms"])
    new_status = FuturesOrderStatus(str(value.status))
    new_rank = _STATUS_RANK[new_status]
    old_status = FuturesOrderStatus(str(row["status"]))
    old_rank = _STATUS_RANK.get(old_status, 0)
    if old_status in _TERMINAL and new_status is not old_status:
        return False
    return not (value.status_time_ms < old_time or new_rank < old_rank)


def _merge_order_optional_fields(
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
        extra=value.extra or _mapping(old["extra_json"]),
    )


def _order(row: sqlite3.Row) -> FuturesOrderSnapshot:
    return FuturesOrderSnapshot(
        account_id=row["account_id"],
        environment=row["environment"],
        product=row["product"],
        order_key=row["order_key"],
        symbol=row["symbol"],
        side=row["side"],
        position_side=row["position_side"],
        kind=row["kind"],
        status=row["status"],
        client_order_id=row["client_order_id"],
        exchange_order_id=row["exchange_order_id"],
        algo_id=row["algo_id"],
        client_algo_id=row["client_algo_id"],
        parent_order_key=row["parent_order_key"],
        protection_plan_id=row["protection_plan_id"],
        order_type=row["order_type"],
        execution_type=row["execution_type"],
        quantity=Decimal(row["quantity"]),
        filled_quantity=Decimal(row["filled_quantity"]),
        average_price=None if row["average_price"] is None else Decimal(row["average_price"]),
        trigger_price=None if row["trigger_price"] is None else Decimal(row["trigger_price"]),
        activate_price=None if row["activate_price"] is None else Decimal(row["activate_price"]),
        callback_rate=None if row["callback_rate"] is None else Decimal(row["callback_rate"]),
        reduce_only=bool(row["reduce_only"]),
        close_position=bool(row["close_position"]),
        working_type=row["working_type"],
        realized_pnl=Decimal(row["realized_pnl"]),
        status_time_ms=int(row["status_time_ms"]),
        updated_at=_parse_time(row["updated_at"]) or datetime.now(UTC),
        extra=_mapping(row["extra_json"]),
    )


def _fill(row: sqlite3.Row) -> FuturesFill:
    return FuturesFill(
        account_id=row["account_id"],
        environment=row["environment"],
        product=row["product"],
        fill_id=row["fill_id"],
        trade_id=row["trade_id"],
        symbol=row["symbol"],
        side=row["side"],
        position_side=row["position_side"],
        quantity=Decimal(row["quantity"]),
        price=Decimal(row["price"]),
        order_key=row["order_key"],
        exchange_order_id=row["exchange_order_id"],
        fee_asset=row["fee_asset"],
        fee_amount=Decimal(row["fee_amount"]),
        realized_pnl=Decimal(row["realized_pnl"]),
        occurred_at=_parse_time(row["occurred_at"]) or datetime.now(UTC),
        extra=_mapping(row["extra_json"]),
    )


def _position(row: sqlite3.Row) -> FuturesPositionSnapshot:
    return FuturesPositionSnapshot(
        account_id=row["account_id"],
        environment=row["environment"],
        product=row["product"],
        symbol=row["symbol"],
        position_side=row["position_side"],
        quantity=Decimal(row["quantity"]),
        entry_price=Decimal(row["entry_price"]),
        break_even_price=Decimal(row["break_even_price"]),
        realized_pnl=Decimal(row["realized_pnl"]),
        unrealized_pnl=Decimal(row["unrealized_pnl"]),
        margin_type=row["margin_type"],
        isolated_wallet=Decimal(row["isolated_wallet"]),
        leverage=row["leverage"],
        updated_at=_parse_time(row["updated_at"]) or datetime.now(UTC),
        extra=_mapping(row["extra_json"]),
    )


def _balance(row: sqlite3.Row) -> FuturesBalanceSnapshot:
    return FuturesBalanceSnapshot(
        account_id=row["account_id"],
        environment=row["environment"],
        product=row["product"],
        asset=row["asset"],
        wallet_balance=Decimal(row["wallet_balance"]),
        available_balance=Decimal(row["available_balance"]),
        cross_wallet_balance=Decimal(row["cross_wallet_balance"]),
        updated_at=_parse_time(row["updated_at"]) or datetime.now(UTC),
        extra=_mapping(row["extra_json"]),
    )


def _command(row: sqlite3.Row) -> FuturesCommand:
    return FuturesCommand(
        account_id=row["account_id"],
        environment=row["environment"],
        product=row["product"],
        command_id=row["command_id"],
        command_type=row["command_type"],
        order_key=row["order_key"],
        payload=_mapping(row["payload_json"]),
        status=FuturesCommandStatus(row["status"]),
        attempt_count=int(row["attempt_count"]),
        owner_id=row["owner_id"],
        fencing_token=row["fencing_token"],
        lease_until=_parse_time(row["lease_until"]),
        created_at=_parse_time(row["created_at"]) or datetime.now(UTC),
        updated_at=_parse_time(row["updated_at"]) or datetime.now(UTC),
        error_code=row["error_code"],
    )


def _plan(row: sqlite3.Row) -> FuturesProtectionPlan:
    return FuturesProtectionPlan(
        account_id=row["account_id"],
        environment=row["environment"],
        product=row["product"],
        plan_id=row["plan_id"],
        revision=int(row["revision"]),
        symbol=row["symbol"],
        position_side=row["position_side"],
        desired_state=row["desired_state"],
        coverage_state=row["coverage_state"],
        entry_order_key=row["entry_order_key"],
        stop_algo_key=row["stop_algo_key"],
        take_profit_algo_key=row["take_profit_algo_key"],
        trailing_algo_key=row["trailing_algo_key"],
        updated_at=_parse_time(row["updated_at"]) or datetime.now(UTC),
        extra=_mapping(row["extra_json"]),
    )
