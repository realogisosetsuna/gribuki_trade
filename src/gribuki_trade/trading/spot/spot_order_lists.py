"""Binance 现货订单列表的独立持久化边界。

订单列表（OCO、OTO、OTOCO）拥有独立的生命周期；列表事件不能被压扁成某一条子订单
的状态。此存储保存列表当前投影和原始事件，允许断线或进程重启后依据 REST 快照重建。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from os import PathLike


@dataclass(frozen=True, slots=True)
class SpotOrderListRecord:
    """一条现货订单列表的当前物化状态。"""

    account_id: str
    order_list_id: int | None
    list_client_order_id: str | None
    symbol: str
    contingency_type: str | None
    list_status_type: str | None
    list_order_status: str | None
    order_ids: tuple[int, ...]
    client_order_ids: tuple[str, ...]
    updated_at: datetime
    transaction_time_ms: int | None = None
    source: str = "REST"

    @property
    def key(self) -> str:
        """返回在账户内稳定且不依赖列表类型的主键。"""

        if self.order_list_id is not None and self.order_list_id >= 0:
            return f"id:{self.order_list_id}"
        if self.list_client_order_id:
            return f"client:{self.list_client_order_id}"
        raise ValueError("spot order list requires an order_list_id or client id")


@dataclass(frozen=True, slots=True)
class SpotOrderListMember:
    """订单列表中的一条子订单引用。"""

    order_id: int | None
    client_order_id: str | None
    status: str | None = None


class SQLiteSpotOrderListStore:
    """以 WAL 和完整同步保存现货订单列表投影与原始事件。"""

    def __init__(self, path: str | PathLike[str]) -> None:
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            path, timeout=5.0, isolation_level=None, check_same_thread=False
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._initialize()

    def _initialize(self) -> None:
        with self._transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS spot_order_lists (
                    account_id TEXT NOT NULL,
                    list_key TEXT NOT NULL,
                    order_list_id INTEGER,
                    list_client_order_id TEXT,
                    symbol TEXT NOT NULL,
                    contingency_type TEXT,
                    list_status_type TEXT,
                    list_order_status TEXT,
                    order_ids_json TEXT NOT NULL,
                    client_order_ids_json TEXT NOT NULL,
                    transaction_time_ms INTEGER,
                    source TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(account_id, list_key)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS spot_order_list_members (
                    account_id TEXT NOT NULL,
                    list_key TEXT NOT NULL,
                    order_id INTEGER,
                    client_order_id TEXT,
                    status TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(account_id, list_key, order_id, client_order_id),
                    CHECK(order_id IS NOT NULL OR client_order_id IS NOT NULL)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_spot_order_lists_account_status
                ON spot_order_lists(account_id, list_order_status, updated_at)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS spot_order_list_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    account_id TEXT NOT NULL,
                    list_key TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    applied INTEGER NOT NULL CHECK(applied IN (0, 1))
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_spot_order_list_events_list
                ON spot_order_list_events(account_id, list_key, sequence)
                """
            )

    def upsert(
        self,
        record: SpotOrderListRecord,
        *,
        event_id: str,
        event_type: str,
        payload: dict[str, object] | None = None,
        occurred_at: datetime | None = None,
    ) -> bool:
        """原子地写入列表事件和投影；重复或过期事件不会倒退状态。"""

        event_id = _required(event_id, "event_id")
        event_type = _required(event_type, "event_type")
        occurred = _utc(occurred_at or record.updated_at)
        record = _normalize_record(record)
        with self._transaction() as connection:
            duplicate = connection.execute(
                "SELECT 1 FROM spot_order_list_events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if duplicate is not None:
                return False
            current_row = connection.execute(
                "SELECT * FROM spot_order_lists WHERE account_id = ? AND list_key = ?",
                (record.account_id, record.key),
            ).fetchone()
            current = _row_to_record(current_row) if current_row is not None else None
            apply = current is None or _record_rank(record) >= _record_rank(current)
            connection.execute(
                """
                INSERT INTO spot_order_list_events
                (event_id, account_id, list_key, event_type, occurred_at, payload_json, applied)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    record.account_id,
                    record.key,
                    event_type,
                    _time(occurred),
                    json.dumps(payload or _record_payload(record), ensure_ascii=False),
                    int(apply),
                ),
            )
            if apply:
                connection.execute(
                    """
                    INSERT INTO spot_order_lists (
                        account_id, list_key, order_list_id, list_client_order_id, symbol,
                        contingency_type, list_status_type, list_order_status,
                        order_ids_json, client_order_ids_json, transaction_time_ms,
                        source, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(account_id, list_key) DO UPDATE SET
                        order_list_id=excluded.order_list_id,
                        list_client_order_id=excluded.list_client_order_id,
                        symbol=excluded.symbol,
                        contingency_type=excluded.contingency_type,
                        list_status_type=excluded.list_status_type,
                        list_order_status=excluded.list_order_status,
                        order_ids_json=excluded.order_ids_json,
                        client_order_ids_json=excluded.client_order_ids_json,
                        transaction_time_ms=excluded.transaction_time_ms,
                        source=excluded.source,
                        updated_at=excluded.updated_at
                    """,
                    _record_values(record),
                )
                connection.execute(
                    "DELETE FROM spot_order_list_members WHERE account_id = ? AND list_key = ?",
                    (record.account_id, record.key),
                )
                connection.executemany(
                    """
                    INSERT INTO spot_order_list_members
                    (account_id, list_key, order_id, client_order_id, status, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    tuple(
                        (
                            record.account_id,
                            record.key,
                            member.order_id,
                            member.client_order_id,
                            member.status,
                            _time(record.updated_at),
                        )
                        for member in _members(record)
                    ),
                )
            return apply

    def get(self, account_id: str, key: str) -> SpotOrderListRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM spot_order_lists WHERE account_id = ? AND list_key = ?",
                (_required(account_id, "account_id"), _required(key, "key")),
            ).fetchone()
        return None if row is None else _row_to_record(row)

    def records(
        self,
        account_id: str,
        *,
        open_only: bool = False,
        symbols: Iterable[str] | None = None,
    ) -> tuple[SpotOrderListRecord, ...]:
        account_id = _required(account_id, "account_id")
        conditions = ["account_id = ?"]
        values: list[object] = [account_id]
        if open_only:
            conditions.append(
                "COALESCE(list_order_status, '') NOT IN ('ALL_DONE', 'ALL_CANCELED', 'REJECT')"
            )
        normalized = tuple(dict.fromkeys(str(value).strip().upper() for value in symbols or ()))
        if normalized:
            placeholders = ",".join("?" for _ in normalized)
            conditions.append(f"symbol IN ({placeholders})")
            values.extend(normalized)
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM spot_order_lists WHERE {' AND '.join(conditions)} "
                "ORDER BY updated_at, list_key",
                values,
            ).fetchall()
        return tuple(_row_to_record(row) for row in rows)

    def members(self, account_id: str, key: str) -> tuple[SpotOrderListMember, ...]:
        """读取某个列表的子订单引用。"""

        with self._lock:
            rows = self._connection.execute(
                "SELECT order_id, client_order_id, status FROM spot_order_list_members "
                "WHERE account_id = ? AND list_key = ? ORDER BY rowid",
                (_required(account_id, "account_id"), _required(key, "key")),
            ).fetchall()
        return tuple(
            SpotOrderListMember(
                order_id=None if row["order_id"] is None else int(row["order_id"]),
                client_order_id=row["client_order_id"],
                status=row["status"],
            )
            for row in rows
        )

    def close(self) -> None:
        """关闭 SQLite 连接，便于安全轮换或删除运行时数据库。"""

        with self._lock:
            self._connection.close()

    def events(self, account_id: str, *, limit: int = 500) -> tuple[dict[str, object], ...]:
        if limit < 1:
            raise ValueError("limit must be positive")
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM spot_order_list_events WHERE account_id = ? "
                "ORDER BY sequence DESC LIMIT ?",
                (_required(account_id, "account_id"), limit),
            ).fetchall()
        return tuple(dict(row) for row in rows)

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except BaseException:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()


def _normalize_record(record: SpotOrderListRecord) -> SpotOrderListRecord:
    if not record.account_id.strip() or not record.symbol.strip():
        raise ValueError("account_id and symbol must not be blank")
    _ = record.key
    return SpotOrderListRecord(
        account_id=record.account_id.strip(),
        order_list_id=record.order_list_id,
        list_client_order_id=(
            record.list_client_order_id.strip() if record.list_client_order_id else None
        ),
        symbol=record.symbol.strip().upper(),
        contingency_type=record.contingency_type,
        list_status_type=record.list_status_type,
        list_order_status=record.list_order_status,
        order_ids=tuple(dict.fromkeys(record.order_ids)),
        client_order_ids=tuple(dict.fromkeys(record.client_order_ids)),
        updated_at=_utc(record.updated_at),
        transaction_time_ms=record.transaction_time_ms,
        source=record.source.strip() or "REST",
    )


def _record_rank(record: SpotOrderListRecord) -> tuple[int, datetime]:
    return (
        record.transaction_time_ms if record.transaction_time_ms is not None else -1,
        record.updated_at,
    )


def _record_values(record: SpotOrderListRecord) -> tuple[object, ...]:
    return (
        record.account_id,
        record.key,
        record.order_list_id,
        record.list_client_order_id,
        record.symbol,
        record.contingency_type,
        record.list_status_type,
        record.list_order_status,
        json.dumps(record.order_ids),
        json.dumps(record.client_order_ids, ensure_ascii=False),
        record.transaction_time_ms,
        record.source,
        _time(record.updated_at),
    )


def _record_payload(record: SpotOrderListRecord) -> dict[str, object]:
    return {
        "orderListId": record.order_list_id,
        "listClientOrderId": record.list_client_order_id,
        "symbol": record.symbol,
        "contingencyType": record.contingency_type,
        "listStatusType": record.list_status_type,
        "listOrderStatus": record.list_order_status,
        "orderIds": list(record.order_ids),
        "clientOrderIds": list(record.client_order_ids),
    }


def _members(record: SpotOrderListRecord) -> tuple[SpotOrderListMember, ...]:
    size = max(len(record.order_ids), len(record.client_order_ids))
    return tuple(
        SpotOrderListMember(
            order_id=record.order_ids[index] if index < len(record.order_ids) else None,
            client_order_id=(
                record.client_order_ids[index] if index < len(record.client_order_ids) else None
            ),
        )
        for index in range(size)
    )


def _row_to_record(row: sqlite3.Row) -> SpotOrderListRecord:
    return SpotOrderListRecord(
        account_id=str(row["account_id"]),
        order_list_id=None if row["order_list_id"] is None else int(row["order_list_id"]),
        list_client_order_id=row["list_client_order_id"],
        symbol=str(row["symbol"]),
        contingency_type=row["contingency_type"],
        list_status_type=row["list_status_type"],
        list_order_status=row["list_order_status"],
        order_ids=tuple(int(value) for value in json.loads(row["order_ids_json"])),
        client_order_ids=tuple(str(value) for value in json.loads(row["client_order_ids_json"])),
        updated_at=datetime.fromisoformat(str(row["updated_at"])).astimezone(UTC),
        transaction_time_ms=row["transaction_time_ms"],
        source=str(row["source"]),
    )


def _required(value: object, name: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name} must not be blank")
    return text


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _time(value: datetime) -> str:
    return _utc(value).isoformat()
