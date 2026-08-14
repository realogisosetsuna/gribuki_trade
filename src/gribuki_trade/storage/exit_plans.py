"""保护退出计划的防篡改只追加事件存储。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from os import PathLike

from gribuki_trade.domain.exit_plans import (
    ExitPlanEvent,
    ExitPlanEventType,
    NewExitPlanEvent,
)


class ExitPlanStoreError(RuntimeError):
    """持久退出计划事件失败的基类。"""


class ExitPlanStoreConflictError(ExitPlanStoreError):
    """同一幂等键被用于不同的语义内容。"""


class ExitPlanStoreConcurrencyError(ExitPlanStoreError):
    """保护流在调用方读取后继续推进。"""


class ExitPlanStoreIntegrityError(ExitPlanStoreError):
    """持久载荷或哈希链链接未通过校验。"""


class SQLiteExitPlanStore:
    """每个保护流各有一条哈希链的单节点 SQLite 存储。"""

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
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS exit_plan_events (
                    protection_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    event_id TEXT NOT NULL UNIQUE,
                    account_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    known_at TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    plan_id TEXT,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    previous_hash TEXT,
                    event_hash TEXT NOT NULL UNIQUE,
                    PRIMARY KEY (protection_id, sequence),
                    UNIQUE (protection_id, idempotency_key)
                ) WITHOUT ROWID
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_exit_plan_account_symbol
                ON exit_plan_events(account_id, symbol, known_at)
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS exit_plan_events_no_update
                BEFORE UPDATE ON exit_plan_events
                BEGIN
                    SELECT RAISE(ABORT, 'exit plan events are append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS exit_plan_events_no_delete
                BEFORE DELETE ON exit_plan_events
                BEGIN
                    SELECT RAISE(ABORT, 'exit plan events are append-only');
                END
                """
            )

    def append(
        self,
        event: NewExitPlanEvent,
        *,
        expected_sequence: int,
    ) -> tuple[ExitPlanEvent, bool]:
        """追加一条事件，或返回其内容完全一致的幂等重放。

        ``expected_sequence`` 是调用方最后观察到的流内序列号；新保护流取零。
        """

        if (
            isinstance(expected_sequence, bool)
            or not isinstance(expected_sequence, int)
            or expected_sequence < 0
        ):
            raise ValueError("expected_sequence must be a non-negative integer")
        payload_sha256 = hashlib.sha256(event.payload_json.encode()).hexdigest()
        with self._transaction() as connection:
            duplicate = connection.execute(
                """
                SELECT * FROM exit_plan_events
                WHERE protection_id = ? AND idempotency_key = ?
                """,
                (event.protection_id, event.idempotency_key),
            ).fetchone()
            if duplicate is not None:
                stored = _row_to_event(duplicate)
                if not _same_content(stored, event):
                    raise ExitPlanStoreConflictError(
                        "idempotency key is bound to different exit-plan content"
                    )
                return stored, False

            previous = connection.execute(
                """
                SELECT sequence, event_hash FROM exit_plan_events
                WHERE protection_id = ? ORDER BY sequence DESC LIMIT 1
                """,
                (event.protection_id,),
            ).fetchone()
            actual_sequence = 0 if previous is None else int(previous["sequence"])
            if actual_sequence != expected_sequence:
                raise ExitPlanStoreConcurrencyError(
                    f"expected protection sequence {expected_sequence}, found {actual_sequence}"
                )
            sequence = actual_sequence + 1
            previous_hash = None if previous is None else str(previous["event_hash"])
            event_hash = _event_hash(
                sequence=sequence,
                event_id=event.event_id,
                protection_id=event.protection_id,
                account_id=event.account_id,
                symbol=event.symbol,
                event_type=event.event_type,
                occurred_at=event.occurred_at,
                known_at=event.known_at,
                idempotency_key=event.idempotency_key,
                plan_id=event.plan_id,
                payload_sha256=payload_sha256,
                previous_hash=previous_hash,
            )
            try:
                connection.execute(
                    """
                    INSERT INTO exit_plan_events (
                        protection_id, sequence, event_id, account_id, symbol,
                        event_type, occurred_at, known_at, idempotency_key,
                        plan_id, payload_json, payload_sha256, previous_hash,
                        event_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.protection_id,
                        sequence,
                        event.event_id,
                        event.account_id,
                        event.symbol,
                        event.event_type.value,
                        _time(event.occurred_at),
                        _time(event.known_at),
                        event.idempotency_key,
                        event.plan_id,
                        event.payload_json,
                        payload_sha256,
                        previous_hash,
                        event_hash,
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise ExitPlanStoreConflictError(
                    "event identity, sequence, idempotency key, or hash is already used"
                ) from error
            row = connection.execute(
                """
                SELECT * FROM exit_plan_events
                WHERE protection_id = ? AND sequence = ?
                """,
                (event.protection_id, sequence),
            ).fetchone()
            if row is None:  # pragma: no cover - SQLite 插入契约
                raise ExitPlanStoreIntegrityError("inserted exit-plan event disappeared")
            return _row_to_event(row), True

    def events(self, protection_id: str) -> tuple[ExitPlanEvent, ...]:
        """读取并校验一条完整保护流。"""

        normalized = protection_id.strip()
        if not normalized:
            raise ValueError("protection_id must not be empty")
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT * FROM exit_plan_events
                WHERE protection_id = ? ORDER BY sequence
                """,
                (normalized,),
            ).fetchall()
        events = tuple(_row_to_event(row) for row in rows)
        _verify_chain(events)
        return events

    def event_by_idempotency_key(
        self,
        protection_id: str,
        idempotency_key: str,
    ) -> ExitPlanEvent | None:
        """返回一条已校验事件，且不接受不完整哈希链。"""

        events = self.events(protection_id)
        normalized = idempotency_key.strip()
        if not normalized:
            raise ValueError("idempotency_key must not be empty")
        return next(
            (event for event in events if event.idempotency_key == normalized),
            None,
        )

    def protection_ids(self, account_id: str, symbol: str) -> tuple[str, ...]:
        """列出账户/标的的保护流，并逐条验证其完整哈希链。"""

        account = account_id.strip()
        normalized_symbol = symbol.strip().upper()
        if not account or not normalized_symbol:
            raise ValueError("account_id and symbol must not be empty")
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT protection_id, MAX(known_at) AS latest_known_at
                FROM exit_plan_events
                WHERE account_id = ? AND symbol = ?
                GROUP BY protection_id
                ORDER BY latest_known_at DESC, protection_id DESC
                """,
                (account, normalized_symbol),
            ).fetchall()
        protection_ids = tuple(str(row["protection_id"]) for row in rows)
        for protection_id in protection_ids:
            self.events(protection_id)
        return protection_ids

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def __enter__(self) -> SQLiteExitPlanStore:
        self._ensure_open()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

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
            raise RuntimeError("exit-plan store is closed")


def _same_content(stored: ExitPlanEvent, event: NewExitPlanEvent) -> bool:
    return (
        stored.event_id == event.event_id
        and stored.account_id == event.account_id
        and stored.symbol == event.symbol
        and stored.event_type is event.event_type
        and stored.occurred_at == event.occurred_at
        and stored.known_at == event.known_at
        and stored.plan_id == event.plan_id
        and stored.payload_json == event.payload_json
    )


def _row_to_event(row: sqlite3.Row) -> ExitPlanEvent:
    return ExitPlanEvent(
        sequence=int(row["sequence"]),
        event_id=str(row["event_id"]),
        protection_id=str(row["protection_id"]),
        account_id=str(row["account_id"]),
        symbol=str(row["symbol"]),
        event_type=ExitPlanEventType(str(row["event_type"])),
        occurred_at=datetime.fromisoformat(str(row["occurred_at"])),
        known_at=datetime.fromisoformat(str(row["known_at"])),
        idempotency_key=str(row["idempotency_key"]),
        plan_id=None if row["plan_id"] is None else str(row["plan_id"]),
        payload_json=str(row["payload_json"]),
        payload_sha256=str(row["payload_sha256"]),
        previous_hash=None if row["previous_hash"] is None else str(row["previous_hash"]),
        event_hash=str(row["event_hash"]),
    )


def _verify_chain(events: tuple[ExitPlanEvent, ...]) -> None:
    previous_hash: str | None = None
    protection_id: str | None = None
    for expected_sequence, event in enumerate(events, start=1):
        if event.sequence != expected_sequence:
            raise ExitPlanStoreIntegrityError(
                f"non-contiguous exit-plan sequence at {event.sequence}"
            )
        if protection_id is None:
            protection_id = event.protection_id
        elif event.protection_id != protection_id:
            raise ExitPlanStoreIntegrityError("multiple protection IDs in one event stream")
        expected_event_id = NewExitPlanEvent(
            protection_id=event.protection_id,
            account_id=event.account_id,
            symbol=event.symbol,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            known_at=event.known_at,
            idempotency_key=event.idempotency_key,
            payload_json=event.payload_json,
            plan_id=event.plan_id,
        ).event_id
        if event.event_id != expected_event_id:
            raise ExitPlanStoreIntegrityError(
                f"event identity mismatch at exit-plan sequence {event.sequence}"
            )
        payload_sha256 = hashlib.sha256(event.payload_json.encode()).hexdigest()
        if event.payload_sha256 != payload_sha256:
            raise ExitPlanStoreIntegrityError(
                f"payload digest mismatch at exit-plan sequence {event.sequence}"
            )
        if event.previous_hash != previous_hash:
            raise ExitPlanStoreIntegrityError(
                f"predecessor mismatch at exit-plan sequence {event.sequence}"
            )
        expected_hash = _event_hash(
            sequence=event.sequence,
            event_id=event.event_id,
            protection_id=event.protection_id,
            account_id=event.account_id,
            symbol=event.symbol,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            known_at=event.known_at,
            idempotency_key=event.idempotency_key,
            plan_id=event.plan_id,
            payload_sha256=event.payload_sha256,
            previous_hash=event.previous_hash,
        )
        if event.event_hash != expected_hash:
            raise ExitPlanStoreIntegrityError(
                f"event hash mismatch at exit-plan sequence {event.sequence}"
            )
        previous_hash = event.event_hash


def _event_hash(
    *,
    sequence: int,
    event_id: str,
    protection_id: str,
    account_id: str,
    symbol: str,
    event_type: ExitPlanEventType,
    occurred_at: datetime,
    known_at: datetime,
    idempotency_key: str,
    plan_id: str | None,
    payload_sha256: str,
    previous_hash: str | None,
) -> str:
    document = {
        "account_id": account_id,
        "event_id": event_id,
        "event_type": event_type.value,
        "idempotency_key": idempotency_key,
        "known_at": _time(known_at),
        "occurred_at": _time(occurred_at),
        "payload_sha256": payload_sha256,
        "plan_id": plan_id,
        "previous_hash": previous_hash,
        "protection_id": protection_id,
        "sequence": sequence,
        "symbol": symbol,
    }
    encoded = json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


__all__ = [
    "ExitPlanStoreConcurrencyError",
    "ExitPlanStoreConflictError",
    "ExitPlanStoreError",
    "ExitPlanStoreIntegrityError",
    "SQLiteExitPlanStore",
]
