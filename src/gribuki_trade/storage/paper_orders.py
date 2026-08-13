"""Append-only order and daily-bar saga storage for A-share PAPER matching."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from os import PathLike
from typing import Any, cast


class PaperOrderStoreError(RuntimeError):
    """Base class for durable paper-order failures."""


class PaperOrderStoreConflictError(PaperOrderStoreError):
    """An idempotency key or run identity was reused with different content."""


class PaperOrderStoreLeaseError(PaperOrderStoreError):
    """A live writer owns the unfinished run."""


class PaperOrderStoreIntegrityError(PaperOrderStoreError):
    """The immutable event stream failed validation."""


class PaperOrderEventType(StrEnum):
    ORDER_SUBMISSION_STARTED = "ORDER_SUBMISSION_STARTED"
    ORDER_STATE_APPLIED = "ORDER_STATE_APPLIED"
    RUN_STARTED = "RUN_STARTED"
    ORDER_FILL_APPLIED = "ORDER_FILL_APPLIED"
    RUN_COMPLETED = "RUN_COMPLETED"


@dataclass(frozen=True, slots=True)
class PaperOrderEvent:
    sequence: int
    event_id: str
    event_type: PaperOrderEventType
    stream_id: str
    run_id: str | None
    occurred_at: datetime
    idempotency_key: str
    payload_json: str
    payload_sha256: str
    previous_hash: str | None
    event_hash: str

    @property
    def payload(self) -> dict[str, Any]:
        value = json.loads(self.payload_json)
        if not isinstance(value, dict):
            raise PaperOrderStoreIntegrityError("paper-order event payload is not an object")
        return value


@dataclass(frozen=True, slots=True)
class PaperRunRecord:
    run_id: str
    symbol: str
    trade_date: date
    config_sha256: str
    run_fingerprint: str
    started_event_sequence: int
    completed: bool


class SQLitePaperOrderStore:
    """Single-node WAL event store with an operational writer lease.

    Domain events and run identities are immutable.  The separate lease table
    is deliberately mutable and contains no business state; it only prevents
    two processes from applying the same cross-database saga concurrently.
    """

    def __init__(self, path: str | PathLike[str]) -> None:
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            path, timeout=5.0, isolation_level=None, check_same_thread=False
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._initialize()

    def _initialize(self) -> None:
        with self._transaction() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS paper_order_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    stream_id TEXT NOT NULL,
                    run_id TEXT,
                    occurred_at TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    previous_hash TEXT,
                    event_hash TEXT NOT NULL UNIQUE
                );
                CREATE INDEX IF NOT EXISTS ix_paper_order_events_stream
                    ON paper_order_events(stream_id, sequence);
                CREATE INDEX IF NOT EXISTS ix_paper_order_events_run
                    ON paper_order_events(run_id, sequence);
                CREATE TABLE IF NOT EXISTS paper_run_identities (
                    run_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    config_sha256 TEXT NOT NULL,
                    run_fingerprint TEXT NOT NULL,
                    started_event_sequence INTEGER NOT NULL,
                    UNIQUE(symbol, trade_date, config_sha256),
                    FOREIGN KEY(started_event_sequence)
                        REFERENCES paper_order_events(sequence)
                );
                CREATE TABLE IF NOT EXISTS paper_run_leases (
                    run_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    lease_until TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES paper_run_identities(run_id)
                );
                CREATE TABLE IF NOT EXISTS paper_order_writer_lease (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    owner_id TEXT NOT NULL,
                    lease_until TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS paper_order_events_no_update
                BEFORE UPDATE ON paper_order_events BEGIN
                    SELECT RAISE(ABORT, 'paper order events are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS paper_order_events_no_delete
                BEFORE DELETE ON paper_order_events BEGIN
                    SELECT RAISE(ABORT, 'paper order events are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS paper_run_identities_no_update
                BEFORE UPDATE ON paper_run_identities BEGIN
                    SELECT RAISE(ABORT, 'paper run identities are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS paper_run_identities_no_delete
                BEFORE DELETE ON paper_run_identities BEGIN
                    SELECT RAISE(ABORT, 'paper run identities are append-only');
                END;
                """
            )

    def acquire_writer(
        self,
        *,
        owner_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> None:
        """Acquire or renew the sole durable-wrapper writer lease."""

        owner_id = _identifier(owner_id, "owner_id")
        now = _aware_utc(now)
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM paper_order_writer_lease WHERE singleton = 1"
            ).fetchone()
            if row is not None:
                current_owner = str(row["owner_id"])
                current_until = datetime.fromisoformat(str(row["lease_until"]))
                if current_owner != owner_id and current_until > now:
                    raise PaperOrderStoreLeaseError(
                        "paper order store has another live writer"
                    )
            connection.execute(
                """
                INSERT INTO paper_order_writer_lease(singleton, owner_id, lease_until)
                VALUES (1, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    owner_id = excluded.owner_id,
                    lease_until = excluded.lease_until
                """,
                (owner_id, (now + lease_duration).isoformat()),
            )

    def append_event(
        self,
        *,
        event_type: PaperOrderEventType,
        stream_id: str,
        run_id: str | None,
        occurred_at: datetime,
        idempotency_key: str,
        payload: Mapping[str, object],
    ) -> tuple[PaperOrderEvent, bool]:
        document = _canonical_document(payload)
        with self._transaction() as connection:
            if run_id is None and event_type in {
                PaperOrderEventType.ORDER_SUBMISSION_STARTED,
                PaperOrderEventType.ORDER_STATE_APPLIED,
            }:
                unfinished = self._unfinished_row(connection)
                if unfinished is not None:
                    raise PaperOrderStoreLeaseError(
                        f"paper run {unfinished['run_id']} must complete before order mutation"
                    )
            return self._append_event_in_transaction(
                connection,
                event_type=event_type,
                stream_id=_identifier(stream_id, "stream_id"),
                run_id=run_id,
                occurred_at=_aware_utc(occurred_at),
                idempotency_key=_identifier(idempotency_key, "idempotency_key"),
                payload_json=document,
            )

    def begin_run(
        self,
        *,
        symbol: str,
        trade_date: date,
        config_document: Mapping[str, object],
        run_document: Mapping[str, object],
        started_at: datetime,
        owner_id: str,
        lease_duration: timedelta = timedelta(minutes=2),
    ) -> tuple[PaperRunRecord, bool]:
        """Persist RUN_STARTED and acquire the only live writer lease."""

        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        started_at = _aware_utc(started_at)
        owner_id = _identifier(owner_id, "owner_id")
        config_json = _canonical_document(config_document)
        config_sha256 = _sha(config_json)
        run_json = _canonical_document(run_document)
        fingerprint = _sha(run_json)
        logical_key = f"{symbol}|{trade_date.isoformat()}|{config_sha256}"
        run_id = "paper-run-" + _sha(logical_key)[:40]
        with self._transaction() as connection:
            dangling_submission = connection.execute(
                """
                SELECT submitted.stream_id FROM paper_order_events AS submitted
                WHERE submitted.event_type = ?
                  AND NOT EXISTS (
                    SELECT 1 FROM paper_order_events AS applied
                    WHERE applied.stream_id = submitted.stream_id
                      AND applied.event_type = ?
                      AND applied.sequence > submitted.sequence
                  )
                ORDER BY submitted.sequence LIMIT 1
                """,
                (
                    PaperOrderEventType.ORDER_SUBMISSION_STARTED.value,
                    PaperOrderEventType.ORDER_STATE_APPLIED.value,
                ),
            ).fetchone()
            if dangling_submission is not None:
                raise PaperOrderStoreLeaseError(
                    f"unfinished {dangling_submission['stream_id']} submission must settle first"
                )
            unfinished = self._unfinished_row(connection)
            if unfinished is not None and str(unfinished["run_id"]) != run_id:
                raise PaperOrderStoreLeaseError(
                    f"unfinished paper run {unfinished['run_id']} must recover first"
                )
            identity = connection.execute(
                "SELECT * FROM paper_run_identities WHERE run_id = ?", (run_id,)
            ).fetchone()
            applied_new = identity is None
            if identity is None:
                event, _ = self._append_event_in_transaction(
                    connection,
                    event_type=PaperOrderEventType.RUN_STARTED,
                    stream_id=f"run:{run_id}",
                    run_id=run_id,
                    occurred_at=started_at,
                    idempotency_key=f"run-start:{run_id}",
                    payload_json=run_json,
                )
                connection.execute(
                    """
                    INSERT INTO paper_run_identities (
                        run_id, symbol, trade_date, config_sha256,
                        run_fingerprint, started_event_sequence
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        symbol,
                        trade_date.isoformat(),
                        config_sha256,
                        fingerprint,
                        event.sequence,
                    ),
                )
            elif str(identity["run_fingerprint"]) != fingerprint:
                raise PaperOrderStoreConflictError(
                    "symbol/date/config is already bound to different bar or order state"
                )
            completed = self._run_completed(connection, run_id)
            if not completed:
                lease_now = datetime.now(UTC)
                self._acquire_lease(
                    connection,
                    run_id=run_id,
                    owner_id=owner_id,
                    now=lease_now,
                    lease_until=lease_now + lease_duration,
                )
            row = connection.execute(
                "SELECT * FROM paper_run_identities WHERE run_id = ?", (run_id,)
            ).fetchone()
            assert row is not None
            return _run_record(row, completed=completed), applied_new

    def complete_run(
        self,
        *,
        run_id: str,
        owner_id: str,
        completed_at: datetime,
        payload: Mapping[str, object],
    ) -> tuple[PaperOrderEvent, bool]:
        completed_at = _aware_utc(completed_at)
        with self._transaction() as connection:
            self._assert_lease(connection, run_id, owner_id, datetime.now(UTC))
            event = self._append_event_in_transaction(
                connection,
                event_type=PaperOrderEventType.RUN_COMPLETED,
                stream_id=f"run:{run_id}",
                run_id=run_id,
                occurred_at=completed_at,
                idempotency_key=f"run-complete:{run_id}",
                payload_json=_canonical_document(payload),
            )
            connection.execute("DELETE FROM paper_run_leases WHERE run_id = ?", (run_id,))
            return event

    def incomplete_runs(self) -> tuple[PaperRunRecord, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT i.* FROM paper_run_identities AS i
                WHERE NOT EXISTS (
                    SELECT 1 FROM paper_order_events AS e
                    WHERE e.run_id = i.run_id AND e.event_type = ?
                ) ORDER BY i.started_event_sequence
                """,
                (PaperOrderEventType.RUN_COMPLETED.value,),
            ).fetchall()
        return tuple(_run_record(row, completed=False) for row in rows)

    def run(self, run_id: str) -> PaperRunRecord | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT * FROM paper_run_identities WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                return None
            completed = self._run_completed(self._connection, run_id)
        return _run_record(row, completed=completed)

    def run_for_identity(
        self,
        *,
        symbol: str,
        trade_date: date,
        config_document: Mapping[str, object],
    ) -> PaperRunRecord | None:
        """Return the immutable run bound to symbol/date/config, if any."""

        config_sha256 = _sha(_canonical_document(config_document))
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                """
                SELECT * FROM paper_run_identities
                WHERE symbol = ? AND trade_date = ? AND config_sha256 = ?
                """,
                (symbol, trade_date.isoformat(), config_sha256),
            ).fetchone()
            if row is None:
                return None
            completed = self._run_completed(self._connection, str(row["run_id"]))
        return _run_record(row, completed=completed)

    def events(self) -> tuple[PaperOrderEvent, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                "SELECT * FROM paper_order_events ORDER BY sequence"
            ).fetchall()
        events = tuple(_row_to_event(row) for row in rows)
        _verify_chain(events)
        return events

    def run_events(self, run_id: str) -> tuple[PaperOrderEvent, ...]:
        return tuple(event for event in self.events() if event.run_id == run_id)

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                self._connection.close()

    def __enter__(self) -> SQLitePaperOrderStore:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _append_event_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        event_type: PaperOrderEventType,
        stream_id: str,
        run_id: str | None,
        occurred_at: datetime,
        idempotency_key: str,
        payload_json: str,
    ) -> tuple[PaperOrderEvent, bool]:
        duplicate = connection.execute(
            "SELECT * FROM paper_order_events WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        if duplicate is not None:
            stored = _row_to_event(duplicate)
            if (
                stored.event_type is not event_type
                or stored.stream_id != stream_id
                or stored.run_id != run_id
                or stored.occurred_at != occurred_at
                or stored.payload_json != payload_json
            ):
                raise PaperOrderStoreConflictError(
                    "idempotency key is bound to different paper-order content"
                )
            return stored, False
        previous = connection.execute(
            "SELECT event_hash FROM paper_order_events ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        previous_hash = None if previous is None else str(previous["event_hash"])
        payload_sha256 = _sha(payload_json)
        event_id = "poe-" + _sha(
            f"{idempotency_key}|{event_type.value}|{payload_sha256}"
        )[:40]
        event_hash = _event_hash(
            event_id=event_id,
            event_type=event_type,
            stream_id=stream_id,
            run_id=run_id,
            occurred_at=occurred_at,
            idempotency_key=idempotency_key,
            payload_sha256=payload_sha256,
            previous_hash=previous_hash,
        )
        try:
            cursor = connection.execute(
                """
                INSERT INTO paper_order_events (
                    event_id, event_type, stream_id, run_id, occurred_at,
                    idempotency_key, payload_json, payload_sha256,
                    previous_hash, event_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    event_type.value,
                    stream_id,
                    run_id,
                    occurred_at.isoformat(),
                    idempotency_key,
                    payload_json,
                    payload_sha256,
                    previous_hash,
                    event_hash,
                ),
            )
        except sqlite3.IntegrityError as error:
            raise PaperOrderStoreConflictError("paper-order event identity conflict") from error
        row = connection.execute(
            "SELECT * FROM paper_order_events WHERE sequence = ?", (cursor.lastrowid,)
        ).fetchone()
        assert row is not None
        return _row_to_event(row), True

    @staticmethod
    def _run_completed(connection: sqlite3.Connection, run_id: str) -> bool:
        return connection.execute(
            """
            SELECT 1 FROM paper_order_events
            WHERE run_id = ? AND event_type = ? LIMIT 1
            """,
            (run_id, PaperOrderEventType.RUN_COMPLETED.value),
        ).fetchone() is not None

    @staticmethod
    def _unfinished_row(connection: sqlite3.Connection) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            connection.execute(
            """
            SELECT i.* FROM paper_run_identities AS i
            WHERE NOT EXISTS (
                SELECT 1 FROM paper_order_events AS e
                WHERE e.run_id = i.run_id AND e.event_type = ?
            ) ORDER BY i.started_event_sequence LIMIT 1
            """,
            (PaperOrderEventType.RUN_COMPLETED.value,),
            ).fetchone(),
        )

    @staticmethod
    def _acquire_lease(
        connection: sqlite3.Connection,
        *,
        run_id: str,
        owner_id: str,
        now: datetime,
        lease_until: datetime,
    ) -> None:
        row = connection.execute(
            "SELECT * FROM paper_run_leases WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is not None:
            current_owner = str(row["owner_id"])
            current_until = datetime.fromisoformat(str(row["lease_until"]))
            if current_owner != owner_id and current_until > now:
                raise PaperOrderStoreLeaseError(
                    f"paper run {run_id} is leased by another live writer"
                )
        connection.execute(
            """
            INSERT INTO paper_run_leases(run_id, owner_id, lease_until)
            VALUES (?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                owner_id = excluded.owner_id,
                lease_until = excluded.lease_until
            """,
            (run_id, owner_id, lease_until.isoformat()),
        )

    @staticmethod
    def _assert_lease(
        connection: sqlite3.Connection,
        run_id: str,
        owner_id: str,
        now: datetime,
    ) -> None:
        row = connection.execute(
            "SELECT * FROM paper_run_leases WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None or str(row["owner_id"]) != owner_id:
            raise PaperOrderStoreLeaseError("paper run lease is missing or owned elsewhere")
        if datetime.fromisoformat(str(row["lease_until"])) < now:
            raise PaperOrderStoreLeaseError("paper run lease expired before completion")

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
            raise RuntimeError("paper order store is closed")


def _row_to_event(row: sqlite3.Row) -> PaperOrderEvent:
    return PaperOrderEvent(
        sequence=int(row["sequence"]),
        event_id=str(row["event_id"]),
        event_type=PaperOrderEventType(str(row["event_type"])),
        stream_id=str(row["stream_id"]),
        run_id=str(row["run_id"]) if row["run_id"] is not None else None,
        occurred_at=datetime.fromisoformat(str(row["occurred_at"])),
        idempotency_key=str(row["idempotency_key"]),
        payload_json=str(row["payload_json"]),
        payload_sha256=str(row["payload_sha256"]),
        previous_hash=str(row["previous_hash"]) if row["previous_hash"] else None,
        event_hash=str(row["event_hash"]),
    )


def _run_record(row: sqlite3.Row, *, completed: bool) -> PaperRunRecord:
    return PaperRunRecord(
        run_id=str(row["run_id"]),
        symbol=str(row["symbol"]),
        trade_date=date.fromisoformat(str(row["trade_date"])),
        config_sha256=str(row["config_sha256"]),
        run_fingerprint=str(row["run_fingerprint"]),
        started_event_sequence=int(row["started_event_sequence"]),
        completed=completed,
    )


def _verify_chain(events: Sequence[PaperOrderEvent]) -> None:
    previous_hash: str | None = None
    for event in events:
        if _sha(event.payload_json) != event.payload_sha256:
            raise PaperOrderStoreIntegrityError(
                f"payload digest mismatch at paper-order event {event.sequence}"
            )
        if event.previous_hash != previous_hash:
            raise PaperOrderStoreIntegrityError(
                f"hash-chain mismatch at paper-order event {event.sequence}"
            )
        expected = _event_hash(
            event_id=event.event_id,
            event_type=event.event_type,
            stream_id=event.stream_id,
            run_id=event.run_id,
            occurred_at=event.occurred_at,
            idempotency_key=event.idempotency_key,
            payload_sha256=event.payload_sha256,
            previous_hash=event.previous_hash,
        )
        if expected != event.event_hash:
            raise PaperOrderStoreIntegrityError(
                f"event digest mismatch at paper-order event {event.sequence}"
            )
        previous_hash = event.event_hash


def _event_hash(
    *,
    event_id: str,
    event_type: PaperOrderEventType,
    stream_id: str,
    run_id: str | None,
    occurred_at: datetime,
    idempotency_key: str,
    payload_sha256: str,
    previous_hash: str | None,
) -> str:
    return _sha(
        _canonical_document(
            {
                "event_id": event_id,
                "event_type": event_type.value,
                "idempotency_key": idempotency_key,
                "occurred_at": occurred_at.isoformat(),
                "payload_sha256": payload_sha256,
                "previous_hash": previous_hash,
                "run_id": run_id,
                "stream_id": stream_id,
            }
        )
    )


def _canonical_document(value: Mapping[str, object]) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise ValueError("paper-order payload must be canonical JSON data") from error


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _identifier(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
