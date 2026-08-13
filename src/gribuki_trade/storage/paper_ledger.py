"""Tamper-evident append-only SQLite ledger for local A-share paper accounts."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from os import PathLike

from gribuki_trade.domain.paper_trading import (
    NewPaperLedgerEvent,
    PaperLedgerEvent,
    PaperLedgerEventType,
)


class PaperLedgerError(RuntimeError):
    """Base class for persistent paper-ledger failures."""


class PaperLedgerConflictError(PaperLedgerError):
    """A command conflicts with an existing idempotency or event identifier."""


class PaperLedgerConcurrencyError(PaperLedgerError):
    """The account changed after the caller projected its state."""


class PaperLedgerIntegrityError(PaperLedgerError):
    """Stored payload or hash-chain validation failed."""


class SQLitePaperLedger:
    """A single-node WAL ledger containing only immutable account events.

    There is deliberately no writable balance or position table.  The service
    rebuilds those projections from this event stream, making restart behavior
    and historical audit use the same code path.  Database triggers reject
    accidental UPDATE and DELETE statements.
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
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_ledger_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    account_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    session_date TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    previous_hash TEXT,
                    event_hash TEXT NOT NULL UNIQUE,
                    UNIQUE(account_id, idempotency_key)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_paper_ledger_account_sequence
                ON paper_ledger_events(account_id, sequence)
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS paper_ledger_no_update
                BEFORE UPDATE ON paper_ledger_events
                BEGIN
                    SELECT RAISE(ABORT, 'paper ledger is append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS paper_ledger_no_delete
                BEFORE DELETE ON paper_ledger_events
                BEGIN
                    SELECT RAISE(ABORT, 'paper ledger is append-only');
                END
                """
            )

    def append(
        self,
        event: NewPaperLedgerEvent,
        *,
        expected_sequence: int,
    ) -> tuple[PaperLedgerEvent, bool]:
        """Append atomically using optimistic concurrency.

        An exact idempotency replay returns the existing row and ``False``.
        Reusing the key with different semantic content fails closed.
        """

        if expected_sequence < 0:
            raise ValueError("expected_sequence must be non-negative")
        payload_sha256 = hashlib.sha256(event.payload_json.encode("utf-8")).hexdigest()
        with self._transaction() as connection:
            duplicate = connection.execute(
                """
                SELECT * FROM paper_ledger_events
                WHERE account_id = ? AND idempotency_key = ?
                """,
                (event.account_id, event.idempotency_key),
            ).fetchone()
            if duplicate is not None:
                stored = _row_to_event(duplicate)
                if (
                    stored.event_type is not event.event_type
                    or stored.session_date != event.session_date
                    or stored.payload_json != event.payload_json
                ):
                    raise PaperLedgerConflictError(
                        "idempotency key is already bound to different event content"
                    )
                return stored, False

            previous = connection.execute(
                """
                SELECT sequence, event_hash FROM paper_ledger_events
                WHERE account_id = ? ORDER BY sequence DESC LIMIT 1
                """,
                (event.account_id,),
            ).fetchone()
            actual_sequence = 0 if previous is None else int(previous["sequence"])
            if actual_sequence != expected_sequence:
                raise PaperLedgerConcurrencyError(
                    f"expected account sequence {expected_sequence}, found {actual_sequence}"
                )
            previous_hash = None if previous is None else str(previous["event_hash"])
            event_hash = _event_hash(
                event_id=event.event_id,
                account_id=event.account_id,
                event_type=event.event_type,
                occurred_at=event.occurred_at,
                session_date=event.session_date,
                idempotency_key=event.idempotency_key,
                payload_sha256=payload_sha256,
                previous_hash=previous_hash,
            )
            try:
                cursor = connection.execute(
                    """
                    INSERT INTO paper_ledger_events (
                        event_id, account_id, event_type, occurred_at,
                        session_date, idempotency_key, payload_json,
                        payload_sha256, previous_hash, event_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.event_id,
                        event.account_id,
                        event.event_type.value,
                        _time(event.occurred_at),
                        event.session_date.isoformat(),
                        event.idempotency_key,
                        event.payload_json,
                        payload_sha256,
                        previous_hash,
                        event_hash,
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise PaperLedgerConflictError(
                    "event_id, idempotency key, or event hash is already in use"
                ) from error
            inserted_sequence = cursor.lastrowid
            if inserted_sequence is None:  # pragma: no cover - SQLite insert contract
                raise PaperLedgerIntegrityError("inserted ledger event has no sequence")
            row = connection.execute(
                "SELECT * FROM paper_ledger_events WHERE sequence = ?",
                (inserted_sequence,),
            ).fetchone()
            if row is None:  # pragma: no cover - SQLite insert contract
                raise PaperLedgerIntegrityError("inserted ledger event disappeared")
            return _row_to_event(row), True

    def events(self, account_id: str) -> tuple[PaperLedgerEvent, ...]:
        """Read and verify one account's complete hash chain."""

        normalized = account_id.strip()
        if not normalized:
            raise ValueError("account_id must not be empty")
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT * FROM paper_ledger_events
                WHERE account_id = ? ORDER BY sequence
                """,
                (normalized,),
            ).fetchall()
        events = tuple(_row_to_event(row) for row in rows)
        _verify_chain(events)
        return events

    def event_by_idempotency_key(
        self,
        account_id: str,
        idempotency_key: str,
    ) -> PaperLedgerEvent | None:
        """Find one event, validating the complete account chain first."""

        return next(
            (
                event
                for event in self.events(account_id)
                if event.idempotency_key == idempotency_key
            ),
            None,
        )

    def account_ids(self) -> tuple[str, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                "SELECT DISTINCT account_id FROM paper_ledger_events ORDER BY account_id"
            ).fetchall()
        return tuple(str(row["account_id"]) for row in rows)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()

    def __enter__(self) -> SQLitePaperLedger:
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
            raise RuntimeError("paper ledger is closed")


def _row_to_event(row: sqlite3.Row) -> PaperLedgerEvent:
    return PaperLedgerEvent(
        sequence=int(row["sequence"]),
        event_id=str(row["event_id"]),
        account_id=str(row["account_id"]),
        event_type=PaperLedgerEventType(str(row["event_type"])),
        occurred_at=datetime.fromisoformat(str(row["occurred_at"])),
        session_date=date.fromisoformat(str(row["session_date"])),
        idempotency_key=str(row["idempotency_key"]),
        payload_json=str(row["payload_json"]),
        payload_sha256=str(row["payload_sha256"]),
        previous_hash=(str(row["previous_hash"]) if row["previous_hash"] is not None else None),
        event_hash=str(row["event_hash"]),
    )


def _verify_chain(events: tuple[PaperLedgerEvent, ...]) -> None:
    previous_hash: str | None = None
    for event in events:
        payload_sha256 = hashlib.sha256(event.payload_json.encode("utf-8")).hexdigest()
        if payload_sha256 != event.payload_sha256:
            raise PaperLedgerIntegrityError(
                f"payload digest mismatch at ledger sequence {event.sequence}"
            )
        if event.previous_hash != previous_hash:
            raise PaperLedgerIntegrityError(
                f"hash-chain predecessor mismatch at ledger sequence {event.sequence}"
            )
        expected_hash = _event_hash(
            event_id=event.event_id,
            account_id=event.account_id,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            session_date=event.session_date,
            idempotency_key=event.idempotency_key,
            payload_sha256=event.payload_sha256,
            previous_hash=event.previous_hash,
        )
        if event.event_hash != expected_hash:
            raise PaperLedgerIntegrityError(
                f"event hash mismatch at ledger sequence {event.sequence}"
            )
        previous_hash = event.event_hash


def _event_hash(
    *,
    event_id: str,
    account_id: str,
    event_type: PaperLedgerEventType,
    occurred_at: datetime,
    session_date: date,
    idempotency_key: str,
    payload_sha256: str,
    previous_hash: str | None,
) -> str:
    document = {
        "account_id": account_id,
        "event_id": event_id,
        "event_type": event_type.value,
        "idempotency_key": idempotency_key,
        "occurred_at": _time(occurred_at),
        "payload_sha256": payload_sha256,
        "previous_hash": previous_hash,
        "session_date": session_date.isoformat(),
    }
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()
