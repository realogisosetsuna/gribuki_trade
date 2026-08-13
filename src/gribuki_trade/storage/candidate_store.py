"""Append-only SQLite storage for the point-in-time candidate universe."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from os import PathLike

from gribuki_trade.domain.candidates import (
    CandidateAuditRecord,
    CandidateControlAction,
    CandidateControlEvent,
    CandidateObservation,
    CandidatePriority,
    CandidateProvenance,
    CandidateRecord,
    CandidateSource,
    CandidateStatus,
    canonical_ashare_symbol,
)

_OBSERVED_EVENT = "observed"


class CandidateEventCollisionError(ValueError):
    """An immutable event identity was reused with different content."""


class CandidateNotFoundError(LookupError):
    """A lifecycle event targeted a symbol with no visible discovery."""


class SQLiteCandidateStore:
    """SQLite WAL event store with deterministic point-in-time projections.

    The append log is authoritative. Candidate rows are reconstructed at the
    requested decision time and are not connected to an OMS or broker adapter.
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
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._initialize()

    def _initialize(self) -> None:
        with self._transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS candidate_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    symbol TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    effective_at TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_candidate_event_symbol_time
                ON candidate_events(symbol, recorded_at, effective_at, sequence)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_candidate_event_visible
                ON candidate_events(recorded_at, effective_at, symbol)
                """
            )

    def append_observation(self, item: CandidateObservation) -> bool:
        """Append a discovery; exact source-run replays return ``False``."""

        payload = _json_bytes(_observation_document(item))
        return self._append(
            event_id=item.observation_id,
            symbol=item.symbol,
            event_type=_OBSERVED_EVENT,
            effective_at=item.observed_at,
            recorded_at=item.observed_at,
            payload=payload,
        )

    def append_control(self, item: CandidateControlEvent) -> bool:
        """Append an explicit lifecycle control after verifying its parent."""

        payload = _json_bytes(_control_document(item))
        digest = hashlib.sha256(payload).hexdigest()
        timestamp = item.occurred_at.isoformat()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT payload_sha256 FROM candidate_events WHERE event_id = ?",
                (item.event_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["payload_sha256"]) != digest:
                    raise CandidateEventCollisionError(
                        "candidate event ID was reused with different content"
                    )
                return False
            parent = connection.execute(
                """
                SELECT 1 FROM candidate_events
                WHERE symbol = ? AND event_type = ?
                  AND recorded_at <= ? AND effective_at <= ?
                LIMIT 1
                """,
                (item.symbol, _OBSERVED_EVENT, timestamp, timestamp),
            ).fetchone()
            if parent is None:
                raise CandidateNotFoundError(
                    "candidate control requires a discovery visible at occurred_at"
                )
            connection.execute(
                """
                INSERT INTO candidate_events(
                    event_id, symbol, event_type, effective_at, recorded_at,
                    payload_json, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.event_id,
                    item.symbol,
                    item.action.value,
                    timestamp,
                    timestamp,
                    payload.decode("utf-8"),
                    digest,
                ),
            )
        return True

    def get_candidate(
        self,
        symbol: str,
        *,
        as_of: datetime,
    ) -> CandidateRecord | None:
        """Return the state visible at ``as_of`` without look-ahead."""

        canonical = canonical_ashare_symbol(symbol)
        cutoff = _utc(as_of, "as_of").isoformat()
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT * FROM candidate_events
                WHERE symbol = ? AND recorded_at <= ? AND effective_at <= ?
                ORDER BY effective_at, sequence
                """,
                (canonical, cutoff, cutoff),
            ).fetchall()
        return _project(canonical, datetime.fromisoformat(cutoff), rows)

    def list_candidates(
        self,
        *,
        as_of: datetime,
        statuses: frozenset[CandidateStatus] | None = None,
        limit: int = 500,
    ) -> tuple[CandidateRecord, ...]:
        """Project and rank all candidates visible at ``as_of``."""

        if limit < 1:
            raise ValueError("limit must be positive")
        if statuses is not None and not statuses:
            raise ValueError("statuses must not be empty when supplied")
        cutoff_at = _utc(as_of, "as_of")
        cutoff = cutoff_at.isoformat()
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT * FROM candidate_events
                WHERE recorded_at <= ? AND effective_at <= ?
                ORDER BY symbol, effective_at, sequence
                """,
                (cutoff, cutoff),
            ).fetchall()
        grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in rows:
            grouped[str(row["symbol"])].append(row)
        projected = tuple(
            item
            for symbol, symbol_rows in grouped.items()
            if (item := _project(symbol, cutoff_at, symbol_rows)) is not None
            and (statuses is None or item.status in statuses)
        )
        ordered = sorted(
            projected,
            key=lambda item: (-int(item.priority), -item.last_observed_at.timestamp(), item.symbol),
        )
        return tuple(ordered[:limit])

    def history(
        self,
        symbol: str,
        *,
        as_of: datetime | None = None,
        limit: int = 1_000,
    ) -> tuple[CandidateAuditRecord, ...]:
        """Return immutable audit metadata in append order."""

        if limit < 1:
            raise ValueError("limit must be positive")
        canonical = canonical_ashare_symbol(symbol)
        parameters: tuple[object, ...]
        query = "SELECT * FROM candidate_events WHERE symbol = ?"
        if as_of is None:
            parameters = (canonical, limit)
        else:
            cutoff = _utc(as_of, "as_of").isoformat()
            query += " AND recorded_at <= ? AND effective_at <= ?"
            parameters = (canonical, cutoff, cutoff, limit)
        query += " ORDER BY sequence LIMIT ?"
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(query, parameters).fetchall()
        return tuple(_audit_record(row) for row in rows)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()

    def __enter__(self) -> SQLiteCandidateStore:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _append(
        self,
        *,
        event_id: str,
        symbol: str,
        event_type: str,
        effective_at: datetime,
        recorded_at: datetime,
        payload: bytes,
    ) -> bool:
        digest = hashlib.sha256(payload).hexdigest()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT payload_sha256 FROM candidate_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["payload_sha256"]) != digest:
                    raise CandidateEventCollisionError(
                        "candidate event ID was reused with different content"
                    )
                return False
            connection.execute(
                """
                INSERT INTO candidate_events(
                    event_id, symbol, event_type, effective_at, recorded_at,
                    payload_json, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    symbol,
                    event_type,
                    effective_at.isoformat(),
                    recorded_at.isoformat(),
                    payload.decode("utf-8"),
                    digest,
                ),
            )
        return True

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
            raise RuntimeError("candidate store is closed")


def _project(
    symbol: str,
    as_of: datetime,
    rows: list[sqlite3.Row] | tuple[sqlite3.Row, ...],
) -> CandidateRecord | None:
    observations: list[CandidateObservation] = []
    removed = False
    cooling_until: datetime | None = None
    for row in rows:
        document = _verified_document(row)
        event_type = str(row["event_type"])
        if str(document.get("symbol")) != symbol:
            raise ValueError("stored candidate event symbol is inconsistent")
        if event_type == _OBSERVED_EVENT:
            observation = _observation_from_document(document)
            if observation.observation_id != str(row["event_id"]):
                raise ValueError("stored candidate observation identity is inconsistent")
            _validate_event_timestamps(
                row,
                effective_at=observation.observed_at,
                recorded_at=observation.observed_at,
            )
            observations.append(observation)
        elif event_type in {item.value for item in CandidateControlAction}:
            control = _control_from_document(document)
            if control.action.value != event_type or control.event_id != str(row["event_id"]):
                raise ValueError("stored candidate control identity is inconsistent")
            _validate_event_timestamps(
                row,
                effective_at=control.occurred_at,
                recorded_at=control.occurred_at,
            )
            if control.action is CandidateControlAction.REMOVE:
                removed = True
                cooling_until = None
            elif control.action is CandidateControlAction.ACTIVATE:
                removed = False
                cooling_until = None
            else:
                cooling_until = control.cooling_until
        else:
            raise ValueError("stored candidate event type is unsupported")
    if not observations:
        return None

    provenance = tuple(
        CandidateProvenance(
            observation_id=item.observation_id,
            source=item.source,
            source_run_id=item.source_run_id,
            discovered_at=item.discovered_at,
            observed_at=item.observed_at,
            expires_at=item.expires_at,
            priority=item.priority,
            reason_codes=item.reason_codes,
            evidence_ids=item.evidence_ids,
        )
        for item in observations
    )
    live = tuple(
        item for item in provenance if item.expires_at is None or item.expires_at > as_of
    )
    priority_pool = live or provenance
    priority = max((item.priority for item in priority_pool), key=int)
    if removed:
        status = CandidateStatus.REMOVED
    elif not live:
        status = CandidateStatus.EXPIRED
    elif cooling_until is not None and cooling_until > as_of:
        status = CandidateStatus.COOLING
    else:
        status = CandidateStatus.ACTIVE

    expiration_values = tuple(
        item.expires_at for item in provenance if item.expires_at is not None
    )
    expires_at = (
        None
        if any(item.expires_at is None for item in provenance)
        else max(expiration_values)
    )
    return CandidateRecord(
        symbol=symbol,
        as_of=as_of,
        status=status,
        priority=priority,
        discovered_at=min(item.discovered_at for item in provenance),
        first_observed_at=min(item.observed_at for item in provenance),
        last_observed_at=max(item.observed_at for item in provenance),
        expires_at=expires_at,
        cooling_until=cooling_until,
        reason_codes=tuple(
            sorted({code for item in provenance for code in item.reason_codes})
        ),
        evidence_ids=tuple(
            sorted({identifier for item in provenance for identifier in item.evidence_ids})
        ),
        sources=tuple(sorted({item.source for item in provenance}, key=lambda item: item.value)),
        provenance=provenance,
    )


def _observation_document(item: CandidateObservation) -> dict[str, object]:
    return {
        "schema_version": 1,
        "observation_id": item.observation_id,
        "symbol": item.symbol,
        "source": item.source.value,
        "source_run_id": item.source_run_id,
        "discovered_at": item.discovered_at.isoformat(),
        "observed_at": item.observed_at.isoformat(),
        "expires_at": None if item.expires_at is None else item.expires_at.isoformat(),
        "priority": int(item.priority),
        "reason_codes": list(item.reason_codes),
        "evidence_ids": list(item.evidence_ids),
    }


def _control_document(item: CandidateControlEvent) -> dict[str, object]:
    return {
        "schema_version": 1,
        "event_id": item.event_id,
        "symbol": item.symbol,
        "action": item.action.value,
        "operation_id": item.operation_id,
        "occurred_at": item.occurred_at.isoformat(),
        "reason_code": item.reason_code,
        "cooling_until": (
            None if item.cooling_until is None else item.cooling_until.isoformat()
        ),
    }


def _observation_from_document(document: dict[str, object]) -> CandidateObservation:
    return CandidateObservation(
        symbol=str(document["symbol"]),
        source=CandidateSource(str(document["source"])),
        source_run_id=str(document["source_run_id"]),
        discovered_at=datetime.fromisoformat(str(document["discovered_at"])),
        observed_at=datetime.fromisoformat(str(document["observed_at"])),
        expires_at=(
            None
            if document.get("expires_at") is None
            else datetime.fromisoformat(str(document["expires_at"]))
        ),
        priority=CandidatePriority(int(str(document["priority"]))),
        reason_codes=tuple(str(item) for item in _list(document, "reason_codes")),
        evidence_ids=tuple(str(item) for item in _list(document, "evidence_ids")),
    )


def _control_from_document(document: dict[str, object]) -> CandidateControlEvent:
    return CandidateControlEvent(
        symbol=str(document["symbol"]),
        action=CandidateControlAction(str(document["action"])),
        operation_id=str(document["operation_id"]),
        occurred_at=datetime.fromisoformat(str(document["occurred_at"])),
        reason_code=str(document["reason_code"]),
        cooling_until=(
            None
            if document.get("cooling_until") is None
            else datetime.fromisoformat(str(document["cooling_until"]))
        ),
    )


def _verified_document(row: sqlite3.Row) -> dict[str, object]:
    payload = str(row["payload_json"]).encode("utf-8")
    if hashlib.sha256(payload).hexdigest() != str(row["payload_sha256"]):
        raise ValueError("stored candidate event payload failed integrity check")
    document = json.loads(payload)
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError("stored candidate event schema is unsupported")
    return document


def _audit_record(row: sqlite3.Row) -> CandidateAuditRecord:
    _verified_document(row)
    return CandidateAuditRecord(
        sequence=int(row["sequence"]),
        event_id=str(row["event_id"]),
        symbol=str(row["symbol"]),
        event_type=str(row["event_type"]),
        effective_at=datetime.fromisoformat(str(row["effective_at"])),
        recorded_at=datetime.fromisoformat(str(row["recorded_at"])),
        payload_sha256=str(row["payload_sha256"]),
    )


def _validate_event_timestamps(
    row: sqlite3.Row,
    *,
    effective_at: datetime,
    recorded_at: datetime,
) -> None:
    if datetime.fromisoformat(str(row["effective_at"])) != effective_at:
        raise ValueError("stored candidate effective_at is inconsistent")
    if datetime.fromisoformat(str(row["recorded_at"])) != recorded_at:
        raise ValueError("stored candidate recorded_at is inconsistent")


def _json_bytes(document: dict[str, object]) -> bytes:
    return json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _list(document: dict[str, object], key: str) -> list[object]:
    value = document.get(key)
    if not isinstance(value, list):
        raise ValueError(f"stored candidate {key} is corrupt")
    return value


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)
