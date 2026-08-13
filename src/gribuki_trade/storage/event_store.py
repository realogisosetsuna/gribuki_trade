"""SQLite point-in-time event revisions and durable news cursors."""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from os import PathLike

from gribuki_trade.domain.events import NormalizedEvent, SourceTier
from gribuki_trade.pipeline.dedupe import DedupeDecision, EventDisposition
from gribuki_trade.ports.news import FetchCursor


class SQLiteEventStore:
    """Append-only normalized events plus per-source collection cursors."""

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
        self._connection.execute("PRAGMA synchronous = NORMAL")
        self._initialize()

    def _initialize(self) -> None:
        with self._transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS normalized_event_revisions (
                    revision_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    canonical_url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    source_tier TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    retrieved_at TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    published_at TEXT,
                    external_id TEXT,
                    raw_document_id TEXT,
                    entities_json TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    revision_number INTEGER NOT NULL,
                    supersedes_revision_id TEXT,
                    UNIQUE(event_id, content_sha256),
                    UNIQUE(event_id, revision_number)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_event_latest
                ON normalized_event_revisions(event_id, revision_number DESC)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_event_available
                ON normalized_event_revisions(available_at DESC)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS news_source_cursors (
                    source_id TEXT PRIMARY KEY,
                    cursor_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def append(self, event: NormalizedEvent) -> DedupeDecision:
        """Append one content revision; exact duplicates are idempotent."""

        with self._transaction() as connection:
            duplicate = connection.execute(
                """
                SELECT * FROM normalized_event_revisions
                WHERE event_id = ? AND content_sha256 = ?
                """,
                (event.event_id, event.content_sha256),
            ).fetchone()
            if duplicate is not None:
                existing = _row_to_event(duplicate)
                return DedupeDecision(EventDisposition.DUPLICATE, existing, False)

            previous = connection.execute(
                """
                SELECT * FROM normalized_event_revisions
                WHERE event_id = ? ORDER BY revision_number DESC LIMIT 1
                """,
                (event.event_id,),
            ).fetchone()
            if previous is None:
                disposition = EventDisposition.NEW
                stored = replace(
                    event,
                    revision_number=1,
                    supersedes_revision_id=None,
                )
            else:
                disposition = EventDisposition.REVISION
                previous_event = _row_to_event(previous)
                stored = replace(
                    event,
                    event_id=previous_event.event_id,
                    revision_number=previous_event.revision_number + 1,
                    supersedes_revision_id=previous_event.revision_id,
                )
            connection.execute(
                """
                INSERT INTO normalized_event_revisions (
                    revision_id, event_id, source_id, canonical_url, title,
                    summary, event_type, source_tier, first_seen_at,
                    retrieved_at, available_at, published_at, external_id,
                    raw_document_id, entities_json, content_sha256,
                    revision_number, supersedes_revision_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _event_values(stored),
            )
        return DedupeDecision(disposition, stored, True)

    def latest(self, *, limit: int = 200) -> tuple[NormalizedEvent, ...]:
        if limit < 1:
            raise ValueError("limit must be positive")
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT current.* FROM normalized_event_revisions AS current
                WHERE current.revision_number = (
                    SELECT MAX(candidate.revision_number)
                    FROM normalized_event_revisions AS candidate
                    WHERE candidate.event_id = current.event_id
                )
                ORDER BY current.available_at DESC, current.event_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(_row_to_event(row) for row in rows)

    def latest_as_of(
        self,
        as_of: datetime,
        *,
        limit: int = 200,
    ) -> tuple[NormalizedEvent, ...]:
        """Return each event's latest revision visible at ``as_of``.

        Unlike :meth:`latest`, this query does not let a correction observed
        after the decision time hide the earlier revision that was actually
        available then.  Stored timestamps are UTC, so normalize the caller's
        aware timestamp before comparing their ISO-8601 representations.
        """

        _aware(as_of, "as_of")
        if limit < 1:
            raise ValueError("limit must be positive")
        cutoff = as_of.astimezone(UTC).isoformat()
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT visible.* FROM normalized_event_revisions AS visible
                WHERE visible.first_seen_at <= ?
                  AND visible.available_at <= ?
                  AND visible.revision_number = (
                      SELECT MAX(candidate.revision_number)
                      FROM normalized_event_revisions AS candidate
                      WHERE candidate.event_id = visible.event_id
                        AND candidate.first_seen_at <= ?
                        AND candidate.available_at <= ?
                  )
                ORDER BY visible.available_at DESC, visible.event_id
                LIMIT ?
                """,
                (cutoff, cutoff, cutoff, cutoff, limit),
            ).fetchall()
        return tuple(_row_to_event(row) for row in rows)

    def get_cursor(self, source_id: str) -> FetchCursor | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT cursor_json FROM news_source_cursors WHERE source_id = ?",
                (source_id,),
            ).fetchone()
        if row is None:
            return None
        document = json.loads(str(row["cursor_json"]))
        if not isinstance(document, dict):
            raise ValueError("stored source cursor is corrupt")
        return _cursor_from_json(document)

    def put_cursor(
        self,
        source_id: str,
        cursor: FetchCursor,
        *,
        updated_at: datetime,
    ) -> None:
        if not source_id.strip():
            raise ValueError("source_id must not be empty")
        _aware(updated_at, "updated_at")
        payload = json.dumps(
            _cursor_to_json(cursor),
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO news_source_cursors(source_id, cursor_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    cursor_json = excluded.cursor_json,
                    updated_at = excluded.updated_at
                """,
                (source_id, payload, updated_at.isoformat()),
            )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()

    def __enter__(self) -> SQLiteEventStore:
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
            raise RuntimeError("event store is closed")


def _event_values(event: NormalizedEvent) -> tuple[object, ...]:
    return (
        event.revision_id,
        event.event_id,
        event.source_id,
        event.canonical_url,
        event.title,
        event.summary,
        event.event_type,
        event.source_tier.value,
        event.first_seen_at.isoformat(),
        event.retrieved_at.isoformat(),
        event.available_at.isoformat(),
        event.published_at.isoformat() if event.published_at is not None else None,
        event.external_id,
        event.raw_document_id,
        json.dumps(event.entities, ensure_ascii=False, separators=(",", ":")),
        event.content_sha256,
        event.revision_number,
        event.supersedes_revision_id,
    )


def _row_to_event(row: sqlite3.Row) -> NormalizedEvent:
    entities = json.loads(str(row["entities_json"]))
    if not isinstance(entities, list):
        raise ValueError("stored event entities are corrupt")
    return NormalizedEvent(
        source_id=str(row["source_id"]),
        canonical_url=str(row["canonical_url"]),
        title=str(row["title"]),
        summary=str(row["summary"]),
        event_type=str(row["event_type"]),
        source_tier=SourceTier(str(row["source_tier"])),
        first_seen_at=datetime.fromisoformat(str(row["first_seen_at"])),
        retrieved_at=datetime.fromisoformat(str(row["retrieved_at"])),
        available_at=datetime.fromisoformat(str(row["available_at"])),
        published_at=(
            datetime.fromisoformat(str(row["published_at"]))
            if row["published_at"] is not None
            else None
        ),
        external_id=row["external_id"],
        raw_document_id=row["raw_document_id"],
        entities=tuple(str(item) for item in entities),
        content_sha256=str(row["content_sha256"]),
        event_id=str(row["event_id"]),
        revision_id=str(row["revision_id"]),
        revision_number=int(row["revision_number"]),
        supersedes_revision_id=row["supersedes_revision_id"],
    )


def _cursor_to_json(cursor: FetchCursor) -> dict[str, object]:
    return {
        "etag": cursor.etag,
        "last_modified": cursor.last_modified,
        "content_sha256": cursor.content_sha256,
        "first_seen_at": _time_or_none(cursor.first_seen_at),
        "consecutive_failures": cursor.consecutive_failures,
        "next_allowed_at": _time_or_none(cursor.next_allowed_at),
    }


def _cursor_from_json(document: dict[str, object]) -> FetchCursor:
    return FetchCursor(
        etag=_str_or_none(document.get("etag")),
        last_modified=_str_or_none(document.get("last_modified")),
        content_sha256=_str_or_none(document.get("content_sha256")),
        first_seen_at=_parse_time_or_none(document.get("first_seen_at")),
        consecutive_failures=_stored_int(document.get("consecutive_failures", 0)),
        next_allowed_at=_parse_time_or_none(document.get("next_allowed_at")),
    )


def _time_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    _aware(value, "cursor datetime")
    return value.isoformat()


def _parse_time_or_none(value: object) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(str(value))


def _str_or_none(value: object) -> str | None:
    return None if value is None else str(value)


def _stored_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("stored source cursor is corrupt")
    return int(value)


def _aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
