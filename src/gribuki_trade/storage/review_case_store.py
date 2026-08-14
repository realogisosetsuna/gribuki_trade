"""用于建议审核案件的仅追加 SQLite 存储。"""

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
    CandidatePriority,
    CandidateSource,
    CandidateStatus,
)
from gribuki_trade.domain.review_cases import (
    TERMINAL_REVIEW_STATUSES,
    CandidateProvenanceSummary,
    RecommendationReviewCase,
    ReviewActor,
    ReviewCaseAuditRecord,
    ReviewCaseOpened,
    ReviewCaseStatus,
    ReviewCaseTransition,
    ReviewResolution,
)

_OPENED = "OPENED"


class ReviewCaseEventCollisionError(ValueError):
    """同一不可变审核事件标识被复用于不同内容。"""


class ReviewCaseNotFoundError(LookupError):
    """一次状态迁移指向了未知审核案件。"""


class InvalidReviewCaseTransitionError(RuntimeError):
    """只有可见、未过期的待处理案件才能进入终态。"""


class SQLiteReviewCaseStore:
    """具有确定性时点投影的 SQLite WAL 事件存储。"""

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
                CREATE TABLE IF NOT EXISTS review_case_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    case_id TEXT NOT NULL,
                    recommendation_id TEXT NOT NULL,
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
                CREATE UNIQUE INDEX IF NOT EXISTS ux_review_case_open
                ON review_case_events(case_id) WHERE event_type = 'OPENED'
                """
            )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_review_recommendation_open
                ON review_case_events(recommendation_id) WHERE event_type = 'OPENED'
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_review_case_time
                ON review_case_events(case_id, recorded_at, effective_at, sequence)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_review_case_visible
                ON review_case_events(recorded_at, effective_at, symbol)
                """
            )

    def append_opened(self, item: ReviewCaseOpened) -> bool:
        """仅创建一次案件；精确重放具有幂等性。"""

        payload = _json_bytes(_opened_document(item))
        digest = hashlib.sha256(payload).hexdigest()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT payload_sha256 FROM review_case_events WHERE event_id = ?",
                (item.event_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["payload_sha256"]) != digest:
                    raise ReviewCaseEventCollisionError(
                        "review case event ID was reused with different content"
                    )
                return False
            try:
                connection.execute(
                    """
                    INSERT INTO review_case_events(
                        event_id, case_id, recommendation_id, symbol, event_type,
                        effective_at, recorded_at, payload_json, payload_sha256
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item.event_id,
                        item.case_id,
                        item.recommendation_id,
                        item.symbol,
                        _OPENED,
                        item.opened_at.isoformat(),
                        item.recorded_at.isoformat(),
                        payload.decode("utf-8"),
                        digest,
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise ReviewCaseEventCollisionError(
                    "recommendation already has a different review case"
                ) from error
        return True

    def append_transition(self, item: ReviewCaseTransition) -> bool:
        """将待处理案件迁移至一个研究审核终态。"""

        payload = _json_bytes(_transition_document(item))
        digest = hashlib.sha256(payload).hexdigest()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT payload_sha256 FROM review_case_events WHERE event_id = ?",
                (item.event_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["payload_sha256"]) != digest:
                    raise ReviewCaseEventCollisionError(
                        "review case event ID was reused with different content"
                    )
                return False

            opened_row = connection.execute(
                """
                SELECT * FROM review_case_events
                WHERE case_id = ? AND event_type = ?
                """,
                (item.case_id, _OPENED),
            ).fetchone()
            if opened_row is None:
                raise ReviewCaseNotFoundError("review case does not exist")
            opened = _opened_from_document(_verified_document(opened_row))
            if opened.case_id != item.case_id:
                raise ValueError("stored review case identity is inconsistent")
            if opened.opened_at > item.occurred_at:
                raise InvalidReviewCaseTransitionError(
                    "transition occurred before the review case opened"
                )
            if opened.recorded_at > item.recorded_at:
                raise InvalidReviewCaseTransitionError(
                    "transition was recorded before the review case was visible"
                )
            if item.recorded_at >= opened.expires_at:
                raise InvalidReviewCaseTransitionError(
                    "expired review case cannot transition"
                )
            terminal = connection.execute(
                """
                SELECT 1 FROM review_case_events
                WHERE case_id = ? AND event_type IN (?, ?, ?)
                LIMIT 1
                """,
                (
                    item.case_id,
                    ReviewCaseStatus.CONFIRMED.value,
                    ReviewCaseStatus.REJECTED.value,
                    ReviewCaseStatus.CANCELLED.value,
                ),
            ).fetchone()
            if terminal is not None:
                raise InvalidReviewCaseTransitionError(
                    "only a pending review case may transition"
                )
            connection.execute(
                """
                INSERT INTO review_case_events(
                    event_id, case_id, recommendation_id, symbol, event_type,
                    effective_at, recorded_at, payload_json, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.event_id,
                    item.case_id,
                    opened.recommendation_id,
                    opened.symbol,
                    item.target_status.value,
                    item.occurred_at.isoformat(),
                    item.recorded_at.isoformat(),
                    payload.decode("utf-8"),
                    digest,
                ),
            )
        return True

    def get_case(
        self,
        case_id: str,
        *,
        as_of: datetime,
    ) -> RecommendationReviewCase | None:
        cutoff_at = _utc(as_of, "as_of")
        cutoff = cutoff_at.isoformat()
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT * FROM review_case_events
                WHERE case_id = ? AND recorded_at <= ? AND effective_at <= ?
                ORDER BY effective_at, sequence
                """,
                (case_id.strip().lower(), cutoff, cutoff),
            ).fetchall()
        return _project(cutoff_at, rows)

    def get_by_recommendation(
        self,
        recommendation_id: str,
        *,
        as_of: datetime,
    ) -> RecommendationReviewCase | None:
        cutoff_at = _utc(as_of, "as_of")
        cutoff = cutoff_at.isoformat()
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT * FROM review_case_events
                WHERE recommendation_id = ?
                  AND recorded_at <= ? AND effective_at <= ?
                ORDER BY effective_at, sequence
                """,
                (recommendation_id.strip(), cutoff, cutoff),
            ).fetchall()
        return _project(cutoff_at, rows)

    def list_cases(
        self,
        *,
        as_of: datetime,
        statuses: frozenset[ReviewCaseStatus] | None = None,
        limit: int = 500,
    ) -> tuple[RecommendationReviewCase, ...]:
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
                SELECT * FROM review_case_events
                WHERE recorded_at <= ? AND effective_at <= ?
                ORDER BY case_id, effective_at, sequence
                """,
                (cutoff, cutoff),
            ).fetchall()
        grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in rows:
            grouped[str(row["case_id"])].append(row)
        projected = tuple(
            item
            for case_rows in grouped.values()
            if (item := _project(cutoff_at, case_rows)) is not None
            and (statuses is None or item.status in statuses)
        )
        ordered = sorted(
            projected,
            key=lambda item: (-item.opened_at.timestamp(), item.case_id),
        )
        return tuple(ordered[:limit])

    def history(
        self,
        case_id: str,
        *,
        as_of: datetime | None = None,
        limit: int = 1_000,
    ) -> tuple[ReviewCaseAuditRecord, ...]:
        if limit < 1:
            raise ValueError("limit must be positive")
        query = "SELECT * FROM review_case_events WHERE case_id = ?"
        normalized = case_id.strip().lower()
        parameters: tuple[object, ...]
        if as_of is None:
            parameters = (normalized, limit)
        else:
            cutoff = _utc(as_of, "as_of").isoformat()
            query += " AND recorded_at <= ? AND effective_at <= ?"
            parameters = (normalized, cutoff, cutoff, limit)
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

    def __enter__(self) -> SQLiteReviewCaseStore:
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
            raise RuntimeError("review case store is closed")


def _project(
    as_of: datetime,
    rows: list[sqlite3.Row] | tuple[sqlite3.Row, ...],
) -> RecommendationReviewCase | None:
    if not rows:
        return None
    opened: ReviewCaseOpened | None = None
    transition: ReviewCaseTransition | None = None
    for row in rows:
        document = _verified_document(row)
        event_type = str(row["event_type"])
        if event_type == _OPENED:
            if opened is not None:
                raise ValueError("stored review case contains duplicate open events")
            opened = _opened_from_document(document)
            if opened.event_id != str(row["event_id"]):
                raise ValueError("stored review open-event identity is inconsistent")
            _validate_row(opened.case_id, opened.opened_at, opened.recorded_at, row)
        elif event_type in {status.value for status in TERMINAL_REVIEW_STATUSES}:
            if transition is not None:
                raise ValueError("stored review case contains multiple terminal events")
            transition = _transition_from_document(document)
            if transition.target_status.value != event_type:
                raise ValueError("stored review transition type is inconsistent")
            if transition.event_id != str(row["event_id"]):
                raise ValueError("stored review transition identity is inconsistent")
            _validate_row(
                transition.case_id,
                transition.occurred_at,
                transition.recorded_at,
                row,
            )
        else:
            raise ValueError("stored review event type is unsupported")
    if opened is None:
        return None
    if transition is not None:
        resolution = ReviewResolution(
            status=transition.target_status,
            actor=transition.actor,
            reason_codes=transition.reason_codes,
            occurred_at=transition.occurred_at,
            recorded_at=transition.recorded_at,
            operation_id=transition.operation_id,
            event_id=transition.event_id,
        )
        status = transition.target_status
    else:
        resolution = None
        status = (
            ReviewCaseStatus.EXPIRED
            if as_of >= opened.expires_at
            else ReviewCaseStatus.PENDING_REVIEW
        )
    return RecommendationReviewCase(
        case_id=opened.case_id,
        recommendation_id=opened.recommendation_id,
        symbol=opened.symbol,
        recommendation_as_of=opened.recommendation_as_of,
        opened_at=opened.opened_at,
        recorded_at=opened.recorded_at,
        expires_at=opened.expires_at,
        as_of=as_of,
        status=status,
        evidence_ids=opened.evidence_ids,
        candidate_provenance=opened.candidate_provenance,
        opened_by=opened.actor,
        open_reason_codes=opened.reason_codes,
        resolution=resolution,
    )


def _opened_document(item: ReviewCaseOpened) -> dict[str, object]:
    return {
        "schema_version": 1,
        "case_id": item.case_id,
        "recommendation_id": item.recommendation_id,
        "symbol": item.symbol,
        "recommendation_as_of": item.recommendation_as_of.isoformat(),
        "opened_at": item.opened_at.isoformat(),
        "recorded_at": item.recorded_at.isoformat(),
        "expires_at": item.expires_at.isoformat(),
        "evidence_ids": list(item.evidence_ids),
        "candidate_provenance": _provenance_document(item.candidate_provenance),
        "actor": item.actor.value,
        "reason_codes": list(item.reason_codes),
    }


def _transition_document(item: ReviewCaseTransition) -> dict[str, object]:
    return {
        "schema_version": 1,
        "event_id": item.event_id,
        "case_id": item.case_id,
        "target_status": item.target_status.value,
        "operation_id": item.operation_id,
        "actor": item.actor.value,
        "reason_codes": list(item.reason_codes),
        "occurred_at": item.occurred_at.isoformat(),
        "recorded_at": item.recorded_at.isoformat(),
    }


def _provenance_document(
    item: CandidateProvenanceSummary | None,
) -> dict[str, object] | None:
    if item is None:
        return None
    return {
        "symbol": item.symbol,
        "candidate_as_of": item.candidate_as_of.isoformat(),
        "candidate_status": item.candidate_status.value,
        "priority": int(item.priority),
        "observation_ids": list(item.observation_ids),
        "sources": [source.value for source in item.sources],
        "source_run_ids": list(item.source_run_ids),
        "first_observed_at": item.first_observed_at.isoformat(),
        "last_observed_at": item.last_observed_at.isoformat(),
        "reason_codes": list(item.reason_codes),
        "evidence_ids": list(item.evidence_ids),
    }


def _opened_from_document(document: dict[str, object]) -> ReviewCaseOpened:
    return ReviewCaseOpened(
        recommendation_id=str(document["recommendation_id"]),
        symbol=str(document["symbol"]),
        recommendation_as_of=datetime.fromisoformat(str(document["recommendation_as_of"])),
        opened_at=datetime.fromisoformat(str(document["opened_at"])),
        recorded_at=datetime.fromisoformat(str(document["recorded_at"])),
        expires_at=datetime.fromisoformat(str(document["expires_at"])),
        evidence_ids=tuple(str(item) for item in _list(document, "evidence_ids")),
        candidate_provenance=_provenance_from_document(
            document.get("candidate_provenance")
        ),
        actor=ReviewActor(str(document["actor"])),
        reason_codes=tuple(str(item) for item in _list(document, "reason_codes")),
    )


def _transition_from_document(document: dict[str, object]) -> ReviewCaseTransition:
    return ReviewCaseTransition(
        case_id=str(document["case_id"]),
        target_status=ReviewCaseStatus(str(document["target_status"])),
        operation_id=str(document["operation_id"]),
        actor=ReviewActor(str(document["actor"])),
        reason_codes=tuple(str(item) for item in _list(document, "reason_codes")),
        occurred_at=datetime.fromisoformat(str(document["occurred_at"])),
        recorded_at=datetime.fromisoformat(str(document["recorded_at"])),
    )


def _provenance_from_document(value: object) -> CandidateProvenanceSummary | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("stored candidate provenance is corrupt")
    return CandidateProvenanceSummary(
        symbol=str(value["symbol"]),
        candidate_as_of=datetime.fromisoformat(str(value["candidate_as_of"])),
        candidate_status=CandidateStatus(str(value["candidate_status"])),
        priority=CandidatePriority(int(str(value["priority"]))),
        observation_ids=tuple(str(item) for item in _list(value, "observation_ids")),
        sources=tuple(CandidateSource(str(item)) for item in _list(value, "sources")),
        source_run_ids=tuple(str(item) for item in _list(value, "source_run_ids")),
        first_observed_at=datetime.fromisoformat(str(value["first_observed_at"])),
        last_observed_at=datetime.fromisoformat(str(value["last_observed_at"])),
        reason_codes=tuple(str(item) for item in _list(value, "reason_codes")),
        evidence_ids=tuple(str(item) for item in _list(value, "evidence_ids")),
    )


def _verified_document(row: sqlite3.Row) -> dict[str, object]:
    payload = str(row["payload_json"]).encode("utf-8")
    if hashlib.sha256(payload).hexdigest() != str(row["payload_sha256"]):
        raise ValueError("stored review event payload failed integrity check")
    document = json.loads(payload)
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError("stored review event schema is unsupported")
    return document


def _validate_row(
    case_id: str,
    effective_at: datetime,
    recorded_at: datetime,
    row: sqlite3.Row,
) -> None:
    if str(row["case_id"]) != case_id:
        raise ValueError("stored review case ID is inconsistent")
    if datetime.fromisoformat(str(row["effective_at"])) != effective_at:
        raise ValueError("stored review effective_at is inconsistent")
    if datetime.fromisoformat(str(row["recorded_at"])) != recorded_at:
        raise ValueError("stored review recorded_at is inconsistent")


def _audit_record(row: sqlite3.Row) -> ReviewCaseAuditRecord:
    _verified_document(row)
    return ReviewCaseAuditRecord(
        sequence=int(row["sequence"]),
        event_id=str(row["event_id"]),
        case_id=str(row["case_id"]),
        event_type=str(row["event_type"]),
        effective_at=datetime.fromisoformat(str(row["effective_at"])),
        recorded_at=datetime.fromisoformat(str(row["recorded_at"])),
        payload_sha256=str(row["payload_sha256"]),
    )


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
        raise ValueError(f"stored review {key} is corrupt")
    return value


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)
