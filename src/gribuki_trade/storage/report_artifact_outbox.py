"""经报告契约验证的文件交付持久边界。

文件上传无法依赖 QQ 提供端到端幂等键。这里采用保守的一次性状态机：进程在调用
NapCat 前持久认领；若进程在收到提供方回执后、写入本地回执前终止，条目保持
``IN_FLIGHT``，后续调用拒绝自动重发并要求人工核对，从而避免静默重复报告。
"""

from __future__ import annotations

import re
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from os import PathLike
from pathlib import Path

from gribuki_trade.ports.notifier import NotificationTargetKind

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class ReportArtifactStatus(StrEnum):
    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    SENT = "SENT"
    AMBIGUOUS = "AMBIGUOUS"


class ReportArtifactOutboxError(RuntimeError):
    """不泄漏路径、目标或提供方正文的稳定文件交付错误。"""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"report artifact outbox failed ({code})")


@dataclass(frozen=True, slots=True)
class ReportArtifactRecord:
    idempotency_key: str
    report_kind: str
    target_kind: NotificationTargetKind
    target_id: str = field(repr=False)
    artifact_name: str
    artifact_sha256: str
    status: ReportArtifactStatus
    created_at: datetime
    claimed_at: datetime | None = None
    provider_identifier: str | None = None
    sent_at: datetime | None = None


class SQLiteReportArtifactOutbox:
    """跨进程事务认领报告文件，并永久保留脱敏提供方回执。"""

    def __init__(self, path: str | PathLike[str]) -> None:
        raw_path = str(path)
        if raw_path != ":memory:":
            resolved = Path(path)
            resolved.parent.mkdir(parents=True, exist_ok=True)
            if resolved.is_symlink():
                raise ReportArtifactOutboxError("REPORT_ARTIFACT_OUTBOX_PATH_INVALID")
            if resolved.exists() and not resolved.is_file():
                raise ReportArtifactOutboxError("REPORT_ARTIFACT_OUTBOX_PATH_INVALID")
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            path,
            timeout=15.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout = 15000")
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._initialize()

    def _initialize(self) -> None:
        with self._transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS report_artifact_outbox (
                    idempotency_key TEXT PRIMARY KEY,
                    report_kind TEXT NOT NULL,
                    target_kind TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    artifact_name TEXT NOT NULL,
                    artifact_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    claimed_at TEXT,
                    provider_identifier TEXT,
                    sent_at TEXT,
                    CHECK(target_kind IN ('private','group')),
                    CHECK(status IN ('PENDING','IN_FLIGHT','SENT','AMBIGUOUS'))
                ) WITHOUT ROWID
                """
            )

    def enqueue(
        self,
        *,
        idempotency_key: str,
        report_kind: str,
        target_kind: NotificationTargetKind,
        target_id: str,
        artifact_name: str,
        artifact_sha256: str,
        created_at: datetime,
    ) -> ReportArtifactRecord:
        key = _text(idempotency_key, "idempotency_key", maximum=200)
        kind = _text(report_kind, "report_kind", maximum=80)
        target = _text(target_id, "target_id", maximum=128)
        name = _artifact_name(artifact_name)
        digest = _digest(artifact_sha256)
        created = _time(created_at)
        values = (
            key,
            kind,
            target_kind.value,
            target,
            name,
            digest,
            ReportArtifactStatus.PENDING.value,
            created,
        )
        with self._transaction() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO report_artifact_outbox(
                        idempotency_key, report_kind, target_kind, target_id,
                        artifact_name, artifact_sha256, status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )
            except sqlite3.IntegrityError:
                row = connection.execute(
                    "SELECT * FROM report_artifact_outbox WHERE idempotency_key = ?",
                    (key,),
                ).fetchone()
                if row is None:
                    raise
                existing = (
                    row["report_kind"],
                    row["target_kind"],
                    row["target_id"],
                    row["artifact_name"],
                    row["artifact_sha256"],
                )
                wanted = (kind, target_kind.value, target, name, digest)
                if existing != wanted:
                    raise ReportArtifactOutboxError(
                        "REPORT_ARTIFACT_IDEMPOTENCY_COLLISION"
                    ) from None
            row = connection.execute(
                "SELECT * FROM report_artifact_outbox WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
        return _record(_required(row))

    def claim(self, idempotency_key: str, *, claimed_at: datetime) -> ReportArtifactRecord:
        key = _text(idempotency_key, "idempotency_key", maximum=200)
        claimed = _time(claimed_at)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM report_artifact_outbox WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
            record = _record(_required(row))
            if record.status is ReportArtifactStatus.SENT:
                return record
            if record.status is not ReportArtifactStatus.PENDING:
                raise ReportArtifactOutboxError("REPORT_ARTIFACT_DELIVERY_AMBIGUOUS")
            connection.execute(
                """
                UPDATE report_artifact_outbox
                SET status = ?, claimed_at = ?
                WHERE idempotency_key = ? AND status = ?
                """,
                (
                    ReportArtifactStatus.IN_FLIGHT.value,
                    claimed,
                    key,
                    ReportArtifactStatus.PENDING.value,
                ),
            )
            row = connection.execute(
                "SELECT * FROM report_artifact_outbox WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
        return _record(_required(row))

    def mark_sent(
        self,
        idempotency_key: str,
        *,
        provider_identifier: str,
        sent_at: datetime,
    ) -> ReportArtifactRecord:
        key = _text(idempotency_key, "idempotency_key", maximum=200)
        provider_id = _text(provider_identifier, "provider_identifier", maximum=256)
        sent = _time(sent_at)
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE report_artifact_outbox
                SET status = ?, provider_identifier = ?, sent_at = ?
                WHERE idempotency_key = ? AND status = ?
                """,
                (
                    ReportArtifactStatus.SENT.value,
                    provider_id,
                    sent,
                    key,
                    ReportArtifactStatus.IN_FLIGHT.value,
                ),
            )
            if cursor.rowcount != 1:
                raise ReportArtifactOutboxError("REPORT_ARTIFACT_STATE_CONFLICT")
            row = connection.execute(
                "SELECT * FROM report_artifact_outbox WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
        return _record(_required(row))

    def mark_ambiguous(self, idempotency_key: str) -> ReportArtifactRecord:
        key = _text(idempotency_key, "idempotency_key", maximum=200)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM report_artifact_outbox WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
            record = _record(_required(row))
            if record.status is ReportArtifactStatus.AMBIGUOUS:
                return record
            if record.status is not ReportArtifactStatus.IN_FLIGHT:
                raise ReportArtifactOutboxError("REPORT_ARTIFACT_STATE_CONFLICT")
            cursor = connection.execute(
                """
                UPDATE report_artifact_outbox
                SET status = ?
                WHERE idempotency_key = ? AND status = ?
                """,
                (
                    ReportArtifactStatus.AMBIGUOUS.value,
                    key,
                    ReportArtifactStatus.IN_FLIGHT.value,
                ),
            )
            if cursor.rowcount != 1:
                raise ReportArtifactOutboxError("REPORT_ARTIFACT_STATE_CONFLICT")
            row = connection.execute(
                "SELECT * FROM report_artifact_outbox WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
        return _record(_required(row))

    def resolve_ambiguous_as_sent(
        self,
        idempotency_key: str,
        *,
        provider_identifier: str,
        resolved_at: datetime,
    ) -> ReportArtifactRecord:
        """在操作员已经向提供方核验收件后，将歧义记录确认为已发送。"""

        key = _text(idempotency_key, "idempotency_key", maximum=200)
        provider_id = _text(provider_identifier, "provider_identifier", maximum=256)
        resolved = _time(resolved_at)
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE report_artifact_outbox
                SET status = ?, provider_identifier = ?, sent_at = ?
                WHERE idempotency_key = ? AND status = ?
                """,
                (
                    ReportArtifactStatus.SENT.value,
                    provider_id,
                    resolved,
                    key,
                    ReportArtifactStatus.AMBIGUOUS.value,
                ),
            )
            if cursor.rowcount != 1:
                raise ReportArtifactOutboxError("REPORT_ARTIFACT_STATE_CONFLICT")
            row = connection.execute(
                "SELECT * FROM report_artifact_outbox WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
        return _record(_required(row))

    def requeue_ambiguous(self, idempotency_key: str) -> ReportArtifactRecord:
        """在操作员已确认提供方未收件后，把歧义记录显式重置为待发送。"""

        key = _text(idempotency_key, "idempotency_key", maximum=200)
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE report_artifact_outbox
                SET status = ?, claimed_at = NULL,
                    provider_identifier = NULL, sent_at = NULL
                WHERE idempotency_key = ? AND status = ?
                """,
                (
                    ReportArtifactStatus.PENDING.value,
                    key,
                    ReportArtifactStatus.AMBIGUOUS.value,
                ),
            )
            if cursor.rowcount != 1:
                raise ReportArtifactOutboxError("REPORT_ARTIFACT_STATE_CONFLICT")
            row = connection.execute(
                "SELECT * FROM report_artifact_outbox WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
        return _record(_required(row))

    def get(self, idempotency_key: str) -> ReportArtifactRecord | None:
        key = _text(idempotency_key, "idempotency_key", maximum=200)
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT * FROM report_artifact_outbox WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
        return None if row is None else _record(row)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._connection.close()
            self._closed = True

    def __enter__(self) -> SQLiteReportArtifactOutbox:
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
            raise RuntimeError("report artifact outbox is closed")


def _record(row: sqlite3.Row) -> ReportArtifactRecord:
    return ReportArtifactRecord(
        idempotency_key=str(row["idempotency_key"]),
        report_kind=str(row["report_kind"]),
        target_kind=NotificationTargetKind(str(row["target_kind"])),
        target_id=str(row["target_id"]),
        artifact_name=str(row["artifact_name"]),
        artifact_sha256=str(row["artifact_sha256"]),
        status=ReportArtifactStatus(str(row["status"])),
        created_at=_parse_time(str(row["created_at"])),
        claimed_at=(None if row["claimed_at"] is None else _parse_time(str(row["claimed_at"]))),
        provider_identifier=(
            None if row["provider_identifier"] is None else str(row["provider_identifier"])
        ),
        sent_at=None if row["sent_at"] is None else _parse_time(str(row["sent_at"])),
    )


def _required(row: sqlite3.Row | None) -> sqlite3.Row:
    if row is None:
        raise ReportArtifactOutboxError("REPORT_ARTIFACT_NOT_FOUND")
    return row


def _text(value: str, name: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{name} must be non-empty normalized text")
    if len(value) > maximum or any(character in value for character in "\r\n\x00"):
        raise ValueError(f"{name} is invalid")
    return value


def _artifact_name(value: str) -> str:
    checked = _text(value, "artifact_name", maximum=255)
    path = Path(checked)
    if path.is_absolute() or path.name != checked or checked in {".", ".."}:
        raise ValueError("artifact_name must be a single relative file name")
    return checked


def _digest(value: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError("artifact_sha256 must be a lowercase SHA-256 digest")
    return value


def _time(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("artifact delivery timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReportArtifactOutboxError("REPORT_ARTIFACT_TIME_INVALID")
    return parsed.astimezone(UTC)
