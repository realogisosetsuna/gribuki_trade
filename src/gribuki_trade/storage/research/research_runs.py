"""用于有界筛选与研究运行的仅追加审计存储。"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from os import PathLike
from typing import TypeAlias

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]


class ResearchRunCollisionError(ValueError):
    """逻辑运行标识被用于不同的不可变内容。"""


@dataclass(frozen=True, slots=True)
class StoredResearchRun:
    """脱敏血缘信息，以及精确的规范配置和输出文档。"""

    run_id: str
    run_type: str
    logical_key: str
    strategy_version: str | None
    status: str
    started_at: datetime
    completed_at: datetime
    source_revisions: tuple[tuple[str, str], ...]
    config_json: str | None
    config_sha256: str
    payload_json: str
    payload_sha256: str
    record_version: int

    def payload_document(self) -> dict[str, JSONValue]:
        parsed = json.loads(self.payload_json)
        if not isinstance(parsed, dict):
            raise ValueError("stored run payload must be a JSON object")
        return {str(key): value for key, value in parsed.items()}

    def config_document(self) -> dict[str, JSONValue] | None:
        """返回冻结配置；旧版预览记录则返回 ``None``。"""

        if self.config_json is None:
            return None
        parsed = json.loads(self.config_json)
        if not isinstance(parsed, dict):
            raise ValueError("stored run config must be a JSON object")
        return {str(key): value for key, value in parsed.items()}


class SQLiteResearchRunStore:
    """由 WAL 支持、用于有限次运行研究的不可变登记簿。

    本存储保留输出与血缘信息，但不声称归档提供方的完整原始响应；原始
    重放仍由独立数据存储负责。这一区分可防止输出摘要被误称为时点输入
    数据集。
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
                CREATE TABLE IF NOT EXISTS research_runs (
                    run_id TEXT PRIMARY KEY,
                    run_type TEXT NOT NULL,
                    logical_key TEXT NOT NULL,
                    strategy_version TEXT,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    source_revisions_json TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    config_sha256 TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    record_version INTEGER NOT NULL,
                    UNIQUE(run_type, logical_key),
                    CHECK(record_version >= 1)
                )
                """
            )
            columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(research_runs)")
            }
            if "config_json" not in columns:
                # 一个短暂使用的预览结构只保留了摘要；应继续支持读取这些
                # 记录，但不得凭空补造已经丢失的配置。
                connection.execute(
                    "ALTER TABLE research_runs ADD COLUMN config_json TEXT"
                )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_research_runs_type_time
                ON research_runs(run_type, completed_at DESC, run_id)
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS research_runs_no_update
                BEFORE UPDATE ON research_runs
                BEGIN
                    SELECT RAISE(ABORT, 'research_runs is append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS research_runs_no_delete
                BEFORE DELETE ON research_runs
                BEGIN
                    SELECT RAISE(ABORT, 'research_runs is append-only');
                END
                """
            )

    def append(
        self,
        *,
        run_type: str,
        logical_key: str,
        status: str,
        started_at: datetime,
        completed_at: datetime,
        payload: Mapping[str, object],
        config: Mapping[str, object],
        strategy_version: str | None = None,
        source_revisions: Sequence[tuple[str, str]] = (),
    ) -> bool:
        """追加一次运行；完全一致的幂等重放返回假值。"""

        resolved_type = _nonempty(run_type, "run_type")
        resolved_key = _nonempty(logical_key, "logical_key")
        resolved_status = _nonempty(status, "status")
        resolved_strategy = (
            None
            if strategy_version is None
            else _nonempty(strategy_version, "strategy_version")
        )
        started = _aware_utc(started_at, "started_at")
        completed = _aware_utc(completed_at, "completed_at")
        if completed < started:
            raise ValueError("completed_at must not precede started_at")
        revisions = _source_revisions(source_revisions)
        payload_json = _canonical_json(dict(payload))
        config_json = _canonical_json(dict(config))
        payload_digest = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        config_digest = hashlib.sha256(config_json.encode("utf-8")).hexdigest()
        run_id = research_run_id(resolved_type, resolved_key)
        revisions_json = json.dumps(
            [list(item) for item in revisions],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._transaction() as connection:
            existing = connection.execute(
                """
                SELECT run_type, logical_key, strategy_version, status,
                       started_at, completed_at, source_revisions_json,
                       config_json, config_sha256, payload_sha256, record_version
                FROM research_runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if existing is not None:
                expected = (
                    resolved_type,
                    resolved_key,
                    resolved_strategy,
                    resolved_status,
                    started.isoformat(),
                    completed.isoformat(),
                    revisions_json,
                    config_json,
                    config_digest,
                    payload_digest,
                    1,
                )
                actual = (
                    str(existing["run_type"]),
                    str(existing["logical_key"]),
                    (
                        None
                        if existing["strategy_version"] is None
                        else str(existing["strategy_version"])
                    ),
                    str(existing["status"]),
                    str(existing["started_at"]),
                    str(existing["completed_at"]),
                    str(existing["source_revisions_json"]),
                    (
                        None
                        if existing["config_json"] is None
                        else str(existing["config_json"])
                    ),
                    str(existing["config_sha256"]),
                    str(existing["payload_sha256"]),
                    int(existing["record_version"]),
                )
                if actual != expected:
                    raise ResearchRunCollisionError(
                        "research run identity was reused with different content"
                    )
                return False
            connection.execute(
                """
                INSERT INTO research_runs(
                    run_id, run_type, logical_key, strategy_version, status,
                    started_at, completed_at, source_revisions_json, config_json,
                    config_sha256, payload_json, payload_sha256, record_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    resolved_type,
                    resolved_key,
                    resolved_strategy,
                    resolved_status,
                    started.isoformat(),
                    completed.isoformat(),
                    revisions_json,
                    config_json,
                    config_digest,
                    payload_json,
                    payload_digest,
                    1,
                ),
            )
        return True

    def get(self, run_id: str) -> StoredResearchRun | None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT * FROM research_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return None if row is None else _row_to_run(row)

    def list_runs(
        self,
        *,
        run_type: str | None = None,
        limit: int = 100,
    ) -> tuple[StoredResearchRun, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be a positive integer")
        parameters: list[object] = []
        query = "SELECT * FROM research_runs"
        if run_type is not None:
            query += " WHERE run_type = ?"
            parameters.append(_nonempty(run_type, "run_type"))
        query += " ORDER BY completed_at DESC, run_id LIMIT ?"
        parameters.append(limit)
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(query, parameters).fetchall()
        return tuple(_row_to_run(row) for row in rows)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()

    def __enter__(self) -> SQLiteResearchRunStore:
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
            raise RuntimeError("research run store is closed")


def _canonical_json(value: object) -> str:
    normalized = _normalize_json(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def research_run_id(run_type: str, logical_key: str) -> str:
    """返回写入方与审计调用方共享的稳定标识。"""

    resolved_type = _nonempty(run_type, "run_type")
    resolved_key = _nonempty(logical_key, "logical_key")
    return hashlib.sha256(
        f"research-run@1\0{resolved_type}\0{resolved_key}".encode()
    ).hexdigest()


def research_run_document_sha256(document: Mapping[str, object]) -> str:
    """按登记簿的规范 JSON 规则计算配置或输出的哈希。"""

    return hashlib.sha256(_canonical_json(dict(document)).encode("utf-8")).hexdigest()


def _normalize_json(value: object) -> JSONValue:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("run documents must not contain non-finite floats")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("run documents must not contain non-finite decimals")
        return format(value, "f")
    if isinstance(value, datetime):
        return _aware_utc(value, "document datetime").isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        normalized: dict[str, JSONValue] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError("run document keys must be non-empty strings")
            normalized[key] = _normalize_json(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_normalize_json(item) for item in value]
    raise ValueError(f"run document contains unsupported value type {type(value).__name__}")


def _source_revisions(
    values: Sequence[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    normalized = tuple(
        sorted(
            (
                _nonempty(source, "source revision source"),
                _nonempty(revision, "source revision value"),
            )
            for source, revision in values
        )
    )
    if len(normalized) != len(set(normalized)):
        raise ValueError("source revisions must be unique")
    sources = tuple(source for source, _ in normalized)
    if len(sources) != len(set(sources)):
        raise ValueError("each source may have only one revision per run")
    return normalized


def _row_to_run(row: sqlite3.Row) -> StoredResearchRun:
    parsed = json.loads(str(row["source_revisions_json"]))
    if not isinstance(parsed, list):
        raise ValueError("stored source revisions are invalid")
    if any(
        not isinstance(item, list)
        or len(item) != 2
        or not all(isinstance(value, str) for value in item)
        for item in parsed
    ):
        raise ValueError("stored source revisions are invalid")
    revisions = _source_revisions(tuple((item[0], item[1]) for item in parsed))
    payload_json = str(row["payload_json"])
    payload_sha256 = str(row["payload_sha256"])
    if hashlib.sha256(payload_json.encode("utf-8")).hexdigest() != payload_sha256:
        raise ValueError("stored run payload digest mismatch")
    config_json = None if row["config_json"] is None else str(row["config_json"])
    config_sha256 = str(row["config_sha256"])
    if (
        config_json is not None
        and hashlib.sha256(config_json.encode("utf-8")).hexdigest() != config_sha256
    ):
        raise ValueError("stored run config digest mismatch")
    run_type = str(row["run_type"])
    logical_key = str(row["logical_key"])
    if str(row["run_id"]) != research_run_id(run_type, logical_key):
        raise ValueError("stored research run identity mismatch")
    return StoredResearchRun(
        run_id=str(row["run_id"]),
        run_type=run_type,
        logical_key=logical_key,
        strategy_version=(
            None if row["strategy_version"] is None else str(row["strategy_version"])
        ),
        status=str(row["status"]),
        started_at=_aware_utc(datetime.fromisoformat(str(row["started_at"])), "started_at"),
        completed_at=_aware_utc(
            datetime.fromisoformat(str(row["completed_at"])), "completed_at"
        ),
        source_revisions=revisions,
        config_json=config_json,
        config_sha256=config_sha256,
        payload_json=payload_json,
        payload_sha256=payload_sha256,
        record_version=int(row["record_version"]),
    )


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _nonempty(value: str, field_name: str) -> str:
    resolved = value.strip()
    if not resolved:
        raise ValueError(f"{field_name} must not be empty")
    return resolved
