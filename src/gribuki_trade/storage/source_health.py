"""面向长时间浸泡测试的仅追加提供方健康观测。

存储刻意只接受有界、结构化字段。特别是，没有用于异常文本、请求 URL、标头或响应
正文的字段。提供方适配器应先将失败映射为稳定 ``error_code``，再记录观测。
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from os import PathLike


class ProviderRunStatus(StrEnum):
    """一次提供方操作的终态。"""

    SUCCESS = "success"
    FAILURE = "failure"


@dataclass(frozen=True, slots=True)
class ProviderRun:
    """一条不可变的提供方操作观测。

    ``latency_ms`` 应尽量使用单调时钟测量；墙上时钟时间戳仍适用于窗口统计和事故复盘。
    """

    run_id: str
    source_id: str
    operation: str
    started_at: datetime
    finished_at: datetime
    status: ProviderRunStatus
    item_count: int
    degraded: bool
    stale: bool
    latency_ms: float
    error_code: str | None = None
    adapter_version: str = "unknown"
    record_version: int = 1

    def __post_init__(self) -> None:
        _require_identifier(self.run_id, "run_id", max_length=128)
        _require_identifier(self.source_id, "source_id", max_length=96)
        _require_identifier(self.operation, "operation", max_length=96)
        _require_identifier(
            self.adapter_version,
            "adapter_version",
            max_length=64,
        )
        if not isinstance(self.status, ProviderRunStatus):
            raise ValueError("status must be a ProviderRunStatus")
        if (
            isinstance(self.item_count, bool)
            or not isinstance(self.item_count, int)
            or self.item_count < 0
        ):
            raise ValueError("item_count must be a non-negative integer")
        if not isinstance(self.degraded, bool):
            raise ValueError("degraded must be a boolean")
        if not isinstance(self.stale, bool):
            raise ValueError("stale must be a boolean")
        if isinstance(self.latency_ms, bool) or not isinstance(
            self.latency_ms, (int, float)
        ):
            raise ValueError("latency_ms must be a finite non-negative number")
        normalized_latency = float(self.latency_ms)
        if not math.isfinite(normalized_latency) or normalized_latency < 0:
            raise ValueError("latency_ms must be a finite non-negative number")
        if isinstance(self.record_version, bool) or not isinstance(
            self.record_version, int
        ):
            raise ValueError("record_version must be a positive integer")
        if self.record_version < 1:
            raise ValueError("record_version must be a positive integer")

        started = _normalize_time(self.started_at, "started_at")
        finished = _normalize_time(self.finished_at, "finished_at")
        if finished < started:
            raise ValueError("finished_at must not be earlier than started_at")

        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "finished_at", finished)
        object.__setattr__(self, "latency_ms", normalized_latency)
        object.__setattr__(self, "error_code", _sanitize_error_code(self.error_code))


@dataclass(frozen=True, slots=True)
class SourceHealthSummary:
    """一项来源及可选操作的聚合健康指标。"""

    source_id: str
    operation: str | None
    window_start: datetime
    window_end: datetime
    total_runs: int
    successful_runs: int
    failed_runs: int
    success_rate: float | None
    degraded_rate: float | None
    stale_rate: float | None
    p50_latency_ms: float | None
    p95_latency_ms: float | None
    latest_success: ProviderRun | None
    latest_failure: ProviderRun | None


class SourceHealthCollisionError(ValueError):
    """同一运行 ID 被以不同不可变内容重放。"""


class SQLiteSourceHealthStore:
    """用于仅追加提供方健康观测的 SQLite WAL 存储。"""

    def __init__(
        self,
        path: str | PathLike[str],
        *,
        busy_timeout_seconds: float = 5.0,
    ) -> None:
        if not math.isfinite(busy_timeout_seconds) or busy_timeout_seconds <= 0:
            raise ValueError("busy_timeout_seconds must be positive")
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            path,
            timeout=busy_timeout_seconds,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute(
            f"PRAGMA busy_timeout = {int(busy_timeout_seconds * 1000)}"
        )
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._initialize()

    def _initialize(self) -> None:
        with self._transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS provider_runs (
                    run_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_code TEXT,
                    item_count INTEGER NOT NULL,
                    degraded INTEGER NOT NULL,
                    stale INTEGER NOT NULL,
                    latency_ms REAL NOT NULL,
                    adapter_version TEXT NOT NULL,
                    record_version INTEGER NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    CHECK (status IN ('success', 'failure')),
                    CHECK (item_count >= 0),
                    CHECK (degraded IN (0, 1)),
                    CHECK (stale IN (0, 1)),
                    CHECK (latency_ms >= 0),
                    CHECK (record_version >= 1)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_provider_runs_source_finished
                ON provider_runs(source_id, finished_at DESC, run_id)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_provider_runs_source_operation_finished
                ON provider_runs(source_id, operation, finished_at DESC, run_id)
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS provider_runs_no_update
                BEFORE UPDATE ON provider_runs
                BEGIN
                    SELECT RAISE(ABORT, 'provider_runs is append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS provider_runs_no_delete
                BEFORE DELETE ON provider_runs
                BEGIN
                    SELECT RAISE(ABORT, 'provider_runs is append-only');
                END
                """
            )

    def append(self, run: ProviderRun) -> bool:
        """追加一次运行；精确幂等重放返回 ``False``。"""

        payload = _canonical_payload(run)
        digest = hashlib.sha256(payload).hexdigest()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT payload_sha256 FROM provider_runs WHERE run_id = ?",
                (run.run_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["payload_sha256"]) != digest:
                    raise SourceHealthCollisionError(
                        "run_id was reused with different provider-run content"
                    )
                return False
            connection.execute(
                """
                INSERT INTO provider_runs (
                    run_id, source_id, operation, started_at, finished_at,
                    status, error_code, item_count, degraded, stale,
                    latency_ms, adapter_version, record_version, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.run_id,
                    run.source_id,
                    run.operation,
                    _serialize_time(run.started_at),
                    _serialize_time(run.finished_at),
                    run.status.value,
                    run.error_code,
                    run.item_count,
                    int(run.degraded),
                    int(run.stale),
                    run.latency_ms,
                    run.adapter_version,
                    run.record_version,
                    digest,
                ),
            )
        return True

    def get(self, run_id: str) -> ProviderRun | None:
        _require_identifier(run_id, "run_id", max_length=128)
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT * FROM provider_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        return None if row is None else _row_to_run(row)

    def list_runs(
        self,
        *,
        source_id: str | None = None,
        operation: str | None = None,
        limit: int = 200,
    ) -> Sequence[ProviderRun]:
        """返回近期观测，最新完成的记录在前。"""

        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be a positive integer")
        if source_id is not None:
            _require_identifier(source_id, "source_id", max_length=96)
        if operation is not None:
            _require_identifier(operation, "operation", max_length=96)
            if source_id is None:
                raise ValueError("operation filtering requires source_id")

        clauses: list[str] = []
        parameters: list[object] = []
        if source_id is not None:
            clauses.append("source_id = ?")
            parameters.append(source_id)
        if operation is not None:
            clauses.append("operation = ?")
            parameters.append(operation)
        query = "SELECT * FROM provider_runs"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY finished_at DESC, run_id LIMIT ?"
        parameters.append(limit)
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(query, parameters).fetchall()
        return tuple(_row_to_run(row) for row in rows)

    def summarize(
        self,
        source_id: str,
        *,
        window_start: datetime,
        window_end: datetime,
        operation: str | None = None,
    ) -> SourceHealthSummary:
        """聚合在半开时间窗口内完成的运行。

        延迟分位数在秩 ``(n - 1) * q`` 处使用线性插值。窗口没有观测时，
        比率和分位数均为 ``None``。
        """

        _require_identifier(source_id, "source_id", max_length=96)
        if operation is not None:
            _require_identifier(operation, "operation", max_length=96)
        start = _normalize_time(window_start, "window_start")
        end = _normalize_time(window_end, "window_end")
        if end <= start:
            raise ValueError("window_end must be later than window_start")

        clauses = ["source_id = ?", "finished_at >= ?", "finished_at < ?"]
        parameters: list[object] = [
            source_id,
            _serialize_time(start),
            _serialize_time(end),
        ]
        if operation is not None:
            clauses.append("operation = ?")
            parameters.append(operation)
        query = (
            "SELECT * FROM provider_runs WHERE "
            + " AND ".join(clauses)
            + " ORDER BY finished_at DESC, run_id"
        )
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(query, parameters).fetchall()

        runs = tuple(_row_to_run(row) for row in rows)
        total = len(runs)
        successful = sum(run.status is ProviderRunStatus.SUCCESS for run in runs)
        failed = total - successful
        latest_success = next(
            (run for run in runs if run.status is ProviderRunStatus.SUCCESS),
            None,
        )
        latest_failure = next(
            (run for run in runs if run.status is ProviderRunStatus.FAILURE),
            None,
        )
        latencies = sorted(run.latency_ms for run in runs)
        if total == 0:
            success_rate = degraded_rate = stale_rate = None
            p50 = p95 = None
        else:
            success_rate = successful / total
            degraded_rate = sum(run.degraded for run in runs) / total
            stale_rate = sum(run.stale for run in runs) / total
            p50 = _percentile(latencies, 0.50)
            p95 = _percentile(latencies, 0.95)
        return SourceHealthSummary(
            source_id=source_id,
            operation=operation,
            window_start=start,
            window_end=end,
            total_runs=total,
            successful_runs=successful,
            failed_runs=failed,
            success_rate=success_rate,
            degraded_rate=degraded_rate,
            stale_rate=stale_rate,
            p50_latency_ms=p50,
            p95_latency_ms=p95,
            latest_success=latest_success,
            latest_failure=latest_failure,
        )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()

    def __enter__(self) -> SQLiteSourceHealthStore:
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
            raise RuntimeError("source health store is closed")


def _canonical_payload(run: ProviderRun) -> bytes:
    document: dict[str, object] = {
        "run_id": run.run_id,
        "source_id": run.source_id,
        "operation": run.operation,
        "started_at": _serialize_time(run.started_at),
        "finished_at": _serialize_time(run.finished_at),
        "status": run.status.value,
        "error_code": run.error_code,
        "item_count": run.item_count,
        "degraded": run.degraded,
        "stale": run.stale,
        "latency_ms": run.latency_ms,
        "adapter_version": run.adapter_version,
        "record_version": run.record_version,
    }
    return json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _row_to_run(row: sqlite3.Row) -> ProviderRun:
    return ProviderRun(
        run_id=str(row["run_id"]),
        source_id=str(row["source_id"]),
        operation=str(row["operation"]),
        started_at=_parse_time(str(row["started_at"])),
        finished_at=_parse_time(str(row["finished_at"])),
        status=ProviderRunStatus(str(row["status"])),
        error_code=None if row["error_code"] is None else str(row["error_code"]),
        item_count=int(row["item_count"]),
        degraded=bool(row["degraded"]),
        stale=bool(row["stale"]),
        latency_ms=float(row["latency_ms"]),
        adapter_version=str(row["adapter_version"]),
        record_version=int(row["record_version"]),
    )


def _percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    rank = (len(values) - 1) * quantile
    lower_index = math.floor(rank)
    upper_index = math.ceil(rank)
    if lower_index == upper_index:
        return values[lower_index]
    weight = rank - lower_index
    return values[lower_index] + (values[upper_index] - values[lower_index]) * weight


def _normalize_time(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _serialize_time(value: datetime) -> str:
    return _normalize_time(value, "datetime").isoformat(timespec="microseconds")


def _parse_time(value: str) -> datetime:
    return _normalize_time(datetime.fromisoformat(value), "stored datetime")


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_SAFE_ERROR_CODE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")


def _require_identifier(value: str, name: str, *, max_length: int) -> None:
    if (
        not isinstance(value, str)
        or len(value) > max_length
        or _SAFE_IDENTIFIER.fullmatch(value) is None
    ):
        raise ValueError(f"{name} must be a safe non-empty identifier")


def _sanitize_error_code(value: str | None) -> str | None:
    if value is None:
        return None
    candidate = value.strip().lower()
    if _SAFE_ERROR_CODE.fullmatch(candidate) is None:
        return "unclassified_error"
    return candidate
