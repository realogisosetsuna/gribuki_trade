"""Append-only SQLite storage for offline strategy experiment audit documents."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from os import PathLike

from gribuki_trade.strategy_lab.experiments import (
    StrategyExperiment,
    experiment_to_json,
)


class StrategyExperimentCollisionError(ValueError):
    """An experiment ID was replayed with different immutable content."""


@dataclass(frozen=True, slots=True)
class StoredStrategyExperiment:
    experiment_id: str
    created_at: datetime
    data_manifest_sha256: str
    strategy_manifest_sha256: str
    strategy_version: str
    objective: str
    trial_count: int
    selected_trial_id: str
    baseline_trial_id: str
    warnings: tuple[str, ...]
    payload_json: str
    payload_sha256: str
    record_version: int

    def payload_document(self) -> dict[str, object]:
        parsed = json.loads(self.payload_json)
        if not isinstance(parsed, dict):
            raise ValueError("stored experiment payload must be a JSON object")
        return {str(key): value for key, value in parsed.items()}


class SQLiteStrategyExperimentStore:
    """WAL-backed immutable experiment registry.

    The canonical JSON contains every fold, candidate, metric, cost scenario,
    contribution, warning, and holdout result.  Indexed columns support audit
    discovery without introducing mutable relational projections.
    """

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
                CREATE TABLE IF NOT EXISTS strategy_experiments (
                    experiment_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    data_manifest_sha256 TEXT NOT NULL,
                    strategy_manifest_sha256 TEXT NOT NULL,
                    strategy_version TEXT NOT NULL,
                    objective TEXT NOT NULL,
                    trial_count INTEGER NOT NULL,
                    selected_trial_id TEXT NOT NULL,
                    baseline_trial_id TEXT NOT NULL,
                    warnings_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    record_version INTEGER NOT NULL,
                    CHECK (trial_count >= 1),
                    CHECK (record_version >= 1)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_strategy_experiments_created
                ON strategy_experiments(created_at DESC, experiment_id)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_strategy_experiments_strategy
                ON strategy_experiments(strategy_version, created_at DESC)
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS strategy_experiments_no_update
                BEFORE UPDATE ON strategy_experiments
                BEGIN
                    SELECT RAISE(ABORT, 'strategy_experiments is append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS strategy_experiments_no_delete
                BEFORE DELETE ON strategy_experiments
                BEGIN
                    SELECT RAISE(ABORT, 'strategy_experiments is append-only');
                END
                """
            )

    def append(self, experiment: StrategyExperiment) -> bool:
        """Append an experiment, returning false for an exact idempotent replay."""

        payload = experiment_to_json(experiment)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        warnings_json = json.dumps(
            list(experiment.warnings),
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT payload_sha256 FROM strategy_experiments WHERE experiment_id = ?",
                (experiment.experiment_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["payload_sha256"]) != digest:
                    raise StrategyExperimentCollisionError(
                        "experiment_id was reused with different immutable content"
                    )
                return False
            connection.execute(
                """
                INSERT INTO strategy_experiments (
                    experiment_id, created_at, data_manifest_sha256,
                    strategy_manifest_sha256, strategy_version, objective,
                    trial_count, selected_trial_id, baseline_trial_id,
                    warnings_json, payload_json, payload_sha256, record_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    experiment.experiment_id,
                    experiment.created_at.isoformat(timespec="microseconds"),
                    experiment.data_manifest.manifest_sha256,
                    experiment.strategy_manifest.manifest_sha256,
                    experiment.strategy_manifest.strategy_version,
                    experiment.objective.value,
                    experiment.trial_count,
                    experiment.selected_trial_id,
                    experiment.baseline_trial_id,
                    warnings_json,
                    payload,
                    digest,
                    1,
                ),
            )
        return True

    def get(self, experiment_id: str) -> StoredStrategyExperiment | None:
        if not experiment_id.strip():
            raise ValueError("experiment_id must not be empty")
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT * FROM strategy_experiments WHERE experiment_id = ?",
                (experiment_id,),
            ).fetchone()
        return None if row is None else _row_to_experiment(row)

    def list_experiments(
        self,
        *,
        strategy_version: str | None = None,
        limit: int = 100,
    ) -> Sequence[StoredStrategyExperiment]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be a positive integer")
        parameters: list[object] = []
        query = "SELECT * FROM strategy_experiments"
        if strategy_version is not None:
            if not strategy_version.strip():
                raise ValueError("strategy_version must not be empty")
            query += " WHERE strategy_version = ?"
            parameters.append(strategy_version)
        query += " ORDER BY created_at DESC, experiment_id LIMIT ?"
        parameters.append(limit)
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(query, parameters).fetchall()
        return tuple(_row_to_experiment(row) for row in rows)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()

    def __enter__(self) -> SQLiteStrategyExperimentStore:
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
            raise RuntimeError("strategy experiment store is closed")


def _row_to_experiment(row: sqlite3.Row) -> StoredStrategyExperiment:
    warnings = json.loads(str(row["warnings_json"]))
    if not isinstance(warnings, list) or not all(
        isinstance(item, str) for item in warnings
    ):
        raise ValueError("stored experiment warnings are invalid")
    created_at = datetime.fromisoformat(str(row["created_at"]))
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("stored experiment timestamp is not timezone-aware")
    return StoredStrategyExperiment(
        experiment_id=str(row["experiment_id"]),
        created_at=created_at.astimezone(UTC),
        data_manifest_sha256=str(row["data_manifest_sha256"]),
        strategy_manifest_sha256=str(row["strategy_manifest_sha256"]),
        strategy_version=str(row["strategy_version"]),
        objective=str(row["objective"]),
        trial_count=int(row["trial_count"]),
        selected_trial_id=str(row["selected_trial_id"]),
        baseline_trial_id=str(row["baseline_trial_id"]),
        warnings=tuple(warnings),
        payload_json=str(row["payload_json"]),
        payload_sha256=str(row["payload_sha256"]),
        record_version=int(row["record_version"]),
    )
