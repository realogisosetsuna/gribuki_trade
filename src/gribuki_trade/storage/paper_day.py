"""A 股 PAPER 交易日的单连接只追加审计存储。"""

from __future__ import annotations

import hashlib
import sqlite3
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from os import PathLike

from gribuki_trade.domain.paper_day import (
    NewPaperDayEvent,
    PaperDayEvent,
    PaperDayPhase,
    PaperDayReplay,
    PaperDayRunManifest,
    PaperDaySeverity,
    aware_utc,
    paper_day_canonical_json,
    paper_day_event_id,
)


class PaperDayStoreError(RuntimeError):
    """脱敏持久交易时段失败的基类。"""


class PaperDayStoreConflictError(PaperDayStoreError):
    """同一确定性身份被用于不同的不可变内容。"""


class PaperDayStoreLeaseError(PaperDayStoreError):
    """调用方不持有有效的单写者租约。"""


class PaperDayStoreIntegrityError(PaperDayStoreError):
    """清单、载荷或哈希链完整性校验失败。"""


class SQLitePaperDayStore:
    """带有一个可变运行租约的防篡改事件日志。

    实例在整个生命周期中只持有一个 SQLite 连接。这对受 2026 年 WAL 重置竞争
    影响的 SQLite 运行时尤其重要：调用方必须在进程内共享该实例，不得并发打开
    指向同一路径的另一存储实例。
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
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS paper_day_runs (
                    run_id TEXT PRIMARY KEY,
                    session_date TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    config_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    target_hash TEXT NOT NULL,
                    initial_cash TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    UNIQUE(session_date, account_id, config_sha256),
                    CHECK(schema_version = 1)
                );
                CREATE INDEX IF NOT EXISTS ix_paper_day_runs_session
                    ON paper_day_runs(session_date DESC, created_at, run_id);

                CREATE TABLE IF NOT EXISTS paper_day_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL,
                    event_key TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    known_at TEXT NOT NULL,
                    notification_required INTEGER NOT NULL,
                    symbol TEXT,
                    correlation_id TEXT,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    previous_hash TEXT,
                    event_hash TEXT NOT NULL UNIQUE,
                    UNIQUE(run_id, event_key),
                    FOREIGN KEY(run_id) REFERENCES paper_day_runs(run_id),
                    CHECK(notification_required IN (0, 1))
                );
                CREATE INDEX IF NOT EXISTS ix_paper_day_events_run_sequence
                    ON paper_day_events(run_id, sequence);
                CREATE INDEX IF NOT EXISTS ix_paper_day_events_run_known
                    ON paper_day_events(run_id, known_at, sequence);
                CREATE INDEX IF NOT EXISTS ix_paper_day_events_symbol
                    ON paper_day_events(run_id, symbol, sequence);

                CREATE TABLE IF NOT EXISTS paper_day_writer_leases (
                    run_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    lease_until TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES paper_day_runs(run_id)
                );

                CREATE TRIGGER IF NOT EXISTS paper_day_runs_no_update
                BEFORE UPDATE ON paper_day_runs BEGIN
                    SELECT RAISE(ABORT, 'paper day runs are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS paper_day_runs_no_delete
                BEFORE DELETE ON paper_day_runs BEGIN
                    SELECT RAISE(ABORT, 'paper day runs are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS paper_day_events_no_update
                BEFORE UPDATE ON paper_day_events BEGIN
                    SELECT RAISE(ABORT, 'paper day events are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS paper_day_events_no_delete
                BEFORE DELETE ON paper_day_events BEGIN
                    SELECT RAISE(ABORT, 'paper day events are append-only');
                END;
                """
            )

    def create_run(self, manifest: PaperDayRunManifest) -> bool:
        """插入一份清单；若为内容完全一致的幂等重放则返回 false。"""

        if not isinstance(manifest, PaperDayRunManifest):
            raise TypeError("manifest must be a PaperDayRunManifest")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM paper_day_runs WHERE run_id = ?", (manifest.run_id,)
            ).fetchone()
            if row is not None:
                try:
                    stored = _row_to_manifest(row)
                except (TypeError, ValueError) as error:
                    raise PaperDayStoreIntegrityError(
                        "stored PAPER-day manifest failed integrity validation"
                    ) from error
                if stored != manifest:
                    raise PaperDayStoreConflictError(
                        "PAPER-day run identity is bound to different content"
                    )
                return False
            try:
                connection.execute(
                    """
                    INSERT INTO paper_day_runs (
                        run_id, session_date, account_id, config_json,
                        config_sha256, created_at, target_hash, initial_cash,
                        schema_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        manifest.run_id,
                        manifest.session_date.isoformat(),
                        manifest.account_id,
                        manifest.config_json,
                        manifest.config_sha256,
                        manifest.created_at.isoformat(),
                        manifest.target_hash,
                        format(manifest.initial_cash, "f"),
                        manifest.schema_version,
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise PaperDayStoreConflictError(
                    "PAPER-day manifest identity conflict"
                ) from error
        return True

    def get_run(self, run_id: str) -> PaperDayRunManifest | None:
        run_id = _identifier(run_id, "run_id")
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT * FROM paper_day_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        if row is None:
            return None
        try:
            return _row_to_manifest(row)
        except (TypeError, ValueError) as error:
            raise PaperDayStoreIntegrityError(
                "stored PAPER-day manifest failed integrity validation"
            ) from error

    def list_runs(self) -> tuple[PaperDayRunManifest, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT * FROM paper_day_runs
                ORDER BY session_date DESC, created_at, run_id
                """
            ).fetchall()
        try:
            return tuple(_row_to_manifest(row) for row in rows)
        except (TypeError, ValueError) as error:
            raise PaperDayStoreIntegrityError(
                "stored PAPER-day manifest failed integrity validation"
            ) from error

    def acquire_lease(
        self,
        run_id: str,
        owner_id: str,
        *,
        now: datetime,
        lease_for: timedelta | None = None,
        lease_duration: timedelta | None = None,
    ) -> None:
        """获取不存在或已过期的租约，或续期当前已持有的租约。"""

        run_id = _identifier(run_id, "run_id")
        owner_id = _identifier(owner_id, "owner_id")
        now = aware_utc(now, "now")
        duration = _lease_duration(lease_for, lease_duration)
        lease_until = now + duration
        with self._transaction() as connection:
            self._assert_run_exists(connection, run_id)
            row = connection.execute(
                "SELECT * FROM paper_day_writer_leases WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is not None:
                current_owner = str(row["owner_id"])
                current_until = aware_utc(
                    datetime.fromisoformat(str(row["lease_until"])), "lease_until"
                )
                if current_owner != owner_id and current_until > now:
                    raise PaperDayStoreLeaseError(
                        "PAPER-day run has another live writer"
                    )
            connection.execute(
                """
                INSERT INTO paper_day_writer_leases(run_id, owner_id, lease_until)
                VALUES (?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    owner_id = excluded.owner_id,
                    lease_until = excluded.lease_until
                """,
                (run_id, owner_id, lease_until.isoformat()),
            )

    def renew_lease(
        self,
        run_id: str,
        owner_id: str,
        *,
        now: datetime,
        lease_for: timedelta | None = None,
        lease_duration: timedelta | None = None,
    ) -> None:
        """延长仍有效的租约；原租约过期后必须显式重新获取。"""

        run_id = _identifier(run_id, "run_id")
        owner_id = _identifier(owner_id, "owner_id")
        now = aware_utc(now, "now")
        duration = _lease_duration(lease_for, lease_duration)
        lease_until = now + duration
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM paper_day_writer_leases WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None or str(row["owner_id"]) != owner_id:
                raise PaperDayStoreLeaseError(
                    "PAPER-day writer lease is missing or owned elsewhere"
                )
            current_until = aware_utc(
                datetime.fromisoformat(str(row["lease_until"])), "lease_until"
            )
            if current_until <= now:
                raise PaperDayStoreLeaseError("PAPER-day writer lease has expired")
            connection.execute(
                """
                UPDATE paper_day_writer_leases SET lease_until = ?
                WHERE run_id = ? AND owner_id = ?
                """,
                (lease_until.isoformat(), run_id, owner_id),
            )

    def release_lease(self, run_id: str, owner_id: str) -> bool:
        """释放已持有租约；租约已不存在时按幂等处理。"""

        run_id = _identifier(run_id, "run_id")
        owner_id = _identifier(owner_id, "owner_id")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT owner_id FROM paper_day_writer_leases WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                return False
            if str(row["owner_id"]) != owner_id:
                raise PaperDayStoreLeaseError(
                    "PAPER-day writer lease is owned elsewhere"
                )
            connection.execute(
                "DELETE FROM paper_day_writer_leases WHERE run_id = ?", (run_id,)
            )
        return True

    def append_event(
        self,
        event: NewPaperDayEvent,
        *,
        owner_id: str,
        lease_checked_at: datetime | None = None,
    ) -> tuple[PaperDayEvent, bool]:
        """在有效租约下追加事件，或返回内容完全一致的幂等事件。"""

        if not isinstance(event, NewPaperDayEvent):
            raise TypeError("event must be a NewPaperDayEvent")
        owner_id = _identifier(owner_id, "owner_id")
        checked_at = (
            datetime.now(UTC)
            if lease_checked_at is None
            else aware_utc(lease_checked_at, "lease_checked_at")
        )
        with self._transaction() as connection:
            self._assert_live_lease(
                connection,
                run_id=event.run_id,
                owner_id=owner_id,
                at=checked_at,
            )
            duplicate = connection.execute(
                """
                SELECT * FROM paper_day_events
                WHERE run_id = ? AND event_key = ?
                """,
                (event.run_id, event.event_key),
            ).fetchone()
            if duplicate is not None:
                try:
                    stored = _row_to_event(duplicate)
                except (TypeError, ValueError) as error:
                    raise PaperDayStoreIntegrityError(
                        "stored PAPER-day event failed integrity validation"
                    ) from error
                if not _same_event_content(stored, event):
                    raise PaperDayStoreConflictError(
                        "PAPER-day event key is bound to different content"
                    )
                return stored, False

            latest_for_run = connection.execute(
                """
                SELECT known_at FROM paper_day_events
                WHERE run_id = ? ORDER BY sequence DESC LIMIT 1
                """,
                (event.run_id,),
            ).fetchone()
            if latest_for_run is not None:
                latest_known = aware_utc(
                    datetime.fromisoformat(str(latest_for_run["known_at"])),
                    "known_at",
                )
                if event.known_at < latest_known:
                    raise PaperDayStoreConflictError(
                        "PAPER-day events cannot be learned retroactively"
                    )

            previous = connection.execute(
                """
                SELECT event_hash FROM paper_day_events
                ORDER BY sequence DESC LIMIT 1
                """
            ).fetchone()
            previous_hash = None if previous is None else str(previous["event_hash"])
            event_id = paper_day_event_id(
                run_id=event.run_id, event_key=event.event_key
            )
            payload_sha256 = _sha(event.payload_json)
            event_hash = _event_hash(
                event_id=event_id,
                run_id=event.run_id,
                event_key=event.event_key,
                event_type=event.event_type,
                phase=event.phase,
                severity=event.severity,
                occurred_at=event.occurred_at,
                known_at=event.known_at,
                notification_required=event.notification_required,
                symbol=event.symbol,
                correlation_id=event.correlation_id,
                payload_sha256=payload_sha256,
                previous_hash=previous_hash,
            )
            try:
                cursor = connection.execute(
                    """
                    INSERT INTO paper_day_events (
                        event_id, run_id, event_key, event_type, phase, severity,
                        occurred_at, known_at, notification_required, symbol,
                        correlation_id, payload_json, payload_sha256,
                        previous_hash, event_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        event.run_id,
                        event.event_key,
                        event.event_type,
                        event.phase.value,
                        event.severity.value,
                        event.occurred_at.isoformat(),
                        event.known_at.isoformat(),
                        int(event.notification_required),
                        event.symbol,
                        event.correlation_id,
                        event.payload_json,
                        payload_sha256,
                        previous_hash,
                        event_hash,
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise PaperDayStoreConflictError(
                    "PAPER-day event identity conflict"
                ) from error
            row = connection.execute(
                "SELECT * FROM paper_day_events WHERE sequence = ?",
                (cursor.lastrowid,),
            ).fetchone()
            if row is None:  # pragma: no cover - SQLite 插入契约
                raise PaperDayStoreIntegrityError(
                    "inserted PAPER-day event disappeared"
                )
            return _row_to_event(row), True

    def events(
        self,
        run_id: str,
        *,
        as_of: datetime | None = None,
        after_sequence: int = 0,
    ) -> tuple[PaperDayEvent, ...]:
        """校验完整哈希链，再返回运行的时点切片。"""

        run_id = _identifier(run_id, "run_id")
        if isinstance(after_sequence, bool) or not isinstance(after_sequence, int):
            raise TypeError("after_sequence must be an integer")
        if after_sequence < 0:
            raise ValueError("after_sequence must be non-negative")
        cutoff = None if as_of is None else aware_utc(as_of, "as_of")
        all_events = self._verified_events()
        return tuple(
            event
            for event in all_events
            if event.run_id == run_id
            and event.sequence > after_sequence
            and (cutoff is None or event.known_at <= cutoff)
        )

    def list_events(
        self,
        run_id: str,
        *,
        as_of: datetime | None = None,
        after_sequence: int = 0,
    ) -> tuple[PaperDayEvent, ...]:
        """为偏好 list 风格仓储命名的调用方提供显式别名。"""

        return self.events(run_id, as_of=as_of, after_sequence=after_sequence)

    def event_by_key(
        self,
        run_id: str,
        event_key: str,
        *,
        as_of: datetime | None = None,
    ) -> PaperDayEvent | None:
        event_key = _identifier(event_key, "event_key")
        return next(
            (
                event
                for event in self.events(run_id, as_of=as_of)
                if event.event_key == event_key
            ),
            None,
        )

    def get_by_event_id(
        self, event_id: str, *, as_of: datetime | None = None
    ) -> PaperDayEvent | None:
        event_id = _identifier(event_id, "event_id")
        cutoff = None if as_of is None else aware_utc(as_of, "as_of")
        return next(
            (
                event
                for event in self._verified_events()
                if event.event_id == event_id
                and (cutoff is None or event.known_at <= cutoff)
            ),
            None,
        )

    def reconstruct(
        self,
        run_id: str,
        *,
        as_of: datetime | None = None,
    ) -> PaperDayReplay | None:
        """构建重放视图；若当时尚不知道清单则返回 none。"""

        manifest = self.get_run(run_id)
        if manifest is None:
            return None
        cutoff = datetime.now(UTC) if as_of is None else aware_utc(as_of, "as_of")
        if manifest.created_at > cutoff:
            return None
        return PaperDayReplay(
            manifest=manifest,
            as_of=cutoff,
            events=self.events(run_id, as_of=cutoff),
        )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()

    def __enter__(self) -> SQLitePaperDayStore:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _verified_events(self) -> tuple[PaperDayEvent, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                "SELECT * FROM paper_day_events ORDER BY sequence"
            ).fetchall()
        try:
            events = tuple(_row_to_event(row) for row in rows)
            _verify_chain(events)
        except (TypeError, ValueError) as error:
            raise PaperDayStoreIntegrityError(
                "PAPER-day event stream failed integrity validation"
            ) from error
        return events

    @staticmethod
    def _assert_run_exists(connection: sqlite3.Connection, run_id: str) -> None:
        row = connection.execute(
            "SELECT 1 FROM paper_day_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise PaperDayStoreConflictError("PAPER-day run does not exist")

    @staticmethod
    def _assert_live_lease(
        connection: sqlite3.Connection,
        *,
        run_id: str,
        owner_id: str,
        at: datetime,
    ) -> None:
        row = connection.execute(
            "SELECT * FROM paper_day_writer_leases WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None or str(row["owner_id"]) != owner_id:
            raise PaperDayStoreLeaseError(
                "PAPER-day writer lease is missing or owned elsewhere"
            )
        lease_until = aware_utc(
            datetime.fromisoformat(str(row["lease_until"])), "lease_until"
        )
        if lease_until <= at:
            raise PaperDayStoreLeaseError("PAPER-day writer lease has expired")

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
            raise RuntimeError("PAPER-day store is closed")


def _row_to_manifest(row: sqlite3.Row) -> PaperDayRunManifest:
    from datetime import date
    from decimal import Decimal

    return PaperDayRunManifest(
        run_id=str(row["run_id"]),
        session_date=date.fromisoformat(str(row["session_date"])),
        account_id=str(row["account_id"]),
        config_json=str(row["config_json"]),
        config_sha256=str(row["config_sha256"]),
        created_at=datetime.fromisoformat(str(row["created_at"])),
        target_hash=str(row["target_hash"]),
        initial_cash=Decimal(str(row["initial_cash"])),
        schema_version=int(row["schema_version"]),
    )


def _row_to_event(row: sqlite3.Row) -> PaperDayEvent:
    return PaperDayEvent(
        sequence=int(row["sequence"]),
        event_id=str(row["event_id"]),
        run_id=str(row["run_id"]),
        event_key=str(row["event_key"]),
        event_type=str(row["event_type"]),
        phase=PaperDayPhase(str(row["phase"])),
        severity=PaperDaySeverity(str(row["severity"])),
        occurred_at=datetime.fromisoformat(str(row["occurred_at"])),
        known_at=datetime.fromisoformat(str(row["known_at"])),
        notification_required=bool(int(row["notification_required"])),
        symbol=None if row["symbol"] is None else str(row["symbol"]),
        correlation_id=(
            None if row["correlation_id"] is None else str(row["correlation_id"])
        ),
        payload_json=str(row["payload_json"]),
        payload_sha256=str(row["payload_sha256"]),
        previous_hash=(
            None if row["previous_hash"] is None else str(row["previous_hash"])
        ),
        event_hash=str(row["event_hash"]),
    )


def _same_event_content(stored: PaperDayEvent, new: NewPaperDayEvent) -> bool:
    return (
        stored.event_id == new.event_id
        and stored.event_type == new.event_type
        and stored.phase is new.phase
        and stored.severity is new.severity
        and stored.occurred_at == new.occurred_at
        and stored.known_at == new.known_at
        and stored.notification_required is new.notification_required
        and stored.symbol == new.symbol
        and stored.correlation_id == new.correlation_id
        and stored.payload_json == new.payload_json
    )


def _verify_chain(events: Sequence[PaperDayEvent]) -> None:
    previous_hash: str | None = None
    expected_sequence = 1
    for event in events:
        if event.sequence != expected_sequence:
            raise PaperDayStoreIntegrityError(
                "PAPER-day event sequence is not contiguous"
            )
        if event.previous_hash != previous_hash:
            raise PaperDayStoreIntegrityError("PAPER-day event hash chain is broken")
        expected_hash = _event_hash(
            event_id=event.event_id,
            run_id=event.run_id,
            event_key=event.event_key,
            event_type=event.event_type,
            phase=event.phase,
            severity=event.severity,
            occurred_at=event.occurred_at,
            known_at=event.known_at,
            notification_required=event.notification_required,
            symbol=event.symbol,
            correlation_id=event.correlation_id,
            payload_sha256=event.payload_sha256,
            previous_hash=event.previous_hash,
        )
        if event.event_hash != expected_hash:
            raise PaperDayStoreIntegrityError("PAPER-day event digest mismatch")
        previous_hash = event.event_hash
        expected_sequence += 1


def _event_hash(
    *,
    event_id: str,
    run_id: str,
    event_key: str,
    event_type: str,
    phase: PaperDayPhase,
    severity: PaperDaySeverity,
    occurred_at: datetime,
    known_at: datetime,
    notification_required: bool,
    symbol: str | None,
    correlation_id: str | None,
    payload_sha256: str,
    previous_hash: str | None,
) -> str:
    document = paper_day_canonical_json(
        {
            "correlation_id": correlation_id,
            "event_id": event_id,
            "event_key": event_key,
            "event_type": event_type,
            "known_at": known_at.isoformat(),
            "notification_required": notification_required,
            "occurred_at": occurred_at.isoformat(),
            "payload_sha256": payload_sha256,
            "phase": phase.value,
            "previous_hash": previous_hash,
            "run_id": run_id,
            "severity": severity.value,
            "symbol": symbol,
        }
    )
    return _sha(document)


def _identifier(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized or len(normalized) > 128:
        raise ValueError(f"{field_name} must be a non-empty safe identifier")
    return normalized


def _lease_duration(
    lease_for: timedelta | None,
    lease_duration: timedelta | None,
) -> timedelta:
    if (lease_for is None) == (lease_duration is None):
        raise ValueError("provide exactly one lease duration")
    value = lease_for if lease_for is not None else lease_duration
    if not isinstance(value, timedelta):
        raise TypeError("lease_duration must be a timedelta")
    if value <= timedelta(0):
        raise ValueError("lease_duration must be positive")
    return value


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
