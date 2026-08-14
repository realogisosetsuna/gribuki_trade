"""用于可靠发送外部通知的事务型 SQLite 发件箱。"""

from __future__ import annotations

import re
import sqlite3
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from os import PathLike

from gribuki_trade.ports.notifier import (
    NotificationDeliveryError,
    NotificationTargetKind,
    Notifier,
    OutboundNotification,
)


class OutboxStatus(StrEnum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    RETRY = "retry"
    SENT = "sent"
    DEAD = "dead"
    EXPIRED = "expired"


@dataclass(frozen=True, slots=True)
class OutboxItem:
    id: int
    notification: OutboundNotification = field(repr=False)
    status: OutboxStatus
    attempt_count: int
    next_attempt_at: datetime
    lease_until: datetime | None = None
    last_error_code: str | None = None
    provider_message_id: str | None = None
    sent_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DispatchSummary:
    claimed: int = 0
    sent: int = 0
    retry_scheduled: int = 0
    dead: int = 0
    expired: int = 0


class SQLiteOutbox:
    """带事务认领与租约机制的进程本地 SQLite 发件箱。

    工作进程认领条目时会递增 ``attempt_count``。若它在记录结果前终止，
    其他工作进程可在租约到期后重新认领该条目。因此投递语义为至少一次；
    唯一的 ``idempotency_key`` 可避免重复入队。
    """

    def __init__(
        self,
        path: str | PathLike[str],
        *,
        max_attempts: int = 8,
        base_backoff: timedelta = timedelta(seconds=5),
        max_backoff: timedelta = timedelta(minutes=15),
        busy_timeout_seconds: float = 5.0,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if base_backoff <= timedelta(0):
            raise ValueError("base_backoff must be positive")
        if max_backoff < base_backoff:
            raise ValueError("max_backoff must not be shorter than base_backoff")
        if busy_timeout_seconds <= 0:
            raise ValueError("busy_timeout_seconds must be positive")

        self._max_attempts = max_attempts
        self._base_backoff = base_backoff
        self._max_backoff = max_backoff
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            path,
            timeout=busy_timeout_seconds,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute(f"PRAGMA busy_timeout = {int(busy_timeout_seconds * 1000)}")
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = NORMAL")
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        with self._write_transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS notification_outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    channel TEXT NOT NULL,
                    target_kind TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    body TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    status TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT NOT NULL,
                    lease_until TEXT,
                    last_error_code TEXT,
                    provider_message_id TEXT,
                    sent_at TEXT,
                    CHECK (target_kind IN ('private', 'group')),
                    CHECK (status IN (
                        'pending', 'in_flight', 'retry', 'sent', 'dead', 'expired'
                    )),
                    CHECK (attempt_count >= 0)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_notification_outbox_due
                ON notification_outbox(status, next_attempt_at, id)
                """
            )

    def enqueue(self, notification: OutboundNotification) -> OutboxItem:
        """仅插入一次；键与载荷完全相同时返回已有记录。"""

        values = (
            notification.idempotency_key,
            notification.channel,
            notification.target_kind.value,
            notification.target_id,
            notification.text,
            _serialize_time(notification.created_at),
            _serialize_optional_time(notification.expires_at),
            OutboxStatus.PENDING.value,
            _serialize_time(notification.created_at),
        )
        with self._write_transaction() as connection:
            try:
                cursor = connection.execute(
                    """
                    INSERT INTO notification_outbox (
                        idempotency_key, channel, target_kind, target_id, body,
                        created_at, expires_at, status, next_attempt_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )
                row = connection.execute(
                    "SELECT * FROM notification_outbox WHERE id = ?", (cursor.lastrowid,)
                ).fetchone()
            except sqlite3.IntegrityError:
                row = connection.execute(
                    "SELECT * FROM notification_outbox WHERE idempotency_key = ?",
                    (notification.idempotency_key,),
                ).fetchone()
                if row is None:
                    raise
                existing_payload = (
                    row["channel"],
                    row["target_kind"],
                    row["target_id"],
                    row["body"],
                    row["expires_at"],
                )
                wanted_payload = (
                    notification.channel,
                    notification.target_kind.value,
                    notification.target_id,
                    notification.text,
                    _serialize_optional_time(notification.expires_at),
                )
                if existing_payload != wanted_payload:
                    raise ValueError(
                        "idempotency_key is already used for a different payload"
                    ) from None
        return _row_to_item(_require_row(row))

    def claim_due(
        self,
        *,
        now: datetime | None = None,
        limit: int = 50,
        lease_for: timedelta = timedelta(seconds=30),
        target_kind: NotificationTargetKind | None = None,
        target_id: str | None = None,
        channels: Sequence[str] | None = None,
    ) -> Sequence[OutboxItem]:
        """原子领取到期消息；可把工作进程严格限定到一个目标。"""

        instant = _normalize_time(now or datetime.now(UTC), "now")
        if limit < 1:
            raise ValueError("limit must be positive")
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        if (target_kind is None) != (target_id is None):
            raise ValueError("target_kind and target_id must be configured together")
        if target_id is not None and not target_id.strip():
            raise ValueError("target_id must not be blank")
        if channels is not None and any(not item.strip() for item in channels):
            raise ValueError("channels must not contain blank values")
        serialized_now = _serialize_time(instant)
        lease_until = _serialize_time(instant + lease_for)

        with self._write_transaction() as connection:
            connection.execute(
                """
                UPDATE notification_outbox
                SET status = ?, lease_until = NULL, last_error_code = ?
                WHERE status IN (?, ?)
                  AND expires_at IS NOT NULL AND expires_at <= ?
                """,
                (
                    OutboxStatus.EXPIRED.value,
                    "ttl_expired",
                    OutboxStatus.PENDING.value,
                    OutboxStatus.RETRY.value,
                    serialized_now,
                ),
            )
            # 不能仅因该工作进程创建认领后缩短了 TTL，就在其 HTTP 调用
            # 期间越过期限时抢占租约；但已到期租约可安全终结而不必重试。
            connection.execute(
                """
                UPDATE notification_outbox
                SET status = ?, lease_until = NULL, last_error_code = ?
                WHERE status = ? AND lease_until <= ?
                  AND expires_at IS NOT NULL AND expires_at <= ?
                """,
                (
                    OutboxStatus.EXPIRED.value,
                    "ttl_expired",
                    OutboxStatus.IN_FLIGHT.value,
                    serialized_now,
                    serialized_now,
                ),
            )
            connection.execute(
                """
                UPDATE notification_outbox
                SET status = ?, lease_until = NULL, next_attempt_at = ?,
                    last_error_code = 'lease_expired'
                WHERE status = ? AND lease_until <= ?
                  AND (expires_at IS NULL OR expires_at > ?)
                """,
                (
                    OutboxStatus.RETRY.value,
                    serialized_now,
                    OutboxStatus.IN_FLIGHT.value,
                    serialized_now,
                    serialized_now,
                ),
            )
            connection.execute(
                """
                UPDATE notification_outbox
                SET status = ?, lease_until = NULL, last_error_code = 'attempts_exhausted'
                WHERE status IN (?, ?) AND attempt_count >= ?
                """,
                (
                    OutboxStatus.DEAD.value,
                    OutboxStatus.PENDING.value,
                    OutboxStatus.RETRY.value,
                    self._max_attempts,
                ),
            )
            target_filter = ""
            target_parameters: tuple[str, ...] = ()
            if target_kind is not None and target_id is not None:
                target_filter = " AND target_kind = ? AND target_id = ?"
                target_parameters = (target_kind.value, target_id)
            channel_filter = ""
            channel_parameters: tuple[str, ...] = ()
            if channels is not None:
                unique_channels = tuple(sorted(set(channels)))
                if not unique_channels:
                    channel_filter = " AND 1 = 0"
                else:
                    channel_filter = " AND channel IN (" + ",".join(
                        "?" for _ in unique_channels
                    ) + ")"
                    channel_parameters = unique_channels
            selected = connection.execute(
                f"""
                SELECT id FROM notification_outbox
                WHERE status IN (?, ?) AND next_attempt_at <= ?
                  AND attempt_count < ?
                  AND (expires_at IS NULL OR expires_at > ?)
                  {target_filter}
                  {channel_filter}
                ORDER BY next_attempt_at, id
                LIMIT ?
                """,
                (
                    OutboxStatus.PENDING.value,
                    OutboxStatus.RETRY.value,
                    serialized_now,
                    self._max_attempts,
                    serialized_now,
                    *target_parameters,
                    *channel_parameters,
                    limit,
                ),
            ).fetchall()
            ids = [int(row["id"]) for row in selected]
            if not ids:
                return ()
            placeholders = ",".join("?" for _ in ids)
            connection.execute(
                f"""
                UPDATE notification_outbox
                SET status = ?, attempt_count = attempt_count + 1, lease_until = ?
                WHERE id IN ({placeholders})
                """,
                (OutboxStatus.IN_FLIGHT.value, lease_until, *ids),
            )
            rows = connection.execute(
                f"SELECT * FROM notification_outbox WHERE id IN ({placeholders}) ORDER BY id",
                ids,
            ).fetchall()
        return tuple(_row_to_item(row) for row in rows)

    def mark_sent(
        self,
        item_id: int,
        *,
        provider_message_id: str | None = None,
        now: datetime | None = None,
    ) -> OutboxItem:
        instant = _normalize_time(now or datetime.now(UTC), "now")
        with self._write_transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE notification_outbox
                SET status = ?, sent_at = ?, provider_message_id = ?,
                    lease_until = NULL, last_error_code = NULL
                WHERE id = ? AND status = ?
                """,
                (
                    OutboxStatus.SENT.value,
                    _serialize_time(instant),
                    provider_message_id,
                    item_id,
                    OutboxStatus.IN_FLIGHT.value,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("outbox item is not in flight")
            row = connection.execute(
                "SELECT * FROM notification_outbox WHERE id = ?", (item_id,)
            ).fetchone()
        return _row_to_item(_require_row(row))

    def mark_failed(
        self,
        item_id: int,
        error_code: str,
        *,
        retryable: bool,
        now: datetime | None = None,
    ) -> OutboxItem:
        instant = _normalize_time(now or datetime.now(UTC), "now")
        safe_error_code = _sanitize_error_code(error_code)
        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM notification_outbox WHERE id = ?", (item_id,)
            ).fetchone()
            if row is None or row["status"] != OutboxStatus.IN_FLIGHT.value:
                raise ValueError("outbox item is not in flight")
            attempts = int(row["attempt_count"])
            expires_at = _parse_optional_time(row["expires_at"])

            if expires_at is not None and expires_at <= instant:
                status = OutboxStatus.EXPIRED
                next_attempt_at = instant
            elif not retryable or attempts >= self._max_attempts:
                status = OutboxStatus.DEAD
                next_attempt_at = instant
            else:
                delay_seconds = min(
                    self._base_backoff.total_seconds() * (2 ** (attempts - 1)),
                    self._max_backoff.total_seconds(),
                )
                next_attempt_at = instant + timedelta(seconds=delay_seconds)
                if expires_at is not None and next_attempt_at >= expires_at:
                    status = OutboxStatus.EXPIRED
                else:
                    status = OutboxStatus.RETRY

            connection.execute(
                """
                UPDATE notification_outbox
                SET status = ?, next_attempt_at = ?, lease_until = NULL,
                    last_error_code = ?
                WHERE id = ?
                """,
                (
                    status.value,
                    _serialize_time(next_attempt_at),
                    safe_error_code,
                    item_id,
                ),
            )
            updated = connection.execute(
                "SELECT * FROM notification_outbox WHERE id = ?", (item_id,)
            ).fetchone()
        return _row_to_item(_require_row(updated))

    def get_by_key(self, idempotency_key: str) -> OutboxItem | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT * FROM notification_outbox WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        return None if row is None else _row_to_item(row)

    def list_items(self, *, status: OutboxStatus | None = None) -> Sequence[OutboxItem]:
        with self._lock:
            self._ensure_open()
            if status is None:
                rows = self._connection.execute(
                    "SELECT * FROM notification_outbox ORDER BY id"
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT * FROM notification_outbox WHERE status = ? ORDER BY id",
                    (status.value,),
                ).fetchall()
        return tuple(_row_to_item(row) for row in rows)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()

    def __enter__(self) -> SQLiteOutbox:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
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
            raise RuntimeError("SQLite outbox is closed")


class OutboxDispatcher:
    """通过各渠道专用的通知端口投递一批已认领条目。"""

    def __init__(
        self,
        outbox: SQLiteOutbox,
        notifiers: Mapping[str, Notifier],
        *,
        target_kind: NotificationTargetKind | None = None,
        target_id: str | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if (target_kind is None) != (target_id is None):
            raise ValueError("target_kind and target_id must be configured together")
        if target_id is not None and not target_id.strip():
            raise ValueError("target_id must not be blank")
        self._outbox = outbox
        self._notifiers = dict(notifiers)
        self._target_kind = target_kind
        self._target_id = target_id
        self._channels = tuple(sorted(self._notifiers)) if target_kind is not None else None
        self._clock = clock

    async def run_once(
        self,
        *,
        limit: int = 50,
        lease_for: timedelta = timedelta(seconds=30),
    ) -> DispatchSummary:
        items = self._outbox.claim_due(
            now=self._clock(),
            limit=limit,
            lease_for=lease_for,
            target_kind=self._target_kind,
            target_id=self._target_id,
            channels=self._channels,
        )
        sent = retry_scheduled = dead = expired = 0
        for item in items:
            attempt_time = self._clock()
            notifier = self._notifiers.get(item.notification.channel)
            if (
                item.notification.expires_at is not None
                and item.notification.expires_at <= attempt_time
            ):
                result = self._outbox.mark_failed(
                    item.id,
                    "ttl_expired",
                    retryable=True,
                    now=attempt_time,
                )
            elif notifier is None:
                result = self._outbox.mark_failed(
                    item.id,
                    "notifier_not_configured",
                    retryable=False,
                    now=attempt_time,
                )
            else:
                try:
                    receipt = await notifier.send(item.notification)
                except NotificationDeliveryError as error:
                    result = self._outbox.mark_failed(
                        item.id,
                        error.code,
                        retryable=error.retryable,
                        now=self._clock(),
                    )
                except Exception:
                    # 适配器缺陷与临时依赖故障不得丢弃条目。仅持久化稳定
                    # 代码，绝不保存可能含消息内容的异常文本。
                    result = self._outbox.mark_failed(
                        item.id,
                        "unexpected_notifier_error",
                        retryable=True,
                        now=self._clock(),
                    )
                else:
                    self._outbox.mark_sent(
                        item.id,
                        provider_message_id=receipt.provider_message_id,
                        now=self._clock(),
                    )
                    sent += 1
                    continue

            if result.status is OutboxStatus.RETRY:
                retry_scheduled += 1
            elif result.status is OutboxStatus.EXPIRED:
                expired += 1
            else:
                dead += 1
        return DispatchSummary(
            claimed=len(items),
            sent=sent,
            retry_scheduled=retry_scheduled,
            dead=dead,
            expired=expired,
        )


def _row_to_item(row: sqlite3.Row) -> OutboxItem:
    notification = OutboundNotification(
        idempotency_key=str(row["idempotency_key"]),
        channel=str(row["channel"]),
        target_kind=NotificationTargetKind(str(row["target_kind"])),
        target_id=str(row["target_id"]),
        text=str(row["body"]),
        created_at=_parse_time(str(row["created_at"])),
        expires_at=_parse_optional_time(row["expires_at"]),
    )
    return OutboxItem(
        id=int(row["id"]),
        notification=notification,
        status=OutboxStatus(str(row["status"])),
        attempt_count=int(row["attempt_count"]),
        next_attempt_at=_parse_time(str(row["next_attempt_at"])),
        lease_until=_parse_optional_time(row["lease_until"]),
        last_error_code=row["last_error_code"],
        provider_message_id=row["provider_message_id"],
        sent_at=_parse_optional_time(row["sent_at"]),
    )


def _normalize_time(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _serialize_time(value: datetime) -> str:
    return _normalize_time(value, "datetime").isoformat(timespec="microseconds")


def _serialize_optional_time(value: datetime | None) -> str | None:
    return None if value is None else _serialize_time(value)


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return _normalize_time(parsed, "stored datetime")


def _parse_optional_time(value: object) -> datetime | None:
    return None if value is None else _parse_time(str(value))


def _require_row(row: sqlite3.Row | None) -> sqlite3.Row:
    if row is None:
        raise RuntimeError("SQLite outbox row disappeared")
    return row


_SAFE_ERROR_CODE = re.compile(r"^[a-z0-9_.-]{1,64}$")


def _sanitize_error_code(value: str) -> str:
    return value if _SAFE_ERROR_CODE.fullmatch(value) else "unclassified_error"
