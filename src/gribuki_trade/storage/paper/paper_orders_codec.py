"""模拟订单事件模型与确定性持久化 codec。

SQLite 存储 facade 负责 schema、事务、租约和恢复；本模块只负责不可变的事件/运行模型、
行解码、规范化 JSON 与哈希链 codec，使持久化边界可以独立审查。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any


class PaperOrderStoreError(RuntimeError):
    """持久化模拟订单故障的基类。"""


class PaperOrderStoreConflictError(PaperOrderStoreError):
    """幂等键或运行标识被用于不同内容。"""


class PaperOrderStoreLeaseError(PaperOrderStoreError):
    """尚未完成的运行正由存活写入者持有。"""


class PaperOrderStoreIntegrityError(PaperOrderStoreError):
    """不可变事件流未通过校验。"""


class PaperOrderEventType(StrEnum):
    ORDER_SUBMISSION_STARTED = "ORDER_SUBMISSION_STARTED"
    ORDER_STATE_APPLIED = "ORDER_STATE_APPLIED"
    RUN_STARTED = "RUN_STARTED"
    ORDER_FILL_APPLIED = "ORDER_FILL_APPLIED"
    RUN_COMPLETED = "RUN_COMPLETED"


@dataclass(frozen=True, slots=True)
class PaperOrderEvent:
    sequence: int
    event_id: str
    event_type: PaperOrderEventType
    stream_id: str
    run_id: str | None
    occurred_at: datetime
    idempotency_key: str
    payload_json: str
    payload_sha256: str
    previous_hash: str | None
    event_hash: str

    @property
    def payload(self) -> dict[str, Any]:
        value = json.loads(self.payload_json)
        if not isinstance(value, dict):
            raise PaperOrderStoreIntegrityError("paper-order event payload is not an object")
        return value


@dataclass(frozen=True, slots=True)
class PaperRunRecord:
    run_id: str
    symbol: str
    trade_date: date
    config_sha256: str
    run_fingerprint: str
    started_event_sequence: int
    completed: bool


def _row_to_event(row: sqlite3.Row) -> PaperOrderEvent:
    return PaperOrderEvent(
        sequence=int(row["sequence"]),
        event_id=str(row["event_id"]),
        event_type=PaperOrderEventType(str(row["event_type"])),
        stream_id=str(row["stream_id"]),
        run_id=str(row["run_id"]) if row["run_id"] is not None else None,
        occurred_at=datetime.fromisoformat(str(row["occurred_at"])),
        idempotency_key=str(row["idempotency_key"]),
        payload_json=str(row["payload_json"]),
        payload_sha256=str(row["payload_sha256"]),
        previous_hash=str(row["previous_hash"]) if row["previous_hash"] else None,
        event_hash=str(row["event_hash"]),
    )


def _run_record(row: sqlite3.Row, *, completed: bool) -> PaperRunRecord:
    return PaperRunRecord(
        run_id=str(row["run_id"]),
        symbol=str(row["symbol"]),
        trade_date=date.fromisoformat(str(row["trade_date"])),
        config_sha256=str(row["config_sha256"]),
        run_fingerprint=str(row["run_fingerprint"]),
        started_event_sequence=int(row["started_event_sequence"]),
        completed=completed,
    )


def _verify_chain(events: Sequence[PaperOrderEvent]) -> None:
    previous_hash: str | None = None
    for event in events:
        if _sha(event.payload_json) != event.payload_sha256:
            raise PaperOrderStoreIntegrityError(
                f"payload digest mismatch at paper-order event {event.sequence}"
            )
        if event.previous_hash != previous_hash:
            raise PaperOrderStoreIntegrityError(
                f"hash-chain mismatch at paper-order event {event.sequence}"
            )
        expected = _event_hash(
            event_id=event.event_id,
            event_type=event.event_type,
            stream_id=event.stream_id,
            run_id=event.run_id,
            occurred_at=event.occurred_at,
            idempotency_key=event.idempotency_key,
            payload_sha256=event.payload_sha256,
            previous_hash=event.previous_hash,
        )
        if expected != event.event_hash:
            raise PaperOrderStoreIntegrityError(
                f"event digest mismatch at paper-order event {event.sequence}"
            )
        previous_hash = event.event_hash


def _event_hash(
    *,
    event_id: str,
    event_type: PaperOrderEventType,
    stream_id: str,
    run_id: str | None,
    occurred_at: datetime,
    idempotency_key: str,
    payload_sha256: str,
    previous_hash: str | None,
) -> str:
    return _sha(
        _canonical_document(
            {
                "event_id": event_id,
                "event_type": event_type.value,
                "idempotency_key": idempotency_key,
                "occurred_at": occurred_at.isoformat(),
                "payload_sha256": payload_sha256,
                "previous_hash": previous_hash,
                "run_id": run_id,
                "stream_id": stream_id,
            }
        )
    )


def _canonical_document(value: Mapping[str, object]) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise ValueError("paper-order payload must be canonical JSON data") from error


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _identifier(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
