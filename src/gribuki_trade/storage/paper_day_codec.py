"""PAPER-day SQLite 行编解码与确定性值辅助函数。

本模块不持有数据库连接，也不执行事务。把行解码、摘要构造和参数校验
集中到这里，可以缩小持久化存储的有状态边界，同时保持既有数据库使用的
字节表示和哈希链格式不变。
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Mapping
from datetime import datetime, timedelta
from decimal import Decimal

from gribuki_trade.domain.paper_day import (
    NewPaperDayEvent,
    PaperDayEvent,
    PaperDayPhase,
    PaperDayRunManifest,
    PaperDaySeverity,
    paper_day_canonical_json,
)


def row_to_manifest(row: sqlite3.Row) -> PaperDayRunManifest:
    """把 ``paper_day_runs`` 行解码为领域清单，不接触存储状态。"""

    from datetime import date

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


def row_to_event(row: sqlite3.Row) -> PaperDayEvent:
    """把 ``paper_day_events`` 行解码为领域事件，不更新任何投影。"""

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


def same_event_content(stored: PaperDayEvent, new: NewPaperDayEvent) -> bool:
    """判断幂等事件键对应的内容是否完全一致。"""

    return (
        stored.event_id == new.event_id
        and stored.event_type == new.event_type
        and stored.phase is new.phase
        and stored.severity is new.severity
        and stored.occurred_at == new.occurred_at
        and stored.known_at == new.known_at
        and stored.notification_required == new.notification_required
        and stored.symbol == new.symbol
        and stored.correlation_id == new.correlation_id
        and stored.payload_json == new.payload_json
    )


def event_hash(
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
    """构造一条持久化事件的稳定哈希链摘要。"""

    document: Mapping[str, object] = {
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
    return sha256_text(paper_day_canonical_json(document))


def validate_identifier(value: str, field_name: str) -> str:
    """规范化有长度上限的存储标识，同时保留原有错误类型。"""

    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized or len(normalized) > 128:
        raise ValueError(f"{field_name} must be a non-empty safe identifier")
    return normalized


def lease_duration(
    lease_for: timedelta | None,
    lease_duration_value: timedelta | None,
) -> timedelta:
    """解析兼容旧调用方式的租约时长参数。"""

    if (lease_for is None) == (lease_duration_value is None):
        raise ValueError("provide exactly one lease duration")
    value = lease_for if lease_for is not None else lease_duration_value
    if not isinstance(value, timedelta):
        raise TypeError("lease_duration must be a timedelta")
    if value <= timedelta(0):
        raise ValueError("lease_duration must be positive")
    return value


def sha256_text(value: str) -> str:
    """返回存储使用的小写 SHA-256 摘要。"""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


__all__ = [
    "event_hash",
    "lease_duration",
    "row_to_event",
    "row_to_manifest",
    "same_event_content",
    "sha256_text",
    "validate_identifier",
]
