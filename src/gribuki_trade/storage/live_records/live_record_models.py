"""实盘观察账本的持久化模型与 SQLite 行投影。

本模块只负责把已经从 SQLite 读取的行转换为领域对象，并保存两个跨事务返回
模型。它不打开连接、不执行 SQL，也不决定账本状态迁移；这些边界仍由
``SQLiteLiveRecordStore`` 管理。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime

from gribuki_trade.domain.live_records import (
    LiveProtectionTracking,
    LiveRecordEvent,
    LiveRecordEventType,
    LiveWorkItem,
    LiveWorkKind,
    LiveWorkStatus,
)
from gribuki_trade.domain.paper_trading import PaperInstrumentType
from gribuki_trade.storage.live_records.live_record_codec import _parse_time


@dataclass(frozen=True, slots=True)
class StoredLiveCommand:
    """命令索引中的一条两阶段确认状态。"""

    command_id: str
    account_id: str
    sender_id: str
    source_message_id: str
    fingerprint: str
    fill_json: str
    state: str
    proposal_event_id: str
    terminal_event_id: str | None
    proposed_at: datetime
    terminal_at: datetime | None


@dataclass(frozen=True, slots=True)
class LiveConfirmationCommit:
    """原子确认结果及其持久保护工作标识。"""

    event: LiveRecordEvent
    created: bool
    protection_id: str | None
    protection_work_id: str | None


def row_to_event(row: sqlite3.Row) -> LiveRecordEvent:
    """将事件表行解码为不可变领域事件。"""

    return LiveRecordEvent(
        sequence=int(row["sequence"]),
        event_id=str(row["event_id"]),
        account_id=str(row["account_id"]),
        event_type=LiveRecordEventType(str(row["event_type"])),
        occurred_at=_parse_time(str(row["occurred_at"])),
        idempotency_key=str(row["idempotency_key"]),
        payload_json=str(row["payload_json"]),
        payload_sha256=str(row["payload_sha256"]),
        previous_hash=(str(row["previous_hash"]) if row["previous_hash"] else None),
        event_hash=str(row["event_hash"]),
    )


def row_to_command(row: sqlite3.Row) -> StoredLiveCommand:
    """将命令索引行解码为两阶段确认状态。"""

    return StoredLiveCommand(
        command_id=str(row["command_id"]),
        account_id=str(row["account_id"]),
        sender_id=str(row["sender_id"]),
        source_message_id=str(row["source_message_id"]),
        fingerprint=str(row["fingerprint"]),
        fill_json=str(row["fill_json"]),
        state=str(row["state"]),
        proposal_event_id=str(row["proposal_event_id"]),
        terminal_event_id=(str(row["terminal_event_id"]) if row["terminal_event_id"] else None),
        proposed_at=_parse_time(str(row["proposed_at"])),
        terminal_at=(None if row["terminal_at"] is None else _parse_time(str(row["terminal_at"]))),
    )


def row_to_tracking(row: sqlite3.Row) -> LiveProtectionTracking:
    """将保护跟踪投影行解码为领域对象。"""

    return LiveProtectionTracking(
        protection_id=str(row["protection_id"]),
        account_id=str(row["account_id"]),
        buy_command_id=str(row["buy_command_id"]),
        symbol=str(row["symbol"]),
        instrument_type=PaperInstrumentType(str(row["instrument_type"])),
        acquired_at=_parse_time(str(row["acquired_at"])),
        original_quantity=int(row["original_quantity"]),
        remaining_quantity=int(row["remaining_quantity"]),
        plan_ready=bool(row["plan_ready"]),
        plan_stream_id=(None if row["plan_stream_id"] is None else str(row["plan_stream_id"])),
        last_observed_bar_end=(
            None
            if row["last_observed_bar_end"] is None
            else _parse_time(str(row["last_observed_bar_end"]))
        ),
        last_alert_bar_end=(
            None
            if row["last_alert_bar_end"] is None
            else _parse_time(str(row["last_alert_bar_end"]))
        ),
    )


def row_to_work(row: sqlite3.Row) -> LiveWorkItem:
    """将可恢复工作队列行解码为领域对象。"""

    return LiveWorkItem(
        work_id=str(row["work_id"]),
        kind=LiveWorkKind(str(row["kind"])),
        account_id=str(row["account_id"]),
        command_id=str(row["command_id"]),
        protection_id=(str(row["protection_id"]) if row["protection_id"] else None),
        payload_json=str(row["payload_json"]),
        status=LiveWorkStatus(str(row["status"])),
        attempts=int(row["attempts"]),
        available_at=_parse_time(str(row["available_at"])),
        lease_until=(None if row["lease_until"] is None else _parse_time(str(row["lease_until"]))),
        created_at=_parse_time(str(row["created_at"])),
        updated_at=_parse_time(str(row["updated_at"])),
        result_code=(str(row["result_code"]) if row["result_code"] else None),
        error_code=(str(row["error_code"]) if row["error_code"] else None),
    )


# facade 别名刻意沿用历史私有标识符，保证旧调用路径不变。
_row_to_event = row_to_event
_row_to_command = row_to_command
_row_to_tracking = row_to_tracking
_row_to_work = row_to_work


__all__ = [
    "LiveConfirmationCommit",
    "StoredLiveCommand",
    "row_to_command",
    "row_to_event",
    "row_to_tracking",
    "row_to_work",
]
