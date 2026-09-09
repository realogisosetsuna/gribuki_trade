"""实盘成交确认前的纯身份校验。

确认流程必须先证明命令索引、成交摘要、发送者和确认指纹属于同一条
两阶段消息链。本模块只比较已解码的值对象，不读取 SQLite，也不改变
任何账本状态；事务边界仍由 ``SQLiteLiveRecordStore`` 保持。
"""

from __future__ import annotations

from gribuki_trade.domain.live_records import ConfirmedLiveFill
from gribuki_trade.storage.live_record_errors import (
    LiveRecordIntegrityError,
    LiveRecordStateError,
)
from gribuki_trade.storage.live_record_models import StoredLiveCommand


def validate_confirmation(
    command: StoredLiveCommand,
    fill: ConfirmedLiveFill,
    sender_id: str,
    fingerprint: str,
) -> None:
    """验证确认消息仍指向原始命令和完整成交事实。"""

    if command.account_id != fill.account_id or command.fill_json != fill.canonical_json():
        raise LiveRecordIntegrityError("command index contains a different fill")
    if command.sender_id != sender_id:
        raise LiveRecordStateError("CONFIRMING_SENDER_MISMATCH")
    if command.fingerprint != fingerprint:
        raise LiveRecordStateError("CONFIRMATION_FINGERPRINT_MISMATCH")


__all__ = ["validate_confirmation"]
