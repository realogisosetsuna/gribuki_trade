"""实盘账本事件完整性和旧账本 JSON 解析。

本模块只处理已经读取到内存的事件或 JSON 文档，不打开 SQLite 连接，也不
执行事务。事件哈希链校验保持严格失败关闭；旧账本恢复只允许明确的提案
事件，遇到无法无歧义重建的成交事实时由存储 facade 中止迁移。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import cast

from gribuki_trade.domain.live_records import LiveRecordEvent, NewLiveRecordEvent
from gribuki_trade.storage.live_record_codec import _event_hash
from gribuki_trade.storage.live_record_errors import LiveRecordIntegrityError


def json_object(payload: str) -> dict[str, object]:
    """解码并验证账本事件中的 JSON 对象。"""

    value = json.loads(payload)
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise LiveRecordIntegrityError("stored live-record payload is invalid")
    return cast(dict[str, object], value)


def mapping(document: Mapping[str, object], name: str) -> dict[str, object]:
    """读取旧账本恢复所需的嵌套对象。"""

    value = document.get(name)
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise LiveRecordIntegrityError("stored live-record mapping is invalid")
    return cast(dict[str, object], value)


def text(document: Mapping[str, object], name: str) -> str:
    """读取旧账本恢复所需的非空文本字段。"""

    value = document.get(name)
    if not isinstance(value, str) or not value:
        raise LiveRecordIntegrityError("stored live-record text is invalid")
    return value


def verify(events: Sequence[LiveRecordEvent]) -> None:
    """验证一组按序排列事件的载荷摘要和链式事件哈希。"""

    previous_hash: str | None = None
    for event in events:
        digest = hashlib.sha256(event.payload_json.encode("utf-8")).hexdigest()
        if digest != event.payload_sha256 or event.previous_hash != previous_hash:
            raise LiveRecordIntegrityError("live-record hash chain is invalid")
        candidate = NewLiveRecordEvent(
            event_id=event.event_id,
            account_id=event.account_id,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            idempotency_key=event.idempotency_key,
            payload_json=event.payload_json,
        )
        if _event_hash(candidate, digest, previous_hash) != event.event_hash:
            raise LiveRecordIntegrityError("live-record event hash is invalid")
        previous_hash = event.event_hash


# facade 兼容别名：历史调用方使用私有名称，迁移期间继续稳定可用。
_json_object = json_object
_mapping = mapping
_text = text
_verify = verify


__all__ = ["json_object", "mapping", "text", "verify"]
