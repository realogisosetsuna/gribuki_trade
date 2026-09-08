"""实盘账本的纯哈希、标识和标量编码工具。

本模块不持有 SQLite 连接，也不执行事务；存储 facade 负责数据库边界。
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal

from gribuki_trade.domain.live_records import NewLiveRecordEvent

_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,95}$")

def _event_hash(
    event: NewLiveRecordEvent,
    payload_sha256: str,
    previous_hash: str | None,
) -> str:
    document = {
        "account_id": event.account_id,
        "event_id": event.event_id,
        "event_type": event.event_type.value,
        "idempotency_key": event.idempotency_key,
        "occurred_at": event.occurred_at.isoformat(),
        "payload_sha256": payload_sha256,
        "previous_hash": previous_hash,
    }
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _protection_id(account_id: str, command_id: str) -> str:
    return "live-protection-" + _digest_key(account_id, command_id)


def _protection_work_id(account_id: str, command_id: str) -> str:
    return "live-work-protect-" + _digest_key(account_id, command_id)


def _deep_protection_work_id(account_id: str, command_id: str) -> str:
    return "live-work-deep-" + _digest_key(account_id, command_id)


def _digest_key(*values: str) -> str:
    return hashlib.sha256("\0".join(values).encode("utf-8")).hexdigest()[:32]


def _canonical_json(document: Mapping[str, object]) -> str:
    return json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def _json_default(value: object) -> str:
    if isinstance(value, (datetime, Decimal)):
        return value.isoformat() if isinstance(value, datetime) else str(value)
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _error_code(value: str) -> str:
    normalized = str(value).strip().upper()
    if _ERROR_CODE.fullmatch(normalized) is None:
        raise ValueError("error/result code must be a stable uppercase identifier")
    return normalized


def _lease_attempt(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("lease_attempt must be a positive integer")
    return value


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("timestamp must be datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _time(value: datetime) -> str:
    return _aware_utc(value).isoformat(timespec="microseconds")


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)
