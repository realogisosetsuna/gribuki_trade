"""实盘账本纯完整性边界的回归测试。"""

from datetime import UTC, datetime
from hashlib import sha256

import pytest

from gribuki_trade.domain.live_records import (
    LiveRecordEvent,
    LiveRecordEventType,
    NewLiveRecordEvent,
)
from gribuki_trade.storage.live_record_codec import _event_hash
from gribuki_trade.storage.live_record_errors import LiveRecordIntegrityError
from gribuki_trade.storage.live_record_integrity import (
    json_object,
    mapping,
    text,
    verify,
)

_NOW = datetime(2026, 9, 9, 6, 0, tzinfo=UTC)


def _stored_event(
    event_id: str,
    payload: str,
    previous_hash: str | None = None,
) -> LiveRecordEvent:
    candidate = NewLiveRecordEvent(
        event_id=event_id,
        account_id="live-main",
        event_type=LiveRecordEventType.PROTECTION_WORK_QUEUED,
        occurred_at=_NOW,
        idempotency_key=f"event:{event_id}",
        payload_json=payload,
    )
    digest = sha256(payload.encode("utf-8")).hexdigest()
    return LiveRecordEvent(
        sequence=1,
        event_id=event_id,
        account_id="live-main",
        event_type=candidate.event_type,
        occurred_at=_NOW,
        idempotency_key=candidate.idempotency_key,
        payload_json=payload,
        payload_sha256=digest,
        previous_hash=previous_hash,
        event_hash=_event_hash(candidate, digest, previous_hash),
    )


def test_legacy_json_helpers_validate_object_and_required_scalars() -> None:
    document = json_object('{"fill":{"command_id":"cmd-1"}}')
    fill = mapping(document, "fill")

    assert text(fill, "command_id") == "cmd-1"

    with pytest.raises(LiveRecordIntegrityError):
        json_object("[]")
    with pytest.raises(LiveRecordIntegrityError):
        mapping(document, "missing")
    with pytest.raises(LiveRecordIntegrityError):
        text({"command_id": ""}, "command_id")


def test_verify_accepts_valid_event_hash_chain_and_rejects_tampering() -> None:
    first = _stored_event("event-1", '{"value":1}')
    second_candidate = NewLiveRecordEvent(
        event_id="event-2",
        account_id="live-main",
        event_type=LiveRecordEventType.PROTECTION_WORK_QUEUED,
        occurred_at=_NOW,
        idempotency_key="event:event-2",
        payload_json='{"value":2}',
    )
    second_digest = sha256(second_candidate.payload_json.encode("utf-8")).hexdigest()
    second = LiveRecordEvent(
        sequence=2,
        event_id=second_candidate.event_id,
        account_id=second_candidate.account_id,
        event_type=second_candidate.event_type,
        occurred_at=second_candidate.occurred_at,
        idempotency_key=second_candidate.idempotency_key,
        payload_json=second_candidate.payload_json,
        payload_sha256=second_digest,
        previous_hash=first.event_hash,
        event_hash=_event_hash(second_candidate, second_digest, first.event_hash),
    )

    verify((first, second))

    tampered = LiveRecordEvent(
        sequence=2,
        event_id=second.event_id,
        account_id=second.account_id,
        event_type=second.event_type,
        occurred_at=second.occurred_at,
        idempotency_key=second.idempotency_key,
        payload_json='{"value":999}',
        payload_sha256=second.payload_sha256,
        previous_hash=second.previous_hash,
        event_hash=second.event_hash,
    )
    with pytest.raises(LiveRecordIntegrityError, match="hash chain"):
        verify((first, tampered))
