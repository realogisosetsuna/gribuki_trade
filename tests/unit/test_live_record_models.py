"""实盘账本 SQLite 行模型的纯解码契约。"""

from __future__ import annotations

import sqlite3

from gribuki_trade.domain.live_records import LiveRecordEventType, LiveWorkKind, LiveWorkStatus
from gribuki_trade.domain.paper_trading import PaperInstrumentType
from gribuki_trade.storage.live_record_models import (
    row_to_command,
    row_to_event,
    row_to_tracking,
    row_to_work,
)


def _row(columns: str, values: tuple[object, ...]) -> sqlite3.Row:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    return connection.execute(f"SELECT {columns}", values).fetchone()


def test_event_row_decoder_preserves_hash_chain_fields() -> None:
    row = _row(
        "1 AS sequence, ? AS event_id, ? AS account_id, ? AS event_type, "
        "? AS occurred_at, ? AS idempotency_key, ? AS payload_json, "
        "? AS payload_sha256, ? AS previous_hash, ? AS event_hash",
        (
            "event-1",
            "live-main",
            LiveRecordEventType.COMMAND_PROPOSED.value,
            "2026-08-14T06:00:00+00:00",
            "proposal:1",
            "{}",
            "sha256",
            None,
            "event-hash",
        ),
    )
    event = row_to_event(row)
    assert event.sequence == 1
    assert event.event_type is LiveRecordEventType.COMMAND_PROPOSED
    assert event.previous_hash is None
    assert event.payload_json == "{}"


def test_command_tracking_and_work_decoders_normalize_storage_scalars() -> None:
    command = row_to_command(
        _row(
            "? AS command_id, ? AS account_id, ? AS sender_id, ? AS source_message_id, "
            "? AS fingerprint, ? AS fill_json, ? AS state, ? AS proposal_event_id, "
            "? AS terminal_event_id, ? AS proposed_at, ? AS terminal_at",
            (
                "cmd-1",
                "live-main",
                "sender-1",
                "message-1",
                "fp",
                "{}",
                "PENDING",
                "event-1",
                None,
                "2026-08-14T06:00:00+00:00",
                None,
            ),
        )
    )
    tracking = row_to_tracking(
        _row(
            "? AS protection_id, ? AS account_id, ? AS buy_command_id, ? AS symbol, "
            "? AS instrument_type, ? AS acquired_at, 100 AS original_quantity, "
            "40 AS remaining_quantity, 1 AS plan_ready, ? AS plan_stream_id, "
            "? AS last_observed_bar_end, ? AS last_alert_bar_end",
            (
                "protect-1",
                "live-main",
                "cmd-1",
                "600000.SH",
                PaperInstrumentType.STOCK.value,
                "2026-08-14T06:00:00+00:00",
                "stream-1",
                "2026-08-14T07:00:00+00:00",
                None,
            ),
        )
    )
    work = row_to_work(
        _row(
            "? AS work_id, ? AS kind, ? AS account_id, ? AS command_id, ? AS protection_id, "
            "? AS payload_json, ? AS status, 2 AS attempts, ? AS available_at, "
            "? AS lease_until, ? AS created_at, ? AS updated_at, ? AS result_code, ? AS error_code",
            (
                "work-1",
                LiveWorkKind.BUILD_PROTECTION.value,
                "live-main",
                "cmd-1",
                "protect-1",
                "{}",
                LiveWorkStatus.RETRY.value,
                "2026-08-14T06:00:00+00:00",
                None,
                "2026-08-14T06:00:00+00:00",
                "2026-08-14T06:01:00+00:00",
                "TRANSIENT",
                None,
            ),
        )
    )
    assert command.command_id == "cmd-1"
    assert command.terminal_at is None
    assert tracking.plan_ready is True
    assert tracking.remaining_quantity == 40
    assert work.kind is LiveWorkKind.BUILD_PROTECTION
    assert work.status is LiveWorkStatus.RETRY
    assert work.attempts == 2
