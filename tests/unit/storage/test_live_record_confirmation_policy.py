"""实盘成交确认身份策略的纯函数测试。"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from gribuki_trade.domain.live_records import ConfirmedLiveFill
from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import PaperFillFees, PaperInstrumentType
from gribuki_trade.storage.live_records.live_record_confirmation_policy import validate_confirmation
from gribuki_trade.storage.live_records.live_record_errors import (
    LiveRecordIntegrityError,
    LiveRecordStateError,
)
from gribuki_trade.storage.live_records.live_record_models import StoredLiveCommand

_NOW = datetime(2026, 9, 9, 6, 0, tzinfo=UTC)


def _fill() -> ConfirmedLiveFill:
    return ConfirmedLiveFill(
        command_id="command-1",
        account_id="live-main",
        side=Side.BUY,
        symbol="600000.SH",
        quantity=100,
        price=Decimal("10.20"),
        instrument_type=PaperInstrumentType.STOCK,
        executed_at=_NOW,
        fees=PaperFillFees(
            commission=Decimal("1"),
            stamp_tax=Decimal("0"),
            transfer_fee=Decimal("0"),
        ),
    )


def _command(fill: ConfirmedLiveFill) -> StoredLiveCommand:
    return StoredLiveCommand(
        command_id=fill.command_id,
        account_id=fill.account_id,
        sender_id="sender-1",
        source_message_id="message-1",
        fingerprint=fill.fingerprint,
        fill_json=fill.canonical_json(),
        state="PENDING",
        proposal_event_id="proposal-1",
        terminal_event_id=None,
        proposed_at=_NOW,
        terminal_at=None,
    )


def test_confirmation_policy_accepts_matching_identity() -> None:
    fill = _fill()

    validate_confirmation(_command(fill), fill, "sender-1", fill.fingerprint)


def test_confirmation_policy_rejects_changed_fill_as_integrity_failure() -> None:
    original = _fill()
    changed = ConfirmedLiveFill(
        command_id=original.command_id,
        account_id=original.account_id,
        side=original.side,
        symbol=original.symbol,
        quantity=101,
        price=original.price,
        instrument_type=original.instrument_type,
        executed_at=original.executed_at,
        fees=original.fees,
    )

    with pytest.raises(LiveRecordIntegrityError, match="different fill"):
        validate_confirmation(_command(original), changed, "sender-1", changed.fingerprint)


def test_confirmation_policy_fences_sender_and_fingerprint() -> None:
    fill = _fill()
    command = _command(fill)

    with pytest.raises(LiveRecordStateError, match="CONFIRMING_SENDER_MISMATCH"):
        validate_confirmation(command, fill, "other-sender", fill.fingerprint)
    with pytest.raises(LiveRecordStateError, match="CONFIRMATION_FINGERPRINT_MISMATCH"):
        validate_confirmation(command, fill, "sender-1", "wrong-fingerprint")
