"""严格 QQ 入站解析与隔离实盘观察账本测试。"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from gribuki_trade.reporting.contracts import ReportKind, validate_text_report_contract
from gribuki_trade.services.live_trade_records import (
    LiveInboundOutcomeStatus,
    LiveTradeRecordError,
    LiveTradeRecordService,
    parse_live_inbound_command,
    parse_onebot_private_message,
)
from gribuki_trade.storage.live_records import SQLiteLiveRecordStore

_SENDER = "123456"
_SELF = "654321"
_NOW = datetime(2026, 8, 14, 6, 0, tzinfo=UTC)


def _proposal(
    command_id: str = "trade-001",
    *,
    side: str = "BUY",
    quantity: int = 100,
    external_order_id: str | None = None,
    external_fill_id: str | None = None,
    executed_at: str = "2026-08-14T13:59:00+08:00",
) -> str:
    broker_id = external_order_id or f"broker-{command_id}"
    fill_id = external_fill_id or f"execution-{command_id}"
    return (
        "GT-LIVE/1"
        f"|command_id={command_id}|account=live-main|side={side}"
        f"|symbol=600000.SH|quantity={quantity}|price=10.20|instrument=STOCK"
        f"|executed_at={executed_at}"
        "|commission=5.00|transfer_fee=0.10|stamp_tax=0.00"
        f"|external_order_id={broker_id}"
        f"|external_fill_id={fill_id}"
    )


def _message(message_id: int, text: str, *, sender: str = _SENDER):
    return parse_onebot_private_message(
        {
            "post_type": "message",
            "message_type": "private",
            "sub_type": "friend",
            "self_id": int(_SELF),
            "user_id": int(sender),
            "message_id": message_id,
            "time": int(_NOW.timestamp()),
            "raw_message": text,
        }
    )


def _service(path: Path) -> tuple[SQLiteLiveRecordStore, LiveTradeRecordService]:
    store = SQLiteLiveRecordStore(path)
    return store, LiveTradeRecordService(
        store,
        allowed_sender_ids=frozenset({_SENDER}),
        execution_session_validator=lambda _executed_at: True,
    )


def test_proposal_requires_second_message_before_position_changes(tmp_path) -> None:
    store, service = _service(tmp_path / "live.sqlite3")
    with store:
        proposed = service.ingest(_message(1, _proposal()), received_at=_NOW)
        snapshot = service.snapshot("live-main")

        assert proposed.status is LiveInboundOutcomeStatus.PROPOSED
        assert proposed.fingerprint is not None
        assert proposed.analysis_required is False
        validate_text_report_contract(ReportKind.EXECUTION_RECEIPT, proposed.response_text)
        assert snapshot.confirmed_fill_count == 0
        assert snapshot.positions == ()

        confirmation = f"GT-LIVE-CONFIRM/1|command_id=trade-001|fingerprint={proposed.fingerprint}"
        confirmed = service.ingest(_message(2, confirmation), received_at=_NOW)
        snapshot = service.snapshot("live-main")

        assert confirmed.status is LiveInboundOutcomeStatus.CONFIRMED
        assert confirmed.analysis_required is True
        assert confirmed.protection_work_id is not None
        assert "成交事务提交时仅表示任务可恢复" in confirmed.response_text
        validate_text_report_contract(ReportKind.EXECUTION_RECEIPT, confirmed.response_text)
        assert snapshot.confirmed_fill_count == 1
        assert snapshot.positions[0].quantity == 100
        assert snapshot.positions[0].average_cost == Decimal("10.251")


def test_wrong_fingerprint_and_different_sender_fail_closed(tmp_path) -> None:
    store, service = _service(tmp_path / "live.sqlite3")
    with store:
        service.ingest(_message(1, _proposal()), received_at=_NOW)
        with pytest.raises(LiveTradeRecordError, match="SENDER_NOT_ALLOWED"):
            service.ingest(_message(2, _proposal("other"), sender="999999"), received_at=_NOW)
        with pytest.raises(LiveTradeRecordError, match="CONFIRMATION_FINGERPRINT_MISMATCH"):
            service.ingest(
                _message(
                    3,
                    "GT-LIVE-CONFIRM/1|command_id=trade-001|fingerprint=000000000000000000000000",
                ),
                received_at=_NOW,
            )
        assert service.snapshot("live-main").confirmed_fill_count == 0


def test_confirmed_sell_updates_only_live_observation_ledger(tmp_path) -> None:
    store, service = _service(tmp_path / "live.sqlite3")
    with store:
        buy = service.ingest(_message(1, _proposal()), received_at=_NOW)
        service.ingest(
            _message(
                2,
                f"GT-LIVE-CONFIRM/1|command_id=trade-001|fingerprint={buy.fingerprint}",
            ),
            received_at=_NOW,
        )
        sell = service.ingest(
            _message(3, _proposal("trade-002", side="SELL", quantity=40)),
            received_at=_NOW,
        )
        outcome = service.ingest(
            _message(
                4,
                f"GT-LIVE-CONFIRM/1|command_id=trade-002|fingerprint={sell.fingerprint}",
            ),
            received_at=_NOW,
        )
        snapshot = service.snapshot("live-main")

        assert outcome.analysis_required is False
        assert snapshot.positions[0].quantity == 60
        assert snapshot.confirmed_fill_count == 2
        assert not (tmp_path / "ledger.sqlite3").exists()


def test_sell_without_recorded_position_is_rejected(tmp_path) -> None:
    store, service = _service(tmp_path / "live.sqlite3")
    with store:
        proposed = service.ingest(
            _message(1, _proposal(side="SELL")),
            received_at=_NOW,
        )
        with pytest.raises(LiveTradeRecordError, match="RECORDED_POSITION_INSUFFICIENT"):
            service.ingest(
                _message(
                    2,
                    f"GT-LIVE-CONFIRM/1|command_id=trade-001|fingerprint={proposed.fingerprint}",
                ),
                received_at=_NOW,
            )


def test_command_grammar_rejects_unknown_or_duplicate_fields() -> None:
    with pytest.raises(ValueError):
        parse_live_inbound_command(_proposal() + "|price=11")
    with pytest.raises(ValueError):
        parse_live_inbound_command(_proposal() + "|unknown=value")
    with pytest.raises(ValueError):
        parse_onebot_private_message(
            {
                "post_type": "message",
                "message_type": "group",
                "sub_type": "normal",
                "self_id": 1,
                "user_id": 2,
                "message_id": 3,
                "time": int(_NOW.timestamp()),
                "raw_message": _proposal(),
            }
        )


def test_execution_time_must_be_past_and_inside_a_share_session(tmp_path) -> None:
    store, service = _service(tmp_path / "live.sqlite3")
    with store:
        with pytest.raises(LiveTradeRecordError, match="EXECUTION_FROM_FUTURE"):
            service.ingest(
                _message(1, _proposal(executed_at="2026-08-14T14:01:00+08:00")),
                received_at=_NOW,
            )
        with pytest.raises(
            LiveTradeRecordError,
            match="EXECUTION_OUTSIDE_A_SHARE_SESSION",
        ):
            service.ingest(
                _message(2, _proposal(executed_at="2026-08-14T12:00:00+08:00")),
                received_at=_NOW,
            )


def test_verified_trading_session_is_mandatory_and_fail_closed(tmp_path) -> None:
    store = SQLiteLiveRecordStore(tmp_path / "live.sqlite3")
    with store:
        with pytest.raises(ValueError, match="verified A-share execution-session"):
            LiveTradeRecordService(store, allowed_sender_ids=frozenset({_SENDER}))
        service = LiveTradeRecordService(
            store,
            allowed_sender_ids=frozenset({_SENDER}),
            execution_session_validator=lambda _executed_at: False,
        )
        with pytest.raises(LiveTradeRecordError, match="EXECUTION_NOT_ON_TRADING_SESSION"):
            service.ingest(_message(1, _proposal()), received_at=_NOW)


@pytest.mark.parametrize(
    ("removed_field", "expected_code"),
    [
        ("|external_order_id=broker-trade-001", "EXTERNAL_ORDER_ID_REQUIRED"),
        ("|external_fill_id=execution-trade-001", "EXTERNAL_FILL_ID_REQUIRED"),
    ],
)
def test_new_proposal_requires_broker_identities_before_calendar_access(
    tmp_path: Path,
    removed_field: str,
    expected_code: str,
) -> None:
    store = SQLiteLiveRecordStore(tmp_path / "live.sqlite3")
    calendar_calls = 0

    def calendar(_executed_at: datetime) -> bool:
        nonlocal calendar_calls
        calendar_calls += 1
        return True

    service = LiveTradeRecordService(
        store,
        allowed_sender_ids=frozenset({_SENDER}),
        execution_session_validator=calendar,
    )
    proposal = _proposal().replace(removed_field, "")
    with store, pytest.raises(LiveTradeRecordError, match=expected_code):
        service.ingest(_message(1, proposal), received_at=_NOW)
    assert calendar_calls == 0


@pytest.mark.parametrize("calendar_result", [None, 1, "true"])
def test_invalid_calendar_validator_result_fails_closed(
    tmp_path: Path,
    calendar_result: object,
) -> None:
    store = SQLiteLiveRecordStore(tmp_path / "live.sqlite3")
    service = LiveTradeRecordService(
        store,
        allowed_sender_ids=frozenset({_SENDER}),
        execution_session_validator=lambda _executed_at: calendar_result,  # type: ignore[arg-type]
    )
    with store, pytest.raises(
        LiveTradeRecordError,
        match="LIVE_TRADING_CALENDAR_UNAVAILABLE",
    ):
        service.ingest(_message(1, _proposal()), received_at=_NOW)


def test_calendar_provider_failure_is_sanitized_and_fails_closed(tmp_path) -> None:
    store = SQLiteLiveRecordStore(tmp_path / "live.sqlite3")

    def unavailable(_executed_at: datetime) -> bool:
        raise RuntimeError("provider response must not escape")

    service = LiveTradeRecordService(
        store,
        allowed_sender_ids=frozenset({_SENDER}),
        execution_session_validator=unavailable,
    )
    with store, pytest.raises(LiveTradeRecordError) as captured:
        service.ingest(_message(1, _proposal()), received_at=_NOW)
    assert captured.value.code == "LIVE_TRADING_CALENDAR_UNAVAILABLE"
    assert "provider response" not in str(captured.value)


def test_received_at_must_be_timezone_aware(tmp_path) -> None:
    store, service = _service(tmp_path / "live.sqlite3")
    with store, pytest.raises(ValueError, match="timezone-aware"):
        service.ingest(
            _message(1, _proposal()),
            received_at=datetime(2026, 8, 14, 6, 0),
        )


def test_external_execution_id_is_deduplicated_across_command_ids(tmp_path) -> None:
    store, service = _service(tmp_path / "live.sqlite3")
    with store:
        first = service.ingest(
            _message(
                1,
                _proposal(
                    external_order_id="broker-same",
                    external_fill_id="execution-same",
                ),
            ),
            received_at=_NOW,
        )
        service.ingest(
            _message(
                2,
                f"GT-LIVE-CONFIRM/1|command_id=trade-001|fingerprint={first.fingerprint}",
            ),
            received_at=_NOW,
        )
        second = service.ingest(
            _message(
                3,
                _proposal(
                    "trade-002",
                    quantity=200,
                    external_order_id="broker-same",
                    external_fill_id="execution-same",
                    executed_at="2026-08-14T13:59:30+08:00",
                ),
            ),
            received_at=_NOW,
        )
        with pytest.raises(LiveTradeRecordError, match="EXTERNAL_FILL_ALREADY_RECORDED"):
            service.ingest(
                _message(
                    4,
                    f"GT-LIVE-CONFIRM/1|command_id=trade-002|fingerprint={second.fingerprint}",
                ),
                received_at=_NOW,
            )


def test_partial_fills_of_one_broker_order_use_execution_ids(tmp_path) -> None:
    store, service = _service(tmp_path / "live.sqlite3")
    with store:
        first = service.ingest(
            _message(
                1,
                _proposal(
                    "partial-001",
                    quantity=100,
                    external_order_id="broker-order-one",
                    external_fill_id="broker-execution-one",
                ),
            ),
            received_at=_NOW,
        )
        service.ingest(
            _message(
                2,
                "GT-LIVE-CONFIRM/1|command_id=partial-001|"
                f"fingerprint={first.fingerprint}",
            ),
            received_at=_NOW,
        )
        second = service.ingest(
            _message(
                3,
                _proposal(
                    "partial-002",
                    quantity=100,
                    external_order_id="broker-order-one",
                    external_fill_id="broker-execution-two",
                ),
            ),
            received_at=_NOW,
        )
        service.ingest(
            _message(
                4,
                "GT-LIVE-CONFIRM/1|command_id=partial-002|"
                f"fingerprint={second.fingerprint}",
            ),
            received_at=_NOW,
        )

        snapshot = service.snapshot("live-main")
        assert snapshot.confirmed_fill_count == 2
        assert snapshot.positions[0].quantity == 200


def test_command_grammar_rejects_noncanonical_whitespace() -> None:
    with pytest.raises(ValueError):
        parse_live_inbound_command(" " + _proposal())
    with pytest.raises(ValueError):
        parse_live_inbound_command(_proposal().replace("|price=", "| price="))


def test_cancel_and_terminal_replay_responses_follow_execution_contract(tmp_path) -> None:
    store, service = _service(tmp_path / "live.sqlite3")
    with store:
        proposed = service.ingest(_message(1, _proposal()), received_at=_NOW)
        cancelled = service.ingest(
            _message(2, "GT-LIVE-CANCEL/1|command_id=trade-001"),
            received_at=_NOW,
        )
        validate_text_report_contract(
            ReportKind.EXECUTION_RECEIPT,
            cancelled.response_text,
        )
        replay = service.ingest(
            _message(3, "GT-LIVE-CANCEL/1|command_id=trade-001"),
            received_at=_NOW,
        )
        validate_text_report_contract(ReportKind.EXECUTION_RECEIPT, replay.response_text)
        assert proposed.fingerprint is not None
        assert replay.status is LiveInboundOutcomeStatus.REPLAYED


def test_confirmed_terminal_replay_response_follows_execution_contract(tmp_path) -> None:
    store, service = _service(tmp_path / "live.sqlite3")
    with store:
        proposed = service.ingest(_message(1, _proposal()), received_at=_NOW)
        command = f"GT-LIVE-CONFIRM/1|command_id=trade-001|fingerprint={proposed.fingerprint}"
        confirmed = service.ingest(_message(2, command), received_at=_NOW)
        replay = service.ingest(_message(3, command), received_at=_NOW)
        validate_text_report_contract(ReportKind.EXECUTION_RECEIPT, replay.response_text)
        assert replay.status is LiveInboundOutcomeStatus.REPLAYED
        assert replay.protection_id == confirmed.protection_id
        assert replay.protection_work_id == confirmed.protection_work_id

        proposal_replay = service.ingest(_message(1, _proposal()), received_at=_NOW)
        validate_text_report_contract(
            ReportKind.EXECUTION_RECEIPT,
            proposal_replay.response_text,
        )
        assert proposal_replay.status is LiveInboundOutcomeStatus.REPLAYED
        assert "已经确认" in proposal_replay.response_text
        assert proposal_replay.event_sequence == replay.event_sequence


def test_mutation_rejects_a_tampered_hash_chain_with_stable_error(tmp_path) -> None:
    path = tmp_path / "live.sqlite3"
    store, service = _service(path)
    with store:
        service.ingest(_message(1, _proposal()), received_at=_NOW)

    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP TRIGGER live_record_no_update")
        connection.execute(
            "UPDATE live_record_events SET payload_json = ? WHERE account_id = ?",
            ('{"tampered":true}', "live-main"),
        )
        connection.commit()
    finally:
        connection.close()

    store, service = _service(path)
    with (
        store,
        pytest.raises(
            LiveTradeRecordError,
            match="LIVE_LEDGER_INTEGRITY_FAILURE",
        ),
    ):
        service.ingest(
            _message(2, _proposal("trade-002")),
            received_at=_NOW,
        )
