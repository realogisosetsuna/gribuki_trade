"""实盘确认的跨连接并发、库存和幂等性测试。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier

import pytest

from gribuki_trade.services.live_trade_records import (
    LiveInboundOutcomeStatus,
    LiveTradeRecordError,
    LiveTradeRecordService,
    parse_onebot_private_message,
)
from gribuki_trade.storage.live_records import LiveRecordStateError, SQLiteLiveRecordStore

_NOW = datetime(2026, 8, 14, 6, 0, tzinfo=UTC)
_SENDER = "123456"
_SELF = "654321"


def _proposal(
    command_id: str,
    *,
    side: str,
    quantity: int,
    broker_id: str,
    fill_id: str | None = None,
) -> str:
    return (
        "GT-LIVE/1"
        f"|command_id={command_id}|account=live-main|side={side}"
        f"|symbol=600000.SH|quantity={quantity}|price=10.20|instrument=STOCK"
        "|executed_at=2026-08-14T13:59:00+08:00"
        "|commission=5.00|transfer_fee=0.10|stamp_tax=0.00"
        f"|external_order_id={broker_id}"
        f"|external_fill_id={fill_id or f'execution-{command_id}'}"
    )


def _message(message_id: int, text: str):
    return parse_onebot_private_message(
        {
            "post_type": "message",
            "message_type": "private",
            "sub_type": "friend",
            "self_id": int(_SELF),
            "user_id": int(_SENDER),
            "message_id": message_id,
            "time": int(_NOW.timestamp()),
            "raw_message": text,
        }
    )


def _service(store: SQLiteLiveRecordStore) -> LiveTradeRecordService:
    return LiveTradeRecordService(
        store,
        allowed_sender_ids=frozenset({_SENDER}),
        execution_session_validator=lambda _executed_at: True,
    )


def _confirm(
    service: LiveTradeRecordService,
    *,
    message_id: int,
    command_id: str,
    fingerprint: str,
):
    return service.ingest(
        _message(
            message_id,
            f"GT-LIVE-CONFIRM/1|command_id={command_id}|fingerprint={fingerprint}",
        ),
        received_at=_NOW,
    )


def test_concurrent_sells_cannot_oversell_recorded_position(tmp_path: Path) -> None:
    path = tmp_path / "live.sqlite3"
    with SQLiteLiveRecordStore(path) as first_store, SQLiteLiveRecordStore(path) as second_store:
        first = _service(first_store)
        second = _service(second_store)
        buy = first.ingest(
            _message(1, _proposal("buy-1", side="BUY", quantity=100, broker_id="b-1")),
            received_at=_NOW,
        )
        _confirm(
            first,
            message_id=2,
            command_id="buy-1",
            fingerprint=str(buy.fingerprint),
        )
        sell_a = first.ingest(
            _message(3, _proposal("sell-a", side="SELL", quantity=80, broker_id="s-a")),
            received_at=_NOW,
        )
        sell_b = second.ingest(
            _message(4, _proposal("sell-b", side="SELL", quantity=80, broker_id="s-b")),
            received_at=_NOW,
        )
        barrier = Barrier(2)

        def run(
            service: LiveTradeRecordService,
            message_id: int,
            command_id: str,
            fingerprint: str,
        ) -> str:
            barrier.wait()
            try:
                return _confirm(
                    service,
                    message_id=message_id,
                    command_id=command_id,
                    fingerprint=fingerprint,
                ).status.value
            except LiveTradeRecordError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = tuple(
                future.result()
                for future in (
                    pool.submit(run, first, 5, "sell-a", str(sell_a.fingerprint)),
                    pool.submit(run, second, 6, "sell-b", str(sell_b.fingerprint)),
                )
            )
        assert sorted(outcomes) == ["CONFIRMED", "RECORDED_POSITION_INSUFFICIENT"]
        assert first.snapshot("live-main").positions[0].quantity == 20


def test_concurrent_same_confirmation_is_written_once(tmp_path: Path) -> None:
    path = tmp_path / "live.sqlite3"
    with SQLiteLiveRecordStore(path) as first_store, SQLiteLiveRecordStore(path) as second_store:
        first = _service(first_store)
        second = _service(second_store)
        proposal = first.ingest(
            _message(1, _proposal("buy-1", side="BUY", quantity=100, broker_id="b-1")),
            received_at=_NOW,
        )
        barrier = Barrier(2)

        def run(service: LiveTradeRecordService, message_id: int) -> LiveInboundOutcomeStatus:
            barrier.wait()
            return _confirm(
                service,
                message_id=message_id,
                command_id="buy-1",
                fingerprint=str(proposal.fingerprint),
            ).status

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = {
                future.result()
                for future in (
                    pool.submit(run, first, 2),
                    pool.submit(run, second, 3),
                )
            }
        assert outcomes == {
            LiveInboundOutcomeStatus.CONFIRMED,
            LiveInboundOutcomeStatus.REPLAYED,
        }
        snapshot = first.snapshot("live-main")
        assert snapshot.confirmed_fill_count == 1
        assert snapshot.positions[0].quantity == 100
        assert len(first_store.tracking("live-main")) == 1


def test_concurrent_external_fill_identity_is_written_once(tmp_path: Path) -> None:
    path = tmp_path / "live.sqlite3"
    with SQLiteLiveRecordStore(path) as first_store, SQLiteLiveRecordStore(path) as second_store:
        first = _service(first_store)
        second = _service(second_store)
        proposal_a = first.ingest(
            _message(
                1,
                _proposal(
                    "buy-a",
                    side="BUY",
                    quantity=100,
                    broker_id="same-order",
                    fill_id="same-fill",
                ),
            ),
            received_at=_NOW,
        )
        proposal_b = second.ingest(
            _message(
                2,
                _proposal(
                    "buy-b",
                    side="BUY",
                    quantity=100,
                    broker_id="same-order",
                    fill_id="same-fill",
                ),
            ),
            received_at=_NOW,
        )
        barrier = Barrier(2)

        def run(
            service: LiveTradeRecordService,
            message_id: int,
            command_id: str,
            fingerprint: str,
        ) -> str:
            barrier.wait()
            try:
                return _confirm(
                    service,
                    message_id=message_id,
                    command_id=command_id,
                    fingerprint=fingerprint,
                ).status.value
            except LiveTradeRecordError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = tuple(
                future.result()
                for future in (
                    pool.submit(run, first, 3, "buy-a", str(proposal_a.fingerprint)),
                    pool.submit(run, second, 4, "buy-b", str(proposal_b.fingerprint)),
                )
            )
        assert sorted(outcomes) == ["CONFIRMED", "EXTERNAL_FILL_ALREADY_RECORDED"]
        assert first.snapshot("live-main").positions[0].quantity == 100
        assert len(first_store.tracking("live-main")) == 1


def test_concurrent_partial_fills_of_same_order_keep_distinct_execution_ids(
    tmp_path: Path,
) -> None:
    path = tmp_path / "live.sqlite3"
    with SQLiteLiveRecordStore(path) as first_store, SQLiteLiveRecordStore(path) as second_store:
        first = _service(first_store)
        second = _service(second_store)
        proposal_a = first.ingest(
            _message(
                1,
                _proposal(
                    "partial-a",
                    side="BUY",
                    quantity=100,
                    broker_id="shared-order",
                    fill_id="execution-a",
                ),
            ),
            received_at=_NOW,
        )
        proposal_b = second.ingest(
            _message(
                2,
                _proposal(
                    "partial-b",
                    side="BUY",
                    quantity=100,
                    broker_id="shared-order",
                    fill_id="execution-b",
                ),
            ),
            received_at=_NOW,
        )
        barrier = Barrier(2)

        def run(
            service: LiveTradeRecordService,
            message_id: int,
            command_id: str,
            fingerprint: str,
        ) -> LiveInboundOutcomeStatus:
            barrier.wait()
            return _confirm(
                service,
                message_id=message_id,
                command_id=command_id,
                fingerprint=fingerprint,
            ).status

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = {
                future.result()
                for future in (
                    pool.submit(
                        run,
                        first,
                        3,
                        "partial-a",
                        str(proposal_a.fingerprint),
                    ),
                    pool.submit(
                        run,
                        second,
                        4,
                        "partial-b",
                        str(proposal_b.fingerprint),
                    ),
                )
            }
        assert outcomes == {LiveInboundOutcomeStatus.CONFIRMED}
        assert first.snapshot("live-main").positions[0].quantity == 200
        assert len(first_store.tracking("live-main")) == 2


def test_confirmation_and_cancellation_have_one_transactional_winner(tmp_path: Path) -> None:
    path = tmp_path / "live.sqlite3"
    with SQLiteLiveRecordStore(path) as first_store, SQLiteLiveRecordStore(path) as second_store:
        first = _service(first_store)
        second = _service(second_store)
        proposal = first.ingest(
            _message(1, _proposal("race-1", side="BUY", quantity=100, broker_id="race-fill")),
            received_at=_NOW,
        )
        barrier = Barrier(2)

        def confirm() -> LiveInboundOutcomeStatus:
            barrier.wait()
            return _confirm(
                first,
                message_id=2,
                command_id="race-1",
                fingerprint=str(proposal.fingerprint),
            ).status

        def cancel() -> LiveInboundOutcomeStatus:
            barrier.wait()
            return second.ingest(
                _message(3, "GT-LIVE-CANCEL/1|command_id=race-1"),
                received_at=_NOW,
            ).status

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = {
                future.result()
                for future in (
                    pool.submit(confirm),
                    pool.submit(cancel),
                )
            }
        assert LiveInboundOutcomeStatus.REPLAYED in outcomes
        assert outcomes & {
            LiveInboundOutcomeStatus.CONFIRMED,
            LiveInboundOutcomeStatus.CANCELLED,
        }
        command = first_store.command("race-1")
        assert command is not None
        assert command.state in {"CONFIRMED", "CANCELLED"}
        assert first.snapshot("live-main").confirmed_fill_count in {0, 1}


def test_expired_work_lease_fences_every_terminal_write_from_old_worker(
    tmp_path: Path,
) -> None:
    path = tmp_path / "live.sqlite3"
    with SQLiteLiveRecordStore(path) as first_store, SQLiteLiveRecordStore(path) as second_store:
        service = _service(first_store)
        proposal = service.ingest(
            _message(1, _proposal("buy-fence", side="BUY", quantity=100, broker_id="b-fence")),
            received_at=_NOW,
        )
        _confirm(
            service,
            message_id=2,
            command_id="buy-fence",
            fingerprint=str(proposal.fingerprint),
        )
        old = first_store.claim_due_work(
            now=_NOW,
            lease_for=timedelta(seconds=1),
            limit=1,
        )[0]
        current = second_store.claim_due_work(
            now=_NOW + timedelta(seconds=2),
            lease_for=timedelta(minutes=5),
            limit=1,
        )[0]
        assert old.attempts == 1
        assert current.attempts == 2

        stale_writes = (
            lambda: first_store.renew_work_lease(
                old.work_id,
                lease_attempt=old.attempts,
                renewed_at=_NOW + timedelta(seconds=3),
            ),
            lambda: first_store.mark_quick_protection_ready(
                old.work_id,
                lease_attempt=old.attempts,
                ready_at=_NOW + timedelta(seconds=3),
                plan_id="stale-quick-plan",
            ),
            lambda: first_store.complete_work(
                old.work_id,
                lease_attempt=old.attempts,
                completed_at=_NOW + timedelta(seconds=3),
                result_code="STALE_COMPLETE",
            ),
            lambda: first_store.complete_protection_work(
                old.work_id,
                lease_attempt=old.attempts,
                completed_at=_NOW + timedelta(seconds=3),
                result_code="STALE_PROTECTION_COMPLETE",
                plan_id="stale-plan",
            ),
            lambda: first_store.fail_work(
                old.work_id,
                lease_attempt=old.attempts,
                failed_at=_NOW + timedelta(seconds=3),
                error_code="STALE_FAILURE",
                retryable=True,
            ),
        )
        for stale_write in stale_writes:
            with pytest.raises(LiveRecordStateError) as captured:
                stale_write()
            assert captured.value.code == "WORK_LEASE_FENCED"

        still_owned = second_store.work_items("live-main", include_terminal=True)[0]
        assert still_owned.status.value == "RUNNING"
        assert still_owned.attempts == current.attempts
        assert second_store.tracking("live-main")[0].plan_ready is False
        completed = second_store.complete_protection_work(
            current.work_id,
            lease_attempt=current.attempts,
            completed_at=_NOW + timedelta(seconds=4),
            result_code="CURRENT_PROTECTION_COMPLETE",
            plan_id="current-plan",
        )
        assert completed.status.value == "COMPLETED"
        assert second_store.tracking("live-main")[0].plan_ready is True
