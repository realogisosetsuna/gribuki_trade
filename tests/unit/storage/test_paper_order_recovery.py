from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.domain.orders import OrderIntent, Side
from gribuki_trade.domain.paper_orders import (
    ASharePaperOrderIntent,
    ExplicitPriceBand,
    PaperOrderStatus,
    SimulatedDailyBar,
)
from gribuki_trade.domain.paper_trading import PaperInstrumentType
from gribuki_trade.services.ashare.paper_day.ashare_paper import ASharePaperTradingService
from gribuki_trade.services.ashare.paper_day.ashare_paper_recovery import (
    DurableASharePaperOrderMatcher,
    PaperRecoveryRequiredError,
)
from gribuki_trade.storage.paper.paper_ledger import SQLitePaperLedger
from gribuki_trade.storage.paper.paper_orders import (
    PaperOrderEventType,
    PaperOrderStoreConflictError,
    PaperOrderStoreLeaseError,
    SQLitePaperOrderStore,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
DECISION_DATE = date(2026, 8, 14)
BAR_DATE = date(2026, 8, 17)


def _intent(order_id: str = "order-1") -> ASharePaperOrderIntent:
    return ASharePaperOrderIntent(
        order=OrderIntent(
            client_order_id=order_id,
            account_id="paper-a",
            strategy_id="recovery-test",
            symbol="600000.SH",
            side=Side.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("10.00"),
            created_at=datetime(2026, 8, 14, 14, tzinfo=SHANGHAI),
        ),
        instrument_type=PaperInstrumentType.STOCK,
        decision_session_date=DECISION_DATE,
    )


def _bar(*, revision: str = "bars-v1") -> SimulatedDailyBar:
    return SimulatedDailyBar(
        symbol="600000.SH",
        trade_date=BAR_DATE,
        open=Decimal("9.90"),
        high=Decimal("10.20"),
        low=Decimal("9.80"),
        close=Decimal("10.00"),
        volume_shares=100_000,
        is_trading=True,
        available_at=datetime(2026, 8, 17, 15, 1, tzinfo=SHANGHAI),
        source_revision=revision,
        price_band=ExplicitPriceBand(Decimal("8"), Decimal("12")),
    )


def _accounts(path: Path) -> tuple[SQLitePaperLedger, ASharePaperTradingService]:
    ledger = SQLitePaperLedger(path)
    service = ASharePaperTradingService(ledger)
    service.open_account(
        "paper-a",
        initial_cash=Decimal("100000"),
        session_date=DECISION_DATE,
        opened_at=datetime(2026, 8, 14, 8, 30, tzinfo=SHANGHAI),
    )
    return ledger, service


def test_store_is_idempotent_append_only_and_conflicts_on_changed_payload(
    tmp_path: Path,
) -> None:
    store = SQLitePaperOrderStore(tmp_path / "orders.sqlite3")
    try:
        first, applied = store.append_event(
            event_type=PaperOrderEventType.ORDER_SUBMISSION_STARTED,
            stream_id="order:one",
            run_id=None,
            occurred_at=datetime(2026, 8, 14, 6, tzinfo=SHANGHAI),
            idempotency_key="submit:one",
            payload={"value": 1},
        )
        replay, replay_applied = store.append_event(
            event_type=PaperOrderEventType.ORDER_SUBMISSION_STARTED,
            stream_id="order:one",
            run_id=None,
            occurred_at=datetime(2026, 8, 14, 6, tzinfo=SHANGHAI),
            idempotency_key="submit:one",
            payload={"value": 1},
        )
        assert applied is True
        assert replay_applied is False
        assert replay == first
        assert len(store.events()) == 1

        with pytest.raises(PaperOrderStoreConflictError):
            store.append_event(
                event_type=PaperOrderEventType.ORDER_SUBMISSION_STARTED,
                stream_id="order:one",
                run_id=None,
                occurred_at=datetime(2026, 8, 14, 6, tzinfo=SHANGHAI),
                idempotency_key="submit:one",
                payload={"value": 2},
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            store._connection.execute(  # noqa: SLF001 - trigger audit
                "UPDATE paper_order_events SET stream_id = 'changed'"
            )
    finally:
        store.close()


def test_run_identity_conflict_and_live_writer_lease_fail_closed(tmp_path: Path) -> None:
    store = SQLitePaperOrderStore(tmp_path / "orders.sqlite3")
    now = datetime(2026, 8, 17, 7, 1, tzinfo=SHANGHAI)
    try:
        record, applied = store.begin_run(
            symbol="600000.SH",
            trade_date=BAR_DATE,
            config_document={"participation": "0.01"},
            run_document={"bar": "v1", "config": "same"},
            started_at=now,
            owner_id="worker-a",
            lease_duration=timedelta(minutes=5),
        )
        assert applied is True
        assert record.completed is False

        with pytest.raises(PaperOrderStoreLeaseError):
            store.begin_run(
                symbol="600000.SH",
                trade_date=BAR_DATE,
                config_document={"participation": "0.01"},
                run_document={"bar": "v1", "config": "same"},
                started_at=now + timedelta(minutes=1),
                owner_id="worker-b",
            )
        with pytest.raises(PaperOrderStoreConflictError):
            store.begin_run(
                symbol="600000.SH",
                trade_date=BAR_DATE,
                config_document={"participation": "0.01"},
                run_document={"bar": "different", "config": "same"},
                started_at=now,
                owner_id="worker-a",
            )
    finally:
        store.close()


@pytest.mark.parametrize(
    "crash_event",
    [PaperOrderEventType.ORDER_FILL_APPLIED, PaperOrderEventType.ORDER_STATE_APPLIED],
)
def test_recovery_after_ledger_fill_never_debits_cash_twice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_event: PaperOrderEventType,
) -> None:
    ledger, accounts = _accounts(tmp_path / "ledger.sqlite3")
    order_path = tmp_path / "orders.sqlite3"
    store = SQLitePaperOrderStore(order_path)
    durable = DurableASharePaperOrderMatcher(
        accounts, store, owner_id="stable-worker"
    )
    durable.recover()
    durable.submit_order(_intent())
    durable.submit_order(_intent("order-2"))
    cash_before = accounts.snapshot("paper-a").cash
    original_append = store.append_event
    crashed = False

    def fail_once(**kwargs: object) -> object:
        nonlocal crashed
        if (
            kwargs["event_type"] is crash_event
            and kwargs.get("run_id") is not None
            and not crashed
        ):
            crashed = True
            raise RuntimeError("simulated process crash")
        return original_append(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(store, "append_event", fail_once)
    with pytest.raises(RuntimeError, match="simulated process crash"):
        durable.process_bar(_bar())
    assert len(accounts.fills("paper-a")) == 2
    cash_after_crash = accounts.snapshot("paper-a").cash
    assert cash_after_crash < cash_before
    with pytest.raises(PaperRecoveryRequiredError):
        durable.process_bar(_bar())
    store.close()

    restarted_store = SQLitePaperOrderStore(order_path)
    restarted = DurableASharePaperOrderMatcher(
        accounts, restarted_store, owner_id="stable-worker"
    )
    try:
        summary = restarted.recover(
            recovered_at=datetime(2026, 8, 17, 15, 2, tzinfo=SHANGHAI)
        )
        assert len(summary.recovered_run_ids) == 1
        assert restarted.matcher.order("order-1").status is PaperOrderStatus.FILLED  # type: ignore[union-attr]
        assert restarted.matcher.order("order-2").status is PaperOrderStatus.FILLED  # type: ignore[union-attr]
        assert len(accounts.fills("paper-a")) == 2
        assert accounts.snapshot("paper-a").cash == cash_after_crash
        event_types = tuple(event.event_type for event in restarted_store.events())
        assert PaperOrderEventType.ORDER_FILL_APPLIED in event_types
        assert event_types[-1] is PaperOrderEventType.RUN_COMPLETED
        assert restarted_store.incomplete_runs() == ()
    finally:
        restarted_store.close()
        ledger.close()


def test_completed_bar_exact_replay_is_receiptless_and_changed_bar_conflicts(
    tmp_path: Path,
) -> None:
    ledger, accounts = _accounts(tmp_path / "ledger.sqlite3")
    store = SQLitePaperOrderStore(tmp_path / "orders.sqlite3")
    durable = DurableASharePaperOrderMatcher(accounts, store, owner_id="worker-a")
    try:
        durable.recover()
        durable.submit_order(_intent())
        first = durable.process_bar(_bar())
        replay = durable.process_bar(_bar())

        assert first.applied_new is True
        assert first.outcomes[0].receipt is not None
        assert replay.applied_new is False
        assert replay.outcomes == ()
        assert replay.consumed_volume == first.consumed_volume
        assert len(accounts.fills("paper-a")) == 1
        with pytest.raises(PaperOrderStoreConflictError):
            durable.process_bar(_bar(revision="bars-v2"))
    finally:
        store.close()
        ledger.close()


def test_cross_instance_submission_is_blocked_during_incomplete_run(
    tmp_path: Path,
) -> None:
    ledger, accounts = _accounts(tmp_path / "ledger.sqlite3")
    path = tmp_path / "orders.sqlite3"
    first_store = SQLitePaperOrderStore(path)
    second_store = SQLitePaperOrderStore(path)
    first = DurableASharePaperOrderMatcher(accounts, first_store, owner_id="worker-a")
    second = DurableASharePaperOrderMatcher(accounts, second_store, owner_id="worker-b")
    try:
        first.recover()
        with pytest.raises(PaperOrderStoreLeaseError):
            second.recover()
        first.submit_order(_intent())
        run_document = {
            "bar": {"revision": "test"},
            "config": {"test": "config"},
            "orders_before": [],
        }
        first_store.begin_run(
            symbol="600000.SH",
            trade_date=BAR_DATE,
            config_document={"test": "config"},
            run_document=run_document,
            started_at=_bar().available_at,
            owner_id="worker-a",
        )
        with pytest.raises(PaperRecoveryRequiredError):
            second.submit_order(_intent("order-2"))
    finally:
        second_store.close()
        first_store.close()
        ledger.close()
