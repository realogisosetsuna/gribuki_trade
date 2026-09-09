from __future__ import annotations

import sqlite3
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import (
    ASharePaperFill,
    PaperFeeSchedule,
    PaperFillFees,
    PaperFillSource,
    PaperInstrumentType,
)
from gribuki_trade.services.ashare.paper_day.ashare_paper import (
    ASharePaperTradingService,
    InsufficientAvailablePositionError,
    InsufficientPaperCashError,
    PaperFillConflictError,
    PaperSessionError,
    replay_paper_account,
)
from gribuki_trade.storage.paper.paper_ledger import SQLitePaperLedger

SHANGHAI = ZoneInfo("Asia/Shanghai")
SESSION = date(2026, 8, 14)
NEXT_SESSION = date(2026, 8, 17)
OPENED_AT = datetime(2026, 8, 14, 8, 30, tzinfo=SHANGHAI)


def _fill(
    fill_id: str,
    *,
    side: Side = Side.BUY,
    symbol: str = "600000.SH",
    quantity: int = 100,
    price: str = "10",
    trading_date: date = SESSION,
    source: PaperFillSource = PaperFillSource.SIMULATED,
    instrument_type: PaperInstrumentType = PaperInstrumentType.STOCK,
    fee_override: PaperFillFees | None = None,
) -> ASharePaperFill:
    return ASharePaperFill(
        account_id="paper-a",
        fill_id=fill_id,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=Decimal(price),
        instrument_type=instrument_type,
        trading_date=trading_date,
        executed_at=datetime(
            trading_date.year,
            trading_date.month,
            trading_date.day,
            10,
            tzinfo=SHANGHAI,
        ),
        source=source,
        fee_override=fee_override,
    )


def _service(
    path: Path,
    *,
    cash: str = "10000",
) -> tuple[SQLitePaperLedger, ASharePaperTradingService]:
    ledger = SQLitePaperLedger(path)
    service = ASharePaperTradingService(ledger)
    service.open_account(
        "paper-a",
        initial_cash=Decimal(cash),
        session_date=SESSION,
        opened_at=OPENED_AT,
    )
    return ledger, service


def test_buy_rollover_and_sell_enforce_t_plus_one_and_fees(tmp_path: Path) -> None:
    ledger, service = _service(tmp_path / "paper.sqlite3")
    try:
        buy = service.record_fill(
            _fill("buy-1"),
            recorded_at=datetime(2026, 8, 14, 10, 1, tzinfo=SHANGHAI),
        )

        assert buy.applied_fill.trade_value == Decimal("1000.00")
        assert buy.applied_fill.fees.commission == Decimal("5.00")
        assert buy.applied_fill.fees.transfer_fee == Decimal("0.01")
        assert buy.applied_fill.fees.stamp_tax == Decimal("0.00")
        assert buy.snapshot.cash == Decimal("8994.99")
        position = buy.snapshot.position("600000.SH")
        assert position is not None
        assert position.quantity == 100
        assert position.available_to_sell == 0
        assert position.today_buy == 100
        assert position.average_cost == Decimal("10.0501")

        with pytest.raises(InsufficientAvailablePositionError):
            service.record_fill(
                _fill("same-day-sell", side=Side.SELL, quantity=1),
                recorded_at=datetime(2026, 8, 14, 10, 2, tzinfo=SHANGHAI),
            )

        rolled = service.rollover_session(
            "paper-a",
            target_session_date=NEXT_SESSION,
            occurred_at=datetime(2026, 8, 17, 9, tzinfo=SHANGHAI),
        )
        position = rolled.position("600000.SH")
        assert position is not None
        assert position.available_to_sell == 100
        assert position.today_buy == 0

        sell = service.record_fill(
            _fill(
                "sell-1",
                side=Side.SELL,
                quantity=40,
                price="11",
                trading_date=NEXT_SESSION,
            ),
            recorded_at=datetime(2026, 8, 17, 10, 1, tzinfo=SHANGHAI),
        )
        assert sell.applied_fill.fees.commission == Decimal("5.00")
        assert sell.applied_fill.fees.transfer_fee == Decimal("0.00")
        assert sell.applied_fill.fees.stamp_tax == Decimal("0.22")
        assert sell.applied_fill.cash_change == Decimal("434.78")
        assert sell.applied_fill.realized_pnl_change == Decimal("32.7760")
        assert sell.snapshot.cash == Decimal("9429.77")
        position = sell.snapshot.position("600000.SH")
        assert position is not None
        assert position.quantity == 60
        assert position.available_to_sell == 60
        assert position.today_buy == 0
        assert position.realized_pnl == Decimal("32.7760")
    finally:
        ledger.close()


def test_manual_fee_override_and_simulated_fill_share_contract(tmp_path: Path) -> None:
    ledger, service = _service(tmp_path / "paper.sqlite3")
    try:
        override = PaperFillFees(
            commission=Decimal("2.50"),
            transfer_fee=Decimal("0.01"),
            stamp_tax=Decimal("0"),
        )
        manual = service.record_fill(
            _fill(
                "manual-buy",
                source=PaperFillSource.MANUAL,
                fee_override=override,
            ),
            recorded_at=datetime(2026, 8, 14, 11, tzinfo=SHANGHAI),
        )
        simulated = service.record_fill(
            _fill("simulated-buy"),
            recorded_at=datetime(2026, 8, 14, 11, 1, tzinfo=SHANGHAI),
        )

        assert manual.applied_fill.fill.source is PaperFillSource.MANUAL
        assert manual.applied_fill.fees == override
        assert simulated.applied_fill.fill.source is PaperFillSource.SIMULATED
        assert simulated.applied_fill.fees.commission == Decimal("5.00")
        assert len(service.fills("paper-a")) == 2
    finally:
        ledger.close()


def test_fill_is_idempotent_and_conflicting_reuse_fails(tmp_path: Path) -> None:
    ledger, service = _service(tmp_path / "paper.sqlite3")
    try:
        fill = _fill("stable-fill")
        first = service.record_fill(
            fill,
            recorded_at=datetime(2026, 8, 14, 10, 1, tzinfo=SHANGHAI),
        )
        repeated = service.record_fill(
            fill,
            recorded_at=datetime(2026, 8, 14, 12, tzinfo=SHANGHAI),
        )

        assert first.applied_new is True
        assert repeated.applied_new is False
        assert repeated.event_sequence == first.event_sequence
        assert repeated.snapshot == first.snapshot
        assert len(service.fills("paper-a")) == 1

        with pytest.raises(PaperFillConflictError):
            service.record_fill(
                _fill("stable-fill", quantity=200),
                recorded_at=datetime(2026, 8, 14, 12, 1, tzinfo=SHANGHAI),
            )
        assert service.snapshot("paper-a") == first.snapshot
    finally:
        ledger.close()


def test_rejected_buy_and_sell_leave_append_only_state_unchanged(tmp_path: Path) -> None:
    ledger, service = _service(tmp_path / "paper.sqlite3", cash="100")
    try:
        before = service.snapshot("paper-a")
        with pytest.raises(InsufficientPaperCashError):
            service.record_fill(
                _fill("too-expensive"),
                recorded_at=datetime(2026, 8, 14, 10, 1, tzinfo=SHANGHAI),
            )
        with pytest.raises(InsufficientAvailablePositionError):
            service.record_fill(
                _fill("naked-sell", side=Side.SELL),
                recorded_at=datetime(2026, 8, 14, 10, 2, tzinfo=SHANGHAI),
            )

        assert service.snapshot("paper-a") == before
        assert len(ledger.events("paper-a")) == 1
    finally:
        ledger.close()


def test_explicit_rollover_is_idempotent_and_sessions_cannot_go_back(tmp_path: Path) -> None:
    ledger, service = _service(tmp_path / "paper.sqlite3")
    try:
        first = service.rollover_session(
            "paper-a",
            target_session_date=NEXT_SESSION,
            occurred_at=datetime(2026, 8, 17, 9, tzinfo=SHANGHAI),
        )
        repeated = service.rollover_session(
            "paper-a",
            target_session_date=NEXT_SESSION,
            occurred_at=datetime(2026, 8, 17, 12, tzinfo=SHANGHAI),
        )

        assert repeated == first
        assert len(ledger.events("paper-a")) == 2
        with pytest.raises(PaperSessionError):
            service.rollover_session(
                "paper-a",
                target_session_date=SESSION,
                occurred_at=datetime(2026, 8, 17, 12, 1, tzinfo=SHANGHAI),
            )
    finally:
        ledger.close()


def test_rollover_timestamp_cannot_precede_latest_event(tmp_path: Path) -> None:
    ledger, service = _service(tmp_path / "paper.sqlite3")
    try:
        with pytest.raises(PaperSessionError, match="precedes"):
            service.rollover_session(
                "paper-a",
                target_session_date=NEXT_SESSION,
                occurred_at=datetime(2026, 8, 14, 8, tzinfo=SHANGHAI),
            )
        assert len(ledger.events("paper-a")) == 1
    finally:
        ledger.close()


def test_restart_and_pure_event_replay_produce_identical_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    ledger, service = _service(path)
    service.record_fill(
        _fill("buy-1"),
        recorded_at=datetime(2026, 8, 14, 10, 1, tzinfo=SHANGHAI),
    )
    expected = service.snapshot("paper-a")
    events = ledger.events("paper-a")
    assert replay_paper_account(events) == expected
    ledger.close()

    with SQLitePaperLedger(path) as reopened:
        recovered = ASharePaperTradingService(reopened).snapshot("paper-a")
        assert recovered == expected


def test_sqlite_ledger_uses_wal_and_rejects_update_or_delete(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    ledger, _service_instance = _service(path)
    ledger.close()

    connection = sqlite3.connect(path)
    try:
        mode = connection.execute("PRAGMA journal_mode").fetchone()
        assert mode is not None and str(mode[0]).lower() == "wal"
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE paper_ledger_events SET payload_json = '{}' WHERE sequence = 1"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM paper_ledger_events WHERE sequence = 1")
    finally:
        connection.close()


def test_fee_schedule_is_configurable(tmp_path: Path) -> None:
    ledger = SQLitePaperLedger(tmp_path / "paper.sqlite3")
    service = ASharePaperTradingService(
        ledger,
        fee_schedule=PaperFeeSchedule(
            commission_rate=Decimal("0.001"),
            minimum_commission_cny=Decimal("1"),
            stock_transfer_fee_rate=Decimal("0.00002"),
            stock_sell_stamp_tax_rate=Decimal("0.001"),
        ),
    )
    service.open_account(
        "paper-a",
        initial_cash=Decimal("10000"),
        session_date=SESSION,
        opened_at=OPENED_AT,
    )
    try:
        stock = service.record_fill(
            _fill("stock"),
            recorded_at=datetime(2026, 8, 14, 10, 1, tzinfo=SHANGHAI),
        )
        assert stock.applied_fill.fees == PaperFillFees(
            commission=Decimal("1.00"),
            transfer_fee=Decimal("0.02"),
            stamp_tax=Decimal("0.00"),
        )
    finally:
        ledger.close()


def test_etf_defaults_have_no_transfer_fee_or_stamp_tax(tmp_path: Path) -> None:
    ledger, service = _service(tmp_path / "paper.sqlite3")
    try:
        buy = service.record_fill(
            _fill(
                "etf-buy",
                symbol="510300.SH",
                instrument_type=PaperInstrumentType.ETF,
            ),
            recorded_at=datetime(2026, 8, 14, 10, 1, tzinfo=SHANGHAI),
        )
        assert buy.applied_fill.fees.transfer_fee == Decimal("0.00")
        assert buy.applied_fill.fees.stamp_tax == Decimal("0.00")
        service.rollover_session(
            "paper-a",
            target_session_date=NEXT_SESSION,
            occurred_at=datetime(2026, 8, 17, 9, tzinfo=SHANGHAI),
        )
        sell = service.record_fill(
            _fill(
                "etf-sell",
                side=Side.SELL,
                symbol="510300.SH",
                instrument_type=PaperInstrumentType.ETF,
                trading_date=NEXT_SESSION,
            ),
            recorded_at=datetime(2026, 8, 17, 10, 1, tzinfo=SHANGHAI),
        )
        assert sell.applied_fill.fees.transfer_fee == Decimal("0.00")
        assert sell.applied_fill.fees.stamp_tax == Decimal("0.00")
    finally:
        ledger.close()


def test_simulated_fills_cannot_override_broker_fees() -> None:
    with pytest.raises(ValueError, match="MANUAL"):
        _fill(
            "invalid",
            fee_override=PaperFillFees(
                commission=Decimal("1"),
                transfer_fee=Decimal("0"),
                stamp_tax=Decimal("0"),
            ),
        )
