from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import (
    ASharePaperFill,
    PaperFillSource,
    PaperInstrumentType,
)
from gribuki_trade.features.ashare_surveillance import (
    IntradayCandidate,
    IntradayCandidateClass,
)
from gribuki_trade.ports.ashare_screening import AShareBoard
from gribuki_trade.services.ashare.intraday.ashare_intraday_paper import IntradayPaperOrder
from gribuki_trade.services.ashare.paper_day import ashare_paper_day as facade
from gribuki_trade.services.ashare.paper_day import ashare_paper_day_documents as documents


def _candidate() -> IntradayCandidate:
    return IntradayCandidate(
        symbol="600000.SH",
        name="浦发银行",
        rank=2,
        candidate_class=IntradayCandidateClass.MOMENTUM_EXPANSION,
        anomaly_score=0.82,
        factor_weight_coverage=0.91,
        last_price=Decimal("10.25"),
        change_percent=Decimal("2.50"),
        session_amount_cny=Decimal("12345678.90"),
        factors=(),
        reason_codes=("PRICE_STRENGTH",),
        previous_close=Decimal("10.00"),
    )


def _order() -> IntradayPaperOrder:
    signal_at = datetime(2026, 8, 14, 1, 30, tzinfo=UTC)
    return IntradayPaperOrder(
        order_id="paper-order-1",
        account_id="paper-account",
        symbol="600000.SH",
        board=AShareBoard.SSE_MAIN,
        session_date=date(2026, 8, 14),
        signal_bar_end=signal_at,
        signal_price=Decimal("10.25"),
        invalidation_price=Decimal("10.00"),
        limit_price=Decimal("10.26"),
        quantity=100,
        created_at=signal_at + timedelta(minutes=1),
        expires_at=signal_at + timedelta(minutes=30),
        previous_close=Decimal("10.00"),
        instrument_type=PaperInstrumentType.STOCK,
    )


def _fill() -> ASharePaperFill:
    return ASharePaperFill(
        account_id="paper-account",
        fill_id="fill-1",
        symbol="600000.SH",
        side=Side.BUY,
        quantity=100,
        price=Decimal("10.25"),
        instrument_type=PaperInstrumentType.STOCK,
        trading_date=date(2026, 8, 14),
        executed_at=datetime(2026, 8, 14, 1, 31, tzinfo=UTC),
        source=PaperFillSource.SIMULATED,
        external_order_id="paper-order-1",
        note="document test",
    )


def test_facade_keeps_historical_document_helpers_as_aliases() -> None:
    assert facade.PaperDayWatchEntry is documents.PaperDayWatchEntry
    assert facade._watch_entry_document is documents.watch_entry_document
    assert facade._candidate_document is documents.candidate_document
    assert facade._order_document is documents.order_document
    assert facade._fill_document is documents.fill_document


def test_watch_entry_and_candidate_documents_round_trip() -> None:
    entry = documents.watch_entry_from_intraday(_candidate())
    restored = documents.watch_entry_from_document(
        documents.watch_entry_document(entry), "RESTORED"
    )
    assert restored == entry

    candidate = _candidate()
    encoded = documents.candidate_document(candidate)
    assert encoded["previous_close"] == "10.00"
    assert documents.candidate_from_document(encoded) == candidate
    assert documents.candidate_from_document({"symbol": "600000.SH"}) is None
    assert documents.candidate_previous_close(candidate) == Decimal("10.00")


def test_order_and_fill_documents_round_trip_preserve_enums_and_times() -> None:
    order = _order()
    assert documents.order_from_document(documents.order_document(order)) == order

    fill = _fill()
    restored = documents.fill_from_document(documents.fill_document(fill))
    assert restored == fill
    assert documents.board_from_symbol("688001.SH") is AShareBoard.STAR
    assert documents.board_from_symbol("300001.SZ") is AShareBoard.CHINEXT


def test_watch_document_rejects_invalid_shape_without_side_effects() -> None:
    assert documents.watch_entry_from_document(None, "RESTORED") is None
    assert documents.watch_entry_from_document({"symbol": "bad"}, "RESTORED") is None
    assert documents.strict_positive_int(1) == 1
