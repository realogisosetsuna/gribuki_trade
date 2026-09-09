from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.domain.orders import OrderIntent, Side
from gribuki_trade.domain.paper_orders import (
    ASharePaperOrderIntent,
    ExplicitPriceBand,
    PaperMatchingConfig,
    PaperMatchReason,
    PaperOrderStatus,
    PaperTimeInForce,
    SimulatedDailyBar,
)
from gribuki_trade.domain.paper_trading import (
    ASharePaperFill,
    PaperFillSource,
    PaperInstrumentType,
)
from gribuki_trade.services.ashare.paper_day.ashare_paper import ASharePaperTradingService
from gribuki_trade.services.ashare.paper_day.ashare_paper_matching import (
    ASharePaperOrderMatcher,
    PaperBarConflictError,
    PaperMatchingSequenceError,
)
from gribuki_trade.storage.paper.paper_ledger import SQLitePaperLedger

SHANGHAI = ZoneInfo("Asia/Shanghai")
DECISION_SESSION = date(2026, 8, 14)
FIRST_BAR = date(2026, 8, 17)
SECOND_BAR = date(2026, 8, 18)
DEFAULT_BAND = ExplicitPriceBand(Decimal("8.00"), Decimal("12.00"))


def _open(
    path: Path,
    *,
    cash: str = "100000",
    config: PaperMatchingConfig | None = None,
) -> tuple[SQLitePaperLedger, ASharePaperTradingService, ASharePaperOrderMatcher]:
    ledger = SQLitePaperLedger(path)
    accounts = ASharePaperTradingService(ledger)
    accounts.open_account(
        "paper-a",
        initial_cash=Decimal(cash),
        session_date=DECISION_SESSION,
        opened_at=datetime(2026, 8, 14, 8, 30, tzinfo=SHANGHAI),
    )
    return ledger, accounts, ASharePaperOrderMatcher(accounts, config=config)


def _intent(
    order_id: str,
    *,
    side: Side = Side.BUY,
    quantity: int = 100,
    limit: str = "10.00",
    symbol: str = "600000.SH",
    instrument_type: PaperInstrumentType = PaperInstrumentType.STOCK,
    decision_session: date = DECISION_SESSION,
    created_hour: int = 14,
    time_in_force: PaperTimeInForce = PaperTimeInForce.NEXT_TRADING_BAR,
    expires_on: date | None = None,
) -> ASharePaperOrderIntent:
    return ASharePaperOrderIntent(
        order=OrderIntent(
            client_order_id=order_id,
            account_id="paper-a",
            strategy_id="test-strategy",
            symbol=symbol,
            side=side,
            quantity=Decimal(quantity),
            limit_price=Decimal(limit),
            created_at=datetime(
                decision_session.year,
                decision_session.month,
                decision_session.day,
                created_hour,
                tzinfo=SHANGHAI,
            ),
        ),
        instrument_type=instrument_type,
        decision_session_date=decision_session,
        time_in_force=time_in_force,
        expires_on=expires_on,
    )


def _bar(
    trade_date: date = FIRST_BAR,
    *,
    symbol: str = "600000.SH",
    open_price: str | None = "9.90",
    high: str | None = "10.20",
    low: str | None = "9.80",
    close: str | None = "10.00",
    volume: int = 100_000,
    is_trading: bool = True,
    price_band: ExplicitPriceBand | None = DEFAULT_BAND,
    source_revision: str | None = None,
) -> SimulatedDailyBar:
    return SimulatedDailyBar(
        symbol=symbol,
        trade_date=trade_date,
        open=Decimal(open_price) if open_price is not None else None,
        high=Decimal(high) if high is not None else None,
        low=Decimal(low) if low is not None else None,
        close=Decimal(close) if close is not None else None,
        volume_shares=volume,
        is_trading=is_trading,
        available_at=datetime(
            trade_date.year,
            trade_date.month,
            trade_date.day,
            15,
            1,
            tzinfo=SHANGHAI,
        ),
        source_revision=source_revision or f"bars-{trade_date.isoformat()}-v1",
        price_band=price_band,
    )


def test_submit_rejects_non_lot_buy_and_bad_tick_but_sell_allows_odd_lot(
    tmp_path: Path,
) -> None:
    ledger, accounts, matcher = _open(tmp_path / "paper.sqlite3")
    try:
        odd_buy = matcher.submit_order(_intent("odd-buy", quantity=150))
        bad_tick = matcher.submit_order(_intent("bad-tick", limit="10.001"))

        assert odd_buy.status is PaperOrderStatus.REJECTED
        assert odd_buy.reason is PaperMatchReason.BUY_QUANTITY_NOT_ROUND_LOT
        assert bad_tick.status is PaperOrderStatus.REJECTED
        assert bad_tick.reason is PaperMatchReason.LIMIT_PRICE_NOT_TICK_ALIGNED

        accounts.record_fill(
            ASharePaperFill(
                account_id="paper-a",
                fill_id="manual-position",
                symbol="600000.SH",
                side=Side.BUY,
                quantity=150,
                price=Decimal("9"),
                instrument_type=PaperInstrumentType.STOCK,
                trading_date=DECISION_SESSION,
                executed_at=datetime(2026, 8, 14, 10, tzinfo=SHANGHAI),
                source=PaperFillSource.MANUAL,
            ),
            recorded_at=datetime(2026, 8, 14, 10, 1, tzinfo=SHANGHAI),
        )
        accounts.rollover_session(
            "paper-a",
            target_session_date=FIRST_BAR,
            occurred_at=datetime(2026, 8, 17, 9, tzinfo=SHANGHAI),
        )
        odd_sell = matcher.submit_order(
            _intent(
                "odd-sell",
                side=Side.SELL,
                quantity=150,
                decision_session=FIRST_BAR,
            )
        )
        oversold = matcher.submit_order(
            _intent(
                "oversold",
                side=Side.SELL,
                quantity=1,
                decision_session=FIRST_BAR,
            )
        )
        assert odd_sell.status is PaperOrderStatus.PENDING
        assert odd_sell.reserved_quantity == 150
        assert oversold.status is PaperOrderStatus.REJECTED
        assert oversold.reason is PaperMatchReason.INSUFFICIENT_AVAILABLE_POSITION
    finally:
        ledger.close()


def test_cash_reservations_conservatively_reject_overcommitted_orders(
    tmp_path: Path,
) -> None:
    ledger, _accounts, matcher = _open(tmp_path / "paper.sqlite3", cash="10000")
    try:
        first = matcher.submit_order(_intent("first", quantity=900))
        second = matcher.submit_order(_intent("second", quantity=100))

        assert first.status is PaperOrderStatus.PENDING
        assert first.reserved_cash == Decimal("9005.09")
        assert second.status is PaperOrderStatus.REJECTED
        assert second.reason is PaperMatchReason.INSUFFICIENT_CASH_BUDGET
    finally:
        ledger.close()


def test_order_only_becomes_eligible_after_decision_and_bar_replay_is_idempotent(
    tmp_path: Path,
) -> None:
    ledger, accounts, matcher = _open(tmp_path / "paper.sqlite3")
    try:
        matcher.submit_order(_intent("buy-1"))
        same_day = _bar(DECISION_SESSION)
        ignored = matcher.process_bar(same_day)
        assert ignored.outcomes[0].reason is PaperMatchReason.ORDER_NOT_YET_ELIGIBLE
        assert matcher.order("buy-1").status is PaperOrderStatus.PENDING  # type: ignore[union-attr]

        next_bar = _bar()
        first = matcher.process_bar(next_bar)
        replay = matcher.process_bar(next_bar)

        assert first.applied_new is True
        assert replay.applied_new is False
        assert first.consumed_volume == 100
        assert first.outcomes[0].status_after is PaperOrderStatus.FILLED
        assert first.outcomes[0].fill_price == Decimal("9.91")
        assert len(accounts.fills("paper-a")) == 1
        position = accounts.snapshot("paper-a").position("600000.SH")
        assert position is not None and position.today_buy == 100

        with pytest.raises(PaperBarConflictError):
            matcher.process_bar(_bar(source_revision="bars-conflicting-v2"))
    finally:
        ledger.close()


@pytest.mark.parametrize(
    ("bar", "reason"),
    [
        (_bar(is_trading=False, open_price=None, high=None, low=None, close=None),
         PaperMatchReason.BAR_SUSPENDED),
        (_bar(open_price=None, high=None, low=None, close=None),
         PaperMatchReason.BAR_OHLC_MISSING),
        (_bar(volume=0), PaperMatchReason.BAR_VOLUME_ZERO),
        (_bar(price_band=None), PaperMatchReason.PRICE_BAND_MISSING),
        (
            _bar(price_band=ExplicitPriceBand(Decimal("9.00"), Decimal("10.00"))),
            PaperMatchReason.BAR_OUTSIDE_PRICE_BAND,
        ),
    ],
)
def test_invalid_or_unverified_bar_never_fills_next_bar_order(
    tmp_path: Path,
    bar: SimulatedDailyBar,
    reason: PaperMatchReason,
) -> None:
    ledger, accounts, matcher = _open(tmp_path / "paper.sqlite3")
    try:
        matcher.submit_order(_intent("buy-1"))
        run = matcher.process_bar(bar)

        assert run.consumed_volume == 0
        assert run.outcomes[0].reason is reason
        assert matcher.order("buy-1").status is PaperOrderStatus.PENDING  # type: ignore[union-attr]
        assert accounts.fills("paper-a") == ()
    finally:
        ledger.close()


def test_next_valid_bar_expires_an_untouched_order(tmp_path: Path) -> None:
    ledger, _accounts, matcher = _open(tmp_path / "paper.sqlite3")
    try:
        matcher.submit_order(_intent("deep-limit", limit="8.00"))
        run = matcher.process_bar(_bar())

        assert run.outcomes[0].reason is PaperMatchReason.EXPIRED_AFTER_ELIGIBLE_BAR
        order = matcher.order("deep-limit")
        assert order is not None
        assert order.status is PaperOrderStatus.EXPIRED
        assert order.reserved_cash == 0
    finally:
        ledger.close()


def test_explicit_bar_price_band_rejects_order_limit_outside_band(tmp_path: Path) -> None:
    ledger, accounts, matcher = _open(tmp_path / "paper.sqlite3")
    try:
        matcher.submit_order(_intent("outside-band", limit="13.00"))
        run = matcher.process_bar(_bar())

        assert run.outcomes[0].reason is PaperMatchReason.ORDER_LIMIT_OUTSIDE_PRICE_BAND
        assert matcher.order("outside-band").status is PaperOrderStatus.REJECTED  # type: ignore[union-attr]
        assert accounts.fills("paper-a") == ()
    finally:
        ledger.close()


def test_good_till_date_order_partially_fills_across_bars_and_commission_is_per_order(
    tmp_path: Path,
) -> None:
    ledger, accounts, matcher = _open(
        tmp_path / "paper.sqlite3",
        config=PaperMatchingConfig(volume_participation_rate=Decimal("0.01")),
    )
    try:
        matcher.submit_order(
            _intent(
                "multi-fill",
                quantity=200,
                time_in_force=PaperTimeInForce.GOOD_TILL_DATE,
                expires_on=SECOND_BAR,
            )
        )
        first = matcher.process_bar(_bar(volume=10_000))
        after_first = matcher.order("multi-fill")
        assert first.consumed_volume == 100
        assert after_first is not None
        assert after_first.status is PaperOrderStatus.PARTIALLY_FILLED
        assert after_first.filled_quantity == 100

        second = matcher.process_bar(_bar(SECOND_BAR, volume=10_000))
        completed = matcher.order("multi-fill")
        fills = accounts.fills("paper-a")

        assert second.consumed_volume == 100
        assert completed is not None
        assert completed.status is PaperOrderStatus.FILLED
        assert completed.filled_quantity == 200
        assert len(fills) == 2
        assert fills[0].fees.commission == Decimal("5.00")
        assert fills[1].fees.commission == Decimal("0.00")
        assert sum((fill.fees.commission for fill in fills), Decimal("0")) == Decimal(
            "5.00"
        )
    finally:
        ledger.close()


def test_volume_participation_is_shared_fifo_across_orders(tmp_path: Path) -> None:
    ledger, _accounts, matcher = _open(
        tmp_path / "paper.sqlite3",
        config=PaperMatchingConfig(volume_participation_rate=Decimal("0.01")),
    )
    try:
        matcher.submit_order(_intent("first", quantity=100, created_hour=13))
        matcher.submit_order(_intent("second", quantity=100, created_hour=14))
        run = matcher.process_bar(_bar(volume=10_000))

        assert run.volume_capacity == 100
        assert run.consumed_volume == 100
        assert matcher.order("first").status is PaperOrderStatus.FILLED  # type: ignore[union-attr]
        assert matcher.order("second").status is PaperOrderStatus.EXPIRED  # type: ignore[union-attr]
        assert run.outcomes[1].reason is PaperMatchReason.EXPIRED_AFTER_ELIGIBLE_BAR
    finally:
        ledger.close()


def test_odd_lot_sell_matches_with_conservative_slippage(tmp_path: Path) -> None:
    ledger, accounts, matcher = _open(tmp_path / "paper.sqlite3")
    try:
        accounts.record_fill(
            ASharePaperFill(
                account_id="paper-a",
                fill_id="seed-odd-position",
                symbol="600000.SH",
                side=Side.BUY,
                quantity=150,
                price=Decimal("9"),
                instrument_type=PaperInstrumentType.STOCK,
                trading_date=DECISION_SESSION,
                executed_at=datetime(2026, 8, 14, 10, tzinfo=SHANGHAI),
                source=PaperFillSource.MANUAL,
            ),
            recorded_at=datetime(2026, 8, 14, 10, 1, tzinfo=SHANGHAI),
        )
        accounts.rollover_session(
            "paper-a",
            target_session_date=FIRST_BAR,
            occurred_at=datetime(2026, 8, 17, 9, tzinfo=SHANGHAI),
        )
        submitted = matcher.submit_order(
            _intent(
                "sell-tail",
                side=Side.SELL,
                quantity=150,
                decision_session=FIRST_BAR,
            )
        )
        assert submitted.status is PaperOrderStatus.PENDING

        run = matcher.process_bar(
            _bar(
                SECOND_BAR,
                open_price="10.20",
                high="10.50",
                low="9.90",
                close="10.30",
            )
        )

        assert run.outcomes[0].fill_price == Decimal("10.19")
        assert run.outcomes[0].status_after is PaperOrderStatus.FILLED
        position = accounts.snapshot("paper-a").position("600000.SH")
        assert position is not None and position.quantity == 0
    finally:
        ledger.close()


def test_bars_cannot_move_an_open_order_backwards_in_time(tmp_path: Path) -> None:
    ledger, _accounts, matcher = _open(tmp_path / "paper.sqlite3")
    try:
        matcher.submit_order(
            _intent(
                "gtd",
                limit="8.00",
                time_in_force=PaperTimeInForce.GOOD_TILL_DATE,
                expires_on=date(2026, 8, 20),
            )
        )
        matcher.process_bar(_bar(SECOND_BAR))
        with pytest.raises(PaperMatchingSequenceError):
            matcher.process_bar(_bar(FIRST_BAR))
    finally:
        ledger.close()


def test_order_cannot_be_backfilled_after_eligible_bar_was_processed(
    tmp_path: Path,
) -> None:
    ledger, _accounts, matcher = _open(tmp_path / "paper.sqlite3")
    try:
        matcher.process_bar(_bar())
        with pytest.raises(PaperMatchingSequenceError, match="backfill"):
            matcher.submit_order(_intent("late-order"))
        assert matcher.order("late-order") is None
    finally:
        ledger.close()


def test_cancel_releases_reservation_and_is_idempotent(tmp_path: Path) -> None:
    ledger, _accounts, matcher = _open(tmp_path / "paper.sqlite3", cash="2000")
    try:
        matcher.submit_order(_intent("first", quantity=100))
        cancelled_at = datetime(2026, 8, 14, 14, 1, tzinfo=SHANGHAI)
        first = matcher.cancel_order("first", cancelled_at=cancelled_at)
        replay = matcher.cancel_order("first", cancelled_at=cancelled_at)
        second = matcher.submit_order(_intent("second", quantity=100))

        assert first == replay
        assert first.status is PaperOrderStatus.CANCELLED
        assert first.reserved_cash == 0
        assert second.status is PaperOrderStatus.PENDING
    finally:
        ledger.close()


def test_unadjusted_daily_bar_adapter_rejects_adjusted_history() -> None:
    adjusted = DailyBar(
        symbol="600000.SH",
        trade_date=FIRST_BAR,
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10"),
        previous_close=Decimal("10"),
        volume=10_000,
        amount=Decimal("100000"),
        turnover_percent=None,
        is_trading=True,
        is_st=False,
        adjustment=PriceAdjustment.FORWARD,
    )
    with pytest.raises(ValueError, match="unadjusted"):
        SimulatedDailyBar.from_unadjusted_daily_bar(
            adjusted,
            available_at=datetime(2026, 8, 17, 15, 1, tzinfo=SHANGHAI),
            source_revision="adjusted-v1",
            price_band=ExplicitPriceBand(Decimal("8"), Decimal("12")),
        )
