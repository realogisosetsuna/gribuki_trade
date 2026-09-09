from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.domain.paper_trading import (
    PaperAccountSnapshot,
    PaperFillSource,
    PaperInstrumentType,
    PaperPosition,
)
from gribuki_trade.domain.recommendations import (
    RecommendationDecision,
    RecommendationHorizon,
)
from gribuki_trade.features.technical import TechnicalSignal
from gribuki_trade.ports.ashare_screening import AShareBoard
from gribuki_trade.ports.market_data import (
    FreshnessStatus,
    IntradayBar,
    MarketDataMeta,
    MinuteInterval,
    SourceSemantics,
)
from gribuki_trade.services.ashare_intraday_paper import (
    IntradayPaperMatchReason,
    IntradayPaperMatchStatus,
    IntradayPaperRiskConfig,
    IntradayPaperRiskReason,
    IntradayPaperRiskStatus,
    build_intraday_buy_order,
    build_intraday_buy_price_acceptance,
    build_intraday_sell_price_acceptance,
    build_intraday_sell_quantity_plan,
    intraday_order_quantity_rule,
    match_intraday_buy_order,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
SESSION = date(2026, 8, 14)
SIGNAL_END = datetime(2026, 8, 14, 10, 0, tzinfo=SHANGHAI)
CREATED_AT = datetime(2026, 8, 14, 10, 0, 30, tzinfo=SHANGHAI)


def _signal(
    *,
    symbol: str = "600000.SH",
    decision: RecommendationDecision = RecommendationDecision.ENTER_CANDIDATE,
    price: str | None = "10",
    stop: str | None = "9.80",
    as_of: datetime = CREATED_AT,
) -> TechnicalSignal:
    return TechnicalSignal(
        symbol=symbol,
        as_of=as_of,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        decision=decision,
        score=Decimal("1"),
        reference_price=Decimal(price) if price is not None else None,
        invalidation_price=Decimal(stop) if stop is not None else None,
        reason_codes=("CLOSED_BAR_BREAKOUT",),
        data_age=timedelta(seconds=30),
        strategy_version="technical-breakout@test",
        metrics=(),
    )


def _position(
    symbol: str,
    *,
    quantity: int = 100,
    average_cost: str = "10",
) -> PaperPosition:
    return PaperPosition(
        symbol=symbol,
        instrument_type=PaperInstrumentType.STOCK,
        quantity=quantity,
        available_to_sell=quantity,
        today_buy=0,
        average_cost=Decimal(average_cost),
        realized_pnl=Decimal("0"),
    )


def _account(
    *,
    cash: str = "200000",
    positions: tuple[PaperPosition, ...] = (),
    session: date = SESSION,
) -> PaperAccountSnapshot:
    return PaperAccountSnapshot(
        account_id="paper-day",
        session_date=session,
        cash=Decimal(cash),
        positions=tuple(sorted(positions, key=lambda item: item.symbol)),
        opened_at=datetime(2026, 8, 14, 8, 0, tzinfo=SHANGHAI),
        updated_at=datetime(2026, 8, 14, 9, 0, tzinfo=SHANGHAI),
        last_sequence=1,
    )


def _approved_order(
    *,
    account: PaperAccountSnapshot | None = None,
    signal: TechnicalSignal | None = None,
    board: AShareBoard | str | None = AShareBoard.SSE_MAIN,
    previous_close: str | None = "10",
    instrument_type: PaperInstrumentType = PaperInstrumentType.STOCK,
):
    outcome = build_intraday_buy_order(
        signal or _signal(),
        account or _account(),
        board=board,
        signal_bar_end=SIGNAL_END,
        previous_close=(
            Decimal(previous_close) if previous_close is not None else None
        ),
        created_at=CREATED_AT,
        instrument_type=instrument_type,
    )
    assert outcome.status is IntradayPaperRiskStatus.APPROVED
    assert outcome.order is not None
    return outcome.order


def _bar(
    *,
    symbol: str = "600000.SH",
    start: datetime = datetime(2026, 8, 14, 10, 1, tzinfo=SHANGHAI),
    open_price: str = "10.00",
    high: str = "10.10",
    low: str = "9.99",
    close: str = "10.05",
    volume_lots: int = 10_000,
    closed: bool = True,
    interval: MinuteInterval = MinuteInterval.ONE_MINUTE,
    fetched_at: datetime | None = None,
) -> IntradayBar:
    end = start + timedelta(minutes=interval.minutes)
    return IntradayBar(
        symbol=symbol,
        start_at=start,
        end_at=end,
        interval=interval,
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume_lots=volume_lots,
        amount=Decimal("1000000"),
        vwap=None,
        is_closed=closed,
        meta=MarketDataMeta(
            provider="test",
            semantics=SourceSemantics.AGGREGATED_MINUTE_BAR,
            fetched_at=fetched_at or end,
            provider_timestamp=end,
            freshness=FreshnessStatus.CURRENT,
        ),
    )


def test_default_risk_formula_reserves_cash_and_rounds_down_to_board_lot() -> None:
    result = build_intraday_buy_order(
        _signal(),
        _account(),
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
    )

    assert result.status is IntradayPaperRiskStatus.APPROVED
    assert result.reason is IntradayPaperRiskReason.APPROVED
    assert result.risk_budget == Decimal("1500.0000")
    assert result.stop_distance == Decimal("0.21")
    assert result.available_cash_budget == Decimal("40000.00")
    assert result.order is not None
    assert result.order.quantity == 3900
    assert result.order.limit_price == Decimal("10.01")
    assert result.order.quantity % 100 == 0


def test_stop_risk_is_binding_before_symbol_cap() -> None:
    order = _approved_order(signal=_signal(stop="8"))
    assert order.quantity == 700


def test_stop_risk_uses_worst_buy_limit_instead_of_lower_signal_price() -> None:
    result = build_intraday_buy_order(
        _signal(stop="9.00"),
        _account(),
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
        config=IntradayPaperRiskConfig(buy_limit_markup=Decimal("0.05")),
    )

    assert result.status is IntradayPaperRiskStatus.APPROVED
    assert result.order is not None
    assert result.order.limit_price == Decimal("10.50")
    assert result.stop_distance == Decimal("1.50")
    assert result.order.quantity == 1000


def test_quick_protective_stop_reduces_quantity_using_worst_buy_limit() -> None:
    result = build_intraday_buy_order(
        _signal(stop="9.80"),
        _account(),
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
        protective_stop_price=Decimal("9.00"),
    )

    assert result.status is IntradayPaperRiskStatus.APPROVED
    assert result.stop_distance == Decimal("1.01")
    assert result.order is not None
    assert result.order.quantity == 1400


def test_default_policy_has_no_position_count_cap_but_keeps_capital_limits() -> None:
    positions = tuple(_position(f"00000{item}.SZ") for item in range(1, 6))
    outcome = build_intraday_buy_order(
        _signal(),
        _account(positions=positions),
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
    )

    assert outcome.status is IntradayPaperRiskStatus.APPROVED
    assert outcome.order is not None
    assert IntradayPaperRiskConfig().audit_document()["maximum_positions"] is None
    assert (
        IntradayPaperRiskConfig().audit_document()["position_count_limit_enabled"]
        is False
    )
    price_policy = IntradayPaperRiskConfig().audit_document()[
        "price_acceptance_policy"
    ]
    assert isinstance(price_policy, dict)
    assert price_policy["version"] == "ashare-intraday-price-acceptance@1"
    assert (
        price_policy["continuous_auction_price_cage"]
        == "NOT_L1_VERIFIED_PAPER_MINUTE_BAR_ONLY"
    )


def test_explicit_position_count_circuit_breaker_remains_available() -> None:
    outcome = build_intraday_buy_order(
        _signal(),
        _account(
            positions=tuple(_position(f"00000{item}.SZ") for item in range(1, 6))
        ),
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
        config=IntradayPaperRiskConfig(maximum_positions=5),
    )

    assert outcome.status is IntradayPaperRiskStatus.REJECTED
    assert outcome.reason is IntradayPaperRiskReason.MAX_POSITIONS_REACHED
    assert outcome.order is None


def test_removing_count_cap_does_not_bypass_cash_reserve_or_round_lot() -> None:
    outcome = build_intraday_buy_order(
        _signal(),
        _account(
            cash="40225.47",
            positions=tuple(
                _position(f"00000{item}.SZ", quantity=3180)
                for item in range(1, 6)
            ),
        ),
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
    )

    assert outcome.status is IntradayPaperRiskStatus.REJECTED
    assert outcome.reason is IntradayPaperRiskReason.BELOW_BOARD_MINIMUM_BUY
    assert outcome.available_cash_budget == Decimal("225.47")
    assert outcome.order is None


@pytest.mark.parametrize("value", (0, -1, True, 1.5))
def test_position_count_circuit_breaker_rejects_invalid_values(value: object) -> None:
    with pytest.raises(ValueError, match="maximum_positions"):
        IntradayPaperRiskConfig(maximum_positions=value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("account", "reason"),
    [
        (
            _account(
                cash="40000",
                positions=(_position("000001.SZ", quantity=16_000),),
            ),
            IntradayPaperRiskReason.GROSS_LIMIT_REACHED,
        ),
        (
            _account(
                cash="160000",
                positions=(_position("600000.SH", quantity=4000),),
            ),
            IntradayPaperRiskReason.SYMBOL_LIMIT_REACHED,
        ),
        (
            _account(cash="40000"),
            IntradayPaperRiskReason.CASH_RESERVE_BINDING,
        ),
    ],
)
def test_portfolio_and_cash_risk_limits_fail_closed(
    account: PaperAccountSnapshot,
    reason: IntradayPaperRiskReason,
) -> None:
    outcome = build_intraday_buy_order(
        _signal(),
        account,
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
    )
    assert outcome.status is IntradayPaperRiskStatus.REJECTED
    assert outcome.reason is reason
    assert outcome.order is None


def test_cash_budget_includes_minimum_commission_and_transfer_fee() -> None:
    fee_blocked = build_intraday_buy_order(
        _signal(stop="8"),
        _account(cash="41006"),
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
    )
    funded = build_intraday_buy_order(
        _signal(stop="8"),
        _account(cash="41007"),
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
    )
    # 人民币 1,006 元在不计费用时足以按涨停价买入 100 股，但计入人民币 5 元
    # 最低佣金及过户费后资金不足。
    assert fee_blocked.reason is IntradayPaperRiskReason.BELOW_BOARD_MINIMUM_BUY
    assert fee_blocked.order is None
    assert funded.order is not None and funded.order.quantity == 100


def test_star_limit_buy_uses_200_minimum_then_one_share_increments() -> None:
    rule = intraday_order_quantity_rule(AShareBoard.STAR)
    assert rule.floor_buy_submission(Decimal("199.99")) == 0
    assert rule.floor_buy_submission(Decimal("201.99")) == 201
    assert not rule.accepts_buy_submission(199)
    assert rule.accepts_buy_submission(200)
    assert rule.accepts_buy_submission(201)
    assert rule.maximum_limit_order_quantity == 100_000

    outcome = build_intraday_buy_order(
        _signal(symbol="688001.SH"),
        _account(cash="42017.10"),
        board=AShareBoard.STAR,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
    )
    assert outcome.status is IntradayPaperRiskStatus.APPROVED
    assert outcome.order is not None and outcome.order.quantity == 201


def test_star_buy_below_200_after_fees_is_rejected() -> None:
    outcome = build_intraday_buy_order(
        _signal(symbol="688001.SH"),
        _account(cash="42007.00"),
        board=AShareBoard.STAR,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
    )
    assert outcome.status is IntradayPaperRiskStatus.REJECTED
    assert outcome.reason is IntradayPaperRiskReason.BELOW_BOARD_MINIMUM_BUY
    assert outcome.order is None


def test_sell_quantity_plan_preserves_one_indivisible_residual_order() -> None:
    star_residual = build_intraday_sell_quantity_plan(
        available_to_sell=199,
        board=AShareBoard.STAR,
    )
    star_regular = build_intraday_sell_quantity_plan(
        available_to_sell=201,
        board=AShareBoard.STAR,
    )
    star_large = build_intraday_sell_quantity_plan(
        available_to_sell=100_001,
        board=AShareBoard.STAR,
    )
    main_residual = build_intraday_sell_quantity_plan(
        available_to_sell=150,
        board=AShareBoard.SSE_MAIN,
    )

    assert star_residual.regular_order_quantities == ()
    assert star_residual.residual_sell_all_quantity == 199
    assert star_residual.residual_component_quantity == 199
    assert star_regular.regular_order_quantities == (201,)
    assert star_regular.residual_sell_all_quantity == 0
    # 不得因先提交 100000 股而人为制造一股零股余额。
    assert star_large.regular_order_quantities == (99_801, 200)
    assert star_large.residual_sell_all_quantity == 0
    # 主板的 50 股零股可以随常规的 100 股一并卖出，但必须一次提交完整的
    # 150 股余额。
    assert main_residual.regular_order_quantities == ()
    assert main_residual.residual_sell_all_quantity == 150
    assert main_residual.residual_component_quantity == 50


def test_quantity_policy_is_complete_in_risk_audit_document() -> None:
    policy = IntradayPaperRiskConfig().audit_document()["order_quantity_policy"]
    assert isinstance(policy, dict)
    assert policy["version"] == "ashare-intraday-order-quantity@1"
    star = policy["STAR"]
    assert isinstance(star, dict)
    assert star == {
        "minimum_buy_quantity": 200,
        "buy_increment": 1,
        "maximum_limit_order_quantity": 100_000,
        "paper_partial_fill_increment": 1,
        "minimum_regular_sell_quantity": 200,
        "sell_increment": 1,
        "sell_residual_policy": "BELOW_200_SELL_ALL_ONCE",
    }


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"board": None}, IntradayPaperRiskReason.BOARD_MISSING),
        ({"board": AShareBoard.BSE}, IntradayPaperRiskReason.BOARD_UNSUPPORTED),
        (
            {"board": AShareBoard.CHINEXT},
            IntradayPaperRiskReason.BOARD_SYMBOL_MISMATCH,
        ),
        ({"previous_close": None}, IntradayPaperRiskReason.PREVIOUS_CLOSE_MISSING),
        (
            {"signal": _signal(price="12")},
            IntradayPaperRiskReason.PRICE_OUTSIDE_DAILY_BAND,
        ),
    ],
)
def test_missing_rules_and_daily_price_bands_reject_before_order(
    kwargs: dict[str, object],
    reason: IntradayPaperRiskReason,
) -> None:
    signal = kwargs.get("signal", _signal())
    assert isinstance(signal, TechnicalSignal)
    board = kwargs.get("board", AShareBoard.SSE_MAIN)
    previous = kwargs.get("previous_close", "10")
    outcome = build_intraday_buy_order(
        signal,
        _account(),
        board=board if isinstance(board, (AShareBoard, str)) else None,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal(previous) if isinstance(previous, str) else None,
        created_at=CREATED_AT,
    )
    assert outcome.reason is reason
    assert outcome.order is None


def test_non_buy_recommendation_and_late_signal_never_create_order() -> None:
    watch = build_intraday_buy_order(
        _signal(decision=RecommendationDecision.WATCH),
        _account(),
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
    )
    late = build_intraday_buy_order(
        _signal(as_of=datetime(2026, 8, 14, 14, 56, tzinfo=SHANGHAI)),
        _account(),
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=datetime(2026, 8, 14, 14, 55, tzinfo=SHANGHAI),
        previous_close=Decimal("10"),
        created_at=datetime(2026, 8, 14, 14, 56, tzinfo=SHANGHAI),
    )
    assert watch.reason is IntradayPaperRiskReason.SIGNAL_NOT_ENTER_CANDIDATE
    assert late.reason is IntradayPaperRiskReason.ENTRY_CUTOFF_PASSED


def test_same_or_overlapping_bar_is_rejected_without_lookahead() -> None:
    order = _approved_order()
    same_bar = _bar(
        start=datetime(2026, 8, 14, 9, 59, tzinfo=SHANGHAI)
    )
    overlapping = _bar(start=SIGNAL_END)

    for bar in (same_bar, overlapping):
        outcome = match_intraday_buy_order(order, bar, match_revision="r1")
        assert outcome.status is IntradayPaperMatchStatus.REJECTED
        assert outcome.reason is IntradayPaperMatchReason.BAR_NOT_STRICTLY_AFTER_SIGNAL
        assert outcome.fill is None


def test_bar_started_before_order_creation_is_rejected_without_lookahead() -> None:
    delayed_order = replace(
        _approved_order(),
        created_at=datetime(2026, 8, 14, 10, 1, 30, tzinfo=SHANGHAI),
    )
    # 延迟订单创建时 10:01 区间已经开始，因此该区间最终形成的最高价和
    # 最低价不能作为成交证据。
    already_in_progress = _bar(
        start=datetime(2026, 8, 14, 10, 1, tzinfo=SHANGHAI),
        fetched_at=datetime(2026, 8, 14, 10, 2, tzinfo=SHANGHAI),
    )

    outcome = match_intraday_buy_order(
        delayed_order,
        already_in_progress,
        match_revision="delayed-r1",
    )

    assert outcome.status is IntradayPaperMatchStatus.REJECTED
    assert outcome.reason is IntradayPaperMatchReason.BAR_STARTED_BEFORE_ORDER
    assert outcome.fill is None


def test_full_fill_uses_adverse_slippage_without_crossing_limit() -> None:
    order = _approved_order()
    outcome = match_intraday_buy_order(order, _bar(), match_revision="bars-r1")

    assert outcome.status is IntradayPaperMatchStatus.FILLED
    assert outcome.reason is IntradayPaperMatchReason.FILLED
    assert outcome.filled_quantity == order.quantity
    assert outcome.cancelled_quantity == 0
    assert outcome.fill_price == Decimal("10.01")
    assert outcome.fill_price <= order.limit_price
    assert outcome.fill is not None
    assert outcome.fill.source is PaperFillSource.SIMULATED
    assert outcome.fill.external_order_id == order.order_id
    assert outcome.fill.executed_at == _bar().end_at


def test_completed_match_bar_that_already_hits_exit_condition_never_fills() -> None:
    order = _approved_order()
    outcome = match_intraday_buy_order(
        order,
        _bar(high="10.50"),
        match_revision="pre-exit-r1",
        protective_stop_price=Decimal("9.50"),
        take_profit_price=Decimal("10.40"),
        time_exit_at=CREATED_AT + timedelta(days=5),
    )

    assert outcome.status is IntradayPaperMatchStatus.NOT_FILLED_IOC
    assert outcome.reason is IntradayPaperMatchReason.EXIT_CONDITION_MET_BEFORE_FILL
    assert outcome.fill is None


def test_gap_above_limit_that_trades_back_fills_at_limit_not_above_it() -> None:
    order = _approved_order()
    outcome = match_intraday_buy_order(
        order,
        _bar(open_price="10.05", high="10.10", low="10.00"),
        match_revision="gap-r1",
    )
    assert outcome.fill_price == order.limit_price


def test_buy_price_acceptance_freezes_strategy_and_exchange_bounds() -> None:
    acceptance = build_intraday_buy_price_acceptance(
        reference_price=Decimal("10"),
        invalidation_price=Decimal("9.80"),
        previous_close=Decimal("10"),
        board=AShareBoard.SSE_MAIN,
    )

    assert acceptance.limit_price == Decimal("10.01")
    assert acceptance.acceptable_lower == Decimal("9.81")
    assert acceptance.acceptable_upper == Decimal("10.01")
    assert acceptance.exchange_lower == Decimal("9.00")
    assert acceptance.exchange_upper == Decimal("11.00")
    assert not acceptance.contains(Decimal("9.80"))
    assert acceptance.contains(Decimal("9.81"))


def test_buy_markup_is_capped_at_daily_limit_instead_of_rejecting_signal() -> None:
    acceptance = build_intraday_buy_price_acceptance(
        reference_price=Decimal("11.00"),
        invalidation_price=Decimal("10.00"),
        previous_close=Decimal("10.00"),
        board=AShareBoard.SSE_MAIN,
    )

    assert acceptance.exchange_upper == Decimal("11.00")
    assert acceptance.limit_price == Decimal("11.00")
    assert acceptance.acceptable_lower == Decimal("10.01")
    assert acceptance.acceptable_upper == Decimal("11.00")

    outcome = build_intraday_buy_order(
        _signal(price="11.00", stop="10.00"),
        _account(),
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10.00"),
        created_at=CREATED_AT,
    )
    assert outcome.status is IntradayPaperRiskStatus.APPROVED
    assert outcome.order is not None
    assert outcome.order.limit_price == Decimal("11.00")


def test_sell_price_acceptance_sets_minimum_limit_and_better_price_ceiling() -> None:
    acceptance = build_intraday_sell_price_acceptance(
        reference_price=Decimal("10"),
        previous_close=Decimal("10"),
        board=AShareBoard.SSE_MAIN,
    )

    assert acceptance.limit_price == Decimal("9.99")
    assert acceptance.acceptable_lower == Decimal("9.99")
    assert acceptance.acceptable_upper == Decimal("11.00")
    assert not acceptance.contains(Decimal("9.98"))
    assert acceptance.contains(Decimal("10.50"))


@pytest.mark.parametrize(
    "bar",
    [
        _bar(open_price="10.00", high="10.10", low="9.80", close="9.90"),
        _bar(open_price="9.75", high="9.90", low="9.70", close="9.85"),
    ],
)
def test_broken_invalidation_is_never_treated_as_a_cheaper_buy(bar: IntradayBar) -> None:
    order = _approved_order()

    outcome = match_intraday_buy_order(
        order,
        bar,
        match_revision="invalidated-r1",
    )

    assert outcome.status is IntradayPaperMatchStatus.NOT_FILLED_IOC
    assert outcome.reason is IntradayPaperMatchReason.SIGNAL_INVALIDATED_BEFORE_FILL
    assert outcome.fill is None


def test_volume_capacity_partially_fills_once_and_cancels_remainder() -> None:
    order = _approved_order()
    outcome = match_intraday_buy_order(
        order,
        _bar(volume_lots=100),
        match_revision="thin-r1",
    )
    assert outcome.status is IntradayPaperMatchStatus.PARTIALLY_FILLED_IOC
    assert outcome.reason is IntradayPaperMatchReason.PARTIAL_VOLUME_CAPACITY
    assert outcome.volume_capacity == 100
    assert outcome.filled_quantity == 100
    assert outcome.cancelled_quantity == order.quantity - 100
    assert outcome.fill is not None and outcome.fill.quantity == 100


def test_star_paper_match_supports_one_share_partial_fill_capacity() -> None:
    order = _approved_order(
        account=_account(cash="42017.10"),
        signal=_signal(symbol="688001.SH"),
        board=AShareBoard.STAR,
    )
    assert order.quantity == 201
    outcome = match_intraday_buy_order(
        order,
        _bar(symbol="688001.SH", volume_lots=1),
        match_revision="star-thin-r1",
    )
    assert outcome.status is IntradayPaperMatchStatus.PARTIALLY_FILLED_IOC
    assert outcome.volume_capacity == 1
    assert outcome.filled_quantity == 1
    assert outcome.cancelled_quantity == 200
    assert outcome.fill is not None and outcome.fill.quantity == 1


def test_legacy_star_100_share_order_is_rejected_before_match() -> None:
    valid = _approved_order(
        account=_account(cash="42017.10"),
        signal=_signal(symbol="688001.SH"),
        board=AShareBoard.STAR,
    )
    legacy_invalid = replace(valid, quantity=100)
    outcome = match_intraday_buy_order(
        legacy_invalid,
        _bar(symbol="688001.SH"),
        match_revision="legacy-star-r1",
    )
    assert outcome.status is IntradayPaperMatchStatus.REJECTED
    assert (
        outcome.reason
        is IntradayPaperMatchReason.ORDER_QUANTITY_OUTSIDE_BOARD_RULES
    )
    assert outcome.fill is None


def test_sub_lot_capacity_and_untouched_limit_do_not_fill() -> None:
    order = _approved_order()
    thin = match_intraday_buy_order(
        order,
        _bar(volume_lots=99),
        match_revision="thin-r1",
    )
    untouched = match_intraday_buy_order(
        order,
        _bar(open_price="10.10", high="10.20", low="10.02", close="10.15"),
        match_revision="away-r1",
    )
    assert thin.reason is IntradayPaperMatchReason.VOLUME_CAPACITY_BELOW_LOT
    assert untouched.reason is IntradayPaperMatchReason.LIMIT_NOT_TOUCHED
    assert thin.fill is None and untouched.fill is None


def test_locked_limit_up_bar_never_assumes_buy_queue_priority() -> None:
    order = replace(_approved_order(), limit_price=Decimal("11.00"))
    locked = _bar(
        open_price="11.00",
        high="11.00",
        low="11.00",
        close="11.00",
    )

    outcome = match_intraday_buy_order(order, locked, match_revision="locked-up-r1")

    assert outcome.status is IntradayPaperMatchStatus.NOT_FILLED_IOC
    assert outcome.reason is IntradayPaperMatchReason.LOCKED_LIMIT_UP_QUEUE_UNMODELED
    assert outcome.fill is None


@pytest.mark.parametrize(
    ("mutator", "reason"),
    [
        (
            lambda bar: replace(bar, is_closed=False),
            IntradayPaperMatchReason.BAR_NOT_CLOSED,
        ),
        (
            lambda bar: replace(bar, volume_lots=0),
            IntradayPaperMatchReason.BAR_VOLUME_ZERO,
        ),
        (
            lambda bar: replace(
                bar,
                open=Decimal("11.01"),
                high=Decimal("11.05"),
                low=Decimal("10.00"),
            ),
            IntradayPaperMatchReason.BAR_OUTSIDE_DAILY_BAND,
        ),
    ],
)
def test_bad_or_untradeable_bar_fails_closed(mutator, reason) -> None:
    order = _approved_order()
    outcome = match_intraday_buy_order(
        order,
        mutator(_bar()),
        match_revision="bad-r1",
    )
    assert outcome.reason is reason
    assert outcome.fill is None


def test_runtime_missing_ohlc_is_rejected_even_if_transport_type_was_bypassed() -> None:
    order = _approved_order()
    bar = _bar()
    object.__setattr__(bar, "open", None)
    outcome = match_intraday_buy_order(order, bar, match_revision="missing-r1")
    assert outcome.reason is IntradayPaperMatchReason.BAR_OHLC_INVALID
    assert outcome.fill is None


def test_fill_identifier_is_stable_for_replay_and_changes_with_revision() -> None:
    order = _approved_order()
    first = match_intraday_buy_order(order, _bar(), match_revision="r1")
    replay = match_intraday_buy_order(order, _bar(), match_revision="r1")
    revised = match_intraday_buy_order(order, _bar(), match_revision="r2")
    assert first.fill is not None and replay.fill is not None and revised.fill is not None
    assert first.fill.fill_id == replay.fill.fill_id
    assert first.fill.fill_id != revised.fill.fill_id


def test_etf_uses_mill_price_tick_and_supported_fee_class() -> None:
    order = _approved_order(instrument_type=PaperInstrumentType.ETF)
    assert order.limit_price == Decimal("10.010")
    outcome = match_intraday_buy_order(order, _bar(), match_revision="etf-r1")
    assert outcome.fill is not None
    assert outcome.fill.instrument_type is PaperInstrumentType.ETF
    assert outcome.fill_price == Decimal("10.002")


def test_reduce_signal_is_outside_buy_matcher_and_generates_no_sell() -> None:
    outcome = build_intraday_buy_order(
        _signal(decision=RecommendationDecision.REDUCE),
        _account(),
        board=AShareBoard.SSE_MAIN,
        signal_bar_end=SIGNAL_END,
        previous_close=Decimal("10"),
        created_at=CREATED_AT,
    )
    assert outcome.reason is IntradayPaperRiskReason.SIGNAL_NOT_ENTER_CANDIDATE
    assert outcome.order is None
