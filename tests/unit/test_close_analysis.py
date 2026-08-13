from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.domain.recommendations import RecommendationDecision
from gribuki_trade.features.close_analysis import (
    CloseAnalysisConfig,
    CloseDiagnosticHorizon,
    CloseInstrumentType,
    CloseSignalFamilyStatus,
    _build_horizon_views,
    _wilder_adx,
    build_close_technical_assessment,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


def daily_bars(
    *,
    count: int = 220,
    trend: str = "up",
    breakout: bool = True,
) -> tuple[DailyBar, ...]:
    closes: list[Decimal] = []
    for index in range(count):
        alternating = Decimal("0.06") if index % 2 else Decimal("-0.03")
        if trend == "up":
            close = Decimal("10") + Decimal(index) * Decimal("0.02") + alternating
        else:
            close = Decimal("15") - Decimal(index) * Decimal("0.04") + alternating
        closes.append(close)

    if trend == "up" and breakout and count > 21:
        closes[-1] = max(closes[-21:-1]) + Decimal("0.12")

    start = date(2026, 5, 1)
    output: list[DailyBar] = []
    for index, close in enumerate(closes):
        output.append(
            DailyBar(
                symbol="510300.SH",
                trade_date=start + timedelta(days=index),
                open=close - Decimal("0.01"),
                high=close + Decimal("0.05"),
                low=close - Decimal("0.05"),
                close=close,
                previous_close=closes[index - 1] if index else close - Decimal("0.02"),
                volume=180_000 if index == count - 1 and breakout else 100_000,
                amount=close * Decimal("100000"),
                turnover_percent=Decimal("0.8"),
                is_trading=True,
                is_st=False,
                adjustment=PriceAdjustment.NONE,
            )
        )
    return tuple(output)


def assessment_time(items: tuple[DailyBar, ...], *, at: time = time(16, 0)) -> datetime:
    return datetime.combine(items[-1].trade_date, at, tzinfo=SHANGHAI)


def test_confirmed_daily_breakout_is_next_session_entry_candidate() -> None:
    items = daily_bars()
    result = build_close_technical_assessment(
        "510300.sh",
        items,
        as_of=assessment_time(items),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
    )

    metrics = dict(result.metrics)
    assert result.decision is RecommendationDecision.ENTER_CANDIDATE
    assert result.score >= Decimal("0.65")
    assert result.reference_price == items[-1].close
    assert result.invalidation_price is not None
    assert result.invalidation_price < result.reference_price
    assert result.next_session == items[-1].trade_date + timedelta(days=1)
    assert result.latest_trade_date == items[-1].trade_date
    assert "TWENTY_DAY_CLOSE_BREAKOUT" in result.reason_codes
    assert "BREAKOUT_VOLUME_CONFIRMED" in result.reason_codes
    assert "MULTIFACTOR_ENTRY_SETUP" in result.reason_codes
    assert metrics["ma_5"] > metrics["ma_20"] > metrics["ma_60"]
    assert metrics["volume_ratio_20"] == Decimal("1.8")


def test_provider_st_flag_is_ignored_only_for_explicit_etf() -> None:
    items = list(daily_bars())
    items[-1] = replace(items[-1], is_st=True)

    etf = build_close_technical_assessment(
        "510300.SH",
        tuple(items),
        as_of=assessment_time(tuple(items)),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
        instrument_type=CloseInstrumentType.ETF,
    )
    stock = build_close_technical_assessment(
        "510300.SH",
        tuple(items),
        as_of=assessment_time(tuple(items)),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
    )

    assert "PROVIDER_ST_FLAG_IGNORED_FOR_ETF" in etf.reason_codes
    assert "ST_RISK_FLAG" not in etf.reason_codes
    assert etf.decision is RecommendationDecision.ENTER_CANDIDATE
    assert "ST_RISK_FLAG" in stock.reason_codes
    assert stock.decision is RecommendationDecision.WATCH


def test_held_downtrend_emits_reduce_without_an_invalidation_entry_level() -> None:
    items = daily_bars(trend="down", breakout=False)
    result = build_close_technical_assessment(
        "510300.SH",
        items,
        as_of=assessment_time(items),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
        is_currently_held=True,
    )

    assert result.decision is RecommendationDecision.REDUCE
    assert result.score <= Decimal("-0.65")
    assert result.reference_price == items[-1].close
    assert result.invalidation_price is None
    assert "DAILY_TREND_EXIT_TRIGGER" in result.reason_codes


def test_minimum_history_abstains_instead_of_using_partial_windows() -> None:
    items = daily_bars(count=40)
    result = build_close_technical_assessment(
        "510300.SH",
        items,
        as_of=assessment_time(items),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
    )

    assert CloseAnalysisConfig().minimum_history == 201
    assert result.decision is RecommendationDecision.ABSTAIN
    assert result.reason_codes == ("INSUFFICIENT_DAILY_HISTORY",)
    assert result.reference_price is None
    assert result.trading_sessions_used == 40

    boundary = daily_bars(count=200)
    boundary_result = build_close_technical_assessment(
        "510300.SH",
        boundary,
        as_of=assessment_time(boundary),
        latest_completed_session=boundary[-1].trade_date,
        next_session=boundary[-1].trade_date + timedelta(days=1),
    )
    assert boundary_result.decision is RecommendationDecision.ABSTAIN

    sufficient = daily_bars(count=201)
    sufficient_result = build_close_technical_assessment(
        "510300.SH",
        sufficient,
        as_of=assessment_time(sufficient),
        latest_completed_session=sufficient[-1].trade_date,
        next_session=sufficient[-1].trade_date + timedelta(days=1),
    )
    assert sufficient_result.decision is not RecommendationDecision.ABSTAIN


def test_suspended_rows_are_filtered_without_forward_filling() -> None:
    items = list(daily_bars())
    items[0] = replace(
        items[0],
        open=None,
        high=None,
        low=None,
        close=None,
        previous_close=None,
        volume=0,
        amount=Decimal(0),
        is_trading=False,
    )
    result = build_close_technical_assessment(
        "510300.SH",
        tuple(items),
        as_of=assessment_time(tuple(items)),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
    )

    assert result.trading_sessions_used == 219
    assert result.latest_trade_date == items[-1].trade_date


def test_missing_latest_completed_session_abstains() -> None:
    items = daily_bars()
    expected_latest = items[-1].trade_date + timedelta(days=1)
    result = build_close_technical_assessment(
        "510300.SH",
        items,
        as_of=datetime.combine(expected_latest, time(16, 0), tzinfo=SHANGHAI),
        latest_completed_session=expected_latest,
        next_session=expected_latest + timedelta(days=1),
    )

    assert result.decision is RecommendationDecision.ABSTAIN
    assert result.reason_codes == ("LATEST_COMPLETED_SESSION_MISSING",)
    assert result.latest_trade_date == items[-1].trade_date


def test_latest_completed_session_suspension_does_not_reuse_old_close() -> None:
    items = list(daily_bars())
    items[-1] = replace(
        items[-1],
        open=None,
        high=None,
        low=None,
        close=None,
        previous_close=None,
        volume=0,
        amount=Decimal(0),
        is_trading=False,
    )
    result = build_close_technical_assessment(
        "510300.SH",
        tuple(items),
        as_of=assessment_time(tuple(items)),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
    )

    assert result.decision is RecommendationDecision.ABSTAIN
    assert result.reason_codes == ("LATEST_SESSION_NOT_TRADING",)
    assert result.reference_price is None


def test_current_session_bar_is_rejected_before_close_confirmation() -> None:
    items = daily_bars()

    with pytest.raises(ValueError, match="not completed"):
        build_close_technical_assessment(
            "510300.SH",
            items,
            as_of=assessment_time(items, at=time(14, 59)),
            latest_completed_session=items[-1].trade_date,
            next_session=items[-1].trade_date + timedelta(days=1),
        )


def test_bar_from_target_session_is_rejected_as_future_information() -> None:
    items = daily_bars()
    target = items[-1].trade_date

    with pytest.raises(ValueError, match="next_session or the future"):
        build_close_technical_assessment(
            "510300.SH",
            items,
            as_of=datetime.combine(target, time(8, 0), tzinfo=SHANGHAI),
            latest_completed_session=target - timedelta(days=1),
            next_session=target,
        )


def test_adjusted_input_and_naive_as_of_are_rejected() -> None:
    items = daily_bars()
    adjusted = list(items)
    adjusted[0] = replace(adjusted[0], adjustment=PriceAdjustment.FORWARD)

    with pytest.raises(ValueError, match="unadjusted"):
        build_close_technical_assessment(
            "510300.SH",
            tuple(adjusted),
            as_of=assessment_time(items),
            latest_completed_session=items[-1].trade_date,
            next_session=items[-1].trade_date + timedelta(days=1),
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        build_close_technical_assessment(
            "510300.SH",
            items,
            as_of=datetime(2026, 7, 14, 16, 0),
            latest_completed_session=items[-1].trade_date,
            next_session=items[-1].trade_date + timedelta(days=1),
        )


def test_unadjusted_corporate_action_discontinuity_abstains() -> None:
    items = list(daily_bars())
    discontinuity_index = len(items) - 10
    prior_close = items[discontinuity_index - 1].close
    assert prior_close is not None
    items[discontinuity_index] = replace(
        items[discontinuity_index],
        previous_close=prior_close * Decimal("0.90"),
    )
    result = build_close_technical_assessment(
        "510300.SH",
        tuple(items),
        as_of=assessment_time(tuple(items)),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
    )

    assert result.decision is RecommendationDecision.ABSTAIN
    assert result.reason_codes == ("UNADJUSTED_PRICE_DISCONTINUITY",)
    assert dict(result.metrics)["max_previous_close_discontinuity"] == Decimal("0.10")


@pytest.mark.parametrize("sessions_ago", [61, 120, 200])
def test_long_horizon_corporate_action_discontinuity_abstains(
    sessions_ago: int,
) -> None:
    items = list(daily_bars())
    index = len(items) - sessions_ago
    prior_close = items[index - 1].close
    assert prior_close is not None
    items[index] = replace(
        items[index],
        previous_close=prior_close * Decimal("0.90"),
    )

    result = build_close_technical_assessment(
        "510300.SH",
        tuple(items),
        as_of=assessment_time(tuple(items)),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
    )

    assert result.decision is RecommendationDecision.ABSTAIN
    assert result.reason_codes == ("UNADJUSTED_PRICE_DISCONTINUITY",)


def test_long_momentum_skip_window_uses_t_minus_125_to_t_minus_5() -> None:
    items = daily_bars()
    result = build_close_technical_assessment(
        "510300.SH",
        items,
        as_of=assessment_time(items),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
    )
    expected = items[-6].close / items[-126].close - Decimal(1)  # type: ignore[operator]

    assert dict(result.metrics)["return_120_skip_5d"] == expected


def test_high_volume_down_day_reduces_volume_family_score() -> None:
    normal = list(daily_bars(breakout=False))
    prior = normal[-2].close
    assert prior is not None
    down_close = prior - Decimal("0.20")
    normal[-1] = replace(
        normal[-1],
        open=prior,
        high=prior + Decimal("0.02"),
        low=down_close - Decimal("0.02"),
        close=down_close,
        previous_close=prior,
        volume=100_000,
        amount=down_close * Decimal("100000"),
    )
    high_volume = list(normal)
    high_volume[-1] = replace(
        high_volume[-1],
        volume=250_000,
        amount=down_close * Decimal("250000"),
    )

    def volume_family(items: tuple[DailyBar, ...]) -> Decimal:
        result = build_close_technical_assessment(
            "510300.SH",
            items,
            as_of=assessment_time(items),
            latest_completed_session=items[-1].trade_date,
            next_session=items[-1].trade_date + timedelta(days=1),
        )
        return next(
            item.score
            for item in result.signal_families
            if item.family_id == "volume_liquidity"
        )

    assert volume_family(tuple(high_volume)) < volume_family(tuple(normal))


def test_horizon_family_contributions_reconcile_and_missing_family_is_renormalized() -> None:
    items = daily_bars()
    result = build_close_technical_assessment(
        "510300.SH",
        items,
        as_of=assessment_time(items),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
    )

    assert tuple(item.horizon for item in result.horizon_views) == (
        CloseDiagnosticHorizon.SHORT_1_TO_5_DAYS,
        CloseDiagnosticHorizon.SWING_2_TO_8_WEEKS,
    )
    for view in result.horizon_views:
        assert sum(
            (value for _, value in view.family_contributions), Decimal(0)
        ) == view.score
        assert view.coverage == Decimal(1)

    without_volume = tuple(
        replace(item, weight=Decimal(0), contribution=Decimal(0))
        if item.family_id == "volume_liquidity"
        else item
        for item in result.signal_families
    )
    normalized = _build_horizon_views(without_volume)
    short, swing = normalized
    assert short.coverage == Decimal("0.80")
    assert swing.coverage == Decimal("0.90")
    assert all(
        family_id != "volume_liquidity"
        for view in normalized
        for family_id, _ in view.family_contributions
    )
    assert sum(
        (value for _, value in short.family_contributions), Decimal(0)
    ) == short.score
    assert sum(
        (value for _, value in swing.family_contributions), Decimal(0)
    ) == swing.score


def test_family_zero_scores_distinguish_neutral_inactive_and_unavailable() -> None:
    items = tuple(
        replace(
            item,
            open=Decimal("10"),
            high=Decimal("10.05"),
            low=Decimal("9.95"),
            close=Decimal("10"),
            previous_close=Decimal("10"),
            amount=Decimal(item.volume) * Decimal("10"),
        )
        for item in daily_bars(breakout=False)
    )
    result = build_close_technical_assessment(
        "510300.SH",
        items,
        as_of=assessment_time(items),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
    )
    families = {item.family_id: item for item in result.signal_families}

    assert families["trend_structure"].score == 0
    assert families["trend_structure"].status is CloseSignalFamilyStatus.NEUTRAL
    assert families["trend_pullback"].score == 0
    assert families["trend_pullback"].status is CloseSignalFamilyStatus.INACTIVE
    assert families["relative_strength_breadth"].score == 0
    assert (
        families["relative_strength_breadth"].status
        is CloseSignalFamilyStatus.UNAVAILABLE
    )


def test_liquidity_metrics_expose_readable_scale_and_source_coverage() -> None:
    items = daily_bars()
    result = build_close_technical_assessment(
        "510300.SH",
        items,
        as_of=assessment_time(items),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
    )
    metrics = dict(result.metrics)

    assert metrics["amihud_coverage_20"] == 1
    assert metrics["amihud_coverage_60"] == 1
    assert metrics["amihud_bps_per_cny_billion_20"] == (
        metrics["amihud_20"] * Decimal("10000000000000")
    )
    assert metrics["amihud_bps_per_cny_billion_60"] == (
        metrics["amihud_60"] * Decimal("10000000000000")
    )


def test_missing_turnover_and_amount_are_not_silently_presented_as_observed_zero() -> None:
    source = daily_bars()
    items = tuple(
        replace(item, turnover_percent=None, amount=Decimal(0))
        for item in source
    )
    result = build_close_technical_assessment(
        "510300.SH",
        items,
        as_of=assessment_time(items),
        latest_completed_session=items[-1].trade_date,
        next_session=items[-1].trade_date + timedelta(days=1),
    )
    metrics = dict(result.metrics)

    assert metrics["turnover_state_20"] == 1
    assert metrics["turnover_coverage_20"] == 0
    assert metrics["amihud_20"] == 0
    assert metrics["amihud_coverage_20"] == 0
    assert metrics["amihud_60"] == 0
    assert metrics["amihud_coverage_60"] == 0


def test_short_and_swing_views_distinguish_fast_reversal_from_slow_trend() -> None:
    fast = list(daily_bars(trend="down", breakout=False))
    pivot = fast[-11].close
    assert pivot is not None
    for offset in range(10):
        index = len(fast) - 10 + offset
        close = pivot + Decimal("0.10") * Decimal(offset + 1)
        previous_close = fast[index - 1].close
        assert previous_close is not None
        volume = 250_000 if offset == 9 else 100_000
        fast[index] = replace(
            fast[index],
            open=close - Decimal("0.01"),
            high=close + Decimal("0.05"),
            low=close - Decimal("0.05"),
            close=close,
            previous_close=previous_close,
            volume=volume,
            amount=close * Decimal(volume),
        )
    fast_items = tuple(fast)
    fast_result = build_close_technical_assessment(
        "510300.SH",
        fast_items,
        as_of=assessment_time(fast_items),
        latest_completed_session=fast_items[-1].trade_date,
        next_session=fast_items[-1].trade_date + timedelta(days=1),
    )
    slow_items = daily_bars(trend="up", breakout=False)
    slow_result = build_close_technical_assessment(
        "510300.SH",
        slow_items,
        as_of=assessment_time(slow_items),
        latest_completed_session=slow_items[-1].trade_date,
        next_session=slow_items[-1].trade_date + timedelta(days=1),
    )

    fast_short, fast_swing = fast_result.horizon_views
    slow_short, slow_swing = slow_result.horizon_views
    assert fast_short.score > fast_swing.score
    assert slow_swing.score > slow_short.score


def test_wilder_adx_14_matches_golden_ohlc_fixture() -> None:
    # Golden values are fixed from the canonical Wilder seed-and-recurrence
    # worksheet for this deterministic 40-bar OHLC fixture.
    adx, positive_di, negative_di = _wilder_adx(
        daily_bars(count=40, trend="up", breakout=False),
        14,
    )

    assert adx.quantize(Decimal("0.000001")) == Decimal("23.672012")
    assert positive_di.quantize(Decimal("0.000001")) == Decimal("40.719675")
    assert negative_di.quantize(Decimal("0.000001")) == Decimal("23.783306")


@pytest.mark.parametrize("amount", [Decimal("NaN"), Decimal("Infinity")])
def test_non_finite_amount_is_rejected(amount: Decimal) -> None:
    items = list(daily_bars())
    items[-1] = replace(items[-1], amount=amount)

    with pytest.raises(ValueError, match="volume and amount"):
        build_close_technical_assessment(
            "510300.SH",
            tuple(items),
            as_of=assessment_time(tuple(items)),
            latest_completed_session=items[-1].trade_date,
            next_session=items[-1].trade_date + timedelta(days=1),
        )


def test_old_last_trading_bar_abstains_even_if_target_session_is_future() -> None:
    items = daily_bars()
    next_session = items[-1].trade_date + timedelta(days=20)
    result = build_close_technical_assessment(
        "510300.SH",
        items,
        as_of=assessment_time(items),
        latest_completed_session=items[-1].trade_date,
        next_session=next_session,
    )

    assert result.decision is RecommendationDecision.ABSTAIN
    assert result.reason_codes == ("STALE_DAILY_MARKET_DATA",)
