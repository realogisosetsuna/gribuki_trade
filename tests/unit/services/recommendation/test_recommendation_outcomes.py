from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.backtest import (
    OutcomeStatus,
    RecommendationEvaluationConfig,
    evaluate_recommendation,
    summarize_outcomes,
)
from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.domain.recommendations import (
    ConfidenceBand,
    EvidenceReference,
    RecommendationDecision,
    RecommendationHorizon,
    ResearchRecommendation,
)

AS_OF = datetime(2026, 8, 3, 7, 0, tzinfo=UTC)


def recommendation(
    decision: RecommendationDecision = RecommendationDecision.ENTER_CANDIDATE,
) -> ResearchRecommendation:
    return ResearchRecommendation(
        recommendation_id=f"rec-{decision.value}",
        symbol="600000.SH",
        as_of=AS_OF,
        expires_at=AS_OF + timedelta(days=1),
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        decision=decision,
        confidence=ConfidenceBand.UNCALIBRATED,
        technical_score=Decimal("0.6"),
        macro_score=None,
        reference_price=Decimal("10"),
        invalidation_price=Decimal("9.5"),
        reason_codes=("TEST",),
        uncertainties=(),
        evidence=(
            EvidenceReference(
                evidence_id="test-evidence",
                title="Retained evidence",
                canonical_url="local://test-evidence",
                published_at=AS_OF - timedelta(minutes=2),
                first_seen_at=AS_OF - timedelta(minutes=1),
                source_tier=2,
            ),
        ),
        strategy_version="test@1",
    )


def bar(day: int, *, open_price: str, close: str, high: str, low: str) -> DailyBar:
    return DailyBar(
        symbol="600000.SH",
        trade_date=date(2026, 8, day),
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        previous_close=None,
        volume=1_000_000,
        amount=Decimal("10000000"),
        turnover_percent=Decimal("1"),
        is_trading=True,
        is_st=False,
        adjustment=PriceAdjustment.NONE,
    )


def future_bars() -> tuple[DailyBar, ...]:
    return (
        bar(4, open_price="10", close="10.1", high="10.2", low="9.8"),
        bar(5, open_price="10.1", close="10.3", high="10.4", low="10"),
        bar(6, open_price="10.3", close="10.2", high="10.5", low="10.1"),
        bar(7, open_price="10.2", close="10.6", high="10.7", low="10.1"),
        bar(10, open_price="10.6", close="11", high="11.2", low="10.5"),
    )


def test_entry_uses_next_session_open_and_reports_cost_adjusted_return() -> None:
    result = evaluate_recommendation(recommendation(), future_bars())

    assert result.status is OutcomeStatus.COMPLETE
    assert result.entry_date == date(2026, 8, 4)
    assert result.exit_date == date(2026, 8, 10)
    assert result.gross_return == Decimal("0.1")
    assert result.net_return_after_costs is not None
    assert result.net_return_after_costs < result.gross_return
    assert result.max_favorable_excursion == Decimal("0.12")
    assert result.max_adverse_excursion == Decimal("-0.02")
    assert result.direction_correct is True


def test_incomplete_horizon_and_non_directional_decisions_do_not_fake_scores() -> None:
    pending = evaluate_recommendation(recommendation(), future_bars()[:4])
    watch = evaluate_recommendation(
        recommendation(RecommendationDecision.WATCH),
        future_bars(),
    )

    assert pending.status is OutcomeStatus.PENDING
    assert pending.reason_code == "HORIZON_NOT_COMPLETE"
    assert watch.status is OutcomeStatus.UNEVALUABLE
    assert watch.direction_correct is None


def test_reduce_is_correct_only_when_asset_falls() -> None:
    falling = tuple(
        replace(item, close=Decimal("9"), high=Decimal("10"), low=Decimal("8.9"))
        for item in future_bars()
    )
    result = evaluate_recommendation(
        recommendation(RecommendationDecision.REDUCE),
        falling,
    )

    assert result.status is OutcomeStatus.COMPLETE
    assert result.direction_correct is True
    assert result.net_return_after_costs is None


def test_rejects_same_day_or_adjusted_future_data() -> None:
    same_day = replace(future_bars()[0], trade_date=AS_OF.date())
    with pytest.raises(ValueError, match="strictly after"):
        evaluate_recommendation(recommendation(), (same_day, *future_bars()[1:]))

    adjusted = replace(future_bars()[0], adjustment=PriceAdjustment.FORWARD)
    with pytest.raises(ValueError, match="unadjusted"):
        evaluate_recommendation(recommendation(), (adjusted, *future_bars()[1:]))


def test_summary_separates_pending_and_unevaluable() -> None:
    complete = evaluate_recommendation(recommendation(), future_bars())
    pending = evaluate_recommendation(recommendation(), future_bars()[:1])
    watch = evaluate_recommendation(
        recommendation(RecommendationDecision.WATCH), future_bars()
    )
    summary = summarize_outcomes((complete, pending, watch))

    assert summary.total == 3
    assert summary.completed == 1
    assert summary.pending == 1
    assert summary.unevaluable == 1
    assert summary.directional_hit_rate == Decimal("1.0")


def test_too_small_notional_is_unevaluable() -> None:
    config = RecommendationEvaluationConfig(target_notional_cny=Decimal("500"))
    result = evaluate_recommendation(recommendation(), future_bars(), config=config)

    assert result.status is OutcomeStatus.UNEVALUABLE
    assert result.reason_code == "NOTIONAL_BELOW_ONE_LOT"
