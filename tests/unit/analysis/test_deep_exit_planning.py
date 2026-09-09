from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.features.deep_exit_planning import (
    DeepExitTimeframe,
    DeepSemanticAssessment,
    aggregate_completed_bars,
    build_deep_exit_plan,
)
from gribuki_trade.features.exit_planning import build_quick_exit_plan
from gribuki_trade.features.technical import TechnicalBar

SHANGHAI = ZoneInfo("Asia/Shanghai")
DECISION = datetime(2026, 8, 14, 14, 30, 30, tzinfo=SHANGHAI)


def _minute_bars(count: int = 450) -> tuple[TechnicalBar, ...]:
    first = DECISION.replace(second=0, microsecond=0) - timedelta(minutes=count)
    output: list[TechnicalBar] = []
    for index in range(count):
        end = first + timedelta(minutes=index + 1)
        drift = Decimal(index % 30) * Decimal("0.002")
        close = Decimal("10.00") + drift
        output.append(
            TechnicalBar(
                end_time=end,
                available_at=end + timedelta(seconds=3),
                open=close - Decimal("0.01"),
                high=close + Decimal("0.08"),
                low=close - Decimal("0.08"),
                close=close,
                volume=1000 + index,
            )
        )
    return tuple(output)


def _quick():
    bars = _minute_bars()
    return build_quick_exit_plan(
        account_id="paper-deep",
        protection_id="protect-deep-generator",
        symbol="600000.SH",
        bars=bars,
        decision_at=DECISION,
        time_exit_at=DECISION + timedelta(days=7),
        worst_entry_price=Decimal("10.20"),
        technical_invalidation_price=Decimal("9.70"),
        strategy_version="deep-test@1",
    ).plan


def _frames() -> tuple[DeepExitTimeframe, ...]:
    bars = _minute_bars()
    return (
        DeepExitTimeframe(
            "1m",
            aggregate_completed_bars(bars, interval_minutes=1),
            Decimal("0.2"),
            timedelta(minutes=5),
        ),
        DeepExitTimeframe(
            "5m",
            aggregate_completed_bars(bars, interval_minutes=5),
            Decimal("0.3"),
            timedelta(minutes=10),
        ),
        DeepExitTimeframe(
            "15m",
            aggregate_completed_bars(bars, interval_minutes=15),
            Decimal("0.5"),
            timedelta(minutes=20),
        ),
    )


def _assessment(system: str, score: str) -> DeepSemanticAssessment:
    return DeepSemanticAssessment(
        assessment_id=f"assessment-{system}",
        system=system,
        score=Decimal(score),
        confidence=Decimal("0.9"),
        market_data_as_of=DECISION - timedelta(seconds=5),
        evidence_ids=(f"evidence-{system}",),
    )


def test_deep_plan_is_deterministic_multiframe_and_never_loosens_hard_risk() -> None:
    quick = _quick()
    first = build_deep_exit_plan(
        quick,
        timeframes=_frames(),
        decision_at=DECISION,
        baseline_assessment=_assessment("baseline", "0.3"),
        adversarial_assessment=_assessment("adversarial", "0.9"),
    )
    second = build_deep_exit_plan(
        quick,
        timeframes=_frames(),
        decision_at=DECISION,
        baseline_assessment=_assessment("baseline", "0.3"),
        adversarial_assessment=_assessment("adversarial", "0.9"),
    )

    assert first == second
    assert first.plan.stop_price >= quick.stop_price
    assert first.plan.time_exit_at <= quick.time_exit_at
    assert first.plan.reward_to_risk == Decimal("3.0")
    assert first.selected_semantic_system == "adversarial"
    assert len(first.timeframe_calculations) == 3


def test_missing_semantics_yields_explicit_degraded_deterministic_plan() -> None:
    result = build_deep_exit_plan(
        _quick(),
        timeframes=_frames(),
        decision_at=DECISION,
    )

    assert result.plan.state.value == "DEGRADED"
    assert result.selected_semantic_system == "DETERMINISTIC_ONLY"
    assert "SEMANTIC_DEGRADED_FALLBACK" in result.plan.reason_codes


def test_deep_plan_rejects_future_or_not_yet_available_evidence() -> None:
    frames = _frames()
    bad_bar = frames[0].bars[-1]
    bad = TechnicalBar(
        end_time=bad_bar.end_time,
        available_at=DECISION + timedelta(seconds=1),
        open=bad_bar.open,
        high=bad_bar.high,
        low=bad_bar.low,
        close=bad_bar.close,
        volume=bad_bar.volume,
    )
    changed = DeepExitTimeframe(
        "1m",
        (*frames[0].bars[:-1], bad),
        Decimal("0.2"),
        timedelta(minutes=5),
    )

    with pytest.raises(ValueError, match="not available"):
        build_deep_exit_plan(
            _quick(),
            timeframes=(changed, *frames[1:]),
            decision_at=DECISION,
        )


def test_aggregation_drops_incomplete_or_gapped_interval() -> None:
    bars = _minute_bars(10)
    assert len(aggregate_completed_bars(bars, interval_minutes=5)) == 2
    gapped = (*bars[:4], *bars[5:])
    assert len(aggregate_completed_bars(gapped, interval_minutes=5)) == 1
