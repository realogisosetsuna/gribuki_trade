from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.features.exit_planning import (
    QuickExitPlanConfig,
    build_quick_exit_plan,
)
from gribuki_trade.features.technical import TechnicalBar

SHANGHAI = ZoneInfo("Asia/Shanghai")
DECISION = datetime(2026, 8, 14, 10, 0, 30, tzinfo=SHANGHAI)


def _bars() -> tuple[TechnicalBar, ...]:
    first_end = DECISION.replace(hour=9, minute=40, second=0)
    output: list[TechnicalBar] = []
    for index in range(21):
        close = Decimal("9.74") + Decimal(index) * Decimal("0.007")
        low = close - Decimal("0.08")
        if index == 17:
            low = Decimal("9.50")
        high = min(Decimal("9.90"), close + Decimal("0.08"))
        if index == 20:
            high = Decimal("10.10")
            close = Decimal("10.00")
        end = first_end + timedelta(minutes=index)
        output.append(
            TechnicalBar(
                end_time=end,
                available_at=end + timedelta(seconds=5),
                open=close - Decimal("0.02"),
                high=high,
                low=low,
                close=close,
                volume=1000 + index,
            )
        )
    return tuple(output)


def _build(**changes: object):
    values: dict[str, object] = {
        "account_id": "paper-a",
        "protection_id": "signal-600000-1000",
        "symbol": "600000.SH",
        "bars": _bars(),
        "decision_at": DECISION,
        "time_exit_at": DECISION + timedelta(days=5),
        "worst_entry_price": Decimal("10.01"),
        "technical_invalidation_price": Decimal("9.60"),
        "strategy_version": "breakout@test",
    }
    values.update(changes)
    return build_quick_exit_plan(**values)  # type: ignore[arg-type]


def test_quick_plan_uses_structure_robust_volatility_and_registered_reward() -> None:
    result = _build()

    assert result.robust_atr > 0
    assert result.breakout_support == Decimal("9.90")
    assert result.confirmed_swing_low == Decimal("9.50")
    assert result.plan.entry_basis_price == Decimal("10.01")
    assert result.plan.stop_price < result.plan.entry_basis_price
    assert result.plan.stop_price == min(
        value for _, value in result.raw_stop_candidates
    ).quantize(Decimal("0.01"))
    assert result.plan.take_profit_price > result.plan.entry_basis_price
    assert result.plan.reward_to_risk == Decimal("1.5")
    assert result.plan.time_exit_at == (DECISION + timedelta(days=5)).astimezone(
        result.plan.time_exit_at.tzinfo
    )
    assert "PROVISIONAL_NOT_EXPECTED_RETURN_FORECAST" in result.plan.reason_codes
    assert "LOOSEST_VALID_PRETRADE_STOP" in result.plan.reason_codes


def test_same_inputs_reproduce_plan_and_snapshot_identity() -> None:
    first = _build()
    second = _build()

    assert first.plan == second.plan
    assert first.plan.feature_snapshot_sha256 == second.plan.feature_snapshot_sha256


def test_point_in_time_future_incomplete_and_stale_bars_fail_closed() -> None:
    bars = _bars()
    with pytest.raises(ValueError, match="not available"):
        _build(bars=(*bars[:-1], replace(bars[-1], available_at=DECISION + timedelta(seconds=1))))
    with pytest.raises(ValueError, match="completed bars"):
        _build(bars=(*bars[:-1], replace(bars[-1], complete=False)))
    with pytest.raises(ValueError, match="stale"):
        _build(decision_at=DECISION + timedelta(minutes=4))


def test_parameter_choice_must_come_from_registered_discrete_grid() -> None:
    with pytest.raises(ValueError, match="registered grid"):
        QuickExitPlanConfig(reward_to_risk=Decimal("1.7"))
    with pytest.raises(ValueError, match="registered grid"):
        QuickExitPlanConfig(atr_stop_multiple=Decimal("1.7"))


def test_time_barrier_must_be_calendar_resolved_and_after_decision() -> None:
    with pytest.raises(ValueError, match="after decision_at"):
        _build(time_exit_at=DECISION)


def test_bar_timestamps_must_be_timezone_aware() -> None:
    bars = _bars()
    naive_end = bars[-1].end_time.replace(tzinfo=None)
    naive_bar = replace(
        bars[-1],
        end_time=naive_end,
        available_at=naive_end + timedelta(seconds=5),
    )

    with pytest.raises(ValueError, match="bar end_time must be timezone-aware"):
        _build(bars=(*bars[:-1], naive_bar))


def test_rounding_uses_actual_tick_multiple_not_only_decimal_places() -> None:
    result = _build(config=QuickExitPlanConfig(price_tick=Decimal("0.05")))

    assert result.plan.entry_basis_price == Decimal("10.05")
    assert result.plan.stop_price % Decimal("0.05") == 0
    assert result.plan.take_profit_price % Decimal("0.05") == 0


def test_irrelevant_older_bars_do_not_change_feature_snapshot() -> None:
    bars = _bars()
    older_end = bars[0].end_time - timedelta(minutes=1)
    older = TechnicalBar(
        end_time=older_end,
        available_at=older_end + timedelta(seconds=5),
        open=Decimal("9.60"),
        high=Decimal("9.70"),
        low=Decimal("9.50"),
        close=Decimal("9.65"),
        volume=900,
    )

    assert _build().plan == _build(bars=(older, *bars)).plan
