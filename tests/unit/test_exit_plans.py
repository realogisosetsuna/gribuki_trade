from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.domain.exit_plans import (
    ExitPlan,
    ExitPlanDepth,
    ExitPlanState,
    exit_plan_document,
    exit_plan_id,
    validate_exit_plan_replacement,
)

DECISION = datetime(2026, 8, 14, 2, 0, tzinfo=UTC)


def _quick() -> ExitPlan:
    digest = "a" * 64
    return ExitPlan(
        plan_id=exit_plan_id(
            protection_id="protect-1",
            version=1,
            feature_snapshot_sha256=digest,
            policy_version="quick-exit@test",
        ),
        protection_id="protect-1",
        account_id="paper-a",
        symbol="600000.SH",
        version=1,
        depth=ExitPlanDepth.QUICK,
        state=ExitPlanState.PROVISIONAL,
        decision_at=DECISION,
        market_data_as_of=DECISION - timedelta(seconds=30),
        time_exit_at=DECISION + timedelta(days=5),
        entry_basis_price=Decimal("10.00"),
        stop_price=Decimal("9.80"),
        take_profit_price=Decimal("10.30"),
        initial_risk_per_share=Decimal("0.20"),
        reward_to_risk=Decimal("1.5"),
        price_tick=Decimal("0.01"),
        technical_invalidation_price=Decimal("9.75"),
        feature_snapshot_sha256=digest,
        policy_version="quick-exit@test",
        strategy_version="breakout@test",
        calibration_id="UNVALIDATED_GRID@test",
        reason_codes=("ROBUST_ATR_STOP",),
        metrics=(("robust_atr", Decimal("0.10")),),
    )


def _deep(previous: ExitPlan) -> ExitPlan:
    digest = "b" * 64
    return ExitPlan(
        plan_id=exit_plan_id(
            protection_id=previous.protection_id,
            version=2,
            feature_snapshot_sha256=digest,
            policy_version="deep-exit@test",
        ),
        protection_id=previous.protection_id,
        account_id=previous.account_id,
        symbol=previous.symbol,
        version=2,
        depth=ExitPlanDepth.DEEP,
        state=ExitPlanState.CONFIRMED,
        decision_at=DECISION + timedelta(minutes=2),
        market_data_as_of=DECISION + timedelta(minutes=1),
        time_exit_at=previous.time_exit_at - timedelta(days=1),
        entry_basis_price=previous.entry_basis_price,
        stop_price=Decimal("10.10"),
        take_profit_price=Decimal("10.40"),
        initial_risk_per_share=previous.initial_risk_per_share,
        reward_to_risk=Decimal("2.0"),
        price_tick=previous.price_tick,
        technical_invalidation_price=Decimal("9.90"),
        feature_snapshot_sha256=digest,
        policy_version="deep-exit@test",
        strategy_version=previous.strategy_version,
        calibration_id="HELD_OUT_DEEP@test",
        reason_codes=("DEEP_EVIDENCE_FUSED",),
        metrics=(("robust_atr", Decimal("0.12")),),
        evidence_ids=("evidence-1",),
        supersedes_plan_id=previous.plan_id,
    )


def test_quick_plan_is_auditable_and_canonical() -> None:
    plan = _quick()
    document = exit_plan_document(plan)

    assert plan.state is ExitPlanState.PROVISIONAL
    assert document["entry_basis_price"] == "10.00"
    assert document["supersedes_plan_id"] is None
    assert document["metrics"] == [["robust_atr", "0.10"]]


def test_deep_replacement_can_lock_profit_but_not_add_initial_risk() -> None:
    quick = _quick()
    deep = _deep(quick)

    validate_exit_plan_replacement(quick, deep)
    assert deep.stop_price > deep.entry_basis_price
    assert deep.initial_risk_per_share == quick.initial_risk_per_share


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("stop_price", Decimal("9.79"), "must not loosen"),
        ("version", 3, "increment by exactly one"),
        ("time_exit_at", DECISION + timedelta(days=6), "must not extend"),
        ("supersedes_plan_id", "another-plan", "exact superseded"),
    ),
)
def test_replacement_fails_closed_on_non_monotonic_changes(
    field: str,
    value: object,
    message: str,
) -> None:
    quick = _quick()
    candidate = replace(_deep(quick), **{field: value})

    with pytest.raises(ValueError, match=message):
        validate_exit_plan_replacement(quick, candidate)


def test_quick_plan_rejects_non_provisional_or_misaligned_prices() -> None:
    with pytest.raises(ValueError, match="QUICK plan must be PROVISIONAL"):
        replace(_quick(), state=ExitPlanState.CONFIRMED)
    with pytest.raises(ValueError, match="align to price_tick"):
        replace(_quick(), stop_price=Decimal("9.805"))
