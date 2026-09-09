from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.domain.exit_plans import (
    ExitBarrierKind,
    ExitPlan,
    ExitPlanDepth,
    ExitPlanEventType,
    ExitPlanState,
    exit_plan_id,
)
from gribuki_trade.features.technical import TechnicalBar
from gribuki_trade.services.exit_plan_lifecycle import (
    ExitPlanLifecycleService,
)
from gribuki_trade.storage.exit_plans import SQLiteExitPlanStore

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


def _create_quick(service: ExitPlanLifecycleService, *, protection_id: str) -> ExitPlan:
    return service.create_quick_plan(
        account_id="paper-a",
        protection_id=protection_id,
        symbol="600000.SH",
        bars=_bars(),
        decision_at=DECISION,
        time_exit_at=DECISION + timedelta(days=5),
        worst_entry_price=Decimal("10.01"),
        technical_invalidation_price=Decimal("9.60"),
        strategy_version="breakout@test",
    ).result.plan


def _deep(previous: ExitPlan, *, loosen_stop: bool = False) -> ExitPlan:
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
        stop_price=(
            previous.stop_price - previous.price_tick
            if loosen_stop
            else previous.stop_price + previous.price_tick
        ),
        take_profit_price=previous.take_profit_price,
        initial_risk_per_share=previous.initial_risk_per_share,
        reward_to_risk=previous.reward_to_risk,
        price_tick=previous.price_tick,
        technical_invalidation_price=previous.technical_invalidation_price,
        feature_snapshot_sha256=digest,
        policy_version="deep-exit@test",
        strategy_version=previous.strategy_version,
        calibration_id="HELD_OUT_DEEP@test",
        reason_codes=("DEEP_EVIDENCE_FUSED",),
        metrics=previous.metrics,
        evidence_ids=("evidence-1",),
        supersedes_plan_id=previous.plan_id,
    )


def _attach(service: ExitPlanLifecycleService, protection_id: str) -> None:
    service.attach_fill_and_request_deep(
        protection_id,
        fill_id="fill-1",
        filled_quantity=100,
        fill_price=Decimal("10.00"),
        filled_at=DECISION + timedelta(minutes=1),
        known_at=DECISION + timedelta(minutes=1, seconds=1),
    )


def test_quick_creation_and_post_fill_events_are_durable_and_idempotent(
    tmp_path: Path,
) -> None:
    with SQLiteExitPlanStore(tmp_path / "exit-plans.sqlite3") as store:
        service = ExitPlanLifecycleService(store)

        first = service.create_quick_plan(
            account_id="paper-a",
            protection_id="protect-quick",
            symbol="600000.SH",
            bars=_bars(),
            decision_at=DECISION,
            time_exit_at=DECISION + timedelta(days=5),
            worst_entry_price=Decimal("10.01"),
            technical_invalidation_price=Decimal("9.60"),
            strategy_version="breakout@test",
        )
        replay = service.create_quick_plan(
            account_id="paper-a",
            protection_id="protect-quick",
            symbol="600000.SH",
            bars=_bars(),
            decision_at=DECISION,
            time_exit_at=DECISION + timedelta(days=5),
            worst_entry_price=Decimal("10.01"),
            technical_invalidation_price=Decimal("9.60"),
            strategy_version="breakout@test",
            known_at=DECISION + timedelta(seconds=30),
        )
        attached = service.attach_fill_and_request_deep(
            "protect-quick",
            fill_id="fill-1",
            filled_quantity=100,
            fill_price=Decimal("10.00"),
            filled_at=DECISION + timedelta(minutes=1),
            known_at=DECISION + timedelta(minutes=1, seconds=1),
        )
        attached_replay = service.attach_fill_and_request_deep(
            "protect-quick",
            fill_id="fill-1",
            filled_quantity=100,
            fill_price=Decimal("10.00"),
            filled_at=DECISION + timedelta(minutes=1),
            known_at=DECISION + timedelta(minutes=2),
        )

        assert first.created is True
        assert replay.created is False
        assert replay.event == first.event
        assert service.active_plan("protect-quick") == first.result.plan
        assert attached.attachment_created is True
        assert attached.deep_request_created is True
        assert attached_replay.attachment_created is False
        assert attached_replay.deep_request_created is False
        assert [event.event_type for event in store.events("protect-quick")] == [
            ExitPlanEventType.PLAN_CREATED,
            ExitPlanEventType.PLAN_ATTACHED_TO_FILL,
            ExitPlanEventType.DEEP_ANALYSIS_REQUESTED,
        ]


def test_deep_replacement_is_monotonic_and_invalid_candidate_retains_quick(
    tmp_path: Path,
) -> None:
    with SQLiteExitPlanStore(tmp_path / "exit-plans.sqlite3") as store:
        service = ExitPlanLifecycleService(store)
        quick = _create_quick(service, protection_id="protect-deep")
        _attach(service, quick.protection_id)

        rejected = service.apply_deep_replacement(
            _deep(quick, loosen_stop=True),
            known_at=DECISION + timedelta(minutes=3),
        )
        accepted_plan = _deep(quick)
        accepted = service.apply_deep_replacement(
            accepted_plan,
            known_at=DECISION + timedelta(minutes=3),
        )
        replay = service.apply_deep_replacement(
            accepted_plan,
            known_at=DECISION + timedelta(minutes=4),
        )

        assert rejected.applied is False
        assert rejected.active_plan == quick
        assert rejected.event.event_type is ExitPlanEventType.DEEP_ANALYSIS_FAILED
        assert "must not loosen" in (rejected.failure_reason or "")
        assert accepted.applied is True
        assert accepted.active_plan == accepted_plan
        assert replay.applied is False
        assert service.active_plan(quick.protection_id) == accepted_plan


def test_external_deep_analysis_failure_is_audited_without_replacing_quick(
    tmp_path: Path,
) -> None:
    with SQLiteExitPlanStore(tmp_path / "exit-plans.sqlite3") as store:
        service = ExitPlanLifecycleService(store)
        quick = _create_quick(service, protection_id="protect-deep-failure")
        _attach(service, quick.protection_id)

        failure = service.record_deep_analysis_failure(
            quick.protection_id,
            attempt_id="llm-attempt-1",
            reason_code="PROVIDER_TIMEOUT",
            failed_at=DECISION + timedelta(minutes=2),
            known_at=DECISION + timedelta(minutes=2, seconds=1),
        )
        replay = service.record_deep_analysis_failure(
            quick.protection_id,
            attempt_id="llm-attempt-1",
            reason_code="PROVIDER_TIMEOUT",
            failed_at=DECISION + timedelta(minutes=2),
            known_at=DECISION + timedelta(minutes=3),
        )

        assert failure.event_type is ExitPlanEventType.DEEP_ANALYSIS_FAILED
        assert replay == failure
        assert service.active_plan(quick.protection_id) == quick


@pytest.mark.parametrize(
    ("expected", "sellable_quantity"),
    (
        (ExitBarrierKind.STOP_LOSS, 0),
        (ExitBarrierKind.TAKE_PROFIT, 100),
        (ExitBarrierKind.TIME, 100),
    ),
)
def test_completed_bar_observation_records_each_barrier_without_orders(
    tmp_path: Path,
    expected: ExitBarrierKind,
    sellable_quantity: int,
) -> None:
    path = tmp_path / f"exit-{expected.value}.sqlite3"
    with SQLiteExitPlanStore(path) as store:
        service = ExitPlanLifecycleService(store)
        plan = _create_quick(service, protection_id=f"protect-{expected.value}")
        _attach(service, plan.protection_id)
        if expected is ExitBarrierKind.STOP_LOSS:
            bar_end = DECISION + timedelta(minutes=5)
            low, high = plan.stop_price, plan.take_profit_price
        elif expected is ExitBarrierKind.TAKE_PROFIT:
            bar_end = DECISION + timedelta(minutes=5)
            low, high = plan.entry_basis_price, plan.take_profit_price
        else:
            bar_end = plan.time_exit_at
            low = high = plan.entry_basis_price
        bar = TechnicalBar(
            end_time=bar_end,
            available_at=bar_end + timedelta(seconds=5),
            open=plan.entry_basis_price,
            high=high,
            low=low,
            close=plan.entry_basis_price,
            volume=1000,
        )

        observed = service.observe_completed_bar(
            plan.protection_id,
            bar=bar,
            observed_at=bar.available_at,
            sellable_quantity=sellable_quantity,
        )
        replay = service.observe_completed_bar(
            plan.protection_id,
            bar=bar,
            observed_at=bar.available_at + timedelta(minutes=1),
            sellable_quantity=sellable_quantity,
        )

        assert observed.selected_barrier is expected
        assert replay.signal_event == observed.signal_event
        assert observed.order_created is False
        assert all(
            event.event_type is not ExitPlanEventType.ORDER_SUBMITTED
            for event in store.events(plan.protection_id)
        )
        if expected is ExitBarrierKind.STOP_LOSS:
            assert observed.crossed_barriers == (
                ExitBarrierKind.STOP_LOSS,
                ExitBarrierKind.TAKE_PROFIT,
            )
            assert observed.suppression_reason == "T1_NO_SELLABLE_QUANTITY"
            assert observed.execution_handoff_required is False
            assert observed.signal_event is not None
            assert observed.signal_event.payload["order_created"] is False
        else:
            assert observed.execution_handoff_required is True
            assert observed.suppression_reason is None


def test_untriggered_bar_has_no_audit_or_order_side_effect(tmp_path: Path) -> None:
    with SQLiteExitPlanStore(tmp_path / "exit-plans.sqlite3") as store:
        service = ExitPlanLifecycleService(store)
        plan = _create_quick(service, protection_id="protect-none")
        _attach(service, plan.protection_id)
        bar_end = DECISION + timedelta(minutes=5)
        bar = TechnicalBar(
            end_time=bar_end,
            available_at=bar_end + timedelta(seconds=5),
            open=plan.entry_basis_price,
            high=plan.entry_basis_price,
            low=plan.entry_basis_price,
            close=plan.entry_basis_price,
            volume=1000,
        )

        observed = service.observe_completed_bar(
            plan.protection_id,
            bar=bar,
            observed_at=bar.available_at,
            sellable_quantity=0,
        )

        assert observed.selected_barrier is None
        assert observed.order_created is False
        assert [event.event_type for event in store.events(plan.protection_id)] == [
            ExitPlanEventType.PLAN_CREATED,
            ExitPlanEventType.PLAN_ATTACHED_TO_FILL,
            ExitPlanEventType.DEEP_ANALYSIS_REQUESTED,
        ]


def test_barrier_observation_before_fill_is_forbidden(tmp_path: Path) -> None:
    with SQLiteExitPlanStore(tmp_path / "exit-prefill.sqlite3") as store:
        service = ExitPlanLifecycleService(store)
        plan = _create_quick(service, protection_id="protect-prefill")
        bar = TechnicalBar(
            end_time=DECISION + timedelta(minutes=1),
            available_at=DECISION + timedelta(minutes=1, seconds=5),
            open=plan.entry_basis_price,
            high=plan.entry_basis_price,
            low=plan.entry_basis_price,
            close=plan.entry_basis_price,
            volume=1000,
        )

        with pytest.raises(RuntimeError, match="before a durable fill attachment"):
            service.observe_completed_bar(
                plan.protection_id,
                bar=bar,
                observed_at=bar.available_at,
                sellable_quantity=0,
            )
