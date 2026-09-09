from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.strategy_lab.exit_policies import (
    ExitPolicySearchSpace,
    ExitPolicyTrace,
    ExitTraceBarrier,
    ExitTraceExecution,
    generate_exit_policy_candidates,
)

NOW = datetime(2026, 8, 14, 2, 0, tzinfo=UTC)


def _space(**changes: object) -> ExitPolicySearchSpace:
    values: dict[str, object] = {
        "search_space_id": "exit-grid@test",
        "policy_version": "exit-policy@test",
        "atr_stop_multiples": (Decimal("1.0"), Decimal("1.5")),
        "structure_buffer_atr_values": (Decimal("0.1"),),
        "reward_to_risk_values": (Decimal("1.5"), Decimal("2.0")),
        "maximum_holding_sessions_values": (3, 5),
        "trailing_atr_multiples": (None, Decimal("1.5")),
        "max_candidates": 16,
    }
    values.update(changes)
    return ExitPolicySearchSpace(**values)  # type: ignore[arg-type]


def _trace() -> ExitPolicyTrace:
    parameters = generate_exit_policy_candidates(_space())[0]
    return ExitPolicyTrace(
        trace_id="trace-1",
        observation_id="observation-1",
        symbol="600000.SH",
        plan_id="exit-plan-1",
        parameter_fingerprint=parameters.fingerprint,
        decision_at=NOW,
        entry_at=NOW + timedelta(minutes=1),
        entry_price=Decimal("10.00"),
        stop_price=Decimal("9.80"),
        take_profit_price=Decimal("10.30"),
        time_exit_at=NOW + timedelta(days=5),
        barrier=ExitTraceBarrier.STOP_LOSS,
        barrier_observed_at=NOW + timedelta(minutes=2),
        execution=ExitTraceExecution.T1_BLOCKED,
        execution_at=None,
        execution_price=None,
        both_price_barriers_touched=True,
        source_revisions=(("minute-bars", "r1"),),
        warning_codes=("OHLC_ORDER_UNKNOWN_STOP_FIRST",),
    )


def test_registered_grid_enumerates_every_trial_deterministically() -> None:
    space = _space()
    first = generate_exit_policy_candidates(space)
    second = generate_exit_policy_candidates(space)

    assert len(first) == space.candidate_count == 16
    assert first == second
    assert len({candidate.fingerprint for candidate in first}) == 16
    assert len(space.manifest_sha256) == 64


def test_search_space_refuses_silent_truncation() -> None:
    with pytest.raises(ValueError, match="exceeds max_candidates"):
        _space(max_candidates=15)


def test_trace_separates_t1_blocked_barrier_from_execution() -> None:
    trace = _trace()

    assert trace.barrier is ExitTraceBarrier.STOP_LOSS
    assert trace.execution is ExitTraceExecution.T1_BLOCKED
    assert trace.execution_price is None


def test_ambiguous_same_bar_must_use_conservative_stop_first() -> None:
    with pytest.raises(ValueError, match="conservative stop-first"):
        replace(_trace(), barrier=ExitTraceBarrier.TAKE_PROFIT)


def test_filled_trace_requires_chronological_execution_evidence() -> None:
    filled_at = NOW + timedelta(minutes=3)
    filled = replace(
        _trace(),
        execution=ExitTraceExecution.FILLED,
        execution_at=filled_at,
        execution_price=Decimal("9.75"),
    )
    assert filled.execution_at == filled_at
    with pytest.raises(ValueError, match="must not precede"):
        replace(filled, execution_at=NOW)


def test_non_fill_cannot_carry_partial_execution_details() -> None:
    with pytest.raises(ValueError, match="only FILLED"):
        replace(_trace(), execution_at=NOW + timedelta(minutes=3))
    with pytest.raises(ValueError, match="only FILLED"):
        replace(_trace(), execution_price=Decimal("9.75"))


def test_execution_outcome_requires_an_observed_barrier() -> None:
    with pytest.raises(ValueError, match="require an observed exit barrier"):
        replace(
            _trace(),
            barrier=ExitTraceBarrier.NONE,
            barrier_observed_at=None,
            execution=ExitTraceExecution.T1_BLOCKED,
            both_price_barriers_touched=False,
        )
