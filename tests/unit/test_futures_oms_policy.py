from datetime import UTC, datetime
from decimal import Decimal

import pytest

from gribuki_trade.trading.futures_models import (
    FuturesOrderSnapshot,
    FuturesProtectionPlan,
)
from gribuki_trade.trading.futures_oms_policy import (
    command_recovery_query,
    should_apply_order,
    should_replace_protection_plan,
)

NOW = datetime(2026, 9, 9, 1, 0, tzinfo=UTC)


def _order(*, status: str = "NEW", status_time_ms: int = 10) -> FuturesOrderSnapshot:
    return FuturesOrderSnapshot(
        account_id="acct",
        environment="LIVE",
        product="USDS_FUTURES",
        order_key="entry-1",
        symbol="BTCUSDT",
        side="BUY",
        position_side="LONG",
        status=status,
        status_time_ms=status_time_ms,
        quantity=Decimal("0.001"),
    )


def _plan(*, revision: int = 1, updated_at: datetime = NOW) -> FuturesProtectionPlan:
    return FuturesProtectionPlan(
        account_id="acct",
        environment="LIVE",
        product="USDS_FUTURES",
        plan_id="protect-1",
        revision=revision,
        symbol="BTCUSDT",
        position_side="LONG",
        desired_state="ACTIVE",
        coverage_state="FULL",
        updated_at=updated_at,
    )


def test_order_projection_rejects_stale_and_terminal_regressions() -> None:
    assert should_apply_order({"status": "NEW", "status_time_ms": 10}, _order(status_time_ms=11))
    assert not should_apply_order(
        {"status": "NEW", "status_time_ms": 10}, _order(status_time_ms=9)
    )
    assert not should_apply_order(
        {"status": "FILLED", "status_time_ms": 10}, _order(status_time_ms=11)
    )
    assert should_apply_order(
        {"status": "FILLED", "status_time_ms": 10}, _order(status="FILLED", status_time_ms=11)
    )


def test_protection_plan_projection_is_revision_and_time_monotonic() -> None:
    assert should_replace_protection_plan(None, None, _plan())
    assert not should_replace_protection_plan(2, NOW, _plan(revision=1))
    assert should_replace_protection_plan(2, NOW, _plan(revision=2, updated_at=NOW))
    assert should_replace_protection_plan(2, NOW, _plan(revision=3))
    assert not should_replace_protection_plan(
        2, NOW, _plan(revision=2, updated_at=datetime(2026, 9, 9, 0, 59, tzinfo=UTC))
    )


def test_recovery_query_separates_active_and_expired_leases() -> None:
    scope = ("acct", "LIVE", "USDS_FUTURES")
    query, args = command_recovery_query(scope, include_active=True, now=NOW)
    assert "lease_until" not in query
    assert args == (*scope, "IN_FLIGHT")
    query, args = command_recovery_query(scope, include_active=False, now=NOW)
    assert "lease_until<=?" in query
    assert args == (*scope, "IN_FLIGHT", NOW.isoformat())
    with pytest.raises(ValueError, match="timezone-aware"):
        command_recovery_query(scope, include_active=False, now=datetime(2026, 9, 9, 1, 0))
