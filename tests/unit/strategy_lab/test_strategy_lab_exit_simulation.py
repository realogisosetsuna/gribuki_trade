from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from gribuki_trade.strategy_lab import exit_evaluator as facade
from gribuki_trade.strategy_lab.exit_evaluator import (
    ExitEvaluationBar,
    ExitPolicyCostModel,
    ExitPolicyEpisode,
)
from gribuki_trade.strategy_lab.exit_policies import ExitPolicyParameters
from gribuki_trade.strategy_lab.exit_simulation import (
    evaluate_episode,
    metrics,
    net_sell_price,
)


def _bar(session_date: date, *, open_: str = "10.00") -> ExitEvaluationBar:
    return ExitEvaluationBar(
        session_date=session_date,
        open=Decimal(open_),
        high=Decimal("10.50"),
        low=Decimal("9.50"),
        close=Decimal("10.10"),
        volume_shares=1000,
        suspended=False,
        lower_price_limit=Decimal("8.00"),
        upper_price_limit=Decimal("12.00"),
        completed_at=datetime.combine(session_date, datetime.min.time(), tzinfo=UTC)
        + timedelta(hours=8),
        source_id="bars",
        source_revision=f"r-{session_date.isoformat()}",
    )


def _episode() -> ExitPolicyEpisode:
    entry_session = date(2026, 8, 13)
    entry_at = datetime.combine(entry_session, datetime.min.time(), tzinfo=UTC) + timedelta(
        hours=2,
    )
    return ExitPolicyEpisode(
        episode_id="simulation@test",
        symbol="600000.SH",
        entry_at=entry_at,
        entry_session_date=entry_session,
        entry_price=Decimal("10.00"),
        quantity=100,
        atr_at_entry=Decimal("1.00"),
        structure_low_at_entry=Decimal("9.50"),
        features_known_at=entry_at - timedelta(minutes=1),
        future_bars=(_bar(date(2026, 8, 14)), _bar(date(2026, 8, 15))),
        source_revisions=(("bars", "r1"),),
    )


def _parameters() -> ExitPolicyParameters:
    return ExitPolicyParameters(
        policy_version="simulation@test",
        atr_stop_multiple=Decimal("1"),
        structure_buffer_atr=Decimal("0.1"),
        reward_to_risk=Decimal("1"),
        maximum_holding_sessions=2,
        trailing_atr_multiple=None,
    )


def _costs() -> ExitPolicyCostModel:
    return ExitPolicyCostModel(
        commission_bps=Decimal("0"),
        tax_bps=Decimal("0"),
        transfer_fee_bps=Decimal("0"),
        sell_slippage_bps=Decimal("100"),
        minimum_commission_cny=Decimal("0"),
    )


def test_simulation_module_and_legacy_facade_share_episode_semantics() -> None:
    episode = _episode()
    parameters = _parameters()
    costs = _costs()

    extracted = evaluate_episode(episode, parameters, costs, 2)
    legacy = facade._evaluate_episode(episode, parameters, costs, 2)

    assert extracted == legacy
    assert extracted.status.value == "FILLED"
    assert extracted.net_execution_price == Decimal("9.999")


def test_pure_pricing_and_metrics_helpers_are_available_from_new_boundary() -> None:
    bar = _bar(date(2026, 8, 14))
    costs = _costs()

    assert net_sell_price(Decimal("11"), bar, costs) == Decimal("10.89")

    outcome = evaluate_episode(_episode(), _parameters(), costs, 2)
    assert metrics((outcome,)) == facade._metrics((outcome,))
