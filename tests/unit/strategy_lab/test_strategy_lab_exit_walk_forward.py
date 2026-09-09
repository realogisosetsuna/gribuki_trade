from __future__ import annotations

from gribuki_trade.strategy_lab import exit_evaluator as facade
from gribuki_trade.strategy_lab import exit_walk_forward as extracted


def test_walk_forward_plan_is_exposed_through_the_legacy_facade() -> None:
    """滚动计划的新边界与历史入口必须共享同一类型和构建函数。"""

    assert facade.ExitPolicyWalkForwardFold is extracted.ExitPolicyWalkForwardFold
    assert facade.ExitPolicyWalkForwardPlan is extracted.ExitPolicyWalkForwardPlan
    assert (
        facade.build_exit_policy_walk_forward_plan
        is extracted.build_exit_policy_walk_forward_plan
    )
