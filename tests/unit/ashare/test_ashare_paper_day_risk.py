"""A 股 PAPER 日风险策略纯模块的回归契约。"""

from __future__ import annotations

from gribuki_trade.ports.ashare_screening import AShareBoard
from gribuki_trade.services.ashare.intraday.ashare_intraday_paper import (
    IntradayPaperRiskConfig,
    build_intraday_sell_quantity_plan,
    intraday_order_quantity_rule,
)
from gribuki_trade.services.ashare.paper_day import ashare_paper_day as paper_day_facade
from gribuki_trade.services.ashare.paper_day.ashare_paper_day_risk import (
    _ADD_BOARD_QUANTITY_RULES,
    _ADD_PRICE_ACCEPTANCE_BOUNDS,
    _REMOVE_POSITION_COUNT_CAP,
    allowed_risk_policy_change_reasons,
    quantity_rule_document,
    sell_quantity_plan_document,
    sell_quantity_plan_text,
)


def _legacy_policy() -> dict[str, object]:
    policy = IntradayPaperRiskConfig().audit_document()
    policy["maximum_positions"] = 5
    policy.pop("position_count_limit_enabled")
    policy.pop("sell_limit_markdown")
    policy.pop("price_acceptance_policy")
    policy.pop("order_quantity_policy")
    return policy


def test_risk_migration_whitelist_is_pure_and_exact() -> None:
    current = IntradayPaperRiskConfig().audit_document()

    assert allowed_risk_policy_change_reasons(_legacy_policy(), current) == (
        _REMOVE_POSITION_COUNT_CAP,
        _ADD_PRICE_ACCEPTANCE_BOUNDS,
        _ADD_BOARD_QUANTITY_RULES,
    )

    changed = dict(current)
    changed["cash_reserve_fraction"] = "0.10"
    assert allowed_risk_policy_change_reasons(_legacy_policy(), changed) is None


def test_risk_document_codecs_are_available_through_facade_aliases() -> None:
    rule = intraday_order_quantity_rule(AShareBoard.STAR)
    plan = build_intraday_sell_quantity_plan(
        available_to_sell=199,
        board=AShareBoard.STAR,
    )

    assert paper_day_facade._quantity_rule_document(rule) == quantity_rule_document(rule)  # noqa: SLF001
    assert paper_day_facade._sell_quantity_plan_document(plan) == sell_quantity_plan_document(plan)  # noqa: SLF001
    assert paper_day_facade._sell_quantity_plan_text(plan) == sell_quantity_plan_text(plan)  # noqa: SLF001
    assert "一次性卖出" in sell_quantity_plan_text(plan)
