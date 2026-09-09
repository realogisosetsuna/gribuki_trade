from __future__ import annotations

from decimal import Decimal

import pytest

from gribuki_trade.ports.ashare_screening import AShareBoard
from gribuki_trade.services.ashare import ashare_intraday_paper as facade
from gribuki_trade.services.ashare.ashare_intraday_quantity import (
    IntradaySellQuantityStatus,
    build_intraday_sell_quantity_plan,
    intraday_order_quantity_rule,
)


def test_quantity_policy_has_a_pure_module_boundary() -> None:
    """数量规则由独立模块实现，同时保持历史 facade 身份不变。"""

    assert facade.intraday_order_quantity_rule is intraday_order_quantity_rule
    assert facade.build_intraday_sell_quantity_plan is build_intraday_sell_quantity_plan
    assert "build_intraday_sell_quantity_plan" in facade.__all__


@pytest.mark.parametrize(
    ("board", "minimum", "increment", "maximum"),
    (
        (AShareBoard.SSE_MAIN, 100, 100, 1_000_000),
        (AShareBoard.SZSE_MAIN, 100, 100, 1_000_000),
        (AShareBoard.CHINEXT, 100, 100, 300_000),
        (AShareBoard.STAR, 200, 1, 100_000),
    ),
)
def test_board_quantity_rules_are_deterministic(
    board: AShareBoard,
    minimum: int,
    increment: int,
    maximum: int,
) -> None:
    rule = intraday_order_quantity_rule(board)

    assert rule.minimum_buy_quantity == minimum
    assert rule.buy_increment == increment
    assert rule.maximum_limit_order_quantity == maximum
    assert rule.accepts_buy_submission(minimum)
    assert not rule.accepts_buy_submission(minimum - 1)
    assert rule.floor_buy_submission(Decimal(minimum - 1)) == 0


def test_sell_plan_keeps_main_board_residual_in_one_final_order() -> None:
    plan = build_intraday_sell_quantity_plan(
        available_to_sell=1_000_150,
        board=AShareBoard.SSE_MAIN,
    )

    assert plan.status is IntradaySellQuantityStatus.REGULAR_ORDERS_THEN_RESIDUAL
    assert plan.regular_order_quantities == (200,)
    assert plan.residual_sell_all_quantity == 999_950
    assert plan.residual_component_quantity == 50
    assert sum(plan.regular_order_quantities) + plan.residual_sell_all_quantity == 1_000_150


def test_sell_plan_rejects_boolean_quantity() -> None:
    with pytest.raises(ValueError, match="available_to_sell"):
        build_intraday_sell_quantity_plan(
            available_to_sell=True,  # type: ignore[arg-type]
            board=AShareBoard.SSE_MAIN,
        )
