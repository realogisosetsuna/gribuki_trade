"""A 股盘中 PAPER 数量规则与卖出拆分的纯值对象。

本模块只负责板块订单数量约束、部分成交容量取整和可卖余额拆分；它不访问
市场数据、时钟、数据库、券商或账户状态。上层盘中执行服务通过稳定的数据类
消费这些规则，便于独立测试与后续迁移到真实订单适配器。
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from enum import StrEnum

from gribuki_trade.ports.ashare_screening import AShareBoard


@dataclass(frozen=True, slots=True)
class IntradayOrderQuantityRule:
    """PAPER 规模确定所用的板块特定限价单数量契约。"""

    board: AShareBoard
    minimum_buy_quantity: int
    buy_increment: int
    maximum_limit_order_quantity: int
    paper_partial_fill_increment: int
    minimum_regular_sell_quantity: int
    sell_increment: int
    sell_residual_policy: str
    policy_version: str = "ashare-intraday-order-quantity@1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "board", AShareBoard(self.board))
        if self.board is AShareBoard.BSE:
            raise ValueError("BSE PAPER execution is intentionally unsupported")
        for name in (
            "minimum_buy_quantity",
            "buy_increment",
            "maximum_limit_order_quantity",
            "paper_partial_fill_increment",
            "minimum_regular_sell_quantity",
            "sell_increment",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.maximum_limit_order_quantity < self.minimum_buy_quantity:
            raise ValueError("maximum quantity must cover the minimum buy quantity")
        if not self.sell_residual_policy.strip() or not self.policy_version.strip():
            raise ValueError("quantity policy identifiers must not be empty")

    def accepts_buy_submission(self, quantity: int) -> bool:
        """返回新限价买单数量是否符合板块规则。"""

        return (
            self.minimum_buy_quantity
            <= quantity
            <= self.maximum_limit_order_quantity
            and (quantity - self.minimum_buy_quantity) % self.buy_increment == 0
        )

    def floor_buy_submission(self, raw_quantity: Decimal) -> int:
        """将容量向下取整为板块允许的最大有效提交数量。"""

        capped = min(raw_quantity, Decimal(self.maximum_limit_order_quantity))
        if capped < self.minimum_buy_quantity:
            return 0
        increments = (
            (capped - Decimal(self.minimum_buy_quantity))
            / Decimal(self.buy_increment)
        ).to_integral_value(rounding=ROUND_FLOOR)
        return self.minimum_buy_quantity + int(increments) * self.buy_increment

    def floor_partial_fill_capacity(self, raw_quantity: Decimal) -> int:
        """将分钟参与容量向下取整为 PAPER 成交增量。"""

        increments = (
            raw_quantity / Decimal(self.paper_partial_fill_increment)
        ).to_integral_value(rounding=ROUND_FLOOR)
        return int(increments) * self.paper_partial_fill_increment

    def accepts_regular_sell_submission(self, quantity: int) -> bool:
        """返回卖单是否无需余股余额例外。"""

        return (
            self.minimum_regular_sell_quantity
            <= quantity
            <= self.maximum_limit_order_quantity
            and (quantity - self.minimum_regular_sell_quantity)
            % self.sell_increment
            == 0
        )


class IntradaySellQuantityStatus(StrEnum):
    """当前可卖数量对应的未来卖单提交形态。"""

    NO_SELLABLE_QUANTITY = "NO_SELLABLE_QUANTITY"
    REGULAR_ORDERS = "REGULAR_ORDERS"
    REGULAR_ORDERS_THEN_RESIDUAL = "REGULAR_ORDERS_THEN_RESIDUAL"
    SELL_ALL_RESIDUAL_ONCE = "SELL_ALL_RESIDUAL_ONCE"


@dataclass(frozen=True, slots=True)
class IntradaySellQuantityPlan:
    """符合板块规则的未来卖出拆分；绝不提交订单。

    ``residual_sell_all_quantity`` 是承载不可拆分余股余额的完整最终订单，因此可能大于
    板块最低数量（例如主板持仓 150 股可一次卖出 150 股，其中包含 50 股余股）。
    它必须是最后一张订单，绝不能拆为多张余股订单。
    """

    rule: IntradayOrderQuantityRule
    available_to_sell: int
    status: IntradaySellQuantityStatus
    regular_order_quantities: tuple[int, ...]
    residual_sell_all_quantity: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", IntradaySellQuantityStatus(self.status))
        for name in (
            "available_to_sell",
            "residual_sell_all_quantity",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in self.regular_order_quantities
        ):
            raise ValueError("regular sell quantities must be positive integers")
        if any(
            value < self.rule.minimum_regular_sell_quantity
            or value > self.rule.maximum_limit_order_quantity
            or (value - self.rule.minimum_regular_sell_quantity)
            % self.rule.sell_increment
            != 0
            for value in self.regular_order_quantities
        ):
            raise ValueError("regular sell quantity violates board rules")
        residual = self.residual_sell_all_quantity
        if residual > self.rule.maximum_limit_order_quantity:
            raise ValueError("residual sell-all order exceeds board maximum")
        if residual > 0 and self.rule.accepts_regular_sell_submission(residual):
            raise ValueError("residual sell-all order must actually contain a residual")
        if (
            self.rule.board is AShareBoard.STAR
            and residual >= self.rule.minimum_regular_sell_quantity
        ):
            raise ValueError("STAR residual must be below 200 shares")
        if (
            self.rule.board is not AShareBoard.STAR
            and residual > 0
            and residual % self.rule.sell_increment == 0
        ):
            raise ValueError("main-board residual order must include an odd lot")
        if (
            sum(self.regular_order_quantities) + residual
            != self.available_to_sell
        ):
            raise ValueError("sell plan must account for every sellable share")
        expected_status = (
            IntradaySellQuantityStatus.NO_SELLABLE_QUANTITY
            if self.available_to_sell == 0
            else (
                IntradaySellQuantityStatus.REGULAR_ORDERS_THEN_RESIDUAL
                if self.regular_order_quantities and residual > 0
                else (
                    IntradaySellQuantityStatus.SELL_ALL_RESIDUAL_ONCE
                    if residual > 0
                    else IntradaySellQuantityStatus.REGULAR_ORDERS
                )
            )
        )
        if self.status is not expected_status:
            raise ValueError("sell plan status is inconsistent with its quantities")

    @property
    def residual_component_quantity(self) -> int:
        """返回嵌入最终全部卖出订单中的低于最低数量余额。"""

        residual_order = self.residual_sell_all_quantity
        if residual_order == 0:
            return 0
        if self.rule.board is AShareBoard.STAR:
            return residual_order
        return residual_order % self.rule.sell_increment


def intraday_order_quantity_rule(
    board: AShareBoard | str,
) -> IntradayOrderQuantityRule:
    """返回受支持板块已冻结的限价单数量规则。

    科创板限价单至少 200 股，超过 200 股后可按 1 股递增；主板与创业板买单仍须为
    100 股整数倍。最大值是交易所提交限制，并非投资组合风险限制。
    """

    resolved = AShareBoard(board)
    if resolved is AShareBoard.BSE:
        raise ValueError("BSE PAPER execution is intentionally unsupported")
    if resolved is AShareBoard.STAR:
        return IntradayOrderQuantityRule(
            board=resolved,
            minimum_buy_quantity=200,
            buy_increment=1,
            maximum_limit_order_quantity=100_000,
            paper_partial_fill_increment=1,
            minimum_regular_sell_quantity=200,
            sell_increment=1,
            sell_residual_policy="BELOW_200_SELL_ALL_ONCE",
        )
    maximum = 300_000 if resolved is AShareBoard.CHINEXT else 1_000_000
    return IntradayOrderQuantityRule(
        board=resolved,
        minimum_buy_quantity=100,
        buy_increment=100,
        maximum_limit_order_quantity=maximum,
        paper_partial_fill_increment=100,
        minimum_regular_sell_quantity=100,
        sell_increment=100,
        sell_residual_policy="BELOW_100_SELL_ALL_ONCE",
    )


def build_intraday_sell_quantity_plan(
    *,
    available_to_sell: int,
    board: AShareBoard | str,
) -> IntradaySellQuantityPlan:
    """描述未来符合板块规则的卖出数量，且不创建订单。"""

    if (
        isinstance(available_to_sell, bool)
        or not isinstance(available_to_sell, int)
        or available_to_sell < 0
    ):
        raise ValueError("available_to_sell must be a non-negative integer")
    rule = intraday_order_quantity_rule(board)
    if available_to_sell == 0:
        return IntradaySellQuantityPlan(
            rule=rule,
            available_to_sell=0,
            status=IntradaySellQuantityStatus.NO_SELLABLE_QUANTITY,
            regular_order_quantities=(),
            residual_sell_all_quantity=0,
        )
    if available_to_sell < rule.minimum_regular_sell_quantity:
        return IntradaySellQuantityPlan(
            rule=rule,
            available_to_sell=available_to_sell,
            status=IntradaySellQuantityStatus.SELL_ALL_RESIDUAL_ONCE,
            regular_order_quantities=(),
            residual_sell_all_quantity=available_to_sell,
        )

    # 科创板超过或等于 200 股的数量均为常规数量，因为递增单位为 1 股。主板/创业板
    # 余额可能包含一份不可拆分零股，可单独提交，也可与常规部分一并提交。
    residual_component = (
        0
        if rule.board is AShareBoard.STAR
        else available_to_sell % rule.sell_increment
    )
    residual_order = 0
    remaining = available_to_sell
    if residual_component > 0:
        maximum_residual_order = min(
            remaining,
            rule.maximum_limit_order_quantity,
        )
        residual_order = residual_component + (
            (maximum_residual_order - residual_component) // rule.sell_increment
        ) * rule.sell_increment
        remaining -= residual_order

    regular: list[int] = []
    while remaining > 0:
        quantity = min(remaining, rule.maximum_limit_order_quantity)
        remainder = remaining - quantity
        if 0 < remainder < rule.minimum_regular_sell_quantity:
        # 不要因取最大值而制造本可避免的余股。例如：100001 股科创板持仓应拆为
        # 99801 + 200，而不是 100000 + 1。
            quantity -= rule.minimum_regular_sell_quantity - remainder
        if not rule.accepts_regular_sell_submission(quantity):
            raise ValueError("sellable quantity cannot be partitioned by board rules")
        regular.append(quantity)
        remaining -= quantity
    return IntradaySellQuantityPlan(
        rule=rule,
        available_to_sell=available_to_sell,
        status=(
            IntradaySellQuantityStatus.SELL_ALL_RESIDUAL_ONCE
            if residual_order > 0 and not regular
            else (
                IntradaySellQuantityStatus.REGULAR_ORDERS_THEN_RESIDUAL
                if residual_order > 0
                else IntradaySellQuantityStatus.REGULAR_ORDERS
            )
        ),
        regular_order_quantities=tuple(regular),
        residual_sell_all_quantity=residual_order,
    )



__all__ = [
    "IntradayOrderQuantityRule",
    "IntradaySellQuantityPlan",
    "IntradaySellQuantityStatus",
    "build_intraday_sell_quantity_plan",
    "intraday_order_quantity_rule",
]
