"""券商中立 OMS 的持仓成交投影。

这里仅计算一次成交对持仓数量、均价和已实现盈亏的影响，不打开数据库。OMS
facade 负责读取当前行并在同一事务中写回这个不可变投影。
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from gribuki_trade.domain.orders import Side
from gribuki_trade.trading.core.models import ExecutionFill

from .oms_codec import same_sign


@dataclass(frozen=True, slots=True)
class PositionProjection:
    """一次成交应用后的最小持仓状态。"""

    quantity: Decimal
    average_entry_price: Decimal
    realized_pnl: Decimal


def project_position_fill(
    fill: ExecutionFill,
    *,
    old_quantity: Decimal = Decimal("0"),
    old_average_entry_price: Decimal = Decimal("0"),
    old_realized_pnl: Decimal = Decimal("0"),
) -> PositionProjection:
    """按多空方向应用成交，并正确处理平仓与反向开仓。"""

    delta = fill.quantity if fill.side is Side.BUY else -fill.quantity
    new_quantity = old_quantity + delta
    realized = old_realized_pnl
    if old_quantity == 0 or same_sign(old_quantity, delta):
        total = abs(old_quantity) + abs(delta)
        new_average = (
            abs(old_quantity) * old_average_entry_price + abs(delta) * fill.price
        ) / total
    else:
        closed = min(abs(old_quantity), abs(delta))
        if old_quantity > 0:
            realized += (fill.price - old_average_entry_price) * closed
        else:
            realized += (old_average_entry_price - fill.price) * closed
        if new_quantity == 0:
            new_average = Decimal("0")
        elif same_sign(new_quantity, old_quantity):
            new_average = old_average_entry_price
        else:
            new_average = fill.price
    return PositionProjection(new_quantity, new_average, realized)


__all__ = ["PositionProjection", "project_position_fill"]
