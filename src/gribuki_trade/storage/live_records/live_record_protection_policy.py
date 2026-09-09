"""实盘保护批次的纯数量策略。

本模块不读取 SQLite，也不写入任何状态；它只根据已经读取的买入批次计算
T+1 可卖数量和 FIFO 卖出分配。调用方仍负责在同一事务中持久化分配结果，
这样数量规则可以脱离数据库直接测试和复用。
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from gribuki_trade.storage.live_records.live_record_codec import _aware_utc


@dataclass(frozen=True, slots=True)
class SellLot:
    """待卖出保护批次的最小纯数据视图。"""

    protection_id: str
    buy_command_id: str
    remaining_quantity: int


@dataclass(frozen=True, slots=True)
class SellLotAllocation:
    """一次 FIFO 分配及其是否耗尽原买入批次。"""

    protection_id: str
    buy_command_id: str
    quantity: int
    closes_lot: bool


class SellLotAllocationError(ValueError):
    """卖出数量无法由现有保护批次覆盖。"""


def sellable_quantity_for_t1(
    acquired_at: datetime,
    remaining_quantity: int,
    *,
    as_of: datetime,
    timezone_name: str = "Asia/Shanghai",
) -> int:
    """按指定交易时区计算买入批次在 T+1 规则下的可卖数量。"""

    if isinstance(remaining_quantity, bool) or not isinstance(remaining_quantity, int):
        raise TypeError("remaining_quantity must be an integer")
    if remaining_quantity < 0:
        raise ValueError("remaining_quantity must not be negative")
    acquired = _aware_utc(acquired_at)
    moment = _aware_utc(as_of)
    zone = ZoneInfo(timezone_name)
    if acquired.astimezone(zone).date() >= moment.astimezone(zone).date():
        return 0
    return remaining_quantity


def allocate_sell_lots(
    quantity: int,
    lots: Sequence[SellLot],
) -> tuple[SellLotAllocation, ...]:
    """按买入时间已排序的批次执行确定性的 FIFO 卖出分配。"""

    if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
        raise SellLotAllocationError("quantity must be a positive integer")
    remaining = quantity
    allocations: list[SellLotAllocation] = []
    for lot in lots:
        if not isinstance(lot.remaining_quantity, int) or isinstance(
            lot.remaining_quantity, bool
        ):
            raise SellLotAllocationError("lot quantity must be an integer")
        if lot.remaining_quantity < 0:
            raise SellLotAllocationError("lot quantity must not be negative")
        if remaining == 0:
            break
        allocated = min(lot.remaining_quantity, remaining)
        if allocated == 0:
            continue
        allocations.append(
            SellLotAllocation(
                protection_id=lot.protection_id,
                buy_command_id=lot.buy_command_id,
                quantity=allocated,
                closes_lot=allocated == lot.remaining_quantity,
            )
        )
        remaining -= allocated
    if remaining:
        raise SellLotAllocationError("sell quantity exceeds available protection lots")
    return tuple(allocations)


__all__ = [
    "SellLot",
    "SellLotAllocation",
    "SellLotAllocationError",
    "allocate_sell_lots",
    "sellable_quantity_for_t1",
]
