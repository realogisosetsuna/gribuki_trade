"""合约 OMS 的纯状态策略与恢复查询构造。

本模块不打开数据库，也不执行交易动作；它只描述订单状态单调性、保护计划版本
以及重启恢复所需的查询条件。把这些规则独立出来后，持久化 facade 只保留事务
边界和行更新，策略本身可以直接进行离线测试。
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from .futures_models import (
    FuturesCommandStatus,
    FuturesOrderSnapshot,
    FuturesOrderStatus,
    FuturesProtectionPlan,
)
from .futures_oms_codec import timestamp

ORDER_STATUS_RANK: Mapping[FuturesOrderStatus, int] = {
    FuturesOrderStatus.UNKNOWN: 0,
    FuturesOrderStatus.NEW: 10,
    FuturesOrderStatus.PARTIALLY_FILLED: 20,
    FuturesOrderStatus.TRIGGERING: 22,
    FuturesOrderStatus.TRIGGERED: 25,
    FuturesOrderStatus.CANCELED: 30,
    FuturesOrderStatus.EXPIRED: 30,
    FuturesOrderStatus.REJECTED: 30,
    FuturesOrderStatus.FILLED: 40,
    FuturesOrderStatus.FINISHED: 40,
    FuturesOrderStatus.EXPIRED_IN_MATCH: 30,
}

TERMINAL_ORDER_STATUSES = frozenset(
    {
        FuturesOrderStatus.CANCELED,
        FuturesOrderStatus.EXPIRED,
        FuturesOrderStatus.REJECTED,
        FuturesOrderStatus.FILLED,
        FuturesOrderStatus.FINISHED,
        FuturesOrderStatus.EXPIRED_IN_MATCH,
    }
)


def should_apply_order(row: Mapping[str, Any], value: FuturesOrderSnapshot) -> bool:
    """判断新订单快照是否能覆盖数据库中的旧状态。"""

    old_time = int(row["status_time_ms"])
    new_status = FuturesOrderStatus(str(value.status))
    new_rank = ORDER_STATUS_RANK[new_status]
    old_status = FuturesOrderStatus(str(row["status"]))
    old_rank = ORDER_STATUS_RANK.get(old_status, 0)
    if old_status in TERMINAL_ORDER_STATUSES and new_status is not old_status:
        return False
    return not (value.status_time_ms < old_time or new_rank < old_rank)


def should_replace_protection_plan(
    current_revision: int | None,
    current_updated_at: datetime | None,
    plan: FuturesProtectionPlan,
) -> bool:
    """应用保护计划时拒绝旧版本和同版本的回退时间。"""

    if current_revision is None:
        return True
    if plan.revision < current_revision:
        return False
    if plan.revision == current_revision and current_updated_at is not None:
        return plan.updated_at >= current_updated_at
    return True


def command_recovery_query(
    scope: tuple[str, str, str], *, include_active: bool, now: datetime
) -> tuple[str, tuple[Any, ...]]:
    """构造重启恢复查询，调用方仍负责事务、行更新和结果解码。"""

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    where = "status=?"
    args: tuple[Any, ...] = (*scope, FuturesCommandStatus.IN_FLIGHT.value)
    if not include_active:
        where += " AND (lease_until IS NULL OR lease_until<=?)"
        args += (timestamp(now.astimezone(UTC)),)
    query = (
        "SELECT * FROM futures_commands WHERE account_id=? AND environment=? "
        "AND product=? AND "
        f"{where} ORDER BY created_at"
    )
    return query, args


__all__ = [
    "ORDER_STATUS_RANK",
    "TERMINAL_ORDER_STATUSES",
    "command_recovery_query",
    "should_apply_order",
    "should_replace_protection_plan",
]
