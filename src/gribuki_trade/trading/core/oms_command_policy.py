"""券商中立 OMS 的命令范围、租约参数与恢复决策。

这些函数只读取不可变输入，不打开 SQLite，也不修改命令或订单。它们把
命令领取、账户/品种筛选和重启恢复中的重复规则集中到一个可独立测试的
边界；事务和实际状态写入仍由 :mod:`gribuki_trade.trading.core.oms` 负责。
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta

from gribuki_trade.domain.orders import OrderStatus

from .models import TradingCommandStatus
from .oms_codec import _TERMINAL_STATUSES, identifier


@dataclass(frozen=True, slots=True)
class CommandScope:
    """命令查询使用的规范化账户和品种范围。"""

    account_id: str | None
    symbols: tuple[str, ...] | None


@dataclass(frozen=True, slots=True)
class UnknownCommandProjection:
    """一次不明确投递在恢复时对命令和订单的纯状态投影。"""

    command_status: TradingCommandStatus
    order_status: OrderStatus | None


def normalize_command_scope(
    account_id: str | None,
    symbols: Iterable[str] | None,
) -> CommandScope:
    """规范化可选账户与品种筛选，并稳定去除重复品种。"""

    normalized_account = (
        None if account_id is None else identifier(account_id, "account_id")
    )
    normalized_symbols = (
        None
        if symbols is None
        else tuple(dict.fromkeys(identifier(value, "symbol") for value in symbols))
    )
    return CommandScope(normalized_account, normalized_symbols)


def validate_claim_limit(limit: int) -> int:
    """验证批量领取上限，返回可直接用于查询的原值。"""

    if limit < 1:
        raise ValueError("limit must be positive")
    return limit


def validate_lease_for(lease_for: timedelta) -> timedelta:
    """验证命令租约时长，避免产生立即失效的租约。"""

    if lease_for <= timedelta(0):
        raise ValueError("lease_for must be positive")
    return lease_for


def project_unknown_command(order_status: OrderStatus) -> UnknownCommandProjection:
    """计算命令进入 UNKNOWN 时，物化订单应采用的状态。"""

    command_status = (
        TradingCommandStatus.RESOLVED
        if order_status in _TERMINAL_STATUSES
        else TradingCommandStatus.UNKNOWN
    )
    return UnknownCommandProjection(
        command_status=command_status,
        order_status=(
            None
            if command_status is TradingCommandStatus.RESOLVED
            else OrderStatus.UNKNOWN
        ),
    )


def unknown_command_event_id(command_id: str, attempt_count: int) -> str:
    """生成一次恢复事件的稳定幂等标识。"""

    return f"command-unknown:{command_id}:{attempt_count}"


__all__ = [
    "CommandScope",
    "UnknownCommandProjection",
    "normalize_command_scope",
    "project_unknown_command",
    "unknown_command_event_id",
    "validate_claim_limit",
    "validate_lease_for",
]
