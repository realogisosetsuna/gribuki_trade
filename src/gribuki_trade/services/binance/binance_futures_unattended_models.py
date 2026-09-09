"""无人值守 USDⓈ-M 合约执行服务返回的不可变结果对象。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FuturesStartupReconciliation:
    """启动或断线对账的可审计摘要。"""

    recovered_commands: int
    open_orders: int
    open_algo_orders: int
    history_orders: int
    history_algo_orders: int
    positions: int
    balances: int
    unresolved_protection_plans: tuple[str, ...] = ()


__all__ = ["FuturesStartupReconciliation"]
