"""币安命令处理器使用的纯结果转换。

处理器负责网络编排和 LIVE 守卫；本模块只负责协议结果到 JSON 的稳定转换，
因此可以在不构造网关或服务的情况下独立测试。
"""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal, InvalidOperation
from typing import Protocol

from gribuki_trade.adapters.binance.transport.gateway import BinanceProtocolError


class _BalanceLike(Protocol):
    asset: str
    free: Decimal
    locked: Decimal


class _AccountLike(Protocol):
    balances: Iterable[_BalanceLike]


class _StartupReconciliationLike(Protocol):
    dispatched_pending_commands: int
    exchange_history_orders: int
    exchange_open_orders: int
    exchange_trades: int
    reconciled_orders: int
    recorded_balances: int
    recorded_fills: int
    recovered_commands: int
    unresolved_order_ids: Iterable[str]


def _binance_balance_decimal(value: object, field: str) -> Decimal:
    """解析一个合约余额字段，不把错误数据静默转换为零。"""

    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise BinanceProtocolError(
            f"Binance Futures balance field {field} is malformed"
        ) from None
    if not number.is_finite():
        raise BinanceProtocolError(f"Binance Futures balance field {field} is not finite")
    return number


def _testnet_balance_diff(
    before: _AccountLike, after: _AccountLike
) -> list[dict[str, str]]:
    """将发生变化的现货余额转换为稳定的十进制文本行。"""

    before_values = {item.asset: (item.free, item.locked) for item in before.balances}
    after_values = {item.asset: (item.free, item.locked) for item in after.balances}
    changed: list[dict[str, str]] = []
    for asset in sorted(before_values.keys() | after_values.keys()):
        before_free, before_locked = before_values.get(asset, (Decimal("0"), Decimal("0")))
        after_free, after_locked = after_values.get(asset, (Decimal("0"), Decimal("0")))
        if (before_free, before_locked) == (after_free, after_locked):
            continue
        changed.append(
            {
                "asset": asset,
                "before_free": format(before_free, "f"),
                "before_locked": format(before_locked, "f"),
                "after_free": format(after_free, "f"),
                "after_locked": format(after_locked, "f"),
                "delta_free": format(after_free - before_free, "f"),
                "delta_locked": format(after_locked - before_locked, "f"),
                "delta_total": format(
                    after_free + after_locked - before_free - before_locked, "f"
                ),
            }
        )
    return changed


def _testnet_reconciliation_payload(
    report: _StartupReconciliationLike,
) -> dict[str, object]:
    """序列化测试网启动对账的持久化计数器。"""

    return {
        "dispatched_pending_commands": report.dispatched_pending_commands,
        "exchange_history_orders": report.exchange_history_orders,
        "exchange_open_orders": report.exchange_open_orders,
        "exchange_trades": report.exchange_trades,
        "reconciled_orders": report.reconciled_orders,
        "recorded_balances": report.recorded_balances,
        "recorded_fills": report.recorded_fills,
        "recovered_commands": report.recovered_commands,
        "unresolved_order_ids": list(report.unresolved_order_ids),
    }
