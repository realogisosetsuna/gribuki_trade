"""Binance 现货执行服务的协议、错误类型与启动对账结果。

这些类型只描述服务边界和不可变结果，不持有网络连接、SQLite 事务或执行状态。
执行 facade 继续负责提交、撤单、监听、租约和对账编排。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol

from gribuki_trade.adapters.binance import (
    BinanceAccount,
    BinanceEnvironment,
    BinanceOrderListSnapshot,
    BinanceOrderSnapshot,
    BinanceOrderUpdate,
    BinanceTrade,
    BinanceUserDataEvent,
)
from gribuki_trade.domain.orders import OrderIntent


class BinanceSpotExecutionGateway(Protocol):
    """可恢复测试网服务所需的 REST 接口面。"""

    @property
    def environment(self) -> BinanceEnvironment: ...

    async def connect(self) -> None: ...

    async def disconnect(self) -> None: ...

    async def submit_order(self, order: OrderIntent) -> None: ...

    async def cancel_order(self, client_order_id: str) -> None: ...

    async def cancel_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> BinanceOrderSnapshot: ...

    def order_update(self, client_order_id: str) -> BinanceOrderUpdate | None: ...

    async def account(self) -> BinanceAccount: ...

    async def open_orders(self, symbol: str | None = None) -> tuple[BinanceOrderSnapshot, ...]: ...

    async def all_orders(
        self,
        symbol: str,
        *,
        order_id: int | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> tuple[BinanceOrderSnapshot, ...]: ...

    async def account_trades(
        self,
        symbol: str,
        *,
        order_id: int | None = None,
        from_id: int | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> tuple[BinanceTrade, ...]: ...

    async def get_order(
        self,
        symbol: str,
        *,
        client_order_id: str | None = None,
        order_id: int | None = None,
    ) -> BinanceOrderSnapshot: ...

    async def open_order_lists(self) -> tuple[BinanceOrderListSnapshot, ...]: ...

    async def all_order_lists(
        self,
        *,
        from_id: int | None = None,
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> tuple[BinanceOrderListSnapshot, ...]: ...

class BinanceUserDataSource(Protocol):
    """执行服务使用的私有数据流接口面。"""

    @property
    def environment(self) -> BinanceEnvironment: ...

    def events(self) -> AsyncIterator[BinanceUserDataEvent]: ...

    async def aclose(self) -> None: ...

class BinanceTestnetOnlyError(ValueError):
    """任一依赖指向 Binance 实盘时在构造前抛出。"""


@dataclass(frozen=True, slots=True)
class BinanceStartupReconciliation:
    """一次失败关闭式启动对账的可观测结果。"""

    recovered_commands: int
    exchange_open_orders: int
    exchange_history_orders: int
    exchange_trades: int
    reconciled_orders: int
    recorded_fills: int
    recorded_balances: int
    dispatched_pending_commands: int = 0
    unresolved_order_ids: tuple[str, ...] = ()
    exchange_order_lists: int = 0
    reconciled_order_lists: int = 0

__all__ = [
    "BinanceSpotExecutionGateway",
    "BinanceStartupReconciliation",
    "BinanceTestnetOnlyError",
    "BinanceUserDataSource",
]
