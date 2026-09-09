"""Binance 现货执行 facade 的纯环境、状态和时间策略。

本模块不访问网关、SQLite 或运行时守卫，只负责把执行服务需要的安全判断和
交易所快照选择规则集中起来。这样测试可以直接覆盖状态投影，而执行 facade
继续拥有提交、撤单、用户流和持久化副作用。
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from gribuki_trade.adapters.binance import BinanceEnvironment, BinanceOrderSnapshot
from gribuki_trade.domain.orders import OrderIntent

from .binance_execution_records import exchange_order_rank


def require_testnet(component: object, label: str) -> None:
    """确认兼容性依赖明确指向 Binance TESTNET。"""

    environment = environment_of(component, label, allow_live=False)
    if environment is not BinanceEnvironment.TESTNET:
        raise ValueError(f"{label} targets {environment.value}; this service is TESTNET-only")


def require_binance_environment(
    component: object,
    label: str,
    *,
    allow_live: bool,
) -> None:
    """确认依赖声明了受支持的 Binance 环境，并显式控制 LIVE。"""

    environment = environment_of(component, label, allow_live=allow_live)
    if environment is BinanceEnvironment.LIVE and not allow_live:
        raise ValueError(
            f"{label} targets LIVE; this service is TESTNET-only; use "
            "BinanceSpotExecutionService with a LiveTradingGuard"
        )


def environment_of(
    component: object,
    label: str,
    *,
    allow_live: bool,
) -> BinanceEnvironment:
    """读取并验证组件环境，避免未知字符串被静默当成测试网。"""

    value = getattr(component, "environment", None)
    try:
        environment = (
            value
            if isinstance(value, BinanceEnvironment)
            else BinanceEnvironment(str(value).upper())
        )
    except ValueError:
        raise ValueError(
            f"{label} must explicitly advertise Binance TESTNET or LIVE"
        ) from None
    if environment is BinanceEnvironment.LIVE and not allow_live:
        # 由调用方统一生成测试网专用错误文本；这里先返回已解析的环境。
        return environment
    return environment


def validate_order(order: OrderIntent, *, account_id: str, symbols: frozenset[str]) -> None:
    """验证订单属于当前账户和显式配置的 Binance 标的集合。"""

    if order.account_id != account_id:
        raise ValueError("order account_id does not match the execution service")
    if order.symbol != order.symbol.upper() or order.symbol not in symbols:
        raise ValueError("order symbol is not in the Binance execution allow-list")


def merge_exchange_orders(
    orders: Sequence[BinanceOrderSnapshot],
) -> dict[str, BinanceOrderSnapshot]:
    """按 client order id 合并 REST 来源，并保留最新/最完整快照。"""

    merged: dict[str, BinanceOrderSnapshot] = {}
    for order in orders:
        client_order_id = order.client_order_id
        if client_order_id is None:
            continue
        current = merged.get(client_order_id)
        if current is None or exchange_order_rank(order) >= exchange_order_rank(current):
            merged[client_order_id] = order
    return merged


def normalize_now(value: datetime) -> datetime:
    """要求服务时钟返回带时区时间，并统一为 UTC。"""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def read_now(clock: Callable[[], datetime]) -> datetime:
    """读取并规范化注入的服务时钟。"""

    return normalize_now(clock())


__all__ = [
    "environment_of",
    "merge_exchange_orders",
    "normalize_now",
    "read_now",
    "require_binance_environment",
    "require_testnet",
    "validate_order",
]
