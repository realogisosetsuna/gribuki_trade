"""Binance 现货执行服务的纯记录与对账投影辅助函数。

本模块只处理交易所快照、成交和余额的确定性转换，不访问网络、SQLite 或运行时
权限。执行 facade 继续拥有提交、撤单、租约和对账事务；这些函数可以独立测试，
也保留原有私有 helper 的行为。
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal

from gribuki_trade.adapters.binance import (
    BinanceOrderListSnapshot,
    BinanceOrderSnapshot,
    BinanceTrade,
)
from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.trading import BalanceValue, ExecutionFill, SpotOrderListRecord


def datetime_from_ms(value: int | None, *, fallback: datetime) -> datetime:
    """将 Binance 毫秒时间戳恢复为带 UTC 时区的时间。"""

    if value is None:
        return fallback.astimezone(UTC)
    return datetime.fromtimestamp(value / 1_000, tz=UTC)


def spot_order_list_record(
    account_id: str,
    snapshot: BinanceOrderListSnapshot,
    *,
    source: str,
    occurred_at: datetime,
) -> SpotOrderListRecord:
    """把 Binance 适配器快照转换为 broker-neutral 的列表记录。"""

    return SpotOrderListRecord(
        account_id=account_id,
        order_list_id=snapshot.order_list_id,
        list_client_order_id=snapshot.list_client_order_id,
        symbol=snapshot.symbol,
        contingency_type=snapshot.contingency_type,
        list_status_type=snapshot.list_status_type,
        list_order_status=snapshot.list_order_status,
        order_ids=tuple(order.order_id for order in snapshot.orders if order.order_id is not None),
        client_order_ids=tuple(
            order.client_order_id for order in snapshot.orders if order.client_order_id
        ),
        updated_at=occurred_at,
        transaction_time_ms=snapshot.transaction_time_ms,
        source=source,
    )


def order_list_rank(record: SpotOrderListRecord) -> tuple[int, datetime]:
    """按交易所事务时间选择重复 REST 快照中的最新列表。"""

    return (
        record.transaction_time_ms if record.transaction_time_ms is not None else -1,
        record.updated_at,
    )


def milliseconds(value: datetime) -> int:
    return int(value.timestamp() * 1_000)


def average_price(snapshot: BinanceOrderSnapshot) -> Decimal | None:
    quote = snapshot.cumulative_quote_quantity
    if snapshot.executed_quantity <= 0 or quote is None or quote <= 0:
        return None
    return quote / snapshot.executed_quantity


def reconciliation_event_id(snapshot: BinanceOrderSnapshot, occurred_at: datetime) -> str:
    order_id = "none" if snapshot.order_id is None else str(snapshot.order_id)
    return (
        f"binance-reconcile:{snapshot.symbol}:{order_id}:{snapshot.status.value}:"
        f"{snapshot.executed_quantity}:{milliseconds(occurred_at)}"
    )


def fill_from_trade(trade: BinanceTrade, order: OrderIntent) -> ExecutionFill:
    side = Side.BUY if trade.is_buyer else Side.SELL
    if side is not order.side or trade.symbol != order.symbol:
        raise ValueError("Binance REST trade identity does not match the local order")
    return ExecutionFill(
        fill_id=f"binance-spot:{trade.symbol}:trade:{trade.trade_id}",
        client_order_id=order.client_order_id,
        account_id=order.account_id,
        symbol=trade.symbol,
        side=side,
        quantity=trade.quantity,
        price=trade.price,
        occurred_at=datetime_from_ms(trade.time_ms, fallback=order.created_at),
        fee_asset=trade.commission_asset,
        fee_amount=trade.commission,
        exchange_order_id=str(trade.order_id),
    )


def balance_digest(balances: Sequence[BalanceValue]) -> str:
    canonical = "|".join(
        f"{item.asset}:{item.free}:{item.locked}"
        for item in sorted(balances, key=lambda value: value.asset)
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def exchange_order_rank(snapshot: BinanceOrderSnapshot) -> tuple[int, int, Decimal]:
    terminal = int(
        snapshot.status
        in {
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.BROKER_REJECTED,
            OrderStatus.EXPIRED,
        }
    )
    return (
        snapshot.transact_time_ms or -1,
        terminal,
        snapshot.executed_quantity,
    )


__all__ = [
    "average_price",
    "balance_digest",
    "datetime_from_ms",
    "exchange_order_rank",
    "fill_from_trade",
    "milliseconds",
    "order_list_rank",
    "reconciliation_event_id",
    "spot_order_list_record",
]
