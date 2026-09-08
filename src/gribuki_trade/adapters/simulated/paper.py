"""面向本地 PAPER 交易的确定性、免注册券商适配器。

该适配器刻意只建模订单生命周期中面向券商的一侧：它接收合法限价单意图，
在内存中维护最新状态，并通过未来真实券商也会复用的异步事件端口发布
不可变状态更新。
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.ports.broker import BrokerEvent

ORDER_STATUS_EVENT = "ORDER_STATUS"
ORDER_FILL_EVENT = "ORDER_FILL"


@dataclass(frozen=True, slots=True)
class PaperOrderUpdate:
    """不可变订单意图在模拟经纪商中的最新状态。"""

    order: OrderIntent
    status: OrderStatus
    filled_quantity: Decimal = Decimal("0")
    average_fill_price: Decimal | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class PaperFill:
    """一笔不可变的模拟成交，按 ``fill_id`` 去重。"""

    fill_id: str
    client_order_id: str
    symbol: str
    side: Side
    quantity: Decimal
    price: Decimal
    occurred_at: datetime


class PaperBroker:
    """支持幂等提交的最小异步模拟经纪商。

    订单标识符会在首次提交时被占用，即使该提交因经纪商断开连接而遭拒也是
    如此。因此重试必须使用新的 ``client_order_id``，这与真实经纪商结果不明
    时的处理方式一致。

    ``events`` 是单消费者流。生产事件分发器可将其分发给订单管理系统、
    持久化层和图形界面。
    """

    def __init__(self) -> None:
        self._connected = False
        self._events: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._orders: dict[str, PaperOrderUpdate] = {}
        self._fills: dict[str, PaperFill] = {}
        self._lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        """模拟会话当前是否接受经纪商操作。"""

        return self._connected

    def order_update(self, client_order_id: str) -> PaperOrderUpdate | None:
        """返回用于测试和对账的最新不可变更新。"""

        return self._orders.get(client_order_id)

    def fills(self) -> tuple[PaperFill, ...]:
        """按插入顺序返回所有模拟成交，供对账使用。"""

        return tuple(self._fills.values())

    async def connect(self) -> None:
        """打开本地模拟会话；重复调用无害。"""

        async with self._lock:
            self._connected = True

    async def disconnect(self) -> None:
        """关闭本地模拟会话，但不丢弃其订单簿。"""

        async with self._lock:
            self._connected = False

    async def submit_order(self, order: OrderIntent) -> None:
        """接受新的限价单，或发布断线拒绝事件。

        重复提交完全相同的意图不会产生操作。若把同一标识符用于不同的订单
        内容，则抛出 ``ValueError``；静默接受该冲突会破坏幂等性保证。
        """

        async with self._lock:
            existing = self._orders.get(order.client_order_id)
            if existing is not None:
                if existing.order != order:
                    raise ValueError(
                        f"client_order_id {order.client_order_id!r} is already used "
                        "for a different order"
                    )
                return

            if self._connected:
                update = PaperOrderUpdate(order=order, status=OrderStatus.ACCEPTED)
            else:
                update = PaperOrderUpdate(
                    order=order,
                    status=OrderStatus.BROKER_REJECTED,
                    reason="paper broker is not connected",
                )

            self._orders[order.client_order_id] = update
            self._publish(update)

    async def cancel_order(self, client_order_id: str) -> None:
        """立即在模拟订单簿中取消已接受的订单。"""

        async with self._lock:
            existing = self._orders.get(client_order_id)
            if existing is None:
                raise KeyError(f"unknown client_order_id: {client_order_id!r}")
            if existing.status is OrderStatus.CANCELED:
                return
            if existing.status not in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED}:
                raise ValueError(
                    f"order {client_order_id!r} cannot be canceled from "
                    f"{existing.status.value}"
                )
            if not self._connected:
                raise ConnectionError("paper broker is not connected")

            update = PaperOrderUpdate(
                order=existing.order,
                status=OrderStatus.CANCELED,
                filled_quantity=existing.filled_quantity,
                average_fill_price=existing.average_fill_price,
            )
            self._orders[client_order_id] = update
            self._publish(update)

    async def record_fill(
        self,
        client_order_id: str,
        *,
        quantity: Decimal | str | int,
        price: Decimal | str,
        fill_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> PaperFill:
        """应用一笔确定性模拟成交并更新订单状态。

        重放相同的 ``fill_id`` 具有幂等性。系统会拒绝超额成交以及终态订单的
        成交，使测试和影子运行遵循与真实执行适配器相同的账务不变量。
        """

        normalized_quantity = Decimal(str(quantity))
        normalized_price = Decimal(str(price))
        if not normalized_quantity.is_finite() or normalized_quantity <= 0:
            raise ValueError("fill quantity must be positive")
        if not normalized_price.is_finite() or normalized_price <= 0:
            raise ValueError("fill price must be positive")
        resolved_fill_id = fill_id or f"paper-{uuid4()}"
        resolved_time = occurred_at or datetime.now(UTC)

        async with self._lock:
            duplicate = self._fills.get(resolved_fill_id)
            if duplicate is not None:
                return duplicate
            if not self._connected:
                raise ConnectionError("paper broker is not connected")
            existing = self._orders.get(client_order_id)
            if existing is None:
                raise KeyError(f"unknown client_order_id: {client_order_id!r}")
            if existing.status not in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED}:
                raise ValueError(
                    f"order {client_order_id!r} cannot fill from {existing.status.value}"
                )

            new_filled = existing.filled_quantity + normalized_quantity
            if new_filled > existing.order.quantity:
                raise ValueError(f"fill would overfill order {client_order_id!r}")

            prior_notional = (
                existing.average_fill_price * existing.filled_quantity
                if existing.average_fill_price is not None
                else Decimal("0")
            )
            average_price = (
                prior_notional + normalized_price * normalized_quantity
            ) / new_filled
            status = (
                OrderStatus.FILLED
                if new_filled == existing.order.quantity
                else OrderStatus.PARTIALLY_FILLED
            )
            fill = PaperFill(
                fill_id=resolved_fill_id,
                client_order_id=client_order_id,
                symbol=existing.order.symbol,
                side=existing.order.side,
                quantity=normalized_quantity,
                price=normalized_price,
                occurred_at=resolved_time,
            )
            update = PaperOrderUpdate(
                order=existing.order,
                status=status,
                filled_quantity=new_filled,
                average_fill_price=average_price,
            )
            self._fills[resolved_fill_id] = fill
            self._orders[client_order_id] = update
            self._publish_fill(fill)
            self._publish(update)
            return fill

    async def match_quote(
        self,
        symbol: str,
        *,
        bid: Decimal | str,
        ask: Decimal | str,
    ) -> tuple[PaperFill, ...]:
        """依据一档合成报价成交可执行的未结限价单。"""

        normalized_bid = Decimal(str(bid))
        normalized_ask = Decimal(str(ask))
        if normalized_bid <= 0 or normalized_ask <= 0 or normalized_bid > normalized_ask:
            raise ValueError("quote must have positive bid <= ask")

        candidates = tuple(self._orders.values())
        fills: list[PaperFill] = []
        for update in candidates:
            if update.order.symbol != symbol:
                continue
            if update.status not in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED}:
                continue
            order = update.order
            execution_price: Decimal | None = None
            if order.side is Side.BUY and order.limit_price >= normalized_ask:
                execution_price = normalized_ask
            elif order.side is Side.SELL and order.limit_price <= normalized_bid:
                execution_price = normalized_bid
            if execution_price is not None:
                remaining = order.quantity - update.filled_quantity
                fills.append(
                    await self.record_fill(
                        order.client_order_id,
                        quantity=remaining,
                        price=execution_price,
                    )
                )
        return tuple(fills)

    async def events(self) -> AsyncIterator[BrokerEvent]:
        """严格按发出顺序产生经纪商事件。"""

        while True:
            yield await self._events.get()

    def _publish(self, update: PaperOrderUpdate) -> None:
        self._events.put_nowait(
            BrokerEvent(
                event_id=str(uuid4()),
                event_type=ORDER_STATUS_EVENT,
                occurred_at=datetime.now(UTC),
                payload=update,
            )
        )

    def _publish_fill(self, fill: PaperFill) -> None:
        self._events.put_nowait(
            BrokerEvent(
                event_id=str(uuid4()),
                event_type=ORDER_FILL_EVENT,
                occurred_at=fill.occurred_at,
                payload=fill,
            )
        )
