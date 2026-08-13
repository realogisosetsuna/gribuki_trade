"""A deterministic, registration-free broker for local paper trading.

The adapter deliberately models only the broker-facing part of an order's
lifecycle.  It accepts valid limit-order intents, keeps their latest state in
memory, and publishes immutable status updates through the same asynchronous
event port that a live broker adapter will use later.
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
    """The latest paper-broker state for an immutable order intent."""

    order: OrderIntent
    status: OrderStatus
    filled_quantity: Decimal = Decimal("0")
    average_fill_price: Decimal | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class PaperFill:
    """One immutable simulated execution, deduplicated by ``fill_id``."""

    fill_id: str
    client_order_id: str
    symbol: str
    side: Side
    quantity: Decimal
    price: Decimal
    occurred_at: datetime


class PaperBroker:
    """Minimal asynchronous paper broker with idempotent submissions.

    An order identifier is reserved on its first submission, including when
    that submission is rejected because the broker is disconnected.  A retry
    therefore needs a new ``client_order_id``, just as it would after an
    uncertain live-broker outcome.

    ``events`` is a single-consumer stream.  A production event dispatcher can
    fan it out to the OMS, persistence layer, and GUI.
    """

    def __init__(self) -> None:
        self._connected = False
        self._events: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._orders: dict[str, PaperOrderUpdate] = {}
        self._fills: dict[str, PaperFill] = {}
        self._lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        """Whether the paper session currently accepts broker operations."""

        return self._connected

    def order_update(self, client_order_id: str) -> PaperOrderUpdate | None:
        """Return the latest immutable update for tests and reconciliation."""

        return self._orders.get(client_order_id)

    def fills(self) -> tuple[PaperFill, ...]:
        """Return all simulated fills in insertion order for reconciliation."""

        return tuple(self._fills.values())

    async def connect(self) -> None:
        """Open the local paper session; repeated calls are harmless."""

        async with self._lock:
            self._connected = True

    async def disconnect(self) -> None:
        """Close the local paper session without discarding its order book."""

        async with self._lock:
            self._connected = False

    async def submit_order(self, order: OrderIntent) -> None:
        """Accept a new limit order or publish a disconnected rejection.

        Repeating the exact same intent is a no-op.  Reusing an identifier for
        different order contents raises ``ValueError`` because silently
        accepting that conflict would break idempotency guarantees.
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
        """Cancel an accepted order immediately in the paper order book."""

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
        """Apply one deterministic simulated fill and update the order state.

        Replaying the same ``fill_id`` is idempotent.  Overfills and fills for
        terminal orders are rejected so test and shadow runs exercise the same
        accounting invariants expected from a live execution adapter.
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
        """Fill marketable open limit orders against one synthetic top quote."""

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
        """Yield broker events in the exact order in which they were emitted."""

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
