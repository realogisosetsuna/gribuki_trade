"""Broker adapter port shared by paper, backtest, and live gateways."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from gribuki_trade.domain.orders import OrderIntent


@dataclass(frozen=True, slots=True)
class BrokerEvent:
    event_id: str
    event_type: str
    occurred_at: datetime
    payload: object


class BrokerAdapter(Protocol):
    async def connect(self) -> None: ...

    async def disconnect(self) -> None: ...

    async def submit_order(self, order: OrderIntent) -> None: ...

    async def cancel_order(self, client_order_id: str) -> None: ...

    def events(self) -> AsyncIterator[BrokerEvent]: ...

