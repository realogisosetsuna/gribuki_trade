"""BrokerAdapter implementation for Schwab limit orders."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TypeAlias
from uuid import uuid4

from gribuki_trade.domain.orders import OrderIntent, OrderStatus, OrderType
from gribuki_trade.ports.broker import BrokerEvent

from .client import PlacedOrder, SchwabApiClient
from .errors import (
    SchwabHttpError,
    SchwabOAuthError,
    SchwabOrderValidationError,
    SchwabRateLimitError,
    SchwabServerError,
)

SCHWAB_ORDER_STATUS_EVENT = "ORDER_STATUS"


@dataclass(frozen=True, slots=True)
class SchwabOrderSpec:
    """Broker-specific choices that are not present in ``OrderIntent``.

    The resolver is injectable because session/duration, option open-vs-close
    instruction, product eligibility, and account permissions are application
    decisions.  The default below is deliberately only an equity DAY order.
    """

    asset_type: str = "EQUITY"
    instruction: str | None = None
    session: str = "NORMAL"
    duration: str = "DAY"
    instrument_fields: Mapping[str, object] = field(default_factory=dict, repr=False)
    order_fields: Mapping[str, object] = field(default_factory=dict, repr=False)


OrderSpecResolver: TypeAlias = Callable[[OrderIntent], SchwabOrderSpec]


@dataclass(frozen=True, slots=True)
class SchwabOrderUpdate:
    """Latest local view of a submitted Schwab order."""

    order: OrderIntent
    status: OrderStatus
    broker_order_id: str | None = None
    reason: str | None = None


class SchwabBroker:
    """A conservative Schwab broker gateway.

    ``connect`` obtains Schwab's raw-account-number to hash mapping.  It does
    not infer account country, product permissions, or quota.  Each submitted
    client ID is locally idempotent.  In particular, an ambiguous POST timeout,
    transport interruption, or 5xx is recorded as ``UNKNOWN`` and a duplicate
    call will not transmit the order again; reconciliation must query Schwab.
    """

    def __init__(
        self,
        api: SchwabApiClient,
        *,
        order_spec_resolver: OrderSpecResolver | None = None,
    ) -> None:
        self._api = api
        self._order_spec_resolver = order_spec_resolver or (lambda _order: SchwabOrderSpec())
        self._connected = False
        self._account_hashes: dict[str, str] = {}
        self._orders: dict[str, SchwabOrderUpdate] = {}
        self._events: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        return self._connected

    def order_update(self, client_order_id: str) -> SchwabOrderUpdate | None:
        return self._orders.get(client_order_id)

    def account_hash(self, account_identifier: str) -> str:
        """Resolve either a linked raw account number or its returned hash."""

        try:
            return self._account_hashes[account_identifier]
        except KeyError as error:
            raise KeyError("account is not present in Schwab accountNumbers") from error

    async def connect(self) -> None:
        """Authenticate and obtain linked account hashes without assumptions."""

        async with self._lock:
            pairs = await self._api.account_numbers()
            resolved: dict[str, str] = {}
            for pair in pairs:
                for identifier in (pair.account_number, pair.hash_value):
                    existing = resolved.get(identifier)
                    if existing is not None and existing != pair.hash_value:
                        raise ValueError("ambiguous identifier returned by accountNumbers")
                    resolved[identifier] = pair.hash_value
            self._account_hashes = resolved
            self._connected = True

    async def disconnect(self) -> None:
        async with self._lock:
            self._connected = False
            self._account_hashes = {}

    async def submit_order(self, order: OrderIntent) -> None:
        """Submit one limit order, conservatively recording uncertain outcomes."""

        async with self._lock:
            existing = self._orders.get(order.client_order_id)
            if existing is not None:
                if existing.order != order:
                    raise ValueError(
                        f"client_order_id {order.client_order_id!r} is already used "
                        "for a different order"
                    )
                return

            if not self._connected:
                self._record(
                    SchwabOrderUpdate(
                        order=order,
                        status=OrderStatus.BROKER_REJECTED,
                        reason="Schwab broker is not connected",
                    )
                )
                return

            try:
                account_hash = self.account_hash(order.account_id)
                spec = self._order_spec_resolver(order)
                payload = build_limit_order_payload(order, spec=spec)
            except (KeyError, ValueError) as error:
                self._record(
                    SchwabOrderUpdate(
                        order=order,
                        status=OrderStatus.LOCAL_REJECTED,
                        reason=str(error),
                    )
                )
                return

            try:
                placed = await self._api.place_order(account_hash, payload)
            except SchwabRateLimitError:
                # The 429 response is a definite non-acceptance with an
                # explicit server retry time.  Preserve that exception and do
                # not reserve the client ID, so the caller can retry later.
                raise
            except (SchwabServerError, TimeoutError, ConnectionError) as error:
                self._record_unknown(order, error)
                return
            except (SchwabHttpError, SchwabOAuthError) as error:
                self._record(
                    SchwabOrderUpdate(
                        order=order,
                        status=OrderStatus.BROKER_REJECTED,
                        reason=_safe_error_reason(error),
                    )
                )
                return

            self._record_accepted(order, placed)

    async def cancel_order(self, client_order_id: str) -> None:
        """Cancel a known accepted order; ambiguous DELETE outcomes are UNKNOWN."""

        async with self._lock:
            existing = self._orders.get(client_order_id)
            if existing is None:
                raise KeyError(f"unknown client_order_id: {client_order_id!r}")
            if existing.status is OrderStatus.CANCELED:
                return
            if existing.status is not OrderStatus.ACCEPTED:
                raise ValueError(
                    f"order {client_order_id!r} cannot be canceled from "
                    f"{existing.status.value}"
                )
            if not self._connected:
                raise ConnectionError("Schwab broker is not connected")
            if existing.broker_order_id is None:
                raise ValueError("accepted order has no broker order ID; reconcile it first")

            account_hash = self.account_hash(existing.order.account_id)
            try:
                await self._api.cancel_order(account_hash, existing.broker_order_id)
            except SchwabRateLimitError:
                raise
            except (SchwabServerError, TimeoutError, ConnectionError) as error:
                self._record_unknown(existing.order, error, existing.broker_order_id)
                return
            except (SchwabHttpError, SchwabOAuthError) as error:
                self._record(
                    SchwabOrderUpdate(
                        order=existing.order,
                        status=OrderStatus.BROKER_REJECTED,
                        broker_order_id=existing.broker_order_id,
                        reason=_safe_error_reason(error),
                    )
                )
                return

            self._record(
                SchwabOrderUpdate(
                    order=existing.order,
                    status=OrderStatus.CANCELED,
                    broker_order_id=existing.broker_order_id,
                )
            )

    async def events(self) -> AsyncIterator[BrokerEvent]:
        while True:
            yield await self._events.get()

    def _record_accepted(self, order: OrderIntent, placed: PlacedOrder) -> None:
        self._record(
            SchwabOrderUpdate(
                order=order,
                status=OrderStatus.ACCEPTED,
                broker_order_id=placed.order_id,
            )
        )

    def _record_unknown(
        self,
        order: OrderIntent,
        error: BaseException,
        broker_order_id: str | None = None,
    ) -> None:
        self._record(
            SchwabOrderUpdate(
                order=order,
                status=OrderStatus.UNKNOWN,
                broker_order_id=broker_order_id,
                reason=_safe_error_reason(error),
            )
        )

    def _record(self, update: SchwabOrderUpdate) -> None:
        self._orders[update.order.client_order_id] = update
        self._events.put_nowait(
            BrokerEvent(
                event_id=str(uuid4()),
                event_type=SCHWAB_ORDER_STATUS_EVENT,
                occurred_at=datetime.now(UTC),
                payload=update,
            )
        )


def build_limit_order_payload(
    order: OrderIntent, *, spec: SchwabOrderSpec | None = None
) -> dict[str, object]:
    """Translate an OrderIntent without silently accepting fractional stock/options."""

    spec = spec or SchwabOrderSpec()
    if order.order_type is not OrderType.LIMIT:
        raise SchwabOrderValidationError("Schwab adapter only supports limit orders")
    asset_type = spec.asset_type.strip().upper()
    if not asset_type:
        raise SchwabOrderValidationError("asset_type must not be empty")
    if not spec.session or not spec.duration:
        raise SchwabOrderValidationError("session and duration must not be empty")

    quantity = _schwab_quantity(order.quantity, asset_type)
    instruction = spec.instruction
    if instruction is None:
        if asset_type == "EQUITY":
            instruction = order.side.value
        elif asset_type == "OPTION":
            raise SchwabOrderValidationError(
                "option orders require an explicit open/close instruction"
            )
        else:
            instruction = order.side.value
    if not instruction:
        raise SchwabOrderValidationError("instruction must not be empty")

    instrument: dict[str, object] = dict(spec.instrument_fields)
    instrument.update({"symbol": order.symbol, "assetType": asset_type})
    payload: dict[str, object] = dict(spec.order_fields)
    payload.update(
        {
            "orderType": "LIMIT",
            "session": spec.session,
            "price": format(order.limit_price, "f"),
            "duration": spec.duration,
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": instruction,
                    "quantity": quantity,
                    "instrument": instrument,
                }
            ],
        }
    )
    return payload


def _schwab_quantity(quantity: Decimal, asset_type: str) -> int | str:
    value = Decimal(str(quantity))
    if not value.is_finite() or value <= 0:
        raise SchwabOrderValidationError("quantity must be a positive finite decimal")
    if asset_type in {"EQUITY", "OPTION"}:
        integral = value.to_integral_value()
        if value != integral:
            raise SchwabOrderValidationError(
                f"Schwab {asset_type.lower()} quantity must be an integer"
            )
        return int(integral)
    return format(value, "f")


def _safe_error_reason(error: BaseException) -> str:
    if isinstance(error, SchwabHttpError):
        return f"Schwab API HTTP {error.status_code}"
    return type(error).__name__
