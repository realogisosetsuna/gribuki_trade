"""Binance Spot public market data and signed Testnet/Live REST gateway."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import re
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode
from uuid import uuid4

from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.ports.broker import BrokerEvent

from .http import (
    AsyncHttpTransport,
    HttpRequest,
    HttpResponse,
    UrllibAsyncHttpTransport,
)
from .models import (
    ORDER_STATUS_EVENT,
    BinanceAccount,
    BinanceBalance,
    BinanceCommissionComponent,
    BinanceCommissionDiscount,
    BinanceCommissionRate,
    BinanceCredentials,
    BinanceEnvironment,
    BinanceOrderSnapshot,
    BinanceOrderUpdate,
    BinanceRateLimitUsage,
    BinanceTrade,
    Kline,
    OrderBookLevel,
    OrderBookSnapshot,
    TickerPrice,
)
from .rules import (
    BinanceValidationError,
    SymbolRules,
    decimal_from_api,
    decimal_to_fixed,
)


class BinanceError(RuntimeError):
    """Base class for safe-to-log Binance adapter failures."""


class BinanceConfigurationError(BinanceError):
    """The selected environment or signed endpoint is not configured safely."""


class BinanceProtocolError(BinanceError):
    """Binance or a transport returned a malformed response."""


class BinanceTransportError(BinanceError):
    """An HTTP request failed; no request URL or credential is retained."""


class BinanceAPIError(BinanceError):
    """A known Binance API rejection with a sanitized message."""

    def __init__(self, *, status_code: int, code: int | None, message: str) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        code_text = "unknown" if code is None else str(code)
        super().__init__(f"Binance API error (HTTP {status_code}, code {code_text}): {message}")


class BinanceUncertainResultError(BinanceAPIError):
    """The exchange may have executed a request and reconciliation is required."""


_CLIENT_ORDER_ID = re.compile(r"^[A-Za-z0-9._:/-]{1,36}$")
_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)\b(api[-_ ]?key|secret(?:[-_ ]?key)?|signature)\b\s*[:=]\s*[^\s,;&]+"
)


def sign_hmac_sha256(secret_key: str, payload: str) -> str:
    """Return the lowercase hexadecimal HMAC-SHA256 Binance signature."""

    return hmac.new(secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def _sanitize_message(message: object, secrets: Sequence[str]) -> str:
    text = str(message).replace("\r", " ").replace("\n", " ")
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    text = _SENSITIVE_ASSIGNMENT.sub(lambda match: f"{match.group(1)}=<redacted>", text)
    return text[:500] or "request rejected"


@dataclass(slots=True)
class _TrackedOrder:
    order: OrderIntent
    update: BinanceOrderUpdate
    submission_attempted: bool = False
    cancellation_attempted: bool = False


class BinanceSpotGateway:
    """Asynchronous Binance Spot REST adapter.

    Public methods never require credentials. Signed methods use HMAC-SHA256
    and the ``X-MBX-APIKEY`` header. LIVE construction is rejected unless
    ``allow_live=True`` is supplied explicitly.

    The gateway never automatically retries a submit or cancel request. A
    transport failure, HTTP 5xx, or Binance ``-1007`` produces an ``UNKNOWN``
    order event; callers must reconcile it through :meth:`query_order`.
    """

    def __init__(
        self,
        *,
        environment: BinanceEnvironment | str = BinanceEnvironment.TESTNET,
        credentials: BinanceCredentials | None = None,
        transport: AsyncHttpTransport | None = None,
        allow_live: bool = False,
        recv_window_ms: int = 5_000,
        timeout_seconds: float = 10.0,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        try:
            selected_environment = (
                environment
                if isinstance(environment, BinanceEnvironment)
                else BinanceEnvironment(str(environment).upper())
            )
        except ValueError:
            raise BinanceConfigurationError(
                f"unknown Binance environment: {environment!r}"
            ) from None
        if selected_environment is BinanceEnvironment.LIVE and not allow_live:
            raise BinanceConfigurationError(
                "Binance LIVE is disabled; pass allow_live=True explicitly to enable it"
            )
        if not 1 <= recv_window_ms <= 60_000:
            raise ValueError("recv_window_ms must be between 1 and 60000")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._environment = selected_environment
        self._credentials = credentials
        self._transport = transport if transport is not None else UrllibAsyncHttpTransport()
        self._recv_window_ms = recv_window_ms
        self._timeout_seconds = timeout_seconds
        self._clock_ms = clock_ms if clock_ms is not None else lambda: time.time_ns() // 1_000_000
        self._server_time_offset_ms = 0
        self._connected = False
        self._events: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._orders: dict[str, _TrackedOrder] = {}
        self._standalone_cancel_results: dict[tuple[str, str], BinanceOrderSnapshot] = {}
        self._symbol_rules: dict[str, SymbolRules] = {}
        self._rate_limit_usage = BinanceRateLimitUsage()
        self._lock = asyncio.Lock()

    def __repr__(self) -> str:
        return (
            f"BinanceSpotGateway(environment={self.environment.value!r}, "
            f"base_url={self.base_url!r}, connected={self.connected!r}, "
            f"credentials_configured={self._credentials is not None!r})"
        )

    @property
    def environment(self) -> BinanceEnvironment:
        return self._environment

    @property
    def base_url(self) -> str:
        return self._environment.base_url

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def rate_limit_usage(self) -> BinanceRateLimitUsage:
        """Return the most recent exchange rate-limit counters.

        Binance applies request-weight limits by IP and order limits by
        account.  Keeping these counters visible lets the engine fail closed
        before an HTTP 429/418 rather than treating rate limiting as an
        ordinary transient error.
        """

        return self._rate_limit_usage

    async def connect(self) -> None:
        """Enable order mutations without making a network request."""

        async with self._lock:
            self._connected = True

    async def disconnect(self) -> None:
        """Disable new order mutations while preserving reconciliation state."""

        async with self._lock:
            self._connected = False

    async def events(self) -> AsyncIterator[BrokerEvent]:
        while True:
            yield await self._events.get()

    def order_update(self, client_order_id: str) -> BinanceOrderUpdate | None:
        tracked = self._orders.get(client_order_id)
        return None if tracked is None else tracked.update

    async def ping(self) -> None:
        payload = await self._request_json("GET", "/api/v3/ping")
        if not isinstance(payload, Mapping):
            raise BinanceProtocolError("Binance ping response must be an object")

    async def server_time(self) -> int:
        payload = await self._request_json("GET", "/api/v3/time")
        mapping = self._require_mapping(payload, "server time")
        try:
            return int(mapping["serverTime"])
        except (KeyError, TypeError, ValueError):
            raise BinanceProtocolError("Binance server time response is malformed") from None

    @property
    def server_time_offset_ms(self) -> int:
        """Most recently measured ``server - local`` clock offset in milliseconds."""

        return self._server_time_offset_ms

    async def synchronize_time(self) -> int:
        """Measure Binance clock offset using the local request midpoint.

        This keeps signed requests inside ``recvWindow`` without changing the
        operating-system clock.  It is safe to call at startup and after an
        explicit Binance ``-1021`` timestamp rejection.
        """

        started_ms = self._clock_ms()
        exchange_ms = await self.server_time()
        finished_ms = self._clock_ms()
        local_midpoint_ms = started_ms + (finished_ms - started_ms) // 2
        self._server_time_offset_ms = exchange_ms - local_midpoint_ms
        return self._server_time_offset_ms

    async def exchange_info(self, symbol: str | None = None) -> dict[str, Any]:
        params: tuple[tuple[str, object], ...] = ()
        normalized_symbol: str | None = None
        if symbol is not None:
            normalized_symbol = self._normalize_symbol(symbol)
            params = (("symbol", normalized_symbol),)
        payload = await self._request_json("GET", "/api/v3/exchangeInfo", params=params)
        mapping = self._require_mapping(payload, "exchangeInfo")
        result = dict(mapping)
        self.cache_exchange_info(result)
        if normalized_symbol is not None and normalized_symbol not in self._symbol_rules:
            raise BinanceProtocolError(
                f"exchangeInfo response did not contain requested symbol {normalized_symbol}"
            )
        return result

    def cache_exchange_info(self, payload: Mapping[str, Any]) -> tuple[SymbolRules, ...]:
        """Parse and cache symbol filters, useful for startup and offline tests."""

        symbols = payload.get("symbols")
        if not isinstance(symbols, list):
            raise BinanceProtocolError("exchangeInfo symbols must be a list")
        parsed: list[SymbolRules] = []
        for value in symbols:
            if not isinstance(value, dict):
                raise BinanceProtocolError("exchangeInfo contains a malformed symbol")
            try:
                rules = SymbolRules.from_exchange_info(value)
            except BinanceValidationError as exc:
                raise BinanceProtocolError(str(exc)) from None
            self._symbol_rules[rules.symbol] = rules
            parsed.append(rules)
        return tuple(parsed)

    async def symbol_rules(self, symbol: str, *, refresh: bool = False) -> SymbolRules:
        normalized = self._normalize_symbol(symbol)
        if refresh or normalized not in self._symbol_rules:
            await self.exchange_info(normalized)
        return self._symbol_rules[normalized]

    async def ticker_price(self, symbol: str) -> TickerPrice:
        normalized = self._normalize_symbol(symbol)
        payload = await self._request_json(
            "GET", "/api/v3/ticker/price", params=(("symbol", normalized),)
        )
        mapping = self._require_mapping(payload, "ticker price")
        try:
            response_symbol = str(mapping["symbol"])
            price = decimal_from_api(mapping["price"], "price")
        except (KeyError, BinanceValidationError):
            raise BinanceProtocolError("Binance ticker price response is malformed") from None
        return TickerPrice(symbol=response_symbol, price=price)

    get_ticker_price = ticker_price

    async def order_book(self, symbol: str, *, limit: int = 100) -> OrderBookSnapshot:
        if limit not in {5, 10, 20, 50, 100, 500, 1_000, 5_000}:
            raise ValueError("unsupported Binance order-book limit")
        normalized = self._normalize_symbol(symbol)
        payload = await self._request_json(
            "GET",
            "/api/v3/depth",
            params=(("symbol", normalized), ("limit", limit)),
        )
        mapping = self._require_mapping(payload, "order book")
        try:
            update_id = int(mapping["lastUpdateId"])
            bids = self._parse_levels(mapping["bids"], "bids")
            asks = self._parse_levels(mapping["asks"], "asks")
        except (KeyError, TypeError, ValueError, BinanceValidationError):
            raise BinanceProtocolError("Binance order book response is malformed") from None
        return OrderBookSnapshot(
            symbol=normalized,
            last_update_id=update_id,
            bids=bids,
            asks=asks,
        )

    get_order_book = order_book

    async def klines(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> tuple[Kline, ...]:
        if not interval or len(interval) > 4:
            raise ValueError("invalid Binance kline interval")
        if not 1 <= limit <= 1_000:
            raise ValueError("kline limit must be between 1 and 1000")
        params: list[tuple[str, object]] = [
            ("symbol", self._normalize_symbol(symbol)),
            ("interval", interval),
            ("limit", limit),
        ]
        if start_time_ms is not None:
            params.append(("startTime", start_time_ms))
        if end_time_ms is not None:
            params.append(("endTime", end_time_ms))
        payload = await self._request_json("GET", "/api/v3/klines", params=tuple(params))
        if not isinstance(payload, list):
            raise BinanceProtocolError("Binance klines response must be a list")
        try:
            return tuple(self._parse_kline(item) for item in payload)
        except (TypeError, ValueError, IndexError, BinanceValidationError):
            raise BinanceProtocolError("Binance klines response is malformed") from None

    get_klines = klines

    async def validate_order_on_exchange(self, order: OrderIntent) -> None:
        """Validate a limit order through Binance without placing it.

        Binance's ``/api/v3/order/test`` endpoint performs signature, account,
        symbol-filter, and order-schema checks but never enters the matching
        engine. This method intentionally does not reserve the client order ID
        in the local OMS.
        """

        if not self._connected:
            raise ConnectionError("Binance gateway is not connected")
        self._require_credentials()
        if not _CLIENT_ORDER_ID.fullmatch(order.client_order_id):
            raise BinanceValidationError(
                "client_order_id must be 1-36 Binance-safe characters"
            )
        rules = await self.symbol_rules(order.symbol)
        rules.validate_limit_order(quantity=order.quantity, price=order.limit_price)
        await self._request_json(
            "POST",
            "/api/v3/order/test",
            params=(
                ("symbol", rules.symbol),
                ("side", order.side.value),
                ("type", "LIMIT"),
                ("timeInForce", "GTC"),
                ("quantity", decimal_to_fixed(order.quantity)),
                ("price", decimal_to_fixed(order.limit_price)),
                ("newClientOrderId", order.client_order_id),
                ("newOrderRespType", "RESULT"),
            ),
            signed=True,
            execution_sensitive=False,
        )

    test_order = validate_order_on_exchange

    async def submit_order(self, order: OrderIntent) -> None:
        """Validate and place one idempotent GTC limit order."""

        async with self._lock:
            existing = self._orders.get(order.client_order_id)
            if existing is not None:
                if existing.order != order:
                    raise ValueError(
                        f"client_order_id {order.client_order_id!r} is already used "
                        "for a different order"
                    )
                # Includes UNKNOWN: reconciliation is required, never a blind resubmit.
                return
            if not self._connected:
                update = BinanceOrderUpdate(
                    order=order,
                    status=OrderStatus.BROKER_REJECTED,
                    reason="Binance gateway is not connected",
                )
                self._orders[order.client_order_id] = _TrackedOrder(order, update)
                self._publish(update)
                return
            self._require_credentials()

            if not _CLIENT_ORDER_ID.fullmatch(order.client_order_id):
                update = BinanceOrderUpdate(
                    order=order,
                    status=OrderStatus.LOCAL_REJECTED,
                    reason="client_order_id must be 1-36 Binance-safe characters",
                )
                self._orders[order.client_order_id] = _TrackedOrder(order, update)
                self._publish(update)
                return

            rules = await self.symbol_rules(order.symbol)
            try:
                rules.validate_limit_order(quantity=order.quantity, price=order.limit_price)
            except (BinanceValidationError, TypeError) as exc:
                update = BinanceOrderUpdate(
                    order=order,
                    status=OrderStatus.LOCAL_REJECTED,
                    reason=str(exc),
                )
                self._orders[order.client_order_id] = _TrackedOrder(order, update)
                self._publish(update)
                return

            initial = BinanceOrderUpdate(order=order, status=OrderStatus.SUBMITTING)
            tracked = _TrackedOrder(order=order, update=initial, submission_attempted=True)
            self._orders[order.client_order_id] = tracked
            params: tuple[tuple[str, object], ...] = (
                ("symbol", rules.symbol),
                ("side", order.side.value),
                ("type", "LIMIT"),
                ("timeInForce", "GTC"),
                ("quantity", decimal_to_fixed(order.quantity)),
                ("price", decimal_to_fixed(order.limit_price)),
                ("newClientOrderId", order.client_order_id),
                ("newOrderRespType", "RESULT"),
            )
            try:
                payload = await self._request_json(
                    "POST",
                    "/api/v3/order",
                    params=params,
                    signed=True,
                    execution_sensitive=True,
                )
                snapshot = self._snapshot_from_payload(payload, fallback_symbol=rules.symbol)
            except (BinanceUncertainResultError, BinanceTransportError, BinanceProtocolError):
                self._record_update(
                    tracked,
                    BinanceOrderUpdate(
                        order=order,
                        status=OrderStatus.UNKNOWN,
                        reason="submission outcome unknown; reconcile with query_order",
                    ),
                )
                return
            except BinanceAPIError as exc:
                self._record_update(
                    tracked,
                    BinanceOrderUpdate(
                        order=order,
                        status=OrderStatus.BROKER_REJECTED,
                        reason=str(exc),
                    ),
                )
                return

            self._record_snapshot(tracked, snapshot)

    async def cancel_order(self, client_order_id: str) -> None:
        """Cancel a locally tracked order without automatic retries."""

        async with self._lock:
            tracked = self._orders.get(client_order_id)
            if tracked is None:
                raise KeyError(f"unknown client_order_id: {client_order_id!r}")
            await self._cancel_order_by_client_id_locked(
                tracked.order.symbol,
                client_order_id,
            )

    async def cancel_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> BinanceOrderSnapshot:
        """Cancel by durable exchange identity, even after a process restart.

        Unlike :meth:`cancel_order`, this method does not require the order to
        have been submitted by this gateway instance.  It performs exactly one
        signed DELETE.  An uncertain transport/protocol result is returned as
        ``UNKNOWN`` and is remembered, so repeating the call cannot blindly
        send a second cancellation before :meth:`get_order` reconciles it.
        """

        async with self._lock:
            return await self._cancel_order_by_client_id_locked(symbol, client_order_id)

    async def _cancel_order_by_client_id_locked(
        self,
        symbol: str,
        client_order_id: str,
    ) -> BinanceOrderSnapshot:
        normalized = self._normalize_symbol(symbol)
        if not _CLIENT_ORDER_ID.fullmatch(client_order_id):
            raise ValueError("client_order_id must be 1-36 Binance-safe characters")
        tracked = self._orders.get(client_order_id)
        if tracked is not None:
            tracked_symbol = self._normalize_symbol(tracked.order.symbol)
            if tracked_symbol != normalized:
                raise ValueError(
                    f"client_order_id {client_order_id!r} belongs to {tracked_symbol}, "
                    f"not {normalized}"
                )
            if tracked.update.status in {
                OrderStatus.CANCELED,
                OrderStatus.FILLED,
                OrderStatus.EXPIRED,
                OrderStatus.BROKER_REJECTED,
                OrderStatus.LOCAL_REJECTED,
            }:
                return self._snapshot_from_tracked(tracked)
            if tracked.cancellation_attempted:
                # An UNKNOWN cancellation must be queried before another attempt.
                return self._snapshot_from_tracked(tracked)
            if tracked.update.status is OrderStatus.UNKNOWN:
                raise ValueError(
                    f"order {client_order_id!r} is UNKNOWN; query it before cancellation"
                )

        cancellation_key = (normalized, client_order_id)
        previous = self._standalone_cancel_results.get(cancellation_key)
        if previous is not None:
            return previous
        if not self._connected:
            raise ConnectionError("Binance gateway is not connected")
        self._require_credentials()

        if tracked is not None:
            tracked.cancellation_attempted = True
        try:
            payload = await self._request_json(
                "DELETE",
                "/api/v3/order",
                params=(
                    ("symbol", normalized),
                    ("origClientOrderId", client_order_id),
                ),
                signed=True,
                execution_sensitive=True,
            )
            snapshot = self._snapshot_from_payload(payload, fallback_symbol=normalized)
        except (BinanceUncertainResultError, BinanceTransportError, BinanceProtocolError):
            snapshot = BinanceOrderSnapshot(
                symbol=normalized,
                client_order_id=client_order_id,
                order_id=(tracked.update.exchange_order_id if tracked is not None else None),
                status=OrderStatus.UNKNOWN,
                exchange_status=None,
                side=(tracked.order.side if tracked is not None else None),
                price=(tracked.order.limit_price if tracked is not None else None),
                original_quantity=(tracked.order.quantity if tracked is not None else None),
                executed_quantity=(
                    tracked.update.executed_quantity if tracked is not None else Decimal("0")
                ),
            )
            self._standalone_cancel_results[cancellation_key] = snapshot
            if tracked is not None:
                self._record_update(
                    tracked,
                    BinanceOrderUpdate(
                        order=tracked.order,
                        status=OrderStatus.UNKNOWN,
                        exchange_order_id=tracked.update.exchange_order_id,
                        executed_quantity=tracked.update.executed_quantity,
                        reason="cancellation outcome unknown; reconcile with query_order",
                    ),
                )
            return snapshot
        except BinanceAPIError:
            # A known rejection did not execute. There is no hidden retry;
            # expose it so the caller can decide what to do next.
            if tracked is not None:
                tracked.cancellation_attempted = False
            raise

        if snapshot.symbol != normalized or (
            snapshot.client_order_id is not None
            and snapshot.client_order_id != client_order_id
        ):
            # The DELETE reached Binance, so a mismatched/malformed identity is
            # still an uncertain execution result.  Quarantine it exactly like
            # a transport failure instead of allowing a blind second request.
            snapshot = BinanceOrderSnapshot(
                symbol=normalized,
                client_order_id=client_order_id,
                order_id=None,
                status=OrderStatus.UNKNOWN,
                exchange_status=None,
                side=(tracked.order.side if tracked is not None else None),
                price=(tracked.order.limit_price if tracked is not None else None),
                original_quantity=(tracked.order.quantity if tracked is not None else None),
                executed_quantity=(
                    tracked.update.executed_quantity if tracked is not None else Decimal("0")
                ),
            )
            self._standalone_cancel_results[cancellation_key] = snapshot
            if tracked is not None:
                self._record_update(
                    tracked,
                    BinanceOrderUpdate(
                        order=tracked.order,
                        status=OrderStatus.UNKNOWN,
                        exchange_order_id=tracked.update.exchange_order_id,
                        executed_quantity=tracked.update.executed_quantity,
                        reason="cancellation response identity unknown; reconcile with query_order",
                    ),
                )
            return snapshot
        self._standalone_cancel_results[cancellation_key] = snapshot
        if tracked is not None:
            self._record_snapshot(tracked, snapshot)
        return snapshot

    async def query_order(
        self,
        client_order_id: str,
        *,
        symbol: str | None = None,
    ) -> BinanceOrderSnapshot:
        """Reconcile an order by client id, including an UNKNOWN submission."""

        tracked = self._orders.get(client_order_id)
        if symbol is None:
            if tracked is None:
                raise KeyError(
                    "symbol is required when client_order_id is not locally tracked"
                )
            symbol = tracked.order.symbol
        return await self.get_order(symbol, client_order_id=client_order_id)

    async def get_order(
        self,
        symbol: str,
        *,
        client_order_id: str | None = None,
        order_id: int | None = None,
    ) -> BinanceOrderSnapshot:
        """Query Binance directly by exactly one exchange or client order id."""

        if (client_order_id is None) == (order_id is None):
            raise ValueError("provide exactly one of client_order_id or order_id")
        self._require_credentials()
        normalized = self._normalize_symbol(symbol)
        params: list[tuple[str, object]] = [("symbol", normalized)]
        if client_order_id is not None:
            params.append(("origClientOrderId", client_order_id))
        else:
            params.append(("orderId", order_id))
        try:
            payload = await self._request_json(
                "GET",
                "/api/v3/order",
                params=tuple(params),
                signed=True,
                execution_sensitive=True,
            )
            snapshot = self._snapshot_from_payload(payload, fallback_symbol=normalized)
        except (BinanceUncertainResultError, BinanceTransportError, BinanceProtocolError):
            snapshot = BinanceOrderSnapshot(
                symbol=normalized,
                client_order_id=client_order_id,
                order_id=order_id,
                status=OrderStatus.UNKNOWN,
                exchange_status=None,
                side=None,
                price=None,
                original_quantity=None,
                executed_quantity=Decimal("0"),
            )
            tracked = self._find_tracked(client_order_id=client_order_id, order_id=order_id)
            if tracked is not None:
                self._record_update(
                    tracked,
                    BinanceOrderUpdate(
                        order=tracked.order,
                        status=OrderStatus.UNKNOWN,
                        exchange_order_id=tracked.update.exchange_order_id,
                        executed_quantity=tracked.update.executed_quantity,
                        reason="order query outcome unknown; reconciliation is still required",
                    ),
                )
            return snapshot

        tracked = self._find_tracked(
            client_order_id=snapshot.client_order_id or client_order_id,
            order_id=snapshot.order_id,
        )
        if client_order_id is not None and snapshot.status in {
            OrderStatus.ACCEPTED,
            OrderStatus.PARTIALLY_FILLED,
        }:
            # A successful query proves a previous standalone UNKNOWN cancel
            # left the order open, permitting one deliberate new cancel.
            self._standalone_cancel_results.pop((normalized, client_order_id), None)
        if tracked is not None:
            self._record_snapshot(tracked, snapshot)
            if snapshot.status in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED}:
                # A successful query proves a previous UNKNOWN cancel did not
                # leave this order terminal, so a deliberate new cancel is safe.
                tracked.cancellation_attempted = False
        return snapshot

    async def open_orders(
        self,
        symbol: str | None = None,
    ) -> tuple[BinanceOrderSnapshot, ...]:
        """Return all currently open Spot orders, optionally for one symbol."""

        self._require_credentials()
        params: tuple[tuple[str, object], ...] = ()
        fallback_symbol = ""
        if symbol is not None:
            fallback_symbol = self._normalize_symbol(symbol)
            params = (("symbol", fallback_symbol),)
        payload = await self._request_json(
            "GET",
            "/api/v3/openOrders",
            params=params,
            signed=True,
            execution_sensitive=False,
        )
        return self._parse_order_list(payload, fallback_symbol=fallback_symbol)

    async def all_orders(
        self,
        symbol: str,
        *,
        order_id: int | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> tuple[BinanceOrderSnapshot, ...]:
        """Return Spot order history for startup and periodic reconciliation."""

        if not 1 <= limit <= 1_000:
            raise ValueError("order-history limit must be between 1 and 1000")
        self._require_credentials()
        normalized = self._normalize_symbol(symbol)
        params: list[tuple[str, object]] = [("symbol", normalized), ("limit", limit)]
        if order_id is not None:
            if order_id < 0:
                raise ValueError("order_id must be non-negative")
            params.append(("orderId", order_id))
        if start_time_ms is not None:
            if start_time_ms < 0:
                raise ValueError("start_time_ms must be non-negative")
            params.append(("startTime", start_time_ms))
        if end_time_ms is not None:
            if end_time_ms < 0:
                raise ValueError("end_time_ms must be non-negative")
            params.append(("endTime", end_time_ms))
        if (
            start_time_ms is not None
            and end_time_ms is not None
            and end_time_ms < start_time_ms
        ):
            raise ValueError("end_time_ms must not precede start_time_ms")
        payload = await self._request_json(
            "GET",
            "/api/v3/allOrders",
            params=tuple(params),
            signed=True,
            execution_sensitive=False,
        )
        return self._parse_order_list(payload, fallback_symbol=normalized)

    async def account_trades(
        self,
        symbol: str,
        *,
        order_id: int | None = None,
        from_id: int | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> tuple[BinanceTrade, ...]:
        """Return immutable account trades used to recover missed user events."""

        if not 1 <= limit <= 1_000:
            raise ValueError("trade-history limit must be between 1 and 1000")
        normalized = self._normalize_symbol(symbol)
        self._require_credentials()
        params: list[tuple[str, object]] = [("symbol", normalized), ("limit", limit)]
        for name, value in (
            ("orderId", order_id),
            ("fromId", from_id),
            ("startTime", start_time_ms),
            ("endTime", end_time_ms),
        ):
            if value is not None:
                if value < 0:
                    raise ValueError(f"{name} must be non-negative")
                params.append((name, value))
        if (
            start_time_ms is not None
            and end_time_ms is not None
            and end_time_ms < start_time_ms
        ):
            raise ValueError("end_time_ms must not precede start_time_ms")
        payload = await self._request_json(
            "GET",
            "/api/v3/myTrades",
            params=tuple(params),
            signed=True,
            execution_sensitive=False,
        )
        if not isinstance(payload, list):
            raise BinanceProtocolError("Binance account trades response must be a list")
        try:
            return tuple(self._parse_trade(item, fallback_symbol=normalized) for item in payload)
        except (KeyError, TypeError, ValueError, BinanceValidationError):
            raise BinanceProtocolError("Binance account trades response is malformed") from None

    async def commission_rate(self, symbol: str) -> BinanceCommissionRate:
        """Query account-specific Spot commission rates for one symbol."""

        normalized = self._normalize_symbol(symbol)
        self._require_credentials()
        payload = await self._request_json(
            "GET",
            "/api/v3/account/commission",
            params=(("symbol", normalized),),
            signed=True,
            execution_sensitive=False,
        )
        mapping = self._require_mapping(payload, "commission rate")
        try:
            discount_value = mapping["discount"]
            if not isinstance(discount_value, Mapping):
                raise TypeError("discount")
            return BinanceCommissionRate(
                symbol=str(mapping.get("symbol", normalized)).upper(),
                standard=self._parse_commission_component(mapping["standardCommission"]),
                tax=self._parse_commission_component(mapping["taxCommission"]),
                special=self._parse_commission_component(mapping["specialCommission"]),
                discount=BinanceCommissionDiscount(
                    enabled_for_account=bool(discount_value.get("enabledForAccount", False)),
                    enabled_for_symbol=bool(discount_value.get("enabledForSymbol", False)),
                    asset=str(discount_value.get("discountAsset", "")),
                    discount=decimal_from_api(discount_value.get("discount", "0"), "discount"),
                ),
            )
        except (KeyError, TypeError, BinanceValidationError):
            raise BinanceProtocolError("Binance commission response is malformed") from None

    get_open_orders = open_orders
    get_all_orders = all_orders
    get_account_trades = account_trades
    get_commission_rate = commission_rate

    async def account(self) -> BinanceAccount:
        """Return signed Spot account balances and permissions."""

        self._require_credentials()
        payload = await self._request_json(
            "GET", "/api/v3/account", signed=True, execution_sensitive=False
        )
        mapping = self._require_mapping(payload, "account")
        balances_value = mapping.get("balances")
        if not isinstance(balances_value, list):
            raise BinanceProtocolError("Binance account balances are malformed")
        try:
            balances = tuple(
                BinanceBalance(
                    asset=str(item["asset"]),
                    free=decimal_from_api(item["free"], "free"),
                    locked=decimal_from_api(item["locked"], "locked"),
                )
                for item in balances_value
                if isinstance(item, Mapping)
            )
            update_time_value = mapping.get("updateTime")
            update_time = int(update_time_value) if update_time_value is not None else None
            permissions_value = mapping.get("permissions", [])
            if not isinstance(permissions_value, list):
                raise TypeError("permissions")
            return BinanceAccount(
                can_trade=bool(mapping.get("canTrade", False)),
                can_withdraw=bool(mapping.get("canWithdraw", False)),
                can_deposit=bool(mapping.get("canDeposit", False)),
                account_type=str(mapping.get("accountType", "")),
                balances=balances,
                update_time_ms=update_time,
                permissions=tuple(str(value).upper() for value in permissions_value),
                uid=self._optional_integer(mapping.get("uid"), "uid"),
                maker_commission=self._optional_integer(
                    mapping.get("makerCommission"), "makerCommission"
                ),
                taker_commission=self._optional_integer(
                    mapping.get("takerCommission"), "takerCommission"
                ),
                buyer_commission=self._optional_integer(
                    mapping.get("buyerCommission"), "buyerCommission"
                ),
                seller_commission=self._optional_integer(
                    mapping.get("sellerCommission"), "sellerCommission"
                ),
                brokered=bool(mapping.get("brokered", False)),
                require_self_trade_prevention=bool(
                    mapping.get("requireSelfTradePrevention", False)
                ),
                prevent_sor=bool(mapping.get("preventSor", False)),
            )
        except (KeyError, TypeError, ValueError, BinanceValidationError):
            raise BinanceProtocolError("Binance account response is malformed") from None

    get_account = account

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: tuple[tuple[str, object], ...] = (),
        signed: bool = False,
        execution_sensitive: bool = False,
        retry_timestamp_rejection: bool = True,
    ) -> Any:
        headers = {"Accept": "application/json"}
        encoded_params = list(params)
        credentials: BinanceCredentials | None = None
        signature = ""
        if signed:
            credentials = self._require_credentials()
            encoded_params.extend(
                (
                    ("recvWindow", self._recv_window_ms),
                    ("timestamp", self._clock_ms() + self._server_time_offset_ms),
                )
            )
        payload = urlencode(
            [(key, self._parameter_text(value)) for key, value in encoded_params],
            encoding="utf-8",
            safe="",
        )
        if credentials is not None:
            signature = sign_hmac_sha256(credentials.secret_key, payload)
            payload = f"{payload}&signature={signature}" if payload else f"signature={signature}"
            headers["X-MBX-APIKEY"] = credentials.api_key

        url = f"{self.base_url}{path}"
        body: bytes | None = None
        if method in {"POST", "PUT", "DELETE"}:
            body = payload.encode("utf-8") if payload else None
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        elif payload:
            url = f"{url}?{payload}"

        request = HttpRequest(
            method=method,
            url=url,
            headers=headers,
            body=body,
            timeout_seconds=self._timeout_seconds,
        )
        try:
            response = await self._transport.request(request)
        except Exception:
            # An injected transport might include the full request in its own
            # exception. Replace it with a stable, credential-free error.
            raise BinanceTransportError("Binance HTTP request failed") from None
        if not isinstance(response, HttpResponse):
            raise BinanceProtocolError("HTTP transport returned an invalid response")
        self._update_rate_limit_usage(response.headers)
        try:
            response_payload = response.json()
        except (UnicodeDecodeError, ValueError):
            raise BinanceProtocolError(
                f"Binance returned non-JSON data (HTTP {response.status_code})"
            ) from None

        if 200 <= response.status_code < 300:
            if isinstance(response_payload, Mapping) and self._api_code(response_payload) == -1007:
                raise self._api_error(
                    response.status_code,
                    response_payload,
                    uncertain=True,
                    signature=signature,
                )
            return response_payload

        api_code = self._api_code(response_payload)
        if signed and api_code == -1021 and retry_timestamp_rejection:
            await self.synchronize_time()
            return await self._request_json(
                method,
                path,
                params=params,
                signed=True,
                execution_sensitive=execution_sensitive,
                retry_timestamp_rejection=False,
            )

        uncertain = api_code == -1007 or (
            execution_sensitive and response.status_code >= 500
        )
        raise self._api_error(
            response.status_code,
            response_payload,
            uncertain=uncertain,
            signature=signature,
        )

    def _api_error(
        self,
        status_code: int,
        payload: object,
        *,
        uncertain: bool,
        signature: str,
    ) -> BinanceAPIError:
        code: int | None = None
        message: object = "request rejected"
        if isinstance(payload, Mapping):
            code = self._api_code(payload)
            message = payload.get("msg", message)
        secrets = [signature]
        if self._credentials is not None:
            secrets.extend((self._credentials.api_key, self._credentials.secret_key))
        error_type = BinanceUncertainResultError if uncertain else BinanceAPIError
        return error_type(
            status_code=status_code,
            code=code,
            message=_sanitize_message(message, secrets),
        )

    @staticmethod
    def _api_code(payload: Mapping[object, object]) -> int | None:
        try:
            value = payload.get("code")
            if isinstance(value, bool) or not isinstance(value, (str, bytes, int)):
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    def _require_credentials(self) -> BinanceCredentials:
        if self._credentials is None:
            raise BinanceConfigurationError(
                "Binance credentials are required for this signed endpoint"
            )
        return self._credentials

    @staticmethod
    def _parameter_text(value: object) -> str:
        if isinstance(value, Decimal):
            return decimal_to_fixed(value)
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (str, int)):
            return str(value)
        if value is None:
            raise TypeError("Binance request parameters cannot be None")
        raise TypeError(f"unsupported Binance request parameter type: {type(value).__name__}")

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        normalized = symbol.strip().upper()
        if not normalized or not normalized.isascii() or not normalized.isalnum():
            raise ValueError(f"invalid Binance symbol: {symbol!r}")
        return normalized

    @staticmethod
    def _require_mapping(payload: object, label: str) -> Mapping[str, Any]:
        if not isinstance(payload, Mapping):
            raise BinanceProtocolError(f"Binance {label} response must be an object")
        return payload

    @staticmethod
    def _parse_levels(value: object, label: str) -> tuple[OrderBookLevel, ...]:
        if not isinstance(value, list):
            raise TypeError(label)
        return tuple(
            OrderBookLevel(
                price=decimal_from_api(item[0], f"{label}.price"),
                quantity=decimal_from_api(item[1], f"{label}.quantity"),
            )
            for item in value
            if isinstance(item, list) and len(item) >= 2
        )

    @staticmethod
    def _parse_kline(item: object) -> Kline:
        if not isinstance(item, list) or len(item) < 11:
            raise TypeError("kline")
        return Kline(
            open_time_ms=int(item[0]),
            open=decimal_from_api(item[1], "open"),
            high=decimal_from_api(item[2], "high"),
            low=decimal_from_api(item[3], "low"),
            close=decimal_from_api(item[4], "close"),
            volume=decimal_from_api(item[5], "volume"),
            close_time_ms=int(item[6]),
            quote_volume=decimal_from_api(item[7], "quoteVolume"),
            trade_count=int(item[8]),
            taker_buy_base_volume=decimal_from_api(item[9], "takerBuyBaseVolume"),
            taker_buy_quote_volume=decimal_from_api(item[10], "takerBuyQuoteVolume"),
        )

    def _parse_order_list(
        self,
        payload: object,
        *,
        fallback_symbol: str,
    ) -> tuple[BinanceOrderSnapshot, ...]:
        if not isinstance(payload, list):
            raise BinanceProtocolError("Binance order-list response must be a list")
        try:
            return tuple(
                self._snapshot_from_payload(item, fallback_symbol=fallback_symbol)
                for item in payload
            )
        except BinanceProtocolError:
            raise
        except (TypeError, ValueError, BinanceValidationError):
            raise BinanceProtocolError("Binance order-list response is malformed") from None

    @staticmethod
    def _parse_trade(item: object, *, fallback_symbol: str) -> BinanceTrade:
        if not isinstance(item, Mapping):
            raise TypeError("trade")
        return BinanceTrade(
            symbol=str(item.get("symbol", fallback_symbol)).upper(),
            trade_id=int(item["id"]),
            order_id=int(item["orderId"]),
            price=decimal_from_api(item["price"], "price"),
            quantity=decimal_from_api(item["qty"], "qty"),
            quote_quantity=decimal_from_api(item["quoteQty"], "quoteQty"),
            commission=decimal_from_api(item["commission"], "commission"),
            commission_asset=str(item["commissionAsset"]),
            time_ms=int(item["time"]),
            is_buyer=bool(item.get("isBuyer", False)),
            is_maker=bool(item.get("isMaker", False)),
            is_best_match=bool(item.get("isBestMatch", False)),
        )

    @staticmethod
    def _parse_commission_component(value: object) -> BinanceCommissionComponent:
        if not isinstance(value, Mapping):
            raise TypeError("commission component")
        return BinanceCommissionComponent(
            maker=decimal_from_api(value["maker"], "maker"),
            taker=decimal_from_api(value["taker"], "taker"),
            buyer=decimal_from_api(value["buyer"], "buyer"),
            seller=decimal_from_api(value["seller"], "seller"),
        )

    @staticmethod
    def _optional_integer(value: object, label: str) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            raise TypeError(label)
        if isinstance(value, int):
            result = value
        elif isinstance(value, str):
            result = int(value)
        else:
            raise TypeError(label)
        if result < 0:
            raise ValueError(label)
        return result

    def _update_rate_limit_usage(self, headers: Mapping[str, str]) -> None:
        normalized = {str(key).lower(): str(value) for key, value in headers.items()}

        def read(name: str) -> int | None:
            raw = normalized.get(name.lower())
            if raw is None:
                return None
            try:
                value = int(raw)
            except ValueError:
                return None
            return value if value >= 0 else None

        observed = BinanceRateLimitUsage(
            used_weight_1m=read("x-mbx-used-weight-1m"),
            order_count_10s=read("x-mbx-order-count-10s"),
            order_count_1d=read("x-mbx-order-count-1d"),
            retry_after_seconds=read("retry-after"),
        )
        prior = self._rate_limit_usage
        self._rate_limit_usage = BinanceRateLimitUsage(
            used_weight_1m=(
                observed.used_weight_1m
                if observed.used_weight_1m is not None
                else prior.used_weight_1m
            ),
            order_count_10s=(
                observed.order_count_10s
                if observed.order_count_10s is not None
                else prior.order_count_10s
            ),
            order_count_1d=(
                observed.order_count_1d
                if observed.order_count_1d is not None
                else prior.order_count_1d
            ),
            retry_after_seconds=observed.retry_after_seconds,
        )

    def _snapshot_from_payload(
        self,
        payload: object,
        *,
        fallback_symbol: str,
    ) -> BinanceOrderSnapshot:
        mapping = self._require_mapping(payload, "order")
        exchange_status_value = mapping.get("status")
        exchange_status = (
            str(exchange_status_value).upper() if exchange_status_value is not None else None
        )
        side: Side | None = None
        if mapping.get("side") is not None:
            try:
                side = Side(str(mapping["side"]).upper())
            except ValueError:
                side = None
        try:
            order_id_value = mapping.get("orderId")
            order_id = int(order_id_value) if order_id_value is not None else None
            price_value = mapping.get("price")
            price = decimal_from_api(price_value, "price") if price_value is not None else None
            original_value = mapping.get("origQty")
            original_quantity = (
                decimal_from_api(original_value, "origQty") if original_value is not None else None
            )
            executed_quantity = decimal_from_api(
                mapping.get("executedQty", "0"), "executedQty"
            )
            cumulative_value = mapping.get("cummulativeQuoteQty")
            cumulative_quote_quantity = (
                decimal_from_api(cumulative_value, "cummulativeQuoteQty")
                if cumulative_value is not None
                else None
            )
            time_value = mapping.get("transactTime", mapping.get("time", mapping.get("updateTime")))
            transact_time = int(time_value) if time_value is not None else None
        except (TypeError, ValueError, BinanceValidationError):
            raise BinanceProtocolError("Binance order response is malformed") from None
        client_value = mapping.get("clientOrderId", mapping.get("origClientOrderId"))
        return BinanceOrderSnapshot(
            symbol=str(mapping.get("symbol", fallback_symbol)).upper(),
            client_order_id=str(client_value) if client_value is not None else None,
            order_id=order_id,
            status=self._map_order_status(exchange_status),
            exchange_status=exchange_status,
            side=side,
            price=price,
            original_quantity=original_quantity,
            executed_quantity=executed_quantity,
            transact_time_ms=transact_time,
            cumulative_quote_quantity=cumulative_quote_quantity,
        )

    @staticmethod
    def _map_order_status(exchange_status: str | None) -> OrderStatus:
        if exchange_status is None:
            return OrderStatus.UNKNOWN
        return {
            "NEW": OrderStatus.ACCEPTED,
            "PENDING_NEW": OrderStatus.SUBMITTING,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "PENDING_CANCEL": OrderStatus.CANCEL_PENDING,
            "CANCELED": OrderStatus.CANCELED,
            "REJECTED": OrderStatus.BROKER_REJECTED,
            "EXPIRED": OrderStatus.EXPIRED,
            "EXPIRED_IN_MATCH": OrderStatus.EXPIRED,
        }.get(exchange_status, OrderStatus.UNKNOWN)

    def _record_snapshot(
        self,
        tracked: _TrackedOrder,
        snapshot: BinanceOrderSnapshot,
    ) -> None:
        self._record_update(
            tracked,
            BinanceOrderUpdate(
                order=tracked.order,
                status=snapshot.status,
                exchange_order_id=snapshot.order_id,
                executed_quantity=snapshot.executed_quantity,
            ),
        )

    @staticmethod
    def _snapshot_from_tracked(tracked: _TrackedOrder) -> BinanceOrderSnapshot:
        return BinanceOrderSnapshot(
            symbol=tracked.order.symbol,
            client_order_id=tracked.order.client_order_id,
            order_id=tracked.update.exchange_order_id,
            status=tracked.update.status,
            exchange_status=None,
            side=tracked.order.side,
            price=tracked.order.limit_price,
            original_quantity=tracked.order.quantity,
            executed_quantity=tracked.update.executed_quantity,
        )

    def _record_update(self, tracked: _TrackedOrder, update: BinanceOrderUpdate) -> None:
        if tracked.update == update:
            return
        tracked.update = update
        self._publish(update)

    def _find_tracked(
        self,
        *,
        client_order_id: str | None,
        order_id: int | None,
    ) -> _TrackedOrder | None:
        if client_order_id is not None:
            tracked = self._orders.get(client_order_id)
            if tracked is not None:
                return tracked
        if order_id is not None:
            for value in self._orders.values():
                if value.update.exchange_order_id == order_id:
                    return value
        return None

    def _publish(self, update: BinanceOrderUpdate) -> None:
        occurred_at = datetime.now(UTC)
        payload = BinanceOrderUpdate(
            order=update.order,
            status=update.status,
            exchange_order_id=update.exchange_order_id,
            executed_quantity=update.executed_quantity,
            reason=update.reason,
            occurred_at=occurred_at,
        )
        tracked = self._orders.get(update.order.client_order_id)
        if tracked is not None:
            tracked.update = payload
        self._events.put_nowait(
            BrokerEvent(
                event_id=str(uuid4()),
                event_type=ORDER_STATUS_EVENT,
                occurred_at=occurred_at,
                payload=payload,
            )
        )
