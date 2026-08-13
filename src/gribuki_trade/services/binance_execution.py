"""Recoverable Binance Spot Testnet execution orchestration.

This service is intentionally Testnet-only.  It joins the Binance REST and
user-data adapters to the durable SQLite OMS without adding a LIVE switch or
an endpoint fallback.  Every submit is stored and leased before the adapter is
called; an outcome that cannot be proved becomes ``UNKNOWN`` and is only
resolved by an authoritative exchange event or REST reconciliation.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

from gribuki_trade.adapters.binance import (
    BinanceAccount,
    BinanceAPIError,
    BinanceBalanceUpdate,
    BinanceEnvironment,
    BinanceEventStreamTerminated,
    BinanceExecutionReport,
    BinanceOrderSnapshot,
    BinanceOrderUpdate,
    BinanceOutboundAccountPosition,
    BinanceTrade,
    BinanceUserDataEvent,
)
from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.trading import (
    AssetBalance,
    BalanceValue,
    ExecutionFill,
    OrderSnapshot,
    SQLiteOrderManagementStore,
    TradingCommand,
    TradingCommandStatus,
    TradingCommandType,
)


class BinanceSpotExecutionGateway(Protocol):
    """REST surface needed by the recoverable Testnet service."""

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


class BinanceUserDataSource(Protocol):
    """Private stream surface used by the execution service."""

    @property
    def environment(self) -> BinanceEnvironment: ...

    def events(self) -> AsyncIterator[BinanceUserDataEvent]: ...

    async def aclose(self) -> None: ...


class BinanceTestnetOnlyError(ValueError):
    """Raised before construction if any dependency targets Binance LIVE."""


@dataclass(frozen=True, slots=True)
class BinanceStartupReconciliation:
    """Observable result of one fail-closed startup reconciliation."""

    recovered_commands: int
    exchange_open_orders: int
    exchange_history_orders: int
    exchange_trades: int
    reconciled_orders: int
    recorded_fills: int
    recorded_balances: int
    dispatched_pending_commands: int = 0
    unresolved_order_ids: tuple[str, ...] = ()


class BinanceSpotTestnetExecutionService:
    """Persist, submit, stream, and reconcile Binance Spot Testnet orders.

    The constructor has no LIVE escape hatch.  Both concrete adapters must
    advertise ``TESTNET`` and every order must belong to the configured local
    account and symbol allow-list.
    """

    def __init__(
        self,
        gateway: BinanceSpotExecutionGateway,
        oms: SQLiteOrderManagementStore,
        *,
        account_id: str,
        symbols: Sequence[str],
        user_stream: BinanceUserDataSource | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        _require_testnet(gateway, "gateway")
        if user_stream is not None:
            _require_testnet(user_stream, "user_stream")
        normalized_account = account_id.strip()
        if not normalized_account:
            raise ValueError("account_id must not be empty")
        normalized_symbols = tuple(dict.fromkeys(symbol.strip().upper() for symbol in symbols))
        if not normalized_symbols or any(not symbol for symbol in normalized_symbols):
            raise ValueError("symbols must contain at least one non-empty symbol")

        self._gateway = gateway
        self._oms = oms
        self._account_id = normalized_account
        self._symbols = normalized_symbols
        self._symbol_set = frozenset(normalized_symbols)
        self._user_stream = user_stream
        self._clock = clock or (lambda: datetime.now(UTC))
        self._operation_lock = asyncio.Lock()
        self._started = False
        self._stopping = False

    @property
    def account_id(self) -> str:
        return self._account_id

    @property
    def symbols(self) -> tuple[str, ...]:
        return self._symbols

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> BinanceStartupReconciliation:
        """Connect, reconcile every authoritative source, then send safe pending work."""

        async with self._operation_lock:
            if self._started:
                raise RuntimeError("Binance Testnet execution service is already started")
            self._stopping = False
            recovered = self._oms.recover_after_restart(
                now=self._now(),
                account_id=self._account_id,
                symbols=self._symbols,
            )
            await self._gateway.connect()
            try:
                result = await self._reconcile_startup(recovered_commands=len(recovered))
                if result.unresolved_order_ids:
                    dispatched = 0
                else:
                    dispatched = await self._dispatch_pending()
            except BaseException:
                await self._gateway.disconnect()
                raise
            self._started = True
            unresolved = tuple(
                order.order.client_order_id
                for order in self._oms.orders_requiring_reconciliation(
                    account_id=self._account_id, symbols=self._symbols
                )
            )
            return replace(
                result,
                dispatched_pending_commands=dispatched,
                unresolved_order_ids=unresolved,
            )

    async def stop(self) -> None:
        """Stop the private stream and disconnect the Testnet REST adapter."""

        self._stopping = True
        if self._user_stream is not None:
            await self._user_stream.aclose()
        async with self._operation_lock:
            await self._gateway.disconnect()
            self._started = False

    async def submit(self, order: OrderIntent) -> OrderSnapshot:
        """Atomically create/lease an order before the Testnet REST call."""

        self._validate_order(order)
        async with self._operation_lock:
            self._require_started()
            persisted = self._oms.create_order(order)
            command = self._submit_command(order.client_order_id)
            if command.status is TradingCommandStatus.PENDING:
                claimed = self._oms.claim_command(command.command_id, now=self._now())
                await self._dispatch_submit(claimed)
            return self._oms.require_order(persisted.order.client_order_id)

    async def cancel(self, client_order_id: str) -> OrderSnapshot:
        """Persist and lease one Testnet cancellation before crossing REST."""

        async with self._operation_lock:
            self._require_started()
            current = self._oms.require_order(client_order_id)
            self._validate_order(current.order)
            existing = self._command(f"cancel:{client_order_id}")
            if existing is not None:
                # SENT and UNKNOWN are both final delivery decisions here: neither
                # permits a blind second cancellation request.
                return current
            if current.status not in {
                OrderStatus.ACCEPTED,
                OrderStatus.PARTIALLY_FILLED,
            }:
                raise ValueError(
                    f"order {client_order_id!r} cannot be canceled from {current.status.value}"
                )
            command = self._oms.enqueue_cancel(
                client_order_id,
                occurred_at=self._now(),
            )
            claimed = self._oms.claim_command(command.command_id, now=self._now())
            await self._dispatch_cancel(claimed)
            return self._oms.require_order(client_order_id)

    async def reconcile_startup(self) -> BinanceStartupReconciliation:
        """Repeat the four-source REST reconciliation without dispatching commands."""

        async with self._operation_lock:
            self._require_started()
            return await self._reconcile_startup(recovered_commands=0)

    async def consume_user_event(self, event: BinanceUserDataEvent) -> bool:
        """Persist one private stream event; return whether it affected this account."""

        async with self._operation_lock:
            self._require_started()
            if isinstance(event, BinanceExecutionReport):
                return self._consume_execution_report(event)
            if isinstance(event, BinanceOutboundAccountPosition):
                self._consume_account_position(event)
                return True
            if isinstance(event, BinanceBalanceUpdate):
                await self._refresh_balances_for_delta(event)
                return True
            if isinstance(event, BinanceEventStreamTerminated):
                return False
        raise TypeError(f"unsupported Binance user-data event: {type(event).__name__}")

    async def run_user_stream(self, *, maximum_events: int | None = None) -> int:
        """Consume the configured private stream until stopped or a test limit is reached."""

        self._require_started()
        if self._user_stream is None:
            raise RuntimeError("no Binance user-data stream is configured")
        if maximum_events is not None and maximum_events <= 0:
            raise ValueError("maximum_events must be positive")
        received = 0
        iterator = self._user_stream.events()
        observed_epoch = getattr(self._user_stream, "connection_epoch", None)
        try:
            async for event in iterator:
                if self._stopping:
                    break
                current_epoch = getattr(self._user_stream, "connection_epoch", None)
                if (
                    observed_epoch is not None
                    and current_epoch is not None
                    and current_epoch != observed_epoch
                ):
                    reconciliation = await self.reconcile_startup()
                    if reconciliation.unresolved_order_ids:
                        raise RuntimeError(
                            "Binance private-stream reconnection left unresolved orders"
                        )
                    observed_epoch = current_epoch
                await self.consume_user_event(event)
                received += 1
                if maximum_events is not None and received >= maximum_events:
                    break
        finally:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()
        return received

    async def _dispatch_pending(self) -> int:
        dispatched = 0
        while True:
            claimed = self._oms.claim_commands(
                now=self._now(),
                limit=1,
                account_id=self._account_id,
                symbols=self._symbols,
            )
            if not claimed:
                return dispatched
            command = claimed[0]
            if command.command_type is TradingCommandType.SUBMIT_ORDER:
                await self._dispatch_submit(command)
            elif command.command_type is TradingCommandType.CANCEL_ORDER:
                await self._dispatch_cancel(command)
            else:
                self._oms.mark_command_unknown(
                    command.command_id,
                    occurred_at=self._now(),
                    error_code="unsupported_command_type",
                )
                raise RuntimeError(
                    "Binance Testnet execution service received an unknown command type"
                )
            dispatched += 1

    async def _dispatch_submit(self, command: TradingCommand) -> None:
        order = self._oms.require_order(command.client_order_id).order
        try:
            await self._gateway.submit_order(order)
            update = self._gateway.order_update(order.client_order_id)
            if update is None or update.status in {
                OrderStatus.CREATED,
                OrderStatus.VALIDATED,
                OrderStatus.SUBMITTING,
                OrderStatus.CANCEL_PENDING,
                OrderStatus.UNKNOWN,
            }:
                self._oms.mark_command_unknown(
                    command.command_id,
                    occurred_at=self._now(),
                    error_code="ambiguous_submission",
                )
                return
            if update.order != order:
                raise RuntimeError("Binance gateway returned an update for a different order")
            occurred_at = update.occurred_at or self._now()
            self._oms.record_order_update(
                order.client_order_id,
                event_id=(f"binance-rest-submit:{command.command_id}:{command.attempt_count}"),
                status=update.status,
                occurred_at=occurred_at,
                filled_quantity=update.executed_quantity,
                exchange_order_id=update.exchange_order_id,
                reason=update.reason,
            )
            # Persist the exchange result before acknowledging the durable command.
            self._oms.mark_command_sent(command.command_id, occurred_at=occurred_at)
        except asyncio.CancelledError:
            self._quarantine_in_flight(command, "submission_cancelled")
            raise
        except BaseException:
            self._quarantine_in_flight(command, "submission_exception")
            raise

    async def _dispatch_cancel(self, command: TradingCommand) -> None:
        try:
            order = self._oms.require_order(command.client_order_id).order
            snapshot = await self._gateway.cancel_order_by_client_id(
                order.symbol,
                command.client_order_id,
            )
            if snapshot.symbol != order.symbol or (
                snapshot.client_order_id is not None
                and snapshot.client_order_id != command.client_order_id
            ):
                raise RuntimeError("Binance gateway returned a cancel result for another order")
            if snapshot.status in {
                OrderStatus.CREATED,
                OrderStatus.VALIDATED,
                OrderStatus.SUBMITTING,
                OrderStatus.CANCEL_PENDING,
                OrderStatus.UNKNOWN,
            }:
                self._oms.mark_command_unknown(
                    command.command_id,
                    occurred_at=self._now(),
                    error_code="ambiguous_cancellation",
                )
                return
            occurred_at = _datetime_from_ms(snapshot.transact_time_ms, fallback=self._now())
            self._oms.record_order_update(
                command.client_order_id,
                event_id=(f"binance-rest-cancel:{command.command_id}:{command.attempt_count}"),
                status=snapshot.status,
                occurred_at=occurred_at,
                filled_quantity=snapshot.executed_quantity,
                average_fill_price=_average_price(snapshot),
                exchange_order_id=snapshot.order_id,
            )
            self._oms.mark_command_sent(command.command_id, occurred_at=occurred_at)
        except asyncio.CancelledError:
            self._quarantine_in_flight(command, "cancellation_cancelled")
            raise
        except BaseException:
            self._quarantine_in_flight(command, "cancellation_exception")
            raise

    async def _reconcile_startup(self, *, recovered_commands: int) -> BinanceStartupReconciliation:
        now = self._now()
        account = await self._gateway.account()
        balances = self._record_account_snapshot(account, now=now, source="startup")
        open_orders = await self._gateway.open_orders()
        histories: list[BinanceOrderSnapshot] = []
        trades: list[BinanceTrade] = []
        for symbol in self._symbols:
            histories.extend(await self._gateway.all_orders(symbol, limit=1_000))
            trades.extend(await self._gateway.account_trades(symbol, limit=1_000))

        by_client_id = self._merge_exchange_orders((*histories, *open_orders))
        candidates = {
            snapshot.order.client_order_id: snapshot
            for snapshot in self._oms.open_orders(account_id=self._account_id)
            if snapshot.status not in {OrderStatus.CREATED, OrderStatus.VALIDATED}
        }
        candidates.update(
            {
                snapshot.order.client_order_id: snapshot
                for snapshot in self._oms.orders_requiring_reconciliation(
                    account_id=self._account_id, symbols=self._symbols
                )
            }
        )

        for client_order_id, candidate_snapshot in candidates.items():
            if client_order_id in by_client_id:
                continue
            try:
                exact = await self._gateway.get_order(
                    candidate_snapshot.order.symbol,
                    client_order_id=client_order_id,
                )
            except BinanceAPIError as error:
                if error.code == -2013:
                    continue
                raise
            if exact.status is not OrderStatus.UNKNOWN:
                by_client_id[client_order_id] = exact

        reconciled = 0
        order_id_to_client: dict[int, str] = {}
        for client_order_id, exchange in by_client_id.items():
            local_snapshot: OrderSnapshot | None = self._oms.order(client_order_id)
            if local_snapshot is None or local_snapshot.order.account_id != self._account_id:
                continue
            if exchange.status is OrderStatus.UNKNOWN:
                continue
            occurred_at = _datetime_from_ms(exchange.transact_time_ms, fallback=now)
            average = _average_price(exchange)
            self._oms.reconcile_order(
                client_order_id,
                status=exchange.status,
                occurred_at=occurred_at,
                filled_quantity=exchange.executed_quantity,
                average_fill_price=average,
                exchange_order_id=exchange.order_id,
                reason="Binance Testnet startup reconciliation",
                event_id=_reconciliation_event_id(exchange, occurred_at),
            )
            reconciled += 1
            if exchange.order_id is not None:
                order_id_to_client[exchange.order_id] = client_order_id

        known_fill_ids = {fill.fill_id for fill in self._oms.fills()}
        recorded_fills = 0
        for trade in trades:
            trade_client_order_id = order_id_to_client.get(trade.order_id)
            if trade_client_order_id is None:
                continue
            local_order = self._oms.require_order(trade_client_order_id).order
            fill = _fill_from_trade(trade, local_order)
            self._oms.record_fill(fill)
            if fill.fill_id not in known_fill_ids:
                known_fill_ids.add(fill.fill_id)
                recorded_fills += 1

        unresolved = tuple(
            order.order.client_order_id
            for order in self._oms.orders_requiring_reconciliation(
                account_id=self._account_id, symbols=self._symbols
            )
        )
        return BinanceStartupReconciliation(
            recovered_commands=recovered_commands,
            exchange_open_orders=len(open_orders),
            exchange_history_orders=len(histories),
            exchange_trades=len(trades),
            reconciled_orders=reconciled,
            recorded_fills=recorded_fills,
            recorded_balances=len(balances),
            unresolved_order_ids=unresolved,
        )

    def _consume_execution_report(self, event: BinanceExecutionReport) -> bool:
        client_order_id = self._local_client_order_id(event)
        if client_order_id is None:
            return False
        current = self._oms.require_order(client_order_id)
        order = current.order
        if event.symbol != order.symbol or event.side is not order.side:
            raise ValueError("Binance executionReport identity does not match the local order")
        occurred_at = _datetime_from_ms(event.transaction_time_ms, fallback=self._now())
        average = (
            event.cumulative_quote_quantity / event.cumulative_filled_quantity
            if event.cumulative_filled_quantity > 0 and event.cumulative_quote_quantity > 0
            else None
        )
        event_id = (
            f"binance-execution:{event.symbol}:{event.order_id}:"
            f"{event.execution_id}:{event.execution_type}"
        )
        reason = (
            event.reject_reason
            if event.reject_reason and event.reject_reason.upper() != "NONE"
            else None
        )
        if current.status is OrderStatus.UNKNOWN and event.status is not OrderStatus.UNKNOWN:
            self._oms.reconcile_order(
                client_order_id,
                status=event.status,
                occurred_at=occurred_at,
                filled_quantity=event.cumulative_filled_quantity,
                average_fill_price=average,
                exchange_order_id=event.order_id,
                reason=reason,
                event_id=event_id,
            )
        else:
            self._oms.record_order_update(
                client_order_id,
                event_id=event_id,
                status=event.status,
                occurred_at=occurred_at,
                filled_quantity=event.cumulative_filled_quantity,
                average_fill_price=average,
                exchange_order_id=event.order_id,
                reason=reason,
            )

        if event.execution_type.upper() == "TRADE":
            if event.last_executed_quantity <= 0 or event.last_executed_price <= 0:
                raise ValueError("Binance TRADE report must contain a positive fill")
            fill_id = (
                f"binance-spot:{event.symbol}:trade:{event.trade_id}"
                if event.trade_id >= 0
                else (
                    f"binance-spot:{event.symbol}:order:{event.order_id}:"
                    f"execution:{event.execution_id}"
                )
            )
            self._oms.record_fill(
                ExecutionFill(
                    fill_id=fill_id,
                    client_order_id=client_order_id,
                    account_id=self._account_id,
                    symbol=event.symbol,
                    side=event.side,
                    quantity=event.last_executed_quantity,
                    price=event.last_executed_price,
                    occurred_at=occurred_at,
                    fee_asset=event.commission_asset,
                    fee_amount=event.commission_amount,
                    exchange_order_id=str(event.order_id),
                )
            )
        return True

    def _consume_account_position(self, event: BinanceOutboundAccountPosition) -> None:
        balances = tuple(
            BalanceValue(item.asset, item.free, item.locked) for item in event.balances
        )
        occurred_at = _datetime_from_ms(event.last_account_update_ms, fallback=self._now())
        digest = _balance_digest(balances)
        self._oms.record_balance_snapshot(
            self._account_id,
            balances,
            event_id=(
                f"binance-balance-position:{event.event_time_ms}:"
                f"{event.last_account_update_ms}:{digest}"
            ),
            occurred_at=occurred_at,
            full_snapshot=False,
        )

    async def _refresh_balances_for_delta(self, event: BinanceBalanceUpdate) -> None:
        account = await self._gateway.account()
        fallback = _datetime_from_ms(event.clear_time_ms, fallback=self._now())
        self._record_account_snapshot(
            account,
            now=fallback,
            source=f"delta-{event.asset}-{event.clear_time_ms}",
        )

    def _record_account_snapshot(
        self, account: BinanceAccount, *, now: datetime, source: str
    ) -> tuple[AssetBalance, ...]:
        balances = tuple(
            BalanceValue(item.asset, item.free, item.locked) for item in account.balances
        )
        occurred_at = _datetime_from_ms(account.update_time_ms, fallback=now)
        digest = _balance_digest(balances)
        return self._oms.record_balance_snapshot(
            self._account_id,
            balances,
            event_id=f"binance-account-{source}:{_milliseconds(occurred_at)}:{digest}",
            occurred_at=occurred_at,
            full_snapshot=True,
        )

    def _merge_exchange_orders(
        self, orders: Sequence[BinanceOrderSnapshot]
    ) -> dict[str, BinanceOrderSnapshot]:
        merged: dict[str, BinanceOrderSnapshot] = {}
        for order in orders:
            client_order_id = order.client_order_id
            if client_order_id is None:
                continue
            current = merged.get(client_order_id)
            if current is None or _exchange_order_rank(order) >= _exchange_order_rank(current):
                merged[client_order_id] = order
        return merged

    def _local_client_order_id(self, event: BinanceExecutionReport) -> str | None:
        if self._is_owned_order(event.client_order_id):
            return event.client_order_id
        original = event.original_client_order_id
        if original is not None and self._is_owned_order(original):
            return original
        return None

    def _is_owned_order(self, client_order_id: str) -> bool:
        snapshot = self._oms.order(client_order_id)
        return snapshot is not None and (
            snapshot.order.account_id == self._account_id
            and snapshot.order.symbol in self._symbol_set
        )

    def _submit_command(self, client_order_id: str) -> TradingCommand:
        command = self._command(f"submit:{client_order_id}")
        if command is not None:
            return command
        raise RuntimeError("durable submit command disappeared")

    def _command(self, command_id: str) -> TradingCommand | None:
        return next(
            (command for command in self._oms.commands() if command.command_id == command_id),
            None,
        )

    def _quarantine_in_flight(self, command: TradingCommand, error_code: str) -> None:
        current = next(
            (value for value in self._oms.commands() if value.command_id == command.command_id),
            None,
        )
        if current is not None and current.status is TradingCommandStatus.IN_FLIGHT:
            self._oms.mark_command_unknown(
                command.command_id,
                occurred_at=self._now(),
                error_code=error_code,
            )

    def _validate_order(self, order: OrderIntent) -> None:
        if order.account_id != self._account_id:
            raise ValueError("order account_id does not match the execution service")
        if order.symbol != order.symbol.upper() or order.symbol not in self._symbol_set:
            raise ValueError("order symbol is not in the Testnet execution allow-list")

    def _require_started(self) -> None:
        if not self._started:
            raise RuntimeError("Binance Testnet execution service is not started")

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value.astimezone(UTC)


def _require_testnet(component: object, label: str) -> None:
    value = getattr(component, "environment", None)
    try:
        environment = (
            value
            if isinstance(value, BinanceEnvironment)
            else BinanceEnvironment(str(value).upper())
        )
    except ValueError:
        raise BinanceTestnetOnlyError(
            f"{label} must explicitly advertise Binance TESTNET"
        ) from None
    if environment is not BinanceEnvironment.TESTNET:
        raise BinanceTestnetOnlyError(
            f"{label} targets {environment.value}; this service is TESTNET-only"
        )


def _datetime_from_ms(value: int | None, *, fallback: datetime) -> datetime:
    if value is None:
        return fallback.astimezone(UTC)
    return datetime.fromtimestamp(value / 1_000, tz=UTC)


def _milliseconds(value: datetime) -> int:
    return int(value.timestamp() * 1_000)


def _average_price(snapshot: BinanceOrderSnapshot) -> Decimal | None:
    quote = snapshot.cumulative_quote_quantity
    if snapshot.executed_quantity <= 0 or quote is None or quote <= 0:
        return None
    return quote / snapshot.executed_quantity


def _reconciliation_event_id(snapshot: BinanceOrderSnapshot, occurred_at: datetime) -> str:
    order_id = "none" if snapshot.order_id is None else str(snapshot.order_id)
    return (
        f"binance-reconcile:{snapshot.symbol}:{order_id}:{snapshot.status.value}:"
        f"{snapshot.executed_quantity}:{_milliseconds(occurred_at)}"
    )


def _fill_from_trade(trade: BinanceTrade, order: OrderIntent) -> ExecutionFill:
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
        occurred_at=_datetime_from_ms(trade.time_ms, fallback=order.created_at),
        fee_asset=trade.commission_asset,
        fee_amount=trade.commission,
        exchange_order_id=str(trade.order_id),
    )


def _balance_digest(balances: Sequence[BalanceValue]) -> str:
    canonical = "|".join(
        f"{item.asset}:{item.free}:{item.locked}"
        for item in sorted(balances, key=lambda value: value.asset)
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _exchange_order_rank(snapshot: BinanceOrderSnapshot) -> tuple[int, int, Decimal]:
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
