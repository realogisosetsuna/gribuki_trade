from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase

from gribuki_trade.adapters.binance import (
    BinanceAccount,
    BinanceBalance,
    BinanceBalanceUpdate,
    BinanceEnvironment,
    BinanceExecutionReport,
    BinanceOrderSnapshot,
    BinanceOrderUpdate,
    BinanceOutboundAccountPosition,
    BinanceTrade,
    BinanceUserBalance,
)
from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.services.binance.binance_execution import (
    BinanceSpotTestnetExecutionService,
    BinanceTestnetOnlyError,
)
from gribuki_trade.trading import (
    SQLiteOrderManagementStore,
    TradingCommandStatus,
    TradingCommandType,
)

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)
ACCOUNT_ID = "binance-spot-testnet"


def make_order(client_order_id: str = "testnet-btc-1") -> OrderIntent:
    return OrderIntent(
        client_order_id=client_order_id,
        account_id=ACCOUNT_ID,
        strategy_id="execution-test",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.010"),
        limit_price=Decimal("100"),
        created_at=NOW,
    )


def order_snapshot(
    *,
    client_order_id: str = "testnet-btc-1",
    status: OrderStatus = OrderStatus.ACCEPTED,
    executed: str = "0",
    quote: str = "0",
    order_id: int = 42,
) -> BinanceOrderSnapshot:
    return BinanceOrderSnapshot(
        symbol="BTCUSDT",
        client_order_id=client_order_id,
        order_id=order_id,
        status=status,
        exchange_status={
            OrderStatus.ACCEPTED: "NEW",
            OrderStatus.PARTIALLY_FILLED: "PARTIALLY_FILLED",
            OrderStatus.FILLED: "FILLED",
        }.get(status, status.value),
        side=Side.BUY,
        price=Decimal("100"),
        original_quantity=Decimal("0.010"),
        executed_quantity=Decimal(executed),
        transact_time_ms=int((NOW + timedelta(seconds=1)).timestamp() * 1_000),
        cumulative_quote_quantity=Decimal(quote),
    )


def execution_report(
    *,
    execution_id: int,
    trade_id: int,
    status: OrderStatus,
    last_quantity: str,
    cumulative_quantity: str,
    last_price: str,
    cumulative_quote: str,
    commission: str,
    commission_asset: str | None,
) -> BinanceExecutionReport:
    at_ms = int((NOW + timedelta(seconds=execution_id)).timestamp() * 1_000)
    return BinanceExecutionReport(
        subscription_id=7,
        event_time_ms=at_ms,
        transaction_time_ms=at_ms,
        symbol="BTCUSDT",
        client_order_id="testnet-btc-1",
        original_client_order_id=None,
        side=Side.BUY,
        order_type="LIMIT",
        time_in_force="GTC",
        original_quantity=Decimal("0.010"),
        order_price=Decimal("100"),
        stop_price=Decimal("0"),
        iceberg_quantity=Decimal("0"),
        order_list_id=-1,
        execution_type="TRADE",
        status=status,
        exchange_order_status=(
            "PARTIALLY_FILLED" if status is OrderStatus.PARTIALLY_FILLED else "FILLED"
        ),
        reject_reason="NONE",
        order_id=42,
        last_executed_quantity=Decimal(last_quantity),
        cumulative_filled_quantity=Decimal(cumulative_quantity),
        last_executed_price=Decimal(last_price),
        commission_amount=Decimal(commission),
        commission_asset=commission_asset,
        trade_id=trade_id,
        prevented_match_id=None,
        execution_id=execution_id,
        is_order_on_book=False,
        is_maker_side=True,
        order_creation_time_ms=int(NOW.timestamp() * 1_000),
        cumulative_quote_quantity=Decimal(cumulative_quote),
        last_quote_quantity=Decimal(last_quantity) * Decimal(last_price),
        quote_order_quantity=Decimal("0"),
        working_time_ms=at_ms,
        self_trade_prevention_mode="EXPIRE_MAKER",
    )


class FakeGateway:
    def __init__(
        self,
        *,
        environment: BinanceEnvironment = BinanceEnvironment.TESTNET,
        account: BinanceAccount | None = None,
    ) -> None:
        self.environment = environment
        self.account_value = account or BinanceAccount(
            can_trade=True,
            can_withdraw=False,
            can_deposit=True,
            account_type="SPOT",
            balances=(
                BinanceBalance("BTC", Decimal("1"), Decimal("0")),
                BinanceBalance("USDT", Decimal("1000"), Decimal("0")),
            ),
            update_time_ms=int(NOW.timestamp() * 1_000),
            permissions=("SPOT",),
        )
        self.open_order_values: tuple[BinanceOrderSnapshot, ...] = ()
        self.history_values: dict[str, tuple[BinanceOrderSnapshot, ...]] = {}
        self.trade_values: dict[str, tuple[BinanceTrade, ...]] = {}
        self.updates: dict[str, BinanceOrderUpdate] = {}
        self.connected = False
        self.calls: list[str] = []
        self.store: SQLiteOrderManagementStore | None = None
        self.cancel_status = OrderStatus.CANCELED

    async def connect(self) -> None:
        self.calls.append("connect")
        self.connected = True

    async def disconnect(self) -> None:
        self.calls.append("disconnect")
        self.connected = False

    async def submit_order(self, order: OrderIntent) -> None:
        self.calls.append(f"submit:{order.client_order_id}")
        if self.store is not None:
            self.store.require_order(order.client_order_id)
            commands = self.store.commands(status=TradingCommandStatus.IN_FLIGHT)
            if not any(value.client_order_id == order.client_order_id for value in commands):
                raise AssertionError("order was not stored and claimed before submission")
        self.updates[order.client_order_id] = BinanceOrderUpdate(
            order=order,
            status=OrderStatus.ACCEPTED,
            exchange_order_id=42,
            occurred_at=NOW + timedelta(seconds=1),
        )

    def order_update(self, client_order_id: str) -> BinanceOrderUpdate | None:
        return self.updates.get(client_order_id)

    async def cancel_order(self, client_order_id: str) -> None:
        if self.store is None:
            raise AssertionError("test store is not configured")
        order = self.store.require_order(client_order_id).order
        await self.cancel_order_by_client_id(order.symbol, client_order_id)

    async def cancel_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> BinanceOrderSnapshot:
        self.calls.append(f"cancel:{client_order_id}")
        if self.store is None:
            raise AssertionError("test store is not configured")
        local = self.store.require_order(client_order_id)
        if local.order.symbol != symbol:
            raise AssertionError("cancel symbol does not match durable order")
        if local.status is not OrderStatus.CANCEL_PENDING:
            raise AssertionError("cancel was not persisted before gateway delivery")
        commands = self.store.commands(status=TradingCommandStatus.IN_FLIGHT)
        if not any(value.command_id == f"cancel:{client_order_id}" for value in commands):
            raise AssertionError("cancel command was not claimed before delivery")
        previous = self.updates.get(client_order_id)
        exchange_order_id = (
            previous.exchange_order_id
            if previous is not None
            else int(local.exchange_order_id or 42)
        )
        executed_quantity = (
            previous.executed_quantity if previous is not None else local.filled_quantity
        )
        self.updates[client_order_id] = BinanceOrderUpdate(
            order=local.order,
            status=self.cancel_status,
            exchange_order_id=exchange_order_id,
            executed_quantity=executed_quantity,
            reason="ambiguous" if self.cancel_status is OrderStatus.UNKNOWN else None,
            occurred_at=NOW + timedelta(seconds=2),
        )
        return BinanceOrderSnapshot(
            symbol=symbol,
            client_order_id=client_order_id,
            order_id=exchange_order_id,
            status=self.cancel_status,
            exchange_status=(
                None if self.cancel_status is OrderStatus.UNKNOWN else self.cancel_status.value
            ),
            side=local.order.side,
            price=local.order.limit_price,
            original_quantity=local.order.quantity,
            executed_quantity=executed_quantity,
            transact_time_ms=int((NOW + timedelta(seconds=2)).timestamp() * 1_000),
        )

    async def account(self) -> BinanceAccount:
        self.calls.append("account")
        return self.account_value

    async def open_orders(self, symbol: str | None = None) -> tuple[BinanceOrderSnapshot, ...]:
        self.calls.append("open_orders")
        if symbol is None:
            return self.open_order_values
        return tuple(item for item in self.open_order_values if item.symbol == symbol)

    async def all_orders(
        self,
        symbol: str,
        *,
        order_id: int | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> tuple[BinanceOrderSnapshot, ...]:
        del order_id, start_time_ms, end_time_ms, limit
        self.calls.append(f"all_orders:{symbol}")
        return self.history_values.get(symbol, ())

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
        del order_id, from_id, start_time_ms, end_time_ms, limit
        self.calls.append(f"account_trades:{symbol}")
        return self.trade_values.get(symbol, ())

    async def get_order(
        self,
        symbol: str,
        *,
        client_order_id: str | None = None,
        order_id: int | None = None,
    ) -> BinanceOrderSnapshot:
        del order_id
        self.calls.append(f"get_order:{symbol}:{client_order_id}")
        for snapshot in self.history_values.get(symbol, ()):
            if snapshot.client_order_id == client_order_id:
                return snapshot
        return BinanceOrderSnapshot(
            symbol=symbol,
            client_order_id=client_order_id,
            order_id=None,
            status=OrderStatus.UNKNOWN,
            exchange_status=None,
            side=None,
            price=None,
            original_quantity=None,
            executed_quantity=Decimal("0"),
        )


class EmptyUserStream:
    def __init__(self, environment: BinanceEnvironment = BinanceEnvironment.TESTNET) -> None:
        self.environment = environment
        self.closed = False

    async def events(self) -> AsyncIterator[object]:
        if False:
            yield object()

    async def aclose(self) -> None:
        self.closed = True


class ReconnectingUserStream(EmptyUserStream):
    def __init__(self, events: tuple[BinanceOutboundAccountPosition, ...]) -> None:
        super().__init__()
        self.connection_epoch = 0
        self._events = events

    async def events(self) -> AsyncIterator[object]:
        for event in self._events:
            self.connection_epoch += 1
            yield event


class BinanceExecutionTests(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._temporary = TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.path = Path(self._temporary.name) / "execution.sqlite3"
        self.store = SQLiteOrderManagementStore(self.path)
        self.addCleanup(self.store.close)
        self.gateway = FakeGateway()
        self.gateway.store = self.store
        self.service = BinanceSpotTestnetExecutionService(
            self.gateway,
            self.store,
            account_id=ACCOUNT_ID,
            symbols=("BTCUSDT", "ETHUSDT"),
            clock=lambda: NOW,
        )

    async def test_submit_is_persisted_and_claimed_before_gateway_call(self) -> None:
        report = await self.service.start()

        submitted = await self.service.submit(make_order())

        self.assertIs(submitted.status, OrderStatus.ACCEPTED)
        self.assertEqual(submitted.exchange_order_id, "42")
        self.assertIs(self.store.commands()[0].status, TradingCommandStatus.SENT)
        self.assertEqual(report.recorded_balances, 2)
        self.assertIn("account", self.gateway.calls)
        self.assertIn("open_orders", self.gateway.calls)
        self.assertIn("all_orders:BTCUSDT", self.gateway.calls)
        self.assertIn("account_trades:ETHUSDT", self.gateway.calls)

    async def test_ambiguous_gateway_result_is_quarantined_without_resubmit(self) -> None:
        await self.service.start()
        order = make_order()
        self.gateway.updates[order.client_order_id] = BinanceOrderUpdate(
            order=order,
            status=OrderStatus.UNKNOWN,
            reason="ambiguous",
        )

        async def ambiguous_submit(intent: OrderIntent) -> None:
            self.gateway.calls.append(f"submit:{intent.client_order_id}")

        self.gateway.submit_order = ambiguous_submit  # type: ignore[assignment]
        first = await self.service.submit(order)
        second = await self.service.submit(order)

        self.assertIs(first.status, OrderStatus.UNKNOWN)
        self.assertEqual(second, first)
        self.assertEqual(self.gateway.calls.count("submit:testnet-btc-1"), 1)
        self.assertIs(self.store.commands()[0].status, TradingCommandStatus.UNKNOWN)

    async def test_broker_rejection_preserves_exchange_code_and_reason(self) -> None:
        await self.service.start()
        order = make_order("rejected-order")

        async def rejected_submit(intent: OrderIntent) -> None:
            self.gateway.calls.append(f"submit:{intent.client_order_id}")
            self.gateway.updates[intent.client_order_id] = BinanceOrderUpdate(
                order=intent,
                status=OrderStatus.BROKER_REJECTED,
                reason="Binance API error (HTTP 400, code -2010): insufficient balance",
                error_code=-2010,
                occurred_at=NOW + timedelta(seconds=1),
            )

        self.gateway.submit_order = rejected_submit  # type: ignore[assignment]
        snapshot = await self.service.submit(order)

        self.assertIs(snapshot.status, OrderStatus.BROKER_REJECTED)
        self.assertEqual(snapshot.broker_error_code, -2010)
        self.assertIn("insufficient balance", snapshot.reason or "")

    async def test_submit_claims_only_its_exact_command(self) -> None:
        await self.service.start()
        older = make_order("older-pending")
        target = make_order("target-submit")
        self.store.create_order(older)

        await self.service.submit(target)

        by_id = {command.client_order_id: command for command in self.store.commands()}
        self.assertIs(by_id["older-pending"].status, TradingCommandStatus.PENDING)
        self.assertIs(by_id["target-submit"].status, TradingCommandStatus.SENT)
        self.assertNotIn("submit:older-pending", self.gateway.calls)

    async def test_start_does_not_dispatch_pending_work_while_unknown_remains(self) -> None:
        unknown = make_order("unknown-order")
        pending = make_order("pending-order")
        self.store.create_order(unknown)
        self.store.claim_command("submit:unknown-order", now=NOW)
        self.store.mark_command_unknown(
            "submit:unknown-order",
            occurred_at=NOW,
            error_code="ambiguous_delivery",
        )
        self.store.create_order(pending)

        report = await self.service.start()

        self.assertEqual(report.unresolved_order_ids, ("unknown-order",))
        self.assertEqual(report.dispatched_pending_commands, 0)
        self.assertNotIn("submit:pending-order", self.gateway.calls)
        command = next(
            item for item in self.store.commands() if item.client_order_id == "pending-order"
        )
        self.assertIs(command.status, TradingCommandStatus.PENDING)

    async def test_cancel_is_durable_and_ambiguous_cancel_is_never_resent(self) -> None:
        await self.service.start()
        await self.service.submit(make_order())
        self.gateway.cancel_status = OrderStatus.UNKNOWN

        first = await self.service.cancel("testnet-btc-1")
        second = await self.service.cancel("testnet-btc-1")

        self.assertIs(first.status, OrderStatus.UNKNOWN)
        self.assertEqual(second, first)
        self.assertEqual(self.gateway.calls.count("cancel:testnet-btc-1"), 1)
        commands = {command.command_type: command for command in self.store.commands()}
        self.assertIs(
            commands[TradingCommandType.CANCEL_ORDER].status,
            TradingCommandStatus.UNKNOWN,
        )

    async def test_successful_cancel_is_persisted_before_delivery_and_acknowledged(self) -> None:
        await self.service.start()
        await self.service.submit(make_order())

        canceled = await self.service.cancel("testnet-btc-1")

        self.assertIs(canceled.status, OrderStatus.CANCELED)
        commands = {command.command_type: command for command in self.store.commands()}
        self.assertIs(
            commands[TradingCommandType.CANCEL_ORDER].status,
            TradingCommandStatus.SENT,
        )
        self.assertEqual(self.gateway.calls.count("cancel:testnet-btc-1"), 1)

    async def test_partial_fills_fees_and_duplicate_reports_are_idempotent(self) -> None:
        await self.service.start()
        await self.service.submit(make_order())
        first = execution_report(
            execution_id=1,
            trade_id=1001,
            status=OrderStatus.PARTIALLY_FILLED,
            last_quantity="0.004",
            cumulative_quantity="0.004",
            last_price="100",
            cumulative_quote="0.4",
            commission="0.00001",
            commission_asset="BNB",
        )
        second = execution_report(
            execution_id=2,
            trade_id=1002,
            status=OrderStatus.FILLED,
            last_quantity="0.006",
            cumulative_quantity="0.010",
            last_price="101",
            cumulative_quote="1.006",
            commission="0.000606",
            commission_asset="USDT",
        )

        self.assertTrue(await self.service.consume_user_event(first))
        self.assertTrue(await self.service.consume_user_event(first))
        partial = self.store.require_order("testnet-btc-1")
        self.assertIs(partial.status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(partial.filled_quantity, Decimal("0.004"))
        self.assertEqual(len(self.store.fills()), 1)
        self.assertEqual(self.store.fills()[0].fee_asset, "BNB")
        self.assertEqual(self.store.fills()[0].fee_amount, Decimal("0.00001"))

        await self.service.consume_user_event(second)

        final = self.store.require_order("testnet-btc-1")
        self.assertIs(final.status, OrderStatus.FILLED)
        self.assertEqual(final.filled_quantity, Decimal("0.010"))
        self.assertEqual(final.average_fill_price, Decimal("100.6"))
        self.assertEqual(len(self.store.fills()), 2)
        self.assertEqual(self.store.positions(account_id=ACCOUNT_ID)[0].quantity, Decimal("0.010"))

    async def test_restart_unknown_is_resolved_by_history_and_trades(self) -> None:
        order = make_order()
        self.store.create_order(order)
        self.store.claim_commands(now=NOW)
        self.store.close()
        self.store = SQLiteOrderManagementStore(self.path)
        self.addCleanup(self.store.close)
        self.gateway.store = self.store
        self.gateway.history_values["BTCUSDT"] = (
            order_snapshot(
                status=OrderStatus.PARTIALLY_FILLED,
                executed="0.004",
                quote="0.4",
            ),
        )
        self.gateway.trade_values["BTCUSDT"] = (
            BinanceTrade(
                symbol="BTCUSDT",
                trade_id=1001,
                order_id=42,
                price=Decimal("100"),
                quantity=Decimal("0.004"),
                quote_quantity=Decimal("0.4"),
                commission=Decimal("0.00001"),
                commission_asset="BNB",
                time_ms=int((NOW + timedelta(seconds=1)).timestamp() * 1_000),
                is_buyer=True,
                is_maker=True,
                is_best_match=True,
            ),
        )
        restarted = BinanceSpotTestnetExecutionService(
            self.gateway,
            self.store,
            account_id=ACCOUNT_ID,
            symbols=("BTCUSDT",),
            clock=lambda: NOW + timedelta(seconds=2),
        )

        report = await restarted.start()

        self.assertEqual(report.recovered_commands, 1)
        self.assertEqual(report.reconciled_orders, 1)
        self.assertEqual(report.recorded_fills, 1)
        self.assertEqual(report.unresolved_order_ids, ())
        self.assertIs(self.store.commands()[0].status, TradingCommandStatus.RESOLVED)
        self.assertIs(
            self.store.require_order(order.client_order_id).status,
            OrderStatus.PARTIALLY_FILLED,
        )
        self.assertEqual(self.store.fills()[0].fee_asset, "BNB")

    async def test_rest_reconciled_order_can_be_canceled_after_gateway_restart(self) -> None:
        order = make_order("restored-cancel-1")
        self.store.create_order(order)
        self.store.claim_commands(now=NOW)
        self.store.close()
        self.store = SQLiteOrderManagementStore(self.path)
        self.addCleanup(self.store.close)
        self.gateway.store = self.store
        self.gateway.history_values["BTCUSDT"] = (
            order_snapshot(client_order_id=order.client_order_id),
        )
        restarted = BinanceSpotTestnetExecutionService(
            self.gateway,
            self.store,
            account_id=ACCOUNT_ID,
            symbols=("BTCUSDT",),
            clock=lambda: NOW + timedelta(seconds=2),
        )

        report = await restarted.start()
        self.assertEqual(report.reconciled_orders, 1)
        self.assertNotIn(order.client_order_id, self.gateway.updates)
        self.assertIs(
            self.store.require_order(order.client_order_id).status,
            OrderStatus.ACCEPTED,
        )

        canceled = await restarted.cancel(order.client_order_id)

        self.assertIs(canceled.status, OrderStatus.CANCELED)
        self.assertEqual(self.gateway.calls.count(f"cancel:{order.client_order_id}"), 1)
        commands = {command.command_type: command for command in self.store.commands()}
        self.assertIs(
            commands[TradingCommandType.CANCEL_ORDER].status,
            TradingCommandStatus.SENT,
        )

    async def test_balance_events_update_partial_and_full_snapshots(self) -> None:
        await self.service.start()
        partial = BinanceOutboundAccountPosition(
            subscription_id=1,
            event_time_ms=int((NOW + timedelta(seconds=1)).timestamp() * 1_000),
            last_account_update_ms=int((NOW + timedelta(seconds=1)).timestamp() * 1_000),
            balances=(BinanceUserBalance("USDT", Decimal("900"), Decimal("100")),),
        )

        await self.service.consume_user_event(partial)
        balances = {item.asset: item for item in self.store.balances(ACCOUNT_ID)}
        self.assertEqual(balances["BTC"].free, Decimal("1"))
        self.assertEqual(balances["USDT"].locked, Decimal("100"))

        self.gateway.account_value = BinanceAccount(
            can_trade=True,
            can_withdraw=False,
            can_deposit=True,
            account_type="SPOT",
            balances=(BinanceBalance("USDT", Decimal("950"), Decimal("0")),),
            update_time_ms=int((NOW + timedelta(seconds=2)).timestamp() * 1_000),
            permissions=("SPOT",),
        )
        await self.service.consume_user_event(
            BinanceBalanceUpdate(
                subscription_id=1,
                event_time_ms=int((NOW + timedelta(seconds=2)).timestamp() * 1_000),
                asset="USDT",
                balance_delta=Decimal("50"),
                clear_time_ms=int((NOW + timedelta(seconds=2)).timestamp() * 1_000),
            )
        )

        refreshed = self.store.balances(ACCOUNT_ID)
        self.assertEqual([item.asset for item in refreshed], ["USDT"])
        self.assertEqual(refreshed[0].free, Decimal("950"))

    async def test_private_stream_subscription_epochs_trigger_rest_reconciliation(self) -> None:
        events = tuple(
            BinanceOutboundAccountPosition(
                subscription_id=index,
                event_time_ms=int((NOW + timedelta(seconds=index)).timestamp() * 1_000),
                last_account_update_ms=int(
                    (NOW + timedelta(seconds=index)).timestamp() * 1_000
                ),
                balances=(BinanceUserBalance("USDT", Decimal("1000"), Decimal("0")),),
            )
            for index in (1, 2)
        )
        stream = ReconnectingUserStream(events)
        service = BinanceSpotTestnetExecutionService(
            self.gateway,
            self.store,
            account_id=ACCOUNT_ID,
            symbols=("BTCUSDT",),
            user_stream=stream,  # type: ignore[arg-type]
            clock=lambda: NOW,
        )
        await service.start()
        initial_account_calls = self.gateway.calls.count("account")

        received = await service.run_user_stream(maximum_events=2)

        self.assertEqual(received, 2)
        self.assertEqual(
            self.gateway.calls.count("account"),
            initial_account_calls + 2,
        )

    async def test_live_dependencies_are_rejected_without_escape_hatch(self) -> None:
        with self.assertRaisesRegex(BinanceTestnetOnlyError, "TESTNET-only"):
            BinanceSpotTestnetExecutionService(
                FakeGateway(environment=BinanceEnvironment.LIVE),
                self.store,
                account_id=ACCOUNT_ID,
                symbols=("BTCUSDT",),
            )
        with self.assertRaisesRegex(BinanceTestnetOnlyError, "TESTNET-only"):
            BinanceSpotTestnetExecutionService(
                self.gateway,
                self.store,
                account_id=ACCOUNT_ID,
                symbols=("BTCUSDT",),
                user_stream=EmptyUserStream(BinanceEnvironment.LIVE),  # type: ignore[arg-type]
            )
