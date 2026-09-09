import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase, TestCase

from gribuki_trade.adapters.simulated.paper import PaperBroker
from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.trading import (
    BalanceValue,
    ExecutionFill,
    OrderSnapshot,
    SQLiteOrderManagementStore,
    TradingCommandStatus,
)

NOW = datetime(2026, 8, 13, 3, 0, tzinfo=UTC)


def make_order(
    client_order_id: str = "btc-buy-1",
    *,
    account_id: str = "binance-spot-testnet",
    symbol: str = "BTCUSDT",
    side: Side = Side.BUY,
    quantity: str = "2",
) -> OrderIntent:
    return OrderIntent(
        client_order_id=client_order_id,
        account_id=account_id,
        strategy_id="paper-smoke",
        symbol=symbol,
        side=side,
        quantity=Decimal(quantity),
        limit_price=Decimal("100"),
        created_at=NOW,
    )


class TradingOMSTests(TestCase):
    def setUp(self) -> None:
        self._temporary = TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.path = Path(self._temporary.name) / "trading.sqlite3"
        self.store = SQLiteOrderManagementStore(self.path)
        self.addCleanup(self.store.close)

    def test_create_order_and_submit_command_are_atomic_and_idempotent(self) -> None:
        order = make_order()

        snapshot = self.store.create_order(order)
        duplicate = self.store.create_order(order)

        self.assertEqual(snapshot, duplicate)
        self.assertIs(snapshot.status, OrderStatus.CREATED)
        commands = self.store.commands()
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command_id, "submit:btc-buy-1")
        self.assertIs(commands[0].status, TradingCommandStatus.PENDING)
        events = self.store.order_events(order.client_order_id)
        self.assertEqual(len(events), 1)
        self.assertTrue(events[0].applied)
        connection = sqlite3.connect(self.path)
        try:
            journal_mode = connection.execute("PRAGMA journal_mode").fetchone()
        finally:
            connection.close()
        self.assertEqual(str(journal_mode[0]).lower(), "wal")

        with self.assertRaisesRegex(ValueError, "different order"):
            self.store.create_order(make_order(quantity="3"))

    def test_restart_quarantines_in_flight_command_until_reconciliation(self) -> None:
        self.store.create_order(make_order())
        claimed = self.store.claim_commands(now=NOW, lease_for=timedelta(minutes=5))
        self.assertEqual(len(claimed), 1)
        self.assertIs(claimed[0].status, TradingCommandStatus.IN_FLIGHT)
        self.store.close()

        self.store = SQLiteOrderManagementStore(self.path)
        self.addCleanup(self.store.close)
        recovered = self.store.recover_after_restart(now=NOW + timedelta(seconds=1))

        self.assertEqual(len(recovered), 1)
        self.assertIs(recovered[0].status, TradingCommandStatus.UNKNOWN)
        self.assertEqual(self.store.claim_commands(now=NOW + timedelta(minutes=10)), ())
        self.assertIs(
            self.store.require_order("btc-buy-1").status,
            OrderStatus.UNKNOWN,
        )
        self.assertEqual(len(self.store.orders_requiring_reconciliation()), 1)

        reconciled = self.store.reconcile_order(
            "btc-buy-1",
            status=OrderStatus.ACCEPTED,
            occurred_at=NOW + timedelta(seconds=2),
            exchange_order_id=42,
            event_id="rest-snapshot-1",
        )
        self.assertIs(reconciled.status, OrderStatus.ACCEPTED)
        self.assertEqual(reconciled.exchange_order_id, "42")
        self.assertEqual(self.store.orders_requiring_reconciliation(), ())
        self.assertIs(self.store.commands()[0].status, TradingCommandStatus.RESOLVED)

    def test_expired_lease_is_not_blindly_retried(self) -> None:
        self.store.create_order(make_order())
        self.store.claim_commands(now=NOW, lease_for=timedelta(seconds=1))

        claimed = self.store.claim_commands(now=NOW + timedelta(seconds=2))

        self.assertEqual(claimed, ())
        self.assertIs(self.store.commands()[0].status, TradingCommandStatus.UNKNOWN)
        self.assertIs(self.store.require_order("btc-buy-1").status, OrderStatus.UNKNOWN)

    def test_exact_command_claim_does_not_lease_unrelated_pending_work(self) -> None:
        self.store.create_order(make_order("btc-buy-1"))
        self.store.create_order(make_order("btc-buy-2"))

        claimed = self.store.claim_command("submit:btc-buy-2", now=NOW)

        self.assertEqual(claimed.client_order_id, "btc-buy-2")
        states = {value.client_order_id: value.status for value in self.store.commands()}
        self.assertIs(states["btc-buy-1"], TradingCommandStatus.PENDING)
        self.assertIs(states["btc-buy-2"], TradingCommandStatus.IN_FLIGHT)

    def test_command_claim_and_restart_recovery_are_account_scoped(self) -> None:
        self.store.create_order(make_order("account-a-order", account_id="account-a"))
        self.store.create_order(make_order("account-b-order", account_id="account-b"))
        self.store.create_order(
            make_order(
                "account-b-eth-order",
                account_id="account-b",
                symbol="ETHUSDT",
            )
        )

        claimed = self.store.claim_commands(
            now=NOW, account_id="account-b", symbols=("BTCUSDT",)
        )

        self.assertEqual(tuple(item.client_order_id for item in claimed), ("account-b-order",))
        states = {value.client_order_id: value.status for value in self.store.commands()}
        self.assertIs(states["account-a-order"], TradingCommandStatus.PENDING)
        self.assertIs(states["account-b-order"], TradingCommandStatus.IN_FLIGHT)
        self.assertIs(states["account-b-eth-order"], TradingCommandStatus.PENDING)

        recovered = self.store.recover_after_restart(
            now=NOW + timedelta(seconds=1),
            account_id="account-a",
            symbols=("BTCUSDT",),
        )
        self.assertEqual(recovered, ())
        self.assertIs(
            self.store.commands()[1].status,
            TradingCommandStatus.IN_FLIGHT,
        )
        self.assertIs(
            self.store.commands()[2].status,
            TradingCommandStatus.PENDING,
        )

        recovered = self.store.recover_after_restart(
            now=NOW + timedelta(seconds=2),
            account_id="account-b",
            symbols=("BTCUSDT",),
        )
        self.assertEqual(tuple(item.client_order_id for item in recovered), ("account-b-order",))
        self.assertEqual(
            tuple(
                item.order.client_order_id
                for item in self.store.orders_requiring_reconciliation(
                    account_id="account-b", symbols=("BTCUSDT",)
                )
            ),
            ("account-b-order",),
        )
        self.assertEqual(
            self.store.orders_requiring_reconciliation(
                account_id="account-a", symbols=("BTCUSDT",)
            ),
            (),
        )

    def test_duplicate_and_stale_status_events_do_not_regress_projection(self) -> None:
        self.store.create_order(make_order())
        accepted = self.store.record_order_update(
            "btc-buy-1",
            event_id="status-accepted",
            status=OrderStatus.ACCEPTED,
            occurred_at=NOW + timedelta(seconds=1),
        )
        duplicate = self.store.record_order_update(
            "btc-buy-1",
            event_id="status-accepted",
            status=OrderStatus.ACCEPTED,
            occurred_at=NOW + timedelta(seconds=1),
        )
        self.assertEqual(duplicate, accepted)

        partial = self.store.record_order_update(
            "btc-buy-1",
            event_id="status-partial",
            status=OrderStatus.PARTIALLY_FILLED,
            occurred_at=NOW + timedelta(seconds=2),
            filled_quantity="1",
            average_fill_price="100",
        )
        stale = self.store.record_order_update(
            "btc-buy-1",
            event_id="status-stale",
            status=OrderStatus.ACCEPTED,
            occurred_at=NOW + timedelta(seconds=3),
            filled_quantity="1",
        )
        self.assertEqual(stale, partial)
        self.assertFalse(self.store.order_events("btc-buy-1")[-1].applied)

        with self.assertRaisesRegex(ValueError, "different order event"):
            self.store.record_order_update(
                "btc-buy-1",
                event_id="status-accepted",
                status=OrderStatus.CANCELED,
                occurred_at=NOW + timedelta(seconds=4),
            )

    def test_terminal_status_and_fill_quantity_invariants_are_monotonic(self) -> None:
        self.store.create_order(make_order())
        with self.assertRaisesRegex(ValueError, "FILLED status requires"):
            self.store.record_order_update(
                "btc-buy-1",
                event_id="invalid-filled",
                status=OrderStatus.FILLED,
                occurred_at=NOW + timedelta(seconds=1),
                filled_quantity="1",
            )
        with self.assertRaisesRegex(ValueError, "PARTIALLY_FILLED status requires"):
            self.store.record_order_update(
                "btc-buy-1",
                event_id="invalid-partial",
                status=OrderStatus.PARTIALLY_FILLED,
                occurred_at=NOW + timedelta(seconds=2),
                filled_quantity="2",
            )

        filled = self.store.record_order_update(
            "btc-buy-1",
            event_id="valid-filled",
            status=OrderStatus.FILLED,
            occurred_at=NOW + timedelta(seconds=3),
            filled_quantity="2",
            average_fill_price="100",
        )
        late_cancel = self.store.record_order_update(
            "btc-buy-1",
            event_id="late-cancel",
            status=OrderStatus.CANCELED,
            occurred_at=NOW + timedelta(seconds=4),
            filled_quantity="2",
        )

        self.assertEqual(late_cancel, filled)
        self.assertIs(late_cancel.status, OrderStatus.FILLED)
        self.assertFalse(self.store.order_events("btc-buy-1")[-1].applied)

    def test_fills_are_idempotent_and_update_weighted_position_and_pnl(self) -> None:
        self.store.create_order(make_order())
        first = ExecutionFill(
            fill_id="trade-1",
            client_order_id="btc-buy-1",
            account_id="binance-spot-testnet",
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=Decimal("1"),
            price=Decimal("100"),
            fee_asset="BNB",
            fee_amount=Decimal("0.001"),
            occurred_at=NOW + timedelta(seconds=1),
            exchange_order_id="42",
        )
        second = ExecutionFill(
            fill_id="trade-2",
            client_order_id="btc-buy-1",
            account_id="binance-spot-testnet",
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=Decimal("1"),
            price=Decimal("110"),
            occurred_at=NOW + timedelta(seconds=2),
        )

        self.store.record_fill(first)
        self.store.record_fill(first)
        self.store.record_fill(second)

        self.assertEqual(len(self.store.fills()), 2)
        order = self.store.require_order("btc-buy-1")
        self.assertIs(order.status, OrderStatus.FILLED)
        self.assertEqual(order.filled_quantity, Decimal("2"))
        self.assertEqual(order.average_fill_price, Decimal("105"))
        position = self.store.positions(account_id="binance-spot-testnet")[0]
        self.assertEqual(position.quantity, Decimal("2"))
        self.assertEqual(position.average_entry_price, Decimal("105"))

        self.store.create_order(make_order("btc-sell-1", side=Side.SELL, quantity="1"))
        self.store.record_fill(
            ExecutionFill(
                fill_id="trade-3",
                client_order_id="btc-sell-1",
                account_id="binance-spot-testnet",
                symbol="BTCUSDT",
                side=Side.SELL,
                quantity=Decimal("1"),
                price=Decimal("120"),
                occurred_at=NOW + timedelta(seconds=3),
            )
        )
        position = self.store.positions(account_id="binance-spot-testnet")[0]
        self.assertEqual(position.quantity, Decimal("1"))
        self.assertEqual(position.average_entry_price, Decimal("105"))
        self.assertEqual(position.realized_pnl, Decimal("15"))

        with self.assertRaisesRegex(ValueError, "different fill"):
            self.store.record_fill(
                ExecutionFill(
                    fill_id="trade-3",
                    client_order_id="btc-sell-1",
                    account_id="binance-spot-testnet",
                    symbol="BTCUSDT",
                    side=Side.SELL,
                    quantity=Decimal("1"),
                    price=Decimal("121"),
                    occurred_at=NOW + timedelta(seconds=3),
                )
            )

    def test_full_and_partial_balance_snapshots_are_durable_and_idempotent(self) -> None:
        initial = self.store.record_balance_snapshot(
            "binance-spot-testnet",
            (
                BalanceValue("BTC", Decimal("1"), Decimal("0.2")),
                BalanceValue("USDT", Decimal("900"), Decimal("100")),
            ),
            event_id="balance-1",
            occurred_at=NOW,
        )
        duplicate = self.store.record_balance_snapshot(
            "binance-spot-testnet",
            (
                BalanceValue("USDT", Decimal("900"), Decimal("100")),
                BalanceValue("BTC", Decimal("1"), Decimal("0.2")),
            ),
            event_id="balance-1",
            occurred_at=NOW,
        )
        self.assertEqual(initial, duplicate)

        updated = self.store.record_balance_snapshot(
            "binance-spot-testnet",
            (BalanceValue("USDT", Decimal("850"), Decimal("150")),),
            event_id="balance-2",
            occurred_at=NOW + timedelta(seconds=1),
            full_snapshot=False,
        )
        self.assertEqual([item.asset for item in updated], ["BTC", "USDT"])
        self.assertEqual(updated[1].free, Decimal("850"))

        self.store.close()
        self.store = SQLiteOrderManagementStore(self.path)
        self.addCleanup(self.store.close)
        self.assertEqual(self.store.balances("binance-spot-testnet"), updated)


class TradingOMSBrokerEventTests(IsolatedAsyncioTestCase):
    async def test_existing_broker_events_can_be_ingested_without_adapter_coupling(self) -> None:
        with TemporaryDirectory() as directory:
            store = SQLiteOrderManagementStore(Path(directory) / "oms.sqlite3")
            order = make_order(quantity="1")
            store.create_order(order)
            broker = PaperBroker()
            await broker.connect()
            await broker.submit_order(order)
            stream = broker.events()

            status = store.record_broker_event(await anext(stream))
            assert isinstance(status, OrderSnapshot)
            self.assertIs(status.status, OrderStatus.ACCEPTED)

            await broker.record_fill(
                order.client_order_id,
                quantity="1",
                price="99",
                fill_id="paper-fill-1",
                occurred_at=NOW + timedelta(seconds=1),
            )
            fill = store.record_broker_event(await anext(stream))
            final = store.record_broker_event(await anext(stream))

            assert isinstance(fill, ExecutionFill)
            assert isinstance(final, OrderSnapshot)
            self.assertIs(final.status, OrderStatus.FILLED)
            self.assertEqual(store.require_order(order.client_order_id).filled_quantity, 1)
            store.close()
