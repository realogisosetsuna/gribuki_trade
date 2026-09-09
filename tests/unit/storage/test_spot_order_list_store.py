from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from gribuki_trade.adapters.binance import BinanceOrderListSnapshot, BinanceOrderSnapshot
from gribuki_trade.domain.orders import OrderStatus, Side
from gribuki_trade.trading import SpotOrderListRecord, SQLiteSpotOrderListStore

NOW = datetime(2026, 9, 9, 12, 0, tzinfo=UTC)


def snapshot(
    *, status: str = "EXEC_STARTED", transaction_ms: int = 100
) -> BinanceOrderListSnapshot:
    order = BinanceOrderSnapshot(
        symbol="BTCUSDT",
        client_order_id="leg-1",
        order_id=11,
        status=OrderStatus.ACCEPTED,
        exchange_status="NEW",
        side=Side.BUY,
        price=Decimal("100"),
        original_quantity=Decimal("1"),
        executed_quantity=Decimal("0"),
        transact_time_ms=transaction_ms,
    )
    return BinanceOrderListSnapshot(
        order_list_id=7,
        contingency_type="OCO",
        list_status_type=status,
        list_order_status="EXECUTING" if status == "EXEC_STARTED" else "ALL_DONE",
        list_client_order_id="list-7",
        symbol="BTCUSDT",
        orders=(order,),
        transaction_time_ms=transaction_ms,
    )


def record(*, status: str = "EXEC_STARTED", transaction_ms: int = 100) -> SpotOrderListRecord:
    current = snapshot(status=status, transaction_ms=transaction_ms)
    return SpotOrderListRecord(
        account_id="acct",
        order_list_id=current.order_list_id,
        list_client_order_id=current.list_client_order_id,
        symbol=current.symbol,
        contingency_type=current.contingency_type,
        list_status_type=current.list_status_type,
        list_order_status=current.list_order_status,
        order_ids=(11,),
        client_order_ids=("leg-1",),
        updated_at=NOW,
        transaction_time_ms=current.transaction_time_ms,
    )
class SpotOrderListStoreTests(TestCase):
    def test_event_and_projection_are_idempotent_and_monotonic(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "lists.sqlite3"
            store = SQLiteSpotOrderListStore(path)
            first = store.upsert(
                record(), event_id="event-1", event_type="SNAPSHOT", occurred_at=NOW
            )
            duplicate = store.upsert(
                record(), event_id="event-1", event_type="SNAPSHOT", occurred_at=NOW
            )
            stale = store.upsert(
                record(status="EXEC_STARTED", transaction_ms=99),
                event_id="event-stale",
                event_type="SNAPSHOT",
                occurred_at=NOW - timedelta(seconds=1),
            )
            latest = store.upsert(
                record(status="ALL_DONE", transaction_ms=101),
                event_id="event-2",
                event_type="SNAPSHOT",
                occurred_at=NOW + timedelta(seconds=1),
            )
            self.assertTrue(first)
            self.assertFalse(duplicate)
            self.assertFalse(stale)
            self.assertTrue(latest)
            loaded = store.get("acct", "id:7")
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.list_order_status, "ALL_DONE")
            self.assertEqual(len(store.events("acct")), 3)
            self.assertEqual(sum(int(event["applied"]) for event in store.events("acct")), 2)
            store.close()

    def test_restart_reads_independent_list_table(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "lists.sqlite3"
            first = SQLiteSpotOrderListStore(path)
            record = SpotOrderListRecord(
                account_id="acct",
                order_list_id=12,
                list_client_order_id="list-12",
                symbol="ETHUSDT",
                contingency_type="OTOCO",
                list_status_type="EXEC_STARTED",
                list_order_status="EXECUTING",
                order_ids=(1, 2, 3),
                client_order_ids=("a", "b", "c"),
                updated_at=NOW,
                transaction_time_ms=123,
                source="USER_STREAM",
            )
            first.upsert(record, event_id="stream-1", event_type="LIST_STATUS")
            restarted = SQLiteSpotOrderListStore(path)
            loaded = restarted.records("acct", open_only=True)
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0].key, "id:12")
            self.assertEqual(loaded[0].client_order_ids, ("a", "b", "c"))
            restarted.close()
            first.close()
