import csv
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from gribuki_trade.adapters.manual_ticket import TICKET_COLUMNS, export_manual_tickets
from gribuki_trade.domain.orders import OrderIntent, Side


def make_order(
    client_order_id: str,
    *,
    symbol: str,
    side: Side,
    quantity: int,
    price: str,
    strategy_id: str = "manual-review",
) -> OrderIntent:
    return OrderIntent(
        client_order_id=client_order_id,
        account_id="paper",
        strategy_id=strategy_id,
        symbol=symbol,
        side=side,
        quantity=quantity,
        limit_price=Decimal(price),
        created_at=datetime(2026, 8, 13, 1, 30, 45, 123456, tzinfo=UTC),
    )


class ManualTicketExportTests(TestCase):
    def test_exports_exact_columns_in_stable_client_order_id_order(self) -> None:
        orders = [
            make_order(
                "ticket-002",
                symbol="000001.SZ",
                side=Side.SELL,
                quantity=300,
                price="12.3000",
            ),
            make_order(
                "ticket-001",
                symbol="600000.SH",
                side=Side.BUY,
                quantity=100,
                price="10.0100",
                strategy_id="周度趋势",
            ),
        ]

        with TemporaryDirectory() as directory:
            destination = Path(directory) / "tickets.csv"
            result = export_manual_tickets(orders, destination)

            self.assertEqual(result, destination)
            self.assertEqual(destination.read_bytes()[:3], b"cli")
            with destination.open(encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream)
                rows = list(reader)

        self.assertEqual(tuple(reader.fieldnames or ()), TICKET_COLUMNS)
        self.assertEqual([row["client_order_id"] for row in rows], ["ticket-001", "ticket-002"])
        self.assertEqual(rows[0]["limit_price"], "10.0100")
        self.assertEqual(rows[0]["strategy_id"], "周度趋势")
        self.assertEqual(rows[0]["created_at"], "2026-08-13T01:30:45.123456+00:00")
        self.assertEqual(rows[1]["side"], "SELL")
        self.assertEqual(rows[1]["quantity"], "300")

    def test_refuses_to_overwrite_existing_file_by_default(self) -> None:
        with TemporaryDirectory() as directory:
            destination = Path(directory) / "tickets.csv"
            destination.write_text("keep me", encoding="utf-8")

            with self.assertRaisesRegex(FileExistsError, "already exists"):
                export_manual_tickets([], destination)

            self.assertEqual(destination.read_text(encoding="utf-8"), "keep me")

    def test_explicit_overwrite_replaces_existing_file(self) -> None:
        order = make_order(
            "ticket-001",
            symbol="600000.SH",
            side=Side.BUY,
            quantity=100,
            price="10.01",
        )
        with TemporaryDirectory() as directory:
            destination = Path(directory) / "tickets.csv"
            destination.write_text("old", encoding="utf-8")

            export_manual_tickets([order], destination, overwrite=True)

            self.assertIn("ticket-001", destination.read_text(encoding="utf-8"))

    def test_duplicate_client_order_id_is_rejected_before_creating_file(self) -> None:
        first = make_order(
            "duplicate",
            symbol="600000.SH",
            side=Side.BUY,
            quantity=100,
            price="10.01",
        )
        second = make_order(
            "duplicate",
            symbol="000001.SZ",
            side=Side.SELL,
            quantity=200,
            price="12.34",
        )

        with TemporaryDirectory() as directory:
            destination = Path(directory) / "tickets.csv"
            with self.assertRaisesRegex(ValueError, "duplicate client_order_id"):
                export_manual_tickets([first, second], destination)

            self.assertFalse(destination.exists())
            self.assertEqual(list(Path(directory).iterdir()), [])
