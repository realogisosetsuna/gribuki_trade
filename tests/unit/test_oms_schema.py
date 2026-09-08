import sqlite3
from unittest import TestCase

from gribuki_trade.trading.oms_schema import initialize_oms_schema


class OmsSchemaTests(TestCase):
    def test_schema_is_idempotent_and_keeps_legacy_migration(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        try:
            initialize_oms_schema(connection)
            initialize_oms_schema(connection)
            tables = {
                str(row["name"])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            self.assertTrue(
                {
                    "oms_orders",
                    "oms_order_events",
                    "oms_command_outbox",
                    "oms_fills",
                    "oms_account_events",
                    "oms_balances",
                    "oms_positions",
                }.issubset(tables)
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(oms_orders)")
            }
            self.assertIn("broker_error_code", columns)
        finally:
            connection.close()

    def test_schema_adds_broker_error_code_to_legacy_orders_table(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(
                """
                CREATE TABLE oms_orders (
                    client_order_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    limit_price TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    filled_quantity TEXT NOT NULL,
                    average_fill_price TEXT,
                    exchange_order_id TEXT,
                    reason TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            initialize_oms_schema(connection)
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(oms_orders)")
            }
            self.assertIn("broker_error_code", columns)
        finally:
            connection.close()
