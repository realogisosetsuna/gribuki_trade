from __future__ import annotations

import sqlite3
import unittest

from gribuki_trade.trading.futures_oms_schema import initialize_futures_oms_schema


class FuturesOMSSchemaTests(unittest.TestCase):
    def test_schema_creation_is_idempotent_and_respects_caller_transaction(self) -> None:
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("BEGIN")
        initialize_futures_oms_schema(connection)
        # DDL 仍属于调用方事务；回滚后表结构应被移除。
        connection.rollback()
        self.assertIsNone(
            connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='futures_orders'"
            ).fetchone()
        )

        connection.execute("BEGIN")
        initialize_futures_oms_schema(connection)
        initialize_futures_oms_schema(connection)
        connection.commit()
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'futures_%'"
            )
        }
        self.assertIn("futures_orders", tables)
        self.assertIn("futures_protection_plans", tables)
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM sqlite_master "
                "WHERE type='index' AND name='ix_futures_commands_due'"
            ).fetchone()[0],
            1,
        )
