"""面向 broker-neutral OMS 的 SQLite 表结构与迁移辅助函数。

将 DDL 放在 :mod:`trading.oms` 之外可以让事务 facade 更易阅读，同时保留单一
持久化边界。辅助函数接收调用方已有的连接，因此锁和事务范围仍由调用方控制。
"""

from __future__ import annotations

import sqlite3


def initialize_oms_schema(connection: sqlite3.Connection) -> None:
    """在 ``connection`` 上创建或迁移 broker-neutral OMS 表。

    事务由调用方持有；重复运行是安全的，并继续支持旧版本数据库的
    ``broker_error_code`` 字段迁移。
    """

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS oms_orders (
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
            broker_error_code INTEGER,
            updated_at TEXT NOT NULL
        )
        """
    )
    columns = {
        str(row["name"])
        for row in connection.execute("PRAGMA table_info(oms_orders)").fetchall()
    }
    if "broker_error_code" not in columns:
        connection.execute("ALTER TABLE oms_orders ADD COLUMN broker_error_code INTEGER")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS oms_order_events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            client_order_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            status TEXT,
            occurred_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            applied INTEGER NOT NULL,
            FOREIGN KEY(client_order_id) REFERENCES oms_orders(client_order_id),
            CHECK(applied IN (0, 1))
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_oms_order_events_order
        ON oms_order_events(client_order_id, sequence)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS oms_command_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            command_id TEXT NOT NULL UNIQUE,
            command_type TEXT NOT NULL,
            client_order_id TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            lease_until TEXT,
            last_error_code TEXT,
            dispatched_at TEXT,
            FOREIGN KEY(client_order_id) REFERENCES oms_orders(client_order_id),
            CHECK(attempt_count >= 0)
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_oms_command_due
        ON oms_command_outbox(status, id)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS oms_fills (
            fill_id TEXT PRIMARY KEY,
            client_order_id TEXT NOT NULL,
            account_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            quantity TEXT NOT NULL,
            price TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            fee_asset TEXT,
            fee_amount TEXT NOT NULL,
            exchange_order_id TEXT,
            FOREIGN KEY(client_order_id) REFERENCES oms_orders(client_order_id)
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_oms_fills_order
        ON oms_fills(client_order_id, occurred_at, fill_id)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS oms_account_events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            account_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS oms_balances (
            account_id TEXT NOT NULL,
            asset TEXT NOT NULL,
            free TEXT NOT NULL,
            locked TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(account_id, asset)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS oms_positions (
            account_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            quantity TEXT NOT NULL,
            average_entry_price TEXT NOT NULL,
            realized_pnl TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(account_id, symbol)
        )
        """
    )
