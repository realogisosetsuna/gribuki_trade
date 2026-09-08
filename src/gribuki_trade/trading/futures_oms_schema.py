"""USDⓈ-M/Coin-M 合约 OMS 的 SQLite 表结构辅助函数。

持久化存储负责事务和状态转换；本模块把表结构集中在一个便于审查的边界。
``initialize_futures_oms_schema`` 使用调用方的连接，不会自行创建连接或提交事务，
因此可以安全地在存储层现有的 ``BEGIN IMMEDIATE`` 事务中执行，并支持进程重启后的幂等初始化。
"""

from __future__ import annotations

import sqlite3

# ruff: noqa: E501


_SCHEMA_SQL = """
                CREATE TABLE IF NOT EXISTS futures_orders (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  order_key TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
                  position_side TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL,
                  client_order_id TEXT, exchange_order_id TEXT, algo_id TEXT, client_algo_id TEXT,
                  parent_order_key TEXT, protection_plan_id TEXT, order_type TEXT,
                  execution_type TEXT, quantity TEXT NOT NULL, filled_quantity TEXT NOT NULL,
                  average_price TEXT, trigger_price TEXT, activate_price TEXT, callback_rate TEXT,
                  reduce_only INTEGER NOT NULL, close_position INTEGER NOT NULL, working_type TEXT,
                  realized_pnl TEXT NOT NULL, status_time_ms INTEGER NOT NULL,
                  updated_at TEXT NOT NULL,
                  extra_json TEXT NOT NULL, PRIMARY KEY(account_id, environment, product, order_key)
                );
                CREATE TABLE IF NOT EXISTS futures_events (
                  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  event_id TEXT NOT NULL, event_type TEXT NOT NULL, event_time_ms INTEGER NOT NULL,
                  transaction_time_ms INTEGER, received_time_ms INTEGER NOT NULL,
                  connection_epoch INTEGER NOT NULL, payload_json TEXT NOT NULL,
                  applied INTEGER NOT NULL,
                  UNIQUE(account_id, environment, product, event_id)
                );
                CREATE TABLE IF NOT EXISTS futures_fills (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  fill_id TEXT NOT NULL, trade_id TEXT, symbol TEXT NOT NULL, side TEXT NOT NULL,
                  position_side TEXT NOT NULL, quantity TEXT NOT NULL, price TEXT NOT NULL,
                  order_key TEXT, exchange_order_id TEXT, fee_asset TEXT, fee_amount TEXT NOT NULL,
                  realized_pnl TEXT NOT NULL, occurred_at TEXT NOT NULL, extra_json TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, fill_id),
                  UNIQUE(account_id, environment, product, symbol, trade_id)
                );
                CREATE TABLE IF NOT EXISTS futures_positions (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  symbol TEXT NOT NULL, position_side TEXT NOT NULL, quantity TEXT NOT NULL,
                  entry_price TEXT NOT NULL, break_even_price TEXT NOT NULL,
                  realized_pnl TEXT NOT NULL,
                  unrealized_pnl TEXT NOT NULL, margin_type TEXT, isolated_wallet TEXT NOT NULL,
                  leverage INTEGER, updated_at TEXT NOT NULL, extra_json TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, symbol, position_side)
                );
                CREATE TABLE IF NOT EXISTS futures_balances (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  asset TEXT NOT NULL, wallet_balance TEXT NOT NULL,
                  available_balance TEXT NOT NULL,
                  cross_wallet_balance TEXT NOT NULL, updated_at TEXT NOT NULL,
                  extra_json TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, asset)
                );
                CREATE TABLE IF NOT EXISTS futures_configs (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  symbol TEXT NOT NULL, leverage INTEGER, margin_type TEXT, position_mode TEXT,
                  multi_assets_mode INTEGER, updated_at TEXT NOT NULL, extra_json TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, symbol)
                );
                CREATE TABLE IF NOT EXISTS futures_snapshot_watermarks (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  snapshot_kind TEXT NOT NULL, cutoff_at TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, snapshot_kind)
                );
                CREATE TABLE IF NOT EXISTS futures_commands (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  command_id TEXT NOT NULL, command_type TEXT NOT NULL, order_key TEXT,
                  payload_json TEXT NOT NULL, status TEXT NOT NULL, attempt_count INTEGER NOT NULL,
                  owner_id TEXT, fencing_token INTEGER, lease_until TEXT, error_code TEXT,
                  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, command_id)
                );
                CREATE TABLE IF NOT EXISTS futures_leases (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  owner_id TEXT NOT NULL, fencing_token INTEGER NOT NULL, lease_until TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product)
                );
                CREATE TABLE IF NOT EXISTS futures_stream_health (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  state TEXT NOT NULL, connection_epoch INTEGER NOT NULL,
                  last_event_time_ms INTEGER,
                  last_received_time_ms INTEGER, gap_count INTEGER NOT NULL, reason TEXT,
                  updated_at TEXT NOT NULL, PRIMARY KEY(account_id, environment, product)
                );
                CREATE TABLE IF NOT EXISTS futures_protection_plans (
                  account_id TEXT NOT NULL, environment TEXT NOT NULL, product TEXT NOT NULL,
                  plan_id TEXT NOT NULL, revision INTEGER NOT NULL, symbol TEXT NOT NULL,
                  position_side TEXT NOT NULL, desired_state TEXT NOT NULL,
                  coverage_state TEXT NOT NULL,
                  entry_order_key TEXT, stop_algo_key TEXT, take_profit_algo_key TEXT,
                  trailing_algo_key TEXT, updated_at TEXT NOT NULL, extra_json TEXT NOT NULL,
                  PRIMARY KEY(account_id, environment, product, plan_id, revision)
                );
                CREATE INDEX IF NOT EXISTS ix_futures_commands_due
                  ON futures_commands(account_id, environment, product, status, created_at);
                """


def initialize_futures_oms_schema(connection: sqlite3.Connection) -> None:
    """创建合约 OMS 所需的表和索引；已存在时保持不变。

    锁和事务边界由调用方控制。重复执行不会破坏已有记录，满足持久化 OMS
    在已有数据库上重启恢复的要求。
    """

    # sqlite3 的 executescript 会隐式提交活动事务；逐条执行 DDL，
    # 让调用方拥有的启动与迁移事务保持原子性。
    for statement in _SCHEMA_SQL.split(";"):
        if statement.strip():
            connection.execute(statement)
