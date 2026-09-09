"""实盘账本的 SQLite 结构与迁移。

本模块集中维护实盘交易账本的持久化形状；存储类负责事务编排与记录语义，
这里负责 DDL 以及可重复执行的结构升级。
"""

from __future__ import annotations

import sqlite3

LIVE_RECORD_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS live_record_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    account_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    previous_hash TEXT,
    event_hash TEXT NOT NULL UNIQUE,
    UNIQUE(account_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS ix_live_record_account_sequence
    ON live_record_events(account_id, sequence);

CREATE TABLE IF NOT EXISTS live_commands (
    command_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    source_message_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    fill_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('PENDING','CONFIRMED','CANCELLED')),
    proposal_event_id TEXT NOT NULL UNIQUE,
    terminal_event_id TEXT UNIQUE,
    proposed_at TEXT NOT NULL,
    terminal_at TEXT,
    FOREIGN KEY(proposal_event_id) REFERENCES live_record_events(event_id),
    FOREIGN KEY(terminal_event_id) REFERENCES live_record_events(event_id)
);
CREATE INDEX IF NOT EXISTS ix_live_commands_account
    ON live_commands(account_id, command_id);

CREATE TABLE IF NOT EXISTS live_external_fills (
    account_id TEXT NOT NULL,
    external_fill_key TEXT NOT NULL,
    command_id TEXT NOT NULL UNIQUE,
    event_id TEXT NOT NULL UNIQUE,
    fill_json TEXT NOT NULL,
    confirmed_at TEXT NOT NULL,
    PRIMARY KEY(account_id, external_fill_key),
    FOREIGN KEY(command_id) REFERENCES live_commands(command_id),
    FOREIGN KEY(event_id) REFERENCES live_record_events(event_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS live_position_projection (
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    instrument_type TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK(quantity >= 0),
    average_cost TEXT NOT NULL,
    realized_pnl TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(account_id, symbol)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS live_protection_tracking (
    protection_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    buy_command_id TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    instrument_type TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    original_quantity INTEGER NOT NULL CHECK(original_quantity > 0),
    remaining_quantity INTEGER NOT NULL
        CHECK(remaining_quantity >= 0 AND remaining_quantity <= original_quantity),
    plan_ready INTEGER NOT NULL DEFAULT 0 CHECK(plan_ready IN (0,1)),
    plan_stream_id TEXT,
    last_observed_bar_end TEXT,
    last_alert_bar_end TEXT,
    FOREIGN KEY(buy_command_id) REFERENCES live_commands(command_id)
);
CREATE INDEX IF NOT EXISTS ix_live_tracking_account_symbol
    ON live_protection_tracking(
        account_id, symbol, remaining_quantity, acquired_at, buy_command_id
    );

CREATE TABLE IF NOT EXISTS live_sell_allocations (
    sell_command_id TEXT NOT NULL,
    buy_command_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK(quantity > 0),
    allocated_at TEXT NOT NULL,
    PRIMARY KEY(sell_command_id, buy_command_id),
    FOREIGN KEY(sell_command_id) REFERENCES live_commands(command_id),
    FOREIGN KEY(buy_command_id) REFERENCES live_commands(command_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS live_work_items (
    work_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    account_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    protection_id TEXT,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(
        status IN ('PENDING','RUNNING','RETRY','COMPLETED','DEAD')
    ),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    available_at TEXT NOT NULL,
    lease_until TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    result_code TEXT,
    error_code TEXT,
    FOREIGN KEY(command_id) REFERENCES live_commands(command_id)
);
CREATE INDEX IF NOT EXISTS ix_live_work_due
    ON live_work_items(status, available_at, lease_until, created_at);

CREATE TRIGGER IF NOT EXISTS live_record_no_update
BEFORE UPDATE ON live_record_events BEGIN
    SELECT RAISE(ABORT, 'live record store is append-only');
END;
CREATE TRIGGER IF NOT EXISTS live_record_no_delete
BEFORE DELETE ON live_record_events BEGIN
    SELECT RAISE(ABORT, 'live record store is append-only');
END;
CREATE TRIGGER IF NOT EXISTS live_external_fills_no_update
BEFORE UPDATE ON live_external_fills BEGIN
    SELECT RAISE(ABORT, 'confirmed external fills are append-only');
END;
CREATE TRIGGER IF NOT EXISTS live_external_fills_no_delete
BEFORE DELETE ON live_external_fills BEGIN
    SELECT RAISE(ABORT, 'confirmed external fills are append-only');
END;
CREATE TRIGGER IF NOT EXISTS live_sell_allocations_no_update
BEFORE UPDATE ON live_sell_allocations BEGIN
    SELECT RAISE(ABORT, 'live sell allocations are append-only');
END;
CREATE TRIGGER IF NOT EXISTS live_sell_allocations_no_delete
BEFORE DELETE ON live_sell_allocations BEGIN
    SELECT RAISE(ABORT, 'live sell allocations are append-only');
END;
CREATE TRIGGER IF NOT EXISTS live_commands_identity_immutable
BEFORE UPDATE ON live_commands
WHEN NEW.command_id != OLD.command_id
  OR NEW.account_id != OLD.account_id
  OR NEW.sender_id != OLD.sender_id
  OR NEW.source_message_id != OLD.source_message_id
  OR NEW.fingerprint != OLD.fingerprint
  OR NEW.fill_json != OLD.fill_json
  OR NEW.proposal_event_id != OLD.proposal_event_id
  OR NEW.proposed_at != OLD.proposed_at
BEGIN
    SELECT RAISE(ABORT, 'live command identity is immutable');
END;
CREATE TRIGGER IF NOT EXISTS live_commands_terminal_immutable
BEFORE UPDATE ON live_commands
WHEN OLD.state != 'PENDING'
  AND (
    NEW.state != OLD.state
    OR COALESCE(NEW.terminal_event_id, '')
       != COALESCE(OLD.terminal_event_id, '')
    OR COALESCE(NEW.terminal_at, '') != COALESCE(OLD.terminal_at, '')
  )
BEGIN
    SELECT RAISE(ABORT, 'terminal live command is immutable');
END;
CREATE TRIGGER IF NOT EXISTS live_commands_terminal_shape
BEFORE UPDATE ON live_commands
WHEN (NEW.state = 'PENDING' AND (
        NEW.terminal_event_id IS NOT NULL OR NEW.terminal_at IS NOT NULL
     ))
  OR (NEW.state != 'PENDING' AND (
        NEW.terminal_event_id IS NULL OR NEW.terminal_at IS NULL
     ))
BEGIN
    SELECT RAISE(ABORT, 'live command terminal shape is invalid');
END;
CREATE TRIGGER IF NOT EXISTS live_tracking_identity_immutable
BEFORE UPDATE ON live_protection_tracking
WHEN NEW.protection_id != OLD.protection_id
  OR NEW.account_id != OLD.account_id
  OR NEW.buy_command_id != OLD.buy_command_id
  OR NEW.symbol != OLD.symbol
  OR NEW.instrument_type != OLD.instrument_type
  OR NEW.acquired_at != OLD.acquired_at
  OR NEW.original_quantity != OLD.original_quantity
BEGIN
    SELECT RAISE(ABORT, 'live protection identity is immutable');
END;
CREATE TRIGGER IF NOT EXISTS live_work_identity_immutable
BEFORE UPDATE ON live_work_items
WHEN NEW.work_id != OLD.work_id
  OR NEW.kind != OLD.kind
  OR NEW.account_id != OLD.account_id
  OR NEW.command_id != OLD.command_id
  OR COALESCE(NEW.protection_id, '') != COALESCE(OLD.protection_id, '')
  OR NEW.payload_json != OLD.payload_json
  OR NEW.created_at != OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'live work identity and payload are immutable');
END;
"""


def ensure_live_record_schema(connection: sqlite3.Connection) -> None:
    """创建实盘账本结构并执行安全、幂等的结构升级。

    ``executescript`` 必须在存储类事务之外运行；后续投影重建由调用方放入
    自己的 ``BEGIN IMMEDIATE`` 事务中执行。
    """

    connection.executescript(LIVE_RECORD_SCHEMA_SQL)
    tracking_columns = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
        for row in connection.execute(
            "PRAGMA table_info(live_protection_tracking)"
        ).fetchall()
    }
    if "plan_stream_id" not in tracking_columns:
        connection.execute(
            "ALTER TABLE live_protection_tracking ADD COLUMN plan_stream_id TEXT"
        )
    connection.execute(
        """
        UPDATE live_protection_tracking
        SET plan_stream_id = protection_id
        WHERE plan_ready = 1 AND plan_stream_id IS NULL
        """
    )
