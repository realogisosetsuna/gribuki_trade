import sqlite3

import pytest

from gribuki_trade.storage.live_record_schema import ensure_live_record_schema


def test_live_record_schema_is_idempotent_and_contains_durable_boundaries() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        ensure_live_record_schema(connection)
        # 初始化可以安全重复执行，以支持进程重启。
        ensure_live_record_schema(connection)

        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert {
            "live_record_events",
            "live_commands",
            "live_external_fills",
            "live_position_projection",
            "live_protection_tracking",
            "live_sell_allocations",
            "live_work_items",
        } <= tables
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(live_protection_tracking)")
        }
        assert "plan_stream_id" in columns

        connection.execute(
            """
            INSERT INTO live_record_events(
                event_id, account_id, event_type, occurred_at,
                idempotency_key, payload_json, payload_sha256,
                previous_hash, event_hash
            ) VALUES ('e1', 'acct', 'COMMAND_PROPOSED', '2025-01-01T00:00:00+00:00',
                      'k1', '{}', 'sha', NULL, 'hash')
            """
        )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE live_record_events SET payload_json = '{}' WHERE event_id = 'e1'"
            )
    finally:
        connection.close()


def test_live_record_schema_migrates_legacy_tracking_table() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute(
            """
            CREATE TABLE live_protection_tracking (
                protection_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                buy_command_id TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                original_quantity INTEGER NOT NULL,
                remaining_quantity INTEGER NOT NULL,
                plan_ready INTEGER NOT NULL DEFAULT 0,
                last_observed_bar_end TEXT,
                last_alert_bar_end TEXT
            )
            """
        )
        ensure_live_record_schema(connection)
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(live_protection_tracking)")
        }
        assert "plan_stream_id" in columns
    finally:
        connection.close()
