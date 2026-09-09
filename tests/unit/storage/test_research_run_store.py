from __future__ import annotations

import hashlib
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from gribuki_trade.storage.research.research_runs import (
    ResearchRunCollisionError,
    SQLiteResearchRunStore,
)

STARTED = datetime(2026, 8, 14, 7, 5, tzinfo=UTC)


def _append(
    store: SQLiteResearchRunStore,
    *,
    key: str = "2026-08-14/close/source-revision-1",
    score: Decimal = Decimal("0.125"),
) -> bool:
    return store.append(
        run_type="ashare_close_screen",
        logical_key=key,
        status="COMPLETE",
        started_at=STARTED,
        completed_at=STARTED + timedelta(seconds=5),
        strategy_version="ashare-cross-section@1",
        source_revisions=(("akshare.universe", "revision-1"),),
        config={"top_n": 30, "minimum": Decimal("0.10")},
        payload={
            "ok": True,
            "score": score,
            "symbols": ["600000.SH", "000001.SZ"],
        },
    )


def test_wal_round_trip_and_exact_replay(tmp_path: Path) -> None:
    path = tmp_path / "runs.sqlite3"
    with SQLiteResearchRunStore(path) as store:
        assert _append(store) is True
        assert _append(store) is False
        runs = store.list_runs()
        assert len(runs) == 1
        run = store.get(runs[0].run_id)
        assert run is not None
        assert run.payload_document() == {
            "ok": True,
            "score": "0.125",
            "symbols": ["600000.SH", "000001.SZ"],
        }
        assert run.config_document() == {"minimum": "0.10", "top_n": 30}
        assert run.source_revisions == (("akshare.universe", "revision-1"),)
        assert run.started_at == STARTED

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)


def test_same_logical_identity_with_changed_content_is_collision(tmp_path: Path) -> None:
    with SQLiteResearchRunStore(tmp_path / "runs.sqlite3") as store:
        assert _append(store)
        with pytest.raises(ResearchRunCollisionError):
            _append(store, score=Decimal("0.126"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "DEGRADED"),
        ("strategy_version", "ashare-cross-section@2"),
        ("completed_at", STARTED + timedelta(seconds=6)),
        ("source_revisions", (("akshare.universe", "revision-2"),)),
    ],
)
def test_same_payload_with_changed_lineage_is_collision(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    with SQLiteResearchRunStore(tmp_path / "runs.sqlite3") as store:
        assert _append(store)
        arguments: dict[str, object] = {
            "run_type": "ashare_close_screen",
            "logical_key": "2026-08-14/close/source-revision-1",
            "status": "COMPLETE",
            "started_at": STARTED,
            "completed_at": STARTED + timedelta(seconds=5),
            "strategy_version": "ashare-cross-section@1",
            "source_revisions": (("akshare.universe", "revision-1"),),
            "config": {"top_n": 30, "minimum": Decimal("0.10")},
            "payload": {
                "ok": True,
                "score": Decimal("0.125"),
                "symbols": ["600000.SH", "000001.SZ"],
            },
        }
        arguments[field] = value
        with pytest.raises(ResearchRunCollisionError):
            store.append(**arguments)  # type: ignore[arg-type]


def test_list_filters_type_and_orders_latest_first(tmp_path: Path) -> None:
    path = tmp_path / "runs.sqlite3"
    with SQLiteResearchRunStore(path) as store:
        assert _append(store, key="first")
        assert store.append(
            run_type="ashare_intraday_surveillance",
            logical_key="second",
            status="DEGRADED",
            started_at=STARTED + timedelta(minutes=1),
            completed_at=STARTED + timedelta(minutes=2),
            payload={"ok": True},
            config={"top_n": 10},
        )
        assert [item.logical_key for item in store.list_runs()] == ["second", "first"]
        assert [
            item.logical_key
            for item in store.list_runs(run_type="ashare_close_screen")
        ] == ["first"]


def test_invalid_time_nonfinite_and_duplicate_source_fail_closed(tmp_path: Path) -> None:
    with SQLiteResearchRunStore(tmp_path / "runs.sqlite3") as store:
        common = {
            "run_type": "screen",
            "logical_key": "key",
            "status": "FAILED",
            "started_at": STARTED,
            "completed_at": STARTED,
            "config": {},
        }
        with pytest.raises(ValueError, match="non-finite"):
            store.append(payload={"bad": float("nan")}, **common)
        with pytest.raises(ValueError, match="precede"):
            store.append(
                payload={},
                **{
                    **common,
                    "logical_key": "earlier",
                    "completed_at": STARTED - timedelta(seconds=1),
                },
            )
        with pytest.raises(ValueError, match="one revision"):
            store.append(
                payload={},
                source_revisions=(("same", "one"), ("same", "two")),
                **{**common, "logical_key": "duplicate-source"},
            )


def test_sqlite_triggers_reject_update_and_delete(tmp_path: Path) -> None:
    path = tmp_path / "runs.sqlite3"
    with SQLiteResearchRunStore(path) as store:
        assert _append(store)
    with sqlite3.connect(path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("UPDATE research_runs SET status = 'FAILED'")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM research_runs")


def test_preview_schema_is_migrated_without_fabricating_config(tmp_path: Path) -> None:
    path = tmp_path / "legacy-runs.sqlite3"
    payload_json = '{"ok":true}'
    payload_sha256 = hashlib.sha256(payload_json.encode()).hexdigest()
    config_sha256 = hashlib.sha256(b"{}").hexdigest()
    from gribuki_trade.storage.research.research_runs import research_run_id

    run_id = research_run_id("legacy", "one")
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE research_runs (
                run_id TEXT PRIMARY KEY, run_type TEXT NOT NULL,
                logical_key TEXT NOT NULL, strategy_version TEXT,
                status TEXT NOT NULL, started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL, source_revisions_json TEXT NOT NULL,
                config_sha256 TEXT NOT NULL, payload_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL, record_version INTEGER NOT NULL,
                UNIQUE(run_type, logical_key)
            )
            """
        )
        connection.execute(
            "INSERT INTO research_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                "legacy",
                "one",
                None,
                "COMPLETE",
                STARTED.isoformat(),
                STARTED.isoformat(),
                "[]",
                config_sha256,
                payload_json,
                payload_sha256,
                1,
            ),
        )

    with SQLiteResearchRunStore(path) as store:
        run = store.get(run_id)
        assert run is not None
        assert run.payload_document() == {"ok": True}
        assert run.config_document() is None
