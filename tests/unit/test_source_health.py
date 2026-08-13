from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from gribuki_trade.storage.source_health import (
    ProviderRun,
    ProviderRunStatus,
    SourceHealthCollisionError,
    SQLiteSourceHealthStore,
)

BASE_TIME = datetime(2026, 8, 1, 1, 0, tzinfo=UTC)


def _run(
    run_id: str,
    *,
    minute: int = 0,
    status: ProviderRunStatus = ProviderRunStatus.SUCCESS,
    latency_ms: float = 100.0,
    degraded: bool = False,
    stale: bool = False,
    operation: str = "bars.5m",
    error_code: str | None = None,
) -> ProviderRun:
    started = BASE_TIME + timedelta(minutes=minute)
    return ProviderRun(
        run_id=run_id,
        source_id="akshare.eastmoney",
        operation=operation,
        started_at=started,
        finished_at=started + timedelta(seconds=1),
        status=status,
        error_code=error_code,
        item_count=20 if status is ProviderRunStatus.SUCCESS else 0,
        degraded=degraded,
        stale=stale,
        latency_ms=latency_ms,
        adapter_version="akshare-1.18.84",
    )


def test_round_trip_wal_and_idempotent_replay(tmp_path: Path) -> None:
    database = tmp_path / "source-health.sqlite3"
    run = _run("run-1")

    with SQLiteSourceHealthStore(database) as store:
        assert store.append(run) is True
        assert store.append(run) is False
        assert store.get("run-1") == run

    with sqlite3.connect(database) as connection:
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()
        count = connection.execute("SELECT COUNT(*) FROM provider_runs").fetchone()
    assert journal_mode == ("wal",)
    assert count == (1,)


def test_same_run_id_with_different_content_is_a_collision(tmp_path: Path) -> None:
    with SQLiteSourceHealthStore(tmp_path / "health.sqlite3") as store:
        original = _run("same-run")
        assert store.append(original) is True
        with pytest.raises(SourceHealthCollisionError):
            store.append(replace(original, item_count=21))
        assert store.get("same-run") == original


def test_summary_rates_percentiles_latest_and_operation_filter(tmp_path: Path) -> None:
    runs = (
        _run("run-1", minute=1, latency_ms=100.0),
        _run(
            "run-2",
            minute=2,
            status=ProviderRunStatus.FAILURE,
            latency_ms=200.0,
            degraded=True,
            error_code="provider_timeout",
        ),
        _run("run-3", minute=3, latency_ms=300.0, stale=True),
        _run("run-4", minute=4, latency_ms=400.0, operation="snapshot"),
    )
    with SQLiteSourceHealthStore(tmp_path / "health.sqlite3") as store:
        for run in runs:
            assert store.append(run)

        summary = store.summarize(
            "akshare.eastmoney",
            operation="bars.5m",
            window_start=BASE_TIME,
            window_end=BASE_TIME + timedelta(minutes=4),
        )

    assert summary.total_runs == 3
    assert summary.successful_runs == 2
    assert summary.failed_runs == 1
    assert summary.success_rate == pytest.approx(2 / 3)
    assert summary.degraded_rate == pytest.approx(1 / 3)
    assert summary.stale_rate == pytest.approx(1 / 3)
    assert summary.p50_latency_ms == 200.0
    assert summary.p95_latency_ms == pytest.approx(290.0)
    assert summary.latest_success == runs[2]
    assert summary.latest_failure == runs[1]


def test_empty_summary_has_explicit_unknown_metrics(tmp_path: Path) -> None:
    offset_time = datetime(2026, 8, 1, 9, 0, tzinfo=timezone(timedelta(hours=8)))
    with SQLiteSourceHealthStore(tmp_path / "health.sqlite3") as store:
        summary = store.summarize(
            "akshare.eastmoney",
            window_start=offset_time,
            window_end=offset_time + timedelta(hours=1),
        )

    assert summary.window_start.tzinfo is UTC
    assert summary.total_runs == 0
    assert summary.success_rate is None
    assert summary.degraded_rate is None
    assert summary.stale_rate is None
    assert summary.p50_latency_ms is None
    assert summary.p95_latency_ms is None
    assert summary.latest_success is None
    assert summary.latest_failure is None


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"started_at": BASE_TIME.replace(tzinfo=None)}, "started_at"),
        ({"finished_at": BASE_TIME.replace(tzinfo=None)}, "finished_at"),
        ({"item_count": -1}, "item_count"),
        ({"item_count": True}, "item_count"),
        ({"latency_ms": -0.1}, "latency_ms"),
        ({"latency_ms": float("nan")}, "latency_ms"),
        ({"record_version": 0}, "record_version"),
        ({"status": "ok"}, "status"),
        ({"source_id": "https://example.test"}, "source_id"),
    ],
)
def test_provider_run_rejects_invalid_values(
    changes: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        replace(_run("valid-run"), **changes)


def test_provider_run_rejects_reverse_wall_clock_interval() -> None:
    run = _run("reverse-time")
    with pytest.raises(ValueError, match="finished_at"):
        replace(run, finished_at=run.started_at - timedelta(microseconds=1))


def test_unsafe_error_text_is_not_persisted(tmp_path: Path) -> None:
    unsafe = "Timeout GET https://example.test/?token=very-secret"
    run = _run(
        "safe-error",
        status=ProviderRunStatus.FAILURE,
        error_code=unsafe,
    )
    assert run.error_code == "unclassified_error"

    database = tmp_path / "health.sqlite3"
    with SQLiteSourceHealthStore(database) as store:
        store.append(run)
        loaded = store.get(run.run_id)

    assert loaded is not None
    assert loaded.error_code == "unclassified_error"
    assert "very-secret" not in database.read_bytes().decode("latin-1")


def test_database_triggers_enforce_append_only_records(tmp_path: Path) -> None:
    database = tmp_path / "health.sqlite3"
    with SQLiteSourceHealthStore(database) as store:
        store.append(_run("immutable-run"))

        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            store._connection.execute(  # noqa: SLF001 - verifies the DB invariant
                "UPDATE provider_runs SET item_count = 99 WHERE run_id = ?",
                ("immutable-run",),
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            store._connection.execute(  # noqa: SLF001 - verifies the DB invariant
                "DELETE FROM provider_runs WHERE run_id = ?",
                ("immutable-run",),
            )


def test_two_store_instances_can_append_concurrently(tmp_path: Path) -> None:
    database = tmp_path / "health.sqlite3"
    first = SQLiteSourceHealthStore(database)
    second = SQLiteSourceHealthStore(database)
    try:
        stores = (first, second)

        def append(index: int) -> bool:
            return stores[index % 2].append(_run(f"run-{index}", minute=index))

        with ThreadPoolExecutor(max_workers=4) as executor:
            results = tuple(executor.map(append, range(20)))

        assert all(results)
        assert len(first.list_runs(source_id="akshare.eastmoney")) == 20
    finally:
        first.close()
        second.close()


def test_window_and_query_validation_and_closed_store(tmp_path: Path) -> None:
    store = SQLiteSourceHealthStore(tmp_path / "health.sqlite3")
    with pytest.raises(ValueError, match="timezone-aware"):
        store.summarize(
            "akshare.eastmoney",
            window_start=BASE_TIME.replace(tzinfo=None),
            window_end=BASE_TIME + timedelta(hours=1),
        )
    with pytest.raises(ValueError, match="later"):
        store.summarize(
            "akshare.eastmoney",
            window_start=BASE_TIME,
            window_end=BASE_TIME,
        )
    with pytest.raises(ValueError, match="requires source_id"):
        store.list_runs(operation="bars.5m")
    with pytest.raises(ValueError, match="positive"):
        store.list_runs(limit=0)

    store.close()
    with pytest.raises(RuntimeError, match="closed"):
        store.get("run-1")
