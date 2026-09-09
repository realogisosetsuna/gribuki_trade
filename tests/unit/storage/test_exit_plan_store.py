from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from gribuki_trade.domain.exit_plans import ExitPlanEventType, NewExitPlanEvent
from gribuki_trade.storage.execution.exit_plans import (
    ExitPlanStoreConcurrencyError,
    ExitPlanStoreConflictError,
    ExitPlanStoreIntegrityError,
    SQLiteExitPlanStore,
)

NOW = datetime(2026, 8, 14, 2, 0, tzinfo=UTC)


def _event(
    *,
    key: str = "plan-created-v1",
    event_type: ExitPlanEventType = ExitPlanEventType.PLAN_CREATED,
    payload: dict[str, object] | None = None,
) -> NewExitPlanEvent:
    return NewExitPlanEvent.create(
        protection_id="protect-1",
        account_id="paper-a",
        symbol="600000.SH",
        event_type=event_type,
        occurred_at=NOW,
        known_at=NOW + timedelta(seconds=1),
        idempotency_key=key,
        payload=payload or {"plan_id": "exit-plan-1", "version": 1},
        plan_id="exit-plan-1",
    )


def test_append_is_hash_chained_and_exactly_idempotent(tmp_path) -> None:
    path = tmp_path / "exit-plans.sqlite3"
    with SQLiteExitPlanStore(path) as store:
        first, inserted = store.append(_event(), expected_sequence=0)
        replay, inserted_again = store.append(_event(), expected_sequence=0)
        second, inserted_second = store.append(
            _event(
                key="deep-requested-v1",
                event_type=ExitPlanEventType.DEEP_ANALYSIS_REQUESTED,
            ),
            expected_sequence=1,
        )

        assert inserted is True
        assert inserted_again is False
        assert inserted_second is True
        assert replay == first
        assert second.previous_hash == first.event_hash
        assert store.events("protect-1") == (first, second)


def test_idempotency_conflict_and_stale_sequence_fail_closed(tmp_path) -> None:
    with SQLiteExitPlanStore(tmp_path / "exit-plans.sqlite3") as store:
        store.append(_event(), expected_sequence=0)
        with pytest.raises(ExitPlanStoreConflictError, match="different"):
            store.append(_event(payload={"version": 2}), expected_sequence=1)
        with pytest.raises(ExitPlanStoreConcurrencyError, match="expected protection"):
            store.append(
                _event(
                    key="deep-requested-v1",
                    event_type=ExitPlanEventType.DEEP_ANALYSIS_REQUESTED,
                ),
                expected_sequence=0,
            )


def test_sqlite_triggers_reject_mutation_and_chain_detects_tampering(tmp_path) -> None:
    path = tmp_path / "exit-plans.sqlite3"
    with SQLiteExitPlanStore(path) as store:
        store.append(_event(), expected_sequence=0)
        with sqlite3.connect(path) as connection:
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                connection.execute(
                    "UPDATE exit_plan_events SET payload_json = '{}' WHERE sequence = 1"
                )
            connection.execute("DROP TRIGGER exit_plan_events_no_update")
            connection.execute(
                "UPDATE exit_plan_events SET payload_json = '{}' WHERE sequence = 1"
            )
        with pytest.raises(ExitPlanStoreIntegrityError, match="payload digest"):
            store.events("protect-1")
