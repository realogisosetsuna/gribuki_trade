from __future__ import annotations

import sqlite3
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from gribuki_trade.domain.paper_day import (
    NewPaperDayEvent,
    PaperDayPhase,
    PaperDayRunManifest,
    PaperDaySeverity,
    paper_day_document_sha256,
    paper_day_event_id,
    paper_day_target_hash,
)
from gribuki_trade.storage.paper_day import (
    PaperDayStoreConflictError,
    PaperDayStoreIntegrityError,
    PaperDayStoreLeaseError,
    SQLitePaperDayStore,
)

SESSION_DATE = date(2026, 8, 14)
T0 = datetime(2026, 8, 14, 0, tzinfo=UTC)
TARGET_HASH = paper_day_target_hash(
    channel="onebot", target_kind="private", target_id="not-retained"
)


def _manifest(
    *, config: dict[str, object] | None = None, created_at: datetime = T0
) -> PaperDayRunManifest:
    return PaperDayRunManifest.create(
        session_date=SESSION_DATE,
        account_id="paper-day-test",
        config={"scan_seconds": 300, "mode": "PAPER"} if config is None else config,
        created_at=created_at,
        target_hash=TARGET_HASH,
        initial_cash=Decimal("200000"),
    )


def _event(
    manifest: PaperDayRunManifest,
    *,
    key: str = "day-started",
    event_type: str = "DAY_STARTED",
    known_at: datetime = T0,
    occurred_at: datetime | None = None,
    payload: dict[str, object] | None = None,
    notification_required: bool = True,
) -> NewPaperDayEvent:
    return NewPaperDayEvent(
        run_id=manifest.run_id,
        event_key=key,
        event_type=event_type,
        phase=PaperDayPhase.BOOTSTRAP,
        severity=PaperDaySeverity.INFO,
        occurred_at=known_at if occurred_at is None else occurred_at,
        known_at=known_at,
        notification_required=notification_required,
        payload={"cash": Decimal("200000")} if payload is None else payload,
        correlation_id="session-start",
    )


def _open_owned_store(
    path: Path,
) -> tuple[SQLitePaperDayStore, PaperDayRunManifest]:
    store = SQLitePaperDayStore(path)
    manifest = _manifest()
    assert store.create_run(manifest) is True
    store.acquire_lease(
        run_id=manifest.run_id,
        owner_id="writer-a",
        now=T0,
        lease_duration=timedelta(minutes=10),
    )
    return store, manifest


def test_values_are_deterministic_canonical_and_deeply_immutable() -> None:
    mutable_config: dict[str, object] = {"z": [2, 1], "cash": Decimal("200000.00")}
    first = _manifest(config=mutable_config)
    second = _manifest(config={"cash": Decimal("200000.00"), "z": [2, 1]})
    assert first == second
    assert first.run_id == second.run_id
    assert first.config_sha256 == paper_day_document_sha256(mutable_config)
    assert "not-retained" not in repr(first)

    mutable_payload: dict[str, object] = {"watchlist": ["600000.SH"]}
    event = _event(first, payload=mutable_payload)
    mutable_payload["watchlist"] = ["000001.SZ"]
    assert event.payload == {"watchlist": ["600000.SH"]}
    assert event.event_id == paper_day_event_id(
        run_id=first.run_id, event_key="day-started"
    )
    with pytest.raises(FrozenInstanceError):
        first.account_id = "changed"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        event.event_type = "CHANGED"  # type: ignore[misc]
    with pytest.raises(ValueError, match="timezone-aware"):
        _manifest(created_at=datetime(2026, 8, 14))


def test_manifest_and_event_replay_are_idempotent_and_collisions_fail(
    tmp_path: Path,
) -> None:
    store, manifest = _open_owned_store(tmp_path / "paper-day.sqlite3")
    try:
        assert store.create_run(manifest) is False
        with pytest.raises(PaperDayStoreConflictError):
            store.create_run(
                replace(
                    manifest,
                    target_hash=paper_day_target_hash(
                        channel="onebot",
                        target_kind="private",
                        target_id="different",
                    ),
                )
            )

        command = _event(manifest)
        first, applied = store.append_event(
            command, owner_id="writer-a", lease_checked_at=T0
        )
        replay, replay_applied = store.append_event(
            command, owner_id="writer-a", lease_checked_at=T0
        )
        assert applied is True
        assert replay_applied is False
        assert replay == first
        assert store.event_by_key(manifest.run_id, command.event_key) == first
        assert store.get_by_event_id(first.event_id) == first

        with pytest.raises(PaperDayStoreConflictError):
            store.append_event(
                _event(manifest, payload={"cash": "changed"}),
                owner_id="writer-a",
                lease_checked_at=T0,
            )
    finally:
        store.close()


def test_hash_chain_append_only_triggers_and_integrity_detection(tmp_path: Path) -> None:
    store, manifest = _open_owned_store(tmp_path / "paper-day.sqlite3")
    try:
        first, _ = store.append_event(
            _event(manifest), owner_id="writer-a", lease_checked_at=T0
        )
        second, _ = store.append_event(
            _event(
                manifest,
                key="preflight-complete",
                event_type="PREFLIGHT_COMPLETED",
                known_at=T0 + timedelta(minutes=1),
                notification_required=False,
            ),
            owner_id="writer-a",
            lease_checked_at=T0 + timedelta(minutes=1),
        )
        assert first.previous_hash is None
        assert second.previous_hash == first.event_hash
        assert first.event_hash != second.event_hash
        assert store.events(manifest.run_id) == (first, second)

        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            store._connection.execute(  # noqa: SLF001 - exercise database trigger
                "UPDATE paper_day_events SET event_type = 'CHANGED'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            store._connection.execute(  # noqa: SLF001 - exercise database trigger
                "DELETE FROM paper_day_runs"
            )

        store._connection.execute(  # noqa: SLF001 - simulate out-of-band corruption
            "DROP TRIGGER paper_day_events_no_update"
        )
        store._connection.execute(  # noqa: SLF001 - simulate out-of-band corruption
            "UPDATE paper_day_events SET payload_json = '{\"tampered\":true}' WHERE sequence = 1"
        )
        with pytest.raises(PaperDayStoreIntegrityError):
            store.events(manifest.run_id)
    finally:
        store.close()


def test_single_writer_lease_acquire_renew_takeover_and_release(tmp_path: Path) -> None:
    store = SQLitePaperDayStore(tmp_path / "paper-day.sqlite3")
    manifest = _manifest()
    store.create_run(manifest)
    try:
        store.acquire_lease(
            run_id=manifest.run_id,
            owner_id="writer-a",
            now=T0,
            lease_duration=timedelta(minutes=2),
        )
        with pytest.raises(PaperDayStoreLeaseError):
            store.acquire_lease(
                run_id=manifest.run_id,
                owner_id="writer-b",
                now=T0 + timedelta(minutes=1),
                lease_duration=timedelta(minutes=2),
            )
        store.renew_lease(
            run_id=manifest.run_id,
            owner_id="writer-a",
            now=T0 + timedelta(minutes=1),
            lease_for=timedelta(minutes=3),
        )
        with pytest.raises(PaperDayStoreLeaseError):
            store.release_lease(run_id=manifest.run_id, owner_id="writer-b")

        store.acquire_lease(
            run_id=manifest.run_id,
            owner_id="writer-b",
            now=T0 + timedelta(minutes=5),
            lease_for=timedelta(minutes=2),
        )
        with pytest.raises(PaperDayStoreLeaseError):
            store.append_event(
                _event(manifest, known_at=T0 + timedelta(minutes=5)),
                owner_id="writer-a",
                lease_checked_at=T0 + timedelta(minutes=5),
            )
        store.append_event(
            _event(manifest, known_at=T0 + timedelta(minutes=5)),
            owner_id="writer-b",
            lease_checked_at=T0 + timedelta(minutes=5),
        )
        assert store.release_lease(run_id=manifest.run_id, owner_id="writer-b") is True
        assert store.release_lease(run_id=manifest.run_id, owner_id="writer-b") is False
    finally:
        store.close()


def test_point_in_time_reconstruction_uses_known_at_not_occurred_at(
    tmp_path: Path,
) -> None:
    store, manifest = _open_owned_store(tmp_path / "paper-day.sqlite3")
    try:
        first, _ = store.append_event(
            _event(manifest), owner_id="writer-a", lease_checked_at=T0
        )
        late, _ = store.append_event(
            _event(
                manifest,
                key="late-source-fact",
                event_type="SOURCE_FACT_OBSERVED",
                occurred_at=T0 - timedelta(minutes=20),
                known_at=T0 + timedelta(minutes=5),
            ),
            owner_id="writer-a",
            lease_checked_at=T0 + timedelta(minutes=5),
        )
        before_late = store.reconstruct(
            manifest.run_id, as_of=T0 + timedelta(minutes=4)
        )
        assert before_late is not None
        assert before_late.events == (first,)
        assert before_late.notification_events == (first,)
        after_late = store.reconstruct(
            manifest.run_id, as_of=T0 + timedelta(minutes=5)
        )
        assert after_late is not None
        assert after_late.events == (first, late)
        assert store.reconstruct(manifest.run_id, as_of=T0 - timedelta(seconds=1)) is None

        with pytest.raises(PaperDayStoreConflictError, match="retroactively"):
            store.append_event(
                _event(
                    manifest,
                    key="retroactive-insert",
                    known_at=T0 + timedelta(minutes=3),
                ),
                owner_id="writer-a",
                lease_checked_at=T0 + timedelta(minutes=6),
            )
    finally:
        store.close()
