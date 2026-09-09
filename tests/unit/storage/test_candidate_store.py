from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from gribuki_trade.domain.candidates import (
    CandidateControlAction,
    CandidateControlEvent,
    CandidateObservation,
    CandidatePriority,
    CandidateSource,
    CandidateStatus,
    canonical_ashare_symbol,
)
from gribuki_trade.storage.research.candidate_store import (
    CandidateEventCollisionError,
    CandidateNotFoundError,
    SQLiteCandidateStore,
)

NOW = datetime(2026, 8, 14, 1, 0, tzinfo=UTC)


def observation(
    *,
    source: CandidateSource = CandidateSource.CLOSE_SCREEN,
    run_id: str = "screen-2026-08-14",
    observed_at: datetime = NOW,
    expires_at: datetime | None = NOW + timedelta(days=1),
    priority: CandidatePriority = CandidatePriority.NORMAL,
    reasons: tuple[str, ...] = ("CLOSE_SCREEN_TOP_N",),
    evidence: tuple[str, ...] = ("evidence-1",),
) -> CandidateObservation:
    return CandidateObservation(
        symbol="600000",
        source=source,
        source_run_id=run_id,
        discovered_at=observed_at - timedelta(minutes=1),
        observed_at=observed_at,
        expires_at=expires_at,
        priority=priority,
        reason_codes=reasons,
        evidence_ids=evidence,
    )


def test_symbol_normalization_includes_bse_and_rejects_ambiguous_values() -> None:
    assert canonical_ashare_symbol("600000") == "600000.SH"
    assert canonical_ashare_symbol("000001.sz") == "000001.SZ"
    assert canonical_ashare_symbol("430047") == "430047.BJ"
    with pytest.raises(ValueError, match="symbol must look like"):
        canonical_ashare_symbol("AAPL")


def test_observation_is_idempotent_and_identity_collision_is_rejected(tmp_path) -> None:
    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        item = observation()
        assert store.append_observation(item) is True
        assert store.append_observation(item) is False
        with pytest.raises(CandidateEventCollisionError):
            store.append_observation(replace(item, priority=CandidatePriority.HIGH))


def test_different_sources_merge_without_losing_provenance(tmp_path) -> None:
    close = observation()
    intraday = observation(
        source=CandidateSource.INTRADAY_ANOMALY,
        run_id="intraday-2026-08-14T0200",
        observed_at=NOW + timedelta(hours=1),
        expires_at=NOW + timedelta(days=2),
        priority=CandidatePriority.HIGH,
        reasons=("VOLUME_PRICE_ANOMALY",),
        evidence=("evidence-2",),
    )
    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        store.append_observation(close)
        store.append_observation(intraday)
        merged = store.get_candidate("600000.SH", as_of=NOW + timedelta(hours=2))

    assert merged is not None
    assert merged.status is CandidateStatus.ACTIVE
    assert merged.priority is CandidatePriority.HIGH
    assert merged.discovered_at == close.discovered_at
    assert merged.first_observed_at == close.observed_at
    assert merged.last_observed_at == intraday.observed_at
    assert merged.expires_at == intraday.expires_at
    assert merged.sources == (
        CandidateSource.CLOSE_SCREEN,
        CandidateSource.INTRADAY_ANOMALY,
    )
    assert len(merged.provenance) == 2
    assert merged.reason_codes == ("CLOSE_SCREEN_TOP_N", "VOLUME_PRICE_ANOMALY")
    assert merged.evidence_ids == ("evidence-1", "evidence-2")


def test_as_of_projection_does_not_see_later_observations(tmp_path) -> None:
    first = observation()
    later = observation(
        source=CandidateSource.REVIEW,
        run_id="review-1",
        observed_at=NOW + timedelta(hours=2),
        expires_at=NOW + timedelta(days=3),
        reasons=("HUMAN_REVIEW",),
    )
    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        store.append_observation(first)
        store.append_observation(later)
        assert store.get_candidate(
            "600000.SH", as_of=NOW - timedelta(microseconds=1)
        ) is None
        historical = store.get_candidate("600000.SH", as_of=NOW + timedelta(hours=1))
        current = store.get_candidate("600000.SH", as_of=NOW + timedelta(hours=3))

    assert historical is not None and len(historical.provenance) == 1
    assert historical.last_observed_at == first.observed_at
    assert current is not None and len(current.provenance) == 2


def test_expiration_boundary_and_status_filter_are_point_in_time(tmp_path) -> None:
    item = observation(expires_at=NOW + timedelta(hours=2))
    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        store.append_observation(item)
        before = store.get_candidate("600000", as_of=NOW + timedelta(hours=1))
        expired = store.get_candidate("600000", as_of=NOW + timedelta(hours=2))
        active = store.list_candidates(
            as_of=NOW + timedelta(hours=2),
            statuses=frozenset({CandidateStatus.ACTIVE}),
        )

    assert before is not None and before.status is CandidateStatus.ACTIVE
    assert expired is not None and expired.status is CandidateStatus.EXPIRED
    assert active == ()


def test_control_requires_a_visible_parent_and_audit_is_append_only(tmp_path) -> None:
    cooling = CandidateControlEvent(
        symbol="600000.SH",
        action=CandidateControlAction.COOL,
        operation_id="cool-1",
        occurred_at=NOW + timedelta(hours=1),
        cooling_until=NOW + timedelta(hours=3),
        reason_code="SIGNAL_COOLDOWN",
    )
    with SQLiteCandidateStore(tmp_path / "candidates.sqlite3") as store:
        with pytest.raises(CandidateNotFoundError):
            store.append_control(cooling)
        store.append_observation(observation(expires_at=None))
        assert store.append_control(cooling) is True
        assert store.append_control(cooling) is False
        history = store.history("600000")

    assert [item.event_type for item in history] == ["observed", "cool"]
    assert history[0].sequence < history[1].sequence


def test_concurrent_exact_replay_commits_once_and_uses_wal(tmp_path) -> None:
    path = tmp_path / "candidates.sqlite3"
    item = observation()
    with SQLiteCandidateStore(path) as store:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = tuple(pool.map(store.append_observation, (item,) * 32))
        assert sum(results) == 1
        assert len(store.history("600000.SH")) == 1
        with sqlite3.connect(path) as connection:
            mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])

    assert mode.lower() == "wal"


def test_domain_rejects_unsafe_point_in_time_and_empty_lineage() -> None:
    with pytest.raises(ValueError, match="discovered_at must not follow"):
        replace(observation(), discovered_at=NOW + timedelta(seconds=1))
    with pytest.raises(ValueError, match="reason_codes must not be empty"):
        replace(observation(), reason_codes=())
    with pytest.raises(ValueError, match="timezone-aware"):
        replace(observation(), observed_at=datetime(2026, 8, 14, 1, 0))
