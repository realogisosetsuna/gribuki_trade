from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from gribuki_trade.domain.events import NormalizedEvent, SourceTier
from gribuki_trade.storage import SQLiteEventStore

NOW = datetime(2026, 8, 13, 3, 0, tzinfo=UTC)


def _event(
    identity: str,
    *,
    summary: str,
    visible_at: datetime,
) -> NormalizedEvent:
    return NormalizedEvent(
        source_id="official.test",
        canonical_url=f"https://official.example.test/events/{identity}",
        title=f"Event {identity}",
        summary=summary,
        event_type="macro",
        source_tier=SourceTier.OFFICIAL,
        first_seen_at=visible_at,
        retrieved_at=visible_at,
        available_at=visible_at,
        published_at=visible_at - timedelta(minutes=1),
        external_id=identity,
    )


def test_latest_as_of_returns_revision_visible_at_decision_time(tmp_path: Path) -> None:
    original = _event("policy", summary="initial release", visible_at=NOW)
    correction_time = NOW + timedelta(hours=2)
    correction = replace(
        original,
        summary="corrected release",
        first_seen_at=correction_time,
        retrieved_at=correction_time,
        available_at=correction_time,
        content_sha256="",
        revision_id="",
    )

    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        stored_original = store.append(original).event
        stored_correction = store.append(correction).event

        historical = store.latest_as_of(NOW + timedelta(hours=1))
        at_correction = store.latest_as_of(correction_time)

        assert store.latest() == (stored_correction,)

    assert historical == (stored_original,)
    assert at_correction == (stored_correction,)


def test_latest_as_of_excludes_events_not_yet_visible_and_applies_limit(
    tmp_path: Path,
) -> None:
    old = _event("old", summary="old", visible_at=NOW - timedelta(hours=1))
    recent = _event("recent", summary="recent", visible_at=NOW)
    future = _event("future", summary="future", visible_at=NOW + timedelta(seconds=1))

    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        store.append(old)
        stored_recent = store.append(recent).event
        store.append(future)

        assert store.latest_as_of(NOW, limit=1) == (stored_recent,)
        assert {item.summary for item in store.latest_as_of(NOW)} == {"old", "recent"}


def test_latest_as_of_normalizes_aware_offsets_before_querying(tmp_path: Path) -> None:
    visible_at = datetime(2026, 8, 13, 4, 0, tzinfo=UTC)
    event = _event("timezone", summary="visible", visible_at=visible_at)
    same_in_shanghai = datetime(
        2026,
        8,
        13,
        12,
        0,
        tzinfo=timezone(timedelta(hours=8)),
    )

    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        stored = store.append(event).event

        assert store.latest_as_of(same_in_shanghai) == (stored,)


def test_latest_as_of_validates_timestamp_and_limit(tmp_path: Path) -> None:
    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        with pytest.raises(ValueError, match="as_of must be timezone-aware"):
            store.latest_as_of(datetime(2026, 8, 13, 3, 0))
        with pytest.raises(ValueError, match="limit must be positive"):
            store.latest_as_of(NOW, limit=0)
