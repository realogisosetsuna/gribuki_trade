from dataclasses import replace
from datetime import UTC, datetime, timedelta

from gribuki_trade.domain.events import NormalizedEvent, SourceTier
from gribuki_trade.pipeline.dedupe import EventDeduplicator, EventDisposition

NOW = datetime(2026, 8, 13, 3, 0, tzinfo=UTC)


def event(*, summary: str, seen: datetime = NOW) -> NormalizedEvent:
    return NormalizedEvent(
        source_id="exchange",
        canonical_url="https://exchange.example/announcement/1",
        title="Announcement",
        summary=summary,
        event_type="announcement",
        source_tier=SourceTier.OFFICIAL,
        published_at=NOW - timedelta(minutes=5),
        first_seen_at=seen,
        retrieved_at=seen,
        available_at=seen,
    )


def test_exact_repeat_is_duplicate_and_changed_content_is_append_only_revision() -> None:
    deduplicator = EventDeduplicator()
    first = deduplicator.ingest(event(summary="version one"))
    repeated = deduplicator.ingest(
        replace(event(summary="version one", seen=NOW + timedelta(minutes=1)))
    )
    revised = deduplicator.ingest(
        event(summary="corrected version", seen=NOW + timedelta(minutes=2))
    )

    assert first.disposition is EventDisposition.NEW and first.accepted
    assert repeated.disposition is EventDisposition.DUPLICATE and not repeated.accepted
    assert repeated.event.revision_id == first.event.revision_id
    assert revised.disposition is EventDisposition.REVISION and revised.accepted
    assert revised.event.revision_number == 2
    assert revised.event.supersedes_revision_id == first.event.revision_id
    assert revised.event.event_id == first.event.event_id
    assert deduplicator.revisions(first.event.event_id) == (first.event, revised.event)


def test_different_canonical_urls_remain_independent_evidence() -> None:
    deduplicator = EventDeduplicator()
    first = deduplicator.ingest(event(summary="same"))
    second = deduplicator.ingest(
        replace(
            event(summary="same"),
            canonical_url="https://exchange.example/announcement/2",
            event_id="",
            revision_id="",
        )
    )

    assert first.event.event_id != second.event.event_id
    assert second.disposition is EventDisposition.NEW
