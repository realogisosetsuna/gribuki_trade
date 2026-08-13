from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from gribuki_trade.domain.events import NormalizedEvent, RawDocument, SourceTier
from gribuki_trade.ingest.http import SourceRateLimited
from gribuki_trade.ports.news import FetchCursor, NewsBatch
from gribuki_trade.services.news_collection import (
    NewsCollectionService,
    SourceCollectionObservation,
    SourceRunStatus,
)
from gribuki_trade.storage import FileRawDocumentStore, SQLiteEventStore

NOW = datetime(2026, 8, 13, 3, 0, tzinfo=UTC)


def document(*, body: bytes = b"body") -> RawDocument:
    return RawDocument(
        source_id="source.one",
        canonical_url="https://news.example.test/list",
        content_type="text/html",
        content=body,
        first_seen_at=NOW,
        retrieved_at=NOW,
        available_at=NOW,
    )


def event(*, summary: str = "first") -> NormalizedEvent:
    return NormalizedEvent(
        source_id="source.one",
        canonical_url="https://news.example.test/a/1",
        title="Evidence title",
        summary=summary,
        event_type="news",
        source_tier=SourceTier.PUBLIC_MEDIA,
        first_seen_at=NOW,
        retrieved_at=NOW,
        available_at=NOW,
        external_id="item-1",
        raw_document_id=document().document_id,
    )


class SequenceSource:
    def __init__(self, batches: list[NewsBatch]) -> None:
        self.batches = batches
        self.cursors: list[FetchCursor | None] = []

    async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch:
        self.cursors.append(cursor)
        return self.batches.pop(0)


class RateLimitedSource:
    async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch:
        del cursor
        failed = FetchCursor(
            consecutive_failures=1,
            next_allowed_at=NOW + timedelta(minutes=1),
        )
        raise SourceRateLimited("rate limited", cursor=failed, status_code=429)


def test_collection_archives_raw_data_events_revisions_and_cursor(tmp_path) -> None:
    first_cursor = FetchCursor(content_sha256="a" * 64, first_seen_at=NOW)
    second_cursor = FetchCursor(content_sha256="b" * 64, first_seen_at=NOW)
    source = SequenceSource(
        [
            NewsBatch(
                source_id="source.one",
                cursor=first_cursor,
                documents=(document(),),
                events=(event(),),
            ),
            NewsBatch(
                source_id="source.one",
                cursor=second_cursor,
                documents=(document(body=b"changed"),),
                events=(event(summary="corrected"),),
            ),
        ]
    )
    with SQLiteEventStore(tmp_path / "events.sqlite3") as state:
        service = NewsCollectionService(
            {"source.one": source},
            raw_store=FileRawDocumentStore(tmp_path / "raw"),
            event_store=state,
            clock=lambda: NOW,
        )

        first = asyncio.run(service.run_once())[0]
        second = asyncio.run(service.run_once())[0]

        assert first.status is SourceRunStatus.SUCCESS
        assert first.documents_saved == 1 and first.events_new == 1
        assert second.documents_saved == 1 and second.events_revised == 1
        latest = state.latest()
        assert len(latest) == 1
        assert latest[0].summary == "corrected"
        assert latest[0].revision_number == 2
        assert latest[0].supersedes_revision_id is not None
        assert state.get_cursor("source.one") == second_cursor
        assert source.cursors == [None, first_cursor]


def test_rate_limit_cursor_is_persisted_and_peer_failure_is_isolated(tmp_path) -> None:
    good = SequenceSource(
        [
            NewsBatch(
                source_id="source.one",
                cursor=FetchCursor(content_sha256="a" * 64),
                events=(event(),),
            )
        ]
    )
    with SQLiteEventStore(tmp_path / "events.sqlite3") as state:
        service = NewsCollectionService(
            {"source.one": good, "source.two": RateLimitedSource()},
            raw_store=FileRawDocumentStore(tmp_path / "raw"),
            event_store=state,
            clock=lambda: NOW,
        )

        results = asyncio.run(service.run_once())

        assert [item.status for item in results] == [
            SourceRunStatus.SUCCESS,
            SourceRunStatus.BACKOFF,
        ]
        assert results[1].error_code == "SourceRateLimited"
        assert state.get_cursor("source.two") is not None


def test_mismatched_source_identity_fails_without_persistence(tmp_path) -> None:
    bad = SequenceSource(
        [NewsBatch(source_id="wrong.source", cursor=FetchCursor())]
    )
    with SQLiteEventStore(tmp_path / "events.sqlite3") as state:
        service = NewsCollectionService(
            {"source.one": bad},
            raw_store=FileRawDocumentStore(tmp_path / "raw"),
            event_store=state,
            clock=lambda: NOW,
        )
        result = asyncio.run(service.run_once())[0]

        assert result.status is SourceRunStatus.FAILED
        assert result.error_code == "source_identity_mismatch"
        assert state.get_cursor("source.one") is None


def test_collection_observer_gets_sanitized_timing_and_cannot_break_run(
    tmp_path,
) -> None:
    source = SequenceSource(
        [
            NewsBatch(
                source_id="source.one",
                cursor=FetchCursor(),
                events=(event(),),
            ),
            NewsBatch(
                source_id="source.one",
                cursor=FetchCursor(),
                events=(event(),),
            ),
        ]
    )
    ticks = iter((10.0, 10.25, 20.0, 20.5))
    observations: list[SourceCollectionObservation] = []

    def observer(item: SourceCollectionObservation) -> None:
        observations.append(item)
        if len(observations) == 2:
            raise RuntimeError("health database secret URL must not affect collection")

    with SQLiteEventStore(tmp_path / "events.sqlite3") as state:
        service = NewsCollectionService(
            {"source.one": source},
            raw_store=FileRawDocumentStore(tmp_path / "raw"),
            event_store=state,
            clock=lambda: NOW,
            monotonic_clock=lambda: next(ticks),
            observer=observer,
        )

        first = asyncio.run(service.run_once())[0]
        second = asyncio.run(service.run_once())[0]

    assert first.status is SourceRunStatus.SUCCESS
    assert second.status is SourceRunStatus.SUCCESS
    assert [item.latency_ms for item in observations] == [250.0, 500.0]
    assert all(item.started_at == NOW for item in observations)
