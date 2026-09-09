from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from gribuki_trade.domain.events import NormalizedEvent, RawDocument, SourceTier
from gribuki_trade.ingest.http import SourceRateLimited, SourceUnavailable
from gribuki_trade.ports.news import FetchCursor, NewsBatch
from gribuki_trade.services.communications.news_collection import (
    NewsCollectionService,
    NewsSourceResiliencePolicy,
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


class UnavailableWithoutRetryAfterSource:
    async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch:
        del cursor
        raise SourceUnavailable(
            "temporary provider failure",
            cursor=FetchCursor(consecutive_failures=1),
        )


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


def test_fetch_failure_without_retry_after_reports_persisted_backoff(tmp_path) -> None:
    with SQLiteEventStore(tmp_path / "events.sqlite3") as state:
        service = NewsCollectionService(
            {"source.one": UnavailableWithoutRetryAfterSource()},
            raw_store=FileRawDocumentStore(tmp_path / "raw"),
            event_store=state,
            clock=lambda: NOW,
        )

        result = asyncio.run(service.run_once())[0]

        assert result.status is SourceRunStatus.BACKOFF
        assert result.error_code == "SourceUnavailable"
        assert result.next_allowed_at is not None
        cursor = state.get_cursor("source.one")
        assert cursor is not None
        assert cursor.next_allowed_at == result.next_allowed_at


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

        assert result.status is SourceRunStatus.BACKOFF
        assert result.error_code == "source_identity_mismatch"
        cursor = state.get_cursor("source.one")
        assert cursor is not None
        assert cursor.consecutive_failures == 1
        assert cursor.next_allowed_at is not None


def test_persisted_circuit_skips_provider_then_allows_one_probe(tmp_path) -> None:
    class CountingSource:
        calls = 0

        async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch:
            self.calls += 1
            return NewsBatch(source_id="source.one", cursor=FetchCursor())

    current = NOW
    source = CountingSource()
    with SQLiteEventStore(tmp_path / "events.sqlite3") as state:
        state.put_cursor(
            "source.one",
            FetchCursor(
                consecutive_failures=3,
                next_allowed_at=NOW + timedelta(minutes=1),
            ),
            updated_at=NOW,
        )
        service = NewsCollectionService(
            {"source.one": source},
            raw_store=FileRawDocumentStore(tmp_path / "raw"),
            event_store=state,
            clock=lambda: current,
        )

        blocked = asyncio.run(service.run_once())[0]
        assert blocked.status is SourceRunStatus.BACKOFF
        assert blocked.error_code == "source_circuit_open"
        assert blocked.next_allowed_at == NOW + timedelta(minutes=1)
        assert source.calls == 0

        current = NOW + timedelta(minutes=1)
        recovered = asyncio.run(service.run_once())[0]
        assert recovered.status is SourceRunStatus.SUCCESS
        assert source.calls == 1
        assert state.get_cursor("source.one") == FetchCursor()


def test_probe_lease_is_atomic_across_connections_and_fences_stale_owner(
    tmp_path,
) -> None:
    path = tmp_path / "events.sqlite3"
    with SQLiteEventStore(path) as first, SQLiteEventStore(path) as second:
        first_claim = first.claim_source_probe(
            "source.one",
            owner_token="owner-a",
            now=NOW,
            lease_until=NOW + timedelta(minutes=1),
        )
        blocked = second.claim_source_probe(
            "source.one",
            owner_token="owner-b",
            now=NOW,
            lease_until=NOW + timedelta(minutes=1),
        )

        assert first_claim.acquired is True
        assert blocked.acquired is False
        assert blocked.blocking_reason == "source_probe_in_progress"
        assert blocked.blocked_until == NOW + timedelta(minutes=1)

        replacement = second.claim_source_probe(
            "source.one",
            owner_token="owner-b",
            now=NOW + timedelta(minutes=1),
            lease_until=NOW + timedelta(minutes=2),
        )
        assert replacement.acquired is True
        assert (
            first.complete_source_probe(
                "source.one",
                owner_token="owner-a",
                cursor=FetchCursor(content_sha256="a" * 64),
                updated_at=NOW + timedelta(minutes=1),
            )
            is False
        )
        assert second.complete_source_probe(
            "source.one",
            owner_token="owner-b",
            cursor=FetchCursor(content_sha256="b" * 64),
            updated_at=NOW + timedelta(minutes=1),
        )
        assert second.get_cursor("source.one") == FetchCursor(
            content_sha256="b" * 64
        )


def test_two_services_share_one_cross_process_probe_lease(tmp_path) -> None:
    calls = 0

    async def scenario(first_state, second_state) -> tuple[object, object]:
        started = asyncio.Event()
        release = asyncio.Event()

        class BlockingSource:
            async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch:
                nonlocal calls
                del cursor
                calls += 1
                started.set()
                await release.wait()
                return NewsBatch(source_id="source.one", cursor=FetchCursor())

        source = BlockingSource()
        first_service = NewsCollectionService(
            {"source.one": source},
            raw_store=FileRawDocumentStore(tmp_path / "raw-a"),
            event_store=first_state,
            clock=lambda: NOW,
        )
        second_service = NewsCollectionService(
            {"source.one": source},
            raw_store=FileRawDocumentStore(tmp_path / "raw-b"),
            event_store=second_state,
            clock=lambda: NOW,
        )
        first_task = asyncio.create_task(first_service.run_once())
        await started.wait()
        blocked = (await second_service.run_once())[0]
        release.set()
        completed = (await first_task)[0]
        return completed, blocked

    path = tmp_path / "events.sqlite3"
    with SQLiteEventStore(path) as first, SQLiteEventStore(path) as second:
        completed, blocked = asyncio.run(scenario(first, second))

    assert completed.status is SourceRunStatus.SUCCESS
    assert blocked.status is SourceRunStatus.BACKOFF
    assert blocked.error_code == "source_probe_in_progress"
    assert calls == 1


def test_unexpected_provider_failure_opens_restart_safe_circuit(tmp_path) -> None:
    class BrokenSource:
        calls = 0

        async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch:
            del cursor
            self.calls += 1
            raise RuntimeError("provider URL and response body must not escape")

    source = BrokenSource()
    with SQLiteEventStore(tmp_path / "events.sqlite3") as state:
        service = NewsCollectionService(
            {"source.one": source},
            raw_store=FileRawDocumentStore(tmp_path / "raw"),
            event_store=state,
            clock=lambda: NOW,
            resilience_policy=NewsSourceResiliencePolicy(
                unexpected_backoff_base=timedelta(seconds=20),
                unexpected_backoff_cap=timedelta(minutes=1),
            ),
        )

        failed = asyncio.run(service.run_once())[0]
        skipped = asyncio.run(service.run_once())[0]

        assert failed.status is SourceRunStatus.BACKOFF
        assert failed.error_code == "unexpected_source_error"
        assert failed.next_allowed_at is not None
        assert skipped.error_code == "source_circuit_open"
        assert source.calls == 1
        cursor = state.get_cursor("source.one")
        assert cursor is not None and cursor.consecutive_failures == 1


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


def test_single_concurrency_collects_four_sources_strictly_serially(tmp_path) -> None:
    active = 0
    maximum_active = 0

    class TrackedSource:
        def __init__(self, source_id: str) -> None:
            self.source_id = source_id

        async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch:
            nonlocal active, maximum_active
            del cursor
            active += 1
            maximum_active = max(maximum_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return NewsBatch(source_id=self.source_id, cursor=FetchCursor())

    sources = {
        f"source.{index}": TrackedSource(f"source.{index}") for index in range(4)
    }
    with SQLiteEventStore(tmp_path / "events.sqlite3") as state:
        service = NewsCollectionService(
            sources,
            raw_store=FileRawDocumentStore(tmp_path / "raw"),
            event_store=state,
            clock=lambda: NOW,
            max_concurrency=1,
        )
        results = asyncio.run(service.run_once())

    assert len(results) == 4
    assert all(item.status is SourceRunStatus.SUCCESS for item in results)
    assert maximum_active == 1


def test_raw_archive_failure_is_source_isolated_and_opens_circuit(
    tmp_path,
    monkeypatch,
) -> None:
    broken = SequenceSource(
        [
            NewsBatch(
                source_id="source.one",
                cursor=FetchCursor(content_sha256="a" * 64),
                documents=(document(),),
            )
        ]
    )
    healthy = SequenceSource(
        [NewsBatch(source_id="source.two", cursor=FetchCursor())]
    )
    raw_store = FileRawDocumentStore(tmp_path / "raw")

    def fail_save(_document: RawDocument):
        raise OSError("simulated archive failure")

    monkeypatch.setattr(raw_store, "save", fail_save)
    with SQLiteEventStore(tmp_path / "events.sqlite3") as state:
        service = NewsCollectionService(
            {"source.one": broken, "source.two": healthy},
            raw_store=raw_store,
            event_store=state,
            clock=lambda: NOW,
        )

        results = asyncio.run(service.run_once())

        assert [item.status for item in results] == [
            SourceRunStatus.BACKOFF,
            SourceRunStatus.SUCCESS,
        ]
        assert results[0].error_code == "source_raw_archive_write_failed"
        cursor = state.get_cursor("source.one")
        assert cursor is not None and cursor.next_allowed_at is not None


def test_event_store_failure_is_source_isolated(tmp_path, monkeypatch) -> None:
    source = SequenceSource(
        [
            NewsBatch(
                source_id="source.one",
                cursor=FetchCursor(),
                events=(event(),),
            )
        ]
    )
    peer = SequenceSource(
        [NewsBatch(source_id="source.two", cursor=FetchCursor())]
    )
    with SQLiteEventStore(tmp_path / "events.sqlite3") as state:
        original_append = state.append

        def selective_append(item: NormalizedEvent):
            if item.source_id == "source.one":
                raise OSError("simulated event-store failure")
            return original_append(item)

        monkeypatch.setattr(state, "append", selective_append)
        service = NewsCollectionService(
            {"source.one": source, "source.two": peer},
            raw_store=FileRawDocumentStore(tmp_path / "raw"),
            event_store=state,
            clock=lambda: NOW,
        )

        results = asyncio.run(service.run_once())

        assert results[0].status is SourceRunStatus.BACKOFF
        assert results[0].error_code == "source_event_store_write_failed"
        assert results[1].status is SourceRunStatus.SUCCESS
