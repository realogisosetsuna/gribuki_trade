"""Durable, source-isolated orchestration for public news collection."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from time import monotonic

from gribuki_trade.ingest.http import SourceFetchError
from gribuki_trade.pipeline.dedupe import EventDisposition
from gribuki_trade.ports.news import NewsSource
from gribuki_trade.storage.event_store import SQLiteEventStore
from gribuki_trade.storage.raw_store import FileRawDocumentStore


class SourceRunStatus(StrEnum):
    SUCCESS = "SUCCESS"
    NOT_MODIFIED = "NOT_MODIFIED"
    BACKOFF = "BACKOFF"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class SourceCollectionResult:
    source_id: str
    status: SourceRunStatus
    documents_saved: int = 0
    events_new: int = 0
    events_revised: int = 0
    events_duplicate: int = 0
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class SourceCollectionObservation:
    """Sanitized timing envelope for optional persistent health telemetry."""

    result: SourceCollectionResult
    started_at: datetime
    finished_at: datetime
    latency_ms: float


class NewsCollectionService:
    """Collect configured sources without allowing one failure to stop peers."""

    def __init__(
        self,
        sources: Mapping[str, NewsSource],
        *,
        raw_store: FileRawDocumentStore,
        event_store: SQLiteEventStore,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic_clock: Callable[[], float] = monotonic,
        observer: Callable[[SourceCollectionObservation], None] | None = None,
        max_concurrency: int = 4,
    ) -> None:
        if not sources:
            raise ValueError("at least one news source is required")
        if any(not source_id.strip() for source_id in sources):
            raise ValueError("source IDs must not be empty")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        self._sources = dict(sources)
        self._raw_store = raw_store
        self._event_store = event_store
        self._clock = clock
        self._monotonic_clock = monotonic_clock
        self._observer = observer
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def run_once(
        self,
        source_ids: Sequence[str] | None = None,
    ) -> tuple[SourceCollectionResult, ...]:
        selected = tuple(source_ids) if source_ids is not None else tuple(self._sources)
        unknown = set(selected) - self._sources.keys()
        if unknown:
            raise ValueError(f"unknown news source IDs: {sorted(unknown)}")
        return tuple(
            await asyncio.gather(*(self._collect_one(source_id) for source_id in selected))
        )

    async def _collect_one(self, source_id: str) -> SourceCollectionResult:
        started_at = self._now()
        started_tick = self._monotonic_clock()
        result = await self._collect_one_impl(source_id)
        finished_at = self._now()
        latency_ms = max(0.0, (self._monotonic_clock() - started_tick) * 1_000)
        if self._observer is not None:
            # Telemetry storage is deliberately best effort. A full or damaged
            # health database must not turn a successfully archived document
            # into a failed source run, and observer exceptions are never kept.
            with suppress(Exception):
                self._observer(
                    SourceCollectionObservation(
                        result=result,
                        started_at=started_at,
                        finished_at=finished_at,
                        latency_ms=latency_ms,
                    )
                )
        return result

    async def _collect_one_impl(self, source_id: str) -> SourceCollectionResult:
        source = self._sources[source_id]
        cursor = self._event_store.get_cursor(source_id)
        async with self._semaphore:
            try:
                batch = await source.collect(cursor)
            except SourceFetchError as error:
                now = self._now()
                self._event_store.put_cursor(source_id, error.cursor, updated_at=now)
                return SourceCollectionResult(
                    source_id=source_id,
                    status=(
                        SourceRunStatus.BACKOFF
                        if error.cursor.next_allowed_at is not None
                        else SourceRunStatus.FAILED
                    ),
                    error_code=type(error).__name__,
                )
            except Exception:
                # Provider exception text can contain URLs, headers, or source
                # content. Persist only a stable class-independent code.
                return SourceCollectionResult(
                    source_id=source_id,
                    status=SourceRunStatus.FAILED,
                    error_code="unexpected_source_error",
                )

        if batch.source_id != source_id:
            return SourceCollectionResult(
                source_id=source_id,
                status=SourceRunStatus.FAILED,
                error_code="source_identity_mismatch",
            )

        saved = sum(
            self._raw_store.save(document).created for document in batch.documents
        )
        dispositions = [self._event_store.append(event).disposition for event in batch.events]
        self._event_store.put_cursor(source_id, batch.cursor, updated_at=self._now())
        status = (
            SourceRunStatus.NOT_MODIFIED
            if batch.not_modified
            else SourceRunStatus.SUCCESS
        )
        return SourceCollectionResult(
            source_id=source_id,
            status=status,
            documents_saved=saved,
            events_new=dispositions.count(EventDisposition.NEW),
            events_revised=dispositions.count(EventDisposition.REVISION),
            events_duplicate=dispositions.count(EventDisposition.DUPLICATE),
        )

    async def run_forever(
        self,
        *,
        interval_seconds: float,
        stop_event: asyncio.Event,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        while not stop_event.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except TimeoutError:
                continue

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value.astimezone(UTC)
