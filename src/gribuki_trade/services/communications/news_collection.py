"""公共新闻采集的持久化、逐来源隔离编排。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from time import monotonic
from uuid import uuid4

from gribuki_trade.ingest.http import SourceFetchError
from gribuki_trade.pipeline.dedupe import EventDisposition
from gribuki_trade.ports.news import FetchCursor, NewsSource
from gribuki_trade.storage.research.event_store import SQLiteEventStore
from gribuki_trade.storage.research.raw_store import FileRawDocumentStore


class SourceRunStatus(StrEnum):
    SUCCESS = "SUCCESS"
    NOT_MODIFIED = "NOT_MODIFIED"
    BACKOFF = "BACKOFF"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class NewsSourceResiliencePolicy:
    """HTTP 适配器之外异常的编排层冷却策略。

    HTTP 重试和 ``Retry-After`` 仍由各来源的 fetcher 处理。这里仅处理提供方
    未知异常、来源输出错误和本地持久化失败，避免轮询循环持续冲击已损坏的
    适配器。冷却游标写入 ``SQLiteEventStore``，因此正常情况下可跨重启保留。
    """

    unexpected_backoff_base: timedelta = timedelta(seconds=30)
    unexpected_backoff_cap: timedelta = timedelta(minutes=15)
    probe_lease_duration: timedelta = timedelta(minutes=5)

    def __post_init__(self) -> None:
        if self.unexpected_backoff_base <= timedelta(0):
            raise ValueError("unexpected_backoff_base must be positive")
        if self.unexpected_backoff_cap <= timedelta(0):
            raise ValueError("unexpected_backoff_cap must be positive")
        if self.unexpected_backoff_base > self.unexpected_backoff_cap:
            raise ValueError("unexpected_backoff_base must not exceed its cap")
        if self.probe_lease_duration <= timedelta(0):
            raise ValueError("probe_lease_duration must be positive")


@dataclass(frozen=True, slots=True)
class SourceCollectionResult:
    source_id: str
    status: SourceRunStatus
    documents_saved: int = 0
    events_new: int = 0
    events_revised: int = 0
    events_duplicate: int = 0
    error_code: str | None = None
    next_allowed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class SourceCollectionObservation:
    """供可选健康遥测使用的脱敏计时记录。"""

    result: SourceCollectionResult
    started_at: datetime
    finished_at: datetime
    latency_ms: float


class NewsCollectionService:
    """采集已配置来源，并保证一个来源失败不会终止其他来源。"""

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
        resilience_policy: NewsSourceResiliencePolicy | None = None,
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
        self._resilience_policy = resilience_policy or NewsSourceResiliencePolicy()

    async def run_once(
        self,
        source_ids: Sequence[str] | None = None,
    ) -> tuple[SourceCollectionResult, ...]:
        selected = tuple(source_ids) if source_ids is not None else tuple(self._sources)
        unknown = set(selected) - self._sources.keys()
        if unknown:
            raise ValueError(f"unknown news source IDs: {sorted(unknown)}")
        gathered = await asyncio.gather(
            *(self._collect_one(source_id) for source_id in selected),
            return_exceptions=True,
        )
        results: list[SourceCollectionResult] = []
        for source_id, item in zip(selected, gathered, strict=True):
            if isinstance(item, BaseException):
                # _collect_one 已经隔离所有普通异常；这里是最后一道防线，禁止
                # 某个异常实现绕过隔离并取消同一批次的其他来源。
                if isinstance(item, asyncio.CancelledError):
                    raise item
                results.append(
                    SourceCollectionResult(
                        source_id=source_id,
                        status=SourceRunStatus.FAILED,
                        error_code="source_collection_unhandled_error",
                    )
                )
            else:
                results.append(item)
        return tuple(results)

    async def _collect_one(self, source_id: str) -> SourceCollectionResult:
        started_at = self._now()
        started_tick = self._monotonic_clock()
        try:
            result = await self._collect_one_impl(source_id)
        except Exception:
            # 任何来源适配器、原文归档或事件存储的普通异常都只能影响本来源。
            # 不记录异常正文，避免 URL、响应正文或凭据进入日志。
            result = SourceCollectionResult(
                source_id=source_id,
                status=SourceRunStatus.FAILED,
                error_code="source_collection_unhandled_error",
            )
        finished_at = self._now()
        latency_ms = max(0.0, (self._monotonic_clock() - started_tick) * 1_000)
        if self._observer is not None:
            # 健康遥测为尽力写入。遥测库已满或损坏不能把成功归档改判为失败。
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
        lease_token = uuid4().hex
        async with self._semaphore:
            claimed_at = self._now()
            try:
                claim = self._event_store.claim_source_probe(
                    source_id,
                    owner_token=lease_token,
                    now=claimed_at,
                    lease_until=(
                        claimed_at + self._resilience_policy.probe_lease_duration
                    ),
                )
            except Exception:
                return SourceCollectionResult(
                    source_id=source_id,
                    status=SourceRunStatus.FAILED,
                    error_code="source_state_read_failed",
                )
            if not claim.acquired:
                return SourceCollectionResult(
                    source_id=source_id,
                    status=SourceRunStatus.BACKOFF,
                    error_code=claim.blocking_reason or "source_probe_in_progress",
                    next_allowed_at=claim.blocked_until,
                )
            cursor = claim.cursor
            try:
                batch = await source.collect(cursor)
            except SourceFetchError as error:
                now = self._now()
                failed_cursor = error.cursor
                if failed_cursor.next_allowed_at is None:
                    failed_cursor = self._unexpected_failure_cursor(
                        source_id,
                        failed_cursor,
                        now=now,
                    )
                if not self._safe_complete_probe(
                    source_id,
                    lease_token,
                    failed_cursor,
                    updated_at=now,
                ):
                    return SourceCollectionResult(
                        source_id=source_id,
                        status=SourceRunStatus.FAILED,
                        error_code="source_state_write_failed",
                        next_allowed_at=failed_cursor.next_allowed_at,
                    )
                return SourceCollectionResult(
                    source_id=source_id,
                    # 即使 provider 没给 Retry-After，本层也已生成并持久化
                    # next_allowed_at，因此状态必须如实报告为 BACKOFF。
                    status=SourceRunStatus.BACKOFF,
                    error_code=type(error).__name__,
                    next_allowed_at=failed_cursor.next_allowed_at,
                )
            except Exception:
                # 提供方异常正文可能含 URL、请求头或正文；只保留稳定错误码。
                now = self._now()
                failed_cursor = self._unexpected_failure_cursor(
                    source_id,
                    cursor,
                    now=now,
                )
                if not self._safe_complete_probe(
                    source_id,
                    lease_token,
                    failed_cursor,
                    updated_at=now,
                ):
                    return SourceCollectionResult(
                        source_id=source_id,
                        status=SourceRunStatus.FAILED,
                        error_code="source_state_write_failed",
                        next_allowed_at=failed_cursor.next_allowed_at,
                    )
                return SourceCollectionResult(
                    source_id=source_id,
                    status=SourceRunStatus.BACKOFF,
                    error_code="unexpected_source_error",
                    next_allowed_at=failed_cursor.next_allowed_at,
                )

        if batch.source_id != source_id:
            now = self._now()
            failed_cursor = self._unexpected_failure_cursor(
                source_id,
                cursor,
                now=now,
            )
            if not self._safe_complete_probe(
                source_id,
                lease_token,
                failed_cursor,
                updated_at=now,
            ):
                return SourceCollectionResult(
                    source_id=source_id,
                    status=SourceRunStatus.FAILED,
                    error_code="source_state_write_failed",
                    next_allowed_at=failed_cursor.next_allowed_at,
                )
            return SourceCollectionResult(
                source_id=source_id,
                status=SourceRunStatus.BACKOFF,
                error_code="source_identity_mismatch",
                next_allowed_at=failed_cursor.next_allowed_at,
            )

        try:
            saved = sum(
                self._raw_store.save(document).created for document in batch.documents
            )
        except Exception:
            return self._persistence_failure_result(
                source_id,
                cursor,
                lease_token=lease_token,
                error_code="source_raw_archive_write_failed",
            )
        try:
            dispositions = [
                self._event_store.append(event).disposition for event in batch.events
            ]
        except Exception:
            return self._persistence_failure_result(
                source_id,
                cursor,
                lease_token=lease_token,
                error_code="source_event_store_write_failed",
            )
        if not self._safe_complete_probe(
            source_id,
            lease_token,
            batch.cursor,
            updated_at=self._now(),
        ):
            return self._persistence_failure_result(
                source_id,
                cursor,
                lease_token=lease_token,
                error_code="source_state_write_failed",
            )
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

    def _persistence_failure_result(
        self,
        source_id: str,
        previous: FetchCursor | None,
        *,
        lease_token: str,
        error_code: str,
    ) -> SourceCollectionResult:
        """把本地落盘失败转成单来源冷却，不推进成功游标。"""

        now = self._now()
        failed_cursor = self._unexpected_failure_cursor(
            source_id,
            previous,
            now=now,
        )
        persisted = self._safe_complete_probe(
            source_id,
            lease_token,
            failed_cursor,
            updated_at=now,
        )
        return SourceCollectionResult(
            source_id=source_id,
            status=(SourceRunStatus.BACKOFF if persisted else SourceRunStatus.FAILED),
            error_code=(error_code if persisted else "source_state_write_failed"),
            next_allowed_at=failed_cursor.next_allowed_at,
        )

    def _safe_complete_probe(
        self,
        source_id: str,
        lease_token: str,
        cursor: FetchCursor,
        *,
        updated_at: datetime,
    ) -> bool:
        """尽力持久化冷却游标；调用方据返回值决定能否声称可跨重启。"""

        try:
            return self._event_store.complete_source_probe(
                source_id,
                owner_token=lease_token,
                cursor=cursor,
                updated_at=updated_at,
            )
        except Exception:
            return False

    def _unexpected_failure_cursor(
        self,
        source_id: str,
        previous: FetchCursor | None,
        *,
        now: datetime,
    ) -> FetchCursor:
        state = previous or FetchCursor()
        failures = state.consecutive_failures + 1
        base_seconds = self._resilience_policy.unexpected_backoff_base.total_seconds()
        cap_seconds = self._resilience_policy.unexpected_backoff_cap.total_seconds()
        delay_seconds = base_seconds
        for _ in range(min(max(0, failures - 1), 64)):
            if delay_seconds >= cap_seconds:
                break
            delay_seconds = min(cap_seconds, delay_seconds * 2)
        # 确定性的来源抖动避免所有故障源在同一时刻重试，同时保持审计可复现。
        jitter_bucket = sum(source_id.encode("utf-8")) % 501
        jitter_fraction = 0.5 + jitter_bucket / 1000
        delay = timedelta(seconds=delay_seconds * jitter_fraction)
        return FetchCursor(
            etag=state.etag,
            last_modified=state.last_modified,
            content_sha256=state.content_sha256,
            first_seen_at=state.first_seen_at,
            consecutive_failures=failures,
            next_allowed_at=now + delay,
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


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)
