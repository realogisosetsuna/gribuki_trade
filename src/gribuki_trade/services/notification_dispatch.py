"""Finite, stoppable orchestration for reliable outbound notifications."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta

from gribuki_trade.ports.notifier import Notifier
from gribuki_trade.storage.outbox import DispatchSummary, OutboxDispatcher, SQLiteOutbox


class NotificationDispatchServiceError(RuntimeError):
    """A stable failure that deliberately omits message and credential data."""

    def __init__(self, code: str = "dispatch_failed") -> None:
        super().__init__(f"notification dispatch service failed ({code})")
        self.code = code


@dataclass(frozen=True, slots=True)
class DispatchRunStatistics:
    """Aggregate results from one finite polling run."""

    cycles_completed: int
    claimed: int
    sent: int
    retry_scheduled: int
    dead: int
    expired: int
    stop_requested: bool
    reached_cycle_limit: bool


class NotificationDispatchService:
    """Run an :class:`OutboxDispatcher` once or for a bounded number of cycles.

    All calls are serialized with an asyncio lock.  A second caller can never
    race the same service instance and attempt to deliver a claimed item twice.
    ``request_stop`` interrupts the wait between cycles; it never cancels an
    HTTP request that is already in progress.
    """

    def __init__(
        self,
        outbox: SQLiteOutbox,
        notifiers: Mapping[str, Notifier],
        *,
        dispatcher: OutboxDispatcher | None = None,
    ) -> None:
        self._dispatcher = dispatcher or OutboxDispatcher(outbox, notifiers)
        self._operation_lock = asyncio.Lock()
        self._stop_requested = asyncio.Event()

    def request_stop(self) -> None:
        """Prevent another polling cycle and interrupt the current wait."""

        self._stop_requested.set()

    def reset_stop(self) -> None:
        """Explicitly allow a new finite run after a prior stop request."""

        self._stop_requested.clear()

    @property
    def stop_requested(self) -> bool:
        return self._stop_requested.is_set()

    async def dispatch_once(
        self,
        *,
        limit: int = 50,
        lease_for: timedelta = timedelta(seconds=30),
    ) -> DispatchSummary:
        """Dispatch one transactional batch and return structured counts."""

        async with self._operation_lock:
            return await self._safe_run_once(limit=limit, lease_for=lease_for)

    async def poll(
        self,
        *,
        max_cycles: int,
        poll_interval: float = 1.0,
        limit: int = 50,
        lease_for: timedelta = timedelta(seconds=30),
    ) -> DispatchRunStatistics:
        """Poll a finite number of times, or return earlier when stopped."""

        if max_cycles < 1:
            raise ValueError("max_cycles must be positive")
        if poll_interval < 0:
            raise ValueError("poll_interval must not be negative")
        if limit < 1:
            raise ValueError("limit must be positive")
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")

        summaries: list[DispatchSummary] = []
        async with self._operation_lock:
            while len(summaries) < max_cycles and not self._stop_requested.is_set():
                summaries.append(await self._safe_run_once(limit=limit, lease_for=lease_for))
                if len(summaries) >= max_cycles or self._stop_requested.is_set():
                    break
                await self._wait_for_stop(poll_interval)

        return DispatchRunStatistics(
            cycles_completed=len(summaries),
            claimed=sum(item.claimed for item in summaries),
            sent=sum(item.sent for item in summaries),
            retry_scheduled=sum(item.retry_scheduled for item in summaries),
            dead=sum(item.dead for item in summaries),
            expired=sum(item.expired for item in summaries),
            stop_requested=self._stop_requested.is_set(),
            reached_cycle_limit=len(summaries) == max_cycles,
        )

    async def _safe_run_once(self, *, limit: int, lease_for: timedelta) -> DispatchSummary:
        result: DispatchSummary | None = None
        # Leave a suppressed exception's handler before raising our public
        # error.  This hides its string and avoids retaining it through
        # ``__context__``, where a token, target, or message could be read.
        with suppress(Exception):
            result = await self._dispatcher.run_once(limit=limit, lease_for=lease_for)
        if result is None:
            raise NotificationDispatchServiceError()
        return result

    async def _wait_for_stop(self, seconds: float) -> None:
        if seconds == 0:
            await asyncio.sleep(0)
            return
        with suppress(TimeoutError):
            await asyncio.wait_for(self._stop_requested.wait(), timeout=seconds)
