"""Finite, stoppable scheduling for multi-symbol A-share research.

The scheduler deliberately depends on a caller-supplied research callable.  It
has no broker, account, order, or execution dependency, and therefore cannot
turn a research result into a trade.  Failures are represented by stable error
codes; provider exception objects and messages are never retained in reports.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Generic, TypeVar

ResearchResultT = TypeVar("ResearchResultT")
ResearchRunOnce = Callable[[str], Awaitable[ResearchResultT]]
ResearchWatchClock = Callable[[], datetime]
ResearchWatchSleep = Callable[[float], Awaitable[None]]


class ResearchSymbolStatus(StrEnum):
    """Outcome of one isolated symbol evaluation."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class ResearchWatchServiceError(RuntimeError):
    """A stable scheduler failure that deliberately omits sensitive details."""

    def __init__(self, code: str) -> None:
        super().__init__(f"research watch service failed ({code})")
        self.code = code


class ResearchWatchAlreadyRunningError(RuntimeError):
    """Raised when two callers try to run the same scheduler instance."""

    def __init__(self) -> None:
        super().__init__("research watch service is already running")


@dataclass(frozen=True, slots=True)
class ResearchSymbolRun(Generic[ResearchResultT]):
    """Sanitized result of one symbol evaluation."""

    symbol: str
    status: ResearchSymbolStatus
    started_at: datetime
    completed_at: datetime
    result: ResearchResultT | None = None
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class ResearchWatchCycle(Generic[ResearchResultT]):
    """All attempted symbol evaluations in one scheduler cycle."""

    cycle_number: int
    started_at: datetime
    completed_at: datetime
    symbol_runs: tuple[ResearchSymbolRun[ResearchResultT], ...]
    succeeded: int
    failed: int


@dataclass(frozen=True, slots=True)
class ResearchWatchStatistics(Generic[ResearchResultT]):
    """Per-cycle details and aggregate counts for one bounded run."""

    started_at: datetime
    completed_at: datetime
    cycles: tuple[ResearchWatchCycle[ResearchResultT], ...]
    cycles_completed: int
    symbols_attempted: int
    succeeded: int
    failed: int
    stop_requested: bool
    reached_cycle_limit: bool


class ResearchWatchService(Generic[ResearchResultT]):
    """Schedule a research callable for each configured A-share symbol.

    Symbols run sequentially inside each cycle.  This makes provider pressure
    predictable and avoids unsafe concurrent use of caller-owned SQLite stores.
    A failure for one symbol is converted to ``research_run_failed`` and the
    remaining symbols continue.  ``request_stop`` interrupts an interval wait
    and prevents new symbol evaluations, but never cancels an evaluation that
    is already in progress.
    """

    def __init__(
        self,
        symbols: Sequence[str],
        run_once: ResearchRunOnce[ResearchResultT],
        *,
        clock: ResearchWatchClock = lambda: datetime.now(UTC),
        sleep: ResearchWatchSleep = asyncio.sleep,
    ) -> None:
        if not symbols:
            raise ValueError("at least one A-share symbol is required")
        canonical_symbols = tuple(_canonical_ashare_symbol(symbol) for symbol in symbols)
        if len(canonical_symbols) != len(set(canonical_symbols)):
            raise ValueError("A-share symbols must be unique")

        self._symbols = canonical_symbols
        self._run_once = run_once
        self._clock = clock
        self._sleep = sleep
        self._stop_requested = asyncio.Event()
        self._running = False

    @property
    def symbols(self) -> tuple[str, ...]:
        """Canonical symbols scheduled in each complete cycle."""

        return self._symbols

    @property
    def stop_requested(self) -> bool:
        return self._stop_requested.is_set()

    @property
    def is_running(self) -> bool:
        return self._running

    def request_stop(self) -> None:
        """Prevent new work and interrupt the wait before the next cycle."""

        self._stop_requested.set()

    def reset_stop(self) -> None:
        """Explicitly allow a later bounded run after a prior stop request."""

        if self._running:
            raise ResearchWatchAlreadyRunningError()
        self._stop_requested.clear()

    async def run(
        self,
        *,
        max_cycles: int,
        interval_seconds: float = 60.0,
    ) -> ResearchWatchStatistics[ResearchResultT]:
        """Run at most ``max_cycles`` and return detailed sanitized statistics."""

        if isinstance(max_cycles, bool) or not isinstance(max_cycles, int) or max_cycles < 1:
            raise ValueError("max_cycles must be positive")
        if interval_seconds < 0:
            raise ValueError("interval_seconds must not be negative")
        if self._running:
            raise ResearchWatchAlreadyRunningError()

        # There is deliberately no await between the guard and assignment, so
        # concurrent tasks on the same event loop cannot both enter the run.
        self._running = True
        try:
            started_at = self._now()
            cycles: list[ResearchWatchCycle[ResearchResultT]] = []
            while len(cycles) < max_cycles and not self._stop_requested.is_set():
                cycles.append(await self._run_cycle(len(cycles) + 1))
                if len(cycles) >= max_cycles or self._stop_requested.is_set():
                    break
                await self._safe_wait(interval_seconds)

            completed_at = self._now()
            return ResearchWatchStatistics(
                started_at=started_at,
                completed_at=completed_at,
                cycles=tuple(cycles),
                cycles_completed=len(cycles),
                symbols_attempted=sum(len(cycle.symbol_runs) for cycle in cycles),
                succeeded=sum(cycle.succeeded for cycle in cycles),
                failed=sum(cycle.failed for cycle in cycles),
                stop_requested=self._stop_requested.is_set(),
                reached_cycle_limit=len(cycles) == max_cycles,
            )
        finally:
            self._running = False

    async def _run_cycle(self, cycle_number: int) -> ResearchWatchCycle[ResearchResultT]:
        started_at = self._now()
        runs: list[ResearchSymbolRun[ResearchResultT]] = []
        for symbol in self._symbols:
            if self._stop_requested.is_set():
                break
            runs.append(await self._run_symbol(symbol))
        completed_at = self._now()
        succeeded = sum(item.status is ResearchSymbolStatus.SUCCESS for item in runs)
        return ResearchWatchCycle(
            cycle_number=cycle_number,
            started_at=started_at,
            completed_at=completed_at,
            symbol_runs=tuple(runs),
            succeeded=succeeded,
            failed=len(runs) - succeeded,
        )

    async def _run_symbol(self, symbol: str) -> ResearchSymbolRun[ResearchResultT]:
        started_at = self._now()
        result: ResearchResultT | None = None
        succeeded = False
        # Exit the suppressed exception's handler before reading the clock or
        # constructing the public result.  No exception message, traceback, or
        # provider-specific class is retained by the report.
        with suppress(Exception):
            result = await self._run_once(symbol)
            succeeded = True
        completed_at = self._now()
        if succeeded:
            return ResearchSymbolRun(
                symbol=symbol,
                status=ResearchSymbolStatus.SUCCESS,
                started_at=started_at,
                completed_at=completed_at,
                result=result,
            )
        return ResearchSymbolRun(
            symbol=symbol,
            status=ResearchSymbolStatus.FAILED,
            started_at=started_at,
            completed_at=completed_at,
            error_code="research_run_failed",
        )

    async def _safe_wait(self, seconds: float) -> None:
        completed = False
        with suppress(Exception):
            await self._wait_for_stop(seconds)
            completed = True
        if not completed:
            raise ResearchWatchServiceError("wait_failed") from None

    async def _wait_for_stop(self, seconds: float) -> None:
        if seconds == 0:
            await self._sleep(0)
            return

        sleeper = asyncio.ensure_future(self._sleep(seconds))
        stopper = asyncio.create_task(self._stop_requested.wait())
        try:
            done, _ = await asyncio.wait((sleeper, stopper), return_when=asyncio.FIRST_COMPLETED)
            if stopper in done:
                return
            await sleeper
        finally:
            for task in (sleeper, stopper):
                if not task.done():
                    task.cancel()
            await asyncio.gather(sleeper, stopper, return_exceptions=True)

    def _now(self) -> datetime:
        value: datetime | None = None
        with suppress(Exception):
            value = self._clock()
        if value is None:
            raise ResearchWatchServiceError("clock_failed") from None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value.astimezone(UTC)


def _canonical_ashare_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if len(value) == 6 and value.isdigit():
        exchange = "SH" if value.startswith(("5", "6", "9")) else "SZ"
        return f"{value}.{exchange}"
    if len(value) == 9 and value[6] == ".":
        code, exchange = value.split(".", maxsplit=1)
        if code.isdigit() and exchange in {"SH", "SZ"}:
            return value
    raise ValueError("symbol must look like 600000.SH or 000001.SZ")
