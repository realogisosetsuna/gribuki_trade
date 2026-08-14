"""针对多标的 A 股研究的有限、可停止调度。

调度器刻意依赖调用方提供的研究可调用对象。它不依赖券商、账户、订单或执行，因此
不能把研究结果变成交易。失败以稳定错误码表示；报告绝不保留供应商异常对象或消息。
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
    """一次隔离标的评估的结果。"""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class ResearchWatchServiceError(RuntimeError):
    """刻意省略敏感细节的稳定调度失败。"""

    def __init__(self, code: str) -> None:
        super().__init__(f"research watch service failed ({code})")
        self.code = code


class ResearchWatchAlreadyRunningError(RuntimeError):
    """两个调用方尝试运行同一调度器实例时抛出。"""

    def __init__(self) -> None:
        super().__init__("research watch service is already running")


@dataclass(frozen=True, slots=True)
class ResearchSymbolRun(Generic[ResearchResultT]):
    """一次标的评估的脱敏结果。"""

    symbol: str
    status: ResearchSymbolStatus
    started_at: datetime
    completed_at: datetime
    result: ResearchResultT | None = None
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class ResearchWatchCycle(Generic[ResearchResultT]):
    """一个调度周期内尝试的全部标的评估。"""

    cycle_number: int
    started_at: datetime
    completed_at: datetime
    symbol_runs: tuple[ResearchSymbolRun[ResearchResultT], ...]
    succeeded: int
    failed: int


@dataclass(frozen=True, slots=True)
class ResearchWatchStatistics(Generic[ResearchResultT]):
    """一次有界运行的逐周期明细与汇总计数。"""

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
    """为每个已配置 A 股标的调度研究可调用对象。

    每个周期内按顺序处理标的，使供应商压力可预测，并避免不安全地并发使用调用方拥有的
    SQLite 存储。单个标的失败会转换为 ``research_run_failed``，其余标的继续执行。
    ``request_stop`` 会中断间隔等待并阻止新的标的评估，但绝不取消正在进行的评估。
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
        """每个完整周期中调度的规范化标的。"""

        return self._symbols

    @property
    def stop_requested(self) -> bool:
        return self._stop_requested.is_set()

    @property
    def is_running(self) -> bool:
        return self._running

    def request_stop(self) -> None:
        """阻止新工作并中断下一周期前的等待。"""

        self._stop_requested.set()

    def reset_stop(self) -> None:
        """在先前停止请求后显式允许后续有界运行。"""

        if self._running:
            raise ResearchWatchAlreadyRunningError()
        self._stop_requested.clear()

    async def run(
        self,
        *,
        max_cycles: int,
        interval_seconds: float = 60.0,
    ) -> ResearchWatchStatistics[ResearchResultT]:
        """最多运行 ``max_cycles`` 个周期并返回详细的脱敏统计。"""

        if isinstance(max_cycles, bool) or not isinstance(max_cycles, int) or max_cycles < 1:
            raise ValueError("max_cycles must be positive")
        if interval_seconds < 0:
            raise ValueError("interval_seconds must not be negative")
        if self._running:
            raise ResearchWatchAlreadyRunningError()

        # 防护检查与赋值之间刻意没有 await，因此同一事件循环上的并发任务无法同时进入运行。
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
        # 读取时钟或构造公开结果前先退出被抑制异常的处理器。报告不会保留异常消息、
        # 回溯或供应商特定类。
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
