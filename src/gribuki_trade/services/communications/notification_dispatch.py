"""可靠出站通知的有限、可停止编排。"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta

from gribuki_trade.ports.notifier import NotificationTargetKind, Notifier
from gribuki_trade.storage.outbox import DispatchSummary, OutboxDispatcher, SQLiteOutbox


class NotificationDispatchServiceError(RuntimeError):
    """刻意省略消息与凭据数据的稳定失败。"""

    def __init__(self, code: str = "dispatch_failed") -> None:
        super().__init__(f"notification dispatch service failed ({code})")
        self.code = code


@dataclass(frozen=True, slots=True)
class DispatchRunStatistics:
    """一次有限轮询运行的汇总结果。"""

    cycles_completed: int
    claimed: int
    sent: int
    retry_scheduled: int
    dead: int
    expired: int
    stop_requested: bool
    reached_cycle_limit: bool


class NotificationDispatchService:
    """运行一次 :class:`OutboxDispatcher` 或运行有界数量的周期。

    所有调用均通过 asyncio 锁串行化。第二个调用方无法与同一服务实例竞争并尝试重复
    交付已认领项目。``request_stop`` 会中断周期之间的等待，但绝不取消正在进行的
    HTTP 请求。
    """

    def __init__(
        self,
        outbox: SQLiteOutbox,
        notifiers: Mapping[str, Notifier],
        *,
        dispatcher: OutboxDispatcher | None = None,
        target_kind: NotificationTargetKind | None = None,
        target_id: str | None = None,
    ) -> None:
        if dispatcher is not None and (target_kind is not None or target_id is not None):
            raise ValueError("a custom dispatcher cannot be combined with a target scope")
        self._dispatcher = dispatcher or OutboxDispatcher(
            outbox,
            notifiers,
            target_kind=target_kind,
            target_id=target_id,
        )
        self._operation_lock = asyncio.Lock()
        self._stop_requested = asyncio.Event()

    def request_stop(self) -> None:
        """阻止下一轮询周期并中断当前等待。"""

        self._stop_requested.set()

    def reset_stop(self) -> None:
        """在先前停止请求后显式允许新的有限运行。"""

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
        """分发一个事务批次并返回结构化计数。"""

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
        """轮询有限次数，或在停止时提前返回。"""

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
        # 抛出公开错误前先退出被抑制异常的处理器，以隐藏其字符串，并避免通过
        # ``__context__`` 保留可能读取到令牌、目标或消息的内容。
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
