"""低延迟 Binance 行情监控。

监控器只消费公共行情流端口，不接触凭据和下单接口，因此可安全运行在
PAPER/SHADOW 模式，并能在测试中稳定测量延迟。
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass

from gribuki_trade.adapters.binance.spot.stream import BinanceMarketEvent

ClockMs = Callable[[], int]
EventHandler = Callable[[BinanceMarketEvent], Awaitable[None] | None]


@dataclass(frozen=True, slots=True)
class BinanceMonitorSnapshot:
    """一次监控运行的计数与接收延迟摘要。"""

    events: int
    timed_events: int
    started_at_ms: int
    ended_at_ms: int
    p50_latency_ms: float | None
    p95_latency_ms: float | None
    max_latency_ms: int | None
    future_timestamp_events: int


class BinanceMarketMonitor:
    """持续消费 Binance 行情流并记录事件延迟。

    ``event_time_ms`` 会与本地接收时钟比较。时钟偏差可能产生负样本；这类样本
    会计入 ``future_timestamp_events``，并按零延迟报告，避免生成无效指标。
    运行退出时会关闭数据源。
    """

    def __init__(
        self,
        source: AsyncIterator[BinanceMarketEvent],
        *,
        clock_ms: ClockMs | None = None,
        on_event: EventHandler | None = None,
    ) -> None:
        self._source = source
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._on_event = on_event
        self._running = False
        self._snapshot: BinanceMonitorSnapshot | None = None

    @property
    def running(self) -> bool:
        return self._running

    @property
    def snapshot(self) -> BinanceMonitorSnapshot | None:
        return self._snapshot

    async def run(
        self,
        *,
        stop_event: asyncio.Event | None = None,
        maximum_events: int | None = None,
    ) -> BinanceMonitorSnapshot:
        """持续消费，直到收到停止信号、数据流结束或达到 ``maximum_events``。"""

        if self._running:
            raise RuntimeError("a BinanceMarketMonitor run is already active")
        if maximum_events is not None and (
            isinstance(maximum_events, bool) or maximum_events <= 0
        ):
            raise ValueError("maximum_events must be positive")

        started = self._clock_ms()
        latencies: list[int] = []
        events = 0
        future_timestamps = 0
        self._running = True
        iterator = self._source.__aiter__()
        try:
            while stop_event is None or not stop_event.is_set():
                try:
                    event = await anext(iterator)
                except StopAsyncIteration:
                    break
                events += 1
                event_time = getattr(event, "event_time_ms", None)
                if isinstance(event_time, int) and not isinstance(event_time, bool):
                    latency = self._clock_ms() - event_time
                    if latency < 0:
                        future_timestamps += 1
                        latency = 0
                    latencies.append(latency)
                if self._on_event is not None:
                    result = self._on_event(event)
                    if result is not None:
                        await result
                if maximum_events is not None and events >= maximum_events:
                    break
        finally:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()
            self._running = False

        ended = self._clock_ms()
        self._snapshot = BinanceMonitorSnapshot(
            events=events,
            timed_events=len(latencies),
            started_at_ms=started,
            ended_at_ms=ended,
            p50_latency_ms=_percentile(latencies, 0.50),
            p95_latency_ms=_percentile(latencies, 0.95),
            max_latency_ms=max(latencies) if latencies else None,
            future_timestamp_events=future_timestamps,
        )
        return self._snapshot


def _percentile(values: list[int], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return float(ordered[index])
