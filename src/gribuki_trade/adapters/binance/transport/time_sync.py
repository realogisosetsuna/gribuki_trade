"""Binance 服务器时钟校准的通用原语。

交易所会使用服务器时钟校验签名请求。单次采样开销较低，但可能受到网络上下行
延迟不对称的影响。因此 ``sample_server_time`` 支持短时突发采样，并保留往返
延迟最低的偏移结果，在不修改操作系统时间的情况下快速、确定性地减少时间戳拒绝。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TimeSyncResult:
    """本次校准中观测到的最佳服务器时钟采样。"""

    offset_ms: int
    rtt_ms: int
    samples: int


async def sample_server_time(
    fetch_server_time: Callable[[], Awaitable[int]],
    clock_ms: Callable[[], int],
    *,
    samples: int = 3,
) -> TimeSyncResult:
    """通过短时突发采样测量交易所时间减本地中点的偏移。

    ``samples`` 有意设置上限，避免命令行校准消耗过多请求权重。函数选择往返
    延迟最低的采样；延迟相同时保留第一次观测，以保证结果确定。
    """

    if not isinstance(samples, int) or isinstance(samples, bool) or not 1 <= samples <= 9:
        raise ValueError("samples must be an integer between 1 and 9")

    best: TimeSyncResult | None = None
    for _ in range(samples):
        started_ms = clock_ms()
        exchange_ms = int(await fetch_server_time())
        finished_ms = clock_ms()
        rtt_ms = max(0, finished_ms - started_ms)
        midpoint_ms = started_ms + (finished_ms - started_ms) // 2
        candidate = TimeSyncResult(
            offset_ms=exchange_ms - midpoint_ms,
            rtt_ms=rtt_ms,
            samples=samples,
        )
        if best is None or candidate.rtt_ms < best.rtt_ms:
            best = candidate
    assert best is not None
    return best


__all__ = ["TimeSyncResult", "sample_server_time"]
