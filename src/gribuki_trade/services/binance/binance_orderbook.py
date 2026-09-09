"""把 Binance 深度流与本地订单簿恢复器连接起来。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from gribuki_trade.adapters.binance.market_data.orderbook import (
    BinanceLocalOrderBook,
    LocalOrderBookView,
    SnapshotFetcher,
)

OrderBookEventHandler = Callable[[Any], Awaitable[None] | None]


class BinanceOrderBookRecoveryService:
    """消费深度流并在启动、缺口或重连后自动读取 REST 快照。

    该服务只把已同步的本地订单簿视图交给策略；缓冲溢出或序列缺口期间，
    ``view`` 的状态会保持为 ``DESYNCED``，不会把不完整的价格传给策略。
    """

    def __init__(
        self,
        source: AsyncIterator[Any],
        book: BinanceLocalOrderBook,
        snapshot_fetcher: SnapshotFetcher,
        *,
        on_event: OrderBookEventHandler | None = None,
    ) -> None:
        self._source = source
        self.book = book
        self._snapshot_fetcher = snapshot_fetcher
        self._on_event = on_event

    @property
    def view(self) -> LocalOrderBookView:
        return self.book.view()

    async def run(
        self,
        *,
        stop_event: asyncio.Event | None = None,
        maximum_events: int | None = None,
    ) -> LocalOrderBookView:
        if maximum_events is not None and (
            isinstance(maximum_events, bool) or maximum_events <= 0
        ):
            raise ValueError("maximum_events must be positive")
        processed = 0
        iterator = self._source.__aiter__()
        try:
            while stop_event is None or not stop_event.is_set():
                if maximum_events is not None and processed >= maximum_events:
                    break
                try:
                    event = await anext(iterator)
                except StopAsyncIteration:
                    break
                processed += 1
                if hasattr(event, "first_update_id") and hasattr(event, "final_update_id"):
                    result = self.book.ingest(event)
                    if result.buffered or result.gap_detected:
                        recovered = await self.book.bootstrap(self._snapshot_fetcher)
                        if not recovered:
                            # 快照没有覆盖缓冲区的首个连续区间；继续缓冲并重新取
                            # 快照，但绝不能把未同步事件交给策略。
                            continue
                    if not self.book.synced:
                        continue
                if self._on_event is not None:
                    handled = self._on_event(event)
                    if handled is not None:
                        await handled

        except (ConnectionError, TimeoutError, OSError):
            # 深度流连接断开后，旧序列不能跨连接沿用；下一个消费者必须重新
            # 从 REST 快照建立首个连续区间。
            self.book.reset()
            raise
        finally:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()
        return self.book.view()
