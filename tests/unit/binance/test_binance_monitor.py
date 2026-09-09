from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase

from gribuki_trade.adapters.binance.spot.stream import (
    BinanceBookTickerEvent,
    BinanceMarketEvent,
    BinanceTradeEvent,
)
from gribuki_trade.services.binance.binance_monitor import BinanceMarketMonitor


class FakeMarket(AsyncIterator[BinanceMarketEvent]):
    def __init__(self, *events: BinanceMarketEvent) -> None:
        self._events = list(events)
        self.closed = False

    def __aiter__(self) -> FakeMarket:
        return self

    async def __anext__(self) -> BinanceMarketEvent:
        if not self._events:
            raise StopAsyncIteration
        return self._events.pop(0)

    async def aclose(self) -> None:
        self.closed = True


def _trade(event_time_ms: int) -> BinanceTradeEvent:
    return BinanceTradeEvent(
        stream="btcusdt@trade",
        symbol="BTCUSDT",
        event_time_ms=event_time_ms,
        trade_id=event_time_ms,
        price=Decimal("100"),
        quantity=Decimal("0.01"),
        buyer_order_id=None,
        seller_order_id=None,
        trade_time_ms=event_time_ms,
        buyer_is_market_maker=False,
    )


def _book(event_time_ms: int | None) -> BinanceBookTickerEvent:
    return BinanceBookTickerEvent(
        stream="btcusdt@bookTicker",
        symbol="BTCUSDT",
        update_id=event_time_ms or 1,
        bid_price=Decimal("99"),
        bid_quantity=Decimal("1"),
        ask_price=Decimal("101"),
        ask_quantity=Decimal("1"),
        event_time_ms=event_time_ms,
    )


class BinanceMarketMonitorTests(IsolatedAsyncioTestCase):
    async def test_latency_percentiles_and_future_timestamps_are_deterministic(self) -> None:
        # 接收时钟固定为 1,000 毫秒，样本延迟为 100、200、900 毫秒。
        source = FakeMarket(_trade(900), _trade(800), _trade(100), _trade(1_100))
        monitor = BinanceMarketMonitor(source, clock_ms=lambda: 1_000)

        snapshot = await monitor.run(maximum_events=4)

        self.assertEqual(snapshot.events, 4)
        self.assertEqual(snapshot.timed_events, 4)
        self.assertEqual(snapshot.started_at_ms, 1_000)
        self.assertEqual(snapshot.ended_at_ms, 1_000)
        self.assertEqual(snapshot.p50_latency_ms, 100.0)
        self.assertEqual(snapshot.p95_latency_ms, 900.0)
        self.assertEqual(snapshot.max_latency_ms, 900)
        self.assertEqual(snapshot.future_timestamp_events, 1)
        self.assertTrue(source.closed)
        self.assertFalse(monitor.running)

    async def test_untimed_events_are_counted_without_polluting_latency(self) -> None:
        source = FakeMarket(_book(None), _trade(950))
        monitor = BinanceMarketMonitor(source, clock_ms=lambda: 1_000)

        snapshot = await monitor.run(maximum_events=2)

        self.assertEqual(snapshot.events, 2)
        self.assertEqual(snapshot.timed_events, 1)
        self.assertEqual(snapshot.p50_latency_ms, 50.0)
        self.assertEqual(snapshot.p95_latency_ms, 50.0)
        self.assertEqual(snapshot.max_latency_ms, 50)
        self.assertEqual(snapshot.future_timestamp_events, 0)

    async def test_callback_runs_in_event_order_before_next_event(self) -> None:
        source = FakeMarket(_trade(900), _trade(950))
        seen: list[int] = []

        async def on_event(event: BinanceMarketEvent) -> None:
            assert isinstance(event, BinanceTradeEvent)
            seen.append(event.trade_id)

        monitor = BinanceMarketMonitor(source, clock_ms=lambda: 1_000, on_event=on_event)
        await monitor.run(maximum_events=2)

        self.assertEqual(seen, [900, 950])

    async def test_stop_event_stops_before_consuming_next_event(self) -> None:
        source = FakeMarket(_trade(900), _trade(950), _trade(975))
        stop = asyncio.Event()

        def on_event(event: BinanceMarketEvent) -> None:
            if isinstance(event, BinanceTradeEvent) and event.trade_id == 900:
                stop.set()

        monitor = BinanceMarketMonitor(source, clock_ms=lambda: 1_000, on_event=on_event)
        snapshot = await monitor.run(stop_event=stop)

        self.assertEqual(snapshot.events, 1)
        self.assertEqual(snapshot.timed_events, 1)
        self.assertEqual(len(source._events), 2)
        self.assertTrue(source.closed)

    async def test_source_exhaustion_returns_snapshot_and_closes_source(self) -> None:
        source = FakeMarket(_trade(900))
        monitor = BinanceMarketMonitor(source, clock_ms=lambda: 1_000)

        snapshot = await monitor.run()

        self.assertEqual(snapshot.events, 1)
        self.assertEqual(snapshot.p50_latency_ms, 100.0)
        self.assertTrue(source.closed)
        self.assertFalse(monitor.running)

    async def test_rejects_concurrent_run_and_non_positive_limit(self) -> None:
        source = FakeMarket(_trade(900))
        monitor = BinanceMarketMonitor(source, clock_ms=lambda: 1_000)

        with self.assertRaisesRegex(ValueError, "maximum_events must be positive"):
            await monitor.run(maximum_events=0)

        # 已经运行的监控器必须安全拒绝并发消费数据源。
        monitor._running = True  # type: ignore[attr-defined]
        with self.assertRaisesRegex(RuntimeError, "already active"):
            await monitor.run(maximum_events=1)
