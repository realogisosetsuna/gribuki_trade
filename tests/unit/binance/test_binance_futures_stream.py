from __future__ import annotations

import json
from collections import deque
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase

from gribuki_trade.adapters.binance.envs import BinanceProduct, BinanceStage, binance_environment
from gribuki_trade.adapters.binance.futures_stream import (
    BinanceFuturesMarketStream,
    FuturesDepthEvent,
    FuturesMarkPriceEvent,
    futures_stream,
    futures_stream_url,
    parse_futures_stream_message,
)
from gribuki_trade.adapters.binance.gateway import BinanceConfigurationError, BinanceProtocolError


def depth_payload(first: int, final: int, previous: int) -> dict[str, object]:
    return {
        "e": "depthUpdate",
        "E": 100,
        "T": 99,
        "s": "BTCUSDT",
        "U": first,
        "u": final,
        "pu": previous,
        "b": [["70000.0", "1.0"]],
        "a": [["70001.0", "2.0"]],
    }


class FuturesStreamParsingTests(TestCase):
    def test_routes_and_parses_depth_mark_price_and_aggregate_trade(self) -> None:
        self.assertEqual(futures_stream("BTCUSDT", "depth@100ms"), "btcusdt@depth@100ms")
        live = binance_environment(BinanceProduct.USDS_FUTURES, BinanceStage.LIVE)
        self.assertIn(
            "/public/", futures_stream_url(("btcusdt@depth",), profile=live, allow_live=True)
        )
        self.assertIn(
            "/market/", futures_stream_url(("btcusdt@markPrice",), profile=live, allow_live=True)
        )

        depth = parse_futures_stream_message(json.dumps(depth_payload(10, 12, 9)))
        self.assertIsInstance(depth, FuturesDepthEvent)
        assert isinstance(depth, FuturesDepthEvent)
        self.assertEqual(depth.final_update_id, 12)
        self.assertEqual(depth.bids[0], (Decimal("70000.0"), Decimal("1.0")))

        mark = parse_futures_stream_message(
            json.dumps(
                {
                    "e": "markPriceUpdate",
                    "E": 100,
                    "s": "BTCUSDT",
                    "p": "70000",
                    "i": "69999",
                    "P": "70001",
                    "r": "0.0001",
                    "T": 200,
                }
            )
        )
        self.assertIsInstance(mark, FuturesMarkPriceEvent)
        assert isinstance(mark, FuturesMarkPriceEvent)
        self.assertEqual(mark.next_funding_time_ms, 200)

    def test_mixed_public_and_market_routes_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            futures_stream_url(("btcusdt@depth", "btcusdt@markPrice"))
        with self.assertRaises(BinanceConfigurationError):
            futures_stream_url(("btcusdt@depth",), stage=BinanceStage.LIVE)


class FakeConnection:
    def __init__(self, *frames: str) -> None:
        self.frames = deque(frames)
        self.closed = False

    async def recv(self) -> str:
        if not self.frames:
            raise ConnectionError("closed")
        return self.frames.popleft()

    async def close(self) -> None:
        self.closed = True


class FakeContext:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class BinanceFuturesMarketStreamTests(IsolatedAsyncioTestCase):
    async def test_depth_gap_fails_closed(self) -> None:
        first = json.dumps(depth_payload(10, 12, 9))
        gap = json.dumps(depth_payload(15, 16, 12))
        connection = FakeConnection(first, gap)
        stream = BinanceFuturesMarketStream(
            ("btcusdt@depth",),
            connector=lambda *_args, **_kwargs: FakeContext(connection),
            max_reconnect_attempts=0,
        )
        iterator = stream.events()
        event = await anext(iterator)
        self.assertEqual(event.final_update_id, 12)  # type: ignore[union-attr]
        with self.assertRaises(BinanceProtocolError):
            await anext(iterator)
