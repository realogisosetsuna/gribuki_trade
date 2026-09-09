import asyncio
import json
from dataclasses import dataclass
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase

from gribuki_trade.adapters.binance import (
    BinanceFuturesOrderBook,
    BinanceProtocolError,
    BinanceSpotOrderBook,
    OrderBookLevel,
    OrderBookRecoveryState,
    OrderBookSnapshot,
)
from gribuki_trade.adapters.binance.futures.client import BinanceFuturesRestClient
from gribuki_trade.adapters.binance.transport.http import HttpRequest, HttpResponse


@dataclass(frozen=True)
class Depth:
    symbol: str
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int | None
    bids: tuple[tuple[Decimal, Decimal], ...] = ()
    asks: tuple[tuple[Decimal, Decimal], ...] = ()


def snapshot(update_id: int = 100) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        symbol="BTCUSDT",
        last_update_id=update_id,
        bids=(OrderBookLevel(Decimal("100"), Decimal("1")),),
        asks=(OrderBookLevel(Decimal("101"), Decimal("2")),),
    )


class LocalOrderBookTests(TestCase):
    def test_spot_replays_buffer_and_applies_zero_quantity_deletes(self) -> None:
        book = BinanceSpotOrderBook("btcusdt")
        event_old = Depth("BTCUSDT", 98, 100, None)
        event_bridge = Depth(
            "BTCUSDT", 101, 103, None,
            ((Decimal("100"), Decimal("0")), (Decimal("99"), Decimal("3"))),
            ((Decimal("101"), Decimal("0")),),
        )
        self.assertTrue(book.ingest(event_old).buffered)
        self.assertTrue(book.ingest(event_bridge).buffered)
        self.assertTrue(book.apply_snapshot(snapshot()))
        self.assertEqual(book.state, OrderBookRecoveryState.SYNCED)
        self.assertEqual(book.last_update_id, 103)
        view = book.view()
        self.assertEqual(view.bids, (OrderBookLevel(Decimal("99"), Decimal("3")),))
        self.assertEqual(view.asks, ())

    def test_spot_gap_invalidates_book_and_requires_new_snapshot(self) -> None:
        book = BinanceSpotOrderBook("BTCUSDT")
        book.apply_snapshot(snapshot())
        result = book.ingest(Depth("BTCUSDT", 105, 106, None))
        self.assertTrue(result.gap_detected)
        self.assertEqual(book.state, OrderBookRecoveryState.DESYNCED)
        self.assertFalse(book.view().bids)

    def test_buffer_overflow_fails_closed_until_bootstrap_resets(self) -> None:
        book = BinanceSpotOrderBook("BTCUSDT", max_buffered_events=1)
        book.ingest(Depth("BTCUSDT", 1, 1, None))
        result = book.ingest(Depth("BTCUSDT", 2, 2, None))
        self.assertEqual(result.state, OrderBookRecoveryState.DESYNCED)
        self.assertFalse(book.apply_snapshot(snapshot(2)))

        async def fetch(_symbol: str) -> OrderBookSnapshot:
            return snapshot(2)

        recovered = asyncio.run(book.bootstrap(fetch))
        self.assertTrue(recovered)
        self.assertTrue(book.synced)

    def test_futures_requires_previous_sequence_after_snapshot(self) -> None:
        book = BinanceFuturesOrderBook("BTCUSDT")
        self.assertTrue(
            book.apply_snapshot(
                snapshot(100)
            )
        )
        first = Depth("BTCUSDT", 99, 101, 98, ((Decimal("100"), Decimal("2")),))
        self.assertTrue(book.ingest(first).applied)
        second = Depth("BTCUSDT", 102, 103, 101, ((Decimal("99"), Decimal("1")),))
        self.assertTrue(book.ingest(second).applied)
        gap = Depth("BTCUSDT", 104, 105, 100)
        self.assertTrue(book.ingest(gap).gap_detected)


class FakeTransport:
    def __init__(self, payload: object) -> None:
        self.payload = payload
        self.captured_request: HttpRequest | None = None

    async def request(self, request: HttpRequest) -> HttpResponse:
        self.captured_request = request
        return HttpResponse(status_code=200, headers={}, body=json.dumps(self.payload).encode())


class FuturesOrderBookGatewayTests(IsolatedAsyncioTestCase):
    async def test_order_book_reads_fapi_depth_snapshot(self) -> None:
        transport = FakeTransport(
            {
                "lastUpdateId": 123,
                "bids": [["100.0", "1.5"]],
                "asks": [["101.0", "2.5"]],
            }
        )
        client = BinanceFuturesRestClient(transport=transport)
        result = await client.order_book("BTCUSDT", limit=100)
        self.assertEqual(result.last_update_id, 123)
        self.assertEqual(result.bids[0].quantity, Decimal("1.5"))
        assert transport.captured_request is not None
        self.assertIn("/fapi/v1/depth", transport.captured_request.url)
        self.assertIn("symbol=BTCUSDT", transport.captured_request.url)

    async def test_order_book_rejects_malformed_levels(self) -> None:
        client = BinanceFuturesRestClient(
            transport=FakeTransport({"lastUpdateId": 1, "bids": "bad", "asks": []})
        )
        with self.assertRaises(BinanceProtocolError):
            await client.order_book("BTCUSDT")
