import json
from collections import deque
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase

from gribuki_trade.adapters.binance import (
    LIVE_WS_BASE_URL,
    TESTNET_WS_BASE_URL,
    BinanceBookTickerEvent,
    BinanceConfigurationError,
    BinanceDepthEvent,
    BinanceKlineEvent,
    BinanceProtocolError,
    BinanceSpotMarketStream,
    BinanceStreamConnectionError,
    BinanceTradeEvent,
    book_ticker_stream,
    build_stream_url,
    depth_stream,
    kline_stream,
    normalize_symbol,
    parse_stream_message,
    trade_stream,
    validate_stream_name,
)


def book_payload(update_id: int = 400900217) -> dict[str, object]:
    # Binance 的 bookTicker 在线路协议中有意不包含事件类型字段。
    return {
        "u": update_id,
        "s": "BTCUSDT",
        "b": "64500.10",
        "B": "1.25000000",
        "a": "64500.20",
        "A": "0.75000000",
    }


def trade_payload(trade_id: int = 12345) -> dict[str, object]:
    return {
        "e": "trade",
        "E": 1_672_515_782_136,
        "s": "BTCUSDT",
        "t": trade_id,
        "p": "64500.15",
        "q": "0.00120000",
        "b": 88,
        "a": 99,
        "T": 1_672_515_782_135,
        "m": True,
        "M": True,
    }


def kline_payload() -> dict[str, object]:
    return {
        "e": "kline",
        "E": 1_493_762_999_999,
        "s": "BTCUSDT",
        "k": {
            "t": 1_493_762_940_000,
            "T": 1_493_762_999_999,
            "s": "BTCUSDT",
            "i": "1m",
            "f": 100,
            "L": 200,
            "o": "0.0010",
            "c": "0.0020",
            "h": "0.0025",
            "l": "0.0010",
            "v": "1000.0",
            "n": 100,
            "x": False,
            "q": "1.0000",
            "V": "500.0",
            "Q": "0.500",
            "B": "123456",
        },
    }


def depth_payload() -> dict[str, object]:
    return {
        "e": "depthUpdate",
        "E": 1_672_515_782_136,
        "s": "BTCUSDT",
        "U": 157,
        "u": 160,
        "pu": 156,
        "b": [["64500.10", "1.25000000"], ["64500.00", "0"]],
        "a": [["64500.20", "0.75000000"]],
    }


class FakeConnection:
    def __init__(self, *frames: str | bytes | Exception) -> None:
        self.frames = deque(frames)
        self.closed = False
        self.context_exited = False

    async def recv(self) -> str | bytes:
        if self.closed or not self.frames:
            raise ConnectionError("offline socket closed")
        result = self.frames.popleft()
        if isinstance(result, Exception):
            raise result
        return result

    async def close(self) -> None:
        self.closed = True


class FakeContext:
    def __init__(self, result: FakeConnection | Exception) -> None:
        self.result = result

    async def __aenter__(self) -> FakeConnection:
        if isinstance(self.result, Exception):
            raise self.result
        return self.result

    async def __aexit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        if isinstance(self.result, FakeConnection):
            self.result.context_exited = True


class FakeConnector:
    def __init__(self, *results: FakeConnection | Exception) -> None:
        self.results = deque(results)
        self.calls: list[tuple[str, dict[str, object]]] = []

    def __call__(self, url: str, **kwargs: object) -> FakeContext:
        self.calls.append((url, kwargs))
        if not self.results:
            return FakeContext(ConnectionError("no more offline sockets"))
        return FakeContext(self.results.popleft())


class BinanceStreamValueTests(TestCase):
    def test_stream_name_builders_normalize_valid_symbols(self) -> None:
        self.assertEqual(normalize_symbol("btcusdt"), "BTCUSDT")
        self.assertEqual(book_ticker_stream("BTCUSDT"), "btcusdt@bookTicker")
        self.assertEqual(trade_stream("btcusdt"), "btcusdt@trade")
        self.assertEqual(kline_stream("BTCUSDT", "1M"), "btcusdt@kline_1M")
        self.assertEqual(depth_stream("BTCUSDT"), "btcusdt@depth")
        self.assertEqual(depth_stream("BTCUSDT", update_speed_ms=100), "btcusdt@depth@100ms")
        self.assertEqual(validate_stream_name("btcusdt@trade"), "btcusdt@trade")

    def test_symbol_and_stream_injection_is_rejected(self) -> None:
        for bad_symbol in ("BTC/USDT", " btcusdt", "BTC?x=1", "币安", "A"):
            with self.subTest(symbol=bad_symbol), self.assertRaises(ValueError):
                trade_stream(bad_symbol)

        for bad_stream in (
            "BTCUSDT@trade",
            "btcusdt@aggTrade",
            "btcusdt@depth@250ms",
            "btcusdt@kline_7m",
            "btcusdt@trade/ethusdt@trade",
            "btcusdt@trade?x=1",
        ):
            with self.subTest(stream=bad_stream), self.assertRaises(ValueError):
                validate_stream_name(bad_stream)

    def test_combined_url_defaults_to_testnet_and_live_requires_opt_in(self) -> None:
        streams = (trade_stream("BTCUSDT"), kline_stream("ETHUSDT", "1m"))

        testnet_url = build_stream_url(streams)

        self.assertEqual(
            testnet_url,
            f"{TESTNET_WS_BASE_URL}/stream?streams="
            "btcusdt@trade/ethusdt@kline_1m",
        )
        with self.assertRaisesRegex(BinanceConfigurationError, "allow_live=True"):
            build_stream_url(streams, environment="live")
        self.assertEqual(
            build_stream_url(
                (trade_stream("BTCUSDT"),),
                environment="LIVE",
                allow_live=True,
            ),
            f"{LIVE_WS_BASE_URL}/ws/btcusdt@trade",
        )

    def test_url_builder_rejects_empty_duplicate_and_invalid_raw_combinations(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least one"):
            build_stream_url(())
        with self.assertRaisesRegex(ValueError, "duplicate"):
            build_stream_url(("btcusdt@trade", "btcusdt@trade"))
        with self.assertRaisesRegex(ValueError, "exactly one"):
            build_stream_url(
                ("btcusdt@trade", "ethusdt@trade"),
                combined=False,
            )

    def test_parse_typed_book_trade_and_kline_events(self) -> None:
        book = parse_stream_message(
            json.dumps(
                {
                    "stream": "btcusdt@bookTicker",
                    "data": book_payload(),
                }
            )
        )
        trade = parse_stream_message(json.dumps(trade_payload()).encode())
        kline = parse_stream_message(json.dumps(kline_payload()))
        depth = parse_stream_message(
            json.dumps(depth_payload()), expected_streams=("btcusdt@depth@100ms",)
        )

        self.assertIsInstance(book, BinanceBookTickerEvent)
        self.assertEqual(book.update_id, 400900217)
        self.assertEqual(book.bid_price, Decimal("64500.10"))
        self.assertIsNone(book.event_time_ms)
        self.assertIsInstance(trade, BinanceTradeEvent)
        self.assertEqual(trade.trade_id, 12345)
        self.assertEqual(trade.quantity, Decimal("0.00120000"))
        self.assertTrue(trade.buyer_is_market_maker)
        self.assertIsInstance(kline, BinanceKlineEvent)
        self.assertEqual(kline.interval, "1m")
        self.assertEqual(kline.close, Decimal("0.0020"))
        self.assertEqual(kline.trade_count, 100)
        self.assertFalse(kline.is_closed)
        self.assertIsInstance(depth, BinanceDepthEvent)
        assert isinstance(depth, BinanceDepthEvent)
        self.assertEqual(depth.first_update_id, 157)
        self.assertEqual(depth.final_update_id, 160)
        self.assertEqual(depth.previous_final_update_id, 156)
        self.assertEqual(depth.bids[0], (Decimal("64500.10"), Decimal("1.25000000")))

    def test_empty_open_kline_accepts_paired_negative_trade_id_sentinels(self) -> None:
        payload = kline_payload()
        raw = payload["k"]
        assert isinstance(raw, dict)
        raw["f"] = -1
        raw["L"] = -1
        raw["n"] = 0

        event = parse_stream_message(json.dumps(payload))

        self.assertIsInstance(event, BinanceKlineEvent)
        assert isinstance(event, BinanceKlineEvent)
        self.assertEqual(event.first_trade_id, -1)
        self.assertEqual(event.last_trade_id, -1)
        self.assertEqual(event.trade_count, 0)

        raw["L"] = 0
        with self.assertRaisesRegex(BinanceProtocolError, "sentinels"):
            parse_stream_message(json.dumps(payload))

    def test_trade_order_ids_are_optional_in_current_public_wire_shape(self) -> None:
        payload = trade_payload()
        payload.pop("b")
        payload.pop("a")

        trade = parse_stream_message(json.dumps(payload))

        self.assertIsInstance(trade, BinanceTradeEvent)
        self.assertIsNone(trade.buyer_order_id)
        self.assertIsNone(trade.seller_order_id)

    def test_raw_book_ticker_is_identified_by_its_required_field_set(self) -> None:
        event = parse_stream_message(json.dumps(book_payload()))

        self.assertIsInstance(event, BinanceBookTickerEvent)
        self.assertEqual(event.stream, "btcusdt@bookTicker")

    def test_parser_rejects_unsubscribed_mismatched_and_malformed_payloads(self) -> None:
        wrong_stream = json.dumps(
            {"stream": "ethusdt@trade", "data": trade_payload()}
        )
        with self.assertRaisesRegex(BinanceProtocolError, "symbol does not match"):
            parse_stream_message(wrong_stream)

        with self.assertRaisesRegex(BinanceProtocolError, "unsubscribed"):
            parse_stream_message(
                json.dumps(trade_payload()),
                expected_streams=("ethusdt@trade",),
            )

        malformed = trade_payload()
        malformed["p"] = "NaN"
        with self.assertRaisesRegex(BinanceProtocolError, "out of range"):
            parse_stream_message(json.dumps(malformed))

        with self.assertRaisesRegex(BinanceProtocolError, "valid JSON"):
            parse_stream_message("not-json")


class BinanceStreamIterationTests(IsolatedAsyncioTestCase):
    async def test_async_iterator_passes_heartbeat_options_and_redacts_url_from_repr(
        self,
    ) -> None:
        frame = json.dumps(
            {"stream": "btcusdt@trade", "data": trade_payload()}
        )
        socket = FakeConnection(frame)
        connector = FakeConnector(socket)
        stream = BinanceSpotMarketStream(
            (trade_stream("BTCUSDT"),),
            connector=connector,
            ping_interval_seconds=11,
            ping_timeout_seconds=7,
            reconnect_delay_seconds=0,
        )
        iterator = stream.events()

        event = await anext(iterator)

        self.assertIsInstance(event, BinanceTradeEvent)
        self.assertTrue(stream.connected)
        self.assertEqual(connector.calls[0][0], f"{TESTNET_WS_BASE_URL}/ws/btcusdt@trade")
        options = connector.calls[0][1]
        self.assertEqual(options["ping_interval"], 11)
        self.assertEqual(options["ping_timeout"], 7)
        self.assertNotIn("streams=", repr(stream))
        self.assertNotIn(stream.url, repr(stream))

        await stream.aclose()
        with self.assertRaises(StopAsyncIteration):
            await anext(iterator)
        self.assertTrue(socket.context_exited)
        self.assertFalse(stream.connected)

    async def test_network_disconnect_reconnects_then_delivers(self) -> None:
        first = FakeConnection(ConnectionError("offline disconnect"))
        second = FakeConnection(json.dumps(trade_payload()))
        connector = FakeConnector(first, second)
        sleeps: list[float] = []

        async def no_wait(delay: float) -> None:
            sleeps.append(delay)

        stream = BinanceSpotMarketStream(
            ("btcusdt@trade",),
            connector=connector,
            max_reconnect_attempts=2,
            reconnect_delay_seconds=0.25,
            sleep=no_wait,
        )
        iterator = stream.events()

        event = await anext(iterator)

        self.assertEqual(event.trade_id, 12345)
        self.assertEqual(len(connector.calls), 2)
        self.assertEqual(sleeps, [0.25])
        await iterator.aclose()

    async def test_reconnect_limit_is_finite(self) -> None:
        connector = FakeConnector(
            ConnectionError("one"),
            TimeoutError("two"),
            OSError("three"),
        )
        sleeps: list[float] = []

        async def no_wait(delay: float) -> None:
            sleeps.append(delay)

        stream = BinanceSpotMarketStream(
            ("btcusdt@trade",),
            connector=connector,
            max_reconnect_attempts=2,
            reconnect_delay_seconds=0.1,
            sleep=no_wait,
        )

        with self.assertRaisesRegex(BinanceStreamConnectionError, "limit exhausted"):
            await anext(stream.events())

        self.assertEqual(len(connector.calls), 3)
        self.assertEqual(sleeps, [0.1, 0.2])

    async def test_protocol_error_is_not_hidden_by_reconnect(self) -> None:
        connector = FakeConnector(FakeConnection("not-json"))
        stream = BinanceSpotMarketStream(
            ("btcusdt@trade",),
            connector=connector,
            reconnect_delay_seconds=0,
        )

        with self.assertRaises(BinanceProtocolError):
            await anext(stream.events())

        self.assertEqual(len(connector.calls), 1)

    async def test_book_and_trade_sequence_regressions_are_not_swallowed(self) -> None:
        socket = FakeConnection(
            json.dumps(book_payload(update_id=20)),
            json.dumps(book_payload(update_id=19)),
        )
        connector = FakeConnector(socket)
        stream = BinanceSpotMarketStream(
            ("btcusdt@bookTicker",),
            connector=connector,
            reconnect_delay_seconds=0,
        )
        iterator = stream.events()

        first = await anext(iterator)
        self.assertEqual(first.update_id, 20)
        with self.assertRaisesRegex(BinanceProtocolError, "sequence did not advance"):
            await anext(iterator)

        self.assertEqual(len(connector.calls), 1)
