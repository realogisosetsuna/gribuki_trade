"""Binance 合约私有数据流适配器契约测试。"""

from __future__ import annotations

import asyncio
import json
from unittest import IsolatedAsyncioTestCase

import pytest

from gribuki_trade.adapters.binance.futures.user_stream import (
    FuturesAlgoUpdate,
    FuturesUnknownEvent,
    normalize_futures_user_event,
    parse_futures_user_event,
)
from gribuki_trade.adapters.binance.transport.gateway import BinanceProtocolError


def test_parse_algo_update_preserves_identity_without_secret() -> None:
    event = parse_futures_user_event(
        json.dumps(
            {
                "e": "ALGO_UPDATE",
                "E": 100,
                "T": 99,
                "apiKey": "hidden",
                "o": {
                    "caid": "client-1",
                    "aid": 42,
                    "ai": "9001",
                    "X": "TRIGGERED",
                    "s": "BTCUSDT",
                },
            }
        ),
        received_time_ms=101,
        connection_epoch=3,
    )
    assert isinstance(event, FuturesAlgoUpdate)
    assert event.payload["o"]["caid"] == "client-1"  # type: ignore[index]
    assert "apiKey" not in event.payload
    normalized = normalize_futures_user_event(event)
    assert normalized["kind"] == "algo"
    assert normalized["client_algo_id"] == "client-1"
    assert normalized["actual_order_id"] == "9001"
    assert normalized["connection_epoch"] == 3


def test_unknown_event_is_explicit_degraded_event() -> None:
    event = parse_futures_user_event('{"e":"FUTURE_EVENT","E":1}')
    assert isinstance(event, FuturesUnknownEvent)
    assert event.known is False
    assert normalize_futures_user_event(event)["kind"] == "unknown"


@pytest.mark.parametrize("payload", ["{}", '{"e":"ORDER_TRADE_UPDATE"}', '{"E":1}'])
def test_malformed_event_fails_closed(payload: str) -> None:
    with pytest.raises(BinanceProtocolError):
        parse_futures_user_event(payload)

class _FakeProfile:
    is_live = False


class _FakeClient:
    profile = _FakeProfile()
    user_data_stream_base_url = "wss://fstream.binance.com/private"

    def __init__(self) -> None:
        self.closed: list[str] = []
        self.kept: list[str] = []

    async def start_user_data_stream(self) -> str:
        return "lk-test"

    async def keepalive_user_data_stream(self, listen_key: str) -> None:
        self.kept.append(listen_key)

    async def close_user_data_stream(self, listen_key: str) -> None:
        self.closed.append(listen_key)


class _FakeContext:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def __aenter__(self) -> object:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class _OneFrameConnection:
    def __init__(self, frame: str) -> None:
        self.frame = frame
        self.done = False
        self.release = asyncio.Event()

    async def recv(self) -> str:
        if not self.done:
            self.done = True
            return self.frame
        await self.release.wait()
        raise ConnectionError("closed")

    async def close(self) -> None:
        self.release.set()


class _Connector:
    def __init__(self, connection: object) -> None:
        self.connection = connection
        self.calls: list[tuple[str, dict[str, object]]] = []

    def __call__(self, url: str, **kwargs: object) -> _FakeContext:
        self.calls.append((url, kwargs))
        return _FakeContext(self.connection)


class FuturesUserStreamLifecycleTests(IsolatedAsyncioTestCase):
    async def test_private_stream_url_lifecycle_and_epoch(self) -> None:
        client = _FakeClient()
        socket = _OneFrameConnection(
            '{"e":"TRADE_LITE","E":10,"T":9,"s":"BTCUSDT","i":3,"c":"cid"}'
        )
        connector = _Connector(socket)
        lifecycle: list[object] = []

        async def callback(item: object) -> None:
            lifecycle.append(item)

        from gribuki_trade.adapters.binance.futures.user_stream import BinanceFuturesUserDataStream

        stream = BinanceFuturesUserDataStream(
            client, account_id="acct", connector=connector, on_lifecycle=callback
        )
        iterator = stream.events()
        event = await anext(iterator)
        self.assertEqual(event.event_type, "TRADE_LITE")
        self.assertEqual(stream.connection_epoch, 1)
        self.assertIn("listenKey=lk-test", connector.calls[0][0])
        self.assertIn("events=", connector.calls[0][0])
        await stream.disconnect()
        await iterator.aclose()
        self.assertEqual(client.closed, ["lk-test"])
        self.assertEqual([item.kind for item in lifecycle[:2]], ["connecting", "connected"])

    async def test_malformed_private_frame_is_terminal_and_degraded(self) -> None:
        client = _FakeClient()
        connector = _Connector(_OneFrameConnection('{"e":"TRADE_LITE"}'))
        from gribuki_trade.adapters.binance.futures.user_stream import BinanceFuturesUserDataStream

        stream = BinanceFuturesUserDataStream(client, account_id="acct", connector=connector)
        with self.assertRaises(BinanceProtocolError):
            await anext(stream.events())
        self.assertFalse(stream.healthy)
        self.assertEqual(len(connector.calls), 1)
