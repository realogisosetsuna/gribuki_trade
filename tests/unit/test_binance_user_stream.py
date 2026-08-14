import hashlib
import hmac
import json
from collections import deque
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase

from gribuki_trade.adapters.binance import (
    LIVE_USER_WS_API_URL,
    TESTNET_USER_WS_API_URL,
    BinanceBalanceUpdate,
    BinanceConfigurationError,
    BinanceCredentials,
    BinanceEventStreamTerminated,
    BinanceExecutionReport,
    BinanceOutboundAccountPosition,
    BinanceProtocolError,
    BinanceSpotUserDataStream,
    BinanceUserStreamAPIError,
    BinanceUserStreamConnectionError,
    parse_user_data_event,
    signature_payload,
)
from gribuki_trade.domain.orders import OrderStatus, Side


def envelope(event: dict[str, object], subscription_id: int = 7) -> str:
    return json.dumps({"subscriptionId": subscription_id, "event": event})


def subscription_response(request_id: int = 1, subscription_id: int = 7) -> str:
    return json.dumps(
        {
            "id": request_id,
            "status": 200,
            "result": {"subscriptionId": subscription_id},
            "rateLimits": [],
        }
    )


def execution_report(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "e": "executionReport",
        "E": 1_499_405_658_659,
        "s": "ETHBTC",
        "c": "client-order-1",
        "S": "BUY",
        "o": "LIMIT",
        "f": "GTC",
        "q": "1.00000000",
        "p": "0.10264410",
        "P": "0.00000000",
        "F": "0.00000000",
        "g": -1,
        "C": "",
        "x": "TRADE",
        "X": "PARTIALLY_FILLED",
        "r": "NONE",
        "i": 4_293_153,
        "l": "0.10000000",
        "z": "0.10000000",
        "L": "0.10260000",
        "n": "0.00001000",
        "N": "BNB",
        "T": 1_499_405_658_658,
        "t": 12_345,
        "v": 3,
        "I": 8_641_984,
        "w": True,
        "m": False,
        "M": False,
        "O": 1_499_405_658_657,
        "Z": "0.01026000",
        "Y": "0.01026000",
        "Q": "0.00000000",
        "W": 1_499_405_658_657,
        "V": "NONE",
    }
    payload.update(overrides)
    return payload


def account_position() -> dict[str, object]:
    return {
        "e": "outboundAccountPosition",
        "E": 1_564_034_571_105,
        "u": 1_564_034_571_073,
        "B": [
            {"a": "ETH", "f": "10000.000000", "l": "0.000000"},
            {"a": "BTC", "f": "1.250000", "l": "0.500000"},
        ],
    }


class FakeConnection:
    def __init__(self, *frames: str | bytes | Exception) -> None:
        self.frames = deque(frames)
        self.sent: list[str | bytes] = []
        self.closed = False
        self.context_exited = False

    async def send(self, message: str | bytes) -> None:
        if self.closed:
            raise ConnectionError("offline socket closed")
        self.sent.append(message)

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


class BinanceUserStreamValueTests(TestCase):
    def test_signing_payload_is_ascii_sorted_and_excludes_signature(self) -> None:
        params = {
            "timestamp": 1_725_000_000_000,
            "signature": "must-not-be-signed",
            "recvWindow": 5_000,
            "apiKey": "public-key",
        }

        payload = signature_payload(params)

        self.assertEqual(
            payload,
            "apiKey=public-key&recvWindow=5000&timestamp=1725000000000",
        )

    def test_parse_all_documented_event_types(self) -> None:
        execution = parse_user_data_event(envelope(execution_report()).encode())
        position = parse_user_data_event(envelope(account_position()))
        balance = parse_user_data_event(
            envelope(
                {
                    "e": "balanceUpdate",
                    "E": 1_573_200_697_110,
                    "a": "BTC",
                    "d": "-0.12500000",
                    "T": 1_573_200_697_068,
                }
            )
        )
        terminated = parse_user_data_event(
            envelope({"e": "eventStreamTerminated", "E": 1_728_973_001_334})
        )

        self.assertIsInstance(execution, BinanceExecutionReport)
        self.assertEqual(execution.side, Side.BUY)
        self.assertEqual(execution.status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(execution.exchange_order_status, "PARTIALLY_FILLED")
        self.assertEqual(execution.original_quantity, Decimal("1.00000000"))
        self.assertEqual(execution.last_executed_price, Decimal("0.10260000"))
        self.assertEqual(execution.commission_amount, Decimal("0.00001000"))
        self.assertEqual(execution.order_id, 4_293_153)
        self.assertEqual(execution.trade_id, 12_345)
        self.assertEqual(execution.prevented_match_id, 3)
        self.assertIsNone(execution.original_client_order_id)

        self.assertIsInstance(position, BinanceOutboundAccountPosition)
        self.assertEqual(position.balances[1].asset, "BTC")
        self.assertEqual(position.balances[1].locked, Decimal("0.500000"))
        self.assertIsInstance(balance, BinanceBalanceUpdate)
        self.assertEqual(balance.balance_delta, Decimal("-0.12500000"))
        self.assertIsInstance(terminated, BinanceEventStreamTerminated)
        self.assertEqual(terminated.subscription_id, 7)

    def test_status_mapping_preserves_unknown_exchange_state(self) -> None:
        event = parse_user_data_event(envelope(execution_report(X="FUTURE_STATUS")))

        self.assertIsInstance(event, BinanceExecutionReport)
        self.assertEqual(event.status, OrderStatus.UNKNOWN)
        self.assertEqual(event.exchange_order_status, "FUTURE_STATUS")

    def test_malformed_and_response_frames_raise_protocol_errors(self) -> None:
        with self.assertRaisesRegex(BinanceProtocolError, "valid JSON"):
            parse_user_data_event("not-json")
        with self.assertRaisesRegex(BinanceProtocolError, "not a user-data event"):
            parse_user_data_event(subscription_response())
        malformed = execution_report(p="NaN")
        with self.assertRaisesRegex(BinanceProtocolError, "out of range"):
            parse_user_data_event(envelope(malformed))

    def test_testnet_default_live_requires_explicit_opt_in_and_repr_is_redacted(self) -> None:
        credentials = BinanceCredentials("visible-api-key", "visible-secret")
        stream = BinanceSpotUserDataStream(credentials)

        self.assertEqual(stream.url, TESTNET_USER_WS_API_URL)
        self.assertNotIn(credentials.api_key, repr(stream))
        self.assertNotIn(credentials.secret_key, repr(stream))
        with self.assertRaisesRegex(BinanceConfigurationError, "allow_live=True"):
            BinanceSpotUserDataStream(credentials, environment="live")
        live = BinanceSpotUserDataStream(
            credentials,
            environment="LIVE",
            allow_live=True,
        )
        self.assertEqual(live.url, LIVE_USER_WS_API_URL)


class BinanceUserStreamIterationTests(IsolatedAsyncioTestCase):
    async def test_subscription_and_event_share_connection_and_route_by_id(self) -> None:
        credentials = BinanceCredentials("public-api-key", "private-secret")
        # 通知允许与订阅响应竞速并先于后者到达。
        socket = FakeConnection(
            envelope(account_position()),
            subscription_response(),
        )
        connector = FakeConnector(socket)
        stream = BinanceSpotUserDataStream(
            credentials,
            connector=connector,
            clock_ms=lambda: 1_725_000_000_000,
            reconnect_delay_seconds=0,
        )
        iterator = stream.events()

        event = await anext(iterator)

        self.assertIsInstance(event, BinanceOutboundAccountPosition)
        self.assertTrue(stream.connected)
        self.assertEqual(stream.subscription_id, 7)
        self.assertEqual(len(socket.sent), 1)
        request = json.loads(str(socket.sent[0]))
        self.assertEqual(request["id"], 1)
        self.assertEqual(request["method"], "userDataStream.subscribe.signature")
        self.assertEqual(request["params"]["apiKey"], "public-api-key")
        self.assertEqual(request["params"]["timestamp"], 1_725_000_000_000)
        self.assertEqual(request["params"]["recvWindow"], 5_000)
        payload = (
            "apiKey=public-api-key&recvWindow=5000&timestamp=1725000000000"
        )
        expected = hmac.new(
            b"private-secret",
            payload.encode(),
            hashlib.sha256,
        ).hexdigest()
        self.assertEqual(request["params"]["signature"], expected)
        self.assertEqual(connector.calls[0][0], TESTNET_USER_WS_API_URL)

        await stream.aclose()
        with self.assertRaises(StopAsyncIteration):
            await anext(iterator)
        self.assertTrue(socket.context_exited)

    async def test_disconnect_reconnects_and_resubscribes_with_new_request_id(self) -> None:
        first = FakeConnection(subscription_response(1), ConnectionError("first lost"))
        second = FakeConnection(
            subscription_response(2, 8),
            envelope(execution_report(X="FILLED", z="1.00000000"), 8),
        )
        connector = FakeConnector(first, second)
        sleeps: list[float] = []

        async def no_wait(delay: float) -> None:
            sleeps.append(delay)

        stream = BinanceSpotUserDataStream(
            BinanceCredentials("api", "secret"),
            connector=connector,
            clock_ms=lambda: 123,
            max_reconnect_attempts=2,
            reconnect_delay_seconds=0.25,
            sleep=no_wait,
        )
        iterator = stream.events()

        event = await anext(iterator)

        self.assertIsInstance(event, BinanceExecutionReport)
        self.assertEqual(event.status, OrderStatus.FILLED)
        self.assertEqual(stream.connection_epoch, 2)
        self.assertEqual(len(connector.calls), 2)
        self.assertEqual(sleeps, [0.25])
        first_request = json.loads(str(first.sent[0]))
        second_request = json.loads(str(second.sent[0]))
        self.assertEqual((first_request["id"], second_request["id"]), (1, 2))
        self.assertEqual(
            first_request["method"],
            "userDataStream.subscribe.signature",
        )
        self.assertEqual(
            second_request["method"],
            "userDataStream.subscribe.signature",
        )
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

        stream = BinanceSpotUserDataStream(
            BinanceCredentials("api", "secret"),
            connector=connector,
            max_reconnect_attempts=2,
            reconnect_delay_seconds=0.1,
            sleep=no_wait,
        )

        with self.assertRaisesRegex(
            BinanceUserStreamConnectionError,
            "limit exhausted",
        ):
            await anext(stream.events())

        self.assertEqual(len(connector.calls), 3)
        self.assertEqual(sleeps, [0.1, 0.2])

    async def test_protocol_error_is_not_reconnected_or_hidden(self) -> None:
        socket = FakeConnection(subscription_response(99))
        connector = FakeConnector(socket, FakeConnection(subscription_response(2)))
        stream = BinanceSpotUserDataStream(
            BinanceCredentials("api", "secret"),
            connector=connector,
            reconnect_delay_seconds=0,
        )

        with self.assertRaisesRegex(BinanceProtocolError, "id does not match"):
            await anext(stream.events())

        self.assertEqual(len(connector.calls), 1)

    async def test_subscription_rejection_redacts_credentials_and_signature(self) -> None:
        api_key = "api-key-must-not-leak"
        secret = "secret-must-not-leak"
        socket = FakeConnection(
            json.dumps(
                {
                    "id": 1,
                    "status": 401,
                    "error": {
                        "code": -2015,
                        "msg": f"apiKey={api_key} secret={secret} signature=abc123",
                    },
                }
            )
        )
        stream = BinanceSpotUserDataStream(
            BinanceCredentials(api_key, secret),
            connector=FakeConnector(socket),
            clock_ms=lambda: 123,
            reconnect_delay_seconds=0,
        )

        with self.assertRaises(BinanceUserStreamAPIError) as captured:
            await anext(stream.events())

        message = str(captured.exception)
        representation = repr(captured.exception)
        self.assertNotIn(api_key, message + representation)
        self.assertNotIn(secret, message + representation)
        sent = json.loads(str(socket.sent[0]))
        self.assertNotIn(sent["params"]["signature"], message + representation)

    async def test_event_subscription_id_mismatch_is_not_swallowed(self) -> None:
        socket = FakeConnection(
            subscription_response(subscription_id=7),
            envelope(account_position(), subscription_id=8),
        )
        connector = FakeConnector(socket)
        stream = BinanceSpotUserDataStream(
            BinanceCredentials("api", "secret"),
            connector=connector,
            reconnect_delay_seconds=0,
        )

        with self.assertRaisesRegex(BinanceProtocolError, "active subscription"):
            await anext(stream.events())

        self.assertEqual(len(connector.calls), 1)

    async def test_terminated_event_is_yielded_then_stops_without_reconnect(self) -> None:
        socket = FakeConnection(
            subscription_response(),
            envelope({"e": "eventStreamTerminated", "E": 99}),
        )
        connector = FakeConnector(socket, ConnectionError("must not reconnect"))
        stream = BinanceSpotUserDataStream(
            BinanceCredentials("api", "secret"),
            connector=connector,
            reconnect_delay_seconds=0,
        )
        iterator = stream.events()

        event = await anext(iterator)
        self.assertIsInstance(event, BinanceEventStreamTerminated)
        with self.assertRaises(StopAsyncIteration):
            await anext(iterator)

        self.assertEqual(len(connector.calls), 1)
