import hashlib
import hmac
import json
from collections import deque
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase
from urllib.parse import parse_qs, urlsplit

from gribuki_trade.adapters.binance.envs import BinanceProduct, BinanceStage
from gribuki_trade.adapters.binance.futures import BinanceFuturesRestClient
from gribuki_trade.adapters.binance.gateway import BinanceConfigurationError
from gribuki_trade.adapters.binance.http import HttpRequest, HttpResponse
from gribuki_trade.adapters.binance.models import BinanceCredentials


def response(status: int, payload: object) -> HttpResponse:
    return HttpResponse(status_code=status, body=json.dumps(payload).encode())


class FakeTransport:
    def __init__(self, *responses: HttpResponse | Exception) -> None:
        self.responses = deque(responses)
        self.requests: list[HttpRequest] = []

    async def request(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        result = self.responses.popleft()
        if isinstance(result, Exception):
            raise result
        return result


class BinanceFuturesValueTests(TestCase):
    def test_defaults_to_usds_demo_and_live_requires_explicit_opt_in(self) -> None:
        client = BinanceFuturesRestClient()

        self.assertEqual(client.product, BinanceProduct.USDS_FUTURES)
        self.assertEqual(client.stage, BinanceStage.DEMO)
        self.assertEqual(client.base_url, "https://demo-fapi.binance.com")

        with self.assertRaisesRegex(BinanceConfigurationError, "allow_live=True"):
            BinanceFuturesRestClient(stage=BinanceStage.LIVE)

    def test_unsupported_futures_testnet_never_falls_back_to_live(self) -> None:
        with self.assertRaisesRegex(BinanceConfigurationError, "refusing to fall back"):
            BinanceFuturesRestClient(stage=BinanceStage.TESTNET)

    def test_non_futures_product_is_rejected(self) -> None:
        with self.assertRaisesRegex(BinanceConfigurationError, "only USD-M or COIN-M"):
            BinanceFuturesRestClient(product=BinanceProduct.SPOT)


class BinanceFuturesClientTests(IsolatedAsyncioTestCase):
    async def test_public_usds_ticker_uses_demo_without_credentials(self) -> None:
        transport = FakeTransport(
            response(200, {"symbol": "BTCUSDT", "price": "64123.40", "time": 12})
        )
        client = BinanceFuturesRestClient(transport=transport)

        ticker = await client.ticker_price("btcusdt")

        self.assertEqual(ticker.price, Decimal("64123.40"))
        request = transport.requests[0]
        self.assertEqual(urlsplit(request.url).netloc, "demo-fapi.binance.com")
        self.assertEqual(urlsplit(request.url).path, "/fapi/v1/ticker/price")
        self.assertNotIn("X-MBX-APIKEY", request.headers)

    async def test_coin_m_routes_are_product_specific(self) -> None:
        transport = FakeTransport(response(200, {}), response(200, []))
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        client = BinanceFuturesRestClient(
            product=BinanceProduct.COIN_FUTURES,
            credentials=credentials,
            transport=transport,
            clock_ms=lambda: 1_700_000_000_000,
        )

        await client.account()
        await client.position_risk("btcusd_perp")

        self.assertEqual(urlsplit(transport.requests[0].url).path, "/dapi/v1/account")
        self.assertEqual(urlsplit(transport.requests[1].url).path, "/dapi/v1/positionRisk")
        self.assertTrue(
            all(
                urlsplit(request.url).netloc == "demo-dapi.binance.com"
                for request in transport.requests
            )
        )

    async def test_coin_m_single_symbol_ticker_accepts_one_element_array(self) -> None:
        transport = FakeTransport(
            response(200, [{"symbol": "BTCUSD_PERP", "price": "63000.1", "time": 12}])
        )
        client = BinanceFuturesRestClient(
            product=BinanceProduct.COIN_FUTURES,
            transport=transport,
        )

        ticker = await client.ticker_price("BTCUSD_PERP")

        self.assertEqual(ticker.symbol, "BTCUSD_PERP")
        self.assertEqual(ticker.price, Decimal("63000.1"))

    async def test_signed_account_uses_timestamp_recv_window_header_and_hmac(self) -> None:
        credentials = BinanceCredentials(
            api_key="offline-api-placeholder", secret_key="offline-secret-placeholder"
        )
        transport = FakeTransport(response(200, {"assets": [], "positions": []}))
        client = BinanceFuturesRestClient(
            credentials=credentials,
            transport=transport,
            clock_ms=lambda: 1_700_000_000_123,
        )

        await client.account()

        request = transport.requests[0]
        query = urlsplit(request.url).query
        unsigned, signature = query.rsplit("&signature=", 1)
        expected = hmac.new(
            credentials.secret_key.encode(), unsigned.encode(), hashlib.sha256
        ).hexdigest()
        self.assertEqual(signature, expected)
        self.assertEqual(request.headers["X-MBX-APIKEY"], credentials.api_key)
        self.assertEqual(parse_qs(query)["recvWindow"], ["5000"])
        self.assertEqual(parse_qs(query)["timestamp"], ["1700000000123"])

    async def test_validate_order_calls_only_test_order_endpoint(self) -> None:
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        transport = FakeTransport(response(200, {}))
        client = BinanceFuturesRestClient(
            credentials=credentials,
            transport=transport,
            clock_ms=lambda: 1_700_000_000_000,
        )

        result = await client.validate_order(
            symbol="btcusdt",
            side="buy",
            order_type="limit",
            quantity=Decimal("0.001"),
            price=Decimal("60000.0"),
            time_in_force="gtc",
            reduce_only=False,
        )

        self.assertEqual(result, {})
        request = transport.requests[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(urlsplit(request.url).path, "/fapi/v1/order/test")
        query = parse_qs(urlsplit(request.url).query)
        self.assertEqual(query["type"], ["LIMIT"])
        self.assertEqual(query["reduceOnly"], ["false"])
