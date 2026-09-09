import hashlib
import hmac
import json
from collections import deque
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase
from urllib.parse import parse_qs, urlsplit

from gribuki_trade.adapters.binance.auth.envs import BinanceProduct, BinanceStage
from gribuki_trade.adapters.binance.futures.client import (
    BinanceFuturesProtectionOrder,
    BinanceFuturesRestClient,
)
from gribuki_trade.adapters.binance.models import BinanceCredentials
from gribuki_trade.adapters.binance.transport.gateway import (
    BinanceAPIError,
    BinanceConfigurationError,
    BinanceProtocolError,
)
from gribuki_trade.adapters.binance.transport.http import HttpRequest, HttpResponse


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
    async def test_risk_settings_use_signed_product_specific_endpoints(self) -> None:
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        transport = FakeTransport(
            response(200, {"leverage": 10}),
            response(200, {"code": 200}),
            response(200, {"code": 200}),
            response(200, {"multiAssetsMargin": False}),
            response(200, {"code": 200}),
        )
        client = BinanceFuturesRestClient(credentials=credentials, transport=transport)
        await client.set_leverage("BTCUSDT", 10)
        await client.set_margin_type("BTCUSDT", "isolated")
        await client.set_position_mode(True)
        self.assertFalse(await client.multi_assets_mode())
        await client.set_multi_assets_mode(False)
        paths = [urlsplit(request.url).path for request in transport.requests]
        self.assertEqual(
            paths,
            [
                "/fapi/v1/leverage",
                "/fapi/v1/marginType",
                "/fapi/v1/positionSide/dual",
                "/fapi/v1/multiAssetsMargin",
                "/fapi/v1/multiAssetsMargin",
            ],
        )
        self.assertTrue(all("signature=" in request.url for request in transport.requests))

    async def test_structured_protection_order_maps_to_stop_market(self) -> None:
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        transport = FakeTransport(
            response(200, {"dualSidePosition": True}),
            response(200, {"algoId": 9, "algoStatus": "NEW", "orderType": "STOP_MARKET"}),
        )
        client = BinanceFuturesRestClient(credentials=credentials, transport=transport)
        result = await client.submit_protection_order(
            BinanceFuturesProtectionOrder(
                kind="stop_loss",
                symbol="BTCUSDT",
                side="SELL",
                position_side="LONG",
                stop_price="60000",
            )
        )
        self.assertEqual(result["algoId"], 9)
        query = parse_qs(urlsplit(transport.requests[1].url).query)
        self.assertEqual(query["type"], ["STOP_MARKET"])
        self.assertEqual(query["triggerPrice"], ["60000"])
        self.assertEqual(query["closePosition"], ["true"])

    async def test_algo_trailing_rate_is_capped_at_ten_percent(self) -> None:
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        client = BinanceFuturesRestClient(
            credentials=credentials,
            transport=FakeTransport(response(200, {"dualSidePosition": False})),
        )
        with self.assertRaisesRegex(ValueError, "0.1 and 10"):
            await client.submit_trailing_stop(
                symbol="BTCUSDT", side="SELL", callback_rate="10.1", quantity="0.001"
            )

    async def test_algo_order_family_uses_algo_routes_and_ten_percent_cap(self) -> None:
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        transport = FakeTransport(
            response(200, {"dualSidePosition": False}),
            response(200, {"algoId": 1}),
            response(200, [{"algoId": 1}]),
            response(200, {"algoId": 1}),
            response(200, {"code": 200}),
        )
        client = BinanceFuturesRestClient(credentials=credentials, transport=transport)
        await client.submit_algo_trailing_stop(
            symbol="BTCUSDT", side="SELL", callback_rate="10", quantity="0.001"
        )
        await client.open_algo_orders("BTCUSDT")
        await client.get_algo_order("BTCUSDT", algo_id=1)
        await client.cancel_algo_order("BTCUSDT", algo_id=1)
        self.assertEqual(
            [urlsplit(item.url).path for item in transport.requests],
            [
                "/fapi/v1/positionSide/dual",
                "/fapi/v1/algoOrder",
                "/fapi/v1/openAlgoOrders",
                "/fapi/v1/algoOrder",
                "/fapi/v1/algoOrder",
            ],
        )
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

    async def test_time_synchronization_exposes_round_trip_latency(self) -> None:
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        transport = FakeTransport(response(200, {"serverTime": 1_700_000_010_000}))
        clock_values = iter((1_700_000_000_000, 1_700_000_000_120))
        client = BinanceFuturesRestClient(
            credentials=credentials,
            transport=transport,
            clock_ms=lambda: next(clock_values),
        )

        offset = await client.synchronize_time()

        self.assertEqual(offset, 9_940)
        self.assertEqual(client.last_time_sync_rtt_ms, 120)

    async def test_calibrate_time_selects_lowest_rtt_sample(self) -> None:
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        transport = FakeTransport(
            response(200, {"serverTime": 1_700_000_010_000}),
            response(200, {"serverTime": 1_700_000_010_040}),
        )
        clock_values = iter(
            (
                1_700_000_000_000,
                1_700_000_000_300,
                1_700_000_000_400,
                1_700_000_000_420,
            )
        )
        client = BinanceFuturesRestClient(
            credentials=credentials,
            transport=transport,
            clock_ms=lambda: next(clock_values),
        )

        result = await client.calibrate_time(samples=2)

        self.assertEqual(result.samples, 2)
        self.assertEqual(result.rtt_ms, 20)
        self.assertEqual(result.offset_ms, 9_630)
        self.assertEqual(client.server_time_offset_ms, result.offset_ms)

    async def test_validate_order_calls_only_test_order_endpoint(self) -> None:
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        transport = FakeTransport(
            response(200, {"dualSidePosition": False}), response(200, {})
        )
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
        self.assertEqual(urlsplit(transport.requests[0].url).path, "/fapi/v1/positionSide/dual")
        request = transport.requests[1]
        self.assertEqual(request.method, "POST")
        self.assertEqual(urlsplit(request.url).path, "/fapi/v1/order/test")
        query = parse_qs(urlsplit(request.url).query)
        self.assertEqual(query["type"], ["LIMIT"])
        self.assertEqual(query["reduceOnly"], ["false"])
        self.assertEqual(query["positionSide"], ["BOTH"])

    async def test_position_side_mode_is_signed_and_rejects_malformed_payload(self) -> None:
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        transport = FakeTransport(response(200, {"dualSidePosition": True}))
        client = BinanceFuturesRestClient(
            credentials=credentials,
            transport=transport,
            clock_ms=lambda: 1_700_000_000_000,
        )

        self.assertTrue(await client.position_side_mode())
        self.assertEqual(urlsplit(transport.requests[0].url).path, "/fapi/v1/positionSide/dual")
        self.assertIn("signature=", transport.requests[0].url)

        malformed = BinanceFuturesRestClient(
            credentials=credentials,
            transport=FakeTransport(response(200, {"dualSidePosition": "true"})),
        )
        with self.assertRaisesRegex(
            BinanceProtocolError, "position side mode response is malformed"
        ):
            await malformed.position_side_mode()

    async def test_order_mode_preflight_requires_explicit_hedge_side_and_rejects_reduce_only(
        self,
    ) -> None:
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        missing_side = BinanceFuturesRestClient(
            credentials=credentials,
            transport=FakeTransport(response(200, {"dualSidePosition": True})),
        )
        with self.assertRaisesRegex(ValueError, "LONG or SHORT"):
            await missing_side.submit_order(symbol="BTCUSDT", side="BUY", type="MARKET")

        reduce_only = BinanceFuturesRestClient(
            credentials=credentials,
            transport=FakeTransport(response(200, {"dualSidePosition": True})),
        )
        with self.assertRaisesRegex(ValueError, "reduce_only is not allowed"):
            await reduce_only.validate_order(
                symbol="BTCUSDT",
                side="BUY",
                order_type="MARKET",
                quantity="0.001",
                position_side="LONG",
                reduce_only=False,
            )

    async def test_one_way_rejects_hedge_side_before_order_write(self) -> None:
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        transport = FakeTransport(response(200, {"dualSidePosition": False}))
        client = BinanceFuturesRestClient(credentials=credentials, transport=transport)
        with self.assertRaisesRegex(ValueError, "BOTH in One-way"):
            await client.submit_order(
                symbol="BTCUSDT", side="BUY", type="MARKET", position_side="LONG"
            )
        self.assertEqual(len(transport.requests), 1)

    async def test_api_error_redacts_credentials_and_signature_assignments(self) -> None:
        credentials = BinanceCredentials(
            api_key="offline-api-key", secret_key="offline-secret-key"
        )
        client = BinanceFuturesRestClient(
            credentials=credentials,
            transport=FakeTransport(
                response(
                    400,
                    {
                        "code": -1100,
                        "msg": "apiKey=offline-api-key signature=private-signature",
                    },
                )
            ),
        )
        with self.assertRaises(BinanceAPIError) as context:
            await client.position_side_mode()
        rendered = str(context.exception)
        self.assertNotIn("offline-api-key", rendered)
        self.assertNotIn("offline-secret-key", rendered)
        self.assertNotIn("private-signature", rendered)
        self.assertIn("apiKey=<redacted>", rendered)

    async def test_live_order_cancel_and_reconciliation_routes_are_signed(self) -> None:
        credentials = BinanceCredentials(api_key="offline-key", secret_key="offline-secret")
        transport = FakeTransport(
            response(200, {"dualSidePosition": False}),
            response(200, {"orderId": 7, "status": "NEW"}),
            response(200, {"orderId": 7, "status": "NEW"}),
            response(200, [{"orderId": 7}]),
            response(200, [{"id": 9}]),
            response(200, {"orderId": 7, "status": "CANCELED"}),
        )
        client = BinanceFuturesRestClient(
            stage=BinanceStage.LIVE,
            credentials=credentials,
            allow_live=True,
            transport=transport,
            clock_ms=lambda: 1_700_000_000_000,
        )

        await client.submit_order(
            symbol="BTCUSDT",
            side="BUY",
            type="LIMIT",
            quantity="0.001",
            price="60000",
            time_in_force="GTC",
            client_order_id="local-1",
        )
        await client.get_order("BTCUSDT", order_id=7)
        await client.all_orders("BTCUSDT")
        await client.account_trades("BTCUSDT")
        await client.cancel_order("BTCUSDT", client_order_id="local-1")

        self.assertEqual(transport.requests[0].method, "GET")
        self.assertEqual(urlsplit(transport.requests[0].url).path, "/fapi/v1/positionSide/dual")
        self.assertEqual(transport.requests[1].method, "POST")
        self.assertEqual(transport.requests[5].method, "DELETE")
        self.assertTrue(all("signature=" in request.url for request in transport.requests))
