import asyncio
import hashlib
import hmac
import json
from collections import deque
from datetime import UTC, datetime
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase
from urllib.parse import parse_qs, urlsplit

from gribuki_trade.adapters.binance import (
    LIVE_REST_BASE_URL,
    TESTNET_REST_BASE_URL,
    BinanceAPIError,
    BinanceCancelReplaceResult,
    BinanceConfigurationError,
    BinanceCredentials,
    BinanceEnvironment,
    BinanceSpotGateway,
    BinanceSpotOcoRequest,
    BinanceSpotOrderLeg,
    BinanceSpotOtocoRequest,
    BinanceSpotOtoRequest,
    HttpRequest,
    HttpResponse,
    SymbolRules,
    decimal_to_fixed,
    sign_hmac_sha256,
)
from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side


def response(
    status: int,
    payload: object,
    *,
    headers: dict[str, str] | None = None,
) -> HttpResponse:
    return HttpResponse(
        status_code=status,
        body=json.dumps(payload).encode(),
        headers=headers or {},
    )


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


def exchange_info() -> dict[str, object]:
    return {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "isSpotTradingAllowed": True,
                "orderTypes": ["LIMIT", "MARKET"],
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "minPrice": "0.01",
                        "maxPrice": "1000000",
                        "tickSize": "0.01",
                    },
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.00001",
                        "maxQty": "1000",
                        "stepSize": "0.00001",
                    },
                    {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                    {
                        "filterType": "PERCENT_PRICE_BY_SIDE",
                        "bidMultiplierUp": "1.2",
                        "bidMultiplierDown": "0.8",
                        "askMultiplierUp": "1.3",
                        "askMultiplierDown": "0.7",
                        "avgPriceMins": 5,
                    },
                    {"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 10},
                    {"filterType": "MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders": 5},
                    {"filterType": "MAX_POSITION", "maxPosition": "2"},
                ],
            }
        ]
    }


def make_order(
    client_order_id: str = "offline-order-1",
    *,
    quantity: Decimal = Decimal("0.01000000"),
    price: Decimal = Decimal("50000.00"),
) -> OrderIntent:
    return OrderIntent(
        client_order_id=client_order_id,
        account_id="offline",
        strategy_id="unit",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=quantity,
        limit_price=price,
        created_at=datetime(2026, 8, 13, tzinfo=UTC),
    )


class BinanceValueTests(TestCase):
    def test_hmac_sha256_and_fixed_decimal_are_exact(self) -> None:
        payload = "symbol=BTCUSDT&quantity=0.01000000&timestamp=123"
        placeholder = "offline-placeholder-not-a-credential"

        expected = hmac.new(placeholder.encode(), payload.encode(), hashlib.sha256).hexdigest()

        self.assertEqual(sign_hmac_sha256(placeholder, payload), expected)
        self.assertEqual(decimal_to_fixed(Decimal("1E-8")), "0.00000001")
        self.assertEqual(decimal_to_fixed(Decimal("1.2300")), "1.2300")

    def test_live_requires_explicit_constructor_opt_in(self) -> None:
        with self.assertRaisesRegex(BinanceConfigurationError, "allow_live=True"):
            BinanceSpotGateway(environment=BinanceEnvironment.LIVE)

        gateway = BinanceSpotGateway(environment="live", allow_live=True)

        self.assertEqual(gateway.base_url, LIVE_REST_BASE_URL)
        self.assertNotIn("secret", repr(gateway).lower())

    def test_credentials_and_http_objects_redact_values(self) -> None:
        credentials = BinanceCredentials(
            api_key="offline-api-placeholder", secret_key="offline-secret-placeholder"
        )
        request = HttpRequest(
            method="GET",
            url="https://example.invalid/path?signature=private-signature",
            headers={"X-MBX-APIKEY": credentials.api_key},
            body=b"secret=private",
        )

        combined = repr(credentials) + repr(request)
        self.assertNotIn(credentials.api_key, combined)
        self.assertNotIn(credentials.secret_key, combined)
        self.assertNotIn("private-signature", combined)
        self.assertNotIn("secret=private", combined)

    def test_symbol_rules_reject_decimal_increment_and_notional(self) -> None:
        rules = SymbolRules.from_exchange_info(exchange_info()["symbols"][0])  # type: ignore[index]

        with self.assertRaisesRegex(ValueError, "increment"):
            rules.validate_limit_order(quantity=Decimal("0.01000"), price=Decimal("50000.001"))
        with self.assertRaisesRegex(ValueError, "notional"):
            rules.validate_limit_order(quantity=Decimal("0.00001"), price=Decimal("10.00"))

    def test_symbol_rules_enforce_dynamic_price_order_and_position_limits(self) -> None:
        rules = SymbolRules.from_exchange_info(exchange_info()["symbols"][0])  # type: ignore[index]

        self.assertTrue(rules.requires_reference_price)
        self.assertEqual(rules.average_price_minutes, 5)
        self.assertEqual(rules.maximum_open_orders, 10)
        self.assertEqual(rules.maximum_algo_orders, 5)
        with self.assertRaisesRegex(ValueError, "dynamic maximum"):
            rules.validate_limit_order(
                quantity=Decimal("0.10"),
                price=Decimal("130.00"),
                side="BUY",
                weighted_average_price=Decimal("100"),
            )
        with self.assertRaisesRegex(ValueError, "open-order limit"):
            rules.validate_limit_order(
                quantity=Decimal("0.10"),
                price=Decimal("100.00"),
                open_order_count=10,
            )
        with self.assertRaisesRegex(ValueError, "position would exceed"):
            rules.validate_limit_order(
                quantity=Decimal("0.10"),
                price=Decimal("100.00"),
                side="BUY",
                current_position=Decimal("1.95"),
            )


class BinanceGatewayTests(IsolatedAsyncioTestCase):
    async def test_account_preserves_permissions_identity_and_safety_flags(self) -> None:
        transport = FakeTransport(
            response(
                200,
                {
                    "canTrade": True,
                    "canWithdraw": False,
                    "canDeposit": True,
                    "accountType": "SPOT",
                    "uid": 123,
                    "permissions": ["SPOT"],
                    "makerCommission": 10,
                    "takerCommission": 20,
                    "buyerCommission": 0,
                    "sellerCommission": 0,
                    "brokered": False,
                    "requireSelfTradePrevention": True,
                    "preventSor": True,
                    "balances": [],
                },
            )
        )
        gateway = BinanceSpotGateway(
            credentials=BinanceCredentials("offline-api", "offline-secret"),
            transport=transport,
        )

        account = await gateway.account()

        self.assertEqual(account.permissions, ("SPOT",))
        self.assertEqual(account.uid, 123)
        self.assertEqual(account.maker_commission, 10)
        self.assertTrue(account.require_self_trade_prevention)
        self.assertTrue(account.prevent_sor)

    async def test_reconciliation_queries_parse_orders_trades_and_commission(self) -> None:
        order = {
            "symbol": "BTCUSDT",
            "orderId": 9,
            "clientOrderId": "reconcile-1",
            "status": "PARTIALLY_FILLED",
            "side": "BUY",
            "price": "50000.00",
            "origQty": "0.01000000",
            "executedQty": "0.00400000",
            "cummulativeQuoteQty": "200.00000000",
            "time": 11,
        }
        trade = {
            "symbol": "BTCUSDT",
            "id": 77,
            "orderId": 9,
            "price": "50000.00",
            "qty": "0.00400000",
            "quoteQty": "200.00000000",
            "commission": "0.00000400",
            "commissionAsset": "BTC",
            "time": 12,
            "isBuyer": True,
            "isMaker": False,
            "isBestMatch": True,
        }
        commission = {
            "symbol": "BTCUSDT",
            "standardCommission": {
                "maker": "0.001",
                "taker": "0.001",
                "buyer": "0",
                "seller": "0",
            },
            "taxCommission": {"maker": "0", "taker": "0", "buyer": "0", "seller": "0"},
            "specialCommission": {
                "maker": "0",
                "taker": "0",
                "buyer": "0",
                "seller": "0",
            },
            "discount": {
                "enabledForAccount": True,
                "enabledForSymbol": True,
                "discountAsset": "BNB",
                "discount": "0.25",
            },
        }
        transport = FakeTransport(
            response(200, [order]),
            response(200, [order]),
            response(200, [trade]),
            response(200, commission),
        )
        gateway = BinanceSpotGateway(
            credentials=BinanceCredentials("offline-api", "offline-secret"),
            transport=transport,
        )

        open_orders = await gateway.open_orders("BTCUSDT")
        all_orders = await gateway.all_orders("BTCUSDT", limit=10)
        trades = await gateway.account_trades("BTCUSDT", limit=10)
        rates = await gateway.commission_rate("BTCUSDT")

        self.assertEqual(open_orders[0].cumulative_quote_quantity, Decimal("200.00000000"))
        self.assertIs(all_orders[0].status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(trades[0].trade_id, 77)
        self.assertEqual(trades[0].commission_asset, "BTC")
        self.assertEqual(rates.standard.taker, Decimal("0.001"))
        self.assertEqual(rates.discount.asset, "BNB")
        self.assertEqual(
            [urlsplit(value.url).path for value in transport.requests],
            [
                "/api/v3/openOrders",
                "/api/v3/allOrders",
                "/api/v3/myTrades",
                "/api/v3/account/commission",
            ],
        )

    async def test_rate_limit_headers_are_exposed_without_resetting_prior_values(self) -> None:
        transport = FakeTransport(
            response(
                200,
                {"serverTime": 1},
                headers={
                    "X-MBX-USED-WEIGHT-1M": "18",
                    "X-MBX-ORDER-COUNT-10S": "2",
                },
            ),
            response(
                429,
                {"code": -1003, "msg": "too many requests"},
                headers={"Retry-After": "7"},
            ),
        )
        gateway = BinanceSpotGateway(transport=transport)

        await gateway.server_time()
        self.assertEqual(gateway.rate_limit_usage.used_weight_1m, 18)
        self.assertEqual(gateway.rate_limit_usage.order_count_10s, 2)
        with self.assertRaises(BinanceAPIError):
            await gateway.ticker_price("BTCUSDT")
        self.assertEqual(gateway.rate_limit_usage.used_weight_1m, 18)
        self.assertEqual(gateway.rate_limit_usage.retry_after_seconds, 7)

    async def test_public_market_data_needs_no_credentials(self) -> None:
        transport = FakeTransport(response(200, {"symbol": "BTCUSDT", "price": "123.4500"}))
        gateway = BinanceSpotGateway(transport=transport)

        ticker = await gateway.ticker_price("btcusdt")

        self.assertEqual(gateway.base_url, TESTNET_REST_BASE_URL)
        self.assertEqual(ticker.price, Decimal("123.4500"))
        request = transport.requests[0]
        self.assertEqual(request.method, "GET")
        self.assertEqual(urlsplit(request.url).path, "/api/v3/ticker/price")
        self.assertNotIn("X-MBX-APIKEY", request.headers)
        self.assertIsNone(request.body)

    async def test_signed_account_request_uses_header_timestamp_and_hmac(self) -> None:
        credentials = BinanceCredentials(
            api_key="offline-api-placeholder", secret_key="offline-secret-placeholder"
        )
        transport = FakeTransport(
            response(
                200,
                {
                    "canTrade": True,
                    "canWithdraw": False,
                    "canDeposit": True,
                    "accountType": "SPOT",
                    "updateTime": 12,
                    "balances": [{"asset": "USDT", "free": "2.50", "locked": "0"}],
                },
            )
        )
        gateway = BinanceSpotGateway(
            credentials=credentials,
            transport=transport,
            clock_ms=lambda: 1_700_000_000_123,
        )

        account = await gateway.account()

        request = transport.requests[0]
        query = urlsplit(request.url).query
        unsigned, signature = query.rsplit("&signature=", 1)
        self.assertEqual(
            signature,
            sign_hmac_sha256(credentials.secret_key, unsigned),
        )
        self.assertEqual(request.headers["X-MBX-APIKEY"], credentials.api_key)
        self.assertEqual(parse_qs(query)["timestamp"], ["1700000000123"])
        self.assertEqual(account.balances[0].free, Decimal("2.50"))

    async def test_timestamp_rejection_synchronizes_once_then_retries(self) -> None:
        credentials = BinanceCredentials(
            api_key="offline-api-placeholder", secret_key="offline-secret-placeholder"
        )
        transport = FakeTransport(
            response(400, {"code": -1021, "msg": "timestamp outside recvWindow"}),
            response(200, {"serverTime": 1_700_000_010_000}),
            response(
                200,
                {
                    "canTrade": True,
                    "canWithdraw": False,
                    "canDeposit": True,
                    "accountType": "SPOT",
                    "balances": [],
                },
            ),
        )
        clock_values = iter(
            [
                1_700_000_000_000,
                1_700_000_000_100,
                1_700_000_000_200,
                1_700_000_000_300,
            ]
        )
        gateway = BinanceSpotGateway(
            credentials=credentials,
            transport=transport,
            clock_ms=lambda: next(clock_values),
        )

        account = await gateway.account()

        self.assertTrue(account.can_trade)
        self.assertEqual(gateway.server_time_offset_ms, 9_850)
        self.assertEqual(gateway.last_time_sync_rtt_ms, 100)
        first_signed = parse_qs(urlsplit(transport.requests[0].url).query)
        retried_signed = parse_qs(urlsplit(transport.requests[2].url).query)
        self.assertEqual(first_signed["timestamp"], ["1700000000000"])
        self.assertEqual(retried_signed["timestamp"], ["1700000010150"])

    async def test_exchange_order_validation_does_not_track_or_place_order(self) -> None:
        credentials = BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder")
        transport = FakeTransport(
            response(200, exchange_info()),
            response(200, {}),
        )
        gateway = BinanceSpotGateway(
            credentials=credentials,
            transport=transport,
            clock_ms=lambda: 1_700_000_000_000,
        )
        await gateway.connect()
        intent = make_order()

        await gateway.validate_order_on_exchange(intent)

        request = transport.requests[-1]
        self.assertEqual(request.method, "POST")
        self.assertEqual(urlsplit(request.url).path, "/api/v3/order/test")
        body = parse_qs(request.body.decode())
        self.assertEqual(body["newClientOrderId"], [intent.client_order_id])
        self.assertIn("signature", body)
        self.assertIsNone(gateway.order_update(intent.client_order_id))

    async def test_limit_submission_is_decimal_fixed_and_emits_accepted(self) -> None:
        credentials = BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder")
        transport = FakeTransport(
            response(200, exchange_info()),
            response(
                200,
                {
                    "symbol": "BTCUSDT",
                    "orderId": 42,
                    "clientOrderId": "offline-order-1",
                    "status": "NEW",
                    "side": "BUY",
                    "price": "50000.00",
                    "origQty": "0.01000000",
                    "executedQty": "0",
                },
            ),
        )
        gateway = BinanceSpotGateway(
            credentials=credentials, transport=transport, clock_ms=lambda: 7
        )
        await gateway.connect()

        await gateway.submit_order(make_order())
        event = await anext(gateway.events())

        self.assertIs(event.payload.status, OrderStatus.ACCEPTED)
        self.assertEqual(event.payload.exchange_order_id, 42)
        order_request = transport.requests[1]
        body = order_request.body.decode() if order_request.body is not None else ""
        unsigned, signature = body.rsplit("&signature=", 1)
        self.assertIn("quantity=0.01000000", unsigned)
        self.assertIn("price=50000.00", unsigned)
        self.assertEqual(signature, sign_hmac_sha256(credentials.secret_key, unsigned))
        self.assertNotIn("signature=", order_request.url)

    async def test_filter_rejection_never_calls_order_endpoint(self) -> None:
        transport = FakeTransport(response(200, exchange_info()))
        gateway = BinanceSpotGateway(
            credentials=BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder"),
            transport=transport,
        )
        await gateway.connect()

        await gateway.submit_order(make_order(price=Decimal("50000.001")))
        event = await anext(gateway.events())

        self.assertIs(event.payload.status, OrderStatus.LOCAL_REJECTED)
        self.assertEqual(len(transport.requests), 1)
        self.assertEqual(urlsplit(transport.requests[0].url).path, "/api/v3/exchangeInfo")

    async def test_api_rejection_preserves_sanitized_code_and_reason(self) -> None:
        transport = FakeTransport(
            response(200, exchange_info()),
            response(400, {"code": -2010, "msg": "Account has insufficient balance."}),
        )
        gateway = BinanceSpotGateway(
            credentials=BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder"),
            transport=transport,
        )
        await gateway.connect()

        await gateway.submit_order(make_order())
        event = await anext(gateway.events())

        self.assertIs(event.payload.status, OrderStatus.BROKER_REJECTED)
        self.assertEqual(event.payload.error_code, -2010)
        self.assertIn("insufficient balance", event.payload.reason or "")

    async def test_5xx_is_unknown_and_duplicate_is_never_resent(self) -> None:
        transport = FakeTransport(response(200, exchange_info()), response(503, {"msg": "busy"}))
        gateway = BinanceSpotGateway(
            credentials=BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder"),
            transport=transport,
        )
        await gateway.connect()
        order = make_order()

        await gateway.submit_order(order)
        event = await anext(gateway.events())
        await gateway.submit_order(order)

        self.assertIs(event.payload.status, OrderStatus.UNKNOWN)
        self.assertIn("reconcile", event.payload.reason)
        self.assertEqual(len(transport.requests), 2)
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(anext(gateway.events()), timeout=0.01)

    async def test_minus_1007_is_unknown_and_error_does_not_leak_credentials(self) -> None:
        credentials = BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder")
        transport = FakeTransport(
            response(200, exchange_info()),
            response(
                504,
                {
                    "code": -1007,
                    "msg": (
                        f"signature={credentials.secret_key} "
                        f"apiKey={credentials.api_key} execution unknown"
                    ),
                },
            ),
        )
        gateway = BinanceSpotGateway(credentials=credentials, transport=transport)
        await gateway.connect()

        await gateway.submit_order(make_order())
        event = await anext(gateway.events())

        rendered = repr(event.payload) + str(event.payload.reason)
        self.assertIs(event.payload.status, OrderStatus.UNKNOWN)
        self.assertNotIn(credentials.api_key, rendered)
        self.assertNotIn(credentials.secret_key, rendered)

    async def test_query_reconciles_unknown_then_cancel_is_signed(self) -> None:
        transport = FakeTransport(
            response(200, exchange_info()),
            response(503, {"msg": "unknown"}),
            response(
                200,
                {
                    "symbol": "BTCUSDT",
                    "orderId": 9,
                    "clientOrderId": "offline-order-1",
                    "status": "NEW",
                    "side": "BUY",
                    "price": "50000.00",
                    "origQty": "0.01000000",
                    "executedQty": "0",
                },
            ),
            response(
                200,
                {
                    "symbol": "BTCUSDT",
                    "orderId": 9,
                    "clientOrderId": "offline-order-1",
                    "status": "CANCELED",
                    "side": "BUY",
                    "price": "50000.00",
                    "origQty": "0.01000000",
                    "executedQty": "0",
                },
            ),
        )
        gateway = BinanceSpotGateway(
            credentials=BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder"),
            transport=transport,
        )
        await gateway.connect()
        await gateway.submit_order(make_order())
        stream = gateway.events()
        self.assertIs((await anext(stream)).payload.status, OrderStatus.UNKNOWN)

        snapshot = await gateway.query_order("offline-order-1")
        self.assertIs(snapshot.status, OrderStatus.ACCEPTED)
        self.assertIs((await anext(stream)).payload.status, OrderStatus.ACCEPTED)

        await gateway.cancel_order("offline-order-1")
        self.assertIs((await anext(stream)).payload.status, OrderStatus.CANCELED)
        self.assertEqual(transport.requests[-1].method, "DELETE")

    async def test_cancel_by_client_id_does_not_require_in_memory_order(self) -> None:
        transport = FakeTransport(
            response(
                200,
                {
                    "symbol": "BTCUSDT",
                    "orderId": 77,
                    "clientOrderId": "restored-order-1",
                    "status": "CANCELED",
                    "side": "BUY",
                    "price": "50000.00",
                    "origQty": "0.01000000",
                    "executedQty": "0",
                },
            )
        )
        gateway = BinanceSpotGateway(
            credentials=BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder"),
            transport=transport,
            clock_ms=lambda: 123_456,
        )
        await gateway.connect()

        snapshot = await gateway.cancel_order_by_client_id(
            "btcusdt",
            "restored-order-1",
        )

        self.assertIs(snapshot.status, OrderStatus.CANCELED)
        request = transport.requests[0]
        self.assertIsNotNone(request.body)
        query = parse_qs(request.body.decode())
        self.assertEqual(request.method, "DELETE")
        self.assertEqual(query["symbol"], ["BTCUSDT"])
        self.assertEqual(query["origClientOrderId"], ["restored-order-1"])
        self.assertIn("signature", query)
        self.assertEqual(request.headers["X-MBX-APIKEY"], "offline-api-placeholder")

    async def test_uncertain_standalone_cancel_waits_for_reconciliation_before_retry(self) -> None:
        transport = FakeTransport(
            response(503, {"msg": "execution status unknown"}),
            response(
                200,
                {
                    "symbol": "BTCUSDT",
                    "orderId": 77,
                    "clientOrderId": "restored-order-1",
                    "status": "NEW",
                    "side": "BUY",
                    "price": "50000.00",
                    "origQty": "0.01000000",
                    "executedQty": "0",
                },
            ),
            response(
                200,
                {
                    "symbol": "BTCUSDT",
                    "orderId": 77,
                    "clientOrderId": "restored-order-1",
                    "status": "CANCELED",
                    "side": "BUY",
                    "price": "50000.00",
                    "origQty": "0.01000000",
                    "executedQty": "0",
                },
            ),
        )
        gateway = BinanceSpotGateway(
            credentials=BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder"),
            transport=transport,
        )
        await gateway.connect()

        first = await gateway.cancel_order_by_client_id("BTCUSDT", "restored-order-1")
        repeated = await gateway.cancel_order_by_client_id("BTCUSDT", "restored-order-1")

        self.assertIs(first.status, OrderStatus.UNKNOWN)
        self.assertEqual(repeated, first)
        self.assertEqual(len(transport.requests), 1)

        reconciled = await gateway.get_order(
            "BTCUSDT",
            client_order_id="restored-order-1",
        )
        canceled = await gateway.cancel_order_by_client_id(
            "BTCUSDT",
            "restored-order-1",
        )

        self.assertIs(reconciled.status, OrderStatus.ACCEPTED)
        self.assertIs(canceled.status, OrderStatus.CANCELED)
        self.assertEqual(
            [request.method for request in transport.requests],
            ["DELETE", "GET", "DELETE"],
        )

    async def test_conflicting_client_id_is_rejected_without_second_order_call(self) -> None:
        transport = FakeTransport(
            response(200, exchange_info()),
            response(
                200,
                {
                    "symbol": "BTCUSDT",
                    "orderId": 1,
                    "clientOrderId": "offline-order-1",
                    "status": "NEW",
                    "executedQty": "0",
                },
            ),
        )
        gateway = BinanceSpotGateway(
            credentials=BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder"),
            transport=transport,
        )
        await gateway.connect()
        await gateway.submit_order(make_order())

        with self.assertRaisesRegex(ValueError, "different order"):
            await gateway.submit_order(make_order(quantity=Decimal("0.02000000")))

        self.assertEqual(len(transport.requests), 2)

    async def test_submit_spot_order_supports_trailing_stop_and_preserves_params(self) -> None:
        transport = FakeTransport(
            response(
                200,
                {
                    "symbol": "BTCUSDT",
                    "orderId": 11,
                    "clientOrderId": "trail-1",
                    "status": "NEW",
                    "side": "SELL",
                    "price": "0",
                    "origQty": "0.010",
                    "executedQty": "0",
                    "transactTime": 12,
                },
            )
        )
        gateway = BinanceSpotGateway(
            credentials=BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder"),
            transport=transport,
        )
        await gateway.connect()

        snapshot = await gateway.submit_spot_order(
            symbol="BTCUSDT",
            side=Side.SELL,
            order_type="STOP_LOSS",
            quantity=Decimal("0.010"),
            trailing_delta=125,
            client_order_id="trail-1",
        )

        self.assertEqual(snapshot.client_order_id, "trail-1")
        query = parse_qs(transport.requests[0].body.decode())
        self.assertEqual(query["type"], ["STOP_LOSS"])
        self.assertEqual(query["trailingDelta"], ["125"])
        self.assertNotIn("stopPrice", query)

    async def test_oco_oto_otoco_and_cancel_replace_use_official_routes(self) -> None:
        list_payload = {
            "orderListId": 8,
            "contingencyType": "OCO",
            "listStatusType": "EXEC_STARTED",
            "listOrderStatus": "EXECUTING",
            "listClientOrderId": "list-1",
            "transactionTime": 12,
            "symbol": "BTCUSDT",
            "orderReports": [
                {
                    "symbol": "BTCUSDT",
                    "orderId": 1,
                    "clientOrderId": "leg-1",
                    "status": "NEW",
                    "side": "SELL",
                    "price": "51000",
                    "origQty": "0.01",
                    "executedQty": "0",
                }
            ],
        }
        cancel_replace_payload = {
            "cancelResult": "SUCCESS",
            "newOrderResult": "SUCCESS",
            "cancelResponse": {
                "symbol": "BTCUSDT",
                "orderId": 1,
                "status": "CANCELED",
                "side": "SELL",
                "origQty": "0.01",
                "executedQty": "0",
            },
            "newOrderResponse": {
                "symbol": "BTCUSDT",
                "orderId": 2,
                "status": "NEW",
                "side": "SELL",
                "price": "52000",
                "origQty": "0.01",
                "executedQty": "0",
            },
        }
        transport = FakeTransport(
            response(200, list_payload),
            response(200, {**list_payload, "contingencyType": "OTO"}),
            response(200, {**list_payload, "contingencyType": "OTOCO"}),
            response(200, cancel_replace_payload),
            response(200, list_payload),
        )
        gateway = BinanceSpotGateway(
            credentials=BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder"),
            transport=transport,
        )
        await gateway.connect()
        quantity = Decimal("0.010")
        sell_limit = BinanceSpotOrderLeg("LIMIT_MAKER", Side.SELL, quantity, price=Decimal("51000"))
        sell_stop = BinanceSpotOrderLeg(
            "STOP_LOSS", Side.SELL, quantity, stop_price=Decimal("49000")
        )
        oco = await gateway.submit_oco(
            BinanceSpotOcoRequest("BTCUSDT", Side.SELL, quantity, sell_limit, sell_stop)
        )
        oto = await gateway.submit_oto(
            BinanceSpotOtoRequest(
                "BTCUSDT",
                BinanceSpotOrderLeg(
                    "LIMIT", Side.BUY, quantity, price=Decimal("48000"), time_in_force="GTC"
                ),
                sell_stop,
            )
        )
        otoco = await gateway.submit_otoco(
            BinanceSpotOtocoRequest("BTCUSDT", sell_limit, sell_limit, sell_stop)
        )
        replaced = await gateway.cancel_replace(
            symbol="BTCUSDT",
            cancel_order_id=1,
            new_order=BinanceSpotOrderLeg(
                "STOP_LOSS",
                Side.SELL,
                quantity,
                stop_price=Decimal("50000"),
                client_order_id="new-stop",
            ),
        )
        canceled = await gateway.cancel_order_list("BTCUSDT", order_list_id=8)

        self.assertEqual(
            (oco.contingency_type, oto.contingency_type, otoco.contingency_type),
            ("OCO", "OTO", "OTOCO"),
        )
        self.assertIsInstance(replaced, BinanceCancelReplaceResult)
        self.assertEqual(replaced.new_order_response.order_id, 2)
        self.assertEqual(canceled.order_list_id, 8)
        self.assertEqual(
            [urlsplit(request.url).path for request in transport.requests],
            [
                "/api/v3/orderList/oco",
                "/api/v3/orderList/oto",
                "/api/v3/orderList/otoco",
                "/api/v3/order/cancelReplace",
                "/api/v3/orderList",
            ],
        )

    async def test_amend_keep_priority_reduces_quantity(self) -> None:
        transport = FakeTransport(
            response(
                200,
                {
                    "amendedOrder": {
                        "symbol": "BTCUSDT",
                        "orderId": 1,
                        "clientOrderId": "amended-1",
                        "status": "NEW",
                        "side": "SELL",
                        "price": "51000",
                        "origQty": "0.005",
                        "executedQty": "0",
                    }
                },
            )
        )
        gateway = BinanceSpotGateway(
            credentials=BinanceCredentials("offline-api-placeholder", "offline-secret-placeholder"),
            transport=transport,
        )
        await gateway.connect()
        amended = await gateway.amend_order_keep_priority(
            symbol="BTCUSDT",
            order_id=1,
            new_quantity=Decimal("0.005"),
        )
        self.assertEqual(amended.original_quantity, Decimal("0.005"))
        self.assertEqual(
            urlsplit(transport.requests[0].url).path, "/api/v3/order/amend/keepPriority"
        )
