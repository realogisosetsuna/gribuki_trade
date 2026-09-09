import asyncio
import json
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import wraps
from urllib.parse import parse_qs, urlsplit

import pytest

from gribuki_trade.adapters.schwab import (
    HttpRequest,
    HttpResponse,
    InMemoryTokenStore,
    OAuthToken,
    SchwabApiClient,
    SchwabBroker,
    SchwabEndpoints,
    SchwabLiveAccessError,
    SchwabOAuthClient,
    SchwabOAuthError,
    SchwabOrderSpec,
    SchwabRateLimitError,
    build_limit_order_payload,
)
from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side

NOW = datetime(2026, 8, 13, 2, 0, tzinfo=UTC)


def async_test(function):
    @wraps(function)
    def run(*args, **kwargs):
        return asyncio.run(function(*args, **kwargs))

    return run


class FakeTransport:
    def __init__(self, *results: HttpResponse | BaseException) -> None:
        self.results = deque(results)
        self.requests: list[HttpRequest] = []

    async def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        result = self.results.popleft()
        if isinstance(result, BaseException):
            raise result
        return result


def response(status: int, payload: object = None, **headers: str) -> HttpResponse:
    body = b"" if payload is None else json.dumps(payload).encode()
    return HttpResponse(status, headers, body)


def token(access: str = "access", refresh: str = "refresh") -> OAuthToken:
    return OAuthToken(
        access_token=access,
        token_type="Bearer",
        issued_at=NOW,
        expires_at=NOW + timedelta(hours=1),
        refresh_token=refresh,
    )


def oauth(transport: FakeTransport, store: InMemoryTokenStore) -> SchwabOAuthClient:
    return SchwabOAuthClient(
        client_id="client",
        client_secret="secret",
        redirect_uri="https://localhost:8182/callback?fixed=1",
        transport=transport,
        token_store=store,
        authorization_endpoint="https://auth.schwab.invalid/authorize",
        token_endpoint="https://auth.schwab.invalid/token",
        clock=lambda: NOW,
        expiry_leeway_seconds=0,
    )


def api(transport: FakeTransport, store: InMemoryTokenStore) -> SchwabApiClient:
    return SchwabApiClient(
        oauth=oauth(transport, store),
        transport=transport,
        endpoints=SchwabEndpoints.offline(),
        clock=lambda: NOW,
    )


def order(client_order_id: str = "schwab-1", quantity: str = "2") -> OrderIntent:
    return OrderIntent(
        client_order_id=client_order_id,
        account_id="acct-1",
        strategy_id="test",
        symbol="AAPL",
        side=Side.BUY,
        quantity=Decimal(quantity),
        limit_price=Decimal("201.25"),
        created_at=NOW,
    )


@async_test
async def test_oauth_authorize_callback_exact_redirect_state_and_expires_in() -> None:
    transport = FakeTransport(
        response(
            200,
            {
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "token_type": "Bearer",
                "expires_in": 123,
            },
        )
    )
    store = InMemoryTokenStore()
    client = oauth(transport, store)

    authorization = client.authorization_request(state="unguessable-state")
    query = parse_qs(urlsplit(authorization.url).query)
    assert query == {
        "client_id": ["client"],
        "redirect_uri": ["https://localhost:8182/callback?fixed=1"],
        "response_type": ["code"],
        "state": ["unguessable-state"],
    }

    exchanged = await client.exchange_callback(
        "https://localhost:8182/callback?fixed=1&code=abc&state=unguessable-state",
        expected_state="unguessable-state",
    )

    assert exchanged.expires_at == NOW + timedelta(seconds=123)
    form = parse_qs(transport.requests[0].body.decode())
    assert form["redirect_uri"] == ["https://localhost:8182/callback?fixed=1"]
    assert form["grant_type"] == ["authorization_code"]
    assert "secret" not in repr(transport.requests[0])


@async_test
async def test_oauth_rejects_state_and_redirect_mismatches_before_transport() -> None:
    transport = FakeTransport()
    client = oauth(transport, InMemoryTokenStore())
    client.authorization_request(state="expected")

    with pytest.raises(SchwabOAuthError, match="redirect URI"):
        await client.exchange_callback(
            "https://localhost:8182/other?fixed=1&code=abc&state=expected",
            expected_state="expected",
        )
    with pytest.raises(SchwabOAuthError, match="state mismatch"):
        await client.exchange_callback(
            "https://localhost:8182/callback?fixed=1&code=abc&state=wrong",
            expected_state="expected",
        )
    assert transport.requests == []


def test_live_endpoints_and_oauth_are_default_deny() -> None:
    with pytest.raises(SchwabLiveAccessError, match="allow_live=True"):
        SchwabEndpoints.production()
    with pytest.raises(SchwabLiveAccessError, match="allow_live=True"):
        SchwabOAuthClient(
            client_id="client",
            client_secret="secret",
            redirect_uri="https://localhost/callback",
            transport=FakeTransport(),
            token_store=InMemoryTokenStore(),
        )
    assert SchwabEndpoints.production(allow_live=True).allow_live is True


@async_test
async def test_401_refreshes_once_and_reuses_refresh_token_when_omitted() -> None:
    transport = FakeTransport(
        response(401),
        response(
            200,
            {"access_token": "fresh", "token_type": "Bearer", "expires_in": 900},
        ),
        response(200, {"AAPL": {"quote": {"lastPrice": 201.0}}}),
    )
    store = InMemoryTokenStore(token())

    result = await api(transport, store).quotes(["AAPL"])

    assert isinstance(result, dict)
    assert len(transport.requests) == 3
    assert transport.requests[0].headers["Authorization"] == "Bearer access"
    assert transport.requests[2].headers["Authorization"] == "Bearer fresh"
    saved = await store.load()
    assert saved is not None and saved.refresh_token == "refresh"


@async_test
async def test_429_preserves_retry_after_without_automatic_retry() -> None:
    transport = FakeTransport(HttpResponse(429, {"Retry-After": "17"}))

    with pytest.raises(SchwabRateLimitError) as raised:
        await api(transport, InMemoryTokenStore(token())).price_history("AAPL")

    assert raised.value.retry_after == 17
    assert raised.value.retry_at == NOW + timedelta(seconds=17)
    assert len(transport.requests) == 1


@async_test
async def test_market_and_trader_paths_use_returned_account_hash() -> None:
    transport = FakeTransport(
        response(200, [{"accountNumber": "acct-1", "hashValue": "hash/one"}]),
        response(200, {"securitiesAccount": {"positions": []}}),
        response(200, []),
    )
    client = api(transport, InMemoryTokenStore(token()))

    hashes = await client.account_numbers()
    await client.positions(hashes[0].hash_value)
    await client.orders(hashes[0].hash_value, status="WORKING")

    assert hashes[0].account_number == "acct-1"
    assert transport.requests[1].url.endswith("/accounts/hash%2Fone?fields=positions")
    assert transport.requests[2].url.endswith("/accounts/hash%2Fone/orders?status=WORKING")


def test_stock_and_option_quantities_must_be_integers() -> None:
    with pytest.raises(ValueError, match="equity quantity must be an integer"):
        build_limit_order_payload(order(quantity="1.5"))
    with pytest.raises(ValueError, match="option quantity must be an integer"):
        build_limit_order_payload(
            order(quantity="1.5"),
            spec=SchwabOrderSpec(asset_type="OPTION", instruction="BUY_TO_OPEN"),
        )

    payload = build_limit_order_payload(order(quantity="2.0"))
    assert payload["orderLegCollection"][0]["quantity"] == 2


@pytest.mark.parametrize("failure", [TimeoutError(), response(503)])
@async_test
async def test_ambiguous_submit_is_unknown_and_duplicate_is_never_resent(
    failure: BaseException | HttpResponse,
) -> None:
    transport = FakeTransport(
        response(200, [{"accountNumber": "acct-1", "hashValue": "hash-one"}]),
        failure,
    )
    broker = SchwabBroker(api(transport, InMemoryTokenStore(token())))
    await broker.connect()
    intent = order()

    await broker.submit_order(intent)
    await broker.submit_order(intent)
    event = await anext(broker.events())

    assert event.payload.status is OrderStatus.UNKNOWN
    assert len(transport.requests) == 2
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(anext(broker.events()), 0.01)


@async_test
async def test_successful_limit_order_and_cancel_publish_broker_events() -> None:
    transport = FakeTransport(
        response(200, [{"accountNumber": "acct-1", "hashValue": "hash-one"}]),
        response(201, None, location="https://api.example/orders/987"),
        response(204),
    )
    broker = SchwabBroker(api(transport, InMemoryTokenStore(token())))
    await broker.connect()

    await broker.submit_order(order())
    accepted = await anext(broker.events())
    await broker.cancel_order("schwab-1")
    canceled = await anext(broker.events())

    assert accepted.payload.status is OrderStatus.ACCEPTED
    assert accepted.payload.broker_order_id == "987"
    assert canceled.payload.status is OrderStatus.CANCELED
    sent = json.loads(transport.requests[1].body)
    assert sent["orderType"] == "LIMIT"
    assert sent["orderLegCollection"][0]["quantity"] == 2
    assert transport.requests[2].method == "DELETE"
