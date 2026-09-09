import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from gribuki_trade.adapters.schwab import (
    HttpRequest,
    HttpxAsyncHttpTransport,
    KeyringOAuthTokenStore,
    OAuthToken,
    SchwabHttpError,
    SchwabOAuthError,
    load_schwab_app_credentials,
)
from gribuki_trade.security import MemorySecretProvider


def test_schwab_app_credentials_are_resolved_and_redacted() -> None:
    provider = MemorySecretProvider(
        {"schwab.client_id": "client-id", "schwab.client_secret": "client-secret"}
    )

    credentials = load_schwab_app_credentials(provider)

    assert credentials.client_id == "client-id"
    assert credentials.client_secret == "client-secret"
    assert "client-id" not in repr(credentials)
    assert "client-secret" not in repr(credentials)


def test_keyring_token_store_round_trip_and_clear() -> None:
    async def scenario() -> None:
        provider = MemorySecretProvider()
        store = KeyringOAuthTokenStore(provider)
        issued_at = datetime(2026, 8, 13, tzinfo=UTC)
        expected = OAuthToken(
            access_token="access",
            token_type="Bearer",
            issued_at=issued_at,
            expires_at=issued_at + timedelta(minutes=30),
            refresh_token="refresh",
            scope="read trade",
        )

        await store.save(expected)
        assert await store.load() == expected
        assert "access" not in repr(store)
        assert await store.clear() is True
        assert await store.load() is None

    asyncio.run(scenario())


def test_keyring_token_store_rejects_malformed_document() -> None:
    async def scenario() -> None:
        provider = MemorySecretProvider({"schwab.oauth.token": "not-json"})
        with pytest.raises(SchwabOAuthError, match="malformed"):
            await KeyringOAuthTokenStore(provider).load()

    asyncio.run(scenario())


def test_httpx_transport_maps_response_without_network() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer placeholder"
        return httpx.Response(200, headers={"X-Test": "yes"}, json={"ok": True})

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        transport = HttpxAsyncHttpTransport(client)
        response = await transport.send(
            HttpRequest(
                method="GET",
                url="https://api.schwab.invalid/test",
                headers={"Authorization": "Bearer placeholder"},
                timeout=2,
            )
        )
        assert response.status_code == 200
        assert response.json() == {"ok": True}
        await client.aclose()

    asyncio.run(scenario())


def test_request_and_http_error_do_not_reveal_account_path() -> None:
    private_url = "https://api.schwabapi.com/trader/v1/accounts/private-hash/orders/9"
    request = HttpRequest("GET", private_url, {"Authorization": "Bearer token"})
    error = SchwabHttpError(404, "GET", private_url)

    assert private_url not in repr(request)
    assert "token" not in repr(request)
    assert private_url not in repr(error)
    assert error.url == "<redacted>"
