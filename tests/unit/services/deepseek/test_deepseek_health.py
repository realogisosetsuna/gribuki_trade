from __future__ import annotations

import asyncio

import httpx
import pytest

from gribuki_trade.adapters.llm import (
    DeepSeekHealthClient,
    DeepSeekHealthErrorCode,
)
from gribuki_trade.security.config import SecretValue


def test_health_check_returns_only_safe_sorted_model_availability() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["authorization"] = request.headers["Authorization"]
        return httpx.Response(
            200,
            json={
                "object": "list",
                "provider_internal": "must not escape",
                "data": [
                    {"id": "deepseek-v4-pro", "owned_by": "deepseek"},
                    {"id": "deepseek-v4-flash", "owned_by": "deepseek"},
                    {"id": "deepseek-v4-pro", "owned_by": "duplicate"},
                ],
            },
        )

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            probe = DeepSeekHealthClient(
                SecretValue("local-test-key"),
                base_url="https://api.example.test/v1/",
                client=client,
            )
            return await probe.check()

    result = asyncio.run(run())

    assert result.ok is True
    assert result.error_code is None
    assert result.available_model_ids == (
        "deepseek-v4-flash",
        "deepseek-v4-pro",
    )
    assert result.deepseek_v4_flash_available is True
    assert result.deepseek_v4_pro_available is True
    assert "provider_internal" not in repr(result)
    assert captured == {
        "method": "GET",
        "url": "https://api.example.test/v1/models",
        "authorization": "Bearer local-test-key",
    }


def test_success_without_v4_pro_is_not_reported_as_transport_failure() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"object": "list", "data": [{"id": "deepseek-v4-flash"}]},
        )

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await DeepSeekHealthClient(
                SecretValue("key"), client=client
            ).check()

    result = asyncio.run(run())

    assert result.ok is True
    assert result.deepseek_v4_flash_available is True
    assert result.deepseek_v4_pro_available is False
    assert result.available_model_ids == ("deepseek-v4-flash",)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (401, DeepSeekHealthErrorCode.AUTHENTICATION_FAILED),
        (402, DeepSeekHealthErrorCode.INSUFFICIENT_BALANCE),
        (429, DeepSeekHealthErrorCode.RATE_LIMITED),
        (400, DeepSeekHealthErrorCode.PROVIDER_ERROR),
        (500, DeepSeekHealthErrorCode.PROVIDER_ERROR),
    ],
)
def test_http_failures_return_stable_codes_without_response_body(
    status: int,
    expected: DeepSeekHealthErrorCode,
) -> None:
    unsafe_body = "provider leaked private request details and api-key-value"

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text=unsafe_body)

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await DeepSeekHealthClient(
                SecretValue("api-key-value"), client=client
            ).check()

    result = asyncio.run(run())
    rendered = repr(result)

    assert result.ok is False
    assert result.error_code is expected
    assert result.available_model_ids == ()
    assert result.deepseek_v4_flash_available is False
    assert result.deepseek_v4_pro_available is False
    assert unsafe_body not in rendered
    assert "api-key-value" not in rendered


def test_network_failure_does_not_escape_request_or_provider_details() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(
            "provider-private-network-detail",
            request=request,
        )

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await DeepSeekHealthClient(
                SecretValue("private-key"), client=client
            ).check()

    result = asyncio.run(run())
    rendered = repr(result)

    assert result.error_code is DeepSeekHealthErrorCode.NETWORK_ERROR
    assert "provider-private-network-detail" not in rendered
    assert "private-key" not in rendered
    assert "api.deepseek.com" not in rendered


@pytest.mark.parametrize(
    "document",
    [
        [],
        {},
        {"object": "unexpected", "data": []},
        {"object": "list", "data": "not-an-array"},
        {"object": "list", "data": ["not-an-object"]},
        {"object": "list", "data": [{}]},
        {"object": "list", "data": [{"id": "model\nlog-injection"}]},
        {"object": "list", "data": [{"id": "x" * 129}]},
    ],
)
def test_invalid_response_schema_returns_stable_error(document: object) -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=document)

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await DeepSeekHealthClient(
                SecretValue("key"), client=client
            ).check()

    result = asyncio.run(run())

    assert result.error_code is DeepSeekHealthErrorCode.INVALID_RESPONSE
    assert result.available_model_ids == ()


def test_invalid_json_returns_stable_error_without_body() -> None:
    body = "not-json private provider response"

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=body)

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await DeepSeekHealthClient(
                SecretValue("key"), client=client
            ).check()

    result = asyncio.run(run())

    assert result.error_code is DeepSeekHealthErrorCode.INVALID_RESPONSE
    assert body not in repr(result)


@pytest.mark.parametrize(
    "base_url",
    [
        "",
        "http://api.deepseek.com",
        "https://key@api.deepseek.com",
        "https://api.deepseek.com?api_key=unsafe",
        "https://api.deepseek.com#fragment",
    ],
)
def test_health_client_rejects_unsafe_base_urls(base_url: str) -> None:
    with pytest.raises(ValueError, match="base_url"):
        DeepSeekHealthClient(SecretValue("key"), base_url=base_url)


def test_health_client_requires_nonempty_key_and_short_timeout() -> None:
    with pytest.raises(ValueError, match="api_key"):
        DeepSeekHealthClient(SecretValue(""))
    with pytest.raises(ValueError, match="api_key"):
        DeepSeekHealthClient(SecretValue("   "))
    with pytest.raises(ValueError, match="timeout"):
        DeepSeekHealthClient(SecretValue("key"), timeout_seconds=0)
    with pytest.raises(ValueError, match="timeout"):
        DeepSeekHealthClient(SecretValue("key"), timeout_seconds=60.1)
