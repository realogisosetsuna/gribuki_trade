from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from gribuki_trade.adapters.llm import (
    DEEPSEEK_API_KEY_SECRET,
    DEFAULT_DEEPSEEK_BASE_URL,
    DEFAULT_DEEPSEEK_MODEL,
    DeepSeekChatMacroAnalyzer,
    DeepSeekMacroAnalyzerError,
)
from gribuki_trade.analysis.schemas import EvidenceItem, MacroAnalysisRequest
from gribuki_trade.security.config import SecretValue

NOW = datetime(2026, 8, 13, 10, 30, tzinfo=UTC)


def request() -> MacroAnalysisRequest:
    return MacroAnalysisRequest(
        analysis_id="analysis-1",
        symbol="600000.SH",
        as_of=NOW,
        horizon="SHORT_1_TO_5_DAYS",
        technical_summary=("CLOSED_BAR_BREAKOUT",),
        evidence=(
            EvidenceItem(
                evidence_id="ev-1",
                publisher="Exchange",
                source_tier=1,
                published_at=NOW - timedelta(minutes=4),
                first_seen_at=NOW - timedelta(minutes=3),
                title="Official announcement",
                excerpt="A bounded excerpt.",
                canonical_url="https://example.test/announcement",
                content_hash="a" * 64,
            ),
        ),
    )


def analysis_body(*, evidence_id: str = "ev-1") -> dict[str, object]:
    return {
        "analysis_id": "analysis-1",
        "as_of": NOW.isoformat(),
        "decision": "PUBLISH",
        "regime": "neutral",
        "technical_alignment": 0.5,
        "macro_impact": 0.2,
        "scenarios": [
            {
                "name": "base",
                "probability": 1.0,
                "drivers": ["official evidence"],
                "evidence_ids": [evidence_id],
            }
        ],
        "claims": [
            {
                "text": "The event has a limited positive impact.",
                "evidence_ids": [evidence_id],
                "contradictions": [],
            }
        ],
        "uncertainties": ["market reaction"],
        "data_gaps": [],
        "invalidation_conditions": ["announcement retracted"],
        "reported_confidence": "MEDIUM",
        "refusal_reason": "",
    }


def response_payload(
    body: dict[str, object] | None = None,
    *,
    content: str | None = None,
    finish_reason: str = "stop",
    tool_calls: list[object] | None = None,
    model: str = DEFAULT_DEEPSEEK_MODEL,
) -> dict[str, object]:
    message: dict[str, object] = {
        "role": "assistant",
        "content": content if content is not None else json.dumps(body or analysis_body()),
        # The adapter must neither expose nor persist provider reasoning.
        "reasoning_content": "private provider reasoning",
    }
    if tool_calls is not None:
        message["tool_calls"] = tool_calls
    return {
        "id": "completion-1",
        "object": "chat.completion",
        "model": model,
        "choices": [
            {
                "index": 0,
                "finish_reason": finish_reason,
                "message": message,
            }
        ],
    }


def test_deepseek_adapter_sends_bounded_json_request_and_validates_output() -> None:
    captured: dict[str, object] = {}

    def handler(http_request: httpx.Request) -> httpx.Response:
        captured["url"] = str(http_request.url)
        captured["authorization"] = http_request.headers["Authorization"]
        captured["payload"] = json.loads(http_request.content)
        return httpx.Response(200, json=response_payload())

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(
                SecretValue("local-secret"),
                base_url="https://api.example.test/v1/",
                client=client,
            )
            return await analyzer.analyze(request())

    result = asyncio.run(run())

    assert result.macro_impact == Decimal("0.2")
    assert result.claims[0].evidence_ids == ("ev-1",)
    assert result.model_version == DEFAULT_DEEPSEEK_MODEL
    assert captured["url"] == "https://api.example.test/v1/chat/completions"
    assert captured["authorization"] == "Bearer local-secret"
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["model"] == DEFAULT_DEEPSEEK_MODEL
    assert payload["response_format"] == {"type": "json_object"}
    assert payload["thinking"] == {"type": "enabled"}
    assert payload["reasoning_effort"] == "high"
    assert payload["stream"] is False
    assert "tools" not in payload
    assert "store" not in payload
    assert "untrusted_source_data" in payload["messages"][1]["content"]
    assert "JSON" in payload["messages"][0]["content"]
    assert "Simplified Chinese" in payload["messages"][0]["content"]
    assert "do not translate or convert units" in payload["messages"][0]["content"]
    assert "put IDs only in evidence_ids arrays" in payload["messages"][0]["content"]


def test_constants_match_official_api_identifiers() -> None:
    assert DEEPSEEK_API_KEY_SECRET == "deepseek.api_key"
    assert DEFAULT_DEEPSEEK_MODEL == "deepseek-v4-flash"
    assert DEFAULT_DEEPSEEK_BASE_URL == "https://api.deepseek.com"


@pytest.mark.parametrize(
    "base_url",
    [
        "http://api.deepseek.com",
        "https://token@api.deepseek.com",
        "https://api.deepseek.com?token=unsafe",
        "",
    ],
)
def test_base_url_rejects_unsafe_values(base_url: str) -> None:
    with pytest.raises(ValueError, match="base_url"):
        DeepSeekChatMacroAnalyzer(SecretValue("secret"), base_url=base_url)


def test_model_and_reasoning_configuration_are_forwarded() -> None:
    captured: dict[str, object] = {}

    def handler(http_request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(http_request.content))
        return httpx.Response(200, json=response_payload(model="private-model"))

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(
                SecretValue("secret"),
                model="private-model",
                base_url="https://compatible.example.test",
                max_tokens=4_096,
                thinking=False,
                reasoning_effort="max",
                client=client,
            )
            return await analyzer.analyze(request())

    result = asyncio.run(run())

    assert result.model_version == "private-model"
    assert captured["model"] == "private-model"
    assert captured["max_tokens"] == 4_096
    assert captured["thinking"] == {"type": "disabled"}
    assert captured["reasoning_effort"] == "max"


def test_unknown_evidence_reference_is_rejected() -> None:
    attempts = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(
            200,
            json=response_payload(analysis_body(evidence_id="invented")),
        )

    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(
                SecretValue("secret"), client=client
            )
            with pytest.raises(DeepSeekMacroAnalyzerError, match="invalid response"):
                await analyzer.analyze(request())

    asyncio.run(run())
    assert attempts == 1


def test_empty_json_completion_is_retried_once_without_thinking() -> None:
    payloads: list[dict[str, object]] = []

    def handler(http_request: httpx.Request) -> httpx.Response:
        payload = json.loads(http_request.content)
        assert isinstance(payload, dict)
        payloads.append(payload)
        if len(payloads) == 1:
            return httpx.Response(200, json=response_payload(content=""))
        return httpx.Response(200, json=response_payload())

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(SecretValue("secret"), client=client)
            return await analyzer.analyze(request())

    result = asyncio.run(run())

    assert result.decision.value == "PUBLISH"
    assert len(payloads) == 2
    assert payloads[0]["thinking"] == {"type": "enabled"}
    assert payloads[1]["thinking"] == {"type": "disabled"}
    recovery_messages = payloads[1]["messages"]
    assert isinstance(recovery_messages, list)
    assert "one-time recovery request" in recovery_messages[0]["content"]


def test_length_limited_completion_is_retried_once_without_thinking() -> None:
    payloads: list[dict[str, object]] = []

    def handler(http_request: httpx.Request) -> httpx.Response:
        payload = json.loads(http_request.content)
        assert isinstance(payload, dict)
        payloads.append(payload)
        if len(payloads) == 1:
            return httpx.Response(
                200,
                json=response_payload(content='{"truncated":', finish_reason="length"),
            )
        return httpx.Response(200, json=response_payload())

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(SecretValue("secret"), client=client)
            return await analyzer.analyze(request())

    result = asyncio.run(run())

    assert result.decision.value == "PUBLISH"
    assert [payload["thinking"] for payload in payloads] == [
        {"type": "enabled"},
        {"type": "disabled"},
    ]


def test_schema_invalid_completion_is_retried_without_relaxing_schema() -> None:
    attempts = 0
    invalid_body = analysis_body()
    invalid_body["unexpected"] = "must never be copied into a recovery request"

    def handler(http_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(200, json=response_payload(invalid_body))
        assert b"must never be copied into a recovery request" not in http_request.content
        return httpx.Response(200, json=response_payload())

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(SecretValue("secret"), client=client)
            return await analyzer.analyze(request())

    result = asyncio.run(run())

    assert result.decision.value == "PUBLISH"
    assert attempts == 2


def test_recovery_completion_is_never_retried_again() -> None:
    attempts = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(200, json=response_payload(content=""))

    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(SecretValue("secret"), client=client)
            with pytest.raises(DeepSeekMacroAnalyzerError) as caught:
                await analyzer.analyze(request())
            assert caught.value.error_code == "DEEPSEEK_COMPLETION_INVALID"

    asyncio.run(run())
    assert attempts == 2


def test_recovery_transport_failure_does_not_trigger_a_third_request() -> None:
    attempts = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(200, json=response_payload(content=""))
        return httpx.Response(429, text="provider-private-rate-limit-details")

    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(SecretValue("secret"), client=client)
            with pytest.raises(DeepSeekMacroAnalyzerError) as caught:
                await analyzer.analyze(request())
            assert caught.value.error_code == "DEEPSEEK_HTTP_429"

    asyncio.run(run())
    assert attempts == 2


@pytest.mark.parametrize("mutation", ["extra_root", "extra_nested", "wrong_number"])
def test_local_schema_validation_rejects_nonconforming_json(mutation: str) -> None:
    body = analysis_body()
    if mutation == "extra_root":
        body["unexpected"] = "must fail"
    elif mutation == "extra_nested":
        scenario = body["scenarios"][0]
        assert isinstance(scenario, dict)
        scenario["unexpected"] = "must fail"
    else:
        body["macro_impact"] = True

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=response_payload(body))

    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(SecretValue("secret"), client=client)
            with pytest.raises(DeepSeekMacroAnalyzerError, match="invalid response"):
                await analyzer.analyze(request())

    asyncio.run(run())


def test_duplicate_json_keys_are_rejected() -> None:
    body = json.dumps(analysis_body())
    duplicate = body.replace(
        '"analysis_id": "analysis-1",',
        '"analysis_id": "analysis-1", "analysis_id": "shadowed",',
        1,
    )

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=response_payload(content=duplicate))

    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(SecretValue("secret"), client=client)
            with pytest.raises(DeepSeekMacroAnalyzerError, match="invalid response"):
                await analyzer.analyze(request())

    asyncio.run(run())


@pytest.mark.parametrize(
    ("finish_reason", "expected_attempts"),
    [
        ("length", 2),
        ("insufficient_system_resource", 2),
        ("content_filter", 1),
        ("tool_calls", 1),
    ],
)
def test_non_normal_finish_reason_is_rejected(
    finish_reason: str,
    expected_attempts: int,
) -> None:
    attempts = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(200, json=response_payload(finish_reason=finish_reason))

    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(SecretValue("secret"), client=client)
            with pytest.raises(DeepSeekMacroAnalyzerError, match="invalid response"):
                await analyzer.analyze(request())

    asyncio.run(run())
    assert attempts == expected_attempts


def test_unexpected_tool_call_is_rejected_even_with_stop_finish() -> None:
    tool_call = {
        "id": "call-1",
        "type": "function",
        "function": {"name": "trade", "arguments": "{}"},
    }

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=response_payload(tool_calls=[tool_call]))

    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(SecretValue("secret"), client=client)
            with pytest.raises(DeepSeekMacroAnalyzerError, match="invalid response"):
                await analyzer.analyze(request())

    asyncio.run(run())


def test_http_error_is_sanitized_and_does_not_retain_secret_or_body() -> None:
    secret = "never-log-this-api-key"
    unsafe_body = "upstream echoed never-log-this-api-key and private evidence"

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text=unsafe_body)

    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(SecretValue(secret), client=client)
            with pytest.raises(DeepSeekMacroAnalyzerError) as caught:
                await analyzer.analyze(request())
        rendered = repr(caught.value) + str(caught.value)
        assert secret not in rendered
        assert unsafe_body not in rendered
        assert caught.value.__cause__ is None
        assert caught.value.__context__ is None

    asyncio.run(run())


def test_rate_limit_is_retried_with_sanitized_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, text="provider-private-rate-limit-details")
        return httpx.Response(200, json=response_payload())

    monkeypatch.setattr(
        "gribuki_trade.adapters.llm.deepseek_chat.asyncio.sleep",
        fake_sleep,
    )

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer(SecretValue("secret"), client=client)
            return await analyzer.analyze(request())

    result = asyncio.run(run())

    assert result.decision.value == "PUBLISH"
    assert attempts == 2
    assert delays == [1]
