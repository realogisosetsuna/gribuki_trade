from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from gribuki_trade.adapters.llm import OpenAIResponsesMacroAnalyzer
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


def response_payload(*, evidence_id: str = "ev-1") -> dict[str, object]:
    body = {
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
    return {
        "output": [
            {
                "type": "message",
                "content": [{"type": "output_text", "text": json.dumps(body)}],
            }
        ]
    }


def test_openai_adapter_sends_bounded_evidence_and_validates_output() -> None:
    captured: dict[str, object] = {}

    def handler(http_request: httpx.Request) -> httpx.Response:
        captured["authorization"] = http_request.headers["Authorization"]
        captured["payload"] = json.loads(http_request.content)
        return httpx.Response(200, json=response_payload())

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = OpenAIResponsesMacroAnalyzer(
                SecretValue("local-secret"),
                base_url="https://api.example.test/v1",
                client=client,
            )
            return await analyzer.analyze(request())

    result = asyncio.run(run())

    assert result.macro_impact == Decimal("0.2")
    assert result.claims[0].evidence_ids == ("ev-1",)
    assert captured["authorization"] == "Bearer local-secret"
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["store"] is False
    assert "tools" not in payload
    assert payload["text"]["format"]["strict"] is True


def test_unknown_evidence_reference_is_rejected() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=response_payload(evidence_id="invented"))

    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = OpenAIResponsesMacroAnalyzer(
                SecretValue("local-secret"),
                base_url="https://api.example.test/v1",
                client=client,
            )
            with pytest.raises(RuntimeError, match="invalid response"):
                await analyzer.analyze(request())

    asyncio.run(run())


def test_request_rejects_future_evidence() -> None:
    item = request().evidence[0]
    future = EvidenceItem(
        evidence_id=item.evidence_id,
        publisher=item.publisher,
        source_tier=item.source_tier,
        published_at=NOW,
        first_seen_at=NOW + timedelta(seconds=1),
        title=item.title,
        excerpt=item.excerpt,
        canonical_url=item.canonical_url,
        content_hash=item.content_hash,
    )
    with pytest.raises(ValueError, match="not available"):
        MacroAnalysisRequest(
            analysis_id="analysis-1",
            symbol="600000.SH",
            as_of=NOW,
            horizon="SHORT_1_TO_5_DAYS",
            technical_summary=(),
            evidence=(future,),
        )
