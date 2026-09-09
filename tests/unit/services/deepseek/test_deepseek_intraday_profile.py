from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from gribuki_trade.adapters.llm import (
    DEEPSEEK_ADAPTER_VERSION,
    DEEPSEEK_PROMPT_SCHEMA_SHA256,
    DEEPSEEK_PROMPT_VERSION,
    DeepSeekChatMacroAnalyzer,
    DeepSeekMacroAnalyzerError,
)
from gribuki_trade.adapters.llm.deepseek_chat import DEEPSEEK_INTRADAY_PROFILE
from gribuki_trade.analysis.schemas import EvidenceItem, MacroAnalysisRequest
from gribuki_trade.security.config import SecretValue


def test_intraday_profile_is_one_attempt_no_thinking_and_audit_identified() -> None:
    assert DEEPSEEK_INTRADAY_PROFILE.timeout_seconds == 15
    calls: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        return httpx.Response(200, content=b"not-json")

    async def scenario() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            analyzer = DeepSeekChatMacroAnalyzer.for_intraday(
                SecretValue("secret"),
                client=client,
            )
            identity = analyzer.audit_identity
            assert identity.adapter_version == DEEPSEEK_ADAPTER_VERSION
            assert identity.prompt_version == DEEPSEEK_PROMPT_VERSION
            assert identity.prompt_schema_sha256 == DEEPSEEK_PROMPT_SCHEMA_SHA256
            with pytest.raises(DeepSeekMacroAnalyzerError) as caught:
                await analyzer.analyze(_request())
            assert caught.value.error_code == "DEEPSEEK_INVALID_JSON"

    asyncio.run(scenario())
    assert len(calls) == 1
    assert calls[0]["thinking"] == {"type": "disabled"}
    assert calls[0]["max_tokens"] == 1_200


def _request() -> MacroAnalysisRequest:
    as_of = datetime(2026, 8, 14, 2, 0, tzinfo=UTC)
    evidence = EvidenceItem(
        evidence_id="evidence-1",
        publisher="official.test",
        source_tier=0,
        published_at=as_of - timedelta(minutes=2),
        first_seen_at=as_of - timedelta(minutes=1),
        title="bounded event",
        excerpt="bounded excerpt",
        canonical_url="https://example.test/event",
        content_hash="a" * 64,
    )
    return MacroAnalysisRequest(
        analysis_id="analysis-1",
        symbol="600000.SH",
        as_of=as_of,
        horizon="INTRADAY_BACKGROUND_REVIEW",
        technical_summary=("MOMENTUM_EXPANSION",),
        evidence=(evidence,),
    )
