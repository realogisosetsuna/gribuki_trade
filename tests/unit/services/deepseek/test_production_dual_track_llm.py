from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx

from gribuki_trade.adapters.llm.deepseek_chat import DeepSeekChatMacroAnalyzer
from gribuki_trade.analysis.schemas import (
    EvidenceItem,
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
    MacroClaim,
)
from gribuki_trade.domain.events import NormalizedEvent, SourceTier
from gribuki_trade.features.deep_exit_planning import DeepExitTimeframe
from gribuki_trade.features.exit_planning import build_quick_exit_plan
from gribuki_trade.features.technical import TechnicalBar
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity, DualTrackMacroAnalysis
from gribuki_trade.security.config import SecretValue
from gribuki_trade.services.adversarial_macro import (
    AdversarialFeatureMode,
    AdversarialMacroAnalyzer,
    AdversarialMacroConfig,
    AdversarialMacroDepth,
    FeatureFlaggedAdversarialMacroAnalyzer,
    ProductionDualTrackMacroAnalyzer,
)
from gribuki_trade.services.llm_production import (
    PaperDayDualTrackDeepExitAssessmentProvider,
)
from gribuki_trade.services.macro_research import (
    MacroResearchRun,
    MacroResearchService,
    select_macro_evidence,
)
from gribuki_trade.storage.adversarial_audit import SQLiteAdversarialAuditStore

NOW = datetime(2026, 8, 14, 7, 0, tzinfo=UTC)
FAKE_IDENTITY = AnalyzerAuditIdentity(
    provider_id="fake.usage",
    requested_model="fake-model@1",
    adapter_version="fake-adapter@1",
    prompt_version="fake-prompt@1",
    prompt_schema_sha256="1" * 64,
)


def _request() -> MacroAnalysisRequest:
    return MacroAnalysisRequest(
        analysis_id="transport-level-dual-track",
        symbol="600000.SH",
        as_of=NOW,
        horizon="next session",
        technical_summary=("确定性技术评分=0.75",),
        evidence=(
            EvidenceItem(
                evidence_id="a" * 64,
                publisher="official.test",
                source_tier=0,
                published_at=NOW - timedelta(minutes=5),
                first_seen_at=NOW - timedelta(minutes=4),
                title="权威证据",
                excerpt="已冻结的权威事实",
                canonical_url="https://example.test/evidence",
                content_hash="b" * 64,
            ),
        ),
    )


def _transport_response(request: httpx.Request) -> httpx.Response:
    payload = json.loads(request.content)
    user = json.loads(payload["messages"][1]["content"])
    summary = tuple(user["technical_summary"])
    role = next(
        (
            item.removeprefix("ADVERSARIAL_ROLE=")
            for item in summary
            if item.startswith("ADVERSARIAL_ROLE=")
        ),
        "BASELINE",
    )
    score = {
        "BASELINE": 0.10,
        "CATALYST_ADVOCATE": 0.60,
        "RISK_CHALLENGER": -0.20,
    }[role]
    analysis = {
        "analysis_id": user["analysis_id"],
        "as_of": user["as_of"],
        "decision": "PUBLISH",
        "regime": f"{role} 完成",
        "technical_alignment": 0.4,
        "macro_impact": score,
        "scenarios": [],
        "claims": [
            {
                "text": f"{role} 基于权威证据形成论据",
                "evidence_ids": [user["untrusted_source_data"][0]["evidence_id"]],
                "contradictions": [],
            }
        ],
        "uncertainties": [],
        "data_gaps": [],
        "invalidation_conditions": ["权威证据发生实质修订时失效"],
        "reported_confidence": "UNCALIBRATED",
        "refusal_reason": "",
    }
    return httpx.Response(
        200,
        json={
            "model": payload["model"],
            "choices": [
                {
                    "finish_reason": "stop",
                    "message": {"content": json.dumps(analysis, ensure_ascii=False)},
                }
            ],
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 40,
                "total_tokens": 140,
                "completion_tokens_details": {"reasoning_tokens": 12},
            },
        },
    )


def test_real_deepseek_adapter_transport_fake_runs_and_persists_dual_track(
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[
        MacroResearchRun,
        tuple[dict[str, object], ...],
    ]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(_transport_response)) as client:
            baseline = DeepSeekChatMacroAnalyzer(
                SecretValue("test-only-key"),
                model="deepseek-test-model",
                client=client,
                timeout_seconds=2,
                transport_attempts=1,
                recovery_enabled=False,
            )
            config = replace(
                AdversarialMacroConfig.for_depth(AdversarialMacroDepth.FAST),
                per_role_timeout=timedelta(seconds=2),
                case_timeout=timedelta(seconds=4),
            )
            adversarial = AdversarialMacroAnalyzer(baseline, config=config)
            with SQLiteAdversarialAuditStore(tmp_path / "llm-audit.sqlite3") as store:
                analyzer = ProductionDualTrackMacroAnalyzer(
                    baseline,
                    adversarial,
                    case_timeout=timedelta(seconds=4),
                    audit_sink=store.append,
                )
                result = await MacroResearchService(analyzer).analyze(
                    symbol="600000.SH",
                    as_of=NOW,
                    horizon="next session",
                    technical_summary=("确定性技术评分=0.75",),
                    events=(),
                    additional_evidence=_request().evidence,
                )
                records = store.read_all()
        return result, records

    result, records = asyncio.run(scenario())
    assert result.selected_track == "ADVERSARIAL"
    assert result.failure_code is None
    assert result.dual_audit_record_sha256 is not None
    assert result.baseline_analysis is not None
    assert result.adversarial_analysis is not None
    assert len(records) == 1
    record = records[0]
    adversarial_run = record["adversarial_run"]
    assert isinstance(adversarial_run, dict)
    rounds = adversarial_run["rounds"]
    assert isinstance(rounds, list)
    roles = rounds[0]["roles"]
    assert {item["usage"]["total_tokens"] for item in roles} == {140}
    baseline_call = record["baseline_call"]
    assert isinstance(baseline_call, dict)
    assert baseline_call["usage"]["reasoning_tokens"] == 12


def test_paper_deep_exit_provider_keeps_both_scores_but_never_returns_prices() -> None:
    bars = tuple(
        TechnicalBar(
            end_time=NOW - timedelta(minutes=60 - index),
            available_at=NOW - timedelta(minutes=60 - index) + timedelta(seconds=2),
            open=Decimal("10.00"),
            high=Decimal("10.10"),
            low=Decimal("9.95"),
            close=Decimal("10.05"),
            volume=1000 + index,
        )
        for index in range(60)
    )
    plan = build_quick_exit_plan(
        account_id="paper-dual",
        protection_id="protection-dual",
        symbol="600000.SH",
        bars=bars,
        decision_at=NOW,
        time_exit_at=NOW + timedelta(days=3),
        worst_entry_price=Decimal("10.20"),
        technical_invalidation_price=Decimal("9.70"),
        strategy_version="dual-provider-test@1",
    ).plan
    frames = (
        DeepExitTimeframe(
            timeframe_id="1m",
            bars=bars,
            weight=Decimal("1"),
            maximum_age=timedelta(minutes=30),
        ),
    )

    class FakeDualAnalyzer:
        async def analyze_dual(self, request: MacroAnalysisRequest) -> DualTrackMacroAnalysis:
            evidence_id = request.evidence[0].evidence_id

            def analysis(*, alignment: str, impact: str, regime: str) -> MacroAnalysis:
                return MacroAnalysis(
                    analysis_id=request.analysis_id,
                    as_of=request.as_of,
                    decision=MacroAnalysisDecision.PUBLISH,
                    regime=regime,
                    technical_alignment=Decimal(alignment),
                    macro_impact=Decimal(impact),
                    scenarios=(),
                    claims=(MacroClaim("冻结技术证据支持该判断", (evidence_id,), ()),),
                    uncertainties=(),
                    data_gaps=(),
                    invalidation_conditions=("后续完成线破坏趋势结构",),
                    reported_confidence="HIGH",
                    refusal_reason="",
                    model_version="fake-dual@1",
                )

            baseline = analysis(alignment="0.4", impact="0.1", regime="baseline")
            adversarial = analysis(alignment="0.8", impact="0.4", regime="adversarial")
            return DualTrackMacroAnalysis(
                selected_analysis=adversarial,
                baseline_analysis=baseline,
                adversarial_analysis=adversarial,
                selected_track="ADVERSARIAL",
                failure_code=None,
                audit_document={"safe": True},
                audit_record_sha256="c" * 64,
            )

    baseline, adversarial = asyncio.run(
        PaperDayDualTrackDeepExitAssessmentProvider(FakeDualAnalyzer()).assess(
            plan=plan,
            timeframes=frames,
            decision_at=NOW,
        )
    )

    assert baseline is not None
    assert adversarial is not None
    assert baseline.system == "BASELINE_LLM"
    assert adversarial.system == "ADVERSARIAL_LLM"
    assert baseline.score == Decimal("0.325")
    assert adversarial.score == Decimal("0.700")
    assert not hasattr(baseline, "stop_price")
    assert not hasattr(adversarial, "take_profit_price")
    assert baseline.evidence_ids == adversarial.evidence_ids


def _media_event(
    source_id: str,
    title: str,
    *,
    canonical_url: str | None = None,
) -> NormalizedEvent:
    return NormalizedEvent(
        source_id=source_id,
        canonical_url=(
            canonical_url or f"https://{source_id.replace('.', '-')}.example/{source_id}"
        ),
        title=title,
        summary="公共媒体报道的市场事实",
        event_type="market_news",
        source_tier=SourceTier.PUBLIC_MEDIA,
        first_seen_at=NOW - timedelta(minutes=4),
        retrieved_at=NOW - timedelta(minutes=3),
        available_at=NOW - timedelta(minutes=4),
        published_at=NOW - timedelta(minutes=5),
        external_id=f"{source_id}-{title}",
    )


def test_public_media_is_explicitly_unconfirmed_until_independent_corroboration() -> None:
    single = select_macro_evidence(
        "600000.SH",
        NOW,
        (_media_event("media.one", "A股流动性边际改善"),),
    )
    assert single.uncorroborated_public_media == 1
    assert "只能作为线索，不得当作已证实事实" in single.items[0].excerpt

    corroborated = select_macro_evidence(
        "600000.SH",
        NOW,
        (
            _media_event("media.one", "A股流动性边际改善"),
            _media_event("media.two", "A股流动性边际改善"),
        ),
    )
    assert corroborated.uncorroborated_public_media == 0
    assert corroborated.corroboration[0].status == "CORROBORATED_INDEPENDENT_SOURCES"
    assert set(corroborated.corroboration[0].independent_publishers) == {
        "publisher-domain:media-one.example",
        "publisher-domain:media-two.example",
    }

    unconfirmed_request = MacroAnalysisRequest(
        analysis_id="unconfirmed-media",
        symbol="600000.SH",
        as_of=NOW,
        horizon="next session",
        technical_summary=("技术候选",),
        evidence=single.items,
    )
    rejected = asyncio.run(
        AdversarialMacroAnalyzer(_DelayedAnalyzer(0)).analyze_case(unconfirmed_request)
    )
    assert rejected.failure_code == "ADVERSARIAL_UNCORROBORATED_MEDIA_CLAIM"
    assert rejected.analysis.decision is MacroAnalysisDecision.ABSTAIN
    assert len(rejected.failed_role_calls) == 2
    rejected_calls = rejected.audit_document()["failed_role_calls"]
    assert isinstance(rejected_calls, list)
    assert any(
        item["termination_reason"] == "ROLE_OUTPUT_REJECTED"
        and item["failure_code"] == "ADVERSARIAL_UNCORROBORATED_MEDIA_CLAIM"
        for item in rejected_calls
    )


def test_two_eastmoney_routes_are_one_publisher_not_corroboration() -> None:
    selection = select_macro_evidence(
        "600000.SH",
        NOW,
        (
            _media_event(
                "akshare.individual_eastmoney.600000",
                "A股流动性边际改善",
                canonical_url="https://so.eastmoney.com/news/s?keyword=600000",
            ),
            _media_event(
                "akshare.global_eastmoney",
                "A股流动性边际改善",
                canonical_url="https://kuaixun.eastmoney.com/7_24.html",
            ),
        ),
    )

    assert selection.uncorroborated_public_media == 1
    assert selection.corroboration[0].status == "UNCORROBORATED_PUBLIC_MEDIA"
    assert selection.corroboration[0].independent_publishers == ("publisher-domain:eastmoney.com",)


def test_official_body_identity_cannot_corroborate_its_own_domain_route() -> None:
    official = replace(
        _media_event(
            "official.pboc.open_market",
            "央行开展公开市场操作",
            canonical_url="https://www.pbc.gov.cn/open-market/notice.html",
        ),
        source_tier=SourceTier.OFFICIAL,
    )
    public_route = replace(
        _media_event(
            "search.route.one",
            "央行今日开展公开市场操作",
            canonical_url="https://pbc.gov.cn/open-market/notice.html",
        ),
        entities=("600000",),
    )

    selection = select_macro_evidence(
        "600000.SH",
        NOW,
        (official, public_route),
    )

    assert selection.uncorroborated_public_media == 1
    assert {item.independent_publishers for item in selection.corroboration} == {
        ("official-body:pboc",),
    }


class _DelayedAnalyzer:
    def __init__(self, delay: float) -> None:
        self.delay = delay

    @property
    def audit_identity(self) -> AnalyzerAuditIdentity:
        return FAKE_IDENTITY

    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        await asyncio.sleep(self.delay)
        evidence_id = request.evidence[0].evidence_id
        return MacroAnalysis(
            analysis_id=request.analysis_id,
            as_of=request.as_of,
            decision=MacroAnalysisDecision.PUBLISH,
            regime="fake",
            technical_alignment=Decimal("0.2"),
            macro_impact=Decimal("0.1"),
            scenarios=(),
            claims=(MacroClaim("有证据的论据", (evidence_id,), ()),),
            uncertainties=(),
            data_gaps=(),
            invalidation_conditions=("证据变化时失效",),
            reported_confidence="UNCALIBRATED",
            refusal_reason="",
            model_version=FAKE_IDENTITY.requested_model,
        )


def test_shadow_returns_baseline_without_waiting_for_slow_adversarial_branch() -> None:
    async def scenario() -> tuple[float, int]:
        observed = []
        baseline = _DelayedAnalyzer(0)
        adversarial = AdversarialMacroAnalyzer(_DelayedAnalyzer(0.15))
        wrapper = FeatureFlaggedAdversarialMacroAnalyzer(
            baseline,
            adversarial,
            mode=AdversarialFeatureMode.SHADOW,
            shadow_observer=observed.append,
        )
        loop = asyncio.get_running_loop()
        started = loop.time()
        await wrapper.analyze(_request())
        elapsed = loop.time() - started
        await asyncio.sleep(0.20)
        return elapsed, len(observed)

    elapsed, observed = asyncio.run(scenario())
    assert elapsed < 0.05
    assert observed == 1
