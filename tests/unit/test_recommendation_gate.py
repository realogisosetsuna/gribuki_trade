from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.analysis.schemas import (
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroClaim,
    MacroScenario,
)
from gribuki_trade.domain.recommendations import (
    ConfidenceBand,
    EvidenceReference,
    RecommendationDecision,
    RecommendationHorizon,
)
from gribuki_trade.features.technical import TechnicalSignal
from gribuki_trade.policy.recommendation_gate import (
    RecommendationGateConfig,
    build_recommendation,
)

NOW = datetime(2026, 8, 13, 10, 35, tzinfo=UTC)


def technical() -> TechnicalSignal:
    return TechnicalSignal(
        symbol="600000.SH",
        as_of=NOW,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        decision=RecommendationDecision.ENTER_CANDIDATE,
        score=Decimal("0.8"),
        reference_price=Decimal("10.20"),
        invalidation_price=Decimal("9.90"),
        reason_codes=("CLOSED_BAR_BREAKOUT",),
        data_age=timedelta(seconds=10),
        strategy_version="test@1",
        metrics=(),
    )


def evidence() -> tuple[EvidenceReference, ...]:
    return (
        EvidenceReference(
            evidence_id="market-bar-1",
            title="Completed 5 minute bar",
            canonical_url="local://market-bar-1",
            published_at=NOW - timedelta(seconds=20),
            first_seen_at=NOW - timedelta(seconds=10),
            source_tier=2,
        ),
    )


def macro(score: str) -> MacroAnalysis:
    return MacroAnalysis(
        analysis_id="macro-1",
        as_of=NOW,
        decision=MacroAnalysisDecision.PUBLISH,
        regime="neutral",
        technical_alignment=Decimal("0.5"),
        macro_impact=Decimal(score),
        scenarios=(
            MacroScenario(
                name="base",
                probability=Decimal("1"),
                drivers=("event",),
                evidence_ids=("market-bar-1",),
            ),
        ),
        claims=(
            MacroClaim(
                text="Evidence-bound claim",
                evidence_ids=("market-bar-1",),
                contradictions=(),
            ),
        ),
        uncertainties=(),
        data_gaps=(),
        invalidation_conditions=(),
        reported_confidence="MEDIUM",
        refusal_reason="",
        model_version="mock-model",
    )


def test_missing_evidence_fails_closed() -> None:
    result = build_recommendation(technical(), ())

    assert result.decision is RecommendationDecision.ABSTAIN
    assert "MISSING_EVIDENCE" in result.reason_codes


def test_negative_macro_veto_downgrades_entry_to_watch() -> None:
    result = build_recommendation(technical(), evidence(), macro=macro("-0.8"))

    assert result.decision is RecommendationDecision.WATCH
    assert "MACRO_NEGATIVE_VETO" in result.reason_codes
    assert result.model_version == "mock-model"
    assert result.combined_score == Decimal("0.400")
    assert result.confidence is ConfidenceBand.UNCALIBRATED


def test_recommendation_identity_is_idempotent() -> None:
    first = build_recommendation(technical(), evidence(), macro=macro("0.2"))
    second = build_recommendation(technical(), evidence(), macro=macro("0.2"))

    assert first.recommendation_id == second.recommendation_id
    assert first.decision is RecommendationDecision.ENTER_CANDIDATE
    assert first.expires_at == NOW + timedelta(minutes=30)


def test_publish_macro_score_is_bounded_and_fused_at_configured_weights() -> None:
    result = build_recommendation(technical(), evidence(), macro=macro("0.2"))

    assert result.technical_score == Decimal("0.8")
    assert result.macro_score == Decimal("0.2")
    assert result.combined_score == Decimal("0.650")
    assert result.macro_evidence_coverage == Decimal("1")
    assert result.fusion_version == "technical-macro-fusion@1"
    assert result.technical_fusion_weight == Decimal("0.75")
    assert result.macro_fusion_weight == Decimal("0.25")
    assert "MACRO_FUSION_APPLIED" in result.fusion_reason_codes
    assert "MACRO_FUSION_POSITIVE" in result.fusion_reason_codes
    assert "COMBINED_SCORE_UNCALIBRATED" in result.fusion_reason_codes


def test_watch_macro_score_has_discounted_influence() -> None:
    watched = replace(macro("0.2"), decision=MacroAnalysisDecision.WATCH)

    result = build_recommendation(technical(), evidence(), macro=watched)

    # WATCH uses 12.5% macro and redistributes the unused 12.5% to technical.
    assert result.combined_score == Decimal("0.7250")
    assert result.technical_fusion_weight == Decimal("0.875")
    assert result.macro_fusion_weight == Decimal("0.125")
    assert "MACRO_FUSION_WATCH_DISCOUNTED" in result.fusion_reason_codes


def test_missing_macro_keeps_technical_score_without_silent_penalty() -> None:
    result = build_recommendation(technical(), evidence())

    assert result.combined_score == result.technical_score
    assert result.macro_score is None
    assert result.macro_evidence_coverage is None
    assert result.technical_fusion_weight == Decimal("1")
    assert result.macro_fusion_weight == Decimal("0")
    assert "MACRO_FUSION_NOT_AVAILABLE" in result.fusion_reason_codes


def test_macro_cannot_create_entry_when_technical_gate_did_not_admit_it() -> None:
    watch_signal = replace(
        technical(),
        decision=RecommendationDecision.WATCH,
        score=Decimal("0"),
    )
    config = RecommendationGateConfig(
        technical_weight=Decimal("0.60"),
        macro_weight=Decimal("0.40"),
        entry_combined_score_threshold=Decimal("0.30"),
    )

    result = build_recommendation(
        watch_signal,
        evidence(),
        macro=macro("1"),
        config=config,
    )

    assert result.combined_score == Decimal("0.40")
    assert result.decision is RecommendationDecision.WATCH
    assert "TECHNICAL_ENTRY_GATE_NOT_MET" in result.fusion_reason_codes


def test_macro_fusion_can_downgrade_marginal_technical_entry() -> None:
    marginal = replace(technical(), score=Decimal("0.55"))

    result = build_recommendation(marginal, evidence(), macro=macro("-0.30"))

    assert result.combined_score == Decimal("0.3375")
    assert result.decision is RecommendationDecision.WATCH
    assert "COMBINED_SCORE_BELOW_ENTRY_THRESHOLD" in result.reason_codes


def test_macro_score_is_ignored_when_retained_evidence_coverage_is_too_low() -> None:
    second = replace(
        evidence()[0],
        evidence_id="market-bar-2",
        title="Additional retained evidence",
        canonical_url="local://market-bar-2",
    )
    config = RecommendationGateConfig(
        minimum_macro_evidence_coverage=Decimal("0.75")
    )

    result = build_recommendation(
        technical(),
        (*evidence(), second),
        macro=macro("1"),
        config=config,
    )

    assert result.macro_score == Decimal("1")
    assert result.macro_evidence_coverage == Decimal("0.5")
    assert result.combined_score == result.technical_score
    assert result.decision is RecommendationDecision.WATCH
    assert "MACRO_EVIDENCE_INSUFFICIENT" in result.reason_codes
    assert "MACRO_FUSION_INSUFFICIENT_COVERAGE" in result.fusion_reason_codes


def test_explicit_macro_abstention_downgrades_entry_to_watch() -> None:
    abstained = MacroAnalysis(
        analysis_id="macro-1",
        as_of=NOW,
        decision=MacroAnalysisDecision.ABSTAIN,
        regime="unknown",
        technical_alignment=Decimal("0"),
        macro_impact=Decimal("0"),
        scenarios=(),
        claims=(),
        uncertainties=("MODEL_UNAVAILABLE",),
        data_gaps=(),
        invalidation_conditions=(),
        reported_confidence="UNCALIBRATED",
        refusal_reason="MODEL_UNAVAILABLE",
        model_version="fail-closed@1",
    )

    result = build_recommendation(technical(), evidence(), macro=abstained)

    assert result.decision is RecommendationDecision.WATCH
    assert "MACRO_ANALYSIS_UNAVAILABLE" in result.reason_codes
    assert result.combined_score == result.technical_score
    assert "MACRO_FUSION_ABSTAINED" in result.fusion_reason_codes


def test_gate_config_rejects_invalid_fusion_parameters() -> None:
    with pytest.raises(ValueError, match="must sum to one"):
        RecommendationGateConfig(
            technical_weight=Decimal("0.8"),
            macro_weight=Decimal("0.3"),
        )
    with pytest.raises(ValueError, match="must not exceed 0.40"):
        RecommendationGateConfig(
            technical_weight=Decimal("0.59"),
            macro_weight=Decimal("0.41"),
        )
