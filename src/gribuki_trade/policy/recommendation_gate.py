"""Combine deterministic signals and optional macro analysis conservatively."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import ClassVar
from uuid import NAMESPACE_URL, uuid5

from gribuki_trade.analysis.schemas import MacroAnalysis, MacroAnalysisDecision
from gribuki_trade.domain.recommendations import (
    ConfidenceBand,
    EvidenceReference,
    RecommendationDecision,
    RecommendationHorizon,
    ResearchRecommendation,
)
from gribuki_trade.features.technical import TechnicalSignal


@dataclass(frozen=True, slots=True)
class RecommendationGateConfig:
    """Conservative, auditable technical/macro score fusion parameters.

    The weights are deliberately constrained to sum to one.  A macro score is
    only admitted when it is backed by retained evidence and the macro model
    did not abstain.  ``WATCH`` macro analyses receive a smaller effective
    weight because they explicitly communicate lower directional conviction.
    None of these scores are calibrated probabilities.
    """

    technical_weight: Decimal = Decimal("0.75")
    macro_weight: Decimal = Decimal("0.25")
    macro_watch_weight_multiplier: Decimal = Decimal("0.50")
    minimum_macro_evidence_coverage: Decimal = Decimal("0.10")
    entry_combined_score_threshold: Decimal = Decimal("0.35")
    macro_veto_threshold: Decimal = Decimal("-0.60")
    short_ttl: timedelta = timedelta(minutes=30)
    swing_ttl: timedelta = timedelta(days=1)
    fusion_version: str = "technical-macro-fusion@1"

    MAXIMUM_MACRO_WEIGHT: ClassVar[Decimal] = Decimal("0.40")

    def __post_init__(self) -> None:
        for name in (
            "technical_weight",
            "macro_weight",
            "macro_watch_weight_multiplier",
            "minimum_macro_evidence_coverage",
            "entry_combined_score_threshold",
            "macro_veto_threshold",
        ):
            if not getattr(self, name).is_finite():
                raise ValueError(f"{name} must be finite")
        if self.technical_weight <= 0 or self.macro_weight < 0:
            raise ValueError("technical_weight must be positive and macro_weight non-negative")
        if self.macro_weight > self.MAXIMUM_MACRO_WEIGHT:
            raise ValueError("macro_weight must not exceed 0.40")
        if self.technical_weight + self.macro_weight != Decimal("1"):
            raise ValueError("technical_weight and macro_weight must sum to one")
        if not Decimal("0") <= self.macro_watch_weight_multiplier <= Decimal("1"):
            raise ValueError("macro_watch_weight_multiplier must be in [0, 1]")
        if not Decimal("0") <= self.minimum_macro_evidence_coverage <= Decimal("1"):
            raise ValueError("minimum_macro_evidence_coverage must be in [0, 1]")
        if not Decimal("-1") <= self.entry_combined_score_threshold <= Decimal("1"):
            raise ValueError("entry_combined_score_threshold must be in [-1, 1]")
        if not Decimal("-1") <= self.macro_veto_threshold <= Decimal("0"):
            raise ValueError("macro_veto_threshold must be in [-1, 0]")
        if self.short_ttl <= timedelta(0) or self.swing_ttl <= timedelta(0):
            raise ValueError("recommendation TTLs must be positive")
        if not self.fusion_version.strip():
            raise ValueError("fusion_version must not be empty")


def build_recommendation(
    technical: TechnicalSignal,
    evidence: tuple[EvidenceReference, ...],
    *,
    macro: MacroAnalysis | None = None,
    config: RecommendationGateConfig | None = None,
) -> ResearchRecommendation:
    """Apply the publication gate without turning a recommendation into an order."""

    resolved = config or RecommendationGateConfig()
    decision = technical.decision
    reasons = list(technical.reason_codes)
    uncertainties: list[str] = []
    macro_score: Decimal | None = None
    combined_score = technical.score
    macro_evidence_coverage: Decimal | None = None
    effective_technical_weight = Decimal("1")
    effective_macro_weight = Decimal("0")
    fusion_reasons = ["COMBINED_SCORE_UNCALIBRATED"]
    model_version: str | None = None

    if not evidence and decision is RecommendationDecision.ENTER_CANDIDATE:
        decision = RecommendationDecision.ABSTAIN
        reasons.append("MISSING_EVIDENCE")

    if macro is None:
        uncertainties.append("MACRO_ANALYSIS_NOT_AVAILABLE")
        fusion_reasons.append("MACRO_FUSION_NOT_AVAILABLE")
    else:
        if macro.as_of != technical.as_of:
            raise ValueError("macro and technical analysis must share the same as_of")
        macro_score = macro.macro_impact
        model_version = macro.model_version
        uncertainties.extend(macro.uncertainties)
        uncertainties.extend(macro.data_gaps)
        macro_evidence_coverage, has_references, has_unknown_references = (
            _macro_evidence_coverage(macro, evidence)
        )
        if macro.decision is MacroAnalysisDecision.ABSTAIN:
            uncertainties.append("MACRO_ANALYSIS_ABSTAINED")
            fusion_reasons.append("MACRO_FUSION_ABSTAINED")
            if decision is RecommendationDecision.ENTER_CANDIDATE:
                decision = RecommendationDecision.WATCH
                reasons.append("MACRO_ANALYSIS_UNAVAILABLE")
        elif has_unknown_references:
            uncertainties.append("MACRO_REFERENCED_UNRETAINED_EVIDENCE")
            fusion_reasons.append("MACRO_FUSION_EVIDENCE_MISMATCH")
            if decision is RecommendationDecision.ENTER_CANDIDATE:
                decision = RecommendationDecision.WATCH
                reasons.append("MACRO_EVIDENCE_INVALID")
        elif not has_references:
            uncertainties.append("MACRO_ANALYSIS_HAS_NO_EVIDENCE_REFERENCES")
            fusion_reasons.append("MACRO_FUSION_NO_EVIDENCE_REFERENCES")
            if decision is RecommendationDecision.ENTER_CANDIDATE:
                decision = RecommendationDecision.WATCH
                reasons.append("MACRO_EVIDENCE_INSUFFICIENT")
        elif macro_evidence_coverage < resolved.minimum_macro_evidence_coverage:
            uncertainties.append("MACRO_EVIDENCE_COVERAGE_INSUFFICIENT")
            fusion_reasons.append("MACRO_FUSION_INSUFFICIENT_COVERAGE")
            if decision is RecommendationDecision.ENTER_CANDIDATE:
                decision = RecommendationDecision.WATCH
                reasons.append("MACRO_EVIDENCE_INSUFFICIENT")
        else:
            macro_multiplier = Decimal("1")
            if macro.decision is MacroAnalysisDecision.WATCH:
                macro_multiplier = resolved.macro_watch_weight_multiplier
                fusion_reasons.append("MACRO_FUSION_WATCH_DISCOUNTED")
            effective_macro_weight = resolved.macro_weight * macro_multiplier
            effective_technical_weight = Decimal("1") - effective_macro_weight
            combined_score = _bounded_score(
                technical.score * effective_technical_weight
                + macro_score * effective_macro_weight
            )
            fusion_reasons.append("MACRO_FUSION_APPLIED")
            if macro_score > 0:
                fusion_reasons.append("MACRO_FUSION_POSITIVE")
            elif macro_score < 0:
                fusion_reasons.append("MACRO_FUSION_NEGATIVE")
            else:
                fusion_reasons.append("MACRO_FUSION_NEUTRAL")

            if (
                decision is RecommendationDecision.ENTER_CANDIDATE
                and macro_score <= resolved.macro_veto_threshold
            ):
                decision = RecommendationDecision.WATCH
                reasons.append("MACRO_NEGATIVE_VETO")
            elif (
                decision is RecommendationDecision.ENTER_CANDIDATE
                and combined_score < resolved.entry_combined_score_threshold
            ):
                decision = RecommendationDecision.WATCH
                reasons.append("COMBINED_SCORE_BELOW_ENTRY_THRESHOLD")
            elif (
                decision is not RecommendationDecision.ENTER_CANDIDATE
                and combined_score >= resolved.entry_combined_score_threshold
            ):
                # Macro context may confirm or strengthen a technical setup,
                # but can never manufacture an entry that the deterministic
                # technical policy did not independently admit.
                fusion_reasons.append("TECHNICAL_ENTRY_GATE_NOT_MET")

    ttl = (
        resolved.short_ttl
        if technical.horizon is RecommendationHorizon.SHORT_1_TO_5_DAYS
        else resolved.swing_ttl
    )
    identity = "|".join(
        (
            technical.symbol,
            technical.as_of.isoformat(),
            technical.horizon.value,
            decision.value,
            technical.strategy_version,
            resolved.fusion_version,
            str(resolved.technical_weight),
            str(resolved.macro_weight),
            str(resolved.macro_watch_weight_multiplier),
            str(resolved.minimum_macro_evidence_coverage),
            str(resolved.entry_combined_score_threshold),
            str(resolved.macro_veto_threshold),
            str(combined_score),
            "" if macro_score is None else str(macro_score),
            "" if macro_evidence_coverage is None else str(macro_evidence_coverage),
            "" if macro is None else macro.analysis_id,
            "" if model_version is None else model_version,
            *(item.evidence_id for item in evidence),
        )
    )
    return ResearchRecommendation(
        recommendation_id=str(uuid5(NAMESPACE_URL, identity)),
        symbol=technical.symbol,
        as_of=technical.as_of,
        expires_at=technical.as_of + ttl,
        horizon=technical.horizon,
        decision=decision,
        confidence=ConfidenceBand.UNCALIBRATED,
        technical_score=technical.score,
        macro_score=macro_score,
        combined_score=combined_score,
        fusion_reason_codes=tuple(dict.fromkeys(fusion_reasons)),
        macro_evidence_coverage=macro_evidence_coverage,
        fusion_version=resolved.fusion_version,
        technical_fusion_weight=effective_technical_weight,
        macro_fusion_weight=effective_macro_weight,
        reference_price=technical.reference_price,
        invalidation_price=technical.invalidation_price,
        reason_codes=tuple(reasons),
        uncertainties=tuple(dict.fromkeys(uncertainties)),
        evidence=evidence,
        strategy_version=technical.strategy_version,
        model_version=model_version,
    )


def _macro_evidence_coverage(
    macro: MacroAnalysis,
    evidence: tuple[EvidenceReference, ...],
) -> tuple[Decimal, bool, bool]:
    """Return retained-evidence coverage and reference integrity flags."""

    referenced = {
        evidence_id
        for claim in macro.claims
        for evidence_id in claim.evidence_ids
    }
    referenced.update(
        evidence_id
        for scenario in macro.scenarios
        for evidence_id in scenario.evidence_ids
    )
    available = {item.evidence_id for item in evidence}
    if not available:
        return Decimal("0"), bool(referenced), bool(referenced)
    retained_references = referenced & available
    coverage = Decimal(len(retained_references)) / Decimal(len(available))
    return coverage, bool(referenced), bool(referenced - available)


def _bounded_score(value: Decimal) -> Decimal:
    return max(Decimal("-1"), min(Decimal("1"), value))
