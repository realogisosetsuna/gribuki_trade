"""Broker-independent research recommendation domain objects.

Recommendations are evidence-backed research records.  They deliberately do
not contain an executable order, account identifier, or broker operation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from gribuki_trade.domain.instruments import ResearchInstrumentProfile


class RecommendationDecision(StrEnum):
    """A deliberately small, non-executable recommendation vocabulary."""

    ENTER_CANDIDATE = "ENTER_CANDIDATE"
    WATCH = "WATCH"
    REDUCE = "REDUCE"
    ABSTAIN = "ABSTAIN"


class RecommendationHorizon(StrEnum):
    """Research horizon rather than an order time-in-force."""

    SHORT_1_TO_5_DAYS = "SHORT_1_TO_5_DAYS"
    SWING_1_TO_8_WEEKS = "SWING_1_TO_8_WEEKS"


class ConfidenceBand(StrEnum):
    """Coarse confidence before a statistically valid calibration exists."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    UNCALIBRATED = "UNCALIBRATED"


@dataclass(frozen=True, slots=True)
class EvidenceReference:
    """A compact reference to evidence retained by the evidence store."""

    evidence_id: str
    title: str
    canonical_url: str
    published_at: datetime
    first_seen_at: datetime
    source_tier: int

    def __post_init__(self) -> None:
        if not self.evidence_id.strip():
            raise ValueError("evidence_id must not be empty")
        if not self.title.strip():
            raise ValueError("evidence title must not be empty")
        if not self.canonical_url.strip():
            raise ValueError("canonical_url must not be empty")
        if not 0 <= self.source_tier <= 4:
            raise ValueError("source_tier must be between 0 and 4")


@dataclass(frozen=True, slots=True)
class ResearchRecommendation:
    """Immutable recommendation emitted after policy and evidence validation."""

    recommendation_id: str
    symbol: str
    as_of: datetime
    expires_at: datetime
    horizon: RecommendationHorizon
    decision: RecommendationDecision
    confidence: ConfidenceBand
    technical_score: Decimal
    macro_score: Decimal | None
    reference_price: Decimal | None
    invalidation_price: Decimal | None
    reason_codes: tuple[str, ...]
    uncertainties: tuple[str, ...]
    evidence: tuple[EvidenceReference, ...]
    strategy_version: str
    model_version: str | None = None
    analysis_mode: str | None = None
    target_session: date | None = None
    technical_metrics: tuple[tuple[str, Decimal], ...] = ()
    instrument_profile: ResearchInstrumentProfile | None = None
    combined_score: Decimal | None = None
    fusion_reason_codes: tuple[str, ...] = ()
    macro_evidence_coverage: Decimal | None = None
    fusion_version: str | None = None
    technical_fusion_weight: Decimal | None = None
    macro_fusion_weight: Decimal | None = None

    def __post_init__(self) -> None:
        if not self.recommendation_id.strip():
            raise ValueError("recommendation_id must not be empty")
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if self.expires_at <= self.as_of:
            raise ValueError("expires_at must be after as_of")
        if not Decimal("-1") <= self.technical_score <= Decimal("1"):
            raise ValueError("technical_score must be in [-1, 1]")
        if self.macro_score is not None and not (
            Decimal("-1") <= self.macro_score <= Decimal("1")
        ):
            raise ValueError("macro_score must be in [-1, 1]")
        if self.combined_score is not None and not (
            Decimal("-1") <= self.combined_score <= Decimal("1")
        ):
            raise ValueError("combined_score must be in [-1, 1]")
        if self.macro_evidence_coverage is not None and not (
            Decimal("0") <= self.macro_evidence_coverage <= Decimal("1")
        ):
            raise ValueError("macro_evidence_coverage must be in [0, 1]")
        technical_fusion_weight = self.technical_fusion_weight
        macro_fusion_weight = self.macro_fusion_weight
        if (technical_fusion_weight is None) != (macro_fusion_weight is None):
            raise ValueError("fusion weights must either both be set or both be absent")
        if technical_fusion_weight is not None and macro_fusion_weight is not None:
            fusion_weights = (technical_fusion_weight, macro_fusion_weight)
            if not all(
                value.is_finite() and Decimal("0") <= value <= Decimal("1")
                for value in fusion_weights
            ):
                raise ValueError("fusion weights must be finite values in [0, 1]")
            if fusion_weights[0] + fusion_weights[1] != Decimal("1"):
                raise ValueError("fusion weights must sum to one")
        if self.reference_price is not None and self.reference_price <= 0:
            raise ValueError("reference_price must be positive")
        if self.invalidation_price is not None and self.invalidation_price <= 0:
            raise ValueError("invalidation_price must be positive")
        if not self.reason_codes:
            raise ValueError("at least one reason code is required")
        if not self.strategy_version.strip():
            raise ValueError("strategy_version must not be empty")
        if self.analysis_mode is not None and not self.analysis_mode.strip():
            raise ValueError("analysis_mode must not be empty")
        if self.fusion_version is not None and not self.fusion_version.strip():
            raise ValueError("fusion_version must not be empty")
        if any(not code.strip() for code in self.fusion_reason_codes):
            raise ValueError("fusion reason codes must not be empty")
        if (
            self.instrument_profile is not None
            and self.instrument_profile.symbol != self.symbol
        ):
            raise ValueError("instrument profile symbol must match recommendation symbol")
        for name, value in self.technical_metrics:
            if not name.strip():
                raise ValueError("technical metric name must not be empty")
            if not value.is_finite():
                raise ValueError("technical metric value must be finite")
        if self.decision is RecommendationDecision.ENTER_CANDIDATE and not self.evidence:
            raise ValueError("ENTER_CANDIDATE recommendations require evidence")
