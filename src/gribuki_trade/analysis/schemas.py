"""受证据约束的 LLM 宏观分析严格领域模式。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class MacroAnalysisDecision(StrEnum):
    PUBLISH = "PUBLISH"
    WATCH = "WATCH"
    ABSTAIN = "ABSTAIN"


@dataclass(frozen=True, slots=True)
class EvidenceItem:
    evidence_id: str
    publisher: str
    source_tier: int
    published_at: datetime
    first_seen_at: datetime
    title: str
    excerpt: str
    canonical_url: str
    content_hash: str

    def __post_init__(self) -> None:
        for name in (
            "evidence_id",
            "publisher",
            "title",
            "canonical_url",
            "content_hash",
        ):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} must not be empty")
        if not 0 <= self.source_tier <= 4:
            raise ValueError("source_tier must be between 0 and 4")
        if self.first_seen_at < self.published_at:
            # 来源时间戳有时会在事后修正，因此只容许相同或更晚的观测时间；
            # 对不可能成立的时间顺序采用失败关闭。
            raise ValueError("first_seen_at must not precede published_at")


@dataclass(frozen=True, slots=True)
class MacroAnalysisRequest:
    analysis_id: str
    symbol: str
    as_of: datetime
    horizon: str
    technical_summary: tuple[str, ...]
    evidence: tuple[EvidenceItem, ...]

    def __post_init__(self) -> None:
        if not self.analysis_id.strip() or not self.symbol.strip() or not self.horizon.strip():
            raise ValueError("analysis_id, symbol, and horizon are required")
        if any(item.first_seen_at > self.as_of for item in self.evidence):
            raise ValueError("request includes evidence not available at as_of")
        identifiers = [item.evidence_id for item in self.evidence]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("evidence IDs must be unique")


@dataclass(frozen=True, slots=True)
class MacroClaim:
    text: str
    evidence_ids: tuple[str, ...]
    contradictions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MacroScenario:
    name: str
    probability: Decimal
    drivers: tuple[str, ...]
    evidence_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MacroAnalysis:
    analysis_id: str
    as_of: datetime
    decision: MacroAnalysisDecision
    regime: str
    technical_alignment: Decimal
    macro_impact: Decimal
    scenarios: tuple[MacroScenario, ...]
    claims: tuple[MacroClaim, ...]
    uncertainties: tuple[str, ...]
    data_gaps: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]
    reported_confidence: str
    refusal_reason: str
    model_version: str

    def validate_against(self, request: MacroAnalysisRequest) -> None:
        """拒绝引用缺失或未来证据，以及格式错误的概率。"""

        if self.analysis_id != request.analysis_id or self.as_of != request.as_of:
            raise ValueError("analysis response identity/as_of mismatch")
        for score_name in ("technical_alignment", "macro_impact"):
            score = getattr(self, score_name)
            if not Decimal("-1") <= score <= Decimal("1"):
                raise ValueError(f"{score_name} must be in [-1, 1]")
        if self.scenarios:
            total = sum((scenario.probability for scenario in self.scenarios), Decimal("0"))
            if abs(total - Decimal("1")) > Decimal("0.0001"):
                raise ValueError("scenario probabilities must sum to one")
            if any(
                scenario.probability < 0 or scenario.probability > 1
                for scenario in self.scenarios
            ):
                raise ValueError("scenario probabilities must be in [0, 1]")
        available = {item.evidence_id for item in request.evidence}
        referenced: set[str] = set()
        for claim in self.claims:
            if not claim.text.strip() or not claim.evidence_ids:
                raise ValueError("every macro claim requires text and evidence")
            referenced.update(claim.evidence_ids)
        for scenario in self.scenarios:
            if not scenario.name.strip() or not scenario.drivers:
                raise ValueError("every macro scenario requires a name and drivers")
            if not scenario.evidence_ids:
                raise ValueError("every macro scenario requires evidence")
            referenced.update(scenario.evidence_ids)
        unknown = referenced - available
        if unknown:
            raise ValueError(f"analysis referenced unknown evidence IDs: {sorted(unknown)}")
        if self.decision is MacroAnalysisDecision.PUBLISH and not self.claims:
            raise ValueError("PUBLISH macro analysis requires evidence-backed claims")
