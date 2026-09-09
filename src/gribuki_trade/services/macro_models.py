"""宏观研究服务使用的配置、证据结果和运行结果模型。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

from gribuki_trade.analysis.schemas import EvidenceItem, MacroAnalysis, MacroAnalysisRequest
from gribuki_trade.domain.recommendations import EvidenceReference

if TYPE_CHECKING:
    from gribuki_trade.services.macro_research import MacroResearchPlan


@dataclass(frozen=True, slots=True)
class MacroEvidenceConfig:
    """不可信来源文本进入模型前应用的边界。"""

    max_age: timedelta = timedelta(days=14)
    max_events: int = 24
    max_events_per_source: int = 6
    max_excerpt_characters: int = 1_200

    def __post_init__(self) -> None:
        if self.max_age <= timedelta(0):
            raise ValueError("max_age must be positive")
        if self.max_events < 1 or self.max_events_per_source < 1:
            raise ValueError("event limits must be positive")
        if self.max_excerpt_characters < 100:
            raise ValueError("max_excerpt_characters must be at least 100")


@dataclass(frozen=True, slots=True)
class EvidenceCorroboration:
    """单条入模证据的来源独立性与事实确认边界。"""

    evidence_id: str
    status: str
    independent_publishers: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.evidence_id.strip():
            raise ValueError("evidence_id must not be empty")
        allowed = {
            "AUTHORITATIVE_SOURCE",
            "CORROBORATED_INDEPENDENT_SOURCES",
            "UNCORROBORATED_PUBLIC_MEDIA",
        }
        if self.status not in allowed:
            raise ValueError("unknown evidence corroboration status")
        if len(self.independent_publishers) != len(set(self.independent_publishers)):
            raise ValueError("independent publishers must be unique")


@dataclass(frozen=True, slots=True)
class EvidenceSelection:
    items: tuple[EvidenceItem, ...]
    references: tuple[EvidenceReference, ...]
    future_rejected: int = 0
    stale_rejected: int = 0
    irrelevant_rejected: int = 0
    injection_rejected: int = 0
    duplicate_rejected: int = 0
    source_limited: int = 0
    corroboration: tuple[EvidenceCorroboration, ...] = ()
    uncorroborated_public_media: int = 0


@dataclass(frozen=True, slots=True)
class MacroResearchRun:
    request: MacroAnalysisRequest
    analysis: MacroAnalysis
    selection: EvidenceSelection
    failure_code: str | None = None
    plan: MacroResearchPlan | None = None
    baseline_analysis: MacroAnalysis | None = None
    adversarial_analysis: MacroAnalysis | None = None
    selected_track: str | None = None
    dual_audit_document: Mapping[str, object] | None = None
    dual_audit_record_sha256: str | None = None
