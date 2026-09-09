"""具有时点约束的证据选择与失败关闭宏观分析。"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256

from gribuki_trade.analysis.schemas import (
    EvidenceItem,
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
)
from gribuki_trade.domain.events import NormalizedEvent
from gribuki_trade.domain.recommendations import EvidenceReference
from gribuki_trade.ports.llm_analyzer import (
    AnalyzerAuditIdentity,
    AuditableMacroAnalyzer,
    DualTrackMacroAnalyzer,
    MacroAnalyzer,
)
from gribuki_trade.services.macro.macro_evidence_selection import (
    _TECHNICAL_ONLY_ADDITIONAL_PUBLISHERS,
)
from gribuki_trade.services.macro.macro_evidence_selection import (
    select_macro_evidence as select_macro_evidence,
)
from gribuki_trade.services.macro.macro_models import (
    EvidenceCorroboration as EvidenceCorroboration,
)
from gribuki_trade.services.macro.macro_models import (
    EvidenceSelection as EvidenceSelection,
)
from gribuki_trade.services.macro.macro_models import (
    MacroEvidenceConfig as MacroEvidenceConfig,
)
from gribuki_trade.services.macro.macro_models import (
    MacroResearchRun as MacroResearchRun,
)


@dataclass(frozen=True, slots=True)
class MacroResearchPlan:
    """在调用任何供应商前准备、已冻结且受哈希约束的输入。"""

    request: MacroAnalysisRequest
    selection: EvidenceSelection
    references: tuple[EvidenceReference, ...]
    request_sha256: str
    evidence_pack_sha256: str
    manifest_sha256: str
    analyzer_identity: AnalyzerAuditIdentity | None
    eligible_for_analysis: bool

    def __post_init__(self) -> None:
        expected_analysis_id = _analysis_id(
            self.request.symbol,
            self.request.as_of,
            self.request.horizon,
            self.request.technical_summary,
            self.request.evidence,
        )
        if self.request.analysis_id != expected_analysis_id:
            raise ValueError("analysis_id does not match complete macro request input")
        expected_evidence = _evidence_pack_sha256(self.request.evidence)
        if self.evidence_pack_sha256 != expected_evidence:
            raise ValueError("evidence_pack_sha256 does not match request evidence")
        expected_request = _request_sha256(self.request)
        if self.request_sha256 != expected_request:
            raise ValueError("request_sha256 does not match macro request")
        expected_manifest = _macro_manifest_sha256(
            request_sha256=self.request_sha256,
            evidence_pack_sha256=self.evidence_pack_sha256,
            analyzer_identity=self.analyzer_identity,
            eligible_for_analysis=self.eligible_for_analysis,
        )
        if self.manifest_sha256 != expected_manifest:
            raise ValueError("manifest_sha256 does not match macro research plan")
        request_ids = tuple(item.evidence_id for item in self.request.evidence)
        reference_ids = tuple(item.evidence_id for item in self.references)
        if request_ids != reference_ids:
            raise ValueError("plan references must exactly match request evidence order")


class MacroResearchService:
    """选择有界证据，并调用本身不具备工具的模型。"""

    def __init__(
        self,
        analyzer: MacroAnalyzer,
        *,
        config: MacroEvidenceConfig | None = None,
        analyzer_identity: AnalyzerAuditIdentity | None = None,
    ) -> None:
        self._analyzer = analyzer
        self._config = config or MacroEvidenceConfig()
        discovered_identity = (
            analyzer.audit_identity if isinstance(analyzer, AuditableMacroAnalyzer) else None
        )
        if (
            analyzer_identity is not None
            and discovered_identity is not None
            and analyzer_identity != discovered_identity
        ):
            raise ValueError("explicit analyzer identity does not match adapter identity")
        self._analyzer_identity = analyzer_identity or discovered_identity

    @property
    def analyzer_identity(self) -> AnalyzerAuditIdentity | None:
        return self._analyzer_identity

    def prepare(
        self,
        *,
        symbol: str,
        as_of: datetime,
        horizon: str,
        technical_summary: Sequence[str],
        events: Sequence[NormalizedEvent],
        additional_evidence: Sequence[EvidenceItem] = (),
    ) -> MacroResearchPlan:
        """在不执行网络 I/O 的情况下冻结一个时点请求及其完整血缘。"""

        _require_aware(as_of, "as_of")
        canonical_symbol = _canonical_symbol(symbol)
        normalized_summary = tuple(_single_line(item) for item in technical_summary)
        selection = select_macro_evidence(
            canonical_symbol,
            as_of,
            events,
            config=self._config,
        )
        additional = tuple(additional_evidence)
        combined_evidence = additional + selection.items
        identifiers = [item.evidence_id for item in combined_evidence]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("additional evidence IDs must not collide")
        for item in additional:
            _require_aware(item.published_at, "additional evidence published_at")
            _require_aware(item.first_seen_at, "additional evidence first_seen_at")
            if item.first_seen_at > as_of or item.published_at > as_of:
                raise ValueError("additional evidence was not available at as_of")
        evidence_pack_sha256 = _evidence_pack_sha256(combined_evidence)
        analysis_id = _analysis_id(
            canonical_symbol,
            as_of,
            horizon,
            normalized_summary,
            combined_evidence,
        )
        request = MacroAnalysisRequest(
            analysis_id=analysis_id,
            symbol=canonical_symbol,
            as_of=as_of,
            horizon=horizon,
            technical_summary=normalized_summary,
            evidence=combined_evidence,
        )
        references = tuple(_evidence_reference(item) for item in combined_evidence)
        macro_capable_additional = any(
            item.publisher not in _TECHNICAL_ONLY_ADDITIONAL_PUBLISHERS for item in additional
        )
        eligible = bool(selection.items or macro_capable_additional)
        request_sha256 = _request_sha256(request)
        manifest_sha256 = _macro_manifest_sha256(
            request_sha256=request_sha256,
            evidence_pack_sha256=evidence_pack_sha256,
            analyzer_identity=self._analyzer_identity,
            eligible_for_analysis=eligible,
        )
        return MacroResearchPlan(
            request=request,
            selection=selection,
            references=references,
            request_sha256=request_sha256,
            evidence_pack_sha256=evidence_pack_sha256,
            manifest_sha256=manifest_sha256,
            analyzer_identity=self._analyzer_identity,
            eligible_for_analysis=eligible,
        )

    async def execute(self, plan: MacroResearchPlan) -> MacroResearchRun:
        """只执行为该分析器身份准备的精确计划。"""

        if plan.analyzer_identity != self._analyzer_identity:
            raise ValueError("macro research plan analyzer identity mismatch")
        request = plan.request
        selection = plan.selection
        if not plan.eligible_for_analysis:
            return MacroResearchRun(
                request=request,
                analysis=_abstain_analysis(request, "NO_ELIGIBLE_EVIDENCE"),
                selection=selection,
                plan=plan,
                failure_code="NO_ELIGIBLE_EVIDENCE",
            )
        try:
            dual = None
            if isinstance(self._analyzer, DualTrackMacroAnalyzer):
                dual = await self._analyzer.analyze_dual(request)
                analysis = dual.selected_analysis
            else:
                analysis = await self._analyzer.analyze(request)
            analysis.validate_against(request)
        except Exception as exc:
            # 上游错误可能包含请求标识或响应片段；研究记录中只保留显式结构化的稳定代码。
            failure_code = _sanitized_analyzer_failure_code(exc)
            return MacroResearchRun(
                request=request,
                analysis=_abstain_analysis(request, failure_code),
                selection=selection,
                plan=plan,
                failure_code=failure_code,
            )
        return MacroResearchRun(
            request=request,
            analysis=analysis,
            selection=selection,
            plan=plan,
            failure_code=None if dual is None else dual.failure_code,
            baseline_analysis=None if dual is None else dual.baseline_analysis,
            adversarial_analysis=(None if dual is None else dual.adversarial_analysis),
            selected_track=None if dual is None else dual.selected_track,
            dual_audit_document=None if dual is None else dual.audit_document,
            dual_audit_record_sha256=(None if dual is None else dual.audit_record_sha256),
        )

    async def analyze(
        self,
        *,
        symbol: str,
        as_of: datetime,
        horizon: str,
        technical_summary: Sequence[str],
        events: Sequence[NormalizedEvent],
        additional_evidence: Sequence[EvidenceItem] = (),
    ) -> MacroResearchRun:
        plan = self.prepare(
            symbol=symbol,
            as_of=as_of,
            horizon=horizon,
            technical_summary=technical_summary,
            events=events,
            additional_evidence=additional_evidence,
        )
        return await self.execute(plan)


def _sanitized_analyzer_failure_code(error: Exception) -> str:
    code = getattr(error, "error_code", None)
    if not isinstance(code, str):
        return "ANALYZER_FAILED"
    normalized = code.strip().upper()
    if not normalized.startswith("DEEPSEEK_"):
        return "ANALYZER_FAILED"
    if not normalized.replace("_", "").isalnum() or len(normalized) > 80:
        return "ANALYZER_FAILED"
    return normalized



def _analysis_id(
    symbol: str,
    as_of: datetime,
    horizon: str,
    technical_summary: Sequence[str],
    evidence: Sequence[EvidenceItem],
) -> str:
    document = {
        "as_of": as_of.astimezone(UTC).isoformat(),
        "evidence": [_evidence_document(item) for item in evidence],
        "horizon": horizon,
        "schema_version": 2,
        "symbol": _canonical_symbol(symbol),
        "technical_summary": list(technical_summary),
    }
    return _document_sha256(document)


def _evidence_reference(item: EvidenceItem) -> EvidenceReference:
    return EvidenceReference(
        evidence_id=item.evidence_id,
        title=item.title,
        canonical_url=item.canonical_url,
        published_at=item.published_at,
        first_seen_at=item.first_seen_at,
        source_tier=item.source_tier,
    )


def _evidence_document(item: EvidenceItem) -> dict[str, object]:
    _require_aware(item.published_at, "evidence published_at")
    _require_aware(item.first_seen_at, "evidence first_seen_at")
    return {
        "canonical_url": item.canonical_url,
        "content_hash": item.content_hash,
        "evidence_id": item.evidence_id,
        "excerpt": item.excerpt,
        "first_seen_at": item.first_seen_at.astimezone(UTC).isoformat(),
        "published_at": item.published_at.astimezone(UTC).isoformat(),
        "publisher": item.publisher,
        "source_tier": item.source_tier,
        "title": item.title,
    }


def _evidence_pack_sha256(evidence: Sequence[EvidenceItem]) -> str:
    return _document_sha256(
        {
            "evidence": [_evidence_document(item) for item in evidence],
            "schema_version": 1,
        }
    )


def _request_sha256(request: MacroAnalysisRequest) -> str:
    _require_aware(request.as_of, "request as_of")
    return _document_sha256(
        {
            "analysis_id": request.analysis_id,
            "as_of": request.as_of.astimezone(UTC).isoformat(),
            "evidence": [_evidence_document(item) for item in request.evidence],
            "horizon": request.horizon,
            "schema_version": 1,
            "symbol": request.symbol,
            "technical_summary": list(request.technical_summary),
        }
    )


def _macro_manifest_sha256(
    *,
    request_sha256: str,
    evidence_pack_sha256: str,
    analyzer_identity: AnalyzerAuditIdentity | None,
    eligible_for_analysis: bool,
) -> str:
    identity_document: dict[str, object] | None = None
    if analyzer_identity is not None:
        identity_document = {
            "adapter_version": analyzer_identity.adapter_version,
            "identity_manifest_sha256": analyzer_identity.manifest_sha256,
            "prompt_schema_sha256": analyzer_identity.prompt_schema_sha256,
            "prompt_version": analyzer_identity.prompt_version,
            "provider_id": analyzer_identity.provider_id,
            "requested_model": analyzer_identity.requested_model,
        }
    return _document_sha256(
        {
            "analyzer_identity": identity_document,
            "eligible_for_analysis": eligible_for_analysis,
            "evidence_pack_sha256": evidence_pack_sha256,
            "request_sha256": request_sha256,
            "schema_version": 1,
        }
    )


def _document_sha256(document: dict[str, object]) -> str:
    encoded = json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return sha256(encoded.encode("utf-8")).hexdigest()


def _abstain_analysis(request: MacroAnalysisRequest, reason: str) -> MacroAnalysis:
    return MacroAnalysis(
        analysis_id=request.analysis_id,
        as_of=request.as_of,
        decision=MacroAnalysisDecision.ABSTAIN,
        regime="unknown",
        technical_alignment=Decimal("0"),
        macro_impact=Decimal("0"),
        scenarios=(),
        claims=(),
        uncertainties=(reason,),
        data_gaps=("MACRO_ANALYSIS_UNAVAILABLE",),
        invalidation_conditions=(),
        reported_confidence="UNCALIBRATED",
        refusal_reason=reason,
        model_version="local-fail-closed@1",
    )


def _canonical_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if len(value) == 6 and value.isdigit():
        if value.startswith(("4", "8", "92")):
            exchange = "BJ"
        else:
            exchange = "SH" if value.startswith(("5", "6", "9")) else "SZ"
        return f"{value}.{exchange}"
    if len(value) == 9 and value[6] == ".":
        code, exchange = value.split(".", maxsplit=1)
        if code.isdigit() and exchange in {"SH", "SZ", "BJ"}:
            return value
    raise ValueError("symbol must look like 600000.SH, 000001.SZ, or 430047.BJ")


def _single_line(value: str) -> str:
    return " ".join(value.split())


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
