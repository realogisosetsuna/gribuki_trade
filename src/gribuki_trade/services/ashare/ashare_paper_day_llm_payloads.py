"""纯 LLM 审计文档恢复、类型校验与兼容投影辅助函数。

本模块只处理已经落盘的不可变审计载荷：严格校验类型、恢复领域对象，
并委托现有投影模块生成门禁文档。它不访问网络、LLM、存储或运行器状态。
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, cast

from gribuki_trade.analysis.schemas import (
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroClaim,
    MacroScenario,
)
from gribuki_trade.domain.recommendations import EvidenceReference
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
from gribuki_trade.reporting.contracts import humanize_internal_code
from gribuki_trade.services.ashare import ashare_paper_day_projection as _paper_day_projection
from gribuki_trade.services.ashare.ashare_intraday_llm import (
    IntradayLLMGateOutcome,
    IntradayLLMReview,
)
from gribuki_trade.services.ashare.ashare_paper_day_serialization import (
    _aware_utc,
    _decimal_display,
)

if TYPE_CHECKING:
    from .ashare_paper_day import PaperDayLLMPreopenContext


def _llm_analyzer_identity_document(
    identity: AnalyzerAuditIdentity,
) -> dict[str, object]:
    return {
        "adapter_version": identity.adapter_version,
        "identity_manifest_sha256": identity.manifest_sha256,
        "prompt_schema_sha256": identity.prompt_schema_sha256,
        "prompt_version": identity.prompt_version,
        "provider_id": identity.provider_id,
        "requested_model": identity.requested_model,
    }


def _llm_analyzer_identity_from_document(value: object) -> AnalyzerAuditIdentity:
    document = _llm_object(value, "analyzer_identity")
    identity = AnalyzerAuditIdentity(
        provider_id=_llm_string(document.get("provider_id"), "provider_id"),
        requested_model=_llm_string(document.get("requested_model"), "requested_model"),
        adapter_version=_llm_string(document.get("adapter_version"), "adapter_version"),
        prompt_version=_llm_string(document.get("prompt_version"), "prompt_version"),
        prompt_schema_sha256=_llm_string(
            document.get("prompt_schema_sha256"), "prompt_schema_sha256"
        ),
    )
    retained_hash = document.get("identity_manifest_sha256")
    if retained_hash is not None and retained_hash != identity.manifest_sha256:
        raise ValueError("retained analyzer identity hash mismatch")
    return identity


def _llm_preopen_context_from_document(value: object) -> PaperDayLLMPreopenContext:
    # 延迟导入避免 facade -> payloads 的初始化循环；调用发生在模块初始化后。
    from .ashare_paper_day import PaperDayLLMPreopenContext

    document = _llm_object(value, "preopen_context")
    baseline_decision: MacroAnalysisDecision | None = None
    baseline_macro_impact: Decimal | None = None
    baseline_model: str | None = None
    adversarial_decision: MacroAnalysisDecision | None = None
    adversarial_macro_impact: Decimal | None = None
    adversarial_model: str | None = None
    selected_track: str | None = None
    dual_audit_record_sha256: str | None = None
    dual_value = document.get("dual_track")
    if dual_value is not None:
        dual = _llm_object(dual_value, "preopen_context.dual_track")
        baseline = _llm_object(
            dual.get("baseline"), "preopen_context.dual_track.baseline"
        )
        adversarial = _llm_object(
            dual.get("adversarial"), "preopen_context.dual_track.adversarial"
        )
        baseline_decision = MacroAnalysisDecision(
            _llm_string(
                baseline.get("decision"),
                "preopen_context.dual_track.baseline.decision",
            )
        )
        baseline_macro_impact = _llm_decimal(
            baseline.get("macro_impact"),
            "preopen_context.dual_track.baseline.macro_impact",
        )
        baseline_model = _llm_string(
            baseline.get("model"), "preopen_context.dual_track.baseline.model"
        )
        adversarial_decision = MacroAnalysisDecision(
            _llm_string(
                adversarial.get("decision"),
                "preopen_context.dual_track.adversarial.decision",
            )
        )
        adversarial_macro_impact = _llm_decimal(
            adversarial.get("macro_impact"),
            "preopen_context.dual_track.adversarial.macro_impact",
        )
        adversarial_model = _llm_string(
            adversarial.get("model"),
            "preopen_context.dual_track.adversarial.model",
        )
        selected_track = _llm_string(
            dual.get("selected_track"), "preopen_context.dual_track.selected_track"
        )
        dual_audit_record_sha256 = _llm_string(
            dual.get("audit_record_sha256"),
            "preopen_context.dual_track.audit_record_sha256",
        )
    return PaperDayLLMPreopenContext(
        context_id=_llm_string(document.get("context_id"), "context_id"),
        evidence_as_of=_llm_datetime(document.get("evidence_as_of"), "evidence_as_of"),
        known_at=_llm_datetime(document.get("known_at"), "known_at"),
        valid_until=_llm_datetime(document.get("valid_until"), "valid_until"),
        analysis_id=_llm_string(document.get("analysis_id"), "analysis_id"),
        decision=MacroAnalysisDecision(_llm_string(document.get("decision"), "decision")),
        macro_impact=_llm_decimal(document.get("macro_impact"), "macro_impact"),
        evidence_pack_sha256=_llm_string(
            document.get("evidence_pack_sha256"), "evidence_pack_sha256"
        ),
        request_sha256=_llm_string(document.get("request_sha256"), "request_sha256"),
        plan_manifest_sha256=_llm_string(
            document.get("plan_manifest_sha256"), "plan_manifest_sha256"
        ),
        analyzer_identity=_llm_analyzer_identity_from_document(document.get("analyzer_identity")),
        response_model=_llm_string(document.get("response_model"), "response_model"),
        baseline_decision=baseline_decision,
        baseline_macro_impact=baseline_macro_impact,
        baseline_model=baseline_model,
        adversarial_decision=adversarial_decision,
        adversarial_macro_impact=adversarial_macro_impact,
        adversarial_model=adversarial_model,
        selected_track=selected_track,
        dual_audit_record_sha256=dual_audit_record_sha256,
    )


def _llm_macro_analysis_from_document(value: object, name: str) -> MacroAnalysis:
    """从不可变事件文档严格恢复一条宏观分析，不接受缺字段或弱类型值。"""

    analysis_document = _llm_object(value, name)
    claims = tuple(
        MacroClaim(
            text=_llm_string(item.get("text"), "claim text"),
            evidence_ids=_llm_string_tuple(item.get("evidence_ids"), "claim evidence_ids"),
            contradictions=_llm_string_tuple(item.get("contradictions"), "claim contradictions"),
        )
        for item in _llm_object_list(analysis_document.get("claims"), "claims")
    )
    scenarios = tuple(
        MacroScenario(
            name=_llm_string(item.get("name"), "scenario name"),
            probability=_llm_decimal(item.get("probability"), "scenario probability"),
            drivers=_llm_string_tuple(item.get("drivers"), "scenario drivers"),
            evidence_ids=_llm_string_tuple(item.get("evidence_ids"), "scenario evidence_ids"),
        )
        for item in _llm_object_list(analysis_document.get("scenarios"), "scenarios")
    )
    return MacroAnalysis(
        analysis_id=_llm_string(analysis_document.get("analysis_id"), "analysis_id"),
        as_of=_llm_datetime(analysis_document.get("as_of"), "analysis as_of"),
        decision=MacroAnalysisDecision(
            _llm_string(analysis_document.get("decision"), "analysis decision")
        ),
        regime=_llm_string(analysis_document.get("regime"), "regime"),
        technical_alignment=_llm_decimal(
            analysis_document.get("technical_alignment"), "technical_alignment"
        ),
        macro_impact=_llm_decimal(analysis_document.get("macro_impact"), "macro_impact"),
        scenarios=scenarios,
        claims=claims,
        uncertainties=_llm_string_tuple(analysis_document.get("uncertainties"), "uncertainties"),
        data_gaps=_llm_string_tuple(analysis_document.get("data_gaps"), "data_gaps"),
        invalidation_conditions=_llm_string_tuple(
            analysis_document.get("invalidation_conditions"),
            "invalidation_conditions",
        ),
        reported_confidence=_llm_string(
            analysis_document.get("reported_confidence"), "reported_confidence"
        ),
        refusal_reason=_llm_string(
            analysis_document.get("refusal_reason"),
            "refusal_reason",
            allow_empty=True,
        ),
        model_version=_llm_string(analysis_document.get("model_version"), "model_version"),
    )


def _llm_review_from_document(value: object) -> IntradayLLMReview:
    document = _llm_object(value, "review")
    analysis = _llm_macro_analysis_from_document(document.get("analysis"), "analysis")
    baseline_analysis: MacroAnalysis | None = None
    adversarial_analysis: MacroAnalysis | None = None
    selected_track: str | None = None
    dual_audit_record_sha256: str | None = None
    dual_value = document.get("dual_track")
    if dual_value is not None:
        dual_document = _llm_object(dual_value, "dual_track")
        baseline_analysis = _llm_macro_analysis_from_document(
            dual_document.get("baseline_analysis"), "dual_track.baseline_analysis"
        )
        adversarial_analysis = _llm_macro_analysis_from_document(
            dual_document.get("adversarial_analysis"), "dual_track.adversarial_analysis"
        )
        selected_track = _llm_string(
            dual_document.get("selected_track"), "dual_track.selected_track"
        )
        audit_value = dual_document.get("audit_record_sha256")
        dual_audit_record_sha256 = (
            None
            if audit_value is None
            else _llm_string(audit_value, "dual_track.audit_record_sha256")
        )
    references = tuple(
        EvidenceReference(
            evidence_id=_llm_string(item.get("evidence_id"), "evidence_id"),
            title=_llm_string(item.get("title"), "evidence title"),
            canonical_url=_llm_string(item.get("canonical_url"), "canonical_url"),
            published_at=_llm_datetime(item.get("published_at"), "published_at"),
            first_seen_at=_llm_datetime(item.get("first_seen_at"), "first_seen_at"),
            source_tier=_llm_int(item.get("source_tier"), "source_tier"),
        )
        for item in _llm_object_list(document.get("references"), "references")
    )
    failure_value = document.get("failure_code")
    failure_code = None if failure_value is None else _llm_string(failure_value, "failure_code")
    return IntradayLLMReview(
        review_id=_llm_string(document.get("review_id"), "review_id"),
        context_id=_llm_string(document.get("context_id"), "context_id"),
        session_date=_llm_date(document.get("session_date"), "session_date"),
        symbol=_llm_string(document.get("symbol"), "symbol"),
        preopen_context_id=_llm_string(document.get("preopen_context_id"), "preopen_context_id"),
        scan_revision=_llm_string(document.get("scan_revision"), "scan_revision"),
        candidate_scope_sha256=_llm_string(
            document.get("candidate_scope_sha256"), "candidate_scope_sha256"
        ),
        analysis=analysis,
        references=references,
        request_sha256=_llm_string(document.get("request_sha256"), "request_sha256"),
        evidence_pack_sha256=_llm_string(
            document.get("evidence_pack_sha256"), "evidence_pack_sha256"
        ),
        plan_manifest_sha256=_llm_string(
            document.get("plan_manifest_sha256"), "plan_manifest_sha256"
        ),
        analyzer_identity=_llm_analyzer_identity_from_document(document.get("analyzer_identity")),
        evidence_as_of=_llm_datetime(document.get("evidence_as_of"), "evidence_as_of"),
        requested_at=_llm_datetime(document.get("requested_at"), "requested_at"),
        completed_at=_llm_datetime(document.get("completed_at"), "completed_at"),
        expires_at=_llm_datetime(document.get("expires_at"), "expires_at"),
        response_model=_llm_string(document.get("response_model"), "response_model"),
        latency_ms=_llm_int(document.get("latency_ms"), "latency_ms"),
        failure_code=failure_code,
        baseline_analysis=baseline_analysis,
        adversarial_analysis=adversarial_analysis,
        selected_track=selected_track,
        dual_audit_record_sha256=dual_audit_record_sha256,
    )


def llm_gate_document(outcome: IntradayLLMGateOutcome) -> dict[str, object]:
    """保持旧模块路径，委托给纯投影模块。"""
    return _paper_day_projection.llm_gate_document(outcome)


def _llm_preopen_dual_text(context: PaperDayLLMPreopenContext) -> str:
    """在用户通知中完整展示盘前双轨，旧记录缺失时不补造。"""

    if not context.has_dual_track:
        return "盘前双轨：历史冻结记录未携带两轨详情，当前不可用，系统未补造模型结论。"
    assert context.baseline_decision is not None
    assert context.baseline_macro_impact is not None
    assert context.baseline_model is not None
    assert context.adversarial_decision is not None
    assert context.adversarial_macro_impact is not None
    assert context.adversarial_model is not None
    assert context.selected_track is not None
    assert context.dual_audit_record_sha256 is not None
    return (
        "盘前双轨：原单分析器="
        f"{humanize_internal_code(context.baseline_decision.value)} / "
        f"{_decimal_display(context.baseline_macro_impact)}（模型 "
        f"{context.baseline_model}）；"
        "结构化对抗分析器="
        f"{humanize_internal_code(context.adversarial_decision.value)} / "
        f"{_decimal_display(context.adversarial_macro_impact)}（模型 "
        f"{context.adversarial_model}）；"
        f"生产采用={humanize_internal_code(context.selected_track)}；"
        f"独立审计摘要={context.dual_audit_record_sha256[:12]}"
    )


def llm_gate_dual_document(outcome: IntradayLLMGateOutcome) -> dict[str, object]:
    """保持旧模块路径，委托给纯投影模块。"""
    return _paper_day_projection.llm_gate_dual_document(outcome)


def llm_gate_dual_text(outcome: IntradayLLMGateOutcome) -> str:
    """保持旧模块路径，委托给纯投影模块。"""
    return _paper_day_projection.llm_gate_dual_text(outcome)


def _llm_object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _llm_object_list(value: object, name: str) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be a list")
    return tuple(_llm_object(item, name) for item in value)


def _llm_string(value: object, name: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise TypeError(f"{name} must be a string")
    return value


def _llm_string_tuple(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise TypeError(f"{name} must be a string list")
    return tuple(cast(list[str], value))


def _llm_datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be an ISO datetime")
    return _aware_utc(datetime.fromisoformat(value), name)


def _llm_date(value: object, name: str) -> date:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be an ISO date")
    return date.fromisoformat(value)


def _llm_decimal(value: object, name: str) -> Decimal:
    if value is None or isinstance(value, bool):
        raise TypeError(f"{name} must be numeric")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _llm_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value
