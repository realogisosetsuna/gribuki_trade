"""对抗宏观分析的纯请求、审计文档与哈希工具。

本模块不执行模型调用、网络访问或持久化；它只负责把分析边界编码为
稳定、可复现且可审计的值。``adversarial_macro.py`` 通过兼容别名继续
暴露历史私有 helper。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from decimal import Decimal

from gribuki_trade.analysis.schemas import (
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
)
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity

_MAX_ROLE_CLAIMS = 6
_MAX_CLAIM_CHARACTERS = 500
_MAX_INVALIDATION_CONDITIONS = 6

def _request_sha256(request: MacroAnalysisRequest) -> str:
    document = {
        "analysis_id": request.analysis_id,
        "as_of": request.as_of.isoformat(),
        "evidence": [
            {
                "content_hash": item.content_hash,
                "evidence_id": item.evidence_id,
                "first_seen_at": item.first_seen_at.isoformat(),
                "published_at": item.published_at.isoformat(),
                "publisher": item.publisher,
                "source_tier": item.source_tier,
            }
            for item in request.evidence
        ],
        "horizon": request.horizon,
        "symbol": request.symbol,
        "technical_summary": list(request.technical_summary),
    }
    return _document_sha256(document)

def _role_round_number(request: MacroAnalysisRequest) -> int | None:
    for item in request.technical_summary:
        if item.startswith("ADVERSARIAL_ROUND="):
            try:
                return int(item.removeprefix("ADVERSARIAL_ROUND="))
            except ValueError:
                return None
    return None

def _evidence_pack_sha256(request: MacroAnalysisRequest) -> str:
    return _document_sha256(
        {
            "evidence": [
                {
                    "content_hash": item.content_hash,
                    "evidence_id": item.evidence_id,
                    "first_seen_at": item.first_seen_at.isoformat(),
                    "published_at": item.published_at.isoformat(),
                    "publisher": item.publisher,
                    "source_tier": item.source_tier,
                }
                for item in request.evidence
            ]
        }
    )

def _prompt_contract_sha256(request: MacroAnalysisRequest) -> str:
    """角色 prompt 的安全指纹；不重复持久化新闻摘录或隐藏思维链。"""

    return _document_sha256(
        {
            "analysis_id": request.analysis_id,
            "horizon": request.horizon,
            "symbol": request.symbol,
            "technical_summary": list(request.technical_summary),
        }
    )

def _identity_document(identity: AnalyzerAuditIdentity) -> dict[str, object]:
    return {
        "provider_id": identity.provider_id,
        "requested_model": identity.requested_model,
        "adapter_version": identity.adapter_version,
        "prompt_version": identity.prompt_version,
        "prompt_schema_sha256": identity.prompt_schema_sha256,
        "manifest_sha256": identity.manifest_sha256,
    }

def _analysis_document(analysis: MacroAnalysis) -> dict[str, object]:
    return {
        "analysis_id": analysis.analysis_id,
        "as_of": analysis.as_of.isoformat(),
        "decision": analysis.decision.value,
        "regime": analysis.regime,
        "technical_alignment": str(analysis.technical_alignment),
        "macro_impact": str(analysis.macro_impact),
        "claims": [
            {
                "text": item.text,
                "evidence_ids": list(item.evidence_ids),
                "contradictions": list(item.contradictions),
            }
            for item in analysis.claims
        ],
        "scenarios": [
            {
                "name": item.name,
                "probability": str(item.probability),
                "drivers": list(item.drivers),
                "evidence_ids": list(item.evidence_ids),
            }
            for item in analysis.scenarios
        ],
        "uncertainties": list(analysis.uncertainties),
        "data_gaps": list(analysis.data_gaps),
        "invalidation_conditions": list(analysis.invalidation_conditions),
        "reported_confidence": analysis.reported_confidence,
        "refusal_reason": analysis.refusal_reason,
        "model_version": analysis.model_version,
    }

def _cross_track_conflict(
    baseline: MacroAnalysis,
    adversarial: MacroAnalysis,
    *,
    threshold: Decimal,
) -> bool:
    if (
        baseline.decision is MacroAnalysisDecision.ABSTAIN
        or adversarial.decision is MacroAnalysisDecision.ABSTAIN
    ):
        return False
    spread = abs(baseline.macro_impact - adversarial.macro_impact)
    opposite_material_signs = (
        baseline.macro_impact >= Decimal("0.20")
        and adversarial.macro_impact <= Decimal("-0.20")
    ) or (
        baseline.macro_impact <= Decimal("-0.20")
        and adversarial.macro_impact >= Decimal("0.20")
    )
    return spread >= threshold or opposite_material_signs

def _document_sha256(document: Mapping[str, object]) -> str:
    encoded = json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

def _median(values: Sequence[Decimal]) -> Decimal:
    if not values:
        raise ValueError("median requires at least one value")
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")

def _unique_text(values: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for value in values:
        text = _single_line(str(value))
        if text and text not in normalized:
            normalized.append(text)
    return tuple(normalized)

def _single_line(value: str) -> str:
    return " ".join(value.split())
