"""A 股盘中 LLM 复核的纯审计序列化工具。

这里仅负责把已验证的复核结果转换为安全、稳定、可哈希的审计文档。
模块不访问网络、LLM、缓存或持久化；调度和状态门禁仍由
:mod:`ashare_intraday_llm` facade 负责。
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, TypeAlias

from gribuki_trade.analysis.schemas import MacroAnalysis

if TYPE_CHECKING:
    from .ashare_intraday_llm import IntradayLLMReview


JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]


def intraday_llm_review_document(review: IntradayLLMReview) -> dict[str, object]:
    """返回安全审计载荷；其中不含提示词、正文和推理内容。"""

    analysis = review.analysis
    return {
        "analysis": _macro_analysis_document(analysis),
        "analyzer_identity": {
            "adapter_version": review.analyzer_identity.adapter_version,
            "identity_manifest_sha256": review.analyzer_identity.manifest_sha256,
            "prompt_schema_sha256": review.analyzer_identity.prompt_schema_sha256,
            "prompt_version": review.analyzer_identity.prompt_version,
            "provider_id": review.analyzer_identity.provider_id,
            "requested_model": review.analyzer_identity.requested_model,
        },
        "candidate_scope_sha256": review.candidate_scope_sha256,
        "completed_at": review.completed_at,
        "context_id": review.context_id,
        "evidence_as_of": review.evidence_as_of,
        "evidence_pack_sha256": review.evidence_pack_sha256,
        "expires_at": review.expires_at,
        "failure_code": review.failure_code,
        "dual_track": (
            None
            if review.baseline_analysis is None or review.adversarial_analysis is None
            else {
                "selected_track": review.selected_track,
                "audit_record_sha256": review.dual_audit_record_sha256,
                "baseline_analysis": _macro_analysis_document(
                    review.baseline_analysis
                ),
                "adversarial_analysis": _macro_analysis_document(
                    review.adversarial_analysis
                ),
            }
        ),
        "latency_ms": review.latency_ms,
        "plan_manifest_sha256": review.plan_manifest_sha256,
        "preopen_context_id": review.preopen_context_id,
        "references": [
            {
                "canonical_url": item.canonical_url,
                "evidence_id": item.evidence_id,
                "first_seen_at": item.first_seen_at,
                "published_at": item.published_at,
                "source_tier": item.source_tier,
                "title": item.title,
            }
            for item in review.references
        ],
        "request_sha256": review.request_sha256,
        "requested_at": review.requested_at,
        "response_model": review.response_model,
        "review_id": review.review_id,
        "scan_revision": review.scan_revision,
        "session_date": review.session_date,
        "symbol": review.symbol,
    }


def _macro_analysis_document(analysis: MacroAnalysis) -> dict[str, object]:
    """编码宏观分析的可审计字段，不泄露提示词或提供方正文。"""

    return {
        "analysis_id": analysis.analysis_id,
        "as_of": analysis.as_of,
        "claims": [
            {
                "contradictions": list(item.contradictions),
                "evidence_ids": list(item.evidence_ids),
                "text": item.text,
            }
            for item in analysis.claims
        ],
        "data_gaps": list(analysis.data_gaps),
        "decision": analysis.decision,
        "invalidation_conditions": list(analysis.invalidation_conditions),
        "macro_impact": analysis.macro_impact,
        "model_version": analysis.model_version,
        "refusal_reason": analysis.refusal_reason,
        "regime": analysis.regime,
        "reported_confidence": analysis.reported_confidence,
        "scenarios": [
            {
                "drivers": list(item.drivers),
                "evidence_ids": list(item.evidence_ids),
                "name": item.name,
                "probability": item.probability,
            }
            for item in analysis.scenarios
        ],
        "technical_alignment": analysis.technical_alignment,
        "uncertainties": list(analysis.uncertainties),
    }


def intraday_llm_document_sha256(document: Mapping[str, object]) -> str:
    """为确定性审计文档计算哈希，并拒绝非常规值。"""

    normalized = _normalize_json(document)
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _normalize_json(value: object) -> JSONValue:
    """将 Decimal、时间和枚举转成稳定 JSON 标量。"""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("audit documents must not contain non-finite floats")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("audit documents must not contain non-finite decimals")
        return format(value, "f")
    if isinstance(value, datetime):
        return _aware_utc(value, "document datetime").isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return _normalize_json(value.value)
    if isinstance(value, Mapping):
        normalized: dict[str, JSONValue] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError("audit document keys must be non-empty strings")
            normalized[key] = _normalize_json(item)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_json(item) for item in value]
    raise ValueError("audit document contains an unsupported value type")


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)
