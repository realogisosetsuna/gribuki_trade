"""对抗宏观分析的纯策略、校验与聚合投影。

这里没有模型调用、计时器或持久化；函数只根据已完成的角色结果构造稳定的
协议文档、校验失败原因和保守聚合结果。门面 ``adversarial_macro`` 保留历史
私有 helper 名称，以便现有调用方和测试继续工作。
"""

from __future__ import annotations

from decimal import Decimal
from typing import Protocol

from gribuki_trade.analysis.schemas import (
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
    MacroClaim,
)
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
from gribuki_trade.services.macro.adversarial_macro_serialization import (
    _MAX_CLAIM_CHARACTERS,
    _MAX_INVALIDATION_CONDITIONS,
    _MAX_ROLE_CLAIMS,
    _median,
    _single_line,
    _unique_text,
)


class _ConfigLike(Protocol):
    @property
    def material_disagreement_threshold(self) -> Decimal: ...

    @property
    def depth(self) -> object: ...


class _OpinionLike(Protocol):
    @property
    def role(self) -> object: ...

    @property
    def analysis(self) -> MacroAnalysis: ...


class _RoundLike(Protocol):
    @property
    def round_number(self) -> int: ...

    @property
    def opinions(self) -> tuple[_OpinionLike, ...]: ...


def _role_value(role: object) -> object:
    return getattr(role, "value", role)


def _role_output_failure(
    analysis: MacroAnalysis,
    *,
    role_request: MacroAnalysisRequest,
    original_request: MacroAnalysisRequest,
    base_identity: AnalyzerAuditIdentity,
) -> str | None:
    """验证模型返回是否满足角色输出契约，不泄露 provider 原始异常。"""

    try:
        analysis.validate_against(role_request)
    except ValueError:
        return "ADVERSARIAL_ROLE_OUTPUT_INVALID"
    if analysis.model_version != base_identity.requested_model:
        return "ADVERSARIAL_ROLE_MODEL_MISMATCH"
    if analysis.decision is MacroAnalysisDecision.ABSTAIN:
        return "ADVERSARIAL_REQUIRED_ROLE_ABSTAINED"
    if not analysis.claims or not analysis.invalidation_conditions:
        return "ADVERSARIAL_ROLE_EVIDENCE_OR_FALSIFIER_MISSING"
    if len(analysis.invalidation_conditions) < len(analysis.claims):
        return "ADVERSARIAL_ROLE_EVIDENCE_OR_FALSIFIER_MISSING"
    available = {item.evidence_id for item in original_request.evidence}
    uncorroborated_media = {
        item.evidence_id
        for item in original_request.evidence
        if "单一公共媒体且未经独立印证" in item.excerpt
    }
    for claim in analysis.claims:
        if (
            not claim.text.strip()
            or len(claim.text) > _MAX_CLAIM_CHARACTERS
            or not claim.evidence_ids
            or not set(claim.evidence_ids).issubset(available)
            or any(
                not item.strip() or len(item) > _MAX_CLAIM_CHARACTERS
                for item in claim.contradictions
            )
        ):
            return "ADVERSARIAL_ROLE_OUTPUT_INVALID"
        if set(claim.evidence_ids).issubset(uncorroborated_media):
            return "ADVERSARIAL_UNCORROBORATED_MEDIA_CLAIM"
    if len(analysis.claims) > _MAX_ROLE_CLAIMS:
        return "ADVERSARIAL_ROLE_OUTPUT_INVALID"
    if len(analysis.invalidation_conditions) > _MAX_INVALIDATION_CONDITIONS or any(
        not item.strip() or len(item) > _MAX_CLAIM_CHARACTERS
        for item in analysis.invalidation_conditions
    ):
        return "ADVERSARIAL_ROLE_OUTPUT_INVALID"
    return None


def _peer_document(
    previous_round: _RoundLike | None,
    *,
    receiving_role: object,
) -> dict[str, object]:
    """将上一轮结果压缩成有界、不可当作证据的 peer envelope。"""

    if previous_round is None:
        return {
            "arguments": [],
            "envelope_version": "untrusted-peer-arguments@1",
            "round_number": 0,
        }
    arguments: list[dict[str, object]] = []
    for opinion in previous_round.opinions:
        if opinion.role is receiving_role:
            continue
        arguments.append(
            {
                "claims": [
                    {
                        "evidence_ids": list(claim.evidence_ids),
                        "text": _single_line(claim.text)[:_MAX_CLAIM_CHARACTERS],
                    }
                    for claim in opinion.analysis.claims[:_MAX_ROLE_CLAIMS]
                ],
                "decision": opinion.analysis.decision.value,
                "invalidation_conditions": [
                    _single_line(item)[:_MAX_CLAIM_CHARACTERS]
                    for item in opinion.analysis.invalidation_conditions[
                        :_MAX_INVALIDATION_CONDITIONS
                    ]
                ],
                "macro_impact": str(opinion.analysis.macro_impact),
                "role": _role_value(opinion.role),
            }
        )
    return {
        "arguments": arguments,
        "envelope_version": "untrusted-peer-arguments@1",
        "round_number": previous_round.round_number,
    }


def _aggregate_analysis(
    request: MacroAnalysisRequest,
    final_round: _RoundLike,
    *,
    audit_identity: AnalyzerAuditIdentity,
    config: _ConfigLike,
    directional_roles: frozenset[object],
) -> MacroAnalysis | None:
    """对角色结果执行确定性的中位数聚合与保守降级。"""

    directional = tuple(
        opinion
        for opinion in final_round.opinions
        if _role_value(opinion.role) in directional_roles
    )
    if not directional:
        return None
    macro_scores = tuple(opinion.analysis.macro_impact for opinion in directional)
    technical_scores = tuple(
        opinion.analysis.technical_alignment for opinion in directional
    )
    macro_impact = _median(macro_scores)
    technical_alignment = _median(technical_scores)
    spread = max(macro_scores) - min(macro_scores)
    material_disagreement = spread >= config.material_disagreement_threshold
    decision = (
        MacroAnalysisDecision.WATCH
        if material_disagreement
        or any(
            opinion.analysis.decision is MacroAnalysisDecision.WATCH
            for opinion in final_round.opinions
        )
        else MacroAnalysisDecision.PUBLISH
    )

    claims: list[MacroClaim] = []
    seen_claims: set[tuple[str, tuple[str, ...]]] = set()
    for opinion in final_round.opinions:
        for claim in opinion.analysis.claims:
            key = (_single_line(claim.text), tuple(claim.evidence_ids))
            if key in seen_claims:
                continue
            seen_claims.add(key)
            claims.append(claim)
            if len(claims) == _MAX_ROLE_CLAIMS:
                break
        if len(claims) == _MAX_ROLE_CLAIMS:
            break
    if not claims:
        return None

    invalidation_conditions = _unique_text(
        item
        for opinion in final_round.opinions
        for item in opinion.analysis.invalidation_conditions
    )[:_MAX_INVALIDATION_CONDITIONS]
    if not invalidation_conditions:
        return None
    uncertainties = list(
        _unique_text(
            item
            for opinion in final_round.opinions
            for item in opinion.analysis.uncertainties
        )[:10]
    )
    if material_disagreement:
        uncertainties.insert(0, "ADVERSARIAL_MATERIAL_DIRECTIONAL_DISAGREEMENT")
        uncertainties = list(dict.fromkeys(uncertainties))[:10]
    data_gaps = _unique_text(
        item
        for opinion in final_round.opinions
        for item in opinion.analysis.data_gaps
    )[:10]
    depth = getattr(config.depth, "value", config.depth)
    analysis = MacroAnalysis(
        analysis_id=request.analysis_id,
        as_of=request.as_of,
        decision=decision,
        regime=(
            f"结构化对抗分析已完成（{depth}）；"
            f"方向分歧幅度={spread}；结果不是校准概率"
        ),
        technical_alignment=technical_alignment,
        macro_impact=macro_impact,
        scenarios=(),
        claims=tuple(claims),
        uncertainties=tuple(uncertainties),
        data_gaps=data_gaps,
        invalidation_conditions=invalidation_conditions,
        reported_confidence="UNCALIBRATED",
        refusal_reason="",
        model_version=audit_identity.requested_model,
    )
    try:
        analysis.validate_against(request)
    except ValueError:
        return None
    return analysis


def _rounds_are_stable(previous: _RoundLike, current: _RoundLike) -> bool:
    return _round_signature(previous) == _round_signature(current)


def _round_signature(value: _RoundLike) -> tuple[object, ...]:
    return tuple(
        (

            _role_value(opinion.role),
            opinion.analysis.decision.value,
            opinion.analysis.technical_alignment,
            opinion.analysis.macro_impact,
            tuple(
                (_single_line(claim.text), tuple(claim.evidence_ids))
                for claim in opinion.analysis.claims
            ),
            tuple(_single_line(item) for item in opinion.analysis.invalidation_conditions),
        )
        for opinion in value.opinions
    )
