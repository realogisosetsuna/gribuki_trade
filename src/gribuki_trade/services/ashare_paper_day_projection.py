"""A 股 PAPER 交易日流程使用的纯结果投影。

PAPER-day 运行器负责调度、持久化和副作用；本模块只把 LLM 与退出复核结果
转换为审计文档或操作员可读文本。这样可以在不构造运行器和存储后端的情况下，
独立验证渲染契约。
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

from gribuki_trade.reporting.contracts import humanize_internal_code
from gribuki_trade.services.ashare_intraday_llm import IntradayLLMGateOutcome


def llm_gate_document(outcome: IntradayLLMGateOutcome) -> dict[str, object]:
    """把门禁结果投影为稳定的 PAPER-day 审计结构。"""

    return {
        "accepted_at": outcome.accepted_at,
        "action": outcome.action.value,
        "approved": outcome.approved,
        "combined_score": outcome.combined_score,
        "evidence_coverage": outcome.evidence_coverage,
        "expires_at": outcome.expires_at,
        "failure_code": outcome.failure_code,
        "macro_score": outcome.macro_score,
        "prompt_schema_sha256": outcome.prompt_schema_sha256,
        "reason_code": outcome.reason.value,
        "requested_model": outcome.requested_model,
        "response_model": outcome.response_model,
        "review_id": outcome.review_id,
        "technical_decision": outcome.technical_decision.value,
        "technical_score": outcome.technical_score,
        "dual_track": llm_gate_dual_document(outcome),
    }


def llm_gate_dual_document(outcome: IntradayLLMGateOutcome) -> dict[str, object]:
    """投影两条 LLM 轨道，不补造缺失结果。"""

    complete = all(
        item is not None
        for item in (
            outcome.baseline_decision,
            outcome.baseline_macro_score,
            outcome.baseline_model,
            outcome.adversarial_decision,
            outcome.adversarial_macro_score,
            outcome.adversarial_model,
            outcome.selected_track,
        )
    )
    if not complete:
        return {
            "status": "UNAVAILABLE",
            "baseline": None,
            "adversarial": None,
            "selected_track": outcome.selected_track,
            "audit_record_sha256": outcome.dual_audit_record_sha256,
        }
    assert outcome.baseline_decision is not None
    assert outcome.baseline_macro_score is not None
    assert outcome.baseline_model is not None
    assert outcome.adversarial_decision is not None
    assert outcome.adversarial_macro_score is not None
    assert outcome.adversarial_model is not None
    return {
        "status": "COMPLETE",
        "baseline": {
            "decision": outcome.baseline_decision.value,
            "macro_score": outcome.baseline_macro_score,
            "model": outcome.baseline_model,
        },
        "adversarial": {
            "decision": outcome.adversarial_decision.value,
            "macro_score": outcome.adversarial_macro_score,
            "model": outcome.adversarial_model,
        },
        "selected_track": outcome.selected_track,
        "audit_record_sha256": outcome.dual_audit_record_sha256,
    }


def llm_gate_dual_text(outcome: IntradayLLMGateOutcome) -> str:
    """渲染适合操作员通知的简短中文摘要。"""

    document = llm_gate_dual_document(outcome)
    if document["status"] != "COMPLETE":
        return "LLM双轨：未形成完整双轨结果；本次按门禁失败关闭或显式旁路处理"
    assert outcome.baseline_decision is not None
    assert outcome.baseline_macro_score is not None
    assert outcome.adversarial_decision is not None
    assert outcome.adversarial_macro_score is not None
    audit = (
        "未记录"
        if outcome.dual_audit_record_sha256 is None
        else outcome.dual_audit_record_sha256[:12]
    )
    return (
        "LLM双轨：原单分析器="
        f"{humanize_internal_code(outcome.baseline_decision.value)} / "
        f"{_decimal_display(outcome.baseline_macro_score)}；"
        "对抗分析器="
        f"{humanize_internal_code(outcome.adversarial_decision.value)} / "
        f"{_decimal_display(outcome.adversarial_macro_score)}；"
        f"生产采用={humanize_internal_code(outcome.selected_track)}；"
        f"审计摘要={audit}"
    )


def deep_exit_llm_assessment_text(value: Mapping[str, object]) -> str:
    """渲染持久化的 DEEP 双轨评分，并明确保留降级状态。"""

    def readable_score(score: object) -> str:
        return str(score) if isinstance(score, Decimal) else "不可用（本次未取得）"

    baseline = value.get("baseline_score")
    adversarial = value.get("adversarial_score")

    if value.get("available") is not True:
        if value.get("status") == "QUICK_PLAN_RETAINED":
            summary = "成交后 DEEP 复核：当前仍为快速保护计划，双轨 LLM 评分尚未取得/不适用。"
        elif value.get("status") == "DEEP_LLM_METRICS_NOT_AVAILABLE":
            summary = (
                "成交后 DEEP 复核：当前深度计划没有可验证的双轨 LLM 指标，"
                "缺失轨道明确记为不可用。"
            )
        else:
            summary = "成交后 DEEP 复核：尚无完整、可验证的双轨评分。"
        return (
            f"{summary}"
            f"单分析器评分={readable_score(baseline)}；"
            f"对抗系统评分={readable_score(adversarial)}；"
            "生产采用=未采用不完整双轨结果；继续按当前确定性保护线处理。"
        )
    selected = value.get("selected_score")

    selected_system = {
        "ADVERSARIAL_LLM": "结构化对抗分析器",
        "BASELINE_LLM": "原单分析器",
        "DETERMINISTIC_ONLY": "确定性价格规则",
        "DETERMINISTIC_OR_BASELINE_FALLBACK": "确定性规则或原单分析器降级结果",
    }.get(str(value.get("selected_system")), "未取得可验证的生产轨道")
    return (
        "成交后 DEEP 复核："
        f"生产采用={selected_system}；"
        f"综合评分={readable_score(selected)}；"
        f"单分析器评分={readable_score(baseline)}；"
        f"对抗系统评分={readable_score(adversarial)}。"
    )


def deep_exit_sell_review(
    *,
    technical_score: Decimal,
    assessment: Mapping[str, object],
) -> dict[str, object]:
    """将 DEEP 语义评分合入技术紧迫度，但不允许 LLM 否决保护信号。"""

    selected = assessment.get("selected_score")
    if assessment.get("available") is not True or not isinstance(selected, Decimal):
        return {
            "action": "TECHNICAL_REDUCE_WITHOUT_SEMANTIC_SCORE",
            "combined_exit_score": technical_score,
            "llm_can_veto": False,
            "policy_version": "deep-exit-sell-urgency@1",
            "selected_semantic_score": None,
            "technical_score": technical_score,
        }
    combined = technical_score + selected * Decimal("0.25")
    if selected <= Decimal("-0.25"):
        action = "ESCALATE_REDUCE_URGENCY"
    elif selected >= Decimal("0.50"):
        action = "RETAIN_TECHNICAL_REDUCE_NO_LLM_VETO"
    else:
        action = "CONFIRM_TECHNICAL_REDUCE"
    return {
        "action": action,
        "combined_exit_score": combined,
        "llm_can_veto": False,
        "policy_version": "deep-exit-sell-urgency@1",
        "selected_semantic_score": selected,
        "technical_score": technical_score,
    }


def deep_exit_sell_review_text(value: Mapping[str, object]) -> str:
    """渲染操作员通知中的 DEEP 紧迫度判断。"""

    action = {
        "TECHNICAL_REDUCE_WITHOUT_SEMANTIC_SCORE": "暂无语义评分，维持技术 REDUCE",
        "ESCALATE_REDUCE_URGENCY": "对抗轨负面评分，升级卖出紧迫度",
        "RETAIN_TECHNICAL_REDUCE_NO_LLM_VETO": "对抗轨偏正面，但不得否决技术 REDUCE",
        "CONFIRM_TECHNICAL_REDUCE": "双轨评分确认维持技术 REDUCE",
    }.get(str(value.get("action")), "维持技术 REDUCE，语义结果不得否决")
    return (
        f"卖出复核：{action}；技术评分={value.get('technical_score')}；"
        f"采用的语义评分={value.get('selected_semantic_score')}；"
        f"组合退出评分={value.get('combined_exit_score')}。"
    )


def _decimal_display(value: Decimal) -> str:
    return format(value, "f")

