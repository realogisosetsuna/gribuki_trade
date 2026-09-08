from __future__ import annotations

from decimal import Decimal

from gribuki_trade.domain.recommendations import RecommendationDecision
from gribuki_trade.services.ashare_intraday_llm import (
    IntradayLLMGateAction,
    IntradayLLMGateOutcome,
    IntradayLLMGateReason,
)
from gribuki_trade.services.ashare_paper_day_projection import (
    deep_exit_sell_review,
    deep_exit_sell_review_text,
    llm_gate_dual_document,
)


def test_llm_gate_projection_marks_incomplete_dual_track_unavailable() -> None:
    outcome = IntradayLLMGateOutcome(
        action=IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
        reason=IntradayLLMGateReason.REVIEW_NOT_READY,
        technical_decision=RecommendationDecision.ENTER_CANDIDATE,
        technical_score=Decimal("0.4"),
    )

    assert llm_gate_dual_document(outcome) == {
        "status": "UNAVAILABLE",
        "baseline": None,
        "adversarial": None,
        "selected_track": None,
        "audit_record_sha256": None,
    }


def test_deep_exit_projection_never_allows_semantic_veto() -> None:
    projected = deep_exit_sell_review(
        technical_score=Decimal("0.8"),
        assessment={"available": True, "selected_score": Decimal("0.8")},
    )

    assert projected["action"] == "RETAIN_TECHNICAL_REDUCE_NO_LLM_VETO"
    assert projected["llm_can_veto"] is False
    assert projected["combined_exit_score"] == Decimal("1.00")
    assert "不得否决技术 REDUCE" in deep_exit_sell_review_text(projected)
