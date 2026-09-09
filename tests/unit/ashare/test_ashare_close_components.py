"""盘后分析拆分后可独立使用的数据契约和纯投影。"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.domain.recommendations import (
    ConfidenceBand,
    EvidenceReference,
    RecommendationDecision,
    RecommendationHorizon,
    ResearchRecommendation,
)
from gribuki_trade.features.close_analysis import CloseTechnicalAssessment
from gribuki_trade.reporting.contracts import ReportKind, validate_text_report_contract
from gribuki_trade.services.ashare.close.ashare_close_analysis import (
    AShareCloseAnalysisRequest as FacadeRequest,
)
from gribuki_trade.services.ashare.close.ashare_close_analysis import (
    format_close_analysis_notifications as facade_notifications,
)
from gribuki_trade.services.ashare.close.ashare_close_models import (
    AShareCloseAnalysisRequest,
    AShareCloseMarketDataCollection,
    canonical_symbol,
)
from gribuki_trade.services.ashare.close.ashare_close_notifications import (
    format_close_analysis_notifications,
)
from gribuki_trade.services.ashare.close.ashare_close_projection import (
    _as_technical_signal,
    _recommendation_evidence,
)
from gribuki_trade.services.ashare.research.ashare_research import ResearchNotificationTarget

NOW = datetime(2026, 8, 13, 8, 30, tzinfo=UTC)
NEXT = date(2026, 8, 14)


def _reference(identifier: str, *, first_seen_at: datetime = NOW) -> EvidenceReference:
    return EvidenceReference(
        evidence_id=identifier,
        title="冻结日线证据",
        canonical_url="local://daily/510300.SH",
        published_at=NOW,
        first_seen_at=first_seen_at,
        source_tier=0,
    )


def _assessment() -> CloseTechnicalAssessment:
    return CloseTechnicalAssessment(
        symbol="510300.SH",
        as_of=NOW,
        next_session=NEXT,
        latest_trade_date=date(2026, 8, 13),
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        decision=RecommendationDecision.WATCH,
        score=Decimal("0.25"),
        reference_price=Decimal("4.321"),
        invalidation_price=Decimal("4.100"),
        reason_codes=("WATCH",),
        trading_sessions_used=220,
        strategy_version="close-test@1",
        metrics=(("close", Decimal("4.321")),),
    )


def test_facade_keeps_stable_public_contracts() -> None:
    assert FacadeRequest is AShareCloseAnalysisRequest
    assert facade_notifications is format_close_analysis_notifications


@pytest.mark.parametrize(
    ("value", "expected"),
    [(" 510300 ", "510300.SH"), ("000001", "000001.SZ"), ("920001", "920001.BJ")],
)
def test_request_symbol_normalization_is_independent(value: str, expected: str) -> None:
    assert canonical_symbol(value) == expected


def test_request_rejects_future_evidence_and_duplicate_failure_codes() -> None:
    with pytest.raises(ValueError, match="first seen after as_of"):
        AShareCloseAnalysisRequest(
            symbol="510300",
            history_start=date(2025, 1, 1),
            latest_completed_session=date(2026, 8, 13),
            next_session=NEXT,
            as_of=NOW,
            market_evidence=_reference("daily", first_seen_at=NOW + timedelta(seconds=1)),
        )
    with pytest.raises(ValueError, match="failure codes must be unique"):
        AShareCloseAnalysisRequest(
            symbol="510300",
            history_start=date(2025, 1, 1),
            latest_completed_session=date(2026, 8, 13),
            next_session=NEXT,
            global_risk_failure_codes=("UNAVAILABLE", "UNAVAILABLE"),
        )


def test_collection_keeps_timezone_and_provider_identity_validation() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        AShareCloseMarketDataCollection(bars=(), fetched_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="source_name must not be blank"):
        AShareCloseMarketDataCollection(bars=(), fetched_at=NOW, source_name=" ")


def test_projection_retains_signal_and_ordered_unique_evidence() -> None:
    assessment = _assessment()
    signal = _as_technical_signal(assessment)
    assert signal.symbol == assessment.symbol
    assert signal.score == assessment.score
    assert signal.metrics == assessment.metrics
    assert signal.data_age == timedelta(0)
    market, news = _reference("market"), _reference("news")
    assert _recommendation_evidence(market, (news,)) == (market, news)
    with pytest.raises(ValueError, match="evidence IDs must be unique"):
        _recommendation_evidence(market, (market,))


def test_notifications_are_deterministic_without_service_side_effects() -> None:
    assessment = _assessment()
    recommendation = ResearchRecommendation(
        recommendation_id="close-510300-20260814",
        symbol=assessment.symbol,
        as_of=NOW,
        expires_at=NOW + timedelta(days=1),
        horizon=assessment.horizon,
        decision=assessment.decision,
        confidence=ConfidenceBand.UNCALIBRATED,
        technical_score=assessment.score,
        macro_score=None,
        reference_price=assessment.reference_price,
        invalidation_price=assessment.invalidation_price,
        reason_codes=assessment.reason_codes,
        uncertainties=("MACRO_DISABLED",),
        evidence=(_reference("daily"),),
        strategy_version=assessment.strategy_version,
    )
    target = ResearchNotificationTarget(target_id="test-recipient", max_characters=600)
    first = format_close_analysis_notifications(
        recommendation, assessment, None, target, calendar_verified=True
    )
    second = format_close_analysis_notifications(
        recommendation, assessment, None, target, calendar_verified=True
    )
    assert first == second
    assert len(first) > 1
    assert len({item.idempotency_key for item in first}) == len(first)
    for item in first:
        assert len(item.text) <= target.max_characters
        validate_text_report_contract(ReportKind.INSTRUMENT_RESEARCH, item.text)
