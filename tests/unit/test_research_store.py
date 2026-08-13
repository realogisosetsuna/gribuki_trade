from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.backtest import OutcomeStatus, RecommendationOutcome
from gribuki_trade.domain.instruments import ResearchInstrumentProfile
from gribuki_trade.domain.recommendations import (
    ConfidenceBand,
    EvidenceReference,
    RecommendationDecision,
    RecommendationHorizon,
    ResearchRecommendation,
)
from gribuki_trade.storage import ResearchRecordCollisionError, SQLiteResearchStore

NOW = datetime(2026, 8, 13, 3, 30, tzinfo=UTC)


def instrument_profile(symbol: str = "600000.SH") -> ResearchInstrumentProfile:
    return ResearchInstrumentProfile(
        symbol=symbol,
        name="浦发银行",
        market="A股",
        asset_type="stock",
        exchange="sse",
        board="sse_main",
        size_tier="large",
        industry="银行",
        styles=("价值", "红利"),
        research_role="银行资产质量与利率周期样本",
        risk_tags=("资产质量", "净息差"),
        source_id="ashare-test-v1",
        verified_on=date(2026, 8, 13),
        background_facts=("主营业务：公司银行与零售银行",),
    )


def recommendation(identifier: str = "rec-1") -> ResearchRecommendation:
    return ResearchRecommendation(
        recommendation_id=identifier,
        symbol="600000.SH",
        as_of=NOW,
        expires_at=NOW + timedelta(hours=1),
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        decision=RecommendationDecision.ENTER_CANDIDATE,
        confidence=ConfidenceBand.UNCALIBRATED,
        technical_score=Decimal("0.75"),
        macro_score=Decimal("0.1"),
        combined_score=Decimal("0.5875"),
        fusion_reason_codes=(
            "COMBINED_SCORE_UNCALIBRATED",
            "MACRO_FUSION_APPLIED",
        ),
        macro_evidence_coverage=Decimal("0.5"),
        fusion_version="technical-macro-fusion@1",
        technical_fusion_weight=Decimal("0.75"),
        macro_fusion_weight=Decimal("0.25"),
        reference_price=Decimal("10.2"),
        invalidation_price=Decimal("9.8"),
        reason_codes=("BREAKOUT",),
        uncertainties=("PUBLIC_WEB_DATA",),
        evidence=(
            EvidenceReference(
                evidence_id="evidence-1",
                title="Retained event",
                canonical_url="https://example.test/event",
                published_at=NOW - timedelta(minutes=2),
                first_seen_at=NOW - timedelta(minutes=1),
                source_tier=2,
            ),
        ),
        strategy_version="test@1",
        model_version="fake@1",
        analysis_mode="AFTER_CLOSE",
        target_session=date(2026, 8, 14),
        technical_metrics=(
            ("ma_20", Decimal("10.1234")),
            ("volume_ratio", Decimal("1.25")),
        ),
        instrument_profile=instrument_profile(),
    )


def outcome() -> RecommendationOutcome:
    return RecommendationOutcome(
        recommendation_id="rec-1",
        symbol="600000.SH",
        decision=RecommendationDecision.ENTER_CANDIDATE,
        status=OutcomeStatus.COMPLETE,
        horizon_sessions=5,
        gross_return=Decimal("0.05"),
        net_return_after_costs=Decimal("0.048"),
        direction_correct=True,
    )


def test_recommendation_is_idempotent_and_round_trips(tmp_path) -> None:
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        assert store.append_recommendation(recommendation()) is True
        assert store.append_recommendation(recommendation()) is False
        restored = store.get_recommendation("rec-1")

    assert restored == recommendation()


def test_recommendation_identity_collision_is_rejected(tmp_path) -> None:
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        store.append_recommendation(recommendation())
        with pytest.raises(ResearchRecordCollisionError):
            store.append_recommendation(
                replace(recommendation(), technical_score=Decimal("0.2"))
            )


def test_recommendation_reader_accepts_legacy_json_without_metadata(tmp_path) -> None:
    path = tmp_path / "research.sqlite3"
    legacy = replace(
        recommendation(),
        analysis_mode=None,
        target_session=None,
        technical_metrics=(),
        instrument_profile=None,
        combined_score=None,
        fusion_reason_codes=(),
        macro_evidence_coverage=None,
        fusion_version=None,
        technical_fusion_weight=None,
        macro_fusion_weight=None,
    )
    with SQLiteResearchStore(path) as store:
        store.append_recommendation(legacy)

    with sqlite3.connect(path) as connection:
        payload = connection.execute(
            "SELECT payload_json FROM research_recommendations "
            "WHERE recommendation_id = ?",
            (legacy.recommendation_id,),
        ).fetchone()[0]
        document = json.loads(str(payload))
        document.pop("analysis_mode")
        document.pop("target_session")
        document.pop("technical_metrics")
        document.pop("instrument_profile")
        document.pop("combined_score")
        document.pop("fusion_reason_codes")
        document.pop("macro_evidence_coverage")
        document.pop("fusion_version")
        document.pop("technical_fusion_weight")
        document.pop("macro_fusion_weight")
        connection.execute(
            "UPDATE research_recommendations SET payload_json = ? "
            "WHERE recommendation_id = ?",
            (json.dumps(document), legacy.recommendation_id),
        )

    with SQLiteResearchStore(path) as store:
        restored = store.get_recommendation(legacy.recommendation_id)

    assert restored == legacy


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"analysis_mode": "  "}, "analysis_mode must not be empty"),
        (
            {"technical_metrics": ((" ", Decimal("1")),)},
            "technical metric name must not be empty",
        ),
        (
            {"technical_metrics": (("rsi_14", Decimal("NaN")),)},
            "technical metric value must be finite",
        ),
        (
            {"instrument_profile": instrument_profile("000001.SZ")},
            "instrument profile symbol must match recommendation symbol",
        ),
        (
            {"combined_score": Decimal("1.1")},
            r"combined_score must be in \[-1, 1\]",
        ),
        (
            {"macro_evidence_coverage": Decimal("-0.1")},
            r"macro_evidence_coverage must be in \[0, 1\]",
        ),
        ({"fusion_reason_codes": (" ",)}, "fusion reason codes must not be empty"),
        (
            {
                "technical_fusion_weight": Decimal("0.8"),
                "macro_fusion_weight": None,
            },
            "fusion weights must either both be set or both be absent",
        ),
        (
            {
                "technical_fusion_weight": Decimal("0.8"),
                "macro_fusion_weight": Decimal("0.3"),
            },
            "fusion weights must sum to one",
        ),
    ],
)
def test_recommendation_metadata_validation(
    changes: dict[str, object],
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        replace(recommendation(), **changes)


def test_latest_can_filter_by_symbol(tmp_path) -> None:
    second = replace(
        recommendation("rec-2"),
        symbol="000001.SZ",
        instrument_profile=instrument_profile("000001.SZ"),
        as_of=NOW + timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=2),
    )
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        store.append_recommendation(recommendation())
        store.append_recommendation(second)
        assert store.latest_recommendations(limit=2) == (second, recommendation())
        assert store.latest_recommendations(symbol="600000.SH") == (recommendation(),)


def test_outcome_requires_parent_and_preserves_history(tmp_path) -> None:
    evaluated_at = NOW + timedelta(days=7)
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        with pytest.raises(ValueError, match="retained recommendation"):
            store.append_outcome(outcome(), evaluated_at=evaluated_at)
        store.append_recommendation(recommendation())
        assert store.append_outcome(outcome(), evaluated_at=evaluated_at) is True
        assert store.append_outcome(outcome(), evaluated_at=evaluated_at) is False
        assert store.latest_outcomes("rec-1") == (outcome(),)


def test_outcome_requires_aware_evaluation_time(tmp_path) -> None:
    with SQLiteResearchStore(tmp_path / "research.sqlite3") as store:
        store.append_recommendation(recommendation())
        with pytest.raises(ValueError, match="timezone-aware"):
            store.append_outcome(outcome(), evaluated_at=datetime(2026, 8, 20))
