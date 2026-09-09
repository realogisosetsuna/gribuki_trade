from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.domain.candidates import (
    CandidatePriority,
    CandidateProvenance,
    CandidateRecord,
    CandidateSource,
    CandidateStatus,
)
from gribuki_trade.domain.recommendations import (
    ConfidenceBand,
    EvidenceReference,
    RecommendationDecision,
    RecommendationHorizon,
    ResearchRecommendation,
)
from gribuki_trade.domain.review_cases import ReviewActor, ReviewCaseStatus
from gribuki_trade.services.research.recommendation_review import (
    RecommendationReviewError,
    RecommendationReviewPolicy,
    RecommendationReviewService,
)
from gribuki_trade.storage.research.review_case_store import (
    ReviewCaseEventCollisionError,
    SQLiteReviewCaseStore,
)

NOW = datetime(2026, 8, 14, 1, 0, tzinfo=UTC)


def recommendation(*, with_evidence: bool = True) -> ResearchRecommendation:
    evidence = (
        (
            EvidenceReference(
                evidence_id="evidence-1",
                title="Retained evidence",
                canonical_url="https://example.test/evidence",
                published_at=NOW - timedelta(minutes=10),
                first_seen_at=NOW - timedelta(minutes=9),
                source_tier=2,
            ),
        )
        if with_evidence
        else ()
    )
    return ResearchRecommendation(
        recommendation_id="recommendation-1",
        symbol="600000.SH",
        as_of=NOW - timedelta(minutes=5),
        expires_at=NOW + timedelta(days=1),
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        decision=(
            RecommendationDecision.ENTER_CANDIDATE
            if with_evidence
            else RecommendationDecision.WATCH
        ),
        confidence=ConfidenceBand.UNCALIBRATED,
        technical_score=Decimal("0.6"),
        macro_score=Decimal("0.1"),
        reference_price=Decimal("10"),
        invalidation_price=Decimal("9.5"),
        reason_codes=("TECHNICAL_SETUP",),
        uncertainties=("PUBLIC_DATA",),
        evidence=evidence,
        strategy_version="test@1",
    )


def candidate() -> CandidateRecord:
    provenance = CandidateProvenance(
        observation_id="observation-1",
        source=CandidateSource.CLOSE_SCREEN,
        source_run_id="close-screen-1",
        discovered_at=NOW - timedelta(minutes=20),
        observed_at=NOW - timedelta(minutes=19),
        expires_at=NOW + timedelta(days=1),
        priority=CandidatePriority.HIGH,
        reason_codes=("CLOSE_SCREEN_TOP_N",),
        evidence_ids=("market-evidence-1",),
    )
    return CandidateRecord(
        symbol="600000.SH",
        as_of=NOW,
        status=CandidateStatus.ACTIVE,
        priority=CandidatePriority.HIGH,
        discovered_at=provenance.discovered_at,
        first_observed_at=provenance.observed_at,
        last_observed_at=provenance.observed_at,
        expires_at=provenance.expires_at,
        cooling_until=None,
        reason_codes=provenance.reason_codes,
        evidence_ids=provenance.evidence_ids,
        sources=(provenance.source,),
        provenance=(provenance,),
    )


def test_open_case_links_recommendation_evidence_and_candidate_provenance(tmp_path) -> None:
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        service = RecommendationReviewService(store, clock=lambda: NOW)
        result = service.open_case(recommendation(), candidate=candidate())
        replay = service.open_case(recommendation(), candidate=candidate())

    assert result.appended is True
    assert replay.appended is False
    assert result.case.status is ReviewCaseStatus.PENDING_REVIEW
    assert result.case.expires_at == NOW + timedelta(hours=4)
    assert result.case.evidence_ids == ("evidence-1",)
    summary = result.case.candidate_provenance
    assert summary is not None
    assert summary.observation_ids == ("observation-1",)
    assert summary.source_run_ids == ("close-screen-1",)
    assert summary.sources == (CandidateSource.CLOSE_SCREEN,)


def test_confirmation_is_research_only_and_exact_replay_is_idempotent(tmp_path) -> None:
    decision_at = NOW + timedelta(minutes=10)
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        service = RecommendationReviewService(store, clock=lambda: NOW)
        pending = service.open_case(recommendation()).case
        confirmed = service.confirm(
            pending.case_id,
            actor=ReviewActor.LOCAL_USER,
            reason_codes=("REPORT_MANUALLY_RECHECKED",),
            at=decision_at,
            operation_id="confirm-1",
        )
        replay = service.confirm(
            pending.case_id,
            actor=ReviewActor.LOCAL_USER,
            reason_codes=("REPORT_MANUALLY_RECHECKED",),
            at=decision_at,
            operation_id="confirm-1",
        )

    assert confirmed.appended is True
    assert replay.appended is False
    assert confirmed.case.status is ReviewCaseStatus.CONFIRMED
    assert confirmed.case.is_research_approved is True
    assert confirmed.case.resolution is not None
    assert confirmed.case.resolution.actor is ReviewActor.LOCAL_USER


def test_same_operation_identity_with_changed_payload_is_a_collision(tmp_path) -> None:
    decision_at = NOW + timedelta(minutes=10)
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        service = RecommendationReviewService(store, clock=lambda: NOW)
        pending = service.open_case(recommendation()).case
        service.confirm(
            pending.case_id,
            actor=ReviewActor.LOCAL_USER,
            reason_codes=("REPORT_MANUALLY_RECHECKED",),
            at=decision_at,
            operation_id="confirm-1",
        )
        with pytest.raises(ReviewCaseEventCollisionError):
            service.confirm(
                pending.case_id,
                actor=ReviewActor.LOCAL_USER,
                reason_codes=("MUTATED_REASON",),
                at=decision_at,
                operation_id="confirm-1",
            )


def test_only_pending_case_can_take_a_different_terminal_transition(tmp_path) -> None:
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        service = RecommendationReviewService(store, clock=lambda: NOW)
        pending = service.open_case(recommendation()).case
        service.reject(
            pending.case_id,
            actor=ReviewActor.SYSTEM,
            reason_codes=("DETAILED_CHECK_FAILED",),
            at=NOW + timedelta(minutes=1),
        )
        with pytest.raises(RecommendationReviewError, match="not pending"):
            service.cancel(
                pending.case_id,
                actor=ReviewActor.LOCAL_USER,
                reason_codes=("USER_CANCELLED",),
                at=NOW + timedelta(minutes=2),
            )


def test_expiration_is_projected_and_cannot_be_confirmed(tmp_path) -> None:
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        service = RecommendationReviewService(store, clock=lambda: NOW)
        pending = service.open_case(
            recommendation(),
            expires_at=NOW + timedelta(minutes=30),
        ).case
        expired = service.get(
            pending.case_id,
            as_of=NOW + timedelta(minutes=30),
        )
        with pytest.raises(RecommendationReviewError, match="EXPIRED"):
            service.confirm(
                pending.case_id,
                actor=ReviewActor.LOCAL_USER,
                reason_codes=("TOO_LATE",),
                at=NOW + timedelta(minutes=30),
            )

    assert expired is not None and expired.status is ReviewCaseStatus.EXPIRED


def test_confirmation_requires_retained_evidence_but_rejection_remains_available(
    tmp_path,
) -> None:
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        service = RecommendationReviewService(store, clock=lambda: NOW)
        pending = service.open_case(recommendation(with_evidence=False)).case
        with pytest.raises(RecommendationReviewError, match="retained evidence"):
            service.confirm(
                pending.case_id,
                actor=ReviewActor.SYSTEM,
                reason_codes=("SYSTEM_RECHECKED",),
                at=NOW + timedelta(minutes=1),
            )
        rejected = service.reject(
            pending.case_id,
            actor=ReviewActor.SYSTEM,
            reason_codes=("NO_RETAINED_EVIDENCE",),
            at=NOW + timedelta(minutes=1),
        )

    assert rejected.case.status is ReviewCaseStatus.REJECTED


def test_future_or_mismatched_candidate_and_invalid_expiry_fail_closed(tmp_path) -> None:
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        service = RecommendationReviewService(store, clock=lambda: NOW)
        with pytest.raises(RecommendationReviewError, match="symbol must match"):
            service.open_case(
                recommendation(),
                candidate=replace(candidate(), symbol="000001.SZ"),
            )
        with pytest.raises(RecommendationReviewError, match="not yet visible"):
            service.open_case(
                recommendation(),
                candidate=replace(candidate(), as_of=NOW + timedelta(seconds=1)),
            )
        with pytest.raises(RecommendationReviewError, match="cannot exceed"):
            service.open_case(
                recommendation(),
                expires_at=NOW + timedelta(days=2),
            )


def test_pending_query_excludes_terminal_and_automatically_expired_cases(tmp_path) -> None:
    second = replace(recommendation(), recommendation_id="recommendation-2")
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        service = RecommendationReviewService(store, clock=lambda: NOW)
        first_case = service.open_case(recommendation()).case
        service.open_case(second, expires_at=NOW + timedelta(minutes=30))
        service.cancel(
            first_case.case_id,
            actor=ReviewActor.LOCAL_USER,
            reason_codes=("NO_LONGER_NEEDED",),
            at=NOW + timedelta(minutes=10),
        )
        pending = service.pending_cases(as_of=NOW + timedelta(minutes=31))

    assert pending == ()


def test_review_policy_must_have_a_positive_window() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        RecommendationReviewPolicy(maximum_review_window=timedelta(0))
