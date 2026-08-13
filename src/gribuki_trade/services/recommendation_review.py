"""Application service for broker-independent recommendation review callbacks."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from gribuki_trade.domain.candidates import CandidateRecord
from gribuki_trade.domain.recommendations import ResearchRecommendation
from gribuki_trade.domain.review_cases import (
    CandidateProvenanceSummary,
    RecommendationReviewCase,
    ReviewActor,
    ReviewCaseOpened,
    ReviewCaseStatus,
    ReviewCaseTransition,
)


class ReviewCaseRepository(Protocol):
    """Persistence boundary used by the review workflow."""

    def append_opened(self, item: ReviewCaseOpened) -> bool: ...

    def append_transition(self, item: ReviewCaseTransition) -> bool: ...

    def get_case(
        self,
        case_id: str,
        *,
        as_of: datetime,
    ) -> RecommendationReviewCase | None: ...

    def get_by_recommendation(
        self,
        recommendation_id: str,
        *,
        as_of: datetime,
    ) -> RecommendationReviewCase | None: ...

    def list_cases(
        self,
        *,
        as_of: datetime,
        statuses: frozenset[ReviewCaseStatus] | None = None,
        limit: int = 500,
    ) -> tuple[RecommendationReviewCase, ...]: ...


class RecommendationReviewError(RuntimeError):
    """A safe, stable validation error in the research-review workflow."""


@dataclass(frozen=True, slots=True)
class RecommendationReviewPolicy:
    """Expiry and evidence rules for detailed-analysis callbacks."""

    maximum_review_window: timedelta = timedelta(hours=4)
    require_evidence_for_confirmation: bool = True

    def __post_init__(self) -> None:
        if self.maximum_review_window <= timedelta(0):
            raise ValueError("maximum_review_window must be positive")


@dataclass(frozen=True, slots=True)
class ReviewCaseMutation:
    """Idempotent write result and the resulting research-only projection."""

    case: RecommendationReviewCase
    appended: bool


class RecommendationReviewService:
    """Open and resolve recommendation reviews without execution side effects."""

    def __init__(
        self,
        repository: ReviewCaseRepository,
        *,
        policy: RecommendationReviewPolicy | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._repository = repository
        self._policy = policy or RecommendationReviewPolicy()
        self._clock = clock

    @property
    def policy(self) -> RecommendationReviewPolicy:
        return self._policy

    def open_case(
        self,
        recommendation: ResearchRecommendation,
        *,
        candidate: CandidateRecord | None = None,
        actor: ReviewActor = ReviewActor.SYSTEM,
        reason_codes: tuple[str, ...] = ("DETAILED_REVIEW_REQUIRED",),
        at: datetime | None = None,
        expires_at: datetime | None = None,
    ) -> ReviewCaseMutation:
        """Create one review per immutable recommendation."""

        opened_at = _utc(at or self._now(), "at")
        recommendation_as_of = _utc(recommendation.as_of, "recommendation.as_of")
        recommendation_expires = _utc(
            recommendation.expires_at,
            "recommendation.expires_at",
        )
        if opened_at < recommendation_as_of:
            raise RecommendationReviewError("recommendation is not yet visible")
        if opened_at >= recommendation_expires:
            raise RecommendationReviewError("expired recommendation cannot enter review")
        requested_expiry = (
            None if expires_at is None else _utc(expires_at, "expires_at")
        )
        if requested_expiry is not None and requested_expiry > recommendation_expires:
            raise RecommendationReviewError(
                "review expiry cannot exceed recommendation expiry"
            )
        resolved_expiry = min(
            recommendation_expires,
            requested_expiry or opened_at + self._policy.maximum_review_window,
        )
        provenance = None
        if candidate is not None:
            if candidate.symbol != recommendation.symbol:
                raise RecommendationReviewError(
                    "candidate symbol must match recommendation symbol"
                )
            if candidate.as_of > opened_at:
                raise RecommendationReviewError(
                    "candidate provenance is not yet visible"
                )
            provenance = CandidateProvenanceSummary.from_candidate(candidate)
        opened = ReviewCaseOpened(
            recommendation_id=recommendation.recommendation_id,
            symbol=recommendation.symbol,
            recommendation_as_of=recommendation_as_of,
            opened_at=opened_at,
            recorded_at=opened_at,
            expires_at=resolved_expiry,
            evidence_ids=tuple(
                sorted({item.evidence_id for item in recommendation.evidence})
            ),
            candidate_provenance=provenance,
            actor=actor,
            reason_codes=reason_codes,
        )
        appended = self._repository.append_opened(opened)
        projected = self._repository.get_case(opened.case_id, as_of=opened.recorded_at)
        if projected is None:
            raise RuntimeError("persisted review case was not visible")
        return ReviewCaseMutation(case=projected, appended=appended)

    def confirm(
        self,
        case_id: str,
        *,
        actor: ReviewActor,
        reason_codes: tuple[str, ...],
        at: datetime | None = None,
        operation_id: str | None = None,
    ) -> ReviewCaseMutation:
        """Confirm a research conclusion; this never authorizes a trade."""

        return self._transition(
            case_id,
            target_status=ReviewCaseStatus.CONFIRMED,
            actor=actor,
            reason_codes=reason_codes,
            at=at,
            operation_id=operation_id,
        )

    def reject(
        self,
        case_id: str,
        *,
        actor: ReviewActor,
        reason_codes: tuple[str, ...],
        at: datetime | None = None,
        operation_id: str | None = None,
    ) -> ReviewCaseMutation:
        return self._transition(
            case_id,
            target_status=ReviewCaseStatus.REJECTED,
            actor=actor,
            reason_codes=reason_codes,
            at=at,
            operation_id=operation_id,
        )

    def cancel(
        self,
        case_id: str,
        *,
        actor: ReviewActor,
        reason_codes: tuple[str, ...],
        at: datetime | None = None,
        operation_id: str | None = None,
    ) -> ReviewCaseMutation:
        return self._transition(
            case_id,
            target_status=ReviewCaseStatus.CANCELLED,
            actor=actor,
            reason_codes=reason_codes,
            at=at,
            operation_id=operation_id,
        )

    def get(
        self,
        case_id: str,
        *,
        as_of: datetime | None = None,
    ) -> RecommendationReviewCase | None:
        return self._repository.get_case(case_id, as_of=as_of or self._now())

    def get_for_recommendation(
        self,
        recommendation_id: str,
        *,
        as_of: datetime | None = None,
    ) -> RecommendationReviewCase | None:
        return self._repository.get_by_recommendation(
            recommendation_id,
            as_of=as_of or self._now(),
        )

    def pending_cases(
        self,
        *,
        as_of: datetime | None = None,
        limit: int = 500,
    ) -> tuple[RecommendationReviewCase, ...]:
        return self._repository.list_cases(
            as_of=as_of or self._now(),
            statuses=frozenset({ReviewCaseStatus.PENDING_REVIEW}),
            limit=limit,
        )

    def _transition(
        self,
        case_id: str,
        *,
        target_status: ReviewCaseStatus,
        actor: ReviewActor,
        reason_codes: tuple[str, ...],
        at: datetime | None,
        operation_id: str | None,
    ) -> ReviewCaseMutation:
        occurred_at = _utc(at or self._now(), "at")
        current = self._repository.get_case(case_id, as_of=occurred_at)
        if current is None:
            raise RecommendationReviewError("review case does not exist")
        resolved_operation = operation_id or _operation_id(
            case_id=current.case_id,
            target_status=target_status,
            actor=actor,
            reason_codes=reason_codes,
            at=occurred_at,
        )
        transition = ReviewCaseTransition(
            case_id=current.case_id,
            target_status=target_status,
            operation_id=resolved_operation,
            actor=actor,
            reason_codes=reason_codes,
            occurred_at=occurred_at,
            recorded_at=occurred_at,
        )
        if current.status is not ReviewCaseStatus.PENDING_REVIEW:
            if (
                current.resolution is not None
                and current.resolution.event_id == transition.event_id
            ):
                appended = self._repository.append_transition(transition)
                return ReviewCaseMutation(case=current, appended=appended)
            raise RecommendationReviewError(
                f"review case is not pending ({current.status.value})"
            )
        if (
            target_status is ReviewCaseStatus.CONFIRMED
            and self._policy.require_evidence_for_confirmation
            and not current.evidence_ids
        ):
            raise RecommendationReviewError(
                "research confirmation requires retained evidence"
            )
        appended = self._repository.append_transition(transition)
        projected = self._repository.get_case(current.case_id, as_of=occurred_at)
        if projected is None:
            raise RuntimeError("persisted review transition was not visible")
        return ReviewCaseMutation(case=projected, appended=appended)

    def _now(self) -> datetime:
        return _utc(self._clock(), "clock")


def _operation_id(
    *,
    case_id: str,
    target_status: ReviewCaseStatus,
    actor: ReviewActor,
    reason_codes: tuple[str, ...],
    at: datetime,
) -> str:
    material = "|".join(
        (
            case_id,
            target_status.value,
            actor.value,
            ",".join(sorted(reason_codes)),
            at.isoformat(),
        )
    ).encode()
    return f"review-{target_status.value.lower()}-{hashlib.sha256(material).hexdigest()[:32]}"


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)
