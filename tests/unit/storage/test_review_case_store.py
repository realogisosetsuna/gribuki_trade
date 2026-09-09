from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from gribuki_trade.domain.review_cases import (
    RecommendationReviewCase,
    ReviewActor,
    ReviewCaseOpened,
    ReviewCaseStatus,
    ReviewCaseTransition,
)
from gribuki_trade.storage.research.review_case_store import (
    InvalidReviewCaseTransitionError,
    ReviewCaseEventCollisionError,
    ReviewCaseNotFoundError,
    SQLiteReviewCaseStore,
)

NOW = datetime(2026, 8, 14, 1, 0, tzinfo=UTC)


def opened(
    *,
    opened_at: datetime = NOW,
    recorded_at: datetime = NOW,
    expires_at: datetime = NOW + timedelta(hours=2),
    evidence_ids: tuple[str, ...] = ("evidence-1",),
) -> ReviewCaseOpened:
    return ReviewCaseOpened(
        recommendation_id="recommendation-1",
        symbol="600000",
        recommendation_as_of=NOW - timedelta(minutes=5),
        opened_at=opened_at,
        recorded_at=recorded_at,
        expires_at=expires_at,
        evidence_ids=evidence_ids,
        candidate_provenance=None,
        actor=ReviewActor.SYSTEM,
        reason_codes=("DETAILED_REVIEW_REQUIRED",),
    )


def transition(
    case: ReviewCaseOpened,
    *,
    target: ReviewCaseStatus = ReviewCaseStatus.CONFIRMED,
    operation_id: str = "review-operation-1",
    at: datetime = NOW + timedelta(minutes=30),
    reasons: tuple[str, ...] = ("EVIDENCE_RECHECKED",),
) -> ReviewCaseTransition:
    return ReviewCaseTransition(
        case_id=case.case_id,
        target_status=target,
        operation_id=operation_id,
        actor=ReviewActor.LOCAL_USER,
        reason_codes=reasons,
        occurred_at=at,
        recorded_at=at,
    )


def required(item: RecommendationReviewCase | None) -> RecommendationReviewCase:
    assert item is not None
    return item


def test_open_is_idempotent_and_identity_collision_is_rejected(tmp_path) -> None:
    item = opened()
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        assert store.append_opened(item) is True
        assert store.append_opened(item) is False
        with pytest.raises(ReviewCaseEventCollisionError):
            store.append_opened(replace(item, evidence_ids=("different",)))


def test_open_visibility_uses_recorded_time_not_backdated_effective_time(tmp_path) -> None:
    item = opened(
        opened_at=NOW,
        recorded_at=NOW + timedelta(minutes=10),
    )
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        store.append_opened(item)
        assert store.get_case(item.case_id, as_of=NOW + timedelta(minutes=5)) is None
        visible = store.get_case(item.case_id, as_of=NOW + timedelta(minutes=10))

    assert required(visible).status is ReviewCaseStatus.PENDING_REVIEW


def test_expiry_is_an_automatic_point_in_time_projection(tmp_path) -> None:
    item = opened(expires_at=NOW + timedelta(hours=1))
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        store.append_opened(item)
        before = required(
            store.get_case(item.case_id, as_of=NOW + timedelta(minutes=59))
        )
        at_boundary = required(
            store.get_case(item.case_id, as_of=NOW + timedelta(hours=1))
        )
        expired = store.list_cases(
            as_of=NOW + timedelta(hours=1),
            statuses=frozenset({ReviewCaseStatus.EXPIRED}),
        )

    assert before.status is ReviewCaseStatus.PENDING_REVIEW
    assert at_boundary.status is ReviewCaseStatus.EXPIRED
    assert expired == (at_boundary,)


def test_terminal_transition_preserves_historical_pending_state_and_audit(tmp_path) -> None:
    item = opened()
    decision = transition(item)
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        store.append_opened(item)
        store.append_transition(decision)
        before = required(
            store.get_case(item.case_id, as_of=decision.occurred_at - timedelta(seconds=1))
        )
        after = required(store.get_case(item.case_id, as_of=decision.occurred_at))
        history = store.history(item.case_id)

    assert before.status is ReviewCaseStatus.PENDING_REVIEW
    assert after.status is ReviewCaseStatus.CONFIRMED
    assert after.is_research_approved is True
    assert after.resolution is not None
    assert after.resolution.actor is ReviewActor.LOCAL_USER
    assert after.resolution.reason_codes == ("EVIDENCE_RECHECKED",)
    assert [entry.event_type for entry in history] == ["OPENED", "CONFIRMED"]
    assert history[0].sequence < history[1].sequence


def test_repeated_transition_is_idempotent_collision_safe_and_terminal(tmp_path) -> None:
    item = opened()
    decision = transition(item)
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        store.append_opened(item)
        assert store.append_transition(decision) is True
        assert store.append_transition(decision) is False
        with pytest.raises(ReviewCaseEventCollisionError):
            store.append_transition(replace(decision, reason_codes=("MUTATED",)))
        with pytest.raises(InvalidReviewCaseTransitionError, match="only a pending"):
            store.append_transition(
                transition(
                    item,
                    target=ReviewCaseStatus.REJECTED,
                    operation_id="second-operation",
                )
            )


def test_expired_or_unknown_cases_cannot_transition(tmp_path) -> None:
    item = opened(expires_at=NOW + timedelta(hours=1))
    expired_transition = transition(item, at=NOW + timedelta(hours=1))
    unknown = replace(expired_transition, case_id="f" * 64)
    with SQLiteReviewCaseStore(tmp_path / "reviews.sqlite3") as store:
        with pytest.raises(ReviewCaseNotFoundError):
            store.append_transition(unknown)
        store.append_opened(item)
        with pytest.raises(InvalidReviewCaseTransitionError, match="expired"):
            store.append_transition(expired_transition)


def test_concurrent_exact_transition_commits_once_and_store_uses_wal(tmp_path) -> None:
    path = tmp_path / "reviews.sqlite3"
    item = opened()
    decision = transition(item)
    with SQLiteReviewCaseStore(path) as store:
        store.append_opened(item)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = tuple(pool.map(store.append_transition, (decision,) * 32))
        assert sum(results) == 1
        assert len(store.history(item.case_id)) == 2
        with sqlite3.connect(path) as connection:
            mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])

    assert mode.lower() == "wal"


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"actor": "USER"}, "actor must be a ReviewActor"),
        ({"reason_codes": ()}, "reason_codes must not be empty"),
        (
            {"expires_at": NOW},
            "expires_at must follow recorded_at",
        ),
        (
            {"recorded_at": NOW - timedelta(seconds=1)},
            "opened_at must not follow recorded_at",
        ),
    ],
)
def test_open_domain_fails_closed(changes: dict[str, object], error: str) -> None:
    with pytest.raises(ValueError, match=error):
        replace(opened(), **changes)
