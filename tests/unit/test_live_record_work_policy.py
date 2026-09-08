"""实盘工作队列租约策略与参数化查询的纯函数测试。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from gribuki_trade.domain.live_records import LiveWorkKind
from gribuki_trade.storage.live_record_work_policy import (
    build_claim_due_work_query,
    normalize_lease_for,
    normalize_work_claim,
    normalize_work_failure,
)

_NOW = datetime(2026, 8, 14, 6, 0, tzinfo=UTC)


def test_claim_policy_normalizes_time_filters_and_query_parameters() -> None:
    policy = normalize_work_claim(
        now=_NOW,
        lease_for=timedelta(minutes=5),
        kinds=frozenset({LiveWorkKind.CLOSE_PROTECTION, LiveWorkKind.BUILD_PROTECTION}),
        work_ids=frozenset({"work-b", "work-a"}),
        limit=3,
    )

    query, parameters = build_claim_due_work_query(policy)

    assert policy.moment == _NOW
    assert policy.kinds == ("BUILD_PROTECTION", "CLOSE_PROTECTION")
    assert policy.work_ids == ("work-a", "work-b")
    assert query.count("?") == 2 + 2 + 2 + 1
    assert parameters == (
        _NOW.isoformat(timespec="microseconds"),
        _NOW.isoformat(timespec="microseconds"),
        "BUILD_PROTECTION",
        "CLOSE_PROTECTION",
        "work-a",
        "work-b",
        3,
    )


def test_empty_work_filter_is_preserved_for_facade_fast_path() -> None:
    policy = normalize_work_claim(
        now=_NOW,
        lease_for=timedelta(seconds=1),
        kinds=None,
        work_ids=frozenset(),
        limit=1,
    )

    assert policy.work_ids == ()


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (timedelta(0), "lease_for must be positive"),
        (timedelta(seconds=-1), "lease_for must be positive"),
    ],
)
def test_lease_duration_must_be_positive(value: timedelta, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        normalize_lease_for(value)


def test_failure_policy_normalizes_error_and_fencing_inputs() -> None:
    policy = normalize_work_failure(
        failed_at=_NOW,
        error_code=" retryable_network ",
        retry_after=timedelta(seconds=30),
        maximum_attempts=4,
        lease_attempt=2,
    )

    assert policy.moment == _NOW
    assert policy.error_code == "RETRYABLE_NETWORK"
    assert policy.retry_after == timedelta(seconds=30)
    assert policy.maximum_attempts == 4
    assert policy.lease_attempt == 2


def test_failure_policy_rejects_invalid_retry_and_attempts() -> None:
    with pytest.raises(ValueError, match="retry_after must not be negative"):
        normalize_work_failure(
            failed_at=_NOW,
            error_code="FAILED",
            retry_after=timedelta(seconds=-1),
            maximum_attempts=1,
            lease_attempt=1,
        )
    with pytest.raises(ValueError, match="maximum_attempts must be positive"):
        normalize_work_failure(
            failed_at=_NOW,
            error_code="FAILED",
            retry_after=timedelta(0),
            maximum_attempts=0,
            lease_attempt=1,
        )
