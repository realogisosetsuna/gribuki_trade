"""实盘工作队列的租约参数与查询策略。

本模块只负责把调用参数规范化，并生成确定性的领取查询；不持有连接、
不执行事务，也不改变工作项状态。SQLite facade 因此只保留事务和行投影。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from gribuki_trade.domain.live_records import LiveWorkKind, LiveWorkStatus
from gribuki_trade.storage.live_record_codec import (
    _aware_utc,
    _error_code,
    _lease_attempt,
    _time,
)


@dataclass(frozen=True)
class WorkClaimPolicy:
    """领取工作时经过校验的参数。"""

    moment: datetime
    lease_for: timedelta
    kinds: tuple[str, ...] | None
    work_ids: tuple[str, ...] | None
    limit: int


@dataclass(frozen=True)
class WorkFailurePolicy:
    """失败重试与租约代际的确定性策略。"""

    moment: datetime
    error_code: str
    retry_after: timedelta
    maximum_attempts: int
    lease_attempt: int


@dataclass(frozen=True)
class WorkFailureProjection:
    """一次失败报告对应的纯状态、可用时间和终态判断。"""

    status: LiveWorkStatus
    available_at: datetime
    dead: bool


def normalize_lease_for(value: timedelta) -> timedelta:
    """验证租约期限必须为正时间间隔。"""

    if not isinstance(value, timedelta) or value <= timedelta(0):
        raise ValueError("lease_for must be positive")
    return value


def normalize_work_ids(work_ids: frozenset[str] | None) -> tuple[str, ...] | None:
    """规范化领取过滤器中的工作 ID，并拒绝空值和重复值。"""

    if work_ids is None:
        return None
    normalized = tuple(
        sorted(
            {
                work_id.strip()
                for work_id in work_ids
                if isinstance(work_id, str) and work_id.strip()
            }
        )
    )
    if len(normalized) != len(work_ids):
        raise ValueError("work_ids must contain only unique non-empty strings")
    return normalized


def normalize_work_claim(
    *,
    now: datetime,
    lease_for: timedelta,
    kinds: frozenset[LiveWorkKind] | None,
    work_ids: frozenset[str] | None,
    limit: int,
) -> WorkClaimPolicy:
    """把领取参数转换成可安全拼接参数化 SQL 的策略对象。"""

    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be a positive integer")
    normalized_kinds = None
    if kinds:
        normalized_kinds = tuple(sorted(str(kind.value) for kind in kinds))
    return WorkClaimPolicy(
        moment=_aware_utc(now),
        lease_for=normalize_lease_for(lease_for),
        kinds=normalized_kinds,
        work_ids=normalize_work_ids(work_ids),
        limit=limit,
    )


def build_claim_due_work_query(policy: WorkClaimPolicy) -> tuple[str, tuple[object, ...]]:
    """生成领取到期工作项的参数化查询，保持筛选顺序稳定。"""

    filters = [
        "available_at <= ?",
        "(status IN ('PENDING','RETRY') OR (status = 'RUNNING' AND lease_until <= ?))",
    ]
    parameters: list[object] = [
        _time(policy.moment),
        _time(policy.moment),
    ]
    if policy.kinds:
        filters.append("kind IN (" + ",".join("?" for _ in policy.kinds) + ")")
        parameters.extend(policy.kinds)
    if policy.work_ids is not None:
        filters.append("work_id IN (" + ",".join("?" for _ in policy.work_ids) + ")")
        parameters.extend(policy.work_ids)
    query = (
        "SELECT work_id FROM live_work_items WHERE "
        + " AND ".join(filters)
        + " ORDER BY available_at, created_at, work_id LIMIT ?"
    )
    parameters.append(policy.limit)
    return query, tuple(parameters)


def normalize_work_failure(
    *,
    failed_at: datetime,
    error_code: str,
    retry_after: timedelta,
    maximum_attempts: int,
    lease_attempt: int,
) -> WorkFailurePolicy:
    """验证失败转移的时间、错误码、重试窗口和租约代际。"""

    if not isinstance(retry_after, timedelta) or retry_after < timedelta(0):
        raise ValueError("retry_after must not be negative")
    if isinstance(maximum_attempts, bool) or not isinstance(maximum_attempts, int):
        raise ValueError("maximum_attempts must be positive")
    if maximum_attempts < 1:
        raise ValueError("maximum_attempts must be positive")
    return WorkFailurePolicy(
        moment=_aware_utc(failed_at),
        error_code=_error_code(error_code),
        retry_after=retry_after,
        maximum_attempts=maximum_attempts,
        lease_attempt=_lease_attempt(lease_attempt),
    )


def project_work_failure(
    *,
    attempts: int,
    retryable: bool,
    policy: WorkFailurePolicy,
) -> WorkFailureProjection:
    """根据当前尝试次数和重试策略投影下一工作状态。"""

    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 1:
        raise ValueError("attempts must be a positive integer")
    dead = not retryable or attempts >= policy.maximum_attempts
    return WorkFailureProjection(
        status=LiveWorkStatus.DEAD if dead else LiveWorkStatus.RETRY,
        available_at=policy.moment if dead else policy.moment + policy.retry_after,
        dead=dead,
    )

__all__: Final = [
    "WorkClaimPolicy",
    "WorkFailurePolicy",
    "WorkFailureProjection",
    "build_claim_due_work_query",
    "normalize_lease_for",
    "normalize_work_claim",
    "normalize_work_failure",
    "normalize_work_ids",
    "project_work_failure",
]
