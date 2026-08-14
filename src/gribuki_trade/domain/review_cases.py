"""独立于券商的推荐复核案例领域模型。

确认复核只代表认可研究结论。本模块中的任何类型都不表示订单、资金配置、账户指令
或执行交易的许可。
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from gribuki_trade.domain.candidates import (
    CandidatePriority,
    CandidateRecord,
    CandidateSource,
    CandidateStatus,
    canonical_ashare_symbol,
)

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/+\-]{0,159}$")


class ReviewCaseStatus(StrEnum):
    """推荐复核在特定时点的状态。"""

    PENDING_REVIEW = "PENDING_REVIEW"
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"


class ReviewActor(StrEnum):
    """允许发起或解决复核的可审计参与者类别。"""

    LOCAL_USER = "LOCAL_USER"
    SYSTEM = "SYSTEM"


TERMINAL_REVIEW_STATUSES = frozenset(
    {
        ReviewCaseStatus.CONFIRMED,
        ReviewCaseStatus.REJECTED,
        ReviewCaseStatus.CANCELLED,
    }
)


@dataclass(frozen=True, slots=True)
class CandidateProvenanceSummary:
    """用于请求复核的候选状态不可变摘要。"""

    symbol: str
    candidate_as_of: datetime
    candidate_status: CandidateStatus
    priority: CandidatePriority
    observation_ids: tuple[str, ...]
    sources: tuple[CandidateSource, ...]
    source_run_ids: tuple[str, ...]
    first_observed_at: datetime
    last_observed_at: datetime
    reason_codes: tuple[str, ...]
    evidence_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        symbol = canonical_ashare_symbol(self.symbol)
        candidate_as_of = _utc(self.candidate_as_of, "candidate_as_of")
        first_observed_at = _utc(self.first_observed_at, "first_observed_at")
        last_observed_at = _utc(self.last_observed_at, "last_observed_at")
        if not isinstance(self.candidate_status, CandidateStatus):
            raise ValueError("candidate_status must be a CandidateStatus")
        if not isinstance(self.priority, CandidatePriority):
            raise ValueError("priority must be a CandidatePriority")
        if first_observed_at > last_observed_at:
            raise ValueError("first_observed_at must not follow last_observed_at")
        if last_observed_at > candidate_as_of:
            raise ValueError("candidate observations must be visible at candidate_as_of")
        observations = _tokens(
            self.observation_ids,
            "observation_ids",
            required=True,
        )
        sources = tuple(sorted(self.sources, key=lambda item: item.value))
        if not sources or any(not isinstance(item, CandidateSource) for item in sources):
            raise ValueError("sources must contain CandidateSource values")
        if len(sources) != len(set(sources)):
            raise ValueError("sources must be unique")
        source_runs = _tokens(self.source_run_ids, "source_run_ids", required=True)
        reasons = _tokens(self.reason_codes, "reason_codes", required=True)
        evidence = _tokens(self.evidence_ids, "evidence_ids", required=False)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "candidate_as_of", candidate_as_of)
        object.__setattr__(self, "first_observed_at", first_observed_at)
        object.__setattr__(self, "last_observed_at", last_observed_at)
        object.__setattr__(self, "observation_ids", observations)
        object.__setattr__(self, "sources", sources)
        object.__setattr__(self, "source_run_ids", source_runs)
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "evidence_ids", evidence)

    @classmethod
    def from_candidate(cls, candidate: CandidateRecord) -> CandidateProvenanceSummary:
        """创建紧凑快照且不保留可变存储对象。"""

        return cls(
            symbol=candidate.symbol,
            candidate_as_of=candidate.as_of,
            candidate_status=candidate.status,
            priority=candidate.priority,
            observation_ids=tuple(item.observation_id for item in candidate.provenance),
            sources=candidate.sources,
            source_run_ids=tuple(
                sorted({item.source_run_id for item in candidate.provenance})
            ),
            first_observed_at=candidate.first_observed_at,
            last_observed_at=candidate.last_observed_at,
            reason_codes=candidate.reason_codes,
            evidence_ids=candidate.evidence_ids,
        )


@dataclass(frozen=True, slots=True)
class ReviewCaseOpened:
    """一个推荐复核案例的不可变创建事件。"""

    recommendation_id: str
    symbol: str
    recommendation_as_of: datetime
    opened_at: datetime
    recorded_at: datetime
    expires_at: datetime
    evidence_ids: tuple[str, ...]
    candidate_provenance: CandidateProvenanceSummary | None
    actor: ReviewActor
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        recommendation_id = self.recommendation_id.strip()
        if not _SAFE_ID.fullmatch(recommendation_id):
            raise ValueError("recommendation_id must be a safe non-empty identifier")
        if not isinstance(self.actor, ReviewActor):
            raise ValueError("actor must be a ReviewActor")
        symbol = canonical_ashare_symbol(self.symbol)
        recommendation_as_of = _utc(self.recommendation_as_of, "recommendation_as_of")
        opened_at = _utc(self.opened_at, "opened_at")
        recorded_at = _utc(self.recorded_at, "recorded_at")
        expires_at = _utc(self.expires_at, "expires_at")
        if recommendation_as_of > opened_at:
            raise ValueError("recommendation_as_of must not follow opened_at")
        if opened_at > recorded_at:
            raise ValueError("opened_at must not follow recorded_at")
        if expires_at <= recorded_at:
            raise ValueError("expires_at must follow recorded_at")
        evidence_ids = _tokens(self.evidence_ids, "evidence_ids", required=False)
        reasons = _tokens(self.reason_codes, "reason_codes", required=True)
        provenance = self.candidate_provenance
        if provenance is not None:
            if provenance.symbol != symbol:
                raise ValueError("candidate provenance symbol must match review symbol")
            if provenance.candidate_as_of > opened_at:
                raise ValueError("candidate provenance must be knowable when review opens")
        object.__setattr__(self, "recommendation_id", recommendation_id)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "recommendation_as_of", recommendation_as_of)
        object.__setattr__(self, "opened_at", opened_at)
        object.__setattr__(self, "recorded_at", recorded_at)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(self, "evidence_ids", evidence_ids)
        object.__setattr__(self, "reason_codes", reasons)

    @property
    def case_id(self) -> str:
        material = f"recommendation-review@1\0{self.recommendation_id}".encode()
        return hashlib.sha256(material).hexdigest()

    @property
    def event_id(self) -> str:
        return self.case_id


@dataclass(frozen=True, slots=True)
class ReviewCaseTransition:
    """保留在审计日志中的一次终态转换尝试。"""

    case_id: str
    target_status: ReviewCaseStatus
    operation_id: str
    actor: ReviewActor
    reason_codes: tuple[str, ...]
    occurred_at: datetime
    recorded_at: datetime

    def __post_init__(self) -> None:
        case_id = self.case_id.strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", case_id):
            raise ValueError("case_id must be a lowercase SHA-256 identifier")
        if self.target_status not in TERMINAL_REVIEW_STATUSES:
            raise ValueError("target_status must be CONFIRMED, REJECTED, or CANCELLED")
        if not isinstance(self.actor, ReviewActor):
            raise ValueError("actor must be a ReviewActor")
        operation_id = self.operation_id.strip()
        if not _SAFE_ID.fullmatch(operation_id):
            raise ValueError("operation_id must be a safe non-empty identifier")
        reasons = _tokens(self.reason_codes, "reason_codes", required=True)
        occurred_at = _utc(self.occurred_at, "occurred_at")
        recorded_at = _utc(self.recorded_at, "recorded_at")
        if occurred_at > recorded_at:
            raise ValueError("occurred_at must not follow recorded_at")
        object.__setattr__(self, "case_id", case_id)
        object.__setattr__(self, "operation_id", operation_id)
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "recorded_at", recorded_at)

    @property
    def event_id(self) -> str:
        material = (
            f"recommendation-review-transition@1\0{self.case_id}"
            f"\0{self.operation_id}"
        ).encode()
        return hashlib.sha256(material).hexdigest()


@dataclass(frozen=True, slots=True)
class ReviewResolution:
    """在案例投影中可见的研究复核终态决定。"""

    status: ReviewCaseStatus
    actor: ReviewActor
    reason_codes: tuple[str, ...]
    occurred_at: datetime
    recorded_at: datetime
    operation_id: str
    event_id: str


@dataclass(frozen=True, slots=True)
class RecommendationReviewCase:
    """一个复核案例的时点投影。"""

    case_id: str
    recommendation_id: str
    symbol: str
    recommendation_as_of: datetime
    opened_at: datetime
    recorded_at: datetime
    expires_at: datetime
    as_of: datetime
    status: ReviewCaseStatus
    evidence_ids: tuple[str, ...]
    candidate_provenance: CandidateProvenanceSummary | None
    opened_by: ReviewActor
    open_reason_codes: tuple[str, ...]
    resolution: ReviewResolution | None

    @property
    def is_research_approved(self) -> bool:
        """研究是否已确认；该结果绝不授权执行。"""

        return self.status is ReviewCaseStatus.CONFIRMED


@dataclass(frozen=True, slots=True)
class ReviewCaseAuditRecord:
    """一条仅追加复核事件的公开完整性元数据。"""

    sequence: int
    event_id: str
    case_id: str
    event_type: str
    effective_at: datetime
    recorded_at: datetime
    payload_sha256: str


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _tokens(
    values: tuple[str, ...],
    field_name: str,
    *,
    required: bool,
) -> tuple[str, ...]:
    normalized = tuple(sorted(item.strip() for item in values))
    if required and not normalized:
        raise ValueError(f"{field_name} must not be empty")
    if any(not item for item in normalized):
        raise ValueError(f"{field_name} must not contain blank values")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} must be unique")
    return normalized
