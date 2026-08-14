"""面向 A 股技术入场的非阻塞、时点一致 LLM 复核。

本模块有意分离四个边界：

* :class:`MacroResearchPlan` 冻结全部提供方输入及哈希；
* 后台任务可以执行这些计划，但不能修改交易状态；
* 已完成复核在日志写入方显式确认精确复核及其持久事件哈希前仍不可用于交易；
* 最终买入门是无网络路径的同步缓存查询。

本模块负责入场复核：LLM 只能确认、否决或降级确定性技术信号，不能把
``WATCH``/``ABSTAIN`` 升级为入场。卖出/``REDUCE`` 的 LLM 评分由成交后
DEEP 退出计划负责，不能绕过硬止损、T+1 和价格边界。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum, StrEnum
from typing import TypeAlias, cast

from gribuki_trade.analysis.schemas import MacroAnalysis, MacroAnalysisDecision
from gribuki_trade.domain.recommendations import (
    EvidenceReference,
    RecommendationDecision,
)
from gribuki_trade.features.technical import TechnicalSignal
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
from gribuki_trade.services.macro_research import (
    MacroResearchPlan,
    MacroResearchRun,
    MacroResearchService,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_FAILURE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,79}$")

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]


@dataclass(frozen=True, slots=True)
class IntradayLLMConfig:
    """仅用于 PAPER 的保守融合参数与后台执行限制。"""

    enabled: bool = True
    required_for_buy: bool = True
    review_top_n: int = 6
    review_ttl: timedelta = timedelta(minutes=20)
    # FAST 双轨 case 的内部 deadline 为 24 秒；外层额外保留落盘余量。
    per_review_timeout: timedelta = timedelta(seconds=28)
    max_concurrency: int = 3
    maximum_reviews_per_session: int | None = None
    technical_weight: Decimal = Decimal("0.75")
    macro_weight: Decimal = Decimal("0.25")
    macro_watch_weight_multiplier: Decimal = Decimal("1.00")
    minimum_macro_evidence_coverage: Decimal = Decimal("0.10")
    entry_combined_score_threshold: Decimal = Decimal("0.70")
    macro_veto_threshold: Decimal = Decimal("-0.60")
    policy_version: str = "ashare-intraday-llm-gate@1"

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool) or not isinstance(self.required_for_buy, bool):
            raise TypeError("enabled and required_for_buy must be bool")
        if self.review_top_n < 1:
            raise ValueError("review_top_n must be positive")
        if self.review_ttl <= timedelta(0) or self.per_review_timeout <= timedelta(0):
            raise ValueError("review TTL and timeout must be positive")
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        if self.maximum_reviews_per_session is not None and (
            isinstance(self.maximum_reviews_per_session, bool)
            or not isinstance(self.maximum_reviews_per_session, int)
            or self.maximum_reviews_per_session < 1
        ):
            raise ValueError(
                "maximum_reviews_per_session must be a positive integer or None"
            )
        for name in (
            "technical_weight",
            "macro_weight",
            "macro_watch_weight_multiplier",
            "minimum_macro_evidence_coverage",
            "entry_combined_score_threshold",
            "macro_veto_threshold",
        ):
            if not getattr(self, name).is_finite():
                raise ValueError(f"{name} must be finite")
        if self.technical_weight <= 0 or self.macro_weight < 0:
            raise ValueError("technical weight must be positive and macro weight non-negative")
        if self.macro_weight > Decimal("0.40"):
            raise ValueError("macro_weight must not exceed 0.40")
        if self.technical_weight + self.macro_weight != Decimal("1"):
            raise ValueError("technical_weight and macro_weight must sum to one")
        if not Decimal("0") <= self.macro_watch_weight_multiplier <= Decimal("1"):
            raise ValueError("macro_watch_weight_multiplier must be in [0, 1]")
        if not Decimal("0") <= self.minimum_macro_evidence_coverage <= Decimal("1"):
            raise ValueError("minimum_macro_evidence_coverage must be in [0, 1]")
        if not Decimal("-1") <= self.entry_combined_score_threshold <= Decimal("1"):
            raise ValueError("entry_combined_score_threshold must be in [-1, 1]")
        if not Decimal("-1") <= self.macro_veto_threshold <= Decimal("0"):
            raise ValueError("macro_veto_threshold must be in [-1, 0]")
        if not self.policy_version.strip():
            raise ValueError("policy_version must not be empty")

    @property
    def manifest_sha256(self) -> str:
        return intraday_llm_document_sha256(
            {
                "enabled": self.enabled,
                "entry_combined_score_threshold": self.entry_combined_score_threshold,
                "macro_veto_threshold": self.macro_veto_threshold,
                "macro_watch_weight_multiplier": self.macro_watch_weight_multiplier,
                "macro_weight": self.macro_weight,
                "max_concurrency": self.max_concurrency,
                "maximum_reviews_per_session": self.maximum_reviews_per_session,
                "minimum_macro_evidence_coverage": self.minimum_macro_evidence_coverage,
                "per_review_timeout_seconds": self.per_review_timeout.total_seconds(),
                "policy_version": self.policy_version,
                "required_for_buy": self.required_for_buy,
                "review_top_n": self.review_top_n,
                "review_ttl_seconds": self.review_ttl.total_seconds(),
                "technical_weight": self.technical_weight,
            }
        )


@dataclass(frozen=True, slots=True)
class IntradayLLMContext:
    """为一次后台复核调度的精确候选与证据输入。"""

    context_id: str
    session_date: date
    symbol: str
    preopen_context_id: str
    scan_revision: str
    candidate_scope_sha256: str
    evidence_as_of: datetime
    requested_at: datetime
    valid_until: datetime
    plan: MacroResearchPlan
    config_sha256: str

    def __post_init__(self) -> None:
        symbol = _canonical_symbol(self.symbol)
        evidence_as_of = _aware_utc(self.evidence_as_of, "evidence_as_of")
        requested_at = _aware_utc(self.requested_at, "requested_at")
        valid_until = _aware_utc(self.valid_until, "valid_until")
        if not self.preopen_context_id.strip():
            raise ValueError("preopen_context_id must not be empty")
        if not self.scan_revision.strip():
            raise ValueError("scan_revision must not be empty")
        for name in ("candidate_scope_sha256", "config_sha256"):
            if not _SHA256.fullmatch(getattr(self, name)):
                raise ValueError(f"{name} must be a lowercase SHA-256 digest")
        if self.plan.analyzer_identity is None:
            raise ValueError("intraday plans require an auditable analyzer identity")
        if self.plan.request.symbol != symbol:
            raise ValueError("plan symbol does not match intraday context")
        if _aware_utc(self.plan.request.as_of, "plan as_of") != evidence_as_of:
            raise ValueError("plan as_of must equal evidence_as_of")
        if requested_at < evidence_as_of:
            raise ValueError("requested_at must not precede evidence_as_of")
        if valid_until <= evidence_as_of:
            raise ValueError("valid_until must follow evidence_as_of")
        expected_id = _context_id(
            session_date=self.session_date,
            symbol=symbol,
            preopen_context_id=self.preopen_context_id,
            scan_revision=self.scan_revision,
            candidate_scope_sha256=self.candidate_scope_sha256,
            evidence_as_of=evidence_as_of,
            valid_until=valid_until,
            plan_manifest_sha256=self.plan.manifest_sha256,
            config_sha256=self.config_sha256,
        )
        if self.context_id != expected_id:
            raise ValueError("context_id does not match intraday LLM context")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "evidence_as_of", evidence_as_of)
        object.__setattr__(self, "requested_at", requested_at)
        object.__setattr__(self, "valid_until", valid_until)

    @classmethod
    def create(
        cls,
        *,
        session_date: date,
        preopen_context_id: str,
        scan_revision: str,
        candidate_scope_sha256: str,
        requested_at: datetime,
        plan: MacroResearchPlan,
        config: IntradayLLMConfig,
    ) -> IntradayLLMContext:
        evidence_as_of = _aware_utc(plan.request.as_of, "plan as_of")
        valid_until = evidence_as_of + config.review_ttl
        symbol = _canonical_symbol(plan.request.symbol)
        context_id = _context_id(
            session_date=session_date,
            symbol=symbol,
            preopen_context_id=preopen_context_id,
            scan_revision=scan_revision,
            candidate_scope_sha256=candidate_scope_sha256,
            evidence_as_of=evidence_as_of,
            valid_until=valid_until,
            plan_manifest_sha256=plan.manifest_sha256,
            config_sha256=config.manifest_sha256,
        )
        return cls(
            context_id=context_id,
            session_date=session_date,
            symbol=symbol,
            preopen_context_id=preopen_context_id,
            scan_revision=scan_revision,
            candidate_scope_sha256=candidate_scope_sha256,
            evidence_as_of=evidence_as_of,
            requested_at=requested_at,
            valid_until=valid_until,
            plan=plan,
            config_sha256=config.manifest_sha256,
        )


@dataclass(frozen=True, slots=True)
class IntradayLLMReview:
    """已完成的提供方观察；经日志接受前不可用于交易。"""

    review_id: str
    context_id: str
    session_date: date
    symbol: str
    preopen_context_id: str
    scan_revision: str
    candidate_scope_sha256: str
    analysis: MacroAnalysis
    references: tuple[EvidenceReference, ...]
    request_sha256: str
    evidence_pack_sha256: str
    plan_manifest_sha256: str
    analyzer_identity: AnalyzerAuditIdentity
    evidence_as_of: datetime
    requested_at: datetime
    completed_at: datetime
    expires_at: datetime
    response_model: str
    latency_ms: int
    failure_code: str | None = None
    baseline_analysis: MacroAnalysis | None = None
    adversarial_analysis: MacroAnalysis | None = None
    selected_track: str | None = None
    dual_audit_record_sha256: str | None = None

    def __post_init__(self) -> None:
        symbol = _canonical_symbol(self.symbol)
        evidence_as_of = _aware_utc(self.evidence_as_of, "evidence_as_of")
        requested_at = _aware_utc(self.requested_at, "requested_at")
        completed_at = _aware_utc(self.completed_at, "completed_at")
        expires_at = _aware_utc(self.expires_at, "expires_at")
        if not evidence_as_of <= requested_at <= completed_at:
            raise ValueError("review timestamps violate PIT chronology")
        if expires_at <= evidence_as_of:
            raise ValueError("review expiry must follow evidence_as_of")
        for name in (
            "candidate_scope_sha256",
            "request_sha256",
            "evidence_pack_sha256",
            "plan_manifest_sha256",
        ):
            if not _SHA256.fullmatch(getattr(self, name)):
                raise ValueError(f"{name} must be a lowercase SHA-256 digest")
        if self.analysis.as_of != evidence_as_of:
            raise ValueError("analysis as_of must equal review evidence_as_of")
        if not self.response_model.strip() or self.response_model != self.analysis.model_version:
            raise ValueError("response_model must match the validated analysis")
        if self.latency_ms < 0:
            raise ValueError("latency_ms must be non-negative")
        if self.failure_code is not None:
            _stable_failure_code(self.failure_code)
            if self.analysis.decision is not MacroAnalysisDecision.ABSTAIN:
                raise ValueError("failed reviews must carry an ABSTAIN analysis")
        dual_values = (self.baseline_analysis, self.adversarial_analysis)
        if any(item is not None for item in dual_values):
            if any(item is None for item in dual_values) or self.selected_track != "ADVERSARIAL":
                raise ValueError(
                    "dual-track review requires both branches and ADVERSARIAL selection"
                )
            assert self.baseline_analysis is not None
            assert self.adversarial_analysis is not None
            if any(
                item.analysis_id != self.analysis.analysis_id
                or item.as_of != evidence_as_of
                for item in dual_values
                if item is not None
            ):
                raise ValueError("dual-track analyses must match the selected review identity")
            if self.dual_audit_record_sha256 is not None and not _SHA256.fullmatch(
                self.dual_audit_record_sha256
            ):
                raise ValueError("dual audit record hash must be a lowercase SHA-256 digest")
        elif self.selected_track is not None or self.dual_audit_record_sha256 is not None:
            raise ValueError("single-track review cannot carry dual-track metadata")
        reference_ids = tuple(item.evidence_id for item in self.references)
        if len(reference_ids) != len(set(reference_ids)):
            raise ValueError("review evidence references must be unique")
        expected_id = _review_id(
            context_id=self.context_id,
            analysis=self.analysis,
            completed_at=completed_at,
            failure_code=self.failure_code,
        )
        if self.review_id != expected_id:
            raise ValueError("review_id does not match completed LLM review")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "evidence_as_of", evidence_as_of)
        object.__setattr__(self, "requested_at", requested_at)
        object.__setattr__(self, "completed_at", completed_at)
        object.__setattr__(self, "expires_at", expires_at)


@dataclass(frozen=True, slots=True)
class JournaledIntradayLLMReview:
    """其精确观察已被调用方持久接受的复核。"""

    review: IntradayLLMReview
    accepted_at: datetime
    journal_event_id: str
    journal_event_sha256: str

    def __post_init__(self) -> None:
        accepted_at = _aware_utc(self.accepted_at, "accepted_at")
        if accepted_at < self.review.completed_at:
            raise ValueError("journal acceptance cannot precede review completion")
        if accepted_at >= self.review.expires_at:
            raise ValueError("an expired review cannot enter the tradable cache")
        if not self.journal_event_id.strip():
            raise ValueError("journal_event_id must not be empty")
        if not _SHA256.fullmatch(self.journal_event_sha256):
            raise ValueError("journal_event_sha256 must be a lowercase SHA-256 digest")
        object.__setattr__(self, "accepted_at", accepted_at)


class IntradayLLMScheduleStatus(StrEnum):
    SCHEDULED = "SCHEDULED"
    IN_FLIGHT = "IN_FLIGHT"
    COMPLETED_AWAITING_JOURNAL = "COMPLETED_AWAITING_JOURNAL"
    CACHE_HIT = "CACHE_HIT"
    DISABLED = "DISABLED"
    SESSION_BUDGET_EXHAUSTED = "SESSION_BUDGET_EXHAUSTED"
    CONTEXT_EXPIRED = "CONTEXT_EXPIRED"
    IDENTITY_MISMATCH = "IDENTITY_MISMATCH"
    CLOSED = "CLOSED"


@dataclass(frozen=True, slots=True)
class IntradayLLMScheduleOutcome:
    status: IntradayLLMScheduleStatus
    context_id: str
    review_id: str | None = None


class IntradayLLMGateAction(StrEnum):
    ALLOW_ENTRY = "ALLOW_ENTRY"
    DOWNGRADE_TO_WATCH = "DOWNGRADE_TO_WATCH"
    VETO_ENTRY = "VETO_ENTRY"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class IntradayLLMGateReason(StrEnum):
    APPROVED = "LLM_GATE_APPROVED"
    OPTIONAL_BYPASS = "LLM_GATE_OPTIONAL_BYPASS"
    DISABLED = "LLM_GATE_DISABLED"
    TECHNICAL_NOT_ENTER = "LLM_TECHNICAL_NOT_ENTER"
    REDUCE_NOT_APPLICABLE = "LLM_NOT_APPLICABLE_REDUCE"
    PREOPEN_CONTEXT_UNAVAILABLE = "LLM_PREOPEN_CONTEXT_UNAVAILABLE"
    REVIEW_NOT_READY = "LLM_REVIEW_NOT_READY"
    REVIEW_NOT_JOURNALED = "LLM_REVIEW_NOT_JOURNALED"
    REVIEW_EXPIRED = "LLM_REVIEW_EXPIRED"
    REVIEW_INPUT_MISMATCH = "LLM_REVIEW_INPUT_MISMATCH"
    REVIEW_KNOWN_AFTER_SIGNAL = "LLM_REVIEW_KNOWN_AFTER_SIGNAL"
    MODEL_IDENTITY_MISMATCH = "LLM_MODEL_IDENTITY_MISMATCH"
    REVIEW_FAILED = "LLM_REVIEW_FAILED"
    REVIEW_ABSTAINED = "LLM_REVIEW_ABSTAINED"
    EVIDENCE_INVALID = "LLM_EVIDENCE_INVALID"
    NEGATIVE_VETO = "LLM_NEGATIVE_VETO"
    COMBINED_SCORE_BELOW_ENTRY_THRESHOLD = (
        "LLM_COMBINED_SCORE_BELOW_ENTRY_THRESHOLD"
    )


@dataclass(frozen=True, slots=True)
class IntradayLLMGateOutcome:
    action: IntradayLLMGateAction
    reason: IntradayLLMGateReason
    technical_decision: RecommendationDecision
    technical_score: Decimal
    review_id: str | None = None
    macro_score: Decimal | None = None
    combined_score: Decimal | None = None
    evidence_coverage: Decimal | None = None
    accepted_at: datetime | None = None
    expires_at: datetime | None = None
    requested_model: str | None = None
    response_model: str | None = None
    prompt_schema_sha256: str | None = None
    failure_code: str | None = None
    baseline_decision: MacroAnalysisDecision | None = None
    baseline_macro_score: Decimal | None = None
    baseline_model: str | None = None
    adversarial_decision: MacroAnalysisDecision | None = None
    adversarial_macro_score: Decimal | None = None
    adversarial_model: str | None = None
    selected_track: str | None = None
    dual_audit_record_sha256: str | None = None

    @property
    def approved(self) -> bool:
        return self.action is IntradayLLMGateAction.ALLOW_ENTRY


def evaluate_intraday_llm_buy(
    signal: TechnicalSignal,
    *,
    decision_at: datetime,
    expected_context_id: str | None,
    review: JournaledIntradayLLMReview | None,
    expected_identity: AnalyzerAuditIdentity | None,
    config: IntradayLLMConfig | None = None,
    completed_not_journaled: bool = False,
) -> IntradayLLMGateOutcome:
    """仅使用本地不可变状态评估技术信号。"""

    resolved = config or IntradayLLMConfig()
    decision_at = _aware_utc(decision_at, "decision_at")
    signal_as_of = _aware_utc(signal.as_of, "signal as_of")
    if signal_as_of > decision_at:
        raise ValueError("technical signal cannot be from the future")
    if signal.decision is RecommendationDecision.REDUCE:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.NOT_APPLICABLE,
            IntradayLLMGateReason.REDUCE_NOT_APPLICABLE,
        )
    if signal.decision is not RecommendationDecision.ENTER_CANDIDATE:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.NOT_APPLICABLE,
            IntradayLLMGateReason.TECHNICAL_NOT_ENTER,
        )
    if not resolved.enabled:
        if resolved.required_for_buy:
            return _gate_outcome(
                signal,
                IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
                IntradayLLMGateReason.DISABLED,
            )
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.ALLOW_ENTRY,
            IntradayLLMGateReason.OPTIONAL_BYPASS,
            combined_score=signal.score,
        )
    if expected_context_id is None:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
            IntradayLLMGateReason.PREOPEN_CONTEXT_UNAVAILABLE,
        )
    if review is None:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
            (
                IntradayLLMGateReason.REVIEW_NOT_JOURNALED
                if completed_not_journaled
                else IntradayLLMGateReason.REVIEW_NOT_READY
            ),
        )

    completed = review.review
    common = _gate_review_fields(review)
    if completed.context_id != expected_context_id or completed.symbol != signal.symbol:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
            IntradayLLMGateReason.REVIEW_INPUT_MISMATCH,
            **common,
        )
    if expected_identity is None or completed.analyzer_identity != expected_identity:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
            IntradayLLMGateReason.MODEL_IDENTITY_MISMATCH,
            **common,
        )
    if review.accepted_at > signal_as_of:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
            IntradayLLMGateReason.REVIEW_KNOWN_AFTER_SIGNAL,
            **common,
        )
    if decision_at >= completed.expires_at:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
            IntradayLLMGateReason.REVIEW_EXPIRED,
            **common,
        )
    if completed.evidence_as_of > signal_as_of:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
            IntradayLLMGateReason.REVIEW_INPUT_MISMATCH,
            **common,
        )
    if completed.failure_code is not None:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
            IntradayLLMGateReason.REVIEW_FAILED,
            **common,
        )
    if completed.response_model != completed.analyzer_identity.requested_model:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
            IntradayLLMGateReason.MODEL_IDENTITY_MISMATCH,
            **common,
        )
    analysis = completed.analysis
    if analysis.decision is MacroAnalysisDecision.ABSTAIN:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
            IntradayLLMGateReason.REVIEW_ABSTAINED,
            **common,
        )
    coverage, has_references, has_unknown = _macro_evidence_coverage(completed)
    common["evidence_coverage"] = coverage
    if (
        has_unknown
        or not has_references
        or coverage < resolved.minimum_macro_evidence_coverage
    ):
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
            IntradayLLMGateReason.EVIDENCE_INVALID,
            **common,
        )

    macro_score = analysis.macro_impact
    macro_multiplier = (
        resolved.macro_watch_weight_multiplier
        if analysis.decision is MacroAnalysisDecision.WATCH
        else Decimal("1")
    )
    effective_macro_weight = resolved.macro_weight * macro_multiplier
    effective_technical_weight = Decimal("1") - effective_macro_weight
    combined_score = _bounded_score(
        signal.score * effective_technical_weight
        + macro_score * effective_macro_weight
    )
    common["macro_score"] = macro_score
    common["combined_score"] = combined_score
    if macro_score <= resolved.macro_veto_threshold:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.VETO_ENTRY,
            IntradayLLMGateReason.NEGATIVE_VETO,
            **common,
        )
    if combined_score < resolved.entry_combined_score_threshold:
        return _gate_outcome(
            signal,
            IntradayLLMGateAction.DOWNGRADE_TO_WATCH,
            IntradayLLMGateReason.COMBINED_SCORE_BELOW_ENTRY_THRESHOLD,
            **common,
        )
    return _gate_outcome(
        signal,
        IntradayLLMGateAction.ALLOW_ENTRY,
        IntradayLLMGateReason.APPROVED,
        **common,
    )


class IntradayLLMCoordinator:
    """管理后台调用，并且只暴露经日志接受的本地缓存状态。"""

    def __init__(
        self,
        research: MacroResearchService,
        *,
        config: IntradayLLMConfig | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._research = research
        self._config = config or IntradayLLMConfig()
        self._identity = research.analyzer_identity
        if self._config.enabled and self._identity is None:
            raise ValueError("enabled intraday LLM requires an auditable analyzer")
        self._clock = clock
        self._monotonic = monotonic
        self._semaphore = asyncio.Semaphore(self._config.max_concurrency)
        self._in_flight: dict[str, asyncio.Task[None]] = {}
        self._contexts: dict[str, IntradayLLMContext] = {}
        self._observations: dict[str, IntradayLLMReview] = {}
        self._observation_by_context: dict[str, str] = {}
        self._completed: deque[str] = deque()
        self._accepted_by_context: dict[str, JournaledIntradayLLMReview] = {}
        self._accepted_by_review: dict[str, JournaledIntradayLLMReview] = {}
        self._reviews_started = 0
        self._closed = False

    @property
    def config(self) -> IntradayLLMConfig:
        return self._config

    @property
    def analyzer_identity(self) -> AnalyzerAuditIdentity | None:
        return self._identity

    @property
    def reviews_started(self) -> int:
        return self._reviews_started

    @property
    def in_flight_count(self) -> int:
        return len(self._in_flight)

    def restore_session_budget(self, consumed: int) -> None:
        """在开始新调度前恢复持久的单交易时段复核预算。

        运行器依据日志中唯一的 ``SCHEDULED`` 上下文重建 ``consumed``。按已调度
        上下文计数，而不是只数已完成复核，可以保留已到达提供方、但在上一进程
        停止时仍在执行中的调用。
        """

        if isinstance(consumed, bool) or not isinstance(consumed, int) or consumed < 0:
            raise ValueError("consumed review budget must be a non-negative integer")
        if self._in_flight or self._contexts or self._observations:
            raise RuntimeError("session budget must be restored before LLM scheduling")
        if self._reviews_started not in {0, consumed}:
            raise ValueError("session review budget was already restored differently")
        self._reviews_started = consumed

    def create_context(
        self,
        *,
        session_date: date,
        preopen_context_id: str,
        scan_revision: str,
        candidate_scope_sha256: str,
        requested_at: datetime,
        plan: MacroResearchPlan,
    ) -> IntradayLLMContext:
        return IntradayLLMContext.create(
            session_date=session_date,
            preopen_context_id=preopen_context_id,
            scan_revision=scan_revision,
            candidate_scope_sha256=candidate_scope_sha256,
            requested_at=requested_at,
            plan=plan,
            config=self._config,
        )

    def schedule(self, context: IntradayLLMContext) -> IntradayLLMScheduleOutcome:
        """启动后台任务并立即返回，不在此处执行提供方 I/O。"""

        if self._closed:
            return IntradayLLMScheduleOutcome(
                IntradayLLMScheduleStatus.CLOSED,
                context.context_id,
            )
        if not self._config.enabled:
            return IntradayLLMScheduleOutcome(
                IntradayLLMScheduleStatus.DISABLED,
                context.context_id,
            )
        now = _aware_utc(self._clock(), "clock")
        if now >= context.valid_until:
            return IntradayLLMScheduleOutcome(
                IntradayLLMScheduleStatus.CONTEXT_EXPIRED,
                context.context_id,
            )
        if (
            context.config_sha256 != self._config.manifest_sha256
            or context.plan.analyzer_identity != self._identity
        ):
            return IntradayLLMScheduleOutcome(
                IntradayLLMScheduleStatus.IDENTITY_MISMATCH,
                context.context_id,
            )
        accepted = self._accepted_by_context.get(context.context_id)
        if accepted is not None and now < accepted.review.expires_at:
            return IntradayLLMScheduleOutcome(
                IntradayLLMScheduleStatus.CACHE_HIT,
                context.context_id,
                accepted.review.review_id,
            )
        if context.context_id in self._in_flight:
            return IntradayLLMScheduleOutcome(
                IntradayLLMScheduleStatus.IN_FLIGHT,
                context.context_id,
            )
        review_id = self._observation_by_context.get(context.context_id)
        if review_id is not None:
            return IntradayLLMScheduleOutcome(
                IntradayLLMScheduleStatus.COMPLETED_AWAITING_JOURNAL,
                context.context_id,
                review_id,
            )
        maximum_reviews = self._config.maximum_reviews_per_session
        if maximum_reviews is not None and self._reviews_started >= maximum_reviews:
            return IntradayLLMScheduleOutcome(
                IntradayLLMScheduleStatus.SESSION_BUDGET_EXHAUSTED,
                context.context_id,
            )
        loop = asyncio.get_running_loop()
        self._contexts[context.context_id] = context
        task = loop.create_task(
            self._run_review(context),
            name=f"intraday-llm:{context.symbol}:{context.context_id[-10:]}",
        )
        self._in_flight[context.context_id] = task
        self._reviews_started += 1
        return IntradayLLMScheduleOutcome(
            IntradayLLMScheduleStatus.SCHEDULED,
            context.context_id,
        )

    def drain_completed(self) -> tuple[IntradayLLMReview, ...]:
        """返回新观察；观察仅因被取出并不会变为可交易状态。"""

        reviews: list[IntradayLLMReview] = []
        while self._completed:
            review_id = self._completed.popleft()
            reviews.append(self._observations[review_id])
        return tuple(reviews)

    def accept_journaled(
        self,
        review_id: str,
        *,
        accepted_at: datetime,
        journal_event_id: str,
        journal_event_sha256: str,
    ) -> JournaledIntradayLLMReview:
        """仅在调用方持久追加后接纳一条已完成结果。"""

        existing = self._accepted_by_review.get(review_id)
        if existing is not None:
            proposed = JournaledIntradayLLMReview(
                review=existing.review,
                accepted_at=accepted_at,
                journal_event_id=journal_event_id,
                journal_event_sha256=journal_event_sha256,
            )
            if proposed != existing:
                raise ValueError("review was already accepted with different journal metadata")
            return existing
        review = self._observations.get(review_id)
        if review is None:
            raise KeyError("unknown completed intraday LLM review")
        accepted = JournaledIntradayLLMReview(
            review=review,
            accepted_at=accepted_at,
            journal_event_id=journal_event_id,
            journal_event_sha256=journal_event_sha256,
        )
        self._validate_review_identity(review)
        self._accepted_by_context[review.context_id] = accepted
        self._accepted_by_review[review.review_id] = accepted
        return accepted

    def restore_journaled(
        self,
        review: IntradayLLMReview,
        *,
        accepted_at: datetime,
        journal_event_id: str,
        journal_event_sha256: str,
    ) -> JournaledIntradayLLMReview:
        """仅恢复已记入日志、仍有效且身份匹配的复核。"""

        self._validate_review_identity(review)
        accepted = JournaledIntradayLLMReview(
            review=review,
            accepted_at=accepted_at,
            journal_event_id=journal_event_id,
            journal_event_sha256=journal_event_sha256,
        )
        existing = self._accepted_by_review.get(review.review_id)
        if existing is not None and existing != accepted:
            raise ValueError("restored review conflicts with existing journal metadata")
        self._accepted_by_context[review.context_id] = accepted
        self._accepted_by_review[review.review_id] = accepted
        return accepted

    def evaluate_buy(
        self,
        signal: TechnicalSignal,
        *,
        expected_context_id: str | None,
        decision_at: datetime,
    ) -> IntradayLLMGateOutcome:
        """零等待最终门：本方法不执行 await，也不执行 I/O。"""

        accepted = (
            None
            if expected_context_id is None
            else self._accepted_by_context.get(expected_context_id)
        )
        completed_not_journaled = (
            expected_context_id is not None
            and expected_context_id in self._observation_by_context
            and accepted is None
        )
        return evaluate_intraday_llm_buy(
            signal,
            decision_at=decision_at,
            expected_context_id=expected_context_id,
            review=accepted,
            expected_identity=self._identity,
            config=self._config,
            completed_not_journaled=completed_not_journaled,
        )

    async def close(self) -> None:
        """取消未完成的提供方工作；被取消结果永不进入队列。"""

        self._closed = True
        tasks = tuple(self._in_flight.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_review(self, context: IntradayLLMContext) -> None:
        started = self._monotonic()
        failure_code: str | None = None
        run: MacroResearchRun | None = None
        try:
            async with self._semaphore:
                async with asyncio.timeout(
                    self._config.per_review_timeout.total_seconds()
                ):
                    run = await self._research.execute(context.plan)
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            failure_code = "LLM_REVIEW_TIMEOUT"
        except Exception:
            # 提供方或请求正文以及异常字符串绝不能越过审计边界。
            # 身份不匹配已由 schedule 预先检查。
            failure_code = "LLM_COORDINATOR_FAILED"
        finally:
            self._in_flight.pop(context.context_id, None)

        completed_at = max(
            context.requested_at,
            _aware_utc(self._clock(), "clock"),
        )
        latency_ms = max(0, int(round((self._monotonic() - started) * 1_000)))
        if run is not None:
            analysis = run.analysis
            failure_code = run.failure_code
        else:
            assert failure_code is not None
            analysis = _failed_analysis(context.plan, failure_code)
        review_id = _review_id(
            context_id=context.context_id,
            analysis=analysis,
            completed_at=completed_at,
            failure_code=failure_code,
        )
        identity = context.plan.analyzer_identity
        assert identity is not None
        review = IntradayLLMReview(
            review_id=review_id,
            context_id=context.context_id,
            session_date=context.session_date,
            symbol=context.symbol,
            preopen_context_id=context.preopen_context_id,
            scan_revision=context.scan_revision,
            candidate_scope_sha256=context.candidate_scope_sha256,
            analysis=analysis,
            references=context.plan.references,
            request_sha256=context.plan.request_sha256,
            evidence_pack_sha256=context.plan.evidence_pack_sha256,
            plan_manifest_sha256=context.plan.manifest_sha256,
            analyzer_identity=identity,
            evidence_as_of=context.evidence_as_of,
            requested_at=context.requested_at,
            completed_at=completed_at,
            expires_at=context.valid_until,
            response_model=analysis.model_version,
            latency_ms=latency_ms,
            failure_code=failure_code,
            baseline_analysis=(None if run is None else run.baseline_analysis),
            adversarial_analysis=(None if run is None else run.adversarial_analysis),
            selected_track=None if run is None else run.selected_track,
            dual_audit_record_sha256=(
                None if run is None else run.dual_audit_record_sha256
            ),
        )
        self._observations[review.review_id] = review
        self._observation_by_context[review.context_id] = review.review_id
        self._completed.append(review.review_id)

    def _validate_review_identity(self, review: IntradayLLMReview) -> None:
        if self._identity is None or review.analyzer_identity != self._identity:
            raise ValueError("intraday LLM review model/prompt identity mismatch")
        context = self._contexts.get(review.context_id)
        if context is not None and (
            review.request_sha256 != context.plan.request_sha256
            or review.evidence_pack_sha256 != context.plan.evidence_pack_sha256
            or review.plan_manifest_sha256 != context.plan.manifest_sha256
            or review.candidate_scope_sha256 != context.candidate_scope_sha256
        ):
            raise ValueError("intraday LLM review input hashes do not match context")


def intraday_llm_review_document(review: IntradayLLMReview) -> dict[str, object]:
    """返回安全审计载荷；其中不含提示词、正文和推理内容。"""

    analysis = review.analysis
    return {
        "analysis": {
            "analysis_id": analysis.analysis_id,
            "as_of": analysis.as_of,
            "claims": [
                {
                    "contradictions": list(item.contradictions),
                    "evidence_ids": list(item.evidence_ids),
                    "text": item.text,
                }
                for item in analysis.claims
            ],
            "data_gaps": list(analysis.data_gaps),
            "decision": analysis.decision,
            "invalidation_conditions": list(analysis.invalidation_conditions),
            "macro_impact": analysis.macro_impact,
            "model_version": analysis.model_version,
            "refusal_reason": analysis.refusal_reason,
            "regime": analysis.regime,
            "reported_confidence": analysis.reported_confidence,
            "scenarios": [
                {
                    "drivers": list(item.drivers),
                    "evidence_ids": list(item.evidence_ids),
                    "name": item.name,
                    "probability": item.probability,
                }
                for item in analysis.scenarios
            ],
            "technical_alignment": analysis.technical_alignment,
            "uncertainties": list(analysis.uncertainties),
        },
        "analyzer_identity": {
            "adapter_version": review.analyzer_identity.adapter_version,
            "identity_manifest_sha256": review.analyzer_identity.manifest_sha256,
            "prompt_schema_sha256": review.analyzer_identity.prompt_schema_sha256,
            "prompt_version": review.analyzer_identity.prompt_version,
            "provider_id": review.analyzer_identity.provider_id,
            "requested_model": review.analyzer_identity.requested_model,
        },
        "candidate_scope_sha256": review.candidate_scope_sha256,
        "completed_at": review.completed_at,
        "context_id": review.context_id,
        "evidence_as_of": review.evidence_as_of,
        "evidence_pack_sha256": review.evidence_pack_sha256,
        "expires_at": review.expires_at,
        "failure_code": review.failure_code,
        "dual_track": (
            None
            if review.baseline_analysis is None or review.adversarial_analysis is None
            else {
                "selected_track": review.selected_track,
                "audit_record_sha256": review.dual_audit_record_sha256,
                "baseline_analysis": _macro_analysis_document(
                    review.baseline_analysis
                ),
                "adversarial_analysis": _macro_analysis_document(
                    review.adversarial_analysis
                ),
            }
        ),
        "latency_ms": review.latency_ms,
        "plan_manifest_sha256": review.plan_manifest_sha256,
        "preopen_context_id": review.preopen_context_id,
        "references": [
            {
                "canonical_url": item.canonical_url,
                "evidence_id": item.evidence_id,
                "first_seen_at": item.first_seen_at,
                "published_at": item.published_at,
                "source_tier": item.source_tier,
                "title": item.title,
            }
            for item in review.references
        ],
        "request_sha256": review.request_sha256,
        "requested_at": review.requested_at,
        "response_model": review.response_model,
        "review_id": review.review_id,
        "scan_revision": review.scan_revision,
        "session_date": review.session_date,
        "symbol": review.symbol,
    }


def _macro_analysis_document(analysis: MacroAnalysis) -> dict[str, object]:
    return {
        "analysis_id": analysis.analysis_id,
        "as_of": analysis.as_of,
        "claims": [
            {
                "contradictions": list(item.contradictions),
                "evidence_ids": list(item.evidence_ids),
                "text": item.text,
            }
            for item in analysis.claims
        ],
        "data_gaps": list(analysis.data_gaps),
        "decision": analysis.decision,
        "invalidation_conditions": list(analysis.invalidation_conditions),
        "macro_impact": analysis.macro_impact,
        "model_version": analysis.model_version,
        "refusal_reason": analysis.refusal_reason,
        "regime": analysis.regime,
        "reported_confidence": analysis.reported_confidence,
        "scenarios": [
            {
                "drivers": list(item.drivers),
                "evidence_ids": list(item.evidence_ids),
                "name": item.name,
                "probability": item.probability,
            }
            for item in analysis.scenarios
        ],
        "technical_alignment": analysis.technical_alignment,
        "uncertainties": list(analysis.uncertainties),
    }


def intraday_llm_document_sha256(document: Mapping[str, object]) -> str:
    """为确定性审计文档计算哈希，并拒绝非常规值。"""

    normalized = _normalize_json(document)
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _context_id(
    *,
    session_date: date,
    symbol: str,
    preopen_context_id: str,
    scan_revision: str,
    candidate_scope_sha256: str,
    evidence_as_of: datetime,
    valid_until: datetime,
    plan_manifest_sha256: str,
    config_sha256: str,
) -> str:
    digest = intraday_llm_document_sha256(
        {
            "candidate_scope_sha256": candidate_scope_sha256,
            "config_sha256": config_sha256,
            "evidence_as_of": evidence_as_of,
            "plan_manifest_sha256": plan_manifest_sha256,
            "preopen_context_id": preopen_context_id,
            "scan_revision": scan_revision,
            "schema_version": 1,
            "session_date": session_date,
            "symbol": symbol,
            "valid_until": valid_until,
        }
    )
    return "intraday-llm-context-" + digest[:40]


def _review_id(
    *,
    context_id: str,
    analysis: MacroAnalysis,
    completed_at: datetime,
    failure_code: str | None,
) -> str:
    digest = intraday_llm_document_sha256(
        {
            "analysis_id": analysis.analysis_id,
            "completed_at": completed_at,
            "context_id": context_id,
            "failure_code": failure_code,
            "macro_impact": analysis.macro_impact,
            "model_version": analysis.model_version,
            "schema_version": 1,
        }
    )
    return "intraday-llm-review-" + digest[:40]


def _failed_analysis(plan: MacroResearchPlan, failure_code: str) -> MacroAnalysis:
    code = _stable_failure_code(failure_code)
    return MacroAnalysis(
        analysis_id=plan.request.analysis_id,
        as_of=plan.request.as_of,
        decision=MacroAnalysisDecision.ABSTAIN,
        regime="unknown",
        technical_alignment=Decimal("0"),
        macro_impact=Decimal("0"),
        scenarios=(),
        claims=(),
        uncertainties=(code,),
        data_gaps=("INTRADAY_LLM_REVIEW_UNAVAILABLE",),
        invalidation_conditions=(),
        reported_confidence="UNCALIBRATED",
        refusal_reason=code,
        model_version="local-intraday-fail-closed@1",
    )


def _macro_evidence_coverage(
    review: IntradayLLMReview,
) -> tuple[Decimal, bool, bool]:
    referenced = {
        evidence_id
        for claim in review.analysis.claims
        for evidence_id in claim.evidence_ids
    }
    referenced.update(
        evidence_id
        for scenario in review.analysis.scenarios
        for evidence_id in scenario.evidence_ids
    )
    available = {item.evidence_id for item in review.references}
    if not available:
        return Decimal("0"), bool(referenced), bool(referenced)
    retained = referenced & available
    return (
        Decimal(len(retained)) / Decimal(len(available)),
        bool(referenced),
        bool(referenced - available),
    )


def _gate_review_fields(review: JournaledIntradayLLMReview) -> dict[str, object]:
    completed = review.review
    baseline = completed.baseline_analysis
    adversarial = completed.adversarial_analysis
    return {
        "accepted_at": review.accepted_at,
        "expires_at": completed.expires_at,
        "failure_code": completed.failure_code,
        "prompt_schema_sha256": completed.analyzer_identity.prompt_schema_sha256,
        "requested_model": completed.analyzer_identity.requested_model,
        "response_model": completed.response_model,
        "review_id": completed.review_id,
        "baseline_decision": None if baseline is None else baseline.decision,
        "baseline_macro_score": None if baseline is None else baseline.macro_impact,
        "baseline_model": None if baseline is None else baseline.model_version,
        "adversarial_decision": (
            None if adversarial is None else adversarial.decision
        ),
        "adversarial_macro_score": (
            None if adversarial is None else adversarial.macro_impact
        ),
        "adversarial_model": (
            None if adversarial is None else adversarial.model_version
        ),
        "selected_track": completed.selected_track,
        "dual_audit_record_sha256": completed.dual_audit_record_sha256,
    }


def _gate_outcome(
    signal: TechnicalSignal,
    action: IntradayLLMGateAction,
    reason: IntradayLLMGateReason,
    **values: object,
) -> IntradayLLMGateOutcome:
    return IntradayLLMGateOutcome(
        action=action,
        reason=reason,
        technical_decision=signal.decision,
        technical_score=signal.score,
        review_id=cast(str | None, values.get("review_id")),
        macro_score=cast(Decimal | None, values.get("macro_score")),
        combined_score=cast(Decimal | None, values.get("combined_score")),
        evidence_coverage=cast(Decimal | None, values.get("evidence_coverage")),
        accepted_at=cast(datetime | None, values.get("accepted_at")),
        expires_at=cast(datetime | None, values.get("expires_at")),
        requested_model=cast(str | None, values.get("requested_model")),
        response_model=cast(str | None, values.get("response_model")),
        prompt_schema_sha256=cast(
            str | None,
            values.get("prompt_schema_sha256"),
        ),
        failure_code=cast(str | None, values.get("failure_code")),
        baseline_decision=cast(
            MacroAnalysisDecision | None,
            values.get("baseline_decision"),
        ),
        baseline_macro_score=cast(
            Decimal | None,
            values.get("baseline_macro_score"),
        ),
        baseline_model=cast(str | None, values.get("baseline_model")),
        adversarial_decision=cast(
            MacroAnalysisDecision | None,
            values.get("adversarial_decision"),
        ),
        adversarial_macro_score=cast(
            Decimal | None,
            values.get("adversarial_macro_score"),
        ),
        adversarial_model=cast(str | None, values.get("adversarial_model")),
        selected_track=cast(str | None, values.get("selected_track")),
        dual_audit_record_sha256=cast(
            str | None,
            values.get("dual_audit_record_sha256"),
        ),
    )


def _stable_failure_code(value: str) -> str:
    normalized = value.strip().upper()
    if not _FAILURE_CODE.fullmatch(normalized):
        raise ValueError("failure code must use stable uppercase identifier form")
    return normalized


def _bounded_score(value: Decimal) -> Decimal:
    return max(Decimal("-1"), min(Decimal("1"), value))


def _canonical_symbol(value: str) -> str:
    normalized = value.strip().upper()
    if not _SYMBOL.fullmatch(normalized):
        raise ValueError("symbol must use canonical 000001.SZ form")
    return normalized


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _normalize_json(value: object) -> JSONValue:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("audit documents must not contain non-finite floats")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("audit documents must not contain non-finite decimals")
        return format(value, "f")
    if isinstance(value, datetime):
        return _aware_utc(value, "document datetime").isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return _normalize_json(value.value)
    if isinstance(value, Mapping):
        normalized: dict[str, JSONValue] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError("audit document keys must be non-empty strings")
            normalized[key] = _normalize_json(item)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_json(item) for item in value]
    raise ValueError("audit document contains an unsupported value type")
