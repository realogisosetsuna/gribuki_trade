"""A 股盘中 LLM 门控的不可变配置、上下文、复核和结果模型。

这些模型只校验时点、身份、哈希和门控结果结构，不启动后台任务、不调用
模型服务，也不写交易状态；网络协调和门控算法留在 ``ashare_intraday_llm.py``。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from gribuki_trade.analysis.schemas import MacroAnalysis, MacroAnalysisDecision
from gribuki_trade.domain.recommendations import EvidenceReference, RecommendationDecision
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
from gribuki_trade.services.ashare import ashare_intraday_llm_policy as _policy
from gribuki_trade.services.ashare.ashare_intraday_llm_policy import (
    aware_utc as _aware_utc,
)
from gribuki_trade.services.ashare.ashare_intraday_llm_policy import (
    canonical_symbol as _canonical_symbol,
)
from gribuki_trade.services.ashare.ashare_intraday_llm_policy import (
    context_id as _context_id,
)
from gribuki_trade.services.ashare.ashare_intraday_llm_policy import (
    review_id as _review_id,
)
from gribuki_trade.services.ashare.ashare_intraday_llm_policy import (
    stable_failure_code as _stable_failure_code,
)
from gribuki_trade.services.ashare.ashare_intraday_llm_serialization import (
    intraday_llm_document_sha256,
)
from gribuki_trade.services.macro_research import MacroResearchPlan

_SHA256 = _policy._SHA256

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


