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
import time
from collections import deque
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast

from gribuki_trade.analysis.schemas import MacroAnalysis, MacroAnalysisDecision
from gribuki_trade.domain.recommendations import (
    RecommendationDecision,
)
from gribuki_trade.features.technical import TechnicalSignal
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
from gribuki_trade.services.ashare import ashare_intraday_llm_policy as _policy
from gribuki_trade.services.ashare.ashare_intraday_llm_models import (
    IntradayLLMConfig,
    IntradayLLMContext,
    IntradayLLMGateAction,
    IntradayLLMGateOutcome,
    IntradayLLMGateReason,
    IntradayLLMReview,
    IntradayLLMScheduleOutcome,
    IntradayLLMScheduleStatus,
    JournaledIntradayLLMReview,
)

__all__ = (
    "IntradayLLMConfig",
    "IntradayLLMContext",
    "IntradayLLMGateAction",
    "IntradayLLMGateOutcome",
    "IntradayLLMGateReason",
    "IntradayLLMReview",
    "IntradayLLMScheduleOutcome",
    "IntradayLLMScheduleStatus",
    "JournaledIntradayLLMReview",
    "evaluate_intraday_llm_buy",
    "IntradayLLMCoordinator",
    "intraday_llm_document_sha256",
    "intraday_llm_review_document",
)
from gribuki_trade.services.ashare.ashare_intraday_llm_policy import (
    aware_utc as _aware_utc,
)
from gribuki_trade.services.ashare.ashare_intraday_llm_policy import (
    bounded_score as _bounded_score,
)
from gribuki_trade.services.ashare.ashare_intraday_llm_policy import (
    review_id as _review_id,
)
from gribuki_trade.services.ashare.ashare_intraday_llm_policy import (
    stable_failure_code as _stable_failure_code,
)
from gribuki_trade.services.ashare.ashare_intraday_llm_serialization import (
    JSONScalar,  # noqa: F401 - historical type alias
    JSONValue,  # noqa: F401 - historical type alias
    _macro_analysis_document,  # noqa: F401 - historical private helper
    _normalize_json,  # noqa: F401 - historical private helper
)
from gribuki_trade.services.ashare.ashare_intraday_llm_serialization import (
    intraday_llm_document_sha256 as intraday_llm_document_sha256,
)
from gribuki_trade.services.ashare.ashare_intraday_llm_serialization import (
    intraday_llm_review_document as intraday_llm_review_document,
)
from gribuki_trade.services.macro_research import (
    MacroResearchPlan,
    MacroResearchRun,
    MacroResearchService,
)

_SHA256 = _policy._SHA256



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
