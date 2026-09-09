from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.analysis.schemas import (
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
    MacroClaim,
)
from gribuki_trade.domain.events import NormalizedEvent, SourceTier
from gribuki_trade.domain.recommendations import (
    RecommendationDecision,
    RecommendationHorizon,
)
from gribuki_trade.features.technical import TechnicalSignal
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
from gribuki_trade.services.ashare_intraday_llm import (
    IntradayLLMConfig,
    IntradayLLMCoordinator,
    IntradayLLMGateAction,
    IntradayLLMGateReason,
    IntradayLLMScheduleStatus,
    evaluate_intraday_llm_buy,
    intraday_llm_document_sha256,
    intraday_llm_review_document,
)
from gribuki_trade.services.macro_research import MacroResearchService

NOW = datetime(2026, 8, 14, 2, 15, tzinfo=UTC)
SESSION_DATE = date(2026, 8, 14)
SYMBOL = "600000.SH"


def _identity(*, model: str = "deepseek-test") -> AnalyzerAuditIdentity:
    return AnalyzerAuditIdentity(
        provider_id="test.provider",
        requested_model=model,
        adapter_version="fake-adapter@1",
        prompt_version="fake-prompt@1",
        prompt_schema_sha256="a" * 64,
    )


class FakeAuditableAnalyzer:
    def __init__(
        self,
        *,
        impact: Decimal = Decimal("0.2"),
        decision: MacroAnalysisDecision = MacroAnalysisDecision.PUBLISH,
        identity: AnalyzerAuditIdentity | None = None,
        release: asyncio.Event | None = None,
        omit_claims: bool = False,
        fail_message: str | None = None,
    ) -> None:
        self.audit_identity = identity or _identity()
        self.impact = impact
        self.decision = decision
        self.release = release
        self.omit_claims = omit_claims
        self.fail_message = fail_message
        self.calls = 0
        self.started = asyncio.Event()

    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        self.calls += 1
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.fail_message is not None:
            raise RuntimeError(self.fail_message)
        evidence_id = request.evidence[0].evidence_id
        claims = (
            ()
            if self.omit_claims
            else (MacroClaim("bounded test claim", (evidence_id,), ()),)
        )
        return MacroAnalysis(
            analysis_id=request.analysis_id,
            as_of=request.as_of,
            decision=self.decision,
            regime="test",
            technical_alignment=Decimal("0"),
            macro_impact=self.impact,
            scenarios=(),
            claims=claims,
            uncertainties=(),
            data_gaps=(),
            invalidation_conditions=(),
            reported_confidence="UNCALIBRATED",
            refusal_reason="",
            model_version=self.audit_identity.requested_model,
        )


def _event(*, summary: str = "bounded summary") -> NormalizedEvent:
    return NormalizedEvent(
        source_id="official.test",
        canonical_url="https://example.test/company-event",
        title="company event",
        summary=summary,
        event_type="company_announcement",
        source_tier=SourceTier.OFFICIAL,
        first_seen_at=NOW - timedelta(minutes=2),
        retrieved_at=NOW - timedelta(minutes=2),
        available_at=NOW - timedelta(minutes=2),
        published_at=NOW - timedelta(minutes=3),
        external_id="company-event",
        entities=("600000",),
    )


def _plan(
    analyzer: FakeAuditableAnalyzer,
    *,
    technical_summary: tuple[str, ...] = ("MOMENTUM_EXPANSION",),
    event: NormalizedEvent | None = None,
):
    service = MacroResearchService(analyzer)
    plan = service.prepare(
        symbol=SYMBOL,
        as_of=NOW,
        horizon="INTRADAY_BACKGROUND_REVIEW",
        technical_summary=technical_summary,
        events=(event or _event(),),
    )
    return service, plan


def _signal(
    *,
    decision: RecommendationDecision = RecommendationDecision.ENTER_CANDIDATE,
    as_of: datetime = NOW + timedelta(minutes=1),
    score: Decimal = Decimal("1"),
) -> TechnicalSignal:
    return TechnicalSignal(
        symbol=SYMBOL,
        as_of=as_of,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        decision=decision,
        score=score,
        reference_price=Decimal("10"),
        invalidation_price=Decimal("9.8"),
        reason_codes=("CLOSED_BAR_BREAKOUT", "VOLUME_CONFIRMED"),
        data_age=timedelta(0),
        strategy_version="test-technical@1",
        metrics=(),
    )


def _context(coordinator: IntradayLLMCoordinator, plan):
    return coordinator.create_context(
        session_date=SESSION_DATE,
        preopen_context_id="preopen-context-1",
        scan_revision="scan-1",
        candidate_scope_sha256=intraday_llm_document_sha256(
            {"candidate_class": "MOMENTUM_EXPANSION", "symbol": SYMBOL}
        ),
        requested_at=NOW,
        plan=plan,
    )


async def _wait_for_review(
    coordinator: IntradayLLMCoordinator,
    *,
    attempts: int = 100,
):
    for _ in range(attempts):
        reviews = coordinator.drain_completed()
        if reviews:
            return reviews[0]
        await asyncio.sleep(0)
    raise AssertionError("background LLM review did not complete")


def test_prepare_is_network_free_and_hashes_all_decision_inputs() -> None:
    analyzer = FakeAuditableAnalyzer()
    service, first = _plan(analyzer)
    second = service.prepare(
        symbol=SYMBOL,
        as_of=NOW,
        horizon="INTRADAY_BACKGROUND_REVIEW",
        technical_summary=("DIFFERENT_TECHNICAL_SCOPE",),
        events=(_event(),),
    )
    revised = service.prepare(
        symbol=SYMBOL,
        as_of=NOW,
        horizon="INTRADAY_BACKGROUND_REVIEW",
        technical_summary=("MOMENTUM_EXPANSION",),
        events=(_event(summary="revised bounded summary"),),
    )
    other_service, other_model = _plan(
        FakeAuditableAnalyzer(identity=_identity(model="different-model"))
    )

    assert analyzer.calls == 0
    assert first.request.analysis_id != second.request.analysis_id
    assert first.request_sha256 != second.request_sha256
    assert first.evidence_pack_sha256 != revised.evidence_pack_sha256
    assert first.request_sha256 == other_model.request_sha256
    assert first.manifest_sha256 != other_model.manifest_sha256
    with pytest.raises(ValueError, match="analyzer identity mismatch"):
        asyncio.run(other_service.execute(first))


def test_background_result_is_not_tradable_until_exact_journal_acceptance() -> None:
    async def scenario() -> None:
        release = asyncio.Event()
        analyzer = FakeAuditableAnalyzer(release=release)
        service, plan = _plan(analyzer)
        coordinator = IntradayLLMCoordinator(service, clock=lambda: NOW)
        context = _context(coordinator, plan)

        outcome = coordinator.schedule(context)
        assert outcome.status is IntradayLLMScheduleStatus.SCHEDULED
        await analyzer.started.wait()
        pending_gate = coordinator.evaluate_buy(
            _signal(),
            expected_context_id=context.context_id,
            decision_at=NOW + timedelta(minutes=1),
        )
        assert pending_gate.reason is IntradayLLMGateReason.REVIEW_NOT_READY

        release.set()
        review = await _wait_for_review(coordinator)
        unjournaled_gate = coordinator.evaluate_buy(
            _signal(),
            expected_context_id=context.context_id,
            decision_at=NOW + timedelta(minutes=1),
        )
        assert unjournaled_gate.reason is IntradayLLMGateReason.REVIEW_NOT_JOURNALED
        assert not unjournaled_gate.approved

        coordinator.accept_journaled(
            review.review_id,
            accepted_at=review.completed_at,
            journal_event_id="pde-test-review",
            journal_event_sha256="f" * 64,
        )
        approved = coordinator.evaluate_buy(
            _signal(),
            expected_context_id=context.context_id,
            decision_at=NOW + timedelta(minutes=1),
        )
        assert approved.approved
        assert approved.reason is IntradayLLMGateReason.APPROVED
        assert approved.combined_score == Decimal("0.80")
        await coordinator.close()

    asyncio.run(scenario())


async def _accepted_review(
    *,
    impact: Decimal = Decimal("0.2"),
    macro_decision: MacroAnalysisDecision = MacroAnalysisDecision.PUBLISH,
    omit_claims: bool = False,
    identity: AnalyzerAuditIdentity | None = None,
    config: IntradayLLMConfig | None = None,
):
    analyzer = FakeAuditableAnalyzer(
        impact=impact,
        decision=macro_decision,
        omit_claims=omit_claims,
        identity=identity,
    )
    service, plan = _plan(analyzer)
    coordinator = IntradayLLMCoordinator(service, config=config, clock=lambda: NOW)
    context = _context(coordinator, plan)
    assert coordinator.schedule(context).status is IntradayLLMScheduleStatus.SCHEDULED
    review = await _wait_for_review(coordinator)
    accepted = coordinator.accept_journaled(
        review.review_id,
        accepted_at=review.completed_at,
        journal_event_id="pde-test-review",
        journal_event_sha256="e" * 64,
    )
    return coordinator, context, accepted


def test_llm_can_veto_or_downgrade_but_never_create_a_technical_entry() -> None:
    async def scenario() -> None:
        veto_coordinator, veto_context, accepted = await _accepted_review(
            impact=Decimal("-0.80")
        )
        veto = veto_coordinator.evaluate_buy(
            _signal(),
            expected_context_id=veto_context.context_id,
            decision_at=NOW + timedelta(minutes=1),
        )
        assert veto.action is IntradayLLMGateAction.VETO_ENTRY
        assert veto.reason is IntradayLLMGateReason.NEGATIVE_VETO

        downgrade_coordinator, downgrade_context, _ = await _accepted_review(
            impact=Decimal("-0.40")
        )
        downgrade = downgrade_coordinator.evaluate_buy(
            _signal(),
            expected_context_id=downgrade_context.context_id,
            decision_at=NOW + timedelta(minutes=1),
        )
        assert downgrade.action is IntradayLLMGateAction.DOWNGRADE_TO_WATCH
        assert (
            downgrade.reason
            is IntradayLLMGateReason.COMBINED_SCORE_BELOW_ENTRY_THRESHOLD
        )
        assert downgrade.combined_score == Decimal("0.65")

        watch = evaluate_intraday_llm_buy(
            _signal(decision=RecommendationDecision.WATCH, score=Decimal("0.5")),
            decision_at=NOW + timedelta(minutes=1),
            expected_context_id=veto_context.context_id,
            review=accepted,
            expected_identity=veto_coordinator.analyzer_identity,
        )
        assert watch.action is IntradayLLMGateAction.NOT_APPLICABLE
        assert watch.reason is IntradayLLMGateReason.TECHNICAL_NOT_ENTER

        reduce = evaluate_intraday_llm_buy(
            _signal(decision=RecommendationDecision.REDUCE, score=Decimal("-0.6")),
            decision_at=NOW + timedelta(minutes=1),
            expected_context_id=None,
            review=None,
            expected_identity=None,
        )
        assert reduce.action is IntradayLLMGateAction.NOT_APPLICABLE
        assert reduce.reason is IntradayLLMGateReason.REDUCE_NOT_APPLICABLE
        await veto_coordinator.close()
        await downgrade_coordinator.close()

    asyncio.run(scenario())


def test_gate_enforces_exact_ttl_pit_model_prompt_and_evidence_boundaries() -> None:
    async def scenario() -> None:
        coordinator, context, accepted = await _accepted_review()
        expired = evaluate_intraday_llm_buy(
            _signal(as_of=NOW + timedelta(minutes=20)),
            decision_at=NOW + timedelta(minutes=20),
            expected_context_id=context.context_id,
            review=accepted,
            expected_identity=coordinator.analyzer_identity,
        )
        assert expired.reason is IntradayLLMGateReason.REVIEW_EXPIRED

        learned_late = evaluate_intraday_llm_buy(
            _signal(as_of=NOW - timedelta(seconds=1)),
            decision_at=NOW,
            expected_context_id=context.context_id,
            review=accepted,
            expected_identity=coordinator.analyzer_identity,
        )
        assert learned_late.reason is IntradayLLMGateReason.REVIEW_KNOWN_AFTER_SIGNAL

        wrong_identity = evaluate_intraday_llm_buy(
            _signal(),
            decision_at=NOW + timedelta(minutes=1),
            expected_context_id=context.context_id,
            review=accepted,
            expected_identity=_identity(model="other-model"),
        )
        assert wrong_identity.reason is IntradayLLMGateReason.MODEL_IDENTITY_MISMATCH

        wrong_input = evaluate_intraday_llm_buy(
            _signal(),
            decision_at=NOW + timedelta(minutes=1),
            expected_context_id="intraday-llm-context-wrong",
            review=accepted,
            expected_identity=coordinator.analyzer_identity,
        )
        assert wrong_input.reason is IntradayLLMGateReason.REVIEW_INPUT_MISMATCH

        evidence_coordinator, evidence_context, _ = await _accepted_review(
            macro_decision=MacroAnalysisDecision.WATCH,
            omit_claims=True,
        )
        invalid_evidence = evidence_coordinator.evaluate_buy(
            _signal(),
            expected_context_id=evidence_context.context_id,
            decision_at=NOW + timedelta(minutes=1),
        )
        assert invalid_evidence.reason is IntradayLLMGateReason.EVIDENCE_INVALID
        await coordinator.close()
        await evidence_coordinator.close()

    asyncio.run(scenario())


def test_timeout_is_sanitized_and_never_leaks_provider_details() -> None:
    async def scenario() -> None:
        never_release = asyncio.Event()
        analyzer = FakeAuditableAnalyzer(
            release=never_release,
            fail_message="secret provider body",
        )
        service, plan = _plan(analyzer)
        config = replace(
            IntradayLLMConfig(),
            per_review_timeout=timedelta(milliseconds=1),
        )
        coordinator = IntradayLLMCoordinator(
            service,
            config=config,
            clock=lambda: NOW,
        )
        context = _context(coordinator, plan)
        assert coordinator.schedule(context).status is IntradayLLMScheduleStatus.SCHEDULED
        review = await _wait_for_review(coordinator, attempts=1_000)

        assert review.failure_code == "LLM_REVIEW_TIMEOUT"
        assert "secret provider body" not in repr(review)
        assert "secret provider body" not in repr(intraday_llm_review_document(review))
        await coordinator.close()

    asyncio.run(scenario())


def test_journal_acceptance_is_idempotent_but_conflicting_metadata_is_rejected() -> None:
    async def scenario() -> None:
        coordinator, _, accepted = await _accepted_review()
        same = coordinator.accept_journaled(
            accepted.review.review_id,
            accepted_at=accepted.accepted_at,
            journal_event_id=accepted.journal_event_id,
            journal_event_sha256=accepted.journal_event_sha256,
        )
        assert same is accepted
        with pytest.raises(ValueError, match="different journal metadata"):
            coordinator.accept_journaled(
                accepted.review.review_id,
                accepted_at=accepted.accepted_at,
                journal_event_id="pde-different",
                journal_event_sha256=accepted.journal_event_sha256,
            )
        await coordinator.close()

    asyncio.run(scenario())


def test_dual_track_review_document_can_be_restored_without_losing_audit_branches() -> None:
    """热恢复必须保留 baseline/对抗两支，不能悄悄退回单分析器语义。"""

    from gribuki_trade.services.ashare_paper_day import _llm_review_from_document

    async def scenario() -> None:
        coordinator, _, accepted = await _accepted_review()
        review = accepted.review
        dual_review = replace(
            review,
            baseline_analysis=review.analysis,
            adversarial_analysis=review.analysis,
            selected_track="ADVERSARIAL",
            dual_audit_record_sha256="f" * 64,
        )

        serialized = json.loads(
            json.dumps(intraday_llm_review_document(dual_review), default=str)
        )
        restored = _llm_review_from_document(serialized)

        assert restored == dual_review
        assert restored.baseline_analysis == review.analysis
        assert restored.adversarial_analysis == review.analysis
        assert restored.selected_track == "ADVERSARIAL"
        assert restored.dual_audit_record_sha256 == "f" * 64

        gate = evaluate_intraday_llm_buy(
            _signal(),
            decision_at=NOW + timedelta(minutes=1),
            expected_context_id=dual_review.context_id,
            review=replace(accepted, review=dual_review),
            expected_identity=dual_review.analyzer_identity,
        )
        assert gate.baseline_decision is MacroAnalysisDecision.PUBLISH
        assert gate.baseline_macro_score == review.analysis.macro_impact
        assert gate.adversarial_decision is MacroAnalysisDecision.PUBLISH
        assert gate.adversarial_macro_score == review.analysis.macro_impact
        assert gate.selected_track == "ADVERSARIAL"
        assert gate.dual_audit_record_sha256 == "f" * 64
        await coordinator.close()

    asyncio.run(scenario())


def test_restored_session_budget_counts_incomplete_calls_and_is_fail_closed() -> None:
    async def scenario() -> None:
        analyzer = FakeAuditableAnalyzer()
        service, plan = _plan(analyzer)
        config = replace(IntradayLLMConfig(), maximum_reviews_per_session=2)
        coordinator = IntradayLLMCoordinator(service, config=config, clock=lambda: NOW)

        coordinator.restore_session_budget(2)
        assert coordinator.reviews_started == 2
        context = _context(coordinator, plan)
        assert (
            coordinator.schedule(context).status
            is IntradayLLMScheduleStatus.SESSION_BUDGET_EXHAUSTED
        )
        assert analyzer.calls == 0
        coordinator.restore_session_budget(2)
        with pytest.raises(ValueError, match="already restored differently"):
            coordinator.restore_session_budget(1)
        await coordinator.close()

    asyncio.run(scenario())


def test_unlimited_session_restores_usage_without_blocking_new_reviews() -> None:
    async def scenario() -> None:
        analyzer = FakeAuditableAnalyzer()
        service, plan = _plan(analyzer)
        coordinator = IntradayLLMCoordinator(service, clock=lambda: NOW)

        assert coordinator.config.maximum_reviews_per_session is None
        coordinator.restore_session_budget(10_000)
        assert coordinator.reviews_started == 10_000
        outcome = coordinator.schedule(_context(coordinator, plan))
        assert outcome.status is IntradayLLMScheduleStatus.SCHEDULED
        assert coordinator.reviews_started == 10_001
        await coordinator.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("value", [0, -1, True, "unlimited"])
def test_session_review_limit_rejects_non_positive_or_non_integer_values(
    value: object,
) -> None:
    with pytest.raises(ValueError, match="positive integer or None"):
        IntradayLLMConfig(maximum_reviews_per_session=value)  # type: ignore[arg-type]
