from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.analysis.schemas import (
    EvidenceItem,
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
    MacroClaim,
)
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
from gribuki_trade.services.adversarial_macro import (
    AdversarialFeatureMode,
    AdversarialMacroAnalyzer,
    AdversarialMacroConfig,
    AdversarialMacroDepth,
    AdversarialMacroRole,
    AdversarialTermination,
    FeatureFlaggedAdversarialMacroAnalyzer,
)

NOW = datetime(2026, 8, 14, 7, 0, tzinfo=UTC)
BASE_IDENTITY = AnalyzerAuditIdentity(
    provider_id="fake.responses",
    requested_model="fake-model@1",
    adapter_version="fake-adapter@1",
    prompt_version="fake-prompt@1",
    prompt_schema_sha256="1" * 64,
)


def _request() -> MacroAnalysisRequest:
    return MacroAnalysisRequest(
        analysis_id="original-analysis",
        symbol="600000.SH",
        as_of=NOW,
        horizon="next session",
        technical_summary=("deterministic technical input",),
        evidence=(
            EvidenceItem(
                evidence_id="a" * 64,
                publisher="official.test",
                source_tier=0,
                published_at=NOW - timedelta(minutes=5),
                first_seen_at=NOW - timedelta(minutes=4),
                title="official evidence",
                excerpt="bounded evidence",
                canonical_url="https://example.test/evidence",
                content_hash="b" * 64,
            ),
        ),
    )


def _role(request: MacroAnalysisRequest) -> AdversarialMacroRole | None:
    prefix = "ADVERSARIAL_ROLE="
    for item in request.technical_summary:
        if item.startswith(prefix):
            return AdversarialMacroRole(item.removeprefix(prefix))
    return None


def _round_number(request: MacroAnalysisRequest) -> int | None:
    prefix = "ADVERSARIAL_ROUND="
    for item in request.technical_summary:
        if item.startswith(prefix):
            return int(item.removeprefix(prefix))
    return None


class FakeAuditableAnalyzer:
    def __init__(
        self,
        *,
        fail_role: AdversarialMacroRole | None = None,
        abstain_role: AdversarialMacroRole | None = None,
        missing_falsifier_role: AdversarialMacroRole | None = None,
        unknown_reference_role: AdversarialMacroRole | None = None,
        delays: dict[AdversarialMacroRole, float] | None = None,
    ) -> None:
        self.requests: list[MacroAnalysisRequest] = []
        self.fail_role = fail_role
        self.abstain_role = abstain_role
        self.missing_falsifier_role = missing_falsifier_role
        self.unknown_reference_role = unknown_reference_role
        self.delays = delays or {}

    @property
    def audit_identity(self) -> AnalyzerAuditIdentity:
        return BASE_IDENTITY

    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        self.requests.append(request)
        role = _role(request)
        if role is not None:
            await asyncio.sleep(self.delays.get(role, 0))
        if role is not None and role is self.fail_role:
            raise RuntimeError("raw provider detail must not cross the boundary")
        if role is None:
            return _analysis(request, role_name="BASELINE", score=Decimal("0.2"))
        if role is not None and role is self.abstain_role:
            return _analysis(
                request,
                role_name=role.value,
                score=Decimal("0"),
                decision=MacroAnalysisDecision.ABSTAIN,
            )
        scores = {
            AdversarialMacroRole.CATALYST_ADVOCATE: Decimal("0.7"),
            AdversarialMacroRole.RISK_CHALLENGER: Decimal("-0.2"),
            AdversarialMacroRole.EVIDENCE_AUDITOR: Decimal("0"),
            AdversarialMacroRole.MARKET_REGIME_ANALYST: Decimal("0.3"),
            AdversarialMacroRole.EXECUTION_RISK_AUDITOR: Decimal("-0.1"),
        }
        evidence_id = (
            "c" * 64
            if self.unknown_reference_role is not None
            and role is self.unknown_reference_role
            else request.evidence[0].evidence_id
        )
        return _analysis(
            request,
            role_name=role.value,
            score=scores[role],
            evidence_id=evidence_id,
            include_falsifier=(
                self.missing_falsifier_role is None
                or role is not self.missing_falsifier_role
            ),
        )


class FakeNonAuditableAnalyzer:
    def __init__(self) -> None:
        self.delegate = FakeAuditableAnalyzer()

    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        return await self.delegate.analyze(request)


def _analysis(
    request: MacroAnalysisRequest,
    *,
    role_name: str,
    score: Decimal,
    decision: MacroAnalysisDecision = MacroAnalysisDecision.PUBLISH,
    evidence_id: str | None = None,
    include_falsifier: bool = True,
) -> MacroAnalysis:
    resolved_evidence = evidence_id or request.evidence[0].evidence_id
    claims = (
        ()
        if decision is MacroAnalysisDecision.ABSTAIN
        else (MacroClaim(f"{role_name} evidence-backed claim", (resolved_evidence,), ()),)
    )
    return MacroAnalysis(
        analysis_id=request.analysis_id,
        as_of=request.as_of,
        decision=decision,
        regime=f"{role_name} regime",
        technical_alignment=Decimal("0.4"),
        macro_impact=score,
        scenarios=(),
        claims=claims,
        uncertainties=(),
        data_gaps=(),
        invalidation_conditions=(
            (f"{role_name} claim is falsified when retained evidence changes",)
            if include_falsifier and decision is not MacroAnalysisDecision.ABSTAIN
            else ()
        ),
        reported_confidence="UNCALIBRATED",
        refusal_reason=(
            "insufficient evidence"
            if decision is MacroAnalysisDecision.ABSTAIN
            else ""
        ),
        model_version=BASE_IDENTITY.requested_model,
    )


def test_depth_profiles_are_bounded_separately_from_optional_session_budget() -> None:
    fast = AdversarialMacroConfig.for_depth(AdversarialMacroDepth.FAST)
    standard = AdversarialMacroConfig.for_depth(AdversarialMacroDepth.STANDARD)
    deep = AdversarialMacroConfig.for_depth(AdversarialMacroDepth.DEEP)

    assert (len(fast.roles), fast.max_rounds) == (2, 1)
    assert (len(standard.roles), standard.max_rounds) == (3, 2)
    assert (len(deep.roles), deep.max_rounds) == (5, 3)
    assert fast.maximum_calls_per_session is None
    assert deep.maximum_calls_per_session is None
    assert all(item.max_rounds <= 3 for item in (fast, standard, deep))

    with pytest.raises(TypeError, match="max_rounds"):
        replace(fast, max_rounds=None)  # type: ignore[arg-type]


def test_first_round_is_blind_and_later_round_receives_only_peer_envelope() -> None:
    fake = FakeAuditableAnalyzer()
    config = replace(
        AdversarialMacroConfig.for_depth(AdversarialMacroDepth.STANDARD),
        early_stop_on_stable_consensus=False,
    )
    analyzer = AdversarialMacroAnalyzer(fake, config=config)

    run = asyncio.run(analyzer.analyze_case(_request()))

    assert run.failure_code is None
    assert run.calls_started == 6
    assert len(run.rounds) == 2
    first_round = [item for item in fake.requests if _round_number(item) == 1]
    second_round = [item for item in fake.requests if _round_number(item) == 2]
    assert len(first_round) == len(second_round) == 3
    assert all(item.evidence == _request().evidence for item in fake.requests)
    assert all(
        not any(
            value.startswith("UNTRUSTED_PEER_ARGUMENTS_JSON=")
            for value in item.technical_summary
        )
        for item in first_round
    )
    assert all(
        any(value.startswith("UNTRUSTED_PEER_ARGUMENTS_JSON=") for value in item.technical_summary)
        for item in second_round
    )
    assert run.analysis.decision is MacroAnalysisDecision.WATCH
    assert "ADVERSARIAL_MATERIAL_DIRECTIONAL_DISAGREEMENT" in run.analysis.uncertainties


@pytest.mark.parametrize(
    ("fake", "failure_code"),
    (
        (
            FakeAuditableAnalyzer(
                missing_falsifier_role=AdversarialMacroRole.RISK_CHALLENGER
            ),
            "ADVERSARIAL_ROLE_EVIDENCE_OR_FALSIFIER_MISSING",
        ),
        (
            FakeAuditableAnalyzer(
                unknown_reference_role=AdversarialMacroRole.CATALYST_ADVOCATE
            ),
            "ADVERSARIAL_ROLE_OUTPUT_INVALID",
        ),
        (
            FakeAuditableAnalyzer(abstain_role=AdversarialMacroRole.RISK_CHALLENGER),
            "ADVERSARIAL_REQUIRED_ROLE_ABSTAINED",
        ),
        (
            FakeAuditableAnalyzer(fail_role=AdversarialMacroRole.CATALYST_ADVOCATE),
            "ADVERSARIAL_ROLE_CALL_FAILED",
        ),
    ),
)
def test_any_required_role_critical_failure_forces_whole_case_to_abstain(
    fake: FakeAuditableAnalyzer,
    failure_code: str,
) -> None:
    analyzer = AdversarialMacroAnalyzer(fake)

    run = asyncio.run(analyzer.analyze_case(_request()))

    assert run.analysis.decision is MacroAnalysisDecision.ABSTAIN
    assert run.failure_code == failure_code
    assert run.termination is AdversarialTermination.CRITICAL_FAILURE
    assert run.analysis.refusal_reason == failure_code


def test_unlimited_session_budget_still_runs_only_fixed_round_count() -> None:
    fake = FakeAuditableAnalyzer()
    analyzer = AdversarialMacroAnalyzer(
        fake,
        config=AdversarialMacroConfig.for_depth(AdversarialMacroDepth.FAST),
    )

    first = asyncio.run(analyzer.analyze_case(_request()))
    second = asyncio.run(analyzer.analyze_case(_request()))

    assert first.calls_started == second.calls_started == 2
    assert len(first.rounds) == len(second.rounds) == 1
    assert analyzer.calls_started == 4


def test_deep_profile_cannot_run_beyond_its_fixed_round_limit() -> None:
    fake = FakeAuditableAnalyzer()
    config = replace(
        AdversarialMacroConfig.for_depth(AdversarialMacroDepth.DEEP),
        early_stop_on_stable_consensus=False,
    )
    analyzer = AdversarialMacroAnalyzer(fake, config=config)

    run = asyncio.run(analyzer.analyze_case(_request()))

    assert run.failure_code is None
    assert run.termination is AdversarialTermination.MAX_ROUNDS_REACHED
    assert len(run.rounds) == 3
    assert run.calls_started == 15
    assert len(fake.requests) == 15


def test_session_budget_is_reserved_per_complete_required_round() -> None:
    fake = FakeAuditableAnalyzer()
    analyzer = AdversarialMacroAnalyzer(
        fake,
        config=AdversarialMacroConfig.for_depth(
            AdversarialMacroDepth.FAST,
            maximum_calls_per_session=3,
        ),
    )

    first = asyncio.run(analyzer.analyze_case(_request()))
    second = asyncio.run(analyzer.analyze_case(_request()))

    assert first.failure_code is None
    assert second.failure_code == "ADVERSARIAL_SESSION_BUDGET_EXHAUSTED"
    assert second.analysis.decision is MacroAnalysisDecision.ABSTAIN
    assert second.calls_started == 0
    assert len(fake.requests) == 2


def test_audit_identity_is_deterministic_and_binds_depth_roles_and_base_prompt() -> None:
    first = AdversarialMacroAnalyzer(FakeAuditableAnalyzer())
    second = AdversarialMacroAnalyzer(FakeAuditableAnalyzer())
    deep = AdversarialMacroAnalyzer(
        FakeAuditableAnalyzer(),
        config=AdversarialMacroConfig.for_depth(AdversarialMacroDepth.DEEP),
    )

    assert first.audit_identity == second.audit_identity
    assert first.audit_identity.manifest_sha256 == second.audit_identity.manifest_sha256
    assert first.audit_identity != deep.audit_identity
    assert first.audit_identity.requested_model.endswith("adversarial-fast@1")


def test_non_auditable_provider_requires_and_accepts_explicit_identity() -> None:
    provider = FakeNonAuditableAnalyzer()
    with pytest.raises(ValueError, match="auditable"):
        AdversarialMacroAnalyzer(provider)

    analyzer = AdversarialMacroAnalyzer(provider, analyzer_identity=BASE_IDENTITY)
    result = asyncio.run(analyzer.analyze(_request()))

    assert result.model_version == analyzer.audit_identity.requested_model


def test_completion_order_cannot_change_deterministic_aggregation() -> None:
    slow_advocate = FakeAuditableAnalyzer(
        delays={
            AdversarialMacroRole.CATALYST_ADVOCATE: 0.02,
            AdversarialMacroRole.RISK_CHALLENGER: 0,
        }
    )
    slow_challenger = FakeAuditableAnalyzer(
        delays={
            AdversarialMacroRole.CATALYST_ADVOCATE: 0,
            AdversarialMacroRole.RISK_CHALLENGER: 0.02,
        }
    )

    first = asyncio.run(AdversarialMacroAnalyzer(slow_advocate).analyze(_request()))
    second = asyncio.run(AdversarialMacroAnalyzer(slow_challenger).analyze(_request()))

    assert first == second


def test_feature_flag_defaults_to_baseline_and_shadow_never_changes_decision() -> None:
    request = _request()
    baseline = FakeAuditableAnalyzer()
    adversarial_provider = FakeAuditableAnalyzer()
    adversarial = AdversarialMacroAnalyzer(adversarial_provider)
    observed = []
    wrapper = FeatureFlaggedAdversarialMacroAnalyzer(
        baseline,
        adversarial,
        shadow_observer=observed.append,
    )

    baseline_result = asyncio.run(wrapper.analyze(request))

    assert wrapper.mode is AdversarialFeatureMode.BASELINE
    assert baseline_result.macro_impact == Decimal("0.2")
    assert len(baseline.requests) == 1
    assert adversarial_provider.requests == []
    assert observed == []

    shadow_wrapper = FeatureFlaggedAdversarialMacroAnalyzer(
        baseline,
        adversarial,
        mode=AdversarialFeatureMode.SHADOW,
        shadow_observer=observed.append,
    )
    shadow_result = asyncio.run(shadow_wrapper.analyze(request))

    assert shadow_result.macro_impact == Decimal("0.2")
    assert len(adversarial_provider.requests) == 2
    assert len(observed) == 1
    assert observed[0].baseline_analysis == shadow_result
    assert observed[0].adversarial_analysis != shadow_result


def test_enforce_mode_returns_adversarial_compatible_outer_analysis() -> None:
    baseline = FakeAuditableAnalyzer()
    adversarial = AdversarialMacroAnalyzer(FakeAuditableAnalyzer())
    wrapper = FeatureFlaggedAdversarialMacroAnalyzer(
        baseline,
        adversarial,
        mode=AdversarialFeatureMode.ENFORCE,
    )

    result = asyncio.run(wrapper.analyze(_request()))

    assert result.analysis_id == _request().analysis_id
    assert result.decision is MacroAnalysisDecision.WATCH
    assert result.model_version == wrapper.audit_identity.requested_model
    assert baseline.requests == []
