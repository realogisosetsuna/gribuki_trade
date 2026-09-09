from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from gribuki_trade.analysis.schemas import (
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
    MacroClaim,
)
from gribuki_trade.domain.events import NormalizedEvent, SourceTier
from gribuki_trade.features.ashare_surveillance import (
    AShareIntradayRanking,
    IntradayCandidate,
    IntradayCandidateClass,
)
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
from gribuki_trade.services.ashare.intraday.ashare_intraday_llm_plans import (
    FrozenPITIntradayLLMPlanFactory,
    build_preopen_context,
    load_frozen_pit_event_snapshot,
    replay_frozen_pit_event_snapshot,
)
from gribuki_trade.services.ashare.research.ashare_surveillance import (
    AShareSurveillanceRun,
    AShareSurveillanceRunStatus,
)
from gribuki_trade.services.macro.macro_research import MacroResearchRun, MacroResearchService
from gribuki_trade.storage import SQLiteEventStore

AS_OF = datetime(2026, 8, 14, 0, 30, tzinfo=UTC)
SESSION = date(2026, 8, 14)


def _event(
    *,
    summary: str = "initial",
    visible_at: datetime = AS_OF - timedelta(minutes=10),
) -> NormalizedEvent:
    return NormalizedEvent(
        source_id="official.test",
        canonical_url="https://official.example.test/policy/1",
        title="policy event",
        summary=summary,
        event_type="macro",
        source_tier=SourceTier.OFFICIAL,
        first_seen_at=visible_at,
        retrieved_at=visible_at,
        available_at=visible_at,
        published_at=visible_at - timedelta(minutes=1),
        external_id="policy-1",
        entities=("510300.SH", "600000.SH"),
    )


def _identity(*, model: str = "deepseek-test") -> AnalyzerAuditIdentity:
    return AnalyzerAuditIdentity(
        provider_id="test.provider",
        requested_model=model,
        adapter_version="test-adapter@1",
        prompt_version="test-prompt@1",
        prompt_schema_sha256="a" * 64,
    )


class _Analyzer:
    def __init__(self) -> None:
        self.audit_identity = _identity()

    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        evidence_id = request.evidence[0].evidence_id
        return _analysis(request, evidence_id=evidence_id)


def _analysis(
    request: MacroAnalysisRequest,
    *,
    evidence_id: str,
    model: str = "deepseek-test",
) -> MacroAnalysis:
    return MacroAnalysis(
        analysis_id=request.analysis_id,
        as_of=request.as_of,
        decision=MacroAnalysisDecision.PUBLISH,
        regime="test",
        technical_alignment=Decimal("0.2"),
        macro_impact=Decimal("0.1"),
        scenarios=(),
        claims=(MacroClaim("bounded", (evidence_id,), ()),),
        uncertainties=(),
        data_gaps=(),
        invalidation_conditions=(),
        reported_confidence="UNCALIBRATED",
        refusal_reason="",
        model_version=model,
    )


def test_read_only_loader_reports_missing_empty_and_invalid_schema(
    tmp_path: Path,
) -> None:
    missing = load_frozen_pit_event_snapshot(tmp_path / "missing.sqlite3", as_of=AS_OF)
    assert missing.failure_code == "LLM_EVIDENCE_DB_NOT_FOUND"

    empty_path = tmp_path / "empty.sqlite3"
    with SQLiteEventStore(empty_path):
        pass
    empty = load_frozen_pit_event_snapshot(empty_path, as_of=AS_OF)
    assert empty.failure_code == "LLM_EVIDENCE_SNAPSHOT_EMPTY"

    invalid_path = tmp_path / "invalid.sqlite3"
    connection = sqlite3.connect(invalid_path)
    connection.execute("CREATE TABLE unrelated(value TEXT)")
    connection.commit()
    connection.close()
    invalid = load_frozen_pit_event_snapshot(invalid_path, as_of=AS_OF)
    assert invalid.failure_code == "LLM_EVIDENCE_DB_INVALID"


def test_read_only_loader_preserves_exact_historical_revision_and_database(
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.sqlite3"
    correction_at = AS_OF + timedelta(hours=1)
    original = _event()
    correction = replace(
        original,
        summary="corrected",
        first_seen_at=correction_at,
        retrieved_at=correction_at,
        available_at=correction_at,
        content_sha256="",
        revision_id="",
    )
    with SQLiteEventStore(path) as store:
        stored_original = store.append(original).event
        stored_correction = store.append(correction).event

    before_bytes = path.read_bytes()
    before_mtime = path.stat().st_mtime_ns
    before_names = {item.name for item in tmp_path.iterdir()}
    historical = load_frozen_pit_event_snapshot(path, as_of=AS_OF)
    corrected = load_frozen_pit_event_snapshot(path, as_of=correction_at)

    assert historical.events == (stored_original,)
    assert historical.events[0].revision_number == 1
    assert historical.events[0].supersedes_revision_id is None
    assert corrected.events == (stored_correction,)
    assert corrected.events[0].revision_number == 2
    assert corrected.events[0].supersedes_revision_id == stored_original.revision_id
    assert path.read_bytes() == before_bytes
    assert path.stat().st_mtime_ns == before_mtime
    assert {item.name for item in tmp_path.iterdir()} == before_names
    assert hashlib.sha256(path.read_bytes()).digest() == hashlib.sha256(
        before_bytes
    ).digest()


def test_snapshot_replay_requires_original_audit_identity(tmp_path: Path) -> None:
    path = tmp_path / "events.sqlite3"
    with SQLiteEventStore(path) as store:
        store.append(_event())
    service = MacroResearchService(_Analyzer())
    original = load_frozen_pit_event_snapshot(path, as_of=AS_OF)
    factory = FrozenPITIntradayLLMPlanFactory(service, original)
    retained_audit = factory.audit_document()

    replayed = replay_frozen_pit_event_snapshot(
        path,
        retained_audit=retained_audit,
        fallback_as_of=AS_OF + timedelta(hours=2),
    )
    assert replayed.snapshot_sha256 == original.snapshot_sha256
    assert replayed.as_of == AS_OF

    retained_audit["snapshot_sha256"] = "f" * 64
    mismatch = replay_frozen_pit_event_snapshot(
        path,
        retained_audit=retained_audit,
        fallback_as_of=AS_OF + timedelta(hours=2),
    )
    assert mismatch.available is False
    assert mismatch.failure_code == "LLM_EVIDENCE_SNAPSHOT_REPLAY_MISMATCH"


def test_factory_uses_explicit_horizons_and_rejects_response_model_swap(
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.sqlite3"
    with SQLiteEventStore(path) as store:
        store.append(_event())
    snapshot = load_frozen_pit_event_snapshot(path, as_of=AS_OF)
    service = MacroResearchService(_Analyzer())
    factory = FrozenPITIntradayLLMPlanFactory(service, snapshot)
    preopen_plan = factory.prepare_preopen(session_date=SESSION)
    assert preopen_plan.request.horizon == "ASHARE_PAPER_PREOPEN_SESSION_BASELINE"

    evidence_id = preopen_plan.request.evidence[0].evidence_id
    swapped_run = MacroResearchRun(
        request=preopen_plan.request,
        analysis=_analysis(
            preopen_plan.request,
            evidence_id=evidence_id,
            model="provider-swapped-model",
        ),
        selection=preopen_plan.selection,
        plan=preopen_plan,
    )
    assert (
        build_preopen_context(
            preopen_plan,
            swapped_run,
            session_date=SESSION,
            known_at=AS_OF + timedelta(minutes=1),
            valid_until=AS_OF + timedelta(hours=8),
        )
        is None
    )

    valid_run = MacroResearchRun(
        request=preopen_plan.request,
        analysis=_analysis(preopen_plan.request, evidence_id=evidence_id),
        selection=preopen_plan.selection,
        plan=preopen_plan,
    )
    other_plan = service.prepare(
        symbol="510300.SH",
        as_of=AS_OF,
        horizon="DIFFERENT_TEST_HORIZON",
        technical_summary=("different",),
        events=snapshot.events,
    )
    assert (
        build_preopen_context(
            preopen_plan,
            replace(valid_run, request=other_plan.request),
            session_date=SESSION,
            known_at=AS_OF + timedelta(minutes=1),
            valid_until=AS_OF + timedelta(hours=8),
        )
        is None
    )
    assert (
        build_preopen_context(
            preopen_plan,
            valid_run,
            session_date=SESSION,
            known_at=AS_OF + timedelta(minutes=1),
            valid_until=AS_OF + timedelta(hours=8),
        )
        is None
    )
    baseline = replace(
        _analysis(
            preopen_plan.request,
            evidence_id=evidence_id,
            model="deepseek-baseline-test",
        ),
        decision=MacroAnalysisDecision.WATCH,
        macro_impact=Decimal("-0.2"),
    )
    adversarial = replace(
        _analysis(preopen_plan.request, evidence_id=evidence_id),
        decision=MacroAnalysisDecision.PUBLISH,
        macro_impact=Decimal("0.3"),
    )
    dual_run = replace(
        valid_run,
        analysis=adversarial,
        baseline_analysis=baseline,
        adversarial_analysis=adversarial,
        selected_track="ADVERSARIAL",
        dual_audit_record_sha256="f" * 64,
    )
    dual_context = build_preopen_context(
        preopen_plan,
        dual_run,
        session_date=SESSION,
        known_at=AS_OF + timedelta(minutes=1),
        valid_until=AS_OF + timedelta(hours=8),
    )
    assert dual_context is not None
    legacy_context = replace(
        dual_context,
        baseline_decision=None,
        baseline_macro_impact=None,
        baseline_model=None,
        adversarial_decision=None,
        adversarial_macro_impact=None,
        adversarial_model=None,
        selected_track=None,
        dual_audit_record_sha256=None,
    )
    candidate = IntradayCandidate(
        symbol="600000.SH",
        name="fixture",
        rank=1,
        candidate_class=IntradayCandidateClass.MOMENTUM_EXPANSION,
        anomaly_score=0.9,
        factor_weight_coverage=1.0,
        last_price=Decimal("10.30"),
        change_percent=Decimal("3"),
        session_amount_cny=Decimal("100000000"),
        factors=(),
        reason_codes=("MOMENTUM_EXPANSION",),
        previous_close=Decimal("10"),
    )
    requested_at = AS_OF + timedelta(hours=2)
    scan = AShareSurveillanceRun(
        session_date=SESSION,
        requested_at=requested_at,
        decision_at=requested_at,
        status=AShareSurveillanceRunStatus.COMPLETE,
        strategy_version="test-scan@1",
        source_id="test.source",
        source_revision="revision-1",
        universe_count=5_000,
        ranking=AShareIntradayRanking(
            candidates=(candidate,),
            excluded=(),
            globally_unavailable_factors=(),
            eligible_count=5_000,
        ),
        warnings=(),
    )
    candidate_plan = factory.prepare_candidate_review(
        candidate=candidate,
        scan=scan,
        preopen_context=legacy_context,
        requested_at=requested_at,
    )
    assert candidate_plan is not None
    assert candidate_plan.request.horizon == "INTRADAY_BACKGROUND_REVIEW"
    assert candidate_plan.request.as_of == requested_at
    assert "PREOPEN_DUAL_TRACK=LEGACY_RECORD_NOT_CARRIED" in (
        candidate_plan.request.technical_summary
    )

    assert dual_context.baseline_decision is MacroAnalysisDecision.WATCH
    assert dual_context.baseline_macro_impact == Decimal("-0.2")
    assert dual_context.baseline_model == "deepseek-baseline-test"
    assert dual_context.adversarial_decision is MacroAnalysisDecision.PUBLISH
    assert dual_context.adversarial_macro_impact == Decimal("0.3")
    assert dual_context.adversarial_model == "deepseek-test"
    assert dual_context.selected_track == "ADVERSARIAL"
    assert dual_context.dual_audit_record_sha256 == "f" * 64
    assert dual_context.audit_document()["dual_track"] == {
        "adversarial": {
            "decision": "PUBLISH",
            "macro_impact": Decimal("0.3"),
            "model": "deepseek-test",
        },
        "audit_record_sha256": "f" * 64,
        "baseline": {
            "decision": "WATCH",
            "macro_impact": Decimal("-0.2"),
            "model": "deepseek-baseline-test",
        },
        "selected_track": "ADVERSARIAL",
    }
    dual_candidate_plan = factory.prepare_candidate_review(
        candidate=candidate,
        scan=scan,
        preopen_context=dual_context,
        requested_at=requested_at,
    )
    assert dual_candidate_plan is not None
    assert dual_candidate_plan.manifest_sha256 != candidate_plan.manifest_sha256
    assert "PREOPEN_DUAL_TRACK=COMPLETE" in dual_candidate_plan.request.technical_summary
    assert "PREOPEN_BASELINE_DECISION=WATCH" in (
        dual_candidate_plan.request.technical_summary
    )
    assert "PREOPEN_ADVERSARIAL_DECISION=PUBLISH" in (
        dual_candidate_plan.request.technical_summary
    )
    assert f"PREOPEN_DUAL_AUDIT_RECORD_SHA256={'f' * 64}" in (
        dual_candidate_plan.request.technical_summary
    )

    assert (
        build_preopen_context(
            preopen_plan,
            replace(dual_run, dual_audit_record_sha256=None),
            session_date=SESSION,
            known_at=AS_OF + timedelta(minutes=1),
            valid_until=AS_OF + timedelta(hours=8),
        )
        is None
    )
    assert (
        build_preopen_context(
            preopen_plan,
            replace(dual_run, selected_track="BASELINE"),
            session_date=SESSION,
            known_at=AS_OF + timedelta(minutes=1),
            valid_until=AS_OF + timedelta(hours=8),
        )
        is None
    )
    assert (
        build_preopen_context(
            preopen_plan,
            replace(
                dual_run,
                adversarial_analysis=replace(
                    adversarial,
                    as_of=adversarial.as_of + timedelta(seconds=1),
                ),
            ),
            session_date=SESSION,
            known_at=AS_OF + timedelta(minutes=1),
            valid_until=AS_OF + timedelta(hours=8),
        )
        is None
    )
    assert (
        build_preopen_context(
            preopen_plan,
            replace(dual_run, failure_code="ADVERSARIAL_CASE_FAILED"),
            session_date=SESSION,
            known_at=AS_OF + timedelta(minutes=1),
            valid_until=AS_OF + timedelta(hours=8),
        )
        is None
    )
    assert (
        build_preopen_context(
            preopen_plan,
            dual_run,
            session_date=SESSION,
            known_at=AS_OF - timedelta(seconds=1),
            valid_until=AS_OF + timedelta(hours=8),
        )
        is None
    )
