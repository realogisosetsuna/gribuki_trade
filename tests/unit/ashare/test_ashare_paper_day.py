from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import gribuki_trade.services.ashare.paper_day.ashare_paper_day as paper_day_module
from gribuki_trade.adapters.ashare.market.surveillance import (
    TENCENT_ENRICHED_SURVEILLANCE_SOURCE_ID,
)
from gribuki_trade.analysis.schemas import (
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
    MacroClaim,
)
from gribuki_trade.domain.events import NormalizedEvent, SourceTier
from gribuki_trade.domain.exit_plans import ExitPlan, ExitPlanDepth, ExitPlanState
from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_day import (
    NewPaperDayEvent,
    PaperDayPhase,
    PaperDayRunManifest,
    PaperDaySeverity,
    paper_day_target_hash,
)
from gribuki_trade.domain.paper_trading import (
    ASharePaperFill,
    PaperFillSource,
    PaperInstrumentType,
)
from gribuki_trade.domain.recommendations import (
    RecommendationDecision,
    RecommendationHorizon,
)
from gribuki_trade.features.ashare_surveillance import (
    AShareIntradayRanking,
    IntradayCandidate,
    IntradayCandidateClass,
)
from gribuki_trade.features.deep_exit_planning import DeepSemanticAssessment
from gribuki_trade.features.technical import TechnicalSignal, TechnicalSignalConfig
from gribuki_trade.ports.ashare_screening import AShareBoard
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
from gribuki_trade.ports.market_data import (
    FreshnessStatus,
    IntradayBar,
    MarketDataMeta,
    MinuteInterval,
    SourceSemantics,
)
from gribuki_trade.ports.notifier import (
    DeliveryReceipt,
    NotificationTargetKind,
    OutboundNotification,
)
from gribuki_trade.reporting.contracts import (
    ReportKind,
    validate_text_report_contract,
)
from gribuki_trade.services.ashare.intraday.ashare_intraday_llm import (
    IntradayLLMConfig,
    IntradayLLMCoordinator,
    IntradayLLMGateAction,
    IntradayLLMGateOutcome,
    IntradayLLMGateReason,
    JournaledIntradayLLMReview,
)
from gribuki_trade.services.ashare.intraday.ashare_intraday_paper import (
    IntradayPaperOrder,
    IntradayPaperRiskConfig,
    build_intraday_sell_quantity_plan,
    intraday_order_quantity_rule,
)
from gribuki_trade.services.ashare.paper_day.ashare_paper import ASharePaperTradingService
from gribuki_trade.services.ashare.paper_day.ashare_paper_day import (
    PAPER_RISK_POLICY_CHANGE_CONFIRMATION,
    ASharePaperDayConfig,
    ASharePaperDayRunner,
    PaperDayAbortRecoveryRequiredError,
    PaperDayEventPublisher,
    PaperDayLLMPreopenContext,
    PaperDayRiskPolicyChangeError,
    PaperDayWatchEntry,
    intraday_llm_manifest_document,
)
from gribuki_trade.services.ashare.research.ashare_surveillance import (
    AShareSurveillanceRun,
    AShareSurveillanceRunStatus,
)
from gribuki_trade.services.communications.notification_dispatch import NotificationDispatchService
from gribuki_trade.services.exit.exit_plan_lifecycle import ExitPlanLifecycleService
from gribuki_trade.services.macro.macro_research import (
    MacroResearchPlan,
    MacroResearchService,
)
from gribuki_trade.storage.execution.exit_plans import SQLiteExitPlanStore
from gribuki_trade.storage.execution.outbox import OutboxDispatcher, OutboxStatus, SQLiteOutbox
from gribuki_trade.storage.paper.paper_day import SQLitePaperDayStore
from gribuki_trade.storage.paper.paper_ledger import SQLitePaperLedger

SESSION = date(2026, 8, 14)
ACCOUNT = "paper-day-test"
TENCENT_ENRICHED_SOURCE = TENCENT_ENRICHED_SURVEILLANCE_SOURCE_ID


def test_paper_day_config_cannot_disable_mandatory_exit_plans() -> None:
    with pytest.raises(ValueError, match="QUICK protection is mandatory"):
        ASharePaperDayConfig(exit_plan_enabled=False)


def test_intraday_buy_gate_report_exposes_both_llm_tracks_and_selection() -> None:
    outcome = IntradayLLMGateOutcome(
        action=IntradayLLMGateAction.ALLOW_ENTRY,
        reason=IntradayLLMGateReason.APPROVED,
        technical_decision=RecommendationDecision.ENTER_CANDIDATE,
        technical_score=Decimal("0.8"),
        macro_score=Decimal("0.4"),
        combined_score=Decimal("0.7"),
        baseline_decision=MacroAnalysisDecision.PUBLISH,
        baseline_macro_score=Decimal("0.2"),
        baseline_model="deepseek-baseline",
        adversarial_decision=MacroAnalysisDecision.WATCH,
        adversarial_macro_score=Decimal("-0.1"),
        adversarial_model="deepseek-adversarial",
        selected_track="ADVERSARIAL",
        dual_audit_record_sha256="a" * 64,
    )

    projected = paper_day_module._llm_gate_document(outcome)
    dual = projected["dual_track"]
    assert isinstance(dual, dict)
    assert dual["status"] == "COMPLETE"
    assert dual["baseline"] == {
        "decision": "PUBLISH",
        "macro_score": Decimal("0.2"),
        "model": "deepseek-baseline",
    }
    assert dual["adversarial"] == {
        "decision": "WATCH",
        "macro_score": Decimal("-0.1"),
        "model": "deepseek-adversarial",
    }
    assert dual["selected_track"] == "ADVERSARIAL"
    readable = paper_day_module._llm_gate_dual_text(outcome)
    assert "原单分析器" in readable
    assert "对抗分析器" in readable
    assert "生产采用=结构化对抗分析器" in readable
    assert "aaaaaaaaaaaa" in readable


def test_preopen_dual_context_serialization_restores_without_fabricating_legacy() -> None:
    identity = _llm_identity()
    context = PaperDayLLMPreopenContext(
        context_id="preopen-dual-serialization-test",
        evidence_as_of=datetime(2026, 8, 14, 0, 30, tzinfo=UTC),
        known_at=datetime(2026, 8, 14, 0, 40, tzinfo=UTC),
        valid_until=datetime(2026, 8, 14, 7, 5, tzinfo=UTC),
        analysis_id="preopen-analysis-test",
        decision=MacroAnalysisDecision.WATCH,
        macro_impact=Decimal("-0.1"),
        evidence_pack_sha256="b" * 64,
        request_sha256="c" * 64,
        plan_manifest_sha256="d" * 64,
        analyzer_identity=identity,
        response_model=identity.requested_model,
        baseline_decision=MacroAnalysisDecision.PUBLISH,
        baseline_macro_impact=Decimal("0.2"),
        baseline_model="deepseek-baseline-test",
        adversarial_decision=MacroAnalysisDecision.WATCH,
        adversarial_macro_impact=Decimal("-0.1"),
        adversarial_model=identity.requested_model,
        selected_track="ADVERSARIAL",
        dual_audit_record_sha256="e" * 64,
    )

    context_document = json.loads(
        paper_day_module.paper_day_canonical_json(context.audit_document())
    )
    restored = paper_day_module._llm_preopen_context_from_document(  # noqa: SLF001
        context_document
    )
    assert restored == context

    legacy = replace(
        context,
        baseline_decision=None,
        baseline_macro_impact=None,
        baseline_model=None,
        adversarial_decision=None,
        adversarial_macro_impact=None,
        adversarial_model=None,
        selected_track=None,
        dual_audit_record_sha256=None,
    )
    legacy_document = json.loads(
        paper_day_module.paper_day_canonical_json(legacy.audit_document())
    )
    assert "dual_track" not in legacy_document
    restored_legacy = paper_day_module._llm_preopen_context_from_document(  # noqa: SLF001
        legacy_document
    )
    assert restored_legacy == legacy
    assert restored_legacy.has_dual_track is False
    legacy_notice = paper_day_module._llm_preopen_dual_text(restored_legacy)  # noqa: SLF001
    assert "当前不可用" in legacy_notice
    assert "系统未补造模型结论" in legacy_notice
    assert "LEGACY" not in legacy_notice

    identity_mismatch = json.loads(
        paper_day_module.paper_day_canonical_json(context.audit_document())
    )
    dual = identity_mismatch["dual_track"]
    assert isinstance(dual, dict)
    adversarial = dual["adversarial"]
    assert isinstance(adversarial, dict)
    adversarial["model"] = "swapped-provider-model"
    identity_mismatch["response_model"] = "swapped-provider-model"
    with pytest.raises(ValueError, match="adversarial model identity mismatch"):
        paper_day_module._llm_preopen_context_from_document(identity_mismatch)  # noqa: SLF001

    time_mismatch = json.loads(
        paper_day_module.paper_day_canonical_json(context.audit_document())
    )
    time_mismatch["known_at"] = "2026-08-14T00:20:00+00:00"
    with pytest.raises(ValueError, match="timestamps violate PIT chronology"):
        paper_day_module._llm_preopen_context_from_document(time_mismatch)  # noqa: SLF001


def test_deep_exit_notification_exposes_both_llm_scores() -> None:
    readable = paper_day_module._deep_exit_llm_assessment_text(
        {
            "available": True,
            "selected_system": "ADVERSARIAL_LLM",
            "selected_score": Decimal("0.63"),
            "baseline_score": Decimal("0.41"),
            "adversarial_score": Decimal("0.63"),
        }
    )

    assert "单分析器评分=0.41" in readable
    assert "对抗系统评分=0.63" in readable
    assert "生产采用=" in readable
    assert "ADVERSARIAL_LLM" not in readable


def test_deep_exit_notification_reports_adversarial_failure_without_masquerading() -> None:
    readable = paper_day_module._deep_exit_llm_assessment_text(
        {
            "available": True,
            "selected_system": "DETERMINISTIC_OR_BASELINE_FALLBACK",
            "selected_score": Decimal("0.32"),
            "baseline_score": Decimal("0.32"),
            "adversarial_score": None,
        }
    )

    assert "生产采用=确定性规则或原单分析器降级结果" in readable
    assert "单分析器评分=0.32" in readable
    assert "对抗系统评分=不可用（本次未取得）" in readable
    assert "ADVERSARIAL_LLM" not in readable
    assert "DETERMINISTIC_OR_BASELINE_FALLBACK" not in readable


def test_quick_exit_notification_states_that_llm_scores_are_not_available() -> None:
    readable = paper_day_module._deep_exit_llm_assessment_text(
        {
            "available": False,
            "status": "QUICK_PLAN_RETAINED",
        }
    )

    assert "当前仍为快速保护计划" in readable
    assert "尚未取得/不适用" in readable
    assert "单分析器评分=不可用（本次未取得）" in readable
    assert "对抗系统评分=不可用（本次未取得）" in readable
    assert "生产采用=未采用不完整双轨结果" in readable
    assert "QUICK_PLAN_RETAINED" not in readable


@pytest.mark.parametrize(
    "metrics,reason_codes",
    (
        (
            (
                ("baseline_semantic_score", Decimal("0.20")),
                ("selected_semantic_score", Decimal("0.20")),
            ),
            ("SEMANTIC_DEGRADED_FALLBACK",),
        ),
        (
            (
                ("adversarial_semantic_score", Decimal("0.40")),
                ("selected_semantic_score", Decimal("0.40")),
            ),
            ("ADVERSARIAL_RESULT_PREFERRED",),
        ),
        (
            (
                ("baseline_semantic_score", Decimal("0.20")),
                ("adversarial_semantic_score", Decimal("0.40")),
                ("selected_semantic_score", Decimal("0.20")),
            ),
            ("ADVERSARIAL_RESULT_PREFERRED",),
        ),
    ),
)
def test_barrier_projection_rejects_partial_or_inconsistent_dual_metrics(
    metrics: tuple[tuple[str, Decimal], ...],
    reason_codes: tuple[str, ...],
) -> None:
    now = datetime(2026, 8, 14, 7, 0, tzinfo=UTC)
    plan = ExitPlan(
        plan_id="deep-plan-dual-integrity",
        protection_id="protection-dual-integrity",
        account_id=ACCOUNT,
        symbol="600000.SH",
        version=2,
        depth=ExitPlanDepth.DEEP,
        state=ExitPlanState.CONFIRMED,
        decision_at=now,
        market_data_as_of=now,
        time_exit_at=now + timedelta(days=1),
        entry_basis_price=Decimal("100.00"),
        stop_price=Decimal("90.00"),
        take_profit_price=Decimal("120.00"),
        initial_risk_per_share=Decimal("10.00"),
        reward_to_risk=Decimal("2.00"),
        price_tick=Decimal("0.01"),
        technical_invalidation_price=Decimal("89.00"),
        feature_snapshot_sha256="a" * 64,
        policy_version="deep-dual-integrity@1",
        strategy_version="paper-day-test@1",
        calibration_id="UNCALIBRATED_TEST@1",
        reason_codes=reason_codes,
        metrics=metrics,
        evidence_ids=("evidence-dual-integrity",),
        supersedes_plan_id="quick-plan-dual-integrity",
    )

    projected = ASharePaperDayRunner._deep_exit_llm_assessment_document(plan)

    assert projected["available"] is False
    assert projected["baseline_status"] == (
        "AVAILABLE" if dict(metrics).get("baseline_semantic_score") is not None else "UNAVAILABLE"
    )
    assert projected["adversarial_status"] == (
        "AVAILABLE"
        if dict(metrics).get("adversarial_semantic_score") is not None
        else "UNAVAILABLE"
    )
    assert projected["selected_score"] is None
    assert projected["selected_system"] == "UNAVAILABLE"
    assert projected["status"] == "DEEP_LLM_METRICS_NOT_AVAILABLE"

    readable = paper_day_module._deep_exit_llm_assessment_text(projected)
    assert "单分析器评分=" in readable
    assert "对抗系统评分=" in readable
    assert "生产采用=未采用不完整双轨结果" in readable
    assert "ADVERSARIAL_RESULT_PREFERRED" not in readable


def test_reduce_review_uses_persisted_deep_score_without_allowing_a_veto() -> None:
    review = paper_day_module._deep_exit_sell_review(
        technical_score=Decimal("-0.60"),
        assessment={"available": True, "selected_score": Decimal("-0.40")},
    )

    assert review["action"] == "ESCALATE_REDUCE_URGENCY"
    assert review["combined_exit_score"] == Decimal("-0.7000")
    assert review["llm_can_veto"] is False
    readable = paper_day_module._deep_exit_sell_review_text(review)
    assert "升级卖出紧迫度" in readable
    assert "组合退出评分=-0.7000" in readable


def test_exit_time_uses_frozen_trading_sessions_instead_of_weekdays() -> None:
    session = date(2026, 9, 30)
    sessions = (
        date(2026, 10, 9),
        date(2026, 10, 12),
        date(2026, 10, 13),
        date(2026, 10, 14),
        date(2026, 10, 15),
    )

    resolved = paper_day_module._calendar_time_exit(
        session,
        trading_sessions=sessions,
        holding_sessions=2,
        market_close=time(15, 0),
    )

    assert resolved == datetime.fromisoformat("2026-10-12T07:00:00+00:00")


class _Notifier:
    channel = "onebot"

    def __init__(self) -> None:
        self.delivered: list[OutboundNotification] = []

    async def send(self, notification: OutboundNotification) -> DeliveryReceipt:
        self.delivered.append(notification)
        return DeliveryReceipt(channel=self.channel, provider_message_id="fixture")


class _Unused:
    async def run(self, **_kwargs: object) -> object:
        raise AssertionError("not used")

    async def run_once(self, **_kwargs: object) -> object:
        raise AssertionError("not used")


class _Market:
    async def fetch_intraday_bars_async(self, *_args: object, **_kwargs: object) -> tuple[()]:
        return ()

    async def fetch_trade_prints_async(self, *_args: object, **_kwargs: object) -> tuple[()]:
        return ()


class _FlakyDeepAssessmentProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def assess(self, *, timeframes, decision_at, **_kwargs: object):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("transient provider failure")
        market_data_as_of = max(frame.bars[-1].end_time for frame in timeframes)
        return (
            DeepSemanticAssessment(
                assessment_id="baseline-deep-test",
                system="BASELINE_LLM",
                score=Decimal("0.4"),
                confidence=Decimal("0.8"),
                market_data_as_of=market_data_as_of,
            ),
            DeepSemanticAssessment(
                assessment_id="adversarial-deep-test",
                system="ADVERSARIAL_LLM",
                score=Decimal("0.6"),
                confidence=Decimal("0.9"),
                market_data_as_of=market_data_as_of,
            ),
        )


class _ApprovedIntradayLLMGate:
    def evaluate_buy(self, signal: TechnicalSignal, **_kwargs: object) -> IntradayLLMGateOutcome:
        return IntradayLLMGateOutcome(
            action=IntradayLLMGateAction.ALLOW_ENTRY,
            reason=IntradayLLMGateReason.APPROVED,
            technical_decision=signal.decision,
            technical_score=signal.score,
            macro_score=Decimal("0.25"),
            combined_score=Decimal("0.65"),
            baseline_decision=MacroAnalysisDecision.PUBLISH,
            baseline_macro_score=Decimal("0.2"),
            baseline_model="test-baseline",
            adversarial_decision=MacroAnalysisDecision.PUBLISH,
            adversarial_macro_score=Decimal("0.25"),
            adversarial_model="test-adversarial",
            selected_track="ADVERSARIAL",
            dual_audit_record_sha256="b" * 64,
        )


def test_paper_runner_exit_plan_chain_and_restart_are_durable(tmp_path: Path) -> None:
    now_box = [datetime(2026, 8, 14, 6, 30, 30, tzinfo=UTC)]
    notifier = _Notifier()
    manifest = _manifest(now_box[0] - timedelta(hours=8))
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as day_store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
        SQLitePaperLedger(tmp_path / "ledger.sqlite3") as ledger,
        SQLiteExitPlanStore(tmp_path / "exit.sqlite3") as exit_store,
    ):
        day_store.create_run(manifest)
        day_store.acquire_lease(
            manifest.run_id,
            "exit-owner",
            now=now_box[0],
            lease_for=timedelta(minutes=30),
        )
        paper = ASharePaperTradingService(ledger)
        paper.open_account(
            ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=SESSION,
            opened_at=manifest.created_at,
        )
        publisher = PaperDayEventPublisher(
            manifest=manifest,
            store=day_store,
            outbox=outbox,
            dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="10001",
            owner_id="exit-owner",
            clock=lambda: now_box[0],
            status_path=tmp_path / "status.json",
        )
        lifecycle = ExitPlanLifecycleService(exit_store)
        deep_provider = _FlakyDeepAssessmentProvider()
        runner = ASharePaperDayRunner(
            manifest=manifest,
            latest_completed_session=date(2026, 8, 13),
            owner_id="exit-owner",
            store=day_store,
            publisher=publisher,
            preopen_screening=_Unused(),  # type: ignore[arg-type]
            surveillance=_Unused(),  # type: ignore[arg-type]
            market_data=_Market(),  # type: ignore[arg-type]
            paper=paper,
            outbox=outbox,
            report_dir=tmp_path / "reports",
            exit_plan_lifecycle=lifecycle,
            deep_exit_assessment_provider=deep_provider,
            technical_config=TechnicalSignalConfig(max_data_age=timedelta(minutes=5)),
            clock=lambda: now_box[0],
        )
        runner._DEEP_SEMANTIC_RETRY_INTERVAL = timedelta(milliseconds=1)  # noqa: SLF001
        runner._intraday_llm = _ApprovedIntradayLLMGate()  # type: ignore[assignment]  # noqa: SLF001
        candidate = _candidate()
        runner._watchlist[candidate.symbol] = PaperDayWatchEntry(  # noqa: SLF001
            candidate.symbol,
            candidate.name,
            AShareBoard.SSE_MAIN,
            "INTRADAY_SCAN",
            1,
            0.9,
        )
        runner._latest_candidates[candidate.symbol] = candidate  # noqa: SLF001
        runner._latest_scan_at = now_box[0]  # noqa: SLF001
        runner._latest_scan_source = TENCENT_ENRICHED_SOURCE  # noqa: SLF001
        runner._latest_scan_status = "COMPLETE"  # noqa: SLF001

        async def scenario() -> None:
            bars = _long_breakout_bars(now_box[0])
            await runner._process_symbol_bars(  # noqa: SLF001
                symbol=candidate.symbol,
                bars=bars,
                phase=PaperDayPhase.MORNING,
                decision_at=now_box[0],
                held=False,
            )
            assert candidate.symbol in runner._pending  # noqa: SLF001
            now_box[0] += timedelta(minutes=2)
            await runner._process_symbol_bars(  # noqa: SLF001
                symbol=candidate.symbol,
                bars=(*bars, _next_bar(bars[-1], now_box[0])),
                phase=PaperDayPhase.MORNING,
                decision_at=now_box[0],
                held=False,
            )
            protection_id = runner._exit_protection_by_symbol[candidate.symbol]  # noqa: SLF001
            quick_plan = lifecycle.active_plan(protection_id)
            assert quick_plan.depth.value == "QUICK"
            assert deep_provider.calls == 1
            entry_bar = _next_bar(bars[-1], now_box[0])
            now_box[0] += timedelta(minutes=1)
            quick_barrier_bar = _bar(
                entry_bar.end_at,
                quick_plan.stop_price,
                1000,
                now_box[0],
            )
            await runner._process_symbol_bars(  # noqa: SLF001
                symbol=candidate.symbol,
                bars=(quick_barrier_bar,),
                phase=PaperDayPhase.MORNING,
                decision_at=now_box[0],
                held=True,
            )
            quick_barrier_event = next(
                item
                for item in day_store.events(manifest.run_id)
                if item.event_type == "EXIT_PLAN_BARRIER_TRIGGERED"
                and item.payload["plan"]["depth"] == "QUICK"
            )
            quick_assessment = quick_barrier_event.payload["deep_exit_llm_assessment"]
            assert quick_assessment["available"] is False
            assert quick_assessment["status"] == "QUICK_PLAN_RETAINED"
            quick_notice = quick_barrier_event.payload["notification_text"]
            assert "当前仍为快速保护计划" in quick_notice
            assert "尚未取得/不适用" in quick_notice
            assert "QUICK_PLAN_RETAINED" not in quick_notice
            await asyncio.sleep(0.05)
            retained = day_store.events(manifest.run_id)
            types = [item.event_type for item in retained]
            assert "EXIT_PLAN_QUICK_CREATED" in types
            assert "FILL_APPLIED" in types
            assert "EXIT_PLAN_ATTACHED_TO_FILL" in types
            assert "EXIT_PLAN_DEEP_FAILED" in types
            assert "EXIT_PLAN_DEEP_APPLIED" in types
            assert types.index("EXIT_PLAN_QUICK_CREATED") < types.index(
                "ORDER_SUBMITTED"
            )
            assert types.index("ORDER_SUBMITTED") < types.index("FILL_APPLIED")
            deep_event = next(
                item for item in retained if item.event_type == "EXIT_PLAN_DEEP_APPLIED"
            )
            assert "deep_exit_llm_assessment" in deep_event.payload
            assert "成交后 DEEP 复核" in deep_event.payload["notification_text"]
            assert "单分析器评分=0.32" in deep_event.payload["notification_text"]
            assert "对抗系统评分=0.54" in deep_event.payload["notification_text"]
            assert deep_provider.calls >= 2
            history = lifecycle.history(protection_id)
            assert sum(item.event_type.value == "PLAN_ATTACHED_TO_FILL" for item in history) == 1
            assert sum(item.event_type.value == "DEEP_ANALYSIS_REQUESTED" for item in history) == 1
            assert sum(item.event_type.value == "PLAN_REPLACED" for item in history) == 1
            active = lifecycle.active_plan(protection_id)
            now_box[0] += timedelta(minutes=1)
            barrier_bar = _bar(
                quick_barrier_bar.end_at,
                active.stop_price,
                1000,
                now_box[0],
            )
            await runner._process_symbol_bars(  # noqa: SLF001
                symbol=candidate.symbol,
                bars=(barrier_bar,),
                phase=PaperDayPhase.MORNING,
                decision_at=now_box[0],
                held=True,
            )
            barrier_event = next(
                item
                for item in day_store.events(manifest.run_id)
                if item.event_type == "EXIT_PLAN_BARRIER_TRIGGERED"
                and item.payload["plan"]["depth"] == "DEEP"
            )
            assert barrier_event.payload["selected_barrier"] == "STOP_LOSS"
            assert barrier_event.payload["order_created"] is False
            assert barrier_event.payload["suppression_reason"] == "T1_NO_SELLABLE_QUANTITY"
            deep_assessment = barrier_event.payload["deep_exit_llm_assessment"]
            assert deep_assessment["available"] is True
            assert deep_assessment["baseline_score"] == "0.32"
            assert deep_assessment["adversarial_score"] == "0.54"
            assert deep_assessment["selected_system"] == "ADVERSARIAL_LLM"
            deep_notice = barrier_event.payload["notification_text"]
            assert "生产采用=结构化对抗分析器" in deep_notice
            assert "单分析器评分=0.32" in deep_notice
            assert "对抗系统评分=0.54" in deep_notice
            assert "ADVERSARIAL_LLM" not in deep_notice
            history_after_barrier = lifecycle.history(protection_id)

            resumed = ASharePaperDayRunner(
                manifest=manifest,
                latest_completed_session=date(2026, 8, 13),
                owner_id="exit-owner",
                store=day_store,
                publisher=publisher,
                preopen_screening=_Unused(),  # type: ignore[arg-type]
                surveillance=_Unused(),  # type: ignore[arg-type]
                market_data=_Market(),  # type: ignore[arg-type]
                paper=paper,
                outbox=outbox,
                report_dir=tmp_path / "reports",
                exit_plan_lifecycle=lifecycle,
                clock=lambda: now_box[0],
            )
            resumed._restore_state()  # noqa: SLF001
            resumed._restore_existing_exit_protections(  # noqa: SLF001
                paper.snapshot(ACCOUNT)
            )
            assert await resumed._recover_exit_plan_followups() == 0  # noqa: SLF001
            assert resumed._exit_protection_by_symbol[candidate.symbol] == protection_id  # noqa: SLF001
            replayed = lifecycle.history(protection_id)
            assert replayed == history_after_barrier

        asyncio.run(scenario())


def test_paper_runner_rejects_buy_when_signal_bar_already_crossed_quick_exit(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 14, 6, 30, 30, tzinfo=UTC)
    manifest = _manifest(now - timedelta(hours=8))
    notifier = _Notifier()
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as day_store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
        SQLitePaperLedger(tmp_path / "ledger.sqlite3") as ledger,
        SQLiteExitPlanStore(tmp_path / "exit.sqlite3") as exit_store,
    ):
        day_store.create_run(manifest)
        day_store.acquire_lease(
            manifest.run_id,
            "pretrade-owner",
            now=now,
            lease_for=timedelta(minutes=30),
        )
        paper = ASharePaperTradingService(ledger)
        paper.open_account(
            ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=SESSION,
            opened_at=manifest.created_at,
        )
        publisher = PaperDayEventPublisher(
            manifest=manifest,
            store=day_store,
            outbox=outbox,
            dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="10001",
            owner_id="pretrade-owner",
            clock=lambda: now,
            status_path=tmp_path / "status.json",
        )
        lifecycle = ExitPlanLifecycleService(exit_store)
        runner = ASharePaperDayRunner(
            manifest=manifest,
            latest_completed_session=date(2026, 8, 13),
            owner_id="pretrade-owner",
            store=day_store,
            publisher=publisher,
            preopen_screening=_Unused(),  # type: ignore[arg-type]
            surveillance=_Unused(),  # type: ignore[arg-type]
            market_data=_Market(),  # type: ignore[arg-type]
            paper=paper,
            outbox=outbox,
            report_dir=tmp_path / "reports",
            exit_plan_lifecycle=lifecycle,
            technical_config=TechnicalSignalConfig(max_data_age=timedelta(minutes=5)),
            clock=lambda: now,
        )
        runner._intraday_llm = _ApprovedIntradayLLMGate()  # type: ignore[assignment]  # noqa: SLF001
        candidate = _candidate()
        runner._watchlist[candidate.symbol] = PaperDayWatchEntry(  # noqa: SLF001
            candidate.symbol,
            candidate.name,
            AShareBoard.SSE_MAIN,
            "INTRADAY_SCAN",
            1,
            0.9,
        )
        runner._latest_candidates[candidate.symbol] = candidate  # noqa: SLF001
        runner._latest_scan_at = now  # noqa: SLF001
        runner._latest_scan_source = TENCENT_ENRICHED_SOURCE  # noqa: SLF001
        runner._latest_scan_status = "COMPLETE"  # noqa: SLF001
        bars = _long_breakout_bars(now)
        crossed_signal_bar = replace(bars[-1], high=Decimal("20.00"))

        asyncio.run(
            runner._process_symbol_bars(  # noqa: SLF001
                symbol=candidate.symbol,
                bars=(*bars[:-1], crossed_signal_bar),
                phase=PaperDayPhase.MORNING,
                decision_at=now,
                held=False,
            )
        )

        retained = day_store.events(manifest.run_id)
        types = [item.event_type for item in retained]
        assert "EXIT_PLAN_QUICK_CREATED" in types
        rejection = next(
            item for item in retained if item.event_type == "EXIT_PLAN_PRETRADE_REJECTED"
        )
        assert rejection.payload["reason_code"] == "QUICK_EXIT_CONDITION_ALREADY_MET"
        assert rejection.payload["order_created"] is False
        assert "TAKE_PROFIT" in rejection.payload["crossed_barriers"]
        assert "ORDER_SUBMITTED" not in types
        assert candidate.symbol not in runner._pending  # noqa: SLF001
        assert paper.fills(ACCOUNT) == ()
        protection_id = runner._exit_protection_by_symbol[candidate.symbol]  # noqa: SLF001
        assert all(
            item.event_type.value != "PLAN_ATTACHED_TO_FILL"
            for item in lifecycle.history(protection_id)
        )


def test_star_quantity_audit_and_chinese_residual_explanation() -> None:
    rule = intraday_order_quantity_rule(AShareBoard.STAR)
    rule_document = paper_day_module._quantity_rule_document(rule)  # noqa: SLF001
    assert rule_document is not None
    assert rule_document["minimum_buy_quantity"] == 200
    assert rule_document["buy_increment"] == 1
    assert rule_document["maximum_limit_order_quantity"] == 100_000

    plan = build_intraday_sell_quantity_plan(
        available_to_sell=199,
        board=AShareBoard.STAR,
    )
    plan_document = paper_day_module._sell_quantity_plan_document(  # noqa: SLF001
        plan
    )
    assert plan_document is not None
    assert plan_document["future_limit_order_sequence"] == [199]
    assert plan_document["residual_must_be_sold_all_once"] is True
    explanation = paper_day_module._sell_quantity_plan_text(plan)  # noqa: SLF001
    assert "最少200股" in explanation
    assert "199股余股" in explanation
    assert "一次性卖出" in explanation


def test_publisher_journals_before_actual_outbox_delivery(tmp_path: Path) -> None:
    now = datetime(2026, 8, 14, 0, 0, tzinfo=UTC)
    manifest = _manifest(now)
    notifier = _Notifier()
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
    ):
        store.create_run(manifest)
        store.acquire_lease(
            manifest.run_id,
            "test-owner",
            now=now,
            lease_for=timedelta(minutes=2),
        )
        publisher = PaperDayEventPublisher(
            manifest=manifest,
            store=store,
            outbox=outbox,
            dispatcher=NotificationDispatchService(
                outbox,
                {"onebot": notifier},
                dispatcher=OutboxDispatcher(
                    outbox,
                    {"onebot": notifier},
                    clock=lambda: now,
                ),
            ),
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="10001",
            owner_id="test-owner",
            clock=lambda: now,
            status_path=tmp_path / "status.json",
        )

        first = asyncio.run(
            publisher.emit(
                event_key="started",
                event_type="DAY_STARTED",
                phase=PaperDayPhase.BOOTSTRAP,
                severity=PaperDaySeverity.NOTICE,
                payload={"cash": Decimal("200000")},
                notification_text="paper day started",
            )
        )
        replay = asyncio.run(
            publisher.emit(
                event_key="started",
                event_type="ignored-on-exact-replay",
                phase=PaperDayPhase.TERMINAL,
                severity=PaperDaySeverity.CRITICAL,
                payload={"different": True},
            )
        )

        assert replay == first
        assert len(store.events(manifest.run_id)) == 1
        assert len(notifier.delivered) == 1
        assert first.payload["report_kind"] == ReportKind.INTRADAY_ALERT.value
        retained_text = first.payload["notification_text"]
        assert isinstance(retained_text, str)
        validate_text_report_contract(ReportKind.INTRADAY_ALERT, retained_text)
        assert notifier.delivered[0].text == retained_text
        assert outbox.list_items()[0].status is OutboxStatus.SENT
        assert (tmp_path / "session.log.jsonl").is_file()


@pytest.mark.parametrize(
    ("event_type", "expected"),
    (
        ("BUY_SIGNAL_TRIGGERED", ReportKind.INTRADAY_ALERT),
        ("FILL_APPLIED", ReportKind.EXECUTION_RECEIPT),
        ("HEALTH_CHECKPOINT", ReportKind.SYSTEM_HEALTH),
        ("DAY_COMPLETED", ReportKind.DAILY_REVIEW),
    ),
)
def test_every_paper_user_message_is_routed_through_a_report_contract(
    event_type: str,
    expected: ReportKind,
) -> None:
    assert paper_day_module._paper_notification_kind(event_type) is expected  # noqa: SLF001


def test_publisher_sidecar_collision_is_nonfatal_and_repaired_from_journal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 14, 0, 0, tzinfo=UTC)
    manifest = _manifest(now)
    notifier = _Notifier()
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
    ):
        store.create_run(manifest)
        store.acquire_lease(
            manifest.run_id,
            "test-owner",
            now=now,
            lease_for=timedelta(minutes=2),
        )
        publisher = PaperDayEventPublisher(
            manifest=manifest,
            store=store,
            outbox=outbox,
            dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="10001",
            owner_id="test-owner",
            clock=lambda: now,
            status_path=tmp_path / "status.json",
        )
        calls = 0
        original_append = paper_day_module._append_and_sync_text

        def collide_for_one_retry_budget(path: Path, text: str) -> None:
            nonlocal calls
            calls += 1
            if calls <= len(PaperDayEventPublisher._SIDECAR_RETRY_DELAYS_SECONDS):
                raise PermissionError("simulated Windows sharing violation")
            original_append(path, text)

        monkeypatch.setattr(
            paper_day_module,
            "_append_and_sync_text",
            collide_for_one_retry_budget,
        )

        first = asyncio.run(
            publisher.emit(
                event_key="first",
                event_type="FIRST",
                phase=PaperDayPhase.MORNING,
                severity=PaperDaySeverity.INFO,
                payload={},
            )
        )
        second = asyncio.run(
            publisher.emit(
                event_key="second",
                event_type="SECOND",
                phase=PaperDayPhase.MORNING,
                severity=PaperDaySeverity.INFO,
                payload={},
            )
        )

        assert calls == len(PaperDayEventPublisher._SIDECAR_RETRY_DELAYS_SECONDS)
        assert [item.sequence for item in store.events(manifest.run_id)] == [
            first.sequence,
            second.sequence,
        ]
        projected = [
            json.loads(line)
            for line in (tmp_path / "session.log.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        assert [item["sequence"] for item in projected] == [
            first.sequence,
            second.sequence,
        ]


def test_signal_next_minute_fill_and_sell_signal_without_sell_order(
    tmp_path: Path,
) -> None:
    now_box = [datetime(2026, 8, 14, 2, 1, tzinfo=UTC)]
    manifest = _manifest(datetime(2026, 8, 13, 20, 0, tzinfo=UTC))
    notifier = _Notifier()
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
        SQLitePaperLedger(tmp_path / "ledger.sqlite3") as ledger,
    ):
        store.create_run(manifest)
        store.acquire_lease(
            manifest.run_id,
            "test-owner",
            now=now_box[0],
            lease_for=timedelta(minutes=30),
        )
        paper = ASharePaperTradingService(ledger)
        paper.open_account(
            ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=SESSION,
            opened_at=manifest.created_at,
        )
        publisher = PaperDayEventPublisher(
            manifest=manifest,
            store=store,
            outbox=outbox,
            dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="10001",
            owner_id="test-owner",
            clock=lambda: now_box[0],
            status_path=tmp_path / "status.json",
        )
        runner = ASharePaperDayRunner(
            manifest=manifest,
            latest_completed_session=date(2026, 8, 13),
            owner_id="test-owner",
            store=store,
            publisher=publisher,
            preopen_screening=_Unused(),  # type: ignore[arg-type]
            surveillance=_Unused(),  # type: ignore[arg-type]
            market_data=_Market(),  # type: ignore[arg-type]
            paper=paper,
            outbox=outbox,
            report_dir=tmp_path,
            config=ASharePaperDayConfig(),
            technical_config=TechnicalSignalConfig(max_data_age=timedelta(minutes=5)),
            clock=lambda: now_box[0],
        )
        runner._watchlist["600000.SH"] = PaperDayWatchEntry(  # noqa: SLF001
            "600000.SH",
            "fixture",
            AShareBoard.SSE_MAIN,
            "PREOPEN_SCREEN",
            1,
            0.9,
        )
        runner._latest_candidates["600000.SH"] = _candidate()  # noqa: SLF001
        runner._latest_scan_at = now_box[0]  # noqa: SLF001
        runner._latest_scan_source = TENCENT_ENRICHED_SOURCE  # noqa: SLF001
        runner._latest_scan_status = "COMPLETE"  # noqa: SLF001
        blocked_bars = tuple(
            replace(
                item,
                meta=replace(
                    item.meta,
                    provider="AKShare/Sina stock_zh_a_minute fallback",
                    degraded=True,
                ),
            )
            for item in _breakout_bars(now_box[0])
        )

        asyncio.run(
            runner._process_symbol_bars(  # noqa: SLF001
                symbol="600000.SH",
                bars=blocked_bars,
                phase=PaperDayPhase.MORNING,
                decision_at=now_box[0],
                held=False,
            )
        )
        assert runner._pending == {}  # noqa: SLF001
        disabled_gate = next(
            event
            for event in store.events(manifest.run_id)
            if event.event_type == "BUY_LLM_GATE_EVALUATED"
        )
        assert disabled_gate.payload["reason"] == "LLM_OPERATOR_DISABLED_NEW_BUY_BLOCKED"
        assert disabled_gate.payload["status"] == "OPERATOR_DISABLED_MONITOR_ONLY"

        runner._intraday_llm = _ApprovedIntradayLLMGate()  # type: ignore[assignment]  # noqa: SLF001
        now_box[0] += timedelta(minutes=1)
        bars = tuple(
            replace(
                item,
                meta=replace(
                    item.meta,
                    provider="AKShare/Sina stock_zh_a_minute fallback",
                    degraded=True,
                ),
            )
            for item in _breakout_bars(now_box[0])
        )
        asyncio.run(
            runner._process_symbol_bars(  # noqa: SLF001
                symbol="600000.SH",
                bars=bars,
                phase=PaperDayPhase.MORNING,
                decision_at=now_box[0],
                held=False,
            )
        )
        assert "600000.SH" in runner._pending  # noqa: SLF001
        assert paper.fills(ACCOUNT) == ()
        submitted = next(
            event
            for event in store.events(manifest.run_id)
            if event.event_type == "ORDER_SUBMITTED"
        )
        buy_corridor = submitted.payload["price_acceptance"]
        assert isinstance(buy_corridor, dict)
        assert Decimal(str(buy_corridor["acceptable_lower_inclusive"])) > Decimal(
            str(buy_corridor["invalidation_boundary_exclusive"])
        )
        assert buy_corridor["acceptable_upper_inclusive"] == buy_corridor["limit_price"]
        buy_quantity_rule = submitted.payload["quantity_rule"]
        assert isinstance(buy_quantity_rule, dict)
        assert buy_quantity_rule["minimum_buy_quantity"] == 100
        assert buy_quantity_rule["buy_increment"] == 100
        corroboration = next(
            event
            for event in store.events(manifest.run_id)
            if event.event_type == "DEGRADED_MINUTE_CORROBORATION_ACCEPTED"
        )
        assert corroboration.payload["scan_source"] == TENCENT_ENRICHED_SOURCE
        assert corroboration.payload["scan_status"] == "COMPLETE"
        assert len(str(corroboration.payload["entry_policy_sha256"])) == 64

        # 信号只有在 10:01 K 线收盘后才可知；此时紧邻的 10:01—10:02 K 线
        # 已经开始，因此第一个完全可观察的成交区间是 10:02—10:03。
        next_bar = _next_bar(bars[-1], now_box[0] + timedelta(minutes=2))
        now_box[0] += timedelta(minutes=2)
        asyncio.run(
            runner._process_symbol_bars(  # noqa: SLF001
                symbol="600000.SH",
                bars=(*bars, next_bar),
                phase=PaperDayPhase.MORNING,
                decision_at=now_box[0],
                held=False,
            )
        )
        fills = paper.fills(ACCOUNT)
        assert len(fills) == 1
        assert fills[0].fill.side.value == "BUY"
        assert paper.snapshot(ACCOUNT).position("600000.SH").today_buy > 0  # type: ignore[union-attr]

        falling = _falling_bars(now_box[0] + timedelta(minutes=1))
        now_box[0] = falling[-1].meta.fetched_at
        asyncio.run(
            runner._process_symbol_bars(  # noqa: SLF001
                symbol="600000.SH",
                bars=falling,
                phase=PaperDayPhase.MORNING,
                decision_at=now_box[0],
                held=True,
            )
        )
        event_types = [item.event_type for item in store.events(manifest.run_id)]
        deep_failure = next(
            item
            for item in store.events(manifest.run_id)
            if item.event_type == "EXIT_PLAN_DEEP_FAILED"
        )
        assert (
            deep_failure.payload["error_code"]
            == "DEEP_DUAL_TRACK_PROVIDER_UNAVAILABLE_QUICK_RETAINED"
        )
        assert "EXIT_PLAN_DEEP_APPLIED" not in event_types
        assert "SELL_SIGNAL_TRIGGERED" in event_types
        assert "SELL_PRICE_ACCEPTANCE_EVALUATED" in event_types
        assert "SELL_NOT_SUBMITTED_T1" in event_types
        sell_event = next(
            event
            for event in store.events(manifest.run_id)
            if event.event_type == "SELL_PRICE_ACCEPTANCE_EVALUATED"
        )
        sell_corridor = sell_event.payload["price_acceptance"]
        assert isinstance(sell_corridor, dict)
        assert sell_corridor["acceptable_lower_inclusive"] == sell_corridor["limit_price"]
        assert Decimal(str(sell_corridor["acceptable_upper_inclusive"])) >= Decimal(
            str(sell_corridor["reference_price"])
        )
        sell_quantity_plan = sell_event.payload["sell_quantity_plan"]
        assert isinstance(sell_quantity_plan, dict)
        assert sell_quantity_plan["status"] == "NO_SELLABLE_QUANTITY"
        assert sell_quantity_plan["available_to_sell"] == 0
        sell_quantity_rule = sell_event.payload["quantity_rule"]
        assert isinstance(sell_quantity_rule, dict)
        assert sell_quantity_rule["minimum_regular_sell_quantity"] == 100
        assert len(paper.fills(ACCOUNT)) == 1


def test_degraded_whole_market_scan_cannot_authorize_entry(tmp_path: Path) -> None:
    now = datetime(2026, 8, 14, 2, 1, tzinfo=UTC)
    manifest = _manifest(datetime(2026, 8, 13, 20, 0, tzinfo=UTC))
    notifier = _Notifier()
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
        SQLitePaperLedger(tmp_path / "ledger.sqlite3") as ledger,
    ):
        store.create_run(manifest)
        store.acquire_lease(
            manifest.run_id,
            "test-owner",
            now=now,
            lease_for=timedelta(minutes=30),
        )
        paper = ASharePaperTradingService(ledger)
        paper.open_account(
            ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=SESSION,
            opened_at=manifest.created_at,
        )
        publisher = PaperDayEventPublisher(
            manifest=manifest,
            store=store,
            outbox=outbox,
            dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="10001",
            owner_id="test-owner",
            clock=lambda: now,
            status_path=tmp_path / "status.json",
        )
        runner = ASharePaperDayRunner(
            manifest=manifest,
            latest_completed_session=date(2026, 8, 13),
            owner_id="test-owner",
            store=store,
            publisher=publisher,
            preopen_screening=_Unused(),  # type: ignore[arg-type]
            surveillance=_Unused(),  # type: ignore[arg-type]
            market_data=_Market(),  # type: ignore[arg-type]
            paper=paper,
            outbox=outbox,
            report_dir=tmp_path,
            clock=lambda: now,
        )
        runner._watchlist["600000.SH"] = PaperDayWatchEntry(  # noqa: SLF001
            "600000.SH",
            "fixture",
            AShareBoard.SSE_MAIN,
            "INTRADAY_SCAN",
            1,
            0.9,
        )
        runner._latest_candidates["600000.SH"] = _candidate()  # noqa: SLF001
        runner._latest_scan_at = now  # noqa: SLF001
        runner._latest_scan_source = "AKShare/Tencent"  # noqa: SLF001
        runner._latest_scan_status = "DEGRADED"  # noqa: SLF001

        asyncio.run(
            runner._process_symbol_bars(  # noqa: SLF001
                symbol="600000.SH",
                bars=_breakout_bars(now),
                phase=PaperDayPhase.MORNING,
                decision_at=now,
                held=False,
            )
        )

        assert runner._pending == {}  # noqa: SLF001
        signal = next(
            event
            for event in store.events(manifest.run_id)
            if event.event_type == "BUY_SIGNAL_TRIGGERED"
        )
        assert signal.payload["gate_reason"] == "CURRENT_SESSION_SCAN_NOT_COMPLETE"

        degraded_sina = replace(
            _breakout_bars(now)[-1],
            meta=replace(
                _breakout_bars(now)[-1].meta,
                provider="AKShare/Sina stock_zh_a_minute fallback",
                degraded=True,
            ),
        )
        runner._latest_scan_status = "COMPLETE"  # noqa: SLF001
        runner._latest_scan_source = "AKShare/Tencent stock_zh_a_spot_tx"  # noqa: SLF001
        assert (  # noqa: SLF001
            runner._entry_gate_reason(  # noqa: SLF001
                symbol="600000.SH",
                latest=degraded_sina,
                candidate=_candidate(),
            )
            == "DEGRADED_SOURCE_NOT_INDEPENDENTLY_CORROBORATED"
        )
        runner._latest_scan_source = TENCENT_ENRICHED_SOURCE  # noqa: SLF001
        assert (  # noqa: SLF001
            runner._entry_gate_reason(  # noqa: SLF001
                symbol="600000.SH",
                latest=degraded_sina,
                candidate=_candidate(),
            )
            is None
        )


def test_empty_preopen_restart_recovers_from_valid_l1_sidecar(tmp_path: Path) -> None:
    now = datetime(2026, 8, 14, 0, 30, tzinfo=UTC)
    manifest = _manifest(datetime(2026, 8, 13, 20, 0, tzinfo=UTC))
    notifier = _Notifier()
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
        SQLitePaperLedger(tmp_path / "ledger.sqlite3") as ledger,
    ):
        store.create_run(manifest)
        store.acquire_lease(
            manifest.run_id,
            "test-owner",
            now=now,
            lease_for=timedelta(minutes=30),
        )
        original = store.append_event(
            NewPaperDayEvent(
                run_id=manifest.run_id,
                event_key="preopen-screen-completed",
                event_type="PREOPEN_SCREEN_COMPLETED",
                phase=PaperDayPhase.PREOPEN,
                severity=PaperDaySeverity.NOTICE,
                occurred_at=now - timedelta(minutes=10),
                known_at=now - timedelta(minutes=10),
                payload={"candidate_count": 0, "candidates": []},
                notification_required=False,
            ),
            owner_id="test-owner",
            lease_checked_at=now,
        )[0]
        assert original.payload["candidate_count"] == 0
        paper = ASharePaperTradingService(ledger)
        paper.open_account(
            ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=SESSION,
            opened_at=manifest.created_at,
        )
        publisher = PaperDayEventPublisher(
            manifest=manifest,
            store=store,
            outbox=outbox,
            dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="10001",
            owner_id="test-owner",
            clock=lambda: now,
            status_path=tmp_path / "status.json",
        )
        seed_path = tmp_path / "preopen-recovery-seed.json"
        seed = {
            "schema_version": "ashare-paper-preopen-recovery@1",
            "session_date": SESSION.isoformat(),
            "verified_latest_completed_session": "2026-08-13",
            "available_at": (now - timedelta(minutes=1)).isoformat(),
            "source_id": "AKShare/Tencent stock_zh_a_spot_tx/preopen-live-observation",
            "quality": "DEGRADED",
            "method": "RELIABLE_L1_HARD_FILTER_THEN_SESSION_AMOUNT_RANK_ONLY",
            "execution_authority": False,
            "raw_universe_count": 5500,
            "eligible_reliable_l1_count": 4000,
            "candidate_count": 1,
            "candidates": [
                {
                    "rank": 1,
                    "symbol": "600000.SH",
                    "name": "fixture",
                    "board": "SSE_MAIN",
                    "last_price": "10",
                    "session_amount_cny": "100000000",
                    "market_cap_cny": "5000000000",
                }
            ],
        }
        content = (json.dumps(seed, ensure_ascii=False) + "\n").encode()
        seed_path.write_bytes(content)
        seed_path.with_name(seed_path.name + ".sha256").write_text(
            hashlib.sha256(content).hexdigest() + "\n",
            encoding="ascii",
        )

        runner = ASharePaperDayRunner(
            manifest=manifest,
            latest_completed_session=date(2026, 8, 13),
            owner_id="test-owner",
            store=store,
            publisher=publisher,
            preopen_screening=_Unused(),  # type: ignore[arg-type]
            surveillance=_Unused(),  # type: ignore[arg-type]
            market_data=_Market(),  # type: ignore[arg-type]
            paper=paper,
            outbox=outbox,
            report_dir=tmp_path,
            preopen_recovery_path=seed_path,
            clock=lambda: now,
        )
        # 夹具中的常规重试会失败；经审计的旁路文件是明确的最终回退，
        # 其自身仍不具备可执行性。
        asyncio.run(runner._run_preopen_phase(now))  # noqa: SLF001

        assert tuple(runner._watchlist) == ("600000.SH",)  # noqa: SLF001
        recovered = store.event_by_key(manifest.run_id, "preopen-screen-recovered")
        assert recovered is not None
        assert recovered.event_type == "PREOPEN_SCREEN_RECOVERED"
        assert recovered.payload["execution_authority"] is False
        assert recovered.payload["recovery_method"] == seed["method"]

        # 第二次重启具有幂等性，恢复的是已找回的列表，而不是先前的空结果。
        resumed = ASharePaperDayRunner(
            manifest=manifest,
            latest_completed_session=date(2026, 8, 13),
            owner_id="test-owner",
            store=store,
            publisher=publisher,
            preopen_screening=_Unused(),  # type: ignore[arg-type]
            surveillance=_Unused(),  # type: ignore[arg-type]
            market_data=_Market(),  # type: ignore[arg-type]
            paper=paper,
            outbox=outbox,
            report_dir=tmp_path,
            preopen_recovery_path=seed_path,
            clock=lambda: now,
        )
        resumed._restore_state()  # noqa: SLF001
        assert tuple(resumed._watchlist) == ("600000.SH",)  # noqa: SLF001


def test_hot_restart_restores_scan_and_journals_policy_hashes(tmp_path: Path) -> None:
    now = datetime(2026, 8, 14, 2, 2, tzinfo=UTC)
    manifest = _manifest(datetime(2026, 8, 13, 20, 0, tzinfo=UTC))
    notifier = _Notifier()
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
        SQLitePaperLedger(tmp_path / "ledger.sqlite3") as ledger,
    ):
        store.create_run(manifest)
        store.acquire_lease(
            manifest.run_id,
            "test-owner",
            now=now,
            lease_for=timedelta(minutes=30),
        )
        previous, created = store.append_event(
            NewPaperDayEvent(
                run_id=manifest.run_id,
                event_key="scan:composite-fixture",
                event_type="SURVEILLANCE_SCAN_COMPLETED",
                phase=PaperDayPhase.MORNING,
                severity=PaperDaySeverity.INFO,
                occurred_at=now - timedelta(minutes=1),
                known_at=now - timedelta(minutes=1),
                payload={
                    "candidate_count": 1,
                    "candidates": [
                        {
                            "anomaly_score": 0.9,
                            "candidate_class": "MOMENTUM_EXPANSION",
                            "change_percent": "1",
                            "factor_weight_coverage": 1.0,
                            "last_price": "10.30",
                            "name": "fixture",
                            "rank": 1,
                            "reason_codes": ["MOMENTUM_EXPANSION"],
                            "session_amount_cny": "100000000",
                            "symbol": "600000.SH",
                        }
                    ],
                    "decision_at": now - timedelta(minutes=1),
                    "source_id": TENCENT_ENRICHED_SOURCE,
                    "source_revision": "fixture-revision",
                    "status": "COMPLETE",
                    "universe_count": 5500,
                    "warnings": [],
                },
                notification_required=False,
            ),
            owner_id="test-owner",
            lease_checked_at=now,
        )
        assert created is True
        paper = ASharePaperTradingService(ledger)
        paper.open_account(
            ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=SESSION,
            opened_at=manifest.created_at,
        )
        publisher = PaperDayEventPublisher(
            manifest=manifest,
            store=store,
            outbox=outbox,
            dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="10001",
            owner_id="test-owner",
            clock=lambda: now,
            status_path=tmp_path / "status.json",
        )
        runner = ASharePaperDayRunner(
            manifest=manifest,
            latest_completed_session=date(2026, 8, 13),
            owner_id="test-owner",
            store=store,
            publisher=publisher,
            preopen_screening=_Unused(),  # type: ignore[arg-type]
            surveillance=_Unused(),  # type: ignore[arg-type]
            market_data=_Market(),  # type: ignore[arg-type]
            paper=paper,
            outbox=outbox,
            report_dir=tmp_path,
            clock=lambda: now,
        )
        retained = store.events(manifest.run_id)
        runner._restore_state()  # noqa: SLF001
        asyncio.run(  # noqa: SLF001
            runner._record_runner_resume(  # noqa: SLF001
                retained_at_start=retained,
                resumed_at=now,
            )
        )

        resumed = store.event_by_key(
            manifest.run_id,
            f"runner-resumed:{previous.event_hash[:24]}",
        )
        assert resumed is not None
        assert resumed.phase is PaperDayPhase.MORNING
        assert resumed.payload["manifest_config_sha256"] == manifest.config_sha256
        assert resumed.payload["restored_candidate_count"] == 1
        assert resumed.payload["restored_scan_source"] == TENCENT_ENRICHED_SOURCE
        assert resumed.payload["restored_scan_status"] == "COMPLETE"
        assert len(str(resumed.payload["entry_policy_sha256"])) == 64
        assert resumed.payload["risk_policy"]["maximum_positions"] is None
        assert resumed.payload["risk_policy_manifest_binding"] == "MATCHES_MANIFEST"
        assert len(str(resumed.payload["risk_policy_sha256"])) == 64
        assert len(str(resumed.payload["runner_config_sha256"])) == 64


def test_aborted_run_requires_operator_and_resumes_same_state_without_replay(
    tmp_path: Path,
) -> None:
    """冻结一次中止运行，并证明恢复操作仅追加且具有幂等性。"""

    now = datetime(2026, 8, 14, 7, 6, tzinfo=UTC)
    event_at = datetime(2026, 8, 14, 2, 50, tzinfo=UTC)
    manifest = _manifest(datetime(2026, 8, 13, 20, 0, tzinfo=UTC))
    notifier = _Notifier()
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
        SQLitePaperLedger(tmp_path / "ledger.sqlite3") as ledger,
    ):
        store.create_run(manifest)
        store.acquire_lease(
            manifest.run_id,
            "seed-owner",
            now=event_at,
            lease_for=timedelta(minutes=30),
        )
        paper = ASharePaperTradingService(ledger)
        paper.open_account(
            ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=SESSION,
            opened_at=manifest.created_at,
        )
        existing_fill = ASharePaperFill(
            account_id=ACCOUNT,
            fill_id="existing-fill",
            symbol="603629.SH",
            side=Side.BUY,
            quantity=100,
            price=Decimal("132.95"),
            instrument_type=PaperInstrumentType.STOCK,
            trading_date=SESSION,
            executed_at=datetime(2026, 8, 14, 2, 18, tzinfo=UTC),
            source=PaperFillSource.SIMULATED,
            external_order_id="existing-order",
        )
        paper.record_fill(existing_fill, recorded_at=existing_fill.executed_at)
        snapshot_before = paper.snapshot(ACCOUNT)
        pending = IntradayPaperOrder(
            order_id="pending-order",
            account_id=ACCOUNT,
            symbol="600000.SH",
            board=AShareBoard.SSE_MAIN,
            session_date=SESSION,
            signal_bar_end=datetime(2026, 8, 14, 2, 40, tzinfo=UTC),
            signal_price=Decimal("10.30"),
            invalidation_price=Decimal("10.00"),
            limit_price=Decimal("10.31"),
            quantity=100,
            created_at=datetime(2026, 8, 14, 2, 41, tzinfo=UTC),
            expires_at=datetime(2026, 8, 14, 2, 42, tzinfo=UTC),
            previous_close=Decimal("10.10"),
            instrument_type=PaperInstrumentType.STOCK,
        )

        def append(
            key: str,
            event_type: str,
            payload: dict[str, object],
            *,
            symbol: str | None = None,
            correlation_id: str | None = None,
            phase: PaperDayPhase = PaperDayPhase.MORNING,
        ) -> None:
            store.append_event(
                NewPaperDayEvent(
                    run_id=manifest.run_id,
                    event_key=key,
                    event_type=event_type,
                    phase=phase,
                    severity=PaperDaySeverity.INFO,
                    occurred_at=event_at,
                    known_at=event_at,
                    payload=payload,
                    notification_required=False,
                    symbol=symbol,
                    correlation_id=correlation_id,
                ),
                owner_id="seed-owner",
                lease_checked_at=event_at,
            )

        watch_document = {
            "board": "SSE_MAIN",
            "name": "fixture",
            "rank": 1,
            "score": 0.9,
            "source": "INTRADAY_SCAN",
            "symbol": "600000.SH",
        }
        append(
            "watchlist:fixture",
            "WATCHLIST_UPDATED",
            {"watchlist": [watch_document]},
        )
        append(
            "scan:recovery-fixture",
            "SURVEILLANCE_SCAN_COMPLETED",
            {
                "candidates": [
                    {
                        "anomaly_score": 0.9,
                        "candidate_class": "MOMENTUM_EXPANSION",
                        "change_percent": "1",
                        "factor_weight_coverage": 1.0,
                        "last_price": "10.30",
                        "name": "fixture",
                        "rank": 1,
                        "reason_codes": ["MOMENTUM_EXPANSION"],
                        "session_amount_cny": "100000000",
                        "symbol": "600000.SH",
                    }
                ],
                "decision_at": event_at,
                "source_id": TENCENT_ENRICHED_SOURCE,
                "status": "COMPLETE",
            },
        )
        append(
            "order-submitted:pending-order",
            "ORDER_SUBMITTED",
            {
                "order": {
                    "account_id": pending.account_id,
                    "board": pending.board.value,
                    "created_at": pending.created_at,
                    "expires_at": pending.expires_at,
                    "instrument_type": pending.instrument_type.value,
                    "invalidation_price": pending.invalidation_price,
                    "limit_price": pending.limit_price,
                    "order_id": pending.order_id,
                    "previous_close": pending.previous_close,
                    "quantity": pending.quantity,
                    "session_date": pending.session_date,
                    "signal_bar_end": pending.signal_bar_end,
                    "signal_price": pending.signal_price,
                    "symbol": pending.symbol,
                }
            },
            symbol=pending.symbol,
            correlation_id=pending.order_id,
        )
        last_bar_end = datetime(2026, 8, 14, 2, 42, tzinfo=UTC)
        append(
            "technical:600000.SH:1m:2026-08-14T02:42:00+00:00",
            "TECHNICAL_SIGNAL_EVALUATED",
            {
                "bar_end": last_bar_end,
                "decision": "WATCH",
                "reference_price": "10.30",
            },
            symbol="600000.SH",
        )
        append(
            "fill-applied:existing-fill",
            "FILL_APPLIED",
            {"fill_id": existing_fill.fill_id},
            symbol=existing_fill.symbol,
            correlation_id=existing_fill.fill_id,
        )
        append(
            "day-aborted:105000",
            "DAY_ABORTED",
            {"error_code": "UNEXPECTED_PAPER_DAY_FAILURE"},
            phase=PaperDayPhase.TERMINAL,
        )
        store.release_lease(manifest.run_id, "seed-owner")
        frozen_events = store.events(manifest.run_id)
        frozen_chain = tuple((item.sequence, item.event_hash) for item in frozen_events)

        denied_publisher = PaperDayEventPublisher(
            manifest=manifest,
            store=store,
            outbox=outbox,
            dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="10001",
            owner_id="denied-owner",
            clock=lambda: now,
            status_path=tmp_path / "status.json",
        )
        denied = ASharePaperDayRunner(
            manifest=manifest,
            latest_completed_session=date(2026, 8, 13),
            owner_id="denied-owner",
            store=store,
            publisher=denied_publisher,
            preopen_screening=_Unused(),  # type: ignore[arg-type]
            surveillance=_Unused(),  # type: ignore[arg-type]
            market_data=_Market(),  # type: ignore[arg-type]
            paper=paper,
            outbox=outbox,
            report_dir=tmp_path / "reports",
            clock=lambda: now,
        )
        with pytest.raises(PaperDayAbortRecoveryRequiredError):
            asyncio.run(denied.run())
        assert (
            tuple((item.sequence, item.event_hash) for item in store.events(manifest.run_id))
            == frozen_chain
        )
        assert paper.snapshot(ACCOUNT) == snapshot_before
        assert len(paper.fills(ACCOUNT)) == 1

        recovery_publisher = PaperDayEventPublisher(
            manifest=manifest,
            store=store,
            outbox=outbox,
            dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="10001",
            owner_id="operator-owner",
            clock=lambda: now,
            status_path=tmp_path / "status.json",
        )
        recovered_runner = ASharePaperDayRunner(
            manifest=manifest,
            latest_completed_session=date(2026, 8, 13),
            owner_id="operator-owner",
            store=store,
            publisher=recovery_publisher,
            preopen_screening=_Unused(),  # type: ignore[arg-type]
            surveillance=_Unused(),  # type: ignore[arg-type]
            market_data=_Market(),  # type: ignore[arg-type]
            paper=paper,
            outbox=outbox,
            report_dir=tmp_path / "reports",
            recover_after_abort=True,
            clock=lambda: now,
        )
        result = asyncio.run(recovered_runner.run())

        retained = store.events(manifest.run_id)
        assert result.completed is True
        assert result.run_id == manifest.run_id
        assert (
            tuple((item.sequence, item.event_hash) for item in retained[: len(frozen_chain)])
            == frozen_chain
        )
        assert sum(item.event_type == "RUNNER_RECOVERY_AFTER_ABORT" for item in retained) == 1
        assert sum(item.event_type == "LLM_INTRADAY_OPERATOR_DISABLED" for item in retained) == 1
        assert sum(item.event_type == "ORDER_SUBMITTED" for item in retained) == 1
        assert sum(item.event_type == "FILL_APPLIED" for item in retained) == 1
        assert len(paper.fills(ACCOUNT)) == 1
        assert paper.snapshot(ACCOUNT).cash == snapshot_before.cash
        recovery = next(
            item for item in retained if item.event_type == "RUNNER_RECOVERY_AFTER_ABORT"
        )
        llm_disabled = next(
            item for item in retained if item.event_type == "LLM_INTRADAY_OPERATOR_DISABLED"
        )
        preflight = next(item for item in retained if item.event_type == "PREFLIGHT_PASSED")
        assert recovery.sequence == len(frozen_chain) + 1
        assert recovery.sequence < llm_disabled.sequence < preflight.sequence
        assert llm_disabled.payload["operator_opt_out"] is True
        assert llm_disabled.payload["new_buy_policy"] == "BLOCKED_MONITOR_ONLY"
        assert (
            llm_disabled.payload["reduce_policy"]
            == "TECHNICAL_PLUS_EXISTING_DEEP_REVIEW_NO_VETO"
        )
        assert recovery.payload["operator_authorized"] is True
        assert recovery.payload["resume_scope"] == "SAME_RUN_APPEND_ONLY"
        assert recovery.payload["pending_order_ids"] == [pending.order_id]
        assert recovery.payload["risk_policy"]["maximum_positions"] is None
        assert len(str(recovery.payload["risk_policy_sha256"])) == 64
        resumed = next(item for item in retained if item.event_type == "RUNNER_RESUMED")
        assert resumed.payload["restored_pending_order_count"] == 1
        assert resumed.payload["restored_position_count"] == 1
        assert resumed.payload["restored_last_processed_bar_count"] == 1
        assert resumed.payload["restored_last_price_count"] == 1
        assert result.report_path.is_file()


def test_runtime_risk_policy_change_requires_exact_confirmation_and_whitelist() -> None:
    manifest = _risk_policy_manifest(datetime(2026, 8, 13, 20, 0, tzinfo=UTC))
    new_policy = IntradayPaperRiskConfig().audit_document()

    with pytest.raises(PaperDayRiskPolicyChangeError) as unconfirmed:
        paper_day_module._requested_risk_policy_migration(  # noqa: SLF001
            events=(),
            manifest=manifest,
            new_policy=new_policy,
            confirmation=None,
        )
    assert unconfirmed.value.code == "RISK_POLICY_CHANGE_CONFIRMATION_REQUIRED"

    migration = paper_day_module._requested_risk_policy_migration(  # noqa: SLF001
        events=(),
        manifest=manifest,
        new_policy=new_policy,
        confirmation=PAPER_RISK_POLICY_CHANGE_CONFIRMATION,
    )
    assert migration is not None
    assert migration.reason_codes == (
        "REMOVE_POSITION_COUNT_CAP",
        "ADD_PRICE_ACCEPTANCE_BOUNDS",
        "ADD_BOARD_QUANTITY_RULES",
    )
    assert {item["field"] for item in migration.policy_diff} == {
        "maximum_positions",
        "order_quantity_policy",
        "position_count_limit_enabled",
        "price_acceptance_policy",
        "sell_limit_markdown",
    }

    unapproved = dict(new_policy)
    unapproved["cash_reserve_fraction"] = "0.10"
    with pytest.raises(PaperDayRiskPolicyChangeError) as rejected:
        paper_day_module._requested_risk_policy_migration(  # noqa: SLF001
            events=(),
            manifest=manifest,
            new_policy=unapproved,
            confirmation=PAPER_RISK_POLICY_CHANGE_CONFIRMATION,
        )
    assert rejected.value.code == "RISK_POLICY_CHANGE_NOT_ALLOWED"


def test_legacy_policy_without_document_still_requires_confirmation() -> None:
    legacy_config = {
        **ASharePaperDayConfig().audit_document(),
        "calendar_provider": "BaoStock",
        "calendar_verified": True,
        "latest_completed_session": "2026-08-13",
        "notification_channel": "onebot",
        "notification_preflight_policy": "GET_STATUS_GOOD_AND_ONLINE",
        "notification_preflight_required": True,
        "notification_target_kind": "private",
    }
    manifest = PaperDayRunManifest.create(
        session_date=SESSION,
        account_id=ACCOUNT,
        config=legacy_config,
        created_at=datetime(2026, 8, 13, 20, 0, tzinfo=UTC),
        target_hash=paper_day_target_hash(
            channel="onebot",
            target_kind="private",
            target_id="10001",
        ),
        initial_cash=Decimal("200000"),
    )

    with pytest.raises(PaperDayRiskPolicyChangeError) as unconfirmed:
        paper_day_module._requested_risk_policy_migration(  # noqa: SLF001
            events=(),
            manifest=manifest,
            new_policy=IntradayPaperRiskConfig().audit_document(),
            confirmation=None,
        )
    assert unconfirmed.value.code == "RISK_POLICY_CHANGE_CONFIRMATION_REQUIRED"

    migration = paper_day_module._requested_risk_policy_migration(  # noqa: SLF001
        events=(),
        manifest=manifest,
        new_policy=IntradayPaperRiskConfig().audit_document(),
        confirmation=PAPER_RISK_POLICY_CHANGE_CONFIRMATION,
    )
    assert migration is not None
    assert migration.baseline_source == "LEGACY_CLI_DEFAULTS_RECONSTRUCTED"


def test_runtime_risk_policy_change_refuses_pending_order_before_event() -> None:
    pending = IntradayPaperOrder(
        order_id="pending-policy-migration",
        account_id=ACCOUNT,
        symbol="600000.SH",
        board=AShareBoard.SSE_MAIN,
        session_date=SESSION,
        signal_bar_end=datetime(2026, 8, 14, 2, 40, tzinfo=UTC),
        signal_price=Decimal("10.30"),
        invalidation_price=Decimal("10.00"),
        limit_price=Decimal("10.31"),
        quantity=100,
        created_at=datetime(2026, 8, 14, 2, 41, tzinfo=UTC),
        expires_at=datetime(2026, 8, 14, 6, 55, tzinfo=UTC),
        previous_close=Decimal("10.10"),
        instrument_type=PaperInstrumentType.STOCK,
    )

    with pytest.raises(PaperDayRiskPolicyChangeError) as rejected:
        paper_day_module._validate_risk_policy_migration_state(  # noqa: SLF001
            pending={pending.symbol: pending},
            events=(),
        )
    assert rejected.value.code == "RISK_POLICY_CHANGE_PENDING_ORDER"


def test_unconfirmed_runtime_risk_policy_change_writes_no_event(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 14, 3, 0, tzinfo=UTC)
    manifest = _risk_policy_manifest(datetime(2026, 8, 13, 20, 0, tzinfo=UTC))
    notifier = _Notifier()
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
        SQLitePaperLedger(tmp_path / "ledger.sqlite3") as ledger,
    ):
        store.create_run(manifest)
        store.acquire_lease(
            manifest.run_id,
            "seed-owner",
            now=now - timedelta(minutes=1),
            lease_for=timedelta(seconds=30),
        )
        store.append_event(
            NewPaperDayEvent(
                run_id=manifest.run_id,
                event_key="unconfirmed-old-policy",
                event_type="RUNNER_RESUMED",
                phase=PaperDayPhase.MORNING,
                severity=PaperDaySeverity.NOTICE,
                occurred_at=now - timedelta(minutes=1),
                known_at=now - timedelta(minutes=1),
                notification_required=False,
                payload={"risk_policy": _old_risk_policy()},
            ),
            owner_id="seed-owner",
            lease_checked_at=now - timedelta(minutes=1),
        )
        store.release_lease(manifest.run_id, "seed-owner")
        frozen = store.events(manifest.run_id)
        publisher = PaperDayEventPublisher(
            manifest=manifest,
            store=store,
            outbox=outbox,
            dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="10001",
            owner_id="unconfirmed-owner",
            clock=lambda: now,
            status_path=tmp_path / "status.json",
        )
        runner = ASharePaperDayRunner(
            manifest=manifest,
            latest_completed_session=date(2026, 8, 13),
            owner_id="unconfirmed-owner",
            store=store,
            publisher=publisher,
            preopen_screening=_Unused(),  # type: ignore[arg-type]
            surveillance=_Unused(),  # type: ignore[arg-type]
            market_data=_Market(),  # type: ignore[arg-type]
            paper=ASharePaperTradingService(ledger),
            outbox=outbox,
            report_dir=tmp_path / "reports",
            clock=lambda: now,
        )

        with pytest.raises(PaperDayRiskPolicyChangeError) as rejected:
            asyncio.run(runner.run())
        assert rejected.value.code == "RISK_POLICY_CHANGE_CONFIRMATION_REQUIRED"
        assert store.events(manifest.run_id) == frozen
        assert not outbox.list_items()


def test_runtime_risk_policy_change_event_precedes_resume_and_is_idempotent(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 14, 7, 6, tzinfo=UTC)
    event_at = datetime(2026, 8, 14, 2, 50, tzinfo=UTC)
    manifest = _risk_policy_manifest(datetime(2026, 8, 13, 20, 0, tzinfo=UTC))
    notifier = _Notifier()
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
        SQLitePaperLedger(tmp_path / "ledger.sqlite3") as ledger,
    ):
        store.create_run(manifest)
        store.acquire_lease(
            manifest.run_id,
            "seed-owner",
            now=event_at,
            lease_for=timedelta(minutes=30),
        )
        paper = ASharePaperTradingService(ledger)
        paper.open_account(
            ACCOUNT,
            initial_cash=Decimal("200000"),
            session_date=SESSION,
            opened_at=manifest.created_at,
        )
        for index, symbol in enumerate(
            ("600001.SH", "600002.SH", "600003.SH", "600004.SH", "600005.SH"),
            start=1,
        ):
            fill = ASharePaperFill(
                account_id=ACCOUNT,
                fill_id=f"migration-fill-{index}",
                symbol=symbol,
                side=Side.BUY,
                quantity=100,
                price=Decimal("10"),
                instrument_type=PaperInstrumentType.STOCK,
                trading_date=SESSION,
                executed_at=event_at - timedelta(minutes=10 - index),
                source=PaperFillSource.SIMULATED,
                external_order_id=f"migration-order-{index}",
            )
            paper.record_fill(fill, recorded_at=fill.executed_at)
            store.append_event(
                NewPaperDayEvent(
                    run_id=manifest.run_id,
                    event_key=f"migration-fill-applied-{index}",
                    event_type="FILL_APPLIED",
                    phase=PaperDayPhase.MORNING,
                    severity=PaperDaySeverity.NOTICE,
                    occurred_at=fill.executed_at,
                    known_at=event_at,
                    notification_required=False,
                    payload={"fill_id": fill.fill_id},
                    symbol=symbol,
                    correlation_id=fill.fill_id,
                ),
                owner_id="seed-owner",
                lease_checked_at=event_at,
            )
        previous, _ = store.append_event(
            NewPaperDayEvent(
                run_id=manifest.run_id,
                event_key="old-policy-boundary",
                event_type="RUNNER_RESUMED",
                phase=PaperDayPhase.MORNING,
                severity=PaperDaySeverity.NOTICE,
                occurred_at=event_at,
                known_at=event_at,
                notification_required=False,
                payload={"risk_policy": _old_risk_policy()},
            ),
            owner_id="seed-owner",
            lease_checked_at=event_at,
        )
        store.release_lease(manifest.run_id, "seed-owner")
        publisher = PaperDayEventPublisher(
            manifest=manifest,
            store=store,
            outbox=outbox,
            dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="10001",
            owner_id="migration-owner",
            clock=lambda: now,
            status_path=tmp_path / "status.json",
        )
        runner = ASharePaperDayRunner(
            manifest=manifest,
            latest_completed_session=date(2026, 8, 13),
            owner_id="migration-owner",
            store=store,
            publisher=publisher,
            preopen_screening=_Unused(),  # type: ignore[arg-type]
            surveillance=_Unused(),  # type: ignore[arg-type]
            market_data=_Market(),  # type: ignore[arg-type]
            paper=paper,
            outbox=outbox,
            report_dir=tmp_path / "reports",
            risk_policy_change_confirmation=PAPER_RISK_POLICY_CHANGE_CONFIRMATION,
            clock=lambda: now,
        )

        result = asyncio.run(runner.run())
        retained = store.events(manifest.run_id)
        changed = next(
            item for item in retained if item.event_type == "OPERATOR_RISK_POLICY_CHANGED"
        )
        resumed = next(
            item
            for item in retained
            if item.event_type == "RUNNER_RESUMED" and item.sequence > changed.sequence
        )
        assert result.completed is True
        assert previous.sequence < changed.sequence < resumed.sequence
        assert resumed.payload["previous_event_sequence"] == changed.sequence
        assert resumed.payload["previous_event_hash"] == changed.event_hash
        assert resumed.payload["previous_event_type"] == changed.event_type
        assert changed.payload["operator_authorized"] is True
        assert changed.payload["pending_order_count"] == 0
        assert changed.payload["existing_fills_recomputed"] is False
        assert changed.payload["existing_positions_recomputed"] is False
        assert changed.payload["validated_fill_count"] == 5
        assert changed.payload["validated_position_count"] == 5
        assert changed.payload["reason_codes"] == [
            "REMOVE_POSITION_COUNT_CAP",
            "ADD_PRICE_ACCEPTANCE_BOUNDS",
            "ADD_BOARD_QUANTITY_RULES",
        ]
        assert len(str(changed.payload["old_risk_policy_sha256"])) == 64
        assert len(str(changed.payload["new_risk_policy_sha256"])) == 64
        assert changed.notification_required is True
        assert outbox.get_by_key(publisher.notification_key(changed)) is not None

        events_before_retry = store.events(manifest.run_id)
        migration = paper_day_module._requested_risk_policy_migration(  # noqa: SLF001
            events=events_before_retry,
            manifest=manifest,
            new_policy=IntradayPaperRiskConfig().audit_document(),
            confirmation=PAPER_RISK_POLICY_CHANGE_CONFIRMATION,
        )
        assert migration is None
        assert store.events(manifest.run_id) == events_before_retry


def _old_risk_policy() -> dict[str, object]:
    policy = IntradayPaperRiskConfig().audit_document()
    policy["maximum_positions"] = 5
    policy.pop("position_count_limit_enabled")
    policy.pop("sell_limit_markdown")
    policy.pop("price_acceptance_policy")
    policy.pop("order_quantity_policy")
    return policy


def _risk_policy_manifest(created_at: datetime) -> PaperDayRunManifest:
    return PaperDayRunManifest.create(
        session_date=SESSION,
        account_id=ACCOUNT,
        config={
            **ASharePaperDayConfig().audit_document(),
            "intraday_risk_policy": _old_risk_policy(),
        },
        created_at=created_at,
        target_hash=paper_day_target_hash(
            channel="onebot",
            target_kind="private",
            target_id="10001",
        ),
        initial_cash=Decimal("200000"),
    )


def _manifest(created_at: datetime) -> PaperDayRunManifest:
    return PaperDayRunManifest.create(
        session_date=SESSION,
        account_id=ACCOUNT,
        config={
            **ASharePaperDayConfig().audit_document(),
            "intraday_risk_policy": IntradayPaperRiskConfig().audit_document(),
        },
        created_at=created_at,
        target_hash=paper_day_target_hash(
            channel="onebot",
            target_kind="private",
            target_id="10001",
        ),
        initial_cash=Decimal("200000"),
    )


def _candidate() -> IntradayCandidate:
    return IntradayCandidate(
        symbol="600000.SH",
        name="fixture",
        rank=1,
        candidate_class=IntradayCandidateClass.MOMENTUM_EXPANSION,
        anomaly_score=0.9,
        factor_weight_coverage=1.0,
        last_price=Decimal("10.30"),
        change_percent=Decimal("1"),
        session_amount_cny=Decimal("100000000"),
        factors=(),
        reason_codes=("MOMENTUM_EXPANSION",),
    )


def _breakout_bars(now: datetime) -> tuple[IntradayBar, ...]:
    start = now - timedelta(minutes=31)
    result = []
    for index in range(31):
        close = Decimal("10") + Decimal(index) / Decimal("100")
        result.append(
            _bar(
                start + timedelta(minutes=index),
                close,
                3000 if index == 30 else 1000,
                now,
            )
        )
    return tuple(result)


def _long_breakout_bars(now: datetime) -> tuple[IntradayBar, ...]:
    start = now - timedelta(minutes=450)
    result: list[IntradayBar] = []
    for index in range(450):
        close = (
            Decimal("10.00")
            if index < 419
            else Decimal("10.00") + Decimal(index - 419) / Decimal("100")
        )
        result.append(
            _bar(
                start + timedelta(minutes=index),
                close,
                3000 if index == 449 else 1000,
                now,
            )
        )
    return tuple(result)


def _falling_bars(now: datetime) -> tuple[IntradayBar, ...]:
    start = now - timedelta(minutes=31)
    return tuple(
        _bar(
            start + timedelta(minutes=index),
            Decimal("10.50") - Decimal(index) / Decimal("50"),
            1000,
            now,
        )
        for index in range(31)
    )


def _next_bar(prior: IntradayBar, fetched_at: datetime) -> IntradayBar:
    return _bar(
        prior.end_at + timedelta(minutes=1),
        Decimal("10.31"),
        5000,
        fetched_at,
    )


def _bar(
    start: datetime,
    close: Decimal,
    volume: int,
    fetched_at: datetime,
) -> IntradayBar:
    return IntradayBar(
        symbol="600000.SH",
        start_at=start,
        end_at=start + timedelta(minutes=1),
        interval=MinuteInterval.ONE_MINUTE,
        open=close - Decimal("0.01"),
        high=close,
        low=close - Decimal("0.02"),
        close=close,
        volume_lots=volume,
        amount=Decimal(volume * 100) * close,
        vwap=close,
        is_closed=True,
        meta=MarketDataMeta(
            provider="AKShare/Eastmoney",
            semantics=SourceSemantics.AGGREGATED_MINUTE_BAR,
            fetched_at=fetched_at,
            provider_timestamp=start + timedelta(minutes=1),
            freshness=FreshnessStatus.CURRENT,
        ),
    )


def _llm_identity() -> AnalyzerAuditIdentity:
    return AnalyzerAuditIdentity(
        provider_id="test.intraday",
        requested_model="deepseek-intraday-test",
        adapter_version="test-adapter@1",
        prompt_version="intraday-test@1",
        prompt_schema_sha256="a" * 64,
    )


class _SlowAuditableAnalyzer:
    def __init__(self, release: asyncio.Event) -> None:
        self.audit_identity = _llm_identity()
        self.release = release
        self.started = asyncio.Event()
        self.fail_next = False
        self.calls = 0

    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        self.calls += 1
        self.started.set()
        await self.release.wait()
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("provider detail must not escape")
        evidence_id = request.evidence[0].evidence_id
        return MacroAnalysis(
            analysis_id=request.analysis_id,
            as_of=request.as_of,
            decision=MacroAnalysisDecision.PUBLISH,
            regime="test",
            technical_alignment=Decimal("0.8"),
            macro_impact=Decimal("0.2"),
            scenarios=(),
            claims=(MacroClaim("bounded claim", (evidence_id,), ()),),
            uncertainties=(),
            data_gaps=(),
            invalidation_conditions=(),
            reported_confidence="UNCALIBRATED",
            refusal_reason="",
            model_version=self.audit_identity.requested_model,
        )


class _RetainedPlanFactory:
    def __init__(
        self,
        research: MacroResearchService,
        *,
        available: bool = True,
    ) -> None:
        self._research = research
        self._available = available
        self.symbols: list[str] = []

    @property
    def available(self) -> bool:
        return self._available

    def audit_document(self) -> dict[str, object]:
        return {
            "evidence_as_of": "2026-08-14T02:01:00+00:00",
            "snapshot_id": "retained-llm-evidence-test",
            "source": "immutable-test-fixture",
        }

    def prepare_candidate_review(
        self,
        *,
        candidate: IntradayCandidate,
        scan: AShareSurveillanceRun,
        preopen_context: PaperDayLLMPreopenContext,
        requested_at: datetime,
    ) -> MacroResearchPlan | None:
        if not self.available:
            raise AssertionError("unavailable snapshot must not be queried")
        assert scan.status is AShareSurveillanceRunStatus.COMPLETE
        assert preopen_context.context_id == "preopen-llm-test"
        self.symbols.append(candidate.symbol)
        event = NormalizedEvent(
            source_id="official.test",
            canonical_url=f"https://example.test/{candidate.symbol}",
            title="retained candidate evidence",
            summary="bounded retained evidence",
            event_type="company_announcement",
            source_tier=SourceTier.OFFICIAL,
            first_seen_at=requested_at - timedelta(minutes=2),
            retrieved_at=requested_at - timedelta(minutes=2),
            available_at=requested_at - timedelta(minutes=2),
            published_at=requested_at - timedelta(minutes=3),
            external_id=f"event-{candidate.symbol}",
            entities=(candidate.symbol[:6],),
        )
        return self._research.prepare(
            symbol=candidate.symbol,
            as_of=requested_at,
            horizon="INTRADAY_BACKGROUND_REVIEW",
            technical_summary=(candidate.candidate_class.value,),
            events=(event,),
        )


class _JournalCheckingCoordinator(IntradayLLMCoordinator):
    def __init__(
        self,
        research: MacroResearchService,
        *,
        config: IntradayLLMConfig,
        store: SQLitePaperDayStore,
        run_id: str,
        clock,
    ) -> None:
        super().__init__(research, config=config, clock=clock)
        self._test_store = store
        self._test_run_id = run_id
        self.journal_seen_before_accept = 0
        self.evaluate_calls = 0

    def accept_journaled(
        self,
        review_id: str,
        *,
        accepted_at: datetime,
        journal_event_id: str,
        journal_event_sha256: str,
    ) -> JournaledIntradayLLMReview:
        event = self._test_store.event_by_key(
            self._test_run_id,
            f"llm-review-completed:{review_id}",
        )
        assert event is not None
        assert event.event_id == journal_event_id
        assert event.event_hash == journal_event_sha256
        self.journal_seen_before_accept += 1
        return super().accept_journaled(
            review_id,
            accepted_at=accepted_at,
            journal_event_id=journal_event_id,
            journal_event_sha256=journal_event_sha256,
        )

    def evaluate_buy(
        self,
        signal: TechnicalSignal,
        *,
        expected_context_id: str | None,
        decision_at: datetime,
    ) -> IntradayLLMGateOutcome:
        self.evaluate_calls += 1
        return super().evaluate_buy(
            signal,
            expected_context_id=expected_context_id,
            decision_at=decision_at,
        )


def _llm_candidate(
    symbol: str,
    candidate_class: IntradayCandidateClass,
    anomaly_score: float,
    rank: int,
) -> IntradayCandidate:
    return IntradayCandidate(
        symbol=symbol,
        name=f"fixture-{symbol}",
        rank=rank,
        candidate_class=candidate_class,
        anomaly_score=anomaly_score,
        factor_weight_coverage=1.0,
        last_price=Decimal("10.30"),
        change_percent=Decimal("3"),
        session_amount_cny=Decimal("100000000"),
        factors=(),
        reason_codes=(candidate_class.value,),
        previous_close=Decimal("10"),
    )


def _llm_scan(
    now: datetime,
    revision: str,
    candidates: tuple[IntradayCandidate, ...],
) -> AShareSurveillanceRun:
    return AShareSurveillanceRun(
        session_date=SESSION,
        requested_at=now,
        decision_at=now,
        status=AShareSurveillanceRunStatus.COMPLETE,
        strategy_version="test-scan@1",
        source_id=TENCENT_ENRICHED_SOURCE,
        source_revision=revision,
        universe_count=5_200,
        ranking=AShareIntradayRanking(
            candidates=candidates,
            excluded=(),
            globally_unavailable_factors=(),
            eligible_count=5_200,
        ),
        warnings=(),
    )


def _llm_signal(now: datetime, decision: RecommendationDecision) -> TechnicalSignal:
    return TechnicalSignal(
        symbol="600000.SH",
        as_of=now,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        decision=decision,
        score=(
            Decimal("1") if decision is RecommendationDecision.ENTER_CANDIDATE else Decimal("-0.6")
        ),
        reference_price=Decimal("10.30"),
        invalidation_price=Decimal("10.00"),
        reason_codes=("TEST_SIGNAL",),
        data_age=timedelta(0),
        strategy_version="test-technical@1",
        metrics=(),
    )


async def _wait_for_llm_idle(coordinator: IntradayLLMCoordinator) -> None:
    for _ in range(1_000):
        if coordinator.in_flight_count == 0:
            return
        await asyncio.sleep(0)
    raise AssertionError("background LLM review did not finish")


def test_intraday_llm_manifest_binds_preopen_dual_policy_and_accepts_v1_resume() -> None:
    coordinator = IntradayLLMCoordinator(
        MacroResearchService(_SlowAuditableAnalyzer(asyncio.Event())),
        config=IntradayLLMConfig(),
    )
    current = intraday_llm_manifest_document(coordinator)
    assert current["schema_version"] == 2
    assert current["preopen_context_contract"] == {
        "audit_record_sha256_required": True,
        "legacy_journal_policy": "RESTORE_WITHOUT_FABRICATION",
        "persisted_track_fields": ["decision", "macro_impact", "model"],
        "production_failure_policy": "FAIL_CLOSED",
        "production_selected_track": "ADVERSARIAL",
    }

    legacy = dict(current)
    legacy.pop("preopen_context_contract")
    legacy["schema_version"] = 1
    assert paper_day_module.intraday_llm_manifest_compatible(legacy, current) is True

    tampered = json.loads(json.dumps(current))
    tampered["preopen_context_contract"]["production_selected_track"] = "BASELINE"
    assert paper_day_module.intraday_llm_manifest_compatible(tampered, current) is False


def test_intraday_llm_runner_is_nonblocking_journal_first_restorable_and_sell_safe(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 14, 2, 1, tzinfo=UTC)
    notifier = _Notifier()
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
        SQLitePaperLedger(tmp_path / "ledger.sqlite3") as ledger,
    ):

        async def scenario() -> None:
            release = asyncio.Event()
            analyzer = _SlowAuditableAnalyzer(release)
            research = MacroResearchService(analyzer)
            llm_config = replace(
                IntradayLLMConfig(),
                review_top_n=2,
                per_review_timeout=timedelta(seconds=5),
                maximum_reviews_per_session=5,
            )
            runner_config = replace(
                ASharePaperDayConfig(),
                intraday_llm_enabled=True,
                intraday_llm_required_for_buy=True,
            )
            placeholder_manifest = PaperDayRunManifest.create(
                session_date=SESSION,
                account_id=ACCOUNT,
                config={},
                created_at=now - timedelta(hours=6),
                target_hash=paper_day_target_hash(
                    channel="onebot",
                    target_kind="private",
                    target_id="10001",
                ),
                initial_cash=Decimal("200000"),
            )
            coordinator = _JournalCheckingCoordinator(
                research,
                config=llm_config,
                store=store,
                run_id=placeholder_manifest.run_id,
                clock=lambda: now,
            )
            factory = _RetainedPlanFactory(research)
            manifest = PaperDayRunManifest.create(
                session_date=SESSION,
                account_id=ACCOUNT,
                config={
                    **runner_config.audit_document(),
                    "intraday_llm_policy": intraday_llm_manifest_document(coordinator),
                    "intraday_llm_evidence_snapshot": (
                        paper_day_module.intraday_llm_evidence_manifest_document(factory)
                    ),
                    "intraday_risk_policy": IntradayPaperRiskConfig().audit_document(),
                },
                created_at=placeholder_manifest.created_at,
                target_hash=placeholder_manifest.target_hash,
                initial_cash=Decimal("200000"),
            )
            coordinator._test_run_id = manifest.run_id
            store.create_run(manifest)
            store.acquire_lease(
                manifest.run_id,
                "test-owner",
                now=now,
                lease_for=timedelta(minutes=30),
            )
            paper = ASharePaperTradingService(ledger)
            paper.open_account(
                ACCOUNT,
                initial_cash=Decimal("200000"),
                session_date=SESSION,
                opened_at=manifest.created_at,
            )
            publisher = PaperDayEventPublisher(
                manifest=manifest,
                store=store,
                outbox=outbox,
                dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
                target_kind=NotificationTargetKind.PRIVATE,
                target_id="10001",
                owner_id="test-owner",
                clock=lambda: now,
                status_path=tmp_path / "status.json",
            )
            preopen = PaperDayLLMPreopenContext(
                context_id="preopen-llm-test",
                evidence_as_of=now - timedelta(minutes=20),
                known_at=now - timedelta(minutes=10),
                valid_until=datetime(2026, 8, 14, 7, 5, tzinfo=UTC),
                analysis_id="macro-preopen-test",
                decision=MacroAnalysisDecision.PUBLISH,
                macro_impact=Decimal("0.1"),
                evidence_pack_sha256="b" * 64,
                request_sha256="c" * 64,
                plan_manifest_sha256="d" * 64,
                analyzer_identity=analyzer.audit_identity,
                response_model=analyzer.audit_identity.requested_model,
                baseline_decision=MacroAnalysisDecision.PUBLISH,
                baseline_macro_impact=Decimal("0.2"),
                baseline_model="deepseek-baseline-test",
                adversarial_decision=MacroAnalysisDecision.WATCH,
                adversarial_macro_impact=Decimal("-0.1"),
                adversarial_model=analyzer.audit_identity.requested_model,
                selected_track="ADVERSARIAL",
                dual_audit_record_sha256="e" * 64,
            )
            runner = ASharePaperDayRunner(
                manifest=manifest,
                latest_completed_session=date(2026, 8, 13),
                owner_id="test-owner",
                store=store,
                publisher=publisher,
                preopen_screening=_Unused(),  # type: ignore[arg-type]
                surveillance=_Unused(),  # type: ignore[arg-type]
                market_data=_Market(),  # type: ignore[arg-type]
                paper=paper,
                outbox=outbox,
                report_dir=tmp_path,
                config=runner_config,
                intraday_llm=coordinator,
                intraday_llm_plan_factory=factory,
                llm_preopen_context=preopen,
                technical_config=TechnicalSignalConfig(max_data_age=timedelta(minutes=5)),
                clock=lambda: now,
            )
            await runner._initialize_intraday_llm()  # noqa: SLF001
            frozen_preopen = next(
                event
                for event in store.events(manifest.run_id)
                if event.event_type == "LLM_PREOPEN_CONTEXT_FROZEN"
            )
            assert frozen_preopen.payload["preopen_context"]["dual_track"] == {
                    "adversarial": {
                        "decision": "WATCH",
                        "macro_impact": "-0.1",
                        "model": analyzer.audit_identity.requested_model,
                },
                "audit_record_sha256": "e" * 64,
                    "baseline": {
                        "decision": "PUBLISH",
                        "macro_impact": "0.2",
                        "model": "deepseek-baseline-test",
                },
                "selected_track": "ADVERSARIAL",
            }
            frozen_notice = next(
                item.text
                for item in notifier.delivered
                if "盘前 LLM 宏观基线已冻结" in item.text
            )
            assert "原单分析器=形成有证据支持的宏观观点" in frozen_notice
            assert "结构化对抗分析器=继续观察" in frozen_notice
            assert "生产采用=结构化对抗分析器" in frozen_notice
            assert "ADVERSARIAL" not in frozen_notice
            assert "PUBLISH" not in frozen_notice
            assert "WATCH" not in frozen_notice

            candidates = (
                _llm_candidate("000001.SZ", IntradayCandidateClass.MOMENTUM_EXPANSION, 0.7, 2),
                _llm_candidate("600001.SH", IntradayCandidateClass.ACTIVE_STRENGTH, 0.99, 1),
                _llm_candidate("600000.SH", IntradayCandidateClass.MOMENTUM_EXPANSION, 0.9, 3),
            )
            scan = _llm_scan(now, "scan-1", candidates)
            await asyncio.wait_for(
                runner._schedule_intraday_llm_reviews(  # noqa: SLF001
                    phase=PaperDayPhase.MORNING,
                    scan=scan,
                    candidates=candidates,
                ),
                timeout=0.5,
            )
            assert factory.symbols == ["600000.SH", "000001.SZ"]
            await asyncio.wait_for(analyzer.started.wait(), timeout=0.5)
            assert coordinator.in_flight_count == 2

            runner._watchlist["600000.SH"] = PaperDayWatchEntry(  # noqa: SLF001
                "600000.SH",
                "fixture",
                AShareBoard.SSE_MAIN,
                "INTRADAY_SCAN",
                1,
                0.9,
            )
            runner._latest_candidates["600000.SH"] = candidates[2]  # noqa: SLF001
            runner._latest_scan_at = now  # noqa: SLF001
            runner._latest_scan_source = TENCENT_ENRICHED_SOURCE  # noqa: SLF001
            runner._latest_scan_status = "COMPLETE"  # noqa: SLF001
            await runner._process_symbol_bars(  # noqa: SLF001
                symbol="600000.SH",
                bars=_breakout_bars(now),
                phase=PaperDayPhase.MORNING,
                decision_at=now,
                held=False,
            )
            pending_gate = next(
                event
                for event in store.events(manifest.run_id)
                if event.event_type == "BUY_LLM_GATE_EVALUATED"
            )
            assert pending_gate.payload["reason_code"] == "LLM_REVIEW_NOT_READY"
            assert pending_gate.payload["blocks_entry"] is True
            assert runner._pending == {}  # noqa: SLF001

            release.set()
            await _wait_for_llm_idle(coordinator)
            await runner._drain_intraday_llm_reviews(phase=PaperDayPhase.MORNING)  # noqa: SLF001
            assert coordinator.journal_seen_before_accept == 2
            approved = coordinator.evaluate_buy(
                _llm_signal(now, RecommendationDecision.ENTER_CANDIDATE),
                expected_context_id=runner._latest_llm_context_by_symbol[  # noqa: SLF001
                    "600000.SH"
                ],
                decision_at=now,
            )
            assert approved.approved is True
            assert runner._llm_service_state == "HEALTHY"  # noqa: SLF001

            analyzer.fail_next = True
            failed_scan = _llm_scan(now, "scan-2", (candidates[2],))
            await runner._schedule_intraday_llm_reviews(  # noqa: SLF001
                phase=PaperDayPhase.MORNING,
                scan=failed_scan,
                candidates=(candidates[2],),
            )
            await _wait_for_llm_idle(coordinator)
            await runner._drain_intraday_llm_reviews(phase=PaperDayPhase.MORNING)  # noqa: SLF001
            assert runner._llm_service_state == "DEGRADED"  # noqa: SLF001

            recovered_scan = _llm_scan(now, "scan-3", (candidates[2],))
            await runner._schedule_intraday_llm_reviews(  # noqa: SLF001
                phase=PaperDayPhase.MORNING,
                scan=recovered_scan,
                candidates=(candidates[2],),
            )
            await _wait_for_llm_idle(coordinator)
            await runner._drain_intraday_llm_reviews(phase=PaperDayPhase.MORNING)  # noqa: SLF001
            assert runner._llm_service_state == "HEALTHY"  # noqa: SLF001
            health_events = tuple(
                event
                for event in store.events(manifest.run_id)
                if event.event_type == "LLM_SERVICE_STATE_CHANGED"
            )
            assert [event.payload["state"] for event in health_events] == [
                "HEALTHY",
                "DEGRADED",
                "HEALTHY",
            ]
            assert sum(event.notification_required for event in health_events) == 2

            # 模拟进程边界：提供方调用已经启动并以 SCHEDULED 写入日志，
            # 但完成观察结果尚未来得及排入持久日志。
            release.clear()
            interrupted_scan = _llm_scan(now, "scan-4", (candidates[2],))
            await runner._schedule_intraday_llm_reviews(  # noqa: SLF001
                phase=PaperDayPhase.MORNING,
                scan=interrupted_scan,
                candidates=(candidates[2],),
            )
            for _ in range(100):
                if coordinator.in_flight_count == 1 and analyzer.calls == 5:
                    break
                await asyncio.sleep(0)
            assert coordinator.in_flight_count == 1
            assert analyzer.calls == 5

            resumed_coordinator = _JournalCheckingCoordinator(
                research,
                config=llm_config,
                store=store,
                run_id=manifest.run_id,
                clock=lambda: now,
            )
            resumed = ASharePaperDayRunner(
                manifest=manifest,
                latest_completed_session=date(2026, 8, 13),
                owner_id="test-owner",
                store=store,
                publisher=publisher,
                preopen_screening=_Unused(),  # type: ignore[arg-type]
                surveillance=_Unused(),  # type: ignore[arg-type]
                market_data=_Market(),  # type: ignore[arg-type]
                paper=paper,
                outbox=outbox,
                report_dir=tmp_path,
                config=runner_config,
                intraday_llm=resumed_coordinator,
                intraday_llm_plan_factory=factory,
                llm_preopen_context=preopen,
                clock=lambda: now,
            )
            resumed._restore_state()  # noqa: SLF001
            assert resumed._llm_preopen_context == preopen  # noqa: SLF001
            await resumed._initialize_intraday_llm()  # noqa: SLF001
            assert resumed._llm_restored_reviews == 4  # noqa: SLF001
            assert resumed_coordinator.reviews_started == 5
            assert resumed._llm_service_state == "HEALTHY"  # noqa: SLF001
            restored_gate = resumed_coordinator.evaluate_buy(
                _llm_signal(now, RecommendationDecision.ENTER_CANDIDATE),
                expected_context_id=resumed._latest_llm_context_by_symbol[  # noqa: SLF001
                    "600000.SH"
                ],
                decision_at=now,
            )
            assert restored_gate.approved is False
            assert restored_gate.reason is IntradayLLMGateReason.REVIEW_NOT_READY

            await coordinator.close()
            calls_before_exhausted_schedule = analyzer.calls
            exhausted_scan = _llm_scan(now, "scan-5", (candidates[2],))
            await resumed._schedule_intraday_llm_reviews(  # noqa: SLF001
                phase=PaperDayPhase.MORNING,
                scan=exhausted_scan,
                candidates=(candidates[2],),
            )
            exhausted_batch = next(
                event
                for event in store.events(manifest.run_id)
                if event.event_key == "llm-review-batch:scan-5"
            )
            assert exhausted_batch.payload["outcomes"][0]["status"] == ("SESSION_BUDGET_EXHAUSTED")
            await asyncio.sleep(0)
            assert analyzer.calls == calls_before_exhausted_schedule

            calls_before_sell = resumed_coordinator.evaluate_calls
            await resumed._record_sell_signal(  # noqa: SLF001
                signal=_llm_signal(now, RecommendationDecision.REDUCE),
                latest=_breakout_bars(now)[-1],
                phase=PaperDayPhase.MORNING,
                notify=True,
            )
            assert resumed_coordinator.evaluate_calls == calls_before_sell
            sell = next(
                event
                for event in reversed(store.events(manifest.run_id))
                if event.event_type == "SELL_SIGNAL_TRIGGERED"
            )
            assert sell.payload["llm_gate"] == {
                "action": "NOT_APPLICABLE",
                "reason_code": "LLM_NOT_APPLICABLE_REDUCE",
                "sell_never_blocked": True,
            }
            assert any("LLM 服务降级" in item.text for item in notifier.delivered)
            assert any("LLM 服务已恢复" in item.text for item in notifier.delivered)

            mismatch_coordinator = IntradayLLMCoordinator(
                research,
                config=llm_config,
                clock=lambda: now,
            )
            mismatch_runner = ASharePaperDayRunner(
                manifest=manifest,
                latest_completed_session=date(2026, 8, 13),
                owner_id="test-owner",
                store=store,
                publisher=publisher,
                preopen_screening=_Unused(),  # type: ignore[arg-type]
                surveillance=_Unused(),  # type: ignore[arg-type]
                market_data=_Market(),  # type: ignore[arg-type]
                paper=paper,
                outbox=outbox,
                report_dir=tmp_path,
                config=runner_config,
                intraday_llm=mismatch_coordinator,
                intraday_llm_plan_factory=factory,
                llm_preopen_context=replace(
                    preopen,
                    baseline_decision=None,
                    baseline_macro_impact=None,
                    baseline_model=None,
                    adversarial_decision=None,
                    adversarial_macro_impact=None,
                    adversarial_model=None,
                    selected_track=None,
                    dual_audit_record_sha256=None,
                    response_model="unexpected-response-model",
                ),
                clock=lambda: now,
            )
            await mismatch_runner._initialize_intraday_llm()  # noqa: SLF001
            model_failure = next(
                event
                for event in store.events(manifest.run_id)
                if event.event_type == "LLM_PREOPEN_CONTEXT_FAILED"
                and event.payload.get("error_code") == "LLM_PREOPEN_CONTEXT_MODEL_MISMATCH"
            )
            assert model_failure.notification_required is True
            await resumed_coordinator.close()
            await mismatch_coordinator.close()

        asyncio.run(scenario())


def test_unavailable_llm_evidence_snapshot_fails_buy_closed_without_aborting_day(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 14, 2, 1, tzinfo=UTC)
    notifier = _Notifier()
    with (
        SQLitePaperDayStore(tmp_path / "day.sqlite3") as store,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
        SQLitePaperLedger(tmp_path / "ledger.sqlite3") as ledger,
    ):

        async def scenario() -> None:
            analyzer = _SlowAuditableAnalyzer(asyncio.Event())
            research = MacroResearchService(analyzer)
            llm_config = IntradayLLMConfig(review_top_n=1)
            coordinator = IntradayLLMCoordinator(
                research,
                config=llm_config,
                clock=lambda: now,
            )
            factory = _RetainedPlanFactory(research, available=False)
            runner_config = replace(
                ASharePaperDayConfig(),
                intraday_llm_enabled=True,
                intraday_llm_required_for_buy=True,
            )
            llm_policy = intraday_llm_manifest_document(coordinator)
            assert llm_policy["maximum_reviews_per_session"] is None
            manifest = PaperDayRunManifest.create(
                session_date=SESSION,
                account_id=ACCOUNT,
                config={
                    **runner_config.audit_document(),
                    "intraday_llm_policy": llm_policy,
                    "intraday_llm_evidence_snapshot": (
                        paper_day_module.intraday_llm_evidence_manifest_document(factory)
                    ),
                    "intraday_risk_policy": IntradayPaperRiskConfig().audit_document(),
                },
                created_at=now - timedelta(hours=6),
                target_hash=paper_day_target_hash(
                    channel="onebot",
                    target_kind="private",
                    target_id="10001",
                ),
                initial_cash=Decimal("200000"),
            )
            store.create_run(manifest)
            store.acquire_lease(
                manifest.run_id,
                "test-owner",
                now=now,
                lease_for=timedelta(minutes=30),
            )
            paper = ASharePaperTradingService(ledger)
            paper.open_account(
                ACCOUNT,
                initial_cash=Decimal("200000"),
                session_date=SESSION,
                opened_at=manifest.created_at,
            )
            publisher = PaperDayEventPublisher(
                manifest=manifest,
                store=store,
                outbox=outbox,
                dispatcher=NotificationDispatchService(outbox, {"onebot": notifier}),
                target_kind=NotificationTargetKind.PRIVATE,
                target_id="10001",
                owner_id="test-owner",
                clock=lambda: now,
                status_path=tmp_path / "status.json",
            )
            runner = ASharePaperDayRunner(
                manifest=manifest,
                latest_completed_session=date(2026, 8, 13),
                owner_id="test-owner",
                store=store,
                publisher=publisher,
                preopen_screening=_Unused(),  # type: ignore[arg-type]
                surveillance=_Unused(),  # type: ignore[arg-type]
                market_data=_Market(),  # type: ignore[arg-type]
                paper=paper,
                outbox=outbox,
                report_dir=tmp_path,
                config=runner_config,
                intraday_llm=coordinator,
                intraday_llm_plan_factory=factory,
                llm_preopen_context=None,
                clock=lambda: now,
            )

            await runner._initialize_intraday_llm()  # noqa: SLF001
            candidate = _llm_candidate(
                "600000.SH",
                IntradayCandidateClass.MOMENTUM_EXPANSION,
                0.9,
                1,
            )
            await runner._schedule_intraday_llm_reviews(  # noqa: SLF001
                phase=PaperDayPhase.MORNING,
                scan=_llm_scan(now, "unavailable-scan", (candidate,)),
                candidates=(candidate,),
            )

            retained = store.events(manifest.run_id)
            policy_event = next(
                event for event in retained if event.event_type == "LLM_INTRADAY_POLICY_CONFIGURED"
            )
            assert policy_event.payload["maximum_reviews_per_session"] is None
            assert policy_event.payload["session_review_budget"] == "UNLIMITED"
            assert any(
                event.event_type == "LLM_EVIDENCE_SNAPSHOT_FAILED"
                and event.payload["error_code"] == "LLM_EVIDENCE_SNAPSHOT_UNAVAILABLE"
                for event in retained
            )
            assert any(event.event_type == "LLM_PREOPEN_CONTEXT_FAILED" for event in retained)
            batch = next(
                event for event in retained if event.event_type == "LLM_REVIEW_BATCH_SCHEDULED"
            )
            outcomes = batch.payload["outcomes"]
            assert isinstance(outcomes, list)
            assert outcomes[0]["error_code"] == "LLM_EVIDENCE_SNAPSHOT_UNAVAILABLE"
            assert coordinator.reviews_started == 0
            assert any("行情监控和卖出信号记录继续运行" in item.text for item in notifier.delivered)
            await coordinator.close()

        asyncio.run(scenario())
