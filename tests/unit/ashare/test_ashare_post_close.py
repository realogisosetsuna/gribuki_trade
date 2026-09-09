from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.analysis.schemas import (
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroClaim,
)
from gribuki_trade.domain.candidates import (
    CandidatePriority,
    CandidateProvenance,
    CandidateRecord,
    CandidateSource,
    CandidateStatus,
)
from gribuki_trade.domain.paper_trading import (
    PaperAccountSnapshot,
    PaperInstrumentType,
    PaperPosition,
)
from gribuki_trade.domain.post_close import (
    PostCloseInstrumentResearch,
    PostCloseResearchStatus,
)
from gribuki_trade.reporting.contracts import (
    ReportKind,
    validate_markdown_report_contract,
)
from gribuki_trade.reporting.post_close import write_post_close_review
from gribuki_trade.services.ashare_close_sessions import (
    CloseAnalysisMode,
    CloseSessionResolution,
)
from gribuki_trade.services.ashare_post_close import (
    ASharePostCloseOrchestrator,
    ExistingCloseAnalysisResearch,
    PostCloseOrchestrationError,
    PostCloseOrchestrationRequest,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
SESSION = date(2026, 8, 14)
NEXT_SESSION = date(2026, 8, 17)
NOW = datetime(2026, 8, 14, 15, 6, tzinfo=SHANGHAI)


def _macro_track(
    *,
    decision: MacroAnalysisDecision,
    score: Decimal,
    model: str,
) -> MacroAnalysis:
    return MacroAnalysis(
        analysis_id="analysis-dual-track-1",
        as_of=NOW.astimezone(UTC),
        decision=decision,
        regime="风险偏好中性",
        technical_alignment=Decimal("0.20"),
        macro_impact=score,
        scenarios=(),
        claims=(MacroClaim("已冻结证据支持", ("evidence-1",), ()),),
        uncertainties=(),
        data_gaps=(),
        invalidation_conditions=("证据修订时失效",),
        reported_confidence="UNCALIBRATED",
        refusal_reason="",
        model_version=model,
    )


def _position(symbol: str, quantity: int = 100) -> PaperPosition:
    return PaperPosition(
        symbol=symbol,
        instrument_type=PaperInstrumentType.STOCK,
        quantity=quantity,
        available_to_sell=quantity,
        today_buy=0,
        average_cost=Decimal("10"),
        realized_pnl=Decimal("3.50"),
    )


def _account(*positions: PaperPosition) -> PaperAccountSnapshot:
    return PaperAccountSnapshot(
        account_id="paper-main",
        session_date=SESSION,
        cash=Decimal("123456.78"),
        positions=tuple(sorted(positions, key=lambda item: item.symbol)),
        opened_at=datetime(2026, 1, 2, tzinfo=UTC),
        updated_at=NOW.astimezone(UTC),
        last_sequence=17,
    )


def _paper_day(root: Path, *positions: PaperPosition) -> SimpleNamespace:
    return SimpleNamespace(
        session_root=root.resolve(),
        status_path=root / "status.json",
        event_log_path=root / "session.log.jsonl",
        session_date=SESSION,
        run_id="paper-day-0123456789abcdef",
        lifecycle="COMPLETED",
        coverage="FULL_SESSION",
        positions=tuple(
            SimpleNamespace(symbol=item.symbol, quantity=item.quantity) for item in positions
        ),
        scan_count=2,
        monitored_symbols=4,
        buy_signal_count=2,
        buy_approved_count=1,
        buy_rejected_count=1,
        buy_reject_reasons=(("LLM_REVIEW_NOT_AVAILABLE", 1),),
        buy_execution_acceptances=(
            SimpleNamespace(
                sequence=12,
                symbol="000001.SZ",
                order_id="paper-order-1",
                quantity=100,
                price_acceptance=SimpleNamespace(
                    reference_price=Decimal("10.40"),
                    limit_price=Decimal("10.41"),
                    acceptable_lower=Decimal("9.81"),
                    acceptable_upper=Decimal("10.41"),
                    invalidation_boundary=Decimal("9.80"),
                ),
                quantity_rule=SimpleNamespace(
                    board="SZSE_MAIN",
                    minimum_buy_quantity=100,
                    buy_increment=100,
                ),
            ),
        ),
        llm=SimpleNamespace(
            enabled=True,
            preopen_status="AVAILABLE",
            reviews_completed=1,
            reviews_failed=0,
            gate_evaluations=2,
            gate_blocked=1,
            gate_action_counts=(("CONFIRM", 1), ("BLOCK", 1)),
            requested_models=("deepseek-v4-flash",),
            response_models=("deepseek-v4-flash",),
        ),
        sell_signal_count=1,
        sell_not_submitted_count=1,
        source_degradation_count=1,
        source_recovery_count=1,
        orders_submitted=1,
        orders_filled=1,
        orders_expired=0,
        fills_applied=1,
        filled_shares=100,
        commission=Decimal("5"),
        transfer_fee=Decimal("0.10"),
        stamp_tax=Decimal("0"),
        final_equity=Decimal("124500"),
        total_mark_to_market_pnl=Decimal("500"),
        warnings=(),
    )


class FakeSessions:
    def __init__(
        self,
        *,
        latest: date = SESSION,
        mode: CloseAnalysisMode = CloseAnalysisMode.POST_CLOSE,
    ) -> None:
        self.latest = latest
        self.mode = mode
        self.calls: list[datetime] = []

    async def resolve(self, now: datetime) -> CloseSessionResolution:
        self.calls.append(now)
        return CloseSessionResolution(
            as_of=now.astimezone(SHANGHAI),
            latest_completed_session=self.latest,
            next_session=NEXT_SESSION,
            calendar_verified=True,
            analysis_mode=self.mode,
        )


class FakeAccount:
    def __init__(self, snapshot: PaperAccountSnapshot) -> None:
        self.value = snapshot

    def snapshot(self, account_id: str) -> PaperAccountSnapshot:
        assert account_id == "paper-main"
        return self.value


class FakeResearch:
    def __init__(self, failing_symbol: str | None = None) -> None:
        self.failing_symbol = failing_symbol
        self.calls: list[str] = []

    async def research(
        self,
        position: PaperPosition,
        *,
        sessions: CloseSessionResolution,
    ) -> PostCloseInstrumentResearch:
        self.calls.append(position.symbol)
        assert sessions.next_session == NEXT_SESSION
        if position.symbol == self.failing_symbol:
            raise RuntimeError("credential=must-not-appear")
        return PostCloseInstrumentResearch(
            symbol=position.symbol,
            status=PostCloseResearchStatus.COMPLETED,
            decision="WATCH",
            technical_score=Decimal("0.25"),
            reference_price=Decimal("10.50"),
            invalidation_price=Decimal("9.80"),
            reason_codes=("HELD_POSITION_REVIEW",),
            daily_bar_count=180,
            technical_decision="WATCH",
            combined_score=Decimal("0.20"),
            macro_score=Decimal("0.10"),
            macro_evidence_coverage=Decimal("0.75"),
            macro_provider="deepseek",
            macro_model="deepseek-v4-flash",
            macro_analysis_id="analysis-dual-track-1",
            macro_selected_track="ADVERSARIAL",
            macro_audit_record_sha256="a" * 64,
            baseline_macro_decision="PUBLISH",
            baseline_macro_regime="风险偏好温和",
            baseline_macro_score=Decimal("0.30"),
            baseline_macro_evidence_coverage=Decimal("0.50"),
            baseline_macro_model="deepseek-v4-flash",
            adversarial_macro_decision="WATCH",
            adversarial_macro_regime="风险约束增强",
            adversarial_macro_score=Decimal("-0.10"),
            adversarial_macro_evidence_coverage=Decimal("0.75"),
            adversarial_macro_model="deepseek-v4-flash-adversarial",
        )


class FakeCandidates:
    def list_candidates(
        self,
        *,
        as_of: datetime,
        statuses: frozenset[CandidateStatus] | None = None,
        limit: int = 500,
    ) -> tuple[CandidateRecord, ...]:
        assert as_of == NOW
        assert statuses == frozenset({CandidateStatus.ACTIVE})
        assert limit == 500
        observed = datetime(2026, 8, 14, 7, tzinfo=UTC)
        provenance = CandidateProvenance(
            observation_id="obs-1",
            source=CandidateSource.CLOSE_SCREEN,
            source_run_id="close-1",
            discovered_at=observed,
            observed_at=observed,
            expires_at=None,
            priority=CandidatePriority.HIGH,
            reason_codes=("BREAKOUT",),
            evidence_ids=(),
        )
        return (
            CandidateRecord(
                symbol="000001.SZ",
                as_of=as_of.astimezone(UTC),
                status=CandidateStatus.ACTIVE,
                priority=CandidatePriority.HIGH,
                discovered_at=observed,
                first_observed_at=observed,
                last_observed_at=observed,
                expires_at=None,
                cooling_until=None,
                reason_codes=("BREAKOUT",),
                evidence_ids=(),
                sources=(CandidateSource.CLOSE_SCREEN,),
                provenance=(provenance,),
            ),
        )


def test_orchestrator_writes_local_report_and_isolates_holding_failure(
    tmp_path: Path,
) -> None:
    first = _position("000001.SZ")
    second = _position("600000.SH", 200)
    account = _account(first, second)
    paper_day = _paper_day(tmp_path, first, second)
    research = FakeResearch(failing_symbol=second.symbol)
    orchestrator = ASharePostCloseOrchestrator(
        session_resolver=FakeSessions(),
        paper_account=FakeAccount(account),
        close_research=research,
        candidate_reader=FakeCandidates(),
        sidecar_loader=lambda _path: paper_day,
    )

    result = asyncio.run(
        orchestrator.run_once(
            PostCloseOrchestrationRequest(
                session_root=tmp_path,
                account_id="paper-main",
                now=NOW,
            )
        )
    )

    assert research.calls == ["000001.SZ", "600000.SH"]
    assert result.review.execution_mode == "PAPER_READ_ONLY"
    assert result.review.delivery_mode == "LOCAL_ARTIFACT_ONLY"
    assert result.review.held_symbols == ("000001.SZ", "600000.SH")
    assert result.review.candidates[0].symbol == "000001.SZ"
    assert result.review.research[0].status is PostCloseResearchStatus.COMPLETED
    assert result.review.research[1].status is PostCloseResearchStatus.FAILED
    assert result.review.research[1].failure_code == "CLOSE_RESEARCH_FAILED"
    text = result.artifact_path.read_text(encoding="utf-8")
    assert "A股盘后复盘与下一交易日知识基线" in text
    assert "收盘深度研究未完成" in text
    assert "CLOSE_RESEARCH_FAILED" not in text
    assert "credential" not in text
    assert "核心分析未持有 NapCat/outbox" in text
    assert "深研结果：部分完成" in text
    assert "`PARTIAL`" not in text
    assert "PAPER 门禁摘要" in text
    assert "PAPER 已提交买单的逐单边界" in text
    assert "盘后 LLM 与数据覆盖" in text
    assert "deepseek-v4-flash" in text
    assert "0.75/0.10" in text
    assert "双轨 LLM 结论与审计" in text
    assert "形成有证据支持的宏观观点" in text
    assert "结构化对抗结论" in text
    assert "analysis-dual-track-1" in text
    assert "a" * 64 in text
    validate_markdown_report_contract(ReportKind.DAILY_REVIEW, text)
    assert result.artifact_path.parent == tmp_path / "reports"
    position_files = tuple(sorted((tmp_path / "reports" / "positions").glob("*.md")))
    assert len(position_files) == 2
    for position_file in position_files:
        position_text = position_file.read_text(encoding="utf-8")
        validate_markdown_report_contract(
            ReportKind.POSITION_REVIEW,
            position_text,
        )
    completed_position_text = position_files[0].read_text(encoding="utf-8")
    assert "原单分析器" in completed_position_text
    assert "结构化对抗分析器" in completed_position_text
    assert "证据覆盖 0.50" in completed_position_text
    assert "记录SHA256" in completed_position_text
    assert not tuple((tmp_path / "reports").glob("*.tmp"))


def test_before_1505_fails_before_calendar_or_any_sidecar_access(tmp_path: Path) -> None:
    sessions = FakeSessions()
    sidecar_calls = 0

    def sidecar_loader(_path: Path) -> SimpleNamespace:
        nonlocal sidecar_calls
        sidecar_calls += 1
        return _paper_day(tmp_path)

    orchestrator = ASharePostCloseOrchestrator(
        session_resolver=sessions,
        paper_account=FakeAccount(_account()),
        close_research=FakeResearch(),
        sidecar_loader=sidecar_loader,
    )
    with pytest.raises(PostCloseOrchestrationError) as captured:
        asyncio.run(
            orchestrator.run_once(
                PostCloseOrchestrationRequest(
                    session_root=tmp_path,
                    account_id="paper-main",
                    now=datetime(2026, 8, 14, 15, 4, 59, tzinfo=SHANGHAI),
                )
            )
        )

    assert captured.value.code == "BEFORE_COMPLETED_BAR_AVAILABLE"
    assert sessions.calls == []
    assert sidecar_calls == 0


def test_weekend_or_holiday_is_rejected_even_after_1505(tmp_path: Path) -> None:
    orchestrator = ASharePostCloseOrchestrator(
        session_resolver=FakeSessions(latest=date(2026, 8, 14)),
        paper_account=FakeAccount(_account()),
        close_research=FakeResearch(),
    )
    weekend = datetime(2026, 8, 15, 16, tzinfo=SHANGHAI)

    with pytest.raises(PostCloseOrchestrationError) as captured:
        asyncio.run(
            orchestrator.run_once(
                PostCloseOrchestrationRequest(tmp_path, "paper-main", weekend)
            )
        )

    assert captured.value.code == "CURRENT_DATE_NOT_TRADING_SESSION"


def test_sidecar_and_hash_verified_ledger_position_must_agree(tmp_path: Path) -> None:
    position = _position("600000.SH")
    paper_day = _paper_day(tmp_path)
    orchestrator = ASharePostCloseOrchestrator(
        session_resolver=FakeSessions(),
        paper_account=FakeAccount(_account(position)),
        close_research=FakeResearch(),
        sidecar_loader=lambda _path: paper_day,
    )

    with pytest.raises(PostCloseOrchestrationError) as captured:
        asyncio.run(
            orchestrator.run_once(
                PostCloseOrchestrationRequest(tmp_path, "paper-main", NOW)
            )
        )

    assert captured.value.code == "PAPER_POSITION_PROJECTION_MISMATCH"


def test_secrets_session_path_is_rejected_without_reading_it(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="secrets"):
        PostCloseOrchestrationRequest(tmp_path / "secrets" / "paper", "paper-main", NOW)


def test_existing_close_adapter_never_requests_notification_or_broker() -> None:
    captured: list[object] = []

    class FakeCloseService:
        async def run_once(self, request: object) -> SimpleNamespace:
            captured.append(request)
            baseline = _macro_track(
                decision=MacroAnalysisDecision.PUBLISH,
                score=Decimal("0.30"),
                model="baseline-model",
            )
            adversarial = _macro_track(
                decision=MacroAnalysisDecision.WATCH,
                score=Decimal("-0.10"),
                model="adversarial-model",
            )
            recommendation = SimpleNamespace(
                decision=SimpleNamespace(value="REDUCE"),
                technical_score=Decimal("-0.4"),
                reference_price=Decimal("9.9"),
                invalidation_price=Decimal("10.5"),
                reason_codes=("HELD_RISK",),
                uncertainties=(),
                evidence=(SimpleNamespace(evidence_id="evidence-1"),),
                combined_score=Decimal("-0.2"),
                macro_score=Decimal("-0.1"),
                macro_evidence_coverage=Decimal("1"),
            )
            return SimpleNamespace(
                recommendation=recommendation,
                assessment=SimpleNamespace(decision=SimpleNamespace(value="REDUCE")),
                daily_bar_count=200,
                macro=adversarial,
                baseline_macro=baseline,
                adversarial_macro=adversarial,
                macro_selected_track="ADVERSARIAL",
                macro_audit_record_sha256="b" * 64,
                macro_failure_code=None,
                market_data_failure_code=None,
            )

    adapter = ExistingCloseAnalysisResearch(FakeCloseService())  # type: ignore[arg-type]
    sessions = CloseSessionResolution(
        as_of=NOW,
        latest_completed_session=SESSION,
        next_session=NEXT_SESSION,
        calendar_verified=True,
        analysis_mode=CloseAnalysisMode.POST_CLOSE,
    )

    result = asyncio.run(adapter.research(_position("600000.SH"), sessions=sessions))

    assert result.decision == "REDUCE"
    assert result.baseline_macro_decision == "PUBLISH"
    assert result.baseline_macro_score == Decimal("0.30")
    assert result.baseline_macro_evidence_coverage == Decimal("1")
    assert result.adversarial_macro_decision == "WATCH"
    assert result.adversarial_macro_score == Decimal("-0.10")
    assert result.adversarial_macro_evidence_coverage == Decimal("1")
    assert result.macro_selected_track == "ADVERSARIAL"
    assert result.macro_analysis_id == "analysis-dual-track-1"
    assert result.macro_audit_record_sha256 == "b" * 64
    assert len(captured) == 1
    request = captured[0]
    assert request.is_currently_held is True  # type: ignore[attr-defined]
    assert request.calendar_verified is True  # type: ignore[attr-defined]


def test_atomic_writer_replaces_same_deterministic_artifact(tmp_path: Path) -> None:
    position = _position("600000.SH")
    account = _account(position)
    paper_day = _paper_day(tmp_path, position)
    research = FakeResearch()
    orchestrator = ASharePostCloseOrchestrator(
        session_resolver=FakeSessions(),
        paper_account=FakeAccount(account),
        close_research=research,
        sidecar_loader=lambda _path: paper_day,
        report_writer=write_post_close_review,
    )
    request = PostCloseOrchestrationRequest(tmp_path, "paper-main", NOW)

    first = asyncio.run(orchestrator.run_once(request))
    second = asyncio.run(orchestrator.run_once(request))

    assert first.artifact_path == second.artifact_path
    assert second.artifact_path.is_file()
    assert not tuple(second.artifact_path.parent.glob(".*.tmp"))
    assert not tuple((second.artifact_path.parent / "positions").glob(".*.tmp"))
