import argparse
import asyncio
import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import gribuki_trade.cli as cli
from gribuki_trade.cli import (
    _ashare_candidates,
    _ashare_intraday_scan_once,
    _ashare_market_screen_once,
    _ashare_news_watch,
    _ashare_paper,
    _ashare_paper_day,
    _ashare_paper_day_report,
    _ashare_paper_day_status,
    _ashare_research_once,
    _ashare_research_runs,
    _ashare_research_watch,
    _ashare_review,
    _ashare_source_health,
    _napcat_dispatch,
    _napcat_send_artifact,
    _napcat_status,
    _non_negative_decimal,
    _non_negative_float,
    _non_negative_integer,
    _positive_decimal,
    _positive_float,
    _positive_integer,
    _positive_integer_or_unlimited,
    _sqlite_runtime_status,
    _strategy_exit_evaluate,
    _strategy_factor_discover,
    _unit_fraction_decimal,
    _verify_ashare_paper_day_calendar,
    build_parser,
)
from gribuki_trade.reporting.contracts import (
    REPORT_CONTRACTS,
    ReportKind,
    render_stable_markdown_report,
    validate_text_report_contract,
)


def test_sqlite_runtime_status_parser_helper_and_main(
    capsys: pytest.CaptureFixture[str],
) -> None:
    parsed = build_parser().parse_args(["sqlite-runtime-status"])
    assert parsed.command == "sqlite-runtime-status"

    expected = _sqlite_runtime_status()
    assert expected["single_connection_local_allowed"] is True
    assert isinstance(expected["shared_wal_safe"], bool)
    assert cli.main(["sqlite-runtime-status"]) == 0
    assert json.loads(capsys.readouterr().out) == expected


def test_temp_root_cli_supports_status_prepare_and_explicit_override(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    target = tmp_path / "operator-scratch"
    parsed = build_parser().parse_args(["temp-root", "status", "--temp-dir", str(target)])
    assert parsed.action == "status"
    assert parsed.temp_dir == str(target)

    assert cli.main(["temp-root", "status", "--temp-dir", str(target)]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["source"] == "EXPLICIT"
    assert status["exists"] is False
    assert not target.exists()

    assert cli.main(["temp-root", "prepare", "--temp-dir", str(target)]) == 0
    prepared = json.loads(capsys.readouterr().out)
    assert prepared["source"] == "EXPLICIT"
    assert prepared["created"] is True
    assert prepared["exists"] is True
    assert target.is_dir()


def test_read_only_day_and_post_close_status_ignore_gui_runtime_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_loaded() -> object:
        raise AssertionError("read-only status must not load integration settings")

    monkeypatch.setattr(cli, "load_integration_settings", fail_if_loaded)
    parser = build_parser()
    for argv in (
        ["ashare-paper-day", "status"],
        ["ashare-post-close", "status"],
        ["ashare-post-close", "report"],
    ):
        parsed = parser.parse_args(argv)
        cli._apply_integration_runtime_defaults(parsed)
        assert parsed.base_url is None


def test_gui_shared_llm_provider_and_models_drive_production_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = SimpleNamespace(
        llm_provider="openai",
        deepseek_model="deepseek-from-gui",
        openai_model="gpt-from-gui",
        onebot_url="http://127.0.0.1:3456",
        napcat_runtime="vendor/NapCat",
    )
    monkeypatch.setattr(cli, "load_integration_settings", lambda: settings)
    parser = build_parser()

    paper = parser.parse_args(["ashare-paper-day", "run"])
    cli._apply_integration_runtime_defaults(paper)
    assert paper.intraday_llm_provider == "openai"
    assert paper.intraday_llm_model == "gpt-from-gui"
    assert paper.base_url == "http://127.0.0.1:3456"

    live = parser.parse_args(["live-sync", "cycle"])
    cli._apply_integration_runtime_defaults(live)
    assert live.llm_provider == "openai"
    assert live.llm_model == "gpt-from-gui"
    assert live.base_url == "http://127.0.0.1:3456"

    live_ingest = parser.parse_args(["live-sync", "ingest"])
    cli._apply_integration_runtime_defaults(live_ingest)
    assert live_ingest.base_url == "http://127.0.0.1:3456"
    assert live_ingest.llm_provider is None
    assert live_ingest.llm_model is None

    close = parser.parse_args(["ashare-close-research-once"])
    cli._apply_integration_runtime_defaults(close)
    assert close.macro_provider == "openai"
    assert close.model == "gpt-from-gui"

    explicit = parser.parse_args(["ashare-close-research-once", "--macro-provider", "deepseek"])
    cli._apply_integration_runtime_defaults(explicit)
    assert explicit.macro_provider == "deepseek"
    assert explicit.model == "deepseek-from-gui"


def test_ashare_paper_day_parser_defaults_and_main_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = build_parser()
    defaults = parser.parse_args(["ashare-paper-day", "status"])
    assert defaults.runtime_dir == "runtime/paper/day"
    assert defaults.session_date is None
    assert defaults.account == "ashare-paper-day"
    assert defaults.initial_cash == Decimal("200000")
    assert defaults.base_url is None
    cli._apply_integration_runtime_defaults(defaults)
    # 只读 status 不依赖 GUI 配置文件，即使该文件损坏也应可诊断运行状态。
    assert defaults.base_url is None
    assert defaults.intraday_llm_model is None
    assert defaults.confirm is None
    assert defaults.recover_after_abort is False
    assert defaults.maximum_positions is None
    assert defaults.confirm_risk_policy_change is None
    assert defaults.report_artifact_recovery_action is None
    assert defaults.report_artifact_provider_id is None
    assert defaults.confirm_report_artifact_recovery is None
    assert defaults.intraday_llm is True
    assert defaults.intraday_llm_review_top_n == 6
    assert defaults.intraday_llm_review_ttl_minutes == 20
    assert defaults.intraday_llm_max_calls is None
    assert defaults.intraday_llm_events_db == "runtime/news/events.sqlite3"

    captured: list[tuple[object, ...]] = []

    async def fake_paper_day(*args: object) -> dict[str, object]:
        captured.append(args)
        return {"ok": True, "action": "run"}

    monkeypatch.setattr(cli, "_ashare_paper_day", fake_paper_day)
    assert (
        cli.main(
            [
                "ashare-paper-day",
                "run",
                "--runtime-dir",
                "paper-runtime",
                "--session-date",
                "2026-08-14",
                "--account",
                "day-account",
                "--initial-cash",
                "200000",
                "--target-kind",
                "private",
                "--target-id",
                "12345",
                "--confirm",
                "PAPER_DAY",
                "--recover-after-abort",
                "--maximum-positions",
                "7",
                "--confirm-risk-policy-change",
                "PAPER_RISK_POLICY_CHANGE",
                "--no-intraday-llm",
                "--intraday-llm-review-top-n",
                "4",
                "--intraday-llm-review-ttl-minutes",
                "15",
                "--intraday-llm-max-calls",
                "40",
                "--intraday-llm-events-db",
                "frozen-events.sqlite3",
            ]
        )
        == 0
    )
    assert captured == [
        (
            "run",
            "paper-runtime",
            date(2026, 8, 14),
            "day-account",
            Decimal("200000"),
            "private",
            "12345",
            "http://127.0.0.1:3000",
            "PAPER_DAY",
            True,
            7,
            "PAPER_RISK_POLICY_CHANGE",
            False,
            4,
            15,
            40,
            "frozen-events.sqlite3",
                "deepseek",
                "deepseek-v4-flash",
                None,
                None,
                None,
            )
    ]
    assert json.loads(capsys.readouterr().out) == {"action": "run", "ok": True}


@pytest.mark.parametrize("status", ("PENDING", "AMBIGUOUS", "NOT_CONFIGURED"))
def test_paper_day_cli_never_reports_ok_when_daily_review_artifact_is_incomplete(
    status: str,
) -> None:
    projected = cli._paper_day_delivery_projection(
        SimpleNamespace(
            completed=True,
            notification_required=2,
            notification_sent=1,
            notification_gaps=1,
            text_notification_required=1,
            text_notification_sent=1,
            text_notification_gaps=0,
            artifact_delivery_status=status,
            artifact_delivery_complete=False,
            daily_review_delivery_complete=False,
        )
    )

    assert projected["ok"] is False
    assert projected["artifact_delivery_status"] == status
    assert projected["artifact_delivery_complete"] is False
    assert projected["notification_gaps"] == 1


def test_paper_day_cli_missing_artifact_projection_fails_closed() -> None:
    projected = cli._paper_day_delivery_projection(
        SimpleNamespace(
            completed=True,
            notification_required=0,
            notification_sent=0,
            notification_gaps=0,
        )
    )

    assert projected["ok"] is False
    assert projected["artifact_delivery_status"] == "LEGACY_UNREPORTED"
    assert projected["artifact_delivery_complete"] is False
    assert projected["daily_review_delivery_complete"] is False


def test_paper_day_artifact_recovery_requires_explicit_consistent_confirmation() -> None:
    parsed = build_parser().parse_args(
        [
            "ashare-paper-day",
            "run",
            "--report-artifact-recovery-action",
            "MARK_SENT_AFTER_PROVIDER_VERIFICATION",
            "--report-artifact-provider-id",
            "provider-file-1",
            "--confirm-report-artifact-recovery",
            "PAPER_REPORT_ARTIFACT_RECOVERY",
        ]
    )
    assert parsed.report_artifact_recovery_action == (
        "MARK_SENT_AFTER_PROVIDER_VERIFICATION"
    )
    assert parsed.report_artifact_provider_id == "provider-file-1"
    assert parsed.confirm_report_artifact_recovery == "PAPER_REPORT_ARTIFACT_RECOVERY"

    with pytest.raises(ValueError, match="only available for run"):
        asyncio.run(
            cli._ashare_paper_day(
                "status",
                "runtime/paper/day",
                date(2026, 8, 14),
                "account",
                Decimal("200000"),
                None,
                None,
                "http://127.0.0.1:3000",
                None,
                report_artifact_recovery_action=(
                    "RESEND_AFTER_PROVIDER_NON_RECEIPT_VERIFICATION"
                ),
                report_artifact_recovery_confirmation=(
                    "PAPER_REPORT_ARTIFACT_RECOVERY"
                ),
            )
        )
    with pytest.raises(ValueError, match="requires explicit confirmation"):
        asyncio.run(
            cli._ashare_paper_day(
                "run",
                "runtime/paper/day",
                date(2026, 8, 14),
                "account",
                Decimal("200000"),
                "private",
                "10001",
                "http://127.0.0.1:3000",
                "PAPER_DAY",
                report_artifact_recovery_action=(
                    "RESEND_AFTER_PROVIDER_NON_RECEIPT_VERIFICATION"
                ),
            )
        )


def test_ashare_paper_day_main_returns_nonzero_for_operational_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def fake_paper_day(*_args: object) -> dict[str, object]:
        return {"ok": False, "error_code": "PREFLIGHT_FAILED"}

    monkeypatch.setattr(cli, "_ashare_paper_day", fake_paper_day)
    assert cli.main(["ashare-paper-day", "status"]) == 1
    assert json.loads(capsys.readouterr().out) == {
        "error_code": "PREFLIGHT_FAILED",
        "ok": False,
    }


def test_post_close_research_adapter_serializes_the_complete_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = 0
    maximum_active = 0

    async def fake_serial(
        _self: object,
        position: object,
        *,
        sessions: object,
    ) -> object:
        nonlocal active, maximum_active
        del sessions
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return position

    monkeypatch.setattr(
        cli._CLIExistingCloseResearch,
        "_research_serial",
        fake_serial,
    )
    adapter = cli._CLIExistingCloseResearch(
        profiles={},
        history_days=540,
        news_runtime_dir="runtime/news",
        research_db="runtime/research/research.sqlite3",
        market_evidence_dir="runtime/research/market_evidence",
        analysis_outbox_db="runtime/research/analysis-outbox.sqlite3",
        news_feeds=None,
        refresh_news=True,
        search_discovery=True,
        searxng_url=None,
        macro_enabled=True,
        macro_provider="deepseek",
        model="deepseek-v4-flash",
        macro_weight=Decimal("0.25"),
    )
    positions = tuple(SimpleNamespace(symbol=f"{index:06d}.SZ") for index in range(5))

    async def run_all() -> tuple[object, ...]:
        return tuple(
            await asyncio.gather(
                *(adapter.research(position, sessions=object()) for position in positions)
            )
        )

    assert asyncio.run(run_all()) == positions
    assert maximum_active == 1


def test_post_close_cli_adapter_projects_dual_track_audit_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_close(*_args: object) -> dict[str, object]:
        return {
            "ok": True,
            "decision": "WATCH",
            "technical_score": "0.250",
            "reference_price": "10.500",
            "invalidation_price": "9.800",
            "reason_codes": ["HELD_POSITION_REVIEW"],
            "uncertainties": [],
            "daily_bar_count": 180,
            "macro_dual_track": {
                "analysis_id": "analysis-dual-track-1",
                "selected_track": "ADVERSARIAL",
                "audit_record_sha256": "c" * 64,
                "baseline": {
                    "decision": "PUBLISH",
                    "regime": "温和",
                    "macro_impact": "0.300",
                    "evidence_coverage": "0.500",
                    "model_version": "baseline-model",
                },
                "adversarial": {
                    "decision": "WATCH",
                    "regime": "谨慎",
                    "macro_impact": "-0.100",
                    "evidence_coverage": "0.750",
                    "model_version": "adversarial-model",
                },
            },
        }

    monkeypatch.setattr(cli, "_ashare_close_research_once", fake_close)
    adapter = cli._CLIExistingCloseResearch(
        profiles={"600000.SH": SimpleNamespace(symbol="600000.SH")},
        history_days=540,
        news_runtime_dir="runtime/news",
        research_db="runtime/research/research.sqlite3",
        market_evidence_dir="runtime/research/market_evidence",
        analysis_outbox_db="runtime/research/analysis-outbox.sqlite3",
        news_feeds=None,
        refresh_news=True,
        search_discovery=True,
        searxng_url=None,
        macro_enabled=True,
        macro_provider="deepseek",
        model="deepseek-v4-flash",
        macro_weight=Decimal("0.25"),
    )
    sessions = SimpleNamespace(
        latest_completed_session=date(2026, 8, 14),
        next_session=date(2026, 8, 17),
    )

    result = asyncio.run(
        adapter._research_serial(
            SimpleNamespace(symbol="600000.SH"),
            sessions=sessions,
        )
    )

    assert result.baseline_macro_decision == "PUBLISH"
    assert result.baseline_macro_score == Decimal("0.300")
    assert result.baseline_macro_evidence_coverage == Decimal("0.500")
    assert result.adversarial_macro_decision == "WATCH"
    assert result.adversarial_macro_score == Decimal("-0.100")
    assert result.adversarial_macro_evidence_coverage == Decimal("0.750")
    assert result.macro_analysis_id == "analysis-dual-track-1"
    assert result.macro_selected_track == "ADVERSARIAL"
    assert result.macro_audit_record_sha256 == "c" * 64


def test_macro_track_document_calculates_its_own_evidence_coverage() -> None:
    from gribuki_trade.analysis.schemas import (
        MacroAnalysis,
        MacroAnalysisDecision,
        MacroClaim,
    )

    analysis = MacroAnalysis(
        analysis_id="analysis-track-1",
        as_of=datetime(2026, 8, 14, tzinfo=UTC),
        decision=MacroAnalysisDecision.PUBLISH,
        regime="中性",
        technical_alignment=Decimal("0.2"),
        macro_impact=Decimal("0.3"),
        scenarios=(),
        claims=(MacroClaim("证据支持", ("evidence-1",), ()),),
        uncertainties=(),
        data_gaps=(),
        invalidation_conditions=("证据修订",),
        reported_confidence="UNCALIBRATED",
        refusal_reason="",
        model_version="model-1",
    )

    document = cli._macro_track_document(
        analysis,
        (
            SimpleNamespace(evidence_id="evidence-1"),
            SimpleNamespace(evidence_id="evidence-2"),
        ),
    )

    assert document is not None
    assert document["analysis_id"] == "analysis-track-1"
    assert document["evidence_coverage"] == "0.500"
    assert document["macro_impact"] == "0.300"
    assert document["model_version"] == "model-1"


def test_post_close_parser_defaults_and_main_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    defaults = build_parser().parse_args(["ashare-post-close", "status"])
    assert defaults.runtime_dir == "runtime/paper/day"
    assert defaults.account == "ashare-paper-day"
    assert defaults.history_days == 540
    assert defaults.refresh_news is True
    assert defaults.search_discovery is True
    assert defaults.macro is True
    assert defaults.macro_provider is None
    assert defaults.recover_analysis is False
    assert defaults.recover_delivery is False

    captured: list[tuple[object, ...]] = []

    async def fake_post_close(*args: object) -> dict[str, object]:
        captured.append(args)
        return {"action": "run", "ok": True}

    monkeypatch.setattr(cli, "_ashare_post_close", fake_post_close)
    assert (
        cli.main(
            [
                "ashare-post-close",
                "run",
                "--target-kind",
                "private",
                "--target-id",
                "123456789",
                "--recover-analysis",
                "--recover-delivery",
                "--confirm",
                "POST_CLOSE",
            ]
        )
        == 0
    )
    assert captured[0][0] == "run"
    assert captured[0][-3:] == (
        "POST_CLOSE",
        True,
        True,
    )
    assert json.loads(capsys.readouterr().out) == {"action": "run", "ok": True}


def test_post_close_main_returns_nonzero_for_operational_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def fake_post_close(*_args: object) -> dict[str, object]:
        return {"error_code": "ALL_HELD_RESEARCH_FAILED", "ok": False}

    monkeypatch.setattr(cli, "_ashare_post_close", fake_post_close)
    assert cli.main(["ashare-post-close", "status"]) == 1
    assert json.loads(capsys.readouterr().out) == {
        "error_code": "ALL_HELD_RESEARCH_FAILED",
        "ok": False,
    }


def test_post_close_os_scheduler_actions_are_not_user_facing_cli_actions() -> None:
    parser = cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["ashare-post-close", "schedule-install"])
    with pytest.raises(SystemExit):
        parser.parse_args(["ashare-post-close", "schedule-status"])


@pytest.mark.parametrize(
    "value",
    (
        "ftp://search.example.invalid/private-prefix",
        "https:///private-prefix",
        "https://user:password@search.example.invalid/private-prefix",
        "https://search.example.invalid/private-prefix?token=not-for-manifest",
        "https://search.example.invalid/private-prefix#not-for-manifest",
    ),
)
def test_post_close_searxng_url_is_rejected_before_post_root_write(
    tmp_path: Path,
    value: str,
) -> None:
    session_root = tmp_path / "2026-08-14"

    with pytest.raises(cli._PostCloseCLIError) as captured:
        asyncio.run(
            cli._ashare_post_close_run(
                session_root=session_root,
                session_date=date(2026, 8, 14),
                account_id="ashare-paper-day",
                target_kind_value="private",
                target_id="123456789",
                base_url="http://127.0.0.1:3000",
                candidate_db="runtime/research/candidates.sqlite3",
                history_days=540,
                news_runtime_dir="runtime/news",
                research_db="runtime/research/research.sqlite3",
                market_evidence_dir="runtime/research/market_evidence",
                news_feeds=None,
                refresh_news=True,
                search_discovery=True,
                searxng_url=value,
                macro_enabled=True,
                macro_provider="deepseek",
                model="deepseek-v4-flash",
                macro_weight=Decimal("0.25"),
                dispatch_cycles=3,
                dispatch_poll_interval=2.0,
                recover_analysis=False,
                recover_delivery=False,
                now=datetime(2026, 8, 14, 16, 0, tzinfo=UTC),
            )
        )

    assert captured.value.code == "SEARXNG_URL_INVALID"
    assert not (session_root / "post-close").exists()


def test_post_close_searxng_manifest_record_redacts_path_and_keeps_runtime_url() -> None:
    sensitive_path = "/private-tenants/one-7f5f80f8/search"
    original = f"HTTPS://Searx.Example.Invalid{sensitive_path}"
    validated = cli.validate_post_close_searxng_url(original)
    canonical = cli.validate_post_close_searxng_url(
        f"https://searx.example.invalid{sensitive_path}"
    )

    assert validated is not None
    assert canonical is not None
    assert cli.validate_post_close_searxng_url(None) is None
    assert validated.runtime_url == original
    assert validated.manifest_document == canonical.manifest_document
    assert validated.manifest_document["origin"] == "https://searx.example.invalid"
    assert (
        validated.manifest_document["path_sha256"]
        == hashlib.sha256(sensitive_path.encode("utf-8")).hexdigest()
    )

    manifest_stdout = json.dumps(
        {
            "config": {"searxng_url": validated.manifest_document},
            "ok": True,
        },
        sort_keys=True,
    )
    assert sensitive_path not in manifest_stdout
    assert original not in manifest_stdout


@pytest.mark.parametrize(
    ("held", "completed", "failed", "expected"),
    [
        (0, 0, 0, "NOT_APPLICABLE"),
        (2, 2, 0, "COMPLETE"),
        (2, 1, 1, "PARTIAL"),
        (2, 0, 2, "FAILED"),
    ],
)
def test_post_close_analysis_outcome_is_explicit(
    held: int,
    completed: int,
    failed: int,
    expected: str,
) -> None:
    assert (
        cli._post_close_analysis_outcome(
            held_count=held,
            research_completed=completed,
            research_failed=failed,
        )
        == expected
    )


def test_post_close_analysis_outcome_rejects_missing_results() -> None:
    with pytest.raises(cli._PostCloseCLIError) as captured:
        cli._post_close_analysis_outcome(
            held_count=2,
            research_completed=1,
            research_failed=0,
        )
    assert captured.value.code == "POST_CLOSE_RESEARCH_RESULT_INVALID"


def test_post_close_delivery_uses_one_contract_summary_and_keeps_markdown_long_form() -> None:
    contract = REPORT_CONTRACTS[ReportKind.DAILY_REVIEW]
    original = render_stable_markdown_report(
        ReportKind.DAILY_REVIEW,
        title="盘后长报告",
        sections={name: f"{name}：" + "研究正文" * 500 for name in contract.required_sections},
    )

    summary = cli._post_close_delivery_summary(
        original,
        artifact_name="盘后长报告.md",
        artifact_sha256="a" * 64,
        run_id="post-close-run-0123456789",
    )

    validate_text_report_contract(ReportKind.DAILY_REVIEW, summary)
    assert len(summary) <= 7_000
    assert len(summary) < len(original)
    assert "完整报告文件：盘后长报告.md" in summary
    assert "内容摘要：aaaaaaaaaaaa" in summary
    assert "Post-close report" not in summary


def test_post_close_profile_uses_only_exact_paper_archive_fields() -> None:
    projection = SimpleNamespace(
        session_date=date(2026, 8, 14),
        events=(
            SimpleNamespace(
                event_type="SURVEILLANCE_SCAN_COMPLETED",
                payload={
                    "candidates": [
                        {"symbol": "600000.SH", "name": "浦发银行"},
                    ]
                },
            ),
            SimpleNamespace(
                event_type="WATCHLIST_UPDATED",
                payload={
                    "watchlist": [
                        {
                            "symbol": "600000.SH",
                            "name": "浦发银行",
                            "board": "SSE_MAIN",
                        }
                    ]
                },
            ),
            SimpleNamespace(
                event_type="FILL_STARTED",
                payload={
                    "fill": {
                        "symbol": "600000.SH",
                        "instrument_type": "stock",
                    }
                },
            ),
        ),
    )

    profiles = cli._paper_session_instrument_profiles(
        projection,
        {"600000.SH": "stock"},
    )

    assert tuple(profiles) == ("600000.SH",)
    profile = profiles["600000.SH"]
    assert profile.name == "浦发银行"
    assert profile.exchange == "sse"
    assert profile.board == "sse_main"
    assert profile.asset_type == "stock"
    assert profile.industry == "unknown-not-provided"
    assert profile.size_tier == "unknown-not-provided"
    assert profile.source_id == "PAPER_SESSION_ARCHIVE"
    assert "DEGRADED_PROFILE" in profile.risk_tags


def test_post_close_profile_fails_closed_on_archive_type_conflict() -> None:
    projection = SimpleNamespace(
        session_date=date(2026, 8, 14),
        events=(
            SimpleNamespace(
                event_type="WATCHLIST_UPDATED",
                payload={
                    "watchlist": [
                        {
                            "symbol": "600000.SH",
                            "name": "浦发银行",
                            "board": "SSE_MAIN",
                        }
                    ]
                },
            ),
            SimpleNamespace(
                event_type="FILL_STARTED",
                payload={
                    "fill": {
                        "symbol": "600000.SH",
                        "instrument_type": "etf",
                    }
                },
            ),
        ),
    )

    assert (
        cli._paper_session_instrument_profiles(
            projection,
            {"600000.SH": "stock"},
        )
        == {}
    )


def test_ashare_paper_day_run_requires_confirmation_and_target_without_io(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        cli,
        "_required_local_secret",
        lambda _name: pytest.fail("secret lookup must not run before validation"),
    )
    now = datetime.fromisoformat("2026-08-14T08:00:00+08:00")
    unconfirmed = asyncio.run(
        _ashare_paper_day(
            "run",
            str(tmp_path),
            None,
            "account",
            Decimal("200000"),
            "private",
            "12345",
            "http://127.0.0.1:3000",
            None,
            now=now,
        )
    )
    assert unconfirmed["error_code"] == "PAPER_DAY_CONFIRMATION_REQUIRED"
    missing_target = asyncio.run(
        _ashare_paper_day(
            "run",
            str(tmp_path),
            None,
            "account",
            Decimal("200000"),
            None,
            None,
            "http://127.0.0.1:3000",
            "PAPER_DAY",
            now=now,
        )
    )
    assert missing_target["error_code"] == "NOTIFICATION_TARGET_REQUIRED"
    assert not tmp_path.joinpath("2026-08-14").exists()


def test_ashare_paper_day_required_llm_missing_key_fails_before_session_db(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from gribuki_trade.ports.market_data import TradeCalendarDay

    class Calendar:
        async def fetch_trade_calendar_async(
            self, start: date, end: date
        ) -> tuple[TradeCalendarDay, ...]:
            return tuple(
                TradeCalendarDay(day, day.weekday() < 5)
                for day in (
                    start + timedelta(days=offset) for offset in range((end - start).days + 1)
                )
            )

    def secret(name: str) -> str:
        if name == cli.NAPCAT_ACCESS_TOKEN_SECRET:
            return "napcat-token"
        raise RuntimeError("missing")

    async def forbidden_run(**_kwargs: object) -> dict[str, object]:
        pytest.fail("runtime must not start without the required LLM key")

    monkeypatch.setattr(cli, "_required_local_secret", secret)
    monkeypatch.setattr(cli, "_run_ashare_paper_day", forbidden_run)
    result = asyncio.run(
        _ashare_paper_day(
            "run",
            str(tmp_path),
            None,
            "account",
            Decimal("200000"),
            "private",
            "12345",
            "http://127.0.0.1:3000",
            "PAPER_DAY",
            now=datetime.fromisoformat("2026-08-14T08:00:00+08:00"),
            calendar_provider=Calendar(),
        )
    )

    assert result["error_code"] == "INTRADAY_LLM_API_KEY_NOT_CONFIGURED"
    assert not tmp_path.joinpath("2026-08-14").exists()


def test_ashare_paper_day_status_and_report_are_sidecar_only(tmp_path: Path) -> None:
    session = date(2026, 8, 14)
    root = tmp_path / session.isoformat()
    root.mkdir()
    status = {"phase": "MORNING", "event_count": 8}
    (root / "status.json").write_text(json.dumps(status), encoding="utf-8")
    report_dir = root / "reports"
    report_dir.mkdir()
    report = report_dir / "ashare-paper-day-2026-08-14-abc.md"
    report.write_text("# A-share PAPER\n\ncomplete\n", encoding="utf-8")

    status_result = _ashare_paper_day_status(root, session)
    report_result = _ashare_paper_day_report(root, session)

    assert status_result["ok"] is True
    assert status_result["read_only_sidecar"] is True
    assert status_result["status"] == status
    assert status_result["artifact_delivery_status"] == "NOT_REPORTED"
    assert status_result["daily_review_delivery_complete"] is False
    assert status_result["operationally_complete"] is False
    assert report_result["ok"] is True
    assert report_result["read_only_sidecar"] is True
    metadata = report_result["report"]
    assert isinstance(metadata, dict)
    assert metadata["path"] == str(report.resolve())
    assert metadata["bytes"] == len(report.read_bytes())
    assert metadata["line_count"] == 3
    assert len(str(metadata["sha256"])) == 64
    assert not tuple(root.glob("*.sqlite3"))


@pytest.mark.parametrize(
    ("artifact_status", "complete"),
    (("SENT", True), ("PENDING", False), ("AMBIGUOUS", False), ("NOT_CONFIGURED", False)),
)
def test_ashare_paper_day_status_projects_exact_daily_review_delivery(
    tmp_path: Path,
    artifact_status: str,
    complete: bool,
) -> None:
    session = date(2026, 8, 14)
    root = tmp_path / session.isoformat()
    root.mkdir()
    status = {
        "artifact_delivery_complete": artifact_status == "SENT",
        "artifact_delivery_status": artifact_status,
        "daily_review_delivery_complete": complete,
        "event_count": 12,
        "notification_gaps": 0 if complete else 1,
        "notification_required": 8,
        "notification_sent": 8 if complete else 7,
        "phase": "POST_CLOSE",
        "text_notification_gaps": 0,
        "text_notification_required": 7,
        "text_notification_sent": 7,
    }
    (root / "status.json").write_text(json.dumps(status), encoding="utf-8")

    result = _ashare_paper_day_status(root, session)

    assert result["ok"] is True  # 表示只读查询成功，不代表双交付完成。
    assert result["artifact_delivery_status"] == artifact_status
    assert result["daily_review_delivery_complete"] is complete
    assert result["operationally_complete"] is complete
    delivery = result["delivery"]
    assert isinstance(delivery, dict)
    assert delivery["projection_exact"] is True
    assert delivery["projection_source"] == "status.json"


def test_ashare_paper_day_status_falls_back_to_latest_artifact_sidecar_event(
    tmp_path: Path,
) -> None:
    session = date(2026, 8, 14)
    root = tmp_path / session.isoformat()
    root.mkdir()
    (root / "status.json").write_text(
        json.dumps({"phase": "POST_CLOSE", "event_count": 3}),
        encoding="utf-8",
    )
    events = (
        {"event_type": "REPORT_ARTIFACT_DELIVERY_PENDING", "payload": {}},
        {"event_type": "REPORT_ARTIFACT_DELIVERY_AMBIGUOUS", "payload": {}},
        {"event_type": "RUNNER_RESUMED", "payload": {}},
    )
    (root / "session.log.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in events),
        encoding="utf-8",
    )

    result = _ashare_paper_day_status(root, session)

    assert result["artifact_delivery_status"] == "AMBIGUOUS"
    assert result["daily_review_delivery_complete"] is False
    assert result["operationally_complete"] is False
    delivery = result["delivery"]
    assert isinstance(delivery, dict)
    assert delivery["projection_exact"] is False
    assert delivery["projection_source"] == "session.log.jsonl"


def test_verify_ashare_paper_day_calendar_is_complete_and_adjacent() -> None:
    from gribuki_trade.ports.market_data import TradeCalendarDay

    class Calendar:
        async def fetch_trade_calendar_async(
            self, start: date, end: date
        ) -> tuple[TradeCalendarDay, ...]:
            return tuple(
                TradeCalendarDay(day, day.weekday() < 5)
                for day in (
                    start + timedelta(days=offset) for offset in range((end - start).days + 1)
                )
            )

    previous = asyncio.run(
        _verify_ashare_paper_day_calendar(
            date(2026, 8, 14),
            now=datetime.fromisoformat("2026-08-14T08:00:00+08:00"),
            calendar_provider=Calendar(),
        )
    )
    assert previous == date(2026, 8, 13)


def test_ashare_paper_day_run_owns_resources_and_awake_guard(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import gribuki_trade.adapters.notifiers as notifiers
    import gribuki_trade.runtime as runtime
    import gribuki_trade.services.ashare_paper_day as paper_day_service

    lifecycle: list[str] = []
    runner_kwargs: list[dict[str, object]] = []

    class FakeAwakeGuard:
        def __enter__(self) -> "FakeAwakeGuard":
            lifecycle.append("awake-enter")
            return self

        def __exit__(self, *_args: object) -> None:
            lifecycle.append("awake-exit")

    class FakeNotifier:
        channel = "onebot"

        def __init__(self, config: object) -> None:
            lifecycle.append("notifier-created")
            self.config = config

        async def __aenter__(self) -> "FakeNotifier":
            lifecycle.append("notifier-enter")
            return self

        async def __aexit__(self, *_args: object) -> None:
            lifecycle.append("notifier-exit")

        async def get_status(self) -> dict[str, bool]:
            lifecycle.append("notifier-preflight")
            return {"good": True, "online": True}

    class FakeRunner:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            runner_kwargs.append(kwargs)
            lifecycle.append("runner-created")

        async def run(self) -> SimpleNamespace:
            manifest = self.kwargs["manifest"]
            paper = self.kwargs["paper"]
            snapshot = paper.open_account(  # type: ignore[union-attr]
                manifest.account_id,  # type: ignore[union-attr]
                initial_cash=manifest.initial_cash,  # type: ignore[union-attr]
                session_date=manifest.session_date,  # type: ignore[union-attr]
                opened_at=manifest.created_at,  # type: ignore[union-attr]
            )
            lifecycle.append("runner-ran")
            return SimpleNamespace(
                run_id=manifest.run_id,  # type: ignore[union-attr]
                session_date=manifest.session_date,  # type: ignore[union-attr]
                completed=True,
                event_count=0,
                notification_required=0,
                notification_sent=0,
                notification_gaps=0,
                artifact_delivery_status="SENT",
                artifact_delivery_complete=True,
                daily_review_delivery_complete=True,
                final_snapshot=snapshot,
                report_path=tmp_path / "report.md",
            )

    monkeypatch.setattr(runtime, "SystemAwakeGuard", FakeAwakeGuard)
    monkeypatch.setattr(notifiers, "OneBotNotifier", FakeNotifier)
    monkeypatch.setattr(paper_day_service, "ASharePaperDayRunner", FakeRunner)
    result = asyncio.run(
        cli._run_ashare_paper_day(
            session_root=tmp_path / "2026-08-14",
            session_date=date(2026, 8, 14),
            latest_completed_session=date(2026, 8, 13),
            account_id="day-account",
            initial_cash=Decimal("200000"),
            target_kind_value="private",
            target_id="12345",
            base_url="http://127.0.0.1:3000",
            access_token="dummy-token",
            started_at=datetime.fromisoformat("2026-08-14T08:00:00+08:00"),
            intraday_llm_enabled=False,
        )
    )

    assert result["ok"] is True
    assert result["execution_mode"] == "PAPER_ONLY_NO_BROKER"
    assert result["system_awake"] == "PROCESS_SCOPED_SYSTEM_ONLY"
    assert result["account"]["cash"] == "200000.00"  # type: ignore[index]
    assert runner_kwargs[0]["risk_config"].maximum_positions is None  # type: ignore[union-attr]
    manifest = runner_kwargs[0]["manifest"]
    assert manifest.config["intraday_risk_policy"]["maximum_positions"] is None  # type: ignore[union-attr,index]
    assert lifecycle == [
        "awake-enter",
        "notifier-created",
        "notifier-enter",
        "notifier-preflight",
        "runner-created",
        "runner-ran",
        "notifier-exit",
        "awake-exit",
    ]
    session_root = tmp_path / "2026-08-14"
    assert (session_root / "journal.sqlite3").is_file()
    assert (session_root / "outbox.sqlite3").is_file()
    assert (session_root / "ledger.sqlite3").is_file()


def test_ashare_paper_day_llm_wiring_freezes_once_and_replays_manifest_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import httpx

    import gribuki_trade.adapters.notifiers as notifiers
    import gribuki_trade.runtime as runtime
    import gribuki_trade.services.ashare_paper_day as paper_day_service
    from gribuki_trade.adapters.llm import DeepSeekChatMacroAnalyzer
    from gribuki_trade.analysis.schemas import (
        MacroAnalysis,
        MacroAnalysisDecision,
        MacroAnalysisRequest,
        MacroClaim,
    )
    from gribuki_trade.domain.events import NormalizedEvent, SourceTier
    from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
    from gribuki_trade.storage import SQLiteEventStore

    session = date(2099, 8, 14)
    first_start = datetime.fromisoformat("2099-08-14T08:00:00+08:00")
    events_path = tmp_path / "events.sqlite3"
    visible_at = first_start.astimezone(UTC) - timedelta(minutes=10)
    with SQLiteEventStore(events_path) as store:
        store.append(
            NormalizedEvent(
                source_id="official.test",
                canonical_url="https://official.example.test/sensitive",
                title="sensitive headline must stay out of the manifest",
                summary="sensitive summary must stay out of the manifest",
                event_type="macro",
                source_tier=SourceTier.OFFICIAL,
                first_seen_at=visible_at,
                retrieved_at=visible_at,
                available_at=visible_at,
                published_at=visible_at - timedelta(minutes=1),
                external_id="sensitive-event",
                entities=("510300.SH",),
            )
        )

    class Awake:
        def __enter__(self) -> "Awake":
            return self

        def __exit__(self, *_args: object) -> None:
            pass

    class Notifier:
        channel = "onebot"

        def __init__(self, _config: object) -> None:
            pass

        async def __aenter__(self) -> "Notifier":
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

        async def get_status(self) -> dict[str, bool]:
            return {"good": True, "online": True}

    identity = AnalyzerAuditIdentity(
        provider_id="test.provider",
        requested_model=cli.DEFAULT_DEEPSEEK_MODEL,
        adapter_version="test-adapter@1",
        prompt_version="test-prompt@1",
        prompt_schema_sha256="a" * 64,
    )

    class Analyzer:
        audit_identity = identity

        def __init__(self) -> None:
            self.calls = 0

        async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
            self.calls += 1
            evidence_id = request.evidence[0].evidence_id
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
                invalidation_conditions=("权威证据发生修订时失效",),
                reported_confidence="UNCALIBRATED",
                refusal_reason="",
                model_version=identity.requested_model,
            )

    analyzer = Analyzer()
    clients: list[httpx.AsyncClient] = []

    def analyzer_factory(
        _api_key: object,
        *,
        model: str,
        client: httpx.AsyncClient,
        **_kwargs: object,
    ) -> Analyzer:
        assert model == cli.DEFAULT_DEEPSEEK_MODEL
        clients.append(client)
        return analyzer

    runner_kwargs: list[dict[str, object]] = []

    class Runner:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            runner_kwargs.append(kwargs)

        async def run(self) -> SimpleNamespace:
            manifest = self.kwargs["manifest"]
            paper = self.kwargs["paper"]
            try:
                snapshot = paper.snapshot(manifest.account_id)  # type: ignore[union-attr]
            except Exception:
                snapshot = paper.open_account(  # type: ignore[union-attr]
                    manifest.account_id,  # type: ignore[union-attr]
                    initial_cash=manifest.initial_cash,  # type: ignore[union-attr]
                    session_date=manifest.session_date,  # type: ignore[union-attr]
                    opened_at=manifest.created_at,  # type: ignore[union-attr]
                )
            return SimpleNamespace(
                run_id=manifest.run_id,  # type: ignore[union-attr]
                session_date=manifest.session_date,  # type: ignore[union-attr]
                completed=True,
                event_count=0,
                notification_required=0,
                notification_sent=0,
                notification_gaps=0,
                artifact_delivery_status="SENT",
                artifact_delivery_complete=True,
                daily_review_delivery_complete=True,
                final_snapshot=snapshot,
                report_path=tmp_path / "report.md",
            )

    monkeypatch.setattr(runtime, "SystemAwakeGuard", Awake)
    monkeypatch.setattr(notifiers, "OneBotNotifier", Notifier)
    monkeypatch.setattr(paper_day_service, "ASharePaperDayRunner", Runner)
    monkeypatch.setattr(
        DeepSeekChatMacroAnalyzer,
        "for_intraday",
        staticmethod(analyzer_factory),
    )

    common = {
        "session_root": tmp_path / session.isoformat(),
        "session_date": session,
        "latest_completed_session": date(2099, 8, 13),
        "account_id": "day-account",
        "initial_cash": Decimal("200000"),
        "target_kind_value": "private",
        "target_id": "12345",
        "base_url": "http://127.0.0.1:3000",
        "access_token": "dummy-token",
        "intraday_llm_events_db": str(events_path),
        "intraday_llm_api_key": "secret-deepseek-key",
    }
    first = asyncio.run(
        cli._run_ashare_paper_day(started_at=first_start, **common)  # type: ignore[arg-type]
    )
    second = asyncio.run(
        cli._run_ashare_paper_day(  # type: ignore[arg-type]
            started_at=first_start + timedelta(minutes=30),
            **common,
        )
    )
    events_path.unlink()
    replay_mismatch = asyncio.run(
        cli._run_ashare_paper_day(  # type: ignore[arg-type]
            started_at=first_start + timedelta(hours=1),
            **common,
        )
    )

    assert first["run_id"] == second["run_id"] == replay_mismatch["run_id"]
    # 首次冻结会并行执行一条 baseline 与 FAST 的两个对抗角色；后续恢复不得重跑。
    assert analyzer.calls == 3
    assert runner_kwargs[0]["llm_preopen_context"] is not None
    assert runner_kwargs[1]["llm_preopen_context"] is None
    assert runner_kwargs[2]["llm_preopen_context"] is None
    assert runner_kwargs[2]["intraday_llm_plan_factory"].available is False  # type: ignore[union-attr]
    assert replay_mismatch["intraday_llm"] == {
        "enabled": True,
        "required_for_buy": True,
        "runtime_evidence_failure_code": ("LLM_EVIDENCE_SNAPSHOT_REPLAY_MISMATCH"),
        "runtime_evidence_status": "UNAVAILABLE",
    }
    first_manifest = runner_kwargs[0]["manifest"]
    second_manifest = runner_kwargs[1]["manifest"]
    assert first_manifest.run_id == second_manifest.run_id  # type: ignore[union-attr]
    binding = first_manifest.config["intraday_llm_evidence_snapshot"]  # type: ignore[union-attr]
    assert binding["evidence_as_of"] == (  # type: ignore[index]
        first_start.astimezone(UTC).isoformat()
    )
    assert all(client.is_closed for client in clients)
    retained = first_manifest.config_json + json.dumps(first, default=str)  # type: ignore[union-attr]
    for secret_text in (
        "secret-deepseek-key",
        "sensitive headline",
        "sensitive summary",
    ):
        assert secret_text not in retained


def test_ashare_paper_day_notification_preflight_fails_before_databases(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import gribuki_trade.adapters.notifiers as notifiers
    import gribuki_trade.runtime as runtime

    class FakeAwakeGuard:
        def __enter__(self) -> "FakeAwakeGuard":
            return self

        def __exit__(self, *_args: object) -> None:
            pass

    class OfflineNotifier:
        channel = "onebot"

        def __init__(self, _config: object) -> None:
            pass

        async def __aenter__(self) -> "OfflineNotifier":
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

        async def get_status(self) -> dict[str, bool]:
            return {"good": True, "online": False}

    monkeypatch.setattr(runtime, "SystemAwakeGuard", FakeAwakeGuard)
    monkeypatch.setattr(notifiers, "OneBotNotifier", OfflineNotifier)
    session_root = tmp_path / "2026-08-14"
    with pytest.raises(cli._PaperDayCLIError) as raised:
        asyncio.run(
            cli._run_ashare_paper_day(
                session_root=session_root,
                session_date=date(2026, 8, 14),
                latest_completed_session=date(2026, 8, 13),
                account_id="day-account",
                initial_cash=Decimal("200000"),
                target_kind_value="private",
                target_id="12345",
                base_url="http://127.0.0.1:3000",
                access_token="dummy-token",
                started_at=datetime.fromisoformat("2026-08-14T08:00:00+08:00"),
                intraday_llm_enabled=False,
            )
        )
    assert raised.value.code == "PAPER_DAY_NOTIFICATION_PREFLIGHT_FAILED"
    assert not tuple(session_root.glob("*.sqlite3"))


def test_ashare_paper_day_reuses_legacy_manifest_when_risk_policy_is_upgraded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """热更新风险策略时必须续写旧运行，绝不能创建重复运行。"""

    import gribuki_trade.adapters.notifiers as notifiers
    import gribuki_trade.runtime as runtime
    import gribuki_trade.services.ashare_paper_day as paper_day_service
    from gribuki_trade.domain.paper_day import (
        PaperDayRunManifest,
        paper_day_target_hash,
    )
    from gribuki_trade.services.ashare_paper_day import ASharePaperDayConfig
    from gribuki_trade.storage.paper_day import SQLitePaperDayStore

    session = date(2026, 8, 14)
    created_at = datetime.fromisoformat("2026-08-14T08:00:00+08:00")
    session_root = tmp_path / session.isoformat()
    session_root.mkdir()
    config = ASharePaperDayConfig(initial_cash=Decimal("200000"))
    legacy_config = {
        **config.audit_document(),
        "calendar_provider": "BaoStock",
        "calendar_verified": True,
        "latest_completed_session": "2026-08-13",
        "notification_channel": "onebot",
        "notification_preflight_policy": "GET_STATUS_GOOD_AND_ONLINE",
        "notification_preflight_required": True,
        "notification_target_kind": "private",
    }
    target_hash = paper_day_target_hash(
        channel="onebot",
        target_kind="private",
        target_id="12345",
    )
    legacy = PaperDayRunManifest.create(
        session_date=session,
        account_id="day-account",
        config=legacy_config,
        created_at=created_at,
        target_hash=target_hash,
        initial_cash=Decimal("200000"),
    )
    with SQLitePaperDayStore(session_root / "journal.sqlite3") as store:
        store.create_run(legacy)

    class Awake:
        def __enter__(self) -> "Awake":
            return self

        def __exit__(self, *_args: object) -> None:
            pass

    class Notifier:
        channel = "onebot"

        def __init__(self, _config: object) -> None:
            pass

        async def __aenter__(self) -> "Notifier":
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

        async def get_status(self) -> dict[str, bool]:
            return {"good": True, "online": True}

    class Runner:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        async def run(self) -> SimpleNamespace:
            manifest = self.kwargs["manifest"]
            assert manifest.run_id == legacy.run_id  # type: ignore[union-attr]
            risk_config = self.kwargs["risk_config"]
            assert risk_config.maximum_positions is None  # type: ignore[union-attr]
            paper = self.kwargs["paper"]
            snapshot = paper.open_account(  # type: ignore[union-attr]
                manifest.account_id,  # type: ignore[union-attr]
                initial_cash=manifest.initial_cash,  # type: ignore[union-attr]
                session_date=manifest.session_date,  # type: ignore[union-attr]
                opened_at=manifest.created_at,  # type: ignore[union-attr]
            )
            return SimpleNamespace(
                run_id=manifest.run_id,  # type: ignore[union-attr]
                session_date=manifest.session_date,  # type: ignore[union-attr]
                completed=True,
                event_count=0,
                notification_required=0,
                notification_sent=0,
                notification_gaps=0,
                artifact_delivery_status="SENT",
                artifact_delivery_complete=True,
                daily_review_delivery_complete=True,
                final_snapshot=snapshot,
                report_path=tmp_path / "report.md",
            )

    monkeypatch.setattr(runtime, "SystemAwakeGuard", Awake)
    monkeypatch.setattr(notifiers, "OneBotNotifier", Notifier)
    monkeypatch.setattr(paper_day_service, "ASharePaperDayRunner", Runner)

    result = asyncio.run(
        cli._run_ashare_paper_day(
            session_root=session_root,
            session_date=session,
            latest_completed_session=date(2026, 8, 13),
            account_id="day-account",
            initial_cash=Decimal("200000"),
            target_kind_value="private",
            target_id="12345",
            base_url="http://127.0.0.1:3000",
            access_token="dummy-token",
            started_at=created_at,
            intraday_llm_enabled=False,
        )
    )

    assert result["run_id"] == legacy.run_id
    with SQLitePaperDayStore(session_root / "journal.sqlite3") as store:
        assert tuple(item.run_id for item in store.list_runs()) == (legacy.run_id,)


def test_ashare_market_screen_parser_defaults_overrides_and_invalid_values() -> None:
    parser = build_parser()

    defaults = parser.parse_args(["ashare-market-screen-once"])
    assert defaults.top_n == 30
    assert defaults.factor_budget == 300
    assert defaults.min_listing_days == 250
    assert defaults.min_session_amount_cny == Decimal("20000000")
    assert defaults.min_average_amount_20_cny == Decimal("50000000")
    assert defaults.min_market_cap_cny == Decimal("2000000000")
    assert defaults.output is None
    assert defaults.candidate_db == "runtime/research/candidates.sqlite3"
    assert defaults.no_store_candidates is False
    assert defaults.run_db == "runtime/research/runs.sqlite3"
    assert defaults.no_store_run is False

    configured = parser.parse_args(
        [
            "ashare-market-screen-once",
            "--top-n",
            "12",
            "--factor-budget",
            "80",
            "--min-listing-days",
            "365",
            "--min-session-amount-cny",
            "30000000.50",
            "--min-average-amount-20-cny",
            "60000000",
            "--min-market-cap-cny",
            "5000000000",
        ]
    )
    assert configured.top_n == 12
    assert configured.factor_budget == 80
    assert configured.min_listing_days == 365
    assert configured.min_session_amount_cny == Decimal("30000000.50")
    assert configured.min_average_amount_20_cny == Decimal("60000000")
    assert configured.min_market_cap_cny == Decimal("5000000000")

    invalid_arguments = (
        ("--top-n", "0"),
        ("--factor-budget", "-1"),
        ("--min-listing-days", "-1"),
        ("--min-session-amount-cny", "-0.01"),
        ("--min-average-amount-20-cny", "NaN"),
        ("--min-market-cap-cny", "Infinity"),
    )
    for option, value in invalid_arguments:
        with pytest.raises(SystemExit):
            parser.parse_args(["ashare-market-screen-once", option, value])


def test_ashare_market_screen_main_dispatches_typed_arguments_and_prints_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: list[tuple[object, ...]] = []

    async def fake_screen(*args: object) -> dict[str, object]:
        captured.append(args)
        return {"ok": True, "status": "COMPLETE"}

    monkeypatch.setattr(cli, "_ashare_market_screen_once", fake_screen)

    assert (
        cli.main(
            [
                "ashare-market-screen-once",
                "--top-n",
                "7",
                "--factor-budget",
                "50",
                "--min-listing-days",
                "300",
                "--min-session-amount-cny",
                "25000000",
                "--min-average-amount-20-cny",
                "55000000",
                "--min-market-cap-cny",
                "3000000000",
            ]
        )
        == 0
    )
    assert captured == [
        (
            7,
            50,
            300,
            Decimal("25000000"),
            Decimal("55000000"),
            Decimal("3000000000"),
            "runtime/research/candidates.sqlite3",
            "runtime/research/runs.sqlite3",
        )
    ]
    assert json.loads(capsys.readouterr().out) == {
        "ok": True,
        "status": "COMPLETE",
    }


def test_ashare_market_screen_optionally_writes_atomic_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    async def fake_screen(*_args: object) -> dict[str, object]:
        return {"ok": True, "status": "COMPLETE"}

    monkeypatch.setattr(cli, "_ashare_market_screen_once", fake_screen)
    destination = tmp_path / "nested" / "screen.json"

    assert (
        cli.main(
            [
                "ashare-market-screen-once",
                "--output",
                str(destination),
            ]
        )
        == 0
    )

    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads(destination.read_text(encoding="utf-8"))
    assert stdout_payload == file_payload
    assert stdout_payload == {
        "ok": True,
        "output": str(destination.resolve()),
        "status": "COMPLETE",
    }
    assert tuple(destination.parent.glob(f".{destination.name}.*.tmp")) == ()


def test_ashare_intraday_scan_parser_and_main_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = build_parser()
    defaults = parser.parse_args(["ashare-intraday-scan-once"])
    assert defaults.top_n == 30
    assert defaults.min_session_amount_cny == Decimal("2000000")
    assert defaults.candidate_db == "runtime/research/candidates.sqlite3"
    assert defaults.no_store_candidates is False
    assert defaults.run_db == "runtime/research/runs.sqlite3"
    assert defaults.no_store_run is False

    captured: list[tuple[object, ...]] = []

    async def fake_scan(*args: object) -> dict[str, object]:
        captured.append(args)
        return {"ok": True, "status": "COMPLETE"}

    monkeypatch.setattr(cli, "_ashare_intraday_scan_once", fake_scan)
    assert (
        cli.main(
            [
                "ashare-intraday-scan-once",
                "--top-n",
                "12",
                "--min-session-amount-cny",
                "3000000",
                "--no-store-candidates",
                "--no-store-run",
            ]
        )
        == 0
    )
    assert captured == [(12, Decimal("3000000"), None, None)]
    assert json.loads(capsys.readouterr().out) == {
        "ok": True,
        "status": "COMPLETE",
    }


def test_ashare_intraday_scan_rejects_closed_market_without_provider() -> None:
    class ProviderMustNotRun:
        async def fetch_intraday_universe(self, **_kwargs: object) -> object:
            raise AssertionError("provider must not run while market is closed")

    result = asyncio.run(
        _ashare_intraday_scan_once(
            30,
            Decimal("2000000"),
            None,
            data_source=ProviderMustNotRun(),  # type: ignore[arg-type]
            requested_at=datetime.fromisoformat("2026-08-14T15:30:00+08:00"),
        )
    )

    assert result["ok"] is False
    assert result["error_code"] == "MARKET_NOT_OPEN"
    assert result["candidates"] == []


def test_strategy_factor_discover_parser_main_and_atomic_inventory(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    parser = build_parser()
    defaults = parser.parse_args(["strategy-factor-discover"])
    assert defaults.max_trials == 100
    assert defaults.output is None
    with pytest.raises(SystemExit):
        parser.parse_args(["strategy-factor-discover", "--max-trials", "0"])

    destination = tmp_path / "factor-inventory.json"
    assert (
        cli.main(
            [
                "strategy-factor-discover",
                "--max-trials",
                "100",
                "--output",
                str(destination),
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result == json.loads(destination.read_text(encoding="utf-8"))
    assert result["ok"] is True
    assert result["research_only"] is True
    assert result["online_configuration_changed"] is False
    assert result["market_data_accessed"] is False
    assert result["llm_accessed"] is False
    assert result["holdout_accessed"] is False
    assert len(result["grammar_sha256"]) == 64
    assert result["candidates"]
    assert result["candidates"][0]["grammar_sha256"] == result["grammar_sha256"]
    assert result["candidates"][0]["expression_sha256"]
    assert result["warnings"] == ["MULTIPLE_HYPOTHESIS_TESTING_REQUIRED"]


def test_strategy_exit_evaluate_parser_and_research_only_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parser = build_parser()
    parsed = parser.parse_args(
        [
            "strategy-exit-evaluate",
            "--dataset",
            "dataset.json",
            "--specification",
            "specification.json",
            "--output",
            "registry.json",
            "--confirm",
            "RESEARCH_ONLY",
        ]
    )
    assert parsed.command == "strategy-exit-evaluate"
    assert parsed.confirm == "RESEARCH_ONLY"

    invoked = False

    def fake_run(*_args: object, **_kwargs: object) -> object:
        nonlocal invoked
        invoked = True
        metrics = SimpleNamespace(
            maximum_drawdown=Decimal("0.1"),
            net_return=Decimal("0.2"),
        )
        registry = SimpleNamespace(
            baseline_holdout=SimpleNamespace(metrics=metrics),
            selected_holdout=SimpleNamespace(metrics=metrics),
            registry_sha256="a" * 64,
            selected_trial_id="exit-selected",
            trials=(1, 2),
        )
        return SimpleNamespace(
            artifact_sha256="b" * 64,
            dataset_file_sha256="c" * 64,
            destination=(tmp_path / "registry.json").resolve(),
            registry=registry,
            specification_file_sha256="d" * 64,
        )

    import gribuki_trade.strategy_lab as strategy_lab

    monkeypatch.setattr(strategy_lab, "run_frozen_exit_policy_experiment", fake_run)
    rejected = _strategy_exit_evaluate(
        dataset="dataset.json",
        specification="specification.json",
        output="registry.json",
        created_at=None,
        overwrite=False,
        confirmation=None,
    )
    assert rejected["error_code"] == "EXIT_POLICY_RESEARCH_CONFIRMATION_REQUIRED"
    assert invoked is False

    accepted = _strategy_exit_evaluate(
        dataset="dataset.json",
        specification="specification.json",
        output="registry.json",
        created_at=datetime(2026, 8, 14, tzinfo=UTC),
        overwrite=False,
        confirmation="RESEARCH_ONLY",
    )
    assert accepted["ok"] is True
    assert accepted["execution_authority"] is False
    assert accepted["promotion_authorized"] is False
    assert accepted["trial_count"] == 2
    assert invoked is True


def test_strategy_factor_discover_budget_failure_is_structured_and_stable() -> None:
    first = _strategy_factor_discover(1)
    second = _strategy_factor_discover(1)

    assert first == second
    assert first == {
        "ok": False,
        "error_code": "FACTOR_SEARCH_BUDGET_EXCEEDED",
        "grammar_version": "technical-factor-grammar@1",
        "grammar_sha256": first["grammar_sha256"],
        "max_trials": 1,
        "required_trials": 2,
        "research_only": True,
        "market_data_accessed": False,
        "llm_accessed": False,
        "holdout_accessed": False,
        "online_configuration_changed": False,
    }


def test_ashare_research_runs_parser_and_absent_database_is_never_created(
    tmp_path: Path,
) -> None:
    parser = build_parser()
    defaults = parser.parse_args(["ashare-research-runs", "list"])
    assert defaults.run_db == "runtime/research/runs.sqlite3"
    assert defaults.run_id is None
    assert defaults.run_type is None
    assert defaults.limit == 100

    absent = tmp_path / "absent" / "runs.sqlite3"
    result = _ashare_research_runs("list", str(absent), None, None, 100)
    assert result["ok"] is False
    assert result["error_code"] == "RESEARCH_RUN_DATABASE_NOT_FOUND"
    assert not absent.exists()
    assert not absent.parent.exists()


def test_ashare_research_runs_list_summary_and_get_full_documents(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    from gribuki_trade.storage import SQLiteResearchRunStore, research_run_id

    run_db = tmp_path / "runs.sqlite3"
    started = datetime.fromisoformat("2026-08-14T07:00:00+00:00")
    completed = started + timedelta(seconds=2)
    with SQLiteResearchRunStore(run_db) as store:
        assert store.append(
            run_type="ashare_close_research",
            logical_key="510300.SH/2026-08-14",
            status="COMPLETE",
            started_at=started,
            completed_at=completed,
            strategy_version="close-v2",
            source_revisions=(("baostock", "2026-08-13"),),
            config={"macro_weight": Decimal("0.25")},
            payload={"decision": "WATCH", "score": Decimal("0.123")},
        )
    expected_id = research_run_id("ashare_close_research", "510300.SH/2026-08-14")

    listed = _ashare_research_runs("list", str(run_db), None, "ashare_close_research", 5)
    assert listed["ok"] is True
    assert listed["count"] == 1
    summary = listed["runs"][0]
    assert summary["run_id"] == expected_id
    assert summary["status"] == "COMPLETE"
    assert "payload" not in summary
    assert "config" not in summary

    fetched = _ashare_research_runs("get", str(run_db), expected_id, None, 100)
    assert fetched["ok"] is True
    assert fetched["run"]["config"] == {"macro_weight": "0.25"}
    assert fetched["run"]["payload"] == {
        "decision": "WATCH",
        "score": "0.123",
    }

    assert (
        cli.main(
            [
                "ashare-research-runs",
                "get",
                "--run-db",
                str(run_db),
                "--run-id",
                expected_id,
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["run"]["payload"]["decision"] == "WATCH"

    missing = _ashare_research_runs("get", str(run_db), "f" * 64, None, 100)
    assert missing["error_code"] == "RESEARCH_RUN_NOT_FOUND"


def test_ashare_research_runs_rejects_action_specific_arguments(tmp_path: Path) -> None:
    path = str(tmp_path / "irrelevant.sqlite3")
    with pytest.raises(ValueError, match="--run-id is required"):
        _ashare_research_runs("get", path, None, None, 100)
    with pytest.raises(ValueError, match="--run-id is only valid"):
        _ashare_research_runs("list", path, "a" * 64, None, 100)
    with pytest.raises(ValueError, match="--run-type is only valid"):
        _ashare_research_runs("get", path, "a" * 64, "type", 100)


def test_ashare_candidates_manual_lifecycle_and_parser(tmp_path: Path) -> None:
    parser = build_parser()
    defaults = parser.parse_args(["ashare-candidates", "list"])
    assert defaults.candidate_db == "runtime/research/candidates.sqlite3"
    assert defaults.cooling_minutes == 240
    assert defaults.limit == 500

    candidate_db = str(tmp_path / "candidates.sqlite3")
    at = datetime.fromisoformat("2026-08-14T10:00:00+08:00")
    added = _ashare_candidates("add", "510300.SH", "MANUAL_TEST", candidate_db, 60, 10, now=at)
    assert added["mutation_appended"] is True
    assert added["candidates"][0]["status"] == "active"
    assert added["candidates"][0]["sources"] == ["manual"]

    cooled = _ashare_candidates(
        "cool",
        "510300.SH",
        "WAIT_FOR_CONFIRMATION",
        candidate_db,
        60,
        10,
        now=at + timedelta(minutes=1),
    )
    assert cooled["candidates"][0]["status"] == "cooling"

    listing = _ashare_candidates(
        "list", None, "unused", candidate_db, 60, 10, now=at + timedelta(minutes=1)
    )
    assert listing["candidates"] == []


def test_ashare_review_parser_and_main_dispatch_are_research_only(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = build_parser()
    defaults = parser.parse_args(["ashare-review", "list"])
    assert defaults.review_db == "runtime/research/reviews.sqlite3"
    assert defaults.research_db == "runtime/research/research.sqlite3"
    assert defaults.candidate_db == "runtime/research/candidates.sqlite3"
    assert defaults.limit == 100
    assert defaults.confirm is None

    captured: list[tuple[object, ...]] = []

    def fake_review(*args: object) -> dict[str, object]:
        captured.append(args)
        return {"ok": True, "execution_authorized": False}

    monkeypatch.setattr(cli, "_ashare_review", fake_review)
    assert (
        cli.main(
            [
                "ashare-review",
                "confirm",
                "--case-id",
                "a" * 64,
                "--reason",
                "MANUALLY_RECHECKED",
                "--confirm",
                "RESEARCH_ONLY",
            ]
        )
        == 0
    )
    assert captured == [
        (
            "confirm",
            "runtime/research/reviews.sqlite3",
            "runtime/research/research.sqlite3",
            "runtime/research/candidates.sqlite3",
            None,
            "a" * 64,
            ["MANUALLY_RECHECKED"],
            100,
            "RESEARCH_ONLY",
        )
    ]
    assert json.loads(capsys.readouterr().out)["execution_authorized"] is False


def test_ashare_review_cli_open_get_confirm_and_list_offline(tmp_path: Path) -> None:
    from dataclasses import replace

    from gribuki_trade.domain.recommendations import (
        ConfidenceBand,
        EvidenceReference,
        RecommendationDecision,
        RecommendationHorizon,
        ResearchRecommendation,
    )
    from gribuki_trade.storage.research_store import SQLiteResearchStore

    now = datetime.fromisoformat("2026-08-14T10:00:00+08:00")
    evidence_at = now - timedelta(minutes=10)
    recommendation = ResearchRecommendation(
        recommendation_id="review-cli-recommendation-1",
        symbol="510300.SH",
        as_of=now - timedelta(minutes=5),
        expires_at=now + timedelta(hours=2),
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        decision=RecommendationDecision.WATCH,
        confidence=ConfidenceBand.MEDIUM,
        technical_score=Decimal("0.25"),
        macro_score=Decimal("0.1"),
        reference_price=Decimal("4.729"),
        invalidation_price=Decimal("4.657"),
        reason_codes=("DETAILED_REVIEW_REQUIRED",),
        uncertainties=("NEXT_SESSION_NOT_OPEN",),
        evidence=(
            EvidenceReference(
                evidence_id="retained-market-evidence-1",
                title="Retained evidence",
                canonical_url="local://market-evidence/1",
                published_at=evidence_at,
                first_seen_at=evidence_at,
                source_tier=1,
            ),
        ),
        strategy_version="close-v1",
    )
    research_db = tmp_path / "research.sqlite3"
    with SQLiteResearchStore(research_db) as store:
        assert store.append_recommendation(recommendation)
        assert store.append_recommendation(
            replace(
                recommendation,
                recommendation_id="review-cli-recommendation-without-candidate",
            )
        )

    candidate_db = str(tmp_path / "candidates.sqlite3")
    _ashare_candidates(
        "add",
        "510300.SH",
        "MANUAL_REVIEW_CANDIDATE",
        candidate_db,
        60,
        10,
        now=now - timedelta(minutes=1),
    )
    review_db = str(tmp_path / "reviews.sqlite3")
    opened = _ashare_review(
        "open",
        review_db,
        str(research_db),
        candidate_db,
        recommendation.recommendation_id,
        None,
        None,
        100,
        None,
        now=now,
    )
    case = opened["cases"][0]
    assert opened["mutation_appended"] is True
    assert opened["execution_authorized"] is False
    assert case["status"] == "PENDING_REVIEW"
    assert case["evidence_count"] == 1
    assert case["candidate_provenance"]["sources"] == ["manual"]
    assert "retained-market-evidence-1" not in json.dumps(opened)

    fetched = _ashare_review(
        "get",
        review_db,
        str(research_db),
        candidate_db,
        recommendation.recommendation_id,
        None,
        None,
        100,
        None,
        now=now,
    )
    assert fetched["cases"][0]["case_id"] == case["case_id"]

    confirmed = _ashare_review(
        "confirm",
        review_db,
        str(research_db),
        candidate_db,
        None,
        case["case_id"],
        ("REPORT_MANUALLY_RECHECKED",),
        100,
        "RESEARCH_ONLY",
        now=now + timedelta(minutes=1),
    )
    assert confirmed["cases"][0]["status"] == "CONFIRMED"
    assert confirmed["cases"][0]["research_confirmed"] is True
    assert confirmed["cases"][0]["execution_authorized"] is False

    opened_without_candidate = _ashare_review(
        "open",
        review_db,
        str(research_db),
        str(tmp_path / "missing-candidates.sqlite3"),
        "review-cli-recommendation-without-candidate",
        None,
        None,
        100,
        None,
        now=now,
    )
    assert opened_without_candidate["cases"][0]["candidate_provenance"] is None

    listed = _ashare_review(
        "list",
        review_db,
        str(research_db),
        str(tmp_path / "missing-candidates.sqlite3"),
        None,
        None,
        None,
        10,
        None,
        now=now + timedelta(minutes=2),
    )
    assert {item["status"] for item in listed["cases"]} == {
        "CONFIRMED",
        "PENDING_REVIEW",
    }


def test_ashare_review_cli_rejects_confirmation_without_explicit_guard(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="--confirm RESEARCH_ONLY"):
        _ashare_review(
            "confirm",
            str(tmp_path / "reviews.sqlite3"),
            str(tmp_path / "research.sqlite3"),
            str(tmp_path / "candidates.sqlite3"),
            None,
            "a" * 64,
            None,
            100,
            None,
            now=datetime.now(UTC),
        )


def test_ashare_paper_cli_open_fill_t_plus_one_rollover(tmp_path: Path) -> None:
    parser = build_parser()
    defaults = parser.parse_args(["ashare-paper", "snapshot"])
    assert defaults.ledger_db == "runtime/paper/ashare-paper.sqlite3"
    assert defaults.account == "personal-paper"

    ledger = str(tmp_path / "paper.sqlite3")
    opened_at = datetime.fromisoformat("2026-08-14T09:00:00+08:00")
    opened = _ashare_paper(
        "open",
        ledger,
        "paper-test",
        Decimal("100000"),
        date(2026, 8, 14),
        None,
        None,
        None,
        None,
        None,
        "MANUAL",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        now=opened_at,
    )
    assert opened["account"]["cash"] == "100000.00"
    assert opened["matching_implemented"] is False

    executed_at = datetime.fromisoformat("2026-08-14T10:00:00+08:00")
    bought = _ashare_paper(
        "fill",
        ledger,
        "paper-test",
        None,
        date(2026, 8, 14),
        "600000.SH",
        "BUY",
        100,
        Decimal("10"),
        "STOCK",
        "MANUAL",
        "fill-1",
        executed_at,
        None,
        "manual test",
        None,
        None,
        None,
        now=executed_at,
    )
    position = bought["account"]["positions"][0]
    assert position["quantity"] == 100
    assert position["available_to_sell"] == 0
    assert position["today_buy"] == 100
    assert bought["fill_receipt"]["applied_new"] is True

    rolled = _ashare_paper(
        "rollover",
        ledger,
        "paper-test",
        None,
        date(2026, 8, 17),
        None,
        None,
        None,
        None,
        None,
        "MANUAL",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        now=datetime.fromisoformat("2026-08-17T09:00:00+08:00"),
    )
    position = rolled["account"]["positions"][0]
    assert position["available_to_sell"] == 100
    assert position["today_buy"] == 0


def test_ashare_market_screen_serializes_candidate_contributions_and_exclusions() -> None:
    from gribuki_trade.features.ashare_screening import (
        AShareFactorRanking,
        FactorEligibilityExclusion,
        FactorEligibilityReason,
        FactorObservationStatus,
        HardFilterExclusion,
        HardFilterReason,
        HardFilterResult,
        RankedAShareCandidate,
        ScreeningCandidateDataStatus,
        ScreeningDeferralReason,
        ScreeningDeferredRecord,
        ScreeningFactorContribution,
    )
    from gribuki_trade.ports.ashare_screening import (
        AShareBoard,
        ScreeningFactorId,
    )
    from gribuki_trade.services.ashare_screening import (
        AShareScreeningRun,
        AShareScreeningRunStatus,
    )

    contribution = ScreeningFactorContribution(
        factor_id=ScreeningFactorId.MOMENTUM_20,
        status=FactorObservationStatus.AVAILABLE,
        raw_value=0.20,
        winsorized_value=0.15,
        percentile_rank=0.90,
        directional_score=0.80,
        configured_weight=0.10,
        contribution=0.08,
        cross_section_observations=100,
    )
    candidate = RankedAShareCandidate(
        symbol="600001.SH",
        name="测试股份",
        board=AShareBoard.SSE_MAIN,
        industry="工业",
        rank=1,
        composite_score=0.32,
        factor_weight_coverage=1.0,
        data_status=ScreeningCandidateDataStatus.DEGRADED,
        degradation_reasons=("SOURCE_DEGRADED",),
        factor_contributions=(contribution,),
    )
    insufficient = RankedAShareCandidate(
        symbol="600002.SH",
        name="缺数股份",
        board=AShareBoard.SSE_MAIN,
        industry=None,
        rank=None,
        composite_score=None,
        factor_weight_coverage=0.60,
        data_status=ScreeningCandidateDataStatus.INSUFFICIENT,
        degradation_reasons=("INSUFFICIENT_FACTOR_WEIGHT_COVERAGE",),
        factor_contributions=(),
    )
    run = AShareScreeningRun(
        as_of=date(2026, 8, 14),
        decision_at=datetime.fromisoformat("2026-08-14T16:00:00+08:00"),
        status=AShareScreeningRunStatus.DEGRADED,
        strategy_version="ashare-cross-section@1",
        universe_source_id="universe-test",
        universe_source_revision="u1",
        factor_source_id="factor-test",
        factor_source_revision="f1",
        feature_version="feature@1",
        universe_count=10,
        hard_filter_eligible_count=8,
        factor_requested_count=7,
        eligible_count=5,
        ranked_count=4,
        hard_filter=HardFilterResult(
            eligible=(),
            excluded=(
                HardFilterExclusion(
                    symbol="600003.SH",
                    name="ST测试",
                    reasons=(HardFilterReason.ST_SECURITY,),
                ),
            ),
        ),
        factor_budget_deferred=(
            ScreeningDeferredRecord(
                symbol="600004.SH",
                name="延后股份",
                reason=ScreeningDeferralReason.FACTOR_BUDGET_DEFERRED,
            ),
        ),
        factor_ranking=AShareFactorRanking(
            ranked_candidates=(candidate,),
            insufficient_candidates=(insufficient,),
            factor_eligibility_exclusions=(
                FactorEligibilityExclusion(
                    symbol="600005.SH",
                    name="低流动性",
                    reasons=(FactorEligibilityReason.LOW_AVERAGE_AMOUNT_20,),
                ),
            ),
            globally_unavailable_factors=(ScreeningFactorId.MOMENTUM_60,),
        ),
        top_candidates=(candidate,),
        warnings=("UNIVERSE_SOURCE_DEGRADED",),
    )

    payload = cli._ashare_screening_run_json(run)

    assert payload["as_of"] == "2026-08-14"
    assert payload["status"] == "DEGRADED"
    assert payload["source"] == {
        "factors": {
            "feature_version": "feature@1",
            "source_id": "factor-test",
            "source_revision": "f1",
        },
        "universe": {"source_id": "universe-test", "source_revision": "u1"},
    }
    top = payload["top_candidates"]
    assert isinstance(top, list)
    assert top[0]["symbol"] == "600001.SH"
    assert top[0]["score"] == 0.32
    assert top[0]["coverage"] == 1.0
    assert top[0]["factor_contributions"] == [
        {
            "contribution": 0.08,
            "cross_section_observations": 100,
            "directional_score": 0.80,
            "factor": "MOMENTUM_20",
            "percentile_rank": 0.90,
            "raw_value": 0.20,
            "status": "AVAILABLE",
            "weight": 0.10,
            "winsorized_value": 0.15,
        }
    ]
    exclusions = payload["exclusions"]
    assert exclusions["hard_filter"] == {
        "by_reason": {"ST_SECURITY": 1},
        "count": 1,
    }
    assert exclusions["factor_eligibility"] == {
        "by_reason": {"LOW_AVERAGE_AMOUNT_20": 1},
        "count": 1,
    }
    assert exclusions["factor_budget_deferred"] == {
        "count": 1,
        "reason": "FACTOR_BUDGET_DEFERRED",
    }


def test_ashare_market_screen_rejects_preclose_without_touching_provider() -> None:
    class ProviderMustNotRun:
        async def fetch_universe_snapshot(self, **_kwargs: object) -> object:
            raise AssertionError("provider must not run before the close boundary")

        async def fetch_factor_snapshot(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("provider must not run before the close boundary")

    result = asyncio.run(
        _ashare_market_screen_once(
            30,
            300,
            250,
            Decimal("20000000"),
            Decimal("50000000"),
            Decimal("2000000000"),
            data_source=ProviderMustNotRun(),  # type: ignore[arg-type]
            decision_at=datetime.fromisoformat("2026-08-14T14:59:59+08:00"),
        )
    )

    assert result["ok"] is False
    assert result["status"] == "FAILED"
    assert result["error_code"] == "MARKET_NOT_CLOSED"
    assert result["top_candidates"] == []


def test_ashare_market_screen_helper_rejects_inconsistent_limits() -> None:
    with pytest.raises(ValueError, match="top_n must not exceed factor_budget"):
        asyncio.run(
            _ashare_market_screen_once(
                31,
                30,
                250,
                Decimal("20000000"),
                Decimal("50000000"),
                Decimal("2000000000"),
                decision_at=datetime.fromisoformat("2026-08-14T16:00:00+08:00"),
            )
        )
    with pytest.raises(ValueError, match="safety limit of 300"):
        asyncio.run(
            _ashare_market_screen_once(
                30,
                301,
                250,
                Decimal("20000000"),
                Decimal("50000000"),
                Decimal("2000000000"),
                decision_at=datetime.fromisoformat("2026-08-14T16:00:00+08:00"),
            )
        )


def test_ashare_market_screen_sanitizes_typed_provider_failure() -> None:
    from gribuki_trade.adapters.ashare_screening import (
        AKShareScreeningSourcesExhaustedError,
    )

    class FailingProvider:
        async def fetch_universe_snapshot(self, **_kwargs: object) -> object:
            raise AKShareScreeningSourcesExhaustedError(
                ("secret-host:credential-bearing-provider-detail",)
            )

        async def fetch_factor_snapshot(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("factor fetch must not run")

    result = asyncio.run(
        _ashare_market_screen_once(
            30,
            300,
            250,
            Decimal("20000000"),
            Decimal("50000000"),
            Decimal("2000000000"),
            data_source=FailingProvider(),  # type: ignore[arg-type]
            decision_at=datetime.fromisoformat("2026-08-14T16:00:00+08:00"),
        )
    )

    assert result["ok"] is False
    assert result["error_code"] == "PROVIDER_SOURCES_EXHAUSTED"
    assert result["retryable"] is True
    assert "secret-host" not in repr(result)


def test_napcat_status_returns_structured_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gribuki_trade.adapters import notifiers

    class FailingNotifier:
        def __init__(self, _config: object) -> None:
            pass

        async def __aenter__(self) -> "FailingNotifier":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def get_status(self) -> dict[str, object]:
            raise notifiers.OneBotError("transport_error", retryable=True)

        async def get_version_info(self) -> dict[str, object]:
            raise AssertionError("version endpoint must not run after status failure")

    monkeypatch.setattr(cli, "_required_local_secret", lambda _name: "test-token")
    monkeypatch.setattr(notifiers, "OneBotNotifier", FailingNotifier)

    result = asyncio.run(_napcat_status("http://127.0.0.1:3000"))

    assert result == {
        "app_name": "unknown",
        "base_url": "http://127.0.0.1:3000",
        "error_code": "transport_error",
        "good": False,
        "next_action": "在 GUI 的‘集成管理’页显式启动 NapCat 并完成 QQ 登录",
        "online": False,
        "protocol_version": "unknown",
        "retryable": True,
    }


def test_napcat_status_returns_structured_secret_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_secret(_name: str) -> str:
        raise RuntimeError("credential backend detail")

    monkeypatch.setattr(cli, "_required_local_secret", fail_secret)

    result = asyncio.run(_napcat_status("http://127.0.0.1:3000"))

    assert result == {
        "app_name": "unknown",
        "base_url": "http://127.0.0.1:3000",
        "error_code": "LOCAL_SECRET_UNAVAILABLE",
        "good": False,
        "next_action": (
            ".\\.venv\\Scripts\\python.exe -m gribuki_trade secret-set napcat.onebot.access_token"
        ),
        "online": False,
        "protocol_version": "unknown",
        "retryable": False,
    }
    assert "credential backend detail" not in repr(result)


def test_napcat_send_test_uses_system_health_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gribuki_trade.adapters import notifiers

    delivered: list[object] = []

    class FakeNotifier:
        channel = "onebot"

        def __init__(self, _config: object) -> None:
            pass

        async def __aenter__(self) -> "FakeNotifier":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def send(self, notification: object) -> object:
            delivered.append(notification)
            return SimpleNamespace(channel="onebot", provider_message_id="health-1")

    monkeypatch.setattr(cli, "_required_local_secret", lambda _name: "test-token")
    monkeypatch.setattr(notifiers, "OneBotNotifier", FakeNotifier)

    result = asyncio.run(cli._napcat_send_test("http://127.0.0.1:3000", "private", "12345"))

    assert result["delivered"] is True
    assert len(delivered) == 1
    text = delivered[0].text  # type: ignore[attr-defined]
    validate_text_report_contract(ReportKind.SYSTEM_HEALTH, text)
    assert "不包含交易指令" in text
    assert "GUI 集成管理页" in text


def test_post_close_delivery_sends_one_contract_summary_then_full_markdown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from gribuki_trade.adapters import notifiers
    from gribuki_trade.ports.notifier import NotificationTargetKind

    delivered: list[object] = []
    uploaded: list[tuple[str, str]] = []

    class FakeNotifier:
        channel = "onebot"

        def __init__(self, _config: object) -> None:
            pass

        async def __aenter__(self) -> "FakeNotifier":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def get_status(self) -> dict[str, object]:
            return {"good": True, "online": True}

        async def send(self, notification: object) -> object:
            delivered.append(notification)
            return SimpleNamespace(channel="onebot", provider_message_id="summary-1")

        async def upload_private_file(self, target_id: str, artifact: str) -> object:
            uploaded.append((target_id, artifact))
            return SimpleNamespace(provider_file_id="file-1")

    post_root = tmp_path / "post-close"
    report_dir = tmp_path / "reports"
    post_root.mkdir()
    report_dir.mkdir()
    artifact = report_dir / "盘后日报.md"
    contract = REPORT_CONTRACTS[ReportKind.DAILY_REVIEW]
    artifact.write_text(
        render_stable_markdown_report(
            ReportKind.DAILY_REVIEW,
            title="A股盘后日报",
            sections={
                name: f"{name}内容" + "详细复盘" * 800 for name in contract.required_sections
            },
        ),
        encoding="utf-8",
    )
    status: dict[str, object] = {}
    status_path = post_root / "status.json"
    audit_path = post_root / "audit.jsonl"
    monkeypatch.setattr(cli, "_required_local_secret", lambda _name: "test-token")
    monkeypatch.setattr(notifiers, "OneBotNotifier", FakeNotifier)

    completed = asyncio.run(
        cli._deliver_post_close_artifact(
            post_root=post_root,
            status=status,
            status_path=status_path,
            audit_path=audit_path,
            run_id="post-close-run-0123456789",
            target_hash="a" * 64,
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="12345",
            base_url="http://127.0.0.1:3000",
            artifact_path=artifact,
            dispatch_cycles=1,
            dispatch_poll_interval=0,
        )
    )

    assert completed is True
    assert len(delivered) == 1
    summary = delivered[0].text  # type: ignore[attr-defined]
    validate_text_report_contract(ReportKind.DAILY_REVIEW, summary)
    assert len(summary) < len(artifact.read_text(encoding="utf-8"))
    assert uploaded == [("12345", artifact.name)]
    assert status["text_part_count"] == 1
    assert status["artifact_delivery"] == "SENT"
    assert status["phase"] == "COMPLETE"


def test_cli_defaults_to_gui_and_testnet_cycle_requires_confirmation() -> None:
    parser = build_parser()

    assert parser.parse_args([]).command is None
    args = parser.parse_args(
        ["binance-testnet-cycle", "--confirm", "TESTNET", "--notional", "12.50"]
    )
    assert args.command == "binance-testnet-cycle"
    assert args.confirm == "TESTNET"
    assert args.notional == Decimal("12.50")

    with pytest.raises(SystemExit):
        parser.parse_args(["binance-testnet-cycle"])

    oms_args = parser.parse_args(
        [
            "binance-testnet-oms-cycle",
            "--confirm",
            "TESTNET",
            "--database",
            "durable.sqlite3",
        ]
    )
    assert oms_args.command == "binance-testnet-oms-cycle"
    assert oms_args.database == "durable.sqlite3"

    with pytest.raises(SystemExit):
        parser.parse_args(["binance-testnet-oms-cycle"])

    fill_args = parser.parse_args(
        [
            "binance-testnet-oms-fill",
            "--confirm",
            "TESTNET_FILL",
            "--database",
            "fills.sqlite3",
        ]
    )
    assert fill_args.command == "binance-testnet-oms-fill"
    assert fill_args.database == "fills.sqlite3"

    with pytest.raises(SystemExit):
        parser.parse_args(["binance-testnet-oms-fill"])
    with pytest.raises(SystemExit):
        parser.parse_args(["binance-testnet-oms-fill", "--confirm", "TESTNET"])


def test_napcat_artifact_parser_requires_explicit_confirmation() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "napcat-send-artifact",
            "--target-kind",
            "private",
            "--target-id",
            "12345",
            "--artifact-kind",
            "file",
            "--report-kind",
            "DAILY_REVIEW",
            "--artifact",
            "report.md",
            "--confirm",
            "SEND_ARTIFACT",
        ]
    )

    assert args.artifact_root == "runtime/reports"
    assert args.artifact == "report.md"
    assert args.report_kind == "DAILY_REVIEW"
    assert args.receipt_db == "runtime/notifications/report-artifacts.sqlite3"
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "napcat-send-artifact",
                "--target-kind",
                "private",
                "--target-id",
                "12345",
                "--artifact-kind",
                "file",
                "--report-kind",
                "DAILY_REVIEW",
                "--artifact",
                "report.md",
            ]
        )


def test_napcat_send_artifact_uses_exact_target_and_trusted_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from gribuki_trade.adapters import notifiers

    report = tmp_path / "report.md"
    report.write_text(
        render_stable_markdown_report(
            ReportKind.DAILY_REVIEW,
            title="日报",
            sections={
                section: f"{section}内容"
                for section in REPORT_CONTRACTS[ReportKind.DAILY_REVIEW].required_sections
            },
        ),
        encoding="utf-8",
    )
    observed: dict[str, object] = {}
    upload_count = 0

    class FakeNotifier:
        def __init__(self, config: object) -> None:
            observed["config"] = config

        async def __aenter__(self) -> "FakeNotifier":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def upload_private_file(self, target_id: str, artifact: str) -> object:
            nonlocal upload_count
            upload_count += 1
            observed["target_id"] = target_id
            observed["artifact"] = artifact
            return SimpleNamespace(provider_file_id="file-1")

    monkeypatch.setattr(cli, "_required_local_secret", lambda _name: "test-token")
    monkeypatch.setattr(notifiers, "OneBotNotifier", FakeNotifier)

    result = asyncio.run(
        _napcat_send_artifact(
            "http://127.0.0.1:3000",
            "private",
            "12345",
            "file",
            "DAILY_REVIEW",
            str(tmp_path),
            "report.md",
            str(tmp_path / "receipts.sqlite3"),
        )
    )
    repeated = asyncio.run(
        _napcat_send_artifact(
            "http://127.0.0.1:3000",
            "private",
            "12345",
            "file",
            "DAILY_REVIEW",
            str(tmp_path),
            "report.md",
            str(tmp_path / "receipts.sqlite3"),
        )
    )

    config = observed["config"]
    assert isinstance(config, notifiers.OneBotConfig)
    assert config.private_target_ids == frozenset({"12345"})
    assert config.group_target_ids == frozenset()
    assert config.artifact_root == tmp_path.resolve()
    assert observed["artifact"] == str(report.resolve())
    assert result["provider_identifier"] == "file-1"
    assert result["report_kind"] == "DAILY_REVIEW"
    assert result["ok"] is True
    assert repeated["already_sent"] is True
    assert upload_count == 1


def test_napcat_send_artifact_rejects_noncontract_report_before_secrets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    report = tmp_path / "report.md"
    report.write_text("# uncontracted report\n", encoding="utf-8")
    receipt_database = tmp_path / "receipts.sqlite3"

    def unexpected_secret(_name: str) -> str:
        raise AssertionError("invalid reports must not read secrets")

    monkeypatch.setattr(cli, "_required_local_secret", unexpected_secret)

    with pytest.raises(ValueError, match="missing required report section"):
        asyncio.run(
            _napcat_send_artifact(
                "http://127.0.0.1:3000",
                "private",
                "12345",
                "file",
                "DAILY_REVIEW",
                str(tmp_path),
                "report.md",
                str(receipt_database),
            )
        )

    assert not receipt_database.exists()


def test_napcat_send_artifact_does_not_retry_an_ambiguous_upload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    report = tmp_path / "report.md"
    report.write_text(
        render_stable_markdown_report(
            ReportKind.POSITION_REVIEW,
            title="持仓复核",
            sections={
                section: f"{section}内容"
                for section in REPORT_CONTRACTS[ReportKind.POSITION_REVIEW].required_sections
            },
        ),
        encoding="utf-8",
    )
    attempts = 0

    class AmbiguousNotifier:
        def __init__(self, _config: object) -> None:
            pass

        async def __aenter__(self) -> "AmbiguousNotifier":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def upload_private_file(self, _target_id: str, _artifact: str) -> object:
            nonlocal attempts
            attempts += 1
            raise OSError("connection ended after upload request")

    from gribuki_trade.adapters import notifiers

    monkeypatch.setattr(cli, "_required_local_secret", lambda _name: "test-token")
    monkeypatch.setattr(notifiers, "OneBotNotifier", AmbiguousNotifier)
    arguments = (
        "http://127.0.0.1:3000",
        "private",
        "12345",
        "file",
        "POSITION_REVIEW",
        str(tmp_path),
        "report.md",
        str(tmp_path / "receipts.sqlite3"),
    )

    first = asyncio.run(_napcat_send_artifact(*arguments))
    second = asyncio.run(_napcat_send_artifact(*arguments))

    assert first["error_code"] == "REPORT_ARTIFACT_DELIVERY_AMBIGUOUS"
    assert second["error_code"] == "REPORT_ARTIFACT_DELIVERY_AMBIGUOUS"
    assert attempts == 1


def test_main_dispatches_binance_testnet_oms_cycle(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[tuple[object, ...]] = []

    async def fake_cycle(*args: object) -> dict[str, object]:
        calls.append(args)
        return {"environment": "TESTNET", "final_status": "CANCELED"}

    monkeypatch.setattr(cli, "_binance_testnet_oms_cycle", fake_cycle)

    assert (
        cli.main(
            [
                "binance-testnet-oms-cycle",
                "--symbol",
                "ETHUSDT",
                "--notional",
                "25",
                "--database",
                "oms.sqlite3",
                "--confirm",
                "TESTNET",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {
        "environment": "TESTNET",
        "final_status": "CANCELED",
    }
    assert calls == [("ETHUSDT", Decimal("25"), "oms.sqlite3")]


def test_main_dispatches_binance_testnet_oms_fill(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[tuple[object, ...]] = []

    async def fake_fill(*args: object) -> dict[str, object]:
        calls.append(args)
        return {"environment": "TESTNET", "order_status": "FILLED"}

    monkeypatch.setattr(cli, "_binance_testnet_oms_fill", fake_fill)

    assert (
        cli.main(
            [
                "binance-testnet-oms-fill",
                "--symbol",
                "ETHUSDT",
                "--notional",
                "25",
                "--database",
                "fills.sqlite3",
                "--confirm",
                "TESTNET_FILL",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {
        "environment": "TESTNET",
        "order_status": "FILLED",
    }
    assert calls == [("ETHUSDT", Decimal("25"), "fills.sqlite3")]


def test_binance_testnet_oms_cycle_persists_streams_cancels_and_reconciles(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class FakeExecutionReport:
        def __init__(self, client_order_id: str, execution_type: str) -> None:
            self.client_order_id = client_order_id
            self.original_client_order_id = None
            self.execution_type = execution_type

    class FakeStream:
        instances: list["FakeStream"] = []

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.subscription_id: int | None = None
            self.queue: asyncio.Queue[object] = asyncio.Queue()
            self.closed = False
            self.instances.append(self)

        async def events(self):  # type: ignore[no-untyped-def]
            self.subscription_id = 7
            while True:
                event = await self.queue.get()
                if event is None:
                    return
                yield event

        async def aclose(self) -> None:
            self.closed = True
            await self.queue.put(None)

    class FakeStore:
        instances: list["FakeStore"] = []

        def __init__(self, path: Path) -> None:
            self.path = path
            self.snapshot = SimpleNamespace(status=cli.OrderStatus.CREATED)
            self.command_values: list[SimpleNamespace] = []
            self.closed = False
            self.instances.append(self)

        def order(self, _client_order_id: str) -> SimpleNamespace:
            return self.snapshot

        def require_order(self, _client_order_id: str) -> SimpleNamespace:
            return self.snapshot

        def commands(self) -> tuple[SimpleNamespace, ...]:
            return tuple(self.command_values)

        def fills(self, *, client_order_id: str) -> tuple[()]:
            assert client_order_id == "gri-cli-test-oms"
            return ()

        def close(self) -> None:
            self.closed = True

    reconciliation = cli.BinanceStartupReconciliation(
        recovered_commands=0,
        exchange_open_orders=0,
        exchange_history_orders=0,
        exchange_trades=0,
        reconciled_orders=1,
        recorded_fills=0,
        recorded_balances=2,
    )

    class FakeService:
        instances: list["FakeService"] = []

        def __init__(
            self,
            _gateway: object,
            store: FakeStore,
            *,
            account_id: str,
            symbols: tuple[str, ...],
            user_stream: FakeStream,
        ) -> None:
            assert account_id == cli.DEFAULT_TESTNET_ACCOUNT
            assert symbols == ("BTCUSDT",)
            self.store = store
            self.stream = user_stream
            self.started = False
            self.instances.append(self)

        async def start(self) -> cli.BinanceStartupReconciliation:
            self.started = True
            return reconciliation

        async def submit(self, order: cli.OrderIntent) -> SimpleNamespace:
            self.store.snapshot = SimpleNamespace(status=cli.OrderStatus.ACCEPTED)
            self.store.command_values.append(
                SimpleNamespace(
                    attempt_count=1,
                    client_order_id=order.client_order_id,
                    command_id=f"submit:{order.client_order_id}",
                    last_error_code=None,
                    status=SimpleNamespace(value="SENT"),
                    command_type=SimpleNamespace(value="SUBMIT_ORDER"),
                )
            )
            await self.stream.queue.put(FakeExecutionReport(order.client_order_id, "NEW"))
            return self.store.snapshot

        async def consume_user_event(self, _event: object) -> bool:
            return True

        async def cancel(self, client_order_id: str) -> SimpleNamespace:
            self.store.snapshot = SimpleNamespace(status=cli.OrderStatus.CANCELED)
            self.store.command_values.append(
                SimpleNamespace(
                    attempt_count=1,
                    client_order_id=client_order_id,
                    command_id=f"cancel:{client_order_id}",
                    last_error_code=None,
                    status=SimpleNamespace(value="SENT"),
                    command_type=SimpleNamespace(value="CANCEL_ORDER"),
                )
            )
            await self.stream.queue.put(FakeExecutionReport(client_order_id, "CANCELED"))
            return self.store.snapshot

        async def reconcile_startup(self) -> cli.BinanceStartupReconciliation:
            return reconciliation

        async def stop(self) -> None:
            await self.stream.aclose()
            self.started = False

    class FakeGateway:
        base_url = "https://testnet.invalid"
        environment = cli.BinanceEnvironment.TESTNET
        server_time_offset_ms = 0

        async def synchronize_time(self) -> None:
            return None

        async def account(self) -> SimpleNamespace:
            return SimpleNamespace(can_trade=True)

    async def fake_build_order(
        _gateway: object,
        symbol: str,
        _notional: Decimal,
        *,
        resting: bool,
    ) -> cli.OrderIntent:
        assert resting
        return cli.OrderIntent(
            client_order_id="gri-cli-test-oms",
            account_id=cli.DEFAULT_TESTNET_ACCOUNT,
            strategy_id="authenticated-smoke",
            symbol=symbol,
            side=cli.Side.BUY,
            quantity=Decimal("0.01"),
            limit_price=Decimal("2000"),
            created_at=datetime.now(UTC),
        )

    monkeypatch.setattr(cli, "BinanceExecutionReport", FakeExecutionReport)
    monkeypatch.setattr(cli, "BinanceSpotUserDataStream", FakeStream)
    monkeypatch.setattr(cli, "BinanceSpotTestnetExecutionService", FakeService)
    monkeypatch.setattr(cli, "SQLiteOrderManagementStore", FakeStore)
    monkeypatch.setattr(cli, "_testnet_gateway", lambda: FakeGateway())
    monkeypatch.setattr(cli, "_build_test_order", fake_build_order)
    monkeypatch.setattr(cli, "load_binance_credentials", lambda *_args: object())

    result = asyncio.run(
        cli._binance_testnet_oms_cycle(
            "BTCUSDT",
            Decimal("20"),
            str(tmp_path / "testnet.sqlite3"),
        )
    )

    assert result["environment"] == "TESTNET"
    assert result["user_stream"] == {
        "cancel_execution": "CANCELED",
        "new_execution": "NEW",
        "subscription": "active",
    }
    assert result["oms"] == {
        "command_statuses": [
            {
                "attempt_count": 1,
                "command_id": "submit:gri-cli-test-oms",
                "error_code": None,
                "status": "SENT",
                "type": "SUBMIT_ORDER",
            },
            {
                "attempt_count": 1,
                "command_id": "cancel:gri-cli-test-oms",
                "error_code": None,
                "status": "SENT",
                "type": "CANCEL_ORDER",
            },
        ],
        "fill_count": 0,
        "order_statuses": {
            "after_new": "ACCEPTED",
            "after_reconciliation": "CANCELED",
            "after_submit": "ACCEPTED",
            "after_cancel": "CANCELED",
        },
    }
    assert FakeStore.instances[0].closed
    assert FakeStream.instances[0].closed
    assert not FakeService.instances[0].started


def test_binance_testnet_oms_fill_uses_durable_testnet_service_offline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class FakeExecutionReport:
        def __init__(self, client_order_id: str) -> None:
            self.client_order_id = client_order_id
            self.original_client_order_id = None
            self.execution_type = "TRADE"
            self.status = cli.OrderStatus.FILLED

    class FakeStream:
        instances: list["FakeStream"] = []

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.subscription_id: int | None = None
            self.queue: asyncio.Queue[object] = asyncio.Queue()
            self.closed = False
            self.instances.append(self)

        async def events(self):  # type: ignore[no-untyped-def]
            self.subscription_id = 9
            while True:
                event = await self.queue.get()
                if event is None:
                    return
                yield event

        async def aclose(self) -> None:
            self.closed = True
            await self.queue.put(None)

    class FakeStore:
        instances: list["FakeStore"] = []

        def __init__(self, path: Path) -> None:
            self.path = path
            self.snapshot = SimpleNamespace(
                status=cli.OrderStatus.CREATED,
                filled_quantity=Decimal("0"),
                average_fill_price=None,
            )
            self.fill_values: tuple[object, ...] = ()
            self.closed = False
            self.instances.append(self)

        def order(self, _client_order_id: str) -> SimpleNamespace:
            return self.snapshot

        def require_order(self, _client_order_id: str) -> SimpleNamespace:
            return self.snapshot

        def fills(self, *, client_order_id: str) -> tuple[object, ...]:
            assert client_order_id == "gri-fill-offline"
            return self.fill_values

        def open_orders(self, *, account_id: str) -> tuple[object, ...]:
            assert account_id == cli.DEFAULT_TESTNET_ACCOUNT
            return ()

        def commands(self) -> tuple[SimpleNamespace, ...]:
            return (
                SimpleNamespace(
                    attempt_count=1,
                    client_order_id="gri-fill-offline",
                    command_id="submit:gri-fill-offline",
                    command_type=SimpleNamespace(value="SUBMIT_ORDER"),
                    last_error_code=None,
                    status=SimpleNamespace(value="SENT"),
                ),
            )

        def close(self) -> None:
            self.closed = True

    reconciliation = cli.BinanceStartupReconciliation(
        recovered_commands=0,
        exchange_open_orders=0,
        exchange_history_orders=1,
        exchange_trades=1,
        reconciled_orders=1,
        recorded_fills=1,
        recorded_balances=2,
    )

    class FakeService:
        instances: list["FakeService"] = []

        def __init__(
            self,
            gateway: object,
            store: FakeStore,
            *,
            account_id: str,
            symbols: tuple[str, ...],
            user_stream: FakeStream,
        ) -> None:
            assert gateway.environment is cli.BinanceEnvironment.TESTNET  # type: ignore[attr-defined]
            assert account_id == cli.DEFAULT_TESTNET_ACCOUNT
            assert symbols == ("BTCUSDT",)
            self.store = store
            self.stream = user_stream
            self.started = False
            self.instances.append(self)

        async def start(self) -> cli.BinanceStartupReconciliation:
            self.started = True
            return reconciliation

        async def submit(self, order: cli.OrderIntent) -> SimpleNamespace:
            self.store.snapshot = SimpleNamespace(
                status=cli.OrderStatus.ACCEPTED,
                filled_quantity=Decimal("0"),
                average_fill_price=None,
            )
            await self.stream.queue.put(FakeExecutionReport(order.client_order_id))
            return self.store.snapshot

        async def consume_user_event(self, _event: object) -> bool:
            self.store.snapshot = SimpleNamespace(
                status=cli.OrderStatus.FILLED,
                filled_quantity=Decimal("0.002"),
                average_fill_price=Decimal("10000"),
            )
            self.store.fill_values = (
                SimpleNamespace(
                    fee_amount=Decimal("0.000002"),
                    fee_asset="BTC",
                    fill_id="trade-1",
                    price=Decimal("10000"),
                    quantity=Decimal("0.002"),
                ),
            )
            return True

        async def cancel(self, _client_order_id: str) -> SimpleNamespace:
            raise AssertionError("a completed fill must not be canceled")

        async def reconcile_startup(self) -> cli.BinanceStartupReconciliation:
            return reconciliation

        async def stop(self) -> None:
            await self.stream.aclose()
            self.started = False

    class FakeGateway:
        base_url = "https://testnet.invalid"
        environment = cli.BinanceEnvironment.TESTNET
        server_time_offset_ms = 0

        def __init__(self) -> None:
            self.account_calls = 0

        async def synchronize_time(self) -> None:
            return None

        async def account(self) -> SimpleNamespace:
            self.account_calls += 1
            btc_free = Decimal("1") if self.account_calls < 3 else Decimal("1.002")
            usdt_free = Decimal("1000") if self.account_calls < 3 else Decimal("980")
            return SimpleNamespace(
                can_trade=True,
                balances=(
                    SimpleNamespace(asset="BTC", free=btc_free, locked=Decimal("0")),
                    SimpleNamespace(asset="USDT", free=usdt_free, locked=Decimal("0")),
                ),
            )

    async def fake_build_order(
        gateway: object,
        symbol: str,
        notional: Decimal,
    ) -> cli.OrderIntent:
        assert gateway.environment is cli.BinanceEnvironment.TESTNET  # type: ignore[attr-defined]
        assert symbol == "BTCUSDT"
        assert notional == Decimal("20")
        return cli.OrderIntent(
            client_order_id="gri-fill-offline",
            account_id=cli.DEFAULT_TESTNET_ACCOUNT,
            strategy_id="authenticated-fill-smoke",
            symbol=symbol,
            side=cli.Side.BUY,
            quantity=Decimal("0.002"),
            limit_price=Decimal("10100"),
            created_at=datetime.now(UTC),
        )

    monkeypatch.setattr(cli, "BinanceExecutionReport", FakeExecutionReport)
    monkeypatch.setattr(cli, "BinanceSpotUserDataStream", FakeStream)
    monkeypatch.setattr(cli, "BinanceSpotTestnetExecutionService", FakeService)
    monkeypatch.setattr(cli, "SQLiteOrderManagementStore", FakeStore)
    monkeypatch.setattr(cli, "_testnet_gateway", lambda: FakeGateway())
    monkeypatch.setattr(cli, "_build_marketable_test_order", fake_build_order)
    monkeypatch.setattr(cli, "load_binance_credentials", lambda *_args: object())

    result = asyncio.run(
        cli._binance_testnet_oms_fill(
            "BTCUSDT",
            Decimal("20"),
            str(tmp_path / "fills.sqlite3"),
        )
    )

    assert result["environment"] == "TESTNET"
    assert result["final_status"] == "FILLED"
    assert result["fee_assets"] == {"BTC": "0.000002"}
    assert result["oms"] == {
        "command_statuses": [
            {
                "attempt_count": 1,
                "command_id": "submit:gri-fill-offline",
                "error_code": None,
                "status": "SENT",
                "type": "SUBMIT_ORDER",
            }
        ],
        "fill_count": 1,
        "submitted_status": "ACCEPTED",
        "unresolved_order_ids": [],
    }
    assert result["user_stream"] == {
        "executions": ["TRADE"],
        "subscription": "active",
    }
    assert result["balance_changes"] == [
        {
            "asset": "BTC",
            "before_free": "1",
            "before_locked": "0",
            "after_free": "1.002",
            "after_locked": "0",
            "delta_free": "0.002",
            "delta_locked": "0",
            "delta_total": "0.002",
        },
        {
            "asset": "USDT",
            "before_free": "1000",
            "before_locked": "0",
            "after_free": "980",
            "after_locked": "0",
            "delta_free": "-20",
            "delta_locked": "0",
            "delta_total": "-20",
        },
    ]
    assert FakeStore.instances[0].closed
    assert FakeStream.instances[0].closed
    assert not FakeService.instances[0].started


def test_binance_testnet_oms_fill_rejects_live_before_opening_database(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        cli,
        "_testnet_gateway",
        lambda: SimpleNamespace(environment=cli.BinanceEnvironment.LIVE),
    )
    monkeypatch.setattr(
        cli,
        "SQLiteOrderManagementStore",
        lambda _path: pytest.fail("LIVE rejection must happen before opening the OMS"),
    )

    with pytest.raises(RuntimeError, match="cannot target Binance LIVE"):
        asyncio.run(
            cli._binance_testnet_oms_fill(
                "BTCUSDT",
                Decimal("20"),
                str(tmp_path / "must-not-exist" / "fills.sqlite3"),
            )
        )

    assert not (tmp_path / "must-not-exist").exists()


def test_binance_testnet_oms_fill_refuses_database_with_pending_work(
    tmp_path: Path,
) -> None:
    store = cli.SQLiteOrderManagementStore(tmp_path / "busy.sqlite3")
    try:
        store.create_order(
            cli.OrderIntent(
                client_order_id="existing-unresolved-order",
                account_id=cli.DEFAULT_TESTNET_ACCOUNT,
                strategy_id="prior-run",
                symbol="BTCUSDT",
                side=cli.Side.BUY,
                quantity=Decimal("0.001"),
                limit_price=Decimal("10000"),
                created_at=datetime.now(UTC),
            )
        )
        with pytest.raises(RuntimeError, match="existing-unresolved-order"):
            cli._assert_testnet_oms_database_idle(store)
    finally:
        store.close()


def test_binance_research_and_futures_demo_parser_defaults() -> None:
    parser = build_parser()

    history = parser.parse_args(["binance-history-sync"])
    assert history.symbol == "BTCUSDT"
    assert history.interval == "5m"
    assert history.days == 30
    assert history.environment == "LIVE"
    assert history.database == "runtime/binance/market.sqlite3"

    backtest = parser.parse_args(["binance-backtest"])
    assert backtest.symbol == "BTCUSDT"
    assert backtest.interval == "5m"
    assert backtest.initial_quote == Decimal("10000")
    assert backtest.fast_window == 20
    assert backtest.slow_window == 50
    assert backtest.target_position == Decimal("0.60")
    assert backtest.rebalance_band == Decimal("0.02")

    shadow = parser.parse_args(["binance-shadow-run"])
    assert shadow.symbol == "BTCUSDT"
    assert shadow.interval == "1m"
    assert shadow.environment == "LIVE"
    assert shadow.closed_bars == 3
    assert shadow.initial_quote == Decimal("10000")
    assert shadow.maximum_order_notional == Decimal("100")

    futures = parser.parse_args(["binance-futures-demo-status"])
    assert futures.product == "USDS_FUTURES"
    assert futures.symbol is None
    assert not futures.validate_order_test
    assert futures.confirm is None
    assert futures.quantity is None
    assert futures.side == "BUY"

    coin_order_test = parser.parse_args(
        [
            "binance-futures-demo-status",
            "--product",
            "COIN_FUTURES",
            "--validate-order-test",
            "--confirm",
            "FUTURES_DEMO_TEST",
            "--quantity",
            "2",
            "--side",
            "SELL",
        ]
    )
    assert coin_order_test.validate_order_test
    assert coin_order_test.quantity == Decimal("2")
    assert coin_order_test.side == "SELL"


def test_main_dispatches_bounded_binance_shadow_run(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[tuple[object, ...]] = []

    async def fake_shadow(*args: object) -> dict[str, object]:
        calls.append(args)
        return {"watermark": "PAPER_SHADOW/NO_REMOTE_ORDERS"}

    monkeypatch.setattr(cli, "_binance_shadow_run", fake_shadow)

    assert (
        cli.main(
            [
                "binance-shadow-run",
                "--symbol",
                "ETHUSDT",
                "--interval",
                "5m",
                "--environment",
                "TESTNET",
                "--closed-bars",
                "2",
                "--initial-quote",
                "2000",
                "--fast-window",
                "5",
                "--slow-window",
                "20",
                "--target-position",
                "0.5",
                "--rebalance-band",
                "0.03",
                "--maximum-order-notional",
                "50",
                "--database",
                "shadow.sqlite3",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {"watermark": "PAPER_SHADOW/NO_REMOTE_ORDERS"}
    assert calls == [
        (
            "ETHUSDT",
            "5m",
            "TESTNET",
            "shadow.sqlite3",
            2,
            Decimal("2000"),
            5,
            20,
            Decimal("0.5"),
            Decimal("0.03"),
            Decimal("50"),
        )
    ]


def test_main_dispatches_binance_history_and_futures_demo(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[tuple[object, ...]] = []

    async def fake_history(*args: object) -> dict[str, object]:
        calls.append(("history", *args))
        return {"row_count": 3}

    async def fake_futures(*args: object) -> dict[str, object]:
        calls.append(("futures", *args))
        return {"ping": "ok"}

    def fake_backtest(*args: object) -> dict[str, object]:
        calls.append(("backtest", *args))
        return {"total_return": "0.01"}

    monkeypatch.setattr(cli, "_binance_history_sync", fake_history)
    monkeypatch.setattr(cli, "_binance_futures_demo_status", fake_futures)
    monkeypatch.setattr(cli, "_binance_backtest", fake_backtest)

    assert (
        cli.main(
            [
                "binance-history-sync",
                "--symbol",
                "ETHUSDT",
                "--interval",
                "1h",
                "--days",
                "2",
                "--environment",
                "TESTNET",
                "--database",
                "market.sqlite3",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {"row_count": 3}
    assert cli.main(["binance-futures-demo-status", "--product", "COIN_FUTURES"]) == 0
    assert json.loads(capsys.readouterr().out) == {"ping": "ok"}
    assert (
        cli.main(
            [
                "binance-backtest",
                "--fast-window",
                "5",
                "--slow-window",
                "10",
                "--target-position",
                "0.5",
                "--rebalance-band",
                "0.03",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {"total_return": "0.01"}
    assert calls == [
        ("history", "ETHUSDT", "1h", 2, "TESTNET", "market.sqlite3"),
        ("futures", "COIN_FUTURES", None, False, None, None, "BUY"),
        (
            "backtest",
            "BTCUSDT",
            "5m",
            "LIVE",
            "runtime/binance/market.sqlite3",
            "BTC",
            "USDT",
            Decimal("10000"),
            5,
            10,
            Decimal("0.5"),
            Decimal("0.03"),
            Decimal("0.001"),
            Decimal("0.001"),
            Decimal("0.0005"),
        ),
    ]


def test_futures_demo_status_is_public_by_default_and_authenticates_per_product(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import gribuki_trade.adapters.binance as binance
    from gribuki_trade.security import MemorySecretProvider, SecretProviderError

    provider_box = [MemorySecretProvider()]
    monkeypatch.setattr(cli, "KeyringSecretProvider", lambda: provider_box[0])

    class FakeFuturesClient:
        instances: list["FakeFuturesClient"] = []

        def __init__(self, *, product: object, credentials: object = None) -> None:
            self.product = product
            self.stage = SimpleNamespace(value="DEMO")
            self.base_url = (
                "https://demo-fapi.binance.com"
                if product is binance.BinanceProduct.USDS_FUTURES
                else "https://demo-dapi.binance.com"
            )
            self.credentials = credentials
            self.calls: list[object] = []
            self.__class__.instances.append(self)

        async def ping(self) -> None:
            self.calls.append("ping")

        async def server_time(self) -> int:
            self.calls.append("server_time")
            return 1_700_000_000_000

        async def synchronize_time(self) -> int:
            self.calls.append("synchronize_time")
            return 7

        async def account(self) -> dict[str, object]:
            self.calls.append("account")
            return {"assets": [{}], "canTrade": True, "positions": [{}, {}]}

        async def position_risk(self, symbol: str) -> tuple[dict[str, object], ...]:
            self.calls.append(("position_risk", symbol))
            return ({"symbol": symbol},)

        async def ticker_price(self, symbol: str) -> SimpleNamespace:
            self.calls.append(("ticker_price", symbol))
            return SimpleNamespace(symbol=symbol, price=Decimal("63000.1"))

        async def exchange_info(self) -> dict[str, object]:
            self.calls.append("exchange_info")
            return {"symbols": [{}, {}]}

        async def validate_order(self, **kwargs: object) -> dict[str, object]:
            self.calls.append(("validate_order", kwargs))
            return {}

    monkeypatch.setattr(binance, "BinanceFuturesRestClient", FakeFuturesClient)

    public = asyncio.run(cli._binance_futures_demo_status("USDS_FUTURES", None))
    assert public["authenticated"] is False
    assert public["account"] is None
    assert public["position_risk_count"] is None
    assert public["order_test"] == {
        "called": False,
        "creates_order": False,
        "order_type": None,
        "quantity": None,
        "quantity_unit": "base_asset",
        "side": None,
    }
    assert FakeFuturesClient.instances[0].credentials is None
    assert "account" not in FakeFuturesClient.instances[0].calls

    class BrokenProvider:
        def get_secret(self, _name: str) -> None:
            raise SecretProviderError("offline test backend unavailable")

    provider_box[0] = BrokenProvider()  # type: ignore[list-item]
    keyring_unavailable = asyncio.run(cli._binance_futures_demo_status("USDS_FUTURES", None))
    assert keyring_unavailable["authenticated"] is False
    assert keyring_unavailable["credential_store_available"] is False
    assert FakeFuturesClient.instances[1].credentials is None

    provider_box[0] = MemorySecretProvider(
        {
            "binance.coin_futures.demo.api_key": "coin-demo-key",
            "binance.coin_futures.demo.secret_key": "coin-demo-secret",
            "binance.usds_futures.demo.api_key": "wrong-product-key",
            "binance.usds_futures.demo.secret_key": "wrong-product-secret",
        }
    )
    authenticated = asyncio.run(
        cli._binance_futures_demo_status(
            "COIN_FUTURES",
            None,
            True,
            "FUTURES_DEMO_TEST",
            Decimal("2"),
            "SELL",
        )
    )
    auth_client = FakeFuturesClient.instances[2]
    assert auth_client.credentials.api_key == "coin-demo-key"
    assert authenticated["authenticated"] is True
    assert authenticated["clock_offset_ms"] == 7
    assert authenticated["position_risk_count"] == 1
    assert authenticated["account"] == {
        "asset_count": 1,
        "can_trade": True,
        "declared_position_count": 2,
    }
    assert authenticated["order_test"] == {
        "called": True,
        "creates_order": False,
        "order_type": "MARKET",
        "quantity": "2",
        "quantity_unit": "contracts",
        "side": "SELL",
    }
    assert (
        "validate_order",
        {
            "symbol": "BTCUSD_PERP",
            "side": "SELL",
            "order_type": "MARKET",
            "quantity": Decimal("2"),
        },
    ) in auth_client.calls


def test_futures_demo_order_test_requires_confirmation_and_whole_coin_contracts() -> None:
    with pytest.raises(RuntimeError, match="requires --confirm FUTURES_DEMO_TEST"):
        asyncio.run(
            cli._binance_futures_demo_status("USDS_FUTURES", None, True, None, Decimal("0.001"))
        )

    with pytest.raises(ValueError, match="whole contract"):
        asyncio.run(
            cli._binance_futures_demo_status(
                "COIN_FUTURES",
                "deepseek-v4-flash",
                True,
                "FUTURES_DEMO_TEST",
                Decimal("0.5"),
            )
        )


@pytest.mark.parametrize("value", ["-0.1", "NaN", "Infinity", "bad"])
def test_cli_rejects_invalid_non_negative_decimal(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        _non_negative_decimal(value)


@pytest.mark.parametrize("value", ["-0.1", "1.1", "NaN", "bad"])
def test_cli_rejects_invalid_unit_fraction(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        _unit_fraction_decimal(value)


def test_cli_secret_set_accepts_only_known_names() -> None:
    parser = build_parser()

    args = parser.parse_args(["secret-set", "schwab.client_id"])
    assert args.name == "schwab.client_id"
    assert parser.parse_args(["secret-set", "openai.api_key"]).name == "openai.api_key"
    assert parser.parse_args(["secret-set", "deepseek.api_key"]).name == "deepseek.api_key"
    assert (
        parser.parse_args(["secret-set", "napcat.onebot.access_token"]).name
        == "napcat.onebot.access_token"
    )
    assert (
        parser.parse_args(["secret-set", "search.tavily.api_key"]).name == "search.tavily.api_key"
    )
    assert (
        parser.parse_args(["secret-set", "search.searxng.bearer_token"]).name
        == "search.searxng.bearer_token"
    )
    assert (
        parser.parse_args(["secret-set", "binance.usds_futures.demo.api_key"]).name
        == "binance.usds_futures.demo.api_key"
    )
    assert (
        parser.parse_args(["secret-set", "binance.coin_futures.demo.secret_key"]).name
        == "binance.coin_futures.demo.secret_key"
    )

    with pytest.raises(SystemExit):
        parser.parse_args(["secret-set", "arbitrary.secret"])


def test_deepseek_convenience_commands_are_safe_aliases(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    configured: list[str] = []
    monkeypatch.setattr(cli, "_set_secret", configured.append)

    assert cli.main(["deepseek-configure"]) == 0
    assert configured == ["deepseek.api_key"]
    assert json.loads(capsys.readouterr().out) == {
        "configured": True,
        "name": "deepseek.api_key",
    }

    async def fake_status() -> dict[str, object]:
        return {
            "default_model_available": True,
            "default_model_id": "deepseek-v4-flash",
            "deepseek_v4_flash_available": True,
            "deepseek_v4_pro_available": True,
            "ok": True,
        }

    monkeypatch.setattr(cli, "_deepseek_status", fake_status)
    assert cli.main(["deepseek-status"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "default_model_available": True,
        "default_model_id": "deepseek-v4-flash",
        "deepseek_v4_flash_available": True,
        "deepseek_v4_pro_available": True,
        "ok": True,
    }


@pytest.mark.parametrize("value", ["0", "-1", "NaN", "Infinity", "nope"])
def test_cli_rejects_invalid_notional(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        _positive_decimal(value)


def test_cli_exposes_research_data_and_explicit_notification_checks() -> None:
    parser = build_parser()

    bars = parser.parse_args(["ashare-bars", "--symbol", "000001.SZ", "--interval", "1m"])
    assert bars.symbol == "000001.SZ"
    assert bars.interval == "1m"
    assert bars.lookback_minutes == 480

    news = parser.parse_args(
        ["ashare-news", "--feed", "individual_eastmoney", "--symbol", "600000"]
    )
    assert news.feed == "individual_eastmoney"
    assert news.symbol == "600000"

    message = parser.parse_args(
        [
            "napcat-send-test",
            "--target-kind",
            "private",
            "--target-id",
            "12345",
            "--confirm",
            "SEND_TEST",
        ]
    )
    assert message.target_id == "12345"

    with pytest.raises(SystemExit):
        parser.parse_args(["napcat-send-test", "--target-kind", "private", "--target-id", "12345"])


def test_napcat_configure_creates_loopback_configs_without_printing_tokens(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from gribuki_trade.napcat_setup import NAPCAT_WEBUI_TOKEN_SECRET
    from gribuki_trade.security import MemorySecretProvider

    runtime = tmp_path / "NapCat.Portable"
    (runtime / "napcat" / "config").mkdir(parents=True)
    (runtime / "napcat.bat").write_text("@echo off\n", encoding="utf-8")
    provider = MemorySecretProvider()
    monkeypatch.setattr(cli, "KeyringSecretProvider", lambda: provider)

    parsed = build_parser().parse_args(["napcat-configure", "--runtime-dir", str(runtime)])
    assert parsed.onebot_port == 3000
    assert parsed.webui_port == 6099
    result = cli._napcat_configure(str(runtime), 3000, 6099, False)

    onebot_token = provider.get_secret(cli.NAPCAT_ACCESS_TOKEN_SECRET)
    webui_token = provider.get_secret(NAPCAT_WEBUI_TOKEN_SECRET)
    assert onebot_token is not None
    assert webui_token is not None
    assert onebot_token not in repr(result)
    assert webui_token not in repr(result)
    assert result["onebot_base_url"] == "http://127.0.0.1:3000"
    assert result["webui_url"] == "http://127.0.0.1:6099"


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "nope"])
def test_cli_rejects_invalid_positive_integer(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        _positive_integer(value)


def test_cli_parses_positive_integer_or_unlimited() -> None:
    assert _positive_integer_or_unlimited("41") == 41
    assert _positive_integer_or_unlimited("unlimited") is None
    assert _positive_integer_or_unlimited("UNLIMITED") is None
    with pytest.raises(argparse.ArgumentTypeError):
        _positive_integer_or_unlimited("0")

    parsed = build_parser().parse_args(
        ["ashare-paper-day", "run", "--intraday-llm-max-calls", "unlimited"]
    )
    assert parsed.intraday_llm_max_calls is None


def test_news_watch_parser_supports_repeated_sources_and_finite_cycles() -> None:
    args = build_parser().parse_args(
        [
            "ashare-news-watch",
            "--feed",
            "global_sina",
            "--feed",
            "global_cailianpress",
            "--symbol",
            "600000.SH",
            "--symbol",
            "000001.SZ",
            "--interval-seconds",
            "0.25",
            "--cycles",
            "3",
            "--runtime-dir",
            "local-news",
        ]
    )

    assert args.command == "ashare-news-watch"
    assert args.feeds == ["global_sina", "global_cailianpress"]
    assert args.symbols == ["600000.SH", "000001.SZ"]
    assert args.interval_seconds == 0.25
    assert args.cycles == 3
    assert args.runtime_dir == "local-news"

    defaults = build_parser().parse_args(["ashare-news-watch"])
    assert defaults.feeds is None
    assert defaults.symbols is None
    assert defaults.interval_seconds == 60.0
    assert defaults.cycles == 0
    assert defaults.runtime_dir == "runtime/news"


def test_research_watch_parser_supports_multiple_symbols_and_bounded_cycles() -> None:
    args = build_parser().parse_args(
        [
            "ashare-research-watch",
            "--symbol",
            "600000.SH",
            "--symbol",
            "000001.SZ",
            "--interval",
            "1m",
            "--lookback-minutes",
            "120",
            "--interval-seconds",
            "0.25",
            "--cycles",
            "4",
            "--macro",
            "--notify-target-kind",
            "private",
            "--notify-target-id",
            "12345",
        ]
    )

    assert args.symbols == ["600000.SH", "000001.SZ"]
    assert args.interval == "1m"
    assert args.lookback_minutes == 120
    assert args.interval_seconds == 0.25
    assert args.cycles == 4
    assert args.macro is True
    assert args.notify_target_id == "12345"

    defaults = build_parser().parse_args(["ashare-research-watch"])
    assert defaults.symbols is None
    assert defaults.watchlist == "config/ashare_research_watchlist.toml"
    assert defaults.watchlist_all is False
    assert defaults.interval == "5m"
    assert defaults.cycles == 3


def test_source_health_parser_and_offline_summary(tmp_path: Path) -> None:
    from gribuki_trade.storage import (
        ProviderRun,
        ProviderRunStatus,
        SQLiteSourceHealthStore,
    )

    args = build_parser().parse_args(
        [
            "ashare-source-health",
            "--source-id",
            "akshare.global_sina",
            "--days",
            "10",
            "--runtime-dir",
            str(tmp_path),
        ]
    )
    assert args.operation == "news_collect"
    assert args.days == 10

    finished = datetime.now(UTC) - timedelta(minutes=1)
    with SQLiteSourceHealthStore(tmp_path / "source_health.sqlite3") as store:
        store.append(
            ProviderRun(
                run_id="run-1",
                source_id="akshare.global_sina",
                operation="news_collect",
                started_at=finished - timedelta(milliseconds=250),
                finished_at=finished,
                status=ProviderRunStatus.SUCCESS,
                item_count=12,
                degraded=False,
                stale=False,
                latency_ms=250,
            )
        )

    result = _ashare_source_health(None, "news_collect", 10, str(tmp_path))

    assert result["operation"] == "news_collect"
    sources = result["sources"]
    assert isinstance(sources, list)
    assert sources[0]["source_id"] == "akshare.global_sina"
    assert sources[0]["success_rate"] == 1.0
    assert sources[0]["p95_latency_ms"] == 250.0


@pytest.mark.parametrize("value", ["-1", "1.5", "nope"])
def test_cli_rejects_invalid_news_watch_cycles(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        _non_negative_integer(value)


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "nope"])
def test_cli_rejects_invalid_news_watch_interval(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        _positive_float(value)


@pytest.mark.parametrize("interval", [0.0, -1.0, float("nan"), float("inf")])
def test_news_watch_helper_rejects_invalid_interval(interval: float) -> None:
    with pytest.raises(ValueError, match="interval_seconds"):
        asyncio.run(_ashare_news_watch(None, None, interval, 1, "unused"))


def test_news_watch_helper_rejects_negative_cycles() -> None:
    with pytest.raises(ValueError, match="cycles"):
        asyncio.run(_ashare_news_watch(None, None, 1.0, -1, "unused"))


def test_news_watch_runs_exact_finite_cycles_without_network(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import gribuki_trade.ingest as ingest
    import gribuki_trade.services as services
    import gribuki_trade.storage as storage

    captured_source_ids: list[str] = []
    sleep_calls: list[float] = []
    event_store_paths: list[Path] = []
    raw_store_paths: list[Path] = []

    class FakeNewsSource:
        def __init__(self, config: object) -> None:
            self.config = config

    class FakeEventStore:
        def __init__(self, path: Path) -> None:
            event_store_paths.append(path)

        def __enter__(self) -> "FakeEventStore":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def latest(self, *, limit: int) -> tuple[object, ...]:
            assert limit == 10_000
            return object(), object()

    class FakeRawStore:
        def __init__(self, path: Path) -> None:
            raw_store_paths.append(path)

    class FakeCollectionService:
        def __init__(
            self,
            sources: dict[str, object],
            *,
            raw_store: object,
            event_store: object,
            observer: object | None = None,
        ) -> None:
            del raw_store, event_store
            self.source_ids = tuple(sources)
            self.observer = observer
            captured_source_ids.extend(self.source_ids)

        async def run_once(self) -> tuple[SimpleNamespace, ...]:
            results = tuple(
                SimpleNamespace(
                    documents_saved=1,
                    error_code=None,
                    events_duplicate=0,
                    events_new=1,
                    events_revised=0,
                    source_id=source_id,
                    status=services.SourceRunStatus.SUCCESS,
                )
                for source_id in self.source_ids
            )
            if self.observer is not None:
                observed_at = datetime.now(UTC)
                for result in results:
                    self.observer(
                        services.SourceCollectionObservation(
                            result=result,
                            started_at=observed_at,
                            finished_at=observed_at,
                            latency_ms=1.0,
                        )
                    )
            return results

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(ingest, "AKShareNewsSource", FakeNewsSource)
    monkeypatch.setattr(services, "NewsCollectionService", FakeCollectionService)
    monkeypatch.setattr(storage, "SQLiteEventStore", FakeEventStore)
    monkeypatch.setattr(storage, "FileRawDocumentStore", FakeRawStore)
    monkeypatch.setattr(cli.asyncio, "sleep", fake_sleep)
    runtime_dir = tmp_path / "news-watch"

    result = asyncio.run(
        _ashare_news_watch(
            ["global_sina"],
            ["600000.SH"],
            0.25,
            3,
            str(runtime_dir),
        )
    )

    assert result == {
        "completed_cycles": 3,
        "latest_sources": [
            {
                "documents_saved": 1,
                "error_code": None,
                "events_duplicate": 0,
                "events_new": 1,
                "events_revised": 0,
                "source_id": "akshare.global_sina",
                "status": "SUCCESS",
            },
            {
                "documents_saved": 1,
                "error_code": None,
                "events_duplicate": 0,
                "events_new": 1,
                "events_revised": 0,
                "source_id": "akshare.individual_eastmoney.600000",
                "status": "SUCCESS",
            },
        ],
        "retained_latest_event_count": 2,
        "runtime_dir": str(runtime_dir.resolve()),
    }
    assert captured_source_ids == [
        "akshare.global_sina",
        "akshare.individual_eastmoney.600000",
    ]
    assert sleep_calls == [0.25, 0.25]
    assert event_store_paths == [runtime_dir.resolve() / "events.sqlite3"]
    assert raw_store_paths == [runtime_dir.resolve() / "raw"]
    health = _ashare_source_health(None, "news_collect", 1, str(runtime_dir))
    assert [item["source_id"] for item in health["sources"]] == [
        "akshare.global_sina",
        "akshare.individual_eastmoney.600000",
    ]
    assert all(item["total_runs"] == 3 for item in health["sources"])
    cycle_lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [line["cycle"] for line in cycle_lines] == [1, 2, 3]
    assert all(len(line["sources"]) == 2 for line in cycle_lines)


def test_research_watch_isolates_symbol_failures_and_runs_exact_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def fake_research_once(
        symbol: str,
        interval_value: str,
        lookback_minutes: int,
        events_db: str,
        research_db: str,
        outbox_db: str,
        macro_enabled: bool,
        macro_provider: str,
        model: str,
        notify_target_kind: str | None,
        notify_target_id: str | None,
    ) -> dict[str, object]:
        del (
            interval_value,
            lookback_minutes,
            events_db,
            research_db,
            outbox_db,
            macro_enabled,
            macro_provider,
            model,
            notify_target_kind,
            notify_target_id,
        )
        calls.append(symbol)
        if symbol == "000001.SZ":
            raise RuntimeError("provider URL and secret must not escape")
        return {"symbol": symbol, "decision": "ABSTAIN"}

    monkeypatch.setattr(cli, "_ashare_research_once", fake_research_once)

    result = asyncio.run(
        _ashare_research_watch(
            ["600000.SH", "000001.SZ"],
            "unused-watchlist.toml",
            False,
            "5m",
            480,
            0,
            2,
            "events.sqlite3",
            "research.sqlite3",
            "outbox.sqlite3",
            False,
            "openai",
            "gpt-5.6",
            None,
            None,
        )
    )

    assert calls == ["600000.SH", "000001.SZ"] * 2
    assert result["cycles_completed"] == 2
    assert result["symbols_attempted"] == 4
    assert result["succeeded"] == 2
    assert result["failed"] == 2
    failed = result["cycles"][0]["symbols"][1]
    assert failed == {
        "error_code": "research_run_failed",
        "result": None,
        "status": "FAILED",
        "symbol": "000001.SZ",
    }
    assert "provider URL" not in repr(result)


def test_research_watch_can_resolve_active_candidates_only(tmp_path: Path) -> None:
    from gribuki_trade.domain.candidates import CandidateSource
    from gribuki_trade.services.candidate_universe import (
        CandidateDiscovery,
        CandidateUniverseService,
    )
    from gribuki_trade.storage.candidate_store import SQLiteCandidateStore

    candidate_db = tmp_path / "candidates.sqlite3"
    at = datetime.fromisoformat("2026-08-14T10:00:00+08:00")
    with SQLiteCandidateStore(candidate_db) as store:
        service = CandidateUniverseService(store, clock=lambda: at)
        service.upsert(
            CandidateDiscovery(
                symbol="600000.SH",
                source=CandidateSource.MANUAL,
                source_run_id="manual-test",
                discovered_at=at,
                observed_at=at,
                reason_codes=("TEST",),
            )
        )
        service.upsert(
            CandidateDiscovery(
                symbol="000001.SZ",
                source=CandidateSource.MANUAL,
                source_run_id="manual-cooling-test",
                discovered_at=at,
                observed_at=at,
                reason_codes=("TEST",),
            )
        )
        service.cool(
            "000001.SZ",
            reason_code="TEST_COOL",
            at=at + timedelta(seconds=1),
            until=at + timedelta(hours=1),
        )

    resolved = cli._resolve_dynamic_research_symbols(
        symbols=None,
        watchlist_path="unused.toml",
        watchlist_all=False,
        candidate_store_path=str(candidate_db),
        candidates_only=True,
        as_of=at + timedelta(minutes=1),
    )

    assert resolved == ("600000.SH",)


def test_main_dispatches_news_watch_arguments_and_prints_summary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    async def fake_watch(
        feeds: list[str] | None,
        symbols: list[str] | None,
        interval_seconds: float,
        cycles: int,
        runtime_dir: str,
    ) -> dict[str, object]:
        captured.update(
            feeds=feeds,
            symbols=symbols,
            interval_seconds=interval_seconds,
            cycles=cycles,
            runtime_dir=runtime_dir,
        )
        return {"completed_cycles": cycles}

    monkeypatch.setattr(cli, "_ashare_news_watch", fake_watch)

    exit_code = cli.main(
        [
            "ashare-news-watch",
            "--feed",
            "global_sina",
            "--symbol",
            "000001.SZ",
            "--interval-seconds",
            "2.5",
            "--cycles",
            "2",
            "--runtime-dir",
            "runtime-test",
        ]
    )

    assert exit_code == 0
    assert captured == {
        "feeds": ["global_sina"],
        "symbols": ["000001.SZ"],
        "interval_seconds": 2.5,
        "cycles": 2,
        "runtime_dir": "runtime-test",
    }
    assert json.loads(capsys.readouterr().out) == {"completed_cycles": 2}


def test_napcat_dispatch_parser_is_finite_and_requires_exact_target() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "napcat-dispatch",
            "--base-url",
            "http://127.0.0.1:3001",
            "--target-kind",
            "group",
            "--target-id",
            "12345",
            "--outbox-path",
            "runtime/test-outbox.sqlite3",
            "--cycles",
            "4",
            "--poll-interval",
            "0",
        ]
    )

    assert args.command == "napcat-dispatch"
    assert args.base_url == "http://127.0.0.1:3001"
    assert args.target_kind == "group"
    assert args.target_id == "12345"
    assert args.outbox_path == "runtime/test-outbox.sqlite3"
    assert args.cycles == 4
    assert args.poll_interval == 0

    defaults = parser.parse_args(
        ["napcat-dispatch", "--target-kind", "private", "--target-id", "12345"]
    )
    assert defaults.cycles == 1
    assert defaults.poll_interval == 1.0
    assert defaults.outbox_path == "runtime/notifications/outbox.sqlite3"

    with pytest.raises(SystemExit):
        parser.parse_args(["napcat-dispatch", "--target-kind", "private"])
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "napcat-dispatch",
                "--target-kind",
                "private",
                "--target-id",
                "12345",
                "--cycles",
                "0",
            ]
        )


@pytest.mark.parametrize("value", ["-1", "nan", "inf", "nope"])
def test_cli_rejects_invalid_non_negative_float(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        _non_negative_float(value)


@pytest.mark.parametrize(
    ("target_kind", "private_ids", "group_ids"),
    [
        ("private", frozenset({"12345"}), frozenset()),
        ("group", frozenset(), frozenset({"12345"})),
    ],
)
def test_napcat_dispatch_helper_uses_exact_allowlist_and_closes_resources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    target_kind: str,
    private_ids: frozenset[str],
    group_ids: frozenset[str],
) -> None:
    import gribuki_trade.adapters.notifiers as notifiers
    import gribuki_trade.services as services
    import gribuki_trade.storage as storage

    state: dict[str, object] = {}

    class FakeOutbox:
        def __init__(self, path: Path) -> None:
            state["outbox_path"] = path

        def __enter__(self) -> "FakeOutbox":
            state["outbox_entered"] = True
            return self

        def __exit__(self, *_args: object) -> None:
            state["outbox_closed"] = True

    class FakeNotifier:
        channel = "onebot"

        def __init__(self, config: object) -> None:
            state["config"] = config

        async def __aenter__(self) -> "FakeNotifier":
            state["notifier_entered"] = True
            return self

        async def __aexit__(self, *_args: object) -> None:
            state["notifier_closed"] = True

    class FakeDispatchService:
        def __init__(
            self,
            outbox: object,
            notifier_map: dict[str, object],
            *,
            target_kind: object,
            target_id: str,
        ) -> None:
            state["service_outbox"] = outbox
            state["notifier_map"] = notifier_map
            state["dispatch_scope"] = (target_kind, target_id)

        async def poll(
            self,
            *,
            max_cycles: int,
            poll_interval: float,
        ) -> SimpleNamespace:
            state["poll"] = (max_cycles, poll_interval)
            return SimpleNamespace(
                claimed=3,
                cycles_completed=max_cycles,
                dead=0,
                expired=1,
                reached_cycle_limit=True,
                retry_scheduled=1,
                sent=1,
                stop_requested=False,
            )

    monkeypatch.setattr(cli, "_required_local_secret", lambda _name: "dummy-token")
    monkeypatch.setattr(notifiers, "OneBotNotifier", FakeNotifier)
    monkeypatch.setattr(services, "NotificationDispatchService", FakeDispatchService)
    monkeypatch.setattr(storage, "SQLiteOutbox", FakeOutbox)
    outbox_path = tmp_path / "nested" / "outbox.sqlite3"

    result = asyncio.run(
        _napcat_dispatch(
            "http://127.0.0.1:3000",
            target_kind,
            "12345",
            str(outbox_path),
            2,
            0.25,
        )
    )

    config = state["config"]
    assert config.private_target_ids == private_ids  # type: ignore[attr-defined]
    assert config.group_target_ids == group_ids  # type: ignore[attr-defined]
    assert state["outbox_path"] == outbox_path.resolve()
    assert state["poll"] == (2, 0.25)
    assert state["dispatch_scope"] == (target_kind, "12345")
    assert set(state["notifier_map"]) == {"onebot"}  # type: ignore[arg-type]
    assert state["notifier_entered"] is True
    assert state["notifier_closed"] is True
    assert state["outbox_entered"] is True
    assert state["outbox_closed"] is True
    assert result == {
        "base_url": "http://127.0.0.1:3000",
        "claimed": 3,
        "cycles_completed": 2,
        "dead": 0,
        "expired": 1,
        "outbox_path": str(outbox_path.resolve()),
        "poll_interval": 0.25,
        "reached_cycle_limit": True,
        "retry_scheduled": 1,
        "sent": 1,
        "stop_requested": False,
        "target_id": "12345",
        "target_kind": target_kind,
    }


def test_napcat_dispatch_closes_resources_when_polling_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import gribuki_trade.adapters.notifiers as notifiers
    import gribuki_trade.services as services
    import gribuki_trade.storage as storage

    closed = {"notifier": False, "outbox": False}

    class FakeOutbox:
        def __init__(self, _path: Path) -> None:
            pass

        def __enter__(self) -> "FakeOutbox":
            return self

        def __exit__(self, *_args: object) -> None:
            closed["outbox"] = True

    class FakeNotifier:
        channel = "onebot"

        def __init__(self, _config: object) -> None:
            pass

        async def __aenter__(self) -> "FakeNotifier":
            return self

        async def __aexit__(self, *_args: object) -> None:
            closed["notifier"] = True

    class FailingDispatchService:
        def __init__(
            self,
            _outbox: object,
            _notifiers: dict[str, object],
            **_scope: object,
        ) -> None:
            pass

        async def poll(self, **_kwargs: object) -> None:
            raise RuntimeError("dispatch failed")

    monkeypatch.setattr(cli, "_required_local_secret", lambda _name: "dummy-token")
    monkeypatch.setattr(notifiers, "OneBotNotifier", FakeNotifier)
    monkeypatch.setattr(services, "NotificationDispatchService", FailingDispatchService)
    monkeypatch.setattr(storage, "SQLiteOutbox", FakeOutbox)

    with pytest.raises(RuntimeError, match="dispatch failed"):
        asyncio.run(
            _napcat_dispatch(
                "http://127.0.0.1:3000",
                "private",
                "12345",
                str(tmp_path / "outbox.sqlite3"),
                1,
                0,
            )
        )

    assert closed == {"notifier": True, "outbox": True}


def test_main_dispatches_finite_napcat_outbox_arguments(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    async def fake_dispatch(
        base_url: str,
        target_kind: str,
        target_id: str,
        outbox_path: str,
        cycles: int,
        poll_interval: float,
    ) -> dict[str, object]:
        captured.update(
            base_url=base_url,
            target_kind=target_kind,
            target_id=target_id,
            outbox_path=outbox_path,
            cycles=cycles,
            poll_interval=poll_interval,
        )
        return {"cycles_completed": cycles, "sent": 0}

    monkeypatch.setattr(cli, "_napcat_dispatch", fake_dispatch)

    exit_code = cli.main(
        [
            "napcat-dispatch",
            "--target-kind",
            "private",
            "--target-id",
            "12345",
            "--outbox-path",
            "runtime/outbox.sqlite3",
            "--cycles",
            "2",
            "--poll-interval",
            "0.5",
        ]
    )

    assert exit_code == 0
    assert captured == {
        "base_url": "http://127.0.0.1:3000",
        "target_kind": "private",
        "target_id": "12345",
        "outbox_path": "runtime/outbox.sqlite3",
        "cycles": 2,
        "poll_interval": 0.5,
    }
    assert json.loads(capsys.readouterr().out) == {
        "cycles_completed": 2,
        "sent": 0,
    }


def test_ashare_research_parser_defaults_and_conditional_arguments() -> None:
    parser = build_parser()
    defaults = parser.parse_args(["ashare-research-once"])

    assert defaults.symbol == "600000.SH"
    assert defaults.interval == "5m"
    assert defaults.lookback_minutes == 480
    assert defaults.events_db == "runtime/news/events.sqlite3"
    assert defaults.research_db == "runtime/research/research.sqlite3"
    assert defaults.outbox_db == "runtime/research/outbox.sqlite3"
    assert defaults.macro is False
    assert defaults.macro_provider is None
    assert defaults.model is None
    assert defaults.notify_target_kind is None
    assert defaults.notify_target_id is None

    configured = parser.parse_args(
        [
            "ashare-research-once",
            "--symbol",
            "000001.SZ",
            "--interval",
            "1m",
            "--lookback-minutes",
            "120",
            "--events-db",
            "events.sqlite3",
            "--research-db",
            "research.sqlite3",
            "--outbox-db",
            "outbox.sqlite3",
            "--macro",
            "--macro-provider",
            "openai",
            "--model",
            "test-model",
            "--notify-target-kind",
            "group",
            "--notify-target-id",
            "12345",
        ]
    )

    assert configured.symbol == "000001.SZ"
    assert configured.interval == "1m"
    assert configured.lookback_minutes == 120
    assert configured.macro is True
    assert configured.macro_provider == "openai"
    assert configured.model == "test-model"
    assert configured.notify_target_kind == "group"
    assert configured.notify_target_id == "12345"

    # argparse 会接收成对参数中的任意一半，使辅助函数能够在打开存储或
    # 读取密钥前给出稳定的成对参数错误。
    incomplete = parser.parse_args(["ashare-research-once", "--notify-target-kind", "private"])
    assert incomplete.notify_target_kind == "private"
    assert incomplete.notify_target_id is None

    with pytest.raises(SystemExit):
        parser.parse_args(["ashare-research-once", "--interval", "15m"])
    with pytest.raises(SystemExit):
        parser.parse_args(["ashare-research-once", "--lookback-minutes", "0"])


@pytest.mark.parametrize(
    ("target_kind", "target_id"),
    [("private", None), (None, "12345")],
)
def test_ashare_research_requires_notification_arguments_as_a_pair(
    target_kind: str | None,
    target_id: str | None,
) -> None:
    with pytest.raises(ValueError, match="supplied together"):
        asyncio.run(
            _ashare_research_once(
                "600000.SH",
                "5m",
                60,
                "unused-events.sqlite3",
                "unused-research.sqlite3",
                "unused-outbox.sqlite3",
                False,
                "deepseek",
                "unused-model",
                target_kind,
                target_id,
            )
        )


def _install_research_cli_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stored_new: bool,
    macro_score: Decimal | None,
    macro_failure_code: str | None = None,
    market_failure_code: str | None = None,
) -> dict[str, object]:
    import gribuki_trade.adapters as adapters
    import gribuki_trade.adapters.llm as llm_adapters
    import gribuki_trade.services as services
    import gribuki_trade.storage as storage

    fixed_now = datetime(2026, 8, 13, 2, 30, tzinfo=UTC)
    state: dict[str, object] = {"fixed_now": fixed_now}
    event_paths: list[Path] = []
    outbox_paths: list[Path] = []
    research_paths: list[Path] = []
    state["event_paths"] = event_paths
    state["outbox_paths"] = outbox_paths
    state["research_paths"] = research_paths
    event = SimpleNamespace(event_id="event-1")
    reference = SimpleNamespace(evidence_id="revision-1")
    selection = SimpleNamespace(references=(reference,))
    macro_analysis = SimpleNamespace(name="bounded-macro-analysis")
    state["event"] = event
    state["selection"] = selection
    state["macro_analysis"] = macro_analysis
    recommendation = SimpleNamespace(
        as_of=fixed_now,
        confidence=SimpleNamespace(value="MEDIUM"),
        decision=SimpleNamespace(value="WATCH"),
        evidence=(
            SimpleNamespace(
                evidence_id="revision-1",
                title="测试证据",
                canonical_url="https://example.test/evidence",
            ),
        ),
        macro_score=macro_score,
        reason_codes=("TECHNICAL_WATCH",),
        recommendation_id="recommendation-1",
        reference_price=Decimal("10.25"),
        symbol="600000.SH",
        technical_score=Decimal("0.42"),
        uncertainties=("research-only",),
    )
    state["recommendation"] = recommendation

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            return fixed_now if tz is None else fixed_now.astimezone(tz)  # type: ignore[arg-type]

    class FakeMarketDataAdapter:
        def __init__(self) -> None:
            state["market_adapter_created"] = True

    class FakeEventStore:
        def __init__(self, path: Path) -> None:
            event_paths.append(path)

        def __enter__(self) -> "FakeEventStore":
            state["event_store_entered"] = True
            return self

        def __exit__(self, *_args: object) -> None:
            state["event_store_closed"] = True

        def latest(self, *, limit: int) -> tuple[object, ...]:
            assert limit == 1_000
            return (event,)

    class FakeOutbox:
        def __init__(self, path: Path) -> None:
            outbox_paths.append(path)

        def __enter__(self) -> "FakeOutbox":
            state["outbox_entered"] = True
            return self

        def __exit__(self, *_args: object) -> None:
            state["outbox_closed"] = True

    class FakeResearchStore:
        def __init__(self, path: Path) -> None:
            research_paths.append(path)

        def __enter__(self) -> "FakeResearchStore":
            state["research_store_entered"] = True
            return self

        def __exit__(self, *_args: object) -> None:
            state["research_store_closed"] = True

        def append_recommendation(self, item: object) -> bool:
            state["persisted_recommendation"] = item
            return stored_new

    class FakeResearchService:
        def __init__(
            self,
            market_data: object,
            *,
            outbox: object | None = None,
            clock: object | None = None,
        ) -> None:
            state["research_market_data"] = market_data
            if outbox is not None:
                state["research_outbox"] = outbox
            if clock is not None:
                state["research_clock"] = clock

        async def collect_market_data(self, request: object) -> SimpleNamespace:
            state["market_collection_request"] = request
            collection = SimpleNamespace(
                bars=() if market_failure_code is not None else (object(),),
                failure_code=market_failure_code,
            )
            state["market_collection"] = collection
            return collection

        def evaluate_collection(
            self,
            request: object,
            collection: object,
            *,
            notification_target: object,
        ) -> SimpleNamespace:
            state["research_request"] = request
            state["evaluated_collection"] = collection
            state["notification_target"] = notification_target
            return SimpleNamespace(
                recommendation=recommendation,
                notification_enqueued=notification_target is not None,
                failure_code=collection.failure_code,  # type: ignore[attr-defined]
            )

    class FakeAnalyzer:
        def __init__(self, api_key: object, *, model: str) -> None:
            state["analyzer_api_key"] = api_key.reveal()  # type: ignore[attr-defined]
            state["analyzer_model"] = model

    class FakeMacroService:
        def __init__(self, analyzer: object) -> None:
            state["macro_analyzer"] = analyzer

        async def analyze(self, **kwargs: object) -> SimpleNamespace:
            state["macro_call"] = kwargs
            return SimpleNamespace(
                analysis=macro_analysis,
                baseline_analysis=macro_analysis,
                adversarial_analysis=macro_analysis,
                selected_track="ADVERSARIAL",
                dual_audit_record_sha256="d" * 64,
                selection=selection,
                failure_code=macro_failure_code,
            )

    class FakeOwnedDualAnalyzer:
        def __init__(self, baseline: object) -> None:
            self.baseline = baseline

        def __enter__(self) -> "FakeOwnedDualAnalyzer":
            state["dual_entered"] = True
            return self

        def __exit__(self, *_args: object) -> None:
            state["dual_closed"] = True

    def fake_build_dual(
        baseline: object,
        **kwargs: object,
    ) -> FakeOwnedDualAnalyzer:
        state["dual_build"] = kwargs
        return FakeOwnedDualAnalyzer(baseline)

    def fake_select(
        symbol: str,
        as_of: datetime,
        events: tuple[object, ...],
    ) -> SimpleNamespace:
        state["selection_call"] = (symbol, as_of, events)
        return selection

    monkeypatch.setattr(cli, "datetime", FixedDateTime)
    monkeypatch.setattr(adapters, "AKShareMarketDataAdapter", FakeMarketDataAdapter)
    monkeypatch.setattr(llm_adapters, "DeepSeekChatMacroAnalyzer", FakeAnalyzer)
    monkeypatch.setattr(llm_adapters, "OpenAIResponsesMacroAnalyzer", FakeAnalyzer)
    monkeypatch.setattr(services, "AShareResearchService", FakeResearchService)
    monkeypatch.setattr(services, "MacroResearchService", FakeMacroService)
    monkeypatch.setattr(services, "build_production_dual_track_analyzer", fake_build_dual)
    monkeypatch.setattr(services, "select_macro_evidence", fake_select)
    monkeypatch.setattr(storage, "SQLiteEventStore", FakeEventStore)
    monkeypatch.setattr(storage, "SQLiteOutbox", FakeOutbox)
    monkeypatch.setattr(storage, "SQLiteResearchStore", FakeResearchStore)
    return state


def test_ashare_research_helper_technical_only_is_offline_and_persisted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state = _install_research_cli_fakes(
        monkeypatch,
        stored_new=True,
        macro_score=None,
    )
    monkeypatch.setattr(
        cli,
        "_required_local_secret",
        lambda _name: pytest.fail("macro-off must not read an API key"),
    )
    events_db = tmp_path / "events.sqlite3"
    events_db.touch()
    research_db = tmp_path / "research" / "research.sqlite3"
    outbox_db = tmp_path / "outbox" / "outbox.sqlite3"

    result = asyncio.run(
        _ashare_research_once(
            "600000.SH",
            "1m",
            120,
            str(events_db),
            str(research_db),
            str(outbox_db),
            False,
            "deepseek",
            "unused-model",
            None,
            None,
        )
    )

    request = state["research_request"]
    collection_request = state["market_collection_request"]
    assert collection_request.decision_time is None  # type: ignore[attr-defined]
    assert request.decision_time == state["fixed_now"]  # type: ignore[attr-defined]
    assert state["evaluated_collection"] is state["market_collection"]
    assert request.interval.value == "1m"  # type: ignore[attr-defined]
    assert request.macro is None  # type: ignore[attr-defined]
    assert request.evidence == state["selection"].references  # type: ignore[attr-defined]
    assert state["selection_call"] == (
        "600000.SH",
        state["fixed_now"],
        (state["event"],),
    )
    assert state["notification_target"] is None
    assert state["persisted_recommendation"] is state["recommendation"]
    assert state["event_store_closed"] is True
    assert state["outbox_closed"] is True
    assert state["research_store_closed"] is True
    assert result["macro_enabled"] is False
    assert result["macro_failure_code"] is None
    assert result["market_data_failure_code"] is None
    assert result["notification_enqueued"] is False
    assert result["stored_new"] is True
    assert result["research_db"] == str(research_db.resolve())
    assert result["macro_score"] is None
    assert result["evidence"] == [
        {
            "evidence_id": "revision-1",
            "title": "测试证据",
            "url": "https://example.test/evidence",
        }
    ]


def test_ashare_research_helper_macro_and_notification_are_offline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state = _install_research_cli_fakes(
        monkeypatch,
        stored_new=False,
        macro_score=Decimal("0.35"),
        macro_failure_code="ANALYZER_FAILED",
    )
    requested_secrets: list[str] = []

    def fake_secret(name: str) -> str:
        requested_secrets.append(name)
        return "dummy-deepseek-key"

    monkeypatch.setattr(cli, "_required_local_secret", fake_secret)
    events_db = tmp_path / "events.sqlite3"
    events_db.touch()

    result = asyncio.run(
        _ashare_research_once(
            "600000.SH",
            "5m",
            60,
            str(events_db),
            str(tmp_path / "research.sqlite3"),
            str(tmp_path / "outbox.sqlite3"),
            True,
            "deepseek",
            None,
            "group",
            "12345",
        )
    )

    assert requested_secrets == [cli.DEEPSEEK_API_KEY_SECRET]
    assert state["analyzer_api_key"] == "dummy-deepseek-key"
    assert state["analyzer_model"] == "deepseek-v4-flash"
    macro_call = state["macro_call"]
    assert macro_call["symbol"] == "600000.SH"  # type: ignore[index]
    assert macro_call["events"] == (state["event"],)  # type: ignore[index]
    request = state["research_request"]
    assert request.interval.value == "5m"  # type: ignore[attr-defined]
    assert request.macro is state["macro_analysis"]  # type: ignore[attr-defined]
    target = state["notification_target"]
    assert target.target_kind.value == "group"  # type: ignore[attr-defined]
    assert target.target_id == "12345"  # type: ignore[attr-defined]
    assert result["macro_enabled"] is True
    assert result["macro_failure_code"] == "ANALYZER_FAILED"
    assert result["macro_score"] == "0.35"
    assert result["notification_enqueued"] is True
    assert result["stored_new"] is False


def test_ashare_research_skips_macro_when_market_data_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_research_cli_fakes(
        monkeypatch,
        stored_new=True,
        macro_score=None,
        market_failure_code="MARKET_DATA_FETCH_FAILED",
    )
    monkeypatch.setattr(
        cli,
        "_required_local_secret",
        lambda _name: pytest.fail("market failure must skip paid macro analysis"),
    )

    result = asyncio.run(
        _ashare_research_once(
            "510300.SH",
            "5m",
            60,
            str(tmp_path / "events.sqlite3"),
            str(tmp_path / "research.sqlite3"),
            str(tmp_path / "outbox.sqlite3"),
            True,
            "deepseek",
            None,
            None,
            None,
        )
    )

    assert result["market_data_failure_code"] == "MARKET_DATA_FETCH_FAILED"
    assert result["macro_failure_code"] == "SKIPPED_MARKET_DATA_UNAVAILABLE"


def test_main_dispatches_ashare_research_once_and_prints_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    async def fake_research(*args: object) -> dict[str, object]:
        captured["args"] = args
        return {"recommendation_id": "recommendation-1", "stored_new": True}

    monkeypatch.setattr(cli, "_ashare_research_once", fake_research)

    exit_code = cli.main(
        [
            "ashare-research-once",
            "--symbol",
            "000001.SZ",
            "--interval",
            "1m",
            "--lookback-minutes",
            "90",
            "--events-db",
            "events.sqlite3",
            "--research-db",
            "research.sqlite3",
            "--outbox-db",
            "outbox.sqlite3",
            "--macro",
            "--macro-provider",
            "openai",
            "--model",
            "macro-model",
            "--notify-target-kind",
            "private",
            "--notify-target-id",
            "12345",
        ]
    )

    assert exit_code == 0
    assert captured["args"] == (
        "000001.SZ",
        "1m",
        90,
        "events.sqlite3",
        "research.sqlite3",
        "outbox.sqlite3",
        True,
        "openai",
        "macro-model",
        "private",
        "12345",
    )
    assert json.loads(capsys.readouterr().out) == {
        "recommendation_id": "recommendation-1",
        "stored_new": True,
    }


def test_close_research_parser_defaults_and_explicit_controls() -> None:
    parser = build_parser()
    defaults = parser.parse_args(["ashare-close-research-once"])
    assert defaults.symbol == "510300.SH"
    assert defaults.history_days == 450
    assert defaults.refresh_news is True
    assert defaults.search_discovery is True
    assert defaults.searxng_url is None
    assert defaults.macro is True
    assert defaults.macro_provider is None
    assert defaults.macro_weight == Decimal("0.25")
    assert defaults.outbox_db == "runtime/notifications/outbox.sqlite3"

    explicit = parser.parse_args(
        [
            "ashare-close-research-once",
            "--session-date",
            "2026-08-13",
            "--next-session",
            "2026-08-14",
            "--news-feed",
            "global_sina",
            "--news-feed",
            "global_cailianpress",
            "--no-refresh-news",
            "--no-search-discovery",
            "--no-macro",
            "--macro-weight",
            "0.35",
            "--held",
        ]
    )
    assert explicit.session_date == date(2026, 8, 13)
    assert explicit.next_session == date(2026, 8, 14)
    assert explicit.news_feeds == ["global_sina", "global_cailianpress"]
    assert explicit.refresh_news is False
    assert explicit.search_discovery is False
    assert explicit.macro is False
    assert explicit.macro_weight == Decimal("0.35")
    assert explicit.held is True

    with pytest.raises(SystemExit):
        parser.parse_args(["ashare-close-research-once", "--session-date", "2026/08/13"])
    with pytest.raises(SystemExit):
        parser.parse_args(["ashare-close-research-once", "--macro-weight", "0.41"])


def test_close_research_batch_parser_is_bounded_and_accepts_candidates() -> None:
    parser = build_parser()
    parsed = parser.parse_args(
        [
            "ashare-close-research-batch",
            "--symbol",
            "600000.SH",
            "--symbol",
            "000001.SZ",
            "--candidate-db",
            "candidates.sqlite3",
            "--limit",
            "7",
            "--held-symbol",
            "600000.SH",
            "--no-macro",
        ]
    )
    assert parsed.symbols == ["600000.SH", "000001.SZ"]
    assert parsed.candidate_db == "candidates.sqlite3"
    assert parsed.limit == 7
    assert parsed.held_symbols == ["600000.SH"]
    assert parsed.macro is False


def test_main_dispatches_close_research_batch_and_writes_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    captured: list[tuple[object, ...]] = []

    async def fake_batch(*args: object) -> dict[str, object]:
        captured.append(args)
        return {"ok": True, "selected_count": 2, "results": []}

    monkeypatch.setattr(cli, "_ashare_close_research_batch", fake_batch)
    output = tmp_path / "batch.json"
    assert (
        cli.main(
            [
                "ashare-close-research-batch",
                "--symbol",
                "600000.SH",
                "--symbol",
                "000001.SZ",
                "--no-refresh-news",
                "--no-search-discovery",
                "--no-macro",
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert captured[0][0] == ["600000.SH", "000001.SZ"]
    assert captured[0][1] is None
    assert captured[0][11:13] == (False, False)
    document = json.loads(output.read_text(encoding="utf-8"))
    assert document["selected_count"] == 2
    assert document["output"] == str(output.resolve())
    assert json.loads(capsys.readouterr().out) == document


def test_close_research_batch_deduplicates_and_isolates_one_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, bool]] = []

    async def fake_close(symbol: str, *args: object) -> dict[str, object]:
        held = bool(args[15])
        calls.append((symbol, held))
        if symbol == "000001.SZ":
            raise RuntimeError("provider response must not escape")
        return {
            "ok": True,
            "symbol": symbol,
            "decision": "WATCH",
            "combined_score": "0.125",
            "recommendation_id": "recommendation-1",
        }

    monkeypatch.setattr(cli, "_ashare_close_research_once", fake_close)
    result = asyncio.run(
        cli._ashare_close_research_batch(
            ["600000", "600000.SH", "000001.SZ"],
            None,
            10,
            450,
            None,
            None,
            str(tmp_path / "news"),
            str(tmp_path / "research.sqlite3"),
            str(tmp_path / "evidence"),
            str(tmp_path / "outbox.sqlite3"),
            None,
            False,
            False,
            None,
            False,
            "deepseek",
            None,
            Decimal("0.25"),
            ["600000.SH"],
            None,
            None,
            None,
        )
    )
    assert calls == [("600000.SH", True), ("000001.SZ", False)]
    assert result["requested_count"] == 2
    assert result["succeeded_count"] == 1
    assert result["failed_count"] == 1
    assert result["ok"] is False
    assert result["results"][1] == {
        "error_code": "UNEXPECTED_RESEARCH_FAILURE",
        "ok": False,
        "symbol": "000001.SZ",
    }


def test_close_research_infers_etf_semantics_before_applying_st_rules() -> None:
    assert cli._infer_close_instrument_type("510300.SH").value == "etf"
    assert cli._infer_close_instrument_type("159915.SZ").value == "etf"
    assert cli._infer_close_instrument_type("600000.SH").value == "stock"


def test_close_research_dynamic_profile_resolver_prefers_retained_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retained = SimpleNamespace(symbol="600000.SH")
    monkeypatch.setattr(
        cli,
        "_resolve_close_instrument_profile",
        lambda _symbol: retained,
    )

    profile, failure = asyncio.run(cli._resolve_close_instrument_profile_for_run("600000.SH"))
    assert profile is retained
    assert failure is None


def test_close_research_dynamic_profile_resolver_fetches_new_market_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import gribuki_trade.adapters as adapters

    dynamic = SimpleNamespace(symbol="601318.SH")
    captured: dict[str, object] = {}

    class FakeProfileAdapter:
        def __init__(self, *, timeout_seconds: float) -> None:
            captured["timeout_seconds"] = timeout_seconds

        async def fetch(self, symbol: str, *, known_at: datetime) -> object:
            captured["symbol"] = symbol
            captured["known_at"] = known_at
            return dynamic

    monkeypatch.setattr(cli, "_resolve_close_instrument_profile", lambda _symbol: None)
    monkeypatch.setattr(adapters, "AKShareInstrumentProfileAdapter", FakeProfileAdapter)

    profile, failure = asyncio.run(cli._resolve_close_instrument_profile_for_run("601318.SH"))
    assert profile is dynamic
    assert failure is None
    assert captured["symbol"] == "601318.SH"
    assert captured["timeout_seconds"] == 20.0
    assert captured["known_at"].tzinfo is not None


def test_close_research_dynamic_profile_resolver_returns_stable_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import gribuki_trade.adapters as adapters

    class FakeProfileAdapter:
        def __init__(self, *, timeout_seconds: float) -> None:
            assert timeout_seconds == 20.0

        async def fetch(self, symbol: str, *, known_at: datetime) -> object:
            del symbol, known_at
            raise adapters.InstrumentProfileDataError(
                adapters.InstrumentProfileFailureCode.PROFILE_TIMEOUT
            )

    monkeypatch.setattr(cli, "_resolve_close_instrument_profile", lambda _symbol: None)
    monkeypatch.setattr(adapters, "AKShareInstrumentProfileAdapter", FakeProfileAdapter)
    profile, failure = asyncio.run(cli._resolve_close_instrument_profile_for_run("601318.SH"))
    assert profile is None
    assert failure == "PROFILE_TIMEOUT"


def test_close_research_retains_daily_route_provenance() -> None:
    assert cli._daily_evidence_provider_id(None) == "baostock.daily"
    assert cli._daily_evidence_provider_id("BaoStock") == "baostock.daily"
    assert cli._daily_evidence_provider_id("AKShare/Sina fund_etf_hist_sina") == "akshare.daily"
    assert (
        cli._daily_evidence_provider_id("MIXED/TAIL_STITCH base=AKShare/Sina tail=BaoStock")
        == "mixed.tail_stitch.daily"
    )


def test_main_dispatches_close_research_and_prints_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: list[tuple[object, ...]] = []

    async def fake_close(*args: object) -> dict[str, object]:
        captured.append(args)
        return {"ok": True, "next_session": "2026-08-14"}

    monkeypatch.setattr(cli, "_ashare_close_research_once", fake_close)
    assert (
        cli.main(
            [
                "ashare-close-research-once",
                "--symbol",
                "000001.SZ",
                "--history-days",
                "365",
                "--session-date",
                "2026-08-13",
                "--next-session",
                "2026-08-14",
                "--news-runtime-dir",
                "news-runtime",
                "--research-db",
                "research.sqlite3",
                "--market-evidence-dir",
                "market-evidence",
                "--outbox-db",
                "outbox.sqlite3",
                "--news-feed",
                "global_sina",
                "--no-refresh-news",
                "--no-search-discovery",
                "--no-macro",
                "--macro-weight",
                "0.35",
                "--held",
                "--notify-target-kind",
                "private",
                "--notify-target-id",
                "12345",
            ]
        )
        == 0
    )
    assert captured == [
        (
            "000001.SZ",
            365,
            date(2026, 8, 13),
            date(2026, 8, 14),
            "news-runtime",
            "research.sqlite3",
            "market-evidence",
            "outbox.sqlite3",
            ["global_sina"],
            False,
            False,
            None,
            False,
            "deepseek",
            None,
            Decimal("0.35"),
            True,
            "private",
            "12345",
            "runtime/reports",
        )
    ]
    assert json.loads(capsys.readouterr().out) == {
        "next_session": "2026-08-14",
        "ok": True,
    }
