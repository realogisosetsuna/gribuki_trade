import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from gribuki_trade.cli import _ashare_paper_day_summary, build_parser
from gribuki_trade.reporting.contracts import (
    ReportKind,
    validate_markdown_report_contract,
)
from gribuki_trade.reporting.paper_day.paper_day_summary import (
    PaperDaySidecarError,
    project_paper_day_sidecars,
    render_paper_day_summary,
    write_paper_day_summary,
)

RUN_ID = "paper-day-0123456789abcdef0123456789abcdef01234567"
SESSION = date(2026, 8, 14)


def test_complete_sidecars_project_full_executive_funnel_and_money(tmp_path: Path) -> None:
    root = _complete_session(tmp_path)

    projected = project_paper_day_sidecars(root)

    assert projected.lifecycle == "COMPLETED"
    assert projected.coverage == "FULL_SESSION"
    assert projected.preopen_outcome == "COMPLETED"
    assert projected.preopen_candidate_count == 2
    assert projected.watchlist_initial_count == 2
    assert projected.watchlist_peak_count == 2
    assert projected.watchlist_current_count == 2
    assert projected.watchlist_update_count == 1
    assert projected.scan_count == 1
    assert projected.scan_status_counts == (("COMPLETE", 1),)
    assert projected.scan_universe_min == 5200
    assert projected.scan_universe_max == 5200
    assert projected.scan_universe_last == 5200
    assert projected.monitor_evaluations == 2
    assert projected.monitored_symbols == 1
    assert projected.technical_decision_counts == (
        ("ENTER_CANDIDATE", 1),
        ("REDUCE", 1),
    )
    assert projected.buy_signal_count == 1
    assert projected.buy_approved_count == 1
    assert projected.buy_rejected_count == 0
    assert projected.sell_signal_count == 1
    assert projected.sell_not_submitted_count == 1
    assert projected.orders_submitted == 1
    assert projected.orders_filled == 1
    assert projected.orders_expired == 0
    assert projected.fills_applied == 1
    assert projected.filled_shares == 100
    assert projected.fill_notional == Decimal("1000")
    assert projected.commission == Decimal("5")
    assert projected.transfer_fee == Decimal("0.1")
    assert projected.stamp_tax == Decimal("0")
    assert projected.starting_cash == Decimal("200000")
    assert projected.final_cash == Decimal("198994.9")
    assert projected.gross_exposure == Decimal("1050")
    assert projected.final_equity == Decimal("200044.9")
    assert projected.total_mark_to_market_pnl == Decimal("44.9")
    assert projected.realized_pnl == Decimal("0")
    assert projected.unrealized_pnl == Decimal("44.900")
    assert projected.positions[0].market_value == Decimal("1050.0")
    assert projected.notifications.required == 9
    assert projected.notifications.sent == 9
    assert projected.notifications.gaps == 0
    assert projected.notifications.retried is None
    assert projected.notifications.dead is None
    assert projected.notifications.exact_final_counts is True
    assert projected.notifications.artifact_delivery_status == "SENT"
    assert projected.notifications.artifact_delivery_complete is True
    assert projected.notifications.daily_review_delivery_complete is True
    assert projected.notifications.text_required == 8
    assert projected.notifications.text_sent == 8
    assert projected.notifications.text_gaps == 0
    assert projected.notifications.delivery_projection_exact is True
    assert projected.source_degradation_count == 1
    assert projected.source_recovery_count == 1


def test_render_and_atomic_write_put_summary_before_timeline_and_link_sources(
    tmp_path: Path,
) -> None:
    root = _complete_session(tmp_path)
    projection = project_paper_day_sidecars(root)

    rendered = render_paper_day_summary(projection)
    written = write_paper_day_summary(projection)

    assert rendered.index("## 执行摘要") < rendered.index("## 九、不可变事件时间线")
    validate_markdown_report_contract(ReportKind.DAILY_REVIEW, rendered)
    assert "全市场盘中扫描" in rendered
    assert "风控/入场拒绝" in rendered
    assert "outbox 重试状态未投影到 sidecar" in rendered
    assert "| Markdown 附件状态 | SENT | 已发送 |" in rendered
    assert "| 日报双交付完成 | 是 |" in rendered
    assert "session.log.jsonl" in rendered
    assert "原始运行报告" in rendered
    assert written.name == "ashare-paper-day-summary-2026-08-14-ef01234567.md"
    assert written.read_text(encoding="utf-8") == rendered
    assert not tuple(root.glob("*.sqlite3-wal"))


def test_policy_price_quantity_and_stable_codes_are_projected_and_explained(
    tmp_path: Path,
) -> None:
    root = _enhanced_policy_session(tmp_path)

    projection = project_paper_day_sidecars(root)
    rendered = render_paper_day_summary(projection)

    assert len(projection.risk_policy_changes) == 1
    change = projection.risk_policy_changes[0]
    assert change.operator_authorized is True
    assert change.reason_codes == (
        "REMOVE_POSITION_COUNT_CAP",
        "ADD_PRICE_ACCEPTANCE_BOUNDS",
        "ADD_BOARD_QUANTITY_RULES",
    )
    assert change.old_maximum_positions == 5
    assert change.new_maximum_positions is None
    assert change.new_position_count_limit_enabled is False
    assert change.validated_fill_count == 5
    assert change.validated_position_count == 5

    policy = projection.current_risk_policy
    assert policy is not None
    assert policy.maximum_positions is None
    assert policy.position_count_limit_enabled is False
    assert policy.cash_reserve_fraction == Decimal("0.20")
    assert policy.maximum_gross_fraction == Decimal("0.80")
    assert policy.maximum_symbol_fraction == Decimal("0.20")
    assert policy.risk_per_trade_fraction == Decimal("0.0075")
    assert policy.price_acceptance_policy_version == (
        "ashare-intraday-price-acceptance@1"
    )
    assert policy.order_quantity_policy_version == (
        "ashare-intraday-order-quantity@1"
    )
    star = next(item for item in policy.order_quantity_rules if item.board == "STAR")
    assert star.minimum_buy_quantity == 200
    assert star.buy_increment == 1
    assert star.maximum_limit_order_quantity == 100_000
    assert policy.unsupported_quantity_boards == ("BSE",)

    assert len(projection.buy_execution_acceptances) == 1
    buy = projection.buy_execution_acceptances[0]
    assert buy.symbol == "688180.SH"
    assert buy.quantity == 201
    assert buy.price_acceptance.acceptable_lower == Decimal("41.00")
    assert buy.price_acceptance.acceptable_upper == Decimal("41.50")
    assert buy.price_acceptance.invalidation_boundary == Decimal("40.99")
    assert buy.quantity_rule is not None
    assert buy.quantity_rule.minimum_buy_quantity == 200
    assert buy.quantity_rule.buy_increment == 1

    assert len(projection.sell_execution_acceptances) == 1
    sell = projection.sell_execution_acceptances[0]
    assert sell.symbol == "688180.SH"
    assert sell.price_acceptance.status == "AVAILABLE"
    assert sell.price_acceptance.acceptable_lower == Decimal("40.90")
    assert sell.price_acceptance.acceptable_upper == Decimal("49.20")
    assert sell.available_to_sell == 0
    assert sell.quantity_plan_status == "NO_SELLABLE_QUANTITY"
    assert sell.current_blockers == (
        "NO_SELL_ORDER_BY_DAY_TEST_POLICY",
        "T1_SELLABLE_QUANTITY_ZERO",
    )

    stable = {
        item.code: (item.count, item.explanation)
        for item in projection.stable_rejection_reasons
    }
    assert stable["NO_CURRENT_SESSION_ANOMALY"] == (
        1,
        "最近一次全市场扫描未将该标的列为当日异动候选，只有分钟技术形态而缺少横截面确认",
    )
    assert stable["CASH_RESERVE_BINDING"] == (
        1,
        "创建委托后会侵占最低现金储备",
    )
    assert stable["SIGNAL_INVALIDATED_BEFORE_FILL"] == (
        1,
        "撮合分钟触及或跌破失效位，买入逻辑已失效",
    )
    assert stable["T1_SELLABLE_QUANTITY_ZERO"][0] == 1

    assert "原 5 个持仓硬上限已解除，但这不等于无限可买" in rendered
    assert "| 最低现金储备 | 20.00% |" in rendered
    assert "| 组合总敞口上限 | 80.00% |" in rendered
    assert "`REMOVE_POSITION_COUNT_CAP`：取消原固定 5 个持仓的计数硬上限" in rendered
    assert "STAR（科创板）" in rendered
    assert "至少 200 股，其后按 1 股递增" in rendered
    assert "[41.000, 41.500]" in rendered
    assert "[40.900, 49.200]" in rendered
    assert "`T1_SELLABLE_QUANTITY_ZERO`：今日买入股份受 T+1 限制" in rendered
    assert "`LOCKED_LIMIT_DOWN_QUEUE_UNMODELED`" in rendered
    assert "`NO_CURRENT_SESSION_ANOMALY`" in rendered
    assert "缺少横截面确认" in rendered
    assert "大写英文是供日志检索、统计和自动化判断使用的稳定机器码" in rendered


def test_in_progress_projection_ignores_one_partial_trailing_append(
    tmp_path: Path,
) -> None:
    root = tmp_path / SESSION.isoformat()
    root.mkdir()
    event = _event(
        1,
        "DAY_STARTED",
        {"cash": "200000", "initial_cash": "200000"},
    )
    (root / "session.log.jsonl").write_bytes(
        (json.dumps(event) + "\n").encode() + b'{"sequence":2'
    )
    _write_status(root, event_count=2, latest="DAY_STARTED")

    projection = project_paper_day_sidecars(root)

    assert projection.lifecycle == "IN_PROGRESS"
    assert projection.coverage == "PENDING_FINAL_CLASSIFICATION"
    assert projection.sidecar_event_count == 1
    assert projection.final_cash is None
    assert any("尚未完成的并发追加片段" in item for item in projection.warnings)
    assert any("event_count" in item for item in projection.warnings)
    assert "不可得（sidecar 未记录）" in render_paper_day_summary(projection)


def test_operator_recovery_after_abort_returns_lifecycle_to_in_progress(
    tmp_path: Path,
) -> None:
    root = tmp_path / SESSION.isoformat()
    events = (
        _event(1, "DAY_STARTED", {"cash": "200000", "initial_cash": "200000"}),
        _event(
            2,
            "DAY_ABORTED",
            {"error_code": "UNEXPECTED_PAPER_DAY_FAILURE"},
            phase="TERMINAL",
        ),
        _event(
            3,
            "RUNNER_RECOVERY_AFTER_ABORT",
            {"operator_authorized": True, "resume_scope": "SAME_RUN_APPEND_ONLY"},
            phase="MORNING",
        ),
    )
    _write_events(root, events)
    _write_status(root, event_count=len(events), latest="RUNNER_RECOVERY_AFTER_ABORT")

    projection = project_paper_day_sidecars(root)

    assert projection.lifecycle == "IN_PROGRESS"
    assert projection.coverage == "PENDING_FINAL_CLASSIFICATION"


def test_llm_audit_health_gates_and_sell_bypass_are_projected(tmp_path: Path) -> None:
    root = tmp_path / "llm-audit"
    identity = {
        "requested_model": "deepseek-intraday-test",
        "prompt_schema_sha256": "a" * 64,
    }
    review = {
        "analyzer_identity": identity,
        "latency_ms": 120,
        "response_model": "deepseek-intraday-test",
    }
    events = (
        _event(1, "DAY_STARTED", {"initial_cash": "200000"}),
        _event(
            2,
            "LLM_INTRADAY_POLICY_CONFIGURED",
            {
                "manifest_binding": {
                    "analyzer_identity": identity,
                    "enabled": True,
                    "required_for_buy": True,
                }
            },
        ),
        _event(
            3,
            "LLM_EVIDENCE_SNAPSHOT_BOUND",
            {
                "evidence_snapshot_binding": {
                    "audit_sha256": "b" * 64,
                },
                "replay_available": True,
            },
        ),
        _event(
            4,
            "LLM_PREOPEN_CONTEXT_FROZEN",
            {
                "preopen_context": {
                    "context_id": "preopen-1",
                    "dual_track": {
                        "adversarial": {
                            "decision": "WATCH",
                            "macro_impact": "-0.20",
                            "model": "deepseek-adversarial-test",
                        },
                        "audit_record_sha256": "c" * 64,
                        "baseline": {
                            "decision": "PUBLISH",
                            "macro_impact": "0.30",
                            "model": "deepseek-baseline-test",
                        },
                        "selected_track": "ADVERSARIAL",
                    },
                }
            },
        ),
        _event(
            5,
            "LLM_REVIEW_BATCH_SCHEDULED",
            {
                "candidate_count": 2,
                "outcomes": [
                    {"status": "SCHEDULED", "symbol": "600000.SH"},
                    {"status": "SCHEDULED", "symbol": "000001.SZ"},
                ],
            },
        ),
        _event(6, "LLM_CANDIDATE_REVIEW_COMPLETED", {"review": review}),
        _event(
            7,
            "LLM_CANDIDATE_REVIEW_FAILED",
            {"review": {**review, "latency_ms": 480}},
        ),
        _event(
            8,
            "LLM_SERVICE_STATE_CHANGED",
            {
                "notification_text": "LLM degraded",
                "previous_state": "UNKNOWN",
                "state": "DEGRADED",
            },
        ),
        _event(
            9,
            "LLM_SERVICE_STATE_CHANGED",
            {
                "notification_text": "LLM recovered",
                "previous_state": "DEGRADED",
                "state": "HEALTHY",
            },
        ),
        _event(
            10,
            "BUY_LLM_GATE_EVALUATED",
            {
                "action": "DOWNGRADE_TO_WATCH",
                "blocks_entry": True,
                "reason_code": "LLM_COMBINED_SCORE_BELOW_ENTRY_THRESHOLD",
                "required_for_buy": True,
                "requested_model": "deepseek-intraday-test",
                "response_model": "deepseek-intraday-test",
                "prompt_schema_sha256": "a" * 64,
            },
            symbol="600000.SH",
        ),
        _event(
            11,
            "BUY_SIGNAL_TRIGGERED",
            {
                "gate_reason": "LLM_COMBINED_SCORE_BELOW_ENTRY_THRESHOLD",
                "risk_approved": False,
            },
            symbol="600000.SH",
        ),
        _event(
            12,
            "SELL_SIGNAL_TRIGGERED",
            {
                "deep_exit_sell_review": {
                    "action": "ESCALATE_REDUCE_URGENCY",
                    "combined_exit_score": "-0.70",
                    "llm_can_veto": False,
                    "selected_semantic_score": "-0.40",
                    "technical_score": "-0.60",
                },
                "llm_gate": {
                    "action": "NOT_APPLICABLE",
                    "reason_code": "LLM_NOT_APPLICABLE_REDUCE",
                }
            },
            symbol="600000.SH",
        ),
        _event(
            13,
            "LLM_REVIEW_CACHE_RESTORED",
            {"restored_review_count": 1},
        ),
        _event(14, "LLM_CANDIDATE_REVIEW_CACHE_REJECTED", {}),
        _event(
            15,
            "EXIT_PLAN_DEEP_APPLIED",
            {
                "deep_exit_llm_assessment": {
                    "adversarial_score": "0.54",
                    "available": True,
                    "baseline_score": "0.32",
                    "plan_id": "exit-plan-deep-fixture",
                    "protection_id": "paper-protection-fixture",
                    "selected_score": "0.54",
                    "selected_system": "ADVERSARIAL_LLM",
                    "status": "DEEP_ASSESSMENT_AVAILABLE",
                },
                "plan": {
                    "plan_id": "exit-plan-deep-fixture",
                    "symbol": "600000.SH",
                },
                "protection_id": "paper-protection-fixture",
            },
            symbol="600000.SH",
        ),
        _event(
            16,
            "DAY_COMPLETED",
            {
                "cash": "200000",
                "estimated_equity": "200000",
                "estimated_market_value": "0",
                "partial_session": False,
                "positions": [],
            },
            phase="TERMINAL",
        ),
    )
    _write_events(root, events)
    _write_status(root, event_count=len(events), latest="DAY_COMPLETED")

    projection = project_paper_day_sidecars(root)
    rendered = render_paper_day_summary(projection)

    assert projection.llm.enabled is True
    assert projection.llm.required_for_buy is True
    assert projection.llm.preopen_status == "FROZEN"
    assert projection.llm.preopen_dual_track is not None
    assert projection.llm.preopen_dual_track.status == "COMPLETE"
    assert projection.llm.preopen_dual_track.baseline_decision == "PUBLISH"
    assert projection.llm.preopen_dual_track.baseline_macro_impact == Decimal("0.30")
    assert projection.llm.preopen_dual_track.adversarial_decision == "WATCH"
    assert projection.llm.preopen_dual_track.adversarial_macro_impact == Decimal("-0.20")
    assert projection.llm.preopen_dual_track.selected_track == "ADVERSARIAL"
    assert projection.llm.evidence_snapshot_status == "BOUND"
    assert projection.llm.review_candidate_count == 2
    assert projection.llm.reviews_completed == 1
    assert projection.llm.reviews_failed == 1
    assert projection.llm.cache_rejected == 1
    assert projection.llm.cache_restored == 1
    assert projection.llm.gate_blocked == 1
    assert projection.llm.sell_not_applicable == 1
    assert projection.llm.latency_p50_ms == 120
    assert projection.llm.latency_p95_ms == 480
    assert projection.llm.service_state == "HEALTHY"
    assert projection.llm.service_degradation_count == 1
    assert projection.llm.service_recovery_count == 1
    assert len(projection.llm.deep_exit_comparisons) == 1
    assert projection.llm.deep_exit_comparisons[0].baseline_score == Decimal("0.32")
    assert projection.llm.deep_exit_comparisons[0].adversarial_score == Decimal("0.54")
    assert len(projection.llm.deep_exit_sell_reviews) == 1
    assert projection.llm.deep_exit_sell_reviews[0].combined_exit_score == Decimal("-0.70")
    assert projection.llm.deep_exit_sell_reviews[0].llm_can_veto is False
    assert "### 盘中 LLM 复核审计" in rendered
    assert "#### 盘前宏观双轨冻结" in rendered
    assert "形成有证据支持的宏观观点" in rendered
    assert "结构化对抗分析器" in rendered
    assert "继续观察" in rendered
    assert "LLM_COMBINED_SCORE_BELOW_ENTRY_THRESHOLD" in rendered
    assert "卖出旁路 | 不适用 1 次" in rendered
    assert "成交后 DEEP 退出计划双轨评分" in rendered
    assert "原单分析器" in rendered
    assert "对抗分析系统" in rendered
    assert "卖出/REDUCE 的 DEEP 紧迫度复核" in rendered
    assert "技术保护信号优先" in rendered


def test_legacy_preopen_event_reports_dual_track_unavailable_without_fabrication(
    tmp_path: Path,
) -> None:
    root = tmp_path / "legacy-preopen"
    events = (
        _event(1, "DAY_STARTED", {"initial_cash": "200000"}),
        _event(
            2,
            "LLM_INTRADAY_POLICY_CONFIGURED",
            {
                "manifest_binding": {
                    "enabled": True,
                    "required_for_buy": True,
                }
            },
        ),
        _event(
            3,
            "LLM_PREOPEN_CONTEXT_FROZEN",
            {"preopen_context": {"context_id": "legacy-preopen-1"}},
        ),
        _event(
            4,
            "DAY_COMPLETED",
            {
                "cash": "200000",
                "estimated_equity": "200000",
                "estimated_market_value": "0",
                "partial_session": False,
                "positions": [],
            },
            phase="TERMINAL",
        ),
    )
    _write_events(root, events)
    _write_status(root, event_count=len(events), latest="DAY_COMPLETED")

    projection = project_paper_day_sidecars(root)
    rendered = render_paper_day_summary(projection)

    assert projection.llm.preopen_dual_track is not None
    assert projection.llm.preopen_dual_track.status == "LEGACY_NOT_CARRIED"
    assert "历史冻结事件未携带两轨详情" in rendered
    assert "系统没有补造模型结论" in rendered
    assert "LEGACY_NOT_CARRIED" not in rendered


def test_noncontiguous_event_sidecar_fails_without_inspecting_sqlite(tmp_path: Path) -> None:
    root = tmp_path / SESSION.isoformat()
    root.mkdir()
    events = (
        _event(1, "DAY_STARTED", {"cash": "200000"}),
        _event(3, "MARKET_OPENED", {}),
    )
    _write_events(root, events)
    _write_status(root, event_count=2, latest="MARKET_OPENED")
    (root / "journal.sqlite3").write_bytes(b"must-not-be-opened")

    with pytest.raises(PaperDaySidecarError) as captured:
        project_paper_day_sidecars(root)

    assert captured.value.code == "EVENT_LOG_SEQUENCE_INVALID"
    assert (root / "journal.sqlite3").read_bytes() == b"must-not-be-opened"


def test_cli_summary_action_is_sidecar_only_and_returns_artifact_metadata(
    tmp_path: Path,
) -> None:
    root = _complete_session(tmp_path)
    parsed = build_parser().parse_args(
        ["ashare-paper-day", "summary", "--session-date", SESSION.isoformat()]
    )

    result = _ashare_paper_day_summary(root, SESSION)

    assert parsed.action == "summary"
    assert result["ok"] is True
    assert result["sidecar_only"] is True
    assert result["sqlite_opened"] is False
    summary = result["summary"]
    assert isinstance(summary, dict)
    assert summary["lifecycle"] == "COMPLETED"
    assert summary["coverage"] == "FULL_SESSION"
    assert summary["event_count"] == 20
    assert Path(str(summary["path"])).is_file()


def _enhanced_policy_session(tmp_path: Path) -> Path:
    root = tmp_path / f"enhanced-{SESSION.isoformat()}"
    old_policy: dict[str, object] = {
        "initial_equity": "200000",
        "cash_reserve_fraction": "0.20",
        "maximum_gross_fraction": "0.80",
        "maximum_positions": 5,
        "position_count_limit_enabled": True,
        "maximum_symbol_fraction": "0.20",
        "risk_per_trade_fraction": "0.0075",
    }
    new_policy: dict[str, object] = {
        **old_policy,
        "maximum_positions": None,
        "position_count_limit_enabled": False,
        "price_acceptance_policy": {
            "version": "ashare-intraday-price-acceptance@1",
        },
        "order_quantity_policy": {
            "version": "ashare-intraday-order-quantity@1",
            "SSE_MAIN": {
                "minimum_buy_quantity": 100,
                "buy_increment": 100,
                "maximum_limit_order_quantity": 1_000_000,
                "paper_partial_fill_increment": 100,
                "minimum_regular_sell_quantity": 100,
                "sell_increment": 100,
                "sell_residual_policy": "BELOW_100_SELL_ALL_ONCE",
            },
            "SZSE_MAIN": {
                "minimum_buy_quantity": 100,
                "buy_increment": 100,
                "maximum_limit_order_quantity": 1_000_000,
                "paper_partial_fill_increment": 100,
                "minimum_regular_sell_quantity": 100,
                "sell_increment": 100,
                "sell_residual_policy": "BELOW_100_SELL_ALL_ONCE",
            },
            "CHINEXT": {
                "minimum_buy_quantity": 100,
                "buy_increment": 100,
                "maximum_limit_order_quantity": 300_000,
                "paper_partial_fill_increment": 100,
                "minimum_regular_sell_quantity": 100,
                "sell_increment": 100,
                "sell_residual_policy": "BELOW_100_SELL_ALL_ONCE",
            },
            "STAR": {
                "minimum_buy_quantity": 200,
                "buy_increment": 1,
                "maximum_limit_order_quantity": 100_000,
                "paper_partial_fill_increment": 1,
                "minimum_regular_sell_quantity": 200,
                "sell_increment": 1,
                "sell_residual_policy": "BELOW_200_SELL_ALL_ONCE",
            },
            "BSE": "UNSUPPORTED",
        },
    }
    quantity_rule: dict[str, object] = {
        "board": "STAR",
        "minimum_buy_quantity": 200,
        "buy_increment": 1,
        "maximum_limit_order_quantity": 100_000,
        "paper_partial_fill_increment": 1,
        "minimum_regular_sell_quantity": 200,
        "sell_increment": 1,
        "sell_residual_policy": "BELOW_200_SELL_ALL_ONCE",
        "policy_version": "ashare-intraday-order-quantity@1",
    }
    buy_acceptance: dict[str, object] = {
        "acceptable_lower_inclusive": "41.00",
        "acceptable_upper_inclusive": "41.50",
        "board": "STAR",
        "exchange_lower": "32.80",
        "exchange_upper": "49.20",
        "invalidation_boundary_exclusive": "40.99",
        "limit_price": "41.50",
        "policy_version": "ashare-intraday-price-acceptance@1",
        "price_tick": "0.01",
        "reference_price": "41.46",
        "side": "BUY",
        "continuous_auction_price_cage_status": (
            "NOT_L1_VERIFIED_PAPER_MINUTE_BAR_ONLY"
        ),
        "real_broker_submission_allowed": False,
    }
    sell_acceptance: dict[str, object] = {
        "acceptable_lower_inclusive": "40.90",
        "acceptable_upper_inclusive": "49.20",
        "board": "STAR",
        "exchange_lower": "32.80",
        "exchange_upper": "49.20",
        "invalidation_boundary_exclusive": None,
        "limit_price": "40.90",
        "policy_version": "ashare-intraday-price-acceptance@1",
        "price_tick": "0.01",
        "reference_price": "40.94",
        "side": "SELL",
        "continuous_auction_price_cage_status": (
            "NOT_L1_VERIFIED_PAPER_MINUTE_BAR_ONLY"
        ),
        "real_broker_submission_allowed": False,
    }
    events = (
        _event(1, "DAY_STARTED", {"cash": "200000", "initial_cash": "200000"}),
        _event(
            2,
            "OPERATOR_RISK_POLICY_CHANGED",
            {
                "operator_authorized": True,
                "reason_codes": [
                    "REMOVE_POSITION_COUNT_CAP",
                    "ADD_PRICE_ACCEPTANCE_BOUNDS",
                    "ADD_BOARD_QUANTITY_RULES",
                ],
                "old_risk_policy": old_policy,
                "new_risk_policy": new_policy,
                "risk_policy": new_policy,
                "old_risk_policy_sha256": "a" * 64,
                "new_risk_policy_sha256": "b" * 64,
                "validated_fill_count": 5,
                "validated_position_count": 5,
            },
            phase="AFTERNOON",
        ),
        _event(
            3,
            "BUY_SIGNAL_TRIGGERED",
            {
                "risk_approved": True,
                "gate_reason": None,
                "price_acceptance": buy_acceptance,
                "quantity_rule": quantity_rule,
            },
            phase="AFTERNOON",
            symbol="688180.SH",
        ),
        _event(
            4,
            "ORDER_SUBMITTED",
            {
                "order": {
                    "order_id": "order-star-1",
                    "quantity": 201,
                    "symbol": "688180.SH",
                },
                "price_acceptance": buy_acceptance,
                "quantity_rule": quantity_rule,
            },
            phase="AFTERNOON",
            symbol="688180.SH",
            correlation_id="order-star-1",
        ),
        _event(
            5,
            "ORDER_MATCH_EVALUATED",
            {
                "filled_quantity": 0,
                "order_id": "order-star-1",
                "reason": "SIGNAL_INVALIDATED_BEFORE_FILL",
                "status": "NOT_FILLED_IOC",
            },
            phase="AFTERNOON",
            symbol="688180.SH",
            correlation_id="order-star-1",
        ),
        _event(
            6,
            "ORDER_EXPIRED_UNFILLED",
            {
                "order_id": "order-star-1",
                "reason": "SIGNAL_INVALIDATED_BEFORE_FILL",
                "status": "NOT_FILLED_IOC",
            },
            phase="AFTERNOON",
            symbol="688180.SH",
            correlation_id="order-star-1",
        ),
        _event(
            7,
            "BUY_SIGNAL_TRIGGERED",
            {
                "risk_approved": False,
                "gate_reason": "NO_CURRENT_SESSION_ANOMALY",
            },
            phase="AFTERNOON",
            symbol="600000.SH",
        ),
        _event(
            8,
            "BUY_SIGNAL_TRIGGERED",
            {
                "risk_approved": False,
                "gate_reason": "CASH_RESERVE_BINDING",
            },
            phase="AFTERNOON",
            symbol="000001.SZ",
        ),
        _event(
            9,
            "SELL_PRICE_ACCEPTANCE_EVALUATED",
            {
                "price_acceptance": sell_acceptance,
                "price_acceptance_status": "AVAILABLE",
                "quantity_rule": quantity_rule,
                "sell_quantity_plan": {
                    "available_to_sell": 0,
                    "future_limit_order_sequence": [],
                    "status": "NO_SELLABLE_QUANTITY",
                },
                "current_execution_blockers": [
                    "NO_SELL_ORDER_BY_DAY_TEST_POLICY",
                    "T1_SELLABLE_QUANTITY_ZERO",
                ],
                "future_non_execution_conditions": [
                    "NEXT_EXECUTION_PRICE_BELOW_SELL_LIMIT",
                    "LOCKED_LIMIT_DOWN_QUEUE_UNMODELED",
                    "SELL_QUANTITY_NOT_BOARD_VALID",
                ],
            },
            phase="AFTERNOON",
            symbol="688180.SH",
        ),
        _event(
            10,
            "SELL_SIGNAL_TRIGGERED",
            {"available_to_sell": 0},
            phase="AFTERNOON",
            symbol="688180.SH",
        ),
        _event(
            11,
            "SELL_NOT_SUBMITTED_T1",
            {"available_to_sell": 0, "policy": "RECORD_ONLY_NO_SELL_ORDER"},
            phase="AFTERNOON",
            symbol="688180.SH",
        ),
        _event(
            12,
            "DAY_COMPLETED",
            {
                "cash": "200000",
                "estimated_equity": "200000",
                "estimated_market_value": "0",
                "partial_session": False,
                "positions": [],
            },
            phase="TERMINAL",
        ),
    )
    _write_events(root, events)
    _write_status(root, event_count=len(events), latest="DAY_COMPLETED")
    return root


def _complete_session(tmp_path: Path) -> Path:
    root = tmp_path / SESSION.isoformat()
    report_dir = root / "reports"
    report_dir.mkdir(parents=True)
    order_id = "order-1"
    fill_id = "fill-1"
    positions = [
        {
            "available_to_sell": 0,
            "average_cost": "10.051",
            "mark": "10.5",
            "quantity": 100,
            "symbol": "600000.SH",
            "today_buy": 100,
        }
    ]
    account = {
        "cash": "198994.9",
        "estimated_equity": "200044.9",
        "estimated_market_value": "1050",
        "positions": positions,
    }
    events = (
        _event(
            1,
            "DAY_STARTED",
            {
                "cash": "200000",
                "initial_cash": "200000",
                "notification_text": "started",
            },
        ),
        _event(
            2,
            "PREOPEN_SCREEN_COMPLETED",
            {
                "candidate_count": 2,
                "candidates": [
                    {"symbol": "600000.SH", "name": "浦发银行"},
                    {"symbol": "000001.SZ", "name": "平安银行"},
                ],
                "notification_text": "preopen",
            },
            phase="PREOPEN",
        ),
        _event(3, "MARKET_OPENED", {"watchlist_count": 2}, phase="OPEN_AUCTION"),
        _event(
            4,
            "SURVEILLANCE_SCAN_COMPLETED",
            {
                "candidate_count": 2,
                "status": "COMPLETE",
                "universe_count": 5200,
            },
            phase="MORNING",
        ),
        _event(
            5,
            "WATCHLIST_UPDATED",
            {
                "added": ["000002.SZ"],
                "removed": ["000001.SZ"],
                "watchlist": [
                    {"symbol": "600000.SH"},
                    {"symbol": "000002.SZ"},
                ],
            },
            phase="MORNING",
        ),
        _event(
            6,
            "SOURCE_STATE_CHANGED",
            {
                "component": "symbol-minute-bars",
                "error_code": "PARTIAL_MINUTE_DATA_FAILURE",
                "notification_text": "degraded",
                "previous_state": "HEALTHY",
                "state": "DEGRADED",
            },
            phase="MORNING",
        ),
        _event(
            7,
            "TECHNICAL_SIGNAL_EVALUATED",
            {"decision": "ENTER_CANDIDATE", "interval": "1m"},
            phase="MORNING",
            symbol="600000.SH",
        ),
        _event(
            8,
            "BUY_SIGNAL_TRIGGERED",
            {"notification_text": "buy", "risk_approved": True},
            phase="MORNING",
            symbol="600000.SH",
        ),
        _event(
            9,
            "ORDER_SUBMITTED",
            {
                "notification_text": "order",
                "order": {"order_id": order_id, "symbol": "600000.SH"},
            },
            phase="MORNING",
            symbol="600000.SH",
            correlation_id=order_id,
        ),
        _event(
            10,
            "ORDER_MATCH_EVALUATED",
            {
                "filled_quantity": 100,
                "order_id": order_id,
                "status": "FILLED",
            },
            phase="MORNING",
            symbol="600000.SH",
            correlation_id=order_id,
        ),
        _event(
            11,
            "FILL_STARTED",
            {
                "fill": {"fill_id": fill_id, "side": "BUY"},
                "order_id": order_id,
            },
            phase="MORNING",
            symbol="600000.SH",
            correlation_id=fill_id,
        ),
        _event(
            12,
            "FILL_APPLIED",
            {
                "cash": "198994.9",
                "commission": "5",
                "fill_id": fill_id,
                "notification_text": "filled",
                "price": "10",
                "quantity": 100,
                "stamp_tax": "0",
                "transfer_fee": "0.1",
            },
            phase="MORNING",
            symbol="600000.SH",
            correlation_id=fill_id,
        ),
        _event(
            13,
            "TECHNICAL_SIGNAL_EVALUATED",
            {"decision": "REDUCE", "interval": "1m"},
            phase="AFTERNOON",
            symbol="600000.SH",
        ),
        _event(
            14,
            "SELL_SIGNAL_TRIGGERED",
            {"notification_text": "sell"},
            phase="AFTERNOON",
            symbol="600000.SH",
        ),
        _event(
            15,
            "SELL_NOT_SUBMITTED_T1",
            {"policy": "RECORD_ONLY_NO_SELL_ORDER"},
            phase="AFTERNOON",
            symbol="600000.SH",
        ),
        _event(
            16,
            "SOURCE_STATE_CHANGED",
            {
                "component": "symbol-minute-bars",
                "error_code": None,
                "previous_state": "DEGRADED",
                "state": "HEALTHY",
            },
            phase="AFTERNOON",
        ),
        _event(17, "MARKET_CLOSED", account, phase="CLOSING"),
        _event(
            18,
            "DAY_COMPLETED",
            {
                **account,
                "notification_gaps_before_summary": 0,
                "notification_required_before_summary": 7,
                "notification_sent_before_summary": 7,
                "notification_text": "complete",
                "partial_session": False,
            },
            phase="TERMINAL",
        ),
        _event(19, "REPORT_GENERATED", {"report_name": "original.md"}, phase="POST_CLOSE"),
        _event(20, "REPORT_UPLOADED", {"delivered": True}, phase="POST_CLOSE"),
    )
    _write_events(root, events)
    _write_status(root, event_count=len(events), latest="REPORT_UPLOADED")
    original = report_dir / "ashare-paper-day-2026-08-14-original.md"
    original.write_text("# original\n", encoding="utf-8")
    final_result = {
        "account": {
            "cash": "198994.9",
            "positions": [
                {
                    "average_cost": "10.051",
                    "quantity": 100,
                    "realized_pnl": "0",
                    "symbol": "600000.SH",
                }
            ],
        },
        "action": "run",
        "notification_gaps": 0,
        "notification_required": 9,
        "notification_sent": 9,
        "artifact_delivery_complete": True,
        "artifact_delivery_status": "SENT",
        "daily_review_delivery_complete": True,
        "ok": True,
        "run_id": RUN_ID,
        "session_date": SESSION.isoformat(),
        "text_notification_gaps": 0,
        "text_notification_required": 8,
        "text_notification_sent": 8,
    }
    (root / "runner.stdout.log").write_text(
        json.dumps(final_result, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return root


def test_preopen_recovery_supersedes_an_empty_initial_completion(
    tmp_path: Path,
) -> None:
    root = tmp_path / SESSION.isoformat()
    root.mkdir(parents=True)
    events = (
        _event(
            1,
            "PREOPEN_SCREEN_COMPLETED",
            {"candidate_count": 0, "candidates": []},
            phase="PREOPEN",
        ),
        _event(
            2,
            "PREOPEN_SCREEN_RECOVERED",
            {
                "candidate_count": 30,
                "candidates": [{"symbol": "600000.SH"}] * 30,
                "recovery_method": "FULL_MULTIFACTOR_RETRY",
            },
            phase="PREOPEN",
        ),
    )
    _write_events(root, events)
    _write_status(root, event_count=2, latest="PREOPEN_SCREEN_RECOVERED")

    projected = project_paper_day_sidecars(root)

    assert projected.preopen_outcome == "RECOVERED:FULL_MULTIFACTOR_RETRY"
    assert projected.preopen_candidate_count == 30
    assert projected.watchlist_initial_count == 30


def _event(
    sequence: int,
    event_type: str,
    payload: dict[str, object],
    *,
    phase: str = "BOOTSTRAP",
    symbol: str | None = None,
    correlation_id: str | None = None,
) -> dict[str, object]:
    known = datetime(2026, 8, 14, 1, 0, tzinfo=UTC) + timedelta(minutes=sequence)
    return {
        "correlation_id": correlation_id,
        "event_id": f"pde-{sequence:040d}",
        "event_type": event_type,
        "known_at": known.isoformat(),
        "occurred_at": known.isoformat(),
        "payload": payload,
        "phase": phase,
        "sequence": sequence,
        "severity": "INFO",
        "symbol": symbol,
    }


def _write_events(root: Path, events: tuple[dict[str, object], ...]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "session.log.jsonl").write_text(
        "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in events),
        encoding="utf-8",
    )


def _write_status(root: Path, *, event_count: int, latest: str) -> None:
    (root / "status.json").write_text(
        json.dumps(
            {
                "event_count": event_count,
                "latest_event": latest,
                "phase": "TERMINAL",
                "run_id": RUN_ID,
                "session_date": SESSION.isoformat(),
            }
        ),
        encoding="utf-8",
    )
