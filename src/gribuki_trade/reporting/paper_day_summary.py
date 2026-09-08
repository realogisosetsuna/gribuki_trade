"""仅依赖伴随文件的单日 A 股 PAPER 执行摘要投影。

日内运行器拥有三份 SQLite 数据库。部分自带 SQLite 构建无法安全容忍第二个
进程再去打开这些 WAL 文件，因此本模块刻意不导入任何存储适配器，只读取
由运行器写出的近似不可变运维伴随文件：

* ``status.json``：运行身份与存活状态；
* ``session.log.jsonl`` for the ordered event projection;
* an optional final CLI result in ``runner.stdout.log``; and
* the already generated Markdown report, used only as a linked artifact.

The append-only SQLite journal remains authoritative.  This projector does
not claim to verify its hash chain; it preserves event IDs and explicitly
labels every value that the sidecars cannot prove.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_CEILING, Decimal
from pathlib import Path
from typing import cast

from gribuki_trade.reporting.contracts import (
    ReportKind,
    report_contract,
    validate_markdown_report_contract,
)

UNAVAILABLE = "不可得（sidecar 未记录）"


class PaperDaySidecarError(RuntimeError):
    """无法构建旁路日志投影时抛出的稳定错误。"""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        super().__init__(detail)


@dataclass(frozen=True, slots=True)
class PaperDaySidecarEvent:
    sequence: int
    event_id: str
    event_type: str
    known_at: datetime
    occurred_at: datetime
    payload: dict[str, object]
    phase: str
    severity: str
    symbol: str | None
    correlation_id: str | None


@dataclass(frozen=True, slots=True)
class PaperDayWatchlistChange:
    sequence: int
    known_at: datetime
    added: tuple[str, ...]
    removed: tuple[str, ...]
    resulting_count: int | None


@dataclass(frozen=True, slots=True)
class PaperDaySourceTransition:
    sequence: int
    known_at: datetime
    component: str
    previous_state: str | None
    state: str
    error_code: str | None


@dataclass(frozen=True, slots=True)
class PaperDayPositionProjection:
    symbol: str
    quantity: int
    today_buy: int | None
    available_to_sell: int | None
    average_cost: Decimal | None
    mark: Decimal | None
    market_value: Decimal | None
    unrealized_pnl: Decimal | None
    realized_pnl: Decimal | None


@dataclass(frozen=True, slots=True)
class PaperDayNotificationProjection:
    required: int
    sent: int | None
    gaps: int | None
    retried: int | None
    dead: int | None
    exact_final_counts: bool
    required_before_summary: int | None
    sent_before_summary: int | None
    gaps_before_summary: int | None
    artifact_delivery_status: str
    artifact_delivery_complete: bool
    daily_review_delivery_complete: bool
    text_required: int | None
    text_sent: int | None
    text_gaps: int | None
    delivery_projection_exact: bool


@dataclass(frozen=True, slots=True)
class PaperDayLLMTrackComparison:
    """一条盘中复核中两条模型轨道的紧凑可读投影。"""

    symbol: str
    review_id: str
    selected_track: str
    audit_record_sha256: str | None
    baseline_decision: str
    baseline_macro_impact: Decimal | None
    baseline_regime: str
    adversarial_decision: str
    adversarial_macro_impact: Decimal | None
    adversarial_regime: str


@dataclass(frozen=True, slots=True)
class PaperDayPreopenLLMComparison:
    """盘前冻结事件中两条模型轨道的完整摘要。"""

    status: str
    selected_track: str | None
    audit_record_sha256: str | None
    baseline_decision: str | None
    baseline_macro_impact: Decimal | None
    baseline_model: str | None
    adversarial_decision: str | None
    adversarial_macro_impact: Decimal | None
    adversarial_model: str | None


@dataclass(frozen=True, slots=True)
class PaperDayDeepExitLLMComparison:
    """成交后 DEEP 退出计划所保留的双轨语义评分。"""

    sequence: int
    known_at: datetime
    symbol: str
    protection_id: str | None
    plan_id: str | None
    selected_system: str
    selected_score: Decimal | None
    baseline_score: Decimal | None
    adversarial_score: Decimal | None
    status: str


@dataclass(frozen=True, slots=True)
class PaperDayDeepExitSellReview:
    """技术 REDUCE 与持久 DEEP 评分合并后的卖出紧迫度复核。"""

    sequence: int
    known_at: datetime
    symbol: str
    action: str
    technical_score: Decimal | None
    selected_semantic_score: Decimal | None
    combined_exit_score: Decimal | None
    llm_can_veto: bool


@dataclass(frozen=True, slots=True)
class PaperDayLLMProjection:
    """仅从日志旁路文件推导的可审计盘中大模型活动。"""

    enabled: bool
    required_for_buy: bool | None
    preopen_status: str
    preopen_context_id: str | None
    preopen_failure_code: str | None
    preopen_dual_track: PaperDayPreopenLLMComparison | None
    evidence_snapshot_status: str
    evidence_snapshot_sha256: str | None
    evidence_snapshot_failure_code: str | None
    review_batch_count: int
    review_candidate_count: int
    schedule_status_counts: tuple[tuple[str, int], ...]
    reviews_completed: int
    reviews_failed: int
    cache_rejected: int
    cache_restored: int
    gate_evaluations: int
    gate_blocked: int
    gate_action_counts: tuple[tuple[str, int], ...]
    gate_reason_counts: tuple[tuple[str, int], ...]
    sell_not_applicable: int
    latency_p50_ms: int | None
    latency_p95_ms: int | None
    latency_max_ms: int | None
    requested_models: tuple[str, ...]
    response_models: tuple[str, ...]
    prompt_schema_sha256: tuple[str, ...]
    service_state: str
    service_state_transition_count: int
    service_degradation_count: int
    service_recovery_count: int
    dual_track_comparisons: tuple[PaperDayLLMTrackComparison, ...]
    deep_exit_comparisons: tuple[PaperDayDeepExitLLMComparison, ...]
    deep_exit_sell_reviews: tuple[PaperDayDeepExitSellReview, ...]


@dataclass(frozen=True, slots=True)
class PaperDayPriceAcceptanceProjection:
    """旁路事件中保留的一条不可变买卖价格区间。"""

    status: str
    side: str | None
    board: str | None
    acceptable_lower: Decimal | None
    acceptable_upper: Decimal | None
    exchange_lower: Decimal | None
    exchange_upper: Decimal | None
    invalidation_boundary: Decimal | None
    limit_price: Decimal | None
    reference_price: Decimal | None
    price_tick: Decimal | None
    policy_version: str | None
    price_cage_status: str | None
    real_broker_submission_allowed: bool | None


@dataclass(frozen=True, slots=True)
class PaperDayQuantityRuleProjection:
    """运行器保留的板块特定限价单数量约束。"""

    board: str | None
    minimum_buy_quantity: int | None
    buy_increment: int | None
    maximum_limit_order_quantity: int | None
    paper_partial_fill_increment: int | None
    minimum_regular_sell_quantity: int | None
    sell_increment: int | None
    sell_residual_policy: str | None
    policy_version: str | None


@dataclass(frozen=True, slots=True)
class PaperDayBuyExecutionProjection:
    """一笔已提交模拟订单采用的买入区间与数量规则。"""

    sequence: int
    known_at: datetime
    symbol: str
    order_id: str | None
    quantity: int | None
    price_acceptance: PaperDayPriceAcceptanceProjection
    quantity_rule: PaperDayQuantityRuleProjection | None


@dataclass(frozen=True, slots=True)
class PaperDaySellExecutionProjection:
    """仅记录的卖出区间、T+1 阻断项与未来数量计划。"""

    sequence: int
    known_at: datetime
    symbol: str
    price_acceptance: PaperDayPriceAcceptanceProjection
    quantity_rule: PaperDayQuantityRuleProjection | None
    available_to_sell: int | None
    quantity_plan_status: str | None
    future_limit_order_sequence: tuple[int, ...]
    current_blockers: tuple[str, ...]
    future_non_execution_conditions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PaperDayRiskPolicyProjection:
    """可由携带策略的旁路事件证明的最新风险边界。"""

    sequence: int
    known_at: datetime
    source_event_type: str
    maximum_positions: int | None
    position_count_limit_enabled: bool | None
    cash_reserve_fraction: Decimal | None
    maximum_gross_fraction: Decimal | None
    maximum_symbol_fraction: Decimal | None
    risk_per_trade_fraction: Decimal | None
    price_acceptance_policy_version: str | None
    order_quantity_policy_version: str | None
    order_quantity_rules: tuple[PaperDayQuantityRuleProjection, ...]
    unsupported_quantity_boards: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PaperDayRiskPolicyChangeProjection:
    """一次经操作员授权、仅追加的盘中风险策略迁移。"""

    sequence: int
    known_at: datetime
    operator_authorized: bool | None
    reason_codes: tuple[str, ...]
    old_policy_sha256: str | None
    new_policy_sha256: str | None
    old_maximum_positions: int | None
    new_maximum_positions: int | None
    new_position_count_limit_enabled: bool | None
    validated_fill_count: int | None
    validated_position_count: int | None


@dataclass(frozen=True, slots=True)
class PaperDayStableReasonProjection:
    """一条已观测的稳定拒绝或阻断代码及其中文解释。"""

    category: str
    code: str
    count: int
    explanation: str


@dataclass(frozen=True, slots=True)
class PaperDayExecutiveProjection:
    """无需打开 SQLite 即可计算的强类型确定性摘要。"""

    session_root: Path
    status_path: Path
    event_log_path: Path
    stdout_path: Path | None
    original_report_path: Path | None
    session_date: date
    run_id: str
    lifecycle: str
    coverage: str
    first_known_at: datetime
    last_known_at: datetime
    market_opened_at: datetime | None
    market_closed_at: datetime | None
    status_event_count: int | None
    sidecar_event_count: int
    phases_seen: tuple[str, ...]
    preopen_outcome: str
    preopen_candidate_count: int | None
    watchlist_initial_count: int | None
    watchlist_current_count: int | None
    watchlist_peak_count: int | None
    watchlist_update_count: int
    watchlist_added_total: int
    watchlist_removed_total: int
    watchlist_changes: tuple[PaperDayWatchlistChange, ...]
    scan_count: int
    scan_status_counts: tuple[tuple[str, int], ...]
    scan_universe_min: int | None
    scan_universe_max: int | None
    scan_universe_last: int | None
    monitor_evaluations: int
    monitor_invalid: int
    monitored_symbols: int
    monitor_interval_counts: tuple[tuple[str, int], ...]
    technical_decision_counts: tuple[tuple[str, int], ...]
    buy_signal_count: int
    buy_approved_count: int
    buy_rejected_count: int
    buy_reject_reasons: tuple[tuple[str, int], ...]
    risk_policy_changes: tuple[PaperDayRiskPolicyChangeProjection, ...]
    current_risk_policy: PaperDayRiskPolicyProjection | None
    buy_execution_acceptances: tuple[PaperDayBuyExecutionProjection, ...]
    sell_execution_acceptances: tuple[PaperDaySellExecutionProjection, ...]
    stable_rejection_reasons: tuple[PaperDayStableReasonProjection, ...]
    sell_signal_count: int
    sell_not_submitted_count: int
    orders_submitted: int
    order_matches: int
    orders_filled: int
    orders_partially_filled: int
    orders_expired: int
    expiry_reason_counts: tuple[tuple[str, int], ...]
    fills_applied: int
    filled_shares: int
    fill_notional: Decimal | None
    commission: Decimal | None
    transfer_fee: Decimal | None
    stamp_tax: Decimal | None
    starting_cash: Decimal | None
    final_cash: Decimal | None
    final_equity: Decimal | None
    gross_exposure: Decimal | None
    realized_pnl: Decimal | None
    unrealized_pnl: Decimal | None
    total_mark_to_market_pnl: Decimal | None
    positions: tuple[PaperDayPositionProjection, ...]
    notifications: PaperDayNotificationProjection
    llm: PaperDayLLMProjection
    source_transitions: tuple[PaperDaySourceTransition, ...]
    source_degradation_count: int
    source_recovery_count: int
    events: tuple[PaperDaySidecarEvent, ...]
    warnings: tuple[str, ...]


def project_paper_day_sidecars(session_root: Path) -> PaperDayExecutiveProjection:
    """读取一个足够一致的旁路快照并计算其投影。

    运行器工作期间调用也安全，因为不会打开任何 SQLite 路径。并发追加
    可能造成一个事件的计数偏差，此偏差会明确显示为警告而不会被隐藏。
    """

    root = session_root.resolve()
    status_path = root / "status.json"
    event_log_path = root / "session.log.jsonl"
    status = _read_json_object(status_path, "STATUS_FILE_INVALID")
    events, load_warnings = _read_events(event_log_path)
    if not events:
        raise PaperDaySidecarError(
            "EVENT_LOG_EMPTY",
            "session.log.jsonl has no complete PAPER-day event",
        )

    run_id = _required_string(status, "run_id", "STATUS_FILE_INVALID")
    session_date = _required_date(status, "session_date", "STATUS_FILE_INVALID")
    status_count = _optional_int(status.get("event_count"))
    warnings = list(load_warnings)
    sequences = tuple(event.sequence for event in events)
    expected = tuple(range(1, len(events) + 1))
    if sequences != expected:
        raise PaperDaySidecarError(
            "EVENT_LOG_SEQUENCE_INVALID",
            "session.log.jsonl sequences are not contiguous from one",
        )
    if status_count is not None and status_count != len(events):
        warnings.append(
            "status.json 的 event_count 与 sidecar 事件行数不一致；若进程仍在运行，"
            "这可能是读取恰逢追加窗口。"
        )

    original_report = _latest_original_report(root, session_date)
    stdout_path = root / "runner.stdout.log"
    final_result = _read_final_result(root, session_date, run_id)

    terminal = _last_event(events, {"DAY_COMPLETED", "DAY_COMPLETED_WITH_NOTIFICATION_GAPS"})
    aborted = _last_event(events, {"DAY_ABORTED"})
    abort_recovery = _last_event(events, {"RUNNER_RECOVERY_AFTER_ABORT"})
    if terminal is not None:
        lifecycle = "COMPLETED"
    elif aborted is not None and (
        abort_recovery is None or abort_recovery.sequence < aborted.sequence
    ):
        lifecycle = "ABORTED"
    else:
        lifecycle = "IN_PROGRESS"
    partial_value = (
        None
        if terminal is None
        else _optional_bool(terminal.payload.get("partial_session"))
    )
    if lifecycle == "COMPLETED" and partial_value is False:
        coverage = "FULL_SESSION"
    elif (
        lifecycle == "COMPLETED" and partial_value is True
    ) or lifecycle == "ABORTED" or _has_event(events, "PREOPEN_SCREEN_MISSED"):
        coverage = "PARTIAL_SESSION"
    else:
        coverage = "PENDING_FINAL_CLASSIFICATION"

    preopen = _last_event(
        events,
        {"PREOPEN_SCREEN_COMPLETED", "PREOPEN_SCREEN_RECOVERED"},
    )
    if preopen is not None and preopen.event_type == "PREOPEN_SCREEN_RECOVERED":
        method = _optional_string(preopen.payload.get("recovery_method"))
        preopen_outcome = "RECOVERED" if method is None else f"RECOVERED:{method}"
    elif preopen is not None:
        preopen_outcome = "COMPLETED"
    elif _has_event(events, "PREOPEN_SCREEN_FAILED"):
        preopen_outcome = "FAILED"
    elif _has_event(events, "PREOPEN_SCREEN_MISSED"):
        preopen_outcome = "MISSED"
    elif _has_event(events, "PREOPEN_SCREEN_STARTED"):
        preopen_outcome = "IN_PROGRESS"
    else:
        preopen_outcome = "NOT_STARTED"
    preopen_count = None
    if preopen is not None:
        preopen_count = _optional_int(preopen.payload.get("candidate_count"))
        if preopen_count is None:
            preopen_count = _list_length(preopen.payload.get("candidates"))

    changes = _watchlist_changes(events)
    initial_watchlist_count = preopen_count
    current_watchlist_count = initial_watchlist_count
    peak_watchlist_count = initial_watchlist_count
    for change in changes:
        if change.resulting_count is not None:
            current_watchlist_count = change.resulting_count
            peak_watchlist_count = (
                change.resulting_count
                if peak_watchlist_count is None
                else max(peak_watchlist_count, change.resulting_count)
            )

    scans = tuple(event for event in events if event.event_type == "SURVEILLANCE_SCAN_COMPLETED")
    scan_statuses = Counter(
        _optional_string(event.payload.get("status")) or "UNKNOWN" for event in scans
    )
    universe_counts = tuple(
        count
        for event in scans
        if (count := _optional_int(event.payload.get("universe_count"))) is not None
    )

    technical = tuple(event for event in events if event.event_type == "TECHNICAL_SIGNAL_EVALUATED")
    technical_decisions = Counter(
        _optional_string(event.payload.get("decision")) or "UNKNOWN"
        for event in technical
    )
    monitor_intervals = Counter(
        _optional_string(event.payload.get("interval")) or "UNKNOWN"
        for event in technical
    )
    monitored_symbols = len({event.symbol for event in technical if event.symbol is not None})

    buy_signals = tuple(event for event in events if event.event_type == "BUY_SIGNAL_TRIGGERED")
    approved = tuple(
        event for event in buy_signals if event.payload.get("risk_approved") is True
    )
    rejected = tuple(
        event for event in buy_signals if event.payload.get("risk_approved") is not True
    )
    reject_reasons = Counter(
        _optional_string(event.payload.get("gate_reason")) or "UNSPECIFIED_REJECTION"
        for event in rejected
    )

    submitted_ids = {
        order_id
        for event in events
        if event.event_type == "ORDER_SUBMITTED"
        if (order_id := _order_id(event)) is not None
    }
    match_events = tuple(event for event in events if event.event_type == "ORDER_MATCH_EVALUATED")
    filled_order_ids = {
        order_id
        for event in match_events
        if (_optional_int(event.payload.get("filled_quantity")) or 0) > 0
        if (order_id := _order_id(event)) is not None
    }
    for event in events:
        if event.event_type != "FILL_STARTED":
            continue
        order_id = _optional_string(event.payload.get("order_id"))
        if order_id is not None:
            filled_order_ids.add(order_id)
    partially_filled = {
        order_id
        for event in match_events
        if _optional_string(event.payload.get("status")) == "PARTIALLY_FILLED_IOC"
        if (order_id := _order_id(event)) is not None
    }
    expiry_types = {
        "ORDER_EXPIRED_UNFILLED",
        "ORDER_EXPIRED_AT_RECESS",
        "ORDER_EXPIRED_AT_CLOSE",
    }
    expiry_events = tuple(event for event in events if event.event_type in expiry_types)
    expired_ids = {
        order_id
        for event in expiry_events
        if (order_id := _order_id(event)) is not None
    }
    expiry_reasons = Counter(
        _optional_string(event.payload.get("reason")) or event.event_type
        for event in expiry_events
    )
    risk_policy_changes = _risk_policy_changes(events)
    current_risk_policy = _current_risk_policy(events)
    buy_execution_acceptances = _buy_execution_acceptances(events)
    sell_execution_acceptances = _sell_execution_acceptances(events)
    stable_rejection_reasons = _stable_rejection_reasons(
        buy_reject_reasons=reject_reasons,
        expiry_reasons=expiry_reasons,
        sell_acceptances=sell_execution_acceptances,
    )

    applied_fills = _unique_applied_fills(events)
    fill_values_complete = True
    fill_notional = Decimal("0")
    commission = Decimal("0")
    transfer_fee = Decimal("0")
    stamp_tax = Decimal("0")
    filled_shares = 0
    for event in applied_fills:
        quantity = _optional_int(event.payload.get("quantity"))
        price = _optional_decimal(event.payload.get("price"))
        commission_value = _optional_decimal(event.payload.get("commission"))
        transfer_value = _optional_decimal(event.payload.get("transfer_fee"))
        stamp_value = _optional_decimal(event.payload.get("stamp_tax"))
        if quantity is not None:
            filled_shares += quantity
        if (
            quantity is None
            or price is None
            or commission_value is None
            or transfer_value is None
            or stamp_value is None
        ):
            fill_values_complete = False
            continue
        fill_notional += price * quantity
        commission += commission_value
        transfer_fee += transfer_value
        stamp_tax += stamp_value

    day_started = _last_event(events, {"DAY_STARTED"})
    starting_cash = None
    if day_started is not None:
        starting_cash = _optional_decimal(day_started.payload.get("initial_cash"))
        if starting_cash is None:
            starting_cash = _optional_decimal(day_started.payload.get("cash"))

    account_event = _latest_account_event(events)
    account_payload = {} if account_event is None else account_event.payload
    final_cash = _optional_decimal(account_payload.get("cash"))
    gross_exposure = _optional_decimal(account_payload.get("estimated_market_value"))
    final_equity = _optional_decimal(account_payload.get("estimated_equity"))
    final_account = _optional_object(final_result.get("account")) if final_result else None
    if final_cash is None and final_account is not None:
        final_cash = _optional_decimal(final_account.get("cash"))

    final_realized_by_symbol = _realized_by_symbol(final_account)
    positions = _positions(account_payload.get("positions"), final_realized_by_symbol)
    if gross_exposure is None and all(position.market_value is not None for position in positions):
        gross_exposure = sum(
            (cast(Decimal, position.market_value) for position in positions),
            Decimal("0"),
        )
    if final_equity is None and final_cash is not None and gross_exposure is not None:
        final_equity = final_cash + gross_exposure
    if all(position.unrealized_pnl is not None for position in positions):
        unrealized_pnl: Decimal | None = sum(
            (cast(Decimal, position.unrealized_pnl) for position in positions),
            Decimal("0"),
        )
    else:
        unrealized_pnl = None

    fill_sides = _applied_fill_sides(events, applied_fills)
    if len(fill_sides) == len(applied_fills) and all(side == "BUY" for side in fill_sides):
        realized_pnl: Decimal | None = Decimal("0")
    elif final_realized_by_symbol is not None:
        realized_pnl = sum(final_realized_by_symbol.values(), Decimal("0"))
    else:
        realized_pnl = None
    total_pnl = (
        final_equity - starting_cash
        if final_equity is not None and starting_cash is not None
        else None
    )

    notifications = _notification_projection(events, terminal, final_result, status)
    llm = _llm_projection(events)
    transitions = _source_transitions(events)
    nonhealthy = tuple(item for item in transitions if item.state != "HEALTHY")
    recoveries = _source_recovery_count(transitions)

    if lifecycle == "COMPLETED" and final_result is None:
        warnings.append(
            "未找到最终 CLI 结果 sidecar；最终 QQ 已发送数与最终缺口不能由事件 sidecar 精确证明。"
        )
    if lifecycle != "COMPLETED":
        warnings.append("会话尚未完成；资金、持仓、通知和覆盖统计均为当前时点快照。")
    if any(position.mark is None for position in positions):
        warnings.append("至少一个持仓缺少收盘标记价，未实现盈亏或总权益可能不可得。")

    return PaperDayExecutiveProjection(
        session_root=root,
        status_path=status_path,
        event_log_path=event_log_path,
        stdout_path=stdout_path if stdout_path.is_file() else None,
        original_report_path=original_report,
        session_date=session_date,
        run_id=run_id,
        lifecycle=lifecycle,
        coverage=coverage,
        first_known_at=events[0].known_at,
        last_known_at=events[-1].known_at,
        market_opened_at=_event_known_at(events, "MARKET_OPENED"),
        market_closed_at=_event_known_at(events, "MARKET_CLOSED"),
        status_event_count=status_count,
        sidecar_event_count=len(events),
        phases_seen=tuple(dict.fromkeys(event.phase for event in events)),
        preopen_outcome=preopen_outcome,
        preopen_candidate_count=preopen_count,
        watchlist_initial_count=initial_watchlist_count,
        watchlist_current_count=current_watchlist_count,
        watchlist_peak_count=peak_watchlist_count,
        watchlist_update_count=len(changes),
        watchlist_added_total=sum(len(change.added) for change in changes),
        watchlist_removed_total=sum(len(change.removed) for change in changes),
        watchlist_changes=changes,
        scan_count=len(scans),
        scan_status_counts=_counter_items(scan_statuses),
        scan_universe_min=min(universe_counts) if universe_counts else None,
        scan_universe_max=max(universe_counts) if universe_counts else None,
        scan_universe_last=universe_counts[-1] if universe_counts else None,
        monitor_evaluations=len(technical),
        monitor_invalid=sum(
            event.event_type == "TECHNICAL_SIGNAL_INVALID" for event in events
        ),
        monitored_symbols=monitored_symbols,
        monitor_interval_counts=_counter_items(monitor_intervals),
        technical_decision_counts=_counter_items(technical_decisions),
        buy_signal_count=len(buy_signals),
        buy_approved_count=len(approved),
        buy_rejected_count=len(rejected),
        buy_reject_reasons=_counter_items(reject_reasons),
        risk_policy_changes=risk_policy_changes,
        current_risk_policy=current_risk_policy,
        buy_execution_acceptances=buy_execution_acceptances,
        sell_execution_acceptances=sell_execution_acceptances,
        stable_rejection_reasons=stable_rejection_reasons,
        sell_signal_count=sum(event.event_type == "SELL_SIGNAL_TRIGGERED" for event in events),
        sell_not_submitted_count=sum(
            event.event_type == "SELL_NOT_SUBMITTED_T1" for event in events
        ),
        orders_submitted=len(submitted_ids),
        order_matches=len(match_events),
        orders_filled=len(filled_order_ids),
        orders_partially_filled=len(partially_filled),
        orders_expired=len(expired_ids),
        expiry_reason_counts=_counter_items(expiry_reasons),
        fills_applied=len(applied_fills),
        filled_shares=filled_shares,
        fill_notional=fill_notional if fill_values_complete else None,
        commission=commission if fill_values_complete else None,
        transfer_fee=transfer_fee if fill_values_complete else None,
        stamp_tax=stamp_tax if fill_values_complete else None,
        starting_cash=starting_cash,
        final_cash=final_cash,
        final_equity=final_equity,
        gross_exposure=gross_exposure,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        total_mark_to_market_pnl=total_pnl,
        positions=positions,
        notifications=notifications,
        llm=llm,
        source_transitions=transitions,
        source_degradation_count=len(nonhealthy),
        source_recovery_count=recoveries,
        events=events,
        warnings=tuple(dict.fromkeys(warnings)),
    )


def render_paper_day_summary(projection: PaperDayExecutiveProjection) -> str:
    """渲染增强版 Markdown 报告，并将漏斗置于时间线之前。"""

    p = projection
    rows = [
        "# A股｜模拟盘全日运行报告",
        "",
        f"> 报告类型：{report_contract(ReportKind.DAILY_REVIEW).chinese_name}",
        "> 口径：sidecar 可读投影；SQLite 哈希链仍是权威交易记录。",
        "",
        "## 执行摘要",
        "",
        "| 项目 | 结果 |",
        "|---|---|",
        f"| 交易日 | {p.session_date.isoformat()} |",
        f"| Run ID | `{p.run_id}` |",
        f"| 生命周期 | {_readable_code(p.lifecycle)} |",
        f"| 会话覆盖 | {_readable_code(p.coverage)} |",
        f"| sidecar 事件 | {p.sidecar_event_count} |",
        f"| 已覆盖阶段 | {'、'.join(_readable_code(item) for item in p.phases_seen)} |",
        f"| 首个/末个可知时间 | {_local_time(p.first_known_at, with_date=True)} / "
        f"{_local_time(p.last_known_at, with_date=True)} |",
        f"| 市场开/收盘事件 | {_time_or_unavailable(p.market_opened_at)} / "
        f"{_time_or_unavailable(p.market_closed_at)} |",
        "",
        "## 市场复盘",
        "",
        "### 选股与监控漏斗",
        "",
        "| 漏斗层级 | 数量/状态 | 口径 |",
        "|---|---:|---|",
        f"| 盘前筛选 | {_optional_number(p.preopen_candidate_count)} | "
        f"状态 {_readable_code(p.preopen_outcome)} |",
        f"| 全市场盘中扫描 | {p.scan_count} | "
        f"状态分布：{_pairs(p.scan_status_counts)} |",
        f"| 扫描股票池 | {_universe_range(p)} | universe_count 的最小/最大/末次 |",
        f"| 关注名单 | {_watchlist_counts(p)} | 初始/峰值/最终 |",
        f"| 名单更新 | {p.watchlist_update_count} | "
        f"累计新增 {p.watchlist_added_total}、移除 {p.watchlist_removed_total} |",
        f"| 技术监控评估 | {p.monitor_evaluations} | "
        f"覆盖 {p.monitored_symbols} 个标的；周期：{_pairs(p.monitor_interval_counts)} |",
        f"| 无效技术样本 | {p.monitor_invalid} | 不计入有效决策 |",
        "",
        "## 操作复盘",
        "",
        "### 信号、风控与订单",
        "",
        "| 指标 | 数量 | 明细 |",
        "|---|---:|---|",
        f"| 技术决策 | {p.monitor_evaluations} | {_pairs(p.technical_decision_counts)} |",
        f"| 买入信号 | {p.buy_signal_count} | "
        f"风控通过 {p.buy_approved_count}；拒绝 {p.buy_rejected_count} |",
        f"| 风控/入场拒绝 | {p.buy_rejected_count} | {_pairs(p.buy_reject_reasons)} |",
        f"| 卖出信号 | {p.sell_signal_count} | "
        f"按 T+1 测试约定未提交 {p.sell_not_submitted_count} |",
        f"| PAPER 委托 | {p.orders_submitted} | 撮合评估 {p.order_matches} |",
        f"| 成交订单 | {p.orders_filled} | 其中部分成交 IOC {p.orders_partially_filled} |",
        f"| 到期/撤销未成交 | {p.orders_expired} | {_pairs(p.expiry_reason_counts)} |",
        "",
        *_llm_audit_lines(p),
        *_risk_policy_audit_lines(p),
        *_buy_execution_audit_lines(p),
        *_sell_execution_audit_lines(p),
        *_stable_rejection_lines(p),
        "### 成交、资金与账户",
        "",
        "| 项目 | 数值 |",
        "|---|---:|",
        f"| 初始现金 | {_money(p.starting_cash)} |",
        f"| 最终现金 | {_money(p.final_cash)} |",
        f"| 持仓市值/总敞口 | {_money(p.gross_exposure)} |",
        f"| 最终估算权益 | {_money(p.final_equity)} |",
        f"| 总盯市盈亏 | {_money(p.total_mark_to_market_pnl)} |",
        f"| 已实现盈亏 | {_money(p.realized_pnl)} |",
        f"| 未实现盈亏 | {_money(p.unrealized_pnl)} |",
        f"| 已应用成交 | {p.fills_applied} 笔 / {p.filled_shares} 股 |",
        f"| 成交名义金额 | {_money(p.fill_notional)} |",
        f"| 佣金 | {_money(p.commission)} |",
        f"| 过户费 | {_money(p.transfer_fee)} |",
        f"| 印花税 | {_money(p.stamp_tax)} |",
        f"| 费用合计 | {_money(_fee_total(p))} |",
        "",
        "## 持仓深研",
        "",
        "当前报告展示持仓、成本和当日可知保护输入；逐标的技术、宏观与对抗复核"
        "由盘后深研报告承载，不以本投影补写缺失结论。",
        "",
        "### 持仓明细与保护输入",
        "",
        "| 标的 | 数量 | 今日买入 | 可卖 | 均价 | 标记价 | 市值 | 已实现 | 未实现 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    if not p.positions:
        rows.append("| 无持仓 | 0 | — | — | — | — | 0.00 | 0.00 | 0.00 |")
    else:
        for position in p.positions:
            rows.append(
                f"| {position.symbol} | {position.quantity} | "
                f"{_optional_number(position.today_buy)} | "
                f"{_optional_number(position.available_to_sell)} | "
                f"{_price(position.average_cost)} | {_price(position.mark)} | "
                f"{_money(position.market_value)} | {_money(position.realized_pnl)} | "
                f"{_money(position.unrealized_pnl)} |"
            )

    held_symbols = "、".join(item.symbol for item in p.positions) or "无"
    rows.extend(
        (
            "",
            "## 次日基线",
            "",
            f"- 收盘留存持仓：{held_symbols}。",
            "- 下一交易日开盘前必须重新核验交易日历、停复牌、价格带、最新新闻与"
            "退出计划版本；本报告不把当日结论自动延续为次日订单。",
            "- 盘后逐标的深研及双轨 LLM 结论以独立盘后报告为准；缺失时明确视为"
            "尚未形成次日研究授权。",
        )
    )

    notification = p.notifications
    notification_basis = (
        "最终 CLI 结果"
        if notification.exact_final_counts
        else "最终精确值不可由 sidecar 证明"
    )
    before_summary_counts = _three_counts(
        notification.required_before_summary,
        notification.sent_before_summary,
        notification.gaps_before_summary,
    )
    final_text_counts = _three_counts(
        notification.text_required,
        notification.text_sent,
        notification.text_gaps,
    )
    delivery_basis = (
        "精确 sidecar/CLI 投影"
        if notification.delivery_projection_exact
        else "无法由旧 sidecar 精确证明"
    )
    artifact_outcome = (
        "已发送" if notification.artifact_delivery_complete else "未完成或回执不确定"
    )
    rows.extend(
        (
            "",
            "## 五、消息投递",
            "",
            "| 指标 | 数值 | 证据口径 |",
            "|---|---:|---|",
            f"| 必推事件 | {notification.required} | "
            "event payload 含 notification_text |",
            f"| 已发送 | {_optional_number(notification.sent)} | "
            f"{notification_basis} |",
            f"| 最终缺口 | {_optional_number(notification.gaps)} | "
            f"{notification_basis} |",
            f"| 重试 | {_optional_number(notification.retried)} | "
            "outbox 重试状态未投影到 sidecar |",
            f"| DEAD | {_optional_number(notification.dead)} | outbox DEAD 状态未投影到 sidecar |",
            f"| 完成摘要前（必推/已发/缺口） | {before_summary_counts} | "
            "DAY_COMPLETED payload；不含该摘要自身 |",
            f"| 文本最终（必推/已发/缺口） | {final_text_counts} | {delivery_basis} |",
            f"| Markdown 附件状态 | {notification.artifact_delivery_status} | "
            f"{artifact_outcome} |",
            f"| 日报双交付完成 | "
            f"{'是' if notification.daily_review_delivery_complete else '否'} | "
            "仅文本无缺口且 Markdown 明确 SENT 时为是 |",
            "",
            "## 六、数据源状态",
            "",
            f"- 非健康状态转换：{p.source_degradation_count} 次；恢复为 HEALTHY："
            f"{p.source_recovery_count} 次。",
            "",
            "| 时间 | 组件 | 状态变化 | 错误码 |",
            "|---|---|---|---|",
        )
    )
    if not p.source_transitions:
        rows.append("| — | — | 未记录状态转换 | — |")
    else:
        for transition in p.source_transitions:
            rows.append(
                f"| {_local_time(transition.known_at)} | {transition.component} | "
                f"{_readable_code(transition.previous_state or 'UNKNOWN')} → "
                f"{_readable_code(transition.state)} | "
                f"{_readable_code(transition.error_code) if transition.error_code else '—'} |"
            )

    rows.extend(("", "## 七、关注名单变更", ""))
    if not p.watchlist_changes:
        rows.append("本次 sidecar 未记录关注名单增删事件。")
    else:
        rows.extend(
            (
                "| 序号/时间 | 新增 | 移除 | 更新后数量 |",
                "|---|---|---|---:|",
            )
        )
        for change in p.watchlist_changes:
            rows.append(
                f"| {change.sequence} / {_local_time(change.known_at)} | "
                f"{', '.join(change.added) or '—'} | "
                f"{', '.join(change.removed) or '—'} | "
                f"{_optional_number(change.resulting_count)} |"
            )

    rows.extend(("", "## 八、文件与可审计边界", ""))
    rows.extend(_artifact_lines(p))
    rows.extend(
        (
            "",
            "- 本报告只读取 sidecar，从未打开 `journal.sqlite3`、`outbox.sqlite3` 或 "
            "`ledger.sqlite3`。",
            "- SQLite 中的哈希链事件日志仍是权威记录；本报告是便于阅读的投影，不是哈希链验签结果。",
            "- 已实现/未实现盈亏仅在 sidecar 提供完整持仓成本、标记价或最终账户结果时计算；"
            "缺失时明确显示不可得。",
        )
    )
    if p.warnings:
        rows.extend(("", "### 投影警告", ""))
        rows.extend(f"- {warning}" for warning in p.warnings)

    rows.extend(("", "## 九、不可变事件时间线（sidecar 投影）", ""))
    rows.append(
        "以下顺序与 `session.log.jsonl` 一致并保留事件 ID；完整哈希链需由权威 "
        "journal 在进程退出后另行校验。"
    )
    rows.append("")
    for event in p.events:
        symbol = "" if event.symbol is None else f" [{event.symbol}]"
        rows.extend(
            (
                f"### {event.sequence}. {_local_time(event.known_at)} "
                f"{event.event_type}{symbol}",
                "",
                f"阶段 `{event.phase}`；级别 `{event.severity}`；事件 `{event.event_id}`。",
                "",
                "```json",
                json.dumps(event.payload, ensure_ascii=False, sort_keys=True),
                "```",
                "",
            )
        )
    rendered = "\n".join(rows).rstrip() + "\n"
    validate_markdown_report_contract(ReportKind.DAILY_REVIEW, rendered)
    return rendered


def write_paper_day_summary(projection: PaperDayExecutiveProjection) -> Path:
    """原子创建或重新生成确定性的增强报告。"""

    report_dir = projection.session_root / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    target = report_dir / (
        f"ashare-paper-day-summary-{projection.session_date.isoformat()}-"
        f"{projection.run_id[-10:]}.md"
    )
    rendered = render_paper_day_summary(projection)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=report_dir,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return target.resolve()


def _read_json_object(path: Path, code: str) -> dict[str, object]:
    """兼容旧私有名称；实际 JSON 解码位于独立 codec 模块。"""

    from .paper_day_codec import read_json_object

    return read_json_object(path, code)


def _read_events(path: Path) -> tuple[tuple[PaperDaySidecarEvent, ...], tuple[str, ...]]:
    """兼容旧私有名称；实际事件解码位于独立 codec 模块。"""

    from .paper_day_codec import read_events

    return read_events(path)


def _required_string(value: Mapping[str, object], key: str, code: str) -> str:
    from .paper_day_codec import required_string

    return required_string(value, key, code)


def _required_date(value: Mapping[str, object], key: str, code: str) -> date:
    from .paper_day_codec import required_date

    return required_date(value, key, code)


def _required_datetime(value: Mapping[str, object], key: str, line: int) -> datetime:
    from .paper_day_codec import required_datetime

    return required_datetime(value, key, line)


def _parse_event(value: dict[str, object], line: int) -> PaperDaySidecarEvent:
    from .paper_day_codec import parse_event

    return parse_event(value, line)


def _optional_string(value: object) -> str | None:
    from .paper_day_codec import optional_string

    return optional_string(value)


def _optional_int(value: object) -> int | None:
    from .paper_day_codec import optional_int

    return optional_int(value)


def _optional_bool(value: object) -> bool | None:
    from .paper_day_codec import optional_bool

    return optional_bool(value)


def _optional_decimal(value: object) -> Decimal | None:
    from .paper_day_codec import optional_decimal

    return optional_decimal(value)


def _optional_object(value: object) -> dict[str, object] | None:
    from .paper_day_codec import optional_object

    return optional_object(value)


def _list_length(value: object) -> int | None:
    from .paper_day_codec import list_length

    return list_length(value)


def _has_event(events: Iterable[PaperDaySidecarEvent], event_type: str) -> bool:
    return any(event.event_type == event_type for event in events)


def _last_event(
    events: Iterable[PaperDaySidecarEvent], event_types: set[str]
) -> PaperDaySidecarEvent | None:
    retained = [event for event in events if event.event_type in event_types]
    return retained[-1] if retained else None


def _event_known_at(
    events: Iterable[PaperDaySidecarEvent], event_type: str
) -> datetime | None:
    event = _last_event(events, {event_type})
    return None if event is None else event.known_at


def _watchlist_changes(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDayWatchlistChange, ...]:
    result = []
    for event in events:
        if event.event_type != "WATCHLIST_UPDATED":
            continue
        added = _string_tuple(event.payload.get("added"))
        removed = _string_tuple(event.payload.get("removed"))
        resulting_count = _list_length(event.payload.get("watchlist"))
        result.append(
            PaperDayWatchlistChange(
                sequence=event.sequence,
                known_at=event.known_at,
                added=added,
                removed=removed,
                resulting_count=resulting_count,
            )
        )
    return tuple(result)


def _string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _counter_items(counter: Counter[str]) -> tuple[tuple[str, int], ...]:
    return tuple(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def _price_acceptance(
    value: object,
    *,
    status: str,
) -> PaperDayPriceAcceptanceProjection:
    document = _optional_object(value) or {}
    return PaperDayPriceAcceptanceProjection(
        status=status,
        side=_optional_string(document.get("side")),
        board=_optional_string(document.get("board")),
        acceptable_lower=_optional_decimal(
            document.get("acceptable_lower_inclusive")
        ),
        acceptable_upper=_optional_decimal(
            document.get("acceptable_upper_inclusive")
        ),
        exchange_lower=_optional_decimal(document.get("exchange_lower")),
        exchange_upper=_optional_decimal(document.get("exchange_upper")),
        invalidation_boundary=_optional_decimal(
            document.get("invalidation_boundary_exclusive")
        ),
        limit_price=_optional_decimal(document.get("limit_price")),
        reference_price=_optional_decimal(document.get("reference_price")),
        price_tick=_optional_decimal(document.get("price_tick")),
        policy_version=_optional_string(document.get("policy_version")),
        price_cage_status=_optional_string(
            document.get("continuous_auction_price_cage_status")
        ),
        real_broker_submission_allowed=_optional_bool(
            document.get("real_broker_submission_allowed")
        ),
    )


def _quantity_rule(
    value: object,
    *,
    board_override: str | None = None,
) -> PaperDayQuantityRuleProjection | None:
    document = _optional_object(value)
    if document is None:
        return None
    return PaperDayQuantityRuleProjection(
        board=board_override or _optional_string(document.get("board")),
        minimum_buy_quantity=_optional_int(document.get("minimum_buy_quantity")),
        buy_increment=_optional_int(document.get("buy_increment")),
        maximum_limit_order_quantity=_optional_int(
            document.get("maximum_limit_order_quantity")
        ),
        paper_partial_fill_increment=_optional_int(
            document.get("paper_partial_fill_increment")
        ),
        minimum_regular_sell_quantity=_optional_int(
            document.get("minimum_regular_sell_quantity")
        ),
        sell_increment=_optional_int(document.get("sell_increment")),
        sell_residual_policy=_optional_string(
            document.get("sell_residual_policy")
        ),
        policy_version=_optional_string(document.get("policy_version")),
    )


def _risk_policy_changes(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDayRiskPolicyChangeProjection, ...]:
    result: list[PaperDayRiskPolicyChangeProjection] = []
    for event in events:
        if event.event_type != "OPERATOR_RISK_POLICY_CHANGED":
            continue
        previous = _optional_object(event.payload.get("old_risk_policy")) or {}
        current = _optional_object(event.payload.get("new_risk_policy")) or {}
        result.append(
            PaperDayRiskPolicyChangeProjection(
                sequence=event.sequence,
                known_at=event.known_at,
                operator_authorized=_optional_bool(
                    event.payload.get("operator_authorized")
                ),
                reason_codes=_string_tuple(event.payload.get("reason_codes")),
                old_policy_sha256=_optional_string(
                    event.payload.get("old_risk_policy_sha256")
                ),
                new_policy_sha256=_optional_string(
                    event.payload.get("new_risk_policy_sha256")
                ),
                old_maximum_positions=_optional_int(
                    previous.get("maximum_positions")
                ),
                new_maximum_positions=_optional_int(
                    current.get("maximum_positions")
                ),
                new_position_count_limit_enabled=_optional_bool(
                    current.get("position_count_limit_enabled")
                ),
                validated_fill_count=_optional_int(
                    event.payload.get("validated_fill_count")
                ),
                validated_position_count=_optional_int(
                    event.payload.get("validated_position_count")
                ),
            )
        )
    return tuple(result)


def _current_risk_policy(
    events: Iterable[PaperDaySidecarEvent],
) -> PaperDayRiskPolicyProjection | None:
    selected: tuple[PaperDaySidecarEvent, dict[str, object]] | None = None
    policy_event_types = {
        "DAY_STARTED",
        "OPERATOR_RISK_POLICY_CHANGED",
        "RUNNER_RECOVERY_AFTER_ABORT",
        "RUNNER_RESUMED",
    }
    for event in events:
        if event.event_type not in policy_event_types:
            continue
        policy = _optional_object(event.payload.get("risk_policy"))
        if policy is None and event.event_type == "OPERATOR_RISK_POLICY_CHANGED":
            policy = _optional_object(event.payload.get("new_risk_policy"))
        if policy is not None:
            selected = (event, policy)
    if selected is None:
        return None
    event, policy = selected
    price_policy = _optional_object(policy.get("price_acceptance_policy")) or {}
    quantity_policy = _optional_object(policy.get("order_quantity_policy")) or {}
    quantity_rules = tuple(
        rule
        for board, value in quantity_policy.items()
        if board != "version"
        if (rule := _quantity_rule(value, board_override=board)) is not None
    )
    unsupported_quantity_boards = tuple(
        board
        for board, value in quantity_policy.items()
        if board != "version" and isinstance(value, str)
    )
    return PaperDayRiskPolicyProjection(
        sequence=event.sequence,
        known_at=event.known_at,
        source_event_type=event.event_type,
        maximum_positions=_optional_int(policy.get("maximum_positions")),
        position_count_limit_enabled=_optional_bool(
            policy.get("position_count_limit_enabled")
        ),
        cash_reserve_fraction=_optional_decimal(
            policy.get("cash_reserve_fraction")
        ),
        maximum_gross_fraction=_optional_decimal(
            policy.get("maximum_gross_fraction")
        ),
        maximum_symbol_fraction=_optional_decimal(
            policy.get("maximum_symbol_fraction")
        ),
        risk_per_trade_fraction=_optional_decimal(
            policy.get("risk_per_trade_fraction")
        ),
        price_acceptance_policy_version=_optional_string(
            price_policy.get("version")
        ),
        order_quantity_policy_version=_optional_string(
            quantity_policy.get("version")
        ),
        order_quantity_rules=quantity_rules,
        unsupported_quantity_boards=unsupported_quantity_boards,
    )


def _buy_execution_acceptances(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDayBuyExecutionProjection, ...]:
    result: list[PaperDayBuyExecutionProjection] = []
    for event in events:
        if event.event_type != "ORDER_SUBMITTED":
            continue
        acceptance_document = _optional_object(
            event.payload.get("price_acceptance")
        )
        if acceptance_document is None:
            continue
        order = _optional_object(event.payload.get("order")) or {}
        symbol = event.symbol or _optional_string(order.get("symbol"))
        if symbol is None:
            continue
        result.append(
            PaperDayBuyExecutionProjection(
                sequence=event.sequence,
                known_at=event.known_at,
                symbol=symbol,
                order_id=_optional_string(order.get("order_id"))
                or event.correlation_id,
                quantity=_optional_int(order.get("quantity")),
                price_acceptance=_price_acceptance(
                    acceptance_document,
                    status="AVAILABLE",
                ),
                quantity_rule=_quantity_rule(event.payload.get("quantity_rule")),
            )
        )
    return tuple(result)


def _sell_execution_acceptances(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDaySellExecutionProjection, ...]:
    result: list[PaperDaySellExecutionProjection] = []
    for event in events:
        if event.event_type != "SELL_PRICE_ACCEPTANCE_EVALUATED":
            continue
        symbol = event.symbol
        if symbol is None:
            continue
        plan = _optional_object(event.payload.get("sell_quantity_plan")) or {}
        status = (
            _optional_string(event.payload.get("price_acceptance_status"))
            or "UNKNOWN"
        )
        result.append(
            PaperDaySellExecutionProjection(
                sequence=event.sequence,
                known_at=event.known_at,
                symbol=symbol,
                price_acceptance=_price_acceptance(
                    event.payload.get("price_acceptance"),
                    status=status,
                ),
                quantity_rule=_quantity_rule(event.payload.get("quantity_rule")),
                available_to_sell=_optional_int(plan.get("available_to_sell")),
                quantity_plan_status=_optional_string(plan.get("status")),
                future_limit_order_sequence=_int_tuple(
                    plan.get("future_limit_order_sequence")
                ),
                current_blockers=_string_tuple(
                    event.payload.get("current_execution_blockers")
                ),
                future_non_execution_conditions=_string_tuple(
                    event.payload.get("future_non_execution_conditions")
                ),
            )
        )
    return tuple(result)


def _int_tuple(value: object) -> tuple[int, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(
        item for item in value if isinstance(item, int) and not isinstance(item, bool)
    )


def _stable_rejection_reasons(
    *,
    buy_reject_reasons: Counter[str],
    expiry_reasons: Counter[str],
    sell_acceptances: tuple[PaperDaySellExecutionProjection, ...],
) -> tuple[PaperDayStableReasonProjection, ...]:
    counts: Counter[tuple[str, str]] = Counter()
    for code, count in buy_reject_reasons.items():
        counts[("买入门控/风控", code)] += count
    for code, count in expiry_reasons.items():
        counts[("PAPER 撮合/到期", code)] += count
    for acceptance in sell_acceptances:
        if acceptance.price_acceptance.status not in {"AVAILABLE", "UNKNOWN"}:
            counts[("卖出价格评估", acceptance.price_acceptance.status)] += 1
        for code in acceptance.current_blockers:
            counts[("卖出当前阻断", code)] += 1
    return tuple(
        PaperDayStableReasonProjection(
            category=category,
            code=code,
            count=count,
            explanation=_stable_reason_explanation(code),
        )
        for (category, code), count in sorted(
            counts.items(),
            key=lambda item: (item[0][0], -item[1], item[0][1]),
        )
    )


def _order_id(event: PaperDaySidecarEvent) -> str | None:
    direct = _optional_string(event.payload.get("order_id"))
    if direct is not None:
        return direct
    order = _optional_object(event.payload.get("order"))
    if order is not None:
        nested = _optional_string(order.get("order_id"))
        if nested is not None:
            return nested
    return event.correlation_id


def _unique_applied_fills(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDaySidecarEvent, ...]:
    result: list[PaperDaySidecarEvent] = []
    seen: set[str] = set()
    for event in events:
        if event.event_type != "FILL_APPLIED":
            continue
        fill_id = _optional_string(event.payload.get("fill_id")) or event.correlation_id
        if fill_id is None or fill_id in seen:
            continue
        seen.add(fill_id)
        result.append(event)
    return tuple(result)


def _latest_account_event(
    events: Iterable[PaperDaySidecarEvent],
) -> PaperDaySidecarEvent | None:
    result = None
    for event in events:
        if _optional_decimal(event.payload.get("cash")) is not None and isinstance(
            event.payload.get("positions"), list
        ):
            result = event
    return result


def _positions(
    value: object,
    realized_by_symbol: dict[str, Decimal] | None,
) -> tuple[PaperDayPositionProjection, ...]:
    if not isinstance(value, list):
        return ()
    result = []
    for item in value:
        document = _optional_object(item)
        if document is None:
            continue
        symbol = _optional_string(document.get("symbol"))
        quantity = _optional_int(document.get("quantity"))
        if symbol is None or quantity is None or quantity < 0:
            continue
        average_cost = _optional_decimal(document.get("average_cost"))
        mark = _optional_decimal(document.get("mark"))
        market_value = mark * quantity if mark is not None else None
        unrealized = (
            (mark - average_cost) * quantity
            if mark is not None and average_cost is not None
            else None
        )
        realized = None if realized_by_symbol is None else realized_by_symbol.get(symbol)
        result.append(
            PaperDayPositionProjection(
                symbol=symbol,
                quantity=quantity,
                today_buy=_optional_int(document.get("today_buy")),
                available_to_sell=_optional_int(document.get("available_to_sell")),
                average_cost=average_cost,
                mark=mark,
                market_value=market_value,
                unrealized_pnl=unrealized,
                realized_pnl=realized,
            )
        )
    return tuple(sorted(result, key=lambda item: item.symbol))


def _realized_by_symbol(
    final_account: dict[str, object] | None,
) -> dict[str, Decimal] | None:
    if final_account is None:
        return None
    values = final_account.get("positions")
    if not isinstance(values, list):
        return None
    result: dict[str, Decimal] = {}
    for value in values:
        document = _optional_object(value)
        if document is None:
            return None
        symbol = _optional_string(document.get("symbol"))
        realized = _optional_decimal(document.get("realized_pnl"))
        if symbol is None or realized is None:
            return None
        result[symbol] = realized
    return result


def _applied_fill_sides(
    events: Iterable[PaperDaySidecarEvent],
    applied: tuple[PaperDaySidecarEvent, ...],
) -> tuple[str, ...]:
    started: dict[str, str] = {}
    for event in events:
        if event.event_type != "FILL_STARTED":
            continue
        fill = _optional_object(event.payload.get("fill"))
        fill_id = None if fill is None else _optional_string(fill.get("fill_id"))
        side = None if fill is None else _optional_string(fill.get("side"))
        if fill_id is not None and side is not None:
            started[fill_id] = side
    result = []
    for event in applied:
        fill_id = _optional_string(event.payload.get("fill_id")) or event.correlation_id
        if fill_id is not None and fill_id in started:
            result.append(started[fill_id])
    return tuple(result)


def _notification_projection(
    events: tuple[PaperDaySidecarEvent, ...],
    terminal: PaperDaySidecarEvent | None,
    final_result: dict[str, object] | None,
    status: dict[str, object],
) -> PaperDayNotificationProjection:
    required = sum(
        isinstance(event.payload.get("notification_text"), str)
        and bool(cast(str, event.payload.get("notification_text")).strip())
        for event in events
    )
    sent = None
    gaps = None
    exact = False
    if final_result is not None:
        final_required = _optional_int(final_result.get("notification_required"))
        final_sent = _optional_int(final_result.get("notification_sent"))
        final_gaps = _optional_int(final_result.get("notification_gaps"))
        if final_required is not None and final_sent is not None and final_gaps is not None:
            required = final_required
            sent = final_sent
            gaps = final_gaps
            exact = True
    payload = {} if terminal is None else terminal.payload
    delivery_source = final_result if final_result is not None else status
    artifact_status = _optional_string(delivery_source.get("artifact_delivery_status"))
    artifact_complete = _optional_bool(
        delivery_source.get("artifact_delivery_complete")
    )
    daily_complete = _optional_bool(
        delivery_source.get("daily_review_delivery_complete")
    )
    text_required = _optional_int(delivery_source.get("text_notification_required"))
    text_sent = _optional_int(delivery_source.get("text_notification_sent"))
    text_gaps = _optional_int(delivery_source.get("text_notification_gaps"))
    delivery_exact = (
        artifact_status in {"PENDING", "SENT", "AMBIGUOUS", "NOT_CONFIGURED"}
        and artifact_complete is not None
        and daily_complete is not None
        and text_required is not None
        and text_sent is not None
        and text_gaps is not None
    )
    if artifact_status not in {"PENDING", "SENT", "AMBIGUOUS", "NOT_CONFIGURED"}:
        artifact_status = _artifact_status_from_events(events)
        artifact_complete = artifact_status == "SENT"
        daily_complete = False
    return PaperDayNotificationProjection(
        required=required,
        sent=sent,
        gaps=gaps,
        retried=None,
        dead=None,
        exact_final_counts=exact,
        required_before_summary=_optional_int(
            payload.get("notification_required_before_summary")
        ),
        sent_before_summary=_optional_int(payload.get("notification_sent_before_summary")),
        gaps_before_summary=_optional_int(payload.get("notification_gaps_before_summary")),
        artifact_delivery_status=artifact_status,
        artifact_delivery_complete=artifact_complete is True,
        daily_review_delivery_complete=daily_complete is True,
        text_required=text_required,
        text_sent=text_sent,
        text_gaps=text_gaps,
        delivery_projection_exact=delivery_exact,
    )


def _artifact_status_from_events(events: tuple[PaperDaySidecarEvent, ...]) -> str:
    """从 sidecar 事件投影附件状态；缺少精确总计时仍保持失败关闭。"""

    by_type = {
        "REPORT_ARTIFACT_DELIVERY_AMBIGUOUS": "AMBIGUOUS",
        "REPORT_ARTIFACT_DELIVERY_NOT_CONFIGURED": "NOT_CONFIGURED",
        "REPORT_ARTIFACT_DELIVERY_PENDING": "PENDING",
        "REPORT_ARTIFACT_DELIVERY_SENT": "SENT",
        "REPORT_ARTIFACT_LEGACY_FAILURE_AMBIGUOUS": "AMBIGUOUS",
        "REPORT_UPLOAD_FAILED": "AMBIGUOUS",
    }
    retained = "NOT_REPORTED"
    for event in events:
        if event.event_type in by_type:
            retained = by_type[event.event_type]
        elif event.event_type == "REPORT_UPLOADED" and event.payload.get("delivered") is True:
            retained = "SENT"
    return retained


def _llm_projection(
    events: tuple[PaperDaySidecarEvent, ...],
) -> PaperDayLLMProjection:
    policy = _last_event(events, {"LLM_INTRADAY_POLICY_CONFIGURED"})
    operator_disabled = _last_event(events, {"LLM_INTRADAY_OPERATOR_DISABLED"})
    policy_source = policy or operator_disabled
    policy_binding = (
        None
        if policy_source is None
        else _optional_object(policy_source.payload.get("manifest_binding"))
    )
    enabled = policy is not None or any(
        event.event_type
        in {
            "BUY_LLM_GATE_EVALUATED",
            "LLM_CANDIDATE_REVIEW_COMPLETED",
            "LLM_CANDIDATE_REVIEW_FAILED",
            "LLM_PREOPEN_CONTEXT_FAILED",
            "LLM_PREOPEN_CONTEXT_FROZEN",
            "LLM_REVIEW_BATCH_SCHEDULED",
            "LLM_SERVICE_STATE_CHANGED",
        }
        for event in events
    )
    required_for_buy = (
        None
        if policy_binding is None
        else _optional_bool(policy_binding.get("required_for_buy"))
    )

    frozen = _last_event(events, {"LLM_PREOPEN_CONTEXT_FROZEN"})
    failed = _last_event(events, {"LLM_PREOPEN_CONTEXT_FAILED"})
    latest_preopen = max(
        (item for item in (frozen, failed) if item is not None),
        key=lambda item: item.sequence,
        default=None,
    )
    if latest_preopen is None:
        preopen_status = "PENDING" if enabled else "DISABLED"
        preopen_context_id = None
        preopen_failure_code = None
        preopen_dual_track = None
    elif latest_preopen.event_type == "LLM_PREOPEN_CONTEXT_FAILED":
        preopen_status = "FAILED"
        preopen_context_id = None
        preopen_dual_track = None
        preopen_failure_code = _optional_string(
            latest_preopen.payload.get("error_code")
        )
        if required_for_buy is None:
            required_for_buy = _optional_bool(
                latest_preopen.payload.get("required_for_buy")
            )
    else:
        preopen_status = "FROZEN"
        preopen = _optional_object(latest_preopen.payload.get("preopen_context"))
        preopen_context_id = (
            None if preopen is None else _optional_string(preopen.get("context_id"))
        )
        preopen_failure_code = None
        dual = None if preopen is None else _optional_object(preopen.get("dual_track"))
        if dual is None:
            preopen_dual_track = PaperDayPreopenLLMComparison(
                status="LEGACY_NOT_CARRIED",
                selected_track=None,
                audit_record_sha256=None,
                baseline_decision=None,
                baseline_macro_impact=None,
                baseline_model=None,
                adversarial_decision=None,
                adversarial_macro_impact=None,
                adversarial_model=None,
            )
        else:
            baseline = _optional_object(dual.get("baseline"))
            adversarial = _optional_object(dual.get("adversarial"))
            selected_track = _optional_string(dual.get("selected_track"))
            audit_record_sha256 = _optional_string(
                dual.get("audit_record_sha256")
            )
            baseline_decision = (
                None if baseline is None else _optional_string(baseline.get("decision"))
            )
            baseline_macro_impact = (
                None
                if baseline is None
                else _optional_decimal(baseline.get("macro_impact"))
            )
            baseline_model = (
                None if baseline is None else _optional_string(baseline.get("model"))
            )
            adversarial_decision = (
                None
                if adversarial is None
                else _optional_string(adversarial.get("decision"))
            )
            adversarial_macro_impact = (
                None
                if adversarial is None
                else _optional_decimal(adversarial.get("macro_impact"))
            )
            adversarial_model = (
                None
                if adversarial is None
                else _optional_string(adversarial.get("model"))
            )
            complete = all(
                item is not None
                for item in (
                    selected_track,
                    audit_record_sha256,
                    baseline_decision,
                    baseline_macro_impact,
                    baseline_model,
                    adversarial_decision,
                    adversarial_macro_impact,
                    adversarial_model,
                )
            )
            preopen_dual_track = PaperDayPreopenLLMComparison(
                status="COMPLETE" if complete else "INCOMPLETE",
                selected_track=selected_track,
                audit_record_sha256=audit_record_sha256,
                baseline_decision=baseline_decision,
                baseline_macro_impact=baseline_macro_impact,
                baseline_model=baseline_model,
                adversarial_decision=adversarial_decision,
                adversarial_macro_impact=adversarial_macro_impact,
                adversarial_model=adversarial_model,
            )

    evidence_bound = _last_event(events, {"LLM_EVIDENCE_SNAPSHOT_BOUND"})
    evidence_failed = _last_event(events, {"LLM_EVIDENCE_SNAPSHOT_FAILED"})
    latest_evidence = max(
        (item for item in (evidence_bound, evidence_failed) if item is not None),
        key=lambda item: item.sequence,
        default=None,
    )
    if latest_evidence is None:
        evidence_snapshot_status = "NOT_RECORDED" if enabled else "DISABLED"
        evidence_snapshot_sha256 = None
        evidence_snapshot_failure_code = None
    else:
        binding = _optional_object(
            latest_evidence.payload.get("evidence_snapshot_binding")
        )
        evidence_snapshot_sha256 = _optional_string(
            latest_evidence.payload.get("evidence_snapshot_binding_sha256")
        )
        if evidence_snapshot_sha256 is None and binding is not None:
            evidence_snapshot_sha256 = _optional_string(
                binding.get("audit_sha256")
            )
        if latest_evidence.event_type == "LLM_EVIDENCE_SNAPSHOT_FAILED":
            evidence_snapshot_status = "FAILED"
            evidence_snapshot_failure_code = _optional_string(
                latest_evidence.payload.get("error_code")
            )
        else:
            evidence_snapshot_status = "BOUND"
            evidence_snapshot_failure_code = None

    batches = tuple(
        event for event in events if event.event_type == "LLM_REVIEW_BATCH_SCHEDULED"
    )
    schedule_statuses: Counter[str] = Counter()
    review_candidate_count = 0
    for batch in batches:
        count = _optional_int(batch.payload.get("candidate_count"))
        outcomes = batch.payload.get("outcomes")
        if count is not None:
            review_candidate_count += count
        elif isinstance(outcomes, list):
            review_candidate_count += len(outcomes)
        if not isinstance(outcomes, list):
            continue
        for value in outcomes:
            item = _optional_object(value)
            if item is None:
                continue
            schedule_statuses[_optional_string(item.get("status")) or "UNKNOWN"] += 1

    completed = tuple(
        event
        for event in events
        if event.event_type == "LLM_CANDIDATE_REVIEW_COMPLETED"
    )
    failed_reviews = tuple(
        event
        for event in events
        if event.event_type == "LLM_CANDIDATE_REVIEW_FAILED"
    )
    review_events = (*completed, *failed_reviews)
    latencies: list[int] = []
    requested_models: set[str] = set()
    response_models: set[str] = set()
    prompt_hashes: set[str] = set()
    dual_track_comparisons: list[PaperDayLLMTrackComparison] = []
    if policy_binding is not None:
        identity = _optional_object(policy_binding.get("analyzer_identity"))
        if identity is not None:
            if (model := _optional_string(identity.get("requested_model"))) is not None:
                requested_models.add(model)
            if (prompt := _optional_string(identity.get("prompt_schema_sha256"))) is not None:
                prompt_hashes.add(prompt)
    for event in review_events:
        review = _optional_object(event.payload.get("review"))
        if review is None:
            continue
        latency = _optional_int(review.get("latency_ms"))
        if latency is not None and latency >= 0:
            latencies.append(latency)
        if (model := _optional_string(review.get("response_model"))) is not None:
            response_models.add(model)
        dual = _optional_object(review.get("dual_track"))
        if dual is not None:
            baseline = _optional_object(dual.get("baseline_analysis"))
            adversarial = _optional_object(dual.get("adversarial_analysis"))
            if baseline is not None and adversarial is not None:
                dual_track_comparisons.append(
                    PaperDayLLMTrackComparison(
                        symbol=_optional_string(review.get("symbol")) or "—",
                        review_id=_optional_string(review.get("review_id")) or "—",
                        selected_track=(
                            _optional_string(dual.get("selected_track")) or "—"
                        ),
                        audit_record_sha256=_optional_string(
                            dual.get("audit_record_sha256")
                        ),
                        baseline_decision=(
                            _optional_string(baseline.get("decision")) or "—"
                        ),
                        baseline_macro_impact=_optional_decimal(
                            baseline.get("macro_impact")
                        ),
                        baseline_regime=(
                            _optional_string(baseline.get("regime")) or "—"
                        ),
                        adversarial_decision=(
                            _optional_string(adversarial.get("decision")) or "—"
                        ),
                        adversarial_macro_impact=_optional_decimal(
                            adversarial.get("macro_impact")
                        ),
                        adversarial_regime=(
                            _optional_string(adversarial.get("regime")) or "—"
                        ),
                    )
                )
        identity = _optional_object(review.get("analyzer_identity"))
        if identity is not None:
            if (model := _optional_string(identity.get("requested_model"))) is not None:
                requested_models.add(model)
            if (prompt := _optional_string(identity.get("prompt_schema_sha256"))) is not None:
                prompt_hashes.add(prompt)

    deep_exit_comparisons: list[PaperDayDeepExitLLMComparison] = []
    for event in events:
        if event.event_type != "EXIT_PLAN_DEEP_APPLIED":
            continue
        assessment = _optional_object(
            event.payload.get("deep_exit_llm_assessment")
        )
        if assessment is None:
            continue
        plan = _optional_object(event.payload.get("plan"))
        deep_exit_comparisons.append(
            PaperDayDeepExitLLMComparison(
                sequence=event.sequence,
                known_at=event.known_at,
                symbol=(
                    event.symbol
                    or (None if plan is None else _optional_string(plan.get("symbol")))
                    or "—"
                ),
                protection_id=_optional_string(
                    assessment.get("protection_id")
                )
                or _optional_string(event.payload.get("protection_id")),
                plan_id=_optional_string(assessment.get("plan_id"))
                or (None if plan is None else _optional_string(plan.get("plan_id"))),
                selected_system=(
                    _optional_string(assessment.get("selected_system")) or "UNKNOWN"
                ),
                selected_score=_optional_decimal(assessment.get("selected_score")),
                baseline_score=_optional_decimal(assessment.get("baseline_score")),
                adversarial_score=_optional_decimal(
                    assessment.get("adversarial_score")
                ),
                status=_optional_string(assessment.get("status")) or "UNKNOWN",
            )
        )

    deep_exit_sell_reviews: list[PaperDayDeepExitSellReview] = []
    for event in events:
        if event.event_type != "SELL_SIGNAL_TRIGGERED":
            continue
        review = _optional_object(event.payload.get("deep_exit_sell_review"))
        if review is None:
            continue
        deep_exit_sell_reviews.append(
            PaperDayDeepExitSellReview(
                sequence=event.sequence,
                known_at=event.known_at,
                symbol=event.symbol or "—",
                action=_optional_string(review.get("action")) or "UNKNOWN",
                technical_score=_optional_decimal(review.get("technical_score")),
                selected_semantic_score=_optional_decimal(
                    review.get("selected_semantic_score")
                ),
                combined_exit_score=_optional_decimal(
                    review.get("combined_exit_score")
                ),
                llm_can_veto=_optional_bool(review.get("llm_can_veto")) is True,
            )
        )

    gates = tuple(
        event for event in events if event.event_type == "BUY_LLM_GATE_EVALUATED"
    )
    gate_actions = Counter(
        _optional_string(event.payload.get("action")) or "UNKNOWN" for event in gates
    )
    gate_reasons = Counter(
        _optional_string(event.payload.get("reason_code")) or "UNKNOWN"
        for event in gates
    )
    for event in gates:
        if required_for_buy is None:
            required_for_buy = _optional_bool(event.payload.get("required_for_buy"))
        if (model := _optional_string(event.payload.get("requested_model"))) is not None:
            requested_models.add(model)
        if (model := _optional_string(event.payload.get("response_model"))) is not None:
            response_models.add(model)
        if (prompt := _optional_string(event.payload.get("prompt_schema_sha256"))) is not None:
            prompt_hashes.add(prompt)

    sell_not_applicable = 0
    for event in events:
        if event.event_type != "SELL_SIGNAL_TRIGGERED":
            continue
        gate = _optional_object(event.payload.get("llm_gate"))
        if gate is not None and gate.get("action") == "NOT_APPLICABLE":
            sell_not_applicable += 1

    restored = sum(
        _optional_int(event.payload.get("restored_review_count")) or 0
        for event in events
        if event.event_type == "LLM_REVIEW_CACHE_RESTORED"
    )
    service_edges = tuple(
        event for event in events if event.event_type == "LLM_SERVICE_STATE_CHANGED"
    )
    service_state = (
        "DISABLED"
        if not enabled
        else (
            "UNKNOWN"
            if not service_edges
            else _optional_string(service_edges[-1].payload.get("state")) or "UNKNOWN"
        )
    )
    ordered_latencies = tuple(sorted(latencies))
    return PaperDayLLMProjection(
        enabled=enabled,
        required_for_buy=required_for_buy,
        preopen_status=preopen_status,
        preopen_context_id=preopen_context_id,
        preopen_failure_code=preopen_failure_code,
        preopen_dual_track=preopen_dual_track,
        evidence_snapshot_status=evidence_snapshot_status,
        evidence_snapshot_sha256=evidence_snapshot_sha256,
        evidence_snapshot_failure_code=evidence_snapshot_failure_code,
        review_batch_count=len(batches),
        review_candidate_count=review_candidate_count,
        schedule_status_counts=_counter_items(schedule_statuses),
        reviews_completed=len(completed),
        reviews_failed=len(failed_reviews),
        cache_rejected=sum(
            event.event_type == "LLM_CANDIDATE_REVIEW_CACHE_REJECTED"
            for event in events
        ),
        cache_restored=restored,
        gate_evaluations=len(gates),
        gate_blocked=sum(event.payload.get("blocks_entry") is True for event in gates),
        gate_action_counts=_counter_items(gate_actions),
        gate_reason_counts=_counter_items(gate_reasons),
        sell_not_applicable=sell_not_applicable,
        latency_p50_ms=_nearest_rank(ordered_latencies, Decimal("0.50")),
        latency_p95_ms=_nearest_rank(ordered_latencies, Decimal("0.95")),
        latency_max_ms=None if not ordered_latencies else ordered_latencies[-1],
        requested_models=tuple(sorted(requested_models)),
        response_models=tuple(sorted(response_models)),
        prompt_schema_sha256=tuple(sorted(prompt_hashes)),
        service_state=service_state,
        service_state_transition_count=len(service_edges),
        service_degradation_count=sum(
            event.payload.get("state") == "DEGRADED" for event in service_edges
        ),
        service_recovery_count=sum(
            event.payload.get("previous_state") == "DEGRADED"
            and event.payload.get("state") == "HEALTHY"
            for event in service_edges
        ),
        dual_track_comparisons=tuple(dual_track_comparisons),
        deep_exit_comparisons=tuple(deep_exit_comparisons),
        deep_exit_sell_reviews=tuple(deep_exit_sell_reviews),
    )


def _nearest_rank(values: tuple[int, ...], percentile: Decimal) -> int | None:
    if not values:
        return None
    index = max(
        0,
        int(
            (Decimal(len(values)) * percentile).to_integral_value(
                rounding=ROUND_CEILING
            )
        )
        - 1,
    )
    return values[index]


def _source_transitions(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDaySourceTransition, ...]:
    result = []
    for event in events:
        if event.event_type != "SOURCE_STATE_CHANGED":
            continue
        component = _optional_string(event.payload.get("component"))
        state = _optional_string(event.payload.get("state"))
        if component is None or state is None:
            continue
        result.append(
            PaperDaySourceTransition(
                sequence=event.sequence,
                known_at=event.known_at,
                component=component,
                previous_state=_optional_string(event.payload.get("previous_state")),
                state=state,
                error_code=_optional_string(event.payload.get("error_code")),
            )
        )
    return tuple(result)


def _source_recovery_count(transitions: tuple[PaperDaySourceTransition, ...]) -> int:
    latest: dict[str, str] = {}
    recoveries = 0
    for item in transitions:
        previous = latest.get(item.component, item.previous_state)
        if item.state == "HEALTHY" and previous not in (None, "HEALTHY"):
            recoveries += 1
        latest[item.component] = item.state
    return recoveries


def _read_final_result(
    root: Path, session_date: date, run_id: str
) -> dict[str, object] | None:
    candidates = (
        root / "final-result.json",
        root / "result.json",
        root / "runner.stdout.log",
    )
    for path in candidates:
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            continue
        documents = [text.strip(), *(line.strip() for line in reversed(text.splitlines()))]
        for document in documents:
            if not document:
                continue
            try:
                parsed = json.loads(document)
            except json.JSONDecodeError:
                continue
            if not isinstance(parsed, dict):
                continue
            value = cast(dict[str, object], parsed)
            if (
                value.get("ok") is True
                and value.get("action") == "run"
                and value.get("run_id") == run_id
                and value.get("session_date") == session_date.isoformat()
            ):
                return value
    return None


def _latest_original_report(root: Path, session_date: date) -> Path | None:
    report_dir = root / "reports"
    if not report_dir.is_dir():
        return None
    reports = tuple(
        sorted(
            report_dir.glob(f"ashare-paper-day-{session_date.isoformat()}-*.md"),
            key=lambda item: (item.stat().st_mtime_ns, item.name),
        )
    )
    return reports[-1].resolve() if reports else None


_STABLE_REASON_EXPLANATIONS: Mapping[str, str] = {
    "UNSPECIFIED_REJECTION": "sidecar 记录了拒绝，但没有提供更具体的稳定原因码",
    "NO_CURRENT_SESSION_ANOMALY": (
        "最近一次全市场扫描未将该标的列为当日异动候选，只有分钟技术形态而缺少横截面确认"
    ),
    "ANOMALY_NOT_MOMENTUM_EXPANSION": "标的虽在异动名单中，但尚未达到动量扩张级别",
    "NO_CURRENT_SESSION_SCAN": "本交易日尚无可用的全市场扫描结果",
    "CURRENT_SESSION_SCAN_NOT_COMPLETE": "最近一次全市场扫描不完整或处于降级状态",
    "CURRENT_SESSION_ANOMALY_EXPIRED": "最近一次异动确认已超过有效期",
    "SYMBOL_ALREADY_ENTERED_TODAY": "该标的今日已经模拟买入，策略不重复加仓",
    "SYMBOL_ORDER_ALREADY_PENDING": "该标的已有待撮合 PAPER 委托",
    "PENDING_CAPACITY_RESERVED": "另一笔待撮合委托已占用保守资金容量",
    "SYMBOL_ALREADY_HELD": "账户已经持有该标的，今日策略禁止重复加仓",
    "MINUTE_DATA_STALE": "最新完整分钟线已经过期",
    "UNAPPROVED_DEGRADED_MINUTE_PROVIDER": "分钟行情来自未获准的降级数据源",
    "DEGRADED_SOURCE_NOT_INDEPENDENTLY_CORROBORATED": (
        "降级分钟行情没有得到独立全市场快照交叉确认"
    ),
    "CORROBORATION_PRICE_MISSING": "交叉确认快照缺少有效价格",
    "CROSS_SOURCE_PRICE_DIVERGENCE": "分钟行情与全市场快照价格偏差超过容许范围",
    "MINUTE_FRESHNESS_NOT_CURRENT": "分钟行情的新鲜度状态不是当前可用",
    "SIGNAL_NOT_ENTER_CANDIDATE": "技术结论不是可入场候选",
    "SIGNAL_PRICE_MISSING": "技术信号缺少有效参考价格",
    "INVALIDATION_PRICE_MISSING": "技术信号缺少有效失效价格",
    "INVALIDATION_NOT_BELOW_ENTRY": "失效价格没有低于拟入场价格",
    "SIGNAL_NOT_COMPLETED": "信号所用分钟线尚未完整收盘",
    "SIGNAL_SESSION_MISMATCH": "信号不属于当前交易日",
    "ACCOUNT_SESSION_MISMATCH": "PAPER 账户交易日未与信号对齐",
    "ENTRY_CUTOFF_PASSED": "已超过当日允许创建买入委托的最晚时间",
    "BOARD_MISSING": "缺少可验证的上市板块信息",
    "BOARD_UNSUPPORTED": "当前盘中执行模型不支持该板块",
    "BOARD_SYMBOL_MISMATCH": "证券代码与上市板块不一致",
    "PREVIOUS_CLOSE_MISSING": "缺少可验证的上一交易日收盘价",
    "PRICE_OUTSIDE_DAILY_BAND": "信号价格超出保守日价格带",
    "LIMIT_OUTSIDE_DAILY_BAND": "拟定限价超出保守日价格带",
    "MAX_POSITIONS_REACHED": "显式启用的持仓数量熔断器已达到上限",
    "GROSS_LIMIT_REACHED": "组合总敞口已达到策略上限",
    "SYMBOL_LIMIT_REACHED": "单一标的资金敞口已达到策略上限",
    "CASH_RESERVE_BINDING": "创建委托后会侵占最低现金储备",
    "BELOW_ROUND_LOT": "风险和资金预算计算出的数量不足一手",
    "BELOW_BOARD_MINIMUM_BUY": "风险、资金、费用和板块规则测算后不足最低买入数量",
    "SYMBOL_MISMATCH": "撮合行情标的与委托标的不一致",
    "BAR_NOT_CLOSED": "用于撮合的分钟线尚未完整收盘",
    "BAR_NOT_ONE_MINUTE": "撮合证据不是策略要求的 1 分钟线",
    "BAR_NOT_STRICTLY_AFTER_SIGNAL": "撮合分钟并未严格晚于信号分钟",
    "BAR_STARTED_BEFORE_ORDER": "撮合分钟早于委托激活时间",
    "SESSION_MISMATCH": "撮合分钟与委托不属于同一交易日",
    "ORDER_EXPIRED": "委托已经超过当日最晚入场时间",
    "BAR_NOT_YET_OBSERVABLE": "撮合分钟在决策时尚不可知，不能用于成交",
    "BAR_OHLC_INVALID": "撮合分钟的 OHLC 关系无效",
    "BAR_VOLUME_ZERO": "撮合分钟没有可验证成交量",
    "BAR_OUTSIDE_DAILY_BAND": "分钟行情超出保守日价格带",
    "LOCKED_LIMIT_UP_QUEUE_UNMODELED": "涨停封死，分钟线无法证明排队买单能够成交",
    "SIGNAL_INVALIDATED_BEFORE_FILL": "撮合分钟触及或跌破失效位，买入逻辑已失效",
    "LIMIT_NOT_TOUCHED": "撮合分钟没有触及买入限价",
    "VOLUME_CAPACITY_BELOW_LOT": "成交量参与上限折算后不足板块规定的 PAPER 撮合单位",
    "FILL_OUTSIDE_ACCEPTABLE_RANGE": "拟成交价不在冻结的策略可接受区间内",
    "ORDER_QUANTITY_OUTSIDE_BOARD_RULES": "委托数量不符合板块最低数量、递增单位或单笔上限",
    "MORNING_SESSION_ENDED": "上午交易时段结束，未撮合委托在午休前撤销",
    "ORDER_EXPIRED_AT_RECESS": "午休边界到达，未成交委托已撤销",
    "ORDER_EXPIRED_AT_CLOSE": "收盘边界到达，未成交委托已撤销",
    "REFERENCE_OR_PREVIOUS_CLOSE_MISSING": "缺少卖出参考价或昨收，无法冻结价格区间",
    "PRICE_CORRIDOR_INVALID": "卖出价格区间或价格带元数据无效",
    "NO_SELL_ORDER_BY_DAY_TEST_POLICY": "本次全天测试明确只记录卖出信号，不创建卖单",
    "T1_SELLABLE_QUANTITY_ZERO": "今日买入股份受 T+1 限制，当前可卖数量为零",
    "SELL_QUANTITY_RULE_UNAVAILABLE": "板块数量规则不可用，未来不得创建卖单",
    "NEXT_EXECUTION_PRICE_BELOW_SELL_LIMIT": "下一执行价低于最低接受卖价时不得假设成交",
    "NEXT_BAR_HIGH_BELOW_SELL_LIMIT": "下一分钟最高价仍低于卖出限价时不得假设成交",
    "LOCKED_LIMIT_DOWN_QUEUE_UNMODELED": "跌停封死时分钟线无法证明卖单排队能够成交",
    "INSUFFICIENT_VERIFIED_VOLUME": "可验证成交量不足，不能模拟足额卖出",
    "SELL_QUANTITY_NOT_BOARD_VALID": "拟卖数量不符合对应板块申报规则",
    "PRICE_BAND_OR_REFERENCE_METADATA_INVALID": "日价格带或参考价格元数据无效",
    "REMOVE_POSITION_COUNT_CAP": "取消原固定 5 个持仓的计数硬上限",
    "ADD_PRICE_ACCEPTANCE_BOUNDS": "为买入和卖出增加可审计的价格接受区间与破位不成交规则",
    "ADD_BOARD_QUANTITY_RULES": "增加主板、创业板和科创板的分板块申报数量规则",
    "NOT_L1_VERIFIED_PAPER_MINUTE_BAR_ONLY": "仅有分钟 K 证据，未验证实时盘口价格笼子",
    "LLM_PREOPEN_CONTEXT_UNAVAILABLE": "盘前宏观基线不可用，强制复核模式下买入失败关闭",
    "LLM_PREOPEN_CONTEXT_MODEL_MISMATCH": "盘前宏观基线的响应模型与冻结请求模型不一致",
    "LLM_EVIDENCE_SNAPSHOT_UNAVAILABLE": "原始证据快照不可按清单精确重放，禁止静默换用新快照",
    "LLM_REVIEW_NOT_READY": "候选复核尚未完成；买入路径不会等待网络或模型",
    "LLM_REVIEW_NOT_JOURNALED": "模型结果已返回但尚未先写入审计日志，因此不可用于交易",
    "LLM_REVIEW_EXPIRED": "候选复核已超过冻结的有效期",
    "LLM_REVIEW_INPUT_MISMATCH": "复核结果与当前候选、扫描版本或证据作用域不一致",
    "LLM_REVIEW_KNOWN_AFTER_SIGNAL": "复核结果在技术信号之后才可知，不允许回看使用",
    "LLM_MODEL_IDENTITY_MISMATCH": "请求模型、响应模型或提示词合约身份不一致",
    "LLM_REVIEW_FAILED": "模型复核失败并以稳定失败码留痕",
    "LLM_REVIEW_ABSTAINED": "模型明确选择弃权，强制复核模式下不得买入",
    "LLM_EVIDENCE_INVALID": "复核证据覆盖不足、无引用或包含未知引用",
    "LLM_NEGATIVE_VETO": "宏观复核达到负面否决阈值，只能否决技术入场",
    "LLM_COMBINED_SCORE_BELOW_ENTRY_THRESHOLD": "技术分与宏观分融合后低于 0.70 入场阈值",
}


def _stable_reason_explanation(code: str) -> str:
    return _STABLE_REASON_EXPLANATIONS.get(
        code,
        "该稳定码暂无内置中文释义，请以对应不可变事件 payload 为准",
    )


def _explained_codes(codes: tuple[str, ...]) -> str:
    if not codes:
        return "—"
    return "<br>".join(
        f"`{code}`：{_stable_reason_explanation(code)}" for code in codes
    )


def _llm_audit_lines(projection: PaperDayExecutiveProjection) -> list[str]:
    llm = projection.llm
    rows = ["### 盘中 LLM 复核审计", ""]
    if not llm.enabled:
        rows.extend(
            (
                "本次会话未启用盘中 LLM。技术面、行情门、风险门和 T+1 记录逻辑不受影响。",
                "",
            )
        )
        return rows
    required = (
        UNAVAILABLE
        if llm.required_for_buy is None
        else ("是（缺失或未就绪时拒绝买入）" if llm.required_for_buy else "否")
    )
    latency = (
        UNAVAILABLE
        if llm.latency_p50_ms is None
        else (
            f"p50={llm.latency_p50_ms}ms；p95={llm.latency_p95_ms}ms；"
            f"max={llm.latency_max_ms}ms"
        )
    )
    preopen_failure = (
        _readable_code(llm.preopen_failure_code) if llm.preopen_failure_code else "无"
    )
    snapshot_failure = (
        _readable_code(llm.evidence_snapshot_failure_code)
        if llm.evidence_snapshot_failure_code
        else "无"
    )
    rows.extend(
        (
            "入场模型只复核已通过确定性行情门的技术入场候选；买入查询只读本地"
            "已留痕缓存，不等待网络。卖出侧的 LLM 评分属于成交后的 DEEP 退出计划，"
            "硬止损、T+1 和价格边界不会等待模型。",
            "",
            "| 项目 | 结果 |",
            "|---|---|",
            f"| 买入是否强制 LLM | {required} |",
            f"| 盘前宏观基线 | {_readable_code(llm.preopen_status)}；上下文标识="
            f"`{llm.preopen_context_id or '—'}`；失败说明="
            f"{preopen_failure} |",
            f"| 证据快照 | {_readable_code(llm.evidence_snapshot_status)}；摘要="
            f"`{(llm.evidence_snapshot_sha256 or '—')[:12]}`；失败说明="
            f"{snapshot_failure} |",
            f"| 完整扫描复核批次/候选 | {llm.review_batch_count} / "
            f"{llm.review_candidate_count}；{_pairs(llm.schedule_status_counts)} |",
            f"| 模型结果 | 完成 {llm.reviews_completed}；失败 {llm.reviews_failed}；"
            f"缓存拒绝 {llm.cache_rejected}；重启恢复 {llm.cache_restored} |",
            f"| 服务健康边沿 | {_readable_code(llm.service_state)}；迁移 "
            f"{llm.service_state_transition_count}；降级 {llm.service_degradation_count}；"
            f"恢复 {llm.service_recovery_count} |",
            f"| 买入复核门 | 评估 {llm.gate_evaluations}；阻断 {llm.gate_blocked}；"
            f"动作 {_pairs(llm.gate_action_counts)} |",
            f"| 复核原因码 | {_pairs(llm.gate_reason_counts)} |",
            f"| 卖出旁路 | 不适用 {llm.sell_not_applicable} 次 |",
            f"| 后台模型延迟 | {latency} |",
            f"| 请求/响应模型 | {', '.join(llm.requested_models) or UNAVAILABLE} / "
            f"{', '.join(llm.response_models) or UNAVAILABLE} |",
            "| 提示词合约摘要 | "
            + (", ".join(f"`{value[:12]}`" for value in llm.prompt_schema_sha256) or UNAVAILABLE)
            + " |",
            "",
        )
    )
    preopen_dual = llm.preopen_dual_track
    if preopen_dual is not None:
        rows.extend(("#### 盘前宏观双轨冻结", ""))
        if preopen_dual.status == "LEGACY_NOT_CARRIED":
            rows.extend(
                (
                    "该历史冻结事件未携带两轨详情；当前不可得，系统没有补造模型结论。",
                    "",
                )
            )
        elif preopen_dual.status != "COMPLETE":
            rows.extend(
                (
                    "盘前冻结事件中的双轨字段不完整，无法形成可信对照；以下报告不推测缺失值。",
                    "",
                )
            )
        else:
            rows.extend(
                (
                    "| 生产采用与审计 | 原单分析器 | 结构化对抗分析器 |",
                    "|---|---|---|",
                    "| "
                    f"{_readable_code(preopen_dual.selected_track or '')}；审计摘要 "
                    f"`{(preopen_dual.audit_record_sha256 or '不可得')[:12]}` | "
                    f"{_readable_code(preopen_dual.baseline_decision or '')}；宏观 "
                    f"{_llm_score_text(preopen_dual.baseline_macro_impact)}；模型 "
                    f"{_safe_table_text(preopen_dual.baseline_model or UNAVAILABLE)} | "
                    f"{_readable_code(preopen_dual.adversarial_decision or '')}；宏观 "
                    f"{_llm_score_text(preopen_dual.adversarial_macro_impact)}；模型 "
                    f"{_safe_table_text(preopen_dual.adversarial_model or UNAVAILABLE)} |",
                    "",
                )
            )
    if llm.dual_track_comparisons:
        rows.extend(
            (
                "#### 单分析器与对抗分析器逐条对照",
                "",
                "| 标的/复核 | 生产选择与审计 | 单分析器结果 | 对抗分析器结果 |",
                "|---|---|---|---|",
            )
        )
        for item in llm.dual_track_comparisons:
            rows.append(
                f"| `{item.symbol}` / `{item.review_id[-12:]}` | "
                f"{_readable_code(item.selected_track)} / "
                f"`{(item.audit_record_sha256 or '未落独立审计库')[:12]}` | "
                f"{_readable_code(item.baseline_decision)}；宏观 "
                f"{_llm_score_text(item.baseline_macro_impact)}；"
                f"{_safe_table_text(item.baseline_regime)} | "
                f"{_readable_code(item.adversarial_decision)}；宏观 "
                f"{_llm_score_text(item.adversarial_macro_impact)}；"
                f"{_safe_table_text(item.adversarial_regime)} |"
            )
        rows.append("")
    if llm.deep_exit_comparisons:
        rows.extend(
            (
                "#### 成交后 DEEP 退出计划双轨评分",
                "",
                "| 时间/标的 | 退出计划定位 | 生产采用 | 原单分析器 | 对抗分析系统 | 状态 |",
                "|---|---|---|---|---|---|",
            )
        )
        for deep_item in llm.deep_exit_comparisons:
            rows.append(
                f"| {_local_time(deep_item.known_at)} / `{deep_item.symbol}` | "
                f"计划 `{_tail_identifier(deep_item.plan_id)}`；保护流 "
                f"`{_tail_identifier(deep_item.protection_id)}` | "
                f"{_deep_selected_system_text(deep_item.selected_system)}；评分 "
                f"{_llm_score_text(deep_item.selected_score)} | "
                f"{_llm_score_text(deep_item.baseline_score)} | "
                f"{_llm_score_text(deep_item.adversarial_score)} | "
                f"{_readable_code(deep_item.status)} |"
            )
        rows.append("")
    if llm.deep_exit_sell_reviews:
        rows.extend(
            (
                "#### 卖出/REDUCE 的 DEEP 紧迫度复核",
                "",
                "| 时间/标的 | 复核动作 | 技术评分 | 采用的语义评分 | 组合退出评分 | LLM 可否决 |",
                "|---|---|---|---|---|---|",
            )
        )
        for sell_item in llm.deep_exit_sell_reviews:
            rows.append(
                f"| {_local_time(sell_item.known_at)} / `{sell_item.symbol}` | "
                f"{_readable_code(sell_item.action)} | "
                f"{_llm_score_text(sell_item.technical_score)} | "
                f"{_llm_score_text(sell_item.selected_semantic_score)} | "
                f"{_llm_score_text(sell_item.combined_exit_score)} | "
                f"{'否，技术保护信号优先' if not sell_item.llm_can_veto else '是'} |"
            )
        rows.append("")
    return rows


def _llm_score_text(value: Decimal | None) -> str:
    from .paper_day_formatting import llm_score_text

    return llm_score_text(value)


def _deep_selected_system_text(value: str) -> str:
    from .paper_day_formatting import deep_selected_system_text

    return deep_selected_system_text(value)


def _tail_identifier(value: str | None) -> str:
    from .paper_day_formatting import tail_identifier

    return tail_identifier(value)


def _safe_table_text(value: str) -> str:
    from .paper_day_formatting import safe_table_text

    return safe_table_text(value)


def _risk_policy_audit_lines(projection: PaperDayExecutiveProjection) -> list[str]:
    rows = ["### 运行中风险策略变更", ""]
    if not projection.risk_policy_changes:
        rows.append("本日 sidecar 未记录 `OPERATOR_RISK_POLICY_CHANGED`。")
    else:
        rows.extend(
            (
                "| 序号/时间 | 人工确认 | 变更码与中文说明 | 持仓计数上限 | "
                "策略哈希 | 账本冻结复核 |",
                "|---|---|---|---|---|---|",
            )
        )
        for change in projection.risk_policy_changes:
            authorization = (
                "是" if change.operator_authorized is True else "否或不可证明"
            )
            new_position_limit = _position_limit_display(
                change.new_maximum_positions,
                change.new_position_count_limit_enabled,
            )
            position_limit = (
                f"{_optional_number(change.old_maximum_positions)} → "
                f"{new_position_limit}"
            )
            validation = (
                f"成交 {_optional_number(change.validated_fill_count)} 笔；"
                f"持仓 {_optional_number(change.validated_position_count)} 个；"
                "既有成交/持仓不重算"
            )
            rows.append(
                f"| {change.sequence} / {_local_time(change.known_at)} | {authorization} | "
                f"{_explained_codes(change.reason_codes)} | {position_limit} | "
                f"{_short_hash(change.old_policy_sha256)} → "
                f"{_short_hash(change.new_policy_sha256)} | {validation} |"
            )

    policy = projection.current_risk_policy
    rows.extend(("", "#### 当前仍生效的资金与风险边界", ""))
    if policy is None:
        rows.append("sidecar 没有足够的策略文档，不能证明当前风险边界。")
        rows.append("")
        return rows

    removed_five_position_cap = any(
        change.old_maximum_positions == 5
        and change.new_maximum_positions is None
        and change.new_position_count_limit_enabled is False
        for change in projection.risk_policy_changes
    )
    if removed_five_position_cap:
        rows.append(
            "> 原 5 个持仓硬上限已解除，但这不等于无限可买：最低现金储备、"
            "总敞口、单标的敞口、单笔止损风险、费用、申报数量和待撮合资金占用仍会逐单约束。"
        )
    elif (
        policy.position_count_limit_enabled is False
        and policy.maximum_positions is None
    ):
        rows.append(
            "> 当前策略未启用固定持仓数量上限；这不等于无限可买，"
            "下列资金、敞口和风险约束仍会逐单生效。"
        )
    current_position_limit = _position_limit_display(
        policy.maximum_positions,
        policy.position_count_limit_enabled,
    )
    rows.extend(
        (
            "",
            "| 约束 | 当前值 | 含义 |",
            "|---|---:|---|",
            f"| 固定持仓数量上限 | "
            f"{current_position_limit} | "
            "只取消计数硬上限，不绕过任何资金约束 |",
            f"| 最低现金储备 | {_percent(policy.cash_reserve_fraction)} | "
            "执行后不得侵占的初始权益比例 |",
            f"| 组合总敞口上限 | {_percent(policy.maximum_gross_fraction)} | "
            "所有持仓市值合计的上限 |",
            f"| 单标的敞口上限 | {_percent(policy.maximum_symbol_fraction)} | "
            "单只证券的资金集中度上限 |",
            f"| 单笔止损风险预算 | {_percent(policy.risk_per_trade_fraction)} | "
            "按入场价与失效位距离计算 |",
            f"| 价格接受策略 | `{policy.price_acceptance_policy_version or '不可得'}` | "
            "买卖区间、日价格带和破位不成交 |",
            f"| 数量策略 | `{policy.order_quantity_policy_version or '不可得'}` | "
            "按上市板块校验申报数量 |",
            f"| 策略证据 | {policy.sequence} / {_local_time(policy.known_at)} | "
            f"`{policy.source_event_type}` |",
        )
    )
    if policy.order_quantity_rules or policy.unsupported_quantity_boards:
        rows.extend(
            (
                "",
                "#### 分板块申报数量规则",
                "",
                "| 板块 | 买入规则 | 限价单单笔上限 | 卖出规则 | PAPER 部分成交单位 |",
                "|---|---|---:|---|---:|",
            )
        )
        for rule in sorted(
            policy.order_quantity_rules,
            key=lambda item: _board_sort_key(item.board),
        ):
            rows.append(
                f"| {_board_display(rule.board)} | {_buy_quantity_rule_text(rule)} | "
                f"{_optional_number(rule.maximum_limit_order_quantity)} 股 | "
                f"{_sell_quantity_rule_text(rule)} | "
                f"{_optional_number(rule.paper_partial_fill_increment)} 股 |"
            )
        for board in sorted(
            policy.unsupported_quantity_boards,
            key=lambda item: _board_sort_key(item),
        ):
            rows.append(
                f"| {_board_display(board)} | 不支持 | — | 不支持 | — |"
            )
    rows.append("")
    return rows


def _buy_execution_audit_lines(projection: PaperDayExecutiveProjection) -> list[str]:
    rows = ["### 买入价格接受区间与数量校验", ""]
    if not projection.buy_execution_acceptances:
        rows.extend(
            (
                "本日 `ORDER_SUBMITTED` 未携带可投影的买入价格区间；"
                "报告不会用新策略反推或重写旧成交。当前策略版本及边界见上节。",
                "",
            )
        )
        return rows
    rows.extend(
        (
            "买入区间两端均可成交，但下界必须严格高于技术失效位；"
            "最终成交还必须同时满足日价格带、限价、成交量和板块数量规则。",
            "",
            "| 序号/时间 | 标的/委托 | 本单数量 | 可接受买入价 | 失效边界 | "
            "保守日价格带 | 数量校验 |",
            "|---|---|---:|---|---|---|---|",
        )
    )
    for item in projection.buy_execution_acceptances:
        acceptance = item.price_acceptance
        rows.append(
            f"| {item.sequence} / {_local_time(item.known_at)} | {item.symbol}<br>"
            f"`{item.order_id or '不可得'}` | {_optional_number(item.quantity)} 股 | "
            f"{_price_range(acceptance)}<br>限价 {_price(acceptance.limit_price)} | "
            f"{_price(acceptance.invalidation_boundary)}（触及或跌破即失效） | "
            f"{_daily_band(acceptance)} | {_buy_quantity_rule_text(item.quantity_rule)} |"
        )
    rows.extend(
        (
            "",
            "- `SIGNAL_INVALIDATED_BEFORE_FILL`："
            f"{_stable_reason_explanation('SIGNAL_INVALIDATED_BEFORE_FILL')}。",
            "- `LOCKED_LIMIT_UP_QUEUE_UNMODELED`："
            f"{_stable_reason_explanation('LOCKED_LIMIT_UP_QUEUE_UNMODELED')}。",
            "- 这些区间来自 PAPER 分钟 K 模型；若事件标记 "
            "`real_broker_submission_allowed=false`，则不能直接转换为真实券商委托，"
            "实时盘口价格笼子仍需另行验证。",
            "",
        )
    )
    return rows


def _sell_execution_audit_lines(projection: PaperDayExecutiveProjection) -> list[str]:
    rows = ["### 卖出价格接受区间、T+1 与未来数量计划", ""]
    if not projection.sell_execution_acceptances:
        rows.extend(("本日 sidecar 未记录 `SELL_PRICE_ACCEPTANCE_EVALUATED`。", ""))
        return rows
    by_symbol: dict[str, list[PaperDaySellExecutionProjection]] = {}
    for item in projection.sell_execution_acceptances:
        by_symbol.setdefault(item.symbol, []).append(item)
    rows.extend(
        (
            "本次全天测试仍为卖出信号只记录、不下单；价格区间和数量计划用于证明"
            "未来执行不能只看一个信号价。",
            "",
            "| 标的 | 评估次数/末次时间 | 可接受卖出价 | 可卖/数量计划 | 当前阻断 |",
            "|---|---|---|---|---|",
        )
    )
    for symbol, values in sorted(by_symbol.items()):
        latest = values[-1]
        acceptance = latest.price_acceptance
        quantity_text = (
            f"可卖 {_optional_number(latest.available_to_sell)} 股；"
            f"`{latest.quantity_plan_status or '不可得'}`；"
            f"{_sell_quantity_rule_text(latest.quantity_rule)}"
        )
        rows.append(
            f"| {symbol} | {len(values)} / {_local_time(latest.known_at)} | "
            f"状态 `{acceptance.status}`<br>{_price_range(acceptance)}<br>"
            f"最低卖价 {_price(acceptance.limit_price)}<br>"
            f"日价格带 {_daily_band(acceptance)} | {quantity_text} | "
            f"{_explained_codes(latest.current_blockers)} |"
        )
    future_codes = tuple(
        dict.fromkeys(
            code
            for item in projection.sell_execution_acceptances
            for code in item.future_non_execution_conditions
        )
    )
    if future_codes:
        rows.extend(("", "未来真实模拟卖出仍必须拒绝以下不可成交情形：", ""))
        rows.extend(
            f"- `{code}`：{_stable_reason_explanation(code)}。" for code in future_codes
        )
    rows.append("")
    return rows


def _stable_rejection_lines(projection: PaperDayExecutiveProjection) -> list[str]:
    rows = ["### 本日实际观察到的稳定拒绝/阻断码", ""]
    if not projection.stable_rejection_reasons:
        rows.extend(("本日 sidecar 未观察到稳定拒绝或当前阻断码。", ""))
        return rows
    rows.extend(
        (
            "大写英文是供日志检索、统计和自动化判断使用的稳定机器码；"
            "中文解释用于人读，两者在下表一一对应。",
            "",
            "| 类别 | 稳定码 | 次数 | 中文解释 |",
            "|---|---|---:|---|",
        )
    )
    rows.extend(
        f"| {item.category} | `{item.code}` | {item.count} | {item.explanation} |"
        for item in projection.stable_rejection_reasons
    )
    rows.append("")
    return rows


def _position_limit_display(value: int | None, enabled: bool | None) -> str:
    if enabled is False:
        return "无固定上限（计数熔断关闭）"
    if enabled is True:
        return f"{_optional_number(value)} 个"
    return UNAVAILABLE


def _short_hash(value: str | None) -> str:
    from .paper_day_formatting import short_hash

    return short_hash(value)


def _percent(value: Decimal | None) -> str:
    from .paper_day_formatting import percent

    return percent(value)


def _price_range(value: PaperDayPriceAcceptanceProjection) -> str:
    if value.acceptable_lower is None or value.acceptable_upper is None:
        return UNAVAILABLE
    return f"[{value.acceptable_lower:.3f}, {value.acceptable_upper:.3f}]"


def _daily_band(value: PaperDayPriceAcceptanceProjection) -> str:
    if value.exchange_lower is None or value.exchange_upper is None:
        return UNAVAILABLE
    return f"[{value.exchange_lower:.3f}, {value.exchange_upper:.3f}]"


def _board_display(value: str | None) -> str:
    labels = {
        "SSE_MAIN": "SSE_MAIN（沪市主板）",
        "SZSE_MAIN": "SZSE_MAIN（深市主板）",
        "CHINEXT": "CHINEXT（创业板）",
        "STAR": "STAR（科创板）",
        "BSE": "BSE（北交所）",
    }
    return UNAVAILABLE if value is None else labels.get(value, value)


def _board_sort_key(value: str | None) -> tuple[int, str]:
    order = {"SSE_MAIN": 0, "SZSE_MAIN": 1, "CHINEXT": 2, "STAR": 3, "BSE": 4}
    resolved = "" if value is None else value
    return order.get(resolved, 99), resolved


def _buy_quantity_rule_text(value: PaperDayQuantityRuleProjection | None) -> str:
    if value is None:
        return UNAVAILABLE
    minimum = _optional_number(value.minimum_buy_quantity)
    increment = _optional_number(value.buy_increment)
    return f"至少 {minimum} 股，其后按 {increment} 股递增"


def _sell_quantity_rule_text(value: PaperDayQuantityRuleProjection | None) -> str:
    if value is None:
        return UNAVAILABLE
    minimum = _optional_number(value.minimum_regular_sell_quantity)
    increment = _optional_number(value.sell_increment)
    residual = {
        "BELOW_100_SELL_ALL_ONCE": "不足 100 股余股须一次性全卖",
        "BELOW_200_SELL_ALL_ONCE": "不足 200 股余股须一次性全卖",
    }.get(
        value.sell_residual_policy or "",
        value.sell_residual_policy or UNAVAILABLE,
    )
    return f"常规至少 {minimum} 股，按 {increment} 股递增；{residual}"


def _pairs(values: tuple[tuple[str, int], ...]) -> str:
    from .paper_day_formatting import pairs

    return pairs(values)


def _readable_code(value: str) -> str:
    from .paper_day_formatting import readable_code

    return readable_code(value)


def _optional_number(value: int | None) -> str:
    from .paper_day_formatting import optional_number

    return optional_number(value)


def _money(value: Decimal | None) -> str:
    from .paper_day_formatting import money

    return money(value)


def _price(value: Decimal | None) -> str:
    from .paper_day_formatting import price

    return price(value)


def _local_time(value: datetime, *, with_date: bool = False) -> str:
    from .paper_day_formatting import local_time

    return local_time(value, with_date=with_date)


def _time_or_unavailable(value: datetime | None) -> str:
    from .paper_day_formatting import time_or_unavailable

    return time_or_unavailable(value)


def _universe_range(projection: PaperDayExecutiveProjection) -> str:
    values = (
        projection.scan_universe_min,
        projection.scan_universe_max,
        projection.scan_universe_last,
    )
    if any(value is None for value in values):
        return UNAVAILABLE
    return "/".join(str(cast(int, value)) for value in values)


def _watchlist_counts(projection: PaperDayExecutiveProjection) -> str:
    values = (
        projection.watchlist_initial_count,
        projection.watchlist_peak_count,
        projection.watchlist_current_count,
    )
    if any(value is None for value in values):
        return UNAVAILABLE
    return "/".join(str(cast(int, value)) for value in values)


def _fee_total(projection: PaperDayExecutiveProjection) -> Decimal | None:
    values = (projection.commission, projection.transfer_fee, projection.stamp_tax)
    if any(value is None for value in values):
        return None
    return sum((cast(Decimal, value) for value in values), Decimal("0"))


def _three_counts(first: int | None, second: int | None, third: int | None) -> str:
    if first is None or second is None or third is None:
        return UNAVAILABLE
    return f"{first}/{second}/{third}"


def _artifact_lines(projection: PaperDayExecutiveProjection) -> list[str]:
    summary_name = (
        f"ashare-paper-day-summary-{projection.session_date.isoformat()}-"
        f"{projection.run_id[-10:]}.md"
    )
    paths: list[tuple[str, Path, str]] = [
        ("本增强报告", projection.session_root / "reports" / summary_name, summary_name),
        ("状态", projection.status_path, "../status.json"),
        ("事件 sidecar", projection.event_log_path, "../session.log.jsonl"),
    ]
    if projection.stdout_path is not None:
        paths.append(("最终 CLI 输出", projection.stdout_path, "../runner.stdout.log"))
    if projection.original_report_path is not None:
        paths.append(
            (
                "原始运行报告",
                projection.original_report_path,
                projection.original_report_path.name,
            )
        )
    rows = ["| 文件 | 链接 | 绝对路径 |", "|---|---|---|"]
    rows.extend(
        f"| {label} | [{path.name}]({link}) | `{path}` |"
        for label, path, link in paths
    )
    return rows


__all__ = [
    "PaperDayBuyExecutionProjection",
    "PaperDayExecutiveProjection",
    "PaperDayLLMProjection",
    "PaperDayNotificationProjection",
    "PaperDayPositionProjection",
    "PaperDayPriceAcceptanceProjection",
    "PaperDayQuantityRuleProjection",
    "PaperDayRiskPolicyChangeProjection",
    "PaperDayRiskPolicyProjection",
    "PaperDaySellExecutionProjection",
    "PaperDaySidecarError",
    "PaperDaySidecarEvent",
    "PaperDaySourceTransition",
    "PaperDayStableReasonProjection",
    "PaperDayWatchlistChange",
    "project_paper_day_sidecars",
    "render_paper_day_summary",
    "write_paper_day_summary",
]
