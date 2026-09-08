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
    """兼容门面；实际渲染位于 ``paper_day_renderer``。"""

    from .paper_day_renderer import render_paper_day_summary as render

    return render(projection)


_RENDERER_COMPAT_NAMES = frozenset({
    "_explained_codes",
    "_llm_audit_lines",
    "_llm_score_text",
    "_deep_selected_system_text",
    "_tail_identifier",
    "_safe_table_text",
    "_risk_policy_audit_lines",
    "_buy_execution_audit_lines",
    "_sell_execution_audit_lines",
    "_stable_rejection_lines",
    "_short_hash",
    "_position_limit_display",
    "_percent",
    "_price_range",
    "_daily_band",
    "_board_display",
    "_board_sort_key",
    "_buy_quantity_rule_text",
    "_sell_quantity_rule_text",
    "_pairs",
    "_readable_code",
    "_optional_number",
    "_money",
    "_price",
    "_local_time",
    "_time_or_unavailable",
    "_universe_range",
    "_watchlist_counts",
    "_fee_total",
    "_three_counts",
    "_artifact_lines",
})


def __getattr__(name: str) -> object:
    """按需转发历史私有渲染辅助名称。"""

    if name in _RENDERER_COMPAT_NAMES:
        from . import paper_day_renderer

        return getattr(paper_day_renderer, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")




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






















def _position_limit_display(value: int | None, enabled: bool | None) -> str:
    if enabled is False:
        return "无固定上限（计数熔断关闭）"
    if enabled is True:
        return f"{value if value is not None else UNAVAILABLE} 个"
    return UNAVAILABLE










































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
