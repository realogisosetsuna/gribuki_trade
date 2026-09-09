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

import os
import tempfile
from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

from . import paper_day_execution_projection as _execution_projection
from . import paper_day_llm_projection as _llm_projection_module
from .paper_day_account_projection import (
    applied_fill_sides,
    artifact_status_from_events,
    latest_account_event,
    notification_projection,
    order_id,
    positions,
    unique_applied_fills,
)
from .paper_day_account_projection import (
    realized_by_symbol as project_realized_by_symbol,
)
from .paper_day_llm_projection import (
    PaperDayDeepExitLLMComparison,
    PaperDayDeepExitSellReview,
    PaperDayLLMProjection,
    PaperDayLLMTrackComparison,
    PaperDayPreopenLLMComparison,
    project_llm_sidecars,
)
from .paper_day_projection_models import (
    PaperDayNotificationProjection,
    PaperDayPositionProjection,
    PaperDaySidecarError,
    PaperDaySidecarEvent,
)
from .paper_day_sidecar_codec import (
    read_final_result as _sidecar_read_final_result,
)
from .paper_day_summary_models import (
    PaperDayBuyExecutionProjection,
    PaperDayExecutiveProjection,
    PaperDayPriceAcceptanceProjection,
    PaperDayQuantityRuleProjection,
    PaperDayRiskPolicyChangeProjection,
    PaperDayRiskPolicyProjection,
    PaperDaySellExecutionProjection,
    PaperDaySourceTransition,
    PaperDayStableReasonProjection,
    PaperDayWatchlistChange,
)

_llm_projection = project_llm_sidecars
_nearest_rank = _llm_projection_module._nearest_rank

UNAVAILABLE = "不可得（sidecar 未记录）"


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
        None if terminal is None else _optional_bool(terminal.payload.get("partial_session"))
    )
    if lifecycle == "COMPLETED" and partial_value is False:
        coverage = "FULL_SESSION"
    elif (
        (lifecycle == "COMPLETED" and partial_value is True)
        or lifecycle == "ABORTED"
        or _has_event(events, "PREOPEN_SCREEN_MISSED")
    ):
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
        _optional_string(event.payload.get("decision")) or "UNKNOWN" for event in technical
    )
    monitor_intervals = Counter(
        _optional_string(event.payload.get("interval")) or "UNKNOWN" for event in technical
    )
    monitored_symbols = len({event.symbol for event in technical if event.symbol is not None})

    buy_signals = tuple(event for event in events if event.event_type == "BUY_SIGNAL_TRIGGERED")
    approved = tuple(event for event in buy_signals if event.payload.get("risk_approved") is True)
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
        order_id for event in expiry_events if (order_id := _order_id(event)) is not None
    }
    expiry_reasons = Counter(
        _optional_string(event.payload.get("reason")) or event.event_type for event in expiry_events
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
        monitor_invalid=sum(event.event_type == "TECHNICAL_SIGNAL_INVALID" for event in events),
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


_RENDERER_COMPAT_NAMES = frozenset(
    {
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
    }
)


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


def _event_known_at(events: Iterable[PaperDaySidecarEvent], event_type: str) -> datetime | None:
    event = _last_event(events, {event_type})
    return None if event is None else event.known_at


def _watchlist_changes(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDayWatchlistChange, ...]:
    return _execution_projection._watchlist_changes(events)


def _string_tuple(value: object) -> tuple[str, ...]:
    return _execution_projection._string_tuple(value)


def _counter_items(counter: Counter[str]) -> tuple[tuple[str, int], ...]:
    return _execution_projection._counter_items(counter)


def _price_acceptance(value: object, *, status: str) -> PaperDayPriceAcceptanceProjection:
    return _execution_projection._price_acceptance(value, status=status)


def _quantity_rule(
    value: object, *, board_override: str | None = None
) -> PaperDayQuantityRuleProjection | None:
    return _execution_projection._quantity_rule(value, board_override=board_override)


def _risk_policy_changes(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDayRiskPolicyChangeProjection, ...]:
    return _execution_projection._risk_policy_changes(events)


def _current_risk_policy(
    events: Iterable[PaperDaySidecarEvent],
) -> PaperDayRiskPolicyProjection | None:
    return _execution_projection._current_risk_policy(events)


def _buy_execution_acceptances(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDayBuyExecutionProjection, ...]:
    return _execution_projection._buy_execution_acceptances(events)


def _sell_execution_acceptances(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDaySellExecutionProjection, ...]:
    return _execution_projection._sell_execution_acceptances(events)


def _int_tuple(value: object) -> tuple[int, ...]:
    return _execution_projection._int_tuple(value)


def _stable_rejection_reasons(
    *,
    buy_reject_reasons: Counter[str],
    expiry_reasons: Counter[str],
    sell_acceptances: tuple[PaperDaySellExecutionProjection, ...],
) -> tuple[PaperDayStableReasonProjection, ...]:
    return _execution_projection._stable_rejection_reasons(
        buy_reject_reasons=buy_reject_reasons,
        expiry_reasons=expiry_reasons,
        sell_acceptances=sell_acceptances,
    )


def _source_transitions(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDaySourceTransition, ...]:
    return _execution_projection._source_transitions(events)


def _source_recovery_count(transitions: tuple[PaperDaySourceTransition, ...]) -> int:
    return _execution_projection._source_recovery_count(transitions)


def _order_id(event: PaperDaySidecarEvent) -> str | None:
    return order_id(event)


def _unique_applied_fills(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDaySidecarEvent, ...]:
    return unique_applied_fills(events)


def _latest_account_event(
    events: Iterable[PaperDaySidecarEvent],
) -> PaperDaySidecarEvent | None:
    return latest_account_event(events)


def _positions(
    value: object,
    realized_by_symbol: dict[str, Decimal] | None,
) -> tuple[PaperDayPositionProjection, ...]:
    return positions(value, realized_by_symbol)


def _realized_by_symbol(
    final_account: dict[str, object] | None,
) -> dict[str, Decimal] | None:
    return project_realized_by_symbol(final_account)


def _applied_fill_sides(
    events: Iterable[PaperDaySidecarEvent],
    applied: tuple[PaperDaySidecarEvent, ...],
) -> tuple[str, ...]:
    return applied_fill_sides(events, applied)


def _notification_projection(
    events: tuple[PaperDaySidecarEvent, ...],
    terminal: PaperDaySidecarEvent | None,
    final_result: dict[str, object] | None,
    status: dict[str, object],
) -> PaperDayNotificationProjection:
    return notification_projection(events, terminal, final_result, status)


def _artifact_status_from_events(events: tuple[PaperDaySidecarEvent, ...]) -> str:
    return artifact_status_from_events(events)


def _read_final_result(root: Path, session_date: date, run_id: str) -> dict[str, object] | None:
    return _sidecar_read_final_result(root, session_date, run_id)


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


_STABLE_REASON_EXPLANATIONS = _execution_projection._STABLE_REASON_EXPLANATIONS


def _stable_reason_explanation(code: str) -> str:
    return _execution_projection._stable_reason_explanation(code)


def _position_limit_display(value: int | None, enabled: bool | None) -> str:
    if enabled is False:
        return "无固定上限（计数熔断关闭）"
    if enabled is True:
        return f"{value if value is not None else UNAVAILABLE} 个"
    return UNAVAILABLE


__all__ = [
    "PaperDayBuyExecutionProjection",
    "PaperDayExecutiveProjection",
    "PaperDayDeepExitLLMComparison",
    "PaperDayDeepExitSellReview",
    "PaperDayLLMProjection",
    "PaperDayLLMTrackComparison",
    "PaperDayPreopenLLMComparison",
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
