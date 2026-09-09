"""A股 PAPER 日报的纯 Markdown 渲染器。

本模块只接收已经完成的 sidecar 投影并生成确定性的 Markdown，不读取文件、
不访问网络、不开启数据库连接。段落与审计区块位于 ``paper_day_rendering``，
此处保留完整报告的顺序、契约校验和历史辅助名称。
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from gribuki_trade.reporting.contracts import (
    ReportKind,
    report_contract,
    validate_markdown_report_contract,
)

if TYPE_CHECKING:
    from .paper_day_summary import PaperDayExecutiveProjection

from .paper_day_rendering import (
    _artifact_lines,
    _board_display,
    _board_sort_key,
    _buy_execution_audit_lines,
    _buy_quantity_rule_text,
    _daily_band,
    _deep_selected_system_text,
    _explained_codes,
    _fee_total,
    _llm_audit_lines,
    _llm_score_text,
    _local_time,
    _money,
    _optional_number,
    _pairs,
    _percent,
    _position_limit_display,
    _price,
    _price_range,
    _readable_code,
    _risk_policy_audit_lines,
    _safe_table_text,
    _sell_execution_audit_lines,
    _sell_quantity_rule_text,
    _short_hash,
    _stable_rejection_lines,
    _tail_identifier,
    _three_counts,
    _time_or_unavailable,
    _universe_range,
    _watchlist_counts,
)

__all__ = [
    "render_paper_day_summary",
    "_artifact_lines",
    "_board_display",
    "_board_sort_key",
    "_buy_execution_audit_lines",
    "_buy_quantity_rule_text",
    "_daily_band",
    "_deep_selected_system_text",
    "_explained_codes",
    "_fee_total",
    "_llm_audit_lines",
    "_llm_score_text",
    "_local_time",
    "_money",
    "_optional_number",
    "_pairs",
    "_percent",
    "_position_limit_display",
    "_price",
    "_price_range",
    "_readable_code",
    "_risk_policy_audit_lines",
    "_safe_table_text",
    "_sell_execution_audit_lines",
    "_sell_quantity_rule_text",
    "_short_hash",
    "_stable_rejection_lines",
    "_tail_identifier",
    "_three_counts",
    "_time_or_unavailable",
    "_universe_range",
    "_watchlist_counts",
]


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
