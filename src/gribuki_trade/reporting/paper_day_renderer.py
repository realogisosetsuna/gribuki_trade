"""A股 PAPER 日报的纯 Markdown 渲染器。

本模块只接收已经完成的 sidecar 投影并生成确定性的 Markdown，不读取文件、
不访问网络、不开启数据库连接。历史 ``paper_day_summary`` 门面通过惰性
导入继续提供原有公开和私有辅助名称。
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, cast

from gribuki_trade.reporting.contracts import (
    ReportKind,
    report_contract,
    validate_markdown_report_contract,
)

if TYPE_CHECKING:
    from .paper_day_summary import (
        PaperDayExecutiveProjection,
        PaperDayPriceAcceptanceProjection,
        PaperDayQuantityRuleProjection,
        PaperDaySellExecutionProjection,
    )

from .paper_day_summary import UNAVAILABLE, _stable_reason_explanation


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
        return f"{value if value is not None else UNAVAILABLE} 个"
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
