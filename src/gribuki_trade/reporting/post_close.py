"""A 股盘后复盘的确定性 Markdown 渲染器。"""

from __future__ import annotations

import os
import tempfile
from decimal import Decimal
from pathlib import Path

from gribuki_trade.domain.paper_trading import PaperAccountSnapshot
from gribuki_trade.domain.post_close import (
    PostCloseInstrumentResearch,
    PostCloseResearchStatus,
    PostCloseReview,
)
from gribuki_trade.reporting.contracts import (
    ReportKind,
    humanize_codes,
    humanize_internal_code,
    render_stable_markdown_report,
    report_contract,
    validate_markdown_report_contract,
)
from gribuki_trade.reporting.paper_day_summary import PaperDayExecutiveProjection


def render_post_close_review(
    review: PostCloseReview,
    *,
    paper_day: PaperDayExecutiveProjection,
    account: PaperAccountSnapshot,
) -> str:
    """渲染市场、操作、研究与下一交易日知识区段。"""

    completed_research = sum(
        item.status is PostCloseResearchStatus.COMPLETED for item in review.research
    )
    failed_research = sum(
        item.status is PostCloseResearchStatus.FAILED for item in review.research
    )
    research_outcome = (
        "NOT_APPLICABLE"
        if not review.held_symbols
        else "FAILED"
        if completed_research == 0
        else "PARTIAL"
        if failed_research
        else "COMPLETE"
    )
    rows = [
        f"# A股盘后复盘与下一交易日知识基线｜{review.session_date.isoformat()}",
        "",
        f"> 报告类型：{report_contract(ReportKind.DAILY_REVIEW).chinese_name}",
        (
            "> 本报告只读 PAPER sidecar 与经哈希链校验的本地账本；"
            "核心分析不会提交真实委托且不持有 notifier；本 Markdown 可由外层"
            "显式授权的 post-close delivery 交付。"
        ),
        "",
        "## 执行摘要",
        "",
        "| 项目 | 值 |",
        "|---|---|",
        f"| 当前交易日 | {review.session_date.isoformat()}（真实交易日历已验证，15:05 后） |",
        f"| 下一交易日 | {review.next_session.isoformat()} |",
        f"| PAPER run | `{review.paper_run_id}` |",
        "| PAPER 生命周期/覆盖 | "
        f"{_human(review.paper_lifecycle)} / {_human(review.paper_coverage)} |",
        "| 执行/交付模式 | "
        f"{_human(review.execution_mode)} / {_human(review.delivery_mode)} |",
        "",
        "## 市场复盘",
        "",
        "| 指标 | 结果 |",
        "|---|---:|",
        f"| 全市场扫描次数 | {paper_day.scan_count} |",
        f"| 监控标的数 | {paper_day.monitored_symbols} |",
        (
            f"| 买入信号（批准/拒绝） | {paper_day.buy_signal_count}"
            f"（{paper_day.buy_approved_count}/{paper_day.buy_rejected_count}） |"
        ),
        (
            f"| 卖出信号/因 T+1 未提交 | {paper_day.sell_signal_count}/"
            f"{paper_day.sell_not_submitted_count} |"
        ),
        (
            f"| 数据源降级/恢复 | {paper_day.source_degradation_count}/"
            f"{paper_day.source_recovery_count} |"
        ),
        "",
        "## 操作复盘",
        "",
        "| 指标 | 结果 |",
        "|---|---:|",
        f"| 账户 | `{review.account_id}` |",
        f"| 账本最新序号 | {review.last_ledger_sequence} |",
        f"| 期末现金 | {_decimal(account.cash)} |",
        (
            f"| 委托提交/成交/过期 | {paper_day.orders_submitted}/"
            f"{paper_day.orders_filled}/{paper_day.orders_expired} |"
        ),
        f"| 成交笔数/股数 | {paper_day.fills_applied}/{paper_day.filled_shares} |",
        f"| 总费用 | {_optional_decimal(_total_fees(paper_day))} |",
        (
            "| 期末权益/当日盯市盈亏 | "
            f"{_optional_decimal(paper_day.final_equity)}/"
            f"{_optional_decimal(paper_day.total_mark_to_market_pnl)} |"
        ),
        "",
        "### 经账本验证的持仓",
        "",
        "| 标的 | 数量 | 可卖 | 今日买入 | 平均成本 | 已实现盈亏 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    positive_positions = tuple(item for item in account.positions if item.quantity > 0)
    if positive_positions:
        rows.extend(
            f"| {item.symbol} | {item.quantity} | {item.available_to_sell} | "
            f"{item.today_buy} | {_decimal(item.average_cost)} | {_decimal(item.realized_pnl)} |"
            for item in positive_positions
        )
    else:
        rows.append("| — | 0 | 0 | 0 | — | — |")

    buy_reject_reasons = tuple(getattr(paper_day, "buy_reject_reasons", ()))
    rows.extend(
        [
            "",
            "### PAPER 门禁摘要",
            "",
            "| 门禁 | 审计结果 |",
            "|---|---|",
            (
                "| 买入规则/风控 | "
                f"批准 {paper_day.buy_approved_count}；拒绝 {paper_day.buy_rejected_count}；"
                f"拒绝原因 {_reason_pairs(buy_reject_reasons)} |"
            ),
        ]
    )
    intraday_llm = getattr(paper_day, "llm", None)
    if intraday_llm is not None:
        requested_models = ", ".join(intraday_llm.requested_models) or "无"
        response_models = ", ".join(intraday_llm.response_models) or "无"
        rows.append(
            "| 盘中 LLM 买入门禁 | "
            f"是否启用={'是' if intraday_llm.enabled else '否'}；盘前状态="
            f"{_human(intraday_llm.preopen_status)}；"
            f"复核完成/失败={intraday_llm.reviews_completed}/"
            f"{intraday_llm.reviews_failed}；门禁评估/阻断="
            f"{intraday_llm.gate_evaluations}/{intraday_llm.gate_blocked}；"
            f"动作 {_reason_pairs(intraday_llm.gate_action_counts)}；"
            f"请求/响应模型={_markdown_cell(requested_models)}/"
            f"{_markdown_cell(response_models)} |"
        )

    buy_acceptances = tuple(getattr(paper_day, "buy_execution_acceptances", ()))
    if buy_acceptances:
        rows.extend(
            [
                "",
                "### PAPER 已提交买单的逐单边界",
                "",
                "| 序号 | 标的 | 订单 | 数量 | 参考/限价 | 可接受区间 | 失效位 | 板块/数量规则 |",
                "|---:|---|---|---:|---|---|---:|---|",
            ]
        )
        for item in buy_acceptances:
            acceptance = item.price_acceptance
            quantity_rule = item.quantity_rule
            price_range = (
                f"{_optional_decimal(acceptance.acceptable_lower)}～"
                f"{_optional_decimal(acceptance.acceptable_upper)}"
            )
            quantity_rule_text = (
                "不可得"
                if quantity_rule is None
                else (
                    f"{_human(quantity_rule.board or 'UNKNOWN')}；最低数量="
                    f"{_optional_integer(quantity_rule.minimum_buy_quantity)}；递增单位="
                    f"{_optional_integer(quantity_rule.buy_increment)}"
                )
            )
            rows.append(
                f"| {item.sequence} | {item.symbol} | "
                f"`{_markdown_cell(item.order_id or '不可得')}` | "
                f"{_optional_integer(item.quantity)} | "
                f"{_optional_decimal(acceptance.reference_price)}/"
                f"{_optional_decimal(acceptance.limit_price)} | {price_range} | "
                f"{_optional_decimal(acceptance.invalidation_boundary)} | "
                f"{_markdown_cell(quantity_rule_text)} |"
            )

    rows.extend(
        [
            "",
            "## 持仓深研",
            "",
            (
                f"> 深研结果：{_human(research_outcome)}；完成 {completed_research}，"
                f"失败 {failed_research}。"
            ),
            "",
            "| 标的 | 状态 | 结论 | 技术分 | 参考价 | 失效价 | 原因/不确定性 |",
            "|---|---|---|---:|---:|---:|---|",
        ]
    )
    if review.research:
        for item in review.research:
            if item.status is PostCloseResearchStatus.FAILED:
                rows.append(
                    f"| {item.symbol} | {_human(str(item.status))} | — | — | — | — | "
                    f"{_human(item.failure_code or 'FAILED')} |"
                )
                continue
            reasons = humanize_codes((*item.reason_codes, *item.uncertainties))
            rows.append(
                f"| {item.symbol} | {_human(str(item.status))} | {_human(item.decision)} | "
                f"{_optional_decimal(item.technical_score)} | "
                f"{_optional_decimal(item.reference_price)} | "
                f"{_optional_decimal(item.invalidation_price)} | {reasons} |"
            )
    else:
        rows.append("| — | 不适用 | 无持仓 | — | — | — | — |")

    if review.research:
        rows.extend(
            [
                "",
                "### 盘后 LLM 与数据覆盖",
                "",
                (
                    "| 标的 | 技术结论/分 | 最终结论/融合分 | "
                    "LLM 供应商/模型 | 宏观覆盖/分 | LLM/市场失败说明 | 日线数 |"
                ),
                "|---|---|---|---|---|---|---:|",
            ]
        )
        for item in review.research:
            failure_codes = humanize_codes(tuple(
                value
                for value in (
                    item.failure_code,
                    item.macro_failure_code,
                    item.market_data_failure_code,
                )
                if value is not None
            ))
            provider_model = (
                f"{item.macro_provider or '—'}/{item.macro_model or '—'}"
            )
            rows.append(
                f"| {item.symbol} | {_human(item.technical_decision)}/"
                f"{_optional_decimal(item.technical_score)} | "
                f"{_human(item.decision)}/{_optional_decimal(item.combined_score)} | "
                f"{_markdown_cell(provider_model)} | "
                f"{_optional_decimal(item.macro_evidence_coverage)}/"
                f"{_optional_decimal(item.macro_score)} | "
                f"{_markdown_cell(failure_codes)} | "
                f"{_optional_integer(item.daily_bar_count)} |"
            )
        rows.extend(
            [
                "",
                "### 双轨 LLM 结论与审计",
                "",
                (
                    "| 标的 | 原单分析器结论 | 评分 | 证据覆盖 | "
                    "结构化对抗结论 | 评分 | 证据覆盖 | 采用轨道 | 审计定位 |"
                ),
                "|---|---|---:|---:|---|---:|---:|---|---|",
            ]
        )
        for item in review.research:
            baseline_conclusion = _track_conclusion(
                item.baseline_macro_decision,
                item.baseline_macro_regime,
                item.baseline_macro_model,
            )
            adversarial_conclusion = _track_conclusion(
                item.adversarial_macro_decision,
                item.adversarial_macro_regime,
                item.adversarial_macro_model,
            )
            rows.append(
                f"| {item.symbol} | {baseline_conclusion} | "
                f"{_optional_decimal(item.baseline_macro_score)} | "
                f"{_optional_decimal(item.baseline_macro_evidence_coverage)} | "
                f"{adversarial_conclusion} | "
                f"{_optional_decimal(item.adversarial_macro_score)} | "
                f"{_optional_decimal(item.adversarial_macro_evidence_coverage)} | "
                f"{_human(item.macro_selected_track)} | "
                f"{_audit_locator(item)} |"
            )
        rows.extend(
            [
                "",
                "> 双轨“评分”是宏观影响值（-1 至 1），不是收益率、胜率或成交授权；"
                "证据覆盖按各轨道实际引用的冻结证据独立计算。",
            ]
        )

    rows.extend(
        [
            "",
            "## 次日基线",
            "",
            f"- 适用交易日：{review.next_session.isoformat()}。",
            f"- 延续持仓：{', '.join(review.held_symbols) or '无'}。",
            f"- 活跃候选：{', '.join(item.symbol for item in review.candidates) or '无/不可得'}。",
            (
                "- 研究结论仅供下一交易日开盘前复核；任何新信息、停牌、涨跌停"
                "或流动性变化都应使旧结论重新评估。"
            ),
            "- 本基线不构成订单、仓位授权或真实券商指令。",
        ]
    )
    if review.candidates:
        rows.extend(
            [
                "",
                "### 活跃候选明细",
                "",
                "| 标的 | 状态 | 优先级 | 原因 |",
                "|---|---|---:|---|",
                *(
                    f"| {item.symbol} | {_human(item.status)} | {item.priority} | "
                    f"{humanize_codes(item.reason_codes)} |"
                    for item in review.candidates
                ),
            ]
        )
    if review.warnings or paper_day.warnings:
        rows.extend(["", "## 数据质量提示", ""])
        rows.extend(
            f"- {_human(warning)}"
            for warning in dict.fromkeys((*review.warnings, *paper_day.warnings))
        )

    rows.extend(
        [
            "",
            "## 本地审计来源",
            "",
            f"- PAPER status：`{paper_day.status_path.name}`",
            f"- PAPER event sidecar：`{paper_day.event_log_path.name}`",
            "- PAPER 账户：本报告只记录账户 ID、账本序号和投影值，不复制原始账本事件。",
            "- 独立持仓复核文件："
            + (
                "、".join(
                    "`positions/ashare-position-review-"
                    f"{review.session_date.isoformat()}-{item.symbol}.md`"
                    for item in review.research
                )
                or "无持仓，不生成"
            )
            + "。",
            (
                "- 交付边界：核心分析未持有 NapCat/outbox；外层 post-close delivery "
                "可在目标冻结与显式授权后交付该报告文件。"
            ),
            "",
        ]
    )
    rendered = "\n".join(rows).rstrip() + "\n"
    validate_markdown_report_contract(ReportKind.DAILY_REVIEW, rendered)
    return rendered


def write_post_close_review(
    review: PostCloseReview,
    *,
    paper_day: PaperDayExecutiveProjection,
    account: PaperAccountSnapshot,
) -> Path:
    """原子写入盘后日报，并为每个已复核持仓写入独立契约报告。"""

    report_dir = paper_day.session_root / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    target = report_dir / (
        f"ashare-post-close-{review.session_date.isoformat()}-{review.paper_run_id[-10:]}.md"
    )
    rendered = render_post_close_review(review, paper_day=paper_day, account=account)
    position_reports = tuple(
        (
            report_dir
            / "positions"
            / (
                f"ashare-position-review-{review.session_date.isoformat()}-"
                f"{item.symbol}.md"
            ),
            render_position_review(review, item),
        )
        for item in review.research
    )
    for position_path, position_markdown in position_reports:
        position_path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write_markdown(position_path, position_markdown)
    _atomic_write_markdown(target, rendered)
    return target.resolve()


def render_position_review(
    review: PostCloseReview,
    item: PostCloseInstrumentResearch,
) -> str:
    """把一条持仓研究投影为可单独阅读、可持续替换的持仓复核报告。"""

    if item.symbol not in review.held_symbols:
        raise ValueError("position review symbol is not held in the post-close snapshot")
    completed = item.status is PostCloseResearchStatus.COMPLETED
    decision = humanize_internal_code(item.decision) if item.decision else "未形成结论"
    current_lines = [
        f"- 标的：{item.symbol}",
        f"- 复核状态：{humanize_internal_code(item.status.value)}",
        f"- 当前结论：{decision}",
        f"- 技术评分：{_optional_decimal(item.technical_score)}",
        f"- 综合评分：{_optional_decimal(item.combined_score)}",
        f"- 参考价格：{_optional_decimal(item.reference_price)}",
        f"- 双轨采用：{_human(item.macro_selected_track)}",
    ]
    protection_lines = [
        f"- 当前确定性失效参考：{_optional_decimal(item.invalidation_price)}",
        "- 本报告只复核观察中的持仓，不创建、修改或提交任何券商订单。",
    ]
    if completed:
        protection_lines.append(
            "- 实际止损、止盈与时间门以成交后持久化退出计划的最新有效版本为准；"
            "本投影不会用收盘研究覆盖更严格的硬保护。"
        )
    else:
        protection_lines.append(
            "- 本次深研失败，不能声称已经形成新的 DEEP 保护；既有持久化保护计划继续有效。"
        )

    reasons = humanize_codes(item.reason_codes)
    uncertainties = humanize_codes(item.uncertainties)
    failure = humanize_internal_code(item.failure_code) if item.failure_code else "无"
    evidence_lines = [
        f"- 支持理由：{reasons}",
        f"- 不确定性与反证：{uncertainties}",
        f"- 失败说明：{failure}",
        f"- 日线样本数：{_optional_integer(item.daily_bar_count)}",
        *_position_dual_track_lines(item),
        "- 各轨道评分是宏观影响值（-1 至 1），不是收益率、胜率或成交授权。",
    ]
    next_lines = [
        f"- 下一例行复核交易日：{review.next_session.isoformat()} 收盘后。",
        "- 若价格触及持久化退出计划触发边界、出现停复牌/涨跌停、重大公告、"
        "数据源降级或模型证据失效，应立即提前复核。",
        "- 新交易日必须使用届时可知的新证据；不得把本报告回填为未来已知事实。",
    ]
    return render_stable_markdown_report(
        ReportKind.POSITION_REVIEW,
        title=f"A股持仓持续复核｜{item.symbol}｜{review.session_date.isoformat()}",
        sections={
            "当前结论": "\n".join(current_lines),
            "保护计划": "\n".join(protection_lines),
            "证据与反证": "\n".join(evidence_lines),
            "下一复核条件": "\n".join(next_lines),
        },
    )


def _atomic_write_markdown(target: Path, rendered: str) -> None:
    """在同一目录内落临时文件并替换，避免留下半份用户报告。"""

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
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


def _total_fees(paper_day: PaperDayExecutiveProjection) -> Decimal | None:
    commission = paper_day.commission
    transfer_fee = paper_day.transfer_fee
    stamp_tax = paper_day.stamp_tax
    if commission is None or transfer_fee is None or stamp_tax is None:
        return None
    return commission + transfer_fee + stamp_tax


def _decimal(value: Decimal) -> str:
    return format(value, "f")


def _optional_decimal(value: Decimal | None) -> str:
    return "不可得" if value is None else _decimal(value)


def _optional_integer(value: int | None) -> str:
    return "不可得" if value is None else str(value)


def _track_conclusion(
    decision: str | None,
    regime: str | None,
    model: str | None,
) -> str:
    if decision is None and regime is None and model is None:
        return "历史结果未携带"
    parts = [f"结论 {_human(decision)}"]
    if regime is not None:
        parts.append(f"市场状态 {_markdown_cell(regime)}")
    if model is not None:
        parts.append(f"模型 {_markdown_cell(model)}")
    return _markdown_cell("；".join(parts))


def _audit_locator(item: PostCloseInstrumentResearch) -> str:
    parts = []
    if item.macro_analysis_id is not None:
        parts.append(f"分析ID `{item.macro_analysis_id}`")
    if item.macro_audit_record_sha256 is not None:
        parts.append(f"记录SHA256 `{item.macro_audit_record_sha256}`")
    return _markdown_cell("；".join(parts) or "历史结果未携带")


def _position_dual_track_lines(item: PostCloseInstrumentResearch) -> list[str]:
    has_dual_track = any(
        value is not None
        for value in (
            item.baseline_macro_decision,
            item.baseline_macro_regime,
            item.baseline_macro_score,
            item.baseline_macro_evidence_coverage,
            item.baseline_macro_model,
            item.adversarial_macro_decision,
            item.adversarial_macro_regime,
            item.adversarial_macro_score,
            item.adversarial_macro_evidence_coverage,
            item.adversarial_macro_model,
        )
    )
    if not has_dual_track:
        return [
            "- 双轨结果：历史/降级结果未携带两条轨道的独立投影；本报告明确标记缺口，"
            "不复用最终融合分，也不补造模型内容。",
            f"- 双轨审计定位：{_audit_locator(item)}。",
        ]
    baseline_conclusion = _track_conclusion(
        item.baseline_macro_decision,
        item.baseline_macro_regime,
        item.baseline_macro_model,
    )
    adversarial_conclusion = _track_conclusion(
        item.adversarial_macro_decision,
        item.adversarial_macro_regime,
        item.adversarial_macro_model,
    )
    return [
        "- 原单分析器："
        f"{baseline_conclusion}；"
        f"宏观影响评分 {_optional_decimal(item.baseline_macro_score)}；"
        f"证据覆盖 {_optional_decimal(item.baseline_macro_evidence_coverage)}。",
        "- 结构化对抗分析器："
        f"{adversarial_conclusion}；"
        f"宏观影响评分 {_optional_decimal(item.adversarial_macro_score)}；"
        f"证据覆盖 {_optional_decimal(item.adversarial_macro_evidence_coverage)}。",
        f"- 双轨审计定位：{_audit_locator(item)}。",
    ]


def _pairs(values: tuple[tuple[str, int], ...]) -> str:
    return "，".join(f"{_markdown_cell(key)}={count}" for key, count in values) or "无"


def _reason_pairs(values: tuple[tuple[str, int], ...]) -> str:
    return "，".join(
        f"{_markdown_cell(humanize_internal_code(key))}={count}" for key, count in values
    ) or "无"


def _human(value: str | None) -> str:
    if value is None or not value.strip():
        return "—"
    normalized = value.strip()
    if normalized.upper() == normalized and " " not in normalized:
        return humanize_internal_code(normalized)
    return normalized


def _markdown_cell(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")
