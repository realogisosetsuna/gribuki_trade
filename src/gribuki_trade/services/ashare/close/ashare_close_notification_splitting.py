"""A 股盘后通知的纯文本分段与双轨摘要辅助。

本模块不访问网络、存储或通知通道，只负责稳定报告契约的分段、双轨摘要
和长度边界计算。盘后通知 facade 继续通过兼容别名导出这些辅助函数。
"""

from __future__ import annotations

from gribuki_trade.analysis.schemas import MacroAnalysis
from gribuki_trade.domain.recommendations import ResearchRecommendation
from gribuki_trade.features.close_analysis import CloseTechnicalAssessment
from gribuki_trade.reporting.contracts import (
    ReportKind,
    humanize_internal_code,
    render_stable_text_report,
)
from gribuki_trade.services.ashare.close.ashare_close_projection import (
    _DECISION_ZH,
    _display_decimal,
    _display_percent,
    _display_score,
    _format_evidence_citations,
    _humanize_model_text,
    _track_evidence_coverage,
)


def _split_contractual_instrument_report(
    text: str,
    *,
    max_characters: int,
    recommendation: ResearchRecommendation,
    assessment: CloseTechnicalAssessment,
    dual_track: bool,
    selected_track: str | None,
) -> tuple[str, ...]:
    """超长深研拆分后，每一条 QQ 消息仍独立满足五节报告契约。"""

    if len(text) <= max_characters:
        return (text,)

    def envelope(detail: str, *, index: int, total: int) -> str:
        return render_stable_text_report(
            ReportKind.INSTRUMENT_RESEARCH,
            title="A股｜收盘研究分析",
            sections={
                "结论": (
                    f"第 {index}/{total} 部分；{recommendation.symbol}；"
                    f"{_DECISION_ZH[recommendation.decision]}。"
                ),
                "技术结构": (
                    f"技术评分 {_display_score(recommendation.technical_score)}；"
                    f"样本 {assessment.trading_sessions_used} 个交易日。"
                ),
                "基本面与宏观": "本部分延续同一报告的冻结证据；不得与其他运行混用。",
                "对抗观点": (
                    f"双轨结果{'已完整保留' if dual_track else '未完整取得'}；"
                    f"生产采用 {selected_track or '未记录'}。"
                ),
                "失效条件": (
                    f"结构失效参考 {_display_decimal(recommendation.invalidation_price)}；"
                    "本报告不授权订单。"
                ),
                "本部分明细": detail,
            },
        )

    # 使用四位分片序号估算最坏包络，避免总数位数增长后越过 QQ 上限。
    overhead = len(envelope("X", index=9999, total=9999)) - 1
    payload_limit = max_characters - overhead
    if payload_limit < 80:
        raise ValueError("notification limit is too small for report contract")
    payloads = _split_plain_text_payload(text, payload_limit)
    total = len(payloads)
    rendered = tuple(
        envelope(payload, index=index, total=total)
        for index, payload in enumerate(payloads, start=1)
    )
    if any(len(item) > max_characters for item in rendered):
        raise RuntimeError("contractual report splitter exceeded notification limit")
    return rendered

def _split_plain_text_payload(text: str, limit: int) -> tuple[str, ...]:
    """按行优先、必要时按字符拆分，且不丢失长报告正文。"""

    pieces: list[str] = []
    current = ""
    for line in text.splitlines():
        candidate = line if not current else f"{current}\n{line}"
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            pieces.append(current)
            current = ""
        remaining = line
        while len(remaining) > limit:
            pieces.append(remaining[:limit])
            remaining = remaining[limit:]
        current = remaining
    if current:
        pieces.append(current)
    return tuple(item for item in pieces if item.strip())

def _dual_track_report_lines(
    baseline: MacroAnalysis,
    adversarial: MacroAnalysis,
    *,
    selected_track: str | None,
    audit_record_sha256: str | None,
    evidence_numbers: dict[str, int],
) -> tuple[str, ...]:
    """同时展示两条模型轨道，避免只报告最终采用分支。"""

    lines = [
        "",
        "LLM 双轨对照（两者使用同一份冻结证据）：",
        f"生产采用轨道：{humanize_internal_code(selected_track)}；"
        f"审计记录摘要：{audit_record_sha256 or '未配置独立审计存储'}。",
    ]
    for label, analysis in (
        ("原单分析器", baseline),
        ("结构化对抗分析器", adversarial),
    ):
        lines.append(
            f"- {label}：结论 {humanize_internal_code(analysis.decision.value)}；宏观倾向 "
            f"{_display_decimal(analysis.macro_impact)}；技术一致性 "
            f"{_display_decimal(analysis.technical_alignment)}；证据覆盖 "
            f"{_track_evidence_coverage(analysis, set(evidence_numbers))}；环境判断："
            f"{_humanize_model_text(analysis.regime, evidence_numbers)}；模型："
            f"{analysis.model_version}。"
        )
        for claim in analysis.claims[:4]:
            lines.append(
                f"  - 论据：{_humanize_model_text(claim.text, evidence_numbers)} "
                f"{_format_evidence_citations(claim.evidence_ids, evidence_numbers)}"
            )
        if analysis.scenarios:
            lines.append(
                "  - 情景："
                + "；".join(
                    f"{_humanize_model_text(item.name, evidence_numbers)} "
                    f"{_display_percent(item.probability)}"
                    for item in analysis.scenarios[:3]
                )
            )
        if analysis.invalidation_conditions:
            lines.append(
                "  - 失效条件："
                + "；".join(
                    _humanize_model_text(item, evidence_numbers)
                    for item in analysis.invalidation_conditions[:4]
                )
            )
        gaps = tuple(dict.fromkeys((*analysis.uncertainties, *analysis.data_gaps)))
        if gaps:
            lines.append(
                "  - 不确定性/缺口："
                + "；".join(
                    _humanize_model_text(item, evidence_numbers)
                    for item in gaps[:4]
                )
            )
    return tuple(lines)

def _split_notification_text(text: str, max_characters: int) -> tuple[str, ...]:
    title = "【A股｜收盘研究分析】"
    section_prefixes = (
        "一、",
        "二、",
        "三、",
        "四、",
        "五、",
        "六、",
        "七、",
        "八、",
        "九、",
        "十、",
        "跨市场历史关系",
    )
    if len(text) <= max_characters:
        return (text,)
    lines = text.splitlines()
    if lines and lines[0] == title:
        lines = lines[1:]
        if lines and not lines[0]:
            lines = lines[1:]
    continuation_prefix = f"{title}\n"
    chunks: list[str] = []
    current = title
    for line in lines:
        if (
            line.startswith(section_prefixes)
            and current != title
            and len(current) >= max_characters * 4 // 5
        ):
            chunks.append(current)
            current = continuation_prefix + line
            continue
        candidate = f"{current}\n{line}"
        if len(candidate) <= max_characters:
            current = candidate
            continue
        if current != title:
            chunks.append(current)
            current = continuation_prefix + line
        else:
            # 正常情况下报告行不会如此长；若上游标题或 URL 超出边界，仍以确定性方式保留内容。
            available = max_characters - len(continuation_prefix)
            for start in range(0, len(line), available):
                part = line[start : start + available]
                if start + available < len(line):
                    chunks.append(continuation_prefix + part)
                else:
                    current = continuation_prefix + part
        if len(current) > max_characters:
            raise ValueError("notification split produced an oversized part")
    if current != title:
        chunks.append(current)
    if not chunks or "\n".join(chunks).count(title) != len(chunks):
        raise ValueError("notification split failed to preserve report title")
    return tuple(chunks)
