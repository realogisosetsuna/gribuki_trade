"""A 股盘后研究通知的稳定报告与分段呈现。

这里接收冻结的推荐、技术评估与宏观结果，只生成通知对象，不发送通知或写入
发件箱。服务负责选择是否发布；本模块负责报告内容、证据索引、幂等键与长度边界。
"""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256

from gribuki_trade.analysis.schemas import MacroAnalysis
from gribuki_trade.domain.recommendations import ResearchRecommendation
from gribuki_trade.features.close_analysis import (
    CloseSignalFamilyStatus,
    CloseTechnicalAssessment,
)
from gribuki_trade.ports.notifier import OutboundNotification
from gribuki_trade.reporting.contracts import (
    ReportKind,
    humanize_codes,
    humanize_internal_code,
    render_stable_text_report,
)
from gribuki_trade.services.ashare.ashare_close_projection import (
    _ASSET_TYPE_ZH,
    _BOARD_ZH,
    _DECISION_ZH,
    _EXCHANGE_ZH,
    _FAMILY_ZH,
    _SIZE_ZH,
    _display_decimal,
    _display_percent,
    _display_score,
    _evidence_destination,
    _evidence_display_title,
    _format_evidence_citations,
    _fusion_summary_lines,
    _horizon_view_line,
    _humanize_model_text,
    _report_evidence,
    _risk_metric_lines,
    _single_line,
    _technical_metric_lines,
    _track_evidence_coverage,
)
from gribuki_trade.services.ashare.ashare_research import ResearchNotificationTarget
from gribuki_trade.services.macro_research import EvidenceSelection


def format_close_analysis_notification(
    recommendation: ResearchRecommendation,
    assessment: CloseTechnicalAssessment,
    macro: MacroAnalysis | None,
    target: ResearchNotificationTarget,
    *,
    calendar_verified: bool,
    ashare_context_report_lines: tuple[str, ...] = (),
    ashare_context_failure_codes: tuple[str, ...] = (),
    ashare_breadth_report_lines: tuple[str, ...] = (),
    ashare_breadth_failure_codes: tuple[str, ...] = (),
    global_risk_report_lines: tuple[str, ...] = (),
    global_risk_failure_codes: tuple[str, ...] = (),
    official_rates_report_lines: tuple[str, ...] = (),
    official_rates_failure_codes: tuple[str, ...] = (),
    ashare_derivatives_report_lines: tuple[str, ...] = (),
    ashare_derivatives_failure_codes: tuple[str, ...] = (),
    cross_market_report_lines: tuple[str, ...] = (),
    cross_market_failure_code: str | None = None,
    cross_market_relation_report_lines: tuple[str, ...] = (),
    cross_market_history_failure_code: str | None = None,
    evidence_selection: EvidenceSelection | None = None,
    baseline_macro: MacroAnalysis | None = None,
    adversarial_macro: MacroAnalysis | None = None,
    selected_track: str | None = None,
    audit_record_sha256: str | None = None,
) -> OutboundNotification:
    metrics = dict(assessment.metrics)
    profile = recommendation.instrument_profile
    report_evidence = _report_evidence(recommendation.evidence, macro)
    evidence_numbers = {
        item.evidence_id: index for index, item in enumerate(report_evidence, start=1)
    }
    lines = ["【A股｜收盘研究分析】", "", "一、标的档案"]
    if profile is None:
        lines.extend((f"代码：{recommendation.symbol}", "证券画像：未配置"))
    else:
        lines.extend(
            (
                f"名称：{profile.name}",
                f"代码：{profile.symbol}",
                f"市场与交易所：{profile.market} / "
                f"{_EXCHANGE_ZH.get(profile.exchange, profile.exchange)}",
                f"资产类型：{_ASSET_TYPE_ZH.get(profile.asset_type, profile.asset_type)}",
                f"板块与规模：{_BOARD_ZH.get(profile.board, profile.board)} / "
                f"{_SIZE_ZH.get(profile.size_tier, profile.size_tier)}",
                f"行业或指数类别：{profile.industry}",
                f"风格标签：{'、'.join(profile.styles)}",
                f"背景与研究定位：{profile.research_role}",
                f"主要风险标签：{'、'.join(profile.risk_tags)}",
                f"画像来源：{profile.source_id}；核验日：{profile.verified_on.isoformat()}",
            )
        )
        lines.extend(f"背景资料：{fact}" for fact in profile.background_facts)
    lines.extend(
        (
            "",
            "二、结论总览",
            f"目标交易日：{assessment.next_session.isoformat()}"
            + ("" if calendar_verified else "（交易日历待复核）"),
            f"研究结论：{_DECISION_ZH[recommendation.decision]}",
            f"技术面评分：{_display_score(recommendation.technical_score)}",
            f"宏观面评分：{_display_score(recommendation.macro_score)}",
            f"综合研究评分：{_display_score(recommendation.combined_score)}"
            "（范围 -1 至 1，尚未校准为概率）",
            *_fusion_summary_lines(recommendation),
            *(_horizon_view_line(view) for view in assessment.horizon_views),
            f"参考收盘价：{_display_decimal(recommendation.reference_price)}",
            f"结构失效参考：{_display_decimal(recommendation.invalidation_price)}",
            f"使用交易日：{assessment.trading_sessions_used}；模型："
            f"{assessment.strategy_version}",
            "",
            "三、多因子技术分析",
            "阅读说明：评分 0.000（中性）表示该因子本期没有方向贡献，"
            "不是计算失败；数据不足时会明确标注\u201c未计入\u201d。",
        )
    )
    for family in assessment.signal_families:
        label = _FAMILY_ZH.get(family.family_id, family.family_id)
        if family.status is CloseSignalFamilyStatus.UNAVAILABLE:
            lines.append(f"- {label}：数据不可用，未计入评分。{family.summary}。")
        elif family.status is CloseSignalFamilyStatus.INACTIVE:
            lines.append(
                f"- {label}：适用条件未激活，本期不产生方向分；"
                f"预设权重 {_display_percent(family.weight)}。{family.summary}。"
            )
        else:
            lines.append(
                f"- {label}：评分 {_display_score(family.score)}；"
                f"权重 {_display_percent(family.weight)}；"
                f"方向贡献 {_display_score(family.contribution)}。"
                f"{family.summary}。"
            )
    lines.extend(("", "四、关键技术指标"))
    lines.extend(_technical_metric_lines(metrics))
    lines.extend(("", "五、波动、回撤与流动性风险"))
    lines.extend(_risk_metric_lines(metrics))
    lines.extend(("", "六、跨市场观测"))
    lines.append("A股资金面、ETF与股指期货上下文：")
    if ashare_context_report_lines:
        lines.extend(ashare_context_report_lines)
    if ashare_context_failure_codes:
        lines.append(
            "A股补充上下文暂不可用："
            + "、".join(ashare_context_failure_codes)
            + "。"
        )
    if not ashare_context_report_lines and not ashare_context_failure_codes:
        lines.append("本次未采集A股补充上下文。")
    if ashare_breadth_report_lines:
        lines.extend(ashare_breadth_report_lines)
    if ashare_breadth_failure_codes:
        lines.append(
            "A股市场宽度暂不可用："
            + "、".join(ashare_breadth_failure_codes)
            + "。"
        )
    if not ashare_breadth_report_lines and not ashare_breadth_failure_codes:
        lines.append("本次未采集A股市场宽度。")
    if global_risk_report_lines:
        lines.extend(global_risk_report_lines)
    if global_risk_failure_codes:
        lines.append(
            "官方全球风险数据暂不可用："
            + "、".join(global_risk_failure_codes)
            + "。"
        )
    if not global_risk_report_lines and not global_risk_failure_codes:
        lines.append("本次未采集官方全球风险数据。")
    if official_rates_report_lines:
        lines.extend(official_rates_report_lines)
    if official_rates_failure_codes:
        lines.append(
            "官方汇率/货币市场定盘暂不可用："
            + "、".join(official_rates_failure_codes)
            + "。"
        )
    if not official_rates_report_lines and not official_rates_failure_codes:
        lines.append("本次未采集SAFE中间价与官方Shibor。")
    if ashare_derivatives_report_lines:
        lines.extend(ashare_derivatives_report_lines)
    if ashare_derivatives_failure_codes:
        lines.append(
            "上交所ETF/期权官方盘后数据部分不可用："
            + "、".join(ashare_derivatives_failure_codes)
            + "。"
        )
    if not ashare_derivatives_report_lines and not ashare_derivatives_failure_codes:
        lines.append("本次未采集上交所ETF份额与期权风险指标。")
    lines.append("跨市场即时观测：")
    if cross_market_report_lines:
        lines.extend(cross_market_report_lines)
    elif cross_market_failure_code is not None:
        lines.append(f"跨市场数据暂不可用：{cross_market_failure_code}。")
    else:
        lines.append("本次未采集跨市场快照。")
    if cross_market_relation_report_lines:
        lines.extend(("", "跨市场历史关系"))
        lines.extend(cross_market_relation_report_lines)
    elif cross_market_history_failure_code is not None:
        lines.append(
            f"跨市场历史关系暂不可用：{cross_market_history_failure_code}。"
        )
    else:
        lines.append("本次未采集跨市场历史关系。")
    if macro is not None:
        lines.extend(("", "七、宏观、新闻与市场传导"))
        if baseline_macro is not None and adversarial_macro is not None:
            lines.extend(
                _dual_track_report_lines(
                    baseline_macro,
                    adversarial_macro,
                    selected_track=selected_track,
                    audit_record_sha256=audit_record_sha256,
                    evidence_numbers=evidence_numbers,
                )
            )
        lines.append(
            f"模型结论：{macro.decision.value}；环境判断："
            f"{_humanize_model_text(macro.regime, evidence_numbers)}；"
            "事件与宏观综合倾向："
            f"{_display_decimal(macro.macro_impact)}；技术一致性："
            f"{_display_decimal(macro.technical_alignment)}；模型自报置信层级："
            f"{_humanize_model_text(macro.reported_confidence, evidence_numbers)}"
            "（未作概率校准）。"
        )
        if evidence_selection is not None:
            lines.append(
                "资讯证据覆盖：采用 "
                f"{len(evidence_selection.items)} 条；未来时点排除 "
                f"{evidence_selection.future_rejected} 条；过期排除 "
                f"{evidence_selection.stale_rejected} 条；不相关排除 "
                f"{evidence_selection.irrelevant_rejected} 条；重复排除 "
                f"{evidence_selection.duplicate_rejected} 条；疑似提示注入排除 "
                f"{evidence_selection.injection_rejected} 条；未获独立印证的公共媒体 "
                f"{evidence_selection.uncorroborated_public_media} 条。"
            )
        for claim in macro.claims[:6]:
            lines.append(
                f"- {_humanize_model_text(claim.text, evidence_numbers)} "
                f"{_format_evidence_citations(claim.evidence_ids, evidence_numbers)}"
            )
            for contradiction in claim.contradictions[:2]:
                lines.append(
                    "  反向或矛盾证据："
                    f"{_humanize_model_text(contradiction, evidence_numbers)}"
                )
        if macro.scenarios:
            lines.extend(("", "八、情景分析"))
            for scenario in macro.scenarios[:3]:
                scenario_drivers = "、".join(
                    _humanize_model_text(item, evidence_numbers)
                    for item in scenario.drivers
                )
                lines.append(
                    f"- {_humanize_model_text(scenario.name, evidence_numbers)}："
                    f"概率 {_display_percent(scenario.probability)}；驱动："
                    f"{scenario_drivers}；"
                    "依据："
                    f"{_format_evidence_citations(scenario.evidence_ids, evidence_numbers)}"
                )
        if macro.invalidation_conditions:
            lines.append(
                "宏观判断失效条件："
                + "；".join(
                    _humanize_model_text(item, evidence_numbers)
                    for item in macro.invalidation_conditions
                )
            )
        macro_gaps = tuple(dict.fromkeys((*macro.uncertainties, *macro.data_gaps)))
        if macro_gaps:
            lines.extend(("", "九、数据缺口与不确定性"))
            lines.extend(
                f"- {_humanize_model_text(item, evidence_numbers)}"
                for item in macro_gaps[:10]
            )
    elif recommendation.uncertainties:
        lines.extend(("", "九、数据缺口与不确定性"))
        lines.extend(
            f"- {_single_line(item)}" for item in recommendation.uncertainties[:10]
        )
    if report_evidence:
        lines.extend(("", "十、证据索引"))
        for index, item in enumerate(report_evidence, start=1):
            lines.append(
                f"- [证据{index}] "
                f"{_evidence_display_title(item, recommendation.symbol)}"
            )
            lines.append(f"  来源：{_evidence_destination(item.canonical_url)}")
    detailed_text = "\n".join(lines)
    text = _contractualize_close_analysis_report(
        detailed_text=detailed_text,
        recommendation=recommendation,
        assessment=assessment,
        macro=macro,
        baseline_macro=baseline_macro,
        adversarial_macro=adversarial_macro,
        selected_track=selected_track,
        audit_record_sha256=audit_record_sha256,
        evidence_selection=evidence_selection,
        evidence_count=len(report_evidence),
    )
    if len(text) > target.max_characters:
        text = _split_contractual_instrument_report(
            text,
            max_characters=target.max_characters,
            recommendation=recommendation,
            assessment=assessment,
            dual_track=(baseline_macro is not None and adversarial_macro is not None),
            selected_track=selected_track,
        )[0]
    destination_hash = sha256(
        f"{target.channel}|{target.target_kind.value}|{target.target_id}".encode()
    ).hexdigest()[:16]
    return OutboundNotification(
        idempotency_key=(
            f"close-analysis:{recommendation.recommendation_id}:"
            f"{target.channel}:{destination_hash}"
        ),
        channel=target.channel,
        target_kind=target.target_kind,
        target_id=target.target_id,
        text=text,
        created_at=recommendation.as_of,
        expires_at=recommendation.expires_at,
    )

def _contractualize_close_analysis_report(
    *,
    detailed_text: str,
    recommendation: ResearchRecommendation,
    assessment: CloseTechnicalAssessment,
    macro: MacroAnalysis | None,
    baseline_macro: MacroAnalysis | None,
    adversarial_macro: MacroAnalysis | None,
    selected_track: str | None,
    audit_record_sha256: str | None,
    evidence_selection: EvidenceSelection | None,
    evidence_count: int,
) -> str:
    """在保留完整研究明细的同时，给长报告加上稳定的五节用户骨架。"""

    conclusion = "\n".join(
        (
            f"标的：{recommendation.symbol}",
            f"适用交易日：{assessment.next_session.isoformat()}",
            f"研究结论：{_DECISION_ZH[recommendation.decision]}",
            f"技术评分：{_display_score(recommendation.technical_score)}；宏观评分："
            f"{_display_score(recommendation.macro_score)}；综合评分："
            f"{_display_score(recommendation.combined_score)}（均不是收益概率）。",
            f"参考收盘价：{_display_decimal(recommendation.reference_price)}",
        )
    )
    technical_lines = [
        f"数据使用 {assessment.trading_sessions_used} 个交易日；策略版本："
        f"{assessment.strategy_version}。",
        *(
            f"- {_FAMILY_ZH.get(family.family_id, family.family_id)}：{family.summary}；"
            f"方向贡献 {_display_score(family.contribution)}。"
            for family in assessment.signal_families
        ),
        f"研究原因：{humanize_codes(recommendation.reason_codes)}。",
    ]
    if macro is None:
        macro_lines = [
            "本次没有可用宏观模型结论，综合结果不得被解释为已经完成宏观复核。",
            f"可追溯证据数量：{evidence_count}。",
        ]
    else:
        coverage = (
            "未单独记录选择统计"
            if evidence_selection is None
            else (
                f"采用 {len(evidence_selection.items)} 条；排除未来 "
                f"{evidence_selection.future_rejected} 条、过期 "
                f"{evidence_selection.stale_rejected} 条、不相关 "
                f"{evidence_selection.irrelevant_rejected} 条、重复 "
                f"{evidence_selection.duplicate_rejected} 条"
            )
        )
        macro_lines = [
            f"最终宏观结论：{humanize_internal_code(macro.decision.value)}；环境判断："
            f"{_single_line(macro.regime)}。",
            f"证据选择：{coverage}；报告证据索引 {evidence_count} 条。",
            *(_single_line(claim.text) for claim in macro.claims[:4]),
        ]
    if baseline_macro is not None and adversarial_macro is not None:
        available_evidence = {item.evidence_id for item in recommendation.evidence}
        adversarial_lines = [
            "两条轨道使用同一份冻结证据，报告同时保留，生产决策优先采用结构化对抗轨道。",
            f"原单分析器：{humanize_internal_code(baseline_macro.decision.value)}；"
            f"宏观倾向 {_display_decimal(baseline_macro.macro_impact)}；"
            f"证据覆盖 {_track_evidence_coverage(baseline_macro, available_evidence)}；"
            f"{_single_line(baseline_macro.regime)}。",
            f"结构化对抗分析器：{humanize_internal_code(adversarial_macro.decision.value)}；"
            f"宏观倾向 {_display_decimal(adversarial_macro.macro_impact)}；"
            f"证据覆盖 {_track_evidence_coverage(adversarial_macro, available_evidence)}；"
            f"{_single_line(adversarial_macro.regime)}。",
            f"生产采用轨道：{humanize_internal_code(selected_track)}；审计摘要："
            f"{audit_record_sha256 or '未配置独立审计记录'}。",
        ]
    else:
        adversarial_lines = [
            "本次未同时取得原单分析器与结构化对抗分析器两条完整结果；"
            "报告如实标记缺口，不把单轨结果伪装成双轨复核。"
        ]
    invalidation_lines = [
        f"结构失效参考：{_display_decimal(recommendation.invalidation_price)}",
        *(
            f"- {_single_line(item)}"
            for item in (
                () if macro is None else macro.invalidation_conditions
            )[:6]
        ),
        *(
            f"- 不确定性：{_single_line(item)}"
            for item in recommendation.uncertainties[:6]
        ),
        "任何新公告、停复牌、价格带、流动性或证据时点变化都要求重新分析；"
        "本报告不直接授权订单。",
    ]
    detail_lines = detailed_text.splitlines()
    if detail_lines and detail_lines[0].startswith("【"):
        detail_lines = detail_lines[1:]
    return render_stable_text_report(
        ReportKind.INSTRUMENT_RESEARCH,
        title="A股｜收盘研究分析",
        sections={
            "结论": conclusion,
            "技术结构": "\n".join(technical_lines),
            "基本面与宏观": "\n".join(macro_lines),
            "对抗观点": "\n".join(adversarial_lines),
            "失效条件": "\n".join(invalidation_lines),
            "完整研究明细": "\n".join(detail_lines).strip() or "无额外明细。",
        },
    )

def format_close_analysis_notifications(
    recommendation: ResearchRecommendation,
    assessment: CloseTechnicalAssessment,
    macro: MacroAnalysis | None,
    target: ResearchNotificationTarget,
    *,
    calendar_verified: bool,
    ashare_context_report_lines: tuple[str, ...] = (),
    ashare_context_failure_codes: tuple[str, ...] = (),
    ashare_breadth_report_lines: tuple[str, ...] = (),
    ashare_breadth_failure_codes: tuple[str, ...] = (),
    global_risk_report_lines: tuple[str, ...] = (),
    global_risk_failure_codes: tuple[str, ...] = (),
    official_rates_report_lines: tuple[str, ...] = (),
    official_rates_failure_codes: tuple[str, ...] = (),
    ashare_derivatives_report_lines: tuple[str, ...] = (),
    ashare_derivatives_failure_codes: tuple[str, ...] = (),
    cross_market_report_lines: tuple[str, ...] = (),
    cross_market_failure_code: str | None = None,
    cross_market_relation_report_lines: tuple[str, ...] = (),
    cross_market_history_failure_code: str | None = None,
    evidence_selection: EvidenceSelection | None = None,
    baseline_macro: MacroAnalysis | None = None,
    adversarial_macro: MacroAnalysis | None = None,
    selected_track: str | None = None,
    audit_record_sha256: str | None = None,
) -> tuple[OutboundNotification, ...]:
    """渲染每一行报告，再拆分为适合 QQ 且可持久化的分段。"""

    unbounded_target = replace(
        target,
        max_characters=max(target.max_characters, 1_000_000),
    )
    base = format_close_analysis_notification(
        recommendation,
        assessment,
        macro,
        unbounded_target,
        calendar_verified=calendar_verified,
        ashare_context_report_lines=ashare_context_report_lines,
        ashare_context_failure_codes=ashare_context_failure_codes,
        ashare_breadth_report_lines=ashare_breadth_report_lines,
        ashare_breadth_failure_codes=ashare_breadth_failure_codes,
        global_risk_report_lines=global_risk_report_lines,
        global_risk_failure_codes=global_risk_failure_codes,
        official_rates_report_lines=official_rates_report_lines,
        official_rates_failure_codes=official_rates_failure_codes,
        ashare_derivatives_report_lines=ashare_derivatives_report_lines,
        ashare_derivatives_failure_codes=ashare_derivatives_failure_codes,
        cross_market_report_lines=cross_market_report_lines,
        cross_market_failure_code=cross_market_failure_code,
        cross_market_relation_report_lines=cross_market_relation_report_lines,
        cross_market_history_failure_code=cross_market_history_failure_code,
        evidence_selection=evidence_selection,
        baseline_macro=baseline_macro,
        adversarial_macro=adversarial_macro,
        selected_track=selected_track,
        audit_record_sha256=audit_record_sha256,
    )
    chunks = _split_contractual_instrument_report(
        base.text,
        max_characters=target.max_characters,
        recommendation=recommendation,
        assessment=assessment,
        dual_track=(baseline_macro is not None and adversarial_macro is not None),
        selected_track=selected_track,
    )
    if len(chunks) == 1:
        return (replace(base, text=chunks[0]),)
    total = len(chunks)
    return tuple(
        replace(
            base,
            idempotency_key=(
                f"{base.idempotency_key}:part-{index:02d}-of-{total:02d}"
            ),
            text=chunk,
        )
        for index, chunk in enumerate(chunks, start=1)
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
