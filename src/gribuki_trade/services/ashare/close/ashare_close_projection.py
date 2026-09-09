"""A 股盘后分析的纯结果投影、通知格式化与共享转换。

本模块不访问市场数据、模型、网络或持久化；它把已冻结的分析结果转换为
技术信号、推荐证据和具备稳定报告契约的通知。
"""

from __future__ import annotations

import re
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from gribuki_trade.analysis.schemas import EvidenceItem, MacroAnalysis
from gribuki_trade.domain.instruments import ResearchInstrumentProfile
from gribuki_trade.domain.recommendations import (
    EvidenceReference,
    RecommendationDecision,
    ResearchRecommendation,
)
from gribuki_trade.features.close_analysis import (
    CloseHorizonView,
    CloseSignalFamily,
    CloseSignalFamilyStatus,
    CloseTechnicalAssessment,
)
from gribuki_trade.features.technical import TechnicalSignal
from gribuki_trade.ports.ashare_breadth import AShareBreadthSnapshot

SHANGHAI = ZoneInfo("Asia/Shanghai")

_DECISION_ZH = {
    RecommendationDecision.ENTER_CANDIDATE: "进入候选",
    RecommendationDecision.WATCH: "观察",
    RecommendationDecision.REDUCE: "降低暴露",
    RecommendationDecision.ABSTAIN: "数据不足，暂不判断",
}
_FAMILY_ZH = {
    "trend_structure": "趋势结构",
    "multi_horizon_momentum": "多周期动量",
    "breakout_compression": "突破与波动压缩",
    "trend_pullback": "顺势回撤位置",
    "volume_liquidity": "量价与流动性",
    "relative_strength_breadth": "相对强弱与市场广度",
}
_HORIZON_ZH = {
    "SHORT_1_TO_5_DAYS": "短线（1至5个交易日）",
    "SWING_2_TO_8_WEEKS": "波段（2至8周）",
}
_ASSET_TYPE_ZH = {"stock": "股票", "etf": "交易型开放式指数基金（ETF）"}
_EXCHANGE_ZH = {"sse": "上海证券交易所", "szse": "深圳证券交易所"}
_BOARD_ZH = {
    "sse_main": "沪市主板",
    "szse_main": "深市主板",
    "chinext": "创业板",
    "star": "科创板",
    "sse_etf": "沪市ETF",
    "szse_etf": "深市ETF",
}
_SIZE_ZH = {
    "mega": "超大盘",
    "large": "大盘",
    "mid": "中盘",
    "small": "小盘",
    "cross_size": "跨规模",
}
_FUSION_REASON_ZH = {
    "COMBINED_SCORE_UNCALIBRATED": "综合分尚未校准为收益概率",
    "MACRO_FUSION_NOT_AVAILABLE": "宏观分析未运行，综合分沿用技术分",
    "MACRO_FUSION_ABSTAINED": "宏观模型弃权，未把宏观分计入综合分",
    "MACRO_FUSION_EVIDENCE_MISMATCH": "宏观引用存在未留存证据，未参与融合",
    "MACRO_FUSION_NO_EVIDENCE_REFERENCES": "宏观结论没有可核验引用，未参与融合",
    "MACRO_FUSION_INSUFFICIENT_COVERAGE": "宏观证据覆盖不足，未参与融合",
    "MACRO_FUSION_WATCH_DISCOUNTED": "宏观结论为观察，宏观权重减半",
    "MACRO_FUSION_APPLIED": "宏观分已按受控权重计入综合分",
    "MACRO_FUSION_POSITIVE": "宏观证据对技术候选形成正向贡献",
    "MACRO_FUSION_NEGATIVE": "宏观证据对技术候选形成负向贡献",
    "MACRO_FUSION_NEUTRAL": "宏观证据本期方向贡献中性",
    "TECHNICAL_ENTRY_GATE_NOT_MET": "技术入场门槛未满足，宏观分不能单独升级入场",
}
def _with_breadth_context(
    assessment: CloseTechnicalAssessment,
    snapshot: AShareBreadthSnapshot,
) -> CloseTechnicalAssessment:
    """附加经审计的市场宽度诊断，且不虚构已校准权重。"""

    metrics: tuple[tuple[str, Decimal], ...] = (
        ("breadth_eligible_count", Decimal(snapshot.eligible_count)),
        (
            "breadth_equal_weight_mean_change_percent",
            snapshot.equal_weight_mean_change_percent,
        ),
        ("breadth_median_change_percent", snapshot.median_change_percent),
    )
    if snapshot.advance_decline_ratio is not None:
        metrics = (
            *metrics,
            ("breadth_advance_decline_ratio", snapshot.advance_decline_ratio),
        )
    if snapshot.advancing_amount_share_percent is not None:
        metrics = (
            *metrics,
            (
                "breadth_advancing_amount_share_percent",
                snapshot.advancing_amount_share_percent,
            ),
        )
    replacement_family = CloseSignalFamily(
        family_id="relative_strength_breadth",
        score=Decimal(0),
        weight=Decimal(0),
        contribution=Decimal(0),
        summary=(
            f"已取得{snapshot.eligible_count}家沪深京A股有效样本；"
            f"上涨{snapshot.advancing_count}家、下跌{snapshot.declining_count}家、"
            f"平盘{snapshot.flat_count}家；涨跌家数比"
            f"{_display_decimal(snapshot.advance_decline_ratio)}；"
            "因尚无样本外校准，本期仅展示且不改变方向总分"
        ),
        metrics=tuple(name for name, _value in metrics),
        status=CloseSignalFamilyStatus.NEUTRAL,
    )
    families = tuple(
        replacement_family if family.family_id == "relative_strength_breadth" else family
        for family in assessment.signal_families
    )
    return replace(
        assessment,
        reason_codes=tuple(
            dict.fromkeys(
                (*assessment.reason_codes, "BREADTH_CONTEXT_OBSERVED_NOT_SCORED")
            )
        ),
        metrics=(*assessment.metrics, *metrics),
        signal_families=families,
    )














def _track_evidence_coverage(
    analysis: MacroAnalysis,
    available_evidence_ids: set[str],
) -> str:
    """显示单条轨道实际引用的冻结证据数；两轨不得共用最终融合覆盖率。"""

    referenced = {
        evidence_id
        for claim in analysis.claims
        for evidence_id in claim.evidence_ids
    }
    referenced.update(
        evidence_id
        for scenario in analysis.scenarios
        for evidence_id in scenario.evidence_ids
    )
    retained = referenced & available_evidence_ids
    if not available_evidence_ids:
        return f"0/0（{_display_percent(Decimal('0'))}）"
    coverage = Decimal(len(retained)) / Decimal(len(available_evidence_ids))
    return (
        f"{len(retained)}/{len(available_evidence_ids)}"
        f"（{_display_percent(coverage)}）"
    )




def _as_technical_signal(assessment: CloseTechnicalAssessment) -> TechnicalSignal:
    return TechnicalSignal(
        symbol=assessment.symbol,
        as_of=assessment.as_of,
        horizon=assessment.horizon,
        decision=assessment.decision,
        score=assessment.score,
        reference_price=assessment.reference_price,
        invalidation_price=assessment.invalidation_price,
        reason_codes=assessment.reason_codes,
        data_age=timedelta(0),
        strategy_version=assessment.strategy_version,
        metrics=assessment.metrics,
    )


def _recommendation_evidence(
    market_evidence: EvidenceReference | None,
    news_evidence: tuple[EvidenceReference, ...],
    cross_market_evidence: tuple[EvidenceReference, ...] = (),
) -> tuple[EvidenceReference, ...]:
    output = (
        *((market_evidence,) if market_evidence is not None else ()),
        *news_evidence,
        *cross_market_evidence,
    )
    identifiers = [item.evidence_id for item in output]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("recommendation evidence IDs must be unique")
    return output


def _technical_summary(
    assessment: CloseTechnicalAssessment,
    market_evidence: EvidenceReference | None,
    profile: ResearchInstrumentProfile | None,
) -> tuple[str, ...]:
    metrics = dict(assessment.metrics)
    output = [
        f"deterministic decision={assessment.decision.value}",
        f"technical score={_display_decimal(assessment.score)}",
        f"latest completed session={assessment.latest_trade_date}",
        f"target next session={assessment.next_session}",
        f"reason codes={','.join(assessment.reason_codes)}",
    ]
    if profile is not None:
        output.extend(
            (
                f"instrument name={profile.name}",
                f"asset type={profile.asset_type}; board={profile.board}",
                f"industry={profile.industry}; styles={','.join(profile.styles)}",
                f"research role={profile.research_role}",
            )
        )
    if market_evidence is not None:
        output.append(f"market evidence id={market_evidence.evidence_id}")
    output.extend(
        f"signal family {item.family_id}: score="
        f"{_display_decimal(item.score)}; {item.summary}"
        for item in assessment.signal_families
    )
    output.extend(
        f"diagnostic horizon {item.horizon.value}: score="
        f"{_display_decimal(item.score)}; coverage="
        f"{_display_decimal(item.coverage)}; {item.summary}"
        for item in assessment.horizon_views
    )
    for name in (
        "close",
        "ma_5",
        "ma_20",
        "ma_60",
        "volume_ratio_20",
        "rsi_14",
        "atr_14",
        "return_5d",
        "return_20d",
        "annualized_volatility_20",
    ):
        if name in metrics:
            output.append(f"{name}={_display_decimal(metrics[name])}")
    return tuple(output)


def _market_macro_evidence(
    reference: EvidenceReference | None,
    assessment: CloseTechnicalAssessment,
    *,
    provider_name: str | None = None,
) -> tuple[EvidenceItem, ...]:
    if reference is None:
        return ()
    selected_names = (
        "close",
        "ma_20",
        "ma_60",
        "ma_120",
        "ma_200",
        "return_20d",
        "return_60d",
        "volume_ratio_20",
        "rsi_14",
        "annualized_volatility_20",
        "max_drawdown_60",
    )
    metric_map = dict(assessment.metrics)
    metrics = ", ".join(
        f"{name}={_display_decimal(metric_map[name])}"
        for name in selected_names
        if name in metric_map
    )
    return (
        EvidenceItem(
            evidence_id=reference.evidence_id,
            publisher=provider_name or "historical.daily",
            source_tier=reference.source_tier,
            published_at=reference.published_at,
            first_seen_at=reference.first_seen_at,
            title=reference.title,
            excerpt=(
                "decision="
                f"{assessment.decision.value}; "
                f"score={_display_decimal(assessment.score)}; "
                f"{metrics}"
            )[:1_200],
            canonical_url=reference.canonical_url,
            content_hash=reference.evidence_id,
        ),
    )



def _single_line(value: str) -> str:
    return " ".join(value.split())


def _display_decimal(value: Decimal | None, *, places: int = 3) -> str:
    """渲染有界显示精度且不改变存储值。"""

    if value is None:
        return "—"
    quantizer = Decimal(1).scaleb(-places)
    return format(value.quantize(quantizer), "f")


def _display_percent(value: Decimal | None) -> str:
    if value is None:
        return "—"
    return f"{_display_decimal(value * Decimal(100))}%"


def _display_score(value: Decimal | None) -> str:
    if value is None:
        return "—"
    rendered = _display_decimal(value)
    return f"{rendered}（中性）" if value == 0 else rendered


def _fusion_summary_lines(
    recommendation: ResearchRecommendation,
) -> tuple[str, ...]:
    """以可读方式解释评分融合，但不将其呈现为概率。"""

    if recommendation.fusion_version is None:
        return ("评分融合：旧版记录未保存融合元数据。",)
    coverage = _display_percent(recommendation.macro_evidence_coverage)
    technical_weight = _display_percent(recommendation.technical_fusion_weight)
    macro_weight = _display_percent(recommendation.macro_fusion_weight)
    reasons = tuple(
        _FUSION_REASON_ZH.get(code, code)
        for code in recommendation.fusion_reason_codes
        if code != "COMBINED_SCORE_UNCALIBRATED"
    )
    summary = "；".join(reasons) if reasons else "本期没有额外融合状态"
    return (
        f"评分融合：本次实际技术面权重 {technical_weight}、宏观面权重 {macro_weight}。"
        "宏观为观察结论时会折减有效权重；宏观不能把未通过技术门槛的标的"
        "单独升级为入场候选。",
        f"融合版本：{recommendation.fusion_version}；宏观证据覆盖率：{coverage}。",
        f"融合状态：{summary}。",
    )


def _horizon_view_line(view: CloseHorizonView) -> str:
    label = _HORIZON_ZH.get(view.horizon.value, view.horizon.value)
    return (
        f"{label}诊断分：{_display_score(view.score)}；家族覆盖率 "
        f"{_display_percent(view.coverage)}。{view.summary}。"
    )


def _technical_metric_lines(metrics: dict[str, Decimal]) -> tuple[str, ...]:
    """渲染确定性指标族且不暴露原始原因码。"""

    value = metrics.get
    return (
        "- 趋势结构：收盘价 "
        f"{_display_decimal(value('close'))}；5/20/60/120/200日均线分别为 "
        f"{_display_decimal(value('ma_5'))} / "
        f"{_display_decimal(value('ma_20'))} / "
        f"{_display_decimal(value('ma_60'))} / "
        f"{_display_decimal(value('ma_120'))} / "
        f"{_display_decimal(value('ma_200'))}。",
        "- 均线斜率（每个交易日的拟合价格变化 ÷ ATR）：20日 "
        f"{_display_decimal(value('trend_slope_atr_20'))}；60日 "
        f"{_display_decimal(value('trend_slope_atr_60'))}。",
        "- Wilder趋势强度：ADX14 "
        f"{_display_decimal(value('adx_14'))}；正向/负向趋势指标（+DI14/-DI14）"
        f"{_display_decimal(value('positive_di_14'))} / "
        f"{_display_decimal(value('negative_di_14'))}。"
        "ADX只描述趋势强弱，不判断上涨或下跌，也不进入方向评分。",
        "- 多周期收益：5日 "
        f"{_display_percent(value('return_5d'))}；20日 "
        f"{_display_percent(value('return_20d'))}；60日 "
        f"{_display_percent(value('return_60d'))}；中期动量（第6至120日前）"
        f"{_display_percent(value('return_120_skip_5d'))}；200日 "
        f"{_display_percent(value('return_200d'))}。",
        "- 唐奇安价格通道：20日阻力、支撑 "
        f"{_display_decimal(value('breakout_level_20'))} / "
        f"{_display_decimal(value('support_level_20'))}；55日阻力、支撑 "
        f"{_display_decimal(value('breakout_level_55'))} / "
        f"{_display_decimal(value('support_level_55'))}。",
        "- 振荡与趋势：简单窗口 RSI14 "
        f"{_display_decimal(value('rsi_14'))}；随机指标K14 "
        f"{_display_decimal(value('stochastic_k_14'))}；MACD差离线 "
        f"{_display_decimal(value('macd_line'))}、信号线 "
        f"{_display_decimal(value('macd_signal'))}、柱值 "
        f"{_display_decimal(value('macd_histogram'))}。",
        "- 布林带：价格相对位置 "
        f"{_display_decimal(value('bollinger_percent_b'))}"
        "（0为下轨，0.500为中轨，1为上轨）；带宽 "
        f"{_display_percent(value('bollinger_bandwidth'))}；"
        "当前带宽 ÷ 前20期平均带宽 "
        f"{_display_decimal(value('bollinger_bandwidth_state'))}。",
    )


def _risk_metric_lines(metrics: dict[str, Decimal]) -> tuple[str, ...]:
    value = metrics.get
    return (
        "- 波动：简单窗口 ATR14 "
        f"{_display_decimal(value('atr_14'))}（占价格 "
        f"{_display_percent(value('atr_fraction'))}）；20日、60日年化波动分别为 "
        f"{_display_percent(value('annualized_volatility_20'))} / "
        f"{_display_percent(value('annualized_volatility_60'))}。",
        "- 尾部与跳空：20日下行波动 "
        f"{_display_percent(value('downside_volatility_20'))}；隔夜跳空波动 "
        f"{_display_percent(value('overnight_gap_volatility_20'))}；"
        f"60日最大回撤 {_display_percent(value('max_drawdown_60'))}。",
        "- 量价：当日成交量 ÷ 近20日均量 "
        f"{_display_decimal(value('volume_ratio_20'))}；上涨/下跌成交额比 "
        f"{_display_decimal(value('up_down_amount_ratio_20'))}。",
        _turnover_metric_line(metrics),
        _amihud_metric_line(metrics),
    )


def _turnover_metric_line(metrics: dict[str, Decimal]) -> str:
    coverage = metrics.get("turnover_coverage_20")
    if coverage is None or coverage < Decimal(1):
        return (
            "- 换手率状态：数据不足（有效覆盖 "
            f"{_display_percent(coverage)}）；未把缺失时的中性占位值解读为真实比值。"
        )
    return (
        "- 换手率状态：当日换手率 ÷ 近20日均值 "
        f"{_display_decimal(metrics.get('turnover_state_20'))}；"
        f"有效覆盖 {_display_percent(coverage)}。"
    )


def _amihud_metric_line(metrics: dict[str, Decimal]) -> str:
    value_20 = metrics.get("amihud_bps_per_cny_billion_20")
    value_60 = metrics.get("amihud_bps_per_cny_billion_60")
    coverage_20 = metrics.get("amihud_coverage_20")
    coverage_60 = metrics.get("amihud_coverage_60")
    if (
        value_20 is None
        or value_60 is None
        or coverage_20 is None
        or coverage_60 is None
        or coverage_20 == 0
        or coverage_60 == 0
    ):
        return "- Amihud非流动性：成交额数据不足，本期不展示。"
    return (
        "- Amihud非流动性（|日收益率| ÷ 成交额，越低表示历史价格冲击越小）："
        f"20日 {_display_decimal(value_20)}、60日 {_display_decimal(value_60)} "
        "基点/10亿元成交额；有效覆盖分别为 "
        f"{_display_percent(coverage_20)} / {_display_percent(coverage_60)}；"
        "20日均值 ÷ 60日均值 "
        f"{_display_decimal(metrics.get('illiquidity_state_20_60'))}。"
    )


def _report_evidence(
    references: tuple[EvidenceReference, ...],
    macro: MacroAnalysis | None,
) -> tuple[EvidenceReference, ...]:
    """为每个已保留报告来源返回完整的人类可读索引。"""

    if macro is None:
        return references
    cited_ids: list[str] = []
    for claim in macro.claims[:6]:
        cited_ids.extend(claim.evidence_ids)
    for scenario in macro.scenarios[:3]:
        cited_ids.extend(scenario.evidence_ids)
    ordered_ids = tuple(dict.fromkeys(cited_ids))
    if not ordered_ids:
        return references
    cited = set(ordered_ids)
    return (
        *(item for item in references if item.evidence_id in cited),
        *(item for item in references if item.evidence_id not in cited),
    )


def _format_evidence_citations(
    evidence_ids: tuple[str, ...],
    evidence_numbers: dict[str, int],
) -> str:
    numbers = tuple(
        dict.fromkeys(
            evidence_numbers[evidence_id]
            for evidence_id in evidence_ids
            if evidence_id in evidence_numbers
        )
    )
    if not numbers:
        return "[证据索引待补]"
    return "[证据" + "、".join(str(number) for number in numbers) + "]"


_EVIDENCE_ID_PATTERN = re.compile(r"(?<![A-Za-z0-9])[0-9a-fA-F]{64}(?![A-Za-z0-9])")


def _humanize_model_text(value: str, evidence_numbers: dict[str, int]) -> str:
    """若模型在文本中复述面向供应商的证据哈希，则将其替换。"""

    text = _single_line(value)
    for evidence_id, number in evidence_numbers.items():
        text = text.replace(evidence_id, f"[证据{number}]")
    return _EVIDENCE_ID_PATTERN.sub("[未匹配证据]", text)


def _evidence_destination(canonical_url: str) -> str:
    value = _single_line(canonical_url)
    if value.startswith("local://"):
        return "本地证据库（完整内容与校验指纹已留存）"
    return value


def _evidence_display_title(item: EvidenceReference, symbol: str) -> str:
    """隐藏供应商路由诊断，同时保留有用的本地标签。"""

    if item.canonical_url.startswith("local://"):
        return (
            f"{symbol} 未复权日线与技术计算输入"
            f"（数据截至 {item.published_at.astimezone(SHANGHAI).date().isoformat()}）"
        )
    return _single_line(item.title)
