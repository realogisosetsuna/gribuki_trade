"""单次 A 股研究编排。

本服务刻意止于研究推荐，不依赖券商、账户、订单或执行。调用方可以明确要求将文本通知
加入队列，但通知始终是单向的，不能把推荐变为订单。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Protocol

from gribuki_trade.analysis.schemas import MacroAnalysis
from gribuki_trade.domain.recommendations import (
    EvidenceReference,
    RecommendationDecision,
    RecommendationHorizon,
    ResearchRecommendation,
)
from gribuki_trade.features.technical import (
    TechnicalBar,
    TechnicalSignal,
    TechnicalSignalConfig,
    build_technical_signal,
)
from gribuki_trade.policy.recommendation_gate import (
    RecommendationGateConfig,
    build_recommendation,
)
from gribuki_trade.ports.market_data import (
    AsyncIntradayMarketData,
    FreshnessStatus,
    IntradayBar,
    MarketDataUnavailableError,
    MinuteInterval,
    SourceSemantics,
)
from gribuki_trade.ports.notifier import NotificationTargetKind, OutboundNotification
from gribuki_trade.reporting.contracts import (
    ReportKind,
    humanize_codes,
    humanize_internal_code,
    render_stable_text_report,
)


class NotificationOutbox(Protocol):
    """由 ``SQLiteOutbox`` 实现的结构子集。"""

    def enqueue(self, notification: OutboundNotification) -> object: ...


@dataclass(frozen=True, slots=True)
class AShareResearchRequest:
    """一次具有时点约束的技术研究评估输入。"""

    symbol: str
    start: datetime
    end: datetime
    horizon: RecommendationHorizon = RecommendationHorizon.SHORT_1_TO_5_DAYS
    interval: MinuteInterval = MinuteInterval.ONE_MINUTE
    decision_time: datetime | None = None
    is_currently_held: bool = False
    evidence: tuple[EvidenceReference, ...] = ()
    macro: MacroAnalysis | None = None
    baseline_macro: MacroAnalysis | None = None
    adversarial_macro: MacroAnalysis | None = None
    macro_selected_track: str | None = None
    macro_audit_record_sha256: str | None = None

    def __post_init__(self) -> None:
        _canonical_ashare_symbol(self.symbol)
        _require_aware(self.start, "start")
        _require_aware(self.end, "end")
        if self.start >= self.end:
            raise ValueError("start must be before end")
        if self.decision_time is not None:
            _require_aware(self.decision_time, "decision_time")
            if self.end > self.decision_time:
                raise ValueError("end must not be after decision_time")
        identifiers = [item.evidence_id for item in self.evidence]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("evidence IDs must be unique")
        dual = (self.baseline_macro, self.adversarial_macro)
        if any(item is not None for item in dual):
            if any(item is None for item in dual) or self.macro_selected_track != "ADVERSARIAL":
                raise ValueError("dual-track research requires both branches")
        elif self.macro_selected_track is not None or self.macro_audit_record_sha256 is not None:
            raise ValueError("single-track research cannot carry dual-track metadata")

    @property
    def canonical_symbol(self) -> str:
        return _canonical_ashare_symbol(self.symbol)


@dataclass(frozen=True, slots=True)
class ResearchNotificationTarget:
    """研究提醒的明确目标与发布策略。"""

    target_id: str
    target_kind: NotificationTargetKind = NotificationTargetKind.PRIVATE
    channel: str = "onebot"
    decisions: frozenset[RecommendationDecision] = frozenset(
        {
            RecommendationDecision.ENTER_CANDIDATE,
            RecommendationDecision.WATCH,
            RecommendationDecision.REDUCE,
        }
    )
    max_characters: int = 3500

    def __post_init__(self) -> None:
        if not self.target_id.strip():
            raise ValueError("target_id must not be empty")
        if not self.channel.strip():
            raise ValueError("channel must not be empty")
        if not self.decisions:
            raise ValueError("at least one notification decision is required")
        if self.max_characters < 200:
            raise ValueError("max_characters must be at least 200")


@dataclass(frozen=True, slots=True)
class AShareResearchRun:
    """一次评估结果及其可选发件箱副作用。"""

    recommendation: ResearchRecommendation
    notification: OutboundNotification | None = None
    notification_enqueued: bool = False
    failure_code: str | None = None


@dataclass(frozen=True, slots=True)
class AShareMarketDataCollection:
    """时点评估市场数据阶段的脱敏结果。"""

    bars: tuple[IntradayBar, ...]
    failure_code: str | None = None

    def __post_init__(self) -> None:
        if self.failure_code is not None and self.bars:
            raise ValueError("failed market-data collection cannot contain bars")


class AShareResearchService:
    """拉取已完成行情柱、构建信号并应用发布门控。"""

    def __init__(
        self,
        market_data: AsyncIntradayMarketData,
        *,
        technical_config: TechnicalSignalConfig | None = None,
        gate_config: RecommendationGateConfig | None = None,
        outbox: NotificationOutbox | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._market_data = market_data
        self._technical_config = technical_config or TechnicalSignalConfig()
        self._gate_config = gate_config or RecommendationGateConfig()
        self._outbox = outbox
        self._clock = clock

    async def run_once(
        self,
        request: AShareResearchRequest,
        *,
        notification_target: ResearchNotificationTarget | None = None,
    ) -> AShareResearchRun:
        """执行一次评估；仅在明确提供目标时入队。"""

        collection = await self.collect_market_data(request)
        return self.evaluate_collection(
            request,
            collection,
            notification_target=notification_target,
        )

    async def collect_market_data(
        self, request: AShareResearchRequest
    ) -> AShareMarketDataCollection:
        """采集行情柱且不预先冻结推荐时间。"""

        try:
            bars = tuple(
                await self._market_data.fetch_intraday_bars_async(
                    request.canonical_symbol,
                    request.start,
                    request.end,
                    interval=request.interval,
                    completed_only=True,
                )
            )
        except MarketDataUnavailableError:
            # 供应商消息可能包含请求细节；只公开该稳定代码，并让无关编程错误保持可见。
            return AShareMarketDataCollection(
                bars=(),
                failure_code="MARKET_DATA_FETCH_FAILED",
            )
        return AShareMarketDataCollection(bars=bars)

    def evaluate_collection(
        self,
        request: AShareResearchRequest,
        collection: AShareMarketDataCollection,
        *,
        notification_target: ResearchNotificationTarget | None = None,
    ) -> AShareResearchRun:
        """在显式或拉取后的时点评估不可变数据集合。"""

        decision_time = request.decision_time or self._clock()
        _require_aware(decision_time, "decision_time")
        self._validate_evidence(request.evidence, decision_time)

        technical = (
            _abstain_signal(
                request.canonical_symbol,
                request.horizon,
                decision_time,
                self._technical_config.strategy_version,
                collection.failure_code,
                latest_end=None,
            )
            if collection.failure_code is not None
            else self._technical_from_market_bars(
                request,
                collection.bars,
                decision_time=decision_time,
            )
        )
        recommendation = build_recommendation(
            technical,
            request.evidence,
            macro=request.macro,
            config=self._gate_config,
        )

        if (
            notification_target is None
            or recommendation.decision not in notification_target.decisions
        ):
            return AShareResearchRun(
                recommendation=recommendation,
                failure_code=collection.failure_code,
            )
        if self._outbox is None:
            raise RuntimeError(
                "notification_target requires an explicitly configured outbox"
            )

        notification = format_recommendation_notification(
            recommendation,
            notification_target,
            baseline_macro=request.baseline_macro,
            adversarial_macro=request.adversarial_macro,
            selected_track=request.macro_selected_track,
            audit_record_sha256=request.macro_audit_record_sha256,
        )
        self._outbox.enqueue(notification)
        return AShareResearchRun(
            recommendation=recommendation,
            notification=notification,
            notification_enqueued=True,
            failure_code=collection.failure_code,
        )

    def _technical_from_market_bars(
        self,
        request: AShareResearchRequest,
        bars: tuple[IntradayBar, ...],
        *,
        decision_time: datetime,
    ) -> TechnicalSignal:
        invalid_reason = _market_bar_invariant_failure(
            request.canonical_symbol,
            request.interval,
            request.start,
            request.end,
            bars,
        )
        if invalid_reason is not None:
            return _abstain_signal(
                request.canonical_symbol,
                request.horizon,
                decision_time,
                self._technical_config.strategy_version,
                invalid_reason,
                latest_end=bars[-1].end_at if bars else None,
            )

        try:
            technical_bars = tuple(
                TechnicalBar(
                    end_time=bar.end_at,
                    # 采集时间是本进程能够证明供应商记录已经可见的最早时点。
                    available_at=bar.meta.fetched_at,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume_lots,
                    complete=bar.is_closed,
                )
                for bar in bars
            )
            signal = build_technical_signal(
                request.canonical_symbol,
                technical_bars,
                decision_time=decision_time,
                horizon=request.horizon,
                is_currently_held=request.is_currently_held,
                config=self._technical_config,
            )
        except ValueError:
            # 供应商时间顺序/模式异常绝不能成为入场信号。请求校验已在边界完成，因此此处
            # 捕获仅限于供应商派生的行情柱。
            return _abstain_signal(
                request.canonical_symbol,
                request.horizon,
                decision_time,
                self._technical_config.strategy_version,
                "INVALID_MARKET_DATA",
                latest_end=bars[-1].end_at if bars else None,
            )

        if signal.decision is RecommendationDecision.ABSTAIN or not bars:
            return signal
        latest = bars[-1]
        if latest.meta.degraded:
            return _force_abstain(signal, "DEGRADED_MARKET_DATA")
        if latest.meta.freshness is FreshnessStatus.STALE:
            return _force_abstain(signal, "UPSTREAM_MARKET_DATA_STALE")
        if latest.meta.freshness is FreshnessStatus.UNKNOWN:
            return _force_abstain(signal, "UPSTREAM_FRESHNESS_UNKNOWN")
        return signal

    @staticmethod
    def _validate_evidence(
        evidence: tuple[EvidenceReference, ...], decision_time: datetime
    ) -> None:
        for item in evidence:
            _require_aware(item.published_at, "evidence published_at")
            _require_aware(item.first_seen_at, "evidence first_seen_at")
            if item.published_at > decision_time or item.first_seen_at > decision_time:
                raise ValueError("evidence was not available at decision_time")
            if item.first_seen_at < item.published_at:
                raise ValueError("evidence first_seen_at must not precede published_at")


def format_recommendation_notification(
    recommendation: ResearchRecommendation,
    target: ResearchNotificationTarget,
    *,
    baseline_macro: MacroAnalysis | None = None,
    adversarial_macro: MacroAnalysis | None = None,
    selected_track: str | None = None,
    audit_record_sha256: str | None = None,
) -> OutboundNotification:
    """渲染有界、纯文本且不可执行的研究通知。"""

    labels = {
        RecommendationDecision.ENTER_CANDIDATE: "进入候选",
        RecommendationDecision.WATCH: "观察",
        RecommendationDecision.REDUCE: "考虑降低暴露",
        RecommendationDecision.ABSTAIN: "暂不判断",
    }
    conclusion_lines = [
        f"标的：{_single_line(recommendation.symbol)}",
        f"结论：{labels[recommendation.decision]}",
        f"周期：{humanize_internal_code(recommendation.horizon.value)}",
        f"决策时点：{recommendation.as_of.isoformat(timespec='seconds')}",
        f"有效至：{recommendation.expires_at.isoformat(timespec='seconds')}",
    ]
    technical_lines = [
        f"技术分：{recommendation.technical_score}",
        f"参考价：{_format_decimal(recommendation.reference_price)}",
        f"原因：{humanize_codes(recommendation.reason_codes)}",
    ]
    macro_lines = [
        f"宏观评分：{recommendation.macro_score}；综合评分："
        f"{recommendation.combined_score}（不是收益概率）。",
        "基本面与宏观详情以冻结证据和对应长篇研究报告为准。",
    ]
    adversarial_lines: list[str] = []
    if baseline_macro is not None and adversarial_macro is not None:
        adversarial_lines.extend(
            (
                "两条轨道使用同一份冻结证据：",
                f"- 原单分析器：{humanize_internal_code(baseline_macro.decision.value)}；宏观倾向 "
                f"{baseline_macro.macro_impact}；证据覆盖 "
                f"{_track_evidence_coverage(baseline_macro, recommendation.evidence)}；"
                f"{_single_line(baseline_macro.regime)}",
                f"- 结构化对抗分析器："
                f"{humanize_internal_code(adversarial_macro.decision.value)}；宏观倾向 "
                f"{adversarial_macro.macro_impact}；证据覆盖 "
                f"{_track_evidence_coverage(adversarial_macro, recommendation.evidence)}；"
                f"{_single_line(adversarial_macro.regime)}",
                f"- 生产采用：{humanize_internal_code(selected_track)}；审计摘要："
                f"{audit_record_sha256 or '未配置独立审计存储'}",
            )
        )
    else:
        adversarial_lines.append(
            "本次未同时取得原单分析器与结构化对抗分析器结果，不能声称完成双轨复核。"
        )
    invalidation_lines = [
        f"失效参考：{_format_decimal(recommendation.invalidation_price)}",
        f"不确定性：{humanize_codes(recommendation.uncertainties)}",
        "仅供个人研究与人工复核，不构成可执行交易指令，也不会自动下单。",
    ]
    sections = {
        "结论": "\n".join(conclusion_lines),
        "技术结构": "\n".join(technical_lines),
        "基本面与宏观": "\n".join(macro_lines),
        "对抗观点": "\n".join(adversarial_lines),
        "失效条件": "\n".join(invalidation_lines),
    }
    if recommendation.evidence:
        sections["证据索引"] = "\n".join(
            f"- {_single_line(item.title)} | {_single_line(item.canonical_url)}"
            for item in recommendation.evidence[:3]
        )
    text = render_stable_text_report(
        ReportKind.INSTRUMENT_RESEARCH,
        title="A股研究建议｜不会自动下单",
        sections=sections,
    )
    if len(text) > target.max_characters:
        compact_sections = dict(sections)
        compact_sections.pop("证据索引", None)
        compact_sections["对抗观点"] = (
            "\n".join(
                (
                    "原单分析器："
                    f"{humanize_internal_code(baseline_macro.decision.value)}；"
                    f"评分 {baseline_macro.macro_impact}；覆盖 "
                    f"{_track_evidence_coverage(baseline_macro, recommendation.evidence)}",
                    "结构化对抗分析器："
                    f"{humanize_internal_code(adversarial_macro.decision.value)}；"
                    f"评分 {adversarial_macro.macro_impact}；覆盖 "
                    f"{_track_evidence_coverage(adversarial_macro, recommendation.evidence)}",
                    f"采用 {humanize_internal_code(selected_track)}；审计 "
                    f"{audit_record_sha256 or '未配置独立审计存储'}",
                )
            )
            if baseline_macro is not None and adversarial_macro is not None
            else adversarial_lines[0]
        )
        text = render_stable_text_report(
            ReportKind.INSTRUMENT_RESEARCH,
            title="A股研究建议｜不会自动下单",
            sections=compact_sections,
        )
    if len(text) > target.max_characters:
        raise ValueError("notification limit is too small for report contract")

    destination_hash = sha256(
        f"{target.channel}|{target.target_kind.value}|{target.target_id}".encode()
    ).hexdigest()[:16]
    return OutboundNotification(
        idempotency_key=(
            f"research:{recommendation.recommendation_id}:"
            f"{target.channel}:{destination_hash}"
        ),
        channel=target.channel,
        target_kind=target.target_kind,
        target_id=target.target_id,
        text=text,
        created_at=recommendation.as_of,
        expires_at=recommendation.expires_at,
    )


def _market_bar_invariant_failure(
    requested_symbol: str,
    requested_interval: MinuteInterval,
    requested_start: datetime,
    requested_end: datetime,
    bars: tuple[IntradayBar, ...],
) -> str | None:
    if any(bar.symbol.upper() != requested_symbol for bar in bars):
        return "MARKET_DATA_SYMBOL_MISMATCH"
    if any(bar.interval is not requested_interval for bar in bars):
        return "MARKET_DATA_INTERVAL_MISMATCH"
    if any(not bar.is_closed for bar in bars):
        return "INCOMPLETE_MARKET_BAR"
    if any(
        bar.start_at < requested_start
        or bar.end_at > requested_end
        or bar.start_at >= bar.end_at
        for bar in bars
    ):
        return "MARKET_DATA_OUTSIDE_REQUEST_WINDOW"
    if any(
        bar.meta.semantics is not SourceSemantics.AGGREGATED_MINUTE_BAR for bar in bars
    ):
        return "UNSUPPORTED_MARKET_DATA_SEMANTICS"
    return None


def _force_abstain(signal: TechnicalSignal, reason: str) -> TechnicalSignal:
    return replace(
        signal,
        decision=RecommendationDecision.ABSTAIN,
        score=Decimal("0"),
        reference_price=None,
        invalidation_price=None,
        reason_codes=(reason,),
        metrics=(),
    )


def _abstain_signal(
    symbol: str,
    horizon: RecommendationHorizon,
    decision_time: datetime,
    strategy_version: str,
    reason: str,
    *,
    latest_end: datetime | None,
) -> TechnicalSignal:
    age = timedelta(0) if latest_end is None else max(decision_time - latest_end, timedelta(0))
    return TechnicalSignal(
        symbol=symbol.strip().upper(),
        as_of=decision_time,
        horizon=horizon,
        decision=RecommendationDecision.ABSTAIN,
        score=Decimal("0"),
        reference_price=None,
        invalidation_price=None,
        reason_codes=(reason,),
        data_age=age,
        strategy_version=strategy_version,
        metrics=(),
    )


def _format_decimal(value: Decimal | None) -> str:
    return "—" if value is None else format(value, "f")


def _track_evidence_coverage(
    analysis: MacroAnalysis,
    evidence: tuple[EvidenceReference, ...],
) -> str:
    available = {item.evidence_id for item in evidence}
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
    retained = referenced & available
    coverage = (
        Decimal("0")
        if not available
        else Decimal(len(retained)) / Decimal(len(available))
    )
    return f"{len(retained)}/{len(available)}（{format(coverage * 100, '.1f')}%）"


def _single_line(value: str) -> str:
    return " ".join(value.split())


def _canonical_ashare_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if len(value) == 6 and value.isdigit():
        exchange = "SH" if value.startswith(("5", "6", "9")) else "SZ"
        return f"{value}.{exchange}"
    if len(value) == 9 and value[6] == ".":
        code, exchange = value.split(".", maxsplit=1)
        if code.isdigit() and exchange in {"SH", "SZ"}:
            return value
    raise ValueError("symbol must look like 600000.SH or 000001.SZ")


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
