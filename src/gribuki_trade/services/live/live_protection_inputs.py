"""从真实公共行情、交易日历和双轨语义分析构建实盘保护输入。"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, time, timedelta
from decimal import ROUND_FLOOR, Decimal
from typing import Protocol
from zoneinfo import ZoneInfo

from gribuki_trade.analysis.schemas import (
    EvidenceItem,
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
)
from gribuki_trade.domain.live_records import ConfirmedLiveFill
from gribuki_trade.features.deep_exit_planning import (
    DeepExitTimeframe,
    DeepSemanticAssessment,
    aggregate_completed_bars,
)
from gribuki_trade.features.exit_planning import QuickExitPlanConfig
from gribuki_trade.features.technical import TechnicalBar
from gribuki_trade.ports.llm_analyzer import DualTrackMacroAnalyzer
from gribuki_trade.ports.market_data import (
    AsyncIntradayMarketData,
    AsyncTradingCalendar,
    MarketDataUnavailableError,
    MinuteInterval,
)
from gribuki_trade.services.live.live_trade_orchestration import (
    LiveProtectionInputError,
    LiveProtectionInputProvider,
    LiveProtectionInputs,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class LiveDualExitSemanticAnalyzer(Protocol):
    """同一冻结证据上并行返回 baseline 与 adversarial 两条分析。"""

    async def analyze(
        self,
        fill: ConfirmedLiveFill,
        *,
        timeframes: tuple[DeepExitTimeframe, ...],
        decision_at: datetime,
    ) -> tuple[DeepSemanticAssessment, DeepSemanticAssessment]: ...


class ProductionLiveDualExitSemanticAnalyzer:
    """把实盘冻结 K 线接入生产双轨分析协议。

    两条轨道有任一条降级、拒答或协议不合法时均不伪造结果，由 durable work
    稍后重试。价格和订单字段不会交给模型决定；模型只输出语义评分。
    """

    def __init__(self, analyzer: DualTrackMacroAnalyzer) -> None:
        self._analyzer = analyzer

    async def analyze(
        self,
        fill: ConfirmedLiveFill,
        *,
        timeframes: tuple[DeepExitTimeframe, ...],
        decision_at: datetime,
    ) -> tuple[DeepSemanticAssessment, DeepSemanticAssessment]:
        request = _live_deep_exit_request(fill, timeframes, decision_at)
        result = await self._analyzer.analyze_dual(request)
        if result.failure_code is not None:
            raise RuntimeError("live dual-track analysis did not complete")
        result.baseline_analysis.validate_against(request)
        result.adversarial_analysis.validate_against(request)
        baseline = _live_semantic_assessment(
            result.baseline_analysis,
            system="BASELINE_LLM",
            request=request,
        )
        adversarial = _live_semantic_assessment(
            result.adversarial_analysis,
            system="ADVERSARIAL_LLM",
            request=request,
        )
        if baseline is None or adversarial is None:
            raise RuntimeError("live dual-track analysis abstained")
        return baseline, adversarial


class PublicMarketLiveProtectionInputProvider(LiveProtectionInputProvider):
    """生产实盘保护输入；任一双轨结果缺失都会关闭本次计划构建。"""

    def __init__(
        self,
        *,
        market_data: AsyncIntradayMarketData,
        calendar: AsyncTradingCalendar,
        semantic_analyzer: LiveDualExitSemanticAnalyzer | None = None,
        holding_sessions: int = 5,
        history_days: int = 10,
        maximum_market_data_age: timedelta = timedelta(hours=8),
        price_tick: Decimal = Decimal("0.01"),
    ) -> None:
        if holding_sessions < 1:
            raise ValueError("holding_sessions must be positive")
        if history_days < 2:
            raise ValueError("history_days must be at least two")
        if maximum_market_data_age <= timedelta(0):
            raise ValueError("maximum_market_data_age must be positive")
        if not price_tick.is_finite() or price_tick <= 0:
            raise ValueError("price_tick must be positive")
        self._market = market_data
        self._calendar = calendar
        self._semantic = semantic_analyzer
        self._holding_sessions = holding_sessions
        self._history_days = history_days
        self._maximum_age = maximum_market_data_age
        self._tick = price_tick

    async def prepare(
        self,
        fill: ConfirmedLiveFill,
        *,
        requested_at: datetime,
    ) -> LiveProtectionInputs:
        requested = _aware_utc(requested_at)
        try:
            raw = await self._market.fetch_intraday_bars_async(
                fill.symbol,
                requested - timedelta(days=self._history_days),
                requested,
                interval=MinuteInterval.ONE_MINUTE,
                completed_only=True,
            )
        except MarketDataUnavailableError:
            raise LiveProtectionInputError(
                "LIVE_PROTECTION_MARKET_DATA_UNAVAILABLE",
                retryable=True,
            ) from None
        decision_at = max(
            requested,
            *(_aware_utc(item.meta.fetched_at) for item in raw),
        )
        bars = tuple(
            TechnicalBar(
                end_time=item.end_at,
                available_at=item.meta.fetched_at,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume_lots,
                complete=item.is_closed,
            )
            for item in sorted(raw, key=lambda value: value.end_at)
            if item.is_closed and item.meta.fetched_at <= decision_at
        )
        quick_config = QuickExitPlanConfig(max_data_age=self._maximum_age)
        if len(bars) < max(300, quick_config.minimum_history):
            raise LiveProtectionInputError(
                "LIVE_PROTECTION_HISTORY_INSUFFICIENT",
                retryable=True,
            )
        if decision_at - bars[-1].end_time.astimezone(UTC) > self._maximum_age:
            raise LiveProtectionInputError("LIVE_PROTECTION_BARS_STALE", retryable=True)

        time_exit_at = await self._resolve_time_exit(decision_at)
        invalidation = _technical_invalidation(
            fill.price,
            bars=bars,
            tick=self._tick,
        )
        timeframes = (
            DeepExitTimeframe(
                timeframe_id="1m",
                bars=bars,
                weight=Decimal("0.5"),
                maximum_age=self._maximum_age,
            ),
            DeepExitTimeframe(
                timeframe_id="5m",
                bars=aggregate_completed_bars(bars, interval_minutes=5),
                weight=Decimal("0.3"),
                maximum_age=self._maximum_age,
            ),
            DeepExitTimeframe(
                timeframe_id="15m",
                bars=aggregate_completed_bars(bars, interval_minutes=15),
                weight=Decimal("0.2"),
                maximum_age=self._maximum_age,
            ),
        )
        if any(len(frame.bars) < frame.minimum_history for frame in timeframes):
            raise LiveProtectionInputError(
                "LIVE_PROTECTION_TIMEFRAME_HISTORY_INSUFFICIENT",
                retryable=True,
            )
        return LiveProtectionInputs(
            decision_at=decision_at,
            bars=bars,
            technical_invalidation_price=invalidation,
            time_exit_at=time_exit_at,
            strategy_version="observed-live-protection@1",
            deep_timeframes=timeframes,
            quick_config=quick_config,
        )

    async def assess_deep(
        self,
        fill: ConfirmedLiveFill,
        *,
        inputs: LiveProtectionInputs,
    ) -> tuple[DeepSemanticAssessment, DeepSemanticAssessment]:
        """在 QUICK 已持久化并可跟踪后执行双轨语义复核。"""

        if self._semantic is None:
            raise LiveProtectionInputError(
                "LIVE_PROTECTION_DUAL_LLM_NOT_CONFIGURED",
                retryable=True,
            )
        try:
            baseline, adversarial = await self._semantic.analyze(
                fill,
                timeframes=inputs.deep_timeframes,
                decision_at=inputs.decision_at,
            )
        except (TimeoutError, OSError, RuntimeError):
            raise LiveProtectionInputError(
                "LIVE_PROTECTION_DUAL_LLM_UNAVAILABLE",
                retryable=True,
            ) from None
        if (
            baseline.market_data_as_of > inputs.decision_at
            or adversarial.market_data_as_of > inputs.decision_at
        ):
            raise LiveProtectionInputError(
                "LIVE_PROTECTION_SEMANTIC_LOOKAHEAD",
                retryable=False,
            )
        return baseline, adversarial

    async def _resolve_time_exit(self, decision_at: datetime) -> datetime:
        local_date = decision_at.astimezone(_SHANGHAI).date()
        end = local_date + timedelta(days=45)
        try:
            calendar = await self._calendar.fetch_trade_calendar_async(local_date, end)
        except MarketDataUnavailableError:
            raise LiveProtectionInputError(
                "LIVE_PROTECTION_CALENDAR_UNAVAILABLE",
                retryable=True,
            ) from None
        expected_count = (end - local_date).days + 1
        expected_dates = tuple(
            local_date + timedelta(days=index) for index in range(expected_count)
        )
        actual_dates = tuple(item.calendar_date for item in calendar)
        if actual_dates != expected_dates:
            raise LiveProtectionInputError(
                "LIVE_PROTECTION_CALENDAR_INCOMPLETE",
                retryable=True,
            )
        sessions = tuple(
            item.calendar_date
            for item in calendar
            if item.is_trading_day and item.calendar_date > local_date
        )
        if len(sessions) < self._holding_sessions:
            raise LiveProtectionInputError(
                "LIVE_PROTECTION_FUTURE_SESSION_MISSING",
                retryable=True,
            )
        exit_date = sessions[self._holding_sessions - 1]
        return datetime.combine(exit_date, time(14, 55), _SHANGHAI).astimezone(UTC)


def _technical_invalidation(
    entry: Decimal,
    *,
    bars: tuple[TechnicalBar, ...],
    tick: Decimal,
) -> Decimal:
    """使用近期结构低点和成交价下方一跳中更宽的有效失效位。"""

    recent = bars[-20:]
    structural = min(item.low for item in recent) - tick
    below_entry = entry - tick
    raw = min(structural, below_entry)
    rounded = (raw / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
    if rounded <= 0 or rounded >= entry:
        raise LiveProtectionInputError(
            "LIVE_PROTECTION_INVALIDATION_UNAVAILABLE",
            retryable=False,
        )
    return rounded


def _live_deep_exit_request(
    fill: ConfirmedLiveFill,
    timeframes: tuple[DeepExitTimeframe, ...],
    decision_at: datetime,
) -> MacroAnalysisRequest:
    decision = _aware_utc(decision_at)
    if not timeframes:
        raise ValueError("live deep exit analysis requires timeframes")
    evidence = tuple(_live_timeframe_evidence(fill, frame) for frame in timeframes)
    material = {
        "command_id": fill.command_id,
        "decision_at": decision.isoformat(),
        "evidence": [item.content_hash for item in evidence],
        "symbol": fill.symbol,
    }
    analysis_id = (
        "live-deep-exit-"
        + hashlib.sha256(
            json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
    )
    return MacroAnalysisRequest(
        analysis_id=analysis_id,
        symbol=fill.symbol,
        as_of=decision,
        horizon="OBSERVED_LIVE_POST_FILL_DEEP_EXIT_REVIEW",
        technical_summary=(
            "仅评估已同步实盘多头持仓的趋势延续和风险收紧程度。",
            "不得生成订单、仓位或价格；具体保护价格由确定性退出计划计算。",
            f"用户确认成交价={fill.price}; 数量={fill.quantity}; "
            f"成交时点={fill.executed_at.isoformat()}",
        ),
        evidence=evidence,
    )


def _live_timeframe_evidence(
    fill: ConfirmedLiveFill,
    frame: DeepExitTimeframe,
) -> EvidenceItem:
    if not frame.bars:
        raise ValueError("live deep exit timeframe bars must not be empty")
    last = frame.bars[-1]
    published_at = _aware_utc(last.end_time)
    first_seen_at = max(_aware_utc(last.available_at), published_at)
    document = {
        "command_id": fill.command_id,
        "symbol": fill.symbol,
        "timeframe": frame.timeframe_id,
        "bars": [
            {
                "available_at": _aware_utc(item.available_at).isoformat(),
                "close": str(item.close),
                "complete": item.complete,
                "end_time": _aware_utc(item.end_time).isoformat(),
                "high": str(item.high),
                "low": str(item.low),
                "open": str(item.open),
                "volume": item.volume,
            }
            for item in frame.bars
        ],
    }
    digest = hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    first = frame.bars[0]
    return EvidenceItem(
        evidence_id=f"live-exit-{frame.timeframe_id}-{digest[:24]}",
        publisher="gribuki.live_exit.technical",
        source_tier=0,
        published_at=published_at,
        first_seen_at=first_seen_at,
        title=f"{fill.symbol} {frame.timeframe_id} 完整线实盘退出复核证据",
        excerpt=(
            f"时间框架={frame.timeframe_id}; 完整线数量={len(frame.bars)}; "
            f"末线开高低收={last.open}/{last.high}/{last.low}/{last.close}; "
            f"区间收盘变化={last.close - first.close}; "
            f"区间最高/最低={max(item.high for item in frame.bars)}/"
            f"{min(item.low for item in frame.bars)}。"
        ),
        canonical_url=f"urn:gribuki-trade:live-exit:{digest}",
        content_hash=digest,
    )


def _live_semantic_assessment(
    analysis: MacroAnalysis,
    *,
    system: str,
    request: MacroAnalysisRequest,
) -> DeepSemanticAssessment | None:
    if analysis.decision is MacroAnalysisDecision.ABSTAIN:
        return None
    score = analysis.technical_alignment * Decimal("0.75") + analysis.macro_impact * Decimal("0.25")
    confidence = {
        "HIGH": Decimal("0.75"),
        "MEDIUM": Decimal("0.50"),
        "LOW": Decimal("0.25"),
        "UNCALIBRATED": Decimal("0.25"),
    }.get(analysis.reported_confidence.strip().upper(), Decimal("0.25"))
    if analysis.decision is MacroAnalysisDecision.WATCH:
        confidence = min(confidence, Decimal("0.50"))
    identity = hashlib.sha256(
        f"{analysis.analysis_id}|{system}|{analysis.model_version}".encode()
    ).hexdigest()
    return DeepSemanticAssessment(
        assessment_id=f"live-deep-semantic-{identity}",
        system=system,
        score=max(Decimal("-1"), min(Decimal("1"), score)),
        confidence=confidence,
        market_data_as_of=max(item.published_at for item in request.evidence),
        evidence_ids=tuple(item.evidence_id for item in request.evidence),
    )


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("timestamp must be datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


__all__ = [
    "LiveDualExitSemanticAnalyzer",
    "ProductionLiveDualExitSemanticAnalyzer",
    "PublicMarketLiveProtectionInputProvider",
]
