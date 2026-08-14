"""面向下一明确有界交易日的 A 股盘后分析。"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Protocol
from zoneinfo import ZoneInfo

from gribuki_trade.analysis.schemas import EvidenceItem, MacroAnalysis
from gribuki_trade.domain.events import NormalizedEvent
from gribuki_trade.domain.instruments import ResearchInstrumentProfile
from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.domain.recommendations import (
    EvidenceReference,
    RecommendationDecision,
    RecommendationHorizon,
    ResearchRecommendation,
)
from gribuki_trade.features.close_analysis import (
    CloseAnalysisConfig,
    CloseHorizonView,
    CloseInstrumentType,
    CloseSignalFamily,
    CloseSignalFamilyStatus,
    CloseTechnicalAssessment,
    build_close_technical_assessment,
)
from gribuki_trade.features.technical import TechnicalSignal
from gribuki_trade.policy.recommendation_gate import (
    RecommendationGateConfig,
    build_recommendation,
)
from gribuki_trade.ports.ashare_breadth import AShareBreadthSnapshot
from gribuki_trade.ports.cross_market import CrossMarketSnapshot
from gribuki_trade.ports.cross_market_history import CrossMarketHistorySnapshot
from gribuki_trade.ports.llm_analyzer import MacroAnalyzer
from gribuki_trade.ports.market_data import (
    AsyncHistoricalDailyData,
    MarketDataUnavailableError,
)
from gribuki_trade.ports.notifier import OutboundNotification
from gribuki_trade.reporting.contracts import (
    ReportKind,
    humanize_codes,
    humanize_internal_code,
    render_stable_text_report,
)
from gribuki_trade.services.ashare_breadth_evidence import (
    AShareBreadthEvidenceBundle,
)
from gribuki_trade.services.ashare_context_evidence import (
    AShareContextEvidenceBundle,
)
from gribuki_trade.services.ashare_derivatives_evidence import (
    AShareDerivativesEvidenceBundle,
)
from gribuki_trade.services.ashare_research import (
    ResearchNotificationTarget,
)
from gribuki_trade.services.cross_market_evidence import (
    CrossMarketEvidenceBundle,
    build_cross_market_evidence,
)
from gribuki_trade.services.cross_market_relation_evidence import (
    CrossMarketRelationEvidenceBundle,
    build_cross_market_relation_evidence,
)
from gribuki_trade.services.global_risk_evidence import GlobalRiskEvidenceBundle
from gribuki_trade.services.macro_research import (
    EvidenceSelection,
    MacroResearchService,
    select_macro_evidence,
)
from gribuki_trade.services.official_rates_evidence import (
    OfficialRatesEvidenceBundle,
)

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


class CloseAnalysisOutbox(Protocol):
    def enqueue(self, notification: OutboundNotification) -> object: ...


@dataclass(frozen=True, slots=True)
class AShareCloseAnalysisRequest:
    symbol: str
    history_start: date
    latest_completed_session: date
    next_session: date
    events: tuple[NormalizedEvent, ...] = ()
    market_evidence: EvidenceReference | None = None
    as_of: datetime | None = None
    is_currently_held: bool = False
    instrument_type: CloseInstrumentType = CloseInstrumentType.STOCK
    instrument_profile: ResearchInstrumentProfile | None = None
    ashare_context: AShareContextEvidenceBundle | None = None
    ashare_context_failure_codes: tuple[str, ...] = ()
    ashare_breadth: AShareBreadthEvidenceBundle | None = None
    ashare_breadth_snapshot: AShareBreadthSnapshot | None = None
    ashare_breadth_failure_codes: tuple[str, ...] = ()
    global_risk: GlobalRiskEvidenceBundle | None = None
    global_risk_failure_codes: tuple[str, ...] = ()
    official_rates: OfficialRatesEvidenceBundle | None = None
    official_rates_failure_codes: tuple[str, ...] = ()
    ashare_derivatives: AShareDerivativesEvidenceBundle | None = None
    ashare_derivatives_failure_codes: tuple[str, ...] = ()
    cross_market_snapshot: CrossMarketSnapshot | None = None
    cross_market_failure_code: str | None = None
    cross_market_history: CrossMarketHistorySnapshot | None = None
    cross_market_history_failure_code: str | None = None
    calendar_verified: bool = False

    def __post_init__(self) -> None:
        _canonical_symbol(self.symbol)
        if self.history_start > self.latest_completed_session:
            raise ValueError("history_start must not follow latest_completed_session")
        if self.latest_completed_session >= self.next_session:
            raise ValueError("latest_completed_session must precede next_session")
        if self.as_of is not None:
            _require_aware(self.as_of, "as_of")
            if (
                self.market_evidence is not None
                and self.market_evidence.first_seen_at > self.as_of
            ):
                raise ValueError("market evidence was first seen after as_of")
        if (
            self.instrument_profile is not None
            and self.instrument_profile.symbol != self.canonical_symbol
        ):
            raise ValueError("instrument profile symbol must match request symbol")
        if any(not code.strip() for code in self.ashare_context_failure_codes):
            raise ValueError("A-share context failure codes must not be blank")
        if len(self.ashare_context_failure_codes) != len(
            set(self.ashare_context_failure_codes)
        ):
            raise ValueError("A-share context failure codes must be unique")
        if any(not code.strip() for code in self.ashare_breadth_failure_codes):
            raise ValueError("A-share breadth failure codes must not be blank")
        if len(self.ashare_breadth_failure_codes) != len(
            set(self.ashare_breadth_failure_codes)
        ):
            raise ValueError("A-share breadth failure codes must be unique")
        if any(not code.strip() for code in self.global_risk_failure_codes):
            raise ValueError("global-risk failure codes must not be blank")
        if len(self.global_risk_failure_codes) != len(
            set(self.global_risk_failure_codes)
        ):
            raise ValueError("global-risk failure codes must be unique")
        if any(not code.strip() for code in self.official_rates_failure_codes):
            raise ValueError("official-rates failure codes must not be blank")
        if len(self.official_rates_failure_codes) != len(
            set(self.official_rates_failure_codes)
        ):
            raise ValueError("official-rates failure codes must be unique")
        if any(not code.strip() for code in self.ashare_derivatives_failure_codes):
            raise ValueError("A-share derivatives failure codes must not be blank")
        if len(self.ashare_derivatives_failure_codes) != len(
            set(self.ashare_derivatives_failure_codes)
        ):
            raise ValueError("A-share derivatives failure codes must be unique")
        if (
            self.as_of is not None
            and self.ashare_context is not None
            and any(
                item.first_seen_at > self.as_of for item in self.ashare_context.items
            )
        ):
            raise ValueError("A-share context evidence was first seen after as_of")
        if (
            self.as_of is not None
            and self.ashare_breadth_snapshot is not None
            and self.ashare_breadth_snapshot.meta.available_at > self.as_of
        ):
            raise ValueError("A-share breadth snapshot was first seen after as_of")
        if (
            self.as_of is not None
            and self.ashare_breadth is not None
            and any(item.first_seen_at > self.as_of for item in self.ashare_breadth.items)
        ):
            raise ValueError("A-share breadth evidence was first seen after as_of")
        if (
            self.as_of is not None
            and self.global_risk is not None
            and any(item.first_seen_at > self.as_of for item in self.global_risk.items)
        ):
            raise ValueError("global-risk evidence was first seen after as_of")
        if (
            self.as_of is not None
            and self.official_rates is not None
            and any(item.first_seen_at > self.as_of for item in self.official_rates.items)
        ):
            raise ValueError("official-rate evidence was first seen after as_of")
        if (
            self.as_of is not None
            and self.ashare_derivatives is not None
            and any(
                item.first_seen_at > self.as_of
                for item in self.ashare_derivatives.items
            )
        ):
            raise ValueError("A-share derivatives evidence was first seen after as_of")
        if self.cross_market_snapshot is not None and self.cross_market_failure_code:
            raise ValueError("cross-market snapshot and failure code are mutually exclusive")
        if self.cross_market_failure_code is not None and not (
            self.cross_market_failure_code.strip()
        ):
            raise ValueError("cross_market_failure_code must not be blank")
        if self.cross_market_history is not None and self.cross_market_history_failure_code:
            raise ValueError(
                "cross-market history and its failure code are mutually exclusive"
            )
        if self.cross_market_history_failure_code is not None and not (
            self.cross_market_history_failure_code.strip()
        ):
            raise ValueError("cross_market_history_failure_code must not be blank")
        if (
            self.as_of is not None
            and self.cross_market_snapshot is not None
            and self.cross_market_snapshot.fetched_at > self.as_of
        ):
            raise ValueError("cross-market snapshot was fetched after as_of")
        if self.as_of is not None and self.cross_market_history is not None:
            if self.cross_market_history.as_of > self.as_of:
                raise ValueError("cross-market history cutoff was after as_of")
            if self.cross_market_history.fetched_at > self.as_of:
                raise ValueError("cross-market history was fetched after as_of")

    @property
    def canonical_symbol(self) -> str:
        return _canonical_symbol(self.symbol)


@dataclass(frozen=True, slots=True)
class AShareCloseMarketDataCollection:
    """在新闻/模型工作前捕获的一份不可变日线数据结果。"""

    bars: tuple[DailyBar, ...]
    fetched_at: datetime
    failure_code: str | None = None
    source_name: str | None = None

    def __post_init__(self) -> None:
        _require_aware(self.fetched_at, "fetched_at")
        if self.failure_code is not None and self.bars:
            raise ValueError("failed market-data collection cannot contain bars")
        if self.source_name is not None and not self.source_name.strip():
            raise ValueError("source_name must not be blank")


@dataclass(frozen=True, slots=True)
class AShareCloseAnalysisRun:
    assessment: CloseTechnicalAssessment
    recommendation: ResearchRecommendation
    evidence_selection: EvidenceSelection
    macro: MacroAnalysis | None
    daily_bar_count: int
    calendar_verified: bool
    baseline_macro: MacroAnalysis | None = None
    adversarial_macro: MacroAnalysis | None = None
    macro_selected_track: str | None = None
    macro_dual_audit_document: Mapping[str, object] | None = None
    macro_audit_record_sha256: str | None = None
    market_data_failure_code: str | None = None
    macro_failure_code: str | None = None
    ashare_context_failure_codes: tuple[str, ...] = ()
    ashare_context_report_lines: tuple[str, ...] = ()
    ashare_breadth_failure_codes: tuple[str, ...] = ()
    ashare_breadth_report_lines: tuple[str, ...] = ()
    global_risk_failure_codes: tuple[str, ...] = ()
    global_risk_report_lines: tuple[str, ...] = ()
    official_rates_failure_codes: tuple[str, ...] = ()
    official_rates_report_lines: tuple[str, ...] = ()
    ashare_derivatives_failure_codes: tuple[str, ...] = ()
    ashare_derivatives_report_lines: tuple[str, ...] = ()
    cross_market_failure_code: str | None = None
    cross_market_history_failure_code: str | None = None
    cross_market_report_lines: tuple[str, ...] = ()
    cross_market_relation_report_lines: tuple[str, ...] = ()
    notification: OutboundNotification | None = None
    notifications: tuple[OutboundNotification, ...] = ()
    notification_enqueued: bool = False


class AShareCloseAnalysisService:
    """收盘后组合已完成日线柱与有界新闻证据。"""

    def __init__(
        self,
        daily_data: AsyncHistoricalDailyData,
        *,
        macro_analyzer: MacroAnalyzer | None = None,
        technical_config: CloseAnalysisConfig | None = None,
        gate_config: RecommendationGateConfig | None = None,
        outbox: CloseAnalysisOutbox | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._daily_data = daily_data
        self._macro_analyzer = macro_analyzer
        self._technical_config = technical_config or CloseAnalysisConfig()
        self._gate_config = gate_config or RecommendationGateConfig()
        self._outbox = outbox
        self._clock = clock

    async def collect_market_data(
        self,
        request: AShareCloseAnalysisRequest,
    ) -> AShareCloseMarketDataCollection:
        """在任何可选模型调用前拉取未复权日线柱。"""

        failure_code: str | None = None
        source_name: str | None = None
        try:
            routed_fetch = getattr(
                self._daily_data,
                "fetch_daily_bars_async_with_route",
                None,
            )
            if callable(routed_fetch):
                route = await routed_fetch(
                    request.canonical_symbol,
                    request.history_start,
                    request.latest_completed_session,
                    adjustment=PriceAdjustment.NONE,
                )
                bars = tuple(route.bars)
                source_name = str(route.selected_source)
            else:
                bars = tuple(
                    await self._daily_data.fetch_daily_bars_async(
                        request.canonical_symbol,
                        request.history_start,
                        request.latest_completed_session,
                        adjustment=PriceAdjustment.NONE,
                    )
                )
        except MarketDataUnavailableError:
            bars = ()
            failure_code = "DAILY_MARKET_DATA_FETCH_FAILED"
        fetched_at = self._clock()
        _require_aware(fetched_at, "clock")
        return AShareCloseMarketDataCollection(
            bars=bars,
            fetched_at=fetched_at,
            failure_code=failure_code,
            source_name=source_name,
        )

    async def run_once(
        self,
        request: AShareCloseAnalysisRequest,
        *,
        notification_target: ResearchNotificationTarget | None = None,
    ) -> AShareCloseAnalysisRun:
        collection = await self.collect_market_data(request)
        return await self.evaluate_collection(
            request,
            collection,
            notification_target=notification_target,
        )

    async def evaluate_collection(
        self,
        request: AShareCloseAnalysisRequest,
        collection: AShareCloseMarketDataCollection,
        *,
        notification_target: ResearchNotificationTarget | None = None,
    ) -> AShareCloseAnalysisRun:
        """根据有界证据评估已经捕获的日线数据结果。"""

        inferred_as_of = collection.fetched_at
        if request.ashare_breadth_snapshot is not None:
            inferred_as_of = max(
                inferred_as_of,
                request.ashare_breadth_snapshot.meta.fetched_at,
            )
        if request.cross_market_snapshot is not None:
            inferred_as_of = max(
                inferred_as_of,
                request.cross_market_snapshot.fetched_at,
            )
        if request.cross_market_history is not None:
            inferred_as_of = max(
                inferred_as_of,
                request.cross_market_history.fetched_at,
            )
        for bundle in (
            request.ashare_context,
            request.ashare_breadth,
            request.global_risk,
            request.official_rates,
            request.ashare_derivatives,
        ):
            if bundle is not None and bundle.items:
                inferred_as_of = max(
                    inferred_as_of,
                    *(item.first_seen_at for item in bundle.items),
                )
        as_of = request.as_of or inferred_as_of
        assessment = self.assess_collection(request, collection, as_of=as_of)
        if request.ashare_breadth_snapshot is not None:
            assessment = _with_breadth_context(
                assessment,
                request.ashare_breadth_snapshot,
            )
        bars = collection.bars
        market_failure = collection.failure_code
        cross_market_bundle: CrossMarketEvidenceBundle | None = None
        if request.cross_market_snapshot is not None:
            cross_market_bundle = build_cross_market_evidence(
                request.cross_market_snapshot
            )
        cross_market_items = (
            cross_market_bundle.items if cross_market_bundle is not None else ()
        )
        cross_market_references = (
            cross_market_bundle.references if cross_market_bundle is not None else ()
        )
        cross_market_report_lines = (
            cross_market_bundle.report_lines if cross_market_bundle is not None else ()
        )
        relation_bundle: CrossMarketRelationEvidenceBundle | None = None
        relation_failure = request.cross_market_history_failure_code
        if request.cross_market_history is not None and bars:
            relation_bundle = build_cross_market_relation_evidence(
                bars,
                request.cross_market_history,
                as_of,
            )
        elif request.cross_market_history is not None:
            relation_failure = "CROSS_MARKET_RELATION_SKIPPED_DAILY_DATA_UNAVAILABLE"
        relation_items = relation_bundle.items if relation_bundle is not None else ()
        relation_references = (
            relation_bundle.references if relation_bundle is not None else ()
        )
        relation_report_lines = (
            relation_bundle.report_lines if relation_bundle is not None else ()
        )
        context_items = (
            request.ashare_context.items if request.ashare_context is not None else ()
        )
        context_references = (
            request.ashare_context.references
            if request.ashare_context is not None
            else ()
        )
        context_report_lines = (
            request.ashare_context.report_lines
            if request.ashare_context is not None
            else ()
        )
        breadth_items = (
            request.ashare_breadth.items if request.ashare_breadth is not None else ()
        )
        breadth_references = (
            request.ashare_breadth.references
            if request.ashare_breadth is not None
            else ()
        )
        breadth_report_lines = (
            request.ashare_breadth.report_lines
            if request.ashare_breadth is not None
            else ()
        )
        global_risk_items = (
            request.global_risk.items if request.global_risk is not None else ()
        )
        global_risk_references = (
            request.global_risk.references if request.global_risk is not None else ()
        )
        global_risk_report_lines = (
            request.global_risk.report_lines if request.global_risk is not None else ()
        )
        official_rate_items = (
            request.official_rates.items if request.official_rates is not None else ()
        )
        official_rate_references = (
            request.official_rates.references
            if request.official_rates is not None
            else ()
        )
        official_rate_report_lines = (
            request.official_rates.report_lines
            if request.official_rates is not None
            else ()
        )
        derivative_items = (
            request.ashare_derivatives.items
            if request.ashare_derivatives is not None
            else ()
        )
        derivative_references = (
            request.ashare_derivatives.references
            if request.ashare_derivatives is not None
            else ()
        )
        derivative_report_lines = (
            request.ashare_derivatives.report_lines
            if request.ashare_derivatives is not None
            else ()
        )

        macro: MacroAnalysis | None = None
        macro_failure: str | None = None
        baseline_macro: MacroAnalysis | None = None
        adversarial_macro: MacroAnalysis | None = None
        macro_selected_track: str | None = None
        macro_dual_audit_document: Mapping[str, object] | None = None
        macro_audit_record_sha256: str | None = None
        if assessment.decision is RecommendationDecision.ABSTAIN:
            selection = select_macro_evidence(
                request.canonical_symbol,
                as_of,
                request.events,
            )
            macro_failure = (
                "SKIPPED_MARKET_DATA_UNAVAILABLE"
                if market_failure is not None
                else "SKIPPED_TECHNICAL_ABSTAIN"
            )
        elif self._macro_analyzer is None:
            selection = select_macro_evidence(
                request.canonical_symbol,
                as_of,
                request.events,
            )
            macro_failure = "MACRO_DISABLED"
        else:
            macro_run = await MacroResearchService(self._macro_analyzer).analyze(
                symbol=request.canonical_symbol,
                as_of=as_of,
                horizon=f"next A-share session {request.next_session.isoformat()}",
                technical_summary=_technical_summary(
                    assessment,
                    request.market_evidence,
                    request.instrument_profile,
                ),
                events=request.events,
                additional_evidence=(
                    *_market_macro_evidence(
                        request.market_evidence,
                        assessment,
                        provider_name=collection.source_name,
                    ),
                    *context_items,
                    *breadth_items,
                    *global_risk_items,
                    *official_rate_items,
                    *derivative_items,
                    *cross_market_items,
                    *relation_items,
                ),
            )
            macro = macro_run.analysis
            baseline_macro = macro_run.baseline_analysis
            adversarial_macro = macro_run.adversarial_analysis
            macro_selected_track = macro_run.selected_track
            macro_dual_audit_document = macro_run.dual_audit_document
            macro_audit_record_sha256 = macro_run.dual_audit_record_sha256
            selection = macro_run.selection
            macro_failure = macro_run.failure_code

        technical = _as_technical_signal(assessment)
        expiry = datetime.combine(request.next_session, time(15, 0), tzinfo=SHANGHAI)
        if expiry <= as_of.astimezone(SHANGHAI):
            raise ValueError("next_session close must be after as_of")
        ttl = expiry.astimezone(UTC) - as_of.astimezone(UTC)
        recommendation = build_recommendation(
            technical,
            _recommendation_evidence(
                request.market_evidence,
                selection.references,
                (
                    *context_references,
                    *breadth_references,
                    *global_risk_references,
                    *official_rate_references,
                    *derivative_references,
                    *cross_market_references,
                    *relation_references,
                ),
            ),
            macro=macro,
            config=replace(self._gate_config, short_ttl=ttl, swing_ttl=ttl),
        )
        recommendation = replace(
            recommendation,
            analysis_mode="A_SHARE_AFTER_CLOSE",
            target_session=request.next_session,
            technical_metrics=assessment.metrics,
            instrument_profile=request.instrument_profile,
        )
        if request.ashare_context_failure_codes:
            recommendation = replace(
                recommendation,
                uncertainties=tuple(
                    dict.fromkeys(
                        (
                            *recommendation.uncertainties,
                            *request.ashare_context_failure_codes,
                        )
                    )
                ),
            )
        if request.ashare_breadth_failure_codes:
            recommendation = replace(
                recommendation,
                uncertainties=tuple(
                    dict.fromkeys(
                        (
                            *recommendation.uncertainties,
                            *request.ashare_breadth_failure_codes,
                        )
                    )
                ),
            )
        if request.global_risk_failure_codes:
            recommendation = replace(
                recommendation,
                uncertainties=tuple(
                    dict.fromkeys(
                        (
                            *recommendation.uncertainties,
                            *request.global_risk_failure_codes,
                        )
                    )
                ),
            )
        if request.official_rates_failure_codes:
            recommendation = replace(
                recommendation,
                uncertainties=tuple(
                    dict.fromkeys(
                        (
                            *recommendation.uncertainties,
                            *request.official_rates_failure_codes,
                        )
                    )
                ),
            )
        if request.ashare_derivatives_failure_codes:
            recommendation = replace(
                recommendation,
                uncertainties=tuple(
                    dict.fromkeys(
                        (
                            *recommendation.uncertainties,
                            *request.ashare_derivatives_failure_codes,
                        )
                    )
                ),
            )
        if request.cross_market_failure_code is not None:
            recommendation = replace(
                recommendation,
                uncertainties=tuple(
                    dict.fromkeys(
                        (
                            *recommendation.uncertainties,
                            request.cross_market_failure_code,
                        )
                    )
                ),
            )
        if relation_failure is not None:
            recommendation = replace(
                recommendation,
                uncertainties=tuple(
                    dict.fromkeys((*recommendation.uncertainties, relation_failure))
                ),
            )
        if not request.calendar_verified:
            recommendation = replace(
                recommendation,
                uncertainties=tuple(
                    dict.fromkeys(
                        (*recommendation.uncertainties, "NEXT_SESSION_DATE_UNVERIFIED")
                    )
                ),
            )

        if (
            notification_target is None
            or recommendation.decision not in notification_target.decisions
        ):
            return AShareCloseAnalysisRun(
                assessment=assessment,
                recommendation=recommendation,
                evidence_selection=selection,
                macro=macro,
                daily_bar_count=len(bars),
                calendar_verified=request.calendar_verified,
                baseline_macro=baseline_macro,
                adversarial_macro=adversarial_macro,
                macro_selected_track=macro_selected_track,
                macro_dual_audit_document=macro_dual_audit_document,
                macro_audit_record_sha256=macro_audit_record_sha256,
                market_data_failure_code=market_failure,
                macro_failure_code=macro_failure,
                ashare_context_failure_codes=request.ashare_context_failure_codes,
                ashare_context_report_lines=context_report_lines,
                ashare_breadth_failure_codes=request.ashare_breadth_failure_codes,
                ashare_breadth_report_lines=breadth_report_lines,
                global_risk_failure_codes=request.global_risk_failure_codes,
                global_risk_report_lines=global_risk_report_lines,
                official_rates_failure_codes=request.official_rates_failure_codes,
                official_rates_report_lines=official_rate_report_lines,
                ashare_derivatives_failure_codes=(
                    request.ashare_derivatives_failure_codes
                ),
                ashare_derivatives_report_lines=derivative_report_lines,
                cross_market_failure_code=request.cross_market_failure_code,
                cross_market_history_failure_code=relation_failure,
                cross_market_report_lines=cross_market_report_lines,
                cross_market_relation_report_lines=relation_report_lines,
            )
        if self._outbox is None:
            raise RuntimeError("notification_target requires an explicitly configured outbox")
        notifications = format_close_analysis_notifications(
            recommendation,
            assessment,
            macro,
            notification_target,
            calendar_verified=request.calendar_verified,
            ashare_context_report_lines=context_report_lines,
            ashare_context_failure_codes=request.ashare_context_failure_codes,
            ashare_breadth_report_lines=breadth_report_lines,
            ashare_breadth_failure_codes=request.ashare_breadth_failure_codes,
            global_risk_report_lines=global_risk_report_lines,
            global_risk_failure_codes=request.global_risk_failure_codes,
            official_rates_report_lines=official_rate_report_lines,
            official_rates_failure_codes=request.official_rates_failure_codes,
            ashare_derivatives_report_lines=derivative_report_lines,
            ashare_derivatives_failure_codes=(
                request.ashare_derivatives_failure_codes
            ),
            cross_market_report_lines=cross_market_report_lines,
            cross_market_failure_code=request.cross_market_failure_code,
            cross_market_relation_report_lines=relation_report_lines,
            cross_market_history_failure_code=relation_failure,
            evidence_selection=selection,
            baseline_macro=baseline_macro,
            adversarial_macro=adversarial_macro,
            selected_track=macro_selected_track,
            audit_record_sha256=macro_audit_record_sha256,
        )
        for notification in notifications:
            self._outbox.enqueue(notification)
        return AShareCloseAnalysisRun(
            assessment=assessment,
            recommendation=recommendation,
            evidence_selection=selection,
            macro=macro,
            daily_bar_count=len(bars),
            calendar_verified=request.calendar_verified,
            baseline_macro=baseline_macro,
            adversarial_macro=adversarial_macro,
            macro_selected_track=macro_selected_track,
            macro_dual_audit_document=macro_dual_audit_document,
            macro_audit_record_sha256=macro_audit_record_sha256,
            market_data_failure_code=market_failure,
            macro_failure_code=macro_failure,
            ashare_context_failure_codes=request.ashare_context_failure_codes,
            ashare_context_report_lines=context_report_lines,
            ashare_breadth_failure_codes=request.ashare_breadth_failure_codes,
            ashare_breadth_report_lines=breadth_report_lines,
            global_risk_failure_codes=request.global_risk_failure_codes,
            global_risk_report_lines=global_risk_report_lines,
            official_rates_failure_codes=request.official_rates_failure_codes,
            official_rates_report_lines=official_rate_report_lines,
            ashare_derivatives_failure_codes=(
                request.ashare_derivatives_failure_codes
            ),
            ashare_derivatives_report_lines=derivative_report_lines,
            cross_market_failure_code=request.cross_market_failure_code,
            cross_market_history_failure_code=relation_failure,
            cross_market_report_lines=cross_market_report_lines,
            cross_market_relation_report_lines=relation_report_lines,
            notification=notifications[0],
            notifications=notifications,
            notification_enqueued=True,
        )

    def assess_collection(
        self,
        request: AShareCloseAnalysisRequest,
        collection: AShareCloseMarketDataCollection,
        *,
        as_of: datetime | None = None,
    ) -> CloseTechnicalAssessment:
        """只构建确定性评估，不读取 API 密钥。"""

        decision_time = as_of or request.as_of or collection.fetched_at
        _require_aware(decision_time, "as_of")
        if collection.fetched_at > decision_time:
            raise ValueError("market data was fetched after the decision as_of")
        if collection.failure_code is not None:
            return _market_failure_assessment(
                request,
                decision_time,
                self._technical_config,
            )
        return build_close_technical_assessment(
            request.canonical_symbol,
            collection.bars,
            as_of=decision_time,
            latest_completed_session=request.latest_completed_session,
            next_session=request.next_session,
            is_currently_held=request.is_currently_held,
            instrument_type=request.instrument_type,
            config=self._technical_config,
        )


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


def _market_failure_assessment(
    request: AShareCloseAnalysisRequest,
    as_of: datetime,
    config: CloseAnalysisConfig,
) -> CloseTechnicalAssessment:
    return CloseTechnicalAssessment(
        symbol=request.canonical_symbol,
        as_of=as_of,
        next_session=request.next_session,
        latest_trade_date=None,
        horizon=_close_horizon(),
        decision=RecommendationDecision.ABSTAIN,
        score=Decimal(0),
        reference_price=None,
        invalidation_price=None,
        reason_codes=("DAILY_MARKET_DATA_FETCH_FAILED",),
        trading_sessions_used=0,
        strategy_version=config.strategy_version,
        metrics=(),
    )


def _close_horizon() -> RecommendationHorizon:
    return RecommendationHorizon.SHORT_1_TO_5_DAYS


def _canonical_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if len(value) == 6 and value.isdigit():
        if value.startswith(("4", "8", "92")):
            exchange = "BJ"
        else:
            exchange = "SH" if value.startswith(("5", "6", "9")) else "SZ"
        return f"{value}.{exchange}"
    if len(value) == 9 and value[6] == ".":
        code, exchange = value.split(".", maxsplit=1)
        if code.isdigit() and exchange in {"SH", "SZ", "BJ"}:
            return value
    raise ValueError("symbol must look like 600000.SH, 000001.SZ, or 430047.BJ")


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


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
