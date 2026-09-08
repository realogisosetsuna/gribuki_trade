"""面向下一明确有界交易日的 A 股盘后分析。"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime, time
from decimal import Decimal
from typing import Protocol
from zoneinfo import ZoneInfo

import gribuki_trade.services.ashare.ashare_close_notifications as _close_notifications
import gribuki_trade.services.ashare.ashare_close_projection as _close_projection
from gribuki_trade.analysis.schemas import MacroAnalysis
from gribuki_trade.domain.market import PriceAdjustment
from gribuki_trade.domain.recommendations import (
    RecommendationDecision,
    RecommendationHorizon,
)
from gribuki_trade.features.close_analysis import (
    CloseAnalysisConfig,
    CloseTechnicalAssessment,
    build_close_technical_assessment,
)
from gribuki_trade.policy.recommendation_gate import (
    RecommendationGateConfig,
    build_recommendation,
)
from gribuki_trade.ports.llm_analyzer import MacroAnalyzer
from gribuki_trade.ports.market_data import (
    AsyncHistoricalDailyData,
    MarketDataUnavailableError,
)
from gribuki_trade.ports.notifier import OutboundNotification
from gribuki_trade.services.ashare.ashare_close_models import (
    AShareCloseAnalysisRequest,
    AShareCloseAnalysisRun,
    AShareCloseMarketDataCollection,
)
from gribuki_trade.services.ashare.ashare_close_models import (
    require_aware as _require_aware,
)
from gribuki_trade.services.ashare.ashare_close_notifications import (
    format_close_analysis_notifications,
)
from gribuki_trade.services.ashare.ashare_close_projection import (
    _as_technical_signal,
    _market_macro_evidence,
    _recommendation_evidence,
    _technical_summary,
    _with_breadth_context,
)
from gribuki_trade.services.ashare.ashare_research import (
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
from gribuki_trade.services.macro_research import (
    MacroResearchService,
    select_macro_evidence,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")

# 保留历史私有辅助函数导入，兼容现有测试和集成调用。
_display_decimal = _close_projection._display_decimal
_display_percent = _close_projection._display_percent
_display_score = _close_projection._display_score
_humanize_model_text = _close_projection._humanize_model_text
_risk_metric_lines = _close_projection._risk_metric_lines
_single_line = _close_projection._single_line
_split_notification_text = _close_notifications._split_notification_text
_track_evidence_coverage = _close_projection._track_evidence_coverage
format_close_analysis_notification = _close_notifications.format_close_analysis_notification
_contractualize_close_analysis_report = (
    _close_notifications._contractualize_close_analysis_report
)
_split_contractual_instrument_report = (
    _close_notifications._split_contractual_instrument_report
)
_split_plain_text_payload = _close_notifications._split_plain_text_payload
_dual_track_report_lines = _close_notifications._dual_track_report_lines
_fusion_summary_lines = _close_projection._fusion_summary_lines
_horizon_view_line = _close_projection._horizon_view_line
_technical_metric_lines = _close_projection._technical_metric_lines
_report_evidence = _close_projection._report_evidence
_format_evidence_citations = _close_projection._format_evidence_citations
_evidence_destination = _close_projection._evidence_destination
_evidence_display_title = _close_projection._evidence_display_title


class CloseAnalysisOutbox(Protocol):
    def enqueue(self, notification: OutboundNotification) -> object: ...


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


__all__ = [
    "AShareCloseAnalysisRequest",
    "AShareCloseAnalysisRun",
    "AShareCloseMarketDataCollection",
    "AShareCloseAnalysisService",
    "format_close_analysis_notification",
    "format_close_analysis_notifications",
]
