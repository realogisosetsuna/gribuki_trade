from __future__ import annotations

import asyncio
import re
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.analysis.schemas import (
    EvidenceItem,
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
    MacroClaim,
    MacroScenario,
)
from gribuki_trade.domain.events import NormalizedEvent, SourceTier
from gribuki_trade.domain.instruments import ResearchInstrumentProfile
from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.domain.recommendations import (
    EvidenceReference,
    RecommendationDecision,
)
from gribuki_trade.ports.cross_market import (
    CrossMarketQuote,
    CrossMarketSegment,
    CrossMarketSnapshot,
)
from gribuki_trade.ports.cross_market_history import (
    CrossMarketHistoryObservation,
    CrossMarketHistorySeries,
    CrossMarketHistorySnapshot,
)
from gribuki_trade.ports.market_data import MarketDataTimeoutError
from gribuki_trade.ports.notifier import NotificationTargetKind, OutboundNotification
from gribuki_trade.reporting.contracts import (
    ReportKind,
    validate_text_report_contract,
)
from gribuki_trade.services import (
    AShareCloseAnalysisRequest,
    AShareCloseAnalysisService,
    AShareContextEvidenceBundle,
    AShareDerivativesEvidenceBundle,
    GlobalRiskEvidenceBundle,
    ResearchNotificationTarget,
)
from gribuki_trade.services.ashare.close.ashare_close_analysis import (
    _risk_metric_lines,
    _split_notification_text,
    _track_evidence_coverage,
)

NOW = datetime(2026, 8, 13, 8, 30, tzinfo=UTC)
LATEST = date(2026, 8, 13)
NEXT = date(2026, 8, 14)


class FakeDailyData:
    def __init__(self, bars: tuple[DailyBar, ...]) -> None:
        self.bars = bars
        self.calls: list[tuple[object, ...]] = []

    async def fetch_daily_bars_async(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> tuple[DailyBar, ...]:
        self.calls.append((symbol, start, end, adjustment))
        return self.bars


class FailingDailyData(FakeDailyData):
    async def fetch_daily_bars_async(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> tuple[DailyBar, ...]:
        self.calls.append((symbol, start, end, adjustment))
        raise MarketDataTimeoutError("provider detail must not escape")


class RecordingAnalyzer:
    def __init__(self) -> None:
        self.requests: list[MacroAnalysisRequest] = []

    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        self.requests.append(request)
        return MacroAnalysis(
            analysis_id=request.analysis_id,
            as_of=request.as_of,
            decision=MacroAnalysisDecision.WATCH,
            regime="neutral",
            technical_alignment=Decimal("0.4"),
            macro_impact=Decimal("0.1"),
            scenarios=(),
            claims=(),
            uncertainties=(),
            data_gaps=(),
            invalidation_conditions=("new material disclosure",),
            reported_confidence="UNCALIBRATED",
            refusal_reason="",
            model_version="fake-macro@1",
        )


class EvidenceCitingAnalyzer(RecordingAnalyzer):
    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        self.requests.append(request)
        first_id = request.evidence[0].evidence_id
        last_id = request.evidence[-1].evidence_id
        return MacroAnalysis(
            analysis_id=request.analysis_id,
            as_of=request.as_of,
            decision=MacroAnalysisDecision.WATCH,
            regime="neutral",
            technical_alignment=Decimal("0.4"),
            macro_impact=Decimal("0.1"),
            scenarios=(
                MacroScenario(
                    "基准情景",
                    Decimal("1"),
                    ("观察量价确认",),
                    (last_id, first_id),
                ),
            ),
            claims=(
                MacroClaim(
                    "市场与事件证据方向不一致。",
                    (first_id, last_id),
                    ("反向证据仍然存在",),
                ),
            ),
            uncertainties=(),
            data_gaps=(),
            invalidation_conditions=("new material disclosure",),
            reported_confidence="UNCALIBRATED",
            refusal_reason="",
            model_version="fake-macro@1",
        )


class FakeOutbox:
    def __init__(self) -> None:
        self.items: list[OutboundNotification] = []

    def enqueue(self, notification: OutboundNotification) -> bool:
        self.items.append(notification)
        return True


def _trading_dates(count: int) -> tuple[date, ...]:
    output: list[date] = []
    current = LATEST
    while len(output) < count:
        if current.weekday() < 5:
            output.append(current)
        current -= timedelta(days=1)
    return tuple(reversed(output))


def _daily_bars(count: int = 220) -> tuple[DailyBar, ...]:
    dates = _trading_dates(count)
    output: list[DailyBar] = []
    previous: Decimal | None = None
    for index, trade_date in enumerate(dates):
        close = Decimal("10") + Decimal(index) * Decimal("0.02")
        baseline = previous or close - Decimal("0.02")
        output.append(
            DailyBar(
                symbol="510300.SH",
                trade_date=trade_date,
                open=baseline,
                high=close + Decimal("0.05"),
                low=min(baseline, close) - Decimal("0.05"),
                close=close,
                previous_close=baseline,
                volume=250_000 if index == count - 1 else 100_000,
                amount=close * Decimal("100000"),
                turnover_percent=Decimal("1"),
                is_trading=True,
                is_st=False,
                adjustment=PriceAdjustment.NONE,
            )
        )
        previous = close
    return tuple(output)


def _event() -> NormalizedEvent:
    return NormalizedEvent(
        source_id="official.test",
        canonical_url="https://example.com/market-event",
        title="沪深300相关政策信息",
        summary="公开市场信息摘要",
        event_type="market_news",
        source_tier=SourceTier.OFFICIAL,
        first_seen_at=NOW - timedelta(hours=1),
        retrieved_at=NOW - timedelta(hours=1),
        available_at=NOW - timedelta(hours=1),
        published_at=NOW - timedelta(hours=2),
        entities=("510300",),
    )


def _market_evidence() -> EvidenceReference:
    return EvidenceReference(
        evidence_id="daily-bars-snapshot",
        title="retained daily bars",
        canonical_url="local://market-evidence/daily-bars-snapshot",
        published_at=NOW - timedelta(hours=1),
        first_seen_at=NOW - timedelta(minutes=1),
        source_tier=2,
    )


def _ashare_context_bundle() -> AShareContextEvidenceBundle:
    item = EvidenceItem(
        evidence_id="fr007-context-evidence",
        publisher="chinabond.chinamoney",
        source_tier=2,
        published_at=NOW - timedelta(minutes=2),
        first_seen_at=NOW - timedelta(minutes=1),
        title="中国货币网FR定盘利率",
        excerpt="FR007=1.425%；这是定盘利率，不是DR007。",
        canonical_url="https://www.chinamoney.com.cn/chinese/bkfrr/",
        content_hash="a" * 64,
    )
    reference = EvidenceReference(
        evidence_id=item.evidence_id,
        title=item.title,
        canonical_url=item.canonical_url,
        published_at=item.published_at,
        first_seen_at=item.first_seen_at,
        source_tier=item.source_tier,
    )
    return AShareContextEvidenceBundle(
        items=(item,),
        references=(reference,),
        report_lines=("FR007定盘利率：1.425%；不是DR007。",),
    )


def _global_risk_bundle() -> GlobalRiskEvidenceBundle:
    item = EvidenceItem(
        evidence_id="cboe-vix-context-evidence",
        publisher="CBOE_VIX_DAILY_HISTORY",
        source_tier=1,
        published_at=NOW - timedelta(hours=10),
        first_seen_at=NOW - timedelta(minutes=1),
        title="Cboe官方VIX日线｜2026-08-12",
        excerpt="Cboe官方VIX EOD收盘=14.550。",
        canonical_url="https://cdn.cboe.com/vix.csv",
        content_hash="b" * 64,
    )
    reference = EvidenceReference(
        evidence_id=item.evidence_id,
        title=item.title,
        canonical_url=item.canonical_url,
        published_at=item.published_at,
        first_seen_at=item.first_seen_at,
        source_tier=item.source_tier,
    )
    return GlobalRiskEvidenceBundle(
        items=(item,),
        references=(reference,),
        report_lines=("Cboe官方VIX日线（2026-08-12）：收=14.550。",),
    )


def _derivatives_bundle() -> AShareDerivativesEvidenceBundle:
    item = EvidenceItem(
        evidence_id="sse-official-etf-shares-evidence",
        publisher="SSE_ETF_POST_SETTLEMENT_TOTAL_SHARES",
        source_tier=1,
        published_at=NOW - timedelta(minutes=2),
        first_seen_at=NOW - timedelta(minutes=1),
        title="上交所官方ETF总份额｜510300｜2026-08-12",
        excerpt="总份额为结算后水平，不是申购赎回净流量。",
        canonical_url="https://query.sse.com.cn/commonQuery.do",
        content_hash="c" * 64,
    )
    reference = EvidenceReference(
        evidence_id=item.evidence_id,
        title=item.title,
        canonical_url=item.canonical_url,
        published_at=item.published_at,
        first_seen_at=item.first_seen_at,
        source_tier=item.source_tier,
    )
    return AShareDerivativesEvidenceBundle(
        items=(item,),
        references=(reference,),
        report_lines=("上交所ETF官方份额：总份额=24,168,187,700.000份。",),
    )


def _request(**changes: object) -> AShareCloseAnalysisRequest:
    values: dict[str, object] = {
        "symbol": "510300.SH",
        "history_start": date(2025, 1, 1),
        "latest_completed_session": LATEST,
        "next_session": NEXT,
        "events": (_event(),),
        "market_evidence": _market_evidence(),
        "as_of": NOW,
        "calendar_verified": True,
        "instrument_profile": ResearchInstrumentProfile(
            symbol="510300.SH",
            name="华泰柏瑞沪深300ETF",
            market="A股",
            asset_type="etf",
            exchange="sse",
            board="sse_etf",
            size_tier="mega",
            industry="宽基指数",
            styles=("大盘", "核心资产"),
            research_role="沪深300系统性风险与大盘风格代理",
            risk_tags=("市场风险", "跟踪误差"),
            source_id="test-profile",
            verified_on=LATEST,
            background_facts=("标的指数：沪深300指数",),
        ),
    }
    values.update(changes)
    return AShareCloseAnalysisRequest(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [
        ("430047.BJ", "430047.BJ"),
        ("920001", "920001.BJ"),
    ],
)
def test_close_request_canonicalizes_bse_symbols(symbol: str, expected: str) -> None:
    base = _request()
    assert base.instrument_profile is not None
    profile = replace(
        base.instrument_profile,
        symbol=expected,
        exchange="bse",
        board="bse",
    )

    request = _request(symbol=symbol, instrument_profile=profile)

    assert request.canonical_symbol == expected


def _cross_market_snapshot() -> CrossMarketSnapshot:
    fetched_at = NOW - timedelta(seconds=10)
    return CrossMarketSnapshot(
        fetched_at=fetched_at,
        provider="AKShare/Eastmoney index_global_spot_em",
        quotes=(
            CrossMarketQuote(
                instrument_id="CBOE_VIX",
                display_name="CBOE波动率指数（美股隐含波动率）",
                segment=CrossMarketSegment.DOLLAR_COMMODITY_RISK,
                provider_code="VIX",
                provider_name="VIX恐慌指数",
                last=Decimal("14.5678"),
                change_percent=Decimal("1.2345"),
                change_amount=Decimal("0.1789"),
                open=Decimal("14.4000"),
                high=Decimal("14.7000"),
                low=Decimal("14.3000"),
                previous_close=Decimal("14.3899"),
                amplitude_percent=Decimal("2.7800"),
                local_quote_time=fetched_at - timedelta(minutes=1),
                local_timezone="America/Chicago",
                fetched_at=fetched_at,
                provider="AKShare/Eastmoney index_global_spot_em",
                stale=False,
                degraded=False,
            ),
        ),
        missing=(),
        degraded=False,
    )


def _cross_market_history() -> CrossMarketHistorySnapshot:
    observations = tuple(
        CrossMarketHistoryObservation(
            market_id="S_AND_P_500",
            session_date=trade_date,
            close=Decimal("4000") + Decimal(index),
            available_at=datetime.combine(
                trade_date,
                datetime.min.time(),
                tzinfo=UTC,
            )
            + timedelta(hours=7),
            source="AKShare/index_us_stock_sina:.INX",
            local_timezone="Asia/Shanghai",
        )
        for index, trade_date in enumerate(_trading_dates(180))
    )
    return CrossMarketHistorySnapshot(
        as_of=NOW - timedelta(seconds=10),
        fetched_at=NOW - timedelta(seconds=5),
        minimum_observations=130,
        series=(
            CrossMarketHistorySeries(
                market_id="S_AND_P_500",
                display_name="标普500指数",
                observations=observations,
            ),
        ),
        missing=(),
        degraded=False,
    )


def test_close_service_uses_real_metrics_for_macro_and_persists_metadata() -> None:
    analyzer = RecordingAnalyzer()
    provider = FakeDailyData(_daily_bars())
    service = AShareCloseAnalysisService(
        provider,
        macro_analyzer=analyzer,
        clock=lambda: NOW,
    )

    run = asyncio.run(service.run_once(_request()))

    assert provider.calls[0][-1] is PriceAdjustment.NONE
    assert run.assessment.decision is RecommendationDecision.WATCH
    assert run.recommendation.decision is RecommendationDecision.WATCH
    assert run.recommendation.analysis_mode == "A_SHARE_AFTER_CLOSE"
    assert run.recommendation.target_session == NEXT
    assert dict(run.recommendation.technical_metrics)["ma_20"] > 0
    assert run.recommendation.expires_at.date() == NEXT
    assert len(run.recommendation.evidence) == 2
    assert len(analyzer.requests) == 1
    summary = "\n".join(analyzer.requests[0].technical_summary)
    assert "ma_20=" in summary
    assert "target next session=2026-08-14" in summary
    assert "market evidence id=daily-bars-snapshot" in summary
    assert analyzer.requests[0].evidence[0].evidence_id == "daily-bars-snapshot"


def test_market_failure_abstains_without_invoking_macro() -> None:
    analyzer = RecordingAnalyzer()
    service = AShareCloseAnalysisService(
        FailingDailyData(()),
        macro_analyzer=analyzer,
        clock=lambda: NOW,
    )

    run = asyncio.run(service.run_once(_request(market_evidence=None)))

    assert run.recommendation.decision is RecommendationDecision.ABSTAIN
    assert run.market_data_failure_code == "DAILY_MARKET_DATA_FETCH_FAILED"
    assert run.macro_failure_code == "SKIPPED_MARKET_DATA_UNAVAILABLE"
    assert analyzer.requests == []
    assert "provider detail" not in repr(run)


def test_close_report_can_enqueue_abstain_explicitly() -> None:
    outbox = FakeOutbox()
    service = AShareCloseAnalysisService(
        FakeDailyData(_daily_bars(20)),
        outbox=outbox,
        clock=lambda: NOW,
    )
    target = ResearchNotificationTarget(
        target_id="1000000001",
        target_kind=NotificationTargetKind.PRIVATE,
        decisions=frozenset(RecommendationDecision),
    )

    run = asyncio.run(service.run_once(_request(), notification_target=target))

    assert run.recommendation.decision is RecommendationDecision.ABSTAIN
    assert run.notification_enqueued is True
    assert len(outbox.items) == 1
    assert outbox.items[0].text.startswith("【A股｜收盘研究分析】")
    assert "1000000001" not in outbox.items[0].idempotency_key


def test_collection_after_as_of_is_rejected() -> None:
    from gribuki_trade.services import AShareCloseMarketDataCollection

    service = AShareCloseAnalysisService(FakeDailyData(_daily_bars()))
    collection = AShareCloseMarketDataCollection(
        bars=_daily_bars(),
        fetched_at=NOW + timedelta(seconds=1),
    )
    with pytest.raises(ValueError, match="fetched after"):
        service.assess_collection(_request(), collection)


def test_close_report_includes_profile_cross_market_and_three_decimal_metrics() -> None:
    analyzer = RecordingAnalyzer()
    outbox = FakeOutbox()
    service = AShareCloseAnalysisService(
        FakeDailyData(_daily_bars()),
        macro_analyzer=analyzer,
        outbox=outbox,
        clock=lambda: NOW,
    )
    target = ResearchNotificationTarget(
        target_id="1000000001",
        target_kind=NotificationTargetKind.PRIVATE,
        decisions=frozenset(RecommendationDecision),
        max_characters=7_500,
    )

    run = asyncio.run(
        service.run_once(
            _request(cross_market_snapshot=_cross_market_snapshot()),
            notification_target=target,
        )
    )

    text = run.notification.text if run.notification is not None else ""
    assert text.startswith("【A股｜收盘研究分析】\n")
    assert "仅供研究" not in text
    assert "不会自动下单" not in text
    assert "名称：华泰柏瑞沪深300ETF" in text
    assert "行业或指数类别：宽基指数" in text
    assert "背景资料：标的指数：沪深300指数" in text
    assert "六、跨市场观测" in text
    assert "CBOE波动率指数（美股隐含波动率）：14.568（+1.235%）" in text
    assert "三、多因子技术分析" in text
    assert "相对强弱与市场广度：数据不可用，未计入评分" in text
    assert "唐奇安价格通道" in text
    assert re.search(r"短线（1至5个交易日）诊断分：-?\d+\.\d{3}", text)
    assert re.search(r"波段（2至8周）诊断分：-?\d+\.\d{3}", text)
    assert "当前不是周K确认" in text
    assert re.search(r"Wilder趋势强度：ADX14 \d+\.\d{3}", text)
    assert "+DI14/-DI14" in text
    assert "ADX只描述趋势强弱，不判断上涨或下跌，也不进入方向评分" in text
    assert re.search(r"技术面评分：-?\d+\.\d{3}", text)
    assert re.search(r"宏观面评分：-?\d+\.\d{3}", text)
    assert re.search(r"综合研究评分：-?\d+\.\d{3}", text)
    assert "评分融合：本次实际技术面权重 100.000%、宏观面权重 0.000%" in text
    assert "宏观不能把未通过技术门槛的标的单独升级为入场候选" in text
    assert "宏观证据覆盖率" in text
    assert all("." not in item or len(item.rsplit(".", 1)[-1]) == 3 for item in (
        text.split("参考收盘价：", 1)[1].splitlines()[0],
        text.split("结构失效参考：", 1)[1].splitlines()[0],
    ))
    assert any(
        item.publisher == "AKShare/Eastmoney index_global_spot_em"
        for item in analyzer.requests[0].evidence
    )


def test_risk_metrics_do_not_present_missing_turnover_as_real_ratio() -> None:
    text = "\n".join(
        _risk_metric_lines(
            {
                "turnover_state_20": Decimal("1"),
                "turnover_coverage_20": Decimal("0.04762"),
                "amihud_bps_per_cny_billion_20": Decimal("17.7356"),
                "amihud_bps_per_cny_billion_60": Decimal("22.9816"),
                "amihud_coverage_20": Decimal("1"),
                "amihud_coverage_60": Decimal("1"),
                "illiquidity_state_20_60": Decimal("0.77172"),
            }
        )
    )

    assert "换手率状态：数据不足（有效覆盖 4.762%）" in text
    assert "未把缺失时的中性占位值解读为真实比值" in text
    assert "20日 17.736、60日 22.982 基点/10亿元成交额" in text
    assert "原值统一 ×" not in text


def test_close_report_replaces_raw_evidence_ids_with_numbered_index() -> None:
    analyzer = EvidenceCitingAnalyzer()
    service = AShareCloseAnalysisService(
        FakeDailyData(_daily_bars()),
        macro_analyzer=analyzer,
        outbox=FakeOutbox(),
        clock=lambda: NOW,
    )
    target = ResearchNotificationTarget(
        target_id="1000000001",
        target_kind=NotificationTargetKind.PRIVATE,
        decisions=frozenset(RecommendationDecision),
        max_characters=7_500,
    )

    run = asyncio.run(
        service.run_once(
            _request(cross_market_snapshot=_cross_market_snapshot()),
            notification_target=target,
        )
    )

    assert run.macro is not None
    raw_ids = tuple(
        dict.fromkeys(
            evidence_id
            for claim in run.macro.claims
            for evidence_id in claim.evidence_ids
        )
    )
    text = "\n".join(item.text for item in run.notifications)
    assert "[证据1、2]" in text
    assert "- [证据1] 510300.SH 未复权日线与技术计算输入" in text
    assert "- [证据2] " in text
    assert "MIXED/TAIL_STITCH" not in text
    assert "来源：本地证据库（完整内容与校验指纹已留存）" in text
    assert "评分融合：本次实际技术面权重 87.500%、宏观面权重 12.500%" in text
    assert all(evidence_id not in text for evidence_id in raw_ids)


def test_model_prose_cannot_echo_raw_evidence_hashes() -> None:
    from gribuki_trade.services.ashare.close.ashare_close_analysis import _humanize_model_text

    known = "a" * 64
    unknown = "b" * 64

    rendered = _humanize_model_text(
        f"known={known}; unknown={unknown}",
        {known: 3},
    )

    assert rendered == "known=[证据3]; unknown=[未匹配证据]"
    assert known not in rendered
    assert unknown not in rendered


def test_close_service_adds_pit_cross_market_relations_to_evidence_and_report() -> None:
    analyzer = RecordingAnalyzer()
    outbox = FakeOutbox()
    service = AShareCloseAnalysisService(
        FakeDailyData(_daily_bars()),
        macro_analyzer=analyzer,
        outbox=outbox,
        clock=lambda: NOW,
    )
    target = ResearchNotificationTarget(
        target_id="1000000001",
        target_kind=NotificationTargetKind.PRIVATE,
        decisions=frozenset(RecommendationDecision),
        max_characters=7_500,
    )

    run = asyncio.run(
        service.run_once(
            _request(cross_market_history=_cross_market_history()),
            notification_target=target,
        )
    )

    assert run.cross_market_history_failure_code is None
    assert any("标普500指数" in line for line in run.cross_market_relation_report_lines)
    assert any(
        item.title == "跨市场历史关系｜标普500指数"
        for item in run.recommendation.evidence
    )
    assert any(
        item.title == "跨市场历史关系｜标普500指数"
        for item in analyzer.requests[0].evidence
    )
    assert run.notification is not None
    assert "跨市场历史关系" in run.notification.text
    assert "相关不代表因果" in run.notification.text


def test_close_service_adds_ashare_context_to_model_evidence_and_report() -> None:
    analyzer = RecordingAnalyzer()
    service = AShareCloseAnalysisService(
        FakeDailyData(_daily_bars()),
        macro_analyzer=analyzer,
        outbox=FakeOutbox(),
        clock=lambda: NOW,
    )
    target = ResearchNotificationTarget(
        target_id="1000000001",
        target_kind=NotificationTargetKind.PRIVATE,
        decisions=frozenset(RecommendationDecision),
        max_characters=7_500,
    )

    run = asyncio.run(
        service.run_once(
            _request(ashare_context=_ashare_context_bundle()),
            notification_target=target,
        )
    )

    assert run.ashare_context_report_lines == (
        "FR007定盘利率：1.425%；不是DR007。",
    )
    assert any(
        item.evidence_id == "fr007-context-evidence"
        for item in analyzer.requests[0].evidence
    )
    assert any(
        item.evidence_id == "fr007-context-evidence"
        for item in run.recommendation.evidence
    )
    assert run.notification is not None
    assert "A股资金面、ETF与股指期货上下文" in run.notification.text
    assert "FR007定盘利率：1.425%；不是DR007。" in run.notification.text


def test_close_service_adds_official_vix_to_model_evidence_and_report() -> None:
    analyzer = RecordingAnalyzer()
    service = AShareCloseAnalysisService(
        FakeDailyData(_daily_bars()),
        macro_analyzer=analyzer,
        outbox=FakeOutbox(),
        clock=lambda: NOW,
    )
    target = ResearchNotificationTarget(
        target_id="1000000001",
        target_kind=NotificationTargetKind.PRIVATE,
        decisions=frozenset(RecommendationDecision),
        max_characters=7_500,
    )

    run = asyncio.run(
        service.run_once(
            _request(global_risk=_global_risk_bundle()),
            notification_target=target,
        )
    )

    assert run.global_risk_report_lines == (
        "Cboe官方VIX日线（2026-08-12）：收=14.550。",
    )
    assert any(
        item.evidence_id == "cboe-vix-context-evidence"
        for item in analyzer.requests[0].evidence
    )
    assert run.notification is not None
    assert "Cboe官方VIX日线（2026-08-12）：收=14.550。" in run.notification.text


def test_close_service_keeps_derivative_data_and_partial_failure_visible() -> None:
    analyzer = RecordingAnalyzer()
    service = AShareCloseAnalysisService(
        FakeDailyData(_daily_bars()),
        macro_analyzer=analyzer,
        outbox=FakeOutbox(),
        clock=lambda: NOW,
    )
    target = ResearchNotificationTarget(
        target_id="1000000001",
        target_kind=NotificationTargetKind.PRIVATE,
        decisions=frozenset(RecommendationDecision),
        max_characters=7_500,
    )

    run = asyncio.run(
        service.run_once(
            _request(
                ashare_derivatives=_derivatives_bundle(),
                ashare_derivatives_failure_codes=(
                    "SSE_OPTION_OFFICIAL_RISK_UNAVAILABLE",
                ),
            ),
            notification_target=target,
        )
    )

    assert run.ashare_derivatives_report_lines == (
        "上交所ETF官方份额：总份额=24,168,187,700.000份。",
    )
    assert run.ashare_derivatives_failure_codes == (
        "SSE_OPTION_OFFICIAL_RISK_UNAVAILABLE",
    )
    assert any(
        item.evidence_id == "sse-official-etf-shares-evidence"
        for item in analyzer.requests[0].evidence
    )
    assert "SSE_OPTION_OFFICIAL_RISK_UNAVAILABLE" in run.recommendation.uncertainties
    assert run.notification is not None
    assert "24,168,187,700.000份" in run.notification.text
    assert "SSE_OPTION_OFFICIAL_RISK_UNAVAILABLE" in run.notification.text


def test_long_close_report_is_split_without_truncating_lines() -> None:
    title = "【A股｜收盘研究分析】"
    source_lines = tuple(f"第{index:02d}行：" + "研究内容" * 12 for index in range(30))
    chunks = _split_notification_text(
        "\n".join((title, "", *source_lines)),
        240,
    )

    assert len(chunks) > 1
    assert all(item.startswith(title) for item in chunks)
    assert all(len(item) <= 240 for item in chunks)
    combined = "\n".join(
        line
        for chunk in chunks
        for line in chunk.splitlines()
        if line and line != title
    )
    assert combined == "\n".join(source_lines)


def test_close_service_enqueues_every_long_report_part_once() -> None:
    outbox = FakeOutbox()
    service = AShareCloseAnalysisService(
        FakeDailyData(_daily_bars()),
        outbox=outbox,
        clock=lambda: NOW,
    )
    target = ResearchNotificationTarget(
        target_id="1000000001",
        target_kind=NotificationTargetKind.PRIVATE,
        decisions=frozenset(RecommendationDecision),
        max_characters=600,
    )

    run = asyncio.run(
        service.run_once(
            _request(cross_market_snapshot=_cross_market_snapshot()),
            notification_target=target,
        )
    )

    assert len(run.notifications) > 1
    assert outbox.items == list(run.notifications)
    assert len({item.idempotency_key for item in run.notifications}) == len(
        run.notifications
    )
    assert all(len(item.text) <= 600 for item in run.notifications)
    assert all(item.text.startswith("【A股｜收盘研究分析】") for item in run.notifications)
    for item in run.notifications:
        validate_text_report_contract(ReportKind.INSTRUMENT_RESEARCH, item.text)


def test_dual_track_evidence_coverage_is_calculated_per_analysis() -> None:
    analysis = MacroAnalysis(
        analysis_id="analysis-track-coverage",
        as_of=NOW,
        decision=MacroAnalysisDecision.PUBLISH,
        regime="neutral",
        technical_alignment=Decimal("0.2"),
        macro_impact=Decimal("0.1"),
        scenarios=(),
        claims=(MacroClaim("bounded", ("evidence-1",), ()),),
        uncertainties=(),
        data_gaps=(),
        invalidation_conditions=("evidence revised",),
        reported_confidence="UNCALIBRATED",
        refusal_reason="",
        model_version="model-1",
    )

    assert _track_evidence_coverage(
        analysis,
        {"evidence-1", "evidence-2"},
    ) == "1/2（50.000%）"
    assert _track_evidence_coverage(analysis, set()) == "0/0（0.000%）"
