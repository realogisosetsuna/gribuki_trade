"""真实行情保护输入与生产双轨语义适配测试。"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.analysis.schemas import MacroAnalysis, MacroAnalysisDecision
from gribuki_trade.domain.live_records import ConfirmedLiveFill
from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import PaperFillFees, PaperInstrumentType
from gribuki_trade.ports.llm_analyzer import DualTrackMacroAnalysis
from gribuki_trade.ports.market_data import (
    FreshnessStatus,
    IntradayBar,
    MarketDataMeta,
    MinuteInterval,
    SourceSemantics,
    TradeCalendarDay,
)
from gribuki_trade.services.live_protection_inputs import (
    ProductionLiveDualExitSemanticAnalyzer,
    PublicMarketLiveProtectionInputProvider,
)
from gribuki_trade.services.live_trade_orchestration import LiveProtectionInputError

_NOW = datetime(2026, 8, 14, 6, 30, tzinfo=UTC)


def _fill() -> ConfirmedLiveFill:
    return ConfirmedLiveFill(
        command_id="live-buy-input-1",
        account_id="live-main",
        side=Side.BUY,
        symbol="600000.SH",
        quantity=100,
        price=Decimal("10.20"),
        instrument_type=PaperInstrumentType.STOCK,
        executed_at=_NOW - timedelta(minutes=1),
        fees=PaperFillFees(
            commission=Decimal("5"),
            transfer_fee=Decimal("0.10"),
            stamp_tax=Decimal("0"),
        ),
        external_order_id="broker-live-input-1",
    )


def _intraday_bars(count: int = 450) -> tuple[IntradayBar, ...]:
    first = _NOW - timedelta(minutes=count)
    output: list[IntradayBar] = []
    for index in range(count):
        start = first + timedelta(minutes=index)
        end = start + timedelta(minutes=1)
        close = Decimal("10.00") + Decimal(index % 20) * Decimal("0.002")
        output.append(
            IntradayBar(
                symbol="600000.SH",
                start_at=start,
                end_at=end,
                interval=MinuteInterval.ONE_MINUTE,
                open=close - Decimal("0.01"),
                high=close + Decimal("0.05"),
                low=close - Decimal("0.05"),
                close=close,
                volume_lots=1000 + index,
                amount=close * (1000 + index),
                vwap=close,
                is_closed=True,
                meta=MarketDataMeta(
                    provider="fake-public-provider",
                    semantics=SourceSemantics.AGGREGATED_MINUTE_BAR,
                    fetched_at=_NOW,
                    provider_timestamp=end,
                    freshness=FreshnessStatus.CURRENT,
                ),
            )
        )
    return tuple(output)


class _Market:
    async def fetch_trade_prints_async(self, _symbol):
        return ()

    async def fetch_intraday_bars_async(
        self,
        _symbol,
        _start,
        _end,
        *,
        interval=MinuteInterval.ONE_MINUTE,
        completed_only=True,
    ):
        assert interval is MinuteInterval.ONE_MINUTE
        assert completed_only is True
        return _intraday_bars()


class _Calendar:
    def __init__(self, *, omit_last: bool = False) -> None:
        self._omit_last = omit_last

    async def fetch_trade_calendar_async(self, start: date, end: date):
        days = tuple(
            TradeCalendarDay(
                calendar_date=start + timedelta(days=index),
                is_trading_day=(start + timedelta(days=index)).weekday() < 5,
            )
            for index in range((end - start).days + 1)
        )
        return days[:-1] if self._omit_last else days


class _DualAnalyzer:
    def __init__(self, *, failure_code: str | None = None) -> None:
        self.failure_code = failure_code
        self.requests = []

    async def analyze(self, request):
        return self._analysis(request, model="baseline")

    async def analyze_dual(self, request):
        self.requests.append(request)
        baseline = self._analysis(request, model="baseline")
        adversarial = self._analysis(request, model="adversarial")
        return DualTrackMacroAnalysis(
            selected_analysis=adversarial,
            baseline_analysis=baseline,
            adversarial_analysis=adversarial,
            selected_track="ADVERSARIAL",
            failure_code=self.failure_code,
            audit_document={},
        )

    @staticmethod
    def _analysis(request, *, model: str) -> MacroAnalysis:
        return MacroAnalysis(
            analysis_id=request.analysis_id,
            as_of=request.as_of,
            decision=MacroAnalysisDecision.WATCH,
            regime="趋势延续但需保护",
            technical_alignment=Decimal("0.4"),
            macro_impact=Decimal("0.2"),
            scenarios=(),
            claims=(),
            uncertainties=(),
            data_gaps=(),
            invalidation_conditions=("完整线跌破保护位",),
            reported_confidence="MEDIUM",
            refusal_reason="",
            model_version=model,
        )


def test_public_provider_freezes_quick_inputs_before_dual_track_analysis() -> None:
    dual = _DualAnalyzer()
    provider = PublicMarketLiveProtectionInputProvider(
        market_data=_Market(),
        calendar=_Calendar(),
        semantic_analyzer=ProductionLiveDualExitSemanticAnalyzer(dual),
    )

    result = asyncio.run(provider.prepare(_fill(), requested_at=_NOW))

    assert result.decision_at == _NOW
    assert len(result.bars) == 450
    assert tuple(frame.timeframe_id for frame in result.deep_timeframes) == (
        "1m",
        "5m",
        "15m",
    )
    assert all(frame.bars for frame in result.deep_timeframes)
    assert result.technical_invalidation_price < _fill().price
    assert result.time_exit_at > _NOW
    assert result.baseline_assessment is None
    assert result.adversarial_assessment is None
    assert dual.requests == []

    baseline, adversarial = asyncio.run(provider.assess_deep(_fill(), inputs=result))

    assert baseline.system == "BASELINE_LLM"
    assert adversarial.system == "ADVERSARIAL_LLM"
    assert baseline.evidence_ids
    assert dual.requests[0].as_of == _NOW


def test_public_provider_builds_quick_without_llm_configuration() -> None:
    provider = PublicMarketLiveProtectionInputProvider(
        market_data=_Market(),
        calendar=_Calendar(),
    )

    quick = asyncio.run(provider.prepare(_fill(), requested_at=_NOW))

    assert quick.bars
    assert quick.technical_invalidation_price < _fill().price
    with pytest.raises(
        LiveProtectionInputError,
        match="LIVE_PROTECTION_DUAL_LLM_NOT_CONFIGURED",
    ) as captured:
        asyncio.run(provider.assess_deep(_fill(), inputs=quick))
    assert captured.value.retryable is True


def test_public_provider_rejects_incomplete_calendar() -> None:
    provider = PublicMarketLiveProtectionInputProvider(
        market_data=_Market(),
        calendar=_Calendar(omit_last=True),
        semantic_analyzer=ProductionLiveDualExitSemanticAnalyzer(_DualAnalyzer()),
    )

    with pytest.raises(
        LiveProtectionInputError,
        match="LIVE_PROTECTION_CALENDAR_INCOMPLETE",
    ):
        asyncio.run(provider.prepare(_fill(), requested_at=_NOW))


def test_public_provider_retries_when_adversarial_track_is_unavailable() -> None:
    provider = PublicMarketLiveProtectionInputProvider(
        market_data=_Market(),
        calendar=_Calendar(),
        semantic_analyzer=ProductionLiveDualExitSemanticAnalyzer(
            _DualAnalyzer(failure_code="ADVERSARIAL_TIMEOUT")
        ),
    )

    quick = asyncio.run(provider.prepare(_fill(), requested_at=_NOW))
    with pytest.raises(
        LiveProtectionInputError,
        match="LIVE_PROTECTION_DUAL_LLM_UNAVAILABLE",
    ):
        asyncio.run(provider.assess_deep(_fill(), inputs=quick))
