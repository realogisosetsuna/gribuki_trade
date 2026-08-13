from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.domain.recommendations import (
    EvidenceReference,
    RecommendationDecision,
    RecommendationHorizon,
)
from gribuki_trade.features.technical import TechnicalSignalConfig
from gribuki_trade.ports.market_data import (
    FreshnessStatus,
    IntradayBar,
    MarketDataMeta,
    MarketDataTimeoutError,
    MarketDataUnavailableError,
    MinuteInterval,
    SourceSemantics,
)
from gribuki_trade.services import (
    AShareResearchRequest,
    AShareResearchService,
    ResearchNotificationTarget,
)
from gribuki_trade.storage import SQLiteOutbox

NOW = datetime(2026, 8, 13, 2, 35, tzinfo=UTC)


class FakeIntradayMarketData:
    def __init__(self, bars: tuple[IntradayBar, ...]) -> None:
        self.bars = bars
        self.calls: list[tuple[object, ...]] = []

    async def fetch_intraday_bars_async(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        interval: MinuteInterval = MinuteInterval.ONE_MINUTE,
        completed_only: bool = True,
    ) -> tuple[IntradayBar, ...]:
        self.calls.append((symbol, start, end, interval, completed_only))
        return self.bars

    async def fetch_trade_prints_async(self, symbol: str) -> tuple[()]:
        del symbol
        return ()


class FailingIntradayMarketData(FakeIntradayMarketData):
    def __init__(self, error: Exception) -> None:
        super().__init__(())
        self.error = error

    async def fetch_intraday_bars_async(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        interval: MinuteInterval = MinuteInterval.ONE_MINUTE,
        completed_only: bool = True,
    ) -> tuple[IntradayBar, ...]:
        self.calls.append((symbol, start, end, interval, completed_only))
        raise self.error


def market_bars() -> tuple[IntradayBar, ...]:
    output: list[IntradayBar] = []
    for index in range(40):
        close = Decimal("10") + Decimal(index) * Decimal("0.02")
        if index == 39:
            close = Decimal("11.20")
        end = NOW - timedelta(minutes=39 - index, seconds=10)
        fetched_at = NOW - timedelta(seconds=5)
        output.append(
            IntradayBar(
                symbol="600000.SH",
                start_at=end - timedelta(minutes=1),
                end_at=end,
                interval=MinuteInterval.ONE_MINUTE,
                open=close - Decimal("0.02"),
                high=close + Decimal("0.03"),
                low=close - Decimal("0.04"),
                close=close,
                volume_lots=200_000 if index == 39 else 100_000,
                amount=close * Decimal("10000000"),
                vwap=None,
                is_closed=True,
                meta=MarketDataMeta(
                    provider="offline-fixture",
                    semantics=SourceSemantics.AGGREGATED_MINUTE_BAR,
                    fetched_at=fetched_at,
                    provider_timestamp=end,
                    freshness=(
                        FreshnessStatus.CURRENT
                        if index == 39
                        else FreshnessStatus.STALE
                    ),
                ),
            )
        )
    return tuple(output)


def technical_config() -> TechnicalSignalConfig:
    return TechnicalSignalConfig(
        fast_ma_bars=5,
        slow_ma_bars=15,
        breakout_bars=10,
        volume_bars=10,
        atr_bars=5,
        volume_ratio_threshold=Decimal("1.5"),
        max_data_age=timedelta(minutes=3),
        strategy_version="research-service-test@1",
    )


def evidence(*, first_seen_at: datetime | None = None) -> tuple[EvidenceReference, ...]:
    return (
        EvidenceReference(
            evidence_id="retained-market-bars-1",
            title="Retained completed minute bars",
            canonical_url="local://evidence/retained-market-bars-1",
            published_at=NOW - timedelta(seconds=20),
            first_seen_at=first_seen_at or NOW - timedelta(seconds=5),
            source_tier=2,
        ),
    )


def request(
    *,
    included_evidence: tuple[EvidenceReference, ...] | None = None,
) -> AShareResearchRequest:
    bars = market_bars()
    return AShareResearchRequest(
        symbol="600000.SH",
        start=bars[0].start_at,
        end=NOW,
        decision_time=NOW,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        evidence=evidence() if included_evidence is None else included_evidence,
    )


def run(service: AShareResearchService, item: AShareResearchRequest, **kwargs: object):
    return asyncio.run(service.run_once(item, **kwargs))


def test_completed_bars_flow_to_evidence_gated_recommendation() -> None:
    market = FakeIntradayMarketData(market_bars())
    service = AShareResearchService(market, technical_config=technical_config())

    result = run(service, request())

    assert result.recommendation.decision is RecommendationDecision.ENTER_CANDIDATE
    assert result.recommendation.reference_price == Decimal("11.20")
    assert result.recommendation.evidence == evidence()
    assert result.notification is None
    assert result.notification_enqueued is False
    assert market.calls[0][-1] is True


def test_implicit_decision_time_is_resolved_after_market_collection() -> None:
    evaluated_at = NOW + timedelta(seconds=30)
    service = AShareResearchService(
        FakeIntradayMarketData(market_bars()),
        technical_config=technical_config(),
        clock=lambda: evaluated_at,
    )

    result = run(service, replace(request(), decision_time=None))

    assert result.recommendation.as_of == evaluated_at
    assert result.recommendation.decision is RecommendationDecision.ENTER_CANDIDATE


def test_bare_code_is_canonicalized_before_fetch_and_publication() -> None:
    market = FakeIntradayMarketData(market_bars())
    service = AShareResearchService(market, technical_config=technical_config())

    result = run(service, replace(request(), symbol="600000"))

    assert market.calls[0][0] == "600000.SH"
    assert result.recommendation.symbol == "600000.SH"


@pytest.mark.parametrize(
    "error",
    [
        MarketDataUnavailableError("provider response omitted"),
        MarketDataTimeoutError("provider request timed out"),
    ],
)
def test_declared_market_data_failures_return_stable_abstain(error: Exception) -> None:
    market = FailingIntradayMarketData(error)
    service = AShareResearchService(market, technical_config=technical_config())

    result = run(service, request())

    assert issubclass(MarketDataTimeoutError, MarketDataUnavailableError)
    assert result.recommendation.decision is RecommendationDecision.ABSTAIN
    assert result.recommendation.reason_codes == ("MARKET_DATA_FETCH_FAILED",)
    assert result.recommendation.reference_price is None
    assert result.failure_code == "MARKET_DATA_FETCH_FAILED"
    assert result.notification is None
    assert market.calls[0][0] == "600000.SH"


def test_unexpected_market_data_error_is_not_swallowed() -> None:
    service = AShareResearchService(
        FailingIntradayMarketData(ValueError("adapter bug")),
        technical_config=technical_config(),
    )

    with pytest.raises(ValueError, match="adapter bug"):
        run(service, request())


def test_insufficient_and_stale_history_fail_closed() -> None:
    insufficient = FakeIntradayMarketData(market_bars()[:8])
    insufficient_result = run(
        AShareResearchService(insufficient, technical_config=technical_config()),
        request(),
    )
    assert insufficient_result.recommendation.decision is RecommendationDecision.ABSTAIN
    assert insufficient_result.recommendation.reason_codes == ("INSUFFICIENT_HISTORY",)

    stale_bars = tuple(
        replace(
            bar,
            start_at=bar.start_at - timedelta(minutes=10),
            end_at=bar.end_at - timedelta(minutes=10),
            meta=replace(
                bar.meta,
                provider_timestamp=bar.end_at - timedelta(minutes=10),
                freshness=FreshnessStatus.STALE,
            ),
        )
        for bar in market_bars()
    )
    stale = FakeIntradayMarketData(stale_bars)
    stale_result = run(
        AShareResearchService(stale, technical_config=technical_config()),
        replace(request(), start=stale_bars[0].start_at),
    )
    assert stale_result.recommendation.decision is RecommendationDecision.ABSTAIN
    assert stale_result.recommendation.reason_codes == ("STALE_MARKET_DATA",)
    assert stale_result.recommendation.reference_price is None


def test_degraded_or_invalid_provider_data_never_publishes_entry() -> None:
    degraded = list(market_bars())
    degraded[-1] = replace(
        degraded[-1],
        meta=replace(degraded[-1].meta, degraded=True),
    )
    degraded_result = run(
        AShareResearchService(
            FakeIntradayMarketData(tuple(degraded)),
            technical_config=technical_config(),
        ),
        request(),
    )
    assert degraded_result.recommendation.decision is RecommendationDecision.ABSTAIN
    assert degraded_result.recommendation.reason_codes == ("DEGRADED_MARKET_DATA",)

    future_observation = list(market_bars())
    future_observation[-1] = replace(
        future_observation[-1],
        meta=replace(
            future_observation[-1].meta,
            fetched_at=NOW + timedelta(seconds=1),
        ),
    )
    invalid_result = run(
        AShareResearchService(
            FakeIntradayMarketData(tuple(future_observation)),
            technical_config=technical_config(),
        ),
        request(),
    )
    assert invalid_result.recommendation.decision is RecommendationDecision.ABSTAIN
    assert invalid_result.recommendation.reason_codes == ("INVALID_MARKET_DATA",)


def test_missing_or_future_evidence_cannot_publish_entry() -> None:
    service = AShareResearchService(
        FakeIntradayMarketData(market_bars()),
        technical_config=technical_config(),
    )
    missing = run(service, request(included_evidence=()))
    assert missing.recommendation.decision is RecommendationDecision.ABSTAIN
    assert "MISSING_EVIDENCE" in missing.recommendation.reason_codes

    with pytest.raises(ValueError, match="not available"):
        run(
            service,
            request(
                included_evidence=evidence(
                    first_seen_at=NOW + timedelta(seconds=1)
                )
            ),
        )


def test_explicit_notification_is_idempotently_enqueued(tmp_path) -> None:
    with SQLiteOutbox(tmp_path / "research-outbox.sqlite3") as outbox:
        service = AShareResearchService(
            FakeIntradayMarketData(market_bars()),
            technical_config=technical_config(),
            outbox=outbox,
        )
        target = ResearchNotificationTarget(target_id="123456")

        first = run(service, request(), notification_target=target)
        second = run(service, request(), notification_target=target)

        assert first.notification_enqueued is True
        assert first.notification is not None
        assert "不会自动下单" in first.notification.text
        assert "可执行交易指令" in first.notification.text
        assert second.notification is not None
        assert second.notification.idempotency_key == first.notification.idempotency_key
        assert len(outbox.list_items()) == 1


def test_notification_requires_explicit_outbox_and_target(tmp_path) -> None:
    target = ResearchNotificationTarget(target_id="123456")
    without_outbox = AShareResearchService(
        FakeIntradayMarketData(market_bars()),
        technical_config=technical_config(),
    )
    with pytest.raises(RuntimeError, match="outbox"):
        run(without_outbox, request(), notification_target=target)

    with SQLiteOutbox(tmp_path / "unused.sqlite3") as outbox:
        service = AShareResearchService(
            FakeIntradayMarketData(market_bars()),
            technical_config=technical_config(),
            outbox=outbox,
        )
        result = run(service, request())
        assert result.notification is None
        assert outbox.list_items() == ()
