from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.domain.recommendations import (
    RecommendationDecision,
    RecommendationHorizon,
)
from gribuki_trade.features.technical import (
    TechnicalBar,
    TechnicalSignalConfig,
    build_technical_signal,
)

NOW = datetime(2026, 8, 13, 10, 35, tzinfo=UTC)


def bars(*, breakout: bool = True, high_volume: bool = True) -> tuple[TechnicalBar, ...]:
    output: list[TechnicalBar] = []
    for index in range(40):
        close = Decimal("10") + Decimal(index) * Decimal("0.02")
        if index == 39:
            close = Decimal("11.20") if breakout else Decimal("10.75")
        end_time = NOW - timedelta(minutes=5 * (39 - index), seconds=10)
        output.append(
            TechnicalBar(
                end_time=end_time,
                available_at=end_time + timedelta(seconds=5),
                open=close - Decimal("0.02"),
                high=close + Decimal("0.03"),
                low=close - Decimal("0.04"),
                close=close,
                volume=(200_000 if index == 39 and high_volume else 100_000),
            )
        )
    return tuple(output)


def config() -> TechnicalSignalConfig:
    return TechnicalSignalConfig(
        fast_ma_bars=5,
        slow_ma_bars=15,
        breakout_bars=10,
        volume_bars=10,
        atr_bars=5,
        volume_ratio_threshold=Decimal("1.5"),
        max_data_age=timedelta(minutes=3),
    )


def test_closed_breakout_with_volume_is_enter_candidate() -> None:
    result = build_technical_signal(
        "600000.SH",
        bars(),
        decision_time=NOW,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        config=config(),
    )

    assert result.decision is RecommendationDecision.ENTER_CANDIDATE
    assert result.score == Decimal("1.00")
    assert result.reference_price == Decimal("11.20")
    assert result.invalidation_price is not None
    assert "CLOSED_BAR_BREAKOUT" in result.reason_codes
    assert "VOLUME_CONFIRMED" in result.reason_codes


def test_stale_data_fails_closed() -> None:
    old_now = NOW + timedelta(minutes=10)
    result = build_technical_signal(
        "600000.SH",
        bars(),
        decision_time=old_now,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        config=config(),
    )

    assert result.decision is RecommendationDecision.ABSTAIN
    assert result.reason_codes == ("STALE_MARKET_DATA",)
    assert result.reference_price is None


def test_recent_download_does_not_make_an_old_bar_fresh() -> None:
    items = tuple(
        TechnicalBar(
            end_time=item.end_time - timedelta(minutes=30),
            available_at=NOW - timedelta(seconds=1),
            open=item.open,
            high=item.high,
            low=item.low,
            close=item.close,
            volume=item.volume,
        )
        for item in bars()
    )

    result = build_technical_signal(
        "600000.SH",
        items,
        decision_time=NOW,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        config=config(),
    )

    assert result.decision is RecommendationDecision.ABSTAIN
    assert result.reason_codes == ("STALE_MARKET_DATA",)


def test_incomplete_and_future_bars_are_rejected() -> None:
    items = list(bars())
    items[-1] = TechnicalBar(
        end_time=items[-1].end_time,
        available_at=items[-1].available_at,
        open=items[-1].open,
        high=items[-1].high,
        low=items[-1].low,
        close=items[-1].close,
        volume=items[-1].volume,
        complete=False,
    )
    with pytest.raises(ValueError, match="completed"):
        build_technical_signal(
            "600000.SH",
            tuple(items),
            decision_time=NOW,
            horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
            config=config(),
        )

    with pytest.raises(ValueError, match="not available"):
        build_technical_signal(
            "600000.SH",
            bars(),
            decision_time=NOW - timedelta(minutes=1),
            horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
            config=config(),
        )


def test_held_symbol_below_slow_average_is_reduce() -> None:
    items = list(bars())
    last = items[-1]
    items[-1] = TechnicalBar(
        end_time=last.end_time,
        available_at=last.available_at,
        open=Decimal("9.52"),
        high=Decimal("9.55"),
        low=Decimal("9.45"),
        close=Decimal("9.50"),
        volume=200_000,
    )
    result = build_technical_signal(
        "600000.SH",
        tuple(items),
        decision_time=NOW,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        is_currently_held=True,
        config=config(),
    )

    assert result.decision is RecommendationDecision.REDUCE
    assert "CLOSE_BELOW_SLOW_MA" in result.reason_codes


def test_insufficient_history_abstains_without_using_partial_window() -> None:
    result = build_technical_signal(
        "600000.SH",
        bars()[:10],
        decision_time=NOW,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        config=config(),
    )

    assert result.decision is RecommendationDecision.ABSTAIN
    assert result.reason_codes == ("INSUFFICIENT_HISTORY",)
