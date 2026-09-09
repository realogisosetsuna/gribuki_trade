from datetime import date, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.adapters.market_data.akshare_daily_stitch import (
    HistoricalDailyTailStitchDiagnostics,
    HistoricalDailyTailStitchPolicy,
    controlled_tail_stitch,
)
from gribuki_trade.domain.market import DailyBar, PriceAdjustment


class TailError(Exception):
    pass


class OverlapError(TailError):
    pass


def _bar(day: date, price: str = "4.00") -> DailyBar:
    value = Decimal(price)
    return DailyBar(
        symbol="510300.SH",
        trade_date=day,
        open=value,
        high=value + Decimal("0.03"),
        low=value - Decimal("0.02"),
        close=value + Decimal("0.01"),
        previous_close=None,
        volume=100,
        amount=value * 100,
        turnover_percent=None,
        is_trading=True,
        is_st=False,
        adjustment=PriceAdjustment.NONE,
    )


def test_policy_requires_a_twenty_session_overlap() -> None:
    with pytest.raises(ValueError, match="at least 20"):
        HistoricalDailyTailStitchPolicy(minimum_overlap_sessions=19)


def test_controlled_stitch_returns_auditable_diagnostics() -> None:
    start = date(2026, 1, 1)
    base = tuple(_bar(start + timedelta(days=i), f"{i + 4}.00") for i in range(21))
    overlap = tuple(_bar(item.trade_date, f"{item.open:.2f}") for item in base[-20:])
    fresh = _bar(start + timedelta(days=21), "25.00")
    stitched, diagnostics = controlled_tail_stitch(
        base_bars=base,
        base_source="archive",
        tail_bars=(*overlap, fresh),
        tail_source="fresh",
        required_latest_session=fresh.trade_date,
        minimum_overlap_sessions=20,
        tail_error=TailError,
        overlap_error=OverlapError,
    )

    assert stitched[-1] is fresh
    assert len(stitched) == 22
    assert isinstance(diagnostics, HistoricalDailyTailStitchDiagnostics)
    assert diagnostics.overlap_sessions_validated == 20
    assert diagnostics.stitched_tail_sessions == 1


def test_controlled_stitch_injects_overlap_error_without_facade_dependency() -> None:
    start = date(2026, 1, 1)
    base = tuple(_bar(start + timedelta(days=i)) for i in range(20))
    overlap = tuple(_bar(item.trade_date, "9.00") for item in base)

    with pytest.raises(OverlapError, match="OHLC mismatch"):
        controlled_tail_stitch(
            base_bars=base,
            base_source="archive",
            tail_bars=(*overlap, _bar(start + timedelta(days=20))),
            tail_source="fresh",
            required_latest_session=start + timedelta(days=20),
            minimum_overlap_sessions=20,
            tail_error=TailError,
            overlap_error=OverlapError,
        )
