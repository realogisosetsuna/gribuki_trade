import hashlib
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.ports.global_risk import (
    GlobalRiskCacheEntry,
    GlobalRiskSourceMeta,
    VIXDailyBar,
    VIXDailyHistory,
)
from gribuki_trade.services.global_risk_evidence import build_vix_evidence

AS_OF = datetime(2026, 8, 13, 8, 30, tzinfo=UTC)
BODY = b"official-vix-document"


def _history() -> VIXDailyHistory:
    available = datetime(2026, 8, 12, 20, 15, tzinfo=UTC)
    fetched = AS_OF - timedelta(minutes=1)
    digest = hashlib.sha256(BODY).hexdigest()
    bar = VIXDailyBar(
        session_date=date(2026, 8, 12),
        open=Decimal("14.2"),
        high=Decimal("14.9"),
        low=Decimal("14.1"),
        close=Decimal("14.55"),
        observed_at=available,
        available_at=available,
    )
    cache = GlobalRiskCacheEntry(
        source_url="https://cdn.cboe.com/vix.csv",
        body=BODY,
        fetched_at=fetched,
        content_sha256=digest,
    )
    return VIXDailyHistory(
        as_of=AS_OF,
        bars=(bar,),
        meta=GlobalRiskSourceMeta(
            source_id="CBOE_VIX_DAILY_HISTORY",
            source_url=cache.source_url,
            available_at=available,
            fetched_at=fetched,
            stale=False,
            content_sha256=digest,
        ),
        cache_entry=cache,
    )


def test_vix_evidence_is_human_readable_stable_and_pit_bounded() -> None:
    first = build_vix_evidence(_history(), as_of=AS_OF)
    second = build_vix_evidence(_history(), as_of=AS_OF)

    assert first == second
    assert first.items[0].publisher == "CBOE_VIX_DAILY_HISTORY"
    assert first.items[0].source_tier == 1
    assert "收=14.550" in first.items[0].excerpt
    assert "隔离了0条OHLC包络异常旧记录" in first.items[0].excerpt
    assert "A股收盘报告仅使用此前已完成的美国交易日" in first.report_lines[0]
    assert first.references[0].evidence_id == first.items[0].evidence_id


def test_vix_evidence_rejects_future_fetch_and_naive_cutoff() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        build_vix_evidence(_history(), as_of=datetime(2026, 8, 13))
    with pytest.raises(ValueError, match="fetched after"):
        build_vix_evidence(
            _history(),
            as_of=datetime(2026, 8, 12, 20, 16, tzinfo=UTC),
        )
