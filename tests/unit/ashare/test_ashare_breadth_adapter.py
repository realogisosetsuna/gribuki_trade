import asyncio
import json
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from gribuki_trade.adapters.ashare_breadth import (
    EASTMONEY_BREADTH_SOURCE_ID,
    TENCENT_BREADTH_SOURCE_ID,
    AKShareAShareBreadthAdapter,
)
from gribuki_trade.ports.ashare_breadth import (
    AShareBreadthCoverageError,
    AShareBreadthPayloadError,
    AShareBreadthSourcesExhaustedError,
    AShareCloseBreadthData,
    AShareExchange,
    AsyncAShareCloseBreadthData,
)

FIXTURE = (
    Path(__file__).parents[2]
    / "fixtures"
    / "ashare_breadth"
    / "close_snapshots.json"
)
FROZEN = json.loads(FIXTURE.read_text(encoding="utf-8"))
NOW = datetime(2026, 8, 13, 16, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
SESSION = date(2026, 8, 13)


def _eastmoney() -> pd.DataFrame:
    return pd.DataFrame(FROZEN["eastmoney"])


def _tencent() -> pd.DataFrame:
    return pd.DataFrame(FROZEN["tencent"])


def test_primary_snapshot_is_strictly_aggregated_from_frozen_fixture() -> None:
    calls: list[str] = []

    def eastmoney() -> pd.DataFrame:
        calls.append("eastmoney")
        return _eastmoney()

    def tencent() -> pd.DataFrame:
        calls.append("tencent")
        return _tencent()

    result = AKShareAShareBreadthAdapter(
        SimpleNamespace(
            stock_zh_a_spot_em=eastmoney,
            stock_zh_a_spot_tx=tencent,
        ),
        minimum_eligible_count=3,
        now=lambda: NOW,
    ).fetch_close_breadth(SESSION)

    assert calls == ["eastmoney"]
    assert result.meta.source_id == EASTMONEY_BREADTH_SOURCE_ID
    assert result.meta.fallback_used is False
    assert result.meta.degraded is False
    assert result.received_count == 7
    assert result.eligible_count == 3
    assert result.duplicate_count == 1
    assert result.excluded_non_equity_count == 2
    assert result.non_trading_count == 1
    assert tuple((item.exchange, item.eligible_count) for item in result.included_exchanges) == (
        (AShareExchange.SHANGHAI, 1),
        (AShareExchange.SHENZHEN, 1),
        (AShareExchange.BEIJING, 1),
    )
    assert result.advancing_count == 1
    assert result.declining_count == 1
    assert result.flat_count == 1
    assert result.advance_decline_ratio == Decimal("1")
    assert result.total_amount_cny == Decimal("350000000")
    assert result.advancing_amount_share_percent == Decimal("100") / Decimal("3.5")
    assert result.equal_weight_mean_change_percent == Decimal("-0.8") / Decimal("3")
    assert result.median_change_percent == Decimal("0.00")
    assert result.expected_count is None
    assert result.coverage_percent is None
    assert result.limit_up_count is None and result.limit_down_count is None
    assert "exact universe coverage is not reported" in " ".join(result.meta.warnings)


def test_tencent_fallback_has_independent_units_and_degraded_provenance() -> None:
    def fail() -> pd.DataFrame:
        raise RuntimeError("frozen disconnect")

    result = AKShareAShareBreadthAdapter(
        SimpleNamespace(
            stock_zh_a_spot_em=fail,
            stock_zh_a_spot_tx=_tencent,
        ),
        minimum_eligible_count=3,
        now=lambda: NOW,
    ).fetch_close_breadth(SESSION)

    assert result.meta.source_id == TENCENT_BREADTH_SOURCE_ID
    assert result.meta.fallback_used is True
    assert result.meta.degraded is True
    assert result.received_count == 5
    assert result.eligible_count == 3
    assert result.total_amount_cny == Decimal("350000000")
    assert "UPSTREAM_ERROR" in " ".join(result.meta.warnings)


def test_each_source_timeout_is_bounded_and_falls_back() -> None:
    def slow() -> pd.DataFrame:
        time.sleep(0.2)
        return _eastmoney()

    result = AKShareAShareBreadthAdapter(
        SimpleNamespace(
            stock_zh_a_spot_em=slow,
            stock_zh_a_spot_tx=_tencent,
        ),
        timeout_seconds=0.02,
        minimum_eligible_count=3,
        now=lambda: NOW,
    ).fetch_close_breadth(SESSION)

    assert result.meta.source_id == TENCENT_BREADTH_SOURCE_ID
    assert "TIMEOUT" in " ".join(result.meta.warnings)


def test_minimum_count_and_all_three_exchanges_are_hard_gates() -> None:
    too_small = _eastmoney().iloc[:2]
    no_beijing = _tencent().loc[lambda frame: ~frame["code"].str.startswith("bj")]
    adapter = AKShareAShareBreadthAdapter(
        SimpleNamespace(
            stock_zh_a_spot_em=lambda: too_small,
            stock_zh_a_spot_tx=lambda: no_beijing,
        ),
        minimum_eligible_count=2,
        now=lambda: NOW,
    )

    with pytest.raises(AShareBreadthSourcesExhaustedError) as raised:
        adapter.fetch_close_breadth(SESSION)

    assert tuple(item.failure_code for item in raised.value.failures) == (
        "INSUFFICIENT_COVERAGE",
        "INSUFFICIENT_COVERAGE",
    )
    assert "BEIJING" in raised.value.failures[1].reason


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("代码", "60000A"),
        ("最新价", "not-a-price"),
        ("涨跌幅", "nan"),
        ("成交额", "-1"),
    ],
)
def test_malformed_primary_fields_fail_closed_without_silent_row_drops(
    column: str,
    value: str,
) -> None:
    frame = _eastmoney()
    frame.loc[0, column] = value
    adapter = AKShareAShareBreadthAdapter(
        SimpleNamespace(stock_zh_a_spot_em=lambda: frame),
        minimum_eligible_count=3,
        now=lambda: NOW,
    )

    with pytest.raises(AShareBreadthSourcesExhaustedError) as raised:
        adapter.fetch_close_breadth(SESSION)

    assert isinstance(raised.value.__cause__, type(None))
    assert raised.value.failures[0].failure_code == "INVALID_PAYLOAD"


def test_conflicting_duplicate_is_rejected() -> None:
    frame = _eastmoney()
    frame.loc[3, "成交额"] = "100000001"
    adapter = AKShareAShareBreadthAdapter(
        SimpleNamespace(stock_zh_a_spot_em=lambda: frame),
        minimum_eligible_count=3,
        now=lambda: NOW,
    )

    with pytest.raises(AShareBreadthSourcesExhaustedError) as raised:
        adapter.fetch_close_breadth(SESSION)

    assert raised.value.failures[0].failure_code == "INVALID_PAYLOAD"
    assert "conflicting duplicate" in raised.value.failures[0].reason


def test_snapshot_is_same_day_post_close_only() -> None:
    client = SimpleNamespace(
        stock_zh_a_spot_em=_eastmoney,
        stock_zh_a_spot_tx=_tencent,
    )
    prior = AKShareAShareBreadthAdapter(
        client,
        minimum_eligible_count=3,
        now=lambda: NOW,
    )
    with pytest.raises(AShareBreadthCoverageError, match="current Shanghai calendar date"):
        prior.fetch_close_breadth(date(2026, 8, 12))

    before_close = NOW.replace(hour=15, minute=3)
    early = AKShareAShareBreadthAdapter(
        client,
        minimum_eligible_count=3,
        now=lambda: before_close,
    )
    with pytest.raises(AShareBreadthCoverageError, match="unavailable before"):
        early.fetch_close_breadth(SESSION)


def test_protocols_and_async_entrypoint() -> None:
    adapter = AKShareAShareBreadthAdapter(
        SimpleNamespace(stock_zh_a_spot_em=_eastmoney),
        minimum_eligible_count=3,
        now=lambda: NOW,
    )
    assert isinstance(adapter, AShareCloseBreadthData)
    assert isinstance(adapter, AsyncAShareCloseBreadthData)

    result = asyncio.run(adapter.fetch_close_breadth_async(SESSION))
    assert result.eligible_count == 3

    with pytest.raises(ValueError, match="stale_after must be positive"):
        AKShareAShareBreadthAdapter(stale_after=timedelta(0))


def test_payload_error_types_remain_market_data_failures() -> None:
    assert issubclass(AShareBreadthPayloadError, RuntimeError)
