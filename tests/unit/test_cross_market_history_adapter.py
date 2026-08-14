import asyncio
import threading
import time as wall_time
from collections.abc import Callable
from datetime import UTC, date, datetime, time
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from gribuki_trade.adapters.cross_market_history import (
    DEFAULT_CROSS_MARKET_HISTORY_UNIVERSE,
    AKShareCrossMarketHistoryAdapter,
    CrossMarketHistorySpec,
)
from gribuki_trade.ports.cross_market_history import (
    MINIMUM_CROSS_MARKET_HISTORY,
    AsyncCrossMarketHistoryData,
    CrossMarketHistoryData,
    CrossMarketHistoryFailureCode,
)

# 冻结的提供方观察值：所有测试都使用这组固定序列，绝不访问实时端点。
# 共有 131 个交易日，因此即使最新一行尚未收盘，仍恰好保留契约要求的
# 最少 130 个观察值。
_FROZEN_SESSIONS: tuple[tuple[str, str], ...] = tuple(
    (timestamp.date().isoformat(), str(Decimal("4000") + index))
    for index, timestamp in enumerate(
        pd.bdate_range(end="2026-08-13", periods=131),
        start=1,
    )
)


def _frame(
    sessions: tuple[tuple[str, str], ...] = _FROZEN_SESSIONS,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": session_date,
                "open": str(Decimal(close) - Decimal("1")),
                "high": str(Decimal(close) + Decimal("2")),
                "low": str(Decimal(close) - Decimal("2")),
                "close": close,
                "volume": "1000000",
            }
            for session_date, close in sessions
        ]
    )


def _spec(market_id: str) -> CrossMarketHistorySpec:
    return next(
        item
        for item in DEFAULT_CROSS_MARKET_HISTORY_UNIVERSE
        if item.market_id == market_id
    )


def test_default_universe_calls_only_audited_exact_sina_symbols() -> None:
    calls: list[tuple[str, str]] = []

    def mainland(*, symbol: str) -> pd.DataFrame:
        calls.append(("stock_zh_index_daily", symbol))
        return _frame()

    def hong_kong(*, symbol: str) -> pd.DataFrame:
        calls.append(("stock_hk_index_daily_sina", symbol))
        return _frame()

    def united_states(*, symbol: str) -> pd.DataFrame:
        calls.append(("index_us_stock_sina", symbol))
        return _frame()

    def global_index(*, symbol: str) -> pd.DataFrame:
        calls.append(("index_global_hist_sina", symbol))
        return _frame()

    adapter = AKShareCrossMarketHistoryAdapter(
        SimpleNamespace(
            stock_zh_index_daily=mainland,
            stock_hk_index_daily_sina=hong_kong,
            index_us_stock_sina=united_states,
            index_global_hist_sina=global_index,
        ),
        now=lambda: datetime(2026, 8, 14, tzinfo=UTC),
    )

    snapshot = adapter.fetch_cross_market_history(
        as_of=datetime(2026, 8, 14, tzinfo=UTC)
    )

    assert snapshot.degraded is False
    assert snapshot.missing == ()
    assert len(snapshot.series) == 9
    assert sorted(calls) == sorted(
        [
            ("stock_zh_index_daily", "sh000300"),
            ("stock_hk_index_daily_sina", "HSI"),
            ("stock_hk_index_daily_sina", "HSCEI"),
            ("index_us_stock_sina", ".INX"),
            ("index_us_stock_sina", ".NDX"),
            ("index_us_stock_sina", ".DJI"),
            ("index_global_hist_sina", "日经225指数"),
            ("index_global_hist_sina", "首尔综合指数"),
            ("index_global_hist_sina", "中国台湾加权指数"),
        ]
    )
    csi = next(item for item in snapshot.series if item.market_id == "CSI_300")
    observation = csi.observations[-1]
    assert observation.market_id == "CSI_300"
    assert observation.session_date == date(2026, 8, 13)
    assert observation.close == Decimal("4131")
    assert observation.available_at.isoformat() == "2026-08-13T15:00:00+08:00"
    assert observation.source == "AKShare/Sina stock_zh_index_daily(sh000300)"
    assert csi.source == observation.source
    assert "no historical release vintages" in " ".join(snapshot.warnings)


def test_sina_history_endpoints_are_processed_serially_in_universe_order() -> None:
    activity_lock = threading.Lock()
    active = 0
    max_active = 0
    calls: list[tuple[str, str]] = []

    def endpoint(method_name: str) -> Callable[..., pd.DataFrame]:
        def fetch(*, symbol: str) -> pd.DataFrame:
            nonlocal active, max_active
            with activity_lock:
                active += 1
                max_active = max(max_active, active)
                calls.append((method_name, symbol))
            try:
                wall_time.sleep(0.01)
                return _frame()
            finally:
                with activity_lock:
                    active -= 1

        return fetch

    universe = (_spec("CSI_300"), _spec("HANG_SENG"), _spec("S_AND_P_500"))
    adapter = AKShareCrossMarketHistoryAdapter(
        SimpleNamespace(
            stock_zh_index_daily=endpoint("stock_zh_index_daily"),
            stock_hk_index_daily_sina=endpoint("stock_hk_index_daily_sina"),
            index_us_stock_sina=endpoint("index_us_stock_sina"),
        ),
        universe=universe,
        timeout_seconds=1.0,
    )

    snapshot = adapter.fetch_cross_market_history(
        as_of=datetime(2026, 8, 14, tzinfo=UTC)
    )

    assert snapshot.missing == ()
    assert max_active == 1
    assert calls == [(spec.method_name, spec.symbol) for spec in universe]


def test_pit_filter_uses_each_local_close_and_us_previous_session() -> None:
    frame = _frame()
    client = SimpleNamespace(
        stock_zh_index_daily=lambda *, symbol: frame,
        stock_hk_index_daily_sina=lambda *, symbol: frame,
        index_us_stock_sina=lambda *, symbol: frame,
    )
    adapter = AKShareCrossMarketHistoryAdapter(
        client,
        universe=(_spec("CSI_300"), _spec("HANG_SENG"), _spec("S_AND_P_500")),
    )
    # 中国时间 15:05 已晚于 A 股收盘，但港股收市竞价尚未完成，距离当日
    # 美股常规交易时段收盘也还很久。
    as_of = datetime(2026, 8, 13, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai"))

    snapshot = adapter.fetch_cross_market_history(as_of=as_of)
    by_id = {item.market_id: item for item in snapshot.series}

    assert by_id["CSI_300"].observations[-1].session_date == date(2026, 8, 13)
    assert by_id["HANG_SENG"].observations[-1].session_date == date(2026, 8, 12)
    assert by_id["S_AND_P_500"].observations[-1].session_date == date(2026, 8, 12)
    assert len(by_id["CSI_300"].observations) == 131
    assert len(by_id["HANG_SENG"].observations) == 130
    assert len(by_id["S_AND_P_500"].observations) == 130
    us_close = by_id["S_AND_P_500"].observations[-1].available_at
    assert us_close.isoformat() == "2026-08-12T16:00:00-04:00"
    assert all(
        observation.available_at <= as_of
        for series in snapshot.series
        for observation in series.observations
    )


def test_hong_kong_current_close_becomes_visible_at_1610_exactly() -> None:
    adapter = AKShareCrossMarketHistoryAdapter(
        SimpleNamespace(stock_hk_index_daily_sina=lambda *, symbol: _frame()),
        universe=(_spec("HANG_SENG"),),
    )

    just_before = adapter.fetch_cross_market_history(
        as_of=datetime(2026, 8, 13, 16, 9, 59, tzinfo=ZoneInfo("Asia/Hong_Kong"))
    )
    at_close = adapter.fetch_cross_market_history(
        as_of=datetime(2026, 8, 13, 16, 10, tzinfo=ZoneInfo("Asia/Hong_Kong"))
    )

    assert just_before.series[0].observations[-1].session_date == date(2026, 8, 12)
    assert at_close.series[0].observations[-1].session_date == date(2026, 8, 13)
    assert at_close.series[0].observations[-1].available_at.isoformat() == (
        "2026-08-13T16:10:00+08:00"
    )


def test_new_york_close_anchor_uses_zoneinfo_winter_offset() -> None:
    sessions = tuple(
        (timestamp.date().isoformat(), str(5000 + index))
        for index, timestamp in enumerate(
            pd.bdate_range(end="2026-01-15", periods=130),
            start=1,
        )
    )
    adapter = AKShareCrossMarketHistoryAdapter(
        SimpleNamespace(index_us_stock_sina=lambda *, symbol: _frame(sessions)),
        universe=(_spec("S_AND_P_500"),),
    )

    snapshot = adapter.fetch_cross_market_history(
        as_of=datetime(2026, 1, 16, 8, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    )

    assert snapshot.series[0].observations[-1].available_at.isoformat() == (
        "2026-01-15T16:00:00-05:00"
    )


def test_insufficient_visible_history_is_explicit_missing_without_proxy() -> None:
    adapter = AKShareCrossMarketHistoryAdapter(
        SimpleNamespace(
            stock_hk_index_daily_sina=lambda *, symbol: _frame(_FROZEN_SESSIONS[-129:])
        ),
        universe=(_spec("HANG_SENG"),),
    )

    snapshot = adapter.fetch_cross_market_history(
        as_of=datetime(2026, 8, 14, tzinfo=UTC)
    )

    assert snapshot.series == ()
    assert snapshot.degraded is True
    assert len(snapshot.missing) == 1
    missing = snapshot.missing[0]
    assert missing.market_id == "HANG_SENG"
    assert missing.failure_code is CrossMarketHistoryFailureCode.INSUFFICIENT_HISTORY
    assert "129" in missing.reason
    assert missing.expected_source.endswith("stock_hk_index_daily_sina(HSI)")
    assert "no proxy series were substituted" in " ".join(snapshot.warnings)


def test_timeout_and_upstream_failures_are_isolated_per_exact_series() -> None:
    slow_finished = threading.Event()

    def slow_mainland(*, symbol: str) -> pd.DataFrame:
        try:
            wall_time.sleep(0.05)
            return _frame()
        finally:
            slow_finished.set()

    def failed_us(*, symbol: str) -> pd.DataFrame:
        raise ConnectionError("frozen disconnect")

    client = SimpleNamespace(
        stock_zh_index_daily=slow_mainland,
        stock_hk_index_daily_sina=lambda *, symbol: _frame(),
        index_us_stock_sina=failed_us,
    )
    adapter = AKShareCrossMarketHistoryAdapter(
        client,
        # 把刻意超时的原生调用放在最后。其工作线程超过截止时间后，会一直
        # 持有进程级安全锁直至真正退出，因而后续 V8 调用无法与之重叠。
        universe=(_spec("HANG_SENG"), _spec("S_AND_P_500"), _spec("CSI_300")),
        timeout_seconds=0.005,
    )

    snapshot = adapter.fetch_cross_market_history(
        as_of=datetime(2026, 8, 14, tzinfo=UTC)
    )
    assert slow_finished.wait(timeout=1.0)

    assert [item.market_id for item in snapshot.series] == ["HANG_SENG"]
    missing = {item.market_id: item for item in snapshot.missing}
    assert missing["CSI_300"].failure_code is CrossMarketHistoryFailureCode.TIMEOUT
    assert missing["S_AND_P_500"].failure_code is CrossMarketHistoryFailureCode.UPSTREAM_ERROR


def test_missing_endpoint_and_bad_payload_remain_separate_missing_items() -> None:
    client = SimpleNamespace(
        stock_zh_index_daily=lambda *, symbol: pd.DataFrame(
            [{"date": "2026-08-13", "not_close": "4000"}]
        )
    )
    adapter = AKShareCrossMarketHistoryAdapter(
        client,
        universe=(_spec("CSI_300"), _spec("HANG_SENG")),
    )

    snapshot = adapter.fetch_cross_market_history(
        as_of=datetime(2026, 8, 14, tzinfo=UTC)
    )

    missing = {item.market_id: item for item in snapshot.missing}
    assert missing["CSI_300"].failure_code is CrossMarketHistoryFailureCode.INVALID_PAYLOAD
    assert missing["HANG_SENG"].failure_code is (
        CrossMarketHistoryFailureCode.ENDPOINT_UNAVAILABLE
    )


def test_future_malformed_close_cannot_contaminate_pit_history() -> None:
    records = _frame().to_dict(orient="records")
    records[-1]["close"] = "not-a-number"
    adapter = AKShareCrossMarketHistoryAdapter(
        SimpleNamespace(
            stock_hk_index_daily_sina=lambda *, symbol: pd.DataFrame(records)
        ),
        universe=(_spec("HANG_SENG"),),
    )

    snapshot = adapter.fetch_cross_market_history(
        as_of=datetime(2026, 8, 13, 16, 9, tzinfo=ZoneInfo("Asia/Hong_Kong"))
    )

    assert snapshot.missing == ()
    assert len(snapshot.series[0].observations) == MINIMUM_CROSS_MARKET_HISTORY
    assert snapshot.series[0].observations[-1].session_date == date(2026, 8, 12)


def test_protocols_and_async_facade_are_typed_and_usable() -> None:
    adapter = AKShareCrossMarketHistoryAdapter(
        SimpleNamespace(stock_zh_index_daily=lambda *, symbol: _frame()),
        universe=(_spec("CSI_300"),),
    )

    assert isinstance(adapter, CrossMarketHistoryData)
    assert isinstance(adapter, AsyncCrossMarketHistoryData)
    snapshot = asyncio.run(
        adapter.fetch_cross_market_history_async(
            as_of=datetime(2026, 8, 14, tzinfo=UTC)
        )
    )
    assert snapshot.series[0].market_id == "CSI_300"


@pytest.mark.parametrize(
    ("as_of", "minimum", "message"),
    [
        (datetime(2026, 8, 13), 130, "as_of must be timezone-aware"),
        (datetime(2026, 8, 13, tzinfo=UTC), 129, "at least 130"),
    ],
)
def test_request_rejects_ambiguous_pit_inputs(
    as_of: datetime,
    minimum: int,
    message: str,
) -> None:
    adapter = AKShareCrossMarketHistoryAdapter(
        SimpleNamespace(stock_zh_index_daily=lambda *, symbol: _frame()),
        universe=(_spec("CSI_300"),),
    )

    with pytest.raises(ValueError, match=message):
        adapter.fetch_cross_market_history(
            as_of=as_of,
            minimum_observations=minimum,
        )


def test_custom_universe_rejects_duplicate_market_ids() -> None:
    duplicate = CrossMarketHistorySpec(
        "CSI_300",
        "duplicate",
        "stock_zh_index_daily",
        "sh000905",
        "Asia/Shanghai",
        time(15, 0),
    )

    with pytest.raises(ValueError, match="duplicate market_id"):
        AKShareCrossMarketHistoryAdapter(
            SimpleNamespace(),
            universe=(_spec("CSI_300"), duplicate),
        )
