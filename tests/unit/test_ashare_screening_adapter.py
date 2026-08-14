from __future__ import annotations

import asyncio
import json
import math
import threading
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from gribuki_trade.adapters.ashare_screening import (
    AKSHARE_HISTORY_SOURCE_ID,
    EASTMONEY_SCREENING_SOURCE_ID,
    SINA_HISTORY_SOURCE_ID,
    TENCENT_SCREENING_SOURCE_ID,
    AKShareAShareScreeningAdapter,
    AKShareScreeningPointInTimeError,
    AKShareScreeningSourcesExhaustedError,
)
from gribuki_trade.ports.ashare_screening import (
    AShareBoard,
    AsyncAShareScreeningData,
    ScreeningFactorId,
    ScreeningHistoryPolicy,
    ScreeningSourceQuality,
)

FIXTURE = (
    Path(__file__).parents[1]
    / "fixtures"
    / "ashare_screening"
    / "provider_payloads.json"
)
FROZEN = json.loads(FIXTURE.read_text(encoding="utf-8"))
SHANGHAI = ZoneInfo("Asia/Shanghai")
AS_OF = date(2026, 8, 14)
NOW = datetime(2026, 8, 14, 16, 0, tzinfo=SHANGHAI)


class FrozenAKShareClient:
    def __init__(
        self,
        *,
        fail_primary: bool = False,
        fail_history_symbols: frozenset[str] = frozenset(),
        corporate_action_symbols: frozenset[str] = frozenset(),
    ) -> None:
        self.fail_primary = fail_primary
        self.fail_history_symbols = fail_history_symbols
        self.corporate_action_symbols = corporate_action_symbols
        self.history_calls: list[dict[str, object]] = []

    def stock_zh_a_spot_em(self) -> pd.DataFrame:
        if self.fail_primary:
            raise RuntimeError("frozen Eastmoney disconnect")
        return pd.DataFrame(FROZEN["eastmoney"])

    def stock_zh_a_spot_tx(self) -> pd.DataFrame:
        return pd.DataFrame(FROZEN["tencent"])

    def stock_info_sh_name_code(self, *, symbol: str) -> pd.DataFrame:
        key = "sse_star" if symbol == "科创板" else "sse_main"
        return pd.DataFrame(FROZEN["metadata"][key])

    def stock_info_sz_name_code(self, *, symbol: str) -> pd.DataFrame:
        assert symbol == "A股列表"
        return pd.DataFrame(FROZEN["metadata"]["szse"])

    def stock_info_bj_name_code(self) -> pd.DataFrame:
        return pd.DataFrame(FROZEN["metadata"]["bse"])

    def stock_zh_a_hist(self, **kwargs: object) -> pd.DataFrame:
        self.history_calls.append(dict(kwargs))
        symbol = str(kwargs["symbol"])
        if symbol in self.fail_history_symbols:
            raise RuntimeError("frozen history failure")
        frame = _history_frame()
        if symbol in self.corporate_action_symbols:
            frame.loc[150, "涨跌额"] = "5.00"
        return frame


class SinaFallbackClient(FrozenAKShareClient):
    def __init__(self) -> None:
        super().__init__(fail_history_symbols=frozenset({"600000"}))
        self.sina_history_calls: list[dict[str, object]] = []

    def stock_zh_a_daily(self, **kwargs: object) -> pd.DataFrame:
        self.sina_history_calls.append(dict(kwargs))
        source = _history_frame()
        return pd.DataFrame(
            {
                "date": source.iloc[:, 0],
                "open": source.iloc[:, 1],
                "close": source.iloc[:, 2],
                "high": source.iloc[:, 3],
                "low": source.iloc[:, 4],
                "volume": source.iloc[:, 5],
                "amount": source.iloc[:, 6],
                "outstanding_share": 1_000_000_000,
                "turnover": 0.01,
            }
        )


class ConcurrentSinaFallbackClient(SinaFallbackClient):
    def __init__(self) -> None:
        super().__init__()
        self.active = 0
        self.maximum_active = 0
        self.counter_lock = threading.Lock()

    def stock_zh_a_hist(self, **kwargs: object) -> pd.DataFrame:
        self.history_calls.append(dict(kwargs))
        raise RuntimeError("frozen history failure")

    def stock_zh_a_daily(self, **kwargs: object) -> pd.DataFrame:
        with self.counter_lock:
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
        try:
            time.sleep(0.02)
            return super().stock_zh_a_daily(**kwargs)
        finally:
            with self.counter_lock:
                self.active -= 1


def _history_frame() -> pd.DataFrame:
    seed = FROZEN["history_seed"]
    sessions = int(seed["sessions"])
    dates: list[date] = []
    cursor = AS_OF
    while len(dates) < sessions:
        if cursor.weekday() < 5:
            dates.append(cursor)
        cursor -= timedelta(days=1)
    dates.reverse()
    start_close = Decimal(seed["start_close"])
    daily_step = Decimal(seed["daily_step"])
    start_volume = Decimal(seed["start_volume"])
    volume_step = Decimal(seed["volume_step"])
    start_amount = Decimal(seed["start_amount"])
    amount_step = Decimal(seed["amount_step"])
    rows: list[dict[str, str]] = []
    for index, trade_date in enumerate(dates):
        close = start_close + daily_step * index
        rows.append(
            {
                "日期": trade_date.isoformat(),
                "开盘": str(close - Decimal("0.02")),
                "收盘": str(close),
                "最高": str(close + Decimal("0.10")),
                "最低": str(close - Decimal("0.10")),
                "成交量": str(start_volume + volume_step * index),
                "成交额": str(start_amount + amount_step * index),
                "涨跌额": str(Decimal(0) if index == 0 else daily_step),
            }
        )
    return pd.DataFrame(rows)


def _adapter(client: FrozenAKShareClient, **kwargs: Any) -> AKShareAShareScreeningAdapter:
    parameters: dict[str, Any] = {
        "minimum_universe_count": 3,
        "now": lambda: NOW,
    }
    parameters.update(kwargs)
    return AKShareAShareScreeningAdapter(client, **parameters)


def test_primary_universe_has_strict_fields_units_metadata_and_revision() -> None:
    adapter = _adapter(FrozenAKShareClient())

    snapshot = asyncio.run(
        adapter.fetch_universe_snapshot(as_of=AS_OF, known_at=NOW)
    )

    assert isinstance(adapter, AsyncAShareScreeningData)
    assert snapshot.source_id == EASTMONEY_SCREENING_SOURCE_ID
    assert snapshot.quality is ScreeningSourceQuality.COMPLETE
    assert snapshot.available_at == datetime(2026, 8, 14, 15, 5, tzinfo=SHANGHAI)
    assert snapshot.observed_at == NOW
    assert len(snapshot.source_revision) == 64
    records = {item.symbol: item for item in snapshot.records}
    assert set(records) == {
        "000001.SZ",
        "300001.SZ",
        "430047.BJ",
        "600000.SH",
        "600001.SH",
        "600002.SH",
        "688001.SH",
    }
    assert records["600000.SH"].board is AShareBoard.SSE_MAIN
    assert records["688001.SH"].board is AShareBoard.STAR
    assert records["300001.SZ"].board is AShareBoard.CHINEXT
    assert records["430047.BJ"].board is AShareBoard.BSE
    assert records["000001.SZ"].industry == "银行"
    assert records["000001.SZ"].listing_days is not None
    assert records["600000.SH"].session_amount_cny == Decimal("483890000")
    assert records["600000.SH"].market_cap_cny == Decimal("305748000000")
    assert records["600001.SH"].is_st is True
    assert records["600002.SH"].is_suspended is True
    assert records["600002.SH"].is_tradable is False
    assert records["600002.SH"].last_price is None

    second = asyncio.run(
        adapter.fetch_universe_snapshot(as_of=AS_OF, known_at=NOW)
    )
    assert second.source_revision == snapshot.source_revision


def test_tencent_fallback_has_independent_schema_and_explicit_unit_conversion() -> None:
    snapshot = asyncio.run(
        _adapter(FrozenAKShareClient(fail_primary=True)).fetch_universe_snapshot(
            as_of=AS_OF,
            known_at=NOW,
        )
    )

    assert snapshot.source_id == TENCENT_SCREENING_SOURCE_ID
    assert snapshot.quality is ScreeningSourceQuality.DEGRADED
    record = next(item for item in snapshot.records if item.symbol == "600000.SH")
    assert record.session_amount_cny == Decimal("483890000")
    assert record.market_cap_cny == Decimal("305748000000")
    suspended = next(item for item in snapshot.records if item.symbol == "600002.SH")
    assert suspended.is_suspended is True
    assert suspended.is_tradable is False
    warning_text = " ".join(snapshot.warnings)
    assert "CNY 10,000" in warning_text
    assert "prior source failed" in warning_text


def test_universe_coverage_and_current_close_are_hard_boundaries() -> None:
    with pytest.raises(AKShareScreeningSourcesExhaustedError):
        asyncio.run(
            _adapter(
                FrozenAKShareClient(),
                minimum_universe_count=10,
            ).fetch_universe_snapshot(as_of=AS_OF, known_at=NOW)
        )

    before_close = NOW.replace(hour=15, minute=4)
    with pytest.raises(AKShareScreeningPointInTimeError, match="unavailable before"):
        asyncio.run(
            _adapter(FrozenAKShareClient()).fetch_universe_snapshot(
                as_of=AS_OF,
                known_at=before_close,
            )
        )

    with pytest.raises(AKShareScreeningPointInTimeError, match="historical replay"):
        asyncio.run(
            _adapter(FrozenAKShareClient()).fetch_universe_snapshot(
                as_of=AS_OF - timedelta(days=1),
                known_at=NOW,
            )
        )


def test_unadjusted_history_computes_all_raw_factors_without_neutral_fill() -> None:
    client = FrozenAKShareClient()
    snapshot = asyncio.run(
        _adapter(client).fetch_factor_snapshot(
            ("600000.SH",),
            as_of=AS_OF,
            known_at=NOW,
        )
    )

    assert snapshot.source_id == AKSHARE_HISTORY_SOURCE_ID
    assert snapshot.history_policy is (
        ScreeningHistoryPolicy.UNADJUSTED_WITH_CORPORATE_ACTION_GUARD
    )
    assert snapshot.quality is ScreeningSourceQuality.COMPLETE
    assert len(snapshot.source_revision) == 64
    assert client.history_calls[0]["adjust"] == ""
    assert client.history_calls[0]["end_date"] == "20260814"
    record = snapshot.records[0]
    assert record.warnings == ()
    values = {item.factor_id: item.value for item in record.values}
    assert set(values) == set(ScreeningFactorId)
    assert all(value is not None and math.isfinite(value) for value in values.values())

    closes = tuple(Decimal(item) for item in _history_frame()["收盘"])
    assert values[ScreeningFactorId.MOMENTUM_20] == pytest.approx(
        float(closes[-1] / closes[-21] - Decimal(1))
    )
    assert values[ScreeningFactorId.MOMENTUM_120_SKIP_5] == pytest.approx(
        float(closes[-6] / closes[-126] - Decimal(1))
    )
    amounts = tuple(Decimal(item) for item in _history_frame()["成交额"])
    expected_average = sum(amounts[-20:], Decimal(0)) / Decimal(20)
    assert values[ScreeningFactorId.AVERAGE_AMOUNT_20_CNY] == pytest.approx(
        float(expected_average)
    )


def test_corporate_action_guard_withholds_price_factors_but_keeps_liquidity_audit() -> None:
    snapshot = asyncio.run(
        _adapter(
            FrozenAKShareClient(corporate_action_symbols=frozenset({"600000"}))
        ).fetch_factor_snapshot(("600000.SH",), as_of=AS_OF, known_at=NOW)
    )

    assert snapshot.quality is ScreeningSourceQuality.DEGRADED
    record = snapshot.records[0]
    assert record.warnings[0].startswith("CORPORATE_ACTION_DISCONTINUITY:")
    assert record.value_for(ScreeningFactorId.MOMENTUM_20) is None
    assert record.value_for(ScreeningFactorId.AVERAGE_AMOUNT_20_CNY) is not None


def test_sina_unadjusted_history_is_an_explicit_degraded_fallback() -> None:
    client = SinaFallbackClient()

    snapshot = asyncio.run(
        _adapter(client).fetch_factor_snapshot(
            ("600000.SH",),
            as_of=AS_OF,
            known_at=NOW,
        )
    )

    assert snapshot.quality is ScreeningSourceQuality.DEGRADED
    assert SINA_HISTORY_SOURCE_ID in snapshot.source_id
    assert "SINA_HISTORY_FALLBACK_SYMBOLS:1/1" in snapshot.warnings
    assert client.sina_history_calls == [
        {
            "symbol": "sh600000",
            "start_date": "20250610",
            "end_date": "20260814",
            "adjust": "",
        }
    ]
    record = snapshot.records[0]
    assert f"HISTORY_SOURCE_FALLBACK:{SINA_HISTORY_SOURCE_ID}" in record.warnings
    assert "SINA_PREVIOUS_CLOSE_DERIVED_FROM_ADJACENT_RAW_CLOSES" in record.warnings
    assert all(item.value is not None for item in record.values)


def test_v8_backed_sina_fallback_is_process_wide_serialized() -> None:
    client = ConcurrentSinaFallbackClient()

    snapshot = asyncio.run(
        _adapter(client, history_concurrency=4).fetch_factor_snapshot(
            ("600000.SH", "000001.SZ"),
            as_of=AS_OF,
            known_at=NOW,
        )
    )

    assert client.maximum_active == 1
    assert all(
        f"HISTORY_SOURCE_FALLBACK:{SINA_HISTORY_SOURCE_ID}" in item.warnings
        for item in snapshot.records
    )


def test_per_symbol_history_failure_is_isolated_and_batch_budget_is_enforced() -> None:
    snapshot = asyncio.run(
        _adapter(
            FrozenAKShareClient(fail_history_symbols=frozenset({"000001"}))
        ).fetch_factor_snapshot(
            ("600000.SH", "000001.SZ"),
            as_of=AS_OF,
            known_at=NOW,
        )
    )

    assert snapshot.quality is ScreeningSourceQuality.DEGRADED
    by_symbol = {item.symbol: item for item in snapshot.records}
    assert by_symbol["600000.SH"].warnings == ()
    assert by_symbol["000001.SZ"].warnings == (
        "HISTORY_FETCH_FAILED:AKShareScreeningDataError",
        "HISTORY_FALLBACK_FAILED:AKShareScreeningDataError",
    )
    assert all(item.value is None for item in by_symbol["000001.SZ"].values)

    with pytest.raises(ValueError, match="exceeds adapter budget"):
        asyncio.run(
            _adapter(FrozenAKShareClient(), max_factor_symbols=1).fetch_factor_snapshot(
                ("600000.SH", "000001.SZ"),
                as_of=AS_OF,
                known_at=NOW,
            )
        )


def test_constructor_rejects_unsafe_history_and_concurrency_parameters() -> None:
    with pytest.raises(ValueError, match="at least 126"):
        AKShareAShareScreeningAdapter(minimum_history_sessions=125)
    with pytest.raises(ValueError, match="between 1 and 16"):
        AKShareAShareScreeningAdapter(history_concurrency=17)
