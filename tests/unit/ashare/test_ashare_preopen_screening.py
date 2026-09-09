from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from gribuki_trade.adapters.ashare_preopen_screening import (
    AKSharePreopenScreeningAdapter,
)
from gribuki_trade.adapters.ashare_screening import (
    AKShareScreeningPointInTimeError,
)
from gribuki_trade.ports.ashare_screening import (
    AsyncAShareScreeningData,
    ScreeningFactorId,
    ScreeningHistoryPolicy,
    ScreeningSourceQuality,
)

FIXTURE = (
    Path(__file__).parents[2]
    / "fixtures"
    / "ashare_screening"
    / "provider_payloads.json"
)
FROZEN = json.loads(FIXTURE.read_text(encoding="utf-8"))
SHANGHAI = ZoneInfo("Asia/Shanghai")
VERIFIED_SESSION = date(2026, 8, 13)
PREOPEN_NOW = datetime(2026, 8, 14, 8, 30, tzinfo=SHANGHAI)


class FrozenPreopenClient:
    def __init__(self, *, include_future_history: bool = False) -> None:
        self.include_future_history = include_future_history
        self.history_calls: list[dict[str, object]] = []

    def stock_zh_a_spot_em(self) -> pd.DataFrame:
        return pd.DataFrame(FROZEN["eastmoney"])

    def stock_zh_a_spot_tx(self) -> pd.DataFrame:
        return pd.DataFrame(FROZEN["tencent"])

    def stock_info_sh_name_code(self, *, symbol: str) -> pd.DataFrame:
        key = "sse_star" if "科" in symbol else "sse_main"
        return pd.DataFrame(FROZEN["metadata"][key])

    def stock_info_sz_name_code(self, *, symbol: str) -> pd.DataFrame:
        return pd.DataFrame(FROZEN["metadata"]["szse"])

    def stock_info_bj_name_code(self) -> pd.DataFrame:
        return pd.DataFrame(FROZEN["metadata"]["bse"])

    def stock_zh_a_hist(self, **kwargs: object) -> pd.DataFrame:
        self.history_calls.append(dict(kwargs))
        frame = _history_frame(VERIFIED_SESSION)
        if self.include_future_history:
            future = frame.iloc[-1].copy()
            future["日期"] = "2026-08-14"
            frame = pd.concat([frame, pd.DataFrame([future])], ignore_index=True)
        return frame


class SequenceClock:
    def __init__(self, *values: datetime) -> None:
        self._values = list(values)

    def __call__(self) -> datetime:
        if len(self._values) > 1:
            return self._values.pop(0)
        return self._values[0]


def _history_frame(last_session: date) -> pd.DataFrame:
    dates: list[date] = []
    cursor = last_session
    while len(dates) < 201:
        if cursor.weekday() < 5:
            dates.append(cursor)
        cursor -= timedelta(days=1)
    dates.reverse()
    rows: list[dict[str, str]] = []
    for index, trade_date in enumerate(dates):
        close = Decimal("10") + Decimal("0.01") * index
        rows.append(
            {
                "日期": trade_date.isoformat(),
                "开盘": str(close - Decimal("0.02")),
                "收盘": str(close),
                "最高": str(close + Decimal("0.10")),
                "最低": str(close - Decimal("0.10")),
                "成交量": str(1_000_000 + 1_000 * index),
                "成交额": str(100_000_000 + 100_000 * index),
                "涨跌额": "0" if index == 0 else "0.01",
            }
        )
    return pd.DataFrame(rows)


def _adapter(
    client: FrozenPreopenClient | None = None,
    *,
    now: Any = lambda: PREOPEN_NOW,
    verified_session: date = VERIFIED_SESSION,
) -> AKSharePreopenScreeningAdapter:
    return AKSharePreopenScreeningAdapter(
        verified_session,
        client or FrozenPreopenClient(),
        minimum_universe_count=3,
        now=now,
    )


def test_preopen_universe_is_sh_sz_only_degraded_and_not_backdated() -> None:
    completed = PREOPEN_NOW + timedelta(seconds=7)
    adapter = _adapter(
        now=SequenceClock(PREOPEN_NOW, completed),
    )

    snapshot = asyncio.run(
        adapter.fetch_universe_snapshot(
            as_of=VERIFIED_SESSION,
            known_at=PREOPEN_NOW,
        )
    )

    assert isinstance(adapter, AsyncAShareScreeningData)
    assert snapshot.as_of == VERIFIED_SESSION
    assert snapshot.available_at == completed
    assert snapshot.observed_at == completed
    assert snapshot.available_at > PREOPEN_NOW
    assert snapshot.quality is ScreeningSourceQuality.DEGRADED
    assert len(snapshot.source_revision) == 64
    assert all(item.symbol.endswith((".SH", ".SZ")) for item in snapshot.records)
    assert not any(item.symbol.endswith(".BJ") for item in snapshot.records)
    bank = next(item for item in snapshot.records if item.symbol == "000001.SZ")
    assert bank.industry == "银行"
    assert bank.listing_days == (VERIFIED_SESSION - date(1991, 4, 3)).days
    warnings = set(snapshot.warnings)
    assert "AVAILABLE_AT_IS_ACTUAL_PREOPEN_COLLECTION_TIME_NOT_PRIOR_CLOSE" in warnings
    assert "SH_SZ_ONLY_BSE_EXCLUDED" in warnings
    assert any(item.startswith("VERIFIED_LATEST_COMPLETED_SESSION:") for item in warnings)


@pytest.mark.parametrize(
    ("verified_session", "as_of", "known_at", "now", "message"),
    (
        (
            VERIFIED_SESSION,
            date(2026, 8, 12),
            PREOPEN_NOW,
            PREOPEN_NOW,
            "must equal verified",
        ),
        (
            date(2026, 8, 14),
            date(2026, 8, 14),
            PREOPEN_NOW,
            PREOPEN_NOW,
            "must precede",
        ),
        (
            VERIFIED_SESSION,
            VERIFIED_SESSION,
            PREOPEN_NOW.replace(hour=9, minute=26),
            PREOPEN_NOW.replace(hour=9, minute=26),
            "after 09:25",
        ),
        (
            VERIFIED_SESSION,
            VERIFIED_SESSION,
            PREOPEN_NOW - timedelta(days=1),
            PREOPEN_NOW,
            "current Shanghai date",
        ),
    ),
)
def test_preopen_point_in_time_boundaries_fail_closed(
    verified_session: date,
    as_of: date,
    known_at: datetime,
    now: datetime,
    message: str,
) -> None:
    adapter = _adapter(now=lambda: now, verified_session=verified_session)

    with pytest.raises(AKShareScreeningPointInTimeError, match=message):
        asyncio.run(
            adapter.fetch_universe_snapshot(as_of=as_of, known_at=known_at)
        )


def test_preopen_factors_query_and_calculate_only_through_verified_session() -> None:
    client = FrozenPreopenClient()
    snapshot = asyncio.run(
        _adapter(client).fetch_factor_snapshot(
            ("600000.SH", "000001.SZ"),
            as_of=VERIFIED_SESSION,
            known_at=PREOPEN_NOW,
        )
    )

    assert snapshot.as_of == VERIFIED_SESSION
    assert snapshot.quality is ScreeningSourceQuality.DEGRADED
    assert snapshot.history_policy is (
        ScreeningHistoryPolicy.UNADJUSTED_WITH_CORPORATE_ACTION_GUARD
    )
    assert len(snapshot.source_revision) == 64
    assert {call["end_date"] for call in client.history_calls} == {"20260813"}
    assert {call["adjust"] for call in client.history_calls} == {""}
    assert all(item.warnings == () for item in snapshot.records)
    assert all(
        item.value_for(ScreeningFactorId.MOMENTUM_20) is not None
        for item in snapshot.records
    )


def test_future_history_row_is_rejected_instead_of_truncated_silently() -> None:
    client = FrozenPreopenClient(include_future_history=True)

    snapshot = asyncio.run(
        _adapter(client).fetch_factor_snapshot(
            ("600000.SH",),
            as_of=VERIFIED_SESSION,
            known_at=PREOPEN_NOW,
        )
    )

    record = snapshot.records[0]
    assert record.warnings == (
        "HISTORY_FETCH_FAILED:AKShareScreeningPayloadError",
        "HISTORY_FALLBACK_FAILED:AKShareScreeningDataError",
    )
    assert all(item.value is None for item in record.values)


@pytest.mark.parametrize("symbol", ("430047.BJ", "430047", "430047.SZ"))
def test_bse_factor_symbol_is_rejected_before_any_provider_call(symbol: str) -> None:
    client = FrozenPreopenClient()

    with pytest.raises(ValueError, match="SH/SZ only"):
        asyncio.run(
            _adapter(client).fetch_factor_snapshot(
                (symbol,),
                as_of=VERIFIED_SESSION,
                known_at=PREOPEN_NOW,
            )
        )

    assert client.history_calls == []
