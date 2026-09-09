from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from gribuki_trade.adapters import (
    AKSHARE_ETF_PROFILE_SOURCE_ID,
    AKSHARE_STOCK_PROFILE_SOURCE_ID,
    AKShareInstrumentProfileAdapter,
    InstrumentProfileDataError,
    InstrumentProfileFailureCode,
    InstrumentProfilePointInTimeError,
)

NOW = datetime(2026, 8, 14, 8, tzinfo=UTC)


def _stock_frame(
    *,
    code: object = "600000",
    name: object = "浦发银行",
    industry: object = "银行",
    market_cap: object = "305748000000",
    listing_date: object = "19991110",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"item": "股票代码", "value": code},
            {"item": "股票简称", "value": name},
            {"item": "行业", "value": industry},
            {"item": "总市值", "value": market_cap},
            {"item": "上市时间", "value": listing_date},
        ]
    )


def _stock_client(frame: pd.DataFrame) -> SimpleNamespace:
    return SimpleNamespace(stock_individual_info_em=lambda *, symbol: frame)


def test_stock_profile_preserves_current_provider_facts_and_provenance() -> None:
    requested: list[str] = []

    def endpoint(*, symbol: str) -> pd.DataFrame:
        requested.append(symbol)
        return _stock_frame()

    profile = asyncio.run(
        AKShareInstrumentProfileAdapter(
            SimpleNamespace(stock_individual_info_em=endpoint),
            now=lambda: NOW,
        ).fetch("600000.SH", known_at=NOW)
    )

    assert requested == ["600000"]
    assert profile.symbol == "600000.SH"
    assert profile.name == "浦发银行"
    assert profile.exchange == "sse"
    assert profile.board == "sse_main"
    assert profile.size_tier == "mega"
    assert profile.industry == "银行"
    assert profile.source_id == AKSHARE_STOCK_PROFILE_SOURCE_ID
    assert profile.verified_on.isoformat() == "2026-08-14"
    assert "当前公开总市值：305748000000元" in profile.background_facts
    assert "公开上市日期：1999-11-10" in profile.background_facts
    assert any(NOW.isoformat() in item for item in profile.background_facts)


@pytest.mark.parametrize(
    ("symbol", "code", "exchange", "board"),
    [
        ("000001", "000001", "szse", "szse_main"),
        ("300001.SZ", "300001", "szse", "chinext"),
        ("688001", "688001", "sse", "star"),
        ("430047", "430047", "bse", "bse"),
        ("920001.BJ", "920001", "bse", "bse"),
    ],
)
def test_stock_code_families_have_exact_exchange_and_board(
    symbol: str,
    code: str,
    exchange: str,
    board: str,
) -> None:
    frame = _stock_frame(code=code, market_cap="--", listing_date="--")
    profile = asyncio.run(
        AKShareInstrumentProfileAdapter(
            _stock_client(frame),
            now=lambda: NOW,
        ).fetch(symbol, known_at=NOW)
    )

    suffix = {"sse": "SH", "szse": "SZ", "bse": "BJ"}[exchange]
    assert profile.symbol == f"{code}.{suffix}"
    assert profile.exchange == exchange
    assert profile.board == board
    assert profile.size_tier == "unclassified"
    assert not any("总市值" in item for item in profile.background_facts)
    assert not any("上市日期" in item for item in profile.background_facts)


@pytest.mark.parametrize(
    ("symbol", "code", "expected_exchange", "expected_board"),
    [
        ("510300", 510300.0, "sse", "sse_etf"),
        ("159915.SZ", "159915", "szse", "szse_etf"),
    ],
)
def test_etf_profile_requires_an_exact_current_etf_row(
    symbol: str,
    code: object,
    expected_exchange: str,
    expected_board: str,
) -> None:
    frame = pd.DataFrame(
        [
            {"代码": "512000", "名称": "证券ETF"},
            {"代码": code, "名称": "目标ETF"},
        ]
    )
    client = SimpleNamespace(fund_etf_spot_em=lambda: frame)

    profile = asyncio.run(
        AKShareInstrumentProfileAdapter(client, now=lambda: NOW).fetch(
            symbol, known_at=NOW
        )
    )

    assert profile.name == "目标ETF"
    assert profile.asset_type == "etf"
    assert profile.exchange == expected_exchange
    assert profile.board == expected_board
    assert profile.source_id == AKSHARE_ETF_PROFILE_SOURCE_ID
    assert "未由该数据源提供" in profile.risk_tags[1]
    assert any("未作推断" in item for item in profile.background_facts)


@pytest.mark.parametrize(
    ("updates", "expected_code"),
    [
        ({"industry": "--"}, InstrumentProfileFailureCode.STOCK_INDUSTRY_MISSING),
        ({"industry": pd.NA}, InstrumentProfileFailureCode.STOCK_INDUSTRY_MISSING),
        ({"name": None}, InstrumentProfileFailureCode.STOCK_NAME_MISSING),
        ({"code": "--"}, InstrumentProfileFailureCode.STOCK_CODE_MISSING),
        ({"code": "not-a-code"}, InstrumentProfileFailureCode.STOCK_CODE_INVALID),
        ({"market_cap": "unknown"}, InstrumentProfileFailureCode.INVALID_MARKET_CAP),
        ({"listing_date": "1999-99-99"}, InstrumentProfileFailureCode.INVALID_LISTING_DATE),
        ({"listing_date": "20270814"}, InstrumentProfileFailureCode.FUTURE_LISTING_DATE),
    ],
)
def test_missing_or_invalid_stock_facts_fail_closed_without_fabrication(
    updates: dict[str, object],
    expected_code: InstrumentProfileFailureCode,
) -> None:
    with pytest.raises(InstrumentProfileDataError) as captured:
        asyncio.run(
            AKShareInstrumentProfileAdapter(
                _stock_client(_stock_frame(**updates)),
                now=lambda: NOW,
            ).fetch("600000.SH", known_at=NOW)
        )

    assert captured.value.failure_code is expected_code
    assert captured.value.code == expected_code.value


def test_stock_symbol_mismatch_and_duplicate_etf_are_rejected() -> None:
    with pytest.raises(InstrumentProfileDataError) as mismatch:
        asyncio.run(
            AKShareInstrumentProfileAdapter(
                _stock_client(_stock_frame(code="600001")),
                now=lambda: NOW,
            ).fetch("600000.SH", known_at=NOW)
        )
    assert mismatch.value.code == "SYMBOL_MISMATCH"

    duplicate = pd.DataFrame(
        [{"代码": "510300", "名称": "A"}, {"代码": "510300", "名称": "B"}]
    )
    with pytest.raises(InstrumentProfileDataError) as duplicated:
        asyncio.run(
            AKShareInstrumentProfileAdapter(
                SimpleNamespace(fund_etf_spot_em=lambda: duplicate),
                now=lambda: NOW,
            ).fetch("510300.SH", known_at=NOW)
        )
    assert duplicated.value.code == "DUPLICATE_ETF_PROFILE"


def test_live_profile_endpoint_cannot_impersonate_a_historical_revision() -> None:
    calls: list[str] = []
    client = SimpleNamespace(
        stock_individual_info_em=lambda *, symbol: calls.append(symbol) or _stock_frame()
    )
    adapter = AKShareInstrumentProfileAdapter(client, now=lambda: NOW)

    with pytest.raises(InstrumentProfilePointInTimeError) as historical:
        asyncio.run(
            adapter.fetch("600000.SH", known_at=NOW - timedelta(days=1))
        )
    assert historical.value.code == "LIVE_PROFILE_HISTORICAL_UNSUPPORTED"

    with pytest.raises(InstrumentProfilePointInTimeError) as stale:
        asyncio.run(
            adapter.fetch("600000.SH", known_at=NOW - timedelta(hours=1))
        )
    assert stale.value.code == "LIVE_PROFILE_CUTOFF_STALE"
    assert calls == []


def test_observation_clock_cannot_move_backward_or_cross_session_date() -> None:
    backward_times = iter((NOW, NOW - timedelta(seconds=1)))
    with pytest.raises(InstrumentProfilePointInTimeError) as backward:
        asyncio.run(
            AKShareInstrumentProfileAdapter(
                _stock_client(_stock_frame()),
                now=lambda: next(backward_times),
            ).fetch("600000.SH", known_at=NOW)
        )
    assert backward.value.code == "COLLECTOR_CLOCK_MOVED_BACKWARD"

    crossed_times = iter((NOW, NOW + timedelta(hours=9)))
    with pytest.raises(InstrumentProfilePointInTimeError) as crossed:
        asyncio.run(
            AKShareInstrumentProfileAdapter(
                _stock_client(_stock_frame()),
                point_in_time_tolerance=timedelta(hours=12),
                now=lambda: next(crossed_times),
            ).fetch("600000.SH", known_at=NOW)
        )
    assert crossed.value.code == "COLLECTOR_DATE_CHANGED"


def test_provider_timeout_and_unexpected_failure_have_stable_codes() -> None:
    def slow(*, symbol: str) -> pd.DataFrame:
        time.sleep(0.1)
        return _stock_frame(code=symbol)

    with pytest.raises(InstrumentProfileDataError) as timeout:
        asyncio.run(
            AKShareInstrumentProfileAdapter(
                SimpleNamespace(stock_individual_info_em=slow),
                timeout_seconds=0.01,
                now=lambda: NOW,
            ).fetch("600000.SH", known_at=NOW)
        )
    assert timeout.value.code == "PROFILE_TIMEOUT"

    def broken(*, symbol: str) -> pd.DataFrame:
        raise RuntimeError(f"upstream failed for {symbol}")

    with pytest.raises(InstrumentProfileDataError) as upstream:
        asyncio.run(
            AKShareInstrumentProfileAdapter(
                SimpleNamespace(stock_individual_info_em=broken),
                now=lambda: NOW,
            ).fetch("600000.SH", known_at=NOW)
        )
    assert upstream.value.code == "PROFILE_UPSTREAM_ERROR"
    assert upstream.value.__cause__ is None


@pytest.mark.parametrize(
    "symbol",
    ["600000.SZ", "000001.SH", "430047.SH", "900901.SH", "not-a-symbol"],
)
def test_symbol_validation_rejects_cross_exchange_and_non_a_share_codes(
    symbol: str,
) -> None:
    with pytest.raises(ValueError):
        asyncio.run(
            AKShareInstrumentProfileAdapter(now=lambda: NOW).fetch(
                symbol, known_at=NOW
            )
        )


def test_constructor_rejects_non_finite_timeout_and_non_positive_pit_window() -> None:
    with pytest.raises(ValueError, match="positive and finite"):
        AKShareInstrumentProfileAdapter(timeout_seconds=float("inf"))
    with pytest.raises(ValueError, match="point_in_time_tolerance"):
        AKShareInstrumentProfileAdapter(point_in_time_tolerance=timedelta(0))
