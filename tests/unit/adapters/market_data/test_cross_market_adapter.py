import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from gribuki_trade.adapters.cross_market import (
    DEFAULT_CROSS_MARKET_UNIVERSE,
    AKShareCrossMarketAdapter,
)
from gribuki_trade.ports.cross_market import (
    CrossMarketDataTimeoutError,
    CrossMarketInstrumentSpec,
    CrossMarketPayloadError,
    CrossMarketSegment,
    build_three_decimal_summary,
)


def _spec(
    *,
    instrument_id: str = "CSI_300",
    code_aliases: tuple[str, ...] = ("000300",),
    name_aliases: tuple[str, ...] = ("沪深300",),
    timezone: str = "Asia/Shanghai",
    stale_after: timedelta = timedelta(hours=36),
) -> CrossMarketInstrumentSpec:
    return CrossMarketInstrumentSpec(
        instrument_id=instrument_id,
        display_name="沪深300指数",
        segment=CrossMarketSegment.A_SHARE,
        code_aliases=code_aliases,
        name_aliases=name_aliases,
        local_timezone=timezone,
        stale_after=stale_after,
    )


def _row(
    *,
    code: str = "000300",
    name: str = "沪深300",
    last: object = "4729.1234",
    change_percent: object = "-0.4567",
    quote_time: object = "2026-08-13 15:00:00",
) -> dict[str, object]:
    return {
        "代码": code,
        "名称": name,
        "最新价": last,
        "涨跌额": "-21.700",
        "涨跌幅": change_percent,
        "开盘价": "4750.000",
        "最高价": "4770.000",
        "最低价": "4700.000",
        "昨收价": "4750.8234",
        "振幅": "1.473",
        "最新行情时间": quote_time,
    }


def _daily_rows(
    observations: tuple[tuple[str, str], ...],
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": session_date,
                "open": str(Decimal(close) - Decimal("5")),
                "high": str(Decimal(close) + Decimal("10")),
                "low": str(Decimal(close) - Decimal("10")),
                "close": close,
                "volume": "1000000",
            }
            for session_date, close in observations
        ]
    )


def _canonical_spec(instrument_id: str) -> CrossMarketInstrumentSpec:
    return next(
        spec
        for spec in DEFAULT_CROSS_MARKET_UNIVERSE
        if spec.instrument_id == instrument_id
    )


def _adapter(
    rows: list[dict[str, object]],
    *,
    specs: tuple[CrossMarketInstrumentSpec, ...] | None = None,
    now: datetime = datetime(2026, 8, 13, 8, 1, tzinfo=UTC),
) -> tuple[AKShareCrossMarketAdapter, list[int]]:
    calls: list[int] = []

    def provider() -> pd.DataFrame:
        calls.append(1)
        return pd.DataFrame(rows)

    client = SimpleNamespace(index_global_spot_em=provider)
    adapter = AKShareCrossMarketAdapter(
        client,
        universe=specs or (_spec(),),
        now=lambda: now,
    )
    return adapter, calls


def test_fetch_uses_one_full_table_call_and_preserves_provenance() -> None:
    adapter, calls = _adapter([_row()])

    snapshot = adapter.fetch_cross_market_snapshot()

    assert len(calls) == 1
    assert snapshot.degraded is False
    assert snapshot.missing == ()
    quote = snapshot.quotes[0]
    assert quote.instrument_id == "CSI_300"
    assert quote.provider_code == "000300"
    assert quote.provider_name == "沪深300"
    assert quote.last == Decimal("4729.1234")
    assert quote.change_percent == Decimal("-0.4567")
    assert quote.previous_close == Decimal("4750.8234")
    assert quote.local_quote_time.isoformat() == "2026-08-13T15:00:00+08:00"
    assert quote.fetched_at == snapshot.fetched_at
    assert "index_global_spot_em" in quote.provider
    assert quote.stale is False
    assert "not synchronized exchange ticks" in " ".join(snapshot.warnings)
    assert "does not establish causality" in " ".join(snapshot.warnings)


def test_market_timestamp_is_converted_to_configured_local_timezone() -> None:
    spec = _spec(timezone="America/New_York")
    adapter, _ = _adapter([_row(quote_time="2026-08-13 04:00:00")], specs=(spec,))

    quote = adapter.fetch_cross_market_snapshot().quotes[0]

    assert quote.local_timezone == "America/New_York"
    assert quote.local_quote_time.isoformat() == "2026-08-12T16:00:00-04:00"
    assert "timezone-naive" in " ".join(quote.warnings)


def test_default_universe_covers_every_segment_and_keeps_gaps_explicit() -> None:
    rows = [
        _row(code="000001", name="上证指数"),
        _row(code="HSI", name="恒生指数"),
        _row(code="N225", name="日经225"),
        _row(code="SPX", name="标普500"),
        _row(code="UDI", name="美元指数"),
        _row(code="CRB", name="路透CRB商品指数"),
        _row(code="BDI", name="波罗的海BDI指数"),
    ]
    adapter, calls = _adapter(rows, specs=DEFAULT_CROSS_MARKET_UNIVERSE)

    snapshot = adapter.fetch_cross_market_snapshot()

    assert len(calls) == 1
    assert {quote.segment for quote in snapshot.quotes} == set(CrossMarketSegment)
    assert snapshot.degraded is True
    assert "CBOE_VIX" in {item.instrument_id for item in snapshot.missing}
    vix = next(item for item in snapshot.missing if item.instrument_id == "CBOE_VIX")
    assert vix.expected_codes == ("VIX",)
    assert "absent" in vix.reason
    assert "no proxy values were fabricated" in " ".join(snapshot.warnings)


def test_exact_name_alias_is_used_when_provider_code_is_not_configured() -> None:
    adapter, _ = _adapter([_row(code="provider-new-code")])

    snapshot = adapter.fetch_cross_market_snapshot()
    quote = snapshot.quotes[0]

    assert quote.provider_code == "provider-new-code"
    assert quote.degraded is True
    assert snapshot.degraded is True
    assert "matched by configured exact provider-name alias" in quote.warnings


def test_stale_quote_is_explicitly_degraded() -> None:
    now = datetime(2026, 8, 15, 8, 0, tzinfo=UTC)
    spec = _spec(stale_after=timedelta(hours=12))
    adapter, _ = _adapter([_row()], specs=(spec,), now=now)

    snapshot = adapter.fetch_cross_market_snapshot()

    assert snapshot.degraded is True
    assert snapshot.quotes[0].stale is True
    assert snapshot.quotes[0].degraded is True
    assert "provider quote age=" in " ".join(snapshot.quotes[0].warnings)


def test_three_decimal_summary_rounds_and_retains_status() -> None:
    adapter, _ = _adapter([_row()])
    snapshot = adapter.fetch_cross_market_snapshot()

    summary = build_three_decimal_summary(snapshot)

    assert summary.items[0].last == "4729.123"
    assert summary.items[0].change_percent == "-0.457"
    assert summary.items[0].local_quote_time == snapshot.quotes[0].local_quote_time
    assert "不构成因果关系" in summary.interpretation_note


def test_missing_required_column_raises_typed_payload_error() -> None:
    row = _row()
    del row["涨跌幅"]
    adapter, _ = _adapter([row])

    with pytest.raises(CrossMarketPayloadError, match="change_percent"):
        adapter.fetch_cross_market_snapshot()


@pytest.mark.parametrize(
    ("field", "bad_value", "message"),
    [
        ("最新价", "not-a-number", "last is not numeric"),
        ("涨跌幅", float("nan"), "change_percent is missing"),
        ("最新行情时间", "yesterday afternoon", "quote_time"),
    ],
)
def test_invalid_matched_fields_raise_typed_payload_error(
    field: str, bad_value: object, message: str
) -> None:
    row = _row()
    row[field] = bad_value
    adapter, _ = _adapter([row])

    with pytest.raises(CrossMarketPayloadError, match=message):
        adapter.fetch_cross_market_snapshot()


def test_duplicate_matching_rows_raise_typed_payload_error() -> None:
    adapter, _ = _adapter([_row(), _row(name="沪深300指数")])

    with pytest.raises(CrossMarketPayloadError, match="multiple provider rows"):
        adapter.fetch_cross_market_snapshot()


def test_materially_future_timestamp_raises_typed_payload_error() -> None:
    adapter, _ = _adapter([_row(quote_time="2026-08-13 16:30:00")])

    with pytest.raises(CrossMarketPayloadError, match="in the future"):
        adapter.fetch_cross_market_snapshot()


def test_async_wait_is_bounded_by_typed_timeout() -> None:
    def slow_provider() -> pd.DataFrame:
        time.sleep(0.05)
        return pd.DataFrame([_row()])

    client = SimpleNamespace(index_global_spot_em=slow_provider)
    adapter = AKShareCrossMarketAdapter(
        client,
        universe=(_spec(),),
        timeout_seconds=0.001,
    )

    with pytest.raises(CrossMarketDataTimeoutError, match="exceeded"):
        asyncio.run(adapter.fetch_cross_market_snapshot_async())


def test_overlapping_configured_aliases_are_rejected() -> None:
    duplicate = _spec(instrument_id="DUPLICATE")

    with pytest.raises(ValueError, match="overlaps"):
        AKShareCrossMarketAdapter(
            SimpleNamespace(index_global_spot_em=lambda: pd.DataFrame([_row()])),
            universe=(_spec(), duplicate),
        )


def test_independent_sina_daily_fallback_covers_exact_csi300_and_hsi() -> None:
    calls: list[tuple[str, str]] = []
    active = 0
    maximum_active = 0
    counter_lock = threading.Lock()

    def primary() -> pd.DataFrame:
        raise ConnectionResetError("simulated Eastmoney disconnect")

    def mainland(*, symbol: str) -> pd.DataFrame:
        nonlocal active, maximum_active
        with counter_lock:
            active += 1
            maximum_active = max(maximum_active, active)
        try:
            time.sleep(0.02)
            calls.append(("mainland", symbol))
            return _daily_rows(
                (
                    ("2026-08-11", "4700"),
                    ("2026-08-12", "4720"),
                    ("2026-08-13", "4729"),
                )
            )
        finally:
            with counter_lock:
                active -= 1

    def hong_kong(*, symbol: str) -> pd.DataFrame:
        nonlocal active, maximum_active
        with counter_lock:
            active += 1
            maximum_active = max(maximum_active, active)
        try:
            time.sleep(0.02)
            calls.append(("hong_kong", symbol))
            return _daily_rows(
                (
                    ("2026-08-11", "24800"),
                    ("2026-08-12", "25000"),
                    ("2026-08-13", "25250"),
                )
            )
        finally:
            with counter_lock:
                active -= 1

    client = SimpleNamespace(
        index_global_spot_em=primary,
        stock_zh_index_daily=mainland,
        stock_hk_index_daily_sina=hong_kong,
    )
    adapter = AKShareCrossMarketAdapter(
        client,
        universe=(_canonical_spec("CSI_300"), _canonical_spec("HANG_SENG")),
        now=lambda: datetime(2026, 8, 13, 9, 0, tzinfo=UTC),
    )

    snapshot = adapter.fetch_cross_market_snapshot()

    assert calls == [("mainland", "sh000300"), ("hong_kong", "HSI")]
    assert maximum_active == 1
    assert snapshot.degraded is True
    assert snapshot.missing == ()
    assert snapshot.provider == "AKShare/Sina daily close fallback"
    by_id = {quote.instrument_id: quote for quote in snapshot.quotes}
    csi = by_id["CSI_300"]
    assert csi.last == Decimal("4729")
    assert csi.previous_close == Decimal("4720")
    assert csi.change_amount == Decimal("9")
    assert csi.local_quote_time.isoformat() == "2026-08-13T15:00:00+08:00"
    assert csi.stale is False
    assert csi.degraded is True
    assert "configured regular-close anchor" in " ".join(csi.warnings)
    hsi = by_id["HANG_SENG"]
    assert hsi.change_percent == Decimal("1.00")
    assert hsi.local_quote_time.isoformat() == "2026-08-13T16:10:00+08:00"


def test_daily_fallback_does_not_use_current_session_before_close() -> None:
    def primary() -> pd.DataFrame:
        raise ConnectionError("simulated primary failure")

    client = SimpleNamespace(
        index_global_spot_em=primary,
        stock_zh_index_daily=lambda *, symbol: _daily_rows(
            (
                ("2026-08-11", "4500"),
                ("2026-08-12", "4600"),
                ("2026-08-13", "9999"),
            )
        ),
    )
    adapter = AKShareCrossMarketAdapter(
        client,
        universe=(_canonical_spec("CSI_300"),),
        now=lambda: datetime(2026, 8, 13, 6, 0, tzinfo=UTC),
    )

    quote = adapter.fetch_cross_market_snapshot().quotes[0]

    assert quote.last == Decimal("4600")
    assert quote.previous_close == Decimal("4500")
    assert quote.local_quote_time.isoformat() == "2026-08-12T15:00:00+08:00"


def test_exact_us_index_sina_symbols_are_used_without_proxy_substitution() -> None:
    calls: list[str] = []

    def primary() -> pd.DataFrame:
        raise ConnectionError("simulated primary failure")

    def us_daily(*, symbol: str) -> pd.DataFrame:
        calls.append(symbol)
        return _daily_rows(
            (("2026-08-12", "5000"), ("2026-08-13", "5050"))
        )

    adapter = AKShareCrossMarketAdapter(
        SimpleNamespace(
            index_global_spot_em=primary,
            index_us_stock_sina=us_daily,
        ),
        universe=(
            _canonical_spec("S_AND_P_500"),
            _canonical_spec("NASDAQ_100"),
            _canonical_spec("DOW_JONES_INDUSTRIAL"),
        ),
        now=lambda: datetime(2026, 8, 13, 21, 0, tzinfo=UTC),
    )

    snapshot = adapter.fetch_cross_market_snapshot()

    assert calls == [".INX", ".NDX", ".DJI"]
    assert snapshot.missing == ()
    assert {quote.provider_code for quote in snapshot.quotes} == {
        ".INX",
        ".NDX",
        ".DJI",
    }
    assert all(
        quote.local_quote_time.isoformat() == "2026-08-13T16:00:00-04:00"
        for quote in snapshot.quotes
    )


def test_unmapped_dxy_crb_and_vix_remain_missing_without_proxy_calls() -> None:
    def primary() -> pd.DataFrame:
        raise ConnectionError("simulated primary failure")

    adapter = AKShareCrossMarketAdapter(
        SimpleNamespace(index_global_spot_em=primary),
        universe=(
            _canonical_spec("US_DOLLAR_INDEX"),
            _canonical_spec("CRB_COMMODITY_INDEX"),
            _canonical_spec("CBOE_VIX"),
        ),
        now=lambda: datetime(2026, 8, 13, 9, 0, tzinfo=UTC),
    )

    snapshot = adapter.fetch_cross_market_snapshot()

    assert snapshot.quotes == ()
    assert {item.instrument_id for item in snapshot.missing} == {
        "US_DOLLAR_INDEX",
        "CRB_COMMODITY_INDEX",
        "CBOE_VIX",
    }
    assert all("no independently audited" in item.reason for item in snapshot.missing)
    assert "no proxy values were fabricated" in " ".join(snapshot.warnings)


def test_each_sina_fallback_timeout_is_bounded_and_becomes_missing() -> None:
    def primary() -> pd.DataFrame:
        raise ConnectionError("simulated primary failure")

    def slow_daily(*, symbol: str) -> pd.DataFrame:
        time.sleep(0.05)
        return _daily_rows((("2026-08-12", "4700"), ("2026-08-13", "4729")))

    adapter = AKShareCrossMarketAdapter(
        SimpleNamespace(
            index_global_spot_em=primary,
            stock_zh_index_daily=slow_daily,
        ),
        universe=(_canonical_spec("CSI_300"),),
        fallback_timeout_seconds=0.001,
        now=lambda: datetime(2026, 8, 13, 9, 0, tzinfo=UTC),
    )
    started = time.monotonic()

    snapshot = adapter.fetch_cross_market_snapshot()

    assert time.monotonic() - started < 0.04
    assert snapshot.quotes == ()
    assert snapshot.missing[0].instrument_id == "CSI_300"
    assert snapshot.missing[0].reason.endswith("timed out")
    assert "1 of 1" in " ".join(snapshot.warnings)


def test_primary_and_sina_fallback_provenance_are_both_retained() -> None:
    fallback_calls: list[str] = []

    def daily(*, symbol: str) -> pd.DataFrame:
        fallback_calls.append(symbol)
        return _daily_rows((("2026-08-12", "4700"), ("2026-08-13", "4729")))

    client = SimpleNamespace(
        index_global_spot_em=lambda: pd.DataFrame(
            [_row(code="SPX", name="标普500")]
        ),
        stock_zh_index_daily=daily,
    )
    adapter = AKShareCrossMarketAdapter(
        client,
        universe=(_canonical_spec("S_AND_P_500"), _canonical_spec("CSI_300")),
        now=lambda: datetime(2026, 8, 13, 9, 0, tzinfo=UTC),
    )

    snapshot = adapter.fetch_cross_market_snapshot()

    assert fallback_calls == ["sh000300"]
    assert len(snapshot.quotes) == 2
    assert " + " in snapshot.provider
    providers = {quote.instrument_id: quote.provider for quote in snapshot.quotes}
    assert "index_global_spot_em" in providers["S_AND_P_500"]
    assert "Sina daily" in providers["CSI_300"]
