import asyncio
import threading
import time
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from gribuki_trade.adapters.akshare_daily import (
    AKShareDailyAssetType,
    AKShareDailyError,
    AKShareDailyNoDataError,
    AKShareDailyPayloadError,
    AKShareDailyTimeoutError,
    AKShareDailyUnsupportedAdjustmentError,
    AKShareHistoricalDailyAdapter,
    HistoricalDailyCoverageError,
    HistoricalDailyFallbackRouter,
    HistoricalDailyOverlapMismatchError,
    HistoricalDailyTailStitchError,
    HistoricalDailyTailStitchPolicy,
)
from gribuki_trade.adapters.baostock import BaoStockDailyAdapter
from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.ports.market_data import (
    AsyncHistoricalDailyData,
    HistoricalDailyData,
    MarketDataTimeoutError,
    MarketDataUnavailableError,
)


def _row(
    trade_date: object = "2026-08-13",
    *,
    code: str | None = None,
    open_price: object = "4.700",
    close: object = "4.729",
    high: object = "4.750",
    low: object = "4.690",
    volume: object = "123456",
    amount: object = "582345678.90",
) -> dict[str, object]:
    row: dict[str, object] = {
        "日期": trade_date,
        "开盘": open_price,
        "收盘": close,
        "最高": high,
        "最低": low,
        "成交量": volume,
        "成交额": amount,
        "振幅": "1.269",
        "涨跌幅": "0.614",
        "涨跌额": "0.029",
        "换手率": "0.137",
    }
    if code is not None:
        row["股票代码"] = code
    return row


class RecordingClient:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows
        self.etf_calls: list[dict[str, object]] = []
        self.stock_calls: list[dict[str, object]] = []

    def fund_etf_hist_em(self, **kwargs: object) -> pd.DataFrame:
        self.etf_calls.append(kwargs)
        return pd.DataFrame(self._rows)

    def stock_zh_a_hist(self, **kwargs: object) -> pd.DataFrame:
        self.stock_calls.append(kwargs)
        return pd.DataFrame(self._rows)


def test_etf_uses_dedicated_unadjusted_endpoint_and_never_inherits_st() -> None:
    row = _row()
    row["是否ST"] = "1"
    client = RecordingClient([row])
    adapter = AKShareHistoricalDailyAdapter(
        client,
        asset_types={"510300.SH": AKShareDailyAssetType.ETF},
    )

    bars = adapter.fetch_daily_bars(
        "510300.SH",
        date(2026, 1, 1),
        date(2026, 8, 13),
    )

    assert len(client.etf_calls) == 1
    assert client.stock_calls == []
    assert client.etf_calls[0] == {
        "symbol": "510300",
        "period": "daily",
        "start_date": "20260101",
        "end_date": "20260813",
        "adjust": "",
    }
    assert bars[0].symbol == "510300.SH"
    assert bars[0].adjustment is PriceAdjustment.NONE
    assert bars[0].is_st is False
    assert bars[0].previous_close == Decimal("4.700")


def test_common_etf_code_family_is_inferred_without_mapping() -> None:
    client = RecordingClient([_row()])

    AKShareHistoricalDailyAdapter(client).fetch_daily_bars(
        "510300", date(2026, 8, 1), date(2026, 8, 13)
    )

    assert len(client.etf_calls) == 1
    assert client.stock_calls == []


def test_stock_uses_stock_endpoint_timeout_and_validates_code() -> None:
    row = _row(code="600000")
    row["是否ST"] = "0"
    client = RecordingClient([row])
    adapter = AKShareHistoricalDailyAdapter(client, timeout_seconds=12.5)

    bar = adapter.fetch_daily_bars(
        "600000.SH", date(2026, 8, 1), date(2026, 8, 13)
    )[0]

    assert client.etf_calls == []
    assert client.stock_calls[0]["adjust"] == ""
    assert client.stock_calls[0]["timeout"] == 12.5
    assert bar.is_st is False
    assert bar.is_trading is True
    assert bar.volume == 123456
    assert bar.amount == Decimal("582345678.90")


@pytest.mark.parametrize(
    ("requested_symbol", "expected_symbol", "provider_code"),
    [
        ("430047.BJ", "430047.BJ", "430047"),
        ("920001", "920001.BJ", "920001"),
    ],
)
def test_bse_stock_routes_to_akshare_daily_stock_endpoint(
    requested_symbol: str,
    expected_symbol: str,
    provider_code: str,
) -> None:
    client = RecordingClient([_row(code=provider_code)])

    bars = AKShareHistoricalDailyAdapter(client).fetch_daily_bars(
        requested_symbol,
        date(2026, 8, 1),
        date(2026, 8, 13),
    )

    assert client.etf_calls == []
    assert client.stock_calls[0]["symbol"] == provider_code
    assert bars[0].symbol == expected_symbol


def test_stock_st_marker_is_preserved_but_missing_marker_is_not_inferred() -> None:
    st_row = _row(code="600000")
    st_row["是否ST"] = "1"
    plain_row = _row("2026-08-12", code="600000")
    client = RecordingClient([st_row, plain_row])

    bars = AKShareHistoricalDailyAdapter(client).fetch_daily_bars(
        "600000.SH", date(2026, 8, 1), date(2026, 8, 13)
    )

    assert bars[0].is_st is False
    assert bars[1].is_st is True


def test_only_original_prices_are_accepted_before_provider_call() -> None:
    client = RecordingClient([_row()])

    with pytest.raises(AKShareDailyUnsupportedAdjustmentError, match="only"):
        AKShareHistoricalDailyAdapter(client).fetch_daily_bars(
            "510300.SH",
            date(2026, 8, 1),
            date(2026, 8, 13),
            adjustment=PriceAdjustment.FORWARD,
        )

    assert client.etf_calls == []


def test_rows_are_sorted_but_duplicate_or_out_of_window_dates_fail() -> None:
    unordered = RecordingClient([_row("2026-08-13"), _row("2026-08-12")])
    bars = AKShareHistoricalDailyAdapter(unordered).fetch_daily_bars(
        "510300.SH", date(2026, 8, 12), date(2026, 8, 13)
    )
    assert [bar.trade_date for bar in bars] == [
        date(2026, 8, 12),
        date(2026, 8, 13),
    ]

    duplicate = RecordingClient([_row(), _row()])
    with pytest.raises(AKShareDailyPayloadError, match="duplicate"):
        AKShareHistoricalDailyAdapter(duplicate).fetch_daily_bars(
            "510300.SH", date(2026, 8, 1), date(2026, 8, 13)
        )

    outside = RecordingClient([_row("2026-07-31")])
    with pytest.raises(AKShareDailyPayloadError, match="outside"):
        AKShareHistoricalDailyAdapter(outside).fetch_daily_bars(
            "510300.SH", date(2026, 8, 1), date(2026, 8, 13)
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("最高", "4.600", "high"),
        ("最低", "4.800", "low"),
        ("开盘", "0", "positive"),
        ("成交量", "1.5", "integer"),
        ("成交量", "-1", "non-negative"),
        ("成交额", "-0.01", "negative"),
        ("收盘", float("nan"), "missing OHLC"),
    ],
)
def test_ohlc_and_turnover_fields_are_strictly_validated(
    field: str, value: object, message: str
) -> None:
    row = _row()
    row[field] = value
    client = RecordingClient([row])

    with pytest.raises(AKShareDailyPayloadError, match=message):
        AKShareHistoricalDailyAdapter(client).fetch_daily_bars(
            "510300.SH", date(2026, 8, 1), date(2026, 8, 13)
        )


def test_explicit_suspension_row_requires_empty_ohlc_and_zero_turnover() -> None:
    row = _row(
        open_price=None,
        close=None,
        high=None,
        low=None,
        volume="0",
        amount="0",
    )
    row["交易状态"] = "停牌"
    row["昨收"] = "4.700"
    client = RecordingClient([row])

    bar = AKShareHistoricalDailyAdapter(client).fetch_daily_bars(
        "510300.SH", date(2026, 8, 1), date(2026, 8, 13)
    )[0]

    assert bar.is_trading is False
    assert bar.open is None and bar.close is None
    assert bar.volume == 0 and bar.amount == 0
    assert bar.previous_close == Decimal("4.700")

    row["成交量"] = "1"
    with pytest.raises(AKShareDailyPayloadError, match="suspended row volume"):
        AKShareHistoricalDailyAdapter(RecordingClient([row])).fetch_daily_bars(
            "510300.SH", date(2026, 8, 1), date(2026, 8, 13)
        )


def test_invalid_status_and_mismatched_stock_code_fail_typed() -> None:
    bad_status = _row()
    bad_status["交易状态"] = "unknown"
    with pytest.raises(AKShareDailyPayloadError, match="trading_status"):
        AKShareHistoricalDailyAdapter(RecordingClient([bad_status])).fetch_daily_bars(
            "510300.SH", date(2026, 8, 1), date(2026, 8, 13)
        )

    with pytest.raises(AKShareDailyPayloadError, match="does not match"):
        AKShareHistoricalDailyAdapter(
            RecordingClient([_row(code="000001")])
        ).fetch_daily_bars(
            "600000.SH", date(2026, 8, 1), date(2026, 8, 13)
        )


def test_empty_or_malformed_frame_raises_typed_errors() -> None:
    with pytest.raises(AKShareDailyNoDataError, match="no rows"):
        AKShareHistoricalDailyAdapter(RecordingClient([])).fetch_daily_bars(
            "510300.SH", date(2026, 8, 1), date(2026, 8, 13)
        )

    with pytest.raises(AKShareDailyPayloadError, match="required columns"):
        AKShareHistoricalDailyAdapter(
            RecordingClient([{"日期": "2026-08-13"}])
        ).fetch_daily_bars(
            "510300.SH", date(2026, 8, 1), date(2026, 8, 13)
        )


def test_provider_failure_and_async_timeout_use_market_data_taxonomy() -> None:
    def failed(**_: object) -> pd.DataFrame:
        raise ConnectionError("upstream unavailable")

    failed_client = SimpleNamespace(
        fund_etf_hist_em=failed,
        stock_zh_a_hist=failed,
    )
    with pytest.raises(AKShareDailyError, match="failed"):
        AKShareHistoricalDailyAdapter(failed_client).fetch_daily_bars(
            "510300.SH", date(2026, 8, 1), date(2026, 8, 13)
        )

    def slow(**_: object) -> pd.DataFrame:
        time.sleep(0.05)
        return pd.DataFrame([_row()])

    slow_client = SimpleNamespace(
        fund_etf_hist_em=slow,
        stock_zh_a_hist=slow,
    )
    adapter = AKShareHistoricalDailyAdapter(slow_client, timeout_seconds=0.001)
    with pytest.raises(AKShareDailyTimeoutError, match="exceeded"):
        asyncio.run(
            adapter.fetch_daily_bars_async(
                "510300.SH", date(2026, 8, 1), date(2026, 8, 13)
            )
        )

    assert issubclass(AKShareDailyError, MarketDataUnavailableError)
    assert issubclass(AKShareDailyTimeoutError, MarketDataTimeoutError)
    assert isinstance(adapter, HistoricalDailyData)
    assert isinstance(adapter, AsyncHistoricalDailyData)


def _sina_row(
    trade_date: object,
    *,
    close: object = "4.729",
    open_price: object = "4.700",
    high: object = "4.750",
    low: object = "4.690",
) -> dict[str, object]:
    return {
        "date": trade_date,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": "123456",
        "amount": "582345678.90",
    }


class SinaFallbackClient:
    def __init__(
        self,
        sina_rows: list[dict[str, object]],
        *,
        eastmoney_rows: list[dict[str, object]] | None = None,
        eastmoney_delay: float = 0,
    ) -> None:
        self.sina_rows = sina_rows
        self.eastmoney_rows = eastmoney_rows
        self.eastmoney_delay = eastmoney_delay
        self.eastmoney_calls: list[dict[str, object]] = []
        self.sina_calls: list[dict[str, object]] = []

    def fund_etf_hist_em(self, **kwargs: object) -> pd.DataFrame:
        self.eastmoney_calls.append(kwargs)
        if self.eastmoney_delay:
            time.sleep(self.eastmoney_delay)
        if self.eastmoney_rows is None:
            raise ConnectionError("Eastmoney unavailable")
        return pd.DataFrame(self.eastmoney_rows)

    def fund_etf_hist_sina(self, **kwargs: object) -> pd.DataFrame:
        self.sina_calls.append(kwargs)
        return pd.DataFrame(self.sina_rows)

    def stock_zh_a_hist(self, **_: object) -> pd.DataFrame:
        raise AssertionError("stock endpoint must not be used for ETF")


class StockSinaFallbackClient:
    def __init__(
        self,
        sina_rows: list[dict[str, object]],
        *,
        eastmoney_rows: list[dict[str, object]] | None = None,
        sina_delay: float = 0,
    ) -> None:
        self.sina_rows = sina_rows
        self.eastmoney_rows = eastmoney_rows
        self.sina_delay = sina_delay
        self.eastmoney_calls: list[dict[str, object]] = []
        self.sina_calls: list[dict[str, object]] = []
        self.active_sina_calls = 0
        self.maximum_active_sina_calls = 0
        self.counter_lock = threading.Lock()

    def stock_zh_a_hist(self, **kwargs: object) -> pd.DataFrame:
        self.eastmoney_calls.append(kwargs)
        if self.eastmoney_rows is None:
            raise ConnectionError("Eastmoney unavailable")
        return pd.DataFrame(self.eastmoney_rows)

    def stock_zh_a_daily(self, **kwargs: object) -> pd.DataFrame:
        self.sina_calls.append(kwargs)
        with self.counter_lock:
            self.active_sina_calls += 1
            self.maximum_active_sina_calls = max(
                self.maximum_active_sina_calls,
                self.active_sina_calls,
            )
        try:
            if self.sina_delay:
                time.sleep(self.sina_delay)
            # 实际的 AKShare/Sina 股票数据帧同时含有这两个字段；``amount`` 是
            # 人民币成交额，``turnover`` 是无量纲比率，绝不能被选作成交额别名。
            rows = [dict(item, turnover="0.031") for item in self.sina_rows]
            return pd.DataFrame(rows)
        finally:
            with self.counter_lock:
                self.active_sina_calls -= 1

    def fund_etf_hist_em(self, **_: object) -> pd.DataFrame:
        raise AssertionError("ETF endpoint must not be used for stock")


def test_stock_falls_back_to_unadjusted_sina_with_auditable_diagnostics() -> None:
    client = StockSinaFallbackClient(
        [
            _sina_row("2026-08-12", close="4.700"),
            _sina_row("2026-08-13", close="4.729"),
            _sina_row("2026-08-14", close="4.740"),
        ]
    )

    result = AKShareHistoricalDailyAdapter(client).fetch_daily_bars_with_source(
        "600000.SH", date(2026, 8, 12), date(2026, 8, 14)
    )

    assert result.selected_source == "AKShare/Sina stock_zh_a_daily adjust=NONE"
    assert result.source_failures == ("stock_zh_a_hist:AKShareDailyError",)
    assert result.warnings == (
        "SINA_FALLBACK_IS_UNADJUSTED_DAILY_HISTORY",
        "SINA_PREVIOUS_CLOSE_DERIVED_FROM_ADJACENT_RAW_CLOSES",
        "SINA_CORPORATE_ACTION_GUARD_HAS_NO_INDEPENDENT_REFERENCE_CLOSE",
    )
    assert client.sina_calls == [
        {
            "symbol": "sh600000",
            "start_date": "20260812",
            "end_date": "20260814",
            "adjust": "",
        }
    ]
    assert [bar.previous_close for bar in result.bars] == [
        None,
        Decimal("4.700"),
        Decimal("4.729"),
    ]
    assert all(bar.amount == Decimal("582345678.90") for bar in result.bars)
    assert all(bar.adjustment is PriceAdjustment.NONE for bar in result.bars)


def test_stock_sources_must_contain_the_exact_requested_end() -> None:
    client = StockSinaFallbackClient(
        [_sina_row("2026-08-13")],
        eastmoney_rows=[_row("2026-08-13", code="600000")],
    )

    with pytest.raises(AKShareDailyError) as error:
        AKShareHistoricalDailyAdapter(client).fetch_daily_bars(
            "600000.SH", date(2026, 8, 13), date(2026, 8, 14)
        )

    assert "stock_zh_a_hist:AKShareDailyNoDataError" in str(error.value)
    assert isinstance(error.value.__cause__, AKShareDailyNoDataError)
    assert "exact requested end 2026-08-14" in str(error.value.__cause__)
    assert len(client.sina_calls) == 1


def test_stock_sina_fallback_uses_the_process_wide_v8_lock() -> None:
    client = StockSinaFallbackClient(
        [_sina_row("2026-08-14")],
        sina_delay=0.03,
    )
    first = AKShareHistoricalDailyAdapter(client)
    second = AKShareHistoricalDailyAdapter(client)

    async def fetch_both() -> None:
        await asyncio.gather(
            first.fetch_daily_bars_async(
                "600000.SH", date(2026, 8, 14), date(2026, 8, 14)
            ),
            second.fetch_daily_bars_async(
                "000001.SZ", date(2026, 8, 14), date(2026, 8, 14)
            ),
        )

    asyncio.run(fetch_both())

    assert client.maximum_active_sina_calls == 1
    assert {call["symbol"] for call in client.sina_calls} == {
        "sh600000",
        "sz000001",
    }


def test_etf_falls_back_to_sina_filters_window_and_derives_visible_preclose() -> None:
    client = SinaFallbackClient(
        [
            # 这条无效记录位于请求窗口之外，既不应导致校验失败，也不能渗入
            # 第一条可见记录的昨收价。
            _sina_row("2026-08-11", high="0"),
            _sina_row("2026-08-12", close="4.700"),
            _sina_row("2026-08-13", close="4.729"),
            _sina_row("2026-08-14", close="4.800"),
        ]
    )
    adapter = AKShareHistoricalDailyAdapter(
        client,
        asset_types={"510300.SH": AKShareDailyAssetType.ETF},
    )

    result = adapter.fetch_daily_bars_with_source(
        "510300.SH", date(2026, 8, 12), date(2026, 8, 13)
    )

    assert result.selected_source == "AKShare/Sina fund_etf_hist_sina"
    assert result.source_failures == ("fund_etf_hist_em:AKShareDailyError",)
    assert client.sina_calls == [{"symbol": "sh510300"}]
    assert [bar.trade_date for bar in result.bars] == [
        date(2026, 8, 12),
        date(2026, 8, 13),
    ]
    assert result.bars[0].previous_close is None
    assert result.bars[1].previous_close == Decimal("4.700")
    assert all(bar.turnover_percent is None for bar in result.bars)
    assert all(bar.is_trading and not bar.is_st for bar in result.bars)
    assert all(bar.adjustment is PriceAdjustment.NONE for bar in result.bars)


def test_sina_uses_sz_prefix_for_shenzhen_etf() -> None:
    client = SinaFallbackClient([_sina_row("2026-08-13")])

    result = AKShareHistoricalDailyAdapter(client).fetch_daily_bars_with_source(
        "159919.SZ", date(2026, 8, 13), date(2026, 8, 13)
    )

    assert result.selected_source.startswith("AKShare/Sina")
    assert client.sina_calls == [{"symbol": "sz159919"}]
    assert result.bars[0].symbol == "159919.SZ"


def test_eastmoney_timeout_has_an_independent_sina_timeout_budget() -> None:
    client = SinaFallbackClient(
        [_sina_row("2026-08-12"), _sina_row("2026-08-13")],
        eastmoney_rows=[_row()],
        eastmoney_delay=0.15,
    )
    adapter = AKShareHistoricalDailyAdapter(client, timeout_seconds=0.05)

    result = asyncio.run(
        adapter.fetch_daily_bars_async_with_source(
            "510300.SH", date(2026, 8, 12), date(2026, 8, 13)
        )
    )

    assert result.selected_source == "AKShare/Sina fund_etf_hist_sina"
    assert result.source_failures == (
        "fund_etf_hist_em:AKShareDailyTimeoutError",
    )
    assert len(client.eastmoney_calls) == 1
    assert len(client.sina_calls) == 1


def test_minimum_source_bars_can_trigger_sina_after_short_eastmoney_result() -> None:
    client = SinaFallbackClient(
        [_sina_row("2026-08-12"), _sina_row("2026-08-13")],
        eastmoney_rows=[_row("2026-08-13")],
    )
    adapter = AKShareHistoricalDailyAdapter(client, minimum_source_bars=2)

    result = adapter.fetch_daily_bars_with_source(
        "510300.SH", date(2026, 8, 12), date(2026, 8, 13)
    )

    assert result.selected_source == "AKShare/Sina fund_etf_hist_sina"
    assert result.source_failures == (
        "fund_etf_hist_em:AKShareDailyNoDataError",
    )


def test_invalid_symbol_range_and_asset_mapping_fail_before_provider_call() -> None:
    client = RecordingClient([_row()])
    adapter = AKShareHistoricalDailyAdapter(client)

    with pytest.raises(ValueError, match="symbol"):
        adapter.fetch_daily_bars("bad", date(2026, 8, 1), date(2026, 8, 13))
    with pytest.raises(ValueError, match="start"):
        adapter.fetch_daily_bars(
            "510300.SH", date(2026, 8, 14), date(2026, 8, 13)
        )
    with pytest.raises(ValueError, match="asset type"):
        AKShareHistoricalDailyAdapter(client, asset_types={"510300.SH": "bond"})
    assert client.etf_calls == []


class FakeHistoryProvider:
    def __init__(
        self,
        bars: tuple[DailyBar, ...] = (),
        *,
        failure: MarketDataUnavailableError | None = None,
    ) -> None:
        self.bars = bars
        self.failure = failure
        self.sync_calls = 0
        self.async_calls = 0

    def fetch_daily_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> tuple[DailyBar, ...]:
        self.sync_calls += 1
        if self.failure is not None:
            raise self.failure
        return self.bars

    async def fetch_daily_bars_async(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> tuple[DailyBar, ...]:
        self.async_calls += 1
        if self.failure is not None:
            raise self.failure
        return self.bars


class RejectingBaoStockClient:
    def __init__(self) -> None:
        self.query_symbols: list[str] = []
        self.logged_out = False

    def login(self) -> SimpleNamespace:
        return SimpleNamespace(error_code="0", error_msg="")

    def logout(self) -> None:
        self.logged_out = True

    def query_history_k_data_plus(
        self,
        symbol: str,
        *_: object,
        **__: object,
    ) -> SimpleNamespace:
        self.query_symbols.append(symbol)
        return SimpleNamespace(error_code="100", error_msg="exchange unsupported")


def _bars(count: int) -> tuple[DailyBar, ...]:
    first = date(2025, 1, 1)
    return tuple(
        DailyBar(
            symbol="510300.SH",
            trade_date=first + timedelta(days=index),
            open=Decimal("4"),
            high=Decimal("4.2"),
            low=Decimal("3.9"),
            close=Decimal("4.1"),
            previous_close=Decimal("4"),
            volume=100,
            amount=Decimal("410"),
            turnover_percent=Decimal("0.1"),
            is_trading=True,
            is_st=False,
            adjustment=PriceAdjustment.NONE,
        )
        for index in range(count)
    )


def test_router_uses_akshare_as_complete_replacement_for_148_bar_primary() -> None:
    primary = FakeHistoryProvider(_bars(148))
    fallback = FakeHistoryProvider(_bars(240))
    router = HistoricalDailyFallbackRouter(
        primary,
        fallback,
        minimum_bars=200,
        primary_name="BaoStock",
        fallback_name="AKShare",
    )

    result = router.fetch_daily_bars_with_route(
        "510300.SH", date(2025, 1, 1), date(2026, 8, 13)
    )

    assert len(result.bars) == 240
    assert result.selected_source == "AKShare"
    assert result.primary_count == 148
    assert result.fallback_count == 240
    assert primary.sync_calls == 1 and fallback.sync_calls == 1


def test_router_does_not_call_fallback_when_primary_is_complete() -> None:
    primary = FakeHistoryProvider(_bars(200))
    fallback = FakeHistoryProvider(_bars(240))
    router = HistoricalDailyFallbackRouter(primary, fallback, minimum_bars=200)

    result = router.fetch_daily_bars_with_route(
        "510300.SH", date(2025, 1, 1), date(2026, 8, 13)
    )

    assert result.selected_source == "primary"
    assert result.fallback_count is None
    assert fallback.sync_calls == 0


def test_nested_router_preserves_inner_selected_source_diagnostics() -> None:
    inner = HistoricalDailyFallbackRouter(
        FakeHistoryProvider(failure=AKShareDailyError("primary unavailable")),
        FakeHistoryProvider(_bars(220)),
        minimum_bars=200,
        primary_name="BaoStock",
        fallback_name="AKShare",
    )
    outer_fallback = FakeHistoryProvider(_bars(240))
    outer = HistoricalDailyFallbackRouter(
        inner,
        outer_fallback,
        minimum_bars=200,
        primary_name="Network",
        fallback_name="Archive",
    )

    result = asyncio.run(
        outer.fetch_daily_bars_async_with_route(
            "510300.SH", date(2025, 1, 1), date(2026, 8, 13)
        )
    )

    assert result.selected_source == "AKShare"
    assert result.selected_source_failures == ()
    assert outer_fallback.async_calls == 0


def test_router_falls_back_on_typed_primary_failure_and_supports_async() -> None:
    primary = FakeHistoryProvider(failure=AKShareDailyError("unavailable"))
    fallback = FakeHistoryProvider(_bars(220))
    router = HistoricalDailyFallbackRouter(
        primary,
        fallback,
        minimum_bars=200,
        primary_name="BaoStock",
        fallback_name="AKShare",
    )

    result = asyncio.run(
        router.fetch_daily_bars_async_with_route(
            "510300.SH", date(2025, 1, 1), date(2026, 8, 13)
        )
    )

    assert result.selected_source == "AKShare"
    assert result.primary_count is None
    assert result.primary_failure == "AKShareDailyError"
    assert len(result.bars) == 220
    assert primary.async_calls == 1 and fallback.async_calls == 1
    assert isinstance(router, HistoricalDailyData)
    assert isinstance(router, AsyncHistoricalDailyData)


def test_router_falls_back_to_akshare_when_baostock_rejects_bse() -> None:
    baostock_client = RejectingBaoStockClient()
    akshare_client = RecordingClient([_row(code="430047")])
    router = HistoricalDailyFallbackRouter(
        BaoStockDailyAdapter(baostock_client, max_attempts=1),
        AKShareHistoricalDailyAdapter(akshare_client),
        minimum_bars=1,
        primary_name="BaoStock",
        fallback_name="AKShare",
    )

    result = asyncio.run(
        router.fetch_daily_bars_async_with_route(
            "430047.BJ", date(2026, 8, 13), date(2026, 8, 13)
        )
    )

    assert baostock_client.query_symbols == ["bj.430047"]
    assert baostock_client.logged_out is True
    assert result.primary_failure == "BaoStockError"
    assert result.selected_source == "AKShare/Eastmoney stock_zh_a_hist"
    assert result.bars[0].symbol == "430047.BJ"
    assert akshare_client.stock_calls[0]["symbol"] == "430047"


def test_router_fails_closed_when_both_sources_have_insufficient_history() -> None:
    router = HistoricalDailyFallbackRouter(
        FakeHistoryProvider(_bars(148)),
        FakeHistoryProvider(_bars(180)),
        minimum_bars=200,
        primary_name="BaoStock",
        fallback_name="AKShare",
    )

    with pytest.raises(HistoricalDailyCoverageError, match="required=200"):
        router.fetch_daily_bars(
            "510300.SH", date(2025, 1, 1), date(2026, 8, 13)
        )


def test_router_reports_selected_sina_subsource_and_eastmoney_failure() -> None:
    requested_end = date(2026, 8, 13)
    first = requested_end - timedelta(days=219)
    sina_rows = [
        _sina_row(
            first + timedelta(days=index),
            close=str(Decimal("4") + Decimal(index) / Decimal("1000")),
            open_price="4",
            high="5",
            low="3.9",
        )
        for index in range(220)
    ]
    akshare = AKShareHistoricalDailyAdapter(SinaFallbackClient(sina_rows))
    router = HistoricalDailyFallbackRouter(
        FakeHistoryProvider(_bars(148)),
        akshare,
        minimum_bars=200,
        primary_name="BaoStock",
        fallback_name="AKShare",
    )

    result = asyncio.run(
        router.fetch_daily_bars_async_with_route(
            "510300.SH", first, requested_end
        )
    )

    assert result.selected_source == "AKShare/Sina fund_etf_hist_sina"
    assert result.fallback_count == 220
    assert result.selected_source_failures == (
        "fund_etf_hist_em:AKShareDailyError",
    )
    assert result.warnings == (
        "SINA_FALLBACK_IS_UNADJUSTED_DAILY_HISTORY",
        "SINA_PREVIOUS_CLOSE_DERIVED_FROM_ADJACENT_RAW_CLOSES",
        "SINA_CORPORATE_ACTION_GUARD_HAS_NO_INDEPENDENT_REFERENCE_CLOSE",
    )


def _stitch_bar(
    trade_date: date,
    *,
    price: Decimal,
    volume: int = 100,
    amount: Decimal | None = None,
) -> DailyBar:
    return DailyBar(
        symbol="510300.SH",
        trade_date=trade_date,
        open=price,
        high=price + Decimal("0.030"),
        low=price - Decimal("0.020"),
        close=price + Decimal("0.010"),
        previous_close=None,
        volume=volume,
        amount=amount if amount is not None else price * Decimal(volume),
        turnover_percent=None,
        is_trading=True,
        is_st=False,
        adjustment=PriceAdjustment.NONE,
    )


def _tail_stitch_sources(
    *,
    overlap_sessions: int = 25,
    tail_sessions: int = 1,
) -> tuple[tuple[DailyBar, ...], tuple[DailyBar, ...], date]:
    start = date(2025, 1, 1)
    base = tuple(
        _stitch_bar(
            start + timedelta(days=index),
            price=Decimal("4") + Decimal(index) / Decimal("1000"),
        )
        for index in range(240)
    )
    overlap = tuple(
        _stitch_bar(
            bar.trade_date,
            price=bar.open or Decimal("0"),
            volume=bar.volume * 100,
            amount=bar.amount * Decimal("100"),
        )
        for bar in base[-overlap_sessions:]
    )
    fresh_tail = tuple(
        _stitch_bar(
            base[-1].trade_date + timedelta(days=index),
            price=Decimal("5") + Decimal(index) / Decimal("1000"),
            volume=999_999,
        )
        for index in range(1, tail_sessions + 1)
    )
    return base, (*overlap, *fresh_tail), fresh_tail[-1].trade_date


def test_controlled_tail_stitch_matches_real_510300_shape() -> None:
    base, tail, required_latest = _tail_stitch_sources()
    router = HistoricalDailyFallbackRouter(
        FakeHistoryProvider(tail),
        FakeHistoryProvider(base),
        minimum_bars=200,
        primary_name="BaoStock",
        fallback_name="AKShare/Sina",
        tail_stitch_policy=HistoricalDailyTailStitchPolicy(
            minimum_overlap_sessions=20
        ),
    )

    result = router.fetch_daily_bars_with_route(
        "510300.SH", base[0].trade_date, required_latest
    )

    assert result.selected_source == (
        "MIXED/TAIL_STITCH base=AKShare/Sina tail=BaoStock"
    )
    assert len(result.bars) == 241
    assert result.bars[-2] is base[-1]
    assert result.bars[-1] is tail[-1]
    assert len({bar.trade_date for bar in result.bars}) == len(result.bars)
    diagnostics = result.tail_stitch
    assert diagnostics is not None
    assert diagnostics.base_source == "AKShare/Sina"
    assert diagnostics.tail_source == "BaoStock"
    assert diagnostics.required_latest_session == required_latest
    assert diagnostics.overlap_sessions_validated == 20
    assert diagnostics.stitched_tail_sessions == 1
    assert diagnostics.volume_mismatch_sessions == 20
    assert diagnostics.amount_mismatch_sessions == 20


def test_tail_stitch_never_overwrites_overlap_from_long_base() -> None:
    base, tail, required_latest = _tail_stitch_sources()
    router = HistoricalDailyFallbackRouter(
        FakeHistoryProvider(tail),
        FakeHistoryProvider(base),
        minimum_bars=200,
        tail_stitch_policy=HistoricalDailyTailStitchPolicy(),
    )

    result = router.fetch_daily_bars_with_route(
        "510300.SH", base[0].trade_date, required_latest
    )

    base_last_in_result = result.bars[-2]
    assert base_last_in_result.volume == 100
    assert base_last_in_result.amount == base[-1].amount
    assert tail[-2].volume == 10_000


def test_tail_stitch_blocks_any_recent_ohlc_mismatch() -> None:
    base, tail, required_latest = _tail_stitch_sources()
    changed = replace(tail[-2], close=(tail[-2].close or Decimal(0)) + Decimal("0.001"))
    bad_tail = (*tail[:-2], changed, tail[-1])
    router = HistoricalDailyFallbackRouter(
        FakeHistoryProvider(bad_tail),
        FakeHistoryProvider(base),
        minimum_bars=200,
        tail_stitch_policy=HistoricalDailyTailStitchPolicy(),
    )

    with pytest.raises(HistoricalDailyOverlapMismatchError, match="OHLC mismatch"):
        router.fetch_daily_bars(
            "510300.SH", base[0].trade_date, required_latest
        )


def test_tail_stitch_blocks_missing_or_extra_recent_overlap_date() -> None:
    base, tail, required_latest = _tail_stitch_sources()
    bad_tail = (*tail[:5], *tail[6:])
    router = HistoricalDailyFallbackRouter(
        FakeHistoryProvider(bad_tail),
        FakeHistoryProvider(base),
        minimum_bars=200,
        tail_stitch_policy=HistoricalDailyTailStitchPolicy(),
    )

    with pytest.raises(HistoricalDailyOverlapMismatchError, match="dates differ"):
        router.fetch_daily_bars(
            "510300.SH", base[0].trade_date, required_latest
        )


def test_tail_stitch_requires_twenty_common_sessions() -> None:
    base, tail, required_latest = _tail_stitch_sources(overlap_sessions=19)
    router = HistoricalDailyFallbackRouter(
        FakeHistoryProvider(tail),
        FakeHistoryProvider(base),
        minimum_bars=200,
        tail_stitch_policy=HistoricalDailyTailStitchPolicy(),
    )

    with pytest.raises(HistoricalDailyOverlapMismatchError, match="dates differ"):
        router.fetch_daily_bars(
            "510300.SH", base[0].trade_date, required_latest
        )


def test_tail_stitch_requires_primary_exact_latest_session() -> None:
    base, tail, required_latest = _tail_stitch_sources()
    router = HistoricalDailyFallbackRouter(
        FakeHistoryProvider(tail[:-1]),
        FakeHistoryProvider(base),
        minimum_bars=200,
        tail_stitch_policy=HistoricalDailyTailStitchPolicy(),
    )

    with pytest.raises(HistoricalDailyTailStitchError, match="required"):
        router.fetch_daily_bars(
            "510300.SH", base[0].trade_date, required_latest
        )


def test_tail_stitch_policy_rejects_weaker_overlap() -> None:
    with pytest.raises(ValueError, match="at least 20"):
        HistoricalDailyTailStitchPolicy(minimum_overlap_sessions=19)
