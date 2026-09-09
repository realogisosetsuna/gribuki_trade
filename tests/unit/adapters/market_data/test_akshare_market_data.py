import asyncio
import time
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
import pytest

from gribuki_trade.adapters.market_data.akshare import (
    AKShareMarketDataAdapter,
    AKSharePayloadError,
    AKShareTimeoutError,
)
from gribuki_trade.ports.market_data import (
    FreshnessStatus,
    MinuteInterval,
    SourceSemantics,
    TradeDirection,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _single_spot_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "item": ["最新", "今开", "最高", "最低", "昨收", "总手", "金额", "换手"],
            "value": [10.21, 10.02, 10.30, 9.98, 10.00, 123456, 125000000.5, 0.42],
        }
    )


def test_spot_snapshot_preserves_public_web_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    client = SimpleNamespace(stock_bid_ask_em=lambda **_: _single_spot_frame())
    monkeypatch.setattr(client, "stock_bid_ask_em", lambda **_: _single_spot_frame())
    now = datetime(2026, 8, 13, 10, 0, tzinfo=SHANGHAI)
    adapter = AKShareMarketDataAdapter(client, now=lambda: now)

    snapshot = adapter.fetch_spot_snapshot("000001.SZ")

    assert snapshot.symbol == "000001.SZ"
    assert snapshot.name is None
    assert snapshot.last == Decimal("10.21")
    assert snapshot.volume_lots == 123456
    assert snapshot.meta.semantics is SourceSemantics.PUBLIC_WEB_QUOTE_SNAPSHOT
    assert snapshot.meta.provider_timestamp is None
    assert snapshot.meta.freshness is FreshnessStatus.UNKNOWN
    assert "stock_bid_ask_em" in snapshot.meta.provider
    assert "not an exchange tick" in " ".join(snapshot.meta.warnings)


def test_spot_refresh_can_degrade_to_bounded_marked_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = SimpleNamespace(stock_bid_ask_em=lambda **_: _single_spot_frame())
    moments = [
        datetime(2026, 8, 13, 10, 0, 0, tzinfo=SHANGHAI),
        datetime(2026, 8, 13, 10, 0, 5, tzinfo=SHANGHAI),
    ]
    calls = 0

    def provider(**_: object) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        if calls == 1:
            return _single_spot_frame()
        raise ConnectionError("upstream unavailable")

    monkeypatch.setattr(client, "stock_bid_ask_em", provider)
    adapter = AKShareMarketDataAdapter(
        client,
        max_attempts=1,
        snapshot_cache_ttl_seconds=2,
        snapshot_stale_fallback_seconds=10,
        now=lambda: moments.pop(0),
    )

    first = adapter.fetch_spot_snapshot("000001")
    fallback = adapter.fetch_spot_snapshot("000001")

    assert first.meta.degraded is False
    assert fallback.meta.degraded is True
    assert fallback.meta.freshness is FreshnessStatus.STALE
    assert fallback.meta.fetched_at == first.meta.fetched_at
    assert "all refresh paths failed" in " ".join(fallback.meta.warnings)


def test_spot_falls_back_to_tencent_table_with_explicit_degradation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = SimpleNamespace(
        stock_bid_ask_em=lambda **_: _single_spot_frame(),
        stock_zh_a_spot_tx=lambda: pd.DataFrame(),
    )

    def failed_primary(**_: object) -> pd.DataFrame:
        raise ConnectionError("eastmoney disconnected")

    tencent = pd.DataFrame(
        [
            {
                "code": "bj920045",
                "name": "北交所样例",
                "zxj": "20",
                "zd": "1",
                "volume": "10",
                "turnover": "2",
                "hsl": "0.1",
            },
            {
                "code": "sz000001",
                "name": "平安银行",
                "zxj": "11.25",
                "zd": "-0.01",
                "volume": "632950.00",
                "turnover": "71161",
                "hsl": "0.33",
            }
        ]
    )
    monkeypatch.setattr(client, "stock_bid_ask_em", failed_primary)
    monkeypatch.setattr(client, "stock_zh_a_spot_tx", lambda: tencent)
    adapter = AKShareMarketDataAdapter(client, max_attempts=1)

    snapshot = adapter.fetch_spot_snapshot("000001.SZ")

    assert snapshot.name == "平安银行"
    assert snapshot.last == Decimal("11.25")
    assert snapshot.previous_close == Decimal("11.26")
    assert snapshot.volume_lots == 632950
    assert snapshot.amount == Decimal("711610000")
    assert snapshot.open is None
    assert snapshot.meta.degraded is True
    assert "Tencent" in snapshot.meta.provider
    assert "full-market" in " ".join(snapshot.meta.warnings)


def test_time_and_sales_is_not_exposed_as_exchange_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame(
        [
            {"时间": "10:00:00", "成交价": 10.2, "手数": 30, "买卖盘性质": "买盘"},
            {"时间": "09:59:59", "成交价": 10.19, "手数": 5, "买卖盘性质": "卖盘"},
        ]
    )
    client = SimpleNamespace(stock_intraday_em=lambda **_: frame)
    monkeypatch.setattr(client, "stock_intraday_em", lambda **_: frame)
    now = datetime(2026, 8, 13, 10, 0, 20, tzinfo=SHANGHAI)
    adapter = AKShareMarketDataAdapter(client, now=lambda: now)

    prints = adapter.fetch_trade_prints("000001.SZ")

    assert [item.occurred_at.strftime("%H:%M:%S") for item in prints] == [
        "09:59:59",
        "10:00:00",
    ]
    assert prints[1].direction is TradeDirection.BUY
    assert prints[1].volume_lots == 30
    assert prints[1].exchange_sequence is None
    assert prints[1].meta.semantics is SourceSemantics.PUBLIC_WEB_TIME_AND_SALES
    assert prints[1].meta.freshness is FreshnessStatus.CURRENT
    assert "no exchange sequence" in " ".join(prints[1].meta.warnings)


def test_minute_bars_drop_unfinished_bar_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame(
        [
            {
                "时间": "2026-08-13 09:30:00",
                "开盘": 10.00,
                "收盘": 10.05,
                "最高": 10.08,
                "最低": 9.99,
                "成交量": 1000,
                "成交额": 1002500,
                "均价": 10.025,
            },
            {
                "时间": "2026-08-13 09:32:00",
                "开盘": 10.05,
                "收盘": 10.07,
                "最高": 10.09,
                "最低": 10.04,
                "成交量": 800,
                "成交额": 804800,
                "均价": 10.06,
            },
        ]
    )
    captured: dict[str, object] = {}
    client = SimpleNamespace(stock_zh_a_hist_min_em=lambda **_: frame)

    def provider(**kwargs: object) -> pd.DataFrame:
        captured.update(kwargs)
        return frame

    monkeypatch.setattr(client, "stock_zh_a_hist_min_em", provider)
    now = datetime(2026, 8, 13, 9, 32, 30, tzinfo=SHANGHAI)
    adapter = AKShareMarketDataAdapter(client, now=lambda: now)

    bars = adapter.fetch_intraday_bars(
        "000001.SZ",
        datetime(2026, 8, 13, 9, 30, tzinfo=SHANGHAI),
        datetime(2026, 8, 13, 9, 33, tzinfo=SHANGHAI),
    )

    assert len(bars) == 1
    assert bars[0].start_at.hour == 9 and bars[0].start_at.minute == 30
    assert bars[0].end_at.minute == 31
    assert bars[0].is_closed is True
    assert bars[0].volume_lots == 1000
    assert bars[0].meta.semantics is SourceSemantics.AGGREGATED_MINUTE_BAR
    assert captured["period"] == "1"
    assert captured["adjust"] == ""


def test_five_minute_request_maps_interval_and_marks_historical_staleness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame(
        [
            {
                "时间": "2026-08-12 14:55:00",
                "开盘": 10,
                "收盘": 10.1,
                "最高": 10.2,
                "最低": 9.9,
                "成交量": 100,
                "成交额": 10050,
            }
        ]
    )
    captured: dict[str, object] = {}
    client = SimpleNamespace(stock_zh_a_hist_min_em=lambda **_: frame)

    def provider(**kwargs: object) -> pd.DataFrame:
        captured.update(kwargs)
        return frame

    monkeypatch.setattr(client, "stock_zh_a_hist_min_em", provider)
    now = datetime(2026, 8, 13, 10, 0, tzinfo=SHANGHAI)
    adapter = AKShareMarketDataAdapter(client, now=lambda: now)

    bars = adapter.fetch_intraday_bars(
        "000001",
        datetime(2026, 8, 12, 14, 55, tzinfo=SHANGHAI),
        datetime(2026, 8, 12, 15, 0, tzinfo=SHANGHAI),
        interval=MinuteInterval.FIVE_MINUTES,
    )

    assert captured["period"] == "5"
    assert bars[0].interval is MinuteInterval.FIVE_MINUTES
    assert bars[0].meta.freshness is FreshnessStatus.STALE


def test_shanghai_etf_uses_fund_endpoint_with_correct_market_routing() -> None:
    frame = pd.DataFrame(
        [
            {
                "时间": "2026-08-13 09:30:00",
                "开盘": 4.10,
                "收盘": 4.11,
                "最高": 4.12,
                "最低": 4.09,
                "成交量": 1_000,
                "成交额": 411_000,
                "均价": 4.11,
            }
        ]
    )
    captured: dict[str, object] = {}

    def fund_provider(**kwargs: object) -> pd.DataFrame:
        captured.update(kwargs)
        return frame

    client = SimpleNamespace(fund_etf_hist_min_em=fund_provider)
    adapter = AKShareMarketDataAdapter(
        client,
        now=lambda: datetime(2026, 8, 13, 10, 0, tzinfo=SHANGHAI),
    )

    bars = adapter.fetch_intraday_bars(
        "510300.SH",
        datetime(2026, 8, 13, 9, 30, tzinfo=SHANGHAI),
        datetime(2026, 8, 13, 9, 31, tzinfo=SHANGHAI),
    )

    assert len(bars) == 1
    assert captured["symbol"] == "510300"
    assert "fund_etf_hist_min_em" in bars[0].meta.provider


def test_real_client_etf_http_route_uses_shanghai_secid() -> None:
    observed_secid: str | None = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal observed_secid
        observed_secid = request.url.params.get("secid")
        return httpx.Response(
            200,
            json={
                "data": {
                    "klines": [
                        "2026-08-13 09:30,4.10,4.11,4.12,4.09,1000,411000"
                    ]
                }
            },
        )

    adapter = AKShareMarketDataAdapter(
        http_transport=httpx.MockTransport(handler),
        now=lambda: datetime(2026, 8, 13, 10, 0, tzinfo=SHANGHAI),
    )
    bars = asyncio.run(
        adapter.fetch_intraday_bars_async(
            "510300.SH",
            datetime(2026, 8, 13, 9, 30, tzinfo=SHANGHAI),
            datetime(2026, 8, 13, 9, 35, tzinfo=SHANGHAI),
            interval=MinuteInterval.FIVE_MINUTES,
        )
    )

    assert observed_secid == "1.510300"
    assert len(bars) == 1
    assert bars[0].close == Decimal("4.11")
    assert bars[0].vwap is None


def test_real_client_primary_http_failure_uses_bounded_sina_fallback() -> None:
    requested_hosts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_hosts.append(request.url.host)
        if request.url.host == "push2his.eastmoney.com":
            return httpx.Response(503, text="temporarily unavailable")
        return httpx.Response(
            200,
            text=(
                '=([{"day":"2026-08-13 09:35:00","open":"4.10",'
                '"high":"4.12","low":"4.09","close":"4.11",'
                '"volume":"100000","amount":"411000"}]);'
            ),
        )

    adapter = AKShareMarketDataAdapter(
        http_transport=httpx.MockTransport(handler),
        now=lambda: datetime(2026, 8, 13, 10, 0, tzinfo=SHANGHAI),
    )
    bars = asyncio.run(
        adapter.fetch_intraday_bars_async(
            "510300.SH",
            datetime(2026, 8, 13, 9, 30, tzinfo=SHANGHAI),
            datetime(2026, 8, 13, 9, 35, tzinfo=SHANGHAI),
            interval=MinuteInterval.FIVE_MINUTES,
        )
    )

    assert requested_hosts == ["push2his.eastmoney.com", "quotes.sina.cn"]
    assert len(bars) == 1
    assert bars[0].meta.degraded is True
    assert "Sina" in bars[0].meta.provider


def test_async_minute_primary_timeout_still_attempts_sina_fallback() -> None:
    sina = pd.DataFrame(
        [
            {
                "day": "2026-08-13 09:35:00",
                "open": "4.10",
                "high": "4.12",
                "low": "4.09",
                "close": "4.11",
                "volume": "100000",
                "amount": "411000",
            }
        ]
    )
    fallback_calls = 0

    def slow_primary(**_: object) -> pd.DataFrame:
        time.sleep(0.60)
        return pd.DataFrame()

    def fallback(**_: object) -> pd.DataFrame:
        nonlocal fallback_calls
        fallback_calls += 1
        return sina

    client = SimpleNamespace(
        stock_zh_a_hist_min_em=slow_primary,
        stock_zh_a_minute=fallback,
    )
    adapter = AKShareMarketDataAdapter(
        client,
        # 为高负载 Windows CI 主机上的独立回退工作线程预留足够调度余量，
        # 同时仍能证明主请求确实超时。
        timeout_seconds=0.20,
        now=lambda: datetime(2026, 8, 13, 10, 0, tzinfo=SHANGHAI),
    )

    bars = asyncio.run(
        adapter.fetch_intraday_bars_async(
            "000001.SZ",
            datetime(2026, 8, 13, 9, 30, tzinfo=SHANGHAI),
            datetime(2026, 8, 13, 9, 35, tzinfo=SHANGHAI),
            interval=MinuteInterval.FIVE_MINUTES,
        )
    )

    assert fallback_calls == 1
    assert len(bars) == 1
    assert bars[0].meta.degraded is True
    assert "Sina" in bars[0].meta.provider


def test_minute_data_falls_back_to_sina_and_strictly_filters_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sina = pd.DataFrame(
        [
            {
                "day": "2026-08-13 09:30:00",
                "open": "10.00",
                "high": "10.02",
                "low": "9.99",
                "close": "10.01",
                "volume": "10000",
                "amount": "100100",
            },
            {
                "day": "2026-08-13 09:35:00",
                "open": "10.01",
                "high": "10.10",
                "low": "10.00",
                "close": "10.08",
                "volume": "227637",
                "amount": "2280000.50",
            },
            {
                "day": "2026-08-13 09:40:00",
                "open": "10.08",
                "high": "10.12",
                "low": "10.07",
                "close": "10.11",
                "volume": "20000",
                "amount": "202000",
            },
        ]
    )
    client = SimpleNamespace(
        stock_zh_a_hist_min_em=lambda **_: pd.DataFrame(),
        stock_zh_a_minute=lambda **_: sina,
    )

    def failed_primary(**_: object) -> pd.DataFrame:
        raise ConnectionError("eastmoney disconnected")

    monkeypatch.setattr(client, "stock_zh_a_hist_min_em", failed_primary)
    captured: dict[str, object] = {}

    def fallback(**kwargs: object) -> pd.DataFrame:
        captured.update(kwargs)
        return sina

    monkeypatch.setattr(client, "stock_zh_a_minute", fallback)
    now = datetime(2026, 8, 13, 10, 0, tzinfo=SHANGHAI)
    adapter = AKShareMarketDataAdapter(client, max_attempts=1, now=lambda: now)

    bars = adapter.fetch_intraday_bars(
        "000001.SZ",
        datetime(2026, 8, 13, 9, 30, tzinfo=SHANGHAI),
        datetime(2026, 8, 13, 9, 35, tzinfo=SHANGHAI),
        interval=MinuteInterval.FIVE_MINUTES,
    )

    assert len(bars) == 1
    assert bars[0].start_at.strftime("%H:%M:%S") == "09:30:00"
    assert bars[0].end_at.strftime("%H:%M:%S") == "09:35:00"
    assert bars[0].volume_lots == 2276
    assert bars[0].amount == Decimal("2280000.50")
    assert bars[0].meta.degraded is True
    assert "Sina" in bars[0].meta.provider
    assert "odd-share remainder 37" in " ".join(bars[0].meta.warnings)
    assert captured == {"symbol": "sz000001", "period": "5", "adjust": ""}


def test_invalid_provider_schema_is_retried_then_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    client = SimpleNamespace(stock_intraday_em=lambda **_: pd.DataFrame())

    def provider(**_: object) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("transient")
        return pd.DataFrame([{"时间": "10:00:00"}])

    monkeypatch.setattr(client, "stock_intraday_em", provider)
    adapter = AKShareMarketDataAdapter(
        client,
        max_attempts=2,
        retry_backoff_seconds=0,
        now=lambda: datetime(2026, 8, 13, 10, 0, tzinfo=SHANGHAI),
    )

    with pytest.raises(AKSharePayloadError, match="missing columns"):
        adapter.fetch_trade_prints("000001")

    assert calls == 2


def test_async_method_moves_blocking_call_and_enforces_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = SimpleNamespace(stock_bid_ask_em=lambda **_: _single_spot_frame())
    adapter = AKShareMarketDataAdapter(client, timeout_seconds=0.005)

    def slow(_: str) -> object:
        time.sleep(0.02)
        return object()

    monkeypatch.setattr(adapter, "fetch_spot_snapshot", slow)

    with pytest.raises(AKShareTimeoutError, match="exceeded"):
        asyncio.run(adapter.fetch_spot_snapshot_async("000001"))
