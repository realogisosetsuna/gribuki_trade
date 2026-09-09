import asyncio
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest

from gribuki_trade.adapters.market_data.baostock import (
    BaoStockDailyAdapter,
    BaoStockError,
    BaoStockTimeoutError,
)
from gribuki_trade.ports.market_data import (
    AsyncTradingCalendar,
    MarketDataTimeoutError,
    MarketDataUnavailableError,
)

FIELDS = [
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "preclose",
    "volume",
    "amount",
    "adjustflag",
    "turn",
    "tradestatus",
    "isST",
]


class FakeResult:
    def __init__(self, rows: list[list[str]], error_code: str = "0") -> None:
        self.fields = FIELDS
        self.error_code = error_code
        self.error_msg = "query error"
        self._rows = iter(rows)
        self._current: list[str] | None = None

    def next(self) -> bool:
        self._current = next(self._rows, None)
        return self._current is not None

    def get_row_data(self) -> list[str]:
        assert self._current is not None
        return self._current


class FakeClient:
    def __init__(self, result: FakeResult, login_code: str = "0") -> None:
        self.result = result
        self.login_code = login_code
        self.logged_out = False
        self.query_args: tuple[object, ...] | None = None
        self.query_kwargs: dict[str, str] | None = None

    def login(self) -> SimpleNamespace:
        return SimpleNamespace(error_code=self.login_code, error_msg="login error")

    def logout(self) -> None:
        self.logged_out = True

    def query_history_k_data_plus(self, *args: object, **kwargs: str) -> FakeResult:
        self.query_args = args
        self.query_kwargs = kwargs
        return self.result


class FakeCalendarClient:
    def __init__(
        self,
        rows: list[list[str]],
        *,
        error_code: str = "0",
    ) -> None:
        self.result = FakeResult(rows, error_code=error_code)
        self.result.fields = ["calendar_date", "is_trading_day"]
        self.logged_out = False
        self.query_kwargs: dict[str, str] | None = None

    def login(self) -> SimpleNamespace:
        return SimpleNamespace(error_code="0", error_msg="")

    def logout(self) -> None:
        self.logged_out = True

    def query_trade_dates(self, **kwargs: str) -> FakeResult:
        self.query_kwargs = kwargs
        return self.result


def test_fetches_anonymous_daily_data_and_preserves_flags() -> None:
    row = [
        "2026-08-12",
        "sh.600000",
        "10.00",
        "10.30",
        "9.90",
        "10.20",
        "9.98",
        "123456",
        "1250000.50",
        "3",
        "0.42",
        "1",
        "0",
    ]
    client = FakeClient(FakeResult([row]))

    bars = BaoStockDailyAdapter(client).fetch_daily_bars(
        "600000.SH",
        date(2026, 8, 1),
        date(2026, 8, 12),
    )

    assert len(bars) == 1
    assert bars[0].symbol == "600000.SH"
    assert bars[0].close == Decimal("10.20")
    assert bars[0].volume == 123456
    assert bars[0].is_trading is True
    assert bars[0].is_st is False
    assert client.query_args == ("sh.600000", BaoStockDailyAdapter._FIELDS)
    assert client.query_kwargs is not None
    assert client.query_kwargs["adjustflag"] == "3"
    assert client.logged_out is True


@pytest.mark.parametrize(
    ("requested_symbol", "expected_symbol", "provider_symbol"),
    [
        ("430047.BJ", "430047.BJ", "bj.430047"),
        ("920001", "920001.BJ", "bj.920001"),
    ],
)
def test_bse_daily_symbol_is_mapped_to_baostock_provider_namespace(
    requested_symbol: str,
    expected_symbol: str,
    provider_symbol: str,
) -> None:
    row = [
        "2026-08-12",
        provider_symbol,
        "10.00",
        "10.30",
        "9.90",
        "10.20",
        "9.98",
        "123456",
        "1250000.50",
        "3",
        "0.42",
        "1",
        "0",
    ]
    client = FakeClient(FakeResult([row]))

    bars = BaoStockDailyAdapter(client).fetch_daily_bars(
        requested_symbol,
        date(2026, 8, 1),
        date(2026, 8, 12),
    )

    assert client.query_args is not None
    assert client.query_args[0] == provider_symbol
    assert bars[0].symbol == expected_symbol


def test_suspended_record_keeps_missing_prices_visible() -> None:
    row = ["2026-08-12", "sz.000001", "", "", "", "", "10", "", "", "3", "", "0", "0"]
    client = FakeClient(FakeResult([row]))

    bar = BaoStockDailyAdapter(client).fetch_daily_bars(
        "000001.SZ", date(2026, 8, 12), date(2026, 8, 12)
    )[0]

    assert bar.close is None
    assert bar.volume == 0
    assert bar.is_trading is False


def test_query_failure_logs_out_and_raises_provider_error() -> None:
    client = FakeClient(FakeResult([], error_code="100"))

    with pytest.raises(BaoStockError, match="query failed"):
        BaoStockDailyAdapter(client, max_attempts=1).fetch_daily_bars(
            "600000", date(2026, 8, 1), date(2026, 8, 12)
        )

    assert client.logged_out is True


@pytest.mark.parametrize("symbol", ["ABC", "600000.XSHG", "12345.SH"])
def test_invalid_symbol_is_rejected_before_login(symbol: str) -> None:
    client = FakeClient(FakeResult([]))

    with pytest.raises(ValueError, match="symbol"):
        BaoStockDailyAdapter(client).fetch_daily_bars(
            symbol, date(2026, 8, 1), date(2026, 8, 12)
        )

    assert client.logged_out is False


def test_transient_query_is_retried_with_a_new_anonymous_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = [
        "2026-08-12",
        "sh.600000",
        "10",
        "10.3",
        "9.9",
        "10.2",
        "9.98",
        "100",
        "1010",
        "3",
        "0.1",
        "1",
        "0",
    ]
    client = FakeClient(FakeResult([row]))
    calls = 0

    def query(*args: object, **kwargs: str) -> FakeResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("temporary transport failure")
        return client.result

    monkeypatch.setattr(client, "query_history_k_data_plus", query)
    delays: list[float] = []
    adapter = BaoStockDailyAdapter(
        client,
        max_attempts=2,
        retry_backoff_seconds=0.1,
        sleep=delays.append,
    )

    bars = adapter.fetch_daily_bars(
        "600000.SH", date(2026, 8, 12), date(2026, 8, 12)
    )

    assert len(bars) == 1
    assert calls == 2
    assert delays == [0.1]


def test_async_daily_wrapper_returns_without_blocking_event_loop() -> None:
    row = [
        "2026-08-12",
        "sh.600000",
        "10",
        "10.3",
        "9.9",
        "10.2",
        "9.98",
        "100",
        "1010",
        "3",
        "0.1",
        "1",
        "0",
    ]
    adapter = BaoStockDailyAdapter(FakeClient(FakeResult([row])))

    bars = asyncio.run(
        adapter.fetch_daily_bars_async(
            "600000.SH", date(2026, 8, 12), date(2026, 8, 12)
        )
    )

    assert bars[0].close == Decimal("10.2")


def test_fetches_complete_trade_calendar_and_preserves_holiday_rows() -> None:
    client = FakeCalendarClient(
        [
            ["2026-08-14", "1"],
            ["2026-08-15", "0"],
            ["2026-08-16", "0"],
            ["2026-08-17", "1"],
        ]
    )
    adapter = BaoStockDailyAdapter(client)

    days = asyncio.run(
        adapter.fetch_trade_calendar_async(date(2026, 8, 14), date(2026, 8, 17))
    )

    assert [day.calendar_date for day in days] == [
        date(2026, 8, 14),
        date(2026, 8, 15),
        date(2026, 8, 16),
        date(2026, 8, 17),
    ]
    assert [day.is_trading_day for day in days] == [True, False, False, True]
    assert client.query_kwargs == {
        "start_date": "2026-08-14",
        "end_date": "2026-08-17",
    }
    assert client.logged_out is True
    assert isinstance(adapter, AsyncTradingCalendar)


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [["2026-08-14", "1"], ["2026-08-16", "0"]],
        [["2026-08-15", "0"], ["2026-08-14", "1"]],
        [["2026-08-14", "1"], ["2026-08-14", "0"]],
    ],
)
def test_trade_calendar_rejects_empty_missing_unordered_or_duplicate_days(
    rows: list[list[str]],
) -> None:
    client = FakeCalendarClient(rows)

    with pytest.raises(BaoStockError, match="cover|unique"):
        BaoStockDailyAdapter(client, max_attempts=1).fetch_trade_calendar(
            date(2026, 8, 14), date(2026, 8, 15)
        )

    assert client.logged_out is True


@pytest.mark.parametrize("marker", ["", "yes", "2", "-1"])
def test_trade_calendar_rejects_non_binary_trading_marker(marker: str) -> None:
    client = FakeCalendarClient([["2026-08-14", marker]])

    with pytest.raises(BaoStockError, match="invalid BaoStock trade-calendar row"):
        BaoStockDailyAdapter(client, max_attempts=1).fetch_trade_calendar(
            date(2026, 8, 14), date(2026, 8, 14)
        )


def test_trade_calendar_rejects_invalid_range_before_login() -> None:
    client = FakeCalendarClient([])

    with pytest.raises(ValueError, match="start"):
        BaoStockDailyAdapter(client).fetch_trade_calendar(
            date(2026, 8, 15), date(2026, 8, 14)
        )

    assert client.logged_out is False


def test_baostock_errors_share_market_data_failure_taxonomy() -> None:
    assert issubclass(BaoStockError, MarketDataUnavailableError)
    assert issubclass(BaoStockTimeoutError, MarketDataTimeoutError)
    assert issubclass(BaoStockTimeoutError, BaoStockError)
