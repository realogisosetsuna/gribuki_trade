from datetime import date, datetime

import pytest

from gribuki_trade.adapters.market_data.akshare_daily_parsing import (
    AKShareDailyAssetType,
    AKShareDailyNoDataError,
    AKShareDailyPayloadError,
    _date_value,
    _frame_records,
    _normalize_symbol,
)


class _Frame:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def to_dict(self, *, orient: str) -> list[dict[str, object]]:
        assert orient == "records"
        return self._rows


def test_symbol_normalization_infers_exchange_without_network() -> None:
    assert _normalize_symbol(" 920001 ") == ("920001.BJ", "920001")
    assert _normalize_symbol("600000.sh") == ("600000.SH", "600000")


def test_date_value_accepts_python_date_and_datetime() -> None:
    assert _date_value(date(2026, 8, 13)) == date(2026, 8, 13)
    assert _date_value(datetime(2026, 8, 13, 9, 30)) == date(2026, 8, 13)


def test_frame_records_rejects_empty_or_non_dataframe_payloads() -> None:
    rows = _frame_records(_Frame([{"date": "2026-08-13"}]), "stock_zh_a_hist")
    assert rows == [{"date": "2026-08-13"}]

    with pytest.raises(AKShareDailyPayloadError, match="did not return a DataFrame"):
        _frame_records(object(), "stock_zh_a_hist")

    class EmptyFrame:
        def to_dict(self, *, orient: str) -> list[dict[str, object]]:
            return []

    with pytest.raises(AKShareDailyNoDataError, match="no rows"):
        _frame_records(EmptyFrame(), "stock_zh_a_hist")


def test_asset_type_enum_keeps_provider_routing_values_stable() -> None:
    assert AKShareDailyAssetType.STOCK.value == "stock"
    assert AKShareDailyAssetType.ETF.value == "etf"
