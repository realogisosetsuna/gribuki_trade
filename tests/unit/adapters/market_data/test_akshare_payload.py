import json
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest

from gribuki_trade.adapters.market_data.akshare import (
    AKSharePayloadError as FacadePayloadError,
)
from gribuki_trade.adapters.market_data.akshare_payload import (
    AKShareNoDataError,
    AKSharePayloadError,
    eastmoney_minute_record,
    frame_records,
    normalize_symbol,
    normalize_tencent_spot_row,
    parse_provider_datetime,
    require_columns,
    sina_jsonp_records,
)


def test_facade_reexports_payload_error_type() -> None:
    assert FacadePayloadError is AKSharePayloadError


def test_frame_records_accepts_dataframe_records_and_rejects_invalid_payload() -> None:
    frame = pd.DataFrame([{"时间": "09:30:00", "收盘": 10.2}])

    assert frame_records(frame, "minute") == [{"时间": "09:30:00", "收盘": 10.2}]
    with pytest.raises(AKSharePayloadError, match="did not return a DataFrame"):
        frame_records(object(), "minute")


def test_sina_jsonp_records_decodes_rows_and_rejects_empty_data() -> None:
    payload = "callback=(" + json.dumps([{"day": "2026-09-09 09:30:00"}]) + ");"

    assert sina_jsonp_records(payload) == [{"day": "2026-09-09 09:30:00"}]
    with pytest.raises(AKShareNoDataError, match="no rows"):
        sina_jsonp_records("callback=([]);")


def test_eastmoney_record_and_column_validation_are_provider_specific() -> None:
    record = eastmoney_minute_record(
        "09:30,10,10.2,10.3,9.9,100,1000,10.1", has_vwap=True
    )
    assert record["均价"] == "10.1"
    with pytest.raises(AKSharePayloadError, match="truncated"):
        eastmoney_minute_record("09:30,10", has_vwap=False)

    rows: list[Mapping[str, object]] = [{"时间": "09:30"}]
    with pytest.raises(AKSharePayloadError, match="missing columns"):
        require_columns(rows, frozenset({"时间", "收盘"}), "minute")


def test_market_scalar_normalization_preserves_units_and_missing_prices() -> None:
    row, warnings = normalize_tencent_spot_row(
        "600000",
        {"volume": "10.5", "zxj": "11", "zd": "1", "turnover": "2"},
    )
    assert row["最新价"] == Decimal("11")
    assert row["昨收"] == Decimal("10")
    assert row["成交量"] == 10
    assert row["成交额"] == Decimal("20000")
    assert row["今开"] is None
    assert any("fractional lot remainder" in value for value in warnings)


def test_symbol_and_provider_time_normalization_use_market_contract() -> None:
    assert normalize_symbol(" 510300 ") == ("510300.SH", "510300")
    parsed = parse_provider_datetime("2026-09-09 09:30:00")
    assert parsed.replace(tzinfo=None) == datetime(2026, 9, 9, 9, 30)
    assert parsed.utcoffset() is not None
