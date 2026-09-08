from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.adapters.market_data import cross_market as facade
from gribuki_trade.adapters.market_data.cross_market_payload import (
    _parse_quote,
    _parse_quote_time,
    _required_decimal,
    _required_session_date,
    _resolve_columns,
)
from gribuki_trade.ports.cross_market import (
    CrossMarketInstrumentSpec,
    CrossMarketPayloadError,
    CrossMarketSegment,
)


def _spec() -> CrossMarketInstrumentSpec:
    return CrossMarketInstrumentSpec(
        instrument_id="CSI_300",
        display_name="沪深300指数",
        segment=CrossMarketSegment.A_SHARE,
        code_aliases=("000300",),
        name_aliases=("沪深300",),
        local_timezone="Asia/Shanghai",
    )


def test_payload_parser_resolves_aliases_and_preserves_decimal_precision() -> None:
    rows = [
        {
            "代码": "000300",
            "名称": "沪深300",
            "最新价": "4729.1234",
            "涨跌幅": "-0.4567",
            "最新行情时间": "2026-08-13 07:00:00+00:00",
        }
    ]

    columns = _resolve_columns(rows)
    quote = _parse_quote(
        rows[0],
        columns,
        _spec(),
        datetime(2026, 8, 13, 8, 1, tzinfo=UTC),
        future_tolerance=timedelta(seconds=5),
        match_basis="code",
    )

    assert quote.last == Decimal("4729.1234")
    assert quote.change_percent == Decimal("-0.4567")
    assert quote.local_quote_time.isoformat() == "2026-08-13T15:00:00+08:00"


def test_historical_facade_reexports_pure_parser_helpers() -> None:
    assert facade._parse_quote is _parse_quote
    assert facade._required_decimal is _required_decimal
    assert facade._required_session_date is _required_session_date


def test_payload_parser_accepts_provider_date_shapes_and_rejects_invalid_numbers() -> None:
    assert _required_session_date("2026-08-13").isoformat() == "2026-08-13"
    assert _required_session_date(datetime(2026, 8, 13, tzinfo=UTC)).isoformat() == "2026-08-13"

    with pytest.raises(CrossMarketPayloadError, match="not numeric"):
        _required_decimal("not-a-number", "close")


def test_quote_time_marks_timezone_naive_provider_values() -> None:
    parsed, warning = _parse_quote_time("2026-08-13 15:00:00")

    assert parsed.isoformat() == "2026-08-13T15:00:00+08:00"
    assert warning is not None
    assert "timezone-naive" in warning
