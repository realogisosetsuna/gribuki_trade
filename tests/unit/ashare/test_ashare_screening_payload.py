from datetime import date
from decimal import Decimal

import pytest

from gribuki_trade.adapters.ashare.screening_payload import (
    AKShareScreeningPayloadError,
    _classify_symbol,
    _parse_history,
    _resolve_columns,
)


def test_screening_payload_normalizes_symbols_and_history() -> None:
    assert _classify_symbol("sh600000") == ("600000.SH", "SSE_MAIN")
    bars = _parse_history(
        (
            {
                "日期": "2026-08-13",
                "开盘": "9.5",
                "收盘": "10",
                "最高": "10.2",
                "最低": "9.4",
                "成交量": "1000",
                "成交额": "10000",
            },
        ),
        as_of=date(2026, 8, 13),
    )
    assert bars[0].close == Decimal("10")


def test_screening_payload_rejects_ambiguous_provider_columns() -> None:
    with pytest.raises(AKShareScreeningPayloadError, match="ambiguous columns"):
        _resolve_columns(
            ({"code": "600000", "代码": "600000"},),
            {"code": ("code", "代码")},
            required=("code",),
        )
