from datetime import UTC
from decimal import Decimal

import pytest

from gribuki_trade.adapters.ashare.context_parsing import (
    _normalize_etf_symbol,
    _optional_decimal,
    _parse_date,
    _parse_datetime,
    _required_positive_decimal,
)
from gribuki_trade.ports.ashare_context import AShareContextPayloadError


def test_context_scalar_parsers_normalize_public_provider_values() -> None:
    assert _normalize_etf_symbol("510300") == ("510300.SH", "510300")
    assert _optional_decimal("1,234.50", "amount") == Decimal("1234.50")
    assert _required_positive_decimal("2.5", "price") == Decimal("2.5")
    assert _parse_date("20260909", "session").isoformat() == "2026-09-09"
    assert _parse_datetime("2026-09-09T08:00:00Z", UTC, "observed").tzinfo is UTC


def test_context_scalar_parsers_fail_closed_on_invalid_symbols_or_prices() -> None:
    with pytest.raises(ValueError):
        _normalize_etf_symbol("159915.SH")
    with pytest.raises(ValueError):
        _normalize_etf_symbol("bad")
    with pytest.raises(AShareContextPayloadError):
        _required_positive_decimal("0", "price")
