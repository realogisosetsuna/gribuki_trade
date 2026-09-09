import argparse
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from gribuki_trade import cli
from gribuki_trade.cli_parsing import (
    add_order_arguments,
    iso_date,
    iso_datetime,
    macro_weight_decimal,
    non_negative_decimal,
    non_negative_float,
    non_negative_integer,
    positive_decimal,
    positive_float,
    positive_integer,
    positive_integer_or_unlimited,
    unit_fraction_decimal,
)


def test_common_order_options_are_shared_with_legacy_cli_alias() -> None:
    parser = argparse.ArgumentParser()
    add_order_arguments(parser)
    parsed = parser.parse_args(["--symbol", "ETHUSDT", "--notional", "12.5"])

    assert parsed.symbol == "ETHUSDT"
    assert parsed.notional == Decimal("12.5")
    assert cli._positive_decimal is positive_decimal
    assert cli._add_order_arguments is add_order_arguments


@pytest.mark.parametrize(
    ("converter", "valid", "expected"),
    [
        (positive_decimal, "1.2", Decimal("1.2")),
        (non_negative_decimal, "0", Decimal("0")),
        (unit_fraction_decimal, "0.5", Decimal("0.5")),
        (macro_weight_decimal, "0.4", Decimal("0.4")),
        (positive_integer, "4", 4),
        (positive_integer_or_unlimited, "unlimited", None),
        (non_negative_integer, "0", 0),
        (positive_float, "2.5", 2.5),
        (non_negative_float, "0", 0.0),
        (iso_date, "2026-09-09", date(2026, 9, 9)),
        (
            iso_datetime,
            "2026-09-09T10:20:30+08:00",
            datetime(2026, 9, 9, 10, 20, 30, tzinfo=UTC),
        ),
    ],
)
def test_argument_converters_accept_valid_values(converter, valid, expected) -> None:
    parsed = converter(valid)
    if converter is iso_datetime:
        assert parsed == datetime.fromisoformat(valid)
    else:
        assert parsed == expected


@pytest.mark.parametrize(
    ("converter", "value"),
    [
        (positive_decimal, "0"),
        (non_negative_decimal, "-1"),
        (unit_fraction_decimal, "1.01"),
        (macro_weight_decimal, "0.41"),
        (positive_integer, "0"),
        (positive_integer_or_unlimited, "nope"),
        (non_negative_integer, "-1"),
        (positive_float, "inf"),
        (non_negative_float, "-0.1"),
        (iso_date, "09/09/2026"),
        (iso_datetime, "2026-09-09T10:20:30"),
    ],
)
def test_argument_converters_fail_closed(converter, value) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        converter(value)
