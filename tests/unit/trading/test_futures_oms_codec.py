from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from gribuki_trade.trading.futures.futures_models import FuturesUserEvent
from gribuki_trade.trading.futures.futures_oms_codec import (
    event_identity,
    json_payload,
    mapping_payload,
    parse_timestamp,
    scope,
    timestamp,
    utc_or_now,
)


def test_json_payload_preserves_decimal_and_sorts_mapping_keys() -> None:
    encoded = json_payload({"z": Decimal("1.20"), "a": (Decimal("2"),)})
    assert encoded == '{"a":["2"],"z":"1.20"}'
    assert mapping_payload(encoded) == {"a": ["2"], "z": "1.20"}


def test_scope_and_timestamps_normalize_values_and_reject_naive_time() -> None:
    assert scope(" acct ", " LIVE ", " USDS_FUTURES ") == ("acct", "LIVE", "USDS_FUTURES")
    instant = datetime(2026, 9, 9, 1, 0, tzinfo=UTC) + timedelta(hours=2)
    assert timestamp(instant) == "2026-09-09T03:00:00+00:00"
    assert parse_timestamp(timestamp(instant)) == instant
    with pytest.raises(ValueError, match="timezone-aware"):
        timestamp(datetime(2026, 9, 9, 1, 0))


def test_event_identity_is_stable_for_equivalent_payload_order() -> None:
    first = FuturesUserEvent(
        event_type="ORDER_TRADE_UPDATE",
        event_time_ms=10,
        transaction_time_ms=11,
        received_time_ms=12,
        connection_epoch=1,
        payload={"b": Decimal("2"), "a": "x"},
    )
    second = FuturesUserEvent(
        event_type="ORDER_TRADE_UPDATE",
        event_time_ms=10,
        transaction_time_ms=11,
        received_time_ms=99,
        connection_epoch=7,
        payload={"a": "x", "b": Decimal("2")},
    )
    assert event_identity(first) == event_identity(second)


def test_utc_or_now_requires_aware_explicit_time() -> None:
    now = datetime(2026, 9, 9, 1, 0, tzinfo=UTC)
    assert utc_or_now(now) is now
    with pytest.raises(ValueError, match="timezone-aware"):
        utc_or_now(datetime(2026, 9, 9, 1, 0))
