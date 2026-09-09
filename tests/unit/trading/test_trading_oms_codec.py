"""券商中立 OMS 编解码边界的纯函数测试。"""

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side
from gribuki_trade.trading.core.models import OrderSnapshot
from gribuki_trade.trading.core.oms_codec import (
    decimal_text,
    identifier,
    json_text,
    non_negative_decimal,
    order_payload,
    parse_time,
    positive_decimal,
    row_to_order,
    should_apply,
    time_text,
    utc,
)

NOW = datetime(2026, 9, 9, 1, 2, 3, 4000, tzinfo=UTC)


def make_order() -> OrderIntent:
    return OrderIntent(
        client_order_id="codec-order",
        account_id="acct",
        strategy_id="strategy",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("2"),
        limit_price=Decimal("100"),
        created_at=NOW,
    )


def test_json_decimal_and_time_codecs_are_deterministic() -> None:
    assert json_text({"z": "1.20", "a": "x"}) == '{"a":"x","z":"1.20"}'
    assert decimal_text(Decimal("1.20")) == "1.20"
    encoded = time_text(NOW)
    assert encoded == "2026-09-09T01:02:03.004000+00:00"
    assert parse_time(encoded) == NOW
    with pytest.raises(ValueError, match="timezone-aware"):
        utc(datetime(2026, 9, 9, 1), "timestamp")


def test_scalar_validation_and_order_payload_are_pure() -> None:
    assert positive_decimal("1.5", "quantity") == Decimal("1.5")
    assert non_negative_decimal("0", "fee") == Decimal("0")
    assert identifier("  event-1 ", "event_id") == "event-1"
    assert order_payload(make_order())["created_at"] == time_text(NOW)
    with pytest.raises(ValueError, match="positive"):
        positive_decimal("0", "quantity")
    with pytest.raises(ValueError, match="must not be empty"):
        identifier("  ", "event_id")


def test_row_codec_reconstructs_decimal_and_status_values() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    row = connection.execute(
        """
        SELECT 'codec-order' client_order_id, 'acct' account_id,
               'strategy' strategy_id, 'BTCUSDT' symbol, 'BUY' side,
               'LIMIT' order_type, '2' quantity, '100' limit_price,
               ? created_at, 'ACCEPTED' status, '1' filled_quantity,
               '101.5' average_fill_price, '42' exchange_order_id,
               NULL reason, NULL broker_error_code, ? updated_at
        """,
        (time_text(NOW), time_text(NOW)),
    ).fetchone()
    assert row is not None
    snapshot = row_to_order(row)
    assert snapshot.order == make_order()
    assert snapshot.status is OrderStatus.ACCEPTED
    assert snapshot.filled_quantity == Decimal("1")
    assert snapshot.average_fill_price == Decimal("101.5")
    assert snapshot.exchange_order_id == "42"
    connection.close()


def test_status_projection_never_regresses_terminal_order() -> None:
    current = OrderSnapshot(
        order=make_order(),
        status=OrderStatus.FILLED,
        filled_quantity=Decimal("2"),
        average_fill_price=Decimal("100"),
        exchange_order_id="42",
        reason=None,
        updated_at=NOW,
    )
    assert not should_apply(current, OrderStatus.CANCELED, Decimal("2"))
