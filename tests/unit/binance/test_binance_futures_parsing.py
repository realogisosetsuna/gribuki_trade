from decimal import Decimal
from types import MappingProxyType

import pytest

from gribuki_trade.adapters.binance.errors import BinanceProtocolError
from gribuki_trade.adapters.binance.futures import BinanceFuturesRestClient
from gribuki_trade.adapters.binance.futures import BinanceFuturesTicker as FacadeTicker
from gribuki_trade.adapters.binance.futures_parsing import (
    BinanceFuturesTicker,
    enum_value,
    normalize_symbol,
    parse_futures_ticker,
    parse_order_book_levels,
    parse_order_book_snapshot,
    require_mapping,
    validate_listen_key,
)
from gribuki_trade.adapters.binance.models import OrderBookLevel


@pytest.mark.parametrize(
    ("payload", "symbol", "time_ms"),
    [
        ({"symbol": "btcusdt", "price": "64000.00000001"}, "BTCUSDT", None),
        (
            [{"symbol": "btcusd_perp", "price": "64000.00000001", "time": "12"}],
            "BTCUSD_PERP",
            12,
        ),
    ],
)
def test_ticker_keeps_product_payload_shapes_and_decimal_precision(
    payload: object, symbol: str, time_ms: int | None
) -> None:
    """单交易对响应允许两种产品载荷形状，价格仍按十进制精确解析。"""

    ticker = parse_futures_ticker(payload)

    assert ticker == BinanceFuturesTicker(symbol, Decimal("64000.00000001"), time_ms)
    assert FacadeTicker is BinanceFuturesTicker


@pytest.mark.parametrize("payload", [[], [{}, {}], ["BTCUSDT"]])
def test_ticker_rejects_ambiguous_single_symbol_arrays(payload: object) -> None:
    with pytest.raises(BinanceProtocolError, match="exactly one symbol"):
        parse_futures_ticker(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {"price": "1"},
        {"symbol": "BTC/USDT", "price": "1"},
        {"symbol": "BTCUSDT", "price": "not-a-price"},
        {"symbol": "BTCUSDT", "price": "1", "time": "bad-time"},
    ],
)
def test_ticker_reports_malformed_fields_as_protocol_errors(payload: object) -> None:
    with pytest.raises(BinanceProtocolError, match="ticker response is malformed"):
        parse_futures_ticker(payload)


def test_snapshot_keeps_update_identity_exact_levels_and_zero_quantity() -> None:
    """快照恢复需要保留序号和零数量档位，不能通过解析悄悄丢弃条目。"""

    snapshot = parse_order_book_snapshot(
        {
            "lastUpdateId": "123",
            "bids": [["100.00000001", "0"]],
            "asks": [("101.00000001", "2.50000000")],
        },
        symbol=" btcusd_perp ",
    )

    assert snapshot.symbol == "BTCUSD_PERP"
    assert snapshot.last_update_id == 123
    assert snapshot.bids == (OrderBookLevel(Decimal("100.00000001"), Decimal("0")),)
    assert snapshot.asks == (OrderBookLevel(Decimal("101.00000001"), Decimal("2.50000000")),)


@pytest.mark.parametrize(
    "levels",
    ["bad-levels", [["100"]], [["100", "1", "extra"]], [["0", "1"]], [["1", "-1"]]],
)
def test_snapshot_never_silently_skips_malformed_levels(levels: object) -> None:
    with pytest.raises(BinanceProtocolError, match="order book response is malformed"):
        parse_order_book_snapshot(
            {"lastUpdateId": 1, "bids": levels, "asks": []}, symbol="BTCUSDT"
        )


def test_scalar_validation_preserves_facade_compatibility() -> None:
    assert normalize_symbol(" btcusd_perp ") == "BTCUSD_PERP"
    assert enum_value(" stop_market ", "type") == "STOP_MARKET"
    assert validate_listen_key("offline-key_123") == "offline-key_123"
    assert BinanceFuturesRestClient._normalize_symbol(" btcusd_perp ") == "BTCUSD_PERP"
    assert BinanceFuturesRestClient._enum_value(" stop_market ", "type") == "STOP_MARKET"
    assert BinanceFuturesRestClient._validate_listen_key("offline-key_123") == "offline-key_123"
    assert BinanceFuturesRestClient._parse_order_book_levels([["1", "2"]]) == (
        parse_order_book_levels([["1", "2"]])
    )
    with pytest.raises(ValueError, match="symbol"):
        normalize_symbol("BTCUSDT?limit=5000")
    with pytest.raises(ValueError, match="invalid characters"):
        validate_listen_key("offline-key&symbol=BTCUSDT")


def test_mapping_validation_accepts_read_only_mappings_and_rejects_lists() -> None:
    mapping = MappingProxyType({"assets": []})
    assert require_mapping(mapping, "account") is mapping
    assert BinanceFuturesRestClient._require_mapping(mapping, "account") is mapping
    with pytest.raises(BinanceProtocolError, match="account response must be an object"):
        require_mapping([], "account")
