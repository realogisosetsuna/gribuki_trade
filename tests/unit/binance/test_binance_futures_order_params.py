from decimal import Decimal

import pytest

from gribuki_trade.adapters.binance.futures.order_params import (
    BinanceFuturesProtectionOrder,
    algo_order_params,
    order_params,
)


def _symbol(value: str) -> str:
    return value.strip().upper()


def _enum(value: str, name: str) -> str:
    del name
    return value.strip().upper()


def test_order_params_encodes_aliases_and_boolean_values() -> None:
    params = order_params(
        {
            "symbol": "btcusdt",
            "side": "buy",
            "type": "limit",
            "quantity": Decimal("0.001"),
            "price": Decimal("50000"),
            "time_in_force": "gtc",
            "position_side": "long",
            "reduce_only": False,
            "client_order_id": "client-1",
            "self_trade_prevention_mode": "EXPIRE_MAKER",
        },
        normalize_symbol=_symbol,
        enum_value=_enum,
    )

    assert params == [
        ("symbol", "BTCUSDT"),
        ("side", "BUY"),
        ("type", "LIMIT"),
        ("quantity", Decimal("0.001")),
        ("price", Decimal("50000")),
        ("timeInForce", "GTC"),
        ("positionSide", "LONG"),
        ("reduceOnly", "false"),
        ("newClientOrderId", "client-1"),
        ("selfTradePreventionMode", "EXPIRE_MAKER"),
    ]


def test_order_params_accepts_order_type_alias_and_requires_core_fields() -> None:
    assert order_params(
        {"symbol": "BTCUSDT", "side": "SELL", "order_type": "MARKET"},
        normalize_symbol=_symbol,
        enum_value=_enum,
    ) == [("symbol", "BTCUSDT"), ("side", "SELL"), ("type", "MARKET")]
    with pytest.raises(ValueError, match="requires symbol, side and type"):
        order_params(
            {"symbol": "BTCUSDT", "side": "BUY"},
            normalize_symbol=_symbol,
            enum_value=_enum,
        )


def test_algo_order_params_maps_trigger_alias_and_default_type() -> None:
    params = algo_order_params(
        {
            "symbol": "btcusdt",
            "side": "sell",
            "type": "trailing_stop_market",
            "position_side": "short",
            "callback_rate": Decimal("1.5"),
            "activate_price": Decimal("51000"),
            "close_position": False,
        },
        enum_value=_enum,
    )
    assert params == [
        ("symbol", "btcusdt"),
        ("side", "SELL"),
        ("type", "TRAILING_STOP_MARKET"),
        ("positionSide", "SHORT"),
        ("closePosition", "false"),
        ("callbackRate", Decimal("1.5")),
        ("activatePrice", Decimal("51000")),
        ("algoType", "CONDITIONAL"),
    ]


def test_algo_order_params_rejects_two_trigger_names() -> None:
    with pytest.raises(ValueError, match="trigger_price or stop_price"):
        algo_order_params(
            {
                "symbol": "BTCUSDT",
                "side": "BUY",
                "trigger_price": "50000",
                "stop_price": "50000",
            },
            enum_value=_enum,
        )


def test_protection_order_validation_is_network_free() -> None:
    BinanceFuturesProtectionOrder(
        kind="stop_loss", symbol="BTCUSDT", side="SELL", stop_price="49000"
    ).validate()
    with pytest.raises(ValueError, match="callback_rate"):
        BinanceFuturesProtectionOrder(
            kind="trailing_stop", symbol="BTCUSDT", side="SELL", quantity="0.1"
        ).validate()
    with pytest.raises(ValueError, match="close_position"):
        BinanceFuturesProtectionOrder(
            kind="trailing_stop",
            symbol="BTCUSDT",
            side="SELL",
            callback_rate="1",
        ).validate()
