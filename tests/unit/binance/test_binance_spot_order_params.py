from decimal import Decimal

import pytest

from gribuki_trade.adapters.binance.models import BinanceSpotOrderLeg
from gribuki_trade.adapters.binance.spot_order_params import (
    list_leg_params,
    prefixed_leg_params,
    spot_order_params,
    validate_leg,
)
from gribuki_trade.domain.orders import Side


def _leg(**changes: object) -> BinanceSpotOrderLeg:
    values: dict[str, object] = {
        "order_type": "LIMIT",
        "side": Side.BUY,
        "quantity": Decimal("0.01000000"),
        "price": Decimal("50000.00"),
        "time_in_force": "GTC",
    }
    values.update(changes)
    return BinanceSpotOrderLeg(**values)  # type: ignore[arg-type]


def test_spot_order_params_serializes_decimal_and_optional_fields() -> None:
    params = spot_order_params(
        symbol="BTCUSDT",
        side=Side.BUY,
        order_type_value="limit",
        quantity=Decimal("0.01000000"),
        quote_order_quantity=None,
        price=Decimal("50000.00"),
        stop_price=None,
        trailing_delta=None,
        time_in_force="gtc",
        client_order_id="params-1",
        iceberg_quantity=Decimal("0.00200"),
        strategy_id=7,
        strategy_type=None,
        self_trade_prevention_mode="EXPIRE_MAKER",
        response_type="result",
    )

    assert params == [
        ("symbol", "BTCUSDT"),
        ("side", "BUY"),
        ("type", "LIMIT"),
        ("timeInForce", "GTC"),
        ("quantity", "0.01000000"),
        ("price", "50000.00"),
        ("newClientOrderId", "params-1"),
        ("icebergQty", "0.00200"),
        ("strategyId", 7),
        ("selfTradePreventionMode", "EXPIRE_MAKER"),
        ("newOrderRespType", "RESULT"),
    ]


@pytest.mark.parametrize(
    ("order_type", "changes", "message"),
    [
        ("LIMIT", {"price": None}, "price is required"),
        ("STOP_LOSS", {}, "stop_price or trailing_delta is required"),
        ("MARKET", {"quantity": None, "quote_order_quantity": None}, "exactly one"),
    ],
)
def test_spot_order_params_rejects_incomplete_orders(
    order_type: str, changes: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        spot_order_params(
            symbol="BTCUSDT",
            side=Side.BUY,
            order_type_value=order_type,
            quantity=changes.pop("quantity", Decimal("0.01")),
            quote_order_quantity=changes.pop("quote_order_quantity", None),
            price=changes.pop("price", Decimal("50000")),
            stop_price=changes.pop("stop_price", None),
            trailing_delta=changes.pop("trailing_delta", None),
            time_in_force=changes.pop("time_in_force", "GTC"),
            client_order_id=None,
            iceberg_quantity=None,
            strategy_id=None,
            strategy_type=None,
            self_trade_prevention_mode=None,
            response_type="RESULT",
        )


def test_prefixed_leg_params_share_validation_and_wire_shape() -> None:
    leg = _leg(trailing_delta=25, client_order_id="leg-1")

    validate_leg(leg, require_quantity=True)

    assert prefixed_leg_params("working", leg, required=True) == [
        ("workingType", "LIMIT"),
        ("workingSide", "BUY"),
        ("workingQuantity", "0.01000000"),
        ("workingPrice", "50000.00"),
        ("workingTrailingDelta", 25),
        ("workingTimeInForce", "GTC"),
        ("workingClientOrderId", "leg-1"),
    ]
    assert list_leg_params("above", leg, quantity=leg.quantity) == [
        ("abovePrice", "50000.00"),
        ("aboveTrailingDelta", 25),
        ("aboveTimeInForce", "GTC"),
        ("aboveClientOrderId", "leg-1"),
    ]


def test_validate_leg_rejects_invalid_client_id_and_trailing_delta() -> None:
    with pytest.raises(ValueError, match="client order id"):
        validate_leg(_leg(client_order_id="contains spaces"), require_quantity=False)
    with pytest.raises(ValueError, match="trailing_delta"):
        validate_leg(_leg(trailing_delta=0), require_quantity=False)
