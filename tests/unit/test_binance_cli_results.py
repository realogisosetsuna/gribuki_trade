from decimal import Decimal
from types import SimpleNamespace

import pytest

from gribuki_trade.adapters.binance.gateway import BinanceProtocolError
from gribuki_trade.adapters.binance.models import BinanceAccount, BinanceBalance
from gribuki_trade.cli_commands.binance_results import (
    _binance_balance_decimal,
    _testnet_balance_diff,
    _testnet_reconciliation_payload,
)


def _account(*balances: BinanceBalance) -> BinanceAccount:
    return BinanceAccount(
        can_trade=True,
        can_withdraw=True,
        can_deposit=True,
        account_type="SPOT",
        balances=balances,
    )


def test_balance_decimal_preserves_exact_decimal_text() -> None:
    assert _binance_balance_decimal("15.12345678", "walletBalance") == Decimal("15.12345678")
    assert _binance_balance_decimal(Decimal("-0.00000001"), "unrealizedProfit") == Decimal(
        "-0.00000001"
    )


@pytest.mark.parametrize("value", [None, "invalid", "NaN", "Infinity", "-Infinity"])
def test_balance_decimal_rejects_malformed_or_non_finite_values(value: object) -> None:
    with pytest.raises(BinanceProtocolError):
        _binance_balance_decimal(value, "walletBalance")


def test_balance_diff_reports_added_removed_and_changed_assets_in_order() -> None:
    before = _account(
        BinanceBalance("USDT", Decimal("10"), Decimal("1")),
        BinanceBalance("BTC", Decimal("0.5"), Decimal("0")),
        BinanceBalance("UNCHANGED", Decimal("2"), Decimal("0")),
    )
    after = _account(
        BinanceBalance("USDT", Decimal("11.25"), Decimal("0.5")),
        BinanceBalance("ETH", Decimal("2"), Decimal("0")),
        BinanceBalance("UNCHANGED", Decimal("2"), Decimal("0")),
    )

    assert _testnet_balance_diff(before, after) == [
        {
            "asset": "BTC",
            "before_free": "0.5",
            "before_locked": "0",
            "after_free": "0",
            "after_locked": "0",
            "delta_free": "-0.5",
            "delta_locked": "0",
            "delta_total": "-0.5",
        },
        {
            "asset": "ETH",
            "before_free": "0",
            "before_locked": "0",
            "after_free": "2",
            "after_locked": "0",
            "delta_free": "2",
            "delta_locked": "0",
            "delta_total": "2",
        },
        {
            "asset": "USDT",
            "before_free": "10",
            "before_locked": "1",
            "after_free": "11.25",
            "after_locked": "0.5",
            "delta_free": "1.25",
            "delta_locked": "-0.5",
            "delta_total": "0.75",
        },
    ]


def test_reconciliation_payload_is_json_ready_and_does_not_expose_extra_fields() -> None:
    report = SimpleNamespace(
        dispatched_pending_commands=1,
        exchange_history_orders=2,
        exchange_open_orders=3,
        exchange_trades=4,
        reconciled_orders=5,
        recorded_balances=6,
        recorded_fills=7,
        recovered_commands=8,
        unresolved_order_ids=("ord-2", "ord-3"),
        secret="must-not-appear",
    )

    assert _testnet_reconciliation_payload(report) == {
        "dispatched_pending_commands": 1,
        "exchange_history_orders": 2,
        "exchange_open_orders": 3,
        "exchange_trades": 4,
        "reconciled_orders": 5,
        "recorded_balances": 6,
        "recorded_fills": 7,
        "recovered_commands": 8,
        "unresolved_order_ids": ["ord-2", "ord-3"],
    }
