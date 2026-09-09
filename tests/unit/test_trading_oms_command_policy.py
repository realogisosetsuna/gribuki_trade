"""券商中立 OMS 命令策略边界的纯函数测试。"""

from datetime import timedelta

import pytest

from gribuki_trade.domain.orders import OrderStatus
from gribuki_trade.trading.models import TradingCommandStatus
from gribuki_trade.trading.oms_command_policy import (
    normalize_command_scope,
    project_unknown_command,
    unknown_command_event_id,
    validate_claim_limit,
    validate_lease_for,
)


def test_scope_normalization_is_stable_and_deduplicates_symbols() -> None:
    scope = normalize_command_scope(" acct ", ("BTCUSDT", " BTCUSDT ", "ETHUSDT"))

    assert scope.account_id == "acct"
    assert scope.symbols == ("BTCUSDT", "ETHUSDT")


def test_scope_preserves_none_and_empty_filter_semantics() -> None:
    assert normalize_command_scope(None, None).symbols is None
    assert normalize_command_scope(None, ()).symbols == ()

    with pytest.raises(ValueError, match="account_id must not be empty"):
        normalize_command_scope(" ", None)


def test_claim_parameters_reject_non_positive_values() -> None:
    assert validate_claim_limit(2) == 2
    assert validate_lease_for(timedelta(seconds=1)) == timedelta(seconds=1)

    with pytest.raises(ValueError, match="limit must be positive"):
        validate_claim_limit(0)
    with pytest.raises(ValueError, match="lease_for must be positive"):
        validate_lease_for(timedelta(0))


def test_unknown_command_projection_resolves_terminal_orders() -> None:
    projection = project_unknown_command(OrderStatus.FILLED)
    assert projection.command_status is TradingCommandStatus.RESOLVED
    assert projection.order_status is None

    projection = project_unknown_command(OrderStatus.ACCEPTED)
    assert projection.command_status is TradingCommandStatus.UNKNOWN
    assert projection.order_status is OrderStatus.UNKNOWN


def test_unknown_command_event_id_is_attempt_specific() -> None:
    assert unknown_command_event_id("submit:order-1", 3) == "command-unknown:submit:order-1:3"
