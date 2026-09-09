from gribuki_trade.adapters.binance.errors import (
    BinanceAPIError,
    BinanceError,
    BinanceUncertainResultError,
)
from gribuki_trade.adapters.binance.gateway import BinanceAPIError as GatewayAPIError
from gribuki_trade.adapters.binance.gateway import BinanceError as GatewayError
from gribuki_trade.adapters.binance.models import BinanceRateLimitUsage
from gribuki_trade.adapters.binance.rate_limit import parse_rate_limit_usage


def test_gateway_error_exports_are_compatible_with_pure_error_module() -> None:
    assert GatewayError is BinanceError
    assert GatewayAPIError is BinanceAPIError


def test_api_error_preserves_safe_fields_and_uncertain_subtype() -> None:
    error = BinanceAPIError(status_code=429, code=-1003, message="too many requests")
    assert error.status_code == 429
    assert error.code == -1003
    assert error.message == "too many requests"
    assert str(error) == "Binance API error (HTTP 429, code -1003): too many requests"

    uncertain = BinanceUncertainResultError(status_code=504, code=-1007, message="timeout")
    assert isinstance(uncertain, BinanceAPIError)
    assert uncertain.code == -1007


def test_rate_limit_parser_keeps_prior_counters_when_headers_are_partial() -> None:
    prior = BinanceRateLimitUsage(
        used_weight_1m=18,
        order_count_10s=2,
        order_count_1d=31,
        retry_after_seconds=4,
    )
    parsed = parse_rate_limit_usage(
        {
            "X-MBX-USED-WEIGHT-1M": "22",
            "Retry-After": "7",
            "X-MBX-ORDER-COUNT-10S": "invalid",
            "X-MBX-ORDER-COUNT-1D": "-1",
        },
        prior=prior,
    )
    assert parsed.used_weight_1m == 22
    assert parsed.order_count_10s == 2
    assert parsed.order_count_1d == 31
    assert parsed.retry_after_seconds == 7


def test_rate_limit_parser_returns_empty_counters_without_prior() -> None:
    parsed = parse_rate_limit_usage({"X-MBX-USED-WEIGHT-1M": "not-a-number"})
    assert parsed == BinanceRateLimitUsage()
