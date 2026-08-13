"""Environment-separated Binance credentials resolved from a secret provider."""

from __future__ import annotations

from dataclasses import dataclass

from gribuki_trade.security import SecretProvider

from .envs import BinanceProduct
from .gateway import BinanceConfigurationError
from .models import BinanceCredentials, BinanceEnvironment

BINANCE_TESTNET_API_KEY_SECRET = "binance.testnet.api_key"
BINANCE_TESTNET_SECRET_KEY_SECRET = "binance.testnet.secret_key"
BINANCE_LIVE_API_KEY_SECRET = "binance.live.api_key"
BINANCE_LIVE_SECRET_KEY_SECRET = "binance.live.secret_key"
BINANCE_USDS_FUTURES_DEMO_API_KEY_SECRET = "binance.usds_futures.demo.api_key"
BINANCE_USDS_FUTURES_DEMO_SECRET_KEY_SECRET = (
    "binance.usds_futures.demo.secret_key"
)
BINANCE_COIN_FUTURES_DEMO_API_KEY_SECRET = "binance.coin_futures.demo.api_key"
BINANCE_COIN_FUTURES_DEMO_SECRET_KEY_SECRET = (
    "binance.coin_futures.demo.secret_key"
)


@dataclass(frozen=True, slots=True)
class BinanceSecretNames:
    api_key: str
    secret_key: str


def binance_secret_names(
    environment: BinanceEnvironment | str,
) -> BinanceSecretNames:
    """Return the exact secret names for one environment without fallback."""

    try:
        selected = (
            environment
            if isinstance(environment, BinanceEnvironment)
            else BinanceEnvironment(str(environment).upper())
        )
    except ValueError:
        raise BinanceConfigurationError(
            f"unknown Binance environment: {environment!r}"
        ) from None
    if selected is BinanceEnvironment.LIVE:
        return BinanceSecretNames(
            BINANCE_LIVE_API_KEY_SECRET,
            BINANCE_LIVE_SECRET_KEY_SECRET,
        )
    return BinanceSecretNames(
        BINANCE_TESTNET_API_KEY_SECRET,
        BINANCE_TESTNET_SECRET_KEY_SECRET,
    )


def binance_futures_demo_secret_names(
    product: BinanceProduct | str,
) -> BinanceSecretNames:
    """Return one Futures product's Demo credentials without cross-product fallback."""

    try:
        selected = (
            product
            if isinstance(product, BinanceProduct)
            else BinanceProduct(str(product).upper().replace("-", "_"))
        )
    except ValueError:
        raise BinanceConfigurationError(
            f"unknown Binance Futures product: {product!r}"
        ) from None
    if selected is BinanceProduct.USDS_FUTURES:
        return BinanceSecretNames(
            BINANCE_USDS_FUTURES_DEMO_API_KEY_SECRET,
            BINANCE_USDS_FUTURES_DEMO_SECRET_KEY_SECRET,
        )
    if selected is BinanceProduct.COIN_FUTURES:
        return BinanceSecretNames(
            BINANCE_COIN_FUTURES_DEMO_API_KEY_SECRET,
            BINANCE_COIN_FUTURES_DEMO_SECRET_KEY_SECRET,
        )
    raise BinanceConfigurationError(
        "Binance Futures Demo credentials support only USDS_FUTURES or COIN_FUTURES"
    )


def _load_credentials(
    provider: SecretProvider,
    names: BinanceSecretNames,
) -> BinanceCredentials:
    api_key = provider.get_secret(names.api_key)
    secret_key = provider.get_secret(names.secret_key)
    missing = [
        name
        for name, value in ((names.api_key, api_key), (names.secret_key, secret_key))
        if not value
    ]
    if missing:
        raise BinanceConfigurationError(
            "missing Binance credential secret(s): " + ", ".join(missing)
        )
    assert api_key is not None and secret_key is not None
    return BinanceCredentials(api_key=api_key, secret_key=secret_key)


def load_binance_credentials(
    provider: SecretProvider,
    environment: BinanceEnvironment | str = BinanceEnvironment.TESTNET,
) -> BinanceCredentials:
    """Resolve one environment's key pair from the configured secret provider.

    Testnet credentials never fall through to LIVE names and vice versa.  This
    prevents a configuration typo from sending a signed request to the wrong
    environment.
    """

    return _load_credentials(provider, binance_secret_names(environment))


def load_binance_futures_demo_credentials(
    provider: SecretProvider,
    product: BinanceProduct | str,
) -> BinanceCredentials:
    """Load exactly one Futures Demo key pair, never Spot, LIVE, or another product."""

    return _load_credentials(provider, binance_futures_demo_secret_names(product))
