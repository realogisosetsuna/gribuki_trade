from gribuki_trade.adapters.binance import (
    BINANCE_COIN_FUTURES_DEMO_API_KEY_SECRET,
    BINANCE_LIVE_API_KEY_SECRET,
    BINANCE_TESTNET_API_KEY_SECRET,
    BINANCE_USDS_FUTURES_DEMO_API_KEY_SECRET,
    BinanceConfigurationError,
    BinanceEnvironment,
    BinanceProduct,
    binance_futures_demo_secret_names,
    load_binance_credentials,
    load_binance_futures_demo_credentials,
)
from gribuki_trade.security import MemorySecretProvider


def test_testnet_credentials_are_loaded_without_live_fallback() -> None:
    provider = MemorySecretProvider(
        {
            "binance.testnet.api_key": "test-key",
            "binance.testnet.secret_key": "test-secret",
            "binance.live.api_key": "live-key",
            "binance.live.secret_key": "live-secret",
        }
    )

    credentials = load_binance_credentials(provider)

    assert credentials.api_key == "test-key"
    assert credentials.secret_key == "test-secret"
    assert "test-key" not in repr(credentials)


def test_live_and_testnet_use_distinct_required_names() -> None:
    provider = MemorySecretProvider(
        {
            "binance.testnet.api_key": "test-key",
            "binance.testnet.secret_key": "test-secret",
        }
    )

    try:
        load_binance_credentials(provider, BinanceEnvironment.LIVE)
    except BinanceConfigurationError as error:
        assert BINANCE_LIVE_API_KEY_SECRET in str(error)
        assert BINANCE_TESTNET_API_KEY_SECRET not in str(error)
    else:
        raise AssertionError("LIVE credentials unexpectedly fell back to Testnet")


def test_futures_demo_credentials_are_strictly_product_separated() -> None:
    provider = MemorySecretProvider(
        {
            "binance.usds_futures.demo.api_key": "usds-key",
            "binance.usds_futures.demo.secret_key": "usds-secret",
            "binance.coin_futures.demo.api_key": "coin-key",
            "binance.coin_futures.demo.secret_key": "coin-secret",
            "binance.testnet.api_key": "spot-key",
            "binance.testnet.secret_key": "spot-secret",
        }
    )

    usds = load_binance_futures_demo_credentials(
        provider, BinanceProduct.USDS_FUTURES
    )
    coin = load_binance_futures_demo_credentials(provider, "COIN_FUTURES")

    assert usds.api_key == "usds-key"
    assert coin.api_key == "coin-key"
    assert usds.secret_key != coin.secret_key
    assert binance_futures_demo_secret_names("USDS_FUTURES").api_key == (
        BINANCE_USDS_FUTURES_DEMO_API_KEY_SECRET
    )
    assert binance_futures_demo_secret_names("COIN_FUTURES").api_key == (
        BINANCE_COIN_FUTURES_DEMO_API_KEY_SECRET
    )


def test_futures_demo_credentials_never_fall_back_to_other_product_or_spot() -> None:
    provider = MemorySecretProvider(
        {
            "binance.usds_futures.demo.api_key": "usds-key",
            "binance.usds_futures.demo.secret_key": "usds-secret",
            "binance.testnet.api_key": "spot-key",
            "binance.testnet.secret_key": "spot-secret",
        }
    )

    try:
        load_binance_futures_demo_credentials(provider, BinanceProduct.COIN_FUTURES)
    except BinanceConfigurationError as error:
        assert BINANCE_COIN_FUTURES_DEMO_API_KEY_SECRET in str(error)
        assert BINANCE_USDS_FUTURES_DEMO_API_KEY_SECRET not in str(error)
        assert BINANCE_TESTNET_API_KEY_SECRET not in str(error)
    else:
        raise AssertionError("COIN-M credentials unexpectedly fell back to another pair")


def test_futures_demo_credential_loader_rejects_non_futures_product() -> None:
    try:
        load_binance_futures_demo_credentials(
            MemorySecretProvider(), BinanceProduct.SPOT
        )
    except BinanceConfigurationError as error:
        assert "only USDS_FUTURES or COIN_FUTURES" in str(error)
    else:
        raise AssertionError("Spot was unexpectedly accepted as Futures Demo")
