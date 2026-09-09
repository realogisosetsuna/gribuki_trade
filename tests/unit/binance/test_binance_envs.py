from types import MappingProxyType
from unittest import TestCase

from gribuki_trade.adapters.binance.auth.envs import (
    BINANCE_ENVIRONMENT_PROFILES,
    BinanceCapability,
    BinanceProduct,
    BinanceStage,
    UnsupportedBinanceEnvironment,
    binance_environment,
)


class BinanceEnvironmentTests(TestCase):
    def test_official_non_production_hosts_are_explicit(self) -> None:
        spot_testnet = binance_environment(BinanceProduct.SPOT, BinanceStage.TESTNET)
        spot_demo = binance_environment(BinanceProduct.SPOT, BinanceStage.DEMO)
        usds_demo = binance_environment(BinanceProduct.USDS_FUTURES, BinanceStage.DEMO)
        coin_demo = binance_environment(BinanceProduct.COIN_FUTURES, BinanceStage.DEMO)

        self.assertEqual(spot_testnet.rest_base_url, "https://testnet.binance.vision")
        self.assertEqual(spot_demo.rest_base_url, "https://demo-api.binance.com")
        self.assertEqual(usds_demo.rest_base_url, "https://demo-fapi.binance.com")
        self.assertEqual(coin_demo.rest_base_url, "https://demo-dapi.binance.com")
        self.assertTrue(usds_demo.supports(BinanceCapability.FUTURES))
        self.assertTrue(spot_demo.supports(BinanceCapability.REALISTIC_MARKET_SIMULATION))

    def test_margin_and_portfolio_margin_have_no_documented_nonprod_endpoint(self) -> None:
        for product in (BinanceProduct.MARGIN, BinanceProduct.PORTFOLIO_MARGIN):
            for stage in (BinanceStage.TESTNET, BinanceStage.DEMO):
                with self.subTest(product=product, stage=stage), self.assertRaisesRegex(
                    UnsupportedBinanceEnvironment, "refusing to fall back to LIVE"
                ):
                    binance_environment(product, stage)

    def test_old_generic_futures_testnet_name_does_not_guess_a_host(self) -> None:
        with self.assertRaisesRegex(
            UnsupportedBinanceEnvironment, "published as Demo Trading"
        ):
            binance_environment(BinanceProduct.USDS_FUTURES, BinanceStage.TESTNET)

    def test_profile_map_is_read_only(self) -> None:
        self.assertIsInstance(BINANCE_ENVIRONMENT_PROFILES, MappingProxyType)
