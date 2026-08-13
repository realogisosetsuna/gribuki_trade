"""Explicit Binance Global product/environment capability matrix.

Binance does not expose one interchangeable test environment for every
product.  Keeping the official endpoints and capabilities in one immutable
matrix prevents an unsupported non-production request from silently falling
back to a live host.

The matrix was verified against the official Binance developer documentation
and ``binance/binance-spot-api-docs`` on 2026-08-13.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Final


class BinanceProduct(StrEnum):
    """Binance Global trading product families relevant to this adapter."""

    SPOT = "SPOT"
    MARGIN = "MARGIN"
    USDS_FUTURES = "USDS_FUTURES"
    COIN_FUTURES = "COIN_FUTURES"
    PORTFOLIO_MARGIN = "PORTFOLIO_MARGIN"


class BinanceStage(StrEnum):
    """Officially distinct Binance execution environments."""

    LIVE = "LIVE"
    TESTNET = "TESTNET"
    DEMO = "DEMO"


class BinanceCapability(StrEnum):
    """Coarse capabilities that are safe to infer from official docs."""

    PUBLIC_REST = "PUBLIC_REST"
    SIGNED_REST = "SIGNED_REST"
    TRADING = "TRADING"
    ORDER_TEST = "ORDER_TEST"
    MARKET_STREAM = "MARKET_STREAM"
    USER_DATA_STREAM = "USER_DATA_STREAM"
    WEBSOCKET_API = "WEBSOCKET_API"
    FIX = "FIX"
    SPOT = "SPOT"
    MARGIN = "MARGIN"
    FUTURES = "FUTURES"
    PORTFOLIO_MARGIN = "PORTFOLIO_MARGIN"
    REALISTIC_MARKET_SIMULATION = "REALISTIC_MARKET_SIMULATION"


class UnsupportedBinanceEnvironment(ValueError):
    """A product has no officially documented endpoint for the requested stage."""


@dataclass(frozen=True, slots=True)
class BinanceEnvironmentProfile:
    """One explicit product/environment routing and capability record."""

    product: BinanceProduct
    stage: BinanceStage
    rest_base_url: str
    api_prefix: str
    market_ws_base_url: str | None
    websocket_api_url: str | None
    capabilities: frozenset[BinanceCapability]
    label: str

    @property
    def is_live(self) -> bool:
        return self.stage is BinanceStage.LIVE

    def supports(self, capability: BinanceCapability) -> bool:
        return capability in self.capabilities


_SPOT_OR_FUTURES_EXECUTION: Final = frozenset(
    {
        BinanceCapability.PUBLIC_REST,
        BinanceCapability.SIGNED_REST,
        BinanceCapability.TRADING,
        BinanceCapability.ORDER_TEST,
        BinanceCapability.MARKET_STREAM,
        BinanceCapability.USER_DATA_STREAM,
    }
)


def _profile(
    product: BinanceProduct,
    stage: BinanceStage,
    rest_base_url: str,
    api_prefix: str,
    market_ws_base_url: str | None,
    *,
    websocket_api_url: str | None = None,
    capabilities: frozenset[BinanceCapability],
    label: str,
) -> BinanceEnvironmentProfile:
    return BinanceEnvironmentProfile(
        product=product,
        stage=stage,
        rest_base_url=rest_base_url,
        api_prefix=api_prefix,
        market_ws_base_url=market_ws_base_url,
        websocket_api_url=websocket_api_url,
        capabilities=capabilities,
        label=label,
    )


_PROFILES: dict[tuple[BinanceProduct, BinanceStage], BinanceEnvironmentProfile] = {
    (BinanceProduct.SPOT, BinanceStage.LIVE): _profile(
        BinanceProduct.SPOT,
        BinanceStage.LIVE,
        "https://api.binance.com",
        "api",
        "wss://stream.binance.com:9443",
        websocket_api_url="wss://ws-api.binance.com:443/ws-api/v3",
        capabilities=_SPOT_OR_FUTURES_EXECUTION
        | {
            BinanceCapability.SPOT,
            BinanceCapability.WEBSOCKET_API,
            BinanceCapability.FIX,
        },
        label="Binance Global Spot live",
    ),
    (BinanceProduct.SPOT, BinanceStage.TESTNET): _profile(
        BinanceProduct.SPOT,
        BinanceStage.TESTNET,
        "https://testnet.binance.vision",
        "api",
        "wss://stream.testnet.binance.vision",
        websocket_api_url="wss://ws-api.testnet.binance.vision/ws-api/v3",
        capabilities=_SPOT_OR_FUTURES_EXECUTION
        | {
            BinanceCapability.SPOT,
            BinanceCapability.WEBSOCKET_API,
            BinanceCapability.FIX,
        },
        label="Binance Global Spot Test Network",
    ),
    (BinanceProduct.SPOT, BinanceStage.DEMO): _profile(
        BinanceProduct.SPOT,
        BinanceStage.DEMO,
        "https://demo-api.binance.com",
        "api",
        "wss://demo-stream.binance.com",
        websocket_api_url="wss://demo-ws-api.binance.com/ws-api/v3",
        capabilities=_SPOT_OR_FUTURES_EXECUTION
        | {
            BinanceCapability.SPOT,
            BinanceCapability.WEBSOCKET_API,
            BinanceCapability.FIX,
            BinanceCapability.REALISTIC_MARKET_SIMULATION,
        },
        label="Binance Global Spot Demo Mode",
    ),
    (BinanceProduct.MARGIN, BinanceStage.LIVE): _profile(
        BinanceProduct.MARGIN,
        BinanceStage.LIVE,
        "https://api.binance.com",
        "sapi",
        "wss://stream.binance.com:9443",
        capabilities=frozenset(
            {
                BinanceCapability.PUBLIC_REST,
                BinanceCapability.SIGNED_REST,
                BinanceCapability.TRADING,
                BinanceCapability.MARKET_STREAM,
                BinanceCapability.USER_DATA_STREAM,
                BinanceCapability.SPOT,
                BinanceCapability.MARGIN,
            }
        ),
        label="Binance Global Margin live",
    ),
    (BinanceProduct.USDS_FUTURES, BinanceStage.LIVE): _profile(
        BinanceProduct.USDS_FUTURES,
        BinanceStage.LIVE,
        "https://fapi.binance.com",
        "fapi",
        "wss://fstream.binance.com",
        capabilities=_SPOT_OR_FUTURES_EXECUTION | {BinanceCapability.FUTURES},
        label="Binance Global USD-M Futures live",
    ),
    (BinanceProduct.USDS_FUTURES, BinanceStage.DEMO): _profile(
        BinanceProduct.USDS_FUTURES,
        BinanceStage.DEMO,
        "https://demo-fapi.binance.com",
        "fapi",
        "wss://demo-fstream.binance.com",
        capabilities=_SPOT_OR_FUTURES_EXECUTION
        | {
            BinanceCapability.FUTURES,
            BinanceCapability.REALISTIC_MARKET_SIMULATION,
        },
        label="Binance Global USD-M Futures Testnet / Demo Trading",
    ),
    (BinanceProduct.COIN_FUTURES, BinanceStage.LIVE): _profile(
        BinanceProduct.COIN_FUTURES,
        BinanceStage.LIVE,
        "https://dapi.binance.com",
        "dapi",
        "wss://dstream.binance.com",
        capabilities=_SPOT_OR_FUTURES_EXECUTION | {BinanceCapability.FUTURES},
        label="Binance Global COIN-M Futures live",
    ),
    (BinanceProduct.COIN_FUTURES, BinanceStage.DEMO): _profile(
        BinanceProduct.COIN_FUTURES,
        BinanceStage.DEMO,
        "https://demo-dapi.binance.com",
        "dapi",
        "wss://demo-dstream.binance.com",
        capabilities=_SPOT_OR_FUTURES_EXECUTION
        | {
            BinanceCapability.FUTURES,
            BinanceCapability.REALISTIC_MARKET_SIMULATION,
        },
        label="Binance Global COIN-M Futures Testnet / Demo Trading",
    ),
    (BinanceProduct.PORTFOLIO_MARGIN, BinanceStage.LIVE): _profile(
        BinanceProduct.PORTFOLIO_MARGIN,
        BinanceStage.LIVE,
        "https://papi.binance.com",
        "papi",
        None,
        capabilities=frozenset(
            {
                BinanceCapability.SIGNED_REST,
                BinanceCapability.TRADING,
                BinanceCapability.USER_DATA_STREAM,
                BinanceCapability.FUTURES,
                BinanceCapability.PORTFOLIO_MARGIN,
            }
        ),
        label="Binance Global Portfolio Margin live",
    ),
}

BINANCE_ENVIRONMENT_PROFILES: Final[
    Mapping[tuple[BinanceProduct, BinanceStage], BinanceEnvironmentProfile]
] = MappingProxyType(_PROFILES)


def _coerce_product(product: BinanceProduct | str) -> BinanceProduct:
    if isinstance(product, BinanceProduct):
        return product
    return BinanceProduct(str(product).upper().replace("-", "_"))


def _coerce_stage(stage: BinanceStage | str) -> BinanceStage:
    if isinstance(stage, BinanceStage):
        return stage
    return BinanceStage(str(stage).upper())


def binance_environment(
    product: BinanceProduct | str,
    stage: BinanceStage | str,
) -> BinanceEnvironmentProfile:
    """Return an exact official profile; never substitute a live endpoint."""

    try:
        selected_product = _coerce_product(product)
        selected_stage = _coerce_stage(stage)
    except ValueError as exc:
        raise UnsupportedBinanceEnvironment(
            f"unknown Binance product/environment: {product!r}/{stage!r}"
        ) from exc

    profile = BINANCE_ENVIRONMENT_PROFILES.get((selected_product, selected_stage))
    if profile is not None:
        return profile

    if selected_product is BinanceProduct.MARGIN and selected_stage is not BinanceStage.LIVE:
        detail = "Margin uses /sapi endpoints, which Spot Testnet does not support"
    elif (
        selected_product is BinanceProduct.PORTFOLIO_MARGIN
        and selected_stage is not BinanceStage.LIVE
    ):
        detail = "Binance documents only the live papi.binance.com endpoint"
    elif (
        selected_product in {BinanceProduct.USDS_FUTURES, BinanceProduct.COIN_FUTURES}
        and selected_stage is BinanceStage.TESTNET
    ):
        detail = "current Futures non-production endpoints are published as Demo Trading"
    else:
        detail = "no official endpoint is documented for this combination"
    raise UnsupportedBinanceEnvironment(
        f"Binance {selected_product.value} {selected_stage.value} is unsupported: "
        f"{detail}; refusing to fall back to LIVE"
    )
