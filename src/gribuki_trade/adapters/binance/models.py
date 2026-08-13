"""Public value objects returned by the Binance Spot REST gateway."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side

LIVE_REST_BASE_URL = "https://api.binance.com"
TESTNET_REST_BASE_URL = "https://testnet.binance.vision"
ORDER_STATUS_EVENT = "ORDER_STATUS"


class BinanceEnvironment(StrEnum):
    TESTNET = "TESTNET"
    LIVE = "LIVE"

    @property
    def base_url(self) -> str:
        if self is BinanceEnvironment.LIVE:
            return LIVE_REST_BASE_URL
        return TESTNET_REST_BASE_URL


@dataclass(frozen=True, slots=True)
class BinanceCredentials:
    """HMAC credentials whose values cannot leak through ``repr``."""

    api_key: str = field(repr=False)
    secret_key: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self.api_key.strip() or not self.secret_key.strip():
            raise ValueError("Binance API credentials must not be blank")

    def __repr__(self) -> str:
        return "BinanceCredentials(api_key=<redacted>, secret_key=<redacted>)"


@dataclass(frozen=True, slots=True)
class TickerPrice:
    symbol: str
    price: Decimal


@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    symbol: str
    last_update_id: int
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]


@dataclass(frozen=True, slots=True)
class Kline:
    open_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    close_time_ms: int
    quote_volume: Decimal
    trade_count: int
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal


@dataclass(frozen=True, slots=True)
class BinanceBalance:
    asset: str
    free: Decimal
    locked: Decimal


@dataclass(frozen=True, slots=True)
class BinanceAccount:
    can_trade: bool
    can_withdraw: bool
    can_deposit: bool
    account_type: str
    balances: tuple[BinanceBalance, ...]
    update_time_ms: int | None = None
    permissions: tuple[str, ...] = ()
    uid: int | None = None
    maker_commission: int | None = None
    taker_commission: int | None = None
    buyer_commission: int | None = None
    seller_commission: int | None = None
    brokered: bool = False
    require_self_trade_prevention: bool = False
    prevent_sor: bool = False


@dataclass(frozen=True, slots=True)
class BinanceOrderSnapshot:
    symbol: str
    client_order_id: str | None
    order_id: int | None
    status: OrderStatus
    exchange_status: str | None
    side: Side | None
    price: Decimal | None
    original_quantity: Decimal | None
    executed_quantity: Decimal
    transact_time_ms: int | None = None
    cumulative_quote_quantity: Decimal | None = None


@dataclass(frozen=True, slots=True)
class BinanceTrade:
    """One immutable Spot account trade returned by ``/api/v3/myTrades``."""

    symbol: str
    trade_id: int
    order_id: int
    price: Decimal
    quantity: Decimal
    quote_quantity: Decimal
    commission: Decimal
    commission_asset: str
    time_ms: int
    is_buyer: bool
    is_maker: bool
    is_best_match: bool


@dataclass(frozen=True, slots=True)
class BinanceCommissionComponent:
    maker: Decimal
    taker: Decimal
    buyer: Decimal
    seller: Decimal


@dataclass(frozen=True, slots=True)
class BinanceCommissionDiscount:
    enabled_for_account: bool
    enabled_for_symbol: bool
    asset: str
    discount: Decimal


@dataclass(frozen=True, slots=True)
class BinanceCommissionRate:
    """Current account-specific Spot commission configuration for a symbol."""

    symbol: str
    standard: BinanceCommissionComponent
    tax: BinanceCommissionComponent
    special: BinanceCommissionComponent
    discount: BinanceCommissionDiscount


@dataclass(frozen=True, slots=True)
class BinanceRateLimitUsage:
    """Latest rate-limit headers observed by the REST adapter."""

    used_weight_1m: int | None = None
    order_count_10s: int | None = None
    order_count_1d: int | None = None
    retry_after_seconds: int | None = None


@dataclass(frozen=True, slots=True)
class BinanceOrderUpdate:
    """Broker-event payload for a locally submitted Binance order."""

    order: OrderIntent
    status: OrderStatus
    exchange_order_id: int | None = None
    executed_quantity: Decimal = Decimal("0")
    reason: str | None = None
    occurred_at: datetime | None = None
