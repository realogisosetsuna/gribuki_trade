"""Binance 现货 REST 网关返回的公开值对象。"""

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
    """不会通过 ``repr`` 泄露真实值的 HMAC 凭据。"""

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
class BinanceSpotOrderLeg:
    """一条现货订单腿，可复用于单笔条件单、OCO、OTO 和 OTOCO。"""

    order_type: str
    side: Side
    quantity: Decimal
    price: Decimal | None = None
    stop_price: Decimal | None = None
    trailing_delta: int | None = None
    time_in_force: str | None = None
    quote_order_quantity: Decimal | None = None
    iceberg_quantity: Decimal | None = None
    client_order_id: str | None = None
    strategy_id: int | None = None
    strategy_type: int | None = None
    self_trade_prevention_mode: str | None = None


@dataclass(frozen=True, slots=True)
class BinanceSpotOcoRequest:
    """现货 OCO 请求；above/below 两腿共享数量。"""

    symbol: str
    side: Side
    quantity: Decimal
    above: BinanceSpotOrderLeg
    below: BinanceSpotOrderLeg
    list_client_order_id: str | None = None
    response_type: str = "RESULT"


@dataclass(frozen=True, slots=True)
class BinanceSpotOtoRequest:
    """现货 OTO 请求：working 完全成交后激活 pending。"""

    symbol: str
    working: BinanceSpotOrderLeg
    pending: BinanceSpotOrderLeg
    list_client_order_id: str | None = None
    response_type: str = "RESULT"


@dataclass(frozen=True, slots=True)
class BinanceSpotOtocoRequest:
    """现货 OTOCO 请求：working 完全成交后激活 pending OCO。"""

    symbol: str
    working: BinanceSpotOrderLeg
    pending_above: BinanceSpotOrderLeg
    pending_below: BinanceSpotOrderLeg
    list_client_order_id: str | None = None
    response_type: str = "RESULT"


@dataclass(frozen=True, slots=True)
class BinanceOrderListSnapshot:
    """Binance 条件订单列表状态和其中的订单快照。"""

    order_list_id: int | None
    contingency_type: str | None
    list_status_type: str | None
    list_order_status: str | None
    list_client_order_id: str | None
    symbol: str
    orders: tuple[BinanceOrderSnapshot, ...]
    transaction_time_ms: int | None = None


@dataclass(frozen=True, slots=True)
class BinanceCancelReplaceResult:
    """现货 cancel-replace 的两个独立结果，供动态止盈止损对账。"""

    cancel_result: str | None
    new_order_result: str | None
    cancel_response: BinanceOrderSnapshot | None
    new_order_response: BinanceOrderSnapshot | None


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
    """由 ``/api/v3/myTrades`` 返回的一条不可变现货账户成交。"""

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
    """某个标的当前账户专属的现货费率配置。"""

    symbol: str
    standard: BinanceCommissionComponent
    tax: BinanceCommissionComponent
    special: BinanceCommissionComponent
    discount: BinanceCommissionDiscount


@dataclass(frozen=True, slots=True)
class BinanceRateLimitUsage:
    """REST 适配器最近一次观察到的限频头信息。"""

    used_weight_1m: int | None = None
    order_count_10s: int | None = None
    order_count_1d: int | None = None
    retry_after_seconds: int | None = None


@dataclass(frozen=True, slots=True)
class BinanceOrderUpdate:
    """本地提交的 Binance 订单所对应的券商事件载荷。"""

    order: OrderIntent
    status: OrderStatus
    exchange_order_id: int | None = None
    executed_quantity: Decimal = Decimal("0")
    reason: str | None = None
    error_code: int | None = None
    occurred_at: datetime | None = None
