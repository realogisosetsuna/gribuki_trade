"""确定性的 A 股模拟订单与日线匹配值对象。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from zoneinfo import ZoneInfo

from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.domain.orders import OrderIntent, OrderType, Side
from gribuki_trade.domain.paper_trading import PaperFillReceipt, PaperInstrumentType

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_SYMBOL = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")


class PaperOrderStatus(StrEnum):
    """本地且不依赖券商的模拟订单生命周期状态。"""

    PENDING = "PENDING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class PaperTimeInForce(StrEnum):
    """一个订单最多可经历多少根已确认且可交易的日线柱。"""

    NEXT_TRADING_BAR = "NEXT_TRADING_BAR"
    GOOD_TILL_DATE = "GOOD_TILL_DATE"


class PaperMatchReason(StrEnum):
    """订单快照与运行记录所保留的机器可读说明。"""

    BUY_QUANTITY_NOT_ROUND_LOT = "BUY_QUANTITY_NOT_ROUND_LOT"
    LIMIT_PRICE_NOT_TICK_ALIGNED = "LIMIT_PRICE_NOT_TICK_ALIGNED"
    INSUFFICIENT_CASH_BUDGET = "INSUFFICIENT_CASH_BUDGET"
    INSUFFICIENT_AVAILABLE_POSITION = "INSUFFICIENT_AVAILABLE_POSITION"
    ACCOUNT_SESSION_AHEAD = "ACCOUNT_SESSION_AHEAD"
    ORDER_NOT_YET_ELIGIBLE = "ORDER_NOT_YET_ELIGIBLE"
    ORDER_EXPIRED_BEFORE_BAR = "ORDER_EXPIRED_BEFORE_BAR"
    BAR_SUSPENDED = "BAR_SUSPENDED"
    BAR_OHLC_MISSING = "BAR_OHLC_MISSING"
    BAR_VOLUME_ZERO = "BAR_VOLUME_ZERO"
    PRICE_BAND_MISSING = "PRICE_BAND_MISSING"
    BAR_OUTSIDE_PRICE_BAND = "BAR_OUTSIDE_PRICE_BAND"
    ORDER_LIMIT_OUTSIDE_PRICE_BAND = "ORDER_LIMIT_OUTSIDE_PRICE_BAND"
    LIMIT_NOT_TOUCHED = "LIMIT_NOT_TOUCHED"
    VOLUME_PARTICIPATION_EXHAUSTED = "VOLUME_PARTICIPATION_EXHAUSTED"
    ACCOUNT_LEDGER_REJECTED = "ACCOUNT_LEDGER_REJECTED"
    FILLED = "FILLED"
    PARTIAL_FILL = "PARTIAL_FILL"
    CANCELLED_BY_CALLER = "CANCELLED_BY_CALLER"
    EXPIRED_AFTER_ELIGIBLE_BAR = "EXPIRED_AFTER_ELIGIBLE_BAR"


@dataclass(frozen=True, slots=True)
class ASharePaperOrderIntent:
    """通用限价 ``OrderIntent`` 与 A 股模拟语义的组合。"""

    order: OrderIntent
    instrument_type: PaperInstrumentType
    decision_session_date: date
    time_in_force: PaperTimeInForce = PaperTimeInForce.NEXT_TRADING_BAR
    expires_on: date | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.decision_session_date, date) or isinstance(
            self.decision_session_date, datetime
        ):
            raise TypeError("decision_session_date must be a date")
        if self.order.order_type is not OrderType.LIMIT:
            raise ValueError("A-share paper matching supports LIMIT orders only")
        if self.order.created_at.tzinfo is None or self.order.created_at.utcoffset() is None:
            raise ValueError("order created_at must be timezone-aware")
        if self.order.created_at.astimezone(_SHANGHAI).date() != self.decision_session_date:
            raise ValueError(
                "order created_at Asia/Shanghai date must equal decision_session_date"
            )
        symbol = self.order.symbol.strip().upper()
        if symbol != self.order.symbol or not _SYMBOL.fullmatch(symbol):
            raise ValueError("order symbol must use the canonical 000001.SZ form")
        if len(self.order.client_order_id) > 128:
            raise ValueError("client_order_id must not exceed 128 characters")
        quantity = self.order.quantity
        if quantity != quantity.to_integral_value():
            raise ValueError("A-share quantity must be an integer number of shares")
        try:
            instrument_type = PaperInstrumentType(self.instrument_type)
            time_in_force = PaperTimeInForce(self.time_in_force)
        except ValueError as error:
            raise ValueError("unsupported paper order enum value") from error
        object.__setattr__(self, "instrument_type", instrument_type)
        object.__setattr__(self, "time_in_force", time_in_force)
        if time_in_force is PaperTimeInForce.NEXT_TRADING_BAR:
            if self.expires_on is not None:
                raise ValueError("NEXT_TRADING_BAR orders must not set expires_on")
        elif self.expires_on is None:
            raise ValueError("GOOD_TILL_DATE orders require expires_on")
        elif not isinstance(self.expires_on, date) or isinstance(
            self.expires_on, datetime
        ):
            raise TypeError("expires_on must be a date")
        elif self.expires_on <= self.decision_session_date:
            raise ValueError("expires_on must be later than the decision session")

    @property
    def quantity(self) -> int:
        return int(self.order.quantity)


@dataclass(frozen=True, slots=True)
class ExplicitPriceBand:
    """由调用方提供的每日价格限制；两个 ``None`` 表示不设边界。"""

    lower: Decimal | None
    upper: Decimal | None

    def __post_init__(self) -> None:
        lower = _optional_positive_decimal(self.lower, "lower")
        upper = _optional_positive_decimal(self.upper, "upper")
        if lower is not None and upper is not None and lower > upper:
            raise ValueError("price-band lower must not exceed upper")
        object.__setattr__(self, "lower", lower)
        object.__setattr__(self, "upper", upper)

    @classmethod
    def unbounded(cls) -> ExplicitPriceBand:
        """明确声明该标的没有建模的每日价格区间。"""

        return cls(lower=None, upper=None)

    def contains(self, price: Decimal) -> bool:
        return (self.lower is None or price >= self.lower) and (
            self.upper is None or price <= self.upper
        )


@dataclass(frozen=True, slots=True)
class SimulatedDailyBar:
    """提供给模拟匹配器、感知时点且未复权的日线柱。

    ``price_band=None`` 表示所需涨跌停元数据缺失，并按失败关闭处理。若确实不设
    区间，应使用 :meth:`ExplicitPriceBand.unbounded`。``volume_shares`` 始终以股为
    单位，绝不是手数或成交额。
    """

    symbol: str
    trade_date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume_shares: int
    is_trading: bool
    available_at: datetime
    source_revision: str
    price_band: ExplicitPriceBand | None

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        if symbol != self.symbol or not _SYMBOL.fullmatch(symbol):
            raise ValueError("bar symbol must use the canonical 000001.SZ form")
        if isinstance(self.volume_shares, bool) or not isinstance(self.volume_shares, int):
            raise TypeError("volume_shares must be an integer")
        if self.volume_shares < 0:
            raise ValueError("volume_shares must be non-negative")
        if not isinstance(self.is_trading, bool):
            raise TypeError("is_trading must be bool")
        if self.available_at.tzinfo is None or self.available_at.utcoffset() is None:
            raise ValueError("available_at must be timezone-aware")
        object.__setattr__(self, "available_at", self.available_at.astimezone(UTC))
        if self.available_at.astimezone(_SHANGHAI).date() < self.trade_date:
            raise ValueError("available_at cannot precede the bar's Shanghai date")
        source_revision = self.source_revision.strip()
        if not source_revision:
            raise ValueError("source_revision must not be empty")
        object.__setattr__(self, "source_revision", source_revision)
        prices = (self.open, self.high, self.low, self.close)
        normalized = tuple(
            _optional_positive_decimal(value, "OHLC price") for value in prices
        )
        object.__setattr__(self, "open", normalized[0])
        object.__setattr__(self, "high", normalized[1])
        object.__setattr__(self, "low", normalized[2])
        object.__setattr__(self, "close", normalized[3])
        if all(value is not None for value in normalized):
            open_price = normalized[0]
            high_price = normalized[1]
            low_price = normalized[2]
            close_price = normalized[3]
            assert open_price is not None
            assert high_price is not None
            assert low_price is not None
            assert close_price is not None
            if low_price > high_price:
                raise ValueError("bar low must not exceed high")
            if not low_price <= open_price <= high_price:
                raise ValueError("bar open must be inside low/high")
            if not low_price <= close_price <= high_price:
                raise ValueError("bar close must be inside low/high")

    @classmethod
    def from_unadjusted_daily_bar(
        cls,
        bar: DailyBar,
        *,
        available_at: datetime,
        source_revision: str,
        price_band: ExplicitPriceBand | None,
    ) -> SimulatedDailyBar:
        """适配一根成交量单位为股的严格未复权 ``DailyBar``。"""

        if bar.adjustment is not PriceAdjustment.NONE:
            raise ValueError("paper matching requires original, unadjusted prices")
        return cls(
            symbol=bar.symbol,
            trade_date=bar.trade_date,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume_shares=bar.volume,
            is_trading=bar.is_trading,
            available_at=available_at,
            source_revision=source_revision,
            price_band=price_band,
        )

    @property
    def has_complete_ohlc(self) -> bool:
        return all(
            value is not None for value in (self.open, self.high, self.low, self.close)
        )

    @property
    def ohlc_inside_price_band(self) -> bool:
        if not self.has_complete_ohlc or self.price_band is None:
            return False
        prices = (self.open, self.high, self.low, self.close)
        return all(
            value is not None and self.price_band.contains(value) for value in prices
        )


@dataclass(frozen=True, slots=True)
class PaperMatchingConfig:
    """保守的日线执行假设。"""

    volume_participation_rate: Decimal = Decimal("0.01")
    stock_slippage_rate: Decimal = Decimal("0.0005")
    etf_slippage_rate: Decimal = Decimal("0.0002")
    stock_price_quantum: Decimal = Decimal("0.01")
    etf_price_quantum: Decimal = Decimal("0.001")
    buy_lot_size: int = 100

    def __post_init__(self) -> None:
        rate = _positive_decimal(
            self.volume_participation_rate, "volume_participation_rate"
        )
        if rate > Decimal("0.25"):
            raise ValueError("volume_participation_rate must not exceed 25%")
        object.__setattr__(self, "volume_participation_rate", rate)
        for name in ("stock_slippage_rate", "etf_slippage_rate"):
            value = _non_negative_decimal(getattr(self, name), name)
            if value > Decimal("0.05"):
                raise ValueError(f"{name} must not exceed 5%")
            object.__setattr__(self, name, value)
        for name in ("stock_price_quantum", "etf_price_quantum"):
            object.__setattr__(self, name, _positive_decimal(getattr(self, name), name))
        if isinstance(self.buy_lot_size, bool) or not isinstance(self.buy_lot_size, int):
            raise TypeError("buy_lot_size must be an integer")
        if self.buy_lot_size <= 0:
            raise ValueError("buy_lot_size must be positive")


@dataclass(frozen=True, slots=True)
class PaperOrderSnapshot:
    """单个模拟限价订单的最新确定性状态。"""

    intent: ASharePaperOrderIntent
    status: PaperOrderStatus
    filled_quantity: int
    average_fill_price: Decimal | None
    reserved_cash: Decimal
    reserved_quantity: int
    submitted_at: datetime
    updated_at: datetime
    reason: PaperMatchReason | None = None
    last_processed_session: date | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", PaperOrderStatus(self.status))
        if self.filled_quantity < 0 or self.filled_quantity > self.intent.quantity:
            raise ValueError("filled_quantity is outside order bounds")
        average = _optional_positive_decimal(
            self.average_fill_price, "average_fill_price"
        )
        if (self.filled_quantity == 0) != (average is None):
            raise ValueError("average_fill_price must exist exactly when quantity is filled")
        object.__setattr__(self, "average_fill_price", average)
        reserved_cash = _non_negative_decimal(self.reserved_cash, "reserved_cash")
        object.__setattr__(self, "reserved_cash", reserved_cash)
        if self.reserved_quantity < 0:
            raise ValueError("reserved_quantity must be non-negative")
        submitted_at = _aware_utc(self.submitted_at, "submitted_at")
        updated_at = _aware_utc(self.updated_at, "updated_at")
        if updated_at < submitted_at:
            raise ValueError("updated_at must not precede submitted_at")
        object.__setattr__(self, "submitted_at", submitted_at)
        object.__setattr__(self, "updated_at", updated_at)
        if self.reason is not None:
            object.__setattr__(self, "reason", PaperMatchReason(self.reason))
        if self.status is PaperOrderStatus.PENDING and self.filled_quantity != 0:
            raise ValueError("PENDING order cannot have fills")
        if self.status is PaperOrderStatus.PARTIALLY_FILLED and not (
            0 < self.filled_quantity < self.intent.quantity
        ):
            raise ValueError("PARTIALLY_FILLED order requires an incomplete fill")
        if (
            self.status is PaperOrderStatus.FILLED
            and self.filled_quantity != self.intent.quantity
        ):
            raise ValueError("FILLED order quantity must equal intent quantity")
        if not self.is_open and (self.reserved_cash != 0 or self.reserved_quantity != 0):
            raise ValueError("terminal orders cannot retain reservations")
        if self.intent.order.side is Side.BUY and self.reserved_quantity != 0:
            raise ValueError("BUY order cannot reserve shares")
        if self.intent.order.side is Side.SELL and self.reserved_cash != 0:
            raise ValueError("SELL order cannot reserve cash")

    @property
    def remaining_quantity(self) -> int:
        return self.intent.quantity - self.filled_quantity

    @property
    def is_open(self) -> bool:
        return self.status in {
            PaperOrderStatus.PENDING,
            PaperOrderStatus.PARTIALLY_FILLED,
        }


@dataclass(frozen=True, slots=True)
class PaperOrderMatchOutcome:
    """单个订单处理一根日线柱后的结果。"""

    client_order_id: str
    status_before: PaperOrderStatus
    status_after: PaperOrderStatus
    reason: PaperMatchReason
    filled_quantity: int = 0
    fill_price: Decimal | None = None
    receipt: PaperFillReceipt | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "status_before", PaperOrderStatus(self.status_before))
        object.__setattr__(self, "status_after", PaperOrderStatus(self.status_after))
        object.__setattr__(self, "reason", PaperMatchReason(self.reason))
        if self.filled_quantity < 0:
            raise ValueError("filled_quantity must be non-negative")
        fill_price = _optional_positive_decimal(self.fill_price, "fill_price")
        if (self.filled_quantity == 0) != (fill_price is None):
            raise ValueError("fill_price must exist exactly when shares filled")
        if (self.filled_quantity == 0) != (self.receipt is None):
            raise ValueError("receipt must exist exactly when shares filled")
        object.__setattr__(self, "fill_price", fill_price)


@dataclass(frozen=True, slots=True)
class PaperBarMatchRun:
    """单个标的/交易日日线柱的幂等结果。"""

    bar: SimulatedDailyBar
    applied_new: bool
    volume_capacity: int
    consumed_volume: int
    outcomes: tuple[PaperOrderMatchOutcome, ...]

    def __post_init__(self) -> None:
        if self.volume_capacity < 0 or self.consumed_volume < 0:
            raise ValueError("bar volumes must be non-negative")
        if self.consumed_volume > self.volume_capacity:
            raise ValueError("consumed_volume cannot exceed volume_capacity")
        order_ids = tuple(outcome.client_order_id for outcome in self.outcomes)
        if len(order_ids) != len(set(order_ids)):
            raise ValueError("bar outcomes must have unique client_order_id values")


def _optional_positive_decimal(value: object, name: str) -> Decimal | None:
    if value is None:
        return None
    return _positive_decimal(value, name)


def _positive_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return value


def _non_negative_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError(f"{name} must be non-negative and finite")
    return value


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)
