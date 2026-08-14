"""持久化订单管理与交易账本值对象。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side


class TradingCommandType(StrEnum):
    """可能跨越进程或交易所边界的命令。"""

    SUBMIT_ORDER = "SUBMIT_ORDER"
    CANCEL_ORDER = "CANCEL_ORDER"


class TradingCommandStatus(StrEnum):
    """交易命令的持久化投递状态。

    ``UNKNOWN`` 被刻意设计为不可重试。OMS 必须先按 ``client_order_id`` 与交易所对账，
    才能解决该命令。
    """

    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    SENT = "SENT"
    UNKNOWN = "UNKNOWN"
    RESOLVED = "RESOLVED"


@dataclass(frozen=True, slots=True)
class OrderSnapshot:
    """由仅追加事件重建的最新物化状态。"""

    order: OrderIntent
    status: OrderStatus
    filled_quantity: Decimal
    average_fill_price: Decimal | None
    exchange_order_id: str | None
    reason: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class OrderEventRecord:
    """按摄取顺序排列的一条不可变订单事件。"""

    sequence: int
    event_id: str
    client_order_id: str
    event_type: str
    status: OrderStatus | None
    occurred_at: datetime
    payload_json: str
    applied: bool


@dataclass(frozen=True, slots=True)
class TradingCommand:
    """事务性发件箱中的一条持久化券商命令。"""

    id: int
    command_id: str
    command_type: TradingCommandType
    client_order_id: str
    payload_json: str
    created_at: datetime
    status: TradingCommandStatus
    attempt_count: int
    lease_until: datetime | None
    last_error_code: str | None
    dispatched_at: datetime | None


@dataclass(frozen=True, slots=True)
class ExecutionFill:
    """供 PAPER、Testnet 与实时账本使用的一条不可变成交记录。"""

    fill_id: str
    client_order_id: str
    account_id: str
    symbol: str
    side: Side
    quantity: Decimal
    price: Decimal
    occurred_at: datetime
    fee_asset: str | None = None
    fee_amount: Decimal = Decimal("0")
    exchange_order_id: str | None = None

    def __post_init__(self) -> None:
        for name in ("fill_id", "client_order_id", "account_id", "symbol"):
            value = str(getattr(self, name)).strip()
            if not value:
                raise ValueError(f"{name} must not be empty")
            object.__setattr__(self, name, value)
        quantity = _decimal(self.quantity, "quantity", positive=True)
        price = _decimal(self.price, "price", positive=True)
        fee_amount = _decimal(self.fee_amount, "fee_amount", positive=False)
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "fee_amount", fee_amount)
        object.__setattr__(self, "occurred_at", _aware_utc(self.occurred_at, "occurred_at"))
        if self.fee_asset is not None:
            fee_asset = self.fee_asset.strip()
            object.__setattr__(self, "fee_asset", fee_asset or None)
        if self.exchange_order_id is not None:
            exchange_order_id = str(self.exchange_order_id).strip()
            object.__setattr__(self, "exchange_order_id", exchange_order_id or None)


@dataclass(frozen=True, slots=True)
class AssetBalance:
    """单项资产最新的权威可用/锁定余额。"""

    account_id: str
    asset: str
    free: Decimal
    locked: Decimal
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in ("account_id", "asset"):
            value = str(getattr(self, name)).strip()
            if not value:
                raise ValueError(f"{name} must not be empty")
            object.__setattr__(self, name, value)
        object.__setattr__(self, "free", _decimal(self.free, "free", positive=False))
        object.__setattr__(self, "locked", _decimal(self.locked, "locked", positive=False))
        object.__setattr__(self, "updated_at", _aware_utc(self.updated_at, "updated_at"))


@dataclass(frozen=True, slots=True)
class BalanceValue:
    """账户余额快照的输入值。"""

    asset: str
    free: Decimal
    locked: Decimal

    def __post_init__(self) -> None:
        asset = self.asset.strip()
        if not asset:
            raise ValueError("asset must not be empty")
        object.__setattr__(self, "asset", asset)
        object.__setattr__(self, "free", _decimal(self.free, "free", positive=False))
        object.__setattr__(self, "locked", _decimal(self.locked, "locked", positive=False))


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    """由不可变成交记录派生的净持仓投影。"""

    account_id: str
    symbol: str
    quantity: Decimal
    average_entry_price: Decimal
    realized_pnl: Decimal
    updated_at: datetime


def _decimal(value: object, name: str, *, positive: bool) -> Decimal:
    try:
        normalized = Decimal(str(value))
    except Exception as error:
        raise ValueError(f"{name} must be a decimal number") from error
    if not normalized.is_finite() or (normalized <= 0 if positive else normalized < 0):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be {qualifier}")
    return normalized


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)
