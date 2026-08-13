"""Durable order-management and trading-ledger value objects."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from gribuki_trade.domain.orders import OrderIntent, OrderStatus, Side


class TradingCommandType(StrEnum):
    """Commands that may cross the process/exchange boundary."""

    SUBMIT_ORDER = "SUBMIT_ORDER"
    CANCEL_ORDER = "CANCEL_ORDER"


class TradingCommandStatus(StrEnum):
    """Durable delivery state for a trading command.

    ``UNKNOWN`` is deliberately not retryable.  The OMS must reconcile the
    exchange by ``client_order_id`` before it can resolve that command.
    """

    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    SENT = "SENT"
    UNKNOWN = "UNKNOWN"
    RESOLVED = "RESOLVED"


@dataclass(frozen=True, slots=True)
class OrderSnapshot:
    """Latest materialized state reconstructed from append-only events."""

    order: OrderIntent
    status: OrderStatus
    filled_quantity: Decimal
    average_fill_price: Decimal | None
    exchange_order_id: str | None
    reason: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class OrderEventRecord:
    """One immutable order event in ingestion order."""

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
    """One durable broker command from the transactional outbox."""

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
    """An immutable execution used by paper, Testnet, and live ledgers."""

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
    """Latest authoritative free/locked balance for one asset."""

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
    """Input value for an account balance snapshot."""

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
    """Net-position projection derived from immutable fills."""

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
