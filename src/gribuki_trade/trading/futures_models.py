"""供持久化合约状态使用的券商中立规范模型。

Binance 载荷由适配器负责解析；这里仅使用字符串、十进制和映射，持久化边界不
导入券商协议模块。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class FuturesOrderKind(StrEnum):
    NORMAL = "NORMAL"
    ALGO = "ALGO"


class FuturesOrderStatus(StrEnum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"
    TRIGGERED = "TRIGGERED"
    TRIGGERING = "TRIGGERING"
    FINISHED = "FINISHED"
    EXPIRED_IN_MATCH = "EXPIRED_IN_MATCH"
    UNKNOWN = "UNKNOWN"


class FuturesCommandStatus(StrEnum):
    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    SENT = "SENT"
    UNKNOWN = "UNKNOWN"
    RESOLVED = "RESOLVED"
    FAILED = "FAILED"


def _decimal(value: object | None, *, default: str = "0") -> Decimal:
    try:
        number = Decimal(default if value is None else str(value))
    except Exception as exc:  # pragma: no cover - 防御性输入边界
        raise ValueError("value must be a finite decimal") from exc
    if not number.is_finite():
        raise ValueError("value must be a finite decimal")
    return number


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _required(value: object, name: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name} must not be blank")
    return text


@dataclass(frozen=True, slots=True)
class FuturesUserEvent:
    """供合约 OMS 消费的规范私有流事件封套。"""

    event_type: str
    event_time_ms: int
    transaction_time_ms: int | None
    received_time_ms: int
    payload: Mapping[str, Any]
    connection_epoch: int

    def __post_init__(self) -> None:
        if self.event_time_ms < 0 or self.received_time_ms < 0:
            raise ValueError("event timestamps must be non-negative")
        if self.transaction_time_ms is not None and self.transaction_time_ms < 0:
            raise ValueError("transaction timestamp must be non-negative")
        object.__setattr__(self, "event_type", _required(self.event_type, "event_type"))
        if self.connection_epoch < 0:
            raise ValueError("connection_epoch must be non-negative")


@dataclass(frozen=True, slots=True)
class FuturesOrderSnapshot:
    account_id: str
    environment: str
    product: str
    order_key: str
    symbol: str
    side: str
    position_side: str
    kind: FuturesOrderKind | str = FuturesOrderKind.NORMAL
    status: FuturesOrderStatus | str = FuturesOrderStatus.NEW
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    algo_id: str | None = None
    client_algo_id: str | None = None
    parent_order_key: str | None = None
    protection_plan_id: str | None = None
    order_type: str | None = None
    execution_type: str | None = None
    quantity: Decimal = Decimal("0")
    filled_quantity: Decimal = Decimal("0")
    average_price: Decimal | None = None
    trigger_price: Decimal | None = None
    activate_price: Decimal | None = None
    callback_rate: Decimal | None = None
    reduce_only: bool = False
    close_position: bool = False
    working_type: str | None = None
    realized_pnl: Decimal = Decimal("0")
    status_time_ms: int = 0
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in (
            "account_id",
            "environment",
            "product",
            "order_key",
            "symbol",
            "side",
            "position_side",
        ):
            object.__setattr__(self, name, _required(getattr(self, name), name))
        object.__setattr__(self, "kind", FuturesOrderKind(str(self.kind)))
        object.__setattr__(self, "status", FuturesOrderStatus(str(self.status)))
        for name in ("quantity", "filled_quantity", "realized_pnl"):
            object.__setattr__(self, name, _decimal(getattr(self, name)))
        for name in ("average_price", "trigger_price", "activate_price", "callback_rate"):
            value = getattr(self, name)
            object.__setattr__(self, name, None if value is None else _decimal(value))
        object.__setattr__(self, "updated_at", _utc(self.updated_at))
        if self.status_time_ms < 0:
            raise ValueError("status_time_ms must be non-negative")


@dataclass(frozen=True, slots=True)
class FuturesFill:
    account_id: str
    environment: str
    product: str
    fill_id: str
    symbol: str
    side: str
    position_side: str
    quantity: Decimal
    price: Decimal
    trade_id: str | None = None
    order_key: str | None = None
    exchange_order_id: str | None = None
    fee_asset: str | None = None
    fee_amount: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in (
            "account_id",
            "environment",
            "product",
            "fill_id",
            "symbol",
            "side",
            "position_side",
        ):
            object.__setattr__(self, name, _required(getattr(self, name), name))
        for name in ("quantity", "price", "fee_amount", "realized_pnl"):
            object.__setattr__(self, name, _decimal(getattr(self, name)))
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at))


@dataclass(frozen=True, slots=True)
class FuturesPositionSnapshot:
    account_id: str
    environment: str
    product: str
    symbol: str
    position_side: str
    quantity: Decimal
    entry_price: Decimal = Decimal("0")
    break_even_price: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    margin_type: str | None = None
    isolated_wallet: Decimal = Decimal("0")
    leverage: int | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("account_id", "environment", "product", "symbol", "position_side"):
            object.__setattr__(self, name, _required(getattr(self, name), name))
        for name in (
            "quantity",
            "entry_price",
            "break_even_price",
            "realized_pnl",
            "unrealized_pnl",
            "isolated_wallet",
        ):
            object.__setattr__(self, name, _decimal(getattr(self, name)))
        object.__setattr__(self, "updated_at", _utc(self.updated_at))


@dataclass(frozen=True, slots=True)
class FuturesBalanceSnapshot:
    account_id: str
    environment: str
    product: str
    asset: str
    wallet_balance: Decimal
    available_balance: Decimal = Decimal("0")
    cross_wallet_balance: Decimal = Decimal("0")
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("account_id", "environment", "product", "asset"):
            object.__setattr__(self, name, _required(getattr(self, name), name))
        for name in ("wallet_balance", "available_balance", "cross_wallet_balance"):
            object.__setattr__(self, name, _decimal(getattr(self, name)))
        object.__setattr__(self, "updated_at", _utc(self.updated_at))


@dataclass(frozen=True, slots=True)
class FuturesConfigSnapshot:
    account_id: str
    environment: str
    product: str
    symbol: str
    leverage: int | None = None
    margin_type: str | None = None
    position_mode: str | None = None
    multi_assets_mode: bool | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("account_id", "environment", "product", "symbol"):
            object.__setattr__(self, name, _required(getattr(self, name), name))
        if self.leverage is not None and self.leverage <= 0:
            raise ValueError("leverage must be positive")
        object.__setattr__(self, "updated_at", _utc(self.updated_at))


@dataclass(frozen=True, slots=True)
class FuturesStreamHealth:
    account_id: str
    environment: str
    product: str
    state: str
    connection_epoch: int
    last_event_time_ms: int | None = None
    last_received_time_ms: int | None = None
    gap_count: int = 0
    reason: str | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        for name in ("account_id", "environment", "product", "state"):
            object.__setattr__(self, name, _required(getattr(self, name), name))
        if self.connection_epoch < 0 or self.gap_count < 0:
            raise ValueError("stream counters must be non-negative")
        object.__setattr__(self, "updated_at", _utc(self.updated_at))


@dataclass(frozen=True, slots=True)
class FuturesCommand:
    account_id: str
    environment: str
    product: str
    command_id: str
    command_type: str
    order_key: str | None
    payload: Mapping[str, Any]
    status: FuturesCommandStatus
    attempt_count: int
    owner_id: str | None
    fencing_token: int | None
    lease_until: datetime | None
    created_at: datetime
    updated_at: datetime
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class FuturesProtectionPlan:
    account_id: str
    environment: str
    product: str
    plan_id: str
    revision: int
    symbol: str
    position_side: str
    desired_state: str
    coverage_state: str
    entry_order_key: str | None = None
    stop_algo_key: str | None = None
    take_profit_algo_key: str | None = None
    trailing_algo_key: str | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in (
            "account_id",
            "environment",
            "product",
            "plan_id",
            "symbol",
            "position_side",
            "desired_state",
            "coverage_state",
        ):
            object.__setattr__(self, name, _required(getattr(self, name), name))
        if self.revision < 0:
            raise ValueError("revision must be non-negative")
        object.__setattr__(self, "updated_at", _utc(self.updated_at))
