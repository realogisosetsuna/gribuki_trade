"""不依赖券商的 A 股模拟账户值对象。

模拟账户刻意由成交而非订单驱动。一笔成交代表一次完整的券商确认（手工或模拟）；
市场数据匹配、停牌检查与涨跌停检查属于执行模拟器的职责，本模块不会猜测这些结果。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from zoneinfo import ZoneInfo

from gribuki_trade.domain.orders import Side

_ASHARE_TIMEZONE = ZoneInfo("Asia/Shanghai")
_SYMBOL = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_MAX_IDENTIFIER_LENGTH = 128
_MAX_NOTE_LENGTH = 1_000


class PaperFillSource(StrEnum):
    """一笔已经确定的成交以何种方式进入本地账本。"""

    MANUAL = "MANUAL"
    SIMULATED = "SIMULATED"


class PaperInstrumentType(StrEnum):
    """现金费用规则不同的标的类别。"""

    STOCK = "STOCK"
    ETF = "ETF"


class PaperLedgerEventType(StrEnum):
    """确定性账户投影器能够识别的事件。"""

    ACCOUNT_OPENED = "ACCOUNT_OPENED"
    SESSION_ROLLED_OVER = "SESSION_ROLLED_OVER"
    FILL_RECORDED = "FILL_RECORDED"


@dataclass(frozen=True, slots=True)
class PaperFeeSchedule:
    """单笔成交可配置的现金费用。

    默认值只是建模假设，并非对特定券商合同的声明。默认过户费按 2022 年起实施的
    股票万分之 0.1 费率建模；佣金仍取决于用户与券商。ETF 默认不收印花税和过户费。
    所有费率均可由配置覆盖。
    """

    commission_rate: Decimal = Decimal("0.0003")
    minimum_commission_cny: Decimal = Decimal("5")
    stock_transfer_fee_rate: Decimal = Decimal("0.00001")
    etf_transfer_fee_rate: Decimal = Decimal("0")
    stock_sell_stamp_tax_rate: Decimal = Decimal("0.0005")
    etf_sell_stamp_tax_rate: Decimal = Decimal("0")
    currency_quantum: Decimal = Decimal("0.01")

    def __post_init__(self) -> None:
        for name in (
            "commission_rate",
            "minimum_commission_cny",
            "stock_transfer_fee_rate",
            "etf_transfer_fee_rate",
            "stock_sell_stamp_tax_rate",
            "etf_sell_stamp_tax_rate",
        ):
            value = _decimal(getattr(self, name), name)
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
            object.__setattr__(self, name, value)
        quantum = _decimal(self.currency_quantum, "currency_quantum")
        if quantum <= 0:
            raise ValueError("currency_quantum must be positive")
        object.__setattr__(self, "currency_quantum", quantum)


@dataclass(frozen=True, slots=True)
class PaperFillFees:
    """随一笔成交保留、显式且回放稳定的费用。"""

    commission: Decimal
    transfer_fee: Decimal
    stamp_tax: Decimal

    def __post_init__(self) -> None:
        for name in ("commission", "transfer_fee", "stamp_tax"):
            value = _decimal(getattr(self, name), name)
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
            object.__setattr__(self, name, value)

    @property
    def total(self) -> Decimal:
        return self.commission + self.transfer_fee + self.stamp_tax


@dataclass(frozen=True, slots=True)
class ASharePaperFill:
    """一条不可变的手工或模拟 A 股成交命令。

    手工与模拟成交使用完全相同的契约。手工券商账单可以提供 ``fee_override``；
    模拟成交不能提供，因此其费用始终由已配置费率表计算。
    """

    account_id: str
    fill_id: str
    symbol: str
    side: Side
    quantity: int
    price: Decimal
    instrument_type: PaperInstrumentType
    trading_date: date
    executed_at: datetime
    source: PaperFillSource
    fee_override: PaperFillFees | None = None
    external_order_id: str | None = None
    note: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", _identifier(self.account_id, "account_id"))
        object.__setattr__(self, "fill_id", _identifier(self.fill_id, "fill_id"))
        symbol = self.symbol.strip().upper()
        if not _SYMBOL.fullmatch(symbol):
            raise ValueError("symbol must use the 000001.SZ/600000.SH/920000.BJ form")
        object.__setattr__(self, "symbol", symbol)
        try:
            side = Side(self.side)
            instrument_type = PaperInstrumentType(self.instrument_type)
            source = PaperFillSource(self.source)
        except ValueError as error:
            raise ValueError("unsupported fill enum value") from error
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "instrument_type", instrument_type)
        object.__setattr__(self, "source", source)
        if isinstance(self.quantity, bool) or not isinstance(self.quantity, int):
            raise TypeError("quantity must be an integer number of shares")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        object.__setattr__(self, "price", _positive_decimal(self.price, "price"))
        if not isinstance(self.trading_date, date) or isinstance(self.trading_date, datetime):
            raise TypeError("trading_date must be a date")
        executed_at = _aware_utc(self.executed_at, "executed_at")
        if executed_at.astimezone(_ASHARE_TIMEZONE).date() != self.trading_date:
            raise ValueError("executed_at Asia/Shanghai date must equal trading_date")
        object.__setattr__(self, "executed_at", executed_at)
        if self.fee_override is not None and source is not PaperFillSource.MANUAL:
            raise ValueError("fee_override is accepted only for MANUAL fills")
        if self.external_order_id is not None:
            object.__setattr__(
                self,
                "external_order_id",
                _optional_identifier(self.external_order_id, "external_order_id"),
            )
        if self.note is not None:
            note = self.note.strip()
            if not note:
                object.__setattr__(self, "note", None)
            elif len(note) > _MAX_NOTE_LENGTH:
                raise ValueError(f"note must not exceed {_MAX_NOTE_LENGTH} characters")
            else:
                object.__setattr__(self, "note", note)


@dataclass(frozen=True, slots=True)
class AppliedPaperFill:
    """一笔成交及已提交到账本的不可变现金影响。"""

    fill: ASharePaperFill
    trade_value: Decimal
    fees: PaperFillFees
    cash_change: Decimal
    realized_pnl_change: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "trade_value", _positive_decimal(self.trade_value, "trade_value")
        )
        object.__setattr__(self, "cash_change", _decimal(self.cash_change, "cash_change"))
        object.__setattr__(
            self,
            "realized_pnl_change",
            _decimal(self.realized_pnl_change, "realized_pnl_change"),
        )


@dataclass(frozen=True, slots=True)
class PaperPosition:
    """一份感知 T+1 规则的持仓投影。"""

    symbol: str
    instrument_type: PaperInstrumentType
    quantity: int
    available_to_sell: int
    today_buy: int
    average_cost: Decimal
    realized_pnl: Decimal

    def __post_init__(self) -> None:
        if self.quantity < 0 or self.available_to_sell < 0 or self.today_buy < 0:
            raise ValueError("position quantities must be non-negative")
        if self.quantity != self.available_to_sell + self.today_buy:
            raise ValueError("quantity must equal available_to_sell + today_buy")
        average_cost = _decimal(self.average_cost, "average_cost")
        if average_cost < 0 or (self.quantity > 0 and average_cost == 0):
            raise ValueError("average_cost is inconsistent with position quantity")
        if self.quantity == 0 and average_cost != 0:
            raise ValueError("an empty position must have zero average_cost")
        object.__setattr__(self, "average_cost", average_cost)
        object.__setattr__(
            self, "realized_pnl", _decimal(self.realized_pnl, "realized_pnl")
        )


@dataclass(frozen=True, slots=True)
class PaperAccountSnapshot:
    """由不可变账本事件重建的确定性投影。"""

    account_id: str
    session_date: date
    cash: Decimal
    positions: tuple[PaperPosition, ...]
    opened_at: datetime
    updated_at: datetime
    last_sequence: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", _identifier(self.account_id, "account_id"))
        cash = _decimal(self.cash, "cash")
        if cash < 0:
            raise ValueError("cash must be non-negative")
        object.__setattr__(self, "cash", cash)
        if self.last_sequence < 1:
            raise ValueError("last_sequence must be positive")
        object.__setattr__(self, "opened_at", _aware_utc(self.opened_at, "opened_at"))
        object.__setattr__(self, "updated_at", _aware_utc(self.updated_at, "updated_at"))
        symbols = tuple(position.symbol for position in self.positions)
        if symbols != tuple(sorted(symbols)) or len(symbols) != len(set(symbols)):
            raise ValueError("positions must be unique and sorted by symbol")

    def position(self, symbol: str) -> PaperPosition | None:
        normalized = symbol.strip().upper()
        return next(
            (position for position in self.positions if position.symbol == normalized),
            None,
        )


@dataclass(frozen=True, slots=True)
class NewPaperLedgerEvent:
    """等待分配 SQLite 序列号的事件。"""

    event_id: str
    account_id: str
    event_type: PaperLedgerEventType
    occurred_at: datetime
    session_date: date
    idempotency_key: str
    payload_json: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _identifier(self.event_id, "event_id"))
        object.__setattr__(self, "account_id", _identifier(self.account_id, "account_id"))
        object.__setattr__(
            self, "idempotency_key", _identifier(self.idempotency_key, "idempotency_key")
        )
        object.__setattr__(self, "event_type", PaperLedgerEventType(self.event_type))
        object.__setattr__(self, "occurred_at", _aware_utc(self.occurred_at, "occurred_at"))
        if not self.payload_json:
            raise ValueError("payload_json must not be empty")


@dataclass(frozen=True, slots=True)
class PaperLedgerEvent:
    """从本地账本读取的一条不可变哈希链记录。"""

    sequence: int
    event_id: str
    account_id: str
    event_type: PaperLedgerEventType
    occurred_at: datetime
    session_date: date
    idempotency_key: str
    payload_json: str
    payload_sha256: str
    previous_hash: str | None
    event_hash: str

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError("sequence must be positive")
        _identifier(self.event_id, "event_id")
        _identifier(self.account_id, "account_id")
        _identifier(self.idempotency_key, "idempotency_key")
        object.__setattr__(self, "event_type", PaperLedgerEventType(self.event_type))
        object.__setattr__(self, "occurred_at", _aware_utc(self.occurred_at, "occurred_at"))


@dataclass(frozen=True, slots=True)
class PaperFillReceipt:
    """一条幂等成交命令的结果。"""

    applied_new: bool
    event_sequence: int
    applied_fill: AppliedPaperFill
    snapshot: PaperAccountSnapshot


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    if len(normalized) > _MAX_IDENTIFIER_LENGTH:
        raise ValueError(f"{name} must not exceed {_MAX_IDENTIFIER_LENGTH} characters")
    return normalized


def _optional_identifier(value: object, name: str) -> str | None:
    normalized = _identifier(value, name)
    return normalized or None


def _decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def _positive_decimal(value: object, name: str) -> Decimal:
    normalized = _decimal(value, name)
    if normalized <= 0:
        raise ValueError(f"{name} must be positive")
    return normalized


def _aware_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)
