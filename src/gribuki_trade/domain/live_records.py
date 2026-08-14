"""手工同步实盘成交所使用的严格领域对象。

这里描述的都是外部券商已经成交的事实，不是订单，也没有任何券商写权限。
实盘观察账本与 PAPER 账户使用完全不同的持久化命名空间。
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import PaperFillFees, PaperInstrumentType

_IDENTIFIER = re.compile(r"^[A-Za-z0-9_.:-]{1,96}$")
_SYMBOL = re.compile(r"^(?:[036689]\d{5})\.(?:SH|SZ|BJ)$")


class LiveRecordEventType(StrEnum):
    COMMAND_PROPOSED = "COMMAND_PROPOSED"
    FILL_CONFIRMED = "FILL_CONFIRMED"
    COMMAND_CANCELLED = "COMMAND_CANCELLED"
    PROTECTION_WORK_QUEUED = "PROTECTION_WORK_QUEUED"
    PROTECTION_QUICK_READY = "PROTECTION_QUICK_READY"
    PROTECTION_DEEP_WORK_QUEUED = "PROTECTION_DEEP_WORK_QUEUED"
    PROTECTION_READY = "PROTECTION_READY"
    PROTECTION_FAILED = "PROTECTION_FAILED"
    PROTECTION_CLOSE_WORK_QUEUED = "PROTECTION_CLOSE_WORK_QUEUED"
    EXIT_ALERT_QUEUED = "EXIT_ALERT_QUEUED"


class LiveWorkKind(StrEnum):
    """由应用运行框架领取的持久工作类型。"""

    BUILD_PROTECTION = "BUILD_PROTECTION"
    BUILD_DEEP_PROTECTION = "BUILD_DEEP_PROTECTION"
    CLOSE_PROTECTION = "CLOSE_PROTECTION"
    DELIVER_EXIT_ALERT = "DELIVER_EXIT_ALERT"


class LiveWorkStatus(StrEnum):
    """工作项状态；租约过期的 ``RUNNING`` 可被另一进程恢复。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRY = "RETRY"
    COMPLETED = "COMPLETED"
    DEAD = "DEAD"


class LiveInboundAction(StrEnum):
    PROPOSE = "PROPOSE"
    CONFIRM = "CONFIRM"
    CANCEL = "CANCEL"


@dataclass(frozen=True, slots=True)
class OneBotPrivateMessage:
    """一条 OneBot 私聊事件的规范化字段子集。"""

    self_id: str
    sender_id: str
    message_id: str
    occurred_at: datetime
    raw_text: str
    sub_type: str = "friend"

    def __post_init__(self) -> None:
        for name in ("self_id", "sender_id", "message_id"):
            value = str(getattr(self, name)).strip()
            if not value.isascii() or not value.isdecimal() or int(value) <= 0:
                raise ValueError(f"{name} must be a positive decimal identifier")
            object.__setattr__(self, name, str(int(value)))
        if self.sender_id == self.self_id:
            raise ValueError("self-authored OneBot messages are not accepted")
        if self.sub_type != "friend":
            raise ValueError("only friend private messages are accepted")
        object.__setattr__(self, "occurred_at", _aware_utc(self.occurred_at))
        if not isinstance(self.raw_text, str) or not self.raw_text.strip():
            raise ValueError("raw_text must not be empty")
        if len(self.raw_text) > 4096:
            raise ValueError("raw_text exceeds the inbound command limit")


@dataclass(frozen=True, slots=True)
class ConfirmedLiveFill:
    """一笔已在外部成交、等待两条消息完成确认的成交事实。"""

    command_id: str
    account_id: str
    side: Side
    symbol: str
    quantity: int
    price: Decimal
    instrument_type: PaperInstrumentType
    executed_at: datetime
    fees: PaperFillFees
    external_order_id: str | None = None
    external_fill_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "command_id", _identifier(self.command_id, "command_id"))
        object.__setattr__(self, "account_id", _identifier(self.account_id, "account_id"))
        object.__setattr__(self, "side", Side(self.side))
        symbol = self.symbol.strip().upper()
        if _SYMBOL.fullmatch(symbol) is None:
            raise ValueError("symbol must be a canonical A-share code")
        object.__setattr__(self, "symbol", symbol)
        if isinstance(self.quantity, bool) or not isinstance(self.quantity, int):
            raise TypeError("quantity must be an integer")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        object.__setattr__(self, "price", _positive_decimal(self.price, "price"))
        object.__setattr__(
            self,
            "instrument_type",
            PaperInstrumentType(self.instrument_type),
        )
        object.__setattr__(self, "executed_at", _aware_utc(self.executed_at))
        if self.external_order_id is not None:
            object.__setattr__(
                self,
                "external_order_id",
                _identifier(self.external_order_id, "external_order_id"),
            )
        if self.external_fill_id is not None:
            object.__setattr__(
                self,
                "external_fill_id",
                _identifier(self.external_fill_id, "external_fill_id"),
            )

    @property
    def trade_value(self) -> Decimal:
        return self.price * self.quantity

    @property
    def fingerprint(self) -> str:
        """由完整规范摘要支撑、便于复制的短确认指纹。"""

        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()[:24]

    @property
    def external_fill_key(self) -> str:
        """跨命令去重键；新入站使用券商成交号，摘要仅兼容旧账本。"""

        if self.external_fill_id is not None:
            return f"broker-fill:{self.external_fill_id}"
        document = self.canonical_document()
        document.pop("command_id")
        canonical = json.dumps(
            document,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return "digest:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def canonical_document(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "command_id": self.command_id,
            "executed_at": self.executed_at.isoformat(),
            "external_fill_id": self.external_fill_id,
            "external_order_id": self.external_order_id,
            "fees": {
                "commission": str(self.fees.commission),
                "stamp_tax": str(self.fees.stamp_tax),
                "transfer_fee": str(self.fees.transfer_fee),
            },
            "instrument_type": self.instrument_type.value,
            "price": str(self.price),
            "quantity": self.quantity,
            "side": self.side.value,
            "symbol": self.symbol,
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.canonical_document(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass(frozen=True, slots=True)
class LiveInboundCommand:
    action: LiveInboundAction
    command_id: str
    fill: ConfirmedLiveFill | None = None
    fingerprint: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "action", LiveInboundAction(self.action))
        object.__setattr__(self, "command_id", _identifier(self.command_id, "command_id"))
        if self.action is LiveInboundAction.PROPOSE:
            if self.fill is None or self.fill.command_id != self.command_id:
                raise ValueError("a proposal must contain its matching fill")
            if self.fingerprint is not None:
                raise ValueError("a proposal does not carry a confirmation fingerprint")
        elif self.action is LiveInboundAction.CONFIRM:
            if self.fill is not None or self.fingerprint is None:
                raise ValueError("confirmation requires only a fingerprint")
            value = self.fingerprint.strip().lower()
            if re.fullmatch(r"[0-9a-f]{24}", value) is None:
                raise ValueError("confirmation fingerprint must contain 24 hex characters")
            object.__setattr__(self, "fingerprint", value)
        elif self.fill is not None or self.fingerprint is not None:
            raise ValueError("cancellation requires only command_id")


@dataclass(frozen=True, slots=True)
class NewLiveRecordEvent:
    event_id: str
    account_id: str
    event_type: LiveRecordEventType
    occurred_at: datetime
    idempotency_key: str
    payload_json: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _identifier(self.event_id, "event_id"))
        object.__setattr__(self, "account_id", _identifier(self.account_id, "account_id"))
        object.__setattr__(self, "event_type", LiveRecordEventType(self.event_type))
        object.__setattr__(self, "occurred_at", _aware_utc(self.occurred_at))
        object.__setattr__(
            self,
            "idempotency_key",
            _identifier(self.idempotency_key, "idempotency_key"),
        )
        if not self.payload_json:
            raise ValueError("payload_json must not be empty")


@dataclass(frozen=True, slots=True)
class LiveRecordEvent:
    sequence: int
    event_id: str
    account_id: str
    event_type: LiveRecordEventType
    occurred_at: datetime
    idempotency_key: str
    payload_json: str
    payload_sha256: str
    previous_hash: str | None
    event_hash: str


@dataclass(frozen=True, slots=True)
class LivePosition:
    symbol: str
    instrument_type: PaperInstrumentType
    quantity: int
    average_cost: Decimal
    realized_pnl: Decimal


@dataclass(frozen=True, slots=True)
class LiveAccountSnapshot:
    account_id: str
    positions: tuple[LivePosition, ...]
    confirmed_fill_count: int
    total_fees: Decimal
    updated_at: datetime | None
    last_sequence: int


@dataclass(frozen=True, slots=True)
class LiveProtectionTracking:
    """一笔实盘买入成交对应的独立保护与剩余数量。"""

    protection_id: str
    account_id: str
    buy_command_id: str
    symbol: str
    instrument_type: PaperInstrumentType
    acquired_at: datetime
    original_quantity: int
    remaining_quantity: int
    plan_ready: bool
    plan_stream_id: str | None
    last_observed_bar_end: datetime | None
    last_alert_bar_end: datetime | None


@dataclass(frozen=True, slots=True)
class LiveWorkItem:
    """SQLite 中可租约领取、可重试且可跨崩溃恢复的工作项。"""

    work_id: str
    kind: LiveWorkKind
    account_id: str
    command_id: str
    protection_id: str | None
    payload_json: str
    status: LiveWorkStatus
    attempts: int
    available_at: datetime
    lease_until: datetime | None
    created_at: datetime
    updated_at: datetime
    result_code: str | None
    error_code: str | None


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    normalized = value.strip()
    if _IDENTIFIER.fullmatch(normalized) is None:
        raise ValueError(f"{name} contains unsupported characters")
    return normalized


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("timestamp must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _positive_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return value
