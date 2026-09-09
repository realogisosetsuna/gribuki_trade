"""实盘成交记录服务的 OneBot 事件与版本化命令解析。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from gribuki_trade.domain.live_records import (
    ConfirmedLiveFill,
    LiveInboundAction,
    LiveInboundCommand,
    OneBotPrivateMessage,
)
from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import PaperFillFees, PaperInstrumentType

_PROPOSE_PREFIX = "GT-LIVE/1"
_CONFIRM_PREFIX = "GT-LIVE-CONFIRM/1"
_CANCEL_PREFIX = "GT-LIVE-CANCEL/1"
_PROPOSAL_REQUIRED = frozenset(
    {
        "account",
        "command_id",
        "commission",
        "executed_at",
        "instrument",
        "price",
        "quantity",
        "side",
        "stamp_tax",
        "symbol",
        "transfer_fee",
    }
)
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def parse_onebot_private_message(payload: Mapping[str, object]) -> OneBotPrivateMessage:
    """除普通好友私聊外，对所有事件形态一律封闭拒绝。"""

    if payload.get("post_type") != "message" or payload.get("message_type") != "private":
        raise ValueError("OneBot event is not a private message")
    if payload.get("sub_type") != "friend":
        raise ValueError("OneBot private message must be from a friend")
    raw = payload.get("raw_message")
    timestamp = payload.get("time")
    if not isinstance(raw, str):
        raise ValueError("OneBot raw_message must be text")
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp <= 0:
        raise ValueError("OneBot time must be a positive Unix timestamp")
    return OneBotPrivateMessage(
        self_id=str(payload.get("self_id", "")),
        sender_id=str(payload.get("user_id", "")),
        message_id=str(payload.get("message_id", "")),
        occurred_at=datetime.fromtimestamp(timestamp, UTC),
        raw_text=raw,
        sub_type="friend",
    )


def parse_live_inbound_command(text: str) -> LiveInboundCommand:
    """解析有意保持精简且便于复制的 QQ 命令语法。"""

    if not isinstance(text, str):
        raise TypeError("command text must be a string")
    if text != text.strip() or any(character in text for character in "\r\n\x00"):
        raise ValueError("command contains outer whitespace or control characters")
    parts = text.split("|")
    if not parts or any(part != part.strip() for part in parts):
        raise ValueError("command is empty")
    prefix = parts[0]
    fields: dict[str, str] = {}
    for part in parts[1:]:
        if "=" not in part:
            raise ValueError("command fields must use key=value")
        key, value = part.split("=", 1)
        if not key or not value or key in fields:
            raise ValueError("command contains an empty or duplicate field")
        fields[key] = value
    if prefix == _PROPOSE_PREFIX:
        allowed = _PROPOSAL_REQUIRED | {"external_fill_id", "external_order_id"}
        if frozenset(fields) - allowed or not _PROPOSAL_REQUIRED.issubset(fields):
            raise ValueError("proposal fields do not match the versioned schema")
        fees = PaperFillFees(
            commission=Decimal(fields["commission"]),
            transfer_fee=Decimal(fields["transfer_fee"]),
            stamp_tax=Decimal(fields["stamp_tax"]),
        )
        fill = ConfirmedLiveFill(
            command_id=fields["command_id"],
            account_id=fields["account"],
            side=Side(fields["side"].upper()),
            symbol=fields["symbol"],
            quantity=int(fields["quantity"]),
            price=Decimal(fields["price"]),
            instrument_type=PaperInstrumentType(fields["instrument"].upper()),
            executed_at=datetime.fromisoformat(fields["executed_at"]),
            fees=fees,
            external_order_id=fields.get("external_order_id"),
            external_fill_id=fields.get("external_fill_id"),
        )
        return LiveInboundCommand(
            action=LiveInboundAction.PROPOSE,
            command_id=fill.command_id,
            fill=fill,
        )
    if prefix == _CONFIRM_PREFIX and frozenset(fields) == {"command_id", "fingerprint"}:
        return LiveInboundCommand(
            action=LiveInboundAction.CONFIRM,
            command_id=fields["command_id"],
            fingerprint=fields["fingerprint"],
        )
    if prefix == _CANCEL_PREFIX and frozenset(fields) == {"command_id"}:
        return LiveInboundCommand(
            action=LiveInboundAction.CANCEL,
            command_id=fields["command_id"],
        )
    raise ValueError("unsupported live-record command")
