"""通过两条消息接收外部已确认的 A 股成交。

中文边界说明：QQ 私聊只用于同步“券商已经成交”的事实，绝不生成券商委托。
第一条结构化消息仅形成待确认记录；只有同一白名单发送者提供正确指纹后，
成交才进入独立的实盘观察账本。PAPER 与实盘记录因此可以复用分析函数，
但不会共享账户状态或写入路径。
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import cast

from gribuki_trade.domain.live_records import (
    ConfirmedLiveFill,
    LiveAccountSnapshot,
    LiveInboundAction,
    LiveInboundCommand,
    LivePosition,
    LiveRecordEvent,
    LiveRecordEventType,
    NewLiveRecordEvent,
    OneBotPrivateMessage,
)
from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import PaperFillFees, PaperInstrumentType
from gribuki_trade.reporting.contracts import ReportKind, render_stable_text_report
from gribuki_trade.services.live_trade_records_parsing import (
    _CONFIRM_PREFIX,
    _SHANGHAI,
    parse_live_inbound_command,
)
from gribuki_trade.services.live_trade_records_parsing import (
    parse_onebot_private_message as _parse_onebot_private_message,
)
from gribuki_trade.storage.live_records import (
    LiveRecordConflictError,
    LiveRecordIntegrityError,
    LiveRecordStateError,
    SQLiteLiveRecordStore,
    StoredLiveCommand,
)

parse_onebot_private_message = _parse_onebot_private_message

__all__ = [
    "LiveInboundOutcome",
    "LiveInboundOutcomeStatus",
    "LiveTradeRecordError",
    "LiveTradeRecordService",
    "parse_live_inbound_command",
    "parse_onebot_private_message",
]


class LiveTradeRecordError(RuntimeError):
    """稳定拒绝入站请求，且不回显不可信消息文本。"""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"live trade record rejected ({code})")


class LiveInboundOutcomeStatus(StrEnum):
    PROPOSED = "PROPOSED"
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"
    REPLAYED = "REPLAYED"


@dataclass(frozen=True, slots=True)
class LiveInboundOutcome:
    status: LiveInboundOutcomeStatus
    command_id: str
    account_id: str
    fingerprint: str | None
    event_sequence: int
    analysis_required: bool
    response_text: str
    protection_id: str | None = None
    protection_work_id: str | None = None


class LiveTradeRecordService:
    """校验 OneBot 消息并投影隔离的实盘观察账户。"""

    def __init__(
        self,
        store: SQLiteLiveRecordStore,
        *,
        allowed_sender_ids: frozenset[str],
        maximum_message_age: timedelta = timedelta(minutes=10),
        maximum_fill_age: timedelta = timedelta(days=7),
        execution_session_validator: Callable[[datetime], bool] | None = None,
    ) -> None:
        normalized = frozenset(_qq_id(item) for item in allowed_sender_ids)
        if not normalized:
            raise ValueError("at least one live-record sender must be allowed")
        if maximum_message_age <= timedelta(0):
            raise ValueError("maximum_message_age must be positive")
        if maximum_fill_age <= timedelta(0):
            raise ValueError("maximum_fill_age must be positive")
        if execution_session_validator is None:
            raise ValueError("a verified A-share execution-session validator is required")
        self._store = store
        self._allowed_senders = normalized
        self._maximum_age = maximum_message_age
        self._maximum_fill_age = maximum_fill_age
        self._execution_session_validator = execution_session_validator

    def ingest(
        self,
        message: OneBotPrivateMessage,
        *,
        received_at: datetime | None = None,
    ) -> LiveInboundOutcome:
        now = _aware_utc(received_at or datetime.now(UTC))
        if message.sender_id not in self._allowed_senders:
            raise LiveTradeRecordError("SENDER_NOT_ALLOWED")
        if message.occurred_at > now + timedelta(seconds=30):
            raise LiveTradeRecordError("MESSAGE_FROM_FUTURE")
        if now - message.occurred_at > self._maximum_age:
            raise LiveTradeRecordError("MESSAGE_TOO_OLD")
        try:
            command = parse_live_inbound_command(message.raw_text)
        except (TypeError, ValueError, json.JSONDecodeError):
            raise LiveTradeRecordError("COMMAND_INVALID") from None
        if command.action is LiveInboundAction.PROPOSE:
            assert command.fill is not None
            # 券商委托号不能唯一标识一次部分成交；新的实盘事实必须携带
            # 券商成交执行号，旧账本中没有该字段的历史记录仍可继续重放。
            existing_command = self._store.command(command.fill.command_id)
            if existing_command is None:
                if command.fill.external_order_id is None:
                    raise LiveTradeRecordError("EXTERNAL_ORDER_ID_REQUIRED")
                if command.fill.external_fill_id is None:
                    raise LiveTradeRecordError("EXTERNAL_FILL_ID_REQUIRED")
            self._validate_execution_time(command.fill, message=message, received_at=now)
            return self._propose(message, command, now)
        if command.action is LiveInboundAction.CONFIRM:
            return self._confirm(message, command, now)
        return self._cancel(message, command, now)

    def snapshot(self, account_id: str) -> LiveAccountSnapshot:
        return project_live_account(account_id, self._store.events(account_id))

    def _propose(
        self,
        message: OneBotPrivateMessage,
        command: LiveInboundCommand,
        received_at: datetime,
    ) -> LiveInboundOutcome:
        assert command.fill is not None
        payload = _canonical_json(
            {
                "fill": command.fill.canonical_document(),
                "fingerprint": command.fill.fingerprint,
                "received_at": received_at.isoformat(),
                "sender_id": message.sender_id,
                "source_occurred_at": message.occurred_at.isoformat(),
                "source_message_id": message.message_id,
            }
        )
        try:
            event, created = self._store.register_proposal(
                fill=command.fill,
                sender_id=message.sender_id,
                source_message_id=message.message_id,
                event=NewLiveRecordEvent(
                    event_id=_message_event_id(message, "proposal"),
                    account_id=command.fill.account_id,
                    event_type=LiveRecordEventType.COMMAND_PROPOSED,
                    occurred_at=received_at,
                    idempotency_key=_message_key(message),
                    payload_json=payload,
                ),
            )
        except LiveRecordStateError as error:
            raise LiveTradeRecordError(error.code) from None
        except LiveRecordConflictError:
            raise LiveTradeRecordError("LIVE_LEDGER_CONFLICT") from None
        except LiveRecordIntegrityError:
            raise LiveTradeRecordError("LIVE_LEDGER_INTEGRITY_FAILURE") from None
        stored_command = self._store.command(command.command_id)
        if stored_command is None:  # pragma: no cover - 同一事务不变量
            raise LiveTradeRecordError("STORED_EVENT_INVALID")
        if not created and stored_command.state != "PENDING":
            terminal_id = stored_command.terminal_event_id
            if terminal_id is None:
                raise LiveTradeRecordError("STORED_EVENT_INVALID")
            terminal = next(
                (
                    candidate
                    for candidate in self._store.events(stored_command.account_id)
                    if candidate.event_id == terminal_id
                ),
                None,
            )
            if terminal is None:
                raise LiveTradeRecordError("STORED_EVENT_INVALID")
            return _terminal_replay_outcome(
                stored_command,
                terminal,
                protection_identity=self._protection_identity(stored_command),
            )
        status = LiveInboundOutcomeStatus.PROPOSED if created else LiveInboundOutcomeStatus.REPLAYED
        return LiveInboundOutcome(
            status=status,
            command_id=command.command_id,
            account_id=command.fill.account_id,
            fingerprint=command.fill.fingerprint,
            event_sequence=event.sequence,
            analysis_required=False,
            response_text=_pending_execution_receipt(
                command.fill,
                replayed=not created,
            ),
        )

    def _confirm(
        self,
        message: OneBotPrivateMessage,
        command: LiveInboundCommand,
        received_at: datetime,
    ) -> LiveInboundOutcome:
        stored_command = self._store.command(command.command_id)
        if stored_command is None:
            raise LiveTradeRecordError("COMMAND_NOT_FOUND")
        fill = _fill_from_document(_json_object(stored_command.fill_json))
        if message.occurred_at + timedelta(seconds=30) < stored_command.proposed_at:
            raise LiveTradeRecordError("CONFIRMATION_PRECEDES_PROPOSAL")
        payload = _canonical_json(
            {
                "command_id": fill.command_id,
                "confirmed_at": received_at.isoformat(),
                "confirmation_message_id": message.message_id,
                "fill": fill.canonical_document(),
                "fingerprint": fill.fingerprint,
                "proposal_event_id": stored_command.proposal_event_id,
                "sender_id": message.sender_id,
            }
        )
        assert command.fingerprint is not None
        try:
            committed = self._store.confirm_fill(
                fill=fill,
                sender_id=message.sender_id,
                fingerprint=command.fingerprint,
                event=NewLiveRecordEvent(
                    event_id=_message_event_id(message, "confirmation"),
                    account_id=fill.account_id,
                    event_type=LiveRecordEventType.FILL_CONFIRMED,
                    occurred_at=received_at,
                    idempotency_key=_message_key(message),
                    payload_json=payload,
                ),
            )
        except LiveRecordStateError as error:
            raise LiveTradeRecordError(error.code) from None
        except LiveRecordConflictError:
            raise LiveTradeRecordError("LIVE_LEDGER_CONFLICT") from None
        except LiveRecordIntegrityError:
            raise LiveTradeRecordError("LIVE_LEDGER_INTEGRITY_FAILURE") from None
        if not committed.created:
            return _terminal_replay_outcome(
                stored_command,
                committed.event,
                protection_identity=(
                    committed.protection_id,
                    committed.protection_work_id,
                ),
            )
        snapshot = self.snapshot(fill.account_id)
        position = next(item for item in snapshot.positions if item.symbol == fill.symbol)
        response = _execution_receipt(
            fill,
            position_quantity=position.quantity,
            protection_work_id=committed.protection_work_id,
        )
        return LiveInboundOutcome(
            status=LiveInboundOutcomeStatus.CONFIRMED,
            command_id=fill.command_id,
            account_id=fill.account_id,
            fingerprint=fill.fingerprint,
            event_sequence=committed.event.sequence,
            analysis_required=committed.protection_work_id is not None,
            response_text=response,
            protection_id=committed.protection_id,
            protection_work_id=committed.protection_work_id,
        )

    def _cancel(
        self,
        message: OneBotPrivateMessage,
        command: LiveInboundCommand,
        received_at: datetime,
    ) -> LiveInboundOutcome:
        stored_command = self._store.command(command.command_id)
        if stored_command is None:
            raise LiveTradeRecordError("COMMAND_NOT_FOUND")
        if message.occurred_at + timedelta(seconds=30) < stored_command.proposed_at:
            raise LiveTradeRecordError("CANCELLATION_PRECEDES_PROPOSAL")
        payload = _canonical_json(
            {
                "cancel_message_id": message.message_id,
                "cancelled_at": received_at.isoformat(),
                "command_id": command.command_id,
                "proposal_event_id": stored_command.proposal_event_id,
                "sender_id": message.sender_id,
            }
        )
        try:
            event, created = self._store.cancel_command(
                command_id=command.command_id,
                sender_id=message.sender_id,
                event=NewLiveRecordEvent(
                    event_id=_message_event_id(message, "cancellation"),
                    account_id=stored_command.account_id,
                    event_type=LiveRecordEventType.COMMAND_CANCELLED,
                    occurred_at=received_at,
                    idempotency_key=_message_key(message),
                    payload_json=payload,
                ),
            )
        except LiveRecordStateError as error:
            raise LiveTradeRecordError(error.code) from None
        except LiveRecordConflictError:
            raise LiveTradeRecordError("LIVE_LEDGER_CONFLICT") from None
        except LiveRecordIntegrityError:
            raise LiveTradeRecordError("LIVE_LEDGER_INTEGRITY_FAILURE") from None
        if not created:
            return _terminal_replay_outcome(
                stored_command,
                event,
                protection_identity=self._protection_identity(stored_command),
            )
        fill = _fill_from_document(_json_object(stored_command.fill_json))
        return LiveInboundOutcome(
            status=(
                LiveInboundOutcomeStatus.CANCELLED if created else LiveInboundOutcomeStatus.REPLAYED
            ),
            command_id=command.command_id,
            account_id=stored_command.account_id,
            fingerprint=None,
            event_sequence=event.sequence,
            analysis_required=False,
            response_text=_cancelled_execution_receipt(fill, replayed=False),
        )

    def _validate_execution_time(
        self,
        fill: ConfirmedLiveFill,
        *,
        message: OneBotPrivateMessage,
        received_at: datetime,
    ) -> None:
        """拒绝未来、过旧、周末或交易时段之外的所谓券商成交。"""

        if fill.executed_at > message.occurred_at + timedelta(seconds=30):
            raise LiveTradeRecordError("EXECUTION_FROM_FUTURE")
        if received_at - fill.executed_at > self._maximum_fill_age:
            raise LiveTradeRecordError("EXECUTION_TOO_OLD")
        local = fill.executed_at.astimezone(_SHANGHAI)
        local_minutes = local.hour * 60 + local.minute
        in_session = local.weekday() < 5 and (
            9 * 60 + 25 <= local_minutes <= 11 * 60 + 30 or 13 * 60 <= local_minutes <= 15 * 60
        )
        if not in_session:
            raise LiveTradeRecordError("EXECUTION_OUTSIDE_A_SHARE_SESSION")
        try:
            verified_session = self._execution_session_validator(fill.executed_at)
        except Exception:
            raise LiveTradeRecordError("LIVE_TRADING_CALENDAR_UNAVAILABLE") from None
        if type(verified_session) is not bool:
            raise LiveTradeRecordError("LIVE_TRADING_CALENDAR_UNAVAILABLE")
        if not verified_session:
            raise LiveTradeRecordError("EXECUTION_NOT_ON_TRADING_SESSION")

    def _protection_identity(
        self,
        command: StoredLiveCommand,
    ) -> tuple[str | None, str | None]:
        tracking = next(
            (
                item
                for item in self._store.tracking(command.account_id, active_only=False)
                if item.buy_command_id == command.command_id
            ),
            None,
        )
        if tracking is None:
            return None, None
        work = next(
            (
                item
                for item in self._store.work_items(command.account_id)
                if item.command_id == command.command_id
                and item.protection_id == tracking.protection_id
            ),
            None,
        )
        return tracking.protection_id, None if work is None else work.work_id



def project_live_account(
    account_id: str,
    events: tuple[LiveRecordEvent, ...],
) -> LiveAccountSnapshot:
    """仅投影已确认成交；提案和取消永远不会改变持仓。"""

    positions: dict[str, LivePosition] = {}
    total_fees = Decimal("0")
    confirmed = 0
    updated_at: datetime | None = None
    last_sequence = 0
    for event in events:
        last_sequence = event.sequence
        updated_at = event.occurred_at
        if event.event_type is not LiveRecordEventType.FILL_CONFIRMED:
            continue
        fill = _fill_from_document(_mapping(_json_object(event.payload_json), "fill"))
        current = positions.get(fill.symbol)
        quantity = 0 if current is None else current.quantity
        average = Decimal("0") if current is None else current.average_cost
        realized = Decimal("0") if current is None else current.realized_pnl
        if fill.side is Side.BUY:
            new_quantity = quantity + fill.quantity
            average = (average * quantity + fill.trade_value + fill.fees.total) / new_quantity
            quantity = new_quantity
        else:
            if quantity < fill.quantity:
                raise LiveTradeRecordError("RECORDED_POSITION_INSUFFICIENT")
            realized += fill.trade_value - fill.fees.total - average * fill.quantity
            quantity -= fill.quantity
            if quantity == 0:
                average = Decimal("0")
        total_fees += fill.fees.total
        positions[fill.symbol] = LivePosition(
            symbol=fill.symbol,
            instrument_type=fill.instrument_type,
            quantity=quantity,
            average_cost=average,
            realized_pnl=realized,
        )
        confirmed += 1
    return LiveAccountSnapshot(
        account_id=account_id.strip(),
        positions=tuple(sorted(positions.values(), key=lambda item: item.symbol)),
        confirmed_fill_count=confirmed,
        total_fees=total_fees,
        updated_at=updated_at,
        last_sequence=last_sequence,
    )


def _terminal_replay_outcome(
    command: StoredLiveCommand,
    terminal: LiveRecordEvent,
    *,
    protection_identity: tuple[str | None, str | None] = (None, None),
) -> LiveInboundOutcome:
    fill = _fill_from_document(_json_object(command.fill_json))
    confirmed = terminal.event_type is LiveRecordEventType.FILL_CONFIRMED
    return LiveInboundOutcome(
        status=LiveInboundOutcomeStatus.REPLAYED,
        command_id=fill.command_id,
        account_id=command.account_id,
        fingerprint=fill.fingerprint,
        event_sequence=terminal.sequence,
        analysis_required=False,
        response_text=(
            _confirmed_replay_receipt(fill)
            if confirmed
            else _cancelled_execution_receipt(fill, replayed=True)
        ),
        protection_id=protection_identity[0] if confirmed else None,
        protection_work_id=protection_identity[1] if confirmed else None,
    )


def _execution_receipt(
    fill: ConfirmedLiveFill,
    *,
    position_quantity: int,
    protection_work_id: str | None,
) -> str:
    """用统一成交回执契约生成不夸大后台状态的中文回复。"""

    side_name = "买入" if fill.side is Side.BUY else "卖出"
    protection = (
        "保护分析与持续跟踪工作已在同一事务中持久排队；"
        f"工作号 {protection_work_id}。成交事务提交时仅表示任务可恢复，"
        "随后一次有限 QUICK/跟踪尝试的真实结果见即时保护回执。"
        if protection_work_id is not None
        else "本次为已执行卖出同步；系统不会代替用户向券商下单。"
    )
    return render_stable_text_report(
        ReportKind.EXECUTION_RECEIPT,
        title="实盘观察账本成交回执",
        sections={
            "成交事实": (
                f"已确认外部券商{side_name}成交：{fill.symbol} "
                f"{fill.quantity} 股，成交价 {fill.price}；命令号 {fill.command_id}；"
                f"{_broker_fill_identity_text(fill)}。"
            ),
            "费用与资金": (
                f"佣金 {fill.fees.commission}，过户费 {fill.fees.transfer_fee}，"
                f"印花税 {fill.fees.stamp_tax}。本账本只观察成交，不推断券商现金余额。"
            ),
            "持仓变化": f"该标的实盘观察数量现为 {position_quantity} 股。",
            "后续保护计划": protection,
        },
    )


def _pending_execution_receipt(
    fill: ConfirmedLiveFill,
    *,
    replayed: bool,
) -> str:
    confirmation = f"{_CONFIRM_PREFIX}|command_id={fill.command_id}|fingerprint={fill.fingerprint}"
    return render_stable_text_report(
        ReportKind.EXECUTION_RECEIPT,
        title="实盘成交待确认回执",
        sections={
            "成交事实": (
                f"已暂存{fill.symbol} {fill.quantity} 股、价格 {fill.price} 的外部成交声明；"
                f"{_broker_fill_identity_text(fill)}；"
                + ("本次为同一消息幂等重放。" if replayed else "当前尚未确认。")
                + f"核对后发送：{confirmation}"
            ),
            "费用与资金": (
                f"声明佣金 {fill.fees.commission}、过户费 {fill.fees.transfer_fee}、"
                f"印花税 {fill.fees.stamp_tax}；确认前均未计入账户。"
            ),
            "持仓变化": "持仓没有变化；待确认提案绝不进入实盘观察库存。",
            "后续保护计划": "尚未创建保护任务；仅在正确的第二条确认消息提交后持久排队。",
        },
    )


def _cancelled_execution_receipt(
    fill: ConfirmedLiveFill,
    *,
    replayed: bool,
) -> str:
    return render_stable_text_report(
        ReportKind.EXECUTION_RECEIPT,
        title="实盘成交取消回执",
        sections={
            "成交事实": (
                f"命令 {fill.command_id} 的待确认外部成交声明已取消。"
                + ("本次为终态幂等重放。" if replayed else "")
            ),
            "费用与资金": "没有费用或资金变化。",
            "持仓变化": "没有写入成交，实盘观察持仓不变。",
            "后续保护计划": "没有创建保护分析或持续跟踪任务。",
        },
    )


def _confirmed_replay_receipt(fill: ConfirmedLiveFill) -> str:
    return render_stable_text_report(
        ReportKind.EXECUTION_RECEIPT,
        title="实盘成交幂等回执",
        sections={
            "成交事实": (f"命令 {fill.command_id} 已经确认；本次消息是重放，未重复写入成交。"),
            "费用与资金": "沿用原确认成交记录，本次没有再次计费或改变资金投影。",
            "持仓变化": "沿用原确认后的持仓，本次没有重复增减数量。",
            "后续保护计划": (
                "买入原有持久保护工作不会重复创建。"
                if fill.side is Side.BUY
                else "卖出原有保护关闭工作不会重复创建。"
            ),
        },
    )


def _broker_fill_identity_text(fill: ConfirmedLiveFill) -> str:
    order_id = fill.external_order_id or "未提供"
    execution_id = fill.external_fill_id or "历史记录未提供"
    return f"券商委托号 {order_id}，成交执行号 {execution_id}"


def _fill_from_document(document: Mapping[str, object]) -> ConfirmedLiveFill:
    fees = _mapping(document, "fees")
    return ConfirmedLiveFill(
        command_id=_string(document, "command_id"),
        account_id=_string(document, "account_id"),
        side=Side(_string(document, "side")),
        symbol=_string(document, "symbol"),
        quantity=_integer(document, "quantity"),
        price=Decimal(_string(document, "price")),
        instrument_type=PaperInstrumentType(_string(document, "instrument_type")),
        executed_at=datetime.fromisoformat(_string(document, "executed_at")),
        fees=PaperFillFees(
            commission=Decimal(_string(fees, "commission")),
            transfer_fee=Decimal(_string(fees, "transfer_fee")),
            stamp_tax=Decimal(_string(fees, "stamp_tax")),
        ),
        external_order_id=_optional_string(document.get("external_order_id")),
        external_fill_id=_optional_string(document.get("external_fill_id")),
    )


def _message_event_id(message: OneBotPrivateMessage, suffix: str) -> str:
    return f"live-{suffix}-{message.self_id}-{message.message_id}"


def _message_key(message: OneBotPrivateMessage) -> str:
    return f"onebot:{message.self_id}:{message.message_id}"


def _canonical_json(document: Mapping[str, object]) -> str:
    return json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_object(payload: str) -> dict[str, object]:
    value = json.loads(payload)
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise LiveTradeRecordError("STORED_EVENT_INVALID")
    return cast(dict[str, object], value)


def _mapping(document: Mapping[str, object], name: str) -> dict[str, object]:
    value = document.get(name)
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise LiveTradeRecordError("STORED_EVENT_INVALID")
    return cast(dict[str, object], value)


def _string(document: Mapping[str, object], name: str) -> str:
    value = document.get(name)
    if not isinstance(value, str) or not value:
        raise LiveTradeRecordError("STORED_EVENT_INVALID")
    return value


def _integer(document: Mapping[str, object], name: str) -> int:
    value = document.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise LiveTradeRecordError("STORED_EVENT_INVALID")
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise LiveTradeRecordError("STORED_EVENT_INVALID")
    return value


def _qq_id(value: str) -> str:
    text = str(value).strip()
    if not text.isascii() or not text.isdecimal() or int(text) <= 0:
        raise ValueError("QQ sender identifiers must be positive decimal integers")
    return str(int(text))


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("timestamp must be datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
