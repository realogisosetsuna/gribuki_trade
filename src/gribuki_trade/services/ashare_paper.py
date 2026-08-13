"""Deterministic A-share paper-account application service.

This service records executions only.  It never contacts a broker or market
data provider and it does not invent suspension, price-limit, queue-position or
liquidity behavior.  A future simulator may produce ``ASharePaperFill`` values;
manual broker confirmations already use the same contract today.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import cast
from uuid import uuid4

from gribuki_trade.backtest.costs import (
    InstrumentType,
    TradingCostConfig,
    calculate_trade_cost,
)
from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import (
    AppliedPaperFill,
    ASharePaperFill,
    NewPaperLedgerEvent,
    PaperAccountSnapshot,
    PaperFeeSchedule,
    PaperFillFees,
    PaperFillReceipt,
    PaperFillSource,
    PaperInstrumentType,
    PaperLedgerEvent,
    PaperLedgerEventType,
    PaperPosition,
)
from gribuki_trade.ports.paper_ledger import PaperLedger

_SCHEMA_VERSION = 1


class ASharePaperError(RuntimeError):
    """Base class for paper-account command failures."""


class PaperAccountNotFoundError(ASharePaperError):
    """The requested account has no opening event."""


class PaperAccountAlreadyExistsError(ASharePaperError):
    """An opening command conflicts with an existing account."""


class InsufficientPaperCashError(ASharePaperError):
    """A buy would make available cash negative."""


class InsufficientAvailablePositionError(ASharePaperError):
    """A sell exceeds the T+1-available share balance."""


class PaperFillConflictError(ASharePaperError):
    """A fill identifier was reused with different execution content."""


class PaperSessionError(ASharePaperError):
    """A fill or rollover violates explicit trading-session ordering."""


class PaperProjectionError(ASharePaperError):
    """Stored events cannot produce a valid deterministic projection."""


@dataclass(slots=True)
class _MutablePosition:
    symbol: str
    instrument_type: PaperInstrumentType
    quantity: int = 0
    available_to_sell: int = 0
    today_buy: int = 0
    average_cost: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")


@dataclass(slots=True)
class _Projection:
    account_id: str
    session_date: date
    cash: Decimal
    positions: dict[str, _MutablePosition]
    opened_at: datetime
    updated_at: datetime
    last_sequence: int


class ASharePaperTradingService:
    """Append commands and rebuild balances from an immutable local ledger."""

    def __init__(
        self,
        ledger: PaperLedger,
        *,
        fee_schedule: PaperFeeSchedule | None = None,
    ) -> None:
        self._ledger = ledger
        self._fees = fee_schedule or PaperFeeSchedule()

    @property
    def fee_schedule(self) -> PaperFeeSchedule:
        return self._fees

    def open_account(
        self,
        account_id: str,
        *,
        initial_cash: Decimal,
        session_date: date,
        opened_at: datetime,
    ) -> PaperAccountSnapshot:
        """Create an account once; an exact replay is idempotent."""

        normalized_account_id = account_id.strip()
        initial_cash = _money(initial_cash, self._fees)
        if initial_cash < 0:
            raise ValueError("initial_cash must be non-negative")
        opened_at = _aware_utc(opened_at, "opened_at")
        payload = _canonical_json(
            {
                "initial_cash": str(initial_cash),
                "opened_at": opened_at.isoformat(),
                "schema_version": _SCHEMA_VERSION,
            }
        )
        existing = self._ledger.events(normalized_account_id)
        if existing:
            first = existing[0]
            if (
                first.event_type is PaperLedgerEventType.ACCOUNT_OPENED
                and first.session_date == session_date
                and first.payload_json == payload
            ):
                return replay_paper_account(existing)
            raise PaperAccountAlreadyExistsError(
                f"paper account {normalized_account_id!r} already exists"
            )
        event = NewPaperLedgerEvent(
            event_id=f"paper-open-{uuid4()}",
            account_id=normalized_account_id,
            event_type=PaperLedgerEventType.ACCOUNT_OPENED,
            occurred_at=opened_at,
            session_date=session_date,
            idempotency_key="account-opened:v1",
            payload_json=payload,
        )
        self._ledger.append(event, expected_sequence=0)
        return self.snapshot(normalized_account_id)

    def snapshot(self, account_id: str) -> PaperAccountSnapshot:
        events = self._ledger.events(account_id)
        if not events:
            raise PaperAccountNotFoundError(f"unknown paper account: {account_id!r}")
        return replay_paper_account(events)

    def rollover_session(
        self,
        account_id: str,
        *,
        target_session_date: date,
        occurred_at: datetime,
    ) -> PaperAccountSnapshot:
        """Make all prior-session buys sellable in a caller-validated session.

        This method deliberately does not guess exchange holidays.  Callers must
        invoke it only with a known trading session from their calendar source.
        """

        occurred_at = _aware_utc(occurred_at, "occurred_at")
        events = self._required_events(account_id)
        current = replay_paper_account(events)
        idempotency_key = f"session-rollover:{target_session_date.isoformat()}"
        prior = self._ledger.event_by_idempotency_key(account_id, idempotency_key)
        if prior is not None:
            return current
        if target_session_date == current.session_date:
            return current
        if target_session_date < current.session_date:
            raise PaperSessionError(
                f"cannot roll account back from {current.session_date} to {target_session_date}"
            )
        if occurred_at < current.updated_at:
            raise PaperSessionError("rollover occurred_at precedes the latest ledger event")
        payload = _canonical_json(
            {
                "from_session_date": current.session_date.isoformat(),
                "schema_version": _SCHEMA_VERSION,
                "to_session_date": target_session_date.isoformat(),
            }
        )
        self._ledger.append(
            NewPaperLedgerEvent(
                event_id=f"paper-rollover-{uuid4()}",
                account_id=current.account_id,
                event_type=PaperLedgerEventType.SESSION_ROLLED_OVER,
                occurred_at=occurred_at,
                session_date=target_session_date,
                idempotency_key=idempotency_key,
                payload_json=payload,
            ),
            expected_sequence=current.last_sequence,
        )
        return self.snapshot(account_id)

    def record_fill(
        self,
        fill: ASharePaperFill,
        *,
        recorded_at: datetime | None = None,
    ) -> PaperFillReceipt:
        """Record one manual or simulated fill through the same contract."""

        events = self._required_events(fill.account_id)
        current = replay_paper_account(events)
        idempotency_key = f"fill:{fill.fill_id}"
        duplicate = self._ledger.event_by_idempotency_key(fill.account_id, idempotency_key)
        if duplicate is not None:
            stored = _parse_applied_fill(duplicate.payload_json)
            if stored.fill != fill:
                raise PaperFillConflictError(
                    f"fill_id {fill.fill_id!r} is already bound to different content"
                )
            return PaperFillReceipt(
                applied_new=False,
                event_sequence=duplicate.sequence,
                applied_fill=stored,
                snapshot=current,
            )
        if fill.trading_date != current.session_date:
            raise PaperSessionError(
                f"fill session {fill.trading_date} does not match account session "
                f"{current.session_date}; roll over explicitly first"
            )
        resolved_recorded_at = _aware_utc(
            recorded_at or datetime.now(UTC), "recorded_at"
        )
        if resolved_recorded_at < fill.executed_at:
            raise ValueError("recorded_at must not precede executed_at")
        if resolved_recorded_at < current.updated_at:
            raise ValueError("recorded_at must not precede the latest ledger event")

        prior_fills = tuple(
            _parse_applied_fill(event.payload_json)
            for event in events
            if event.event_type is PaperLedgerEventType.FILL_RECORDED
        )
        applied = self._price_fill(current, fill, prior_fills=prior_fills)
        event, applied_new = self._ledger.append(
            NewPaperLedgerEvent(
                event_id=f"paper-fill-{uuid4()}",
                account_id=fill.account_id,
                event_type=PaperLedgerEventType.FILL_RECORDED,
                occurred_at=resolved_recorded_at,
                session_date=fill.trading_date,
                idempotency_key=idempotency_key,
                payload_json=_canonical_json(_applied_fill_document(applied)),
            ),
            expected_sequence=current.last_sequence,
        )
        refreshed = self.snapshot(fill.account_id)
        return PaperFillReceipt(
            applied_new=applied_new,
            event_sequence=event.sequence,
            applied_fill=applied,
            snapshot=refreshed,
        )

    def fills(self, account_id: str) -> tuple[AppliedPaperFill, ...]:
        return tuple(
            _parse_applied_fill(event.payload_json)
            for event in self._required_events(account_id)
            if event.event_type is PaperLedgerEventType.FILL_RECORDED
        )

    def _price_fill(
        self,
        snapshot: PaperAccountSnapshot,
        fill: ASharePaperFill,
        *,
        prior_fills: tuple[AppliedPaperFill, ...],
    ) -> AppliedPaperFill:
        config = _cost_config(self._fees)
        calculated = calculate_trade_cost(
            side=fill.side,
            price=fill.price,
            quantity=fill.quantity,
            instrument_type=InstrumentType(fill.instrument_type.value),
            config=config,
        )
        fees = fill.fee_override or PaperFillFees(
            commission=_simulated_commission_increment(
                fill=fill,
                default_commission=calculated.commission,
                prior_fills=prior_fills,
                schedule=self._fees,
            ),
            transfer_fee=calculated.transfer_fee,
            stamp_tax=calculated.stamp_tax,
        )
        _require_fee_quantization(fees, self._fees)
        cash_change = (
            -(calculated.trade_value + fees.total)
            if fill.side is Side.BUY
            else calculated.trade_value - fees.total
        )
        position = snapshot.position(fill.symbol)
        if position is not None and position.instrument_type is not fill.instrument_type:
            raise PaperFillConflictError(
                f"instrument type for {fill.symbol} conflicts with account history"
            )
        if fill.side is Side.BUY:
            if snapshot.cash + cash_change < 0:
                raise InsufficientPaperCashError(
                    f"buy requires {-cash_change} CNY but only {snapshot.cash} is available"
                )
            realized = Decimal("0")
        else:
            available = position.available_to_sell if position is not None else 0
            if available < fill.quantity:
                raise InsufficientAvailablePositionError(
                    f"sell requires {fill.quantity} shares but only {available} are T+1 available"
                )
            if cash_change < 0:
                raise ASharePaperError("sell fees exceed trade value")
            if position is None:  # pragma: no cover - guarded by available check
                raise PaperProjectionError("available position disappeared")
            realized = cash_change - position.average_cost * fill.quantity
        return AppliedPaperFill(
            fill=fill,
            trade_value=calculated.trade_value,
            fees=fees,
            cash_change=cash_change,
            realized_pnl_change=realized,
        )

    def _required_events(self, account_id: str) -> tuple[PaperLedgerEvent, ...]:
        events = self._ledger.events(account_id)
        if not events:
            raise PaperAccountNotFoundError(f"unknown paper account: {account_id!r}")
        return events


def replay_paper_account(events: tuple[PaperLedgerEvent, ...]) -> PaperAccountSnapshot:
    """Pure projector used by both normal reads and restart/recovery tests."""

    if not events:
        raise PaperAccountNotFoundError("cannot replay an empty event stream")
    first = events[0]
    if first.event_type is not PaperLedgerEventType.ACCOUNT_OPENED:
        raise PaperProjectionError("the first account event must be ACCOUNT_OPENED")
    opening = _document(first.payload_json)
    _schema(opening)
    opened_at = _datetime_field(opening, "opened_at")
    if opened_at != first.occurred_at:
        raise PaperProjectionError("opening payload timestamp does not match event timestamp")
    projection = _Projection(
        account_id=first.account_id,
        session_date=first.session_date,
        cash=_non_negative_decimal_field(opening, "initial_cash"),
        positions={},
        opened_at=opened_at,
        updated_at=first.occurred_at,
        last_sequence=first.sequence,
    )
    prior_sequence = 0
    for index, event in enumerate(events):
        if event.account_id != projection.account_id:
            raise PaperProjectionError("event stream mixes account identifiers")
        if event.sequence <= prior_sequence:
            raise PaperProjectionError("ledger sequences must be strictly increasing")
        prior_sequence = event.sequence
        if index == 0:
            continue
        if event.event_type is PaperLedgerEventType.ACCOUNT_OPENED:
            raise PaperProjectionError("account stream contains multiple opening events")
        if event.occurred_at < projection.updated_at:
            raise PaperProjectionError("ledger event timestamps must be non-decreasing")
        if event.event_type is PaperLedgerEventType.SESSION_ROLLED_OVER:
            _apply_rollover(projection, event)
        elif event.event_type is PaperLedgerEventType.FILL_RECORDED:
            _apply_stored_fill(projection, event)
        else:  # pragma: no cover - enum exhaustiveness
            raise PaperProjectionError(f"unsupported paper event: {event.event_type}")
        projection.updated_at = event.occurred_at
        projection.last_sequence = event.sequence
    return _snapshot(projection)


def _apply_rollover(projection: _Projection, event: PaperLedgerEvent) -> None:
    document = _document(event.payload_json)
    _schema(document)
    from_session = _date_field(document, "from_session_date")
    to_session = _date_field(document, "to_session_date")
    if from_session != projection.session_date or to_session != event.session_date:
        raise PaperProjectionError("rollover payload conflicts with projected session")
    if to_session <= from_session:
        raise PaperProjectionError("rollover target must be a later trading session")
    projection.session_date = to_session
    for position in projection.positions.values():
        position.available_to_sell = position.quantity
        position.today_buy = 0


def _apply_stored_fill(projection: _Projection, event: PaperLedgerEvent) -> None:
    applied = _parse_applied_fill(event.payload_json)
    fill = applied.fill
    if fill.account_id != projection.account_id:
        raise PaperProjectionError("fill account does not match event stream")
    if fill.trading_date != projection.session_date or event.session_date != fill.trading_date:
        raise PaperProjectionError("fill event does not match projected trading session")
    expected_cash_change = (
        -(applied.trade_value + applied.fees.total)
        if fill.side is Side.BUY
        else applied.trade_value - applied.fees.total
    )
    if applied.cash_change != expected_cash_change:
        raise PaperProjectionError("stored fill cash effect is inconsistent")
    position = projection.positions.get(fill.symbol)
    if position is None:
        position = _MutablePosition(
            symbol=fill.symbol,
            instrument_type=fill.instrument_type,
        )
        projection.positions[fill.symbol] = position
    elif position.instrument_type is not fill.instrument_type:
        raise PaperProjectionError("stored fill changes instrument type")

    if fill.side is Side.BUY:
        if projection.cash + applied.cash_change < 0:
            raise PaperProjectionError("stored buy would make cash negative")
        if applied.realized_pnl_change != 0:
            raise PaperProjectionError("buy fill cannot realize profit or loss")
        prior_cost = position.average_cost * position.quantity
        position.quantity += fill.quantity
        position.today_buy += fill.quantity
        position.average_cost = (
            prior_cost + applied.trade_value + applied.fees.total
        ) / position.quantity
    else:
        if position.available_to_sell < fill.quantity:
            raise PaperProjectionError("stored sell exceeds T+1 available position")
        expected_realized = (
            applied.trade_value
            - applied.fees.total
            - position.average_cost * fill.quantity
        )
        if applied.realized_pnl_change != expected_realized:
            raise PaperProjectionError("stored realized PnL is inconsistent")
        position.quantity -= fill.quantity
        position.available_to_sell -= fill.quantity
        position.realized_pnl += applied.realized_pnl_change
        if position.quantity == 0:
            position.average_cost = Decimal("0")
    projection.cash += applied.cash_change
    if projection.cash < 0:
        raise PaperProjectionError("stored event stream produces negative cash")


def _snapshot(projection: _Projection) -> PaperAccountSnapshot:
    positions = tuple(
        PaperPosition(
            symbol=position.symbol,
            instrument_type=position.instrument_type,
            quantity=position.quantity,
            available_to_sell=position.available_to_sell,
            today_buy=position.today_buy,
            average_cost=position.average_cost,
            realized_pnl=position.realized_pnl,
        )
        for position in sorted(projection.positions.values(), key=lambda item: item.symbol)
    )
    return PaperAccountSnapshot(
        account_id=projection.account_id,
        session_date=projection.session_date,
        cash=projection.cash,
        positions=positions,
        opened_at=projection.opened_at,
        updated_at=projection.updated_at,
        last_sequence=projection.last_sequence,
    )


def _cost_config(schedule: PaperFeeSchedule) -> TradingCostConfig:
    return TradingCostConfig(
        commission_rate=schedule.commission_rate,
        minimum_commission_cny=schedule.minimum_commission_cny,
        stock_sell_stamp_tax_rate=schedule.stock_sell_stamp_tax_rate,
        stock_transfer_fee_rate=schedule.stock_transfer_fee_rate,
        stock_slippage_rate=Decimal("0"),
        etf_sell_stamp_tax_rate=schedule.etf_sell_stamp_tax_rate,
        etf_transfer_fee_rate=schedule.etf_transfer_fee_rate,
        etf_slippage_rate=Decimal("0"),
        currency_quantum=schedule.currency_quantum,
    )


def _simulated_commission_increment(
    *,
    fill: ASharePaperFill,
    default_commission: Decimal,
    prior_fills: tuple[AppliedPaperFill, ...],
    schedule: PaperFeeSchedule,
) -> Decimal:
    """Return this fill's share of an order-level minimum commission.

    Historical events retain the already charged commission, so replays are
    stable when fee configuration later changes.  Only simulated fills with an
    explicit external order identifier participate in aggregation; standalone
    and manual fills preserve the original transaction-level behavior.
    """

    if (
        fill.source is not PaperFillSource.SIMULATED
        or fill.external_order_id is None
    ):
        return default_commission
    matching = tuple(
        applied
        for applied in prior_fills
        if applied.fill.source is PaperFillSource.SIMULATED
        and applied.fill.external_order_id == fill.external_order_id
    )
    for applied in matching:
        prior = applied.fill
        if (
            prior.account_id != fill.account_id
            or prior.symbol != fill.symbol
            or prior.side is not fill.side
            or prior.instrument_type is not fill.instrument_type
        ):
            raise PaperFillConflictError(
                "external_order_id is already bound to a different simulated order"
            )
    prior_trade_value = sum(
        (
            applied.fill.price * applied.fill.quantity
            for applied in matching
        ),
        start=Decimal("0"),
    )
    prior_commission = sum(
        (applied.fees.commission for applied in matching),
        start=Decimal("0"),
    )
    target_cumulative = _money(
        max(
            (prior_trade_value + fill.price * fill.quantity)
            * schedule.commission_rate,
            schedule.minimum_commission_cny,
        ),
        schedule,
    )
    increment = target_cumulative - prior_commission
    if increment < 0:
        raise PaperProjectionError(
            "stored simulated commissions exceed the current order-level schedule"
        )
    return increment


def _require_fee_quantization(fees: PaperFillFees, schedule: PaperFeeSchedule) -> None:
    for name in ("commission", "transfer_fee", "stamp_tax"):
        value = cast(Decimal, getattr(fees, name))
        if _money(value, schedule) != value:
            raise ValueError(f"fee_override {name} must align with currency_quantum")


def _applied_fill_document(applied: AppliedPaperFill) -> dict[str, object]:
    fill = applied.fill
    return {
        "cash_change": str(applied.cash_change),
        "fees": {
            "commission": str(applied.fees.commission),
            "stamp_tax": str(applied.fees.stamp_tax),
            "transfer_fee": str(applied.fees.transfer_fee),
        },
        "fill": {
            "account_id": fill.account_id,
            "executed_at": fill.executed_at.isoformat(),
            "external_order_id": fill.external_order_id,
            "fee_override": _fee_document(fill.fee_override),
            "fill_id": fill.fill_id,
            "instrument_type": fill.instrument_type.value,
            "note": fill.note,
            "price": str(fill.price),
            "quantity": fill.quantity,
            "side": fill.side.value,
            "source": fill.source.value,
            "symbol": fill.symbol,
            "trading_date": fill.trading_date.isoformat(),
        },
        "realized_pnl_change": str(applied.realized_pnl_change),
        "schema_version": _SCHEMA_VERSION,
        "trade_value": str(applied.trade_value),
    }


def _fee_document(fees: PaperFillFees | None) -> dict[str, object] | None:
    if fees is None:
        return None
    return {
        "commission": str(fees.commission),
        "stamp_tax": str(fees.stamp_tax),
        "transfer_fee": str(fees.transfer_fee),
    }


def _parse_applied_fill(payload_json: str) -> AppliedPaperFill:
    document = _document(payload_json)
    _schema(document)
    fill_document = _mapping_field(document, "fill")
    override_value = fill_document.get("fee_override")
    fee_override = (
        None
        if override_value is None
        else _parse_fees(_as_mapping(override_value, "fee_override"))
    )
    fill = ASharePaperFill(
        account_id=_string_field(fill_document, "account_id"),
        fill_id=_string_field(fill_document, "fill_id"),
        symbol=_string_field(fill_document, "symbol"),
        side=Side(_string_field(fill_document, "side")),
        quantity=_integer_field(fill_document, "quantity"),
        price=_decimal_field(fill_document, "price"),
        instrument_type=PaperInstrumentType(
            _string_field(fill_document, "instrument_type")
        ),
        trading_date=_date_field(fill_document, "trading_date"),
        executed_at=_datetime_field(fill_document, "executed_at"),
        source=PaperFillSource(_string_field(fill_document, "source")),
        fee_override=fee_override,
        external_order_id=_optional_string_field(fill_document, "external_order_id"),
        note=_optional_string_field(fill_document, "note"),
    )
    return AppliedPaperFill(
        fill=fill,
        trade_value=_decimal_field(document, "trade_value"),
        fees=_parse_fees(_mapping_field(document, "fees")),
        cash_change=_decimal_field(document, "cash_change"),
        realized_pnl_change=_decimal_field(document, "realized_pnl_change"),
    )


def _parse_fees(document: dict[str, object]) -> PaperFillFees:
    return PaperFillFees(
        commission=_decimal_field(document, "commission"),
        transfer_fee=_decimal_field(document, "transfer_fee"),
        stamp_tax=_decimal_field(document, "stamp_tax"),
    )


def _canonical_json(document: dict[str, object]) -> str:
    return json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _document(payload_json: str) -> dict[str, object]:
    try:
        value = json.loads(payload_json)
    except (TypeError, json.JSONDecodeError) as error:
        raise PaperProjectionError("paper ledger payload is not valid JSON") from error
    return _as_mapping(value, "payload")


def _as_mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise PaperProjectionError(f"{name} must be a JSON object")
    return cast(dict[str, object], value)


def _schema(document: dict[str, object]) -> None:
    if document.get("schema_version") != _SCHEMA_VERSION:
        raise PaperProjectionError("unsupported paper ledger payload schema")


def _mapping_field(document: dict[str, object], name: str) -> dict[str, object]:
    if name not in document:
        raise PaperProjectionError(f"paper ledger payload is missing {name}")
    return _as_mapping(document[name], name)


def _string_field(document: dict[str, object], name: str) -> str:
    value = document.get(name)
    if not isinstance(value, str) or not value:
        raise PaperProjectionError(f"paper ledger field {name} must be a string")
    return value


def _optional_string_field(document: dict[str, object], name: str) -> str | None:
    value = document.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise PaperProjectionError(f"paper ledger field {name} must be a string or null")
    return value


def _integer_field(document: dict[str, object], name: str) -> int:
    value = document.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise PaperProjectionError(f"paper ledger field {name} must be an integer")
    return value


def _decimal_field(document: dict[str, object], name: str) -> Decimal:
    value = document.get(name)
    if not isinstance(value, str):
        raise PaperProjectionError(f"paper ledger field {name} must be a decimal string")
    try:
        result = Decimal(value)
    except Exception as error:
        raise PaperProjectionError(f"paper ledger field {name} is invalid") from error
    if not result.is_finite():
        raise PaperProjectionError(f"paper ledger field {name} must be finite")
    return result


def _non_negative_decimal_field(document: dict[str, object], name: str) -> Decimal:
    result = _decimal_field(document, name)
    if result < 0:
        raise PaperProjectionError(f"paper ledger field {name} must be non-negative")
    return result


def _date_field(document: dict[str, object], name: str) -> date:
    try:
        return date.fromisoformat(_string_field(document, name))
    except ValueError as error:
        raise PaperProjectionError(f"paper ledger field {name} is not an ISO date") from error


def _datetime_field(document: dict[str, object], name: str) -> datetime:
    try:
        value = datetime.fromisoformat(_string_field(document, name))
    except ValueError as error:
        raise PaperProjectionError(f"paper ledger field {name} is not ISO datetime") from error
    return _aware_utc(value, name)


def _money(value: Decimal, schedule: PaperFeeSchedule) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError("money values must be Decimal")
    if not value.is_finite():
        raise ValueError("money values must be finite")
    return value.quantize(schedule.currency_quantum, rounding=ROUND_HALF_UP)


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)
