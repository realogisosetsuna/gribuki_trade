"""Conservative deterministic daily-bar matcher for A-share PAPER orders.

The matcher models neither an order-book queue nor broker acceptance latency.
It consumes only explicit, unadjusted OHLC/volume/price-band inputs and sends
deterministic fills through :class:`ASharePaperTradingService`.

Order state is process-local in this class.  The durable wrapper in
``ashare_paper_recovery`` restores snapshots and bar-run sagas around it.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import date, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from gribuki_trade.backtest.costs import (
    InstrumentType,
    TradingCostConfig,
    calculate_trade_cost,
)
from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_orders import (
    ASharePaperOrderIntent,
    PaperBarMatchRun,
    PaperMatchingConfig,
    PaperMatchReason,
    PaperOrderMatchOutcome,
    PaperOrderSnapshot,
    PaperOrderStatus,
    PaperTimeInForce,
    SimulatedDailyBar,
)
from gribuki_trade.domain.paper_trading import (
    ASharePaperFill,
    PaperFillSource,
    PaperInstrumentType,
)
from gribuki_trade.services.ashare_paper import (
    ASharePaperError,
    ASharePaperTradingService,
    PaperAccountNotFoundError,
)


class PaperMatchingError(RuntimeError):
    """Base class for deterministic paper matching failures."""


class PaperOrderConflictError(PaperMatchingError):
    """A client order identifier was reused with different contents."""


class PaperBarConflictError(PaperMatchingError):
    """The same symbol/session was replayed with a different bar revision."""


class PaperMatchingSequenceError(PaperMatchingError):
    """A bar or command timestamp would move state backwards."""


class ASharePaperOrderMatcher:
    """Process-local FIFO paper order book backed by the durable fill ledger."""

    def __init__(
        self,
        account_service: ASharePaperTradingService,
        *,
        config: PaperMatchingConfig | None = None,
    ) -> None:
        self._accounts = account_service
        self._config = config or PaperMatchingConfig()
        self._orders: dict[str, PaperOrderSnapshot] = {}
        self._runs: dict[tuple[str, date], PaperBarMatchRun] = {}

    @property
    def config(self) -> PaperMatchingConfig:
        return self._config

    def submit_order(self, intent: ASharePaperOrderIntent) -> PaperOrderSnapshot:
        """Validate and reserve a conservative cash/share budget.

        Exact repeated submission is idempotent.  Business rejections are
        retained as terminal order snapshots rather than raised as exceptions.
        """

        client_order_id = intent.order.client_order_id
        existing = self._orders.get(client_order_id)
        if existing is not None:
            if existing.intent != intent:
                raise PaperOrderConflictError(
                    f"client_order_id {client_order_id!r} already has different content"
                )
            return existing
        if any(
            symbol == intent.order.symbol
            and trade_date > intent.decision_session_date
            for symbol, trade_date in self._runs
        ):
            raise PaperMatchingSequenceError(
                "cannot backfill an order after an eligible symbol bar was processed"
            )
        submitted_at = intent.order.created_at
        tick = self._price_quantum(intent.instrument_type)
        if intent.order.limit_price % tick != 0:
            return self._store_rejection(
                intent,
                PaperMatchReason.LIMIT_PRICE_NOT_TICK_ALIGNED,
                submitted_at,
            )
        if (
            intent.order.side is Side.BUY
            and intent.quantity % self._config.buy_lot_size != 0
        ):
            return self._store_rejection(
                intent,
                PaperMatchReason.BUY_QUANTITY_NOT_ROUND_LOT,
                submitted_at,
            )
        try:
            account = self._accounts.snapshot(intent.order.account_id)
        except PaperAccountNotFoundError:
            return self._store_rejection(
                intent,
                PaperMatchReason.ACCOUNT_LEDGER_REJECTED,
                submitted_at,
            )
        if account.session_date > intent.decision_session_date:
            return self._store_rejection(
                intent,
                PaperMatchReason.ACCOUNT_SESSION_AHEAD,
                submitted_at,
            )

        reserved_cash = Decimal("0")
        reserved_quantity = 0
        if intent.order.side is Side.BUY:
            reserved_cash = self._worst_case_buy_cash(intent)
            other_reserved_cash = sum(
                (
                    order.reserved_cash
                    for order in self._orders.values()
                    if order.is_open
                    and order.intent.order.account_id == intent.order.account_id
                    and order.intent.order.side is Side.BUY
                ),
                start=Decimal("0"),
            )
            if account.cash - other_reserved_cash < reserved_cash:
                return self._store_rejection(
                    intent,
                    PaperMatchReason.INSUFFICIENT_CASH_BUDGET,
                    submitted_at,
                )
        else:
            position = account.position(intent.order.symbol)
            available = position.available_to_sell if position is not None else 0
            other_reserved_quantity = sum(
                order.reserved_quantity
                for order in self._orders.values()
                if order.is_open
                and order.intent.order.account_id == intent.order.account_id
                and order.intent.order.symbol == intent.order.symbol
                and order.intent.order.side is Side.SELL
            )
            if available - other_reserved_quantity < intent.quantity:
                return self._store_rejection(
                    intent,
                    PaperMatchReason.INSUFFICIENT_AVAILABLE_POSITION,
                    submitted_at,
                )
            reserved_quantity = intent.quantity

        snapshot = PaperOrderSnapshot(
            intent=intent,
            status=PaperOrderStatus.PENDING,
            filled_quantity=0,
            average_fill_price=None,
            reserved_cash=reserved_cash,
            reserved_quantity=reserved_quantity,
            submitted_at=submitted_at,
            updated_at=submitted_at,
        )
        self._orders[client_order_id] = snapshot
        return snapshot

    def order(self, client_order_id: str) -> PaperOrderSnapshot | None:
        return self._orders.get(client_order_id)

    def orders(self, *, account_id: str | None = None) -> tuple[PaperOrderSnapshot, ...]:
        values = tuple(self._orders.values())
        if account_id is not None:
            values = tuple(
                order for order in values if order.intent.order.account_id == account_id
            )
        return tuple(
            sorted(
                values,
                key=lambda item: (
                    item.submitted_at,
                    item.intent.order.client_order_id,
                ),
            )
        )

    def restore_order_snapshot(self, snapshot: PaperOrderSnapshot) -> None:
        """Restore an audited snapshot without re-running reservation checks.

        This is intentionally narrower than order submission and exists only
        for crash recovery from the append-only paper-order event store.  A
        conflicting snapshot fails closed; callers should construct a fresh
        matcher when rebuilding a complete durable projection.
        """

        client_order_id = snapshot.intent.order.client_order_id
        existing = self._orders.get(client_order_id)
        if existing is not None and existing != snapshot:
            raise PaperOrderConflictError(
                f"client_order_id {client_order_id!r} has conflicting restored state"
            )
        self._orders[client_order_id] = snapshot

    def cancel_order(
        self,
        client_order_id: str,
        *,
        cancelled_at: datetime,
    ) -> PaperOrderSnapshot:
        """Cancel an open order and release its process-local reservation."""

        order = self._required_order(client_order_id)
        if not order.is_open:
            return order
        cancelled_at = _not_before(cancelled_at, order.updated_at, "cancelled_at")
        updated = replace(
            order,
            status=PaperOrderStatus.CANCELLED,
            reserved_cash=Decimal("0"),
            reserved_quantity=0,
            updated_at=cancelled_at,
            reason=PaperMatchReason.CANCELLED_BY_CALLER,
        )
        self._orders[client_order_id] = updated
        return updated

    def expire_order(
        self,
        client_order_id: str,
        *,
        expired_at: datetime,
    ) -> PaperOrderSnapshot:
        """Explicitly expire an open order and release reservations."""

        order = self._required_order(client_order_id)
        if not order.is_open:
            return order
        expired_at = _not_before(expired_at, order.updated_at, "expired_at")
        updated = self._expire(
            order,
            expired_at=expired_at,
            processed_session=order.last_processed_session,
            reason=PaperMatchReason.ORDER_EXPIRED_BEFORE_BAR,
        )
        self._orders[client_order_id] = updated
        return updated

    def process_bar(self, bar: SimulatedDailyBar) -> PaperBarMatchRun:
        """Apply one daily bar at most once, FIFO across eligible orders."""

        key = (bar.symbol, bar.trade_date)
        previous_run = self._runs.get(key)
        if previous_run is not None:
            if previous_run.bar != bar:
                raise PaperBarConflictError(
                    f"bar {bar.symbol} {bar.trade_date} was already processed "
                    "with different content"
                )
            return replace(previous_run, applied_new=False)

        volume_capacity = _volume_capacity(
            bar.volume_shares,
            self._config.volume_participation_rate,
        )
        consumed_volume = 0
        outcomes: list[PaperOrderMatchOutcome] = []
        candidates = tuple(
            order
            for order in self.orders()
            if order.is_open and order.intent.order.symbol == bar.symbol
        )
        for original in candidates:
            current = self._orders[original.intent.order.client_order_id]
            if not current.is_open:
                continue
            outcome, consumed = self._process_order_on_bar(
                current,
                bar,
                available_volume=volume_capacity - consumed_volume,
            )
            outcomes.append(outcome)
            consumed_volume += consumed
        run = PaperBarMatchRun(
            bar=bar,
            applied_new=True,
            volume_capacity=volume_capacity,
            consumed_volume=consumed_volume,
            outcomes=tuple(outcomes),
        )
        self._runs[key] = run
        return run

    def _process_order_on_bar(
        self,
        order: PaperOrderSnapshot,
        bar: SimulatedDailyBar,
        *,
        available_volume: int,
    ) -> tuple[PaperOrderMatchOutcome, int]:
        status_before = order.status
        intent = order.intent
        if (
            order.last_processed_session is not None
            and bar.trade_date <= order.last_processed_session
        ):
            raise PaperMatchingSequenceError(
                f"bar {bar.trade_date} does not follow order's last processed session "
                f"{order.last_processed_session}"
            )
        if bar.available_at < order.updated_at:
            raise PaperMatchingSequenceError(
                "bar available_at precedes the latest order state"
            )
        if bar.trade_date <= intent.decision_session_date:
            return self._outcome(
                order,
                status_before,
                PaperMatchReason.ORDER_NOT_YET_ELIGIBLE,
            ), 0
        if intent.expires_on is not None and bar.trade_date > intent.expires_on:
            expired = self._expire(
                order,
                expired_at=bar.available_at,
                processed_session=bar.trade_date,
                reason=PaperMatchReason.ORDER_EXPIRED_BEFORE_BAR,
            )
            self._orders[intent.order.client_order_id] = expired
            return self._outcome(
                expired,
                status_before,
                PaperMatchReason.ORDER_EXPIRED_BEFORE_BAR,
            ), 0

        invalid_reason = _bar_failure_reason(bar)
        if invalid_reason is not None:
            final = self._maybe_expire_after_bar(order, bar)
            self._orders[intent.order.client_order_id] = final
            reason = (
                PaperMatchReason.EXPIRED_AFTER_ELIGIBLE_BAR
                if final.status is PaperOrderStatus.EXPIRED
                else invalid_reason
            )
            return self._outcome(final, status_before, reason), 0
        assert bar.price_band is not None
        if not bar.price_band.contains(intent.order.limit_price):
            rejected = replace(
                order,
                status=PaperOrderStatus.REJECTED,
                reserved_cash=Decimal("0"),
                reserved_quantity=0,
                updated_at=bar.available_at,
                reason=PaperMatchReason.ORDER_LIMIT_OUTSIDE_PRICE_BAND,
                last_processed_session=bar.trade_date,
            )
            self._orders[intent.order.client_order_id] = rejected
            return self._outcome(
                rejected,
                status_before,
                PaperMatchReason.ORDER_LIMIT_OUTSIDE_PRICE_BAND,
            ), 0

        if not _limit_touched(order, bar):
            final = self._maybe_expire_after_bar(order, bar)
            self._orders[intent.order.client_order_id] = final
            reason = (
                PaperMatchReason.EXPIRED_AFTER_ELIGIBLE_BAR
                if final.status is PaperOrderStatus.EXPIRED
                else PaperMatchReason.LIMIT_NOT_TOUCHED
            )
            return self._outcome(final, status_before, reason), 0

        fill_quantity = min(order.remaining_quantity, available_volume)
        if intent.order.side is Side.BUY:
            fill_quantity -= fill_quantity % self._config.buy_lot_size
        if fill_quantity <= 0:
            final = self._maybe_expire_after_bar(order, bar)
            self._orders[intent.order.client_order_id] = final
            reason = (
                PaperMatchReason.EXPIRED_AFTER_ELIGIBLE_BAR
                if final.status is PaperOrderStatus.EXPIRED
                else PaperMatchReason.VOLUME_PARTICIPATION_EXHAUSTED
            )
            return self._outcome(final, status_before, reason), 0

        fill_price = self._execution_price(order, bar)
        try:
            account = self._accounts.snapshot(intent.order.account_id)
            if account.session_date > bar.trade_date:
                raise PaperMatchingSequenceError(
                    f"account session {account.session_date} is ahead of bar {bar.trade_date}"
                )
            if account.session_date < bar.trade_date:
                self._accounts.rollover_session(
                    intent.order.account_id,
                    target_session_date=bar.trade_date,
                    occurred_at=bar.available_at,
                )
            receipt = self._accounts.record_fill(
                ASharePaperFill(
                    account_id=intent.order.account_id,
                    fill_id=_fill_id(intent.order.account_id, intent.order.client_order_id, bar),
                    symbol=intent.order.symbol,
                    side=intent.order.side,
                    quantity=fill_quantity,
                    price=fill_price,
                    instrument_type=intent.instrument_type,
                    trading_date=bar.trade_date,
                    executed_at=bar.available_at,
                    source=PaperFillSource.SIMULATED,
                    external_order_id=intent.order.client_order_id,
                    note=f"daily-bar:{bar.source_revision}",
                ),
                recorded_at=bar.available_at,
            )
        except PaperMatchingSequenceError:
            raise
        except (ASharePaperError, ValueError):
            rejected = replace(
                order,
                status=PaperOrderStatus.REJECTED,
                reserved_cash=Decimal("0"),
                reserved_quantity=0,
                updated_at=bar.available_at,
                reason=PaperMatchReason.ACCOUNT_LEDGER_REJECTED,
                last_processed_session=bar.trade_date,
            )
            self._orders[intent.order.client_order_id] = rejected
            return self._outcome(
                rejected,
                status_before,
                PaperMatchReason.ACCOUNT_LEDGER_REJECTED,
            ), 0

        new_filled = order.filled_quantity + fill_quantity
        prior_notional = (
            order.average_fill_price * order.filled_quantity
            if order.average_fill_price is not None
            else Decimal("0")
        )
        average_fill_price = (
            prior_notional + fill_price * fill_quantity
        ) / new_filled
        fully_filled = new_filled == intent.quantity
        status = (
            PaperOrderStatus.FILLED
            if fully_filled
            else PaperOrderStatus.PARTIALLY_FILLED
        )
        reserved_cash = max(
            Decimal("0"),
            order.reserved_cash + receipt.applied_fill.cash_change,
        )
        reserved_quantity = max(0, order.reserved_quantity - fill_quantity)
        updated = replace(
            order,
            status=status,
            filled_quantity=new_filled,
            average_fill_price=average_fill_price,
            reserved_cash=(Decimal("0") if fully_filled else reserved_cash),
            reserved_quantity=(0 if fully_filled else reserved_quantity),
            updated_at=bar.available_at,
            reason=(
                PaperMatchReason.FILLED
                if fully_filled
                else PaperMatchReason.PARTIAL_FILL
            ),
            last_processed_session=bar.trade_date,
        )
        if not fully_filled:
            updated = self._maybe_expire_after_bar(updated, bar)
        self._orders[intent.order.client_order_id] = updated
        reason = (
            PaperMatchReason.EXPIRED_AFTER_ELIGIBLE_BAR
            if updated.status is PaperOrderStatus.EXPIRED
            else (
                PaperMatchReason.FILLED
                if fully_filled
                else PaperMatchReason.PARTIAL_FILL
            )
        )
        return (
            PaperOrderMatchOutcome(
                client_order_id=intent.order.client_order_id,
                status_before=status_before,
                status_after=updated.status,
                reason=reason,
                filled_quantity=fill_quantity,
                fill_price=fill_price,
                receipt=receipt,
            ),
            fill_quantity,
        )

    def _maybe_expire_after_bar(
        self,
        order: PaperOrderSnapshot,
        bar: SimulatedDailyBar,
    ) -> PaperOrderSnapshot:
        intent = order.intent
        should_expire = (
            intent.time_in_force is PaperTimeInForce.NEXT_TRADING_BAR
            and _bar_failure_reason(bar) is None
        ) or (
            intent.time_in_force is PaperTimeInForce.GOOD_TILL_DATE
            and intent.expires_on is not None
            and bar.trade_date >= intent.expires_on
        )
        if not should_expire:
            return replace(
                order,
                last_processed_session=bar.trade_date,
            )
        return self._expire(
            order,
            expired_at=bar.available_at,
            processed_session=bar.trade_date,
            reason=PaperMatchReason.EXPIRED_AFTER_ELIGIBLE_BAR,
        )

    @staticmethod
    def _expire(
        order: PaperOrderSnapshot,
        *,
        expired_at: datetime,
        processed_session: date | None,
        reason: PaperMatchReason,
    ) -> PaperOrderSnapshot:
        return replace(
            order,
            status=PaperOrderStatus.EXPIRED,
            reserved_cash=Decimal("0"),
            reserved_quantity=0,
            updated_at=expired_at,
            reason=reason,
            last_processed_session=processed_session,
        )

    def _execution_price(
        self,
        order: PaperOrderSnapshot,
        bar: SimulatedDailyBar,
    ) -> Decimal:
        open_price = bar.open
        assert open_price is not None
        limit_price = order.intent.order.limit_price
        instrument_type = order.intent.instrument_type
        slippage = (
            self._config.stock_slippage_rate
            if instrument_type is PaperInstrumentType.STOCK
            else self._config.etf_slippage_rate
        )
        quantum = self._price_quantum(instrument_type)
        if order.intent.order.side is Side.BUY:
            reference = min(open_price, limit_price)
            slipped = min(limit_price, reference * (Decimal("1") + slippage))
            return slipped.quantize(quantum, rounding=ROUND_CEILING)
        reference = max(open_price, limit_price)
        slipped = max(limit_price, reference * (Decimal("1") - slippage))
        return slipped.quantize(quantum, rounding=ROUND_FLOOR)

    def _worst_case_buy_cash(self, intent: ASharePaperOrderIntent) -> Decimal:
        schedule = self._accounts.fee_schedule
        cost = calculate_trade_cost(
            side=Side.BUY,
            price=intent.order.limit_price,
            quantity=intent.quantity,
            instrument_type=InstrumentType(intent.instrument_type.value),
            config=TradingCostConfig(
                commission_rate=schedule.commission_rate,
                minimum_commission_cny=schedule.minimum_commission_cny,
                stock_sell_stamp_tax_rate=schedule.stock_sell_stamp_tax_rate,
                stock_transfer_fee_rate=schedule.stock_transfer_fee_rate,
                stock_slippage_rate=Decimal("0"),
                etf_sell_stamp_tax_rate=schedule.etf_sell_stamp_tax_rate,
                etf_transfer_fee_rate=schedule.etf_transfer_fee_rate,
                etf_slippage_rate=Decimal("0"),
                currency_quantum=schedule.currency_quantum,
            ),
        )
        return -cost.cash_change

    def _price_quantum(self, instrument_type: PaperInstrumentType) -> Decimal:
        return (
            self._config.stock_price_quantum
            if instrument_type is PaperInstrumentType.STOCK
            else self._config.etf_price_quantum
        )

    def _store_rejection(
        self,
        intent: ASharePaperOrderIntent,
        reason: PaperMatchReason,
        at: datetime,
    ) -> PaperOrderSnapshot:
        snapshot = PaperOrderSnapshot(
            intent=intent,
            status=PaperOrderStatus.REJECTED,
            filled_quantity=0,
            average_fill_price=None,
            reserved_cash=Decimal("0"),
            reserved_quantity=0,
            submitted_at=at,
            updated_at=at,
            reason=reason,
        )
        self._orders[intent.order.client_order_id] = snapshot
        return snapshot

    def _required_order(self, client_order_id: str) -> PaperOrderSnapshot:
        try:
            return self._orders[client_order_id]
        except KeyError as error:
            raise KeyError(f"unknown paper order: {client_order_id!r}") from error

    @staticmethod
    def _outcome(
        order: PaperOrderSnapshot,
        status_before: PaperOrderStatus,
        reason: PaperMatchReason,
    ) -> PaperOrderMatchOutcome:
        return PaperOrderMatchOutcome(
            client_order_id=order.intent.order.client_order_id,
            status_before=status_before,
            status_after=order.status,
            reason=reason,
        )


def _bar_failure_reason(bar: SimulatedDailyBar) -> PaperMatchReason | None:
    if not bar.is_trading:
        return PaperMatchReason.BAR_SUSPENDED
    if not bar.has_complete_ohlc:
        return PaperMatchReason.BAR_OHLC_MISSING
    if bar.volume_shares <= 0:
        return PaperMatchReason.BAR_VOLUME_ZERO
    if bar.price_band is None:
        return PaperMatchReason.PRICE_BAND_MISSING
    if not bar.ohlc_inside_price_band:
        return PaperMatchReason.BAR_OUTSIDE_PRICE_BAND
    return None


def _limit_touched(order: PaperOrderSnapshot, bar: SimulatedDailyBar) -> bool:
    if order.intent.order.side is Side.BUY:
        assert bar.low is not None
        return bar.low <= order.intent.order.limit_price
    assert bar.high is not None
    return bar.high >= order.intent.order.limit_price


def _volume_capacity(volume_shares: int, participation_rate: Decimal) -> int:
    return int(
        (Decimal(volume_shares) * participation_rate).to_integral_value(
            rounding=ROUND_FLOOR
        )
    )


def _fill_id(account_id: str, client_order_id: str, bar: SimulatedDailyBar) -> str:
    key = "|".join(
        (
            account_id,
            client_order_id,
            bar.symbol,
            bar.trade_date.isoformat(),
            bar.source_revision,
        )
    )
    return "pm-" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:40]


def _not_before(value: datetime, floor: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    if value < floor:
        raise PaperMatchingSequenceError(f"{name} precedes the latest order state")
    return value
