"""Long-running Binance market-data to paper-trading orchestration."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol, TypeAlias

from gribuki_trade.adapters.binance.stream import (
    BinanceBookTickerEvent,
    BinanceKlineEvent,
    BinanceMarketEvent,
)
from gribuki_trade.adapters.paper import PaperBroker, PaperFill
from gribuki_trade.adapters.paper_account import (
    InsufficientPaperBalance,
    PaperLiquidityRole,
    PaperSpotAccount,
)
from gribuki_trade.domain.orders import OrderIntent
from gribuki_trade.trading import (
    BalanceValue,
    ExecutionFill,
    SQLiteOrderManagementStore,
)


class BinanceMarketSource(Protocol):
    def events(self) -> AsyncIterator[BinanceMarketEvent]: ...


PaperDecision: TypeAlias = Callable[
    [BinanceKlineEvent, "PaperEngineSnapshot"],
    Iterable[OrderIntent] | Awaitable[Iterable[OrderIntent]],
]


@dataclass(frozen=True, slots=True)
class PaperRiskLimits:
    allowed_symbols: frozenset[str] = frozenset({"BTCUSDT", "ETHUSDT"})
    maximum_order_notional: Decimal = Decimal("20")
    maximum_open_orders: int = 2
    maximum_actions_per_minute: int = 2
    maximum_market_age_seconds: Decimal = Decimal("3")

    def __post_init__(self) -> None:
        normalized = frozenset(symbol.strip().upper() for symbol in self.allowed_symbols)
        if not normalized or any(not symbol for symbol in normalized):
            raise ValueError("allowed_symbols must not be empty")
        if not self.maximum_order_notional.is_finite() or self.maximum_order_notional <= 0:
            raise ValueError("maximum_order_notional must be positive")
        if self.maximum_open_orders <= 0 or self.maximum_actions_per_minute <= 0:
            raise ValueError("order limits must be positive")
        if (
            not self.maximum_market_age_seconds.is_finite()
            or self.maximum_market_age_seconds <= 0
        ):
            raise ValueError("maximum_market_age_seconds must be positive")
        object.__setattr__(self, "allowed_symbols", normalized)


@dataclass(frozen=True, slots=True)
class PaperEngineSnapshot:
    at: datetime
    processed_market_events: int
    processed_closed_bars: int
    submitted_orders: int
    fill_count: int
    rejected_signals: int
    stale_market_events: int


class BinancePaperEngine:
    """Drive a deterministic paper broker from Binance public streams.

    Closed kline events are the only strategy decision points.  Top-of-book
    events only match already accepted limits.  Every accepted intent is
    persisted together with a durable submit command before the in-memory
    paper broker receives it, mirroring the live delivery boundary.
    """

    def __init__(
        self,
        market: BinanceMarketSource,
        broker: PaperBroker,
        oms: SQLiteOrderManagementStore,
        decision: PaperDecision,
        *,
        account: PaperSpotAccount | None = None,
        limits: PaperRiskLimits | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._market = market
        self._broker = broker
        self._oms = oms
        self._decision = decision
        self._account = account
        self._limits = limits or PaperRiskLimits()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._stopping = False
        self._action_times: list[datetime] = []
        self._processed_market_events = 0
        self._processed_closed_bars = 0
        self._submitted_orders = 0
        self._fill_count = 0
        self._rejected_signals = 0
        self._stale_market_events = 0

    @property
    def snapshot(self) -> PaperEngineSnapshot:
        return PaperEngineSnapshot(
            at=self._clock(),
            processed_market_events=self._processed_market_events,
            processed_closed_bars=self._processed_closed_bars,
            submitted_orders=self._submitted_orders,
            fill_count=self._fill_count,
            rejected_signals=self._rejected_signals,
            stale_market_events=self._stale_market_events,
        )

    def stop(self) -> None:
        self._stopping = True

    async def run(self, *, maximum_events: int | None = None) -> PaperEngineSnapshot:
        if maximum_events is not None and maximum_events <= 0:
            raise ValueError("maximum_events must be positive")
        self._stopping = False
        await self._broker.connect()
        event_iterator = self._market.events()
        try:
            async for event in event_iterator:
                if self._stopping:
                    break
                await self.process_market_event(event)
                if (
                    maximum_events is not None
                    and self._processed_market_events >= maximum_events
                ):
                    break
        finally:
            close = getattr(event_iterator, "aclose", None)
            if close is not None:
                await close()
            await self._broker.disconnect()
        return self.snapshot

    async def process_market_event(self, event: BinanceMarketEvent) -> None:
        self._processed_market_events += 1
        if isinstance(event, BinanceBookTickerEvent):
            if self._is_stale(event.event_time_ms):
                self._stale_market_events += 1
                return
            fills = await self._broker.match_quote(
                event.symbol,
                bid=event.bid_price,
                ask=event.ask_price,
            )
            for fill in fills:
                self._persist_fill(fill)
            return
        if isinstance(event, BinanceKlineEvent) and event.is_closed:
            if self._is_stale(event.event_time_ms):
                self._stale_market_events += 1
                return
            self._processed_closed_bars += 1
            decisions = self._decision(event, self.snapshot)
            if inspect.isawaitable(decisions):
                decisions = await decisions
            for order in decisions:
                await self._submit(order, decision_event=event)

    async def _submit(
        self,
        order: OrderIntent,
        *,
        decision_event: BinanceKlineEvent,
    ) -> None:
        if order.symbol.upper() != decision_event.symbol:
            self._rejected_signals += 1
            return
        if order.symbol.upper() not in self._limits.allowed_symbols:
            self._rejected_signals += 1
            return
        if order.quantity * order.limit_price > self._limits.maximum_order_notional:
            self._rejected_signals += 1
            return
        open_count = len(self._oms.open_orders(account_id=order.account_id))
        if open_count >= self._limits.maximum_open_orders:
            self._rejected_signals += 1
            return
        now = self._clock()
        cutoff = now.timestamp() - 60
        self._action_times = [value for value in self._action_times if value.timestamp() > cutoff]
        if len(self._action_times) >= self._limits.maximum_actions_per_minute:
            self._rejected_signals += 1
            return

        if self._account is not None:
            try:
                self._account.reserve_order(order)
            except (InsufficientPaperBalance, ValueError):
                self._rejected_signals += 1
                return
            self._persist_account_balances(
                event_id=f"paper-reserve:{order.client_order_id}",
                occurred_at=order.created_at,
            )
        try:
            self._oms.create_order(order)
            command = self._oms.claim_command(
                f"submit:{order.client_order_id}",
                now=now,
            )
        except BaseException:
            if self._account is not None:
                self._account.cancel_order(order.client_order_id)
            raise
        if command.client_order_id != order.client_order_id:
            if self._account is not None:
                self._account.cancel_order(order.client_order_id)
            raise RuntimeError("durable submit command was not claimable")
        try:
            await self._broker.submit_order(order)
            update = self._broker.order_update(order.client_order_id)
            if update is None:
                raise RuntimeError("paper broker produced no order update")
            if self._account is not None and update.status.value.endswith("REJECTED"):
                self._account.cancel_order(order.client_order_id)
                self._persist_account_balances(
                    event_id=f"paper-release:{order.client_order_id}",
                    occurred_at=now,
                )
            self._oms.record_order_update(
                order.client_order_id,
                event_id=f"paper-submit:{order.client_order_id}",
                status=update.status,
                occurred_at=now,
                filled_quantity=update.filled_quantity,
                average_fill_price=update.average_fill_price,
                reason=update.reason,
            )
            self._oms.mark_command_sent(command.command_id, occurred_at=now)
        except BaseException:
            if self._account is not None:
                reservation = self._account.reservation(order.client_order_id)
                if reservation is not None and reservation.status.value == "ACTIVE":
                    self._account.cancel_order(order.client_order_id)
                    self._persist_account_balances(
                        event_id=f"paper-release:{order.client_order_id}",
                        occurred_at=now,
                    )
            self._oms.mark_command_unknown(
                command.command_id,
                occurred_at=now,
                error_code="paper_delivery_failed",
            )
            raise
        self._action_times.append(now)
        self._submitted_orders += 1

    def _persist_fill(self, fill: PaperFill) -> None:
        order = self._oms.require_order(fill.client_order_id).order
        fee_asset: str | None = None
        fee_amount = Decimal("0")
        if self._account is not None:
            receipt = self._account.apply_fill(
                fill.client_order_id,
                quantity=fill.quantity,
                price=fill.price,
                fill_id=fill.fill_id,
                liquidity_role=PaperLiquidityRole.TAKER,
            )
            fee_asset = receipt.fee_asset
            fee_amount = receipt.fee_amount
        self._oms.record_fill(
            ExecutionFill(
                fill_id=fill.fill_id,
                client_order_id=fill.client_order_id,
                account_id=order.account_id,
                symbol=fill.symbol,
                side=fill.side,
                quantity=fill.quantity,
                price=fill.price,
                occurred_at=fill.occurred_at,
                fee_asset=fee_asset,
                fee_amount=fee_amount,
            )
        )
        if self._account is not None:
            self._persist_account_balances(
                event_id=f"paper-account-fill:{fill.fill_id}",
                occurred_at=fill.occurred_at,
            )
        self._fill_count += 1

    def _persist_account_balances(self, *, event_id: str, occurred_at: datetime) -> None:
        assert self._account is not None
        self._oms.record_balance_snapshot(
            self._account.account_id,
            tuple(
                BalanceValue(balance.asset, balance.free, balance.locked)
                for balance in self._account.balances()
            ),
            event_id=event_id,
            occurred_at=occurred_at,
        )

    def _is_stale(self, event_time_ms: int | None) -> bool:
        if event_time_ms is None:
            return False
        age_ms = Decimal(str(self._clock().timestamp() * 1_000 - event_time_ms))
        return age_ms > self._limits.maximum_market_age_seconds * Decimal("1000")
