"""Point-in-time-safe moving-average target-position strategy for crypto spot.

The strategy is deliberately broker independent.  It consumes only completed
bars exposed by the backtest/paper event and emits at most one deterministic
market order for that decision timestamp.  Execution is left to the caller,
which makes the same policy reusable in historical replay and paper trading.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from enum import StrEnum

from gribuki_trade.backtest.crypto import (
    BacktestBarEvent,
    CryptoBar,
    CryptoOrderRequest,
    CryptoOrderType,
)
from gribuki_trade.domain.orders import Side


class CryptoTrendRegime(StrEnum):
    """Long-only regime selected from completed closing prices."""

    WARMUP = "WARMUP"
    LONG = "LONG"
    FLAT = "FLAT"


@dataclass(frozen=True, slots=True)
class CryptoTrendConfig:
    """Parameters for a long-only moving-average target-position strategy."""

    fast_window: int = 20
    slow_window: int = 50
    minimum_history: int = 50
    target_position_fraction: Decimal = Decimal("0.95")
    quantity_step: Decimal = Decimal("0.000001")
    minimum_order_quantity: Decimal = Decimal("0.000001")
    rebalance_tolerance_quantity: Decimal = Decimal("0")
    rebalance_tolerance_fraction: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        for name, value in (
            ("fast_window", self.fast_window),
            ("slow_window", self.slow_window),
            ("minimum_history", self.minimum_history),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an integer")
        if self.fast_window <= 0 or self.fast_window >= self.slow_window:
            raise ValueError("fast_window must be positive and less than slow_window")
        if self.minimum_history < self.slow_window:
            raise ValueError("minimum_history must be at least slow_window")
        for name, decimal_value in (
            ("target_position_fraction", self.target_position_fraction),
            ("quantity_step", self.quantity_step),
            ("minimum_order_quantity", self.minimum_order_quantity),
            ("rebalance_tolerance_quantity", self.rebalance_tolerance_quantity),
            ("rebalance_tolerance_fraction", self.rebalance_tolerance_fraction),
        ):
            _require_decimal(name, decimal_value)
        if not Decimal("0") <= self.target_position_fraction <= Decimal("1"):
            raise ValueError("target_position_fraction must be in [0, 1]")
        if self.quantity_step <= 0:
            raise ValueError("quantity_step must be positive")
        if self.minimum_order_quantity <= 0:
            raise ValueError("minimum_order_quantity must be positive")
        if self.rebalance_tolerance_quantity < 0:
            raise ValueError("rebalance_tolerance_quantity must be non-negative")
        if not Decimal("0") <= self.rebalance_tolerance_fraction <= Decimal("1"):
            raise ValueError("rebalance_tolerance_fraction must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class CryptoTrendDecision:
    """Auditable calculation made from one point-in-time event."""

    decision_time: datetime
    symbol: str
    observed_bars: int
    regime: CryptoTrendRegime
    fast_average: Decimal | None
    slow_average: Decimal | None
    equity_quote: Decimal
    current_base_quantity: Decimal
    target_base_quantity: Decimal
    order: CryptoOrderRequest | None
    reason: str


class MovingAverageCryptoTrendStrategy:
    """Move a spot portfolio between flat and a configured long allocation."""

    def __init__(
        self,
        *,
        symbol: str,
        base_asset: str,
        quote_asset: str,
        config: CryptoTrendConfig | None = None,
    ) -> None:
        normalized_symbol = symbol.strip().upper()
        normalized_base = base_asset.strip().upper()
        normalized_quote = quote_asset.strip().upper()
        if not normalized_symbol or not normalized_base or not normalized_quote:
            raise ValueError("symbol, base_asset and quote_asset must not be empty")
        if normalized_base == normalized_quote:
            raise ValueError("base_asset and quote_asset must be distinct")
        self._symbol = normalized_symbol
        self._base_asset = normalized_base
        self._quote_asset = normalized_quote
        self._config = config or CryptoTrendConfig()

    @property
    def config(self) -> CryptoTrendConfig:
        return self._config

    def __call__(self, event: BacktestBarEvent, /) -> tuple[CryptoOrderRequest, ...]:
        decision = self.evaluate(event)
        return () if decision.order is None else (decision.order,)

    def evaluate(self, event: BacktestBarEvent, /) -> CryptoTrendDecision:
        """Calculate one target using no information available after the event."""

        self._validate_event(event)
        history = event.history
        current_quantity = event.portfolio.balance(self._base_asset)
        quote_balance = event.portfolio.balance(self._quote_asset)
        mark = event.bar.close
        equity = quote_balance + current_quantity * mark
        if equity < 0 or current_quantity < 0 or quote_balance < 0:
            raise ValueError("spot strategy requires non-negative balances and equity")

        if len(history) < self._config.minimum_history:
            return CryptoTrendDecision(
                decision_time=event.decision_time,
                symbol=self._symbol,
                observed_bars=len(history),
                regime=CryptoTrendRegime.WARMUP,
                fast_average=None,
                slow_average=None,
                equity_quote=equity,
                current_base_quantity=current_quantity,
                target_base_quantity=current_quantity,
                order=None,
                reason="MINIMUM_HISTORY_NOT_REACHED",
            )

        fast_average = _average_close(history, self._config.fast_window)
        slow_average = _average_close(history, self._config.slow_window)
        regime = (
            CryptoTrendRegime.LONG
            if fast_average > slow_average
            else CryptoTrendRegime.FLAT
        )
        desired_fraction = (
            self._config.target_position_fraction
            if regime is CryptoTrendRegime.LONG
            else Decimal("0")
        )
        raw_target = equity * desired_fraction / mark
        target_quantity = _round_down(raw_target, self._config.quantity_step)
        delta = target_quantity - current_quantity
        current_fraction = (
            current_quantity * mark / equity if equity > 0 else Decimal("0")
        )
        fraction_within_tolerance = (
            abs(current_fraction - desired_fraction)
            <= self._config.rebalance_tolerance_fraction
        )
        if (
            abs(delta) <= self._config.rebalance_tolerance_quantity
            or fraction_within_tolerance
        ):
            return CryptoTrendDecision(
                decision_time=event.decision_time,
                symbol=self._symbol,
                observed_bars=len(history),
                regime=regime,
                fast_average=fast_average,
                slow_average=slow_average,
                equity_quote=equity,
                current_base_quantity=current_quantity,
                target_base_quantity=target_quantity,
                order=None,
                reason="WITHIN_REBALANCE_TOLERANCE",
            )

        side = Side.BUY if delta > 0 else Side.SELL
        quantity = _round_down(abs(delta), self._config.quantity_step)
        if side is Side.SELL:
            quantity = min(quantity, _round_down(current_quantity, self._config.quantity_step))
        if quantity < self._config.minimum_order_quantity:
            return CryptoTrendDecision(
                decision_time=event.decision_time,
                symbol=self._symbol,
                observed_bars=len(history),
                regime=regime,
                fast_average=fast_average,
                slow_average=slow_average,
                equity_quote=equity,
                current_base_quantity=current_quantity,
                target_base_quantity=target_quantity,
                order=None,
                reason="BELOW_MINIMUM_ORDER_QUANTITY",
            )

        order = CryptoOrderRequest(
            order_id=_order_id(self._symbol, event.bar.open_time, side),
            side=side,
            order_type=CryptoOrderType.MARKET,
            quantity=quantity,
        )
        return CryptoTrendDecision(
            decision_time=event.decision_time,
            symbol=self._symbol,
            observed_bars=len(history),
            regime=regime,
            fast_average=fast_average,
            slow_average=slow_average,
            equity_quote=equity,
            current_base_quantity=current_quantity,
            target_base_quantity=target_quantity,
            order=order,
            reason="TARGET_REBALANCE",
        )

    def _validate_event(self, event: BacktestBarEvent) -> None:
        if not event.history:
            raise ValueError("strategy event history must not be empty")
        if event.bar != event.history[-1]:
            raise ValueError("event bar must be the final history bar")
        if event.bar.symbol != self._symbol:
            raise ValueError("event symbol does not match strategy symbol")
        if event.decision_time.tzinfo is None or event.decision_time.utcoffset() is None:
            raise ValueError("decision_time must be timezone-aware")
        if event.decision_time < event.bar.available_at:
            raise ValueError("decision_time precedes current bar availability")
        # Only the trailing slow window contributes to this decision.  Scanning
        # an ever-growing prefix on every bar would make long replays quadratic.
        # Backtest inputs are validated globally by the engine; standalone
        # callers still receive full validation for every value the strategy
        # can actually observe in its calculation.
        relevant_history = event.history[-self._config.slow_window :]
        if any(not bar.complete for bar in relevant_history):
            raise ValueError("strategy requires completed bars")
        if any(bar.symbol != self._symbol for bar in relevant_history):
            raise ValueError("all history bars must match strategy symbol")
        if any(bar.available_at > event.decision_time for bar in relevant_history):
            raise ValueError("history contains a bar unavailable at decision_time")
        opens = [bar.open_time for bar in relevant_history]
        if opens != sorted(opens) or len(opens) != len(set(opens)):
            raise ValueError("history bars must be strictly ordered and unique")


def _average_close(history: Sequence[CryptoBar], window: int) -> Decimal:
    # BacktestBarEvent.history is validated above.  Keeping this helper Decimal-only
    # makes its numerical result reproducible across platforms.
    bars = history[-window:]
    closes = [bar.close for bar in bars]
    return sum(closes, Decimal("0")) / Decimal(window)


def _round_down(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _order_id(symbol: str, candle_open_time: datetime, side: Side) -> str:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = candle_open_time.astimezone(UTC) - epoch
    milliseconds = (
        (delta.days * 86_400 + delta.seconds) * 1_000 + delta.microseconds // 1_000
    )
    side_code = "b" if side is Side.BUY else "s"
    # Binance client-order IDs are limited to 36 characters.  Timestamp and side
    # make the ID stable even if the same completed candle is delivered late.
    return f"ct-{symbol[:12].lower()}-{milliseconds}-{side_code}"


def _require_decimal(name: str, value: object) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
