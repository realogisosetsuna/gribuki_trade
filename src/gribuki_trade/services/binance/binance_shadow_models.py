"""Binance 影子会话的配置、安全水印与结果值对象。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from gribuki_trade.adapters.binance.models import BinanceEnvironment
from gribuki_trade.adapters.binance.stream import KLINE_INTERVALS
from gribuki_trade.adapters.paper_account import PaperAssetBalance, SpotSymbolAssets
from gribuki_trade.services.binance.binance_paper import PaperEngineSnapshot

_ALLOWED_SYMBOL_ASSETS: Mapping[str, SpotSymbolAssets] = {
    "BTCUSDT": SpotSymbolAssets("BTC", "USDT"),
    "ETHUSDT": SpotSymbolAssets("ETH", "USDT"),
}


class ShadowMarketIntegrityError(RuntimeError):
    """公共市场数据不满足作为策略决策输入的安全条件。"""


class ShadowRecoveryError(RuntimeError):
    """无法在不作猜测的情况下恢复已持久化模拟状态。"""


class ShadowTermination(StrEnum):
    STREAM_ENDED = "STREAM_ENDED"
    MAXIMUM_CLOSED_BARS = "MAXIMUM_CLOSED_BARS"
    MAXIMUM_EVENTS = "MAXIMUM_EVENTS"
    EXTERNAL_STOP = "EXTERNAL_STOP"
    FAILED_CLOSED = "FAILED_CLOSED"


@dataclass(frozen=True, slots=True)
class ShadowEnvironmentWatermark:
    public_market_environment: BinanceEnvironment
    execution_mode: str = field(default="PAPER_SHADOW", init=False)
    venue: str = field(default="BINANCE_SPOT", init=False)
    remote_order_submission_enabled: bool = field(default=False, init=False)

    @property
    def label(self) -> str:
        return (
            f"{self.venue}/{self.execution_mode}/"
            f"{self.public_market_environment.value}/NO_REMOTE_ORDERS"
        )


@dataclass(frozen=True, slots=True)
class BinanceShadowConfig:
    symbol: str = "BTCUSDT"
    interval: str = "1m"
    account_id: str = "binance-shadow"
    strategy_id: str = "crypto-trend-shadow-v1"
    initial_balances: Mapping[str, Decimal | str | int] = field(
        default_factory=lambda: {"BTC": "0", "ETH": "0", "USDT": "10000"}
    )
    minimum_order_quantity: Decimal = Decimal("0.000001")
    quantity_step: Decimal = Decimal("0.000001")
    minimum_order_notional: Decimal = Decimal("10")
    maximum_order_notional: Decimal = Decimal("1000")
    price_step: Decimal = Decimal("0.01")
    aggressive_limit_offset_bps: Decimal = Decimal("1")
    maximum_market_age_seconds: Decimal = Decimal("5")
    history_capacity: int = 2_000
    maximum_open_orders: int = 1
    maximum_actions_per_minute: int = 6

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        if symbol not in _ALLOWED_SYMBOL_ASSETS:
            raise ValueError("shadow runtime only supports long-only BTCUSDT/ETHUSDT")
        if self.interval not in KLINE_INTERVALS:
            raise ValueError(f"unsupported Binance kline interval: {self.interval!r}")
        if not self.account_id.strip() or not self.strategy_id.strip():
            raise ValueError("account_id and strategy_id must not be empty")
        for name, value in (
            ("minimum_order_quantity", self.minimum_order_quantity),
            ("quantity_step", self.quantity_step),
            ("minimum_order_notional", self.minimum_order_notional),
            ("maximum_order_notional", self.maximum_order_notional),
            ("price_step", self.price_step),
            ("maximum_market_age_seconds", self.maximum_market_age_seconds),
        ):
            _positive_decimal(value, name)
        offset = _non_negative_decimal(
            self.aggressive_limit_offset_bps, "aggressive_limit_offset_bps"
        )
        if offset >= Decimal("10000"):
            raise ValueError("aggressive_limit_offset_bps must be below 10000")
        if self.minimum_order_notional > self.maximum_order_notional:
            raise ValueError("minimum_order_notional must not exceed maximum_order_notional")
        if not isinstance(self.history_capacity, int) or isinstance(
            self.history_capacity, bool
        ) or self.history_capacity <= 0:
            raise ValueError("history_capacity must be a positive integer")
        if not isinstance(self.maximum_open_orders, int) or isinstance(
            self.maximum_open_orders, bool
        ) or self.maximum_open_orders <= 0:
            raise ValueError("maximum_open_orders must be a positive integer")
        if (
            not isinstance(self.maximum_actions_per_minute, int)
            or isinstance(self.maximum_actions_per_minute, bool)
            or self.maximum_actions_per_minute <= 0
        ):
            raise ValueError("maximum_actions_per_minute must be a positive integer")
        object.__setattr__(self, "symbol", symbol)

    @property
    def assets(self) -> SpotSymbolAssets:
        return _ALLOWED_SYMBOL_ASSETS[self.symbol]


@dataclass(frozen=True, slots=True)
class ShadowQuote:
    symbol: str
    bid: Decimal
    ask: Decimal
    observed_at: datetime
    update_id: int


@dataclass(frozen=True, slots=True)
class ShadowAdapterSnapshot:
    observed_closed_bars: int
    generated_signals: int
    skipped_warmup_or_tolerance: int
    skipped_open_order: int
    skipped_below_minimum: int
    capped_to_maximum_notional: int


@dataclass(frozen=True, slots=True)
class BinanceShadowStatistics:
    watermark: ShadowEnvironmentWatermark
    termination: ShadowTermination
    started_at: datetime
    ended_at: datetime
    paper_engine: PaperEngineSnapshot
    adapter: ShadowAdapterSnapshot
    recovered_open_orders: int
    ignored_duplicate_closed_bars: int
    detected_bar_gaps: int
    stale_market_events: int
    balances: tuple[PaperAssetBalance, ...]
    final_equity_quote: Decimal | None
    failure_reason: str | None = None


__all__ = [
    "BinanceShadowConfig",
    "BinanceShadowStatistics",
    "ShadowAdapterSnapshot",
    "ShadowEnvironmentWatermark",
    "ShadowMarketIntegrityError",
    "ShadowQuote",
    "ShadowRecoveryError",
    "ShadowTermination",
]


def _positive_decimal(value: object, name: str) -> Decimal:
    normalized = _non_negative_decimal(value, name)
    if normalized <= 0:
        raise ValueError(f"{name} must be positive")
    return normalized


def _non_negative_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return value
