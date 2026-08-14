"""针对一个加密货币现货交易对的确定性、时点一致回测。

在 OHLCV 柱不能揭示真实执行路径的地方，模拟器刻意采取保守处理：

* 策略仅在蜡烛线已完成且可用后才能看到它；
* 订单最早可在下一根蜡烛线成交；
* 市价单支付配置的逆向滑点；
* 被触及的限价单按其限价成交，不假定存在价格改善；以及
* 成交量占用该蜡烛线基础资产成交量中可配置的一部分。

这是研究模拟器，不是交易所撮合引擎的复刻。不过相同的会计不变量仍适用于
PAPER 与实时影子测试。
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, overload

from gribuki_trade.domain.orders import Side


class CryptoOrderType(StrEnum):
    """柱状模拟器支持的订单类型。"""

    MARKET = "MARKET"
    LIMIT = "LIMIT"


class LiquidityRole(StrEnum):
    """分配给模拟成交的费率角色。"""

    MAKER = "MAKER"
    TAKER = "TAKER"


class SimulatedOrderStatus(StrEnum):
    """回放结束时订单的最终状态。"""

    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    REJECTED = "REJECTED"
    OPEN = "OPEN"


@dataclass(frozen=True, slots=True)
class CryptoBar:
    """一根时点一致的 OHLCV 蜡烛线。

    ``volume`` 以基础资产计量。``available_at`` 记录已完成蜡烛线最早可被策略使用的时间。
    """

    symbol: str
    open_time: datetime
    close_time: datetime
    available_at: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    complete: bool = True

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        _require_aware("open_time", self.open_time)
        _require_aware("close_time", self.close_time)
        _require_aware("available_at", self.available_at)
        if self.close_time <= self.open_time:
            raise ValueError("close_time must be after open_time")
        if self.available_at < self.close_time:
            raise ValueError("available_at must not precede close_time")
        prices = {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
        }
        for name, value in prices.items():
            _require_decimal(name, value)
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.high < self.low:
            raise ValueError("high must not be below low")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("bar OHLC values are inconsistent")
        _require_decimal("volume", self.volume)
        if self.volume < 0:
            raise ValueError("volume must be non-negative")
        if not isinstance(self.complete, bool):
            raise TypeError("complete must be a bool")
        object.__setattr__(self, "symbol", self.symbol.upper())


@dataclass(frozen=True, slots=True)
class CryptoFeeConfig:
    """以计价资产计量的挂单/吃单费率表。

    ``discount_fraction`` 表示已符合条件的折扣，例如配置的手续费代币折扣。
    手续费代币的库存和兑换刻意不属于此单交易对模拟器的范围。
    """

    maker_rate: Decimal = Decimal("0.001")
    taker_rate: Decimal = Decimal("0.001")
    discount_fraction: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        for name, value in (
            ("maker_rate", self.maker_rate),
            ("taker_rate", self.taker_rate),
            ("discount_fraction", self.discount_fraction),
        ):
            _require_decimal(name, value)
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.maker_rate >= 1 or self.taker_rate >= 1:
            raise ValueError("maker_rate and taker_rate must be below one")
        if self.discount_fraction >= 1:
            raise ValueError("discount_fraction must be below one")

    def effective_rate(self, role: LiquidityRole) -> Decimal:
        """返回某流动性角色折扣后的费率。"""

        rate = self.maker_rate if role is LiquidityRole.MAKER else self.taker_rate
        return rate * (Decimal("1") - self.discount_fraction)


@dataclass(frozen=True, slots=True)
class CryptoOrderRequest:
    """当前蜡烛线收盘后创建的策略订单。"""

    order_id: str
    side: Side
    order_type: CryptoOrderType
    quantity: Decimal
    limit_price: Decimal | None = None

    def __post_init__(self) -> None:
        if not self.order_id.strip():
            raise ValueError("order_id must not be empty")
        try:
            side = Side(self.side)
            order_type = CryptoOrderType(self.order_type)
        except ValueError as error:
            raise ValueError("unsupported side or order_type") from error
        _require_decimal("quantity", self.quantity)
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if order_type is CryptoOrderType.LIMIT:
            if self.limit_price is None:
                raise ValueError("limit orders require limit_price")
            _require_decimal("limit_price", self.limit_price)
            if self.limit_price <= 0:
                raise ValueError("limit_price must be positive")
        elif self.limit_price is not None:
            raise ValueError("market orders must not define limit_price")
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "order_type", order_type)


@dataclass(frozen=True, slots=True)
class PortfolioSnapshot:
    """向策略代码暴露的不可变 Decimal 余额。"""

    balances: tuple[tuple[str, Decimal], ...]

    def balance(self, asset: str) -> Decimal:
        normalized = asset.upper()
        return next(
            (value for name, value in self.balances if name == normalized),
            Decimal("0"),
        )


class InsufficientBalanceError(ValueError):
    """模拟成交将导致现货余额为负。"""


class SpotLedger:
    """最小化的多资产、仅使用 Decimal 的现货余额账本。"""

    def __init__(self, balances: Mapping[str, Decimal]) -> None:
        normalized: dict[str, Decimal] = {}
        for raw_asset, value in balances.items():
            asset = raw_asset.strip().upper()
            if not asset:
                raise ValueError("asset must not be empty")
            if asset in normalized:
                raise ValueError(f"duplicate normalized asset: {asset}")
            _require_decimal(f"balance[{asset}]", value)
            if value < 0:
                raise ValueError("spot balances must be non-negative")
            normalized[asset] = value
        self._balances = normalized

    def balance(self, asset: str) -> Decimal:
        return self._balances.get(asset.upper(), Decimal("0"))

    def snapshot(self) -> PortfolioSnapshot:
        return PortfolioSnapshot(tuple(sorted(self._balances.items())))

    def apply_fill(
        self,
        *,
        side: Side,
        base_asset: str,
        quote_asset: str,
        quantity: Decimal,
        price: Decimal,
        fee_quote: Decimal,
    ) -> None:
        """应用原子化现货成交，并以计价货币收取费用。"""

        for name, value in (
            ("quantity", quantity),
            ("price", price),
            ("fee_quote", fee_quote),
        ):
            _require_decimal(name, value)
        if quantity <= 0 or price <= 0 or fee_quote < 0:
            raise ValueError("fill quantity/price must be positive and fee non-negative")
        try:
            resolved_side = Side(side)
        except ValueError as error:
            raise ValueError(f"unsupported side: {side!r}") from error
        base = base_asset.upper()
        quote = quote_asset.upper()
        if not base or not quote or base == quote:
            raise ValueError("base and quote assets must be distinct and non-empty")
        notional = quantity * price
        if resolved_side is Side.BUY:
            required_quote = notional + fee_quote
            if self.balance(quote) < required_quote:
                raise InsufficientBalanceError(
                    f"insufficient {quote}: need {required_quote}, have {self.balance(quote)}"
                )
            self._balances[quote] = self.balance(quote) - required_quote
            self._balances[base] = self.balance(base) + quantity
            return
        if self.balance(base) < quantity:
            raise InsufficientBalanceError(
                f"insufficient {base}: need {quantity}, have {self.balance(base)}"
            )
        proceeds = notional - fee_quote
        if proceeds < 0:
            raise ValueError("fee must not exceed sell proceeds")
        self._balances[base] = self.balance(base) - quantity
        self._balances[quote] = self.balance(quote) + proceeds


@dataclass(frozen=True, slots=True)
class BacktestBarEvent:
    """某一决策时点唯一可用的市场历史。"""

    decision_time: datetime
    bar: CryptoBar
    history: Sequence[CryptoBar]
    portfolio: PortfolioSnapshot


class _BarHistoryPrefix(Sequence[CryptoBar]):
    """经验证回放数据集上的不可变 O(1) 前缀视图。

    为每根蜡烛线构造元组前缀，会令长回放在引用复制数量与运行时间上均呈二次增长。
    此视图保持完全相同的时点边界，仅物化显式请求的切片，例如策略最终的移动平均窗口。
    """

    __slots__ = ("_bars", "_stop")

    def __init__(self, bars: tuple[CryptoBar, ...], stop: int) -> None:
        if not 0 <= stop <= len(bars):
            raise ValueError("history prefix stop is outside the dataset")
        self._bars = bars
        self._stop = stop

    def __len__(self) -> int:
        return self._stop

    @overload
    def __getitem__(self, index: int) -> CryptoBar: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[CryptoBar, ...]: ...

    def __getitem__(self, index: int | slice) -> CryptoBar | tuple[CryptoBar, ...]:
        if isinstance(index, slice):
            start, stop, step = index.indices(self._stop)
            return tuple(self._bars[position] for position in range(start, stop, step))
        position = index
        if position < 0:
            position += self._stop
        if position < 0 or position >= self._stop:
            raise IndexError("history index out of range")
        return self._bars[position]


class CryptoStrategy(Protocol):
    """由 :class:`CryptoBacktestEngine` 使用的可调用接口。"""

    def __call__(self, event: BacktestBarEvent, /) -> Iterable[CryptoOrderRequest]: ...


@dataclass(frozen=True, slots=True)
class SimulatedFill:
    fill_id: str
    order_id: str
    symbol: str
    side: Side
    order_type: CryptoOrderType
    liquidity_role: LiquidityRole
    quantity: Decimal
    price: Decimal
    notional_quote: Decimal
    fee_quote: Decimal
    executed_at: datetime


@dataclass(frozen=True, slots=True)
class SimulatedOrderResult:
    order_id: str
    submitted_at: datetime
    side: Side
    order_type: CryptoOrderType
    requested_quantity: Decimal
    filled_quantity: Decimal
    remaining_quantity: Decimal
    status: SimulatedOrderStatus
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class EquityPoint:
    at: datetime
    equity_quote: Decimal
    portfolio: PortfolioSnapshot


@dataclass(frozen=True, slots=True)
class CryptoBacktestConfig:
    """一个现货交易对的执行与估值假设。"""

    symbol: str
    base_asset: str
    quote_asset: str
    fees: CryptoFeeConfig = field(default_factory=CryptoFeeConfig)
    market_slippage_rate: Decimal = Decimal("0")
    max_bar_volume_fraction: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        base = self.base_asset.strip().upper()
        quote = self.quote_asset.strip().upper()
        if not symbol or not base or not quote:
            raise ValueError("symbol, base_asset and quote_asset must not be empty")
        if base == quote:
            raise ValueError("base_asset and quote_asset must be distinct")
        if not isinstance(self.fees, CryptoFeeConfig):
            raise TypeError("fees must be CryptoFeeConfig")
        _require_decimal("market_slippage_rate", self.market_slippage_rate)
        if not Decimal("0") <= self.market_slippage_rate < Decimal("1"):
            raise ValueError("market_slippage_rate must be in [0, 1)")
        _require_decimal("max_bar_volume_fraction", self.max_bar_volume_fraction)
        if not Decimal("0") < self.max_bar_volume_fraction <= Decimal("1"):
            raise ValueError("max_bar_volume_fraction must be in (0, 1]")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "base_asset", base)
        object.__setattr__(self, "quote_asset", quote)


@dataclass(frozen=True, slots=True)
class CryptoBacktestReport:
    symbol: str
    initial_equity_quote: Decimal
    final_equity_quote: Decimal
    net_profit_quote: Decimal
    total_return: Decimal
    max_drawdown: Decimal
    total_fees_quote: Decimal
    turnover_quote: Decimal
    fills: tuple[SimulatedFill, ...]
    orders: tuple[SimulatedOrderResult, ...]
    equity_curve: tuple[EquityPoint, ...]
    final_portfolio: PortfolioSnapshot
    pending_order_ids: tuple[str, ...]

    @property
    def trade_count(self) -> int:
        return len(self.fills)


@dataclass(slots=True)
class _WorkingOrder:
    request: CryptoOrderRequest
    submitted_at: datetime
    remaining: Decimal
    filled: Decimal = Decimal("0")
    status: SimulatedOrderStatus = SimulatedOrderStatus.OPEN
    reason: str | None = None

    def result(self) -> SimulatedOrderResult:
        return SimulatedOrderResult(
            order_id=self.request.order_id,
            submitted_at=self.submitted_at,
            side=self.request.side,
            order_type=self.request.order_type,
            requested_quantity=self.request.quantity,
            filled_quantity=self.filled,
            remaining_quantity=self.remaining,
            status=self.status,
            reason=self.reason,
        )


class CryptoBacktestEngine:
    """通过确定性的单交易对模拟器回放已完成蜡烛线。"""

    def __init__(self, config: CryptoBacktestConfig) -> None:
        self._config = config

    def run(
        self,
        bars: tuple[CryptoBar, ...],
        strategy: CryptoStrategy,
        *,
        initial_balances: Mapping[str, Decimal],
    ) -> CryptoBacktestReport:
        """执行一次全新回放，调用之间不保留可变状态。"""

        self._validate_bars(bars)
        ledger = SpotLedger(initial_balances)
        unsupported = tuple(
            asset
            for asset, value in ledger.snapshot().balances
            if asset not in {self._config.base_asset, self._config.quote_asset} and value != 0
        )
        if unsupported:
            raise ValueError("single-pair backtest cannot value assets: " + ", ".join(unsupported))

        initial_equity = self._equity(ledger, bars[0].open)
        if initial_equity <= 0:
            raise ValueError("initial marked equity must be positive")
        equity_curve = [
            EquityPoint(
                at=bars[0].open_time,
                equity_quote=initial_equity,
                portfolio=ledger.snapshot(),
            )
        ]
        pending: list[_WorkingOrder] = []
        all_orders: list[_WorkingOrder] = []
        fills: list[SimulatedFill] = []
        seen_order_ids: set[str] = set()

        for index, bar in enumerate(bars):
            capacity = bar.volume * self._config.max_bar_volume_fraction
            capacity = self._match_pending(
                bar=bar,
                capacity=capacity,
                pending=pending,
                ledger=ledger,
                fills=fills,
            )
            del capacity  # 明确未使用的蜡烛线流动性不会结转
            equity_curve.append(
                EquityPoint(
                    at=bar.available_at,
                    equity_quote=self._equity(ledger, bar.close),
                    portfolio=ledger.snapshot(),
                )
            )
            event = BacktestBarEvent(
                decision_time=bar.available_at,
                bar=bar,
                history=_BarHistoryPrefix(bars, index + 1),
                portfolio=ledger.snapshot(),
            )
            for request in strategy(event):
                if not isinstance(request, CryptoOrderRequest):
                    raise TypeError("strategy must yield CryptoOrderRequest instances")
                if request.order_id in seen_order_ids:
                    raise ValueError(f"duplicate order_id: {request.order_id}")
                seen_order_ids.add(request.order_id)
                working = _WorkingOrder(
                    request=request,
                    submitted_at=event.decision_time,
                    remaining=request.quantity,
                )
                pending.append(working)
                all_orders.append(working)

        pending_ids = tuple(order.request.order_id for order in pending)
        for order in pending:
            if order.filled > 0:
                order.status = SimulatedOrderStatus.PARTIALLY_FILLED
                order.reason = "REPLAY_ENDED_WITH_REMAINDER"
            else:
                order.status = SimulatedOrderStatus.OPEN
                order.reason = "REPLAY_ENDED_BEFORE_FILL"

        final_equity = self._equity(ledger, bars[-1].close)
        total_fees = sum((fill.fee_quote for fill in fills), Decimal("0"))
        turnover = sum((fill.notional_quote for fill in fills), Decimal("0"))
        return CryptoBacktestReport(
            symbol=self._config.symbol,
            initial_equity_quote=initial_equity,
            final_equity_quote=final_equity,
            net_profit_quote=final_equity - initial_equity,
            total_return=final_equity / initial_equity - Decimal("1"),
            max_drawdown=_max_drawdown(equity_curve),
            total_fees_quote=total_fees,
            turnover_quote=turnover,
            fills=tuple(fills),
            orders=tuple(order.result() for order in all_orders),
            equity_curve=tuple(equity_curve),
            final_portfolio=ledger.snapshot(),
            pending_order_ids=pending_ids,
        )

    def _validate_bars(self, bars: tuple[CryptoBar, ...]) -> None:
        if not bars:
            raise ValueError("backtest requires at least one bar")
        if any(bar.symbol != self._config.symbol for bar in bars):
            raise ValueError("all bars must match configured symbol")
        if any(not bar.complete for bar in bars):
            raise ValueError("backtest requires completed bars")
        open_times = [bar.open_time for bar in bars]
        if open_times != sorted(open_times) or len(open_times) != len(set(open_times)):
            raise ValueError("bars must be strictly ordered and unique")
        for previous, current in zip(bars, bars[1:], strict=False):
            if previous.close_time > current.open_time:
                raise ValueError("bars must not overlap")
            if previous.available_at > current.open_time:
                raise ValueError("a bar must be available before the following bar opens")

    def _match_pending(
        self,
        *,
        bar: CryptoBar,
        capacity: Decimal,
        pending: list[_WorkingOrder],
        ledger: SpotLedger,
        fills: list[SimulatedFill],
    ) -> Decimal:
        retained: list[_WorkingOrder] = []
        for order in pending:
            if bar.open_time < order.submitted_at:
                retained.append(order)
                continue
            matched = self._match_price(order.request, bar)
            if matched is None:
                retained.append(order)
                continue
            price, role, executed_at = matched
            fill_quantity = min(order.remaining, capacity)
            if fill_quantity <= 0:
                if order.request.order_type is CryptoOrderType.MARKET:
                    self._finish_unfilled_market(order)
                else:
                    retained.append(order)
                continue
            notional = fill_quantity * price
            fee = notional * self._config.fees.effective_rate(role)
            try:
                ledger.apply_fill(
                    side=order.request.side,
                    base_asset=self._config.base_asset,
                    quote_asset=self._config.quote_asset,
                    quantity=fill_quantity,
                    price=price,
                    fee_quote=fee,
                )
            except InsufficientBalanceError as error:
                order.status = SimulatedOrderStatus.REJECTED
                order.reason = str(error)
                continue
            order.filled += fill_quantity
            order.remaining -= fill_quantity
            capacity -= fill_quantity
            fills.append(
                SimulatedFill(
                    fill_id=f"{order.request.order_id}:{len(fills) + 1}",
                    order_id=order.request.order_id,
                    symbol=self._config.symbol,
                    side=order.request.side,
                    order_type=order.request.order_type,
                    liquidity_role=role,
                    quantity=fill_quantity,
                    price=price,
                    notional_quote=notional,
                    fee_quote=fee,
                    executed_at=executed_at,
                )
            )
            if order.remaining == 0:
                order.status = SimulatedOrderStatus.FILLED
            elif order.request.order_type is CryptoOrderType.MARKET:
                order.status = SimulatedOrderStatus.PARTIALLY_FILLED
                order.reason = "MARKET_REMAINDER_CANCELED_BY_VOLUME_CAP"
            else:
                order.status = SimulatedOrderStatus.PARTIALLY_FILLED
                retained.append(order)
        pending[:] = retained
        return capacity

    def _match_price(
        self,
        request: CryptoOrderRequest,
        bar: CryptoBar,
    ) -> tuple[Decimal, LiquidityRole, datetime] | None:
        if request.order_type is CryptoOrderType.MARKET:
            multiplier = (
                Decimal("1") + self._config.market_slippage_rate
                if request.side is Side.BUY
                else Decimal("1") - self._config.market_slippage_rate
            )
            return bar.open * multiplier, LiquidityRole.TAKER, bar.open_time

        assert request.limit_price is not None
        if request.side is Side.BUY:
            if bar.low > request.limit_price:
                return None
            role = LiquidityRole.TAKER if request.limit_price >= bar.open else LiquidityRole.MAKER
        else:
            if bar.high < request.limit_price:
                return None
            role = LiquidityRole.TAKER if request.limit_price <= bar.open else LiquidityRole.MAKER
        executed_at = bar.open_time if role is LiquidityRole.TAKER else bar.close_time
        return request.limit_price, role, executed_at

    def _finish_unfilled_market(self, order: _WorkingOrder) -> None:
        if order.filled > 0:
            order.status = SimulatedOrderStatus.PARTIALLY_FILLED
            order.reason = "MARKET_REMAINDER_CANCELED_BY_VOLUME_CAP"
        else:
            order.status = SimulatedOrderStatus.REJECTED
            order.reason = "NO_BAR_LIQUIDITY"

    def _equity(self, ledger: SpotLedger, mark_price: Decimal) -> Decimal:
        return (
            ledger.balance(self._config.quote_asset)
            + ledger.balance(self._config.base_asset) * mark_price
        )


def _max_drawdown(points: list[EquityPoint]) -> Decimal:
    peak = points[0].equity_quote
    maximum = Decimal("0")
    for point in points[1:]:
        if point.equity_quote > peak:
            peak = point.equity_quote
            continue
        if peak > 0:
            maximum = max(maximum, (peak - point.equity_quote) / peak)
    return maximum


def _require_decimal(name: str, value: object) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")


def _require_aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
