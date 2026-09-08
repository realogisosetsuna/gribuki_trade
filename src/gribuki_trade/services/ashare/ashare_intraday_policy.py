"""A 股盘中 PAPER 执行策略的纯价格与风险配置。

本模块只包含不可变配置、价格走廊和板块价格规则。它不访问账户、行情、
SQLite 或券商；运行器通过历史兼容 facade 使用这些策略。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import PaperInstrumentType
from gribuki_trade.ports.ashare_screening import AShareBoard

if TYPE_CHECKING:
    from .ashare_intraday_paper import IntradayPaperOrder

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_ONE = Decimal("1")

@dataclass(frozen=True, slots=True)
class IntradayPaperRiskConfig:
    """实时 PAPER 交易日的资本、流动性与执行假设。"""

    initial_equity: Decimal = Decimal("200000")
    cash_reserve_fraction: Decimal = Decimal("0.20")
    maximum_gross_fraction: Decimal = Decimal("0.80")
    # ``None`` 刻意表示投资组合广度仅由现金、总敞口、单标的敞口与止损风险预算约束。
    # 对明确需要按数量熔断的运维人员，仍可配置正整数。
    maximum_positions: int | None = None
    maximum_symbol_fraction: Decimal = Decimal("0.20")
    risk_per_trade_fraction: Decimal = Decimal("0.0075")
    minimum_stop_fraction: Decimal = Decimal("0.015")
    stock_lot_size: int = 100
    volume_participation_rate: Decimal = Decimal("0.01")
    buy_limit_markup: Decimal = Decimal("0.001")
    sell_limit_markdown: Decimal = Decimal("0.001")
    stock_slippage_rate: Decimal = Decimal("0.0005")
    etf_slippage_rate: Decimal = Decimal("0.0002")
    stock_price_quantum: Decimal = Decimal("0.01")
    etf_price_quantum: Decimal = Decimal("0.001")
    latest_entry_time: time = time(14, 55)

    def __post_init__(self) -> None:
        _positive(self.initial_equity, "initial_equity")
        for name in (
            "cash_reserve_fraction",
            "maximum_gross_fraction",
            "maximum_symbol_fraction",
            "risk_per_trade_fraction",
            "minimum_stop_fraction",
            "volume_participation_rate",
        ):
            value = _positive(getattr(self, name), name)
            if value > _ONE:
                raise ValueError(f"{name} must not exceed one")
        if self.cash_reserve_fraction + self.maximum_gross_fraction > _ONE:
            raise ValueError("cash reserve plus maximum gross must not exceed one")
        if self.maximum_symbol_fraction > self.maximum_gross_fraction:
            raise ValueError("maximum_symbol_fraction cannot exceed maximum gross")
        if self.volume_participation_rate > Decimal("0.25"):
            raise ValueError("volume_participation_rate must not exceed 25%")
        for name in (
            "buy_limit_markup",
            "sell_limit_markdown",
            "stock_slippage_rate",
            "etf_slippage_rate",
        ):
            value = _non_negative(getattr(self, name), name)
            if value > Decimal("0.05"):
                raise ValueError(f"{name} must not exceed 5%")
        _positive(self.stock_price_quantum, "stock_price_quantum")
        _positive(self.etf_price_quantum, "etf_price_quantum")
        maximum_positions = self.maximum_positions
        if maximum_positions is not None and (
            isinstance(maximum_positions, bool)
            or not isinstance(maximum_positions, int)
            or maximum_positions <= 0
        ):
            raise ValueError("maximum_positions must be a positive integer or None")
        if (
            isinstance(self.stock_lot_size, bool)
            or not isinstance(self.stock_lot_size, int)
            or self.stock_lot_size <= 0
        ):
            raise ValueError("stock_lot_size must be a positive integer")
        if self.stock_lot_size != 100:
            raise ValueError("standard A-share stock_lot_size must be 100")
        if not isinstance(self.latest_entry_time, time):
            raise TypeError("latest_entry_time must be a time")
        if self.latest_entry_time.tzinfo is not None:
            raise ValueError("latest_entry_time must be an Asia/Shanghai wall time")

    def audit_document(self) -> dict[str, object]:
        """返回用于哈希链审计的完整非秘密风险策略。"""

        return {
            "initial_equity": str(self.initial_equity),
            "cash_reserve_fraction": str(self.cash_reserve_fraction),
            "maximum_gross_fraction": str(self.maximum_gross_fraction),
            "maximum_positions": self.maximum_positions,
            "position_count_limit_enabled": self.maximum_positions is not None,
            "maximum_symbol_fraction": str(self.maximum_symbol_fraction),
            "risk_per_trade_fraction": str(self.risk_per_trade_fraction),
            "minimum_stop_fraction": str(self.minimum_stop_fraction),
            "stock_lot_size": self.stock_lot_size,
            "volume_participation_rate": str(self.volume_participation_rate),
            "buy_limit_markup": str(self.buy_limit_markup),
            "sell_limit_markdown": str(self.sell_limit_markdown),
            "stock_slippage_rate": str(self.stock_slippage_rate),
            "etf_slippage_rate": str(self.etf_slippage_rate),
            "stock_price_quantum": str(self.stock_price_quantum),
            "etf_price_quantum": str(self.etf_price_quantum),
            "latest_entry_time": self.latest_entry_time.isoformat(),
            "execution_policy": "NEXT_FULLY_POST_SIGNAL_1M_INTERVAL_IOC",
            "price_acceptance_policy": {
                "version": "ashare-intraday-price-acceptance@1",
                "buy_lower": "FIRST_TICK_STRICTLY_ABOVE_INVALIDATION",
                "buy_upper": "SIGNAL_PRICE_PLUS_MARKUP_CAPPED_BY_DAILY_BAND",
                "sell_lower": "REFERENCE_PRICE_MINUS_MARKDOWN_FLOORED_AT_DAILY_BAND",
                "sell_upper": "DAILY_LIMIT_UP_BETTER_PRICE_ACCEPTED",
                "standard_non_st_board_rates": {
                    "SSE_MAIN": "0.10",
                    "SZSE_MAIN": "0.10",
                    "CHINEXT": "0.20",
                    "STAR": "0.20",
                },
                "st_and_bse_execution": "UNSUPPORTED",
                "ipo_special_price_band": (
                    "NO_SPECIAL_BAND_INFERENCE_CONSERVATIVE_STANDARD_BAND_ONLY"
                ),
                "buy_bar_touching_invalidation": "NO_FILL_IOC",
                "locked_limit_up_queue": "NO_FILL_UNMODELED",
                "continuous_auction_price_cage": (
                    "NOT_L1_VERIFIED_PAPER_MINUTE_BAR_ONLY"
                ),
                "real_broker_pretrade_requirement": (
                    "VERIFY_EXCHANGE_STATIC_RULES_AND_L1_BASIS_PRICE"
                ),
            },
            "order_quantity_policy": {
                "version": "ashare-intraday-order-quantity@1",
                "SSE_MAIN": {
                    "minimum_buy_quantity": 100,
                    "buy_increment": 100,
                    "maximum_limit_order_quantity": 1_000_000,
                    "paper_partial_fill_increment": 100,
                    "minimum_regular_sell_quantity": 100,
                    "sell_increment": 100,
                    "sell_residual_policy": "BELOW_100_SELL_ALL_ONCE",
                },
                "SZSE_MAIN": {
                    "minimum_buy_quantity": 100,
                    "buy_increment": 100,
                    "maximum_limit_order_quantity": 1_000_000,
                    "paper_partial_fill_increment": 100,
                    "minimum_regular_sell_quantity": 100,
                    "sell_increment": 100,
                    "sell_residual_policy": "BELOW_100_SELL_ALL_ONCE",
                },
                "CHINEXT": {
                    "minimum_buy_quantity": 100,
                    "buy_increment": 100,
                    "maximum_limit_order_quantity": 300_000,
                    "paper_partial_fill_increment": 100,
                    "minimum_regular_sell_quantity": 100,
                    "sell_increment": 100,
                    "sell_residual_policy": "BELOW_100_SELL_ALL_ONCE",
                },
                "STAR": {
                    "minimum_buy_quantity": 200,
                    "buy_increment": 1,
                    "maximum_limit_order_quantity": 100_000,
                    "paper_partial_fill_increment": 1,
                    "minimum_regular_sell_quantity": 200,
                    "sell_increment": 1,
                    "sell_residual_policy": "BELOW_200_SELL_ALL_ONCE",
                },
                "BSE": "UNSUPPORTED",
            },
        }


@dataclass(frozen=True, slots=True)
class IntradayPriceAcceptance:
    """单侧订单的不可变策略与交易所价格走廊。

    ``acceptable_lower`` 与 ``acceptable_upper`` 是按已配置最小变动价位计算、包含端点的
    可执行价格。买入时，下边界是严格高于技术失效价的首个合法价位，上边界是买入限价；
    卖出时，下边界是卖出限价，上边界是交易所涨停价（更优卖价仍可接受）。

    该走廊刻意区别于预测或目标价格区间。它是失败关闭的执行契约，可以随信号记入日志，
    并在不查询未来数据的情况下回放。
    """

    side: Side
    board: AShareBoard
    reference_price: Decimal
    limit_price: Decimal
    acceptable_lower: Decimal
    acceptable_upper: Decimal
    exchange_lower: Decimal
    exchange_upper: Decimal
    price_tick: Decimal
    invalidation_price: Decimal | None = None
    policy_version: str = "ashare-intraday-price-acceptance@1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "side", Side(self.side))
        object.__setattr__(self, "board", AShareBoard(self.board))
        for name in (
            "reference_price",
            "limit_price",
            "acceptable_lower",
            "acceptable_upper",
            "exchange_lower",
            "exchange_upper",
            "price_tick",
        ):
            _positive(getattr(self, name), name)
        if self.invalidation_price is not None:
            _positive(self.invalidation_price, "invalidation_price")
        if not (
            self.exchange_lower
            <= self.acceptable_lower
            <= self.acceptable_upper
            <= self.exchange_upper
        ):
            raise ValueError("acceptable corridor must be inside the exchange band")
        if not self.exchange_lower <= self.reference_price <= self.exchange_upper:
            raise ValueError("reference_price must be inside the exchange band")
        if self.side is Side.BUY:
            if self.limit_price != self.acceptable_upper:
                raise ValueError("BUY limit must equal acceptable upper edge")
            if self.invalidation_price is None:
                raise ValueError("BUY acceptance requires an invalidation price")
            if self.acceptable_lower <= self.invalidation_price:
                raise ValueError("BUY acceptable prices must be above invalidation")
        else:
            if self.limit_price != self.acceptable_lower:
                raise ValueError("SELL limit must equal acceptable lower edge")
            if self.invalidation_price is not None:
                raise ValueError("SELL acceptance does not use a long invalidation price")
        if not self.policy_version.strip():
            raise ValueError("policy_version must not be empty")

    def contains(self, price: Decimal) -> bool:
        """返回 ``price`` 在冻结价格走廊下是否可执行。"""

        return (
            price.is_finite()
            and self.acceptable_lower <= price <= self.acceptable_upper
        )



def _price_quantum(
    instrument_type: PaperInstrumentType,
    config: IntradayPaperRiskConfig,
) -> Decimal:
    return (
        config.stock_price_quantum
        if instrument_type is PaperInstrumentType.STOCK
        else config.etf_price_quantum
    )


def build_intraday_buy_price_acceptance(
    *,
    reference_price: Decimal,
    invalidation_price: Decimal,
    previous_close: Decimal,
    board: AShareBoard | str,
    instrument_type: PaperInstrumentType = PaperInstrumentType.STOCK,
    config: IntradayPaperRiskConfig | None = None,
) -> IntradayPriceAcceptance:
    """冻结允许模拟建立新多头仓位的唯一价格集合。

    失效边界不包含端点。例如，失效价为 9.80 元、股票最小变动价位为 0.01 元时，
    9.81 元是首个可接受价格；9.80 元或更低的价格不会被视为便宜入场机会。
    """

    resolved = config or IntradayPaperRiskConfig()
    board_value = AShareBoard(board)
    instrument = PaperInstrumentType(instrument_type)
    tick = _price_quantum(instrument, resolved)
    for value, name in (
        (reference_price, "reference_price"),
        (invalidation_price, "invalidation_price"),
        (previous_close, "previous_close"),
    ):
        _positive(value, name)
    if invalidation_price >= reference_price:
        raise ValueError("invalidation_price must be below reference_price")
    exchange_lower, exchange_upper = ashare_daily_price_band(
        previous_close,
        board_value,
        tick,
    )
    proposed_limit = (reference_price * (_ONE + resolved.buy_limit_markup)).quantize(
        tick,
        rounding=ROUND_CEILING,
    )
    if not exchange_lower <= reference_price <= exchange_upper:
        raise ValueError("reference_price is outside the exchange band")
    # 冻结策略明确将策略加价上限限制在涨停价。若信号本身仍符合交易所规则，不能只因
    # 配置加价会越过该边界而拒绝。执行仍保持保守：匹配器会拒绝封死涨停的行情柱，
    # 因为分钟 OHLC 无法证明队列优先级。
    limit = min(proposed_limit, exchange_upper)
    first_tick_above_invalidation = (
        invalidation_price.quantize(tick, rounding=ROUND_FLOOR) + tick
    )
    acceptable_lower = max(exchange_lower, first_tick_above_invalidation)
    if acceptable_lower > limit:
        raise ValueError("BUY acceptable price corridor is empty")
    return IntradayPriceAcceptance(
        side=Side.BUY,
        board=board_value,
        reference_price=reference_price,
        limit_price=limit,
        acceptable_lower=acceptable_lower,
        acceptable_upper=limit,
        exchange_lower=exchange_lower,
        exchange_upper=exchange_upper,
        price_tick=tick,
        invalidation_price=invalidation_price,
    )


def build_intraday_sell_price_acceptance(
    *,
    reference_price: Decimal,
    previous_close: Decimal,
    board: AShareBoard | str,
    instrument_type: PaperInstrumentType = PaperInstrumentType.STOCK,
    config: IntradayPaperRiskConfig | None = None,
) -> IntradayPriceAcceptance:
    """为未来 REDUCE 执行冻结保守的限价走廊。

    当前日内运行器记录 REDUCE 信号，但刻意不提交。持久化该走廊可使记录信号供未来
    T+1 卖出执行器使用：低于 ``limit_price`` 的跳空明确不可执行，而从卖出限价到
    交易所涨停价之间的任何价格都可接受。
    """

    resolved = config or IntradayPaperRiskConfig()
    board_value = AShareBoard(board)
    instrument = PaperInstrumentType(instrument_type)
    tick = _price_quantum(instrument, resolved)
    _positive(reference_price, "reference_price")
    _positive(previous_close, "previous_close")
    exchange_lower, exchange_upper = ashare_daily_price_band(
        previous_close,
        board_value,
        tick,
    )
    if not exchange_lower <= reference_price <= exchange_upper:
        raise ValueError("reference_price is outside the exchange band")
    raw_limit = (reference_price * (_ONE - resolved.sell_limit_markdown)).quantize(
        tick,
        rounding=ROUND_FLOOR,
    )
    limit = max(exchange_lower, raw_limit)
    if limit > exchange_upper:
        raise ValueError("SELL acceptable price corridor is empty")
    return IntradayPriceAcceptance(
        side=Side.SELL,
        board=board_value,
        reference_price=reference_price,
        limit_price=limit,
        acceptable_lower=limit,
        acceptable_upper=exchange_upper,
        exchange_lower=exchange_lower,
        exchange_upper=exchange_upper,
        price_tick=tick,
    )


def intraday_buy_order_price_acceptance(
    order: IntradayPaperOrder,
    *,
    config: IntradayPaperRiskConfig | None = None,
) -> IntradayPriceAcceptance:
    """为买单重建并核验不可变价格走廊。"""

    resolved = config or IntradayPaperRiskConfig()
    tick = _price_quantum(order.instrument_type, resolved)
    exchange_lower, exchange_upper = ashare_daily_price_band(
        order.previous_close,
        order.board,
        tick,
    )
    if not exchange_lower <= order.limit_price <= exchange_upper:
        raise ValueError("stored order limit is outside the exchange band")
    acceptable_lower = max(
        exchange_lower,
        order.invalidation_price.quantize(tick, rounding=ROUND_FLOOR) + tick,
    )
    if acceptable_lower > order.limit_price:
        raise ValueError("stored BUY order has an empty acceptable corridor")
    return IntradayPriceAcceptance(
        side=Side.BUY,
        board=order.board,
        reference_price=order.signal_price,
        limit_price=order.limit_price,
        acceptable_lower=acceptable_lower,
        acceptable_upper=order.limit_price,
        exchange_lower=exchange_lower,
        exchange_upper=exchange_upper,
        price_tick=tick,
        invalidation_price=order.invalidation_price,
    )



def ashare_daily_price_band(
    previous_close: Decimal,
    board: AShareBoard,
    tick: Decimal,
) -> tuple[Decimal, Decimal]:
    """返回受支持板块非 ST A 股的标准每日涨跌幅限制。

    运行器的标的全集排除 ST 证券，执行路径拒绝北交所订单，因此主板股票采用 ±10%，
    创业板与科创板采用 ±20%。缺少上市阶段元数据时，IPO/无涨跌幅限制交易日绝不会
    获得更宽区间；在更丰富的标的规则适配器核验特殊制度前，本函数保持保守标准区间。
    """

    _positive(previous_close, "previous_close")
    _positive(tick, "tick")
    board = AShareBoard(board)
    if board is AShareBoard.BSE:
        raise ValueError("BSE PAPER execution is intentionally unsupported")
    rate = (
        Decimal("0.20")
        if board in {AShareBoard.CHINEXT, AShareBoard.STAR}
        else Decimal("0.10")
    )
    return (
        (previous_close * (_ONE - rate)).quantize(tick, rounding=ROUND_HALF_UP),
        (previous_close * (_ONE + rate)).quantize(tick, rounding=ROUND_HALF_UP),
    )


def _board_matches_symbol(symbol: str, board: AShareBoard) -> bool:
    if (
        len(symbol) != 9
        or symbol[6] != "."
        or not symbol[:6].isascii()
        or not symbol[:6].isdigit()
    ):
        return False
    code = symbol[:6]
    exchange = symbol[-2:]
    if board is AShareBoard.STAR:
        return exchange == "SH" and code.startswith(("688", "689"))
    if board is AShareBoard.CHINEXT:
        return exchange == "SZ" and code.startswith(("300", "301"))
    if board is AShareBoard.SSE_MAIN:
        return exchange == "SH" and not code.startswith(("688", "689"))
    if board is AShareBoard.SZSE_MAIN:
        return exchange == "SZ" and not code.startswith(("300", "301"))
    return False



def _is_positive(value: object) -> bool:
    return isinstance(value, Decimal) and value.is_finite() and value > 0


def _positive(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return value


def _non_negative(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError(f"{name} must be non-negative and finite")
    return value


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)




__all__ = [
    "IntradayPaperRiskConfig",
    "IntradayPriceAcceptance",
    "ashare_daily_price_band",
    "build_intraday_buy_price_acceptance",
    "build_intraday_sell_price_acceptance",
    "intraday_buy_order_price_acceptance",
    "_SHANGHAI", "_ONE", "_board_matches_symbol", "_is_positive",
    "_non_negative", "_positive", "_aware_utc", "_price_quantum",
]
