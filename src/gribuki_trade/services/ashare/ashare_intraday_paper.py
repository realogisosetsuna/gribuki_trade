"""面向 A 股买入信号、失败关闭的纯盘中 PAPER 执行。

本模块刻意不产生时钟、数据库、券商或市场数据副作用。调用方先根据已完成技术信号与
不可变模拟账户快照确定买单规模，再向 IOC 匹配器提供恰好一根随后完成的一分钟行情柱。
成功结果包含可由 ``ASharePaperTradingService`` 记录的 ``ASharePaperFill`` 命令。

行情柱匹配器采用保守模型，而非订单簿模拟器。它只在下一根已完成行情柱可观测后使用
该柱，假设从开盘价发生不利滑点，绝不以穿透订单限价的价格成交，并将成交量限制在
行情柱所报股份成交量的百分之一。它不能模拟队列优先级、封死涨停、延迟或隐藏流动性。
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from enum import StrEnum

from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import (
    ASharePaperFill,
    PaperAccountSnapshot,
    PaperFeeSchedule,
    PaperFillSource,
    PaperInstrumentType,
)
from gribuki_trade.domain.recommendations import RecommendationDecision
from gribuki_trade.features.technical import TechnicalSignal
from gribuki_trade.ports.ashare_screening import AShareBoard
from gribuki_trade.ports.market_data import IntradayBar, MinuteInterval
from gribuki_trade.services.ashare.ashare_intraday_quantity import (
    IntradayOrderQuantityRule,
    IntradaySellQuantityPlan,
    IntradaySellQuantityStatus,
    build_intraday_sell_quantity_plan,
    intraday_order_quantity_rule,
)

from .ashare_intraday_policy import (
    _ONE,
    _SHANGHAI,
    IntradayPaperRiskConfig,
    IntradayPriceAcceptance,
    _aware_utc,
    _board_matches_symbol,
    _is_positive,
    _non_negative,
    _positive,
    _price_quantum,
    ashare_daily_price_band,
    build_intraday_buy_price_acceptance,
    build_intraday_sell_price_acceptance,
    intraday_buy_order_price_acceptance,
)


class IntradayPaperRiskStatus(StrEnum):
    """信号是否生成了风险批准的 PAPER 订单。"""

    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class IntradayPaperRiskReason(StrEnum):
    """稳定的规模确定与交易前拒绝说明。"""

    APPROVED = "APPROVED"
    SIGNAL_NOT_ENTER_CANDIDATE = "SIGNAL_NOT_ENTER_CANDIDATE"
    SIGNAL_PRICE_MISSING = "SIGNAL_PRICE_MISSING"
    INVALIDATION_PRICE_MISSING = "INVALIDATION_PRICE_MISSING"
    INVALIDATION_NOT_BELOW_ENTRY = "INVALIDATION_NOT_BELOW_ENTRY"
    SIGNAL_NOT_COMPLETED = "SIGNAL_NOT_COMPLETED"
    SIGNAL_SESSION_MISMATCH = "SIGNAL_SESSION_MISMATCH"
    ACCOUNT_SESSION_MISMATCH = "ACCOUNT_SESSION_MISMATCH"
    ENTRY_CUTOFF_PASSED = "ENTRY_CUTOFF_PASSED"
    BOARD_MISSING = "BOARD_MISSING"
    BOARD_UNSUPPORTED = "BOARD_UNSUPPORTED"
    BOARD_SYMBOL_MISMATCH = "BOARD_SYMBOL_MISMATCH"
    PREVIOUS_CLOSE_MISSING = "PREVIOUS_CLOSE_MISSING"
    PRICE_OUTSIDE_DAILY_BAND = "PRICE_OUTSIDE_DAILY_BAND"
    LIMIT_OUTSIDE_DAILY_BAND = "LIMIT_OUTSIDE_DAILY_BAND"
    PROTECTIVE_STOP_INVALID = "PROTECTIVE_STOP_INVALID"
    MAX_POSITIONS_REACHED = "MAX_POSITIONS_REACHED"
    GROSS_LIMIT_REACHED = "GROSS_LIMIT_REACHED"
    SYMBOL_LIMIT_REACHED = "SYMBOL_LIMIT_REACHED"
    CASH_RESERVE_BINDING = "CASH_RESERVE_BINDING"
    BELOW_ROUND_LOT = "BELOW_ROUND_LOT"
    BELOW_BOARD_MINIMUM_BUY = "BELOW_BOARD_MINIMUM_BUY"


class IntradayPaperMatchStatus(StrEnum):
    """单行情柱 IOC PAPER 匹配的终态。"""

    FILLED = "FILLED"
    PARTIALLY_FILLED_IOC = "PARTIALLY_FILLED_IOC"
    NOT_FILLED_IOC = "NOT_FILLED_IOC"
    REJECTED = "REJECTED"


class IntradayPaperMatchReason(StrEnum):
    """单行情柱匹配终态的稳定说明。"""

    FILLED = "FILLED"
    PARTIAL_VOLUME_CAPACITY = "PARTIAL_VOLUME_CAPACITY"
    SYMBOL_MISMATCH = "SYMBOL_MISMATCH"
    BAR_NOT_CLOSED = "BAR_NOT_CLOSED"
    BAR_NOT_ONE_MINUTE = "BAR_NOT_ONE_MINUTE"
    BAR_NOT_STRICTLY_AFTER_SIGNAL = "BAR_NOT_STRICTLY_AFTER_SIGNAL"
    BAR_STARTED_BEFORE_ORDER = "BAR_STARTED_BEFORE_ORDER"
    SESSION_MISMATCH = "SESSION_MISMATCH"
    ORDER_EXPIRED = "ORDER_EXPIRED"
    BAR_NOT_YET_OBSERVABLE = "BAR_NOT_YET_OBSERVABLE"
    BAR_OHLC_INVALID = "BAR_OHLC_INVALID"
    BAR_VOLUME_ZERO = "BAR_VOLUME_ZERO"
    BAR_OUTSIDE_DAILY_BAND = "BAR_OUTSIDE_DAILY_BAND"
    LOCKED_LIMIT_UP_QUEUE_UNMODELED = "LOCKED_LIMIT_UP_QUEUE_UNMODELED"
    SIGNAL_INVALIDATED_BEFORE_FILL = "SIGNAL_INVALIDATED_BEFORE_FILL"
    EXIT_CONDITION_MET_BEFORE_FILL = "EXIT_CONDITION_MET_BEFORE_FILL"
    EXIT_PLAN_MISSING_BEFORE_FILL = "EXIT_PLAN_MISSING_BEFORE_FILL"
    LIMIT_NOT_TOUCHED = "LIMIT_NOT_TOUCHED"
    VOLUME_CAPACITY_BELOW_LOT = "VOLUME_CAPACITY_BELOW_LOT"
    FILL_OUTSIDE_ACCEPTABLE_RANGE = "FILL_OUTSIDE_ACCEPTABLE_RANGE"
    ORDER_QUANTITY_OUTSIDE_BOARD_RULES = "ORDER_QUANTITY_OUTSIDE_BOARD_RULES"


@dataclass(frozen=True, slots=True)
class IntradayPaperOrder:
    """经风险批准、仅买入、使用单分钟柱 IOC 的 PAPER 订单。"""

    order_id: str
    account_id: str
    symbol: str
    board: AShareBoard
    session_date: date
    signal_bar_end: datetime
    signal_price: Decimal
    invalidation_price: Decimal
    limit_price: Decimal
    quantity: int
    created_at: datetime
    expires_at: datetime
    previous_close: Decimal
    instrument_type: PaperInstrumentType

    def __post_init__(self) -> None:
        if not self.order_id.strip() or len(self.order_id) > 128:
            raise ValueError("order_id must be non-empty and at most 128 characters")
        if not self.account_id.strip():
            raise ValueError("account_id must not be empty")
        symbol = self.symbol.strip().upper()
        if symbol != self.symbol or len(symbol) != 9 or symbol[6] != ".":
            raise ValueError("symbol must use canonical 600000.SH form")
        object.__setattr__(self, "board", AShareBoard(self.board))
        if self.board is AShareBoard.BSE:
            raise ValueError("BSE PAPER execution is intentionally unsupported")
        if not _board_matches_symbol(symbol, self.board):
            raise ValueError("symbol is inconsistent with its A-share board")
        object.__setattr__(self, "instrument_type", PaperInstrumentType(self.instrument_type))
        for name in (
            "signal_price",
            "invalidation_price",
            "limit_price",
            "previous_close",
        ):
            _positive(getattr(self, name), name)
        if self.invalidation_price >= self.signal_price:
            raise ValueError("invalidation_price must be below signal_price")
        if isinstance(self.quantity, bool) or not isinstance(self.quantity, int):
            raise TypeError("quantity must be an integer number of shares")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        signal_bar_end = _aware_utc(self.signal_bar_end, "signal_bar_end")
        created_at = _aware_utc(self.created_at, "created_at")
        expires_at = _aware_utc(self.expires_at, "expires_at")
        if signal_bar_end > created_at:
            raise ValueError("created_at cannot precede signal_bar_end")
        if expires_at < created_at:
            raise ValueError("expires_at cannot precede created_at")
        if signal_bar_end.astimezone(_SHANGHAI).date() != self.session_date:
            raise ValueError("signal_bar_end must belong to session_date")
        object.__setattr__(self, "signal_bar_end", signal_bar_end)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "expires_at", expires_at)


@dataclass(frozen=True, slots=True)
class IntradayPaperRiskOutcome:
    """为单个技术买入信号确定规模后的类型化可审计结果。"""

    status: IntradayPaperRiskStatus
    reason: IntradayPaperRiskReason
    order: IntradayPaperOrder | None
    risk_budget: Decimal
    stop_distance: Decimal
    gross_exposure: Decimal
    available_cash_budget: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", IntradayPaperRiskStatus(self.status))
        object.__setattr__(self, "reason", IntradayPaperRiskReason(self.reason))
        for name in (
            "risk_budget",
            "stop_distance",
            "gross_exposure",
            "available_cash_budget",
        ):
            _non_negative(getattr(self, name), name)
        if (self.status is IntradayPaperRiskStatus.APPROVED) != (self.order is not None):
            raise ValueError("an approved risk outcome must contain exactly one order")
        if self.status is IntradayPaperRiskStatus.APPROVED and (
            self.reason is not IntradayPaperRiskReason.APPROVED
        ):
            raise ValueError("approved risk outcome must use APPROVED reason")


@dataclass(frozen=True, slots=True)
class IntradayPaperMatchOutcome:
    """向 IOC 订单提供一根已完成行情柱后的终态结果。"""

    status: IntradayPaperMatchStatus
    reason: IntradayPaperMatchReason
    order_id: str
    requested_quantity: int
    filled_quantity: int
    cancelled_quantity: int
    volume_capacity: int
    fill_price: Decimal | None = None
    fill: ASharePaperFill | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", IntradayPaperMatchStatus(self.status))
        object.__setattr__(self, "reason", IntradayPaperMatchReason(self.reason))
        if min(
            self.requested_quantity,
            self.filled_quantity,
            self.cancelled_quantity,
            self.volume_capacity,
        ) < 0:
            raise ValueError("match quantities must be non-negative")
        if self.filled_quantity + self.cancelled_quantity != self.requested_quantity:
            raise ValueError("a terminal IOC outcome must fill or cancel every share")
        has_fill = self.filled_quantity > 0
        if has_fill != (self.fill_price is not None and self.fill is not None):
            raise ValueError("fill, price, and positive filled quantity must agree")
        if self.fill is not None and self.fill.quantity != self.filled_quantity:
            raise ValueError("fill command quantity conflicts with outcome")


def build_intraday_buy_order(
    signal: TechnicalSignal,
    account: PaperAccountSnapshot,
    *,
    board: AShareBoard | str | None,
    signal_bar_end: datetime,
    previous_close: Decimal | None,
    created_at: datetime,
    instrument_type: PaperInstrumentType = PaperInstrumentType.STOCK,
    position_marks: Mapping[str, Decimal] | None = None,
    config: IntradayPaperRiskConfig | None = None,
    fee_schedule: PaperFeeSchedule | None = None,
    protective_stop_price: Decimal | None = None,
) -> IntradayPaperRiskOutcome:
    """在不查询未来市场数据的情况下确定仅买入 PAPER 订单规模。

    应用风险、单标的、总敞口与现金容量（包括最坏情形买入费用）后，数量按板块提交规则
    向下取整。科创板最低 200 股，超过后按 1 股递增；其他受支持股票板块保持 100 股整数倍。
    """

    resolved = config or IntradayPaperRiskConfig()
    fees = fee_schedule or PaperFeeSchedule()
    created_at = _aware_utc(created_at, "created_at")
    signal_bar_end = _aware_utc(signal_bar_end, "signal_bar_end")
    instrument_type = PaperInstrumentType(instrument_type)
    zero = Decimal("0")
    risk_budget = resolved.initial_equity * resolved.risk_per_trade_fraction

    def reject(
        reason: IntradayPaperRiskReason,
        *,
        stop_distance: Decimal = zero,
        gross: Decimal = zero,
        cash_budget: Decimal = zero,
    ) -> IntradayPaperRiskOutcome:
        return IntradayPaperRiskOutcome(
            status=IntradayPaperRiskStatus.REJECTED,
            reason=reason,
            order=None,
            risk_budget=risk_budget,
            stop_distance=stop_distance,
            gross_exposure=gross,
            available_cash_budget=cash_budget,
        )

    if signal.decision is not RecommendationDecision.ENTER_CANDIDATE:
        return reject(IntradayPaperRiskReason.SIGNAL_NOT_ENTER_CANDIDATE)
    entry = signal.reference_price
    if not _is_positive(entry):
        return reject(IntradayPaperRiskReason.SIGNAL_PRICE_MISSING)
    invalidation = signal.invalidation_price
    if not _is_positive(invalidation):
        return reject(IntradayPaperRiskReason.INVALIDATION_PRICE_MISSING)
    assert entry is not None and invalidation is not None
    if invalidation >= entry:
        return reject(IntradayPaperRiskReason.INVALIDATION_NOT_BELOW_ENTRY)
    if signal.as_of.tzinfo is None or signal.as_of.utcoffset() is None:
        raise ValueError("signal.as_of must be timezone-aware")
    signal_known_at = signal.as_of.astimezone(UTC)
    if signal_bar_end > signal_known_at or signal_known_at > created_at:
        return reject(IntradayPaperRiskReason.SIGNAL_NOT_COMPLETED)
    session = created_at.astimezone(_SHANGHAI).date()
    if signal_bar_end.astimezone(_SHANGHAI).date() != session:
        return reject(IntradayPaperRiskReason.SIGNAL_SESSION_MISMATCH)
    if account.session_date != session:
        return reject(IntradayPaperRiskReason.ACCOUNT_SESSION_MISMATCH)
    cutoff = datetime.combine(session, resolved.latest_entry_time, tzinfo=_SHANGHAI)
    if created_at > cutoff.astimezone(UTC):
        return reject(IntradayPaperRiskReason.ENTRY_CUTOFF_PASSED)
    if board is None:
        return reject(IntradayPaperRiskReason.BOARD_MISSING)
    try:
        resolved_board = AShareBoard(board)
    except ValueError:
        return reject(IntradayPaperRiskReason.BOARD_MISSING)
    if resolved_board is AShareBoard.BSE:
        return reject(IntradayPaperRiskReason.BOARD_UNSUPPORTED)
    quantity_rule = intraday_order_quantity_rule(resolved_board)
    normalized_symbol = signal.symbol.strip().upper()
    if not _board_matches_symbol(normalized_symbol, resolved_board):
        return reject(IntradayPaperRiskReason.BOARD_SYMBOL_MISMATCH)
    if not _is_positive(previous_close):
        return reject(IntradayPaperRiskReason.PREVIOUS_CLOSE_MISSING)
    assert previous_close is not None

    tick = _price_quantum(instrument_type, resolved)
    lower, upper = ashare_daily_price_band(previous_close, resolved_board, tick)
    if not lower <= entry <= upper:
        return reject(IntradayPaperRiskReason.PRICE_OUTSIDE_DAILY_BAND)
    try:
        price_acceptance = build_intraday_buy_price_acceptance(
            reference_price=entry,
            invalidation_price=invalidation,
            previous_close=previous_close,
            board=resolved_board,
            instrument_type=instrument_type,
            config=resolved,
        )
    except ValueError:
        return reject(IntradayPaperRiskReason.LIMIT_OUTSIDE_DAILY_BAND)
    limit_price = price_acceptance.limit_price
    risk_stop = invalidation if protective_stop_price is None else protective_stop_price
    if not _is_positive(risk_stop) or risk_stop >= limit_price:
        return reject(IntradayPaperRiskReason.PROTECTIVE_STOP_INVALID)
    assert risk_stop is not None

    marks = {key.strip().upper(): value for key, value in (position_marks or {}).items()}
    gross = Decimal("0")
    symbol_exposure = Decimal("0")
    active_positions = 0
    for position in account.positions:
        if position.quantity <= 0:
            continue
        active_positions += 1
        mark = marks.get(position.symbol, position.average_cost)
        if not _is_positive(mark):
            mark = position.average_cost
        value = mark * position.quantity
        gross += value
        if position.symbol == normalized_symbol:
            symbol_exposure = value
    if (
        symbol_exposure == 0
        and resolved.maximum_positions is not None
        and active_positions >= resolved.maximum_positions
    ):
        return reject(IntradayPaperRiskReason.MAX_POSITIONS_REACHED, gross=gross)

    gross_headroom = resolved.initial_equity * resolved.maximum_gross_fraction - gross
    if gross_headroom <= 0:
        return reject(IntradayPaperRiskReason.GROSS_LIMIT_REACHED, gross=gross)
    symbol_headroom = (
        resolved.initial_equity * resolved.maximum_symbol_fraction - symbol_exposure
    )
    if symbol_headroom <= 0:
        return reject(IntradayPaperRiskReason.SYMBOL_LIMIT_REACHED, gross=gross)
    cash_headroom = (
        account.cash
        - resolved.initial_equity * resolved.cash_reserve_fraction
    )
    if cash_headroom <= 0:
        return reject(IntradayPaperRiskReason.CASH_RESERVE_BINDING, gross=gross)
    cash_budget = min(gross_headroom, symbol_headroom, cash_headroom)

    # 按该限价单允许支付的最坏价格确定规模。若实际按已配置买入加价成交，在此使用较低
    # 信号参考价会低估每股亏损。
    stop_distance = max(
        limit_price - risk_stop,
        limit_price * resolved.minimum_stop_fraction,
    )
    risk_quantity = risk_budget / stop_distance
    # 单标的敞口也必须按可能实际支付的最坏限价计算。用较低的信号价会在
    # 上浮限价成交时悄悄突破 maximum_symbol_fraction。
    symbol_quantity = symbol_headroom / limit_price
    budget_quantity = cash_budget / limit_price
    raw_quantity = min(risk_quantity, symbol_quantity, budget_quantity)
    quantity = quantity_rule.floor_buy_submission(raw_quantity)
    while quantity >= quantity_rule.minimum_buy_quantity and (
        _buy_cash_required(limit_price, quantity, instrument_type, fees) > cash_budget
    ):
        quantity -= quantity_rule.buy_increment
    if not quantity_rule.accepts_buy_submission(quantity):
        return reject(
            IntradayPaperRiskReason.BELOW_BOARD_MINIMUM_BUY,
            stop_distance=stop_distance,
            gross=gross,
            cash_budget=cash_budget,
        )

    order_id = _order_id(
        account.account_id,
        normalized_symbol,
        session,
        signal_bar_end,
        signal.strategy_version,
    )
    order = IntradayPaperOrder(
        order_id=order_id,
        account_id=account.account_id,
        symbol=normalized_symbol,
        board=resolved_board,
        session_date=session,
        signal_bar_end=signal_bar_end,
        signal_price=entry,
        invalidation_price=invalidation,
        limit_price=limit_price,
        quantity=quantity,
        created_at=created_at,
        expires_at=cutoff,
        previous_close=previous_close,
        instrument_type=instrument_type,
    )
    return IntradayPaperRiskOutcome(
        status=IntradayPaperRiskStatus.APPROVED,
        reason=IntradayPaperRiskReason.APPROVED,
        order=order,
        risk_budget=risk_budget,
        stop_distance=stop_distance,
        gross_exposure=gross,
        available_cash_budget=cash_budget,
    )


def match_intraday_buy_order(
    order: IntradayPaperOrder,
    bar: IntradayBar,
    *,
    match_revision: str,
    config: IntradayPaperRiskConfig | None = None,
    protective_stop_price: Decimal | None = None,
    take_profit_price: Decimal | None = None,
    time_exit_at: datetime | None = None,
) -> IntradayPaperMatchOutcome:
    """将一张订单与随后完成的一根一分钟行情柱匹配一次（IOC）。

    每个返回结果均为终态：部分成交会取消剩余数量，零成交绝不滚入下一行情柱。
    ``match_revision`` 必须是调用方为日内日志中保留的精确供应商版本提供的稳定标识；
    它参与确定性成交标识的生成。
    """

    resolved = config or IntradayPaperRiskConfig()
    revision = match_revision.strip()
    if not revision:
        raise ValueError("match_revision must not be empty")
    if len(revision) > 128:
        raise ValueError("match_revision must not exceed 128 characters")

    def no_fill(
        status: IntradayPaperMatchStatus,
        reason: IntradayPaperMatchReason,
        *,
        capacity: int = 0,
    ) -> IntradayPaperMatchOutcome:
        return IntradayPaperMatchOutcome(
            status=status,
            reason=reason,
            order_id=order.order_id,
            requested_quantity=order.quantity,
            filled_quantity=0,
            cancelled_quantity=order.quantity,
            volume_capacity=capacity,
        )

    if bar.symbol.strip().upper() != order.symbol:
        return no_fill(
            IntradayPaperMatchStatus.REJECTED,
            IntradayPaperMatchReason.SYMBOL_MISMATCH,
        )
    quantity_rule = intraday_order_quantity_rule(order.board)
    if not quantity_rule.accepts_buy_submission(order.quantity):
        return no_fill(
            IntradayPaperMatchStatus.REJECTED,
            IntradayPaperMatchReason.ORDER_QUANTITY_OUTSIDE_BOARD_RULES,
        )
    if not bar.is_closed:
        return no_fill(
            IntradayPaperMatchStatus.REJECTED,
            IntradayPaperMatchReason.BAR_NOT_CLOSED,
        )
    if (
        bar.interval is not MinuteInterval.ONE_MINUTE
        or bar.end_at - bar.start_at != timedelta(minutes=1)
    ):
        return no_fill(
            IntradayPaperMatchStatus.REJECTED,
            IntradayPaperMatchReason.BAR_NOT_ONE_MINUTE,
        )
    start_at = _aware_utc(bar.start_at, "bar.start_at")
    end_at = _aware_utc(bar.end_at, "bar.end_at")
    if protective_stop_price is not None:
        _positive(protective_stop_price, "protective_stop_price")
    if take_profit_price is not None:
        _positive(take_profit_price, "take_profit_price")
    if (protective_stop_price is None) != (take_profit_price is None):
        raise ValueError("protective stop and take profit must be supplied together")
    resolved_time_exit = (
        None if time_exit_at is None else _aware_utc(time_exit_at, "time_exit_at")
    )
    # 严格延后的开始时间可防止信号柱及与其收盘重叠的行情柱成为执行证据。
    if start_at <= order.signal_bar_end or end_at <= order.signal_bar_end:
        return no_fill(
            IntradayPaperMatchStatus.REJECTED,
            IntradayPaperMatchReason.BAR_NOT_STRICTLY_AFTER_SIGNAL,
        )
    if start_at < order.created_at:
        return no_fill(
            IntradayPaperMatchStatus.REJECTED,
            IntradayPaperMatchReason.BAR_STARTED_BEFORE_ORDER,
        )
    if (
        start_at.astimezone(_SHANGHAI).date() != order.session_date
        or end_at.astimezone(_SHANGHAI).date() != order.session_date
    ):
        return no_fill(
            IntradayPaperMatchStatus.REJECTED,
            IntradayPaperMatchReason.SESSION_MISMATCH,
        )
    if end_at > order.expires_at:
        return no_fill(
            IntradayPaperMatchStatus.NOT_FILLED_IOC,
            IntradayPaperMatchReason.ORDER_EXPIRED,
        )
    if bar.meta.fetched_at < bar.end_at:
        return no_fill(
            IntradayPaperMatchStatus.REJECTED,
            IntradayPaperMatchReason.BAR_NOT_YET_OBSERVABLE,
        )
    if not _valid_bar_ohlc(bar):
        return no_fill(
            IntradayPaperMatchStatus.REJECTED,
            IntradayPaperMatchReason.BAR_OHLC_INVALID,
        )
    # 成交分钟内的先后顺序不可知。若这根完成线已经触及预先登记的任一退出
    # 条件，就不能声称先买入、后退出；整根 IOC 按买入前失效处理。
    if protective_stop_price is not None:
        assert take_profit_price is not None
        if (
            bar.low <= protective_stop_price
            or bar.high >= take_profit_price
            or (resolved_time_exit is not None and end_at >= resolved_time_exit)
        ):
            return no_fill(
                IntradayPaperMatchStatus.NOT_FILLED_IOC,
                IntradayPaperMatchReason.EXIT_CONDITION_MET_BEFORE_FILL,
            )
    if bar.volume_lots <= 0:
        return no_fill(
            IntradayPaperMatchStatus.NOT_FILLED_IOC,
            IntradayPaperMatchReason.BAR_VOLUME_ZERO,
        )
    tick = _price_quantum(order.instrument_type, resolved)
    lower, upper = ashare_daily_price_band(order.previous_close, order.board, tick)
    if any(not lower <= price <= upper for price in (bar.open, bar.high, bar.low, bar.close)):
        return no_fill(
            IntradayPaperMatchStatus.REJECTED,
            IntradayPaperMatchReason.BAR_OUTSIDE_DAILY_BAND,
        )
    acceptance = intraday_buy_order_price_acceptance(order, config=resolved)
    if (
        bar.open == upper
        and bar.high == upper
        and bar.low == upper
        and bar.close == upper
    ):
        return no_fill(
            IntradayPaperMatchStatus.NOT_FILLED_IOC,
            IntradayPaperMatchReason.LOCKED_LIMIT_UP_QUEUE_UNMODELED,
        )
    # 分钟 OHLC 不揭示柱内成交顺序。一旦行情柱触及信号失效边界，就无法证明多头逻辑
    # 仍有效时存在合格流动性。应拒绝整根行情柱，不能把已经失效的形态解释为更便宜的入场。
    if bar.low <= order.invalidation_price:
        return no_fill(
            IntradayPaperMatchStatus.NOT_FILLED_IOC,
            IntradayPaperMatchReason.SIGNAL_INVALIDATED_BEFORE_FILL,
        )
    if bar.low > order.limit_price:
        return no_fill(
            IntradayPaperMatchStatus.NOT_FILLED_IOC,
            IntradayPaperMatchReason.LIMIT_NOT_TOUCHED,
        )

    capacity_shares = int(
        (Decimal(bar.volume_lots * 100) * resolved.volume_participation_rate)
        .to_integral_value(rounding=ROUND_FLOOR)
    )
    capacity = quantity_rule.floor_partial_fill_capacity(
        Decimal(capacity_shares)
    )
    if capacity <= 0:
        return no_fill(
            IntradayPaperMatchStatus.NOT_FILLED_IOC,
            IntradayPaperMatchReason.VOLUME_CAPACITY_BELOW_LOT,
        )
    filled = min(order.quantity, capacity)
    fill_price = _buy_fill_price(order, bar, resolved)
    if not acceptance.contains(fill_price):
        return no_fill(
            IntradayPaperMatchStatus.REJECTED,
            IntradayPaperMatchReason.FILL_OUTSIDE_ACCEPTABLE_RANGE,
            capacity=capacity,
        )
    fill_id = _fill_id(order, revision)
    fill = ASharePaperFill(
        account_id=order.account_id,
        fill_id=fill_id,
        symbol=order.symbol,
        side=Side.BUY,
        quantity=filled,
        price=fill_price,
        instrument_type=order.instrument_type,
        trading_date=order.session_date,
        executed_at=end_at,
        source=PaperFillSource.SIMULATED,
        external_order_id=order.order_id,
        note=(
            "single-bar IOC PAPER fill from a completed 1m bar; "
            f"revision={revision}; no order-book queue model"
        ),
    )
    partial = filled < order.quantity
    return IntradayPaperMatchOutcome(
        status=(
            IntradayPaperMatchStatus.PARTIALLY_FILLED_IOC
            if partial
            else IntradayPaperMatchStatus.FILLED
        ),
        reason=(
            IntradayPaperMatchReason.PARTIAL_VOLUME_CAPACITY
            if partial
            else IntradayPaperMatchReason.FILLED
        ),
        order_id=order.order_id,
        requested_quantity=order.quantity,
        filled_quantity=filled,
        cancelled_quantity=order.quantity - filled,
        volume_capacity=capacity,
        fill_price=fill_price,
        fill=fill,
    )


def _buy_fill_price(
    order: IntradayPaperOrder,
    bar: IntradayBar,
    config: IntradayPaperRiskConfig,
) -> Decimal:
    tick = _price_quantum(order.instrument_type, config)
    slippage = (
        config.stock_slippage_rate
        if order.instrument_type is PaperInstrumentType.STOCK
        else config.etf_slippage_rate
    )
    # 若市场以穿透限价的价格开盘后回到限价，柱内顺序不可知，因此保守成交价恰为限价。
    # 否则对开盘价施加不利滑点，并同时受成交最高价与限价约束。
    if bar.open > order.limit_price:
        return order.limit_price
    raw = bar.open * (_ONE + slippage)
    rounded = raw.quantize(tick, rounding=ROUND_CEILING)
    traded_ceiling = bar.high.quantize(tick, rounding=ROUND_FLOOR)
    limit_ceiling = order.limit_price.quantize(tick, rounding=ROUND_FLOOR)
    return min(rounded, traded_ceiling, limit_ceiling)


def _buy_cash_required(
    price: Decimal,
    quantity: int,
    instrument_type: PaperInstrumentType,
    schedule: PaperFeeSchedule,
) -> Decimal:
    value = price * quantity
    commission = max(value * schedule.commission_rate, schedule.minimum_commission_cny)
    transfer_rate = (
        schedule.stock_transfer_fee_rate
        if instrument_type is PaperInstrumentType.STOCK
        else schedule.etf_transfer_fee_rate
    )
    return value + commission + value * transfer_rate


def _valid_bar_ohlc(bar: IntradayBar) -> bool:
    prices = (bar.open, bar.high, bar.low, bar.close)
    if not all(_is_positive(value) for value in prices):
        return False
    return (
        bar.low <= bar.open <= bar.high
        and bar.low <= bar.close <= bar.high
        and bar.start_at < bar.end_at
    )


def _order_id(
    account_id: str,
    symbol: str,
    session: date,
    signal_bar_end: datetime,
    strategy_version: str,
) -> str:
    key = "|".join(
        (
            account_id,
            symbol,
            session.isoformat(),
            signal_bar_end.astimezone(UTC).isoformat(),
            strategy_version,
            "BUY",
        )
    )
    return "ip-order-" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:40]


def _fill_id(order: IntradayPaperOrder, match_revision: str) -> str:
    key = "|".join(
        (
            order.account_id,
            order.session_date.isoformat(),
            order.symbol,
            order.signal_bar_end.isoformat(),
            order.order_id,
            match_revision,
        )
    )
    return "ip-fill-" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:40]


__all__ = [
    "IntradayOrderQuantityRule",
    "IntradayPriceAcceptance",
    "IntradaySellQuantityPlan",
    "IntradaySellQuantityStatus",
    "ashare_daily_price_band",
    "build_intraday_buy_price_acceptance",
    "build_intraday_sell_price_acceptance",
    "build_intraday_sell_quantity_plan",
    "intraday_buy_order_price_acceptance",
    "intraday_order_quantity_rule",

    "IntradayPaperMatchOutcome",
    "IntradayPaperMatchReason",
    "IntradayPaperMatchStatus",
    "IntradayPaperOrder",
    "IntradayPaperRiskConfig",
    "IntradayPaperRiskOutcome",
    "IntradayPaperRiskReason",
    "IntradayPaperRiskStatus",
    "build_intraday_buy_order",
    "match_intraday_buy_order",
]
