"""对已发布研究建议进行时点一致的结果评估。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum
from statistics import fmean

from gribuki_trade.backtest.costs import (
    InstrumentType,
    TradingCostConfig,
    calculate_trade_cost,
)
from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.recommendations import (
    RecommendationDecision,
    ResearchRecommendation,
)


class OutcomeStatus(StrEnum):
    COMPLETE = "COMPLETE"
    PENDING = "PENDING"
    UNEVALUABLE = "UNEVALUABLE"


@dataclass(frozen=True, slots=True)
class RecommendationEvaluationConfig:
    horizon_sessions: int = 5
    target_notional_cny: Decimal = Decimal("100000")
    lot_size: int = 100
    instrument_type: InstrumentType = InstrumentType.STOCK
    costs: TradingCostConfig = TradingCostConfig()

    def __post_init__(self) -> None:
        if self.horizon_sessions < 1:
            raise ValueError("horizon_sessions must be positive")
        if not self.target_notional_cny.is_finite() or self.target_notional_cny <= 0:
            raise ValueError("target_notional_cny must be finite and positive")
        if self.lot_size < 1:
            raise ValueError("lot_size must be positive")


@dataclass(frozen=True, slots=True)
class RecommendationOutcome:
    recommendation_id: str
    symbol: str
    decision: RecommendationDecision
    status: OutcomeStatus
    horizon_sessions: int
    entry_date: date | None = None
    exit_date: date | None = None
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    gross_return: Decimal | None = None
    net_return_after_costs: Decimal | None = None
    max_favorable_excursion: Decimal | None = None
    max_adverse_excursion: Decimal | None = None
    direction_correct: bool | None = None
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class OutcomeSummary:
    total: int
    completed: int
    pending: int
    unevaluable: int
    directional_count: int
    directional_hit_rate: Decimal | None
    average_gross_return: Decimal | None
    average_net_return_after_costs: Decimal | None


def evaluate_recommendation(
    recommendation: ResearchRecommendation,
    future_bars: tuple[DailyBar, ...],
    *,
    config: RecommendationEvaluationConfig | None = None,
) -> RecommendationOutcome:
    """只评估严格晚于原始建议时间的交易时段。

    入场候选按下一可交易时段的开盘价计价，并在配置观察期末的收盘价退出。
    这避免了常见的同收盘价前视错误。停牌或缺价记录不会被向前填充。
    """

    resolved = config or RecommendationEvaluationConfig()
    if any(bar.symbol != recommendation.symbol for bar in future_bars):
        raise ValueError("future bars must match recommendation symbol")
    dates = [bar.trade_date for bar in future_bars]
    if dates != sorted(dates) or len(dates) != len(set(dates)):
        raise ValueError("future bars must be strictly ordered and unique")
    if any(bar.trade_date <= recommendation.as_of.date() for bar in future_bars):
        raise ValueError("future bars must be strictly after recommendation as_of")
    if any(bar.adjustment is not PriceAdjustment.NONE for bar in future_bars):
        raise ValueError("outcome evaluation requires unadjusted tradable prices")
    if recommendation.decision in {
        RecommendationDecision.WATCH,
        RecommendationDecision.ABSTAIN,
    }:
        return _base_outcome(
            recommendation,
            resolved,
            OutcomeStatus.UNEVALUABLE,
            "NON_DIRECTIONAL_DECISION",
        )

    tradable = tuple(
        bar
        for bar in future_bars
        if bar.is_trading
        and bar.open is not None
        and bar.high is not None
        and bar.low is not None
        and bar.close is not None
    )
    if not tradable:
        return _base_outcome(
            recommendation,
            resolved,
            OutcomeStatus.PENDING,
            "NO_FUTURE_TRADABLE_SESSION",
        )
    if len(tradable) < resolved.horizon_sessions:
        return _base_outcome(
            recommendation,
            resolved,
            OutcomeStatus.PENDING,
            "HORIZON_NOT_COMPLETE",
        )

    window = tradable[: resolved.horizon_sessions]
    entry = window[0]
    exit_bar = window[-1]
    assert entry.open is not None
    assert exit_bar.close is not None
    highs = tuple(bar.high for bar in window if bar.high is not None)
    lows = tuple(bar.low for bar in window if bar.low is not None)
    gross_return = exit_bar.close / entry.open - Decimal("1")
    favorable = max(highs) / entry.open - Decimal("1")
    adverse = min(lows) / entry.open - Decimal("1")

    if recommendation.decision is RecommendationDecision.REDUCE:
        net_return = None
        direction_correct = gross_return < 0
    else:
        quantity = int(
            resolved.target_notional_cny / entry.open / resolved.lot_size
        ) * resolved.lot_size
        if quantity < resolved.lot_size:
            return _base_outcome(
                recommendation,
                resolved,
                OutcomeStatus.UNEVALUABLE,
                "NOTIONAL_BELOW_ONE_LOT",
            )
        buy = calculate_trade_cost(
            side=Side.BUY,
            price=entry.open,
            quantity=quantity,
            instrument_type=resolved.instrument_type,
            config=resolved.costs,
        )
        sell = calculate_trade_cost(
            side=Side.SELL,
            price=exit_bar.close,
            quantity=quantity,
            instrument_type=resolved.instrument_type,
            config=resolved.costs,
        )
        committed_cash = -buy.cash_change
        net_return = (buy.cash_change + sell.cash_change) / committed_cash
        direction_correct = net_return > 0

    return RecommendationOutcome(
        recommendation_id=recommendation.recommendation_id,
        symbol=recommendation.symbol,
        decision=recommendation.decision,
        status=OutcomeStatus.COMPLETE,
        horizon_sessions=resolved.horizon_sessions,
        entry_date=entry.trade_date,
        exit_date=exit_bar.trade_date,
        entry_price=entry.open,
        exit_price=exit_bar.close,
        gross_return=gross_return,
        net_return_after_costs=net_return,
        max_favorable_excursion=favorable,
        max_adverse_excursion=adverse,
        direction_correct=direction_correct,
    )


def summarize_outcomes(
    outcomes: tuple[RecommendationOutcome, ...],
) -> OutcomeSummary:
    completed = tuple(item for item in outcomes if item.status is OutcomeStatus.COMPLETE)
    directional = tuple(
        item for item in completed if item.direction_correct is not None
    )
    gross = tuple(float(item.gross_return) for item in completed if item.gross_return is not None)
    net = tuple(
        float(item.net_return_after_costs)
        for item in completed
        if item.net_return_after_costs is not None
    )
    return OutcomeSummary(
        total=len(outcomes),
        completed=len(completed),
        pending=sum(item.status is OutcomeStatus.PENDING for item in outcomes),
        unevaluable=sum(
            item.status is OutcomeStatus.UNEVALUABLE for item in outcomes
        ),
        directional_count=len(directional),
        directional_hit_rate=(
            None
            if not directional
            else Decimal(
                sum(item.direction_correct is True for item in directional)
            )
            / Decimal(len(directional))
        ),
        average_gross_return=None if not gross else Decimal(str(fmean(gross))),
        average_net_return_after_costs=(
            None if not net else Decimal(str(fmean(net)))
        ),
    )


def _base_outcome(
    recommendation: ResearchRecommendation,
    config: RecommendationEvaluationConfig,
    status: OutcomeStatus,
    reason: str,
) -> RecommendationOutcome:
    return RecommendationOutcome(
        recommendation_id=recommendation.recommendation_id,
        symbol=recommendation.symbol,
        decision=recommendation.decision,
        status=status,
        horizon_sessions=config.horizon_sessions,
        reason_code=reason,
    )
