"""退出策略的纯日线重放、费用结算与评估指标。

本模块只接收已冻结的退出样本和参数，不负责数据集构建、滚动切分、持久化或
生产配置。把执行语义与指标投影集中在这里，便于独立验证保守止损、停牌重试、
费用滑点和回撤计算；``exit_evaluator`` 通过兼容包装器继续提供历史私有名称。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from decimal import Decimal

from gribuki_trade.strategy_lab.exit_evaluator import (
    ExitEvaluationBar,
    ExitPolicyBarrier,
    ExitPolicyCostModel,
    ExitPolicyEpisode,
    ExitPolicyEvaluation,
    ExitPolicyFoldResult,
    ExitPolicyInvalidPlanError,
    ExitPolicyMetrics,
    ExitPolicyObjective,
    ExitPolicyOutcome,
    ExitPolicyOutcomeStatus,
)
from gribuki_trade.strategy_lab.exit_policies import ExitPolicyParameters

_BPS = Decimal("10000")
_ONE = Decimal("1")


def evaluate_episode(
    episode: ExitPolicyEpisode,
    parameters: ExitPolicyParameters,
    costs: ExitPolicyCostModel,
    label_horizon_sessions: int,
) -> ExitPolicyOutcome:
    atr_stop = episode.entry_price - parameters.atr_stop_multiple * episode.atr_at_entry
    structure_stop = (
        episode.structure_low_at_entry
        - parameters.structure_buffer_atr * episode.atr_at_entry
    )
    initial_stop = min(atr_stop, structure_stop)
    if initial_stop <= 0 or initial_stop >= episode.entry_price:
        raise ExitPolicyInvalidPlanError("candidate creates a non-positive or invalid stop")
    risk = episode.entry_price - initial_stop
    take_profit = episode.entry_price + parameters.reward_to_risk * risk
    current_stop = initial_stop
    pending_barrier: ExitPolicyBarrier | None = None
    pending_observed_on: date | None = None
    pending_both = False
    blocked_sessions = 0
    warnings: list[str] = []
    bars = episode.future_bars[:label_horizon_sessions]

    for held, bar in enumerate(bars, start=1):
        barrier = pending_barrier
        barrier_observed_on = pending_observed_on
        both_touched = pending_both
        raw_price: Decimal | None = None
        if barrier is not None:
            raw_price = bar.open
        else:
            stop_touched = bar.low <= current_stop
            target_touched = bar.high >= take_profit
            both_touched = stop_touched and target_touched
            if stop_touched:
                barrier = (
                    ExitPolicyBarrier.TRAILING_STOP
                    if current_stop > initial_stop
                    else ExitPolicyBarrier.STOP_LOSS
                )
                raw_price = min(bar.open, current_stop)
                barrier_observed_on = bar.session_date
                if both_touched:
                    warnings.append("OHLC_ORDER_UNKNOWN_STOP_FIRST")
            elif target_touched:
                barrier = ExitPolicyBarrier.TAKE_PROFIT
                raw_price = max(bar.open, take_profit)
                barrier_observed_on = bar.session_date
            elif held >= parameters.maximum_holding_sessions:
                barrier = ExitPolicyBarrier.TIME
                raw_price = bar.close
                barrier_observed_on = bar.session_date
                warnings.append("DAILY_TIME_EXIT_AT_CLOSE")

        if barrier is not None and raw_price is not None:
            if barrier_observed_on is None:
                raise AssertionError("exit barrier is missing its observation date")
            if bar.suspended:
                pending_barrier = barrier
                pending_observed_on = barrier_observed_on
                pending_both = both_touched
                blocked_sessions += 1
                warnings.append("EXIT_BLOCKED_SUSPENDED")
            elif bar.lower_limit_locked:
                pending_barrier = barrier
                pending_observed_on = barrier_observed_on
                pending_both = both_touched
                blocked_sessions += 1
                warnings.append("EXIT_BLOCKED_LOWER_LIMIT_LOCKED")
            else:
                return filled_outcome(
                    episode,
                    parameters,
                    costs,
                    initial_stop=initial_stop,
                    final_stop=current_stop,
                    take_profit=take_profit,
                    barrier=barrier,
                    barrier_date=barrier_observed_on,
                    raw_price=raw_price,
                    bar=bar,
                    holding_sessions=held,
                    both_touched=both_touched,
                    blocked_sessions=blocked_sessions,
                    warnings=warnings,
                )

        if pending_barrier is None and parameters.trailing_atr_multiple is not None:
            trailing_candidate = (
                bar.high - parameters.trailing_atr_multiple * episode.atr_at_entry
            )
            current_stop = max(current_stop, min(trailing_candidate, take_profit))

    terminal_bar = bars[-1]
    terminal_price = net_sell_price(terminal_bar.close, terminal_bar, costs)
    pnl, trade_return = net_trade_result(episode, terminal_price, costs)
    terminal_warnings = tuple(
        sorted(
            {
                *warnings,
                "TERMINAL_POSITION_MARKED_NOT_FILLED",
                *("EXIT_PENDING_AT_TERMINAL" for _ in (0,) if pending_barrier is not None),
            }
        )
    )
    return ExitPolicyOutcome(
        episode_id=episode.episode_id,
        entry_session_date=episode.entry_session_date,
        parameter_fingerprint=parameters.fingerprint,
        initial_stop_price=initial_stop,
        final_stop_price=current_stop,
        take_profit_price=take_profit,
        barrier=pending_barrier or ExitPolicyBarrier.TERMINAL_MARK,
        barrier_observed_on=pending_observed_on or terminal_bar.session_date,
        status=ExitPolicyOutcomeStatus.MARKED_OPEN,
        execution_on=None,
        raw_execution_price=None,
        net_execution_price=terminal_price,
        holding_sessions=len(bars),
        both_price_barriers_touched=pending_both,
        blocked_sessions=blocked_sessions,
        net_pnl_cny=pnl,
        net_return=trade_return,
        warning_codes=terminal_warnings,
    )


def filled_outcome(
    episode: ExitPolicyEpisode,
    parameters: ExitPolicyParameters,
    costs: ExitPolicyCostModel,
    *,
    initial_stop: Decimal,
    final_stop: Decimal,
    take_profit: Decimal,
    barrier: ExitPolicyBarrier,
    barrier_date: date,
    raw_price: Decimal,
    bar: ExitEvaluationBar,
    holding_sessions: int,
    both_touched: bool,
    blocked_sessions: int,
    warnings: Sequence[str],
) -> ExitPolicyOutcome:
    net_price = net_sell_price(raw_price, bar, costs)
    pnl, trade_return = net_trade_result(episode, net_price, costs)
    return ExitPolicyOutcome(
        episode_id=episode.episode_id,
        entry_session_date=episode.entry_session_date,
        parameter_fingerprint=parameters.fingerprint,
        initial_stop_price=initial_stop,
        final_stop_price=final_stop,
        take_profit_price=take_profit,
        barrier=barrier,
        barrier_observed_on=barrier_date,
        status=ExitPolicyOutcomeStatus.FILLED,
        execution_on=bar.session_date,
        raw_execution_price=raw_price,
        net_execution_price=net_price,
        holding_sessions=holding_sessions,
        both_price_barriers_touched=both_touched,
        blocked_sessions=blocked_sessions,
        net_pnl_cny=pnl,
        net_return=trade_return,
        warning_codes=tuple(sorted(set(warnings))),
    )


def net_sell_price(
    raw_price: Decimal,
    bar: ExitEvaluationBar,
    costs: ExitPolicyCostModel,
) -> Decimal:
    slipped = raw_price * (_ONE - costs.sell_slippage_bps / _BPS)
    return max(bar.lower_price_limit, min(slipped, bar.upper_price_limit))


def net_trade_result(
    episode: ExitPolicyEpisode,
    net_sell_price: Decimal,
    costs: ExitPolicyCostModel,
) -> tuple[Decimal, Decimal]:
    entry_notional = episode.entry_price * episode.quantity
    exit_notional = net_sell_price * episode.quantity
    buy_variable_bps = costs.commission_bps + costs.transfer_fee_bps
    buy_fee = max(
        costs.minimum_commission_cny,
        entry_notional * buy_variable_bps / _BPS,
    )
    sell_fee = max(
        costs.minimum_commission_cny,
        exit_notional * (costs.commission_bps + costs.transfer_fee_bps) / _BPS,
    ) + exit_notional * costs.tax_bps / _BPS
    capital = entry_notional + buy_fee
    pnl = exit_notional - sell_fee - capital
    return pnl, pnl / capital


def metrics(outcomes: tuple[ExitPolicyOutcome, ...]) -> ExitPolicyMetrics:
    if not outcomes:
        raise ValueError("outcomes must not be empty")
    equity = _ONE
    peak = _ONE
    maximum_drawdown = Decimal("0")
    # 同一入场日采用亏损优先的保守顺序，避免样本编号偶然改变逐笔回撤。
    return_path = tuple(
        sorted(
            outcomes,
            key=lambda outcome: (
                outcome.entry_session_date,
                outcome.net_return,
                outcome.episode_id,
            ),
        )
    )
    for outcome in return_path:
        equity *= _ONE + outcome.net_return
        peak = max(peak, equity)
        if peak > 0:
            maximum_drawdown = max(maximum_drawdown, (peak - equity) / peak)
    completed = tuple(
        outcome for outcome in outcomes if outcome.status is ExitPolicyOutcomeStatus.FILLED
    )
    wins = tuple(outcome for outcome in completed if outcome.net_pnl_cny > 0)
    losses = tuple(outcome for outcome in completed if outcome.net_pnl_cny < 0)
    gross_profit = sum((outcome.net_pnl_cny for outcome in wins), Decimal("0"))
    gross_loss = -sum((outcome.net_pnl_cny for outcome in losses), Decimal("0"))
    hit_rate = (
        Decimal(len(wins)) / Decimal(len(completed)) if completed else Decimal("0")
    )
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else None
    return ExitPolicyMetrics(
        episode_count=len(outcomes),
        completed_count=len(completed),
        marked_open_count=len(outcomes) - len(completed),
        winning_count=len(wins),
        losing_count=len(losses),
        net_return=equity - _ONE,
        maximum_drawdown=maximum_drawdown,
        average_return=(
            sum((outcome.net_return for outcome in outcomes), Decimal("0"))
            / Decimal(len(outcomes))
        ),
        hit_rate=hit_rate,
        profit_factor=profit_factor,
        gross_profit_cny=gross_profit,
        gross_loss_cny=gross_loss,
        blocked_session_count=sum(outcome.blocked_sessions for outcome in outcomes),
    )


def combine_evaluations(
    parameters: ExitPolicyParameters,
    fold_results: tuple[ExitPolicyFoldResult, ...],
) -> ExitPolicyEvaluation:
    outcomes = tuple(
        outcome for fold in fold_results for outcome in fold.evaluation.outcomes
    )
    indices = tuple(
        index
        for fold in fold_results
        for index in fold.evaluation.observation_indices
    )
    if len(indices) != len(set(indices)):
        raise ValueError("walk-forward validation episodes must not overlap")
    return ExitPolicyEvaluation(
        parameters=parameters,
        observation_indices=indices,
        metrics=metrics(outcomes),
        outcomes=outcomes,
    )


def objective_score(
    metrics: ExitPolicyMetrics,
    objective: ExitPolicyObjective,
) -> Decimal:
    if objective is ExitPolicyObjective.NET_RETURN:
        return metrics.net_return
    if objective is ExitPolicyObjective.HIT_RATE:
        return metrics.hit_rate
    if metrics.maximum_drawdown == 0:
        return metrics.net_return
    return metrics.net_return / metrics.maximum_drawdown




__all__ = [
    "combine_evaluations",
    "evaluate_episode",
    "filled_outcome",
    "metrics",
    "net_sell_price",
    "net_trade_result",
    "objective_score",
]
