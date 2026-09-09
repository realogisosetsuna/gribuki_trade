"""确定性的时点安全 A 股日线策略评估器。

本评估器将冻结的日线信号观测适配到 :mod:`strategy_lab.experiments` 使用的
精简 ``StrategyEvaluator`` 协议。它是研究模拟器而非订单路由器。信号输入
必须在信号时间戳之前已经可知；后一根已完成 K 线仅用于模拟既定订单并为
模拟持仓计价。
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal, localcontext
from itertools import pairwise
from typing import Any

from gribuki_trade.backtest.costs import InstrumentType
from gribuki_trade.strategy_lab.ashare_evaluator_models import (
    AShareDailyEvaluatorConfig as AShareDailyEvaluatorConfig,
)
from gribuki_trade.strategy_lab.ashare_evaluator_models import (
    AShareDailyStrategyObservation as AShareDailyStrategyObservation,
)
from gribuki_trade.strategy_lab.ashare_evaluator_models import (
    AShareEvaluationAction as AShareEvaluationAction,
)
from gribuki_trade.strategy_lab.ashare_evaluator_models import (
    AShareEvaluationEvent as AShareEvaluationEvent,
)
from gribuki_trade.strategy_lab.ashare_evaluator_models import (
    AShareEvaluationResult as AShareEvaluationResult,
)
from gribuki_trade.strategy_lab.ashare_evaluator_models import (
    AShareExecutionPolicy as AShareExecutionPolicy,
)
from gribuki_trade.strategy_lab.ashare_evaluator_models import (
    CompletedAShareDailyBar as CompletedAShareDailyBar,
)
from gribuki_trade.strategy_lab.ashare_evaluator_models import (
    PITStrategyScore as PITStrategyScore,
)
from gribuki_trade.strategy_lab.experiments import (
    CostScenario,
    DataManifest,
    PerformanceMetrics,
    StrategyManifest,
    StrategyWeights,
)

_BPS = Decimal("10000")
_MONEY = Decimal("0.01")
_SCORE_BOUND = Decimal("1")


class AShareDailyStrategyEvaluator:
    """实现协议的单标的、多头或现金日线评估器。

    每个评估器只保留一个冻结标的，可避免当前持仓标的缺席某日候选记录时
    凭空构造横截面计价。组合或横截面实验应先拼接多个完整标的面板，再
    增加独立的组合评估器。
    """

    def __init__(
        self,
        observations: tuple[AShareDailyStrategyObservation, ...],
        *,
        data_manifest: DataManifest,
        strategy_manifest: StrategyManifest,
        config: AShareDailyEvaluatorConfig | None = None,
    ) -> None:
        self._observations = observations
        self._data_manifest = data_manifest
        self._strategy_manifest = strategy_manifest
        self._config = config or AShareDailyEvaluatorConfig()
        self._validate_frozen_inputs()

    @property
    def data_manifest(self) -> DataManifest:
        return self._data_manifest

    @property
    def strategy_manifest(self) -> StrategyManifest:
        return self._strategy_manifest

    def evaluate(
        self,
        weights: StrategyWeights,
        observation_indices: tuple[int, ...],
        cost_scenario: CostScenario,
    ) -> PerformanceMetrics:
        return self.evaluate_with_trace(weights, observation_indices, cost_scenario).metrics

    def evaluate_with_trace(
        self,
        weights: StrategyWeights,
        observation_indices: tuple[int, ...],
        cost_scenario: CostScenario,
    ) -> AShareEvaluationResult:
        indices = _validated_indices(observation_indices, len(self._observations))
        instrument_type = self._observations[0].instrument_type
        if instrument_type is InstrumentType.ETF and cost_scenario.tax_bps != 0:
            raise ValueError("ETF cost scenarios must set tax_bps to zero explicitly")
        families = tuple(score.family_id for score in self._observations[0].technical_scores)
        weight_map = dict(weights.technical)
        if tuple(weight_map) != families:
            raise ValueError("weight families do not match the frozen observation families")

        cash = self._config.starting_cash_cny
        quantity = 0
        bought_session: date | None = None
        cost_basis_per_share = Decimal("0")
        cycle_realized = Decimal("0")
        completed_cycles = 0
        winning_cycles = 0
        trade_count = 0
        traded_notional = Decimal("0")
        equities: list[Decimal] = []
        events: list[AShareEvaluationEvent] = []
        contribution_sums = {family: Decimal("0") for family in (*families, "macro")}

        for index in indices:
            observation = self._observations[index]
            bar = observation.execution_bar
            fused = weights.macro * observation.macro_score.value
            contribution_sums["macro"] += weights.macro * observation.macro_score.value
            score_by_family = {
                score.family_id: score.value for score in observation.technical_scores
            }
            for family, weight in weights.technical:
                contribution = weight * score_by_family[family]
                contribution_sums[family] += contribution
                fused += contribution

            action = AShareEvaluationAction.HOLD if quantity else AShareEvaluationAction.STAY_CASH
            event_quantity = 0
            fill_price: Decimal | None = None

            if quantity and fused <= self._config.exit_threshold:
                if bought_session is not None and bar.session_date <= bought_session:
                    action = AShareEvaluationAction.NO_FILL_T_PLUS_ONE
                else:
                    action, sell_quantity, resolved_price = self._match(
                        observation,
                        side="SELL",
                        desired_quantity=quantity,
                        cost_scenario=cost_scenario,
                    )
                    if sell_quantity:
                        notional, fees = self._cash_components(
                            side="SELL",
                            price=resolved_price,
                            quantity=sell_quantity,
                            scenario=cost_scenario,
                        )
                        proceeds = notional - fees
                        cash += proceeds
                        quantity -= sell_quantity
                        cycle_realized += proceeds - cost_basis_per_share * sell_quantity
                        traded_notional += notional
                        trade_count += 1
                        event_quantity = sell_quantity
                        fill_price = resolved_price
                        if quantity == 0:
                            completed_cycles += 1
                            if cycle_realized > 0:
                                winning_cycles += 1
                            bought_session = None
                            cost_basis_per_share = Decimal("0")
                            cycle_realized = Decimal("0")

            if quantity == 0 and fused >= self._config.enter_threshold:
                action, buy_quantity, resolved_price = self._match(
                    observation,
                    side="BUY",
                    desired_quantity=None,
                    cost_scenario=cost_scenario,
                    available_cash=cash,
                )
                if buy_quantity:
                    notional, fees = self._cash_components(
                        side="BUY",
                        price=resolved_price,
                        quantity=buy_quantity,
                        scenario=cost_scenario,
                    )
                    cash -= notional + fees
                    quantity = buy_quantity
                    bought_session = bar.session_date
                    cost_basis_per_share = (notional + fees) / buy_quantity
                    cycle_realized = Decimal("0")
                    traded_notional += notional
                    trade_count += 1
                    event_quantity = buy_quantity
                    fill_price = resolved_price

            equity = cash + Decimal(quantity) * bar.close
            equities.append(equity)
            events.append(
                AShareEvaluationEvent(
                    observation_id=observation.observation_id,
                    session_date=bar.session_date,
                    action=action,
                    fused_score=fused,
                    quantity=event_quantity,
                    fill_price=fill_price,
                    cash_after=cash,
                )
            )

        divisor = Decimal(len(indices))
        contributions = tuple(
            (family, contribution_sums[family] / divisor)
            for family in sorted(contribution_sums)
        )
        metrics = _performance_metrics(
            starting_cash=self._config.starting_cash_cny,
            equities=tuple(equities),
            annual_sessions=self._config.annual_sessions,
            turnover=traded_notional / self._config.starting_cash_cny,
            trade_count=trade_count,
            completed_cycles=completed_cycles,
            winning_cycles=winning_cycles,
            family_contributions=contributions,
        )
        return AShareEvaluationResult(
            metrics=metrics,
            events=tuple(events),
            data_manifest_sha256=self._data_manifest.manifest_sha256,
            strategy_manifest_sha256=self._strategy_manifest.manifest_sha256,
        )

    def _validate_frozen_inputs(self) -> None:
        if not self._observations:
            raise ValueError("observations must not be empty")
        observation_ids = tuple(item.observation_id for item in self._observations)
        if len(set(observation_ids)) != len(observation_ids):
            raise ValueError("observation_id values must be unique")
        signal_times = tuple(item.signal_as_of for item in self._observations)
        sessions = tuple(item.execution_bar.session_date for item in self._observations)
        if signal_times != tuple(sorted(signal_times)) or len(set(signal_times)) != len(
            signal_times
        ):
            raise ValueError("signal timestamps must be strictly increasing")
        if sessions != tuple(sorted(sessions)) or len(set(sessions)) != len(sessions):
            raise ValueError("execution sessions must be strictly increasing")
        symbols = {item.symbol for item in self._observations}
        instruments = {item.instrument_type for item in self._observations}
        if len(symbols) != 1 or len(instruments) != 1:
            raise ValueError("one evaluator dataset must contain exactly one instrument")
        families = tuple(score.family_id for score in self._observations[0].technical_scores)
        if any(
            tuple(score.family_id for score in item.technical_scores) != families
            for item in self._observations
        ):
            raise ValueError("technical score families must be identical for every observation")

        if self._data_manifest.observation_count != len(self._observations):
            raise ValueError("data manifest observation_count mismatch")
        if self._data_manifest.observed_start != self._observations[0].signal_date:
            raise ValueError("data manifest observed_start mismatch")
        if self._data_manifest.observed_end != self._observations[-1].signal_date:
            raise ValueError("data manifest observed_end mismatch")
        if self._data_manifest.label_id != self._config.label_id:
            raise ValueError("data manifest label_id mismatch")
        expected_features = tuple(sorted((*families, "macro")))
        if self._data_manifest.feature_ids != expected_features:
            raise ValueError("data manifest feature_ids mismatch")
        canonical = canonical_ashare_evaluation_content(self._observations)
        if hashlib.sha256(canonical).hexdigest() != self._data_manifest.content_sha256:
            raise ValueError("data manifest content hash mismatch")
        if self._data_manifest.frozen_at < max(
            item.execution_bar.completed_at for item in self._observations
        ):
            raise ValueError("data manifest was frozen before the final bar completed")
        expected_revisions = ashare_evaluation_source_revisions(self._observations)
        if self._data_manifest.source_revisions != expected_revisions:
            raise ValueError("data manifest source_revisions mismatch")

        if self._strategy_manifest.strategy_version != self._config.strategy_version:
            raise ValueError("strategy manifest version mismatch")
        if self._strategy_manifest.parameters != self._config.manifest_parameters:
            raise ValueError("strategy manifest parameters mismatch")
        if tuple(name for name, _ in self._strategy_manifest.factor_expressions) != families:
            raise ValueError("strategy manifest factor families mismatch")

    def _match(
        self,
        observation: AShareDailyStrategyObservation,
        *,
        side: str,
        desired_quantity: int | None,
        cost_scenario: CostScenario,
        available_cash: Decimal | None = None,
    ) -> tuple[AShareEvaluationAction, int, Decimal]:
        bar = observation.execution_bar
        if bar.suspended:
            return AShareEvaluationAction.NO_FILL_SUSPENDED, 0, bar.open
        if bar.lower_price_limit is None or bar.upper_price_limit is None:
            return AShareEvaluationAction.NO_FILL_PRICE_LIMIT_UNKNOWN, 0, bar.open
        if side == "BUY" and bar.open >= bar.upper_price_limit:
            return AShareEvaluationAction.NO_FILL_LIMIT_LOCKED, 0, bar.open
        if side == "SELL" and bar.open <= bar.lower_price_limit:
            return AShareEvaluationAction.NO_FILL_LIMIT_LOCKED, 0, bar.open
        if self._config.execution_policy is AShareExecutionPolicy.CONSERVATIVE_OPEN_LIMIT:
            limit = (
                observation.buy_limit_price if side == "BUY" else observation.sell_limit_price
            )
            if limit is None:
                return AShareEvaluationAction.NO_FILL_ORDER_LIMIT, 0, bar.open
            if (side == "BUY" and bar.open > limit) or (side == "SELL" and bar.open < limit):
                return AShareEvaluationAction.NO_FILL_ORDER_LIMIT, 0, bar.open

        slip = cost_scenario.slippage_bps / _BPS
        price = bar.open * (Decimal("1") + slip if side == "BUY" else Decimal("1") - slip)
        if price > bar.upper_price_limit or price < bar.lower_price_limit:
            return AShareEvaluationAction.NO_FILL_LIMIT_LOCKED, 0, price
        volume_cap = int(
            (Decimal(bar.volume_shares) * self._config.max_volume_participation).to_integral_value(
                rounding=ROUND_FLOOR
            )
        )
        if side == "SELL":
            quantity = min(desired_quantity or 0, volume_cap)
            if quantity <= 0:
                return AShareEvaluationAction.NO_FILL_VOLUME, 0, price
            return AShareEvaluationAction.SELL, quantity, price

        volume_lots = volume_cap // 100
        if volume_lots <= 0:
            return AShareEvaluationAction.NO_FILL_VOLUME, 0, price
        assert available_cash is not None
        affordable_lots = int((available_cash / (price * 100)).to_integral_value(ROUND_FLOOR))
        quantity = min(volume_lots, affordable_lots) * 100
        while quantity > 0:
            notional, fees = self._cash_components(
                side="BUY",
                price=price,
                quantity=quantity,
                scenario=cost_scenario,
            )
            if notional + fees <= available_cash:
                return AShareEvaluationAction.BUY, quantity, price
            quantity -= 100
        return AShareEvaluationAction.NO_FILL_CASH, 0, price

    def _cash_components(
        self,
        *,
        side: str,
        price: Decimal,
        quantity: int,
        scenario: CostScenario,
    ) -> tuple[Decimal, Decimal]:
        raw_notional = price * quantity
        notional = _money(raw_notional)
        commission = _money(
            max(raw_notional * scenario.commission_bps / _BPS, scenario.minimum_commission_cny)
        )
        transfer_fee = _money(raw_notional * self._config.transfer_fee_bps / _BPS)
        tax = _money(raw_notional * scenario.tax_bps / _BPS) if side == "SELL" else Decimal("0")
        return notional, commission + transfer_fee + tax


def canonical_ashare_evaluation_content(
    observations: tuple[AShareDailyStrategyObservation, ...],
) -> bytes:
    """传给 ``DataManifest.freeze`` 的规范字节序列。"""

    document = [_observation_document(item) for item in observations]
    return json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def ashare_evaluation_source_revisions(
    observations: tuple[AShareDailyStrategyObservation, ...],
) -> tuple[tuple[str, str], ...]:
    """为 ``DataManifest`` 构建逐观测唯一的血缘条目。

    数据提供方可以合理地为每个交易日发布不同修订版。``DataManifest``
    要求键唯一，因此不能直接用提供方标识作为键。位置组件键在规范冻结
    观测序列中保持稳定，值则保留确切的提供方与修订版本。
    """

    entries: list[tuple[str, str]] = []
    for observation_index, observation in enumerate(observations):
        prefix = f"observation/{observation_index:08d}"
        for score_index, score in enumerate(observation.technical_scores):
            entries.append(
                (
                    f"{prefix}/technical/{score_index:04d}",
                    _lineage_value(score.source_id, score.source_revision),
                )
            )
        entries.append(
            (
                f"{prefix}/macro",
                _lineage_value(
                    observation.macro_score.source_id,
                    observation.macro_score.source_revision,
                ),
            )
        )
        entries.append(
            (
                f"{prefix}/execution-bar",
                _lineage_value(
                    observation.execution_bar.source_id,
                    observation.execution_bar.source_revision,
                ),
            )
        )
    return tuple(sorted(entries))


def _performance_metrics(
    *,
    starting_cash: Decimal,
    equities: tuple[Decimal, ...],
    annual_sessions: int,
    turnover: Decimal,
    trade_count: int,
    completed_cycles: int,
    winning_cycles: int,
    family_contributions: tuple[tuple[str, Decimal], ...],
) -> PerformanceMetrics:
    net_return = equities[-1] / starting_cash - Decimal("1")
    previous = starting_cash
    daily_returns: list[Decimal] = []
    peak = starting_cash
    max_drawdown = Decimal("0")
    for equity in equities:
        daily_returns.append(equity / previous - Decimal("1"))
        previous = equity
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, Decimal("1") - equity / peak)
    with localcontext() as context:
        context.prec = 34
        periods = Decimal(len(equities))
        if equities[-1] > 0:
            annualized_return = (
                (equities[-1] / starting_cash).ln()
                * (Decimal(annual_sessions) / periods)
            ).exp() - Decimal("1")
        else:
            annualized_return = None
        mean = sum(daily_returns, Decimal("0")) / periods
        variance = sum((value - mean) ** 2 for value in daily_returns) / periods
        if variance > 0:
            daily_volatility = variance.sqrt()
            annualized_volatility = daily_volatility * Decimal(annual_sessions).sqrt()
            sharpe = mean / daily_volatility * Decimal(annual_sessions).sqrt()
        else:
            annualized_volatility = Decimal("0")
            sharpe = None
    hit_rate = (
        Decimal(winning_cycles) / Decimal(completed_cycles) if completed_cycles else None
    )
    return PerformanceMetrics(
        net_return=net_return,
        annualized_return=annualized_return,
        annualized_volatility=annualized_volatility,
        sharpe=sharpe,
        max_drawdown=max_drawdown,
        turnover=turnover,
        trade_count=trade_count,
        hit_rate=hit_rate,
        family_contributions=family_contributions,
    )


def _observation_document(item: AShareDailyStrategyObservation) -> dict[str, Any]:
    bar = item.execution_bar
    return {
        "buy_limit_price": _decimal_text(item.buy_limit_price),
        "execution_bar": {
            "close": str(bar.close),
            "completed_at": bar.completed_at.isoformat(),
            "high": str(bar.high),
            "low": str(bar.low),
            "lower_price_limit": _decimal_text(bar.lower_price_limit),
            "open": str(bar.open),
            "session_date": bar.session_date.isoformat(),
            "source_id": bar.source_id,
            "source_revision": bar.source_revision,
            "suspended": bar.suspended,
            "upper_price_limit": _decimal_text(bar.upper_price_limit),
            "volume_shares": bar.volume_shares,
        },
        "instrument_type": item.instrument_type.value,
        "macro_score": _score_document(item.macro_score),
        "observation_id": item.observation_id,
        "sell_limit_price": _decimal_text(item.sell_limit_price),
        "signal_as_of": item.signal_as_of.isoformat(),
        "symbol": item.symbol,
        "technical_scores": [_score_document(score) for score in item.technical_scores],
    }


def _score_document(score: PITStrategyScore) -> dict[str, str]:
    return {
        "family_id": score.family_id,
        "known_at": score.known_at.isoformat(),
        "source_id": score.source_id,
        "source_revision": score.source_revision,
        "value": str(score.value),
    }


def _lineage_value(source_id: str, source_revision: str) -> str:
    return json.dumps(
        {"source_id": source_id, "source_revision": source_revision},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _validated_indices(indices: tuple[int, ...], length: int) -> tuple[int, ...]:
    if not indices:
        raise ValueError("observation_indices must not be empty")
    if any(isinstance(index, bool) or not isinstance(index, int) for index in indices):
        raise TypeError("observation indices must be integers")
    if indices != tuple(sorted(indices)) or len(set(indices)) != len(indices):
        raise ValueError("observation indices must be strictly increasing and unique")
    if indices[0] < 0 or indices[-1] >= length:
        raise IndexError("observation index is outside the frozen dataset")
    if any(current != previous + 1 for previous, current in pairwise(indices)):
        raise ValueError("observation indices must form one contiguous evaluation slice")
    return indices


def _money(value: Decimal) -> Decimal:
    return value.quantize(_MONEY, rounding=ROUND_HALF_UP)


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    normalized = value.strip()
    if not normalized or len(normalized) > 128:
        raise ValueError(f"{name} must contain 1-128 characters")
    return normalized


def _finite_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def _positive_decimal(value: object, name: str) -> Decimal:
    resolved = _finite_decimal(value, name)
    if resolved <= 0:
        raise ValueError(f"{name} must be positive")
    return resolved


def _aware_utc(value: object, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)
