"""确定性的时点安全 A 股日线策略评估器。

本评估器将冻结的日线信号观测适配到 :mod:`strategy_lab.experiments` 使用的
精简 ``StrategyEvaluator`` 协议。它是研究模拟器而非订单路由器。信号输入
必须在信号时间戳之前已经可知；后一根已完成 K 线仅用于模拟既定订单并为
模拟持仓计价。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal, localcontext
from enum import StrEnum
from itertools import pairwise
from typing import Any
from zoneinfo import ZoneInfo

from gribuki_trade.backtest.costs import InstrumentType
from gribuki_trade.strategy_lab.experiments import (
    CostScenario,
    DataManifest,
    PerformanceMetrics,
    StrategyManifest,
    StrategyWeights,
)

_MARKET_TZ = ZoneInfo("Asia/Shanghai")
_BPS = Decimal("10000")
_MONEY = Decimal("0.01")
_SCORE_BOUND = Decimal("1")


class AShareExecutionPolicy(StrEnum):
    """支持的日线撮合策略。"""

    NEXT_OPEN = "NEXT_OPEN"
    CONSERVATIVE_OPEN_LIMIT = "CONSERVATIVE_OPEN_LIMIT"


class AShareEvaluationAction(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    STAY_CASH = "STAY_CASH"
    NO_FILL_SUSPENDED = "NO_FILL_SUSPENDED"
    NO_FILL_PRICE_LIMIT_UNKNOWN = "NO_FILL_PRICE_LIMIT_UNKNOWN"
    NO_FILL_LIMIT_LOCKED = "NO_FILL_LIMIT_LOCKED"
    NO_FILL_ORDER_LIMIT = "NO_FILL_ORDER_LIMIT"
    NO_FILL_VOLUME = "NO_FILL_VOLUME"
    NO_FILL_CASH = "NO_FILL_CASH"
    NO_FILL_T_PLUS_ONE = "NO_FILL_T_PLUS_ONE"


@dataclass(frozen=True, slots=True)
class PITStrategyScore:
    """一个分数，以及证明其何时可知的血缘信息。"""

    family_id: str
    value: Decimal
    known_at: datetime
    source_id: str
    source_revision: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "family_id", _identifier(self.family_id, "family_id"))
        object.__setattr__(self, "value", _finite_decimal(self.value, "value"))
        if not -_SCORE_BOUND <= self.value <= _SCORE_BOUND:
            raise ValueError("score value must be in [-1, 1]")
        object.__setattr__(self, "known_at", _aware_utc(self.known_at, "known_at"))
        object.__setattr__(self, "source_id", _identifier(self.source_id, "source_id"))
        object.__setattr__(
            self,
            "source_revision",
            _identifier(self.source_revision, "source_revision"),
        )


@dataclass(frozen=True, slots=True)
class CompletedAShareDailyBar:
    """带明确执行门槛的下一交易日已完成、不复权 K 线。

    涨跌停价格为可选项，因为冻结数据源中某些标的或交易日确实没有可用
    区间。任一边界缺失时，执行按闭锁原则失败；评估器绝不推测板块或
    ST 标的特有的百分比。
    """

    session_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume_shares: int
    suspended: bool
    lower_price_limit: Decimal | None
    upper_price_limit: Decimal | None
    completed_at: datetime
    source_id: str
    source_revision: str

    def __post_init__(self) -> None:
        if not isinstance(self.session_date, date) or isinstance(self.session_date, datetime):
            raise TypeError("session_date must be a date")
        for field_name in ("open", "high", "low", "close"):
            value = _positive_decimal(getattr(self, field_name), field_name)
            object.__setattr__(self, field_name, value)
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("OHLC values are inconsistent")
        if self.low > self.high:
            raise ValueError("bar low must not exceed high")
        if isinstance(self.volume_shares, bool) or not isinstance(self.volume_shares, int):
            raise TypeError("volume_shares must be an integer")
        if self.volume_shares < 0:
            raise ValueError("volume_shares must be non-negative")
        if not isinstance(self.suspended, bool):
            raise TypeError("suspended must be a bool")
        if (self.lower_price_limit is None) != (self.upper_price_limit is None):
            raise ValueError("lower and upper price limits must be supplied together")
        if self.lower_price_limit is not None and self.upper_price_limit is not None:
            lower = _positive_decimal(self.lower_price_limit, "lower_price_limit")
            upper = _positive_decimal(self.upper_price_limit, "upper_price_limit")
            if lower >= upper:
                raise ValueError("lower_price_limit must be below upper_price_limit")
            if self.low < lower or self.high > upper:
                raise ValueError("OHLC values must remain inside the explicit price band")
            object.__setattr__(self, "lower_price_limit", lower)
            object.__setattr__(self, "upper_price_limit", upper)
        completed_at = _aware_utc(self.completed_at, "completed_at")
        local_completion = completed_at.astimezone(_MARKET_TZ)
        if local_completion < datetime.combine(
            self.session_date,
            time(15, 0),
            tzinfo=_MARKET_TZ,
        ):
            raise ValueError("completed_at must not precede the session close")
        object.__setattr__(self, "completed_at", completed_at)
        object.__setattr__(self, "source_id", _identifier(self.source_id, "source_id"))
        object.__setattr__(
            self,
            "source_revision",
            _identifier(self.source_revision, "source_revision"),
        )


@dataclass(frozen=True, slots=True)
class AShareDailyStrategyObservation:
    """一个时点信号，以及严格晚于它的执行和计价交易日。"""

    observation_id: str
    symbol: str
    instrument_type: InstrumentType
    signal_as_of: datetime
    technical_scores: tuple[PITStrategyScore, ...]
    macro_score: PITStrategyScore
    execution_bar: CompletedAShareDailyBar
    buy_limit_price: Decimal | None = None
    sell_limit_price: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "observation_id",
            _identifier(self.observation_id, "observation_id"),
        )
        symbol = self.symbol.strip().upper()
        if len(symbol) != 9 or symbol[6:] not in {".SH", ".SZ", ".BJ"}:
            raise ValueError("symbol must use the 000001.SZ/600000.SH/920000.BJ form")
        if not symbol[:6].isdigit():
            raise ValueError("symbol must begin with six digits")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "instrument_type", InstrumentType(self.instrument_type))
        signal_as_of = _aware_utc(self.signal_as_of, "signal_as_of")
        object.__setattr__(self, "signal_as_of", signal_as_of)
        technical = tuple(sorted(self.technical_scores, key=lambda item: item.family_id))
        if not technical or len({score.family_id for score in technical}) != len(technical):
            raise ValueError("technical_scores must be non-empty and have unique families")
        if any(score.family_id == "macro" for score in technical):
            raise ValueError("macro is reserved for macro_score")
        if self.macro_score.family_id != "macro":
            raise ValueError("macro_score.family_id must be 'macro'")
        for score in (*technical, self.macro_score):
            if score.known_at > signal_as_of:
                raise ValueError("score known_at must not exceed signal_as_of")
        signal_date = signal_as_of.astimezone(_MARKET_TZ).date()
        if self.execution_bar.session_date <= signal_date:
            raise ValueError("execution bar must be from a later session than the signal")
        object.__setattr__(self, "technical_scores", technical)
        for field_name in ("buy_limit_price", "sell_limit_price"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _positive_decimal(value, field_name))

    @property
    def signal_date(self) -> date:
        return self.signal_as_of.astimezone(_MARKET_TZ).date()


@dataclass(frozen=True, slots=True)
class AShareDailyEvaluatorConfig:
    strategy_version: str = "ashare-daily-pit-evaluator@1"
    label_id: str = "next-session-open-mark-close@1"
    starting_cash_cny: Decimal = Decimal("1000000")
    enter_threshold: Decimal = Decimal("0.15")
    exit_threshold: Decimal = Decimal("0")
    max_volume_participation: Decimal = Decimal("0.01")
    transfer_fee_bps: Decimal = Decimal("0")
    annual_sessions: int = 252
    execution_policy: AShareExecutionPolicy = AShareExecutionPolicy.NEXT_OPEN

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "strategy_version",
            _identifier(self.strategy_version, "strategy_version"),
        )
        object.__setattr__(self, "label_id", _identifier(self.label_id, "label_id"))
        starting_cash = _positive_decimal(self.starting_cash_cny, "starting_cash_cny")
        enter = _finite_decimal(self.enter_threshold, "enter_threshold")
        exit_ = _finite_decimal(self.exit_threshold, "exit_threshold")
        participation = _finite_decimal(
            self.max_volume_participation,
            "max_volume_participation",
        )
        transfer = _finite_decimal(self.transfer_fee_bps, "transfer_fee_bps")
        if not -_SCORE_BOUND <= exit_ < enter <= _SCORE_BOUND:
            raise ValueError("thresholds must satisfy -1 <= exit < enter <= 1")
        if not Decimal("0") < participation <= Decimal("1"):
            raise ValueError("max_volume_participation must be in (0, 1]")
        if transfer < 0:
            raise ValueError("transfer_fee_bps must be non-negative")
        if isinstance(self.annual_sessions, bool) or self.annual_sessions < 1:
            raise ValueError("annual_sessions must be a positive integer")
        object.__setattr__(self, "starting_cash_cny", starting_cash)
        object.__setattr__(self, "enter_threshold", enter)
        object.__setattr__(self, "exit_threshold", exit_)
        object.__setattr__(self, "max_volume_participation", participation)
        object.__setattr__(self, "transfer_fee_bps", transfer)
        object.__setattr__(
            self,
            "execution_policy",
            AShareExecutionPolicy(self.execution_policy),
        )

    @property
    def manifest_parameters(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            sorted(
                (
                    ("annual_sessions", str(self.annual_sessions)),
                    ("enter_threshold", str(self.enter_threshold)),
                    ("execution_policy", self.execution_policy.value),
                    ("exit_threshold", str(self.exit_threshold)),
                    ("max_volume_participation", str(self.max_volume_participation)),
                    ("starting_cash_cny", str(self.starting_cash_cny)),
                    ("transfer_fee_bps", str(self.transfer_fee_bps)),
                )
            )
        )


@dataclass(frozen=True, slots=True)
class AShareEvaluationEvent:
    observation_id: str
    session_date: date
    action: AShareEvaluationAction
    fused_score: Decimal
    quantity: int = 0
    fill_price: Decimal | None = None
    cash_after: Decimal | None = None


@dataclass(frozen=True, slots=True)
class AShareEvaluationResult:
    """一个隔离评估切片的指标与审计轨迹。

    ``PerformanceMetrics.family_contributions`` 包含加权决策分数贡献均值，
    因此有意不将其标注为盈亏归因。
    """

    metrics: PerformanceMetrics
    events: tuple[AShareEvaluationEvent, ...]
    data_manifest_sha256: str
    strategy_manifest_sha256: str


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
