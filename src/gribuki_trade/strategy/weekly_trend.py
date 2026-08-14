"""与券商无关的周频趋势和动量策略。

本模块刻意不包含时钟、数据源或券商访问。调用方必须使用在 ``decision_date`` 已知的
信息，为每个标的构建一个 :class:`SymbolDailySnapshot`。这样可使排序函数保持确定性，
并将下一交易时段的执行排除在信号计算之外。
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from statistics import fmean, stdev


@dataclass(frozen=True, slots=True)
class WeeklyTrendConfig:
    """周频趋势策略的参数。"""

    momentum_lookback_days: int = 120
    momentum_skip_days: int = 5
    fast_ma_days: int = 20
    slow_ma_days: int = 60
    volatility_days: int = 60
    annualization_days: int = 252
    min_listing_days: int = 250
    min_average_turnover_20_cny: float = 50_000_000.0
    max_positions: int = 10
    retention_rank: int = 20
    max_weight_per_symbol: float = 0.12
    min_cash_weight: float = 0.05
    volatility_floor: float = 1e-6

    def __post_init__(self) -> None:
        if self.momentum_lookback_days <= self.momentum_skip_days:
            raise ValueError("momentum_lookback_days must exceed momentum_skip_days")
        if self.momentum_skip_days < 0:
            raise ValueError("momentum_skip_days must not be negative")
        if self.fast_ma_days <= 0 or self.fast_ma_days >= self.slow_ma_days:
            raise ValueError("fast_ma_days must be positive and less than slow_ma_days")
        if self.volatility_days < 2:
            raise ValueError("volatility_days must be at least 2")
        if self.annualization_days <= 0:
            raise ValueError("annualization_days must be positive")
        if self.min_listing_days < 0:
            raise ValueError("min_listing_days must not be negative")
        if (
            not math.isfinite(self.min_average_turnover_20_cny)
            or self.min_average_turnover_20_cny < 0
        ):
            raise ValueError("min_average_turnover_20_cny must be finite and non-negative")
        if self.max_positions <= 0:
            raise ValueError("max_positions must be positive")
        if self.retention_rank < self.max_positions:
            raise ValueError("retention_rank must be at least max_positions")
        if not 0 < self.max_weight_per_symbol <= 1:
            raise ValueError("max_weight_per_symbol must be in (0, 1]")
        if not 0 <= self.min_cash_weight < 1:
            raise ValueError("min_cash_weight must be in [0, 1)")
        if not math.isfinite(self.volatility_floor) or self.volatility_floor <= 0:
            raise ValueError("volatility_floor must be finite and positive")


@dataclass(frozen=True, slots=True)
class SymbolDailySnapshot:
    """截至 ``as_of`` 已知的单个标的日频信息。

    ``closes`` 必须从最早到最新排序，并终止于 ``as_of``。其中刻意不包含未来收益或
    下一交易时段价格。流动性和上市时长均为仅使用当日可用数据计算的快照。
    """

    symbol: str
    as_of: date
    closes: tuple[float, ...]
    average_turnover_20_cny: float
    listing_days: int
    is_tradable: bool = True
    is_st: bool = False

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if self.listing_days < 0:
            raise ValueError("listing_days must not be negative")
        if (
            not math.isfinite(self.average_turnover_20_cny)
            or self.average_turnover_20_cny < 0
        ):
            raise ValueError("average_turnover_20_cny must be finite and non-negative")
        if any(not math.isfinite(close) or close <= 0 for close in self.closes):
            raise ValueError("closes must contain only finite positive values")


@dataclass(frozen=True, slots=True)
class CandidateMetrics:
    """仅根据截至决策日的数据计算的指标。"""

    symbol: str
    rank: int
    momentum: float
    fast_ma: float
    slow_ma: float
    annualized_volatility: float
    risk_adjusted_momentum: float


@dataclass(frozen=True, slots=True)
class TargetWeight:
    """策略选出的目标；执行由另一层负责。"""

    symbol: str
    weight: float
    rank: int
    was_held: bool


@dataclass(frozen=True, slots=True)
class WeeklyTrendDecision:
    """一个周频决策日对应的不可变策略输出。"""

    decision_date: date
    targets: tuple[TargetWeight, ...]
    cash_weight: float
    ranked_candidates: tuple[CandidateMetrics, ...]


@dataclass(frozen=True, slots=True)
class _UnrankedMetrics:
    symbol: str
    momentum: float
    fast_ma: float
    slow_ma: float
    annualized_volatility: float
    risk_adjusted_momentum: float


def build_weekly_trend_decision(
    decision_date: date,
    snapshots: Iterable[SymbolDailySnapshot],
    current_holdings: Iterable[str] = (),
    config: WeeklyTrendConfig | None = None,
) -> WeeklyTrendDecision:
    """对符合条件的标的排序，并生成设有上限的反波动率目标权重。

    日期早于或晚于 ``decision_date`` 的快照会被拒绝，而不会静默混合不同日期的
    横截面。策略运行器最早应在下一交易时段执行返回的目标。
    """

    resolved_config = config or WeeklyTrendConfig()
    snapshot_items = tuple(snapshots)
    symbols = [snapshot.symbol for snapshot in snapshot_items]
    if len(symbols) != len(set(symbols)):
        raise ValueError("snapshots must contain unique symbols")

    unranked: list[_UnrankedMetrics] = []
    for snapshot in snapshot_items:
        if snapshot.as_of != decision_date:
            raise ValueError(
                f"snapshot for {snapshot.symbol} must be dated exactly at decision_date"
            )
        metrics = _calculate_metrics(snapshot, resolved_config)
        if metrics is not None:
            unranked.append(metrics)

    ordered = sorted(
        unranked,
        key=lambda item: (
            -item.risk_adjusted_momentum,
            -item.momentum,
            item.annualized_volatility,
            item.symbol,
        ),
    )
    ranked = tuple(
        CandidateMetrics(
            symbol=item.symbol,
            rank=rank,
            momentum=item.momentum,
            fast_ma=item.fast_ma,
            slow_ma=item.slow_ma,
            annualized_volatility=item.annualized_volatility,
            risk_adjusted_momentum=item.risk_adjusted_momentum,
        )
        for rank, item in enumerate(ordered, start=1)
    )

    held = frozenset(current_holdings)
    selected = [
        candidate
        for candidate in ranked
        if candidate.symbol in held and candidate.rank <= resolved_config.retention_rank
    ][: resolved_config.max_positions]
    selected_symbols = {candidate.symbol for candidate in selected}
    for candidate in ranked:
        if len(selected) >= resolved_config.max_positions:
            break
        if candidate.symbol not in selected_symbols:
            selected.append(candidate)
            selected_symbols.add(candidate.symbol)

    selected.sort(key=lambda candidate: candidate.rank)
    weights = _capped_inverse_volatility_weights(selected, resolved_config)
    targets = tuple(
        TargetWeight(
            symbol=candidate.symbol,
            weight=weights[candidate.symbol],
            rank=candidate.rank,
            was_held=candidate.symbol in held,
        )
        for candidate in selected
    )
    invested_weight = math.fsum(target.weight for target in targets)
    cash_weight = max(0.0, 1.0 - invested_weight)

    return WeeklyTrendDecision(
        decision_date=decision_date,
        targets=targets,
        cash_weight=cash_weight,
        ranked_candidates=ranked,
    )


def _calculate_metrics(
    snapshot: SymbolDailySnapshot, config: WeeklyTrendConfig
) -> _UnrankedMetrics | None:
    if (
        not snapshot.is_tradable
        or snapshot.is_st
        or snapshot.listing_days < config.min_listing_days
        or snapshot.average_turnover_20_cny < config.min_average_turnover_20_cny
    ):
        return None

    required_closes = max(
        config.momentum_lookback_days + 1,
        config.slow_ma_days,
        config.volatility_days + 1,
    )
    if len(snapshot.closes) < required_closes:
        return None

    closes = snapshot.closes
    momentum_start = closes[-(config.momentum_lookback_days + 1)]
    momentum_end = closes[-(config.momentum_skip_days + 1)]
    momentum = momentum_end / momentum_start - 1.0

    fast_ma = fmean(closes[-config.fast_ma_days :])
    slow_ma = fmean(closes[-config.slow_ma_days :])
    if fast_ma <= slow_ma:
        return None

    volatility_closes = closes[-(config.volatility_days + 1) :]
    returns = [
        current / previous - 1.0
        for previous, current in zip(
            volatility_closes, volatility_closes[1:], strict=False
        )
    ]
    annualized_volatility = stdev(returns) * math.sqrt(config.annualization_days)
    scaled_volatility = max(annualized_volatility, config.volatility_floor)

    return _UnrankedMetrics(
        symbol=snapshot.symbol,
        momentum=momentum,
        fast_ma=fast_ma,
        slow_ma=slow_ma,
        annualized_volatility=annualized_volatility,
        risk_adjusted_momentum=momentum / scaled_volatility,
    )


def _capped_inverse_volatility_weights(
    selected: list[CandidateMetrics], config: WeeklyTrendConfig
) -> dict[str, float]:
    if not selected:
        return {}

    investable_budget = 1.0 - config.min_cash_weight
    target_budget = min(
        investable_budget,
        len(selected) * config.max_weight_per_symbol,
    )
    remaining_budget = target_budget
    remaining = {candidate.symbol: candidate for candidate in selected}
    weights: dict[str, float] = {}

    while remaining:
        inverse_volatility = {
            symbol: 1.0 / max(candidate.annualized_volatility, config.volatility_floor)
            for symbol, candidate in remaining.items()
        }
        inverse_total = math.fsum(inverse_volatility.values())
        proposed = {
            symbol: remaining_budget * inverse_volatility[symbol] / inverse_total
            for symbol in remaining
        }
        capped = [
            symbol
            for symbol, weight in proposed.items()
            if weight > config.max_weight_per_symbol
        ]
        if not capped:
            weights.update(proposed)
            break

        for symbol in capped:
            weights[symbol] = config.max_weight_per_symbol
            remaining_budget -= config.max_weight_per_symbol
            del remaining[symbol]

    return weights
