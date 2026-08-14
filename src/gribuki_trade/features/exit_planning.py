"""为新建多头仓位提供时点一致、确定性的保护退出计划。

本模块估计的是临时风险策略，而不是预期收益。止盈门槛是预先登记的收益风险
选项；部署前必须通过策略实验室的样本外评估证明其有效性。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from gribuki_trade.domain.exit_plans import (
    ExitPlan,
    ExitPlanDepth,
    ExitPlanState,
    exit_plan_id,
)
from gribuki_trade.features.technical import TechnicalBar


@dataclass(frozen=True, slots=True)
class QuickExitPlanConfig:
    """为低成本成交前计算预先登记的参数选项。"""

    atr_bars: int = 14
    breakout_bars: int = 20
    swing_search_bars: int = 20
    swing_left_bars: int = 2
    swing_right_bars: int = 2
    # QUICK 是成交前的临时保险。这里有意使用已登记网格中较宽的波动倍数，
    # 并在下方选择最松的有效候选，避免一分钟噪声把新仓过早洗出。
    atr_stop_multiple: Decimal = Decimal("2.0")
    structure_buffer_atr: Decimal = Decimal("0.10")
    reward_to_risk: Decimal = Decimal("1.5")
    allowed_atr_stop_multiples: tuple[Decimal, ...] = (
        Decimal("1.0"),
        Decimal("1.25"),
        Decimal("1.5"),
        Decimal("2.0"),
    )
    allowed_reward_to_risk: tuple[Decimal, ...] = (
        Decimal("1.0"),
        Decimal("1.5"),
        Decimal("2.0"),
        Decimal("2.5"),
        Decimal("3.0"),
    )
    price_tick: Decimal = Decimal("0.01")
    max_data_age: timedelta = timedelta(minutes=3)
    bar_interval_id: str = "1m"
    policy_version: str = "quick-exit-loose-provisional@2"
    calibration_id: str = "QUICK_LOOSE_REGISTERED_GRID_UNCALIBRATED@2"

    def __post_init__(self) -> None:
        for name in (
            "atr_bars",
            "breakout_bars",
            "swing_search_bars",
            "swing_left_bars",
            "swing_right_bars",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.swing_search_bars < self.swing_left_bars + self.swing_right_bars + 1:
            raise ValueError("swing_search_bars is too short to confirm a swing")
        if self.atr_stop_multiple not in self.allowed_atr_stop_multiples:
            raise ValueError("atr_stop_multiple must be selected from the registered grid")
        if self.reward_to_risk not in self.allowed_reward_to_risk:
            raise ValueError("reward_to_risk must be selected from the registered grid")
        for name in ("structure_buffer_atr", "price_tick"):
            value = getattr(self, name)
            if not isinstance(value, Decimal):
                raise TypeError(f"{name} must be a Decimal")
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be positive and finite")
        for name in ("allowed_atr_stop_multiples", "allowed_reward_to_risk"):
            values = getattr(self, name)
            if not values or len(values) != len(set(values)):
                raise ValueError(f"{name} must be non-empty and unique")
            if any(
                not isinstance(value, Decimal) or not value.is_finite() or value <= 0
                for value in values
            ):
                raise ValueError(f"{name} must contain positive finite Decimals")
        if self.max_data_age <= timedelta(0):
            raise ValueError("max_data_age must be positive")
        for name in ("bar_interval_id", "policy_version", "calibration_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must not be empty")

    @property
    def minimum_history(self) -> int:
        return max(
            self.atr_bars + 1,
            self.breakout_bars + 1,
            self.swing_search_bars,
        )


@dataclass(frozen=True, slots=True)
class QuickExitPlanResult:
    """计划及审计与实验所需的中间值。"""

    plan: ExitPlan
    true_ranges: tuple[Decimal, ...]
    median_true_range: Decimal
    mad_true_range: Decimal
    ewma_true_range: Decimal
    robust_atr: Decimal
    breakout_support: Decimal
    confirmed_swing_low: Decimal | None
    raw_stop_candidates: tuple[tuple[str, Decimal], ...]


def build_quick_exit_plan(
    *,
    account_id: str,
    protection_id: str,
    symbol: str,
    bars: tuple[TechnicalBar, ...],
    decision_at: datetime,
    time_exit_at: datetime,
    worst_entry_price: Decimal,
    technical_invalidation_price: Decimal,
    strategy_version: str,
    config: QuickExitPlanConfig | None = None,
) -> QuickExitPlanResult:
    """构建低成本的临时止损、目标位及由调用方解析的时间门槛。

    ``time_exit_at`` 必须由了解交易日历的调用方提供。本函数有意不直接增加
    自然日，以免错误地把周末或交易所休市日当成交易时段。
    """

    resolved = config or QuickExitPlanConfig()
    decision_at = _aware_utc(decision_at, "decision_at")
    time_exit_at = _aware_utc(time_exit_at, "time_exit_at")
    entry = _positive_decimal(worst_entry_price, "worst_entry_price")
    invalidation = _positive_decimal(
        technical_invalidation_price,
        "technical_invalidation_price",
    )
    if invalidation >= entry:
        raise ValueError("technical invalidation must be below worst entry")
    if time_exit_at <= decision_at:
        raise ValueError("time_exit_at must be after decision_at")
    _validate_bars(bars, decision_at=decision_at, config=resolved)

    true_ranges = _true_ranges(bars, resolved.atr_bars)
    median_true_range = _median(true_ranges)
    absolute_deviations = tuple(abs(value - median_true_range) for value in true_ranges)
    mad_true_range = _median(absolute_deviations)
    ewma_true_range = _ewma(true_ranges)
    # 中位数/MAD 抵抗孤立坏点，EWMA 则保留对近期真实波动变化的响应。
    robust_atr = max(
        median_true_range + Decimal("1.4826") * mad_true_range,
        ewma_true_range,
    )
    if robust_atr <= 0:
        raise ValueError("robust ATR is not positive")

    breakout_window = bars[-(resolved.breakout_bars + 1) : -1]
    breakout_support = max(bar.high for bar in breakout_window)
    confirmed_swing_low = _latest_confirmed_swing_low(bars, resolved)
    buffer = robust_atr * resolved.structure_buffer_atr
    raw_candidates: list[tuple[str, Decimal]] = [
        ("technical_invalidation", invalidation),
        ("volatility_stop", entry - robust_atr * resolved.atr_stop_multiple),
        ("breakout_support", breakout_support - buffer),
    ]
    if confirmed_swing_low is not None:
        raw_candidates.append(("confirmed_swing_low", confirmed_swing_low - buffer))
    if any(value <= 0 for _, value in raw_candidates):
        raise ValueError("a stop candidate is non-positive")
    # 用户要求 QUICK 采用“最松”的临时保护。对多头而言价格越低越松，
    # 因此取有效候选最小值。风险定仓仍按这个较宽止损与最坏买入价计算，
    # 不会因为放松止损而放大名义风险。
    raw_stop = min(value for _, value in raw_candidates)
    stop = _round_to_tick(raw_stop, resolved.price_tick, rounding=ROUND_FLOOR)
    entry = _round_to_tick(entry, resolved.price_tick, rounding=ROUND_CEILING)
    if stop >= entry:
        raise ValueError("provisional stop is not below the worst entry price")
    initial_risk = entry - stop
    take_profit = _round_to_tick(
        entry + initial_risk * resolved.reward_to_risk,
        resolved.price_tick,
        rounding=ROUND_CEILING,
    )

    feature_snapshot_sha256 = _feature_snapshot_sha256(
        symbol=symbol,
        bars=bars[-resolved.minimum_history :],
        decision_at=decision_at,
        time_exit_at=time_exit_at,
        entry=entry,
        invalidation=invalidation,
        config=resolved,
    )
    reasons = [
        "PROVISIONAL_NOT_EXPECTED_RETURN_FORECAST",
        "LOOSEST_VALID_PRETRADE_STOP",
        "UNCALIBRATED_PROVISIONAL_POLICY",
        "WORST_BUY_LIMIT_ENTRY_BASIS",
        "ROBUST_ATR_STOP",
        "BREAKOUT_STRUCTURE_STOP",
        "REGISTERED_REWARD_RISK_TARGET",
        "CALLER_RESOLVED_TIME_BARRIER",
    ]
    if confirmed_swing_low is None:
        reasons.append("NO_CONFIRMED_SWING_LOW")
    else:
        reasons.append("CONFIRMED_SWING_LOW_STOP")
    metrics: list[tuple[str, Decimal]] = [
        ("breakout_support", breakout_support),
        ("ewma_true_range", ewma_true_range),
        ("mad_true_range", mad_true_range),
        ("median_true_range", median_true_range),
        ("robust_atr", robust_atr),
    ]
    if confirmed_swing_low is not None:
        metrics.append(("confirmed_swing_low", confirmed_swing_low))
    for name, value in raw_candidates:
        metrics.append((f"candidate_{name}", value))
    plan = ExitPlan(
        plan_id=exit_plan_id(
            protection_id=protection_id,
            version=1,
            feature_snapshot_sha256=feature_snapshot_sha256,
            policy_version=resolved.policy_version,
        ),
        protection_id=protection_id,
        account_id=account_id,
        symbol=symbol,
        version=1,
        depth=ExitPlanDepth.QUICK,
        state=ExitPlanState.PROVISIONAL,
        decision_at=decision_at,
        market_data_as_of=bars[-1].end_time,
        time_exit_at=time_exit_at,
        entry_basis_price=entry,
        stop_price=stop,
        take_profit_price=take_profit,
        initial_risk_per_share=initial_risk,
        reward_to_risk=resolved.reward_to_risk,
        price_tick=resolved.price_tick,
        technical_invalidation_price=invalidation,
        feature_snapshot_sha256=feature_snapshot_sha256,
        policy_version=resolved.policy_version,
        strategy_version=strategy_version,
        calibration_id=resolved.calibration_id,
        reason_codes=tuple(reasons),
        metrics=tuple(metrics),
    )
    return QuickExitPlanResult(
        plan=plan,
        true_ranges=true_ranges,
        median_true_range=median_true_range,
        mad_true_range=mad_true_range,
        ewma_true_range=ewma_true_range,
        robust_atr=robust_atr,
        breakout_support=breakout_support,
        confirmed_swing_low=confirmed_swing_low,
        raw_stop_candidates=tuple(raw_candidates),
    )


def _validate_bars(
    bars: tuple[TechnicalBar, ...],
    *,
    decision_at: datetime,
    config: QuickExitPlanConfig,
) -> None:
    if len(bars) < config.minimum_history:
        raise ValueError("insufficient completed history for quick exit planning")
    for bar in bars:
        _aware_utc(bar.end_time, "bar end_time")
        _aware_utc(bar.available_at, "bar available_at")
    if any(not bar.complete for bar in bars):
        raise ValueError("quick exit planning requires completed bars")
    if any(bar.available_at > decision_at for bar in bars):
        raise ValueError("a bar was not available at decision_at")
    end_times = tuple(bar.end_time for bar in bars)
    if end_times != tuple(sorted(end_times)) or len(end_times) != len(set(end_times)):
        raise ValueError("bars must be strictly ordered and unique")
    age = decision_at - bars[-1].end_time
    if age < timedelta(0):
        raise ValueError("latest bar is from the future")
    if age > config.max_data_age:
        raise ValueError("latest completed bar is stale")


def _true_ranges(bars: tuple[TechnicalBar, ...], lookback: int) -> tuple[Decimal, ...]:
    window = bars[-(lookback + 1) :]
    return tuple(
        max(
            current.high - current.low,
            abs(current.high - previous.close),
            abs(current.low - previous.close),
        )
        for previous, current in zip(window, window[1:], strict=False)
    )


def _median(values: tuple[Decimal, ...]) -> Decimal:
    ordered = tuple(sorted(values))
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal(2)


def _ewma(values: tuple[Decimal, ...]) -> Decimal:
    alpha = Decimal(2) / Decimal(len(values) + 1)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (Decimal(1) - alpha) * result
    return result


def _latest_confirmed_swing_low(
    bars: tuple[TechnicalBar, ...],
    config: QuickExitPlanConfig,
) -> Decimal | None:
    start = max(config.swing_left_bars, len(bars) - config.swing_search_bars)
    stop = len(bars) - config.swing_right_bars
    for index in range(stop - 1, start - 1, -1):
        candidate = bars[index].low
        left = bars[index - config.swing_left_bars : index]
        right = bars[index + 1 : index + config.swing_right_bars + 1]
        if candidate < min(item.low for item in left) and candidate <= min(
            item.low for item in right
        ):
            return candidate
    return None


def _feature_snapshot_sha256(
    *,
    symbol: str,
    bars: tuple[TechnicalBar, ...],
    decision_at: datetime,
    time_exit_at: datetime,
    entry: Decimal,
    invalidation: Decimal,
    config: QuickExitPlanConfig,
) -> str:
    document = {
        "bars": [
            {
                "available_at": bar.available_at.astimezone(UTC).isoformat(),
                "close": str(bar.close),
                "end_time": bar.end_time.astimezone(UTC).isoformat(),
                "high": str(bar.high),
                "low": str(bar.low),
                "open": str(bar.open),
                "volume": bar.volume,
            }
            for bar in bars
        ],
        "config": {
            "atr_bars": config.atr_bars,
            "atr_stop_multiple": str(config.atr_stop_multiple),
            "bar_interval_id": config.bar_interval_id,
            "breakout_bars": config.breakout_bars,
            "calibration_id": config.calibration_id,
            "policy_version": config.policy_version,
            "price_tick": str(config.price_tick),
            "reward_to_risk": str(config.reward_to_risk),
            "structure_buffer_atr": str(config.structure_buffer_atr),
            "swing_left_bars": config.swing_left_bars,
            "swing_right_bars": config.swing_right_bars,
            "swing_search_bars": config.swing_search_bars,
        },
        "decision_at": decision_at.isoformat(),
        "entry": str(entry),
        "invalidation": str(invalidation),
        "symbol": symbol.strip().upper(),
        "time_exit_at": time_exit_at.isoformat(),
    }
    encoded = json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _positive_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return value


def _round_to_tick(
    value: Decimal,
    tick: Decimal,
    *,
    rounding: str,
) -> Decimal:
    units = (value / tick).to_integral_value(rounding=rounding)
    return units * tick


def _aware_utc(value: object, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


__all__ = [
    "QuickExitPlanConfig",
    "QuickExitPlanResult",
    "build_quick_exit_plan",
]
