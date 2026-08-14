"""用于研究推荐、基于已完成行情柱的确定性技术信号。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from statistics import fmean

from gribuki_trade.domain.recommendations import (
    RecommendationDecision,
    RecommendationHorizon,
)


@dataclass(frozen=True, slots=True)
class TechnicalBar:
    """本进程观测到的一根已完成 OHLCV 行情柱。"""

    end_time: datetime
    available_at: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    complete: bool = True

    def __post_init__(self) -> None:
        prices = (self.open, self.high, self.low, self.close)
        if any(price <= 0 or not price.is_finite() for price in prices):
            raise ValueError("bar prices must be finite and positive")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("bar OHLC values are inconsistent")
        if self.high < self.low:
            raise ValueError("bar high must not be below low")
        if self.volume < 0:
            raise ValueError("bar volume must not be negative")
        if self.available_at < self.end_time:
            raise ValueError("available_at must not precede bar end_time")


@dataclass(frozen=True, slots=True)
class TechnicalSignalConfig:
    """短周期与波段信号实例共享的参数。"""

    fast_ma_bars: int = 10
    slow_ma_bars: int = 30
    breakout_bars: int = 20
    volume_bars: int = 20
    atr_bars: int = 14
    volume_ratio_threshold: Decimal = Decimal("1.5")
    near_breakout_fraction: Decimal = Decimal("0.01")
    atr_invalidation_multiple: Decimal = Decimal("1.5")
    max_data_age: timedelta = timedelta(minutes=3)
    strategy_version: str = "technical-breakout@1"

    def __post_init__(self) -> None:
        if self.fast_ma_bars <= 0 or self.fast_ma_bars >= self.slow_ma_bars:
            raise ValueError("fast_ma_bars must be positive and below slow_ma_bars")
        if min(self.breakout_bars, self.volume_bars, self.atr_bars) <= 1:
            raise ValueError("lookback values must exceed one")
        if self.volume_ratio_threshold <= 0:
            raise ValueError("volume_ratio_threshold must be positive")
        if not Decimal("0") <= self.near_breakout_fraction < Decimal("1"):
            raise ValueError("near_breakout_fraction must be in [0, 1)")
        if self.atr_invalidation_multiple <= 0:
            raise ValueError("atr_invalidation_multiple must be positive")
        if self.max_data_age <= timedelta(0):
            raise ValueError("max_data_age must be positive")
        if not self.strategy_version.strip():
            raise ValueError("strategy_version must not be empty")


@dataclass(frozen=True, slots=True)
class TechnicalSignal:
    """不含可执行订单字段的可审计技术结果。"""

    symbol: str
    as_of: datetime
    horizon: RecommendationHorizon
    decision: RecommendationDecision
    score: Decimal
    reference_price: Decimal | None
    invalidation_price: Decimal | None
    reason_codes: tuple[str, ...]
    data_age: timedelta
    strategy_version: str
    metrics: tuple[tuple[str, Decimal], ...]


def build_technical_signal(
    symbol: str,
    bars: tuple[TechnicalBar, ...],
    *,
    decision_time: datetime,
    horizon: RecommendationHorizon,
    is_currently_held: bool = False,
    config: TechnicalSignalConfig | None = None,
) -> TechnicalSignal:
    """只使用在 ``decision_time`` 可用的行情柱构建信号。

    未完成、未来、重复及乱序行情柱会被拒绝而非静默修复，从而保证同一函数可安全回放。
    """

    if not symbol.strip():
        raise ValueError("symbol must not be empty")
    resolved = config or TechnicalSignalConfig()
    if any(not bar.complete for bar in bars):
        raise ValueError("technical signals require completed bars")
    if any(bar.available_at > decision_time for bar in bars):
        raise ValueError("bar data was not available at decision_time")
    end_times = [bar.end_time for bar in bars]
    if end_times != sorted(end_times) or len(end_times) != len(set(end_times)):
        raise ValueError("bars must be strictly ordered and unique")

    required = max(
        resolved.slow_ma_bars,
        resolved.breakout_bars + 1,
        resolved.volume_bars + 1,
        resolved.atr_bars + 1,
    )
    if len(bars) < required:
        return _abstain(
            symbol,
            decision_time,
            horizon,
            resolved,
            "INSUFFICIENT_HISTORY",
        )

    latest = bars[-1]
    # ``available_at`` 是时点可见性边界。即使供应商返回旧行情柱，它也可能非常接近当前
    # 时间（例如笔记本恢复后的首次拉取）。因此，新鲜度必须从市场行情柱结束时间而非
    # 下载时间计量；否则旧价格只因刚刚拉取就可能显得是当前价格。
    data_age = decision_time - latest.end_time
    if data_age < timedelta(0):
        raise ValueError("latest bar is from the future")
    if data_age > resolved.max_data_age:
        return _abstain(
            symbol,
            decision_time,
            horizon,
            resolved,
            "STALE_MARKET_DATA",
            data_age=data_age,
        )

    closes = [float(bar.close) for bar in bars]
    fast_ma = Decimal(str(fmean(closes[-resolved.fast_ma_bars :])))
    slow_ma = Decimal(str(fmean(closes[-resolved.slow_ma_bars :])))
    prior = bars[-(resolved.breakout_bars + 1) : -1]
    breakout_level = max(bar.high for bar in prior)
    volume_window = bars[-(resolved.volume_bars + 1) : -1]
    average_volume = Decimal(str(fmean(bar.volume for bar in volume_window)))
    volume_ratio = (
        Decimal(latest.volume) / average_volume
        if average_volume > 0
        else Decimal("0")
    )
    atr = _average_true_range(bars, resolved.atr_bars)

    uptrend = fast_ma > slow_ma
    breakout = latest.close > breakout_level
    near_breakout = latest.close >= breakout_level * (
        Decimal("1") - resolved.near_breakout_fraction
    )
    volume_confirmation = volume_ratio >= resolved.volume_ratio_threshold

    reason_codes: list[str] = []
    raw_score = Decimal("0")
    if uptrend:
        reason_codes.append("FAST_MA_ABOVE_SLOW_MA")
        raw_score += Decimal("0.30")
    else:
        reason_codes.append("FAST_MA_NOT_ABOVE_SLOW_MA")
        raw_score -= Decimal("0.30")
    if breakout:
        reason_codes.append("CLOSED_BAR_BREAKOUT")
        raw_score += Decimal("0.40")
    elif near_breakout:
        reason_codes.append("NEAR_BREAKOUT")
        raw_score += Decimal("0.15")
    if volume_confirmation:
        reason_codes.append("VOLUME_CONFIRMED")
        raw_score += Decimal("0.30")
    elif volume_ratio < Decimal("0.8"):
        reason_codes.append("LOW_VOLUME")
        raw_score -= Decimal("0.10")

    if is_currently_held and latest.close < slow_ma:
        decision = RecommendationDecision.REDUCE
        reason_codes.append("CLOSE_BELOW_SLOW_MA")
        raw_score = min(raw_score, Decimal("-0.60"))
    elif uptrend and breakout and volume_confirmation:
        decision = RecommendationDecision.ENTER_CANDIDATE
    else:
        decision = RecommendationDecision.WATCH

    score = max(Decimal("-1"), min(Decimal("1"), raw_score))
    invalidation_value = latest.close - atr * resolved.atr_invalidation_multiple
    invalidation: Decimal | None = (
        invalidation_value if invalidation_value > 0 else None
    )
    return TechnicalSignal(
        symbol=symbol.upper(),
        as_of=decision_time,
        horizon=horizon,
        decision=decision,
        score=score,
        reference_price=latest.close,
        invalidation_price=invalidation,
        reason_codes=tuple(reason_codes),
        data_age=data_age,
        strategy_version=resolved.strategy_version,
        metrics=(
            ("fast_ma", fast_ma),
            ("slow_ma", slow_ma),
            ("breakout_level", breakout_level),
            ("volume_ratio", volume_ratio),
            ("atr", atr),
        ),
    )


def _average_true_range(bars: tuple[TechnicalBar, ...], lookback: int) -> Decimal:
    window = bars[-(lookback + 1) :]
    ranges: list[Decimal] = []
    for previous, current in zip(window, window[1:], strict=False):
        ranges.append(
            max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            )
        )
    return sum(ranges, Decimal("0")) / Decimal(len(ranges))


def _abstain(
    symbol: str,
    decision_time: datetime,
    horizon: RecommendationHorizon,
    config: TechnicalSignalConfig,
    reason: str,
    *,
    data_age: timedelta = timedelta(0),
) -> TechnicalSignal:
    return TechnicalSignal(
        symbol=symbol.upper(),
        as_of=decision_time,
        horizon=horizon,
        decision=RecommendationDecision.ABSTAIN,
        score=Decimal("0"),
        reference_price=None,
        invalidation_price=None,
        reason_codes=(reason,),
        data_age=data_age,
        strategy_version=config.strategy_version,
        metrics=(),
    )
