"""收盘分析使用的纯日线指标与评分原语。

本模块只接受已标准化的日线数据和 ``Decimal`` 值，不读取文件、不访问网络，
也不依赖收盘分析编排状态。保留这些函数的独立边界，便于指标回归测试和后续
策略复用。
"""

from __future__ import annotations

from decimal import Decimal

from gribuki_trade.domain.market import DailyBar


def _required_price(value: Decimal | None) -> Decimal:
    if value is None or not value.is_finite() or value <= 0:
        raise ValueError("daily bar prices must be finite and positive")
    return value


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def _average_true_range(bars: tuple[DailyBar, ...], lookback: int) -> Decimal:
    window = bars[-(lookback + 1) :]
    ranges: list[Decimal] = []
    for previous, current in zip(window, window[1:], strict=False):
        previous_close = _required_price(previous.close)
        high = _required_price(current.high)
        low = _required_price(current.low)
        ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
    return _mean(tuple(ranges))


def _relative_strength_index(closes: tuple[Decimal, ...], lookback: int) -> Decimal:
    window = closes[-(lookback + 1) :]
    changes = tuple(
        current - previous for previous, current in zip(window, window[1:], strict=False)
    )
    average_gain = _mean(tuple(max(change, Decimal(0)) for change in changes))
    average_loss = _mean(tuple(max(-change, Decimal(0)) for change in changes))
    if average_loss == 0:
        return Decimal(100) if average_gain > 0 else Decimal(50)
    relative_strength = average_gain / average_loss
    return Decimal(100) - Decimal(100) / (Decimal(1) + relative_strength)


def _wilder_adx(bars: tuple[DailyBar, ...], lookback: int) -> tuple[Decimal, Decimal, Decimal]:
    """使用规范递归平滑返回 Wilder ADX、+DI 与 -DI。

    不同于本基线其他位置刻意采用简单窗口的 ATR 与 RSI，方向运动以 ``lookback``
    个值之和为种子，再用 Wilder 的 ``previous - previous / n + current`` 递推更新。
    首个 ADX 是前 ``lookback`` 个 DX 值的算术平均。
    """

    if len(bars) < lookback * 2:
        raise ValueError("ADX input does not cover its initialization period")
    true_ranges: list[Decimal] = []
    positive_movements: list[Decimal] = []
    negative_movements: list[Decimal] = []
    for previous, current in zip(bars[:-1], bars[1:], strict=True):
        previous_high = _required_price(previous.high)
        previous_low = _required_price(previous.low)
        previous_close = _required_price(previous.close)
        current_high = _required_price(current.high)
        current_low = _required_price(current.low)
        upward_move = current_high - previous_high
        downward_move = previous_low - current_low
        true_ranges.append(
            max(
                current_high - current_low,
                abs(current_high - previous_close),
                abs(current_low - previous_close),
            )
        )
        positive_movements.append(
            upward_move if upward_move > downward_move and upward_move > 0 else Decimal(0)
        )
        negative_movements.append(
            downward_move if downward_move > upward_move and downward_move > 0 else Decimal(0)
        )

    smoothed_tr = sum(true_ranges[:lookback], Decimal(0))
    smoothed_positive = sum(positive_movements[:lookback], Decimal(0))
    smoothed_negative = sum(negative_movements[:lookback], Decimal(0))
    dx_values: list[Decimal] = []
    positive_di = negative_di = Decimal(0)

    def append_dx() -> None:
        nonlocal positive_di, negative_di
        if smoothed_tr <= 0:
            positive_di = negative_di = Decimal(0)
            dx_values.append(Decimal(0))
            return
        positive_di = Decimal(100) * smoothed_positive / smoothed_tr
        negative_di = Decimal(100) * smoothed_negative / smoothed_tr
        denominator = positive_di + negative_di
        dx_values.append(
            Decimal(0)
            if denominator <= 0
            else Decimal(100) * abs(positive_di - negative_di) / denominator
        )

    append_dx()
    for true_range, positive_movement, negative_movement in zip(
        true_ranges[lookback:],
        positive_movements[lookback:],
        negative_movements[lookback:],
        strict=True,
    ):
        smoothed_tr = smoothed_tr - smoothed_tr / Decimal(lookback) + true_range
        smoothed_positive = (
            smoothed_positive - smoothed_positive / Decimal(lookback) + positive_movement
        )
        smoothed_negative = (
            smoothed_negative - smoothed_negative / Decimal(lookback) + negative_movement
        )
        append_dx()

    if len(dx_values) < lookback:
        raise ValueError("ADX input does not cover its DX initialization period")
    adx = _mean(tuple(dx_values[:lookback]))
    for dx in dx_values[lookback:]:
        adx = (adx * Decimal(lookback - 1) + dx) / Decimal(lookback)
    return adx, positive_di, negative_di


def _period_return(closes: tuple[Decimal, ...], sessions: int) -> Decimal:
    return closes[-1] / closes[-(sessions + 1)] - Decimal(1)


def _last_day_return(bars: tuple[DailyBar, ...]) -> Decimal:
    latest = bars[-1]
    baseline = latest.previous_close or _required_price(bars[-2].close)
    return _required_price(latest.close) / baseline - Decimal(1)


def _annualized_volatility(closes: tuple[Decimal, ...], sessions: int) -> Decimal:
    window = closes[-(sessions + 1) :]
    returns = tuple(
        current / previous - Decimal(1)
        for previous, current in zip(window, window[1:], strict=False)
    )
    average = _mean(returns)
    variance = _mean(tuple((value - average) ** 2 for value in returns))
    return variance.sqrt() * Decimal(252).sqrt()


def _standard_deviation(values: tuple[Decimal, ...]) -> Decimal:
    average = _mean(values)
    return _mean(tuple((value - average) ** 2 for value in values)).sqrt()


def _bollinger_bandwidth_series(closes: tuple[Decimal, ...], sessions: int) -> tuple[Decimal, ...]:
    output: list[Decimal] = []
    for end in range(sessions, len(closes) + 1):
        window = closes[end - sessions : end]
        center = _mean(window)
        deviation = _standard_deviation(window)
        output.append(Decimal(0) if center <= 0 else Decimal(4) * deviation / center)
    return tuple(output)


def _ema(values: tuple[Decimal, ...], sessions: int) -> Decimal:
    if len(values) < sessions:
        raise ValueError("EMA input does not cover its lookback")
    alpha = Decimal(2) / Decimal(sessions + 1)
    result = _mean(values[:sessions])
    for value in values[sessions:]:
        result = alpha * value + (Decimal(1) - alpha) * result
    return result


def _macd_series(
    closes: tuple[Decimal, ...], short_sessions: int, long_sessions: int
) -> tuple[Decimal, ...]:
    if len(closes) < long_sessions:
        raise ValueError("MACD input does not cover its long lookback")
    values: list[Decimal] = []
    for end in range(long_sessions, len(closes) + 1):
        window = closes[:end]
        values.append(_ema(window, short_sessions) - _ema(window, long_sessions))
    return tuple(values)


def _stochastic_k(bars: tuple[DailyBar, ...], sessions: int) -> Decimal:
    window = bars[-sessions:]
    low = min(_required_price(item.low) for item in window)
    high = max(_required_price(item.high) for item in window)
    return (
        (_required_price(window[-1].close) - low) / (high - low) * Decimal(100)
        if high > low
        else Decimal(50)
    )


def _money_flow_ratio(bars: tuple[DailyBar, ...], sessions: int) -> Decimal:
    window = bars[-(sessions + 1) :]
    positive = negative = Decimal(0)
    for previous, current in zip(window, window[1:], strict=False):
        amount = current.amount
        current_close = _required_price(current.close)
        previous_close = _required_price(previous.close)
        if current_close > previous_close:
            positive += amount
        elif current_close < previous_close:
            negative += amount
    if negative == 0:
        return Decimal(3) if positive > 0 else Decimal(1)
    return min(Decimal(3), positive / negative)


def _amihud_illiquidity(bars: tuple[DailyBar, ...], sessions: int) -> Decimal:
    value, _ = _amihud_illiquidity_with_coverage(bars, sessions)
    return value


def _amihud_illiquidity_with_coverage(
    bars: tuple[DailyBar, ...],
    sessions: int,
) -> tuple[Decimal, Decimal]:
    """返回原始 Amihud 非流动性指标与可用交易日比例。"""

    values: list[Decimal] = []
    window = bars[-(sessions + 1) :]
    for previous, current in zip(window[:-1], window[1:], strict=True):
        if current.amount <= 0:
            continue
        daily_return = abs(
            _required_price(current.close) / _required_price(previous.close) - Decimal(1)
        )
        values.append(daily_return / current.amount)
    coverage = Decimal(len(values)) / Decimal(sessions)
    return (_mean(tuple(values)) if values else Decimal(0), coverage)


def _amihud_bps_per_cny_billion(raw_value: Decimal) -> Decimal:
    """以可读市场单位表示 ``mean(abs(return) / CNY amount)``。

    原始 Amihud 值保留在 ``metrics`` 中以支持复现。辅助尺度表示每 10 亿元人民币成交额
    对应的原始比率有多少基点：``raw * 1e9 CNY * 1e4 bp``。它是归一化非流动性统计量，
    不是因果价格冲击估计。
    """

    return raw_value * Decimal("10000000000000")


def _turnover_state(bars: tuple[DailyBar, ...], sessions: int) -> Decimal:
    value, _ = _turnover_state_with_coverage(bars, sessions)
    return value


def _turnover_state_with_coverage(
    bars: tuple[DailyBar, ...],
    sessions: int,
) -> tuple[Decimal, Decimal]:
    available = tuple(
        item.turnover_percent
        for item in bars[-(sessions + 1) :]
        if item.turnover_percent is not None
    )
    coverage = Decimal(len(available)) / Decimal(sessions + 1)
    if len(available) < sessions + 1:
        return Decimal(1), coverage
    baseline = _mean(tuple(available[:-1]))
    return (available[-1] / baseline if baseline > 0 else Decimal(1), coverage)


def _max_drawdown(closes: tuple[Decimal, ...]) -> Decimal:
    peak = closes[0]
    largest = Decimal(0)
    for close in closes:
        peak = max(peak, close)
        largest = max(largest, Decimal(1) - close / peak)
    return largest


def _downside_volatility(closes: tuple[Decimal, ...], sessions: int) -> Decimal:
    window = closes[-(sessions + 1) :]
    returns = tuple(
        min(Decimal(0), current / previous - Decimal(1))
        for previous, current in zip(window[:-1], window[1:], strict=True)
    )
    return _mean(tuple(value * value for value in returns)).sqrt() * Decimal(252).sqrt()


def _overnight_gap_volatility(bars: tuple[DailyBar, ...], sessions: int) -> Decimal:
    window = bars[-(sessions + 1) :]
    gaps = tuple(
        _required_price(item.open) / _required_price(previous.close) - Decimal(1)
        for previous, item in zip(window[:-1], window[1:], strict=True)
    )
    return _standard_deviation(gaps) * Decimal(252).sqrt()


def _normalized_slope(closes: tuple[Decimal, ...], sessions: int, atr: Decimal) -> Decimal:
    window = closes[-sessions:]
    x_mean = Decimal(sessions - 1) / Decimal(2)
    y_mean = _mean(window)
    numerator = sum(
        ((Decimal(index) - x_mean) * (value - y_mean) for index, value in enumerate(window)),
        Decimal(0),
    )
    denominator = sum(
        ((Decimal(index) - x_mean) ** 2 for index in range(sessions)),
        Decimal(0),
    )
    slope = numerator / denominator
    return slope / atr if atr > 0 else Decimal(0)


def _sign(value: Decimal) -> Decimal:
    return Decimal(1) if value > 0 else Decimal(-1) if value < 0 else Decimal(0)


def _clamp_unit(value: Decimal) -> Decimal:
    return max(Decimal(-1), min(Decimal(1), value))


def _risk_adjusted_return(
    value: Decimal,
    annualized_volatility: Decimal,
    sessions: int,
) -> Decimal:
    if annualized_volatility <= 0:
        return Decimal(0)
    horizon_volatility = annualized_volatility * (Decimal(sessions) / Decimal(252)).sqrt()
    return _clamp_unit(value / horizon_volatility)


def _level_score(close: Decimal, high: Decimal, low: Decimal) -> Decimal:
    midpoint = (high + low) / Decimal(2)
    half_range = (high - low) / Decimal(2)
    return _clamp_unit((close - midpoint) / half_range) if half_range > 0 else Decimal(0)


def _pullback_score(
    *,
    latest_close: Decimal,
    trend_ma: Decimal,
    atr: Decimal,
    rsi: Decimal,
    stochastic_k: Decimal,
    percent_b: Decimal,
    trend_regime: bool,
) -> Decimal:
    if not trend_regime:
        return min(
            Decimal(0),
            _mean(
                (
                    _clamp_unit((rsi - Decimal(50)) / Decimal(25)),
                    _clamp_unit((stochastic_k - Decimal(50)) / Decimal(40)),
                    _clamp_unit((percent_b - Decimal("0.5")) * Decimal(2)),
                )
            ),
        )
    distance = (latest_close - trend_ma) / max(atr, Decimal("0.000001"))
    proximity = Decimal(1) - min(Decimal(1), abs(distance) / Decimal(2))
    oscillator = _mean(
        (
            _clamp_unit((Decimal(65) - rsi) / Decimal(25)),
            _clamp_unit((Decimal(70) - stochastic_k) / Decimal(40)),
            _clamp_unit((Decimal("0.8") - percent_b) * Decimal(2)),
        )
    )
    return _clamp_unit((proximity + oscillator) / Decimal(2))
