"""面向下一 A 股交易日的时点技术评估。

盘中信号引擎刻意将旧分钟柱视为过期。盘后评估采用不同的时钟：只消费已完成日线柱，
并面向显式提供的未来交易日。保持该逻辑独立，可防止隔夜日线分析削弱盘中研究所用的
新鲜度规则。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import StrEnum
from zoneinfo import ZoneInfo

from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.domain.recommendations import (
    RecommendationDecision,
    RecommendationHorizon,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


class CloseInstrumentType(StrEnum):
    STOCK = "stock"
    ETF = "etf"


class CloseDiagnosticHorizon(StrEnum):
    """只用于展示的诊断期限，绝不是订单有效期。"""

    SHORT_1_TO_5_DAYS = "SHORT_1_TO_5_DAYS"
    SWING_2_TO_8_WEEKS = "SWING_2_TO_8_WEEKS"


class CloseSignalFamilyStatus(StrEnum):
    """信号族评分为何具有或不具有数值信息。"""

    ACTIVE = "ACTIVE"
    NEUTRAL = "NEUTRAL"
    INACTIVE = "INACTIVE"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class CloseAnalysisConfig:
    """确定性收盘分析的透明参数。"""

    short_ma_sessions: int = 5
    trend_ma_sessions: int = 20
    regime_ma_sessions: int = 60
    breakout_sessions: int = 20
    volume_sessions: int = 20
    atr_sessions: int = 14
    rsi_sessions: int = 14
    adx_sessions: int = 14
    slow_trend_sessions: int = 120
    long_trend_sessions: int = 200
    long_breakout_sessions: int = 55
    bollinger_sessions: int = 20
    bollinger_deviations: Decimal = Decimal("2")
    stochastic_sessions: int = 14
    money_flow_sessions: int = 20
    drawdown_sessions: int = 60
    volume_confirmation_ratio: Decimal = Decimal("1.20")
    near_breakout_fraction: Decimal = Decimal("0.02")
    atr_invalidation_multiple: Decimal = Decimal("2")
    max_previous_close_discontinuity: Decimal = Decimal("0.02")
    max_calendar_gap_days: int = 14
    same_day_bar_available_after: time = time(15, 5)
    session_open_time: time = time(9, 30)
    strategy_version: str = "close-multifactor@2"

    def __post_init__(self) -> None:
        if not (
            1 < self.short_ma_sessions
            < self.trend_ma_sessions
            < self.regime_ma_sessions
        ):
            raise ValueError(
                "moving-average sessions must be ordered: 1 < short < trend < regime"
            )
        if min(
            self.breakout_sessions,
            self.volume_sessions,
            self.atr_sessions,
            self.rsi_sessions,
            self.adx_sessions,
            self.slow_trend_sessions,
            self.long_trend_sessions,
            self.long_breakout_sessions,
            self.bollinger_sessions,
            self.stochastic_sessions,
            self.money_flow_sessions,
            self.drawdown_sessions,
        ) <= 1:
            raise ValueError("indicator lookbacks must exceed one session")
        if not (
            self.regime_ma_sessions
            < self.slow_trend_sessions
            < self.long_trend_sessions
        ):
            raise ValueError("long moving-average sessions must exceed regime sessions")
        if self.bollinger_deviations <= 0:
            raise ValueError("bollinger_deviations must be positive")
        if self.volume_confirmation_ratio <= 0:
            raise ValueError("volume_confirmation_ratio must be positive")
        if not Decimal("0") <= self.near_breakout_fraction < Decimal("1"):
            raise ValueError("near_breakout_fraction must be in [0, 1)")
        if self.atr_invalidation_multiple <= 0:
            raise ValueError("atr_invalidation_multiple must be positive")
        if not Decimal("0") <= self.max_previous_close_discontinuity < Decimal("1"):
            raise ValueError("max_previous_close_discontinuity must be in [0, 1)")
        if self.max_calendar_gap_days <= 0:
            raise ValueError("max_calendar_gap_days must be positive")
        if not self.strategy_version.strip():
            raise ValueError("strategy_version must not be empty")

    @property
    def minimum_history(self) -> int:
        """使用全部已配置指标所需的交易行情柱数量。"""

        return max(
            201,
            self.regime_ma_sessions,
            self.slow_trend_sessions,
            self.long_trend_sessions + 1,
            self.breakout_sessions + 1,
            self.long_breakout_sessions + 1,
            self.volume_sessions + 1,
            self.atr_sessions + 1,
            self.rsi_sessions + 1,
            self.adx_sessions * 2,
            self.bollinger_sessions,
            self.stochastic_sessions,
            self.money_flow_sessions + 1,
            self.drawdown_sessions,
        )


@dataclass(frozen=True, slots=True)
class CloseTechnicalAssessment:
    """面向一个未来交易日、可审计且不可执行的技术视图。"""

    symbol: str
    as_of: datetime
    next_session: date
    latest_trade_date: date | None
    horizon: RecommendationHorizon
    decision: RecommendationDecision
    score: Decimal
    reference_price: Decimal | None
    invalidation_price: Decimal | None
    reason_codes: tuple[str, ...]
    trading_sessions_used: int
    strategy_version: str
    metrics: tuple[tuple[str, Decimal], ...]
    signal_families: tuple[CloseSignalFamily, ...] = ()
    horizon_views: tuple[CloseHorizonView, ...] = ()


@dataclass(frozen=True, slots=True)
class CloseSignalFamily:
    """一个设有上限并独立保存以避免重复计算的信号族。"""

    family_id: str
    score: Decimal
    weight: Decimal
    contribution: Decimal
    summary: str
    metrics: tuple[str, ...]
    status: CloseSignalFamilyStatus = CloseSignalFamilyStatus.ACTIVE

    def __post_init__(self) -> None:
        if not self.family_id.strip() or not self.summary.strip() or not self.metrics:
            raise ValueError("signal family text and metrics must not be empty")
        if not Decimal("-1") <= self.score <= Decimal("1"):
            raise ValueError("signal family score must be in [-1, 1]")
        if not Decimal("0") <= self.weight <= Decimal("1"):
            raise ValueError("signal family weight must be in [0, 1]")
        if self.contribution != self.score * self.weight:
            raise ValueError("signal family contribution must equal score times weight")
        if self.status is CloseSignalFamilyStatus.UNAVAILABLE and (
            self.score != 0 or self.weight != 0 or self.contribution != 0
        ):
            raise ValueError("unavailable signal family must not affect the score")
        if self.status in {
            CloseSignalFamilyStatus.NEUTRAL,
            CloseSignalFamilyStatus.INACTIVE,
        } and self.score != 0:
            raise ValueError("neutral or inactive signal family score must be zero")


@dataclass(frozen=True, slots=True)
class CloseHorizonView:
    """对已计算信号族重新加权后的诊断视图。

    ``score`` 是有界方向诊断值而非概率。它刻意不携带可执行决定，防止两个期限意外
    创建两张冲突订单。
    """

    horizon: CloseDiagnosticHorizon
    score: Decimal
    family_contributions: tuple[tuple[str, Decimal], ...]
    coverage: Decimal
    summary: str

    def __post_init__(self) -> None:
        if not Decimal("-1") <= self.score <= Decimal("1"):
            raise ValueError("horizon view score must be in [-1, 1]")
        if not Decimal(0) <= self.coverage <= Decimal(1):
            raise ValueError("horizon view coverage must be in [0, 1]")
        identifiers = tuple(item[0] for item in self.family_contributions)
        if any(not item.strip() for item in identifiers):
            raise ValueError("horizon view family IDs must not be empty")
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("horizon view family IDs must be unique")
        if any(not value.is_finite() for _, value in self.family_contributions):
            raise ValueError("horizon view contributions must be finite")
        if sum(
            (value for _, value in self.family_contributions), Decimal(0)
        ) != self.score:
            raise ValueError("horizon view contributions must sum to score")
        if not self.summary.strip():
            raise ValueError("horizon view summary must not be empty")


def build_close_technical_assessment(
    symbol: str,
    bars: tuple[DailyBar, ...],
    *,
    as_of: datetime,
    latest_completed_session: date,
    next_session: date,
    is_currently_held: bool = False,
    instrument_type: CloseInstrumentType = CloseInstrumentType.STOCK,
    config: CloseAnalysisConfig | None = None,
) -> CloseTechnicalAssessment:
    """仅使用截至 ``as_of`` 已完成的日线柱评估下一交易日。

    调用方从交易日历提供两个交易日边界。本函数绝不猜测工作日或节假日，并拒绝闭区间
    之外的任何行情柱，使回放与实时执行共享相同的时点边界。
    """

    canonical_symbol = symbol.strip().upper()
    if not canonical_symbol:
        raise ValueError("symbol must not be empty")
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")

    resolved = config or CloseAnalysisConfig()
    local_as_of = as_of.astimezone(SHANGHAI)
    if latest_completed_session >= next_session:
        raise ValueError("latest_completed_session must precede next_session")
    if latest_completed_session > local_as_of.date():
        raise ValueError("latest_completed_session was not completed at as_of")
    if (
        latest_completed_session == local_as_of.date()
        and local_as_of.time().replace(tzinfo=None) < resolved.same_day_bar_available_after
    ):
        raise ValueError("latest_completed_session was not completed at as_of")
    if next_session < local_as_of.date():
        raise ValueError("next_session must not be before the Shanghai as_of date")
    if (
        next_session == local_as_of.date()
        and local_as_of.time().replace(tzinfo=None) >= resolved.session_open_time
    ):
        raise ValueError("next_session has already opened at as_of")

    trade_dates = [bar.trade_date for bar in bars]
    if trade_dates != sorted(trade_dates) or len(trade_dates) != len(set(trade_dates)):
        raise ValueError("daily bars must be strictly ordered and unique")

    for bar in bars:
        _validate_daily_bar(
            bar,
            symbol=canonical_symbol,
            local_as_of=local_as_of,
            latest_completed_session=latest_completed_session,
            next_session=next_session,
            config=resolved,
        )

    trading_bars = tuple(bar for bar in bars if bar.is_trading)
    if not bars or bars[-1].trade_date != latest_completed_session:
        return _abstain(
            canonical_symbol,
            as_of,
            next_session,
            resolved,
            "LATEST_COMPLETED_SESSION_MISSING",
            latest_trade_date=trading_bars[-1].trade_date if trading_bars else None,
            trading_sessions_used=len(trading_bars),
        )
    if not bars[-1].is_trading:
        return _abstain(
            canonical_symbol,
            as_of,
            next_session,
            resolved,
            "LATEST_SESSION_NOT_TRADING",
            latest_trade_date=trading_bars[-1].trade_date if trading_bars else None,
            trading_sessions_used=len(trading_bars),
        )
    if len(trading_bars) < resolved.minimum_history:
        return _abstain(
            canonical_symbol,
            as_of,
            next_session,
            resolved,
            "INSUFFICIENT_DAILY_HISTORY",
            latest_trade_date=trading_bars[-1].trade_date if trading_bars else None,
            trading_sessions_used=len(trading_bars),
        )

    latest = trading_bars[-1]
    if (next_session - latest.trade_date).days > resolved.max_calendar_gap_days:
        return _abstain(
            canonical_symbol,
            as_of,
            next_session,
            resolved,
            "STALE_DAILY_MARKET_DATA",
            latest_trade_date=latest.trade_date,
            trading_sessions_used=len(trading_bars),
        )

    discontinuity = _largest_previous_close_discontinuity(
        trading_bars[-resolved.minimum_history :]
    )
    if discontinuity > resolved.max_previous_close_discontinuity:
        return _abstain(
            canonical_symbol,
            as_of,
            next_session,
            resolved,
            "UNADJUSTED_PRICE_DISCONTINUITY",
            latest_trade_date=latest.trade_date,
            trading_sessions_used=len(trading_bars),
            metrics=(("max_previous_close_discontinuity", discontinuity),),
        )

    closes = tuple(_required_price(bar.close) for bar in trading_bars)
    short_ma = _mean(closes[-resolved.short_ma_sessions :])
    trend_ma = _mean(closes[-resolved.trend_ma_sessions :])
    regime_ma = _mean(closes[-resolved.regime_ma_sessions :])
    slow_ma = _mean(closes[-resolved.slow_trend_sessions :])
    long_ma = _mean(closes[-resolved.long_trend_sessions :])
    prior_breakout_window = trading_bars[-(resolved.breakout_sessions + 1) : -1]
    breakout_level = max(_required_price(bar.high) for bar in prior_breakout_window)
    support_level = min(_required_price(bar.low) for bar in prior_breakout_window)
    long_breakout_window = trading_bars[-(resolved.long_breakout_sessions + 1) : -1]
    breakout_level_55 = max(_required_price(bar.high) for bar in long_breakout_window)
    support_level_55 = min(_required_price(bar.low) for bar in long_breakout_window)
    prior_volume_window = trading_bars[-(resolved.volume_sessions + 1) : -1]
    average_volume = _mean(tuple(Decimal(bar.volume) for bar in prior_volume_window))
    volume_ratio = Decimal(latest.volume) / average_volume if average_volume > 0 else Decimal(0)
    atr = _average_true_range(trading_bars, resolved.atr_sessions)
    rsi = _relative_strength_index(closes, resolved.rsi_sessions)
    adx, positive_di, negative_di = _wilder_adx(
        trading_bars,
        resolved.adx_sessions,
    )
    return_5d = _period_return(closes, 5)
    return_20d = _period_return(closes, 20)
    last_day_return = _last_day_return(trading_bars)
    annualized_volatility = _annualized_volatility(closes, 20)
    annualized_volatility_60 = _annualized_volatility(closes, 60)
    latest_close = _required_price(latest.close)
    atr_fraction = atr / latest_close

    ema_12 = _ema(closes, 12)
    ema_26 = _ema(closes, 26)
    macd_line = ema_12 - ema_26
    macd_signal = _ema(_macd_series(closes, 12, 26), 9)
    macd_histogram = macd_line - macd_signal
    bollinger_mean = _mean(closes[-resolved.bollinger_sessions :])
    bollinger_std = _standard_deviation(closes[-resolved.bollinger_sessions :])
    bollinger_upper = bollinger_mean + resolved.bollinger_deviations * bollinger_std
    bollinger_lower = bollinger_mean - resolved.bollinger_deviations * bollinger_std
    bollinger_bandwidth = (
        (bollinger_upper - bollinger_lower) / bollinger_mean
        if bollinger_mean > 0
        else Decimal(0)
    )
    prior_bollinger_bandwidths = _bollinger_bandwidth_series(
        closes[:-1],
        resolved.bollinger_sessions,
    )[-20:]
    bollinger_bandwidth_state = (
        bollinger_bandwidth / _mean(prior_bollinger_bandwidths)
        if prior_bollinger_bandwidths
        and _mean(prior_bollinger_bandwidths) > 0
        else Decimal(1)
    )
    bollinger_percent_b = (
        (latest_close - bollinger_lower) / (bollinger_upper - bollinger_lower)
        if bollinger_upper > bollinger_lower
        else Decimal("0.5")
    )
    stochastic_k = _stochastic_k(trading_bars, resolved.stochastic_sessions)
    money_flow_ratio = _money_flow_ratio(trading_bars, resolved.money_flow_sessions)
    amihud_20, amihud_coverage_20 = _amihud_illiquidity_with_coverage(
        trading_bars,
        20,
    )
    amihud_60, amihud_coverage_60 = _amihud_illiquidity_with_coverage(
        trading_bars,
        60,
    )
    illiquidity_state = amihud_20 / amihud_60 if amihud_60 > 0 else Decimal(1)
    turnover_state, turnover_coverage = _turnover_state_with_coverage(
        trading_bars,
        20,
    )
    max_drawdown_60 = _max_drawdown(closes[-resolved.drawdown_sessions :])
    downside_volatility_20 = _downside_volatility(closes, 20)
    overnight_gap_volatility_20 = _overnight_gap_volatility(trading_bars, 20)
    trend_slope_20 = _normalized_slope(closes, 20, atr)
    trend_slope_60 = _normalized_slope(closes, 60, atr)
    return_60d = _period_return(closes, 60)
    return_120_skip_5d = closes[-6] / closes[-126] - Decimal(1)
    return_200d = _period_return(closes, 200)

    close_above_trend = latest_close > trend_ma
    short_above_trend = short_ma > trend_ma
    trend_above_regime = trend_ma > regime_ma
    breakout = latest_close > breakout_level
    breakdown = latest_close < support_level
    near_breakout = latest_close >= breakout_level * (
        Decimal(1) - resolved.near_breakout_fraction
    )
    volume_confirmed = volume_ratio >= resolved.volume_confirmation_ratio
    reasons = _audit_reasons(
        close_above_trend=close_above_trend,
        short_above_trend=short_above_trend,
        trend_above_regime=trend_above_regime,
        breakout=breakout,
        breakdown=breakdown,
        near_breakout=near_breakout,
        volume_confirmed=volume_confirmed,
        rsi=rsi,
        return_20d=return_20d,
    )

    trend_score = _mean(
        (
            _sign(latest_close - trend_ma),
            _sign(trend_ma - regime_ma),
            _sign(regime_ma - slow_ma),
            _sign(slow_ma - long_ma),
            _clamp_unit(trend_slope_20 / Decimal("0.20")),
            _clamp_unit(trend_slope_60 / Decimal("0.20")),
        )
    )
    momentum_score = _mean(
        (
            _risk_adjusted_return(return_5d, annualized_volatility, 5),
            _risk_adjusted_return(return_20d, annualized_volatility, 20),
            _risk_adjusted_return(return_60d, annualized_volatility_60, 60),
            _risk_adjusted_return(
                return_120_skip_5d,
                annualized_volatility_60,
                120,
            ),
            _risk_adjusted_return(return_200d, annualized_volatility_60, 200),
            _clamp_unit(macd_histogram / max(atr, Decimal("0.000001"))),
        )
    )
    channel_position = _mean(
        (
            _level_score(latest_close, breakout_level, support_level),
            _level_score(latest_close, breakout_level_55, support_level_55),
        )
    )
    compression_release = _sign(channel_position) * max(
        Decimal(0),
        _clamp_unit(
            (bollinger_bandwidth_state - Decimal(1)) / Decimal("0.50")
        ),
    )
    breakout_score = _mean(
        (
            channel_position,
            compression_release,
        )
    )
    trend_regime = trend_ma > regime_ma and latest_close > trend_ma
    pullback_score = _pullback_score(
        latest_close=latest_close,
        trend_ma=trend_ma,
        atr=atr,
        rsi=rsi,
        stochastic_k=stochastic_k,
        percent_b=bollinger_percent_b,
        trend_regime=trend_regime,
    )
    directional_volume_confirmation = _sign(last_day_return) * max(
        Decimal(0),
        _clamp_unit((volume_ratio - Decimal(1)) / Decimal("0.75")),
    )
    volume_score = _mean(
        (
            directional_volume_confirmation,
            _clamp_unit((money_flow_ratio - Decimal(1)) / Decimal("0.75")),
        )
    )
    families = (
        _signal_family(
            "trend_structure",
            trend_score,
            Decimal("0.30"),
            "长期均线结构、价格位置与ATR归一化斜率",
            (
                "ma_20",
                "ma_60",
                "ma_120",
                "ma_200",
                "trend_slope_atr_20",
                "trend_slope_atr_60",
            ),
        ),
        _signal_family(
            "multi_horizon_momentum",
            momentum_score,
            Decimal("0.25"),
            "5/20/60/120/200日风险调整动量与MACD状态",
            (
                "return_5d",
                "return_20d",
                "return_60d",
                "return_120_skip_5d",
                "return_200d",
                "macd_histogram",
            ),
        ),
        _signal_family(
            "breakout_compression",
            breakout_score,
            Decimal("0.20"),
            "20/55日通道、布林位置与趋势释放",
            (
                "breakout_level_20",
                "support_level_20",
                "breakout_level_55",
                "support_level_55",
                "bollinger_bandwidth_state",
            ),
        ),
        _signal_family(
            "trend_pullback",
            pullback_score,
            Decimal("0.10"),
            "仅在上升趋势中识别回撤位置；逆势超卖不转为看多",
            ("rsi_14", "stochastic_k_14", "bollinger_percent_b"),
            status=(
                CloseSignalFamilyStatus.INACTIVE
                if not trend_regime and pullback_score == 0
                else None
            ),
        ),
        _signal_family(
            "volume_liquidity",
            volume_score,
            Decimal("0.15"),
            "放量方向确认与上涨/下跌成交额比；换手和Amihud仅进入风险覆盖",
            ("volume_ratio_20", "up_down_amount_ratio_20", "last_day_return"),
        ),
        _signal_family(
            "relative_strength_breadth",
            Decimal(0),
            Decimal(0),
            "当前数据包未包含同时间点基准、同类排名与成分股广度，仅展示为数据缺口",
            ("relative_strength_20", "breadth_above_ma20"),
            status=CloseSignalFamilyStatus.UNAVAILABLE,
        ),
    )
    horizon_views = _build_horizon_views(families)
    raw_directional_score = _clamp_score(
        sum((item.contribution for item in families), Decimal(0))
    )
    score = raw_directional_score

    if volume_ratio >= Decimal("1.50") and last_day_return < 0:
        reasons.append("HIGH_VOLUME_DOWN_DAY")
    if last_day_return >= Decimal("0.095"):
        reasons.append("LARGE_ONE_DAY_ADVANCE")
    if atr_fraction >= Decimal("0.05"):
        reasons.append("ELEVATED_ATR_RISK")
    if latest.is_st and instrument_type is CloseInstrumentType.STOCK:
        reasons.append("ST_RISK_FLAG")
    elif latest.is_st:
        reasons.append("PROVIDER_ST_FLAG_IGNORED_FOR_ETF")

    elevated_risk = (
        atr_fraction >= Decimal("0.05")
        or max_drawdown_60 >= Decimal("0.20")
        or overnight_gap_volatility_20 >= Decimal("0.25")
    )
    if elevated_risk:
        reasons.append("RISK_GATE_ACTIVE")
    reduce_trigger = breakdown or not trend_above_regime or (
        not close_above_trend and not short_above_trend
    )
    entry_setup = (
        close_above_trend
        and short_above_trend
        and trend_above_regime
        and breakout
        and volume_confirmed
        and rsi <= Decimal("85")
        and last_day_return < Decimal("0.095")
        and not elevated_risk
        and (instrument_type is CloseInstrumentType.ETF or not latest.is_st)
        and score >= Decimal("0.55")
    )

    if is_currently_held and reduce_trigger:
        decision = RecommendationDecision.REDUCE
        reasons.append("DAILY_TREND_EXIT_TRIGGER")
    elif entry_setup:
        decision = RecommendationDecision.ENTER_CANDIDATE
        reasons.append("MULTIFACTOR_ENTRY_SETUP")
    else:
        decision = RecommendationDecision.WATCH

    invalidation_price = _invalidation_price(
        latest_close,
        trend_ma,
        atr,
        resolved.atr_invalidation_multiple,
        decision,
    )
    metrics = (
        ("close", latest_close),
        ("ma_5", short_ma),
        ("ma_20", trend_ma),
        ("ma_60", regime_ma),
        ("ma_120", slow_ma),
        ("ma_200", long_ma),
        ("breakout_level_20", breakout_level),
        ("support_level_20", support_level),
        ("breakout_level_55", breakout_level_55),
        ("support_level_55", support_level_55),
        ("volume_ratio_20", volume_ratio),
        ("rsi_14", rsi),
        ("adx_14", adx),
        ("positive_di_14", positive_di),
        ("negative_di_14", negative_di),
        ("stochastic_k_14", stochastic_k),
        ("atr_14", atr),
        ("atr_fraction", atr_fraction),
        ("macd_line", macd_line),
        ("macd_signal", macd_signal),
        ("macd_histogram", macd_histogram),
        ("bollinger_percent_b", bollinger_percent_b),
        ("bollinger_bandwidth", bollinger_bandwidth),
        ("bollinger_bandwidth_state", bollinger_bandwidth_state),
        ("up_down_amount_ratio_20", money_flow_ratio),
        ("turnover_state_20", turnover_state),
        ("turnover_coverage_20", turnover_coverage),
        ("amihud_20", amihud_20),
        ("amihud_60", amihud_60),
        (
            "amihud_bps_per_cny_billion_20",
            _amihud_bps_per_cny_billion(amihud_20),
        ),
        (
            "amihud_bps_per_cny_billion_60",
            _amihud_bps_per_cny_billion(amihud_60),
        ),
        ("amihud_coverage_20", amihud_coverage_20),
        ("amihud_coverage_60", amihud_coverage_60),
        ("illiquidity_state_20_60", illiquidity_state),
        ("trend_pullback_regime_active", Decimal(1) if trend_regime else Decimal(0)),
        ("return_5d", return_5d),
        ("return_20d", return_20d),
        ("return_60d", return_60d),
        ("return_120_skip_5d", return_120_skip_5d),
        ("return_200d", return_200d),
        ("last_day_return", last_day_return),
        ("annualized_volatility_20", annualized_volatility),
        ("annualized_volatility_60", annualized_volatility_60),
        ("downside_volatility_20", downside_volatility_20),
        ("overnight_gap_volatility_20", overnight_gap_volatility_20),
        ("max_drawdown_60", max_drawdown_60),
        ("trend_slope_atr_20", trend_slope_20),
        ("trend_slope_atr_60", trend_slope_60),
        ("raw_directional_score", raw_directional_score),
        ("risk_gate_active", Decimal(1) if elevated_risk else Decimal(0)),
        *(
            (f"family_score_{item.family_id}", item.score)
            for item in families
        ),
    )
    return CloseTechnicalAssessment(
        symbol=canonical_symbol,
        as_of=as_of,
        next_session=next_session,
        latest_trade_date=latest.trade_date,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        decision=decision,
        score=score,
        reference_price=latest_close,
        invalidation_price=invalidation_price,
        reason_codes=tuple(reasons),
        trading_sessions_used=len(trading_bars),
        strategy_version=resolved.strategy_version,
        metrics=metrics,
        signal_families=families,
        horizon_views=horizon_views,
    )


def _validate_daily_bar(
    bar: DailyBar,
    *,
    symbol: str,
    local_as_of: datetime,
    latest_completed_session: date,
    next_session: date,
    config: CloseAnalysisConfig,
) -> None:
    if bar.symbol.strip().upper() != symbol:
        raise ValueError("daily bar symbol does not match requested symbol")
    if bar.adjustment is not PriceAdjustment.NONE:
        raise ValueError("close analysis requires unadjusted daily bars")
    if bar.trade_date >= next_session:
        raise ValueError("daily bar is from next_session or the future")
    if bar.trade_date > latest_completed_session:
        raise ValueError("daily bar is after latest_completed_session")
    if bar.trade_date > local_as_of.date():
        raise ValueError("daily bar was not available at as_of")
    if (
        bar.trade_date == local_as_of.date()
        and local_as_of.time().replace(tzinfo=None) < config.same_day_bar_available_after
    ):
        raise ValueError("same-day daily bar is not completed at as_of")
    if bar.volume < 0 or not bar.amount.is_finite() or bar.amount < 0:
        raise ValueError("daily bar volume and amount must not be negative")
    if bar.turnover_percent is not None and (
        not bar.turnover_percent.is_finite() or bar.turnover_percent < 0
    ):
        raise ValueError("daily bar turnover_percent must be finite and non-negative")
    if bar.previous_close is not None and (
        not bar.previous_close.is_finite() or bar.previous_close <= 0
    ):
        raise ValueError("daily bar previous_close must be finite and positive")
    if not bar.is_trading:
        return
    if None in (bar.open, bar.high, bar.low, bar.close):
        raise ValueError("trading daily bars require complete OHLC prices")
    open_price = _required_price(bar.open)
    high = _required_price(bar.high)
    low = _required_price(bar.low)
    close = _required_price(bar.close)
    if high < max(open_price, close) or low > min(open_price, close) or high < low:
        raise ValueError("daily bar OHLC values are inconsistent")


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
        current - previous
        for previous, current in zip(window, window[1:], strict=False)
    )
    average_gain = _mean(tuple(max(change, Decimal(0)) for change in changes))
    average_loss = _mean(tuple(max(-change, Decimal(0)) for change in changes))
    if average_loss == 0:
        return Decimal(100) if average_gain > 0 else Decimal(50)
    relative_strength = average_gain / average_loss
    return Decimal(100) - Decimal(100) / (Decimal(1) + relative_strength)


def _wilder_adx(
    bars: tuple[DailyBar, ...], lookback: int
) -> tuple[Decimal, Decimal, Decimal]:
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
            upward_move
            if upward_move > downward_move and upward_move > 0
            else Decimal(0)
        )
        negative_movements.append(
            downward_move
            if downward_move > upward_move and downward_move > 0
            else Decimal(0)
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
            smoothed_positive
            - smoothed_positive / Decimal(lookback)
            + positive_movement
        )
        smoothed_negative = (
            smoothed_negative
            - smoothed_negative / Decimal(lookback)
            + negative_movement
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


def _bollinger_bandwidth_series(
    closes: tuple[Decimal, ...], sessions: int
) -> tuple[Decimal, ...]:
    output: list[Decimal] = []
    for end in range(sessions, len(closes) + 1):
        window = closes[end - sessions : end]
        center = _mean(window)
        deviation = _standard_deviation(window)
        output.append(
            Decimal(0) if center <= 0 else Decimal(4) * deviation / center
        )
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


def _normalized_slope(
    closes: tuple[Decimal, ...], sessions: int, atr: Decimal
) -> Decimal:
    window = closes[-sessions:]
    x_mean = Decimal(sessions - 1) / Decimal(2)
    y_mean = _mean(window)
    numerator = sum(
        (
            (Decimal(index) - x_mean) * (value - y_mean)
            for index, value in enumerate(window)
        ),
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
    horizon_volatility = annualized_volatility * (
        Decimal(sessions) / Decimal(252)
    ).sqrt()
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


def _signal_family(
    family_id: str,
    score: Decimal,
    weight: Decimal,
    summary: str,
    metrics: tuple[str, ...],
    status: CloseSignalFamilyStatus | None = None,
) -> CloseSignalFamily:
    bounded = _clamp_unit(score)
    resolved_status = status or (
        CloseSignalFamilyStatus.NEUTRAL
        if bounded == 0
        else CloseSignalFamilyStatus.ACTIVE
    )
    return CloseSignalFamily(
        family_id=family_id,
        score=bounded,
        weight=weight,
        contribution=bounded * weight,
        summary=summary,
        metrics=metrics,
        status=resolved_status,
    )


def _build_horizon_views(
    families: tuple[CloseSignalFamily, ...],
) -> tuple[CloseHorizonView, ...]:
    """在不重新计算指标的情况下，为两种用途重新加权信号族评分。"""

    available = {
        family.family_id: family
        for family in families
        if family.weight > 0
    }
    definitions = (
        (
            CloseDiagnosticHorizon.SHORT_1_TO_5_DAYS,
            (
                ("trend_structure", Decimal("0.15")),
                ("multi_horizon_momentum", Decimal("0.20")),
                ("breakout_compression", Decimal("0.25")),
                ("trend_pullback", Decimal("0.20")),
                ("volume_liquidity", Decimal("0.20")),
            ),
            "用于1至5个交易日的节奏、突破确认与入场位置诊断；评分不是概率，且不生成第二套交易结论",
        ),
        (
            CloseDiagnosticHorizon.SWING_2_TO_8_WEEKS,
            (
                ("trend_structure", Decimal("0.35")),
                ("multi_horizon_momentum", Decimal("0.30")),
                ("breakout_compression", Decimal("0.20")),
                ("trend_pullback", Decimal("0.05")),
                ("volume_liquidity", Decimal("0.10")),
            ),
            "使用日线长周期结构诊断2至8周波段；当前不是周K确认，评分不是概率，且不生成第二套交易结论",
        ),
    )
    views: list[CloseHorizonView] = []
    for horizon, configured_weights, summary in definitions:
        configured_total = sum(
            (weight for _, weight in configured_weights), Decimal(0)
        )
        eligible = tuple(
            (family_id, weight)
            for family_id, weight in configured_weights
            if weight > 0 and family_id in available
        )
        eligible_total = sum((weight for _, weight in eligible), Decimal(0))
        contributions = (
            tuple(
                (
                    family_id,
                    available[family_id].score * weight / eligible_total,
                )
                for family_id, weight in eligible
            )
            if eligible_total > 0
            else ()
        )
        score = sum((value for _, value in contributions), Decimal(0))
        coverage = (
            eligible_total / configured_total
            if configured_total > 0
            else Decimal(0)
        )
        views.append(
            CloseHorizonView(
                horizon=horizon,
                score=score,
                family_contributions=contributions,
                coverage=coverage,
                summary=summary,
            )
        )
    return tuple(views)


def _audit_reasons(
    *,
    close_above_trend: bool,
    short_above_trend: bool,
    trend_above_regime: bool,
    breakout: bool,
    breakdown: bool,
    near_breakout: bool,
    volume_confirmed: bool,
    rsi: Decimal,
    return_20d: Decimal,
) -> list[str]:
    reasons = [
        "CLOSE_ABOVE_MA20" if close_above_trend else "CLOSE_NOT_ABOVE_MA20",
        "MA5_ABOVE_MA20" if short_above_trend else "MA5_NOT_ABOVE_MA20",
        "MA20_ABOVE_MA60" if trend_above_regime else "MA20_NOT_ABOVE_MA60",
    ]
    if breakout:
        reasons.extend(
            (
                "TWENTY_DAY_CLOSE_BREAKOUT",
                "BREAKOUT_VOLUME_CONFIRMED"
                if volume_confirmed
                else "BREAKOUT_WITHOUT_VOLUME_CONFIRMATION",
            )
        )
    elif breakdown:
        reasons.append("TWENTY_DAY_CLOSE_BREAKDOWN")
    elif near_breakout:
        reasons.append("NEAR_TWENTY_DAY_BREAKOUT")
    if Decimal(52) <= rsi <= Decimal(72):
        reasons.append("RSI_POSITIVE_NOT_EXTREME")
    elif rsi > Decimal(80):
        reasons.append("RSI_OVERBOUGHT")
    elif rsi < Decimal(40):
        reasons.append("RSI_WEAK")
    reasons.append(
        "POSITIVE_TWENTY_DAY_RETURN"
        if return_20d > 0
        else "NON_POSITIVE_TWENTY_DAY_RETURN"
    )
    return reasons


def _largest_previous_close_discontinuity(bars: tuple[DailyBar, ...]) -> Decimal:
    largest = Decimal(0)
    for previous, current in zip(bars, bars[1:], strict=False):
        if current.previous_close is None:
            return Decimal(1)
        previous_close = _required_price(previous.close)
        difference = abs(current.previous_close / previous_close - Decimal(1))
        largest = max(largest, difference)
    return largest


def _binary_component(
    condition: bool,
    *,
    positive: Decimal,
    negative: Decimal,
    reasons: list[str],
    positive_reason: str,
    negative_reason: str,
) -> Decimal:
    reasons.append(positive_reason if condition else negative_reason)
    return positive if condition else negative


def _clamp_score(score: Decimal) -> Decimal:
    return max(Decimal(-1), min(Decimal(1), score))


def _invalidation_price(
    close: Decimal,
    trend_ma: Decimal,
    atr: Decimal,
    atr_multiple: Decimal,
    decision: RecommendationDecision,
) -> Decimal | None:
    if decision is RecommendationDecision.REDUCE:
        return None
    atr_floor = close - atr * atr_multiple
    structure_floor = trend_ma - atr * Decimal("0.5")
    candidate = max(atr_floor, structure_floor)
    return candidate if Decimal(0) < candidate < close else None


def _abstain(
    symbol: str,
    as_of: datetime,
    next_session: date,
    config: CloseAnalysisConfig,
    reason: str,
    *,
    latest_trade_date: date | None,
    trading_sessions_used: int,
    metrics: tuple[tuple[str, Decimal], ...] = (),
) -> CloseTechnicalAssessment:
    return CloseTechnicalAssessment(
        symbol=symbol,
        as_of=as_of,
        next_session=next_session,
        latest_trade_date=latest_trade_date,
        horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
        decision=RecommendationDecision.ABSTAIN,
        score=Decimal(0),
        reference_price=None,
        invalidation_price=None,
        reason_codes=(reason,),
        trading_sessions_used=trading_sessions_used,
        strategy_version=config.strategy_version,
        metrics=metrics,
    )
