"""收盘多因子分析的配置与不可变结果模型。

这些值对象不读取行情、不执行订单；指标计算和完整评估流程仍由
``close_analysis`` 门面负责。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import StrEnum

from gribuki_trade.domain.recommendations import (
    RecommendationDecision,
    RecommendationHorizon,
)


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
        if not (1 < self.short_ma_sessions < self.trend_ma_sessions < self.regime_ma_sessions):
            raise ValueError("moving-average sessions must be ordered: 1 < short < trend < regime")
        if (
            min(
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
            )
            <= 1
        ):
            raise ValueError("indicator lookbacks must exceed one session")
        if not (self.regime_ma_sessions < self.slow_trend_sessions < self.long_trend_sessions):
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
        if (
            self.status
            in {
                CloseSignalFamilyStatus.NEUTRAL,
                CloseSignalFamilyStatus.INACTIVE,
            }
            and self.score != 0
        ):
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
        if sum((value for _, value in self.family_contributions), Decimal(0)) != self.score:
            raise ValueError("horizon view contributions must sum to score")
        if not self.summary.strip():
            raise ValueError("horizon view summary must not be empty")
