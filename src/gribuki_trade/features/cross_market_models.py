"""跨市场关系计算的输入、输出与风险标签模型。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal, TypeAlias

Numeric: TypeAlias = Decimal | float



MINIMUM_COMMON_SAMPLES = 120
METHODOLOGY_VERSION = "cross-market-relations@1"
NON_CAUSALITY_NOTICE = (
    "Cross-market correlations and regressions are descriptive associations, not evidence "
    "of causation, predictability, or an executable trading edge."
)


class CrossMarketRiskDirection(StrEnum):
    """因子序列正收益的含义。"""

    POSITIVE_IS_RISK_ON = "POSITIVE_IS_RISK_ON"
    POSITIVE_IS_RISK_OFF = "POSITIVE_IS_RISK_OFF"


class CrossMarketRiskAlignment(StrEnum):
    """应用因子风险方向约定后的目标敏感度。"""

    RISK_ON_SENSITIVE = "RISK_ON_SENSITIVE"
    RISK_OFF_SENSITIVE = "RISK_OFF_SENSITIVE"
    UNSTABLE_OR_NEUTRAL = "UNSTABLE_OR_NEUTRAL"


class CrossMarketCorrelationSign(StrEnum):
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"
    NEUTRAL = "NEUTRAL"
    UNAVAILABLE = "UNAVAILABLE"


class CrossMarketSignRegime(StrEnum):
    STABLE_POSITIVE = "STABLE_POSITIVE"
    STABLE_NEGATIVE = "STABLE_NEGATIVE"
    STABLE_NEUTRAL = "STABLE_NEUTRAL"
    MIXED = "MIXED"
    UNAVAILABLE = "UNAVAILABLE"


class CrossMarketRelationFailureReason(StrEnum):
    """计算无效或不完整时稳定且机器可读的原因。"""

    EMPTY_TARGET_SYMBOL = "EMPTY_TARGET_SYMBOL"
    EMPTY_TARGET_SERIES = "EMPTY_TARGET_SERIES"
    EMPTY_FACTOR_ID = "EMPTY_FACTOR_ID"
    DUPLICATE_FACTOR_ID = "DUPLICATE_FACTOR_ID"
    INVALID_RISK_DIRECTION = "INVALID_RISK_DIRECTION"
    INVALID_NUMERIC_VALUE = "INVALID_NUMERIC_VALUE"
    NON_FINITE_VALUE = "NON_FINITE_VALUE"
    NON_POSITIVE_CLOSE = "NON_POSITIVE_CLOSE"
    NAIVE_TIMESTAMP = "NAIVE_TIMESTAMP"
    TARGET_DATES_NOT_STRICTLY_ORDERED = "TARGET_DATES_NOT_STRICTLY_ORDERED"
    TARGET_DECISIONS_NOT_STRICTLY_ORDERED = "TARGET_DECISIONS_NOT_STRICTLY_ORDERED"
    FACTOR_DATES_NOT_STRICTLY_ORDERED = "FACTOR_DATES_NOT_STRICTLY_ORDERED"
    UNKNOWN_TARGET_DATE = "UNKNOWN_TARGET_DATE"
    FUTURE_AVAILABLE_AT = "FUTURE_AVAILABLE_AT"
    INSUFFICIENT_COMMON_SAMPLES = "INSUFFICIENT_COMMON_SAMPLES"
    ZERO_FACTOR_VARIANCE = "ZERO_FACTOR_VARIANCE"
    ZERO_TARGET_VARIANCE = "ZERO_TARGET_VARIANCE"
    UNDEFINED_CORRELATION = "UNDEFINED_CORRELATION"
    BETA_STANDARD_ERROR_UNAVAILABLE = "BETA_STANDARD_ERROR_UNAVAILABLE"


class CrossMarketRelationInputError(ValueError):
    """带稳定失败原因的无效时点关系输入。"""

    failure_reason: CrossMarketRelationFailureReason

    def __init__(
        self,
        failure_reason: CrossMarketRelationFailureReason,
        message: str,
    ) -> None:
        super().__init__(message)
        self.failure_reason = failure_reason


@dataclass(frozen=True, slots=True)
class TargetCloseObservation:
    """一次已完成 A 股收盘及其决策点时间戳。"""

    trade_date: date
    close: Numeric
    decision_at: datetime


@dataclass(frozen=True, slots=True)
class AlignedFactorReturn:
    """由调用方对齐到一个 A 股决策日期的因子收益。"""

    target_trade_date: date
    value: Numeric
    available_at: datetime


@dataclass(frozen=True, slots=True)
class CrossMarketFactorSeries:
    """一个有序、日期唯一的因子序列及其风险约定。"""

    factor_id: str
    risk_direction: CrossMarketRiskDirection
    observations: tuple[AlignedFactorReturn, ...]


@dataclass(frozen=True, slots=True)
class CrossMarketLagRelation:
    """一个明确定义因子滞后的最新描述性指标。

    ``coverage`` 是共同观测数除以合格目标收益数。``correlation_sign_stability``
    是 20/60/120 个观测窗口 Pearson 相关符号的众数频率（1 表示三者符号一致）。
    任一窗口相关未定义时，该值不可用。
    """

    lag: Literal[0, 1]
    eligible_target_samples: int
    common_samples: int
    coverage: float
    first_common_date: date | None
    last_common_date: date | None
    ewma_correlation_half_life_20: float | None
    ewma_correlation_half_life_60: float | None
    correlation_20: float | None
    correlation_60: float | None
    correlation_120: float | None
    correlation_sign_20: CrossMarketCorrelationSign
    correlation_sign_60: CrossMarketCorrelationSign
    correlation_sign_120: CrossMarketCorrelationSign
    correlation_sign_stability: float | None
    sign_regime: CrossMarketSignRegime
    alpha_120: float | None
    beta_120: float | None
    beta_t_stat_120: float | None
    risk_alignment: CrossMarketRiskAlignment
    failure_reason: CrossMarketRelationFailureReason | None


@dataclass(frozen=True, slots=True)
class CrossMarketFactorRelation:
    factor_id: str
    risk_direction: CrossMarketRiskDirection
    lag_0: CrossMarketLagRelation
    lag_1: CrossMarketLagRelation


@dataclass(frozen=True, slots=True)
class CrossMarketRelationsReport:
    target_symbol: str
    as_of_trade_date: date
    target_close_samples: int
    minimum_common_samples: int
    factors: tuple[CrossMarketFactorRelation, ...]
    methodology_version: str
    non_causality_notice: str


@dataclass(frozen=True, slots=True)
class _TargetReturn:
    target_index: int
    trade_date: date
    previous_trade_date: date
    value: float


@dataclass(frozen=True, slots=True)
class _Pair:
    target_index: int
    trade_date: date
    target_return: float
    factor_return: float

