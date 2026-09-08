"""A 股筛选适配器的纯历史因子计算。

本模块不处理 provider、超时、调度或存储。筛选 facade 负责获取并校验 K 线，
本模块只把已经校验的未复权 K 线转换为确定性因子和审计告警。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from statistics import stdev

from gribuki_trade.ports.ashare_screening import (
    AShareFactorValue,
    ScreeningFactorId,
)

_CORPORATE_ACTION_TOLERANCE = Decimal("0.02")
_MIN_CORPORATE_ACTION_COVERAGE = Decimal("0.95")


@dataclass(frozen=True, slots=True)
class HistoryBar:
    """因子计算使用的已校验未复权日线。"""

    trade_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    previous_close: Decimal | None
    volume: Decimal
    amount: Decimal


def calculate_factors(bars: tuple[HistoryBar, ...]) -> tuple[AShareFactorValue, ...]:
    """根据足够长度的 K 线序列计算确定性的原始因子。

    动量使用收盘到收盘收益；120 日变体跳过最近五个交易日；波动率使用 60 个
    收益的年化样本标准差。缺失的流动性因子保持为 ``None``，不替换成中性值。
    """

    closes = tuple(item.close for item in bars)
    current = closes[-1]
    momentum_20 = current / closes[-21] - Decimal(1)
    momentum_60 = current / closes[-61] - Decimal(1)
    momentum_120_skip_5 = closes[-6] / closes[-126] - Decimal(1)
    ma20 = _mean(closes[-20:])
    ma60 = _mean(closes[-60:])
    prior20 = bars[-21:-1]
    prior_high = max(item.high for item in prior20)
    breakout_position = current / prior_high - Decimal(1)
    prior_volume = _mean(tuple(item.volume for item in prior20))
    volume_ratio = None if prior_volume == 0 else bars[-1].volume / prior_volume
    returns_60 = _returns(closes[-61:])
    volatility = Decimal(str(stdev(float(item) for item in returns_60))) * Decimal(
        str(math.sqrt(252))
    )
    max_drawdown = _max_drawdown_magnitude(closes[-60:])
    amihud_values = tuple(
        abs(current_close / previous_close - Decimal(1)) / bar.amount
        for previous_close, current_close, bar in zip(
            closes[-21:-1], closes[-20:], bars[-20:], strict=True
        )
        if bar.amount > 0
    )
    amihud = _mean(amihud_values) if len(amihud_values) == 20 else None
    average_amount = average_amount_20(bars)
    values: dict[ScreeningFactorId, Decimal | None] = {
        ScreeningFactorId.MOMENTUM_20: momentum_20,
        ScreeningFactorId.MOMENTUM_60: momentum_60,
        ScreeningFactorId.MOMENTUM_120_SKIP_5: momentum_120_skip_5,
        ScreeningFactorId.TREND_MA20_OVER_MA60: ma20 / ma60 - Decimal(1),
        ScreeningFactorId.BREAKOUT_20_POSITION: breakout_position,
        ScreeningFactorId.VOLUME_RATIO_20: volume_ratio,
        ScreeningFactorId.ANNUALIZED_VOLATILITY_60: volatility,
        ScreeningFactorId.MAX_DRAWDOWN_60_MAGNITUDE: max_drawdown,
        ScreeningFactorId.AMIHUD_ILLIQUIDITY_20: amihud,
        ScreeningFactorId.AVERAGE_AMOUNT_20_CNY: average_amount,
    }
    return tuple(
        AShareFactorValue(
            factor_id=factor_id,
            value=None if value is None else float(value),
        )
        for factor_id, value in values.items()
    )


def corporate_action_guard(bars: tuple[HistoryBar, ...]) -> str | None:
    """当原始昨收覆盖不足或不连续时返回审计告警。"""

    comparable = 0
    for previous, current in zip(bars, bars[1:], strict=False):
        if current.previous_close is None:
            continue
        comparable += 1
        discontinuity = abs(current.previous_close / previous.close - Decimal(1))
        if discontinuity > _CORPORATE_ACTION_TOLERANCE:
            return f"CORPORATE_ACTION_DISCONTINUITY:{current.trade_date.isoformat()}"
    possible = max(1, len(bars) - 1)
    coverage = Decimal(comparable) / Decimal(possible)
    if coverage < _MIN_CORPORATE_ACTION_COVERAGE:
        return f"CORPORATE_ACTION_GUARD_INCOMPLETE:{coverage:.3f}"
    return None


def empty_factor_values() -> tuple[AShareFactorValue, ...]:
    """为降级记录返回明确的全缺失因子向量。"""

    return tuple(AShareFactorValue(factor_id=item, value=None) for item in ScreeningFactorId)


def factor_values_with_average_amount(
    average_amount: Decimal | None,
) -> tuple[AShareFactorValue, ...]:
    """在历史数据降级时保留可独立审计的流动性因子。"""

    return tuple(
        AShareFactorValue(
            factor_id=item,
            value=(
                float(average_amount)
                if item is ScreeningFactorId.AVERAGE_AMOUNT_20_CNY
                and average_amount is not None
                else None
            ),
        )
        for item in ScreeningFactorId
    )


def average_amount_20(bars: tuple[HistoryBar, ...]) -> Decimal | None:
    if len(bars) < 20:
        return None
    return _mean(tuple(item.amount for item in bars[-20:]))


def _returns(closes: tuple[Decimal, ...]) -> tuple[Decimal, ...]:
    return tuple(
        current / previous - Decimal(1)
        for previous, current in zip(closes, closes[1:], strict=False)
    )


def _max_drawdown_magnitude(closes: tuple[Decimal, ...]) -> Decimal:
    peak = closes[0]
    largest = Decimal(0)
    for close in closes:
        peak = max(peak, close)
        largest = max(largest, Decimal(1) - close / peak)
    return largest


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


# 公开别名让兼容 facade 可以继续访问这些纯辅助函数。
returns = _returns
max_drawdown_magnitude = _max_drawdown_magnitude
mean = _mean
CORPORATE_ACTION_TOLERANCE = _CORPORATE_ACTION_TOLERANCE
MIN_CORPORATE_ACTION_COVERAGE = _MIN_CORPORATE_ACTION_COVERAGE
