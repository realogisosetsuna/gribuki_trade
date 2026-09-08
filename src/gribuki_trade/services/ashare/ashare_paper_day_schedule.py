"""纯 A 股 PAPER 日程与交易时段计算。

该模块只负责把交易日和墙上时钟转换为带时区的 UTC 时间，并根据冻结配置
判定运行器所处的交易阶段。它不读取日历、文件、数据库或行情，也不负责
等待；运行器可以将这些确定性结果用于恢复、心跳和调度。
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

from gribuki_trade.domain.paper_day import PaperDayPhase
from gribuki_trade.services.ashare.ashare_paper_day_config import ASharePaperDayConfig
from gribuki_trade.services.ashare.ashare_paper_day_serialization import _aware_utc

SHANGHAI = ZoneInfo("Asia/Shanghai")


def session_datetime(session_date: date, wall_time: time) -> datetime:
    """将交易日的上海墙上时钟转换为 UTC。

    ``datetime.combine`` 的结果始终带有上海时区，避免调用方在系统本地时区
    与交易所时区之间发生隐式转换。
    """

    if not isinstance(session_date, date):
        raise TypeError("session_date must be a date")
    if not isinstance(wall_time, time):
        raise TypeError("wall_time must be a time")
    return datetime.combine(session_date, wall_time, tzinfo=SHANGHAI).astimezone(UTC)


def phase_at(
    value: datetime,
    session_date: date,
    config: ASharePaperDayConfig,
) -> PaperDayPhase:
    """根据上海本地时间判定 PAPER 日运行阶段。

    交易日前的时间属于 ``BOOTSTRAP``，交易日后的时间属于 ``TERMINAL``；
    交易日内的边界采用左闭右开区间，保证阶段转换没有重叠或空洞。
    """

    local = _aware_utc(value, "phase time").astimezone(SHANGHAI)
    if local.date() < session_date:
        return PaperDayPhase.BOOTSTRAP
    if local.date() > session_date:
        return PaperDayPhase.TERMINAL
    current = local.timetz().replace(tzinfo=None)
    if current < config.market_open:
        return PaperDayPhase.PREOPEN
    if current < config.morning_end:
        return PaperDayPhase.MORNING
    if current < config.afternoon_start:
        return PaperDayPhase.LUNCH
    if current < config.market_close:
        return PaperDayPhase.AFTERNOON
    if current < config.finalization_time:
        return PaperDayPhase.POST_CLOSE
    return PaperDayPhase.TERMINAL


def scheduler_sleep_seconds(
    now: datetime,
    target: datetime,
    tick_seconds: float,
) -> float:
    """返回一次调度循环应休眠的秒数。

    调用方应在 ``now < target`` 时调用；这里仍返回不小于 0.05 秒的值，
    避免系统时钟抖动导致忙等，同时不超过配置的调度粒度。
    """

    if not isinstance(tick_seconds, (int, float)) or isinstance(tick_seconds, bool):
        raise TypeError("tick_seconds must be a number")
    if tick_seconds <= 0:
        raise ValueError("tick_seconds must be positive")
    remaining = (target - now).total_seconds()
    return min(float(tick_seconds), max(0.05, remaining))


__all__ = ["SHANGHAI", "phase_at", "scheduler_sleep_seconds", "session_datetime"]
