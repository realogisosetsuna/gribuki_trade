"""与券商无关的市场数据领域类型。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum


class PriceAdjustment(StrEnum):
    """历史数据供应商采用的复权方式。"""

    NONE = "NONE"
    FORWARD = "FORWARD"
    BACKWARD = "BACKWARD"


@dataclass(frozen=True, slots=True)
class DailyBar:
    """一条具有时点约束的日线记录。

    价格允许缺失，因为 BaoStock 也会返回停牌交易日。策略必须显式过滤
    ``is_trading`` 与缺失价格，不得静默向前填充。
    """

    symbol: str
    trade_date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    previous_close: Decimal | None
    volume: int
    amount: Decimal
    turnover_percent: Decimal | None
    is_trading: bool
    is_st: bool
    adjustment: PriceAdjustment
