"""Broker-neutral market-data domain types."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum


class PriceAdjustment(StrEnum):
    """Adjustment applied by a historical-data provider."""

    NONE = "NONE"
    FORWARD = "FORWARD"
    BACKWARD = "BACKWARD"


@dataclass(frozen=True, slots=True)
class DailyBar:
    """One point-in-time daily record.

    Prices are optional because BaoStock also reports suspended sessions.  A
    strategy must explicitly filter ``is_trading`` and missing prices instead
    of silently forward-filling them.
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
