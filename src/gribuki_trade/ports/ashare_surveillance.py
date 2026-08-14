"""当前 A 股交易时段监控的时点契约。

监控端口刻意区别于盘后筛选器。它描述的是一个“当前交易时段尚未完成”的公开快照，
绝不会将其中记录标为闭合 K 线、交易所逐笔数据或可执行报价。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, runtime_checkable

from gribuki_trade.ports.ashare_screening import AShareBoard


class SurveillanceSourceQuality(StrEnum):
    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"


class AShareSurveillanceDataError(RuntimeError):
    """全市场盘中快照的稳定提供方边界。"""

    def __init__(self, code: str) -> None:
        normalized = code.strip()
        if not normalized:
            raise ValueError("surveillance failure code must not be empty")
        self.code = normalized
        super().__init__(f"A-share surveillance data failed ({normalized})")


@dataclass(frozen=True, slots=True)
class AShareIntradayUniverseRecord:
    """一条具有显式可空字段的研究级行情记录。"""

    symbol: str
    name: str
    board: AShareBoard
    is_st: bool | None
    is_suspended: bool | None
    last_price: Decimal | None
    previous_close: Decimal | None
    open_price: Decimal | None
    high_price: Decimal | None
    low_price: Decimal | None
    change_percent: Decimal | None
    session_amount_cny: Decimal | None
    turnover_rate_percent: Decimal | None
    volume_ratio: Decimal | None

    def __post_init__(self) -> None:
        _validate_symbol_board(self.symbol, self.board)
        if not self.name.strip():
            raise ValueError("name must not be empty")
        for field_name in (
            "last_price",
            "previous_close",
            "open_price",
            "high_price",
            "low_price",
            "change_percent",
            "session_amount_cny",
            "turnover_rate_percent",
            "volume_ratio",
        ):
            value = getattr(self, field_name)
            if value is not None and not value.is_finite():
                raise ValueError(f"{field_name} must be finite when present")


@dataclass(frozen=True, slots=True)
class AShareIntradayUniverseSnapshot:
    """首次在 ``available_at`` 可见的全市场观测。"""

    session_date: date
    available_at: datetime
    observed_at: datetime
    source_id: str
    source_revision: str
    records: tuple[AShareIntradayUniverseRecord, ...]
    quality: SurveillanceSourceQuality = SurveillanceSourceQuality.COMPLETE
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("available_at", "observed_at"):
            value = getattr(self, name)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{name} must be timezone-aware")
        if self.observed_at < self.available_at:
            raise ValueError("observed_at must not precede available_at")
        if not self.source_id.strip() or not self.source_revision.strip():
            raise ValueError("source metadata must not be empty")
        if len(self.records) != len({item.symbol for item in self.records}):
            raise ValueError("surveillance records must have unique symbols")
        if any(not warning.strip() for warning in self.warnings):
            raise ValueError("warnings must be non-empty")


@runtime_checkable
class AsyncAShareIntradayUniverseData(Protocol):
    async def fetch_intraday_universe(
        self,
        *,
        session_date: date,
        known_at: datetime,
    ) -> AShareIntradayUniverseSnapshot: ...


_SYMBOL = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")


def _validate_symbol_board(symbol: str, board: AShareBoard) -> None:
    if _SYMBOL.fullmatch(symbol) is None:
        raise ValueError("symbol must look like 600000.SH, 000001.SZ, or 430047.BJ")
    exchange = symbol[-2:]
    expected = {
        AShareBoard.SSE_MAIN: "SH",
        AShareBoard.STAR: "SH",
        AShareBoard.SZSE_MAIN: "SZ",
        AShareBoard.CHINEXT: "SZ",
        AShareBoard.BSE: "BJ",
    }[board]
    if exchange != expected:
        raise ValueError("symbol exchange is inconsistent with board")
