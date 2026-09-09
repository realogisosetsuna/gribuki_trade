"""PAPER-day sidecar projection 的共享值对象。

这些对象只描述伴随文件中的不可变事件和账户/通知结果，不打开文件、不访问
SQLite，也不依赖报告渲染器。把模型放在独立模块后，解码器、投影器和摘要
门面可以沿单向依赖协作，同时继续从历史 ``paper_day_summary`` 路径导出它们。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


class PaperDaySidecarError(RuntimeError):
    """无法构建旁路日志投影时抛出的稳定错误。"""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        super().__init__(detail)


@dataclass(frozen=True, slots=True)
class PaperDaySidecarEvent:
    """一条按序追加、可用于报告投影的 sidecar 事件。"""

    sequence: int
    event_id: str
    event_type: str
    known_at: datetime
    occurred_at: datetime
    payload: dict[str, object]
    phase: str
    severity: str
    symbol: str | None
    correlation_id: str | None


@dataclass(frozen=True, slots=True)
class PaperDayPositionProjection:
    """从最新账户事件恢复的一条持仓投影。"""

    symbol: str
    quantity: int
    today_buy: int | None
    available_to_sell: int | None
    average_cost: Decimal | None
    mark: Decimal | None
    market_value: Decimal | None
    unrealized_pnl: Decimal | None
    realized_pnl: Decimal | None


@dataclass(frozen=True, slots=True)
class PaperDayNotificationProjection:
    """从事件和最终结果 sidecar 恢复的通知交付统计。"""

    required: int
    sent: int | None
    gaps: int | None
    retried: int | None
    dead: int | None
    exact_final_counts: bool
    required_before_summary: int | None
    sent_before_summary: int | None
    gaps_before_summary: int | None
    artifact_delivery_status: str
    artifact_delivery_complete: bool
    daily_review_delivery_complete: bool
    text_required: int | None
    text_sent: int | None
    text_gaps: int | None
    delivery_projection_exact: bool


__all__ = [
    "PaperDayNotificationProjection",
    "PaperDayPositionProjection",
    "PaperDaySidecarError",
    "PaperDaySidecarEvent",
]
