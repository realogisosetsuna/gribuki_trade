"""A 股 PAPER 日运行结果的不可变模型。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from gribuki_trade.domain.paper_trading import PaperAccountSnapshot


@dataclass(frozen=True, slots=True)
class PaperDayResult:
    run_id: str
    session_date: date
    completed: bool
    event_count: int
    notification_required: int
    notification_sent: int
    notification_gaps: int
    final_snapshot: PaperAccountSnapshot
    report_path: Path
    artifact_delivery_status: str = "NOT_CONFIGURED"
    artifact_delivery_complete: bool = False
    daily_review_delivery_complete: bool = False
    text_notification_required: int = 0
    text_notification_sent: int = 0
    text_notification_gaps: int = 0


__all__ = ["PaperDayResult"]
