"""A 股 PAPER-day 状态、报告与结果投影。

这些函数只读取 sidecar 或构造纯 CLI 结果，不打开 PAPER-day SQLite。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol


class _LazyCliFacade:
    def __getattr__(self, name: str) -> Any:
        from gribuki_trade import cli
        return getattr(cli, name)

_cli: Any = _LazyCliFacade()

__all__ = [
    "_PaperDayResultProjectionSource",
    "_ashare_paper_day_report",
    "_ashare_paper_day_status",
    "_ashare_paper_day_summary",
    "_latest_paper_day_artifact_event_status",
    "_non_negative_int_or_none",
    "_paper_day_delivery_projection",
    "_paper_day_delivery_sidecar_projection",
]

class _PaperDayResultProjectionSource(Protocol):
    @property
    def completed(self) -> bool: ...

    @property
    def notification_required(self) -> int: ...

    @property
    def notification_sent(self) -> int: ...

    @property
    def notification_gaps(self) -> int: ...

    @property
    def artifact_delivery_status(self) -> str: ...

    @property
    def artifact_delivery_complete(self) -> bool: ...

    @property
    def daily_review_delivery_complete(self) -> bool: ...


def _paper_day_delivery_projection(
    result: _PaperDayResultProjectionSource,
) -> dict[str, object]:
    """把交易终态与 DAILY_REVIEW 双交付状态分开投影给 CLI。"""

    completed = bool(result.completed)
    notification_required = int(result.notification_required)
    notification_sent = int(result.notification_sent)
    notification_gaps = int(result.notification_gaps)
    artifact_delivery_status = str(
        getattr(result, "artifact_delivery_status", "LEGACY_UNREPORTED")
    )
    artifact_delivery_complete = bool(
        getattr(
            result,
            "artifact_delivery_complete",
            False,
        )
    )
    daily_review_delivery_complete = bool(
        getattr(
            result,
            "daily_review_delivery_complete",
            False,
        )
    )
    return {
        "ok": completed and daily_review_delivery_complete,
        "notification_required": notification_required,
        "notification_sent": notification_sent,
        "notification_gaps": notification_gaps,
        "text_notification_required": int(
            getattr(result, "text_notification_required", notification_required)
        ),
        "text_notification_sent": int(
            getattr(result, "text_notification_sent", notification_sent)
        ),
        "text_notification_gaps": int(
            getattr(result, "text_notification_gaps", notification_gaps)
        ),
        "artifact_delivery_status": artifact_delivery_status,
        "artifact_delivery_complete": artifact_delivery_complete,
        "daily_review_delivery_complete": daily_review_delivery_complete,
    }
def _ashare_paper_day_status(
    session_root: Path,
    session_date: date,
) -> dict[str, object]:
    status_path = session_root / "status.json"
    if not status_path.is_file():
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            "status",
            session_date,
            session_root,
            "STATUS_NOT_AVAILABLE",
        )
    try:
        payload = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            "status",
            session_date,
            session_root,
            "STATUS_FILE_INVALID",
        )
    if not isinstance(payload, dict):
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            "status",
            session_date,
            session_root,
            "STATUS_FILE_INVALID",
        )
    delivery = _cli._paper_day_delivery_sidecar_projection(session_root, payload)
    return {
        "ok": True,
        "action": "status",
        "artifact_delivery_status": delivery["artifact_delivery_status"],
        "daily_review_delivery_complete": delivery[
            "daily_review_delivery_complete"
        ],
        "delivery": delivery,
        "operationally_complete": delivery["daily_review_delivery_complete"],
        "read_only_sidecar": True,
        "runtime_dir": str(session_root),
        "status_path": str(status_path),
        "status": payload,
    }


def _paper_day_delivery_sidecar_projection(
    session_root: Path,
    status: Mapping[str, object],
) -> dict[str, object]:
    """只读投影 PAPER 日报的文本与附件交付状态，不打开 SQLite。"""

    allowed_statuses = {"PENDING", "SENT", "AMBIGUOUS", "NOT_CONFIGURED"}
    artifact_status = status.get("artifact_delivery_status")
    if isinstance(artifact_status, str) and artifact_status in allowed_statuses:
        artifact_complete = status.get("artifact_delivery_complete") is True
        daily_complete = status.get("daily_review_delivery_complete") is True
        return {
            "artifact_delivery_complete": artifact_complete,
            "artifact_delivery_status": artifact_status,
            "daily_review_delivery_complete": daily_complete,
            "notification_gaps": _non_negative_int_or_none(
                status.get("notification_gaps")
            ),
            "notification_required": _non_negative_int_or_none(
                status.get("notification_required")
            ),
            "notification_sent": _non_negative_int_or_none(
                status.get("notification_sent")
            ),
            "projection_exact": all(
                (
                    isinstance(status.get("artifact_delivery_complete"), bool),
                    isinstance(status.get("daily_review_delivery_complete"), bool),
                    _non_negative_int_or_none(status.get("notification_required"))
                    is not None,
                    _non_negative_int_or_none(status.get("notification_sent"))
                    is not None,
                    _non_negative_int_or_none(status.get("notification_gaps"))
                    is not None,
                    _non_negative_int_or_none(status.get("text_notification_required"))
                    is not None,
                    _non_negative_int_or_none(status.get("text_notification_sent"))
                    is not None,
                    _non_negative_int_or_none(status.get("text_notification_gaps"))
                    is not None,
                )
            ),
            "projection_source": "status.json",
            "text_notification_gaps": _non_negative_int_or_none(
                status.get("text_notification_gaps")
            ),
            "text_notification_required": _non_negative_int_or_none(
                status.get("text_notification_required")
            ),
            "text_notification_sent": _non_negative_int_or_none(
                status.get("text_notification_sent")
            ),
        }

    event_status = _latest_paper_day_artifact_event_status(
        session_root / "session.log.jsonl"
    )
    return {
        "artifact_delivery_complete": event_status == "SENT",
        "artifact_delivery_status": event_status,
        "daily_review_delivery_complete": False,
        "notification_gaps": None,
        "notification_required": None,
        "notification_sent": None,
        "projection_exact": False,
        "projection_source": (
            "session.log.jsonl" if event_status != "NOT_REPORTED" else "unavailable"
        ),
        "text_notification_gaps": None,
        "text_notification_required": None,
        "text_notification_sent": None,
    }


def _latest_paper_day_artifact_event_status(event_log_path: Path) -> str:
    """从追加式 sidecar 中读取最后一个完整附件状态；未知时失败关闭。"""

    try:
        lines = event_log_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return "NOT_REPORTED"
    event_status = "NOT_REPORTED"
    by_type = {
        "REPORT_ARTIFACT_DELIVERY_AMBIGUOUS": "AMBIGUOUS",
        "REPORT_ARTIFACT_DELIVERY_NOT_CONFIGURED": "NOT_CONFIGURED",
        "REPORT_ARTIFACT_DELIVERY_PENDING": "PENDING",
        "REPORT_ARTIFACT_DELIVERY_SENT": "SENT",
        "REPORT_ARTIFACT_LEGACY_FAILURE_AMBIGUOUS": "AMBIGUOUS",
    }
    for line in lines:
        if not line.strip():
            continue
        try:
            document = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(document, dict):
            continue
        event_type = document.get("event_type")
        if isinstance(event_type, str) and event_type in by_type:
            event_status = by_type[event_type]
            continue
        if event_type == "REPORT_UPLOADED":
            payload = document.get("payload")
            if isinstance(payload, dict) and payload.get("delivered") is True:
                event_status = "SENT"
        elif event_type == "REPORT_UPLOAD_FAILED":
            event_status = "AMBIGUOUS"
    return event_status


def _non_negative_int_or_none(value: object) -> int | None:
    """只接受不是布尔值的非负整数。"""

    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _ashare_paper_day_report(
    session_root: Path,
    session_date: date,
) -> dict[str, object]:
    report_dir = session_root / "reports"
    reports = (
        tuple(
            sorted(
                report_dir.glob(f"ashare-paper-day-{session_date.isoformat()}-*.md"),
                key=lambda item: item.name,
            )
        )
        if report_dir.is_dir()
        else ()
    )
    if not reports:
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            "report",
            session_date,
            session_root,
            "REPORT_NOT_AVAILABLE",
        )
    report_path = reports[-1].resolve()
    try:
        content = report_path.read_bytes()
        text_content = content.decode("utf-8")
        modified_at = datetime.fromtimestamp(report_path.stat().st_mtime, UTC)
    except (OSError, UnicodeError):
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            "report",
            session_date,
            session_root,
            "REPORT_FILE_INVALID",
        )
    return {
        "ok": True,
        "action": "report",
        "read_only_sidecar": True,
        "runtime_dir": str(session_root),
        "report": {
            "path": str(report_path),
            "name": report_path.name,
            "bytes": len(content),
            "line_count": len(text_content.splitlines()),
            "modified_at": modified_at.isoformat(),
            "sha256": hashlib.sha256(content).hexdigest(),
        },
    }


def _ashare_paper_day_summary(
    session_root: Path,
    session_date: date,
) -> dict[str, object]:
    """从伴随文件重新生成增强报告，且不打开 SQLite。"""

    from gribuki_trade.reporting.paper_day_summary import (
        PaperDaySidecarError,
        project_paper_day_sidecars,
        write_paper_day_summary,
    )

    try:
        projection = project_paper_day_sidecars(session_root)
    except PaperDaySidecarError as error:
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            "summary",
            session_date,
            session_root,
            error.code,
        )
    if projection.session_date != session_date:
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            "summary",
            session_date,
            session_root,
            "SUMMARY_SESSION_CONFLICT",
        )
    try:
        summary_path = write_paper_day_summary(projection)
        content = summary_path.read_bytes()
        modified_at = datetime.fromtimestamp(summary_path.stat().st_mtime, UTC)
    except OSError:
        return _cli._ashare_paper_day_error(  # type: ignore[no-any-return]
            "summary",
            session_date,
            session_root,
            "SUMMARY_WRITE_FAILED",
        )
    return {
        "ok": True,
        "action": "summary",
        "sidecar_only": True,
        "sqlite_opened": False,
        "runtime_dir": str(session_root),
        "summary": {
            "path": str(summary_path),
            "name": summary_path.name,
            "bytes": len(content),
            "line_count": len(content.decode("utf-8").splitlines()),
            "modified_at": modified_at.isoformat(),
            "sha256": hashlib.sha256(content).hexdigest(),
            "lifecycle": projection.lifecycle,
            "coverage": projection.coverage,
            "event_count": projection.sidecar_event_count,
            "warning_count": len(projection.warnings),
        },
    }
