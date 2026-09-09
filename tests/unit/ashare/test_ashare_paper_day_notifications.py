from __future__ import annotations

from datetime import UTC, datetime

from gribuki_trade.ports.notifier import NotificationTargetKind
from gribuki_trade.reporting.contracts import ReportKind
from gribuki_trade.services.ashare.paper_day import ashare_paper_day as facade
from gribuki_trade.services.ashare.paper_day import ashare_paper_day_notifications as notifications


def test_notification_projection_is_reexported_by_runner_facade() -> None:
    assert facade._paper_notification_kind is notifications._paper_notification_kind
    assert facade._paper_report_artifact_key is notifications._paper_report_artifact_key
    assert (
        facade._contractualize_paper_notification
        is notifications._contractualize_paper_notification
    )


def test_notification_kind_and_artifact_identity_are_stable() -> None:
    assert notifications._paper_notification_kind("FILL_APPLIED") is ReportKind.EXECUTION_RECEIPT
    assert notifications._paper_notification_kind("HEALTH_CHECKPOINT") is ReportKind.SYSTEM_HEALTH
    assert notifications._paper_notification_kind("ORDER_SUBMITTED") is ReportKind.INTRADAY_ALERT
    first = notifications._paper_report_artifact_key(
        run_id="run-1",
        target_kind=NotificationTargetKind.GROUP,
        target_id="group-1",
        artifact_sha256="a" * 64,
    )
    second = notifications._paper_report_artifact_key(
        run_id="run-1",
        target_kind=NotificationTargetKind.GROUP,
        target_id="group-1",
        artifact_sha256="a" * 64,
    )
    assert first == second
    assert first.startswith("paper-report-artifact:")


def test_contractual_notification_projection_keeps_event_facts_and_point_in_time() -> None:
    rendered = notifications._contractualize_paper_notification(
        kind=ReportKind.INTRADAY_ALERT,
        event_type="ORDER_SUBMITTED",
        raw_text="【提交】\n已提交 PAPER 委托",
        payload={"price": "10.20", "quantity": 100},
        symbol="600000.SH",
        evidence_at=datetime(2024, 1, 2, 1, 2, 3, tzinfo=UTC),
    )
    assert "已提交 PAPER 委托" in rendered
    assert "600000.SH" in rendered
    assert "10.20" in rendered
    assert "2024-01-02 09:02:03" in rendered
