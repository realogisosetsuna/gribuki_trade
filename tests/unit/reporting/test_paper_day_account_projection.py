from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from gribuki_trade.reporting.paper_day.paper_day_account_projection import (
    applied_fill_sides,
    artifact_status_from_events,
    latest_account_event,
    notification_projection,
    order_id,
    positions,
    realized_by_symbol,
    unique_applied_fills,
)
from gribuki_trade.reporting.paper_day.paper_day_projection_models import PaperDaySidecarEvent

NOW = datetime(2026, 8, 14, 7, 0, tzinfo=UTC)


def event(
    event_type: str,
    payload: dict[str, object] | None = None,
    *,
    sequence: int = 1,
    correlation_id: str | None = None,
) -> PaperDaySidecarEvent:
    return PaperDaySidecarEvent(
        sequence=sequence,
        event_id=f"event-{sequence}",
        event_type=event_type,
        known_at=NOW,
        occurred_at=NOW,
        payload={} if payload is None else payload,
        phase="TEST",
        severity="INFO",
        symbol=None,
        correlation_id=correlation_id,
    )


def test_order_identity_and_fill_deduplication_keep_legacy_precedence() -> None:
    direct = event("ORDER_SUBMITTED", {"order_id": "direct"}, correlation_id="corr")
    nested = event("ORDER_SUBMITTED", {"order": {"order_id": "nested"}}, correlation_id="corr")
    fallback = event("ORDER_SUBMITTED", correlation_id="corr")
    assert order_id(direct) == "direct"
    assert order_id(nested) == "nested"
    assert order_id(fallback) == "corr"

    first = event("FILL_APPLIED", {"fill_id": "fill-1"}, sequence=1)
    duplicate = event("FILL_APPLIED", {"fill_id": "fill-1"}, sequence=2)
    second = event("FILL_APPLIED", correlation_id="fill-2", sequence=3)
    assert unique_applied_fills((first, duplicate, second)) == (first, second)


def test_account_projection_calculates_sorted_position_values_and_realized_pnl() -> None:
    account = event(
        "ACCOUNT_SNAPSHOT",
        {
            "cash": "1000",
            "positions": [
                {"symbol": "600001.SH", "quantity": 100, "average_cost": "9.5", "mark": "10"},
                {
                    "symbol": "000001.SZ",
                    "quantity": 200,
                    "today_buy": 100,
                    "available_to_sell": 100,
                    "average_cost": "8",
                    "mark": "7.5",
                },
            ],
        },
    )
    assert latest_account_event((account,)) is account
    realized = realized_by_symbol(
        {"positions": [{"symbol": "600001.SH", "realized_pnl": "12.5"}]}
    )
    projected = positions(account.payload["positions"], realized)
    assert tuple(item.symbol for item in projected) == ("000001.SZ", "600001.SH")
    assert projected[0].market_value == Decimal("1500.0")
    assert projected[0].unrealized_pnl == Decimal("-100.0")
    assert projected[1].realized_pnl == Decimal("12.5")


def test_fill_sides_and_notification_projection_fail_closed_on_missing_final_counts() -> None:
    started_buy = event(
        "FILL_STARTED",
        {"fill": {"fill_id": "fill-1", "side": "BUY"}},
        sequence=1,
    )
    applied = event("FILL_APPLIED", {"fill_id": "fill-1"}, sequence=2)
    assert applied_fill_sides((started_buy, applied), (applied,)) == ("BUY",)

    report_sent = event(
        "REPORT_UPLOADED",
        {"delivered": True},
        sequence=3,
    )
    projection = notification_projection(
        (
            event("NOTIFICATION_QUEUED", {"notification_text": "hello"}),
            report_sent,
        ),
        terminal=None,
        final_result=None,
        status={},
    )
    assert projection.required == 1
    assert projection.sent is None
    assert projection.artifact_delivery_status == "SENT"
    assert projection.delivery_projection_exact is False
    assert artifact_status_from_events((report_sent,)) == "SENT"
