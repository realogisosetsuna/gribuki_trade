from dataclasses import dataclass
from enum import StrEnum

from gribuki_trade.cli_commands.live_sync_payloads import (
    _append_live_immediate_protection_receipt,
    _live_cycle_payload,
    _live_immediate_protection_payload,
    _live_ingest_payload,
    _live_status_payload,
)


class Value(StrEnum):
    STOCK = "STOCK"
    PENDING = "PENDING"
    BUILD = "BUILD_PROTECTION"


@dataclass(frozen=True)
class Position:
    average_cost: str = "10.00"
    instrument_type: Value = Value.STOCK
    quantity: int = 100
    realized_pnl: str = "0"
    symbol: str = "600000.SH"


@dataclass(frozen=True)
class Snapshot:
    account_id: str = "live-main"
    confirmed_fill_count: int = 1
    last_sequence: int = 2
    positions: tuple[Position, ...] = (Position(),)
    total_fees: str = "1.20"


@dataclass(frozen=True)
class Tracking:
    buy_command_id: str = "buy-1"
    plan_ready: bool = False
    plan_stream_id: str | None = None
    protection_id: str = "protection-1"
    remaining_quantity: int = 100
    symbol: str = "600000.SH"


@dataclass(frozen=True)
class Work:
    attempts: int = 0
    error_code: str | None = None
    kind: Value = Value.BUILD
    protection_id: str | None = "protection-1"
    result_code: str | None = None
    status: Value = Value.PENDING
    work_id: str = "work-1"


@dataclass(frozen=True)
class Outcome:
    account_id: str = "live-main"
    analysis_required: bool = True
    command_id: str = "cmd-1"
    event_sequence: int = 3
    fingerprint: str | None = "fingerprint"
    protection_id: str | None = "protection-1"
    protection_work_id: str | None = "work-1"
    response_text: str = "ok"
    status: Value = Value.PENDING


@dataclass(frozen=True)
class Run:
    claimed: int = 1
    completed: int = 1
    dead: int = 0
    retried: int = 0


@dataclass(frozen=True)
class Cycle:
    barrier_observations: int = 2
    failures: tuple[object, ...] = ()
    fetched_bars: int = 3
    queued_alerts: int = 1
    target_count: int = 1


def test_live_status_and_ingest_payloads_are_stable() -> None:
    status = _live_status_payload(Snapshot(), (Tracking(),), (Work(),))
    assert status["account_id"] == "live-main"
    assert status["positions"][0]["instrument_type"] == "STOCK"
    assert status["work_items"][0]["status"] == "PENDING"
    ingest = _live_ingest_payload(Outcome())
    assert ingest["status"] == "PENDING"
    assert ingest["protection_work_id"] == "work-1"


def test_immediate_payload_adds_error_only_when_not_ready() -> None:
    result = _live_immediate_protection_payload(
        protection_id="protection-1",
        protection_work_id="work-1",
        deep_work=None,
        quick=Run(),
        work=Work(),
        tracking_state=Tracking(),
        tracking_result={"ok": False, "error_code": "TRACKING_PENDING"},
        quick_ok=False,
        tracking_ok=False,
        notification_target_fallback=True,
    )
    assert result["ok"] is False
    assert result["error_code"] == "TRACKING_PENDING"
    assert result["deep"]["queued"] is False


def test_cycle_payload_sums_finite_tracking_runs() -> None:
    result = _live_cycle_payload(
        build_work=Run(),
        deep_work=Run(claimed=2, completed=1),
        close_work=Run(claimed=0, completed=0),
        tracking_runs=(Cycle(), Cycle(queued_alerts=0, fetched_bars=1)),
        dispatch={"dead": 0, "retry_scheduled": 0, "sent": 1},
        dispatch_failed=False,
        deep_timeout_seconds=600.0,
    )
    assert result["ok"] is True
    assert result["tracking"] == {
        "barrier_observations": 4,
        "failures": [],
        "fetched_bars": 4,
        "pump_runs": 1,
        "queued_alerts": 1,
        "target_count": 1,
    }


def test_immediate_receipt_keeps_user_response_and_reports_dispatch() -> None:
    text = _append_live_immediate_protection_receipt(
        "成交事务提交时仅表示任务可恢复。",
        {
            "ok": True,
            "quick": {"plan_ready": True},
            "tracking": {"ok": True},
            "dispatch": {"attempted": True, "ok": True, "sent": 1},
        },
    )
    assert text.startswith("成交事务提交时仅表示任务可恢复。")
    assert "QUICK 已生成" in text
    assert "发送 1 条" in text
