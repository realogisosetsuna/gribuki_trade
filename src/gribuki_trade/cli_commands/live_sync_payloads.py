"""实盘同步命令的纯事件与结果文档投影。

CLI facade 负责读取 OneBot 事件、调用账本和编排服务；本模块只把这些服务
返回的不可变对象转换为稳定 JSON 文档或用户可读回执。这里没有文件、网络、
SQLite、调度器或 broker 访问，因此可以独立测试，也不会改变实盘权限边界。
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Protocol


class _ValueLike(Protocol):
    value: str


class _PositionLike(Protocol):
    average_cost: object
    instrument_type: _ValueLike
    quantity: int
    realized_pnl: object
    symbol: str


class _SnapshotLike(Protocol):
    account_id: str
    confirmed_fill_count: int
    last_sequence: int
    positions: Iterable[_PositionLike]
    total_fees: object


class _TrackingLike(Protocol):
    buy_command_id: str
    plan_ready: bool
    plan_stream_id: str | None
    protection_id: str
    remaining_quantity: int
    symbol: str


class _WorkLike(Protocol):
    attempts: int
    error_code: str | None
    kind: _ValueLike
    protection_id: str | None
    result_code: str | None
    status: _ValueLike
    work_id: str


class _InboundOutcomeLike(Protocol):
    account_id: str
    analysis_required: bool
    command_id: str
    event_sequence: int
    fingerprint: str | None
    protection_id: str | None
    protection_work_id: str | None
    response_text: str
    status: _ValueLike


class _RunSummaryLike(Protocol):
    claimed: int
    completed: int
    dead: int
    retried: int


class _CycleRunLike(Protocol):
    barrier_observations: int
    failures: Iterable[object]
    fetched_bars: int
    queued_alerts: int
    target_count: int


class _FailureLike(Protocol):
    account_id: str
    error_code: str
    symbol: str


def _live_status_payload(
    snapshot: Any,
    tracking: Iterable[Any],
    work_items: Iterable[Any],
) -> dict[str, object]:
    """将实盘观察账户、保护跟踪和恢复工作投影成稳定状态文档。"""

    return {
        "account_id": snapshot.account_id,
        "confirmed_fill_count": snapshot.confirmed_fill_count,
        "integrity_verified": True,
        "last_sequence": snapshot.last_sequence,
        "ok": True,
        "positions": [
            {
                "average_cost": str(item.average_cost),
                "instrument_type": item.instrument_type.value,
                "quantity": item.quantity,
                "realized_pnl": str(item.realized_pnl),
                "symbol": item.symbol,
            }
            for item in snapshot.positions
        ],
        "protection_tracking": [
            {
                "buy_command_id": item.buy_command_id,
                "plan_ready": item.plan_ready,
                "plan_stream_id": item.plan_stream_id,
                "protection_id": item.protection_id,
                "remaining_quantity": item.remaining_quantity,
                "symbol": item.symbol,
            }
            for item in tracking
        ],
        "total_fees": str(snapshot.total_fees),
        "work_items": [
            {
                "attempts": item.attempts,
                "error_code": item.error_code,
                "kind": item.kind.value,
                "protection_id": item.protection_id,
                "result_code": item.result_code,
                "status": item.status.value,
                "work_id": item.work_id,
            }
            for item in work_items
        ],
    }


def _live_ingest_payload(outcome: Any) -> dict[str, object]:
    """将单条 OneBot 入站结果转换为不含领域对象的 JSON 文档。"""

    return {
        "account_id": outcome.account_id,
        "analysis_required": outcome.analysis_required,
        "command_id": outcome.command_id,
        "event_sequence": outcome.event_sequence,
        "fingerprint": outcome.fingerprint,
        "ok": True,
        "protection_id": outcome.protection_id,
        "protection_work_id": outcome.protection_work_id,
        "response_text": outcome.response_text,
        "status": outcome.status.value,
    }


def _live_immediate_protection_payload(
    *,
    protection_id: str,
    protection_work_id: str,
    deep_work: Any,
    quick: Any,
    work: Any,
    tracking_state: Any,
    tracking_result: Mapping[str, object],
    quick_ok: bool,
    tracking_ok: bool,
    notification_target_fallback: bool,
) -> dict[str, object]:
    """生成确认成交后 QUICK/跟踪结果；不执行任何副作用。"""

    result: dict[str, object] = {
        "deep": {
            "queued": deep_work is not None,
            "status": None if deep_work is None else deep_work.status.value,
            "work_id": None if deep_work is None else deep_work.work_id,
        },
        "execution_authority": False,
        "ok": quick_ok and tracking_ok,
        "notification_target_fallback": notification_target_fallback,
        "protection_id": protection_id,
        "protection_work_id": protection_work_id,
        "quick": {
            "claimed": quick.claimed,
            "completed": quick.completed,
            "dead": quick.dead,
            "error_code": work.error_code,
            "plan_ready": tracking_state.plan_ready,
            "remaining_quantity": tracking_state.remaining_quantity,
            "result_code": work.result_code,
            "retried": quick.retried,
            "status": work.status.value,
        },
        "tracking": dict(tracking_result),
    }
    if not result["ok"]:
        result["error_code"] = (
            work.error_code
            or tracking_result.get("error_code")
            or "LIVE_IMMEDIATE_PROTECTION_PENDING"
        )
    return result


def _append_live_immediate_protection_receipt(
    response_text: str,
    immediate: Mapping[str, object],
) -> str:
    """把提交后尝试的真实状态追加到成交回执，而不改写成交结论。"""

    quick = immediate.get("quick")
    tracking = immediate.get("tracking")
    if (
        isinstance(quick, Mapping)
        and quick.get("plan_ready") is True
        and isinstance(tracking, Mapping)
        and tracking.get("ok") is True
    ):
        detail = "QUICK 已生成并已完成一次有限行情跟踪；DEEP 已持久排队。"
    elif isinstance(quick, Mapping) and quick.get("plan_ready") is True:
        error_code = immediate.get("error_code") or "LIVE_IMMEDIATE_TRACKING_INCOMPLETE"
        detail = f"QUICK 已生成，但本次有限跟踪未完整完成（{error_code}）；后续 cycle 将继续恢复。"
    elif immediate.get("ok") is True:
        detail = "持仓已在并发同步中关闭，无需再激活保护计划。"
    else:
        error_code = immediate.get("error_code") or "LIVE_IMMEDIATE_PROTECTION_PENDING"
        detail = f"本次有限 QUICK/跟踪未完整完成（{error_code}）；持久工作仍由后续 cycle 恢复。"
    dispatch = immediate.get("dispatch")
    if isinstance(dispatch, Mapping) and dispatch.get("attempted") is True:
        if dispatch.get("ok") is True:
            detail += f" NapCat 有限派发已完成，发送 {dispatch.get('sent', 0)} 条。"
        else:
            dispatch_code = dispatch.get("error_code") or "LIVE_IMMEDIATE_ALERT_DISPATCH_PENDING"
            detail += f" NapCat 派发未完成（{dispatch_code}），提醒仍保留在 durable outbox。"
    return response_text.rstrip() + "\n即时保护结果：" + detail


def _live_cycle_payload(
    *,
    build_work: Any,
    deep_work: Any,
    close_work: Any,
    tracking_runs: Iterable[Any],
    dispatch: Mapping[str, Any],
    dispatch_failed: bool,
    deep_timeout_seconds: float,
) -> dict[str, object]:
    """将一次有限 cycle 的工作、跟踪和派发统计投影为稳定 JSON。"""

    runs = tuple(tracking_runs)
    result: dict[str, object] = {
        "analysis": {
            "build": {
                "claimed": build_work.claimed,
                "completed": build_work.completed,
                "dead": build_work.dead,
                "retried": build_work.retried,
            },
            "build_limit": 1,
            "deep_timeout_seconds": deep_timeout_seconds,
            "deep": {
                "claimed": deep_work.claimed,
                "completed": deep_work.completed,
                "dead": deep_work.dead,
                "retried": deep_work.retried,
            },
            "close": {
                "claimed": close_work.claimed,
                "completed": close_work.completed,
                "dead": close_work.dead,
                "retried": close_work.retried,
            },
        },
        "dispatch": {
            "dead": dispatch["dead"],
            "retry_scheduled": dispatch["retry_scheduled"],
            "sent": dispatch["sent"],
        },
        "execution_authority": False,
        "ok": not dispatch_failed,
        "tracking": {
            "barrier_observations": sum(item.barrier_observations for item in runs),
            "failures": [
                {
                    "account_id": item.account_id,
                    "error_code": item.error_code,
                    "symbol": item.symbol,
                }
                for run in runs
                for item in run.failures
            ],
            "fetched_bars": sum(item.fetched_bars for item in runs),
            "pump_runs": len(runs) - 1,
            "queued_alerts": sum(item.queued_alerts for item in runs),
            "target_count": max(item.target_count for item in runs),
        },
    }
    if dispatch_failed:
        result["error_code"] = "LIVE_ALERT_DISPATCH_FAILED"
    return result
