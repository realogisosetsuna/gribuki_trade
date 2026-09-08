"""A 股 PAPER 日事件通知与报告投影。

这些函数只把不可变事件字段投影为稳定报告文本或投递身份，不访问存储、网络、
调度器或运行器状态。``ashare_paper_day`` 保留历史私有名称并重新导出它们，
因此持久化投递流程可以逐步迁移而不改变外部调用契约。
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import datetime

from gribuki_trade.ports.notifier import NotificationTargetKind
from gribuki_trade.reporting.contracts import (
    ReportKind,
    humanize_internal_code,
    render_stable_text_report,
)
from gribuki_trade.services.ashare.ashare_paper_day_schedule import SHANGHAI
from gribuki_trade.services.ashare.ashare_paper_day_serialization import _aware_utc

_PAPER_HEALTH_EVENTS = frozenset(
    {
        "DAY_ABORTED",
        "HEALTH_CHECKPOINT",
        "LLM_EVIDENCE_SNAPSHOT_FAILED",
        "LLM_PREOPEN_CONTEXT_FAILED",
        "LLM_SERVICE_STATE_CHANGED",
        "PREFLIGHT_PASSED",
        "PREOPEN_SCREEN_FAILED",
        "PREOPEN_SCREEN_MISSED",
        "PREOPEN_SCREEN_RECOVERY_UNAVAILABLE",
        "RUNNER_RECOVERY_AFTER_ABORT",
        "RUNNER_RESUMED",
        "SOURCE_STATE_CHANGED",
    }
)


def _paper_notification_kind(event_type: str) -> ReportKind:
    """把每个 PAPER 必推事件绑定到六类报告契约之一。"""

    normalized = event_type.strip().upper()
    if normalized == "FILL_APPLIED":
        return ReportKind.EXECUTION_RECEIPT
    if normalized in {"DAY_COMPLETED", "DAY_COMPLETED_WITH_NOTIFICATION_GAPS"}:
        return ReportKind.DAILY_REVIEW
    if normalized in _PAPER_HEALTH_EVENTS:
        return ReportKind.SYSTEM_HEALTH
    return ReportKind.INTRADAY_ALERT


def _paper_report_artifact_key(
    *,
    run_id: str,
    target_kind: NotificationTargetKind,
    target_id: str,
    artifact_sha256: str,
) -> str:
    material = "\0".join(
        (
            "paper-day-report-artifact@1",
            run_id,
            ReportKind.DAILY_REVIEW.value,
            target_kind.value,
            target_id,
            artifact_sha256,
        )
    )
    return "paper-report-artifact:" + hashlib.sha256(material.encode("utf-8")).hexdigest()


def _contractualize_paper_notification(
    *,
    kind: ReportKind,
    event_type: str,
    raw_text: str,
    payload: Mapping[str, object],
    symbol: str | None,
    evidence_at: datetime,
) -> str:
    """保留原消息事实，同时补齐用户可稳定扫描的契约字段。"""

    title, detail = _paper_notification_title_and_detail(raw_text, event_type)
    local_time = _aware_utc(evidence_at, "notification evidence_at").astimezone(SHANGHAI)
    status = humanize_internal_code(event_type)
    error_code = payload.get("error_code")
    error_text = (
        humanize_internal_code(error_code)
        if isinstance(error_code, str) and error_code.strip()
        else "无已记录错误"
    )
    price_text = _paper_notification_price_text(payload, symbol=symbol)

    if kind is ReportKind.EXECUTION_RECEIPT:
        fees = "；".join(
            f"{label}{_notification_scalar(payload.get(key))}"
            for key, label in (
                ("commission", "佣金："),
                ("transfer_fee", "过户费："),
                ("stamp_tax", "印花税："),
                ("cash", "成交后现金："),
            )
            if payload.get(key) is not None
        ) or "费用或现金明细未随本事件投影，请以不可变账本为准。"
        protection_id = payload.get("exit_protection_id")
        protection = (
            f"已绑定保护流标识 {protection_id}；成交后继续生成并复核 DEEP 计划。"
            if isinstance(protection_id, str) and protection_id.strip()
            else "本回执未携带保护流标识；不得声称买后保护已经完成。"
        )
        return render_stable_text_report(
            kind,
            title=title,
            sections={
                "成交事实": detail,
                "费用与资金": fees,
                "持仓变化": (
                    "成交已经写入独立 PAPER 资金与持仓账本；A 股当日买入股份按 T+1 "
                    "不可卖。"
                ),
                "后续保护计划": protection,
            },
        )

    if kind is ReportKind.SYSTEM_HEALTH:
        component = _notification_scalar(payload.get("component"))
        state = _notification_scalar(payload.get("state"))
        data_source = (
            f"组件：{component}；状态：{state}。"
            if component != "未记录" or state != "未记录"
            else "本事件未改变数据源状态。"
        )
        return render_stable_text_report(
            kind,
            title=title,
            sections={
                "总体状态": f"{status}。\n{detail}",
                "数据源": data_source,
                "模型与通知": "消息先写入不可变事件，再进入持久 outbox；模型状态以事件正文为准。",
                "缺口与恢复动作": (
                    f"{error_text}。若处于降级或失败状态，买入门禁按既定失败关闭规则处理；"
                    "恢复必须产生新的状态边沿事件。"
                ),
            },
        )

    if kind is ReportKind.DAILY_REVIEW:
        positions = payload.get("positions")
        held_count = len(positions) if isinstance(positions, list) else "未记录"
        return render_stable_text_report(
            kind,
            title=title,
            sections={
                "执行摘要": detail,
                "市场复盘": "全天市场扫描、信号漏斗与数据源变化见随后上传的 Markdown 日报。",
                "操作复盘": (
                    f"收盘现金：{_notification_scalar(payload.get('cash'))}；"
                    f"持仓数：{held_count}；通知缺口："
                    f"{_notification_scalar(payload.get('notification_gaps_before_summary'))}。"
                ),
                "持仓深研": "持仓逐标的深研由收盘后编排独立生成；本摘要不补写尚未完成的结论。",
                "次日基线": "次日必须使用最新交易日历、行情、公告和退出计划重新复核。",
            },
        )

    return render_stable_text_report(
        ReportKind.INTRADAY_ALERT,
        title=title,
        sections={
            "发生了什么": detail,
            "执行结果": status,
            "关键价格": price_text,
            "证据时点": f"{local_time:%Y-%m-%d %H:%M:%S %Z}；事件稳定码：{event_type}",
        },
    )


def _paper_notification_title_and_detail(raw_text: str, event_type: str) -> tuple[str, str]:
    lines = raw_text.strip().splitlines()
    first = lines[0].strip() if lines else ""
    if first.startswith("【") and first.endswith("】"):
        title = first[1:-1].strip()
        detail = "\n".join(lines[1:]).strip()
    else:
        title = f"A股模拟盘｜{humanize_internal_code(event_type)}"
        detail = raw_text.strip()
    return title, detail or "事件已记录；正文没有更多可读细节。"


def _paper_notification_price_text(
    payload: Mapping[str, object],
    *,
    symbol: str | None,
) -> str:
    values: list[str] = []
    if symbol is not None:
        values.append(f"标的：{symbol}")
    labels = {
        "price": "价格",
        "signal_price": "信号价",
        "reference_price": "参考价",
        "limit_price": "限价",
        "invalidation_price": "失效参考",
        "stop_price": "止损价",
        "take_profit_price": "趋势目标",
        "quantity": "数量",
    }
    for key, label in labels.items():
        value = payload.get(key)
        if value is not None:
            values.append(f"{label}：{_notification_scalar(value)}")
    price_acceptance = payload.get("price_acceptance")
    if isinstance(price_acceptance, Mapping):
        for key, label in (
            ("acceptable_lower", "可接受下限"),
            ("acceptable_upper", "可接受上限"),
            ("invalidation_boundary", "失效边界"),
        ):
            value = price_acceptance.get(key)
            if value is not None:
                values.append(f"{label}：{_notification_scalar(value)}")
    return "；".join(values) if values else "本事件不涉及可执行价格。"


def _notification_scalar(value: object) -> str:
    if value is None:
        return "未记录"
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, datetime):
        return value.astimezone(SHANGHAI).isoformat(timespec="seconds")
    return str(value)


__all__ = [
    "_PAPER_HEALTH_EVENTS",
    "_contractualize_paper_notification",
    "_notification_scalar",
    "_paper_notification_kind",
    "_paper_notification_price_text",
    "_paper_notification_title_and_detail",
    "_paper_report_artifact_key",
]
