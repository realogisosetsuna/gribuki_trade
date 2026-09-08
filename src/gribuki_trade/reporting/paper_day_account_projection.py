"""PAPER-day 账户、订单和通知事件的纯投影函数。

本模块只消费已经解码的 sidecar 事件及最终结果对象。它不读取文件、不写入
SQLite，也不决定交易状态；摘要门面负责调用这些函数并组合完整报告。这样
账户恢复、订单身份去重和通知交付判断可以独立测试，并在其他报告入口复用。
"""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import cast

from .paper_day_codec import (
    optional_bool,
    optional_decimal,
    optional_int,
    optional_object,
    optional_string,
)
from .paper_day_projection_models import (
    PaperDayNotificationProjection,
    PaperDayPositionProjection,
    PaperDaySidecarEvent,
)


def order_id(event: PaperDaySidecarEvent) -> str | None:
    """按 sidecar 兼容顺序恢复订单身份。"""

    direct = optional_string(event.payload.get("order_id"))
    if direct is not None:
        return direct
    order = optional_object(event.payload.get("order"))
    if order is not None:
        nested = optional_string(order.get("order_id"))
        if nested is not None:
            return nested
    return event.correlation_id


def unique_applied_fills(
    events: Iterable[PaperDaySidecarEvent],
) -> tuple[PaperDaySidecarEvent, ...]:
    """按 fill_id/correlation_id 去重已应用成交事件，保持首次出现顺序。"""

    result: list[PaperDaySidecarEvent] = []
    seen: set[str] = set()
    for event in events:
        if event.event_type != "FILL_APPLIED":
            continue
        fill_id = optional_string(event.payload.get("fill_id")) or event.correlation_id
        if fill_id is None or fill_id in seen:
            continue
        seen.add(fill_id)
        result.append(event)
    return tuple(result)


def latest_account_event(
    events: Iterable[PaperDaySidecarEvent],
) -> PaperDaySidecarEvent | None:
    """返回最后一条同时携带现金和持仓数组的账户快照事件。"""

    result = None
    for event in events:
        if optional_decimal(event.payload.get("cash")) is not None and isinstance(
            event.payload.get("positions"), list
        ):
            result = event
    return result


def positions(
    value: object,
    realized_by_symbol: dict[str, Decimal] | None,
) -> tuple[PaperDayPositionProjection, ...]:
    """把账户 payload 中可验证的持仓数组转换为按标的排序的投影。"""

    if not isinstance(value, list):
        return ()
    result: list[PaperDayPositionProjection] = []
    for item in value:
        document = optional_object(item)
        if document is None:
            continue
        symbol = optional_string(document.get("symbol"))
        quantity = optional_int(document.get("quantity"))
        if symbol is None or quantity is None or quantity < 0:
            continue
        average_cost = optional_decimal(document.get("average_cost"))
        mark = optional_decimal(document.get("mark"))
        market_value = mark * quantity if mark is not None else None
        unrealized = (
            (mark - average_cost) * quantity
            if mark is not None and average_cost is not None
            else None
        )
        realized = None if realized_by_symbol is None else realized_by_symbol.get(symbol)
        result.append(
            PaperDayPositionProjection(
                symbol=symbol,
                quantity=quantity,
                today_buy=optional_int(document.get("today_buy")),
                available_to_sell=optional_int(document.get("available_to_sell")),
                average_cost=average_cost,
                mark=mark,
                market_value=market_value,
                unrealized_pnl=unrealized,
                realized_pnl=realized,
            )
        )
    return tuple(sorted(result, key=lambda item: item.symbol))


def realized_by_symbol(
    final_account: dict[str, object] | None,
) -> dict[str, Decimal] | None:
    """读取最终账户中每个标的的已实现盈亏；缺字段时返回未知。"""

    if final_account is None:
        return None
    values = final_account.get("positions")
    if not isinstance(values, list):
        return None
    result: dict[str, Decimal] = {}
    for value in values:
        document = optional_object(value)
        if document is None:
            return None
        symbol = optional_string(document.get("symbol"))
        realized = optional_decimal(document.get("realized_pnl"))
        if symbol is None or realized is None:
            return None
        result[symbol] = realized
    return result


def applied_fill_sides(
    events: Iterable[PaperDaySidecarEvent],
    applied: tuple[PaperDaySidecarEvent, ...],
) -> tuple[str, ...]:
    """根据 FILL_STARTED 事件恢复已应用成交的买卖方向。"""

    started: dict[str, str] = {}
    for event in events:
        if event.event_type != "FILL_STARTED":
            continue
        fill = optional_object(event.payload.get("fill"))
        fill_id = None if fill is None else optional_string(fill.get("fill_id"))
        side = None if fill is None else optional_string(fill.get("side"))
        if fill_id is not None and side is not None:
            started[fill_id] = side
    result: list[str] = []
    for event in applied:
        fill_id = optional_string(event.payload.get("fill_id")) or event.correlation_id
        if fill_id is not None and fill_id in started:
            result.append(started[fill_id])
    return tuple(result)


def notification_projection(
    events: tuple[PaperDaySidecarEvent, ...],
    terminal: PaperDaySidecarEvent | None,
    final_result: dict[str, object] | None,
    status: dict[str, object],
) -> PaperDayNotificationProjection:
    """合并事件、最终结果和状态 sidecar，恢复失败关闭的通知统计。"""

    required = sum(
        isinstance(event.payload.get("notification_text"), str)
        and bool(cast(str, event.payload.get("notification_text")).strip())
        for event in events
    )
    sent = None
    gaps = None
    exact = False
    if final_result is not None:
        final_required = optional_int(final_result.get("notification_required"))
        final_sent = optional_int(final_result.get("notification_sent"))
        final_gaps = optional_int(final_result.get("notification_gaps"))
        if final_required is not None and final_sent is not None and final_gaps is not None:
            required = final_required
            sent = final_sent
            gaps = final_gaps
            exact = True
    payload = {} if terminal is None else terminal.payload
    delivery_source = final_result if final_result is not None else status
    artifact_status = optional_string(delivery_source.get("artifact_delivery_status"))
    artifact_complete = optional_bool(delivery_source.get("artifact_delivery_complete"))
    daily_complete = optional_bool(delivery_source.get("daily_review_delivery_complete"))
    text_required = optional_int(delivery_source.get("text_notification_required"))
    text_sent = optional_int(delivery_source.get("text_notification_sent"))
    text_gaps = optional_int(delivery_source.get("text_notification_gaps"))
    delivery_exact = (
        artifact_status in {"PENDING", "SENT", "AMBIGUOUS", "NOT_CONFIGURED"}
        and artifact_complete is not None
        and daily_complete is not None
        and text_required is not None
        and text_sent is not None
        and text_gaps is not None
    )
    if artifact_status not in {"PENDING", "SENT", "AMBIGUOUS", "NOT_CONFIGURED"}:
        artifact_status = artifact_status_from_events(events)
        artifact_complete = artifact_status == "SENT"
        daily_complete = False
    return PaperDayNotificationProjection(
        required=required,
        sent=sent,
        gaps=gaps,
        retried=None,
        dead=None,
        exact_final_counts=exact,
        required_before_summary=optional_int(payload.get("notification_required_before_summary")),
        sent_before_summary=optional_int(payload.get("notification_sent_before_summary")),
        gaps_before_summary=optional_int(payload.get("notification_gaps_before_summary")),
        artifact_delivery_status=artifact_status,
        artifact_delivery_complete=artifact_complete is True,
        daily_review_delivery_complete=daily_complete is True,
        text_required=text_required,
        text_sent=text_sent,
        text_gaps=text_gaps,
        delivery_projection_exact=delivery_exact,
    )


def artifact_status_from_events(events: tuple[PaperDaySidecarEvent, ...]) -> str:
    """从 sidecar 事件投影附件状态；缺少精确总计时仍保持失败关闭。"""

    by_type = {
        "REPORT_ARTIFACT_DELIVERY_AMBIGUOUS": "AMBIGUOUS",
        "REPORT_ARTIFACT_DELIVERY_NOT_CONFIGURED": "NOT_CONFIGURED",
        "REPORT_ARTIFACT_DELIVERY_PENDING": "PENDING",
        "REPORT_ARTIFACT_DELIVERY_SENT": "SENT",
        "REPORT_ARTIFACT_LEGACY_FAILURE_AMBIGUOUS": "AMBIGUOUS",
        "REPORT_UPLOAD_FAILED": "AMBIGUOUS",
    }
    retained = "NOT_REPORTED"
    for event in events:
        if event.event_type in by_type:
            retained = by_type[event.event_type]
        elif event.event_type == "REPORT_UPLOADED" and event.payload.get("delivered") is True:
            retained = "SENT"
    return retained


__all__ = [
    "applied_fill_sides",
    "artifact_status_from_events",
    "latest_account_event",
    "notification_projection",
    "order_id",
    "positions",
    "realized_by_symbol",
    "unique_applied_fills",
]
