"""A 股 PAPER 日的账户摘要与日报投影。

本模块只消费不可变账户、成交和事件值对象，生成稳定的账户摘要或 Markdown
日报；不读取 SQLite、不调用通知提供方，也不推进 PAPER 日状态。运行器仍负责
读取权威账本、查询退出计划和写入日报文件，并通过兼容门面暴露历史私有方法。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from decimal import Decimal

from gribuki_trade.domain.exit_plans import ExitPlan
from gribuki_trade.domain.paper_day import PaperDayEvent
from gribuki_trade.domain.paper_trading import AppliedPaperFill, PaperAccountSnapshot
from gribuki_trade.reporting.contracts import (
    ReportKind,
    report_contract,
    validate_markdown_report_contract,
)
from gribuki_trade.services.ashare.paper_day.ashare_paper_day_schedule import SHANGHAI
from gribuki_trade.services.ashare.paper_day.ashare_paper_day_serialization import _decimal_display


def account_summary_document(
    snapshot: PaperAccountSnapshot,
    last_prices: Mapping[str, Decimal],
) -> dict[str, object]:
    """把账户快照和已留存价格投影为通知/事件共用的摘要文档。"""

    market_value = sum(
        (
            last_prices.get(item.symbol, item.average_cost) * item.quantity
            for item in snapshot.positions
        ),
        Decimal("0"),
    )
    return {
        "cash": snapshot.cash,
        "estimated_equity": snapshot.cash + market_value,
        "estimated_market_value": market_value,
        "positions": [
            {
                "available_to_sell": item.available_to_sell,
                "average_cost": item.average_cost,
                "mark": last_prices.get(item.symbol),
                "quantity": item.quantity,
                "symbol": item.symbol,
                "today_buy": item.today_buy,
            }
            for item in snapshot.positions
            if item.quantity > 0
        ],
    }


def account_summary_text(
    document: Mapping[str, object],
    *,
    label: str,
    pending_order_count: int,
) -> str:
    """将账户摘要文档投影为稳定的中文通知文本。"""

    positions = document.get("positions")
    if not isinstance(positions, list):  # pragma: no cover - 调用边界保证文档形状
        positions = []
    cash = Decimal(str(document.get("cash", "0")))
    market_value = Decimal(str(document.get("estimated_market_value", "0")))
    equity = Decimal(str(document.get("estimated_equity", "0")))
    return (
        f"【A股模拟盘｜{label}摘要】\n"
        f"现金：{cash:.2f} 元\n"
        f"估算持仓市值：{market_value:.2f} 元\n"
        f"估算权益：{equity:.2f} 元\n"
        f"持仓数量：{len(positions)}；待撮合：{pending_order_count}。"
    )


def render_report(
    *,
    events: tuple[PaperDayEvent, ...],
    snapshot: PaperAccountSnapshot,
    fills: tuple[AppliedPaperFill, ...],
    session_date: date,
    run_id: str,
    strategy_version: str,
    partial_session: bool,
    initial_cash: Decimal,
    last_prices: Mapping[str, Decimal],
    exit_plans: Mapping[str, ExitPlan],
    required: int,
    sent: int,
    gaps: int,
) -> str:
    """生成 A 股 PAPER 全天日报，并验证日报契约。"""

    rows = [
        "# A股模拟盘全天运行报告",
        "",
        f"> 报告类型：{report_contract(ReportKind.DAILY_REVIEW).chinese_name}",
        "> 权威边界：本报告是不可变交易事件与 PAPER 账本的可读投影。",
        "",
        "## 执行摘要",
        "",
        f"- 交易日：{session_date.isoformat()}",
        f"- Run ID：`{run_id}`",
        f"- 策略：`{strategy_version}`",
        f"- 会话覆盖：{'PARTIAL_SESSION' if partial_session else 'FULL_SESSION'}",
        f"- 初始资金：{initial_cash:.2f} 元",
        "- 执行边界：仅本地 PAPER；从未连接或调用真实券商下单接口",
        "- 撮合边界：完成分钟线产生信号，仅使用信号可知后才开始的"
        "首个完整 1 分钟区间做单次 IOC；非盘口仿真",
        "",
        "## 市场复盘",
        "",
        f"- 不可变事件总数：{len(events)}。",
        "- 全市场扫描、关注名单、技术信号及数据源状态均按事件可知时点留痕；"
        "详细证据见本报告末尾时间线。",
        "- 公开网页行情不等同于交易所可执行盘口，任何降级源均不得静默提高入场权限。",
        "",
        "## 操作复盘",
        "",
        "### 最终账户",
        "",
        f"- 现金：{snapshot.cash:.2f} 元",
        f"- 成交笔数：{len(fills)}",
        f"- 必推事件：{required}；已发送：{sent}；投递缺口：{gaps}",
        "",
        "| 标的 | 数量 | 当日买入 | 可卖 | 均价 | 最新留存价 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for position in snapshot.positions:
        if position.quantity <= 0:
            continue
        mark = last_prices.get(position.symbol)
        rows.append(
            f"| {position.symbol} | {position.quantity} | {position.today_buy} | "
            f"{position.available_to_sell} | {position.average_cost:.3f} | "
            f"{_decimal_display(mark)} |"
        )
    rows.extend(("", "## 持仓深研", "", "### 持仓退出计划", ""))
    rows.extend(
        (
            "| 标的 | 深度 | 状态 | 确定止损 | 趋势目标 | 时间门槛 |",
            "|---|---|---|---:|---:|---|",
        )
    )
    planned_symbols: set[str] = set()
    for symbol, plan in sorted(exit_plans.items()):
        held_position = snapshot.position(symbol)
        if held_position is None or held_position.quantity <= 0:
            continue
        planned_symbols.add(symbol)
        rows.append(
            f"| {symbol} | {plan.depth.value} | {plan.state.value} | "
            f"{plan.stop_price:.3f} | {plan.take_profit_price:.3f} | "
            f"{plan.time_exit_at.astimezone(SHANGHAI):%Y-%m-%d %H:%M} |"
        )
    unplanned = tuple(
        item.symbol
        for item in snapshot.positions
        if item.quantity > 0 and item.symbol not in planned_symbols
    )
    if unplanned:
        rows.append("")
        rows.append(
            "未绑定退出计划的历史持仓：" + "、".join(unplanned) + "。"
            "这些标的不会被伪装为已持续监控。"
        )
    rows.extend(("", "## 成交与费用", ""))
    if not fills:
        rows.append("当日没有满足全部双门信号、风险和下一分钟撮合条件的成交。")
    else:
        rows.extend(
            (
                "| 时间 | 标的 | 方向 | 数量 | 价格 | 佣金 | 过户费 | 印花税 |",
                "|---|---|---|---:|---:|---:|---:|---:|",
            )
        )
        for item in fills:
            fill = item.fill
            rows.append(
                f"| {fill.executed_at.astimezone(SHANGHAI):%H:%M:%S} | "
                f"{fill.symbol} | {fill.side.value} | {fill.quantity} | "
                f"{fill.price:.3f} | {item.fees.commission:.2f} | "
                f"{item.fees.transfer_fee:.2f} | {item.fees.stamp_tax:.2f} |"
            )
    held_symbols = "、".join(
        item.symbol for item in snapshot.positions if item.quantity > 0
    ) or "无"
    rows.extend(
        (
            "",
            "## 次日基线",
            "",
            f"- 收盘留存持仓：{held_symbols}。",
            "- 下一交易日开盘前重新核验交易日历、停复牌、价格带、公告和最新退出计划；"
            "不得把今日信号直接复用为次日订单。",
            "- 逐标的 LLM 双轨深研由盘后编排独立生成；未生成时不得声称已经完成次日复核。",
        )
    )
    rows.extend(("", "## 不可变事件时间线", ""))
    for event in events:
        local = event.known_at.astimezone(SHANGHAI)
        symbol = "" if event.symbol is None else f" [{event.symbol}]"
        rows.append(f"### {event.sequence}. {local:%H:%M:%S} {event.event_type}{symbol}")
        rows.append("")
        rows.append(
            f"阶段 `{event.phase.value}`；级别 `{event.severity.value}`；"
            f"事件 `{event.event_id}`。"
        )
        rows.append("")
        rows.append("```json")
        rows.append(json.dumps(event.payload, ensure_ascii=False, sort_keys=True))
        rows.append("```")
        rows.append("")
    rows.extend(
        (
            "## 真实性与限制",
            "",
            "- 数据来自公开网页聚合源，不是交易所 tick/L1/L2 或可执行报价。",
            "- 盘前网页快照以今晨首次抓取时间作为 available_at，未倒填为昨日已知。",
            "- 降级分钟源只有在独立全市场快照价格交叉核对后才可通过入场数据门。",
            "- 同日卖出信号全部留存；本次按约定不提交任何卖单。",
            "- QQ outbox 是至少一次投递；极端崩溃窗口可能产生可识别的重复消息。",
            "",
        )
    )
    rendered = "\n".join(rows).rstrip() + "\n"
    validate_markdown_report_contract(ReportKind.DAILY_REVIEW, rendered)
    return rendered


__all__ = ["account_summary_document", "account_summary_text", "render_report"]
