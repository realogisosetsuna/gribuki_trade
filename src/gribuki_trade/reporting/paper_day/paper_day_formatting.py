"""PAPER-day 报告的纯文本格式化函数。

这些函数只把已经投影出的值转换为 Markdown 单元格文本，不读取 sidecar、
不写文件，也不参与报告状态判断。将它们集中后，投影逻辑可以专注于事件
聚合，渲染器仍可通过历史私有名称调用它们。
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from gribuki_trade.reporting.contracts import humanize_internal_code

SHANGHAI = ZoneInfo("Asia/Shanghai")
UNAVAILABLE = "不可得（sidecar 未记录）"


def readable_code(value: str) -> str:
    """把稳定机器码显示为中文，同时保留审计参数。"""

    normalized = value.strip()
    if not normalized:
        return "—"
    if ":" in normalized:
        prefix, suffix = normalized.split(":", maxsplit=1)
        return f"{humanize_internal_code(prefix)}（审计参数：{suffix}）"
    return humanize_internal_code(normalized)


def optional_number(value: int | None) -> str:
    return UNAVAILABLE if value is None else str(value)


def money(value: Decimal | None) -> str:
    return UNAVAILABLE if value is None else f"{value:.2f} 元"


def price(value: Decimal | None) -> str:
    return UNAVAILABLE if value is None else f"{value:.3f}"


def local_time(value: datetime, *, with_date: bool = False) -> str:
    local = value.astimezone(SHANGHAI)
    return local.strftime("%Y-%m-%d %H:%M:%S" if with_date else "%H:%M:%S")


def time_or_unavailable(value: datetime | None) -> str:
    return UNAVAILABLE if value is None else local_time(value)


def pairs(values: Iterable[tuple[str, int]]) -> str:
    """把稳定排序后的键值对渲染为中文逗号分隔文本。"""

    items = tuple(values)
    return "、".join(f"{readable_code(key)}={count}" for key, count in items) if items else "无"


def short_hash(value: str | None) -> str:
    return UNAVAILABLE if value is None else f"`{value[:12]}`"


def percent(value: Decimal | None) -> str:
    return UNAVAILABLE if value is None else f"{value * 100:.2f}%"


def tail_identifier(value: str | None) -> str:
    return "不可得" if value is None else value[-16:]


def safe_table_text(value: str) -> str:
    return " ".join(value.split()).replace("|", "\\|")


def llm_score_text(value: Decimal | None) -> str:
    return UNAVAILABLE if value is None else format(value, "f")


def deep_selected_system_text(value: str) -> str:
    return {
        "ADVERSARIAL_LLM": "结构化对抗分析系统",
        "BASELINE_LLM": "原单分析器",
        "DETERMINISTIC_ONLY": "仅确定性价格规则",
        "DETERMINISTIC_OR_BASELINE_FALLBACK": "确定性规则或原单分析器降级结果",
    }.get(value, readable_code(value))


__all__ = [
    "deep_selected_system_text",
    "llm_score_text",
    "local_time",
    "money",
    "optional_number",
    "pairs",
    "percent",
    "price",
    "readable_code",
    "safe_table_text",
    "short_hash",
    "tail_identifier",
    "time_or_unavailable",
]
