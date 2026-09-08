"""AKShare 供应商载荷的纯解析边界。

本模块只负责把 AKShare、Eastmoney 和 Sina 返回的原始载荷转换为记录，
以及验证表格列。网络请求、重试、缓存和行情领域对象仍由适配器 facade
负责。异常类型放在这里后，历史 facade 仍会重新导出这些名称。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from gribuki_trade.ports.market_data import (
    MarketDataTimeoutError,
    MarketDataUnavailableError,
)


class AKShareError(MarketDataUnavailableError):
    """数据提供者、传输和载荷失败的基类。"""


class AKShareNoDataError(AKShareError):
    """数据提供者没有为有效请求返回记录。"""


class AKSharePayloadError(AKShareError):
    """数据提供者返回的字段缺失或无效。"""


class AKShareTimeoutError(MarketDataTimeoutError, AKShareError):
    """异步适配器调用超过调用方可见的超时。"""


def frame_records(frame: Any, operation: str) -> list[Mapping[str, Any]]:
    """把 DataFrame 类对象解码为只读记录，拒绝模糊载荷。"""

    if frame is None or not hasattr(frame, "to_dict"):
        raise AKSharePayloadError(f"AKShare {operation} did not return a DataFrame")
    try:
        records = frame.to_dict(orient="records")
    except (TypeError, ValueError, AttributeError) as exc:
        raise AKSharePayloadError(
            f"AKShare {operation} returned an unreadable DataFrame"
        ) from exc
    if not isinstance(records, list) or any(
        not isinstance(row, Mapping) for row in records
    ):
        raise AKSharePayloadError(f"AKShare {operation} returned invalid records")
    return records


def sina_jsonp_records(payload: str) -> list[Mapping[str, Any]]:
    """解码新浪分钟端点的 JSONP 响应，并验证记录形状。"""

    marker = "=("
    start = payload.find(marker)
    end = payload.rfind(");")
    if start < 0 or end <= start + len(marker):
        raise AKSharePayloadError("AKShare stock_zh_a_minute returned invalid JSONP")
    try:
        decoded = json.loads(payload[start + len(marker) : end])
    except (json.JSONDecodeError, TypeError) as exc:
        raise AKSharePayloadError(
            "AKShare stock_zh_a_minute returned invalid JSONP"
        ) from exc
    if not isinstance(decoded, list) or any(
        not isinstance(row, Mapping) for row in decoded
    ):
        raise AKSharePayloadError("AKShare stock_zh_a_minute returned invalid records")
    if not decoded:
        raise AKShareNoDataError("AKShare stock_zh_a_minute returned no rows")
    return decoded


def eastmoney_minute_record(value: str, *, has_vwap: bool) -> Mapping[str, Any]:
    """将 Eastmoney 的逗号分隔分钟记录转换为适配器字段。"""

    fields = value.split(",")
    if len(fields) < 7:
        raise AKSharePayloadError(
            "AKShare Eastmoney minute endpoint returned a truncated record"
        )
    return {
        "时间": fields[0],
        "开盘": fields[1],
        "收盘": fields[2],
        "最高": fields[3],
        "最低": fields[4],
        "成交量": fields[5],
        "成交额": fields[6],
        "均价": fields[7] if has_vwap and len(fields) > 7 else None,
    }


def require_columns(
    rows: list[Mapping[str, Any]], required: frozenset[str], operation: str
) -> None:
    """验证供应商记录非空且首行包含适配器所需字段。"""

    if not rows:
        raise AKShareNoDataError(f"AKShare {operation} returned no rows")
    missing = required.difference(rows[0])
    if missing:
        raise AKSharePayloadError(
            f"AKShare {operation} missing columns: {', '.join(sorted(missing))}"
        )


__all__ = [
    "AKShareError",
    "AKShareNoDataError",
    "AKSharePayloadError",
    "AKShareTimeoutError",
    "eastmoney_minute_record",
    "frame_records",
    "require_columns",
    "sina_jsonp_records",
]
