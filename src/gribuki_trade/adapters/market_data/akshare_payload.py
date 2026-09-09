"""AKShare 供应商载荷的纯解析边界。

本模块只负责把 AKShare、Eastmoney 和 Sina 返回的原始载荷转换为记录，
以及验证表格列。网络请求、重试、缓存和行情领域对象仍由适配器 facade
负责。异常类型放在这里后，历史 facade 仍会重新导出这些名称。
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from datetime import time as wall_time
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from gribuki_trade.ports.market_data import (
    MarketDataTimeoutError,
    MarketDataUnavailableError,
    TradeDirection,
)

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


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


def normalize_symbol(symbol: str) -> tuple[str, str]:
    """规范化带交易所后缀的 A 股代码。"""

    value = symbol.strip().upper()
    if "." not in value:
        if len(value) != 6 or not value.isdigit():
            raise ValueError("symbol must look like 600000.SH or 000001.SZ")
        suffix = "SH" if value.startswith(("5", "6", "9")) else "SZ"
        value = f"{value}.{suffix}"
    code, exchange = value.split(".", maxsplit=1)
    if len(code) != 6 or not code.isdigit() or exchange not in {"SH", "SZ"}:
        raise ValueError("symbol must look like 600000.SH or 000001.SZ")
    return value, code


def normalize_code(value: Any) -> str:
    """把供应商股票代码恢复为六位数字。"""

    if isinstance(value, bool):
        raise AKSharePayloadError(f"invalid AKShare stock code: {value!r}")
    if isinstance(value, int):
        text = str(value).zfill(6)
    elif isinstance(value, float) and math.isfinite(value) and value.is_integer():
        text = str(int(value)).zfill(6)
    else:
        text = str(value).strip().zfill(6)
    if len(text) != 6 or not text.isdigit():
        raise AKSharePayloadError(f"invalid AKShare stock code: {value!r}")
    return text


def normalize_tencent_code(value: Any) -> str:
    text = str(value).strip().lower()
    if text.startswith(("sh", "sz")):
        text = text[2:]
    return normalize_code(text)


def sina_symbol(canonical_symbol: str) -> str:
    code, exchange = canonical_symbol.split(".", maxsplit=1)
    return f"{exchange.lower()}{code}"


def minute_primary_operation(code: str) -> str:
    """为 ETF 选择与市场编号匹配的 AKShare 分钟端点。"""

    if code.startswith("5") or code.startswith("159"):
        return "fund_etf_hist_min_em"
    return "stock_zh_a_hist_min_em"


def minute_primary_provider(operation: str) -> str:
    return f"AKShare/Eastmoney {operation}"


def normalize_tencent_spot_row(
    code: str, record: Mapping[str, Any]
) -> tuple[Mapping[str, Any], tuple[str, ...]]:
    volume, volume_warning = tencent_volume_lots(record.get("volume"))
    last = optional_decimal(record.get("zxj"), "zxj")
    change = optional_decimal(record.get("zd"), "zd")
    previous_close = None if last is None or change is None else last - change
    warnings = (
        "Tencent fallback does not provide open/high/low in this schema",
        *(() if volume_warning is None else (volume_warning,)),
    )
    return (
        {
            "代码": code,
            "名称": record.get("name"),
            "最新价": last,
            "今开": None,
            "最高": None,
            "最低": None,
            "昨收": previous_close,
            "成交量": volume,
            "成交额": tencent_turnover_yuan(record.get("turnover")),
            "换手率": record.get("hsl"),
        },
        warnings,
    )


def tencent_volume_lots(value: Any) -> tuple[int, str | None]:
    """把腾讯显示的小数手数转换为整数手数。"""

    number = non_negative_decimal(value, "Tencent volume")
    whole_lots = int(number)
    remainder = number - whole_lots
    if remainder:
        return (
            whole_lots,
            f"Tencent fractional lot remainder {remainder} omitted by integer volume_lots contract",
        )
    return whole_lots, None


def tencent_turnover_yuan(value: Any) -> Decimal:
    """腾讯行情表的 turnover 以人民币万元显示。"""

    return non_negative_decimal(value, "Tencent turnover") * Decimal(10_000)


def sina_volume_lots(value: Any) -> tuple[int, tuple[str, ...]]:
    """把新浪分钟成交量（股）转换为完整 A 股手数。"""

    shares = non_negative_integer(value, "Sina volume")
    lots, odd_shares = divmod(shares, 100)
    if odd_shares:
        return lots, (
            f"Sina volume converted from shares; odd-share remainder {odd_shares} "
            "omitted by integer volume_lots contract",
        )
    return lots, ("Sina volume converted from shares to 100-share lots",)


def optional_text(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def optional_decimal(value: Any, field: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip().replace(",", "")
    if text.lower() in {"", "-", "--", "nan", "none", "null"}:
        return None
    try:
        number = Decimal(text)
    except InvalidOperation as exc:
        raise AKSharePayloadError(f"{field} is not numeric: {value!r}") from exc
    if not number.is_finite():
        raise AKSharePayloadError(f"{field} is not finite: {value!r}")
    return number


def positive_decimal(value: Any, field: str) -> Decimal:
    number = optional_decimal(value, field)
    if number is None or number <= 0:
        raise AKSharePayloadError(f"{field} must be positive: {value!r}")
    return number


def non_negative_decimal(
    value: Any, field: str, *, missing_zero: bool = False
) -> Decimal:
    number = optional_decimal(value, field)
    if number is None:
        if missing_zero:
            return Decimal(0)
        raise AKSharePayloadError(f"{field} is missing")
    if number < 0:
        raise AKSharePayloadError(f"{field} cannot be negative: {value!r}")
    return number


def non_negative_integer(
    value: Any, field: str, *, missing_zero: bool = False
) -> int:
    number = optional_decimal(value, field)
    if number is None:
        if missing_zero:
            return 0
        raise AKSharePayloadError(f"{field} is missing")
    integer = int(number)
    if number != integer or integer < 0:
        raise AKSharePayloadError(f"{field} must be a non-negative integer: {value!r}")
    return integer


def parse_direction(value: Any) -> TradeDirection:
    normalized = str(value).strip().upper()
    if normalized in {"买盘", "买", "B", "BUY"}:
        return TradeDirection.BUY
    if normalized in {"卖盘", "卖", "S", "SELL"}:
        return TradeDirection.SELL
    if normalized in {"中性盘", "中性", "N", "NEUTRAL"}:
        return TradeDirection.NEUTRAL
    return TradeDirection.UNKNOWN


def parse_trade_time(value: Any, fetched_at: datetime) -> tuple[datetime, bool]:
    text = str(value).strip()
    for datetime_format in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(text, datetime_format).replace(tzinfo=SHANGHAI_TZ)
            return parsed, False
        except ValueError:
            pass
    try:
        parsed_time = wall_time.fromisoformat(text)
    except ValueError as exc:
        raise AKSharePayloadError(f"invalid trade time: {value!r}") from exc
    return datetime.combine(fetched_at.date(), parsed_time, tzinfo=SHANGHAI_TZ), True


def parse_provider_datetime(value: Any) -> datetime:
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise AKSharePayloadError(f"invalid provider datetime: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


def to_shanghai(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(SHANGHAI_TZ)


def aware_now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now() must return a timezone-aware datetime")
    return value.astimezone(SHANGHAI_TZ)


def non_negative_age(current: datetime, past: datetime) -> timedelta:
    return max(current - past, timedelta(0))


__all__ = [
    "AKShareError",
    "AKShareNoDataError",
    "AKSharePayloadError",
    "AKShareTimeoutError",
    "SHANGHAI_TZ",
    "aware_now",
    "eastmoney_minute_record",
    "frame_records",
    "minute_primary_operation",
    "minute_primary_provider",
    "non_negative_age",
    "non_negative_decimal",
    "non_negative_integer",
    "normalize_code",
    "normalize_symbol",
    "normalize_tencent_code",
    "normalize_tencent_spot_row",
    "optional_decimal",
    "optional_text",
    "parse_direction",
    "parse_provider_datetime",
    "parse_trade_time",
    "positive_decimal",
    "require_columns",
    "sina_symbol",
    "sina_jsonp_records",
    "sina_volume_lots",
    "tencent_turnover_yuan",
    "tencent_volume_lots",
    "to_shanghai",
]
