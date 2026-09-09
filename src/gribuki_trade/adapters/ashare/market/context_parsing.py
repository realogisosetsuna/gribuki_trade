"""A 股研究上下文的纯供应商字段解析与标量校验。

本模块不调用 AKShare、不创建线程，也不访问数据库；它只把已读取的供应商
行转换为严格的字段、日期、Decimal 和元数据值，供上下文适配器复用。
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, TypeVar
from zoneinfo import ZoneInfo

from gribuki_trade.ports.ashare_context import (
    AShareContextMeta,
    AShareContextPayloadError,
    RepoFixingFamily,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")

ETF_SOURCE_ID = "AKShare/Eastmoney fund_etf_spot_em"
ETF_SOURCE_URL = "https://quote.eastmoney.com/center/gridlist.html#fund_etf"
FR_SOURCE_ID = "AKShare/ChinaMoney repo_rate_query(FR)"
FDR_SOURCE_ID = "AKShare/ChinaMoney repo_rate_query(FDR)"
REPO_SOURCE_URL = "https://www.chinamoney.com.cn/chinese/bkfrr/"
GOVERNMENT_CURVE_SOURCE_ID = "AKShare/ChinaBond bond_china_yield"
GOVERNMENT_CURVE_SOURCE_URL = "https://yield.chinabond.com.cn/"
CFFEX_IF_SOURCE_ID = "AKShare/CFFEX futures_hist_daily_cffex"
CFFEX_IF_SOURCE_URL = "http://www.cffex.com.cn/cn/rtj.html"

_GOVERNMENT_CURVE_NAME = "中债国债收益率曲线"
_REPO_SYMBOLS: Mapping[RepoFixingFamily, str] = {
    RepoFixingFamily.FR: "回购定盘利率",
    RepoFixingFamily.FDR: "银银间回购定盘利率",
}
_REPO_CONTEXT_IDS: Mapping[RepoFixingFamily, str] = {
    RepoFixingFamily.FR: "CHINAMONEY_FR_FIXING",
    RepoFixingFamily.FDR: "CHINAMONEY_FDR_FIXING",
}
_REPO_DISPLAY_NAMES: Mapping[RepoFixingFamily, str] = {
    RepoFixingFamily.FR: "回购定盘利率（FR001/FR007/FR014）",
    RepoFixingFamily.FDR: "银银间回购定盘利率（FDR001/FDR007/FDR014）",
}
_TENOR_COLUMNS: tuple[tuple[str, Decimal], ...] = (
    ("3月", Decimal("0.25")),
    ("6月", Decimal("0.5")),
    ("1年", Decimal("1")),
    ("3年", Decimal("3")),
    ("5年", Decimal("5")),
    ("7年", Decimal("7")),
    ("10年", Decimal("10")),
    ("30年", Decimal("30")),
)

_ETF_ALIASES: Mapping[str, tuple[str, ...]] = {
    "code": ("代码", "code"),
    "name": ("名称", "name"),
    "last": ("最新价", "last"),
    "iopv": ("IOPV实时估值", "iopv"),
    "discount": ("基金折价率", "discount_rate_percent"),
    "turnover": ("换手率", "turnover_percent"),
    "shares": ("最新份额", "shares_outstanding"),
    "amount": ("成交额", "amount_cny"),
    "main_flow": ("主力净流入-净额", "main_net_inflow_cny"),
    "main_flow_percent": ("主力净流入-净占比", "main_net_inflow_percent"),
    "bid1": ("买一", "bid1"),
    "ask1": ("卖一", "ask1"),
    "data_date": ("数据日期", "data_date"),
    "update_time": ("更新时间", "update_time"),
}
_ETF_REQUIRED = frozenset({"code", "name", "last"})

_IF_ALIASES: Mapping[str, tuple[str, ...]] = {
    "symbol": ("symbol", "合约代码"),
    "date": ("date", "日期"),
    "open": ("open", "今开盘"),
    "high": ("high", "最高价"),
    "low": ("low", "最低价"),
    "close": ("close", "今收盘"),
    "volume": ("volume", "成交量"),
    "open_interest": ("open_interest", "持仓量"),
    "turnover": ("turnover", "成交额"),
    "settle": ("settle", "今结算"),
    "pre_settle": ("pre_settle", "前结算"),
    "variety": ("variety", "品种"),
}
_IF_REQUIRED = frozenset(_IF_ALIASES)

_T = TypeVar("_T")

def _resolve_columns(
    records: Sequence[Mapping[str, Any]],
    aliases: Mapping[str, tuple[str, ...]],
    required: frozenset[str],
    label: str,
) -> dict[str, str]:
    available = {str(key): str(key) for row in records for key in row}
    resolved: dict[str, str] = {}
    for canonical, candidates in aliases.items():
        matches = [candidate for candidate in candidates if candidate in available]
        if len(matches) > 1:
            raise AShareContextPayloadError(
                f"{label} has ambiguous columns for {canonical}: {matches}"
            )
        if matches:
            resolved[canonical] = available[matches[0]]
    missing = sorted(required.difference(resolved))
    if missing:
        raise AShareContextPayloadError(f"{label} missing required columns: {', '.join(missing)}")
    return resolved



def _date_only_meta(
    *,
    source_id: str,
    source_url: str,
    fetched_at: datetime,
    stale: bool,
    warnings: tuple[str, ...],
) -> AShareContextMeta:
    return AShareContextMeta(
        source_id=source_id,
        source_url=source_url,
        observed_at=fetched_at,
        available_at=fetched_at,
        fetched_at=fetched_at,
        stale=stale,
        degraded=stale,
        warnings=warnings,
    )


def _repo_source_id(family: RepoFixingFamily) -> str:
    return FR_SOURCE_ID if family is RepoFixingFamily.FR else FDR_SOURCE_ID


def _optional_row_value(
    row: Mapping[str, Any],
    columns: Mapping[str, str],
    canonical: str,
) -> Any:
    actual = columns.get(canonical)
    return None if actual is None else row.get(actual)


def _normalize_etf_symbol(symbol: str) -> tuple[str, str]:
    value = symbol.strip().upper()
    if len(value) == 6 and value.isdigit():
        suffix = "SH" if value.startswith("5") else "SZ"
        value = f"{value}.{suffix}"
    if len(value) != 9 or value[6] != ".":
        raise ValueError("ETF symbol must look like 510300.SH or 159915.SZ")
    code, exchange = value.split(".", maxsplit=1)
    if len(code) != 6 or not code.isdigit() or exchange not in {"SH", "SZ"}:
        raise ValueError("ETF symbol must look like 510300.SH or 159915.SZ")
    if (exchange == "SH") != code.startswith("5"):
        raise ValueError("ETF symbol exchange is inconsistent with public A-share ETF code")
    return value, code


def _provider_security_code(value: Any) -> str:
    if _is_missing(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6) if text.isdigit() else text


def _required_text(value: Any, field: str) -> str:
    if _is_missing(value):
        raise AShareContextPayloadError(f"{field} is missing")
    text = " ".join(str(value).split())
    if not text:
        raise AShareContextPayloadError(f"{field} is blank")
    return text


def _optional_decimal(value: Any, field: str) -> Decimal | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        raise AShareContextPayloadError(f"{field} cannot be boolean")
    try:
        result = Decimal(str(value).strip().replace(",", ""))
    except (InvalidOperation, ValueError) as exc:
        raise AShareContextPayloadError(f"{field} is not numeric") from exc
    if not result.is_finite():
        raise AShareContextPayloadError(f"{field} is not finite")
    return result


def _required_positive_decimal(value: Any, field: str) -> Decimal:
    result = _optional_decimal(value, field)
    if result is None or result <= 0:
        raise AShareContextPayloadError(f"{field} must be positive")
    return result


def _required_non_negative_decimal(value: Any, field: str) -> Decimal:
    result = _optional_decimal(value, field)
    if result is None or result < 0:
        raise AShareContextPayloadError(f"{field} must be non-negative")
    return result


def _required_non_negative_integer(value: Any, field: str) -> int:
    result = _required_non_negative_decimal(value, field)
    if result != result.to_integral_value():
        raise AShareContextPayloadError(f"{field} must be an integer")
    return int(result)


def _parse_date(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "to_pydatetime"):
        try:
            converted = value.to_pydatetime()
        except (TypeError, ValueError, AttributeError) as exc:
            raise AShareContextPayloadError(f"{field} is invalid") from exc
        if isinstance(converted, datetime):
            return converted.date()
        if isinstance(converted, date):
            return converted
    if _is_missing(value):
        raise AShareContextPayloadError(f"{field} is missing")
    text = str(value).strip().replace("/", "-")
    if len(text) == 8 and text.isdigit():
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise AShareContextPayloadError(f"{field} is not an ISO-compatible date") from exc


def _parse_datetime(value: Any, default_zone: ZoneInfo, field: str) -> datetime:
    if hasattr(value, "to_pydatetime"):
        try:
            value = value.to_pydatetime()
        except (TypeError, ValueError, AttributeError) as exc:
            raise AShareContextPayloadError(f"{field} is invalid") from exc
    if isinstance(value, datetime):
        result = value
    else:
        if _is_missing(value):
            raise AShareContextPayloadError(f"{field} is missing")
        try:
            result = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise AShareContextPayloadError(f"{field} is not ISO-compatible") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        result = result.replace(tzinfo=default_zone)
    return result


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().casefold() in {"", "-", "--", "nan", "nat", "none", "null"}
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Decimal):
        return not value.is_finite()
    return False


def _aware_now(now: Callable[[], datetime]) -> datetime:
    value = now()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now() must return a timezone-aware datetime")
    return value


def _validate_timeout(value: float) -> None:
    if value <= 0:
        raise ValueError("timeout_seconds must be positive")




__all__ = [
    "SHANGHAI",
    "ETF_SOURCE_ID", "ETF_SOURCE_URL", "FR_SOURCE_ID", "FDR_SOURCE_ID",
    "GOVERNMENT_CURVE_SOURCE_ID", "GOVERNMENT_CURVE_SOURCE_URL",
    "CFFEX_IF_SOURCE_ID", "CFFEX_IF_SOURCE_URL",
    "_GOVERNMENT_CURVE_NAME", "_REPO_SYMBOLS", "_REPO_CONTEXT_IDS",
    "_REPO_DISPLAY_NAMES", "_TENOR_COLUMNS", "_ETF_ALIASES", "_ETF_REQUIRED",
    "_IF_ALIASES", "_IF_REQUIRED", "_resolve_columns", "_date_only_meta",
    "_repo_source_id", "_optional_row_value", "_normalize_etf_symbol",
    "_provider_security_code", "_required_text", "_optional_decimal",
    "_required_positive_decimal", "_required_non_negative_decimal",
    "_required_non_negative_integer", "_parse_date", "_parse_datetime",
    "_is_missing", "_aware_now", "_validate_timeout",
]
