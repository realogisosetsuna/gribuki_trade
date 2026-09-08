"""AKShare 日线载荷解析与标识规范化纯函数。

本模块只负责将 AKShare 返回的表格记录校验并转换为领域日线；
不访问网络、不持有客户端状态，供适配器与离线测试复用。
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.ports.market_data import (
    MarketDataTimeoutError,
    MarketDataUnavailableError,
)


class AKShareDailyError(MarketDataUnavailableError):
    """AKShare 历史日线数据的提供者/传输失败基类。"""


class AKShareDailyNoDataError(AKShareDailyError):
    """所选 AKShare 端点没有返回请求窗口内的记录。"""


class AKShareDailyPayloadError(AKShareDailyError):
    """所选端点返回了无效或有歧义的日线载荷。"""


class AKShareDailyTimeoutError(MarketDataTimeoutError, AKShareDailyError):
    """阻塞式 AKShare 来源超过异步调用方的时间限制。"""


class AKShareDailyUnsupportedAdjustmentError(AKShareDailyError):
    """只接受数据提供者的原始不复权价格。"""



class AKShareDailyAssetType(StrEnum):
    STOCK = "stock"
    ETF = "etf"


_COLUMN_ALIASES: Mapping[str, tuple[str, ...]] = {
    "date": ("日期", "date", "trade_date"),
    "symbol": ("股票代码", "基金代码", "代码", "symbol", "code"),
    "open": ("开盘", "开盘价", "open"),
    "close": ("收盘", "收盘价", "close"),
    "high": ("最高", "最高价", "high"),
    "low": ("最低", "最低价", "low"),
    "volume": ("成交量", "volume"),
    "amount": ("成交额", "amount", "turnover"),
    "amplitude_percent": ("振幅", "amplitude", "amplitude_percent"),
    "change_percent": ("涨跌幅", "change_percent", "pct_change"),
    "change_amount": ("涨跌额", "change", "change_amount"),
    "turnover_percent": ("换手率", "turnover_percent"),
    "previous_close": ("昨收", "昨收价", "preclose", "previous_close"),
    "trading_status": ("交易状态", "交易状态码", "tradestatus", "is_trading"),
    "is_st": ("是否ST", "isST", "is_st"),
}
_SINA_STOCK_COLUMN_ALIASES: Mapping[str, tuple[str, ...]] = {
    "date": ("date",),
    "open": ("open",),
    "close": ("close",),
    "high": ("high",),
    "low": ("low",),
    "volume": ("volume",),
    "amount": ("amount",),
}
_REQUIRED_COLUMNS = frozenset(
    {"date", "open", "close", "high", "low", "volume", "amount"}
)
_TRADING_MARKERS = frozenset({"1", "true", "交易", "正常", "交易中"})
_SUSPENDED_MARKERS = frozenset({"0", "false", "停牌", "暂停交易"})
_TRUE_MARKERS = frozenset({"1", "true", "是", "st"})
_FALSE_MARKERS = frozenset({"0", "false", "否", "非st", "normal"})
_SINA_DAILY_FALLBACK_WARNINGS = (
    "SINA_FALLBACK_IS_UNADJUSTED_DAILY_HISTORY",
    "SINA_PREVIOUS_CLOSE_DERIVED_FROM_ADJACENT_RAW_CLOSES",
    "SINA_CORPORATE_ACTION_GUARD_HAS_NO_INDEPENDENT_REFERENCE_CLOSE",
)


def _normalize_asset_types(
    values: Mapping[str, AKShareDailyAssetType | str],
) -> dict[str, AKShareDailyAssetType]:
    output: dict[str, AKShareDailyAssetType] = {}
    for symbol, raw_type in values.items():
        canonical, _ = _normalize_symbol(symbol)
        try:
            asset_type = AKShareDailyAssetType(raw_type)
        except ValueError as exc:
            raise ValueError(f"unsupported asset type for {canonical}: {raw_type!r}") from exc
        output[canonical] = asset_type
    return output


def _normalize_symbol(symbol: str) -> tuple[str, str]:
    value = symbol.strip().upper()
    if "." not in value:
        if len(value) != 6 or not value.isdigit():
            raise ValueError(
                "symbol must look like 600000.SH, 000001.SZ, or 430047.BJ"
            )
        suffix = _infer_exchange(value)
        value = f"{value}.{suffix}"
    parts = value.split(".")
    if len(parts) != 2:
        raise ValueError(
            "symbol must look like 600000.SH, 000001.SZ, or 430047.BJ"
        )
    code, exchange = parts
    if len(code) != 6 or not code.isdigit() or exchange not in {"SH", "SZ", "BJ"}:
        raise ValueError(
            "symbol must look like 600000.SH, 000001.SZ, or 430047.BJ"
        )
    return value, code


def _infer_exchange(code: str) -> str:
    # 北交所股票使用旧式 4/8 系代码，当前编号方案还使用 92 系代码。应先检查
    # 92，再应用上海更宽泛的 9 系规则，避免无后缀的当前北交所代码被错误路由。
    if code.startswith(("4", "8", "92")):
        return "BJ"
    if code.startswith(("5", "6", "9")):
        return "SH"
    return "SZ"


def _infer_asset_type(code: str) -> AKShareDailyAssetType:
    if code.startswith(("1", "5")):
        return AKShareDailyAssetType.ETF
    return AKShareDailyAssetType.STOCK


def _validate_request(
    start: date,
    end: date,
    adjustment: PriceAdjustment,
) -> None:
    if start > end:
        raise ValueError("start must be on or before end")
    if adjustment is not PriceAdjustment.NONE:
        raise AKShareDailyUnsupportedAdjustmentError(
            "AKShare historical daily adapter accepts only PriceAdjustment.NONE"
        )


def _frame_records(frame: Any, operation: str) -> list[Mapping[str, Any]]:
    if frame is None or not hasattr(frame, "to_dict"):
        raise AKShareDailyPayloadError(f"AKShare {operation} did not return a DataFrame")
    try:
        records = frame.to_dict(orient="records")
    except (TypeError, ValueError, AttributeError) as exc:
        raise AKShareDailyPayloadError(
            f"AKShare {operation} returned an unreadable DataFrame"
        ) from exc
    if not isinstance(records, list) or any(not isinstance(row, Mapping) for row in records):
        raise AKShareDailyPayloadError(f"AKShare {operation} returned invalid records")
    if not records:
        raise AKShareDailyNoDataError(f"AKShare {operation} returned no rows")
    return records


def _resolve_columns(
    rows: Sequence[Mapping[str, Any]], operation: str
) -> dict[str, str]:
    provider_columns = {str(key) for row in rows for key in row}
    resolved: dict[str, str] = {}
    aliases_by_field = (
        _SINA_STOCK_COLUMN_ALIASES
        if operation == "stock_zh_a_daily"
        else _COLUMN_ALIASES
    )
    for canonical, aliases in aliases_by_field.items():
        matches = [alias for alias in aliases if alias in provider_columns]
        if len(matches) > 1:
            raise AKShareDailyPayloadError(
                f"AKShare {operation} has ambiguous columns for {canonical}: {matches}"
            )
        if matches:
            resolved[canonical] = matches[0]
    missing = sorted(_REQUIRED_COLUMNS.difference(resolved))
    if missing:
        raise AKShareDailyPayloadError(
            f"AKShare {operation} missing required columns: {', '.join(missing)}"
        )
    return resolved


def _parse_row(
    row: Mapping[str, Any],
    columns: Mapping[str, str],
    *,
    symbol: str,
    code: str,
    asset_type: AKShareDailyAssetType,
) -> DailyBar:
    try:
        trade_date = _date_value(row.get(columns["date"]))
        _validate_row_symbol(row, columns, code)
        is_trading = _trading_status(row, columns)
        open_price = _optional_decimal(row.get(columns["open"]), "open")
        close = _optional_decimal(row.get(columns["close"]), "close")
        high = _optional_decimal(row.get(columns["high"]), "high")
        low = _optional_decimal(row.get(columns["low"]), "low")
        volume = _volume(row.get(columns["volume"]), is_trading=is_trading)
        amount = _amount(row.get(columns["amount"]), is_trading=is_trading)
        turnover_percent = _optional_column_decimal(row, columns, "turnover_percent")
        change_amount = _optional_column_decimal(row, columns, "change_amount")
        previous_close = _optional_column_decimal(row, columns, "previous_close")
        amplitude = _optional_column_decimal(row, columns, "amplitude_percent")
        change_percent = _optional_column_decimal(row, columns, "change_percent")
        if is_trading:
            if any(value is None for value in (open_price, close, high, low)):
                raise AKShareDailyPayloadError("trading row has missing OHLC")
            assert open_price is not None
            assert close is not None
            assert high is not None
            assert low is not None
            _validate_ohlc(open_price, high, low, close)
        elif any(value is not None for value in (open_price, close, high, low)):
            raise AKShareDailyPayloadError("suspended row must not contain partial OHLC")
        if turnover_percent is not None and turnover_percent < 0:
            raise AKShareDailyPayloadError("turnover_percent cannot be negative")
        if amplitude is not None and amplitude < 0:
            raise AKShareDailyPayloadError("amplitude_percent cannot be negative")
        if change_percent is not None and not change_percent.is_finite():
            raise AKShareDailyPayloadError("change_percent must be finite")
        if previous_close is None and close is not None and change_amount is not None:
            previous_close = close - change_amount
        if previous_close is not None and previous_close <= 0:
            raise AKShareDailyPayloadError("previous_close must be positive")
        is_st = (
            False
            if asset_type is AKShareDailyAssetType.ETF
            else _stock_st_status(row, columns)
        )
        return DailyBar(
            symbol=symbol,
            trade_date=trade_date,
            open=open_price,
            high=high,
            low=low,
            close=close,
            previous_close=previous_close,
            volume=volume,
            amount=amount,
            turnover_percent=turnover_percent,
            is_trading=is_trading,
            is_st=is_st,
            adjustment=PriceAdjustment.NONE,
        )
    except AKShareDailyPayloadError as exc:
        raise AKShareDailyPayloadError(
            f"invalid AKShare daily row for {symbol}: {exc}"
        ) from exc


def _validate_and_sort_bars(
    bars: tuple[DailyBar, ...],
    *,
    start: date,
    end: date,
    operation: str,
    filter_to_window: bool,
    derive_adjacent_previous_close: bool,
) -> tuple[DailyBar, ...]:
    if not bars:
        raise AKShareDailyNoDataError(f"AKShare {operation} returned no rows")
    if not filter_to_window and any(
        bar.trade_date < start or bar.trade_date > end for bar in bars
    ):
        raise AKShareDailyPayloadError(
            f"AKShare {operation} returned a date outside the requested window"
        )
    ordered = tuple(
        bar
        for bar in sorted(bars, key=lambda item: item.trade_date)
        if not filter_to_window or start <= bar.trade_date <= end
    )
    if not ordered:
        raise AKShareDailyNoDataError(
            f"AKShare {operation} returned no rows inside the requested window"
        )
    dates = [bar.trade_date for bar in ordered]
    if len(dates) != len(set(dates)):
        raise AKShareDailyPayloadError(
            f"AKShare {operation} returned duplicate trading dates"
        )
    if derive_adjacent_previous_close:
        # 只在窗口过滤后进行派生。首条可见记录不得使用早于调用方时点序列的
        # 提供者记录。
        derived: list[DailyBar] = []
        previous: DailyBar | None = None
        for bar in ordered:
            if (
                bar.previous_close is None
                and previous is not None
                and previous.close is not None
            ):
                bar = replace(bar, previous_close=previous.close)
            derived.append(bar)
            previous = bar
        return tuple(derived)
    return ordered


def _validate_row_symbol(
    row: Mapping[str, Any], columns: Mapping[str, str], expected_code: str
) -> None:
    symbol_column = columns.get("symbol")
    if symbol_column is None or _is_missing(row.get(symbol_column)):
        return
    raw = str(row.get(symbol_column)).strip().lower()
    normalized = raw.removeprefix("sh.").removeprefix("sz.")
    if normalized != expected_code:
        raise AKShareDailyPayloadError(
            f"row symbol {raw!r} does not match requested code {expected_code}"
        )


def _date_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "to_pydatetime"):
        try:
            converted = value.to_pydatetime()
        except (TypeError, ValueError, AttributeError) as exc:
            raise AKShareDailyPayloadError("date is invalid") from exc
        if isinstance(converted, datetime):
            return converted.date()
    if _is_missing(value):
        raise AKShareDailyPayloadError("date is missing")
    text = str(value).strip()
    try:
        if len(text) == 8:
            return datetime.strptime(text, "%Y%m%d").date()
        return date.fromisoformat(text)
    except ValueError as exc:
        raise AKShareDailyPayloadError(f"invalid date: {text!r}") from exc


def _trading_status(row: Mapping[str, Any], columns: Mapping[str, str]) -> bool:
    status_column = columns.get("trading_status")
    if status_column is None:
        # 东方财富日线历史端点只发出交易行情柱。停牌日期直接缺失，而不是以向前
        # 填充的记录表示。
        return True
    value = row.get(status_column)
    if _is_missing(value):
        raise AKShareDailyPayloadError("trading_status is missing")
    marker = str(value).strip().casefold()
    if marker in _TRADING_MARKERS:
        return True
    if marker in _SUSPENDED_MARKERS:
        return False
    raise AKShareDailyPayloadError(f"unsupported trading_status: {value!r}")


def _stock_st_status(row: Mapping[str, Any], columns: Mapping[str, str]) -> bool:
    st_column = columns.get("is_st")
    if st_column is None or _is_missing(row.get(st_column)):
        # stock_zh_a_hist 没有时点 ST 列。适配器不会从名称推断 ST；需要权威 ST
        # 历史的调用方应继续以 BaoStock 为首选来源。
        return False
    value = row.get(st_column)
    marker = str(value).strip().casefold()
    if marker in _TRUE_MARKERS:
        return True
    if marker in _FALSE_MARKERS:
        return False
    raise AKShareDailyPayloadError(f"unsupported is_st marker: {value!r}")


def _validate_ohlc(
    open_price: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
) -> None:
    if any(value <= 0 for value in (open_price, high, low, close)):
        raise AKShareDailyPayloadError("trading OHLC values must be positive")
    if high < max(open_price, close, low):
        raise AKShareDailyPayloadError("high is below another OHLC value")
    if low > min(open_price, close, high):
        raise AKShareDailyPayloadError("low is above another OHLC value")


def _optional_column_decimal(
    row: Mapping[str, Any], columns: Mapping[str, str], field: str
) -> Decimal | None:
    column = columns.get(field)
    if column is None:
        return None
    return _optional_decimal(row.get(column), field)


def _optional_decimal(value: Any, field: str) -> Decimal | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        raise AKShareDailyPayloadError(f"{field} cannot be boolean")
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise AKShareDailyPayloadError(f"{field} is not numeric") from exc
    if not result.is_finite():
        raise AKShareDailyPayloadError(f"{field} is not finite")
    return result


def _volume(value: Any, *, is_trading: bool) -> int:
    parsed = _optional_decimal(value, "volume")
    if parsed is None:
        if is_trading:
            raise AKShareDailyPayloadError("trading row has missing volume")
        return 0
    if parsed < 0 or parsed != parsed.to_integral_value():
        raise AKShareDailyPayloadError("volume must be a non-negative integer")
    if not is_trading and parsed != 0:
        raise AKShareDailyPayloadError("suspended row volume must be zero")
    return int(parsed)


def _amount(value: Any, *, is_trading: bool) -> Decimal:
    parsed = _optional_decimal(value, "amount")
    if parsed is None:
        if is_trading:
            raise AKShareDailyPayloadError("trading row has missing amount")
        return Decimal(0)
    if parsed < 0:
        raise AKShareDailyPayloadError("amount cannot be negative")
    if not is_trading and parsed != 0:
        raise AKShareDailyPayloadError("suspended row amount must be zero")
    return parsed


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().casefold() in {"", "-", "--", "nan", "none", "null"}
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Decimal):
        return not value.is_finite()
    return False
