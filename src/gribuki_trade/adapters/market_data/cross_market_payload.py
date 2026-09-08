"""跨市场适配器的纯 payload、日期和指标解析工具。

该模块不执行网络请求，也不包含超时、回退或编排逻辑。所有函数只负责
把上游记录转换成严格类型，供 ``cross_market`` 适配器及其历史导入路径复用。
"""

from __future__ import annotations

import math
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from gribuki_trade.ports.cross_market import (
    CrossMarketDataError,
    CrossMarketDataTimeoutError,
    CrossMarketInstrumentSpec,
    CrossMarketMissingItem,
    CrossMarketPayloadError,
    CrossMarketQuote,
)

PROVIDER = "AKShare/Eastmoney index_global_spot_em"
SINA_DAILY_PROVIDER = "AKShare/Sina daily close fallback"
_PROVIDER_TIMEZONE = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True, slots=True)
class _SinaDailyFallbackSpec:
    method_name: str
    symbol: str
    provider_name: str
    local_timezone: str
    regular_close: time
    completion_grace: timedelta = timedelta(minutes=30)


_COLUMN_ALIASES: Mapping[str, tuple[str, ...]] = {
    "code": ("代码", "code", "symbol"),
    "name": ("名称", "name"),
    "last": ("最新价", "last", "price"),
    "change_amount": ("涨跌额", "change", "change_amount"),
    "change_percent": ("涨跌幅", "change_percent", "pct_change"),
    "open": ("开盘价", "开盘", "open"),
    "high": ("最高价", "最高", "high"),
    "low": ("最低价", "最低", "low"),
    "previous_close": ("昨收价", "昨收", "previous_close", "pre_close"),
    "amplitude_percent": ("振幅", "amplitude", "amplitude_percent"),
    "quote_time": ("最新行情时间", "行情时间", "quote_time", "timestamp"),
}
_REQUIRED_COLUMNS = frozenset({"code", "name", "last", "change_percent", "quote_time"})

_DAILY_COLUMN_ALIASES: Mapping[str, tuple[str, ...]] = {
    "date": ("date", "日期"),
    "open": ("open", "开盘"),
    "high": ("high", "最高"),
    "low": ("low", "最低"),
    "close": ("close", "收盘"),
}
_DAILY_REQUIRED_COLUMNS = frozenset({"date", "close"})


def _frame_records(
    frame: Any,
    *,
    label: str = "AKShare index_global_spot_em",
) -> list[Mapping[str, Any]]:
    if frame is None or not hasattr(frame, "to_dict"):
        raise CrossMarketPayloadError(f"{label} did not return a DataFrame")
    try:
        records = frame.to_dict(orient="records")
    except (TypeError, ValueError, AttributeError) as exc:
        raise CrossMarketPayloadError(f"{label} returned an unreadable DataFrame") from exc
    if not isinstance(records, list) or any(not isinstance(row, Mapping) for row in records):
        raise CrossMarketPayloadError(f"{label} returned invalid records")
    if not records:
        raise CrossMarketPayloadError(f"{label} returned no rows")
    return records


def _missing_item(
    spec: CrossMarketInstrumentSpec,
    *,
    reason: str,
) -> CrossMarketMissingItem:
    return CrossMarketMissingItem(
        instrument_id=spec.instrument_id,
        display_name=spec.display_name,
        segment=spec.segment,
        expected_codes=spec.code_aliases,
        expected_names=spec.name_aliases,
        reason=reason,
    )


def _source_error_code(error: CrossMarketDataError) -> str:
    if isinstance(error, CrossMarketDataTimeoutError):
        return "timeout"
    if isinstance(error, CrossMarketPayloadError):
        return "incompatible_payload"
    return "provider_error"


def _fallback_failure_reason(error: CrossMarketDataError) -> str:
    if isinstance(error, CrossMarketDataTimeoutError):
        return "independent audited Sina daily fallback timed out"
    if isinstance(error, CrossMarketPayloadError):
        return "independent audited Sina daily fallback returned invalid data"
    return "independent audited Sina daily fallback was unavailable"


def _parse_sina_daily_quote(
    rows: Sequence[Mapping[str, Any]],
    spec: CrossMarketInstrumentSpec,
    fallback: _SinaDailyFallbackSpec,
    fetched_at: datetime,
    future_tolerance: timedelta,
) -> CrossMarketQuote:
    columns = _resolve_daily_columns(rows)
    local_zone = ZoneInfo(fallback.local_timezone)
    collector_local = fetched_at.astimezone(local_zone)
    completed: dict[date, tuple[Mapping[str, Any], Decimal, datetime]] = {}
    for row in rows:
        session_date = _required_session_date(row.get(columns["date"]))
        quote_time = datetime.combine(
            session_date,
            fallback.regular_close,
            tzinfo=local_zone,
        )
        if quote_time + fallback.completion_grace > collector_local:
            continue
        close = _required_decimal(row.get(columns["close"]), "close")
        if close <= 0:
            raise CrossMarketPayloadError(f"close must be positive for {spec.instrument_id}")
        if session_date in completed:
            raise CrossMarketPayloadError(
                f"duplicate completed daily row for {spec.instrument_id}: "
                f"{session_date.isoformat()}"
            )
        completed[session_date] = (row, close, quote_time)

    ordered = sorted(completed.items(), key=lambda item: item[0])
    if len(ordered) < 2:
        raise CrossMarketPayloadError(
            f"Sina daily fallback has fewer than two completed sessions for {spec.instrument_id}"
        )
    previous_date, (_, previous_close, _) = ordered[-2]
    latest_date, (latest_row, last, local_quote_time) = ordered[-1]
    open_price = _optional_daily_decimal(latest_row, columns, "open")
    high = _optional_daily_decimal(latest_row, columns, "high")
    low = _optional_daily_decimal(latest_row, columns, "low")
    for field, value in (("open", open_price), ("high", high), ("low", low)):
        if value is not None and value <= 0:
            raise CrossMarketPayloadError(f"{field} must be positive for {spec.instrument_id}")
    if high is not None and low is not None and high < low:
        raise CrossMarketPayloadError(f"high is below low for {spec.instrument_id}")
    if high is not None and last > high:
        raise CrossMarketPayloadError(f"close is above high for {spec.instrument_id}")
    if low is not None and last < low:
        raise CrossMarketPayloadError(f"close is below low for {spec.instrument_id}")

    change_amount = last - previous_close
    change_percent = change_amount / previous_close * Decimal("100")
    amplitude = (
        (high - low) / previous_close * Decimal("100")
        if high is not None and low is not None
        else None
    )
    future_by = local_quote_time.astimezone(UTC) - fetched_at.astimezone(UTC)
    if future_by > future_tolerance:
        raise CrossMarketPayloadError(
            f"daily close anchor is {future_by.total_seconds():.1f}s in the future "
            f"for {spec.instrument_id}"
        )
    age = fetched_at.astimezone(UTC) - local_quote_time.astimezone(UTC)
    if age < timedelta(0):
        age = timedelta(0)
    stale = age > spec.stale_after
    warnings = [
        "degraded independent fallback after the primary snapshot was unavailable or absent",
        "Sina daily data supplies only a session date; local_quote_time is a "
        "configured regular-close anchor, not a provider timestamp",
        "change_percent was computed from two consecutive completed Sina daily "
        f"closes ({previous_date.isoformat()} to {latest_date.isoformat()})",
    ]
    missing_optional = [
        field
        for field, value in (("open", open_price), ("high", high), ("low", low))
        if value is None
    ]
    if missing_optional:
        warnings.append("optional daily fields unavailable: " + ", ".join(missing_optional))
    if stale:
        warnings.append(f"daily close age={age.total_seconds():.1f}s")

    return CrossMarketQuote(
        instrument_id=spec.instrument_id,
        display_name=spec.display_name,
        segment=spec.segment,
        provider_code=fallback.symbol,
        provider_name=fallback.provider_name,
        last=last,
        change_percent=change_percent,
        change_amount=change_amount,
        open=open_price,
        high=high,
        low=low,
        previous_close=previous_close,
        amplitude_percent=amplitude,
        local_quote_time=local_quote_time,
        local_timezone=fallback.local_timezone,
        fetched_at=fetched_at,
        provider=f"{SINA_DAILY_PROVIDER} ({fallback.method_name})",
        stale=stale,
        degraded=True,
        warnings=tuple(warnings),
    )


def _resolve_daily_columns(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    provider_columns = {str(key) for row in rows for key in row}
    resolved: dict[str, str] = {}
    for canonical, aliases in _DAILY_COLUMN_ALIASES.items():
        matches = [alias for alias in aliases if alias in provider_columns]
        if len(matches) > 1:
            raise CrossMarketPayloadError(
                f"ambiguous Sina daily columns for {canonical}: {matches}"
            )
        if matches:
            resolved[canonical] = matches[0]
    missing = sorted(_DAILY_REQUIRED_COLUMNS.difference(resolved))
    if missing:
        raise CrossMarketPayloadError(
            "Sina daily fallback missing required columns: " + ", ".join(missing)
        )
    return resolved


def _required_session_date(value: Any) -> date:
    if _is_missing(value) or str(value).strip().casefold() == "nat":
        raise CrossMarketPayloadError("date is missing")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "to_pydatetime"):
        try:
            candidate = value.to_pydatetime()
        except (TypeError, ValueError, AttributeError) as exc:
            raise CrossMarketPayloadError("date is invalid") from exc
        if isinstance(candidate, datetime):
            return candidate.date()
        if isinstance(candidate, date):
            return candidate
    text = str(value).strip()
    try:
        return date.fromisoformat(text)
    except ValueError:
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError as exc:
            raise CrossMarketPayloadError(f"date is not ISO-compatible: {text!r}") from exc


def _optional_daily_decimal(
    row: Mapping[str, Any],
    columns: Mapping[str, str],
    canonical: str,
) -> Decimal | None:
    provider_column = columns.get(canonical)
    if provider_column is None or _is_missing(row.get(provider_column)):
        return None
    return _required_decimal(row.get(provider_column), canonical)


def _resolve_columns(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    provider_columns = {str(key) for row in rows for key in row}
    resolved: dict[str, str] = {}
    for canonical, aliases in _COLUMN_ALIASES.items():
        matches = [alias for alias in aliases if alias in provider_columns]
        if len(matches) > 1:
            raise CrossMarketPayloadError(f"ambiguous provider columns for {canonical}: {matches}")
        if matches:
            resolved[canonical] = matches[0]
    missing = sorted(_REQUIRED_COLUMNS.difference(resolved))
    if missing:
        raise CrossMarketPayloadError(
            "AKShare index_global_spot_em missing required columns: " + ", ".join(missing)
        )
    return resolved


def _index_rows(
    rows: Sequence[Mapping[str, Any]],
    columns: Mapping[str, str],
) -> tuple[dict[str, tuple[int, ...]], dict[str, tuple[int, ...]]]:
    code_lists: dict[str, list[int]] = {}
    name_lists: dict[str, list[int]] = {}
    for index, row in enumerate(rows):
        code = _optional_text(row.get(columns["code"]))
        name = _optional_text(row.get(columns["name"]))
        if code is not None:
            code_lists.setdefault(_normalize_code(code), []).append(index)
        if name is not None:
            name_lists.setdefault(_normalize_name(name), []).append(index)
    return (
        {key: tuple(value) for key, value in code_lists.items()},
        {key: tuple(value) for key, value in name_lists.items()},
    )


def _match_row(
    spec: CrossMarketInstrumentSpec,
    indexed_codes: Mapping[str, tuple[int, ...]],
    indexed_names: Mapping[str, tuple[int, ...]],
) -> tuple[int, str] | None:
    code_matches = {
        row_index
        for alias in spec.code_aliases
        for row_index in indexed_codes.get(_normalize_code(alias), ())
    }
    if len(code_matches) > 1:
        raise CrossMarketPayloadError(
            f"multiple provider rows match code aliases for {spec.instrument_id}"
        )
    if code_matches:
        return next(iter(code_matches)), "code"

    name_matches = {
        row_index
        for alias in spec.name_aliases
        for row_index in indexed_names.get(_normalize_name(alias), ())
    }
    if len(name_matches) > 1:
        raise CrossMarketPayloadError(
            f"multiple provider rows match name aliases for {spec.instrument_id}"
        )
    if name_matches:
        return next(iter(name_matches)), "name"
    return None


def _parse_quote(
    row: Mapping[str, Any],
    columns: Mapping[str, str],
    spec: CrossMarketInstrumentSpec,
    fetched_at: datetime,
    future_tolerance: timedelta,
    match_basis: str,
) -> CrossMarketQuote:
    try:
        code = _required_text(row.get(columns["code"]), "code")
        name = _required_text(row.get(columns["name"]), "name")
        last = _required_decimal(row.get(columns["last"]), "last")
        change_percent = _required_decimal(row.get(columns["change_percent"]), "change_percent")
        provider_time, time_warning = _parse_quote_time(row.get(columns["quote_time"]))
        change_amount = _optional_column_decimal(row, columns, "change_amount")
        open_price = _optional_column_decimal(row, columns, "open")
        high = _optional_column_decimal(row, columns, "high")
        low = _optional_column_decimal(row, columns, "low")
        previous_close = _optional_column_decimal(row, columns, "previous_close")
        amplitude = _optional_column_decimal(row, columns, "amplitude_percent")
    except CrossMarketPayloadError as exc:
        raise CrossMarketPayloadError(
            f"invalid matched row for {spec.instrument_id}: {exc}"
        ) from exc

    if last <= 0:
        raise CrossMarketPayloadError(f"last must be positive for {spec.instrument_id}")
    for field, value in (
        ("open", open_price),
        ("high", high),
        ("low", low),
        ("previous_close", previous_close),
    ):
        if value is not None and value <= 0:
            raise CrossMarketPayloadError(f"{field} must be positive for {spec.instrument_id}")
    if high is not None and low is not None and high < low:
        raise CrossMarketPayloadError(f"high is below low for {spec.instrument_id}")
    if amplitude is not None and amplitude < 0:
        raise CrossMarketPayloadError(
            f"amplitude_percent cannot be negative for {spec.instrument_id}"
        )

    warnings: list[str] = []
    if time_warning is not None:
        warnings.append(time_warning)
    if match_basis == "name":
        warnings.append("matched by configured exact provider-name alias")
    elif _normalize_name(name) not in {_normalize_name(alias) for alias in spec.name_aliases}:
        warnings.append("provider name differs from configured aliases; stable code matched")

    future_by = provider_time.astimezone(UTC) - fetched_at.astimezone(UTC)
    if future_by > future_tolerance:
        raise CrossMarketPayloadError(
            f"provider quote time is {future_by.total_seconds():.1f}s in the future "
            f"for {spec.instrument_id}"
        )
    clock_skew = future_by > timedelta(0)
    if clock_skew:
        warnings.append("provider quote time is slightly ahead of collector clock")
    age = fetched_at.astimezone(UTC) - provider_time.astimezone(UTC)
    if age < timedelta(0):
        age = timedelta(0)
    stale = age > spec.stale_after
    if stale:
        warnings.append(f"provider quote age={age.total_seconds():.1f}s")

    optional_values = {
        "change_amount": change_amount,
        "open": open_price,
        "high": high,
        "low": low,
        "previous_close": previous_close,
        "amplitude_percent": amplitude,
    }
    missing_optional = tuple(field for field, value in optional_values.items() if value is None)
    if missing_optional:
        warnings.append("optional quote fields unavailable: " + ", ".join(missing_optional))

    local_time = provider_time.astimezone(ZoneInfo(spec.local_timezone))
    return CrossMarketQuote(
        instrument_id=spec.instrument_id,
        display_name=spec.display_name,
        segment=spec.segment,
        provider_code=code,
        provider_name=name,
        last=last,
        change_percent=change_percent,
        change_amount=change_amount,
        open=open_price,
        high=high,
        low=low,
        previous_close=previous_close,
        amplitude_percent=amplitude,
        local_quote_time=local_time,
        local_timezone=spec.local_timezone,
        fetched_at=fetched_at,
        provider=PROVIDER,
        stale=stale,
        degraded=stale or clock_skew or match_basis == "name" or bool(missing_optional),
        warnings=tuple(warnings),
    )


def _parse_quote_time(value: Any) -> tuple[datetime, str | None]:
    if _is_missing(value):
        raise CrossMarketPayloadError("quote_time is missing")
    parsed: datetime
    if isinstance(value, datetime):
        parsed = value
    elif hasattr(value, "to_pydatetime"):
        try:
            candidate = value.to_pydatetime()
        except (TypeError, ValueError, AttributeError) as exc:
            raise CrossMarketPayloadError("quote_time is invalid") from exc
        if not isinstance(candidate, datetime):
            raise CrossMarketPayloadError("quote_time is not a datetime")
        parsed = candidate
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and not math.isfinite(value):
            raise CrossMarketPayloadError("quote_time is not finite")
        try:
            parsed = datetime.fromtimestamp(float(value), tz=UTC)
        except (OverflowError, OSError, ValueError) as exc:
            raise CrossMarketPayloadError("quote_time epoch is invalid") from exc
    else:
        text = str(value).strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CrossMarketPayloadError(f"quote_time is not ISO-compatible: {text!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return (
            parsed.replace(tzinfo=_PROVIDER_TIMEZONE),
            "provider emitted timezone-naive text; interpreted as Asia/Shanghai "
            "per AKShare interface contract",
        )
    return parsed, None


def _optional_column_decimal(
    row: Mapping[str, Any], columns: Mapping[str, str], canonical: str
) -> Decimal | None:
    provider_column = columns.get(canonical)
    if provider_column is None:
        return None
    value = row.get(provider_column)
    if _is_missing(value):
        return None
    return _required_decimal(value, canonical)


def _required_decimal(value: Any, field: str) -> Decimal:
    if _is_missing(value) or isinstance(value, bool):
        raise CrossMarketPayloadError(f"{field} is missing")
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise CrossMarketPayloadError(f"{field} is not numeric") from exc
    if not result.is_finite():
        raise CrossMarketPayloadError(f"{field} is not finite")
    return result


def _required_text(value: Any, field: str) -> str:
    result = _optional_text(value)
    if result is None:
        raise CrossMarketPayloadError(f"{field} is missing")
    return result


def _optional_text(value: Any) -> str | None:
    if _is_missing(value):
        return None
    result = str(value).strip()
    return result or None


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


def _normalize_code(value: str) -> str:
    return unicodedata.normalize("NFKC", value).strip().casefold()


def _normalize_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).strip().casefold()
    return " ".join(normalized.split())


def _aware_now(now: Callable[[], datetime]) -> datetime:
    value = now()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now() must return a timezone-aware datetime")
    return value


def _validate_universe(specs: Sequence[CrossMarketInstrumentSpec]) -> None:
    if not specs:
        raise ValueError("universe cannot be empty")
    ids = [spec.instrument_id for spec in specs]
    if len(ids) != len(set(ids)):
        raise ValueError("universe instrument_id values must be unique")
    seen_codes: dict[str, str] = {}
    seen_names: dict[str, str] = {}
    for spec in specs:
        for alias in spec.code_aliases:
            normalized = _normalize_code(alias)
            existing = seen_codes.setdefault(normalized, spec.instrument_id)
            if existing != spec.instrument_id:
                raise ValueError(
                    f"code alias {alias!r} overlaps {existing} and {spec.instrument_id}"
                )
        for alias in spec.name_aliases:
            normalized = _normalize_name(alias)
            existing = seen_names.setdefault(normalized, spec.instrument_id)
            if existing != spec.instrument_id:
                raise ValueError(
                    f"name alias {alias!r} overlaps {existing} and {spec.instrument_id}"
                )
