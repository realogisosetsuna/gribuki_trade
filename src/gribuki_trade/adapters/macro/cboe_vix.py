"""Cboe 官方 VIX 日终 CSV 历史数据的只读适配器。"""

from __future__ import annotations

import csv
import hashlib
import io
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Final
from zoneinfo import ZoneInfo

import httpx

from gribuki_trade.ports.global_risk import (
    GlobalRiskCacheEntry,
    GlobalRiskCacheError,
    GlobalRiskHTTPStatusError,
    GlobalRiskNoDataError,
    GlobalRiskSchemaError,
    GlobalRiskSourceMeta,
    GlobalRiskTimeoutError,
    GlobalRiskTransportError,
    VIXDailyBar,
    VIXDailyHistory,
)

CBOE_VIX_SOURCE_ID: Final = "CBOE_VIX_DAILY_HISTORY"
CBOE_VIX_EOD_CSV_URL: Final = (
    "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
)

_NEW_YORK = ZoneInfo("America/New_York")
_VIX_REGULAR_CLOSE = time(16, 15)
_EXPECTED_COLUMNS = ("DATE", "OPEN", "HIGH", "LOW", "CLOSE")
_ACCEPTED_MEDIA_TYPES = frozenset(
    {"text/csv", "application/csv", "application/octet-stream", "text/plain"}
)


class CboeVIXDailyAdapter:
    """获取精确的 Cboe 官方 VIX 日线历史并按时点过滤。

    Cboe 文件是当前历史文档，而非分版本归档。因此交易日行按 VIX 常规时段
    收盘时间过滤，但适配器无法重建旧 ``as_of`` 时点尚未知晓的后续修订。
    每份结果都会保留这一警告。
    """

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        timeout_seconds: float = 15.0,
        stale_after: timedelta = timedelta(days=4),
        max_response_bytes: int = 4_000_000,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        if max_response_bytes < 1:
            raise ValueError("max_response_bytes must be positive")
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._stale_after = stale_after
        self._max_response_bytes = max_response_bytes
        self._now = now or (lambda: datetime.now(tz=UTC))

    async def fetch_vix_daily_history(
        self,
        *,
        as_of: datetime,
        cache: GlobalRiskCacheEntry | None = None,
    ) -> VIXDailyHistory:
        """仅返回纽约时间 16:15 收盘已经完成的 VIX 交易日。"""

        _require_aware(as_of, "as_of")
        if cache is not None and cache.source_url != CBOE_VIX_EOD_CSV_URL:
            raise GlobalRiskCacheError("cached VIX document belongs to another source")

        headers = {
            "Accept": "text/csv, application/csv;q=0.9, text/plain;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": "gribuki-trade/0.1 official-market-data-reader",
        }
        if cache is not None:
            if cache.etag:
                headers["If-None-Match"] = cache.etag
            if cache.last_modified:
                headers["If-Modified-Since"] = cache.last_modified

        response = await self._get(headers)
        fetched_at = _aware_now(self._now)
        revalidated = response.status_code == 304
        if revalidated:
            if cache is None:
                raise GlobalRiskCacheError(
                    "official VIX source returned 304 without a cached document"
                )
            body = cache.body
            etag = _header(response.headers, "ETag") or cache.etag
            last_modified = (
                _header(response.headers, "Last-Modified") or cache.last_modified
            )
        elif response.status_code == 200:
            _validate_content_type(response.headers)
            _validate_content_length(response.headers, self._max_response_bytes)
            body = response.content
            if not body:
                raise GlobalRiskSchemaError("official VIX CSV response is empty")
            if len(body) > self._max_response_bytes:
                raise GlobalRiskSchemaError("official VIX CSV exceeds configured size")
            etag = _header(response.headers, "ETag")
            last_modified = _header(response.headers, "Last-Modified")
        else:
            raise GlobalRiskHTTPStatusError(response.status_code)

        digest = hashlib.sha256(body).hexdigest()
        cache_entry = GlobalRiskCacheEntry(
            source_url=CBOE_VIX_EOD_CSV_URL,
            body=body,
            fetched_at=fetched_at,
            content_sha256=digest,
            etag=etag,
            last_modified=last_modified,
        )
        parsed = _parse_vix_csv(body, as_of=as_of)
        if parsed.latest_visible_session_date in parsed.invalid_ohlc_dates:
            latest_date = parsed.latest_visible_session_date
            assert latest_date is not None  # 成员关系检查收窄了运行时不变量。
            raise GlobalRiskSchemaError(
                "official VIX CSV latest visible row has inconsistent OHLC for "
                f"{latest_date.isoformat()}; refusing to fall back to an older close"
            )
        visible_bars = parsed.bars
        if not visible_bars:
            raise GlobalRiskNoDataError(
                "official VIX history has no completed US session visible at as_of"
            )

        latest = visible_bars[-1]
        stale = as_of - latest.available_at > self._stale_after
        warnings = [
            "VIX daily availability is anchored to the completed 16:15 New York session",
            "Cboe publishes current history without historical release vintages; old-as-of "
            "requests cannot reconstruct corrections learned later",
            "VIX is a volatility index observation, not a directly investable price",
        ]
        if parsed.invalid_ohlc_dates:
            dates = ", ".join(item.isoformat() for item in parsed.invalid_ohlc_dates)
            warnings.append(
                "Cboe source contains "
                f"{len(parsed.invalid_ohlc_dates)} older completed row(s) whose HIGH/LOW "
                "do not envelope OPEN/CLOSE; raw values were preserved in cache and the "
                f"affected dates were excluded: {dates}"
            )
        return VIXDailyHistory(
            as_of=as_of,
            bars=visible_bars,
            meta=GlobalRiskSourceMeta(
                source_id=CBOE_VIX_SOURCE_ID,
                source_url=CBOE_VIX_EOD_CSV_URL,
                available_at=latest.available_at,
                fetched_at=fetched_at,
                stale=stale,
                content_sha256=digest,
                etag=etag,
                last_modified=last_modified,
                cache_revalidated=revalidated,
                warnings=tuple(warnings),
                skipped_invalid_ohlc_rows=len(parsed.invalid_ohlc_dates),
            ),
            cache_entry=cache_entry,
        )

    async def _get(self, headers: Mapping[str, str]) -> httpx.Response:
        timeout = httpx.Timeout(self._timeout_seconds)
        try:
            if self._client is not None:
                return await self._client.get(
                    CBOE_VIX_EOD_CSV_URL,
                    headers=headers,
                    timeout=timeout,
                )
            async with httpx.AsyncClient(follow_redirects=False) as client:
                return await client.get(
                    CBOE_VIX_EOD_CSV_URL,
                    headers=headers,
                    timeout=timeout,
                )
        except httpx.TimeoutException:
            raise GlobalRiskTimeoutError("official VIX request timed out") from None
        except (httpx.HTTPError, OSError):
            raise GlobalRiskTransportError("official VIX request failed") from None


@dataclass(frozen=True, slots=True)
class _ParsedVIXCSV:
    bars: tuple[VIXDailyBar, ...]
    latest_visible_session_date: date | None
    invalid_ohlc_dates: tuple[date, ...]


def _parse_vix_csv(body: bytes, *, as_of: datetime) -> _ParsedVIXCSV:
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise GlobalRiskSchemaError("official VIX CSV is not UTF-8") from exc

    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    try:
        header = next(reader)
    except (StopIteration, csv.Error) as exc:
        raise GlobalRiskSchemaError("official VIX CSV has no readable header") from exc
    normalized_header = tuple(item.strip().upper() for item in header)
    if normalized_header != _EXPECTED_COLUMNS:
        raise GlobalRiskSchemaError(
            "official VIX CSV must contain exactly DATE,OPEN,HIGH,LOW,CLOSE in that order"
        )

    bars: list[VIXDailyBar] = []
    invalid_ohlc_dates: list[date] = []
    latest_visible_session_date: date | None = None
    previous_date: date | None = None
    try:
        rows: Sequence[list[str]] = tuple(reader)
    except csv.Error as exc:
        raise GlobalRiskSchemaError("official VIX CSV contains malformed quoting") from exc
    if not rows:
        raise GlobalRiskSchemaError("official VIX CSV contains no rows")
    for line_number, row in enumerate(rows, start=2):
        if len(row) != len(_EXPECTED_COLUMNS):
            raise GlobalRiskSchemaError(
                f"official VIX CSV row {line_number} has an unexpected field count"
            )
        session_date = _parse_date(row[0], line_number)
        if previous_date is not None and session_date <= previous_date:
            detail = "duplicate" if session_date == previous_date else "out-of-order"
            raise GlobalRiskSchemaError(
                f"official VIX CSV row {line_number} has a {detail} DATE"
            )
        previous_date = session_date
        available_at = datetime.combine(
            session_date,
            _VIX_REGULAR_CLOSE,
            tzinfo=_NEW_YORK,
        )
        # 未来行不能影响更早的时点请求。表头、字段数、日期有效性、唯一性和
        # 顺序仍采用关闭失败；只有对应美国交易日结束后才解析其数值。
        if available_at > as_of:
            continue
        latest_visible_session_date = session_date
        open_value, high, low, close = (
            _parse_value(value, column, line_number)
            for value, column in zip(row[1:], _EXPECTED_COLUMNS[1:], strict=True)
        )
        if high < max(open_value, low, close) or low > min(
            open_value, high, close
        ):
        # Cboe 的不可变历史文件至少包含一条开盘价高于最高价的旧记录。精确保留
        # 来源字节和数值，但不暴露语义无效的行情柱。
            invalid_ohlc_dates.append(session_date)
            continue
        bars.append(
            VIXDailyBar(
                session_date=session_date,
                open=open_value,
                high=high,
                low=low,
                close=close,
                observed_at=available_at,
                available_at=available_at,
            )
        )
    return _ParsedVIXCSV(
        bars=tuple(bars),
        latest_visible_session_date=latest_visible_session_date,
        invalid_ohlc_dates=tuple(invalid_ohlc_dates),
    )


def _parse_date(value: str, line_number: int) -> date:
    text = value.strip()
    formats = ("%m/%d/%Y", "%Y-%m-%d")
    for date_format in formats:
        try:
            return datetime.strptime(text, date_format).date()
        except ValueError:
            continue
    raise GlobalRiskSchemaError(
        f"official VIX CSV row {line_number} has an invalid DATE"
    )


def _parse_value(value: str, column: str, line_number: int) -> Decimal:
    text = value.strip()
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise GlobalRiskSchemaError(
            f"official VIX CSV row {line_number} has a non-numeric {column}"
        ) from exc
    if not parsed.is_finite() or parsed <= 0:
        raise GlobalRiskSchemaError(
            f"official VIX CSV row {line_number} has a non-positive {column}"
        )
    return parsed


def _validate_content_type(headers: httpx.Headers) -> None:
    value = _header(headers, "Content-Type")
    if value is None:
        raise GlobalRiskSchemaError("official VIX response omitted Content-Type")
    media_type = value.partition(";")[0].strip().casefold()
    if media_type not in _ACCEPTED_MEDIA_TYPES:
        raise GlobalRiskSchemaError("official VIX response is not CSV content")


def _validate_content_length(headers: httpx.Headers, maximum: int) -> None:
    value = _header(headers, "Content-Length")
    if value is None:
        return
    try:
        length = int(value)
    except ValueError as exc:
        raise GlobalRiskSchemaError("official VIX Content-Length is invalid") from exc
    if length < 0 or length > maximum:
        raise GlobalRiskSchemaError("official VIX CSV exceeds configured size")


def _header(headers: httpx.Headers, name: str) -> str | None:
    value = headers.get(name)
    return value.strip() if value is not None and value.strip() else None


def _aware_now(now: Callable[[], datetime]) -> datetime:
    value = now()
    _require_aware(value, "now()")
    return value


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
