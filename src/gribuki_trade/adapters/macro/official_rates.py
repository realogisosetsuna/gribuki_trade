"""外汇管理局美元/人民币中间价与官方 Shibor 数据的严格 HTTPS 适配器。"""

from __future__ import annotations

import json
import ssl
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from typing import Any, Final
from zoneinfo import ZoneInfo

import httpx

from gribuki_trade.ports.official_rates import (
    AsyncSafeCentralParityData,
    AsyncShiborData,
    FXQuoteConvention,
    InterestRateUnit,
    OfficialRateSourceMeta,
    SafeCentralParityHistory,
    SafeCentralParityHTTPStatusError,
    SafeCentralParityNoDataError,
    SafeCentralParitySchemaError,
    SafeCentralParityTimeoutError,
    SafeCentralParityTransportError,
    ShiborDailyObservation,
    ShiborHistory,
    ShiborHTTPStatusError,
    ShiborNoDataError,
    ShiborRate,
    ShiborSchemaError,
    ShiborTenor,
    ShiborTimeoutError,
    ShiborTransportError,
    USDCNYCentralParityObservation,
    content_sha256,
)

SAFE_USD_CNY_SOURCE_ID: Final = "SAFE_RMB_CENTRAL_PARITY_USD_CNY"
SAFE_CENTRAL_PARITY_URL: Final = (
    "https://www.safe.gov.cn/AppStructured/hlw/RMBQuery.do"
)
SHIBOR_SOURCE_ID: Final = "CFETS_SHIBOR_OFFICIAL_HISTORY"
SHIBOR_HISTORY_URL: Final = (
    "https://www.shibor.net.cn/ags/ms/cm-u-bk-shibor/ShiborHis"
)

_SAFE_HOSTS = frozenset({"www.safe.gov.cn"})
_SHIBOR_HOSTS = frozenset({"www.shibor.net.cn"})
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_SAFE_RELEASE_TIME = time(9, 15)
_SHIBOR_RELEASE_TIME = time(11, 0)
_MAX_QUERY_DAYS = 366

_SAFE_HEADERS = (
    "日期",
    "美元",
    "欧元",
    "日元",
    "港元",
    "英镑",
    "澳元",
    "新西兰元",
    "新加坡元",
    "瑞士法郎",
    "加元",
    "澳门元",
    "林吉特",
    "卢布",
    "兰特",
    "韩元",
    "迪拉姆",
    "里亚尔",
    "福林",
    "兹罗提",
    "丹麦克朗",
    "瑞典克朗",
    "挪威克朗",
    "里拉",
    "比索",
    "泰铢",
)
_SHIBOR_KEYS: tuple[tuple[ShiborTenor, str], ...] = (
    (ShiborTenor.OVERNIGHT, "ON"),
    (ShiborTenor.ONE_WEEK, "1W"),
    (ShiborTenor.TWO_WEEK, "2W"),
    (ShiborTenor.ONE_MONTH, "1M"),
    (ShiborTenor.THREE_MONTH, "3M"),
    (ShiborTenor.SIX_MONTH, "6M"),
    (ShiborTenor.NINE_MONTH, "9M"),
    (ShiborTenor.ONE_YEAR, "1Y"),
)
_SHIBOR_RECORD_KEYS = frozenset(
    {"showDateCN", "showDateEN", *(provider_key for _, provider_key in _SHIBOR_KEYS)}
)
_SHIBOR_TENOR_DAYS = {
    "O/N": "1",
    "1W": "7",
    "2W": "14",
    "1M": "30",
    "3M": "90",
    "6M": "180",
    "9M": "270",
    "1Y": "360",
}


class SafeCentralParityAdapter(AsyncSafeCentralParityData):
    """读取外汇管理局官方表格，仅暴露精确的美元/人民币行。"""

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        timeout_seconds: float = 15.0,
        max_response_bytes: int = 1_000_000,
        stale_after: timedelta = timedelta(days=4),
        now: Callable[[], datetime] | None = None,
    ) -> None:
        _validate_settings(timeout_seconds, max_response_bytes, stale_after)
        _validate_static_url(SAFE_CENTRAL_PARITY_URL, _SAFE_HOSTS)
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes
        self._stale_after = stale_after
        self._now = now or (lambda: datetime.now(tz=UTC))

    async def fetch_usd_cny_history(
        self,
        *,
        start_date: date,
        end_date: date,
        as_of: datetime,
    ) -> SafeCentralParityHistory:
        _validate_query(start_date, end_date, as_of)
        response = await self._get(
            params={
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "queryYN": "true",
            }
        )
        fetched_at = _aware_now(self._now)
        if response.status_code != 200:
            raise SafeCentralParityHTTPStatusError(response.status_code)
        _validate_response_origin(response, _SAFE_HOSTS, SafeCentralParitySchemaError)
        body = _validated_body(
            response,
            max_response_bytes=self._max_response_bytes,
            accepted_media_types=frozenset({"text/html", "application/xhtml+xml"}),
            error_type=SafeCentralParitySchemaError,
            label="SAFE central-parity HTML",
        )
        observations = _parse_safe_usd_cny(
            body,
            start_date=start_date,
            end_date=end_date,
        )
        visible = tuple(item for item in observations if item.available_at <= as_of)
        if not visible:
            raise SafeCentralParityNoDataError(
                "SAFE returned no USD/CNY central parity visible at as_of"
            )
        _ensure_published_before_fetch(
            visible[-1].available_at,
            fetched_at,
            SafeCentralParitySchemaError,
            "SAFE central-parity",
        )
        digest = content_sha256(body)
        latest = visible[-1]
        return SafeCentralParityHistory(
            as_of=as_of,
            start_date=start_date,
            end_date=end_date,
            observations=visible,
            meta=OfficialRateSourceMeta(
                source_id=SAFE_USD_CNY_SOURCE_ID,
                source_url=SAFE_CENTRAL_PARITY_URL,
                available_at=latest.available_at,
                fetched_at=fetched_at,
                stale=as_of - latest.available_at > self._stale_after,
                content_sha256=digest,
                warnings=(
                    "SAFE identifies China Foreign Exchange Trade System as the data source",
                    "SAFE displays USD as CNY per 100 USD; cny_per_usd is normalized by 100",
                    "09:15 Asia/Shanghai is the official scheduled publication time",
                    "the live table has no historical release vintages for later corrections",
                ),
            ),
        )

    async def _get(self, *, params: Mapping[str, str]) -> httpx.Response:
        headers = {
            "Accept": "text/html, application/xhtml+xml;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": "gribuki-trade/0.1 official-rates-reader",
        }
        timeout = httpx.Timeout(self._timeout_seconds)
        try:
            if self._client is not None:
                return await self._client.get(
                    SAFE_CENTRAL_PARITY_URL,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    follow_redirects=False,
                )
            async with httpx.AsyncClient(follow_redirects=False) as client:
                return await client.get(
                    SAFE_CENTRAL_PARITY_URL,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    follow_redirects=False,
                )
        except httpx.TimeoutException:
            raise SafeCentralParityTimeoutError("SAFE central-parity request timed out") from None
        except (httpx.HTTPError, OSError):
            raise SafeCentralParityTransportError(
                "SAFE central-parity request failed"
            ) from None


class OfficialShiborAdapter(AsyncShiborData):
    """读取 Shibor 官方下载页使用的 JSON 端点。"""

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        timeout_seconds: float = 15.0,
        max_response_bytes: int = 1_000_000,
        stale_after: timedelta = timedelta(days=4),
        now: Callable[[], datetime] | None = None,
    ) -> None:
        _validate_settings(timeout_seconds, max_response_bytes, stale_after)
        _validate_static_url(SHIBOR_HISTORY_URL, _SHIBOR_HOSTS)
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes
        self._stale_after = stale_after
        self._now = now or (lambda: datetime.now(tz=UTC))

    async def fetch_shibor_history(
        self,
        *,
        start_date: date,
        end_date: date,
        as_of: datetime,
    ) -> ShiborHistory:
        _validate_query(start_date, end_date, as_of)
        response = await self._get(
            params={
                "lang": "cn",
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
            }
        )
        fetched_at = _aware_now(self._now)
        if response.status_code != 200:
            raise ShiborHTTPStatusError(response.status_code)
        _validate_response_origin(response, _SHIBOR_HOSTS, ShiborSchemaError)
        body = _validated_body(
            response,
            max_response_bytes=self._max_response_bytes,
            accepted_media_types=frozenset({"application/json", "text/json"}),
            error_type=ShiborSchemaError,
            label="official Shibor JSON",
        )
        observations = _parse_shibor_json(
            body,
            start_date=start_date,
            end_date=end_date,
        )
        visible = tuple(item for item in observations if item.available_at <= as_of)
        if not visible:
            raise ShiborNoDataError("official Shibor source returned no row visible at as_of")
        _ensure_published_before_fetch(
            visible[-1].available_at,
            fetched_at,
            ShiborSchemaError,
            "official Shibor",
        )
        latest = visible[-1]
        return ShiborHistory(
            as_of=as_of,
            start_date=start_date,
            end_date=end_date,
            observations=visible,
            meta=OfficialRateSourceMeta(
                source_id=SHIBOR_SOURCE_ID,
                source_url=SHIBOR_HISTORY_URL,
                available_at=latest.available_at,
                fetched_at=fetched_at,
                stale=as_of - latest.available_at > self._stale_after,
                content_sha256=content_sha256(body),
                warnings=(
                    "values are annual percentage points, not decimal fractions",
                    "official convention is simple interest, ACT/360, T+0, unsecured wholesale",
                    "11:00 Asia/Shanghai is the official scheduled publication time",
                    "the official HTTPS server currently requires legacy-server-connect; "
                    "certificate and hostname verification remain enabled",
                    "the live endpoint has no historical release vintages for later corrections",
                ),
            ),
        )

    async def _get(self, *, params: Mapping[str, str]) -> httpx.Response:
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": "gribuki-trade/0.1 official-rates-reader",
        }
        timeout = httpx.Timeout(self._timeout_seconds)
        try:
            if self._client is not None:
                return await self._client.get(
                    SHIBOR_HISTORY_URL,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    follow_redirects=False,
                )
    # 官方服务器当前要求旧式 TLS 重协商。只允许客户端兼容位，绝不禁用 CA
    # 或主机名验证；请求必须固定在静态 HTTPS 主机允许列表中，并在发送前和
    # 收到响应后分别检查。
            ssl_context = ssl.create_default_context()
            legacy_server_connect = getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0)
            ssl_context.options |= legacy_server_connect
            transport = httpx.AsyncHTTPTransport(verify=ssl_context)
            async with httpx.AsyncClient(
                follow_redirects=False,
                transport=transport,
            ) as client:
                return await client.get(
                    SHIBOR_HISTORY_URL,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    follow_redirects=False,
                )
        except httpx.TimeoutException:
            raise ShiborTimeoutError("official Shibor request timed out") from None
        except (httpx.HTTPError, OSError):
            raise ShiborTransportError("official Shibor request failed") from None


class _SafeInfoTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[tuple[str, ...]] = []
        self._in_table = False
        self._table_depth = 0
        self._row: list[str] | None = None
        self._cell_parts: list[str] | None = None

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        attributes = dict(attrs)
        if tag == "table":
            if self._in_table:
                self._table_depth += 1
            elif attributes.get("id") == "InfoTable":
                self._in_table = True
                self._table_depth = 1
            return
        if not self._in_table or self._table_depth != 1:
            return
        if tag == "tr":
            if self._row is not None:
                raise SafeCentralParitySchemaError("SAFE InfoTable contains nested rows")
            self._row = []
        elif tag in {"th", "td"} and self._row is not None:
            if self._cell_parts is not None:
                raise SafeCentralParitySchemaError("SAFE InfoTable contains nested cells")
            self._cell_parts = []

    def handle_endtag(self, tag: str) -> None:
        if not self._in_table:
            return
        if tag == "table":
            self._table_depth -= 1
            if self._table_depth == 0:
                self._in_table = False
            return
        if self._table_depth != 1:
            return
        if tag in {"th", "td"} and self._cell_parts is not None:
            if self._row is None:
                raise SafeCentralParitySchemaError("SAFE InfoTable cell is outside a row")
            self._row.append(" ".join("".join(self._cell_parts).split()))
            self._cell_parts = None
        elif tag == "tr" and self._row is not None:
            if self._cell_parts is not None:
                raise SafeCentralParitySchemaError("SAFE InfoTable row has an unclosed cell")
            if self._row:
                self.rows.append(tuple(self._row))
            self._row = None

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)


def _parse_safe_usd_cny(
    body: bytes,
    *,
    start_date: date,
    end_date: date,
) -> tuple[USDCNYCentralParityObservation, ...]:
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SafeCentralParitySchemaError("SAFE HTML is not UTF-8") from exc
    parser = _SafeInfoTableParser()
    try:
        parser.feed(text)
        parser.close()
    except SafeCentralParitySchemaError:
        raise
    except Exception as exc:  # pragma: no cover - HTMLParser defensive boundary
        raise SafeCentralParitySchemaError("SAFE InfoTable is malformed") from exc
    if not parser.rows:
        raise SafeCentralParitySchemaError("SAFE response omitted InfoTable rows")
    if parser.rows[0] != _SAFE_HEADERS:
        raise SafeCentralParitySchemaError(
            "SAFE InfoTable schema must match the official 26-column currency table"
        )
    rows = parser.rows[1:]
    if not rows:
        raise SafeCentralParityNoDataError("SAFE InfoTable contains no observations")

    parsed_descending: list[USDCNYCentralParityObservation] = []
    previous_date: date | None = None
    for line_number, row in enumerate(rows, start=2):
        if len(row) != len(_SAFE_HEADERS):
            raise SafeCentralParitySchemaError(
                f"SAFE InfoTable row {line_number} has an unexpected field count"
            )
        session_date = _parse_iso_date(
            row[0],
            error_type=SafeCentralParitySchemaError,
            label=f"SAFE InfoTable row {line_number} date",
        )
        if not (start_date <= session_date <= end_date):
            raise SafeCentralParitySchemaError("SAFE returned a row outside the requested range")
        if previous_date is not None and session_date >= previous_date:
            detail = "duplicate" if session_date == previous_date else "out-of-order"
            raise SafeCentralParitySchemaError(
                f"SAFE InfoTable row {line_number} has a {detail} date"
            )
        previous_date = session_date
        raw_cny_per_100_usd = _parse_decimal(
            row[1],
            error_type=SafeCentralParitySchemaError,
            label=f"SAFE InfoTable row {line_number} USD value",
            positive=True,
        )
        available_at = datetime.combine(
            session_date,
            _SAFE_RELEASE_TIME,
            tzinfo=_SHANGHAI,
        )
        parsed_descending.append(
            USDCNYCentralParityObservation(
                session_date=session_date,
                base_currency="USD",
                quote_currency="CNY",
                quote_convention=FXQuoteConvention.QUOTE_CURRENCY_PER_BASE_CURRENCY,
                cny_per_usd=raw_cny_per_100_usd / Decimal("100"),
                source_base_amount_usd=Decimal("100"),
                source_quote_amount_cny=raw_cny_per_100_usd,
                observed_at=available_at,
                available_at=available_at,
            )
        )
    return tuple(reversed(parsed_descending))


def _parse_shibor_json(
    body: bytes,
    *,
    start_date: date,
    end_date: date,
) -> tuple[ShiborDailyObservation, ...]:
    try:
        payload = json.loads(body.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ShiborSchemaError("official Shibor response is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ShiborSchemaError("official Shibor JSON root must be an object")
    head = payload.get("head")
    data = payload.get("data")
    records = payload.get("records")
    if not isinstance(head, dict) or not isinstance(data, dict) or not isinstance(records, list):
        raise ShiborSchemaError("official Shibor JSON omitted head, data, or records")
    if head.get("rep_code") != "200" or head.get("provider") != "CWAP":
        raise ShiborSchemaError("official Shibor JSON has an unsuccessful or unknown envelope")
    if data.get("startDateCN") != start_date.isoformat() or data.get(
        "endDateCN"
    ) != end_date.isoformat():
        raise ShiborSchemaError("official Shibor JSON query dates do not match the request")
    _validate_shibor_tenor_config(data.get("baseCurveCfgList"))
    if not records:
        raise ShiborNoDataError("official Shibor JSON contains no records")

    parsed_descending: list[ShiborDailyObservation] = []
    previous_date: date | None = None
    for line_number, record in enumerate(records, start=1):
        if not isinstance(record, dict) or frozenset(record) != _SHIBOR_RECORD_KEYS:
            raise ShiborSchemaError(
                f"official Shibor record {line_number} has an unexpected schema"
            )
        session_date = _parse_iso_date(
            record["showDateCN"],
            error_type=ShiborSchemaError,
            label=f"official Shibor record {line_number} date",
        )
        if not (start_date <= session_date <= end_date):
            raise ShiborSchemaError("official Shibor returned a row outside the requested range")
        if previous_date is not None and session_date >= previous_date:
            detail = "duplicate" if session_date == previous_date else "out-of-order"
            raise ShiborSchemaError(
                f"official Shibor record {line_number} has a {detail} date"
            )
        previous_date = session_date
        rates = tuple(
            ShiborRate(
                tenor=tenor,
                value_percent=_parse_decimal(
                    record[provider_key],
                    error_type=ShiborSchemaError,
                    label=f"official Shibor record {line_number} {tenor.value}",
                    positive=False,
                ),
            )
            for tenor, provider_key in _SHIBOR_KEYS
        )
        available_at = datetime.combine(
            session_date,
            _SHIBOR_RELEASE_TIME,
            tzinfo=_SHANGHAI,
        )
        parsed_descending.append(
            ShiborDailyObservation(
                session_date=session_date,
                rates=rates,
                unit=InterestRateUnit.PERCENT_PER_ANNUM,
                day_count="ACT/360",
                settlement="T+0",
                observed_at=available_at,
                available_at=available_at,
            )
        )
    return tuple(reversed(parsed_descending))


def _validate_shibor_tenor_config(value: Any) -> None:
    if not isinstance(value, list) or len(value) != len(_SHIBOR_KEYS):
        raise ShiborSchemaError("official Shibor JSON must configure exactly eight tenors")
    observed: list[tuple[str, str, int]] = []
    for item in value:
        if not isinstance(item, dict):
            raise ShiborSchemaError("official Shibor tenor configuration is malformed")
        tenor = item.get("cfgItem")
        days = item.get("cfgItemEnNm")
        sequence = item.get("sqncCd")
        if not isinstance(tenor, str) or not isinstance(days, str) or not isinstance(
            sequence, int
        ):
            raise ShiborSchemaError("official Shibor tenor configuration is malformed")
        observed.append((tenor, days, sequence))
    expected = [
        (tenor.value, _SHIBOR_TENOR_DAYS[tenor.value], index)
        for index, (tenor, _) in enumerate(_SHIBOR_KEYS, start=1)
    ]
    if observed != expected:
        raise ShiborSchemaError("official Shibor tenor configuration changed unexpectedly")


def _validated_body(
    response: httpx.Response,
    *,
    max_response_bytes: int,
    accepted_media_types: frozenset[str],
    error_type: type[SafeCentralParitySchemaError] | type[ShiborSchemaError],
    label: str,
) -> bytes:
    content_type = response.headers.get("Content-Type")
    if content_type is None:
        raise error_type(f"{label} omitted Content-Type")
    media_type = content_type.partition(";")[0].strip().casefold()
    if media_type not in accepted_media_types:
        raise error_type(f"{label} has an unacceptable Content-Type")
    content_length = response.headers.get("Content-Length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except ValueError as exc:
            raise error_type(f"{label} Content-Length is invalid") from exc
        if declared_length < 0 or declared_length > max_response_bytes:
            raise error_type(f"{label} exceeds the configured size limit")
    body = response.content
    if not body:
        raise error_type(f"{label} is empty")
    if len(body) > max_response_bytes:
        raise error_type(f"{label} exceeds the configured size limit")
    return body


def _validate_response_origin(
    response: httpx.Response,
    allowed_hosts: frozenset[str],
    error_type: type[SafeCentralParitySchemaError] | type[ShiborSchemaError],
) -> None:
    url = response.request.url
    if url.scheme != "https" or url.host not in allowed_hosts:
        raise error_type("official-rate response origin is outside the HTTPS host allowlist")


def _validate_static_url(url: str, allowed_hosts: frozenset[str]) -> None:
    parsed = httpx.URL(url)
    if parsed.scheme != "https" or parsed.host not in allowed_hosts:
        raise RuntimeError("official-rate endpoint is outside its HTTPS host allowlist")


def _validate_query(start_date: date, end_date: date, as_of: datetime) -> None:
    _require_aware(as_of, "as_of")
    if start_date > end_date:
        raise ValueError("start_date cannot follow end_date")
    if (end_date - start_date).days > _MAX_QUERY_DAYS:
        raise ValueError("official-rate query interval cannot exceed 366 days")


def _validate_settings(
    timeout_seconds: float,
    max_response_bytes: int,
    stale_after: timedelta,
) -> None:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if max_response_bytes < 1:
        raise ValueError("max_response_bytes must be positive")
    if stale_after <= timedelta(0):
        raise ValueError("stale_after must be positive")


def _parse_iso_date(
    value: Any,
    *,
    error_type: type[SafeCentralParitySchemaError] | type[ShiborSchemaError],
    label: str,
) -> date:
    if not isinstance(value, str):
        raise error_type(f"{label} is not text")
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise error_type(f"{label} is not YYYY-MM-DD") from exc


def _parse_decimal(
    value: Any,
    *,
    error_type: type[SafeCentralParitySchemaError] | type[ShiborSchemaError],
    label: str,
    positive: bool,
) -> Decimal:
    if not isinstance(value, str) or not value.strip():
        raise error_type(f"{label} is missing")
    try:
        parsed = Decimal(value.strip())
    except (InvalidOperation, ValueError) as exc:
        raise error_type(f"{label} is not numeric") from exc
    if not parsed.is_finite() or (parsed <= 0 if positive else parsed < 0):
        qualifier = "positive" if positive else "non-negative"
        raise error_type(f"{label} must be finite and {qualifier}")
    if not positive and parsed > Decimal("100"):
        raise error_type(f"{label} exceeds 100 annual percentage points")
    return parsed


def _ensure_published_before_fetch(
    available_at: datetime,
    fetched_at: datetime,
    error_type: type[SafeCentralParitySchemaError] | type[ShiborSchemaError],
    label: str,
) -> None:
    if available_at > fetched_at:
        raise error_type(f"{label} row is scheduled after fetched_at")


def _aware_now(now: Callable[[], datetime]) -> datetime:
    value = now()
    _require_aware(value, "now()")
    return value


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
