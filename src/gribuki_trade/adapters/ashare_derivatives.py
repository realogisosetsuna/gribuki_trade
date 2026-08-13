"""Read-only adapters for official SSE ETF shares and option closing risk."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Final, TypeAlias, cast
from zoneinfo import ZoneInfo

import httpx

from gribuki_trade.ports.ashare_derivatives import (
    SSEETFShareHTTPStatusError,
    SSEETFShareNoDataError,
    SSEETFShareNotVisibleError,
    SSEETFShareObservation,
    SSEETFShareSchemaError,
    SSEETFShareTimeoutError,
    SSEETFShareTransportError,
    SSEOfficialSourceMeta,
    SSEOptionRiskContract,
    SSEOptionRiskHTTPStatusError,
    SSEOptionRiskNoDataError,
    SSEOptionRiskNotVisibleError,
    SSEOptionRiskSchemaError,
    SSEOptionRiskSnapshot,
    SSEOptionRiskTimeoutError,
    SSEOptionRiskTransportError,
    digest_official_payload,
)

SSE_QUERY_URL: Final = "https://query.sse.com.cn/commonQuery.do"
SSE_OPTION_RISK_PAGE_URL: Final = "https://www.sse.com.cn/assortment/options/risk/"
SSE_ETF_SCALE_PAGE_URL_TEMPLATE: Final = (
    "https://www.sse.com.cn/assortment/fund/list/etfinfo/scale/"
    "index.shtml?FUNDID={symbol}"
)
SSE_OPTION_RISK_SOURCE_ID: Final = "SSE_OPTION_CLOSING_RISK"
SSE_ETF_SHARE_SOURCE_ID: Final = "SSE_ETF_POST_SETTLEMENT_TOTAL_SHARES"

_OPTION_UNDERLYING_SQL_ID = "SSE_ZQPZ_YSP_SOPTZSXT_BASIC_INFO_YSHQ_BDZQDM_L"
_OPTION_RISK_SQL_ID = "SSE_ZQPZ_YSP_GGQQZSXT_YSHQ_QQFXZB_DATE_L"
_ETF_SHARE_EXACT_SQL_ID = "COMMON_SSE_ZQPZ_ETFZL_ETFJBXX_JJGM_SEARCH_L"
_ETF_SHARE_LATEST_SQL_ID = "COMMON_SSE_ZQPZ_ETFZL_ETFJBXX_JJGM_MOREN_L"

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_SYMBOL_PATTERN = re.compile(r"^\d{6}$")
_SECURITY_ID_PATTERN = re.compile(r"^\d{8}$")
_CONTRACT_ID_PATTERN = re.compile(r"^(?P<underlying>\d{6})(?P<side>[CP])\d{4}[A-Z]\d{5}$")
_JSON_MEDIA_TYPES = frozenset({"application/json", "text/json", "text/plain"})
_OPTION_MAX_RESPONSE_BYTES = 4_000_000
_ETF_MAX_RESPONSE_BYTES = 1_000_000

_JSONObject: TypeAlias = dict[str, object]


@dataclass(frozen=True, slots=True)
class _OfficialDocument:
    payload: _JSONObject
    body: bytes
    source_url: str
    fetched_at: datetime


class _RequestError(RuntimeError):
    pass


class _RequestTimeout(_RequestError):
    pass


class _RequestTransport(_RequestError):
    pass


class _RequestNotVisible(_RequestError):
    pass


class _RequestSchema(_RequestError):
    pass


class _RequestHTTPStatus(_RequestError):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(str(status_code))


class SSEOptionRiskAdapter:
    """Fetch official SSE closing Greeks and implied volatility by date."""

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        timeout_seconds: float = 15.0,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._now = now or (lambda: datetime.now(tz=UTC))

    async def fetch_option_risk(
        self,
        underlying_symbol: str,
        requested_date: date,
        *,
        as_of: datetime | None = None,
        allow_latest_available: bool = True,
    ) -> SSEOptionRiskSnapshot:
        """Return an exact session or an explicitly dated latest fallback.

        The fallback uses the exchange's unfiltered latest document and then
        filters by the exact six-digit underlying code.  Its actual
        ``TRADE_DATE`` is retained and is never rewritten as ``requested_date``.
        Omit ``as_of`` for a live collection; first-seen visibility then starts
        at ``fetched_at``.  Supplying a historical cutoff is fail-closed.
        """

        symbol = _validate_symbol(underlying_symbol)
        _validate_optional_as_of(as_of)
        try:
            underlying_document = await self._request(
                params={
                    "isPagination": "true",
                    "sqlId": _OPTION_UNDERLYING_SQL_ID,
                },
                referer=SSE_OPTION_RISK_PAGE_URL,
                maximum_bytes=_OPTION_MAX_RESPONSE_BYTES,
            )
            effective_cutoff = as_of or underlying_document.fetched_at
            _validate_request_visibility(requested_date, effective_cutoff)
            _assert_first_seen_visible(underlying_document.fetched_at, as_of)
            underlying_name = _resolve_underlying_name(underlying_document.payload, symbol)

            exact_document = await self._request(
                params={
                    "isPagination": "false",
                    "trade_date": requested_date.strftime("%Y%m%d"),
                    "sqlId": _OPTION_RISK_SQL_ID,
                    "contractSymbol": underlying_name,
                },
                referer=SSE_OPTION_RISK_PAGE_URL,
                maximum_bytes=_OPTION_MAX_RESPONSE_BYTES,
            )
            _assert_first_seen_visible(exact_document.fetched_at, as_of)
            exact_rows = _result_rows(exact_document.payload)
            if exact_rows:
                contracts, observed_date = _parse_option_rows(
                    exact_rows,
                    symbol=symbol,
                    requested_date=requested_date,
                    allow_other_underlyings=False,
                )
                if observed_date != requested_date:
                    raise SSEOptionRiskSchemaError(
                        "exact-date SSE option response returned a different TRADE_DATE"
                    )
                return _option_snapshot(
                    symbol=symbol,
                    underlying_name=underlying_name,
                    requested_date=requested_date,
                    observed_date=observed_date,
                    contracts=contracts,
                    document=exact_document,
                    fallback=False,
                )

            if not allow_latest_available:
                raise SSEOptionRiskNoDataError(
                    f"official SSE option risk has no rows for {symbol} on {requested_date}"
                )

            latest_document = await self._request(
                params={
                    "isPagination": "false",
                    "trade_date": "",
                    "sqlId": _OPTION_RISK_SQL_ID,
                    "contractSymbol": "",
                },
                referer=SSE_OPTION_RISK_PAGE_URL,
                maximum_bytes=_OPTION_MAX_RESPONSE_BYTES,
            )
            _assert_first_seen_visible(latest_document.fetched_at, as_of)
            latest_rows = _result_rows(latest_document.payload)
            contracts, observed_date = _parse_option_rows(
                latest_rows,
                symbol=symbol,
                requested_date=requested_date,
                allow_other_underlyings=True,
            )
            if observed_date >= requested_date:
                # Equality would have been supplied by the exact query.  Treat
                # disagreement between the two official query modes as drift.
                raise SSEOptionRiskSchemaError(
                    "latest SSE option document conflicts with empty exact-date response"
                )
            return _option_snapshot(
                symbol=symbol,
                underlying_name=underlying_name,
                requested_date=requested_date,
                observed_date=observed_date,
                contracts=contracts,
                document=latest_document,
                fallback=True,
            )
        except SSEOptionRiskDataErrors:
            raise
        except _RequestTimeout as exc:
            raise SSEOptionRiskTimeoutError("official SSE option-risk request timed out") from exc
        except _RequestTransport as exc:
            raise SSEOptionRiskTransportError(
                "official SSE option-risk endpoint could not be reached"
            ) from exc
        except _RequestHTTPStatus as exc:
            raise SSEOptionRiskHTTPStatusError(exc.status_code) from exc
        except _RequestNotVisible as exc:
            raise SSEOptionRiskNotVisibleError(
                "official SSE option-risk document was first seen after as_of"
            ) from exc
        except _RequestSchema as exc:
            raise SSEOptionRiskSchemaError(str(exc)) from exc

    async def _request(
        self,
        *,
        params: Mapping[str, str],
        referer: str,
        maximum_bytes: int,
    ) -> _OfficialDocument:
        return await _request_document(
            self._client,
            params=params,
            referer=referer,
            timeout_seconds=self._timeout_seconds,
            maximum_bytes=maximum_bytes,
            now=self._now,
        )


class SSEETFShareAdapter:
    """Fetch exact official SSE post-settlement ETF total shares."""

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        timeout_seconds: float = 15.0,
        latest_page_size: int = 100,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if latest_page_size < 1 or latest_page_size > 1000:
            raise ValueError("latest_page_size must be between 1 and 1000")
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._latest_page_size = latest_page_size
        self._now = now or (lambda: datetime.now(tz=UTC))

    async def fetch_etf_shares(
        self,
        symbol: str,
        requested_date: date,
        *,
        as_of: datetime | None = None,
        allow_latest_available: bool = True,
    ) -> SSEETFShareObservation:
        """Return exact shares or an explicitly older observation.

        The SSE field ``TOT_VOL`` is published in ten-thousand shares after
        daily settlement.  It is not converted into fund assets or labelled as
        subscription/redemption flow.  Omit ``as_of`` for live collection or
        provide an explicit historical cutoff for fail-closed replay.
        """

        validated_symbol = _validate_symbol(symbol)
        _validate_optional_as_of(as_of)
        referer = SSE_ETF_SCALE_PAGE_URL_TEMPLATE.format(symbol=validated_symbol)
        try:
            exact_document = await self._request(
                params={
                    "isPagination": "false",
                    "sqlId": _ETF_SHARE_EXACT_SQL_ID,
                    "SEC_CODE": validated_symbol,
                    "STAT_DATE": requested_date.isoformat(),
                },
                referer=referer,
            )
            effective_cutoff = as_of or exact_document.fetched_at
            _validate_request_visibility(requested_date, effective_cutoff)
            _assert_first_seen_visible(exact_document.fetched_at, as_of)
            exact_rows = _result_rows(exact_document.payload)
            if exact_rows:
                if len(exact_rows) != 1:
                    raise SSEETFShareSchemaError(
                        "exact-date SSE ETF-share response contains multiple rows"
                    )
                return _parse_etf_share_row(
                    exact_rows[0],
                    symbol=validated_symbol,
                    requested_date=requested_date,
                    document=exact_document,
                    fallback=False,
                )

            if not allow_latest_available:
                raise SSEETFShareNoDataError(
                    f"official SSE ETF shares have no row for {validated_symbol} "
                    f"on {requested_date}"
                )

            latest_document = await self._request(
                params={
                    "isPagination": "true",
                    "sqlId": _ETF_SHARE_LATEST_SQL_ID,
                    "SEC_CODE": validated_symbol,
                    "pageHelp.pageSize": str(self._latest_page_size),
                },
                referer=referer,
            )
            _assert_first_seen_visible(latest_document.fetched_at, as_of)
            eligible = _eligible_etf_share_rows(
                _result_rows(latest_document.payload),
                symbol=validated_symbol,
                requested_date=requested_date,
            )
            if not eligible:
                raise SSEETFShareNoDataError(
                    "official SSE ETF latest-share page has no observation at or before "
                    f"{requested_date}"
                )
            observed_date, row = max(eligible, key=lambda item: item[0])
            if observed_date == requested_date:
                raise SSEETFShareSchemaError(
                    "latest SSE ETF-share page conflicts with empty exact-date response"
                )
            return _parse_etf_share_row(
                row,
                symbol=validated_symbol,
                requested_date=requested_date,
                document=latest_document,
                fallback=True,
            )
        except SSEETFShareDataErrors:
            raise
        except _RequestTimeout as exc:
            raise SSEETFShareTimeoutError("official SSE ETF-share request timed out") from exc
        except _RequestTransport as exc:
            raise SSEETFShareTransportError(
                "official SSE ETF-share endpoint could not be reached"
            ) from exc
        except _RequestHTTPStatus as exc:
            raise SSEETFShareHTTPStatusError(exc.status_code) from exc
        except _RequestNotVisible as exc:
            raise SSEETFShareNotVisibleError(
                "official SSE ETF-share document was first seen after as_of"
            ) from exc
        except _RequestSchema as exc:
            raise SSEETFShareSchemaError(str(exc)) from exc

    async def _request(
        self,
        *,
        params: Mapping[str, str],
        referer: str,
    ) -> _OfficialDocument:
        return await _request_document(
            self._client,
            params=params,
            referer=referer,
            timeout_seconds=self._timeout_seconds,
            maximum_bytes=_ETF_MAX_RESPONSE_BYTES,
            now=self._now,
        )


SSEOptionRiskDataErrors = (
    SSEOptionRiskHTTPStatusError,
    SSEOptionRiskNoDataError,
    SSEOptionRiskNotVisibleError,
    SSEOptionRiskSchemaError,
    SSEOptionRiskTimeoutError,
    SSEOptionRiskTransportError,
)
SSEETFShareDataErrors = (
    SSEETFShareHTTPStatusError,
    SSEETFShareNoDataError,
    SSEETFShareNotVisibleError,
    SSEETFShareSchemaError,
    SSEETFShareTimeoutError,
    SSEETFShareTransportError,
)


async def _request_document(
    client: httpx.AsyncClient | None,
    *,
    params: Mapping[str, str],
    referer: str,
    timeout_seconds: float,
    maximum_bytes: int,
    now: Callable[[], datetime],
) -> _OfficialDocument:
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Referer": referer,
        "User-Agent": "gribuki-trade/0.1 official-sse-research-reader",
    }
    timeout = httpx.Timeout(timeout_seconds)
    try:
        if client is not None:
            response = await client.get(
                SSE_QUERY_URL,
                params=params,
                headers=headers,
                timeout=timeout,
            )
        else:
            async with httpx.AsyncClient(follow_redirects=False) as local_client:
                response = await local_client.get(
                    SSE_QUERY_URL,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                )
    except httpx.TimeoutException as exc:
        raise _RequestTimeout from exc
    except httpx.HTTPError as exc:
        raise _RequestTransport from exc

    if response.status_code != 200:
        raise _RequestHTTPStatus(response.status_code)
    _validate_json_content_type(response.headers)
    body = response.content
    if not body:
        raise _RequestSchema("official SSE JSON response is empty")
    if len(body) > maximum_bytes:
        raise _RequestSchema("official SSE JSON response exceeds configured size")
    content_length = response.headers.get("Content-Length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except ValueError as exc:
            raise _RequestSchema("official SSE Content-Length is invalid") from exc
        if declared_length < 0 or declared_length > maximum_bytes:
            raise _RequestSchema("official SSE JSON response exceeds configured size")
    try:
        decoded = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _RequestSchema("official SSE response is not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise _RequestSchema("official SSE JSON root must be an object")
    payload = cast(_JSONObject, decoded)
    fetched_at = now()
    _require_aware(fetched_at, "now()")
    return _OfficialDocument(
        payload=payload,
        body=body,
        source_url=str(response.request.url),
        fetched_at=fetched_at,
    )


def _resolve_underlying_name(payload: _JSONObject, symbol: str) -> str:
    rows = _result_rows(payload)
    matches: list[str] = []
    seen_codes: set[str] = set()
    for row in rows:
        code = _required_text(row, "UNDERLYING_SECURITY_ID")
        name = _required_text(row, "UNDERLYING_SYMBOL")
        if not _SYMBOL_PATTERN.fullmatch(code):
            raise SSEOptionRiskSchemaError("SSE option underlying code has invalid format")
        if code in seen_codes:
            raise SSEOptionRiskSchemaError("SSE option underlying list contains duplicates")
        seen_codes.add(code)
        if code == symbol:
            matches.append(name)
    if not matches:
        raise SSEOptionRiskNoDataError(f"{symbol} is not an SSE option underlying")
    if len(matches) != 1:
        raise SSEOptionRiskSchemaError("SSE option underlying lookup is ambiguous")
    return matches[0]


def _parse_option_rows(
    rows: Sequence[_JSONObject],
    *,
    symbol: str,
    requested_date: date,
    allow_other_underlyings: bool,
) -> tuple[tuple[SSEOptionRiskContract, ...], date]:
    selected: list[_JSONObject] = []
    for row in rows:
        contract_id = _required_text(row, "CONTRACT_ID")
        match = _CONTRACT_ID_PATTERN.fullmatch(contract_id)
        if match is None:
            raise SSEOptionRiskSchemaError("SSE option CONTRACT_ID has invalid format")
        if match.group("underlying") == symbol:
            selected.append(row)
        elif not allow_other_underlyings:
            raise SSEOptionRiskSchemaError(
                "filtered SSE option response contains another underlying"
            )
    if not selected:
        raise SSEOptionRiskNoDataError(
            f"official SSE latest option-risk document has no rows for {symbol}"
        )

    contracts: list[SSEOptionRiskContract] = []
    observed_dates: set[date] = set()
    security_ids: set[str] = set()
    contract_ids: set[str] = set()
    for row in selected:
        security_id = _required_text(row, "SECURITY_ID")
        contract_id = _required_text(row, "CONTRACT_ID")
        contract_symbol = _required_text(row, "CONTRACT_SYMBOL")
        contract_type = _required_text(row, "CONTRACT_TYPE")
        trade_date = _parse_date(_required_text(row, "TRADE_DATE"), "TRADE_DATE")
        if trade_date > requested_date:
            raise SSEOptionRiskNoDataError(
                "official SSE option-risk observation is later than requested_date"
            )
        observed_dates.add(trade_date)
        if not _SECURITY_ID_PATTERN.fullmatch(security_id):
            raise SSEOptionRiskSchemaError("SSE option SECURITY_ID has invalid format")
        if security_id in security_ids or contract_id in contract_ids:
            raise SSEOptionRiskSchemaError("SSE option-risk response contains duplicate contracts")
        security_ids.add(security_id)
        contract_ids.add(contract_id)
        contract_match = _CONTRACT_ID_PATTERN.fullmatch(contract_id)
        assert contract_match is not None
        expected_type = "认购" if contract_match.group("side") == "C" else "认沽"
        if contract_type != expected_type:
            raise SSEOptionRiskSchemaError(
                "SSE option CONTRACT_TYPE conflicts with CONTRACT_ID"
            )

        raw_numeric = tuple(
            (field_name, _required_text(row, field_name))
            for field_name in (
                "DELTA_VALUE",
                "THETA_VALUE",
                "GAMMA_VALUE",
                "VEGA_VALUE",
                "RHO_VALUE",
                "IMPLC_VOLATLTY",
            )
        )
        parsed = {
            name: _parse_decimal(value, name)
            for name, value in raw_numeric
        }
        try:
            contract = SSEOptionRiskContract(
                security_id=security_id,
                contract_id=contract_id,
                contract_symbol=contract_symbol,
                contract_type=contract_type,
                delta=parsed["DELTA_VALUE"],
                theta=parsed["THETA_VALUE"],
                gamma=parsed["GAMMA_VALUE"],
                vega=parsed["VEGA_VALUE"],
                rho=parsed["RHO_VALUE"],
                implied_volatility=parsed["IMPLC_VOLATLTY"],
                raw_fields=raw_numeric,
            )
        except ValueError as exc:
            raise SSEOptionRiskSchemaError(str(exc)) from exc
        contracts.append(contract)

    if len(observed_dates) != 1:
        raise SSEOptionRiskSchemaError(
            "SSE option-risk response mixes multiple TRADE_DATE values"
        )
    return tuple(contracts), next(iter(observed_dates))


def _option_snapshot(
    *,
    symbol: str,
    underlying_name: str,
    requested_date: date,
    observed_date: date,
    contracts: tuple[SSEOptionRiskContract, ...],
    document: _OfficialDocument,
    fallback: bool,
) -> SSEOptionRiskSnapshot:
    warnings = [
        "SSE does not expose a machine-readable release timestamp; available_at "
        "is this collector's first-seen time",
        "risk values are calculated from official session close data and are not "
        "intraday Greeks",
        "IMPLC_VOLATLTY is retained exactly as the exchange publishes it; zero is "
        "not rewritten as missing",
    ]
    if fallback:
        warnings.append(
            f"requested {requested_date.isoformat()} was unavailable; retained actual "
            f"latest TRADE_DATE {observed_date.isoformat()}"
        )
    return SSEOptionRiskSnapshot(
        underlying_symbol=symbol,
        underlying_name=underlying_name,
        contracts=contracts,
        meta=_metadata(
            source_id=SSE_OPTION_RISK_SOURCE_ID,
            document=document,
            requested_date=requested_date,
            observed_date=observed_date,
            fallback=fallback,
            warnings=tuple(warnings),
        ),
    )


def _eligible_etf_share_rows(
    rows: Sequence[_JSONObject],
    *,
    symbol: str,
    requested_date: date,
) -> tuple[tuple[date, _JSONObject], ...]:
    eligible: list[tuple[date, _JSONObject]] = []
    seen_dates: set[date] = set()
    for row in rows:
        row_symbol = _required_text(row, "SEC_CODE")
        if row_symbol != symbol:
            raise SSEETFShareSchemaError(
                "SSE ETF-share response contains a different SEC_CODE"
            )
        observed_date = _parse_date(_required_text(row, "STAT_DATE"), "STAT_DATE")
        if observed_date in seen_dates:
            raise SSEETFShareSchemaError("SSE ETF-share response contains duplicate dates")
        seen_dates.add(observed_date)
        if observed_date <= requested_date:
            eligible.append((observed_date, row))
    return tuple(eligible)


def _parse_etf_share_row(
    row: _JSONObject,
    *,
    symbol: str,
    requested_date: date,
    document: _OfficialDocument,
    fallback: bool,
) -> SSEETFShareObservation:
    row_symbol = _required_text(row, "SEC_CODE")
    if row_symbol != symbol:
        raise SSEETFShareSchemaError("SSE ETF-share SEC_CODE does not match request")
    observed_date = _parse_date(_required_text(row, "STAT_DATE"), "STAT_DATE")
    if observed_date > requested_date:
        raise SSEETFShareNoDataError(
            "official SSE ETF-share observation is later than requested_date"
        )
    if fallback == (observed_date == requested_date):
        raise SSEETFShareSchemaError("ETF share fallback flag conflicts with STAT_DATE")
    raw_total_shares = _required_text(row, "TOT_VOL")
    total_shares_ten_thousands = _parse_decimal(raw_total_shares, "TOT_VOL")
    if total_shares_ten_thousands < 0:
        raise SSEETFShareSchemaError("SSE ETF TOT_VOL cannot be negative")
    expanded_name = _optional_text(row, "FUND_EXPANSION_ABBR")
    etf_type = _optional_text(row, "ETF_TYPE")
    warnings = [
        "SSE does not expose a machine-readable release timestamp; available_at "
        "is this collector's first-seen time",
        "TOT_VOL is post-settlement total shares in units of 10,000; it is not "
        "fund NAV, assets under management, or subscription/redemption flow",
    ]
    if fallback:
        warnings.append(
            f"requested {requested_date.isoformat()} was unavailable; retained actual "
            f"latest STAT_DATE {observed_date.isoformat()}"
        )
    try:
        return SSEETFShareObservation(
            symbol=symbol,
            name=_required_text(row, "SEC_NAME"),
            expanded_name=expanded_name,
            etf_type=etf_type,
            total_shares_ten_thousands=total_shares_ten_thousands,
            total_shares=total_shares_ten_thousands * Decimal("10000"),
            raw_total_shares=raw_total_shares,
            meta=_metadata(
                source_id=SSE_ETF_SHARE_SOURCE_ID,
                document=document,
                requested_date=requested_date,
                observed_date=observed_date,
                fallback=fallback,
                warnings=tuple(warnings),
            ),
        )
    except ValueError as exc:
        raise SSEETFShareSchemaError(str(exc)) from exc


def _metadata(
    *,
    source_id: str,
    document: _OfficialDocument,
    requested_date: date,
    observed_date: date,
    fallback: bool,
    warnings: tuple[str, ...],
) -> SSEOfficialSourceMeta:
    return SSEOfficialSourceMeta(
        source_id=source_id,
        source_url=document.source_url,
        requested_date=requested_date,
        observed_date=observed_date,
        # No exchange release timestamp is exposed.  First seen and fetched are
        # intentionally identical rather than backdating visibility to session close.
        available_at=document.fetched_at,
        fetched_at=document.fetched_at,
        exact_date_match=not fallback,
        latest_available_fallback=fallback,
        content_sha256=digest_official_payload(document.body),
        warnings=warnings,
    )


def _result_rows(payload: _JSONObject) -> tuple[_JSONObject, ...]:
    action_errors = payload.get("actionErrors")
    if action_errors not in (None, []):
        raise _RequestSchema("official SSE response contains actionErrors")
    result = payload.get("result")
    if not isinstance(result, list):
        raise _RequestSchema("official SSE response result must be an array")
    rows: list[_JSONObject] = []
    for item in result:
        if not isinstance(item, dict):
            raise _RequestSchema("official SSE result rows must be objects")
        rows.append(cast(_JSONObject, item))
    return tuple(rows)


def _required_text(row: Mapping[str, object], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str) or not value.strip():
        raise _RequestSchema(f"official SSE row has missing or invalid {name}")
    return value.strip()


def _optional_text(row: Mapping[str, object], name: str) -> str | None:
    value = row.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise _RequestSchema(f"official SSE row has invalid {name}")
    stripped = value.strip()
    return stripped or None


def _parse_decimal(value: str, field_name: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise _RequestSchema(f"official SSE {field_name} is not numeric") from exc
    if not parsed.is_finite():
        raise _RequestSchema(f"official SSE {field_name} must be finite")
    return parsed


def _parse_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise _RequestSchema(f"official SSE {field_name} is not YYYY-MM-DD") from exc


def _validate_json_content_type(headers: httpx.Headers) -> None:
    value = headers.get("Content-Type")
    if value is None:
        raise _RequestSchema("official SSE response omitted Content-Type")
    media_type = value.partition(";")[0].strip().casefold()
    if media_type not in _JSON_MEDIA_TYPES:
        raise _RequestSchema("official SSE response is not JSON content")


def _validate_symbol(value: str) -> str:
    symbol = value.strip().upper().removesuffix(".SH")
    if _SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise ValueError("SSE symbol must be six digits, optionally followed by .SH")
    return symbol


def _validate_request_visibility(requested_date: date, as_of: datetime) -> None:
    _require_aware(as_of, "as_of")
    if requested_date > as_of.astimezone(_SHANGHAI).date():
        raise ValueError("requested_date cannot be later than the Shanghai as_of date")


def _assert_first_seen_visible(
    fetched_at: datetime,
    as_of: datetime | None,
) -> None:
    if as_of is not None and fetched_at > as_of:
        raise _RequestNotVisible


def _validate_optional_as_of(as_of: datetime | None) -> None:
    if as_of is not None:
        _require_aware(as_of, "as_of")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
