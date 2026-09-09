"""上交所官方 ETF 份额与期权收盘风险的只读适配器。"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime
from typing import Final, cast

import httpx

from gribuki_trade.ports.ashare_derivatives import (
    SSEETFShareHTTPStatusError,
    SSEETFShareNoDataError,
    SSEETFShareNotVisibleError,
    SSEETFShareObservation,
    SSEETFShareSchemaError,
    SSEETFShareTimeoutError,
    SSEETFShareTransportError,
    SSEOfficialSourceMeta,  # noqa: F401 - 保留历史适配器导出
    SSEOptionRiskContract,  # noqa: F401 - 保留历史适配器导出
    SSEOptionRiskHTTPStatusError,
    SSEOptionRiskNoDataError,
    SSEOptionRiskNotVisibleError,
    SSEOptionRiskSchemaError,
    SSEOptionRiskSnapshot,
    SSEOptionRiskTimeoutError,
    SSEOptionRiskTransportError,
    digest_official_payload,  # noqa: F401 - 保留历史适配器导出
)

from . import derivatives_parsing as _derivatives_parsing
from .derivatives_parsing import (
    _assert_first_seen_visible,
    _eligible_etf_share_rows,
    _JSONObject,
    _OfficialDocument,
    _option_snapshot,
    _parse_etf_share_row,
    _parse_option_rows,
    _RequestHTTPStatus,
    _RequestNotVisible,
    _RequestSchema,
    _RequestTimeout,
    _RequestTransport,
    _require_aware,
    _resolve_underlying_name,
    _result_rows,
    _validate_json_content_type,
    _validate_optional_as_of,
    _validate_request_visibility,
    _validate_symbol,
)

SSE_ETF_SHARE_SOURCE_ID = _derivatives_parsing.SSE_ETF_SHARE_SOURCE_ID
SSE_OPTION_RISK_SOURCE_ID = _derivatives_parsing.SSE_OPTION_RISK_SOURCE_ID

SSE_QUERY_URL: Final = "https://query.sse.com.cn/commonQuery.do"
SSE_OPTION_RISK_PAGE_URL: Final = "https://www.sse.com.cn/assortment/options/risk/"
SSE_ETF_SCALE_PAGE_URL_TEMPLATE: Final = (
    "https://www.sse.com.cn/assortment/fund/list/etfinfo/scale/"
    "index.shtml?FUNDID={symbol}"
)

_OPTION_UNDERLYING_SQL_ID = "SSE_ZQPZ_YSP_SOPTZSXT_BASIC_INFO_YSHQ_BDZQDM_L"
_OPTION_RISK_SQL_ID = "SSE_ZQPZ_YSP_GGQQZSXT_YSHQ_QQFXZB_DATE_L"
_ETF_SHARE_EXACT_SQL_ID = "COMMON_SSE_ZQPZ_ETFZL_ETFJBXX_JJGM_SEARCH_L"
_ETF_SHARE_LATEST_SQL_ID = "COMMON_SSE_ZQPZ_ETFZL_ETFJBXX_JJGM_MOREN_L"
_OPTION_MAX_RESPONSE_BYTES = 4_000_000
_ETF_MAX_RESPONSE_BYTES = 1_000_000



class SSEOptionRiskAdapter:
    """按日期获取上交所官方收盘希腊字母和隐含波动率。"""

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
        """返回精确交易日，或带明确日期的最新回退。

        回退使用交易所未过滤的最新文档，再按精确六位标的代码过滤。保留实际
        ``TRADE_DATE``，绝不改写为 ``requested_date``。实时采集时省略 ``as_of``，
        此时首次可见性从 ``fetched_at`` 开始；提供历史截止时点则关闭失败。
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
        # 精确查询本应给出相同结果；将两种官方查询模式之间的不一致视为漂移。
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
    """获取上交所官方结算后 ETF 精确总份额。"""

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
        """返回精确份额或明确更早的观测。

        上交所字段 ``TOT_VOL`` 在每日结算后以万份发布。它不会转换为基金资产，
        也不会标记为申购/赎回流量。实时采集时省略 ``as_of``；历史重放则提供
        明确截止时点，以便关闭失败。
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
