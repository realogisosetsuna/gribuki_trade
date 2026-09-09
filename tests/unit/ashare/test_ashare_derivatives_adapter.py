import asyncio
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from gribuki_trade.adapters.ashare.market.derivatives import (
    SSE_QUERY_URL,
    SSEETFShareAdapter,
    SSEOptionRiskAdapter,
)
from gribuki_trade.ports.ashare_derivatives import (
    AsyncSSEETFShareData,
    AsyncSSEOptionRiskData,
    SSEETFShareHTTPStatusError,
    SSEETFShareNoDataError,
    SSEETFShareNotVisibleError,
    SSEETFShareSchemaError,
    SSEETFShareTimeoutError,
    SSEOptionRiskHTTPStatusError,
    SSEOptionRiskNoDataError,
    SSEOptionRiskNotVisibleError,
    SSEOptionRiskSchemaError,
    SSEOptionRiskTimeoutError,
)

FETCHED_AT = datetime(2026, 8, 14, 0, 0, tzinfo=UTC)
AS_OF = datetime(2026, 8, 14, 0, 1, tzinfo=UTC)

# 冻结并裁剪上交所官网页面使用的公开响应字段；所有数值均保留为字符串，
# 以便测试能够发现精度损失。
UNDERLYINGS = {
    "actionErrors": [],
    "result": [
        {
            "UNDERLYING_SYMBOL": "50ETF",
            "UNDERLYING_SECURITY_ID": "510050",
            "EXPIRE_DATE": "2026-08,2026-09",
        },
        {
            "UNDERLYING_SYMBOL": "300ETF",
            "UNDERLYING_SECURITY_ID": "510300",
            "EXPIRE_DATE": "2026-08,2026-09",
        },
    ],
}

OPTION_ROWS_20260813 = [
    {
        "CONTRACT_TYPE": "认购",
        "DELTA_VALUE": "0.613",
        "SECURITY_ID": "10011870",
        "THETA_VALUE": "-0.747",
        "CONTRACT_SYMBOL": "300ETF购8月4700",
        "GAMMA_VALUE": "3.095",
        "VEGA_VALUE": "0.342",
        "TRADE_DATE": "2026-08-13",
        "RHO_VALUE": "0.101",
        "CONTRACT_ID": "510300C2608M04700",
        "IMPLC_VOLATLTY": "0.139",
    },
    {
        "CONTRACT_TYPE": "认沽",
        "DELTA_VALUE": "-0.387",
        "SECURITY_ID": "10011890",
        "THETA_VALUE": "-0.681",
        "CONTRACT_SYMBOL": "300ETF沽8月4700",
        "GAMMA_VALUE": "3.095",
        "VEGA_VALUE": "0.342",
        "TRADE_DATE": "2026-08-13",
        "RHO_VALUE": "-0.064",
        "CONTRACT_ID": "510300P2608M04700",
        "IMPLC_VOLATLTY": "0.141",
    },
]

ETF_ROW_20260812 = {
    "STAT_DATE": "2026-08-12",
    "ETF_TYPE": "跨市",
    "SEC_CODE": "510300",
    "SEC_NAME": "300ETF",
    "TOT_VOL": "2416818.77",
    "FUND_EXPANSION_ABBR": "沪深300ETF华泰柏瑞",
}


def _response(
    request: httpx.Request,
    payload: object,
    *,
    status: int = 200,
    content_type: str = "application/json;charset=UTF-8",
) -> httpx.Response:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    return httpx.Response(
        status,
        content=body,
        headers={"Content-Type": content_type},
        request=request,
    )


def _run_option(
    handler: httpx.MockTransport,
    *,
    requested_date: date = date(2026, 8, 13),
    as_of: datetime | None = AS_OF,
    allow_latest_available: bool = True,
):
    async def run():
        async with httpx.AsyncClient(transport=handler, follow_redirects=False) as client:
            adapter = SSEOptionRiskAdapter(client, now=lambda: FETCHED_AT)
            return await adapter.fetch_option_risk(
                "510300.SH",
                requested_date,
                as_of=as_of,
                allow_latest_available=allow_latest_available,
            )

    return asyncio.run(run())


def _run_etf(
    handler: httpx.MockTransport,
    *,
    requested_date: date = date(2026, 8, 12),
    as_of: datetime | None = AS_OF,
    allow_latest_available: bool = True,
):
    async def run():
        async with httpx.AsyncClient(transport=handler, follow_redirects=False) as client:
            adapter = SSEETFShareAdapter(client, now=lambda: FETCHED_AT)
            return await adapter.fetch_etf_shares(
                "510300.SH",
                requested_date,
                as_of=as_of,
                allow_latest_available=allow_latest_available,
            )

    return asyncio.run(run())


def _payload_handler(function):
    """把载荷选择函数转换为 httpx 冻结夹具处理器。"""

    def handler(request: httpx.Request) -> httpx.Response:
        risk_request = "BASIC_INFO" not in request.url.params["sqlId"]
        return _response(request, function(request, risk_request))

    return handler


def test_option_exact_date_preserves_precision_provenance_and_zero_semantics() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        sql_id = request.url.params["sqlId"]
        payload = UNDERLYINGS if "BASIC_INFO" in sql_id else {
            "actionErrors": [],
            "result": [
                OPTION_ROWS_20260813[0],
                {**OPTION_ROWS_20260813[1], "IMPLC_VOLATLTY": "0.000"},
            ],
        }
        return _response(request, payload)

    snapshot = _run_option(httpx.MockTransport(handler))

    assert all(str(request.url).startswith(SSE_QUERY_URL) for request in requests)
    assert requests[0].headers["Referer"].endswith("/assortment/options/risk/")
    assert requests[1].url.params["trade_date"] == "20260813"
    assert requests[1].url.params["contractSymbol"] == "300ETF"
    assert snapshot.underlying_symbol == "510300"
    assert snapshot.underlying_name == "300ETF"
    assert snapshot.meta.requested_date == date(2026, 8, 13)
    assert snapshot.meta.observed_date == date(2026, 8, 13)
    assert snapshot.meta.exact_date_match is True
    assert snapshot.meta.latest_available_fallback is False
    assert snapshot.meta.available_at == FETCHED_AT
    assert snapshot.meta.fetched_at == FETCHED_AT
    assert len(snapshot.meta.content_sha256) == 64
    assert "trade_date=20260813" in snapshot.meta.source_url
    assert snapshot.contracts[0].delta == Decimal("0.613")
    assert snapshot.contracts[0].delta.as_tuple().exponent == -3
    assert snapshot.contracts[0].implied_volatility == Decimal("0.139")
    assert snapshot.contracts[1].implied_volatility == Decimal("0.000")
    assert snapshot.contracts[1].implied_volatility.as_tuple().exponent == -3
    assert dict(snapshot.contracts[1].raw_fields)["IMPLC_VOLATLTY"] == "0.000"
    assert "zero is not rewritten as missing" in " ".join(snapshot.meta.warnings)


def test_option_latest_available_keeps_actual_date_and_filters_exact_underlying() -> None:
    other_underlying = {
        **OPTION_ROWS_20260813[0],
        "SECURITY_ID": "10011111",
        "CONTRACT_ID": "510050C2608M02750",
        "CONTRACT_SYMBOL": "50ETF购8月2750",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        sql_id = request.url.params["sqlId"]
        if "BASIC_INFO" in sql_id:
            return _response(request, UNDERLYINGS)
        if request.url.params["trade_date"]:
            return _response(request, {"actionErrors": [], "result": []})
        return _response(
            request,
            {"actionErrors": [], "result": [other_underlying, *OPTION_ROWS_20260813]},
        )

    snapshot = _run_option(
        httpx.MockTransport(handler),
        requested_date=date(2026, 8, 14),
    )

    assert snapshot.meta.requested_date == date(2026, 8, 14)
    assert snapshot.meta.observed_date == date(2026, 8, 13)
    assert snapshot.meta.exact_date_match is False
    assert snapshot.meta.latest_available_fallback is True
    assert {item.contract_id[:6] for item in snapshot.contracts} == {"510300"}
    assert "actual latest TRADE_DATE 2026-08-13" in " ".join(snapshot.meta.warnings)


def test_option_empty_exact_can_fail_without_requesting_fallback() -> None:
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        payload = UNDERLYINGS if request_count == 1 else {"actionErrors": [], "result": []}
        return _response(request, payload)

    with pytest.raises(SSEOptionRiskNoDataError, match="no rows"):
        _run_option(
            httpx.MockTransport(handler),
            allow_latest_available=False,
        )
    assert request_count == 2


def test_option_rejects_future_latest_row_and_malformed_implied_volatility() -> None:
    future_row = {**OPTION_ROWS_20260813[0], "TRADE_DATE": "2026-08-15"}

    def future_handler(request: httpx.Request) -> httpx.Response:
        sql_id = request.url.params["sqlId"]
        if "BASIC_INFO" in sql_id:
            return _response(request, UNDERLYINGS)
        result = [] if request.url.params["trade_date"] else [future_row]
        return _response(request, {"actionErrors": [], "result": result})

    with pytest.raises(SSEOptionRiskNoDataError, match="later than requested_date"):
        _run_option(
            httpx.MockTransport(future_handler),
            requested_date=date(2026, 8, 14),
        )

    def malformed_handler(request: httpx.Request) -> httpx.Response:
        sql_id = request.url.params["sqlId"]
        if "BASIC_INFO" in sql_id:
            return _response(request, UNDERLYINGS)
        bad = {**OPTION_ROWS_20260813[0], "IMPLC_VOLATLTY": "not-a-number"}
        return _response(request, {"actionErrors": [], "result": [bad]})

    with pytest.raises(SSEOptionRiskSchemaError, match="IMPLC_VOLATLTY is not numeric"):
        _run_option(httpx.MockTransport(malformed_handler))


def test_option_contract_type_and_duplicate_rows_fail_closed() -> None:
    bad_type = {**OPTION_ROWS_20260813[0], "CONTRACT_TYPE": "认沽"}

    @_payload_handler
    def bad_type_handler(request: httpx.Request, risk_request: bool) -> object:
        return {"actionErrors": [], "result": [bad_type]} if risk_request else UNDERLYINGS

    with pytest.raises(SSEOptionRiskSchemaError, match="conflicts with CONTRACT_ID"):
        _run_option(httpx.MockTransport(bad_type_handler))

    @_payload_handler
    def duplicate_handler(request: httpx.Request, risk_request: bool) -> object:
        result = [OPTION_ROWS_20260813[0], OPTION_ROWS_20260813[0]]
        return {"actionErrors": [], "result": result} if risk_request else UNDERLYINGS

    with pytest.raises(SSEOptionRiskSchemaError, match="duplicate contracts"):
        _run_option(httpx.MockTransport(duplicate_handler))

def test_etf_exact_date_preserves_official_units_and_raw_precision() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _response(request, {"actionErrors": [], "result": [ETF_ROW_20260812]})

    observation = _run_etf(httpx.MockTransport(handler))

    assert len(requests) == 1
    assert requests[0].url.params["STAT_DATE"] == "2026-08-12"
    assert requests[0].headers["Referer"].endswith("FUNDID=510300")
    assert observation.symbol == "510300"
    assert observation.name == "300ETF"
    assert observation.expanded_name == "沪深300ETF华泰柏瑞"
    assert observation.etf_type == "跨市"
    assert observation.raw_total_shares == "2416818.77"
    assert observation.total_shares_ten_thousands == Decimal("2416818.77")
    assert observation.total_shares_ten_thousands.as_tuple().exponent == -2
    assert observation.total_shares == Decimal("24168187700.00")
    assert observation.meta.observed_date == date(2026, 8, 12)
    assert observation.meta.exact_date_match is True
    assert observation.meta.available_at == FETCHED_AT
    assert "not fund NAV" in " ".join(observation.meta.warnings)


def test_live_collection_without_as_of_starts_visibility_at_fetch_time() -> None:
    observation = _run_etf(
        httpx.MockTransport(
            lambda request: _response(
                request,
                {"actionErrors": [], "result": [ETF_ROW_20260812]},
            )
        ),
        as_of=None,
    )

    assert observation.meta.available_at == FETCHED_AT
    assert observation.meta.fetched_at == FETCHED_AT


def test_etf_latest_available_selects_latest_eligible_date_without_masquerading() -> None:
    future = {**ETF_ROW_20260812, "STAT_DATE": "2026-08-14", "TOT_VOL": "2500000.00"}
    older = {**ETF_ROW_20260812, "STAT_DATE": "2026-08-11", "TOT_VOL": "2482428.77"}

    def handler(request: httpx.Request) -> httpx.Response:
        if "SEARCH_L" in request.url.params["sqlId"]:
            return _response(request, {"actionErrors": [], "result": []})
        return _response(
            request,
            {"actionErrors": [], "result": [future, older, ETF_ROW_20260812]},
        )

    observation = _run_etf(
        httpx.MockTransport(handler),
        requested_date=date(2026, 8, 13),
    )

    assert observation.meta.requested_date == date(2026, 8, 13)
    assert observation.meta.observed_date == date(2026, 8, 12)
    assert observation.meta.latest_available_fallback is True
    assert observation.raw_total_shares == "2416818.77"
    assert "actual latest STAT_DATE 2026-08-12" in " ".join(observation.meta.warnings)


def test_etf_no_eligible_fallback_and_duplicate_dates_are_typed() -> None:
    future = {**ETF_ROW_20260812, "STAT_DATE": "2026-08-14"}

    def no_data(request: httpx.Request) -> httpx.Response:
        result = [] if "SEARCH_L" in request.url.params["sqlId"] else [future]
        return _response(request, {"actionErrors": [], "result": result})

    with pytest.raises(SSEETFShareNoDataError, match="at or before"):
        _run_etf(
            httpx.MockTransport(no_data),
            requested_date=date(2026, 8, 13),
        )

    def duplicate(request: httpx.Request) -> httpx.Response:
        result = (
            []
            if "SEARCH_L" in request.url.params["sqlId"]
            else [ETF_ROW_20260812, ETF_ROW_20260812]
        )
        return _response(request, {"actionErrors": [], "result": result})

    with pytest.raises(SSEETFShareSchemaError, match="duplicate dates"):
        _run_etf(
            httpx.MockTransport(duplicate),
            requested_date=date(2026, 8, 13),
        )


def test_etf_wrong_code_negative_shares_and_exact_duplicates_fail_closed() -> None:
    cases = [
        (
            [{**ETF_ROW_20260812, "SEC_CODE": "510500"}],
            "SEC_CODE does not match",
        ),
        ([{**ETF_ROW_20260812, "TOT_VOL": "-1.00"}], "cannot be negative"),
        ([ETF_ROW_20260812, ETF_ROW_20260812], "multiple rows"),
    ]
    for rows, message in cases:
        with pytest.raises(SSEETFShareSchemaError, match=message):
            _run_etf(
                httpx.MockTransport(
                    lambda request, rows=rows: _response(
                        request,
                        {"actionErrors": [], "result": rows},
                    )
                )
            )


@pytest.mark.parametrize(
    ("runner", "error_type"),
    [
        (_run_option, SSEOptionRiskHTTPStatusError),
        (_run_etf, SSEETFShareHTTPStatusError),
    ],
)
def test_endpoint_http_failures_remain_source_specific(runner, error_type) -> None:
    with pytest.raises(error_type) as raised:
        runner(
            httpx.MockTransport(
                lambda request: _response(request, {}, status=503)
            )
        )
    assert raised.value.status_code == 503


@pytest.mark.parametrize(
    ("runner", "error_type"),
    [
        (_run_option, SSEOptionRiskTimeoutError),
        (_run_etf, SSEETFShareTimeoutError),
    ],
)
def test_endpoint_timeouts_remain_source_specific(runner, error_type) -> None:
    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("frozen timeout", request=request)

    with pytest.raises(error_type):
        runner(httpx.MockTransport(timeout))


@pytest.mark.parametrize(
    ("runner", "error_type"),
    [
        (_run_option, SSEOptionRiskNotVisibleError),
        (_run_etf, SSEETFShareNotVisibleError),
    ],
)
def test_first_seen_after_as_of_is_never_backdated(runner, error_type) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if runner is _run_option:
            return _response(request, UNDERLYINGS)
        return _response(request, {"actionErrors": [], "result": [ETF_ROW_20260812]})

    with pytest.raises(error_type, match="first seen after as_of"):
        runner(
            httpx.MockTransport(handler),
            as_of=FETCHED_AT.replace(microsecond=0) - timedelta(microseconds=1),
        )


def test_protocols_and_request_validation() -> None:
    assert isinstance(SSEOptionRiskAdapter(), AsyncSSEOptionRiskData)
    assert isinstance(SSEETFShareAdapter(), AsyncSSEETFShareData)
    with pytest.raises(ValueError, match="timeout_seconds"):
        SSEOptionRiskAdapter(timeout_seconds=0)
    with pytest.raises(ValueError, match="latest_page_size"):
        SSEETFShareAdapter(latest_page_size=0)

    async def run() -> None:
        with pytest.raises(ValueError, match="six digits"):
            await SSEETFShareAdapter().fetch_etf_shares(
                "not-a-symbol",
                date(2026, 8, 13),
                as_of=AS_OF,
            )
        with pytest.raises(ValueError, match="timezone-aware"):
            await SSEOptionRiskAdapter().fetch_option_risk(
                "510300",
                date(2026, 8, 13),
                as_of=datetime(2026, 8, 14),
            )

    asyncio.run(run())
