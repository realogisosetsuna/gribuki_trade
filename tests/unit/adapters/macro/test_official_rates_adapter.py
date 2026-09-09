import asyncio
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest

from gribuki_trade.adapters.macro.official_rates import (
    SAFE_CENTRAL_PARITY_URL,
    SAFE_USD_CNY_SOURCE_ID,
    SHIBOR_HISTORY_URL,
    SHIBOR_SOURCE_ID,
    OfficialShiborAdapter,
    SafeCentralParityAdapter,
)
from gribuki_trade.ports.official_rates import (
    AsyncSafeCentralParityData,
    AsyncShiborData,
    FXQuoteConvention,
    InterestRateUnit,
    SafeCentralParityHistory,
    SafeCentralParityHTTPStatusError,
    SafeCentralParityNoDataError,
    SafeCentralParitySchemaError,
    SafeCentralParityTimeoutError,
    SafeCentralParityTransportError,
    ShiborHistory,
    ShiborHTTPStatusError,
    ShiborNoDataError,
    ShiborSchemaError,
    ShiborTenor,
    ShiborTimeoutError,
    ShiborTransportError,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
FETCHED_AT = datetime(2026, 8, 13, 13, 30, tzinfo=UTC)

SAFE_HEADERS = (
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
SAFE_ROWS = (
    (
        "2026-08-13",
        "678.88",
        "780.14",
        "4.2508",
        "86.518",
        "913.55",
        "477.97",
        "396.52",
        "528.82",
        "832.59",
        "485.26",
        "119.11",
        "60.297",
        "1227.52",
        "238.34",
        "20958.0",
        "54.293",
        "55.499",
        "4663.06",
        "55.127",
        "95.83",
        "141.6",
        "140.38",
        "705.547",
        "251.71",
        "489.82",
    ),
    (
        "2026-08-12",
        "678.82",
        "780.95",
        "4.2548",
        "86.515",
        "914.07",
        "477.93",
        "397.67",
        "528.84",
        "834.61",
        "485.74",
        "119.11",
        "60.4",
        "1222.56",
        "239.12",
        "20916.0",
        "54.31",
        "55.519",
        "4670.7",
        "55.014",
        "95.74",
        "140.75",
        "140.37",
        "705.36",
        "251.92",
        "490.62",
    ),
)


def _safe_html(
    *,
    headers: tuple[str, ...] = SAFE_HEADERS,
    rows: tuple[tuple[str, ...], ...] = SAFE_ROWS,
) -> bytes:
    header_html = "".join(f"<th>{item}</th>" for item in headers)
    rows_html = "".join(
        "<tr>" + "".join(f"<td>{item}</td>" for item in row) + "</tr>"
        for row in rows
    )
    return (
        "<!doctype html><html><head><meta charset='utf-8'></head><body>"
        f"<table class='list' id='InfoTable'><tr>{header_html}</tr>{rows_html}</table>"
        "</body></html>"
    ).encode()


SHIBOR_CONFIG = [
    {
        "cfgItem": tenor,
        "cfgItemEnNm": days,
        "sqncCd": sequence,
    }
    for sequence, (tenor, days) in enumerate(
        (
            ("O/N", "1"),
            ("1W", "7"),
            ("2W", "14"),
            ("1M", "30"),
            ("3M", "90"),
            ("6M", "180"),
            ("9M", "270"),
            ("1Y", "360"),
        ),
        start=1,
    )
]
SHIBOR_RECORDS: list[dict[str, str]] = [
    {
        "9M": "1.4700",
        "2W": "1.4097",
        "1W": "1.3991",
        "6M": "1.4500",
        "1Y": "1.4800",
        "3M": "1.4300",
        "1M": "1.4157",
        "showDateEN": "13 Aug 2026",
        "showDateCN": "2026-08-13",
        "ON": "1.3670",
    },
    {
        "9M": "1.4700",
        "2W": "1.4069",
        "1W": "1.3981",
        "6M": "1.4500",
        "1Y": "1.4810",
        "3M": "1.4300",
        "1M": "1.4157",
        "showDateEN": "12 Aug 2026",
        "showDateCN": "2026-08-12",
        "ON": "1.3712",
    },
]


def _shibor_json(
    *,
    records: list[dict[str, str]] | None = None,
    config: list[dict[str, str | int]] | None = None,
    start_date: str = "2026-08-12",
    end_date: str = "2026-08-13",
) -> bytes:
    payload: dict[str, Any] = {
        "head": {"version": "2.0", "provider": "CWAP", "rep_code": "200"},
        "data": {
            "baseCurveCfgList": SHIBOR_CONFIG if config is None else config,
            "startDateCN": start_date,
            "endDateCN": end_date,
        },
        "records": SHIBOR_RECORDS if records is None else records,
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()


FROZEN_SAFE_HTML = _safe_html()
FROZEN_SHIBOR_JSON = _shibor_json()


def _response(
    request: httpx.Request,
    *,
    body: bytes,
    content_type: str,
    status: int = 200,
    request_override: httpx.Request | None = None,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    response_headers = {"Content-Type": content_type}
    if headers:
        response_headers.update(headers)
    return httpx.Response(
        status,
        content=body,
        headers=response_headers,
        request=request if request_override is None else request_override,
    )


def _run_safe(
    handler: httpx.MockTransport,
    *,
    as_of: datetime = FETCHED_AT,
    now: datetime = FETCHED_AT,
    start_date: date = date(2026, 8, 12),
    end_date: date = date(2026, 8, 13),
    max_response_bytes: int = 1_000_000,
) -> SafeCentralParityHistory:
    async def run() -> SafeCentralParityHistory:
        async with httpx.AsyncClient(transport=handler) as client:
            adapter = SafeCentralParityAdapter(
                client,
                now=lambda: now,
                max_response_bytes=max_response_bytes,
            )
            return await adapter.fetch_usd_cny_history(
                start_date=start_date,
                end_date=end_date,
                as_of=as_of,
            )

    return asyncio.run(run())


def _run_shibor(
    handler: httpx.MockTransport,
    *,
    as_of: datetime = FETCHED_AT,
    now: datetime = FETCHED_AT,
    start_date: date = date(2026, 8, 12),
    end_date: date = date(2026, 8, 13),
    max_response_bytes: int = 1_000_000,
) -> ShiborHistory:
    async def run() -> ShiborHistory:
        async with httpx.AsyncClient(transport=handler) as client:
            adapter = OfficialShiborAdapter(
                client,
                now=lambda: now,
                max_response_bytes=max_response_bytes,
            )
            return await adapter.fetch_shibor_history(
                start_date=start_date,
                end_date=end_date,
                as_of=as_of,
            )

    return asyncio.run(run())


def test_safe_quote_is_normalized_without_losing_source_convention() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _response(request, body=FROZEN_SAFE_HTML, content_type="text/html;charset=UTF-8")

    history = _run_safe(httpx.MockTransport(handler))

    assert requests[0].url.copy_with(query=None) == httpx.URL(SAFE_CENTRAL_PARITY_URL)
    assert dict(requests[0].url.params) == {
        "startDate": "2026-08-12",
        "endDate": "2026-08-13",
        "queryYN": "true",
    }
    assert requests[0].method == "GET"
    assert tuple(item.session_date for item in history.observations) == (
        date(2026, 8, 12),
        date(2026, 8, 13),
    )
    latest = history.observations[-1]
    assert latest.base_currency == "USD"
    assert latest.quote_currency == "CNY"
    assert latest.quote_convention is FXQuoteConvention.QUOTE_CURRENCY_PER_BASE_CURRENCY
    assert latest.source_base_amount_usd == Decimal("100")
    assert latest.source_quote_amount_cny == Decimal("678.88")
    assert latest.cny_per_usd == Decimal("6.7888")
    assert latest.available_at.isoformat() == "2026-08-13T09:15:00+08:00"
    assert history.meta.source_id == SAFE_USD_CNY_SOURCE_ID
    assert history.meta.source_url == SAFE_CENTRAL_PARITY_URL
    assert "normalized by 100" in " ".join(history.meta.warnings)


def test_shibor_exact_eight_tenors_units_and_conventions() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _response(
            request,
            body=FROZEN_SHIBOR_JSON,
            content_type="application/json;charset=UTF-8",
        )

    history = _run_shibor(httpx.MockTransport(handler))

    assert requests[0].url.copy_with(query=None) == httpx.URL(SHIBOR_HISTORY_URL)
    assert dict(requests[0].url.params) == {
        "lang": "cn",
        "startDate": "2026-08-12",
        "endDate": "2026-08-13",
    }
    latest = history.observations[-1]
    assert tuple(item.tenor for item in latest.rates) == tuple(ShiborTenor)
    assert latest.rate(ShiborTenor.OVERNIGHT) == Decimal("1.3670")
    assert latest.rate(ShiborTenor.ONE_WEEK) == Decimal("1.3991")
    assert latest.rate(ShiborTenor.TWO_WEEK) == Decimal("1.4097")
    assert latest.rate(ShiborTenor.ONE_MONTH) == Decimal("1.4157")
    assert latest.rate(ShiborTenor.THREE_MONTH) == Decimal("1.4300")
    assert latest.rate(ShiborTenor.SIX_MONTH) == Decimal("1.4500")
    assert latest.rate(ShiborTenor.NINE_MONTH) == Decimal("1.4700")
    assert latest.rate(ShiborTenor.ONE_YEAR) == Decimal("1.4800")
    assert latest.unit is InterestRateUnit.PERCENT_PER_ANNUM
    assert latest.day_count == "ACT/360"
    assert latest.settlement == "T+0"
    assert latest.available_at.isoformat() == "2026-08-13T11:00:00+08:00"
    assert history.meta.source_id == SHIBOR_SOURCE_ID
    assert history.meta.source_url == SHIBOR_HISTORY_URL
    assert "not decimal fractions" in " ".join(history.meta.warnings)


def test_point_in_time_filter_uses_0915_and_1100_release_times() -> None:
    safe = _run_safe(
        httpx.MockTransport(
            lambda request: _response(
                request,
                body=FROZEN_SAFE_HTML,
                content_type="text/html",
            )
        ),
        as_of=datetime(2026, 8, 13, 10, 59, tzinfo=SHANGHAI),
    )
    shibor = _run_shibor(
        httpx.MockTransport(
            lambda request: _response(
                request,
                body=FROZEN_SHIBOR_JSON,
                content_type="application/json",
            )
        ),
        as_of=datetime(2026, 8, 13, 10, 59, tzinfo=SHANGHAI),
    )

    assert safe.observations[-1].session_date == date(2026, 8, 13)
    assert shibor.observations[-1].session_date == date(2026, 8, 12)


def test_prepublication_as_of_has_typed_no_data_per_source() -> None:
    safe_body = _safe_html(rows=(SAFE_ROWS[0],))
    shibor_body = _shibor_json(records=[SHIBOR_RECORDS[0]], start_date="2026-08-13")
    with pytest.raises(SafeCentralParityNoDataError):
        _run_safe(
            httpx.MockTransport(
                lambda request: _response(request, body=safe_body, content_type="text/html")
            ),
            start_date=date(2026, 8, 13),
            as_of=datetime(2026, 8, 13, 9, 14, 59, tzinfo=SHANGHAI),
        )
    with pytest.raises(ShiborNoDataError):
        _run_shibor(
            httpx.MockTransport(
                lambda request: _response(
                    request,
                    body=shibor_body,
                    content_type="application/json",
                )
            ),
            start_date=date(2026, 8, 13),
            as_of=datetime(2026, 8, 13, 10, 59, 59, tzinfo=SHANGHAI),
        )


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (_safe_html(headers=SAFE_HEADERS[:-1]), "26-column"),
        (_safe_html(rows=(SAFE_ROWS[0], SAFE_ROWS[0])), "duplicate date"),
        (_safe_html(rows=(SAFE_ROWS[1], SAFE_ROWS[0])), "out-of-order date"),
        (
            _safe_html(rows=((SAFE_ROWS[0][0], "not-a-number", *SAFE_ROWS[0][2:]),)),
            "not numeric",
        ),
    ],
)
def test_safe_schema_drift_duplicates_order_and_bad_values_fail_closed(
    body: bytes,
    message: str,
) -> None:
    with pytest.raises(SafeCentralParitySchemaError, match=message):
        _run_safe(
            httpx.MockTransport(
                lambda request: _response(request, body=body, content_type="text/html")
            )
        )


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (
            _shibor_json(config=SHIBOR_CONFIG[:-1]),
            "exactly eight tenors",
        ),
        (
            _shibor_json(records=[SHIBOR_RECORDS[0], SHIBOR_RECORDS[0]]),
            "duplicate date",
        ),
        (
            _shibor_json(records=[SHIBOR_RECORDS[1], SHIBOR_RECORDS[0]]),
            "out-of-order date",
        ),
        (
            _shibor_json(
                records=[{**SHIBOR_RECORDS[0], "ON": "not-a-number"}],
            ),
            "not numeric",
        ),
        (
            _shibor_json(
                records=[{**SHIBOR_RECORDS[0], "unexpected": "field"}],
            ),
            "unexpected schema",
        ),
    ],
)
def test_shibor_schema_drift_duplicates_order_and_bad_values_fail_closed(
    body: bytes,
    message: str,
) -> None:
    with pytest.raises(ShiborSchemaError, match=message):
        _run_shibor(
            httpx.MockTransport(
                lambda request: _response(
                    request,
                    body=body,
                    content_type="application/json",
                )
            )
        )


@pytest.mark.parametrize("source", ["safe", "shibor"])
def test_content_type_size_and_https_origin_are_fail_closed(source: str) -> None:
    body = FROZEN_SAFE_HTML if source == "safe" else FROZEN_SHIBOR_JSON
    runner = _run_safe if source == "safe" else _run_shibor
    schema_error = SafeCentralParitySchemaError if source == "safe" else ShiborSchemaError

    with pytest.raises(schema_error, match="Content-Type"):
        runner(
            httpx.MockTransport(
                lambda request: _response(request, body=body, content_type="text/plain")
            )
        )
    with pytest.raises(schema_error, match="size limit"):
        runner(
            httpx.MockTransport(
                lambda request: _response(
                    request,
                    body=body,
                    content_type=("text/html" if source == "safe" else "application/json"),
                )
            ),
            max_response_bytes=len(body) - 1,
        )
    evil_request = httpx.Request("GET", "https://evil.example.invalid/data")

    class FixedResponseClient:
        async def get(self, *_args: Any, **_kwargs: Any) -> httpx.Response:
            return _response(
                evil_request,
                body=body,
                content_type=("text/html" if source == "safe" else "application/json"),
            )

    async def fetch_with_evil_origin() -> None:
        client: Any = FixedResponseClient()
        if source == "safe":
            adapter = SafeCentralParityAdapter(client, now=lambda: FETCHED_AT)
            await adapter.fetch_usd_cny_history(
                start_date=date(2026, 8, 12),
                end_date=date(2026, 8, 13),
                as_of=FETCHED_AT,
            )
        else:
            adapter = OfficialShiborAdapter(client, now=lambda: FETCHED_AT)
            await adapter.fetch_shibor_history(
                start_date=date(2026, 8, 12),
                end_date=date(2026, 8, 13),
                as_of=FETCHED_AT,
            )

    with pytest.raises(schema_error, match="host allowlist"):
        asyncio.run(fetch_with_evil_origin())


def test_http_status_failures_are_source_specific() -> None:
    with pytest.raises(SafeCentralParityHTTPStatusError) as safe_error:
        _run_safe(
            httpx.MockTransport(
                lambda request: _response(
                    request,
                    body=b"unavailable",
                    content_type="text/html",
                    status=503,
                )
            )
        )
    with pytest.raises(ShiborHTTPStatusError) as shibor_error:
        _run_shibor(
            httpx.MockTransport(
                lambda request: _response(
                    request,
                    body=b"unavailable",
                    content_type="application/json",
                    status=429,
                )
            )
        )
    assert safe_error.value.status_code == 503
    assert shibor_error.value.status_code == 429


@pytest.mark.parametrize("source", ["safe", "shibor"])
def test_timeout_and_transport_failures_are_source_specific(source: str) -> None:
    runner = _run_safe if source == "safe" else _run_shibor
    timeout_error = SafeCentralParityTimeoutError if source == "safe" else ShiborTimeoutError
    transport_error = (
        SafeCentralParityTransportError if source == "safe" else ShiborTransportError
    )

    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("frozen timeout", request=request)

    def disconnect(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("frozen disconnect", request=request)

    with pytest.raises(timeout_error):
        runner(httpx.MockTransport(timeout))
    with pytest.raises(transport_error):
        runner(httpx.MockTransport(disconnect))


def test_query_validation_protocols_and_staleness() -> None:
    safe_adapter = SafeCentralParityAdapter()
    shibor_adapter = OfficialShiborAdapter()
    assert isinstance(safe_adapter, AsyncSafeCentralParityData)
    assert isinstance(shibor_adapter, AsyncShiborData)

    async def invalid() -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            await safe_adapter.fetch_usd_cny_history(
                start_date=date(2026, 8, 12),
                end_date=date(2026, 8, 13),
                as_of=datetime(2026, 8, 13),
            )
        with pytest.raises(ValueError, match="cannot follow"):
            await shibor_adapter.fetch_shibor_history(
                start_date=date(2026, 8, 13),
                end_date=date(2026, 8, 12),
                as_of=FETCHED_AT,
            )

    asyncio.run(invalid())
    safe = _run_safe(
        httpx.MockTransport(
            lambda request: _response(
                request,
                body=FROZEN_SAFE_HTML,
                content_type="text/html",
            )
        ),
        as_of=datetime(2026, 8, 20, tzinfo=UTC),
        now=datetime(2026, 8, 20, tzinfo=UTC),
    )
    assert safe.meta.stale is True


def test_public_domain_objects_reject_unit_and_quote_confusion() -> None:
    safe = _run_safe(
        httpx.MockTransport(
            lambda request: _response(
                request,
                body=FROZEN_SAFE_HTML,
                content_type="text/html",
            )
        )
    )
    observation = safe.observations[-1]
    with pytest.raises(ValueError, match="normalized USD/CNY"):
        type(observation)(
            session_date=observation.session_date,
            base_currency="USD",
            quote_currency="CNY",
            quote_convention=FXQuoteConvention.QUOTE_CURRENCY_PER_BASE_CURRENCY,
            cny_per_usd=Decimal("678.88"),
            source_base_amount_usd=Decimal("100"),
            source_quote_amount_cny=Decimal("678.88"),
            observed_at=observation.observed_at,
            available_at=observation.available_at,
        )

    with pytest.raises(ValueError, match="ACT/360"):
        shibor = _run_shibor(
            httpx.MockTransport(
                lambda request: _response(
                    request,
                    body=FROZEN_SHIBOR_JSON,
                    content_type="application/json",
                )
            )
        ).observations[-1]
        type(shibor)(
            session_date=shibor.session_date,
            rates=shibor.rates,
            unit=shibor.unit,
            day_count="ACT/365",
            settlement="T+0",
            observed_at=shibor.observed_at,
            available_at=shibor.available_at,
        )


def test_constructor_settings_are_validated() -> None:
    with pytest.raises(ValueError, match="timeout_seconds"):
        SafeCentralParityAdapter(timeout_seconds=0)
    with pytest.raises(ValueError, match="max_response_bytes"):
        OfficialShiborAdapter(max_response_bytes=0)
    with pytest.raises(ValueError, match="stale_after"):
        OfficialShiborAdapter(stale_after=timedelta(0))
