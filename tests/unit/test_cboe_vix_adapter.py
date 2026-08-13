import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest

from gribuki_trade.adapters.cboe_vix import (
    CBOE_VIX_EOD_CSV_URL,
    CBOE_VIX_SOURCE_ID,
    CboeVIXDailyAdapter,
)
from gribuki_trade.ports.global_risk import (
    AsyncVIXDailyData,
    GlobalRiskCacheEntry,
    GlobalRiskCacheError,
    GlobalRiskHTTPStatusError,
    GlobalRiskSchemaError,
    GlobalRiskTimeoutError,
    GlobalRiskTransportError,
    VIXDailyHistory,
)

FIXTURE = Path(__file__).parents[1] / "fixtures" / "cboe" / "vix_history.csv"
FROZEN_CSV = FIXTURE.read_bytes()
FETCHED_AT = datetime(2026, 8, 14, 7, 1, tzinfo=UTC)


def _response(
    request: httpx.Request,
    *,
    body: bytes = FROZEN_CSV,
    status: int = 200,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    response_headers = {
        "Content-Type": "text/csv; charset=utf-8",
        "ETag": '"vix-fixture-1"',
        "Last-Modified": "Thu, 13 Aug 2026 21:00:00 GMT",
    }
    if headers:
        response_headers.update(headers)
    return httpx.Response(status, content=body, headers=response_headers, request=request)


def _run_with_handler(
    handler: httpx.MockTransport,
    *,
    as_of: datetime,
    cache: GlobalRiskCacheEntry | None = None,
    now: datetime = FETCHED_AT,
) -> VIXDailyHistory:
    async def run() -> VIXDailyHistory:
        async with httpx.AsyncClient(transport=handler, follow_redirects=False) as client:
            adapter = CboeVIXDailyAdapter(client, now=lambda: now)
            return await adapter.fetch_vix_daily_history(as_of=as_of, cache=cache)

    return asyncio.run(run())


def test_official_csv_is_strictly_parsed_and_filtered_at_china_close() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _response(request)

    parsed = _run_with_handler(
        httpx.MockTransport(handler),
        as_of=datetime(2026, 8, 13, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    assert requests[0].url == httpx.URL(CBOE_VIX_EOD_CSV_URL)
    assert requests[0].method == "GET"
    assert parsed.bars[-1].session_date.isoformat() == "2026-08-12"
    assert parsed.bars[-1].close.as_tuple().exponent == -6
    assert str(parsed.bars[-1].close) == "17.940000"
    assert parsed.bars[-1].available_at.isoformat() == "2026-08-12T16:15:00-04:00"
    assert parsed.meta.source_id == CBOE_VIX_SOURCE_ID
    assert parsed.meta.source_url == CBOE_VIX_EOD_CSV_URL
    assert parsed.meta.available_at == parsed.bars[-1].available_at
    assert parsed.meta.fetched_at == FETCHED_AT
    assert parsed.meta.stale is False
    assert parsed.meta.etag == '"vix-fixture-1"'
    assert parsed.meta.cache_revalidated is False
    assert parsed.meta.content_sha256 == hashlib.sha256(FROZEN_CSV).hexdigest()
    assert parsed.cache_entry.body == FROZEN_CSV
    assert all(bar.available_at <= parsed.as_of for bar in parsed.bars)
    assert "release vintages" in " ".join(parsed.meta.warnings)
    assert parsed.meta.skipped_invalid_ohlc_rows == 1
    assert "1992-02-11" in " ".join(parsed.meta.warnings)
    assert all(bar.session_date.isoformat() != "1992-02-11" for bar in parsed.bars)
    assert b"02/11/1992,19.240000,18.570000,17.610000,17.700000" in (
        parsed.cache_entry.body
    )


def test_latest_visible_invalid_ohlc_fails_instead_of_falling_back() -> None:
    with pytest.raises(
        GlobalRiskSchemaError,
        match=(
            "latest visible row has inconsistent OHLC for 1992-02-11; "
            "refusing to fall back"
        ),
    ):
        _run_with_handler(
            httpx.MockTransport(lambda request: _response(request)),
            as_of=datetime(
                1992,
                2,
                11,
                16,
                15,
                tzinfo=ZoneInfo("America/New_York"),
            ),
            now=datetime(2026, 8, 14, tzinfo=UTC),
        )


def test_not_yet_visible_invalid_ohlc_cannot_contaminate_pit_history() -> None:
    body = (
        b"DATE,OPEN,HIGH,LOW,CLOSE\n"
        b"08/12/2026,17,18,16,17\n"
        b"08/13/2026,19.24,18.57,17.61,17.70\n"
    )
    history = _run_with_handler(
        httpx.MockTransport(lambda request: _response(request, body=body)),
        as_of=datetime(2026, 8, 13, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert history.bars[-1].session_date.isoformat() == "2026-08-12"
    assert history.meta.skipped_invalid_ohlc_rows == 0
    assert "affected dates were excluded" not in " ".join(history.meta.warnings)


def test_current_us_session_appears_only_when_vix_regular_session_is_complete() -> None:
    transport = httpx.MockTransport(lambda request: _response(request))
    before = _run_with_handler(
        transport,
        as_of=datetime(2026, 8, 13, 16, 14, 59, tzinfo=ZoneInfo("America/New_York")),
    )
    at_close = _run_with_handler(
        transport,
        as_of=datetime(2026, 8, 13, 16, 15, tzinfo=ZoneInfo("America/New_York")),
    )

    assert before.bars[-1].session_date.isoformat() == "2026-08-12"
    assert at_close.bars[-1].session_date.isoformat() == "2026-08-13"


def test_conditional_304_reuses_caller_owned_cache_and_refreshes_metadata() -> None:
    first = _run_with_handler(
        httpx.MockTransport(lambda request: _response(request)),
        as_of=datetime(2026, 8, 14, tzinfo=UTC),
    )
    requests: list[httpx.Request] = []

    def not_modified(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(304, request=request)

    refreshed_at = datetime(2026, 8, 14, 7, 0, tzinfo=UTC)
    second = _run_with_handler(
        httpx.MockTransport(not_modified),
        as_of=datetime(2026, 8, 14, tzinfo=UTC),
        cache=first.cache_entry,
        now=refreshed_at,
    )

    assert requests[0].headers["If-None-Match"] == '"vix-fixture-1"'
    assert requests[0].headers["If-Modified-Since"] == (
        "Thu, 13 Aug 2026 21:00:00 GMT"
    )
    assert second.bars == first.bars
    assert second.meta.cache_revalidated is True
    assert second.meta.fetched_at == refreshed_at
    assert second.cache_entry.fetched_at == refreshed_at
    assert second.cache_entry.content_sha256 == first.cache_entry.content_sha256


def test_304_without_cache_is_a_typed_cache_error() -> None:
    with pytest.raises(GlobalRiskCacheError, match="without a cached document"):
        _run_with_handler(
            httpx.MockTransport(lambda request: httpx.Response(304, request=request)),
            as_of=datetime(2026, 8, 14, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (b"DATE,OPEN,HIGH,LOW\n08/12/2026,1,2,1\n", "exactly DATE"),
        (
            b"DATE,OPEN,HIGH,LOW,CLOSE\n"
            b"08/12/2026,17,18,16,17\n"
            b"08/12/2026,18,19,17,18\n",
            "duplicate DATE",
        ),
        (
            b"DATE,OPEN,HIGH,LOW,CLOSE\n"
            b"08/13/2026,17,18,16,17\n"
            b"08/12/2026,18,19,17,18\n",
            "out-of-order DATE",
        ),
        (
            b"DATE,OPEN,HIGH,LOW,CLOSE\n08/12/2026,not-a-number,18,16,17\n",
            "non-numeric OPEN",
        ),
        (
            b"DATE,OPEN,HIGH,LOW,CLOSE\n08/12/2026,17,16,15,18\n",
            "inconsistent OHLC",
        ),
    ],
)
def test_schema_drift_duplicates_order_and_bad_ohlc_fail_closed(
    body: bytes,
    message: str,
) -> None:
    with pytest.raises(GlobalRiskSchemaError, match=message):
        _run_with_handler(
            httpx.MockTransport(lambda request: _response(request, body=body)),
            as_of=datetime(2026, 8, 14, tzinfo=UTC),
        )


def test_non_csv_content_and_http_status_have_typed_failures() -> None:
    with pytest.raises(GlobalRiskSchemaError, match="not CSV"):
        _run_with_handler(
            httpx.MockTransport(
                lambda request: _response(
                    request,
                    body=b"<html>not csv</html>",
                    headers={"Content-Type": "text/html"},
                )
            ),
            as_of=datetime(2026, 8, 14, tzinfo=UTC),
        )

    with pytest.raises(GlobalRiskHTTPStatusError) as raised:
        _run_with_handler(
            httpx.MockTransport(lambda request: _response(request, status=503)),
            as_of=datetime(2026, 8, 14, tzinfo=UTC),
        )
    assert raised.value.status_code == 503


def test_timeout_and_transport_failures_remain_distinct() -> None:
    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("frozen timeout", request=request)

    def disconnect(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("frozen disconnect", request=request)

    with pytest.raises(GlobalRiskTimeoutError):
        _run_with_handler(
            httpx.MockTransport(timeout),
            as_of=datetime(2026, 8, 14, tzinfo=UTC),
        )
    with pytest.raises(GlobalRiskTransportError):
        _run_with_handler(
            httpx.MockTransport(disconnect),
            as_of=datetime(2026, 8, 14, tzinfo=UTC),
        )


def test_stale_flag_uses_latest_visible_session_not_fetch_clock() -> None:
    history = _run_with_handler(
        httpx.MockTransport(lambda request: _response(request)),
        as_of=datetime(2026, 8, 20, tzinfo=UTC),
        now=datetime(2026, 8, 20, tzinfo=UTC),
    )
    assert history.meta.stale is True


def test_protocol_and_aware_datetime_contract() -> None:
    adapter = CboeVIXDailyAdapter()
    assert isinstance(adapter, AsyncVIXDailyData)

    async def run() -> None:
        with pytest.raises(ValueError, match="as_of must be timezone-aware"):
            await adapter.fetch_vix_daily_history(as_of=datetime(2026, 8, 13))

    asyncio.run(run())

    with pytest.raises(ValueError, match="stale_after must be positive"):
        CboeVIXDailyAdapter(stale_after=timedelta(0))
