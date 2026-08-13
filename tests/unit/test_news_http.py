import asyncio
from collections import deque
from datetime import UTC, datetime, timedelta
from functools import wraps

import pytest

from gribuki_trade.domain.events import SourcePolicy
from gribuki_trade.ingest.http import (
    PublicHttpFetcher,
    SourceAccessDenied,
    SourceBackoffActive,
    SourceRateLimited,
    SourceUnavailable,
)
from gribuki_trade.ports.news import (
    FetchCursor,
    NewsHttpRequest,
    NewsHttpResponse,
)

NOW = datetime(2026, 8, 13, 1, 2, 3, tzinfo=UTC)


def async_test(function):
    @wraps(function)
    def run(*args, **kwargs):
        return asyncio.run(function(*args, **kwargs))

    return run


class FakeTransport:
    def __init__(self, *responses: NewsHttpResponse | BaseException) -> None:
        self.responses = deque(responses)
        self.requests: list[NewsHttpRequest] = []

    async def send(self, request: NewsHttpRequest) -> NewsHttpResponse:
        self.requests.append(request)
        result = self.responses.popleft()
        if isinstance(result, BaseException):
            raise result
        return result


def response(status: int, body: bytes = b"", **headers: str) -> NewsHttpResponse:
    return NewsHttpResponse(status, headers, body)


def policy(**overrides: object) -> SourcePolicy:
    values: dict[str, object] = {
        "source_id": "official",
        "allowed_hosts": frozenset({"news.example.com"}),
        "backoff_base_seconds": 1,
        "backoff_cap_seconds": 8,
    }
    values.update(overrides)
    return SourcePolicy(**values)  # type: ignore[arg-type]


@async_test
async def test_conditional_fetch_preserves_observation_time_and_handles_304() -> None:
    transport = FakeTransport(
        response(
            200,
            b"<rss />",
            **{
                "Content-Type": "application/rss+xml; charset=utf-8",
                "ETag": '"revision-1"',
                "Last-Modified": "Wed, 12 Aug 2026 01:00:00 GMT",
            },
        ),
        response(304),
    )
    fetcher = PublicHttpFetcher(transport, clock=lambda: NOW)

    first = await fetcher.fetch(policy(), "https://news.example.com/feed?utm_source=x")
    second = await fetcher.fetch(policy(), "https://news.example.com/feed", first.cursor)

    assert first.document is not None
    assert first.document.canonical_url == "https://news.example.com/feed"
    assert first.document.first_seen_at == NOW
    assert first.document.retrieved_at == NOW
    assert first.document.encoding == "utf-8"
    assert second.document is None and second.not_modified is True
    assert transport.requests[1].headers["If-None-Match"] == '"revision-1"'
    assert transport.requests[1].headers["If-Modified-Since"].startswith("Wed, 12 Aug")
    assert "utm_source" not in transport.requests[0].url


@async_test
async def test_same_unconditionally_returned_body_keeps_first_seen() -> None:
    headers = {"Content-Type": "text/html"}
    transport = FakeTransport(response(200, b"same", **headers), response(200, b"same", **headers))
    moments = iter((NOW, NOW, NOW + timedelta(minutes=2), NOW + timedelta(minutes=2)))
    fetcher = PublicHttpFetcher(transport, clock=lambda: next(moments))

    first = await fetcher.fetch(policy(), "https://news.example.com/list")
    second = await fetcher.fetch(policy(), "https://news.example.com/list", first.cursor)

    assert first.document is not None and second.document is not None
    assert second.document.first_seen_at == NOW
    assert second.document.retrieved_at == NOW + timedelta(minutes=2)


@async_test
async def test_transient_failures_use_bounded_exponential_retry() -> None:
    transport = FakeTransport(
        response(503),
        response(502),
        response(200, b"ok", **{"Content-Type": "text/html"}),
    )
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    result = await PublicHttpFetcher(
        transport,
        clock=lambda: NOW,
        sleeper=sleep,
    ).fetch(policy(), "https://news.example.com/list")

    assert result.document is not None
    assert delays == [1.0, 2.0]
    assert len(transport.requests) == 3


@pytest.mark.parametrize("status", [401, 403])
@async_test
async def test_access_denial_is_fail_closed_without_retry(status: int) -> None:
    transport = FakeTransport(response(status), response(200))
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    with pytest.raises(SourceAccessDenied) as raised:
        await PublicHttpFetcher(transport, clock=lambda: NOW, sleeper=sleep).fetch(
            policy(), "https://news.example.com/list"
        )

    assert raised.value.status_code == status
    assert raised.value.cursor.next_allowed_at == NOW + timedelta(seconds=1)
    assert len(transport.requests) == 1
    assert delays == []


@async_test
async def test_rate_limit_honours_retry_after_and_blocks_early_call() -> None:
    transport = FakeTransport(response(429, **{"Retry-After": "17"}))
    fetcher = PublicHttpFetcher(transport, clock=lambda: NOW)

    with pytest.raises(SourceRateLimited) as raised:
        await fetcher.fetch(policy(), "https://news.example.com/feed")

    assert raised.value.cursor.next_allowed_at == NOW + timedelta(seconds=17)
    with pytest.raises(SourceBackoffActive):
        await fetcher.fetch(
            policy(),
            "https://news.example.com/feed",
            raised.value.cursor,
        )
    assert len(transport.requests) == 1


@async_test
async def test_redirect_outside_allowlist_and_oversized_body_are_rejected() -> None:
    redirect = FakeTransport(response(302, **{"Location": "https://evil.example/collect"}))
    with pytest.raises(ValueError, match="host"):
        await PublicHttpFetcher(redirect, clock=lambda: NOW).fetch(
            policy(), "https://news.example.com/feed"
        )

    oversized = FakeTransport(
        response(200, b"12345", **{"Content-Type": "text/html", "Content-Length": "5"})
    )
    with pytest.raises(SourceUnavailable, match="exceeds"):
        await PublicHttpFetcher(oversized, clock=lambda: NOW).fetch(
            policy(max_response_bytes=4), "https://news.example.com/list"
        )


def test_source_policy_rejects_local_and_unlisted_hosts_before_network() -> None:
    with pytest.raises(ValueError, match="public"):
        SourcePolicy(source_id="bad", allowed_hosts=frozenset({"127.0.0.1"}))
    configured = policy()
    with pytest.raises(ValueError, match="host"):
        configured.validate_url("https://other.example/feed")
    with pytest.raises(ValueError, match="public"):
        configured.validate_url("https://127.0.0.1/feed")


def test_cursor_can_represent_persisted_backoff() -> None:
    cursor = FetchCursor(consecutive_failures=2, next_allowed_at=NOW)
    assert cursor.consecutive_failures == 2
