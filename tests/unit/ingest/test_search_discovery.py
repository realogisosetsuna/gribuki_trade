from __future__ import annotations

import asyncio
import hashlib
import json
from collections import deque
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlsplit

import pytest

from gribuki_trade.domain.events import SourceTier
from gribuki_trade.ingest.http import HttpxNewsTransport
from gribuki_trade.ingest.search_discovery import (
    DISCOVERY_CONFIRMED_EVENT_TYPE,
    DiscoveryConfirmationBasis,
    DiscoverySourcesExhausted,
    MultiProviderDiscoverySource,
    SearchProviderAccessDenied,
    SearchProviderPayloadError,
    SearchProviderRateLimited,
    SearchProviderTimeout,
    SearXNGSearchProvider,
    TavilySearchProvider,
    select_confirmed_discovery_events,
)
from gribuki_trade.ports.news import (
    DiscoveryHit,
    DiscoveryQuery,
    DiscoveryQueryKind,
    DiscoveryTimeRange,
    FetchCursor,
    NewsHttpRequest,
    NewsHttpResponse,
)

NOW = datetime(2026, 8, 13, 9, 30, tzinfo=UTC)


class QueueTransport:
    def __init__(self, *responses: NewsHttpResponse | BaseException) -> None:
        self.responses = deque(responses)
        self.requests: list[NewsHttpRequest] = []

    async def send(self, request: NewsHttpRequest) -> NewsHttpResponse:
        self.requests.append(request)
        result = self.responses.popleft()
        if isinstance(result, BaseException):
            raise result
        return result


class HangingTransport:
    async def send(self, request: NewsHttpRequest) -> NewsHttpResponse:
        del request
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


def json_response(payload: object, status: int = 200, **headers: str) -> NewsHttpResponse:
    values = {"Content-Type": "application/json", **headers}
    return NewsHttpResponse(status, values, json.dumps(payload).encode())


def stock_query() -> DiscoveryQuery:
    return DiscoveryQuery.for_stock(
        symbol="510300.SH",
        name="沪深300ETF",
        industry="宽基指数",
        aliases=("华泰柏瑞沪深300ETF",),
        max_results=5,
    )


def test_query_factories_cover_stock_industry_and_macro_entities() -> None:
    stock = stock_query()
    industry = DiscoveryQuery.for_industry("半导体", related_entities=("中芯国际",))
    macro = DiscoveryQuery.for_macro(
        "人民币汇率",
        related_entities=("USDCNY", "美元指数"),
        time_range=DiscoveryTimeRange.MONTH,
    )

    assert [item.kind for item in (stock, industry, macro)] == [
        DiscoveryQueryKind.STOCK,
        DiscoveryQueryKind.INDUSTRY,
        DiscoveryQueryKind.MACRO,
    ]
    assert stock.entities == (
        "510300.SH",
        "沪深300ETF",
        "华泰柏瑞沪深300ETF",
        "宽基指数",
    )
    assert "人民币汇率" in macro.text and macro.entities[-1] == "美元指数"


def test_httpx_transport_forwards_optional_body_without_breaking_old_positionals(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "application/json"}
        content = b"{}"

    class FakeClient:
        def __init__(self, *, follow_redirects: bool, trust_env: bool) -> None:
            captured["follow_redirects"] = follow_redirects
            captured["trust_env"] = trust_env

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def request(self, method, url, *, headers, content, timeout, extensions):
            captured.update(
                method=method,
                url=url,
                headers=headers,
                content=content,
                timeout=timeout,
                extensions=extensions,
            )
            return FakeResponse()

    monkeypatch.setattr("gribuki_trade.ingest.http.httpx.AsyncClient", FakeClient)
    request = NewsHttpRequest(
        "POST",
        "https://api.tavily.com/search",
        {"Content-Type": "application/json"},
        7.0,
        b'{"query":"secret query"}',
    )

    response = asyncio.run(HttpxNewsTransport().send(request))

    assert response.status_code == 200
    assert captured["content"] == b'{"query":"secret query"}'
    assert captured["timeout"] == 7.0
    assert captured["follow_redirects"] is False
    assert captured["trust_env"] is False
    assert captured["extensions"] == {}
    assert "secret query" not in repr(request)


def test_tavily_uses_official_post_contract_and_keeps_secret_out_of_repr() -> None:
    secret = "tvly-secret-value"
    transport = QueueTransport(
        json_response(
            {
                "results": [
                    {
                        "title": "ETF news",
                        "url": "https://news.example.com/item",
                        "content": "A short search snippet",
                        "published_date": "2026-08-13T08:00:00Z",
                    }
                ]
            }
        )
    )
    provider = TavilySearchProvider(transport, api_key=secret)

    hits = asyncio.run(provider.search(stock_query()))

    request = transport.requests[0]
    payload = json.loads((request.body or b"").decode())
    assert request.method == "POST"
    assert request.url == "https://api.tavily.com/search"
    assert request.headers["Authorization"] == f"Bearer {secret}"
    assert payload["query"] == stock_query().text
    assert payload["include_answer"] is False
    assert payload["include_raw_content"] is False
    assert payload["search_depth"] == "basic"
    assert hits[0].published_at == datetime(2026, 8, 13, 8, 0, tzinfo=UTC)
    assert hits[0].entities == stock_query().entities
    assert secret not in repr(request) and secret not in repr(provider)
    assert stock_query().text not in repr(request)


def test_searxng_uses_get_search_json_contract_without_credentials_in_url() -> None:
    secret = "private-proxy-token"
    transport = QueueTransport(
        json_response(
            {
                "results": [
                    {
                        "title": "Policy item",
                        "url": "https://media.example.com/policy",
                        "content": "Result snippet",
                        "publishedDate": "2026-08-13 10:00:00+08:00",
                    }
                ]
            }
        )
    )
    provider = SearXNGSearchProvider(
        transport,
        base_url="https://search.example.com/searxng",
        bearer_token=secret,
    )
    query = DiscoveryQuery.for_macro("央行公开市场操作")

    hits = asyncio.run(provider.search(query))

    request = transport.requests[0]
    parsed = urlsplit(request.url)
    parameters = parse_qs(parsed.query)
    assert request.method == "GET"
    assert parsed.path == "/searxng/search"
    assert parameters["format"] == ["json"]
    assert parameters["q"] == [query.text]
    assert parameters["time_range"] == ["day"]
    assert request.headers["Authorization"] == f"Bearer {secret}"
    assert secret not in request.url
    assert secret not in repr(request)
    assert secret not in repr(provider)
    assert hits[0].published_at == datetime(2026, 8, 13, 2, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "constructor",
    [
        lambda transport: SearXNGSearchProvider(
            transport,
            base_url="http://search.example.com",
        ),
        lambda transport: SearXNGSearchProvider(
            transport,
            base_url="https://user:password@search.example.com",
        ),
        lambda transport: SearXNGSearchProvider(
            transport,
            base_url="https://127.0.0.1",
        ),
    ],
)
def test_searxng_rejects_non_https_credentialed_and_private_endpoints(constructor) -> None:
    with pytest.raises(ValueError):
        constructor(QueueTransport())


def test_provider_status_payload_and_timeout_errors_are_typed() -> None:
    access = TavilySearchProvider(
        QueueTransport(json_response({}, status=401)),
        api_key="tvly-test",
    )
    with pytest.raises(SearchProviderAccessDenied) as denied:
        asyncio.run(access.search(stock_query()))
    assert denied.value.status_code == 401

    limited = TavilySearchProvider(
        QueueTransport(json_response({}, status=429, **{"Retry-After": "19"})),
        api_key="tvly-test",
    )
    with pytest.raises(SearchProviderRateLimited) as rate_limited:
        asyncio.run(limited.search(stock_query()))
    assert rate_limited.value.retry_after_seconds == 19

    malformed = TavilySearchProvider(
        QueueTransport(NewsHttpResponse(200, {"Content-Type": "application/json"}, b"[")),
        api_key="tvly-test",
    )
    with pytest.raises(SearchProviderPayloadError):
        asyncio.run(malformed.search(stock_query()))

    hanging = TavilySearchProvider(
        HangingTransport(),
        api_key="tvly-test",
        timeout_seconds=0.001,
    )
    with pytest.raises(SearchProviderTimeout):
        asyncio.run(hanging.search(stock_query()))


class StubProvider:
    def __init__(self, provider_id: str, outcomes: dict[DiscoveryQueryKind, object]) -> None:
        self.provider_id = provider_id
        self._outcomes = outcomes

    async def search(self, query: DiscoveryQuery) -> tuple[DiscoveryHit, ...]:
        outcome = self._outcomes[query.kind]
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, tuple)
        return outcome


def hit(
    provider_id: str,
    query: DiscoveryQuery,
    *,
    url: str,
    title: str,
    summary: str = "search snippet",
) -> DiscoveryHit:
    return DiscoveryHit(
        provider_id=provider_id,
        url=url,
        title=title,
        summary=summary,
        published_at=NOW,
        entities=query.entities,
    )


def test_multi_provider_failure_isolation_dedupe_and_confirmation_lineage() -> None:
    stock = stock_query()
    industry = DiscoveryQuery.for_industry("半导体")
    macro = DiscoveryQuery.for_macro("美元利率")
    tavily = StubProvider(
        "tavily",
        {
            DiscoveryQueryKind.STOCK: (
                hit(
                    "tavily",
                    stock,
                    url="https://media.example.com/same?utm_source=search",
                    title="共同发现的公司事件",
                ),
                hit(
                    "tavily",
                    stock,
                    url="https://media.example.com/hint",
                    title="只有一个搜索源的消息",
                ),
            ),
            DiscoveryQueryKind.INDUSTRY: (
                hit(
                    "tavily",
                    industry,
                    url="https://www.sse.com.cn/disclosure/item",
                    title="交易所官方域名结果",
                ),
            ),
            DiscoveryQueryKind.MACRO: (
                hit(
                    "tavily",
                    macro,
                    url="https://one.example.net/story",
                    title="跨市场同一故事！",
                ),
            ),
        },
    )
    searxng = StubProvider(
        "searxng",
        {
            DiscoveryQueryKind.STOCK: (
                hit(
                    "searxng",
                    stock,
                    url="https://media.example.com/same",
                    title="共同发现的公司事件",
                ),
            ),
            DiscoveryQueryKind.INDUSTRY: SearchProviderAccessDenied(
                "searxng",
                "search provider denied access",
                status_code=403,
            ),
            DiscoveryQueryKind.MACRO: (
                hit(
                    "searxng",
                    macro,
                    url="https://two.example.org/another-path",
                    title="跨市场同一故事",
                ),
            ),
        },
    )
    source = MultiProviderDiscoverySource(
        (tavily, searxng),
        (stock, industry, macro),
        clock=lambda: NOW,
    )

    result = asyncio.run(source.collect_with_diagnostics())

    assert result.succeeded_provider_ids == ("tavily", "searxng")
    assert len(result.failures) == 1
    assert result.failures[0].error_code == "SearchProviderAccessDenied"
    assert len(result.batch.events) == 4
    assert len(result.confirmed_events) == 2
    assert len(result.hint_events) == 2
    assert all(event.source_tier is SourceTier.PUBLIC_MEDIA for event in result.batch.events)
    assert all(event.first_seen_at == NOW for event in result.batch.events)
    assert all(len(event.content_sha256) == 64 for event in result.batch.events)
    assert all(
        event.content_sha256
        == hashlib.sha256(
            (
                f"{event.title}\0{event.summary}\0"
                f"{event.published_at.isoformat() if event.published_at else ''}"
            ).encode()
        ).hexdigest()
        for event in result.batch.events
    )
    assert all(event.entities for event in result.batch.events)

    same_url = next(
        item for item in result.lineage if "media.example.com/same" in item.canonical_url
    )
    assert same_url.provider_ids == ("searxng", "tavily")
    assert same_url.publisher_identities == ("publisher-domain:example.com",)
    assert same_url.independent_publisher_count == 1
    assert same_url.confirmation_basis is DiscoveryConfirmationBasis.NONE
    assert same_url.eligible_for_evidence is False

    official = next(item for item in result.lineage if item.official_host == "sse.com.cn")
    assert official.confirmation_basis is DiscoveryConfirmationBasis.OFFICIAL_HOST
    assert official.publisher_identities == ("official-body:sse",)
    assert official.event_type == DISCOVERY_CONFIRMED_EVENT_TYPE

    hint_lineage = next(
        item for item in result.lineage if "media.example.com/hint" in item.canonical_url
    )
    assert hint_lineage.provider_ids == ("tavily",)
    assert hint_lineage.eligible_for_evidence is False
    hint_event = next(item for item in result.hint_events if item.event_id == hint_lineage.event_id)
    assert "不可直接作为可操作证据" in hint_event.summary
    assert hint_event.entities  # 仅凭实体元数据绝不能提升提示等级。
    assert hint_event not in select_confirmed_discovery_events(result.batch.events)


def test_same_url_from_two_search_providers_remains_a_hint() -> None:
    query = stock_query()
    source = MultiProviderDiscoverySource(
        (
            StubProvider(
                "tavily",
                {
                    DiscoveryQueryKind.STOCK: (
                        hit(
                            "tavily",
                            query,
                            url="https://news.example.com/item?utm_source=tavily",
                            title="同一页面不是两家发布者",
                        ),
                    )
                },
            ),
            StubProvider(
                "searxng",
                {
                    DiscoveryQueryKind.STOCK: (
                        hit(
                            "searxng",
                            query,
                            url="https://news.example.com/item",
                            title="同一页面不是两家发布者",
                        ),
                    )
                },
            ),
        ),
        (query,),
        clock=lambda: NOW,
    )

    result = asyncio.run(source.collect_with_diagnostics())

    assert len(result.lineage) == 1
    lineage = result.lineage[0]
    assert lineage.provider_ids == ("searxng", "tavily")
    assert lineage.matched_urls == ("https://news.example.com/item",)
    assert lineage.publisher_identities == ("publisher-domain:example.com",)
    assert lineage.confirmation_basis is DiscoveryConfirmationBasis.NONE
    assert result.confirmed_events == ()
    assert len(result.hint_events) == 1


def test_same_registered_publisher_domain_with_different_urls_remains_a_hint() -> None:
    query = stock_query()
    title = "同一媒体域名下的同一条报道"
    source = MultiProviderDiscoverySource(
        (
            StubProvider(
                "tavily",
                {
                    DiscoveryQueryKind.STOCK: (
                        hit(
                            "tavily",
                            query,
                            url="https://finance.media.example.com/story/1",
                            title=title,
                        ),
                    )
                },
            ),
            StubProvider(
                "searxng",
                {
                    DiscoveryQueryKind.STOCK: (
                        hit(
                            "searxng",
                            query,
                            url="https://news.media.example.com/story/2",
                            title=title,
                        ),
                    )
                },
            ),
        ),
        (query,),
        clock=lambda: NOW,
    )

    result = asyncio.run(source.collect_with_diagnostics())

    assert len(result.lineage) == 1
    lineage = result.lineage[0]
    assert lineage.matched_urls == (
        "https://finance.media.example.com/story/1",
        "https://news.media.example.com/story/2",
    )
    assert lineage.publisher_identities == ("publisher-domain:example.com",)
    assert lineage.confirmation_basis is DiscoveryConfirmationBasis.NONE
    assert lineage.eligible_for_evidence is False
    assert result.confirmed_events == ()


def test_matching_story_from_independent_publisher_domains_is_confirmed() -> None:
    query = stock_query()
    title = "两家独立媒体报道同一公司事项"
    source = MultiProviderDiscoverySource(
        (
            StubProvider(
                "tavily",
                {
                    DiscoveryQueryKind.STOCK: (
                        hit(
                            "tavily",
                            query,
                            url="https://wire-one.example/story",
                            title=title,
                        ),
                        hit(
                            "tavily",
                            query,
                            url="https://wire-two.example/story",
                            title=title,
                        ),
                    )
                },
            ),
        ),
        (query,),
        clock=lambda: NOW,
    )

    result = asyncio.run(source.collect_with_diagnostics())

    assert len(result.lineage) == 1
    lineage = result.lineage[0]
    assert lineage.provider_ids == ("tavily",)
    assert lineage.publisher_identities == (
        "publisher-domain:wire-one.example",
        "publisher-domain:wire-two.example",
    )
    assert lineage.independent_publisher_count == 2
    assert (
        lineage.confirmation_basis
        is DiscoveryConfirmationBasis.INDEPENDENT_PUBLISHERS
    )
    assert lineage.confirmation_basis.value == "independent_publishers"
    assert lineage.eligible_for_evidence is True
    assert len(result.confirmed_events) == 1


def test_short_generic_equal_titles_do_not_create_false_confirmation() -> None:
    query = stock_query()
    providers = (
        StubProvider(
            "tavily",
            {
                DiscoveryQueryKind.STOCK: (
                    hit(
                        "tavily",
                        query,
                        url="https://one.example.com/news",
                        title="新闻",
                    ),
                ),
            },
        ),
        StubProvider(
            "searxng",
            {
                DiscoveryQueryKind.STOCK: (
                    hit(
                        "searxng",
                        query,
                        url="https://two.example.com/news",
                        title="新闻",
                    ),
                ),
            },
        ),
    )
    source = MultiProviderDiscoverySource(providers, (query,), clock=lambda: NOW)

    result = asyncio.run(source.collect_with_diagnostics())

    assert len(result.batch.events) == 2
    assert len(result.confirmed_events) == 0
    assert len(result.hint_events) == 2


class BarrierProvider:
    def __init__(self, provider_id: str, entered: set[str], gate: asyncio.Event) -> None:
        self.provider_id = provider_id
        self._entered = entered
        self._gate = gate

    async def search(self, query: DiscoveryQuery) -> tuple[DiscoveryHit, ...]:
        self._entered.add(self.provider_id)
        if len(self._entered) == 2:
            self._gate.set()
        await asyncio.wait_for(self._gate.wait(), timeout=0.2)
        return ()


def test_providers_are_started_concurrently() -> None:
    async def run() -> None:
        entered: set[str] = set()
        gate = asyncio.Event()
        source = MultiProviderDiscoverySource(
            (
                BarrierProvider("one", entered, gate),
                BarrierProvider("two", entered, gate),
            ),
            (stock_query(),),
            clock=lambda: NOW,
            max_concurrency=2,
        )
        result = await source.collect_with_diagnostics()
        assert result.succeeded_provider_ids == ("one", "two")
        assert entered == {"one", "two"}

    asyncio.run(run())


def test_all_provider_failures_raise_sanitized_source_error_with_backoff() -> None:
    secret = "secret-in-provider-error"
    failed = StubProvider(
        "tavily",
        {DiscoveryQueryKind.STOCK: RuntimeError(secret)},
    )
    denied = StubProvider(
        "searxng",
        {
            DiscoveryQueryKind.STOCK: SearchProviderAccessDenied(
                "searxng",
                "sanitized",
                status_code=401,
            )
        },
    )
    source = MultiProviderDiscoverySource(
        (failed, denied),
        (stock_query(),),
        clock=lambda: NOW,
        failure_backoff_seconds=30,
    )

    with pytest.raises(DiscoverySourcesExhausted) as raised:
        asyncio.run(source.collect(FetchCursor(consecutive_failures=2)))

    assert raised.value.cursor.consecutive_failures == 3
    assert raised.value.cursor.next_allowed_at is not None
    assert secret not in str(raised.value) and secret not in repr(raised.value)


def test_insecure_result_url_is_not_persisted() -> None:
    query = stock_query()
    provider = StubProvider(
        "tavily",
        {
            DiscoveryQueryKind.STOCK: (
                hit(
                    "tavily",
                    query,
                    url="http://media.example.com/insecure",
                    title="HTTP result",
                ),
            )
        },
    )
    source = MultiProviderDiscoverySource((provider,), (query,), clock=lambda: NOW)

    batch = asyncio.run(source.collect())

    assert batch.events == ()
