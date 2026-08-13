"""Low-trust search discovery through Tavily and self-hosted SearXNG.

Search responses are persisted only as ``discovery`` events.  They are leads
for subsequent source retrieval and verification, never official evidence.
No Brave adapter is provided because Brave's default API terms do not permit
general-purpose persistent storage of search results.
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from urllib.parse import urlencode, urlsplit, urlunsplit

from gribuki_trade.domain.events import (
    DISCOVERY_CONFIRMED_EVENT_TYPE,
    DISCOVERY_HINT_EVENT_TYPE,
    NormalizedEvent,
    SourceTier,
)
from gribuki_trade.ingest.http import (
    NewsTransportError,
    SourceBackoffActive,
    SourceFetchError,
)
from gribuki_trade.pipeline.normalize import (
    canonicalize_url,
    normalise_text,
    parse_published_datetime,
)
from gribuki_trade.ports.news import (
    DiscoveryHit,
    DiscoveryQuery,
    DiscoveryQueryKind,
    FetchCursor,
    NewsBatch,
    NewsHttpRequest,
    NewsHttpResponse,
    NewsHttpTransport,
    NewsSearchProvider,
)

TAVILY_SEARCH_ENDPOINT = "https://api.tavily.com/search"
DISCOVERY_HINT_PREFIX = "搜索引擎单源线索（未完成来源核验，不可直接作为可操作证据）："
DISCOVERY_CONFIRMED_PREFIX = "搜索发现已达到晋级条件（仍需抓取原文核验）："

DEFAULT_OFFICIAL_DISCOVERY_HOSTS = frozenset(
    {
        "cninfo.com.cn",
        "csrc.gov.cn",
        "gov.cn",
        "mof.gov.cn",
        "ndrc.gov.cn",
        "pbc.gov.cn",
        "safe.gov.cn",
        "sse.com.cn",
        "stats.gov.cn",
        "szse.cn",
    }
)

_PROVIDER_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,47}$")
_TITLE_KEY = re.compile(r"[\W_]+", re.UNICODE)
_GENERIC_TITLE_KEYS = frozenset(
    {
        "详情",
        "首页",
        "新闻",
        "公告",
        "最新消息",
        "市场动态",
        "untitled",
    }
)
_MIN_TITLE_CONFIRMATION_CHARACTERS = 6


class SearchProviderError(RuntimeError):
    """Sanitized base error for one provider/query attempt."""

    def __init__(
        self,
        provider_id: str,
        message: str,
        *,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.provider_id = provider_id
        self.status_code = status_code


class SearchProviderAccessDenied(SearchProviderError):
    """The provider returned 401/403; no credential-bypass is attempted."""


class SearchProviderRateLimited(SearchProviderError):
    """The provider returned 429."""

    def __init__(
        self,
        provider_id: str,
        *,
        retry_after_seconds: int | None,
    ) -> None:
        super().__init__(provider_id, "search provider rate limited", status_code=429)
        self.retry_after_seconds = retry_after_seconds


class SearchProviderTimeout(SearchProviderError):
    """The injected transport did not complete within the provider timeout."""


class SearchProviderTransportError(SearchProviderError):
    """The request failed before an HTTP response was available."""


class SearchProviderUnavailable(SearchProviderError):
    """The provider returned a non-success status or oversized response."""


class SearchProviderPayloadError(SearchProviderError):
    """A success response was not a bounded JSON search-result object."""


class DiscoverySourcesExhausted(SourceFetchError):
    """Every provider/query attempt failed in one discovery run."""


@dataclass(frozen=True, slots=True)
class SearchAttemptFailure:
    """Safe diagnostics without query text, URL, headers, body, or secrets."""

    provider_id: str
    query_kind: DiscoveryQueryKind
    error_code: str
    status_code: int | None = None


class DiscoveryConfirmationBasis(StrEnum):
    NONE = "none"
    OFFICIAL_HOST = "official_host"
    MULTI_PROVIDER = "multi_provider"


@dataclass(frozen=True, slots=True)
class DiscoveryLineage:
    """Why one discovery event may, or may not, enter an evidence selector."""

    event_id: str
    canonical_url: str
    matched_urls: tuple[str, ...]
    provider_ids: tuple[str, ...]
    event_type: str
    confirmation_basis: DiscoveryConfirmationBasis
    official_host: str | None = None

    @property
    def eligible_for_evidence(self) -> bool:
        return self.event_type == DISCOVERY_CONFIRMED_EVENT_TYPE


@dataclass(frozen=True, slots=True)
class DiscoveryCollection:
    batch: NewsBatch
    succeeded_provider_ids: tuple[str, ...]
    failures: tuple[SearchAttemptFailure, ...]
    lineage: tuple[DiscoveryLineage, ...]

    @property
    def confirmed_events(self) -> tuple[NormalizedEvent, ...]:
        """Only events eligible for a future macro evidence selector."""

        return select_confirmed_discovery_events(self.batch.events)

    @property
    def hint_events(self) -> tuple[NormalizedEvent, ...]:
        """Displayable leads which must not drive an actionable recommendation."""

        return tuple(
            event
            for event in self.batch.events
            if event.event_type == DISCOVERY_HINT_EVENT_TYPE
        )


def select_confirmed_discovery_events(
    events: Sequence[NormalizedEvent],
) -> tuple[NormalizedEvent, ...]:
    """Fail closed: entity tags never promote a single-provider search hint."""

    return tuple(
        event
        for event in events
        if event.event_type == DISCOVERY_CONFIRMED_EVENT_TYPE
    )


def _header(headers: Mapping[str, str], name: str) -> str | None:
    expected = name.casefold()
    return next(
        (value for key, value in headers.items() if key.casefold() == expected),
        None,
    )


def _public_host(host: str) -> bool:
    if host == "localhost" or host.endswith((".localhost", ".local")):
        return False
    try:
        return ipaddress.ip_address(host).is_global
    except ValueError:
        return True


def _https_endpoint(value: str, *, exact_host: str | None = None) -> str:
    parts = urlsplit(value.strip())
    host = (parts.hostname or "").rstrip(".").lower()
    try:
        host = host.encode("idna").decode("ascii")
        port = parts.port
    except (UnicodeError, ValueError) as error:
        raise ValueError("search endpoint is invalid") from error
    if parts.scheme.lower() != "https":
        raise ValueError("search endpoint must use HTTPS")
    if not host or not _public_host(host):
        raise ValueError("search endpoint must use a public host")
    if parts.username is not None or parts.password is not None:
        raise ValueError("search endpoint must not contain credentials")
    if exact_host is not None and host != exact_host:
        raise ValueError("search endpoint host is not the official provider host")
    if parts.query or parts.fragment:
        raise ValueError("search endpoint must not contain a query or fragment")
    include_port = port is not None and port != 443
    netloc = f"{host}:{port}" if include_port else host
    return urlunsplit(("https", netloc, parts.path or "/", "", ""))


def _provider_id(value: str) -> str:
    provider_id = value.strip().lower()
    if not _PROVIDER_ID.fullmatch(provider_id):
        raise ValueError("provider_id must be a safe lowercase identifier")
    return provider_id


def _retry_after_seconds(response: NewsHttpResponse) -> int | None:
    value = _header(response.headers, "Retry-After")
    if value is None:
        return None
    try:
        return max(0, int(value.strip()))
    except ValueError:
        return None


async def _request_json(
    *,
    provider_id: str,
    transport: NewsHttpTransport,
    request: NewsHttpRequest,
    max_response_bytes: int,
) -> dict[str, object]:
    try:
        response = await asyncio.wait_for(
            transport.send(request),
            timeout=request.timeout_seconds,
        )
    except TimeoutError:
        raise SearchProviderTimeout(
            provider_id,
            "search provider request timed out",
        ) from None
    except (NewsTransportError, OSError):
        raise SearchProviderTransportError(
            provider_id,
            "search provider transport failed",
        ) from None
    except Exception:
        # A custom injected transport may use a provider-specific exception
        # carrying request details.  Normalize it before it reaches logs.
        raise SearchProviderTransportError(
            provider_id,
            "search provider transport failed",
        ) from None

    if response.status_code in {401, 403}:
        raise SearchProviderAccessDenied(
            provider_id,
            "search provider denied access",
            status_code=response.status_code,
        )
    if response.status_code == 429:
        raise SearchProviderRateLimited(
            provider_id,
            retry_after_seconds=_retry_after_seconds(response),
        )
    if response.status_code != 200:
        raise SearchProviderUnavailable(
            provider_id,
            "search provider returned a non-success response",
            status_code=response.status_code,
        )
    if len(response.body) > max_response_bytes:
        raise SearchProviderUnavailable(
            provider_id,
            "search provider response exceeded its configured limit",
            status_code=response.status_code,
        )
    content_type = _header(response.headers, "Content-Type")
    if content_type is None or content_type.partition(";")[0].strip().lower() != "application/json":
        raise SearchProviderPayloadError(
            provider_id,
            "search provider did not return application/json",
            status_code=response.status_code,
        )
    try:
        payload = json.loads(response.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise SearchProviderPayloadError(
            provider_id,
            "search provider returned invalid JSON",
            status_code=response.status_code,
        ) from None
    if not isinstance(payload, dict):
        raise SearchProviderPayloadError(
            provider_id,
            "search provider JSON root must be an object",
            status_code=response.status_code,
        )
    return {str(key): value for key, value in payload.items()}


def _result_items(
    payload: Mapping[str, object],
    *,
    provider_id: str,
) -> Sequence[object]:
    results = payload.get("results")
    if not isinstance(results, list):
        raise SearchProviderPayloadError(
            provider_id,
            "search provider results must be an array",
            status_code=200,
        )
    return results


def _published(item: Mapping[str, object]) -> datetime | None:
    for field_name in ("published_date", "publishedDate", "published_at", "date"):
        value = item.get(field_name)
        if isinstance(value, str):
            parsed = parse_published_datetime(value)
            if parsed is not None:
                return parsed
    return None


def _parse_hit(
    item: object,
    *,
    provider_id: str,
    entities: tuple[str, ...],
    summary_fields: tuple[str, ...],
) -> DiscoveryHit | None:
    if not isinstance(item, dict):
        return None
    values = {str(key): value for key, value in item.items()}
    url = values.get("url")
    title = values.get("title")
    if not isinstance(url, str) or not isinstance(title, str):
        return None
    title_text = normalise_text(title, limit=300)
    if not title_text:
        return None
    summary = ""
    for field_name in summary_fields:
        value = values.get(field_name)
        if isinstance(value, str) and value.strip():
            summary = normalise_text(value, limit=1_000)
            break
    return DiscoveryHit(
        provider_id=provider_id,
        url=url.strip(),
        title=title_text,
        summary=summary,
        published_at=_published(values),
        entities=entities,
    )


class TavilySearchProvider:
    """Tavily's official ``POST https://api.tavily.com/search`` contract."""

    provider_id = "tavily"

    def __init__(
        self,
        transport: NewsHttpTransport,
        *,
        api_key: str,
        timeout_seconds: float = 12.0,
        max_response_bytes: int = 2_000_000,
        topic: str = "news",
    ) -> None:
        key = api_key.strip()
        if not key or "\r" in key or "\n" in key:
            raise ValueError("Tavily API key must not be empty or contain newlines")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_response_bytes < 1:
            raise ValueError("max_response_bytes must be positive")
        if topic not in {"news", "finance"}:
            raise ValueError("Tavily topic must be news or finance")
        self._transport = transport
        self._api_key = key
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes
        self._topic = topic
        self._endpoint = _https_endpoint(
            TAVILY_SEARCH_ENDPOINT,
            exact_host="api.tavily.com",
        )

    def __repr__(self) -> str:
        return (
            "TavilySearchProvider(provider_id='tavily', "
            f"timeout_seconds={self._timeout_seconds!r}, topic={self._topic!r})"
        )

    async def search(self, query: DiscoveryQuery) -> tuple[DiscoveryHit, ...]:
        document: dict[str, object] = {
            "query": query.text,
            "topic": self._topic,
            "search_depth": "basic",
            "max_results": query.max_results,
            "include_answer": False,
            "include_raw_content": False,
            "include_images": False,
        }
        if query.time_range is not None:
            document["time_range"] = query.time_range.value
        body = json.dumps(
            document,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        request = NewsHttpRequest(
            method="POST",
            url=self._endpoint,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            timeout_seconds=self._timeout_seconds,
            body=body,
        )
        payload = await _request_json(
            provider_id=self.provider_id,
            transport=self._transport,
            request=request,
            max_response_bytes=self._max_response_bytes,
        )
        hits = (
            _parse_hit(
                item,
                provider_id=self.provider_id,
                entities=query.entities,
                summary_fields=("content", "snippet"),
            )
            for item in _result_items(payload, provider_id=self.provider_id)
        )
        return tuple(item for item in hits if item is not None)[: query.max_results]


class SearXNGSearchProvider:
    """A configured SearXNG instance's official ``GET /search`` JSON API."""

    provider_id = "searxng"

    def __init__(
        self,
        transport: NewsHttpTransport,
        *,
        base_url: str,
        bearer_token: str | None = None,
        timeout_seconds: float = 12.0,
        max_response_bytes: int = 2_000_000,
        categories: str = "news",
        language: str = "zh-CN",
        safesearch: int = 1,
    ) -> None:
        endpoint = _https_endpoint(base_url)
        parts = urlsplit(endpoint)
        path = parts.path.rstrip("/")
        if not path.endswith("/search"):
            path = f"{path}/search" if path else "/search"
        self._endpoint = urlunsplit(("https", parts.netloc, path, "", ""))
        token = None if bearer_token is None else bearer_token.strip()
        if token is not None and (not token or "\r" in token or "\n" in token):
            raise ValueError("SearXNG bearer token must not be empty or contain newlines")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_response_bytes < 1:
            raise ValueError("max_response_bytes must be positive")
        if not categories.strip() or not language.strip():
            raise ValueError("SearXNG categories and language must not be empty")
        if safesearch not in {0, 1, 2}:
            raise ValueError("SearXNG safesearch must be 0, 1, or 2")
        self._transport = transport
        self._bearer_token = token
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes
        self._categories = categories.strip()
        self._language = language.strip()
        self._safesearch = safesearch

    def __repr__(self) -> str:
        return (
            "SearXNGSearchProvider(provider_id='searxng', "
            f"endpoint={self._endpoint!r}, timeout_seconds={self._timeout_seconds!r})"
        )

    async def search(self, query: DiscoveryQuery) -> tuple[DiscoveryHit, ...]:
        parameters: list[tuple[str, str]] = [
            ("q", query.text),
            ("format", "json"),
            ("categories", self._categories),
            ("language", self._language),
            ("safesearch", str(self._safesearch)),
            ("pageno", "1"),
        ]
        if query.time_range is not None:
            parameters.append(("time_range", query.time_range.value))
        headers = {"Accept": "application/json"}
        if self._bearer_token is not None:
            headers["Authorization"] = f"Bearer {self._bearer_token}"
        request = NewsHttpRequest(
            method="GET",
            url=f"{self._endpoint}?{urlencode(parameters)}",
            headers=headers,
            timeout_seconds=self._timeout_seconds,
        )
        payload = await _request_json(
            provider_id=self.provider_id,
            transport=self._transport,
            request=request,
            max_response_bytes=self._max_response_bytes,
        )
        hits = (
            _parse_hit(
                item,
                provider_id=self.provider_id,
                entities=query.entities,
                summary_fields=("content", "snippet"),
            )
            for item in _result_items(payload, provider_id=self.provider_id)
        )
        return tuple(item for item in hits if item is not None)[: query.max_results]


def _safe_hit(hit: DiscoveryHit, *, now: datetime) -> DiscoveryHit | None:
    provider_id = _provider_id(hit.provider_id)
    try:
        url = canonicalize_url(hit.url)
    except ValueError:
        return None
    if urlsplit(url).scheme != "https":
        return None
    title = normalise_text(hit.title, limit=300)
    if not title:
        return None
    summary = normalise_text(hit.summary, limit=1_000)
    published = hit.published_at
    if published is not None:
        if published.tzinfo is None or published.utcoffset() is None:
            published = None
        else:
            published = published.astimezone(UTC)
            if published > now:
                published = None
    entities = tuple(dict.fromkeys(item.strip() for item in hit.entities if item.strip()))
    if not entities:
        return None
    return DiscoveryHit(provider_id, url, title, summary, published, entities)


def _title_key(title: str) -> str:
    return _TITLE_KEY.sub("", title.casefold())


def _official_hosts(values: Sequence[str]) -> frozenset[str]:
    hosts: set[str] = set()
    for value in values:
        host = value.strip().rstrip(".").lower()
        if "://" in host or "/" in host or "@" in host:
            raise ValueError("official discovery hosts must be host names only")
        try:
            host = host.encode("idna").decode("ascii")
        except UnicodeError as error:
            raise ValueError("official discovery host is invalid") from error
        if not host or not _public_host(host):
            raise ValueError("official discovery hosts must be public host names")
        hosts.add(host)
    return frozenset(hosts)


def _matched_official_host(url: str, official_hosts: frozenset[str]) -> str | None:
    host = (urlsplit(url).hostname or "").rstrip(".").lower()
    return next(
        (
            official
            for official in sorted(official_hosts)
            if host == official or host.endswith(f".{official}")
        ),
        None,
    )


@dataclass(slots=True)
class _DiscoveryCluster:
    hits: list[DiscoveryHit]

    def matches(self, hit: DiscoveryHit) -> bool:
        candidate_title = _title_key(hit.title)
        return any(
            existing.url == hit.url
            or (
                _title_is_specific_enough(candidate_title)
                and _title_key(existing.title) == candidate_title
            )
            for existing in self.hits
        )


def _title_is_specific_enough(value: str) -> bool:
    """Reject generic/short title equality as independent corroboration."""

    return (
        len(value) >= _MIN_TITLE_CONFIRMATION_CHARACTERS
        and value not in _GENERIC_TITLE_KEYS
    )


@dataclass(frozen=True, slots=True)
class _ResolvedDiscovery:
    hit: DiscoveryHit
    matched_urls: tuple[str, ...]
    provider_ids: tuple[str, ...]
    confirmation_basis: DiscoveryConfirmationBasis
    official_host: str | None


def _cluster_hits(
    hits: Sequence[DiscoveryHit],
    *,
    now: datetime,
    official_hosts: frozenset[str],
) -> tuple[_ResolvedDiscovery, ...]:
    clusters: list[_DiscoveryCluster] = []
    for candidate in hits:
        hit = _safe_hit(candidate, now=now)
        if hit is None:
            continue
        matching = [cluster for cluster in clusters if cluster.matches(hit)]
        if not matching:
            clusters.append(_DiscoveryCluster([hit]))
            continue
        target = matching[0]
        target.hits.append(hit)
        for extra in matching[1:]:
            target.hits.extend(extra.hits)
            clusters.remove(extra)

    resolved: list[_ResolvedDiscovery] = []
    for cluster in clusters:
        provider_ids = tuple(dict.fromkeys(hit.provider_id for hit in cluster.hits))
        matched_urls = tuple(dict.fromkeys(hit.url for hit in cluster.hits))
        official_hit = next(
            (
                hit
                for hit in cluster.hits
                if _matched_official_host(hit.url, official_hosts) is not None
            ),
            None,
        )
        representative = official_hit or cluster.hits[0]
        entities = tuple(
            dict.fromkeys(entity for hit in cluster.hits for entity in hit.entities)
        )
        representative = DiscoveryHit(
            representative.provider_id,
            representative.url,
            representative.title,
            representative.summary,
            representative.published_at,
            entities,
        )
        official_host = _matched_official_host(representative.url, official_hosts)
        if official_host is not None:
            basis = DiscoveryConfirmationBasis.OFFICIAL_HOST
        elif len(provider_ids) >= 2:
            basis = DiscoveryConfirmationBasis.MULTI_PROVIDER
        else:
            basis = DiscoveryConfirmationBasis.NONE
        resolved.append(
            _ResolvedDiscovery(
                hit=representative,
                matched_urls=matched_urls,
                provider_ids=provider_ids,
                confirmation_basis=basis,
                official_host=official_host,
            )
        )
    return tuple(resolved)


class MultiProviderDiscoverySource:
    """Concurrent, failure-isolated search discovery exposed as ``NewsSource``."""

    def __init__(
        self,
        providers: Sequence[NewsSearchProvider],
        queries: Sequence[DiscoveryQuery],
        *,
        source_id: str = "discovery.search",
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        max_concurrency: int = 6,
        failure_backoff_seconds: float = 60.0,
        official_hosts: Sequence[str] = tuple(DEFAULT_OFFICIAL_DISCOVERY_HOSTS),
    ) -> None:
        if not providers:
            raise ValueError("at least one search provider is required")
        if not queries:
            raise ValueError("at least one discovery query is required")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        if failure_backoff_seconds <= 0:
            raise ValueError("failure_backoff_seconds must be positive")
        provider_ids = tuple(_provider_id(item.provider_id) for item in providers)
        if len(set(provider_ids)) != len(provider_ids):
            raise ValueError("search provider IDs must be unique")
        self._providers = tuple(providers)
        self._queries = tuple(queries)
        self._source_id = _provider_id(source_id)
        self._clock = clock
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._failure_backoff_seconds = failure_backoff_seconds
        self._official_hosts = _official_hosts(official_hosts)

    async def _search_one(
        self,
        provider: NewsSearchProvider,
        query: DiscoveryQuery,
    ) -> tuple[DiscoveryHit, ...]:
        async with self._semaphore:
            return await provider.search(query)

    async def collect_with_diagnostics(
        self,
        cursor: FetchCursor | None = None,
    ) -> DiscoveryCollection:
        state = cursor or FetchCursor()
        started_at = self._now()
        if state.next_allowed_at is not None and started_at < state.next_allowed_at.astimezone(UTC):
            raise SourceBackoffActive("source backoff is active", cursor=state)

        attempts = tuple(
            (provider, query)
            for provider in self._providers
            for query in self._queries
        )
        outcomes = await asyncio.gather(
            *(self._search_one(provider, query) for provider, query in attempts),
            return_exceptions=True,
        )
        hits: list[DiscoveryHit] = []
        failures: list[SearchAttemptFailure] = []
        succeeded: list[str] = []
        retry_after_seconds: list[int] = []
        for (provider, query), outcome in zip(attempts, outcomes, strict=True):
            if isinstance(outcome, asyncio.CancelledError):
                raise outcome
            if isinstance(outcome, BaseException):
                if isinstance(outcome, SearchProviderError):
                    error_code = type(outcome).__name__
                    status_code = outcome.status_code
                    if (
                        isinstance(outcome, SearchProviderRateLimited)
                        and outcome.retry_after_seconds is not None
                    ):
                        retry_after_seconds.append(outcome.retry_after_seconds)
                else:
                    # Never retain provider exception text: it can contain a
                    # credential-bearing header, URL, or raw response body.
                    error_code = "unexpected_provider_error"
                    status_code = None
                failures.append(
                    SearchAttemptFailure(
                        provider_id=provider.provider_id,
                        query_kind=query.kind,
                        error_code=error_code,
                        status_code=status_code,
                    )
                )
                continue
            succeeded.append(provider.provider_id)
            hits.extend(outcome)

        now = self._now()
        if not succeeded:
            backoff_seconds = max(
                [
                    self._failure_backoff_seconds,
                    *(float(item) for item in retry_after_seconds),
                ]
            )
            failed_cursor = FetchCursor(
                etag=state.etag,
                last_modified=state.last_modified,
                content_sha256=state.content_sha256,
                first_seen_at=state.first_seen_at,
                consecutive_failures=state.consecutive_failures + 1,
                next_allowed_at=now + timedelta(seconds=backoff_seconds),
            )
            raise DiscoverySourcesExhausted(
                "all configured search provider attempts failed",
                cursor=failed_cursor,
            )

        discoveries = _cluster_hits(
            hits,
            now=now,
            official_hosts=self._official_hosts,
        )
        event_lineage = tuple(
            self._event_from_discovery(discovery, observed_at=now)
            for discovery in discoveries
        )
        events = tuple(item[0] for item in event_lineage)
        lineage = tuple(item[1] for item in event_lineage)
        digest = hashlib.sha256(
            "\n".join(event.revision_id for event in events).encode("utf-8")
        ).hexdigest()
        unchanged = state.content_sha256 == digest
        first_seen = (
            state.first_seen_at
            if unchanged and state.first_seen_at is not None
            else now
        )
        batch = NewsBatch(
            source_id=self._source_id,
            cursor=FetchCursor(
                content_sha256=digest,
                first_seen_at=first_seen,
            ),
            events=() if unchanged else events,
            not_modified=unchanged,
        )
        return DiscoveryCollection(
            batch=batch,
            succeeded_provider_ids=tuple(dict.fromkeys(succeeded)),
            failures=tuple(failures),
            lineage=lineage,
        )

    async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch:
        return (await self.collect_with_diagnostics(cursor)).batch

    def _event_from_discovery(
        self,
        discovery: _ResolvedDiscovery,
        *,
        observed_at: datetime,
    ) -> tuple[NormalizedEvent, DiscoveryLineage]:
        confirmed = discovery.confirmation_basis is not DiscoveryConfirmationBasis.NONE
        event_type = (
            DISCOVERY_CONFIRMED_EVENT_TYPE if confirmed else DISCOVERY_HINT_EVENT_TYPE
        )
        providers = ",".join(discovery.provider_ids)
        prefix = DISCOVERY_CONFIRMED_PREFIX if confirmed else DISCOVERY_HINT_PREFIX
        summary_body = discovery.hit.summary or "（搜索结果未提供摘要）"
        summary = f"{prefix}[providers={providers}] {summary_body}"
        source_id = (
            "discovery.multi"
            if len(discovery.provider_ids) >= 2
            else f"discovery.{discovery.hit.provider_id}"
        )
        event = NormalizedEvent(
            source_id=source_id,
            canonical_url=discovery.hit.url,
            title=discovery.hit.title,
            summary=summary,
            event_type=event_type,
            source_tier=SourceTier.PUBLIC_MEDIA,
            first_seen_at=observed_at,
            retrieved_at=observed_at,
            available_at=observed_at,
            published_at=discovery.hit.published_at,
            external_id=discovery.hit.url,
            entities=discovery.hit.entities,
        )
        lineage = DiscoveryLineage(
            event_id=event.event_id,
            canonical_url=event.canonical_url,
            matched_urls=discovery.matched_urls,
            provider_ids=discovery.provider_ids,
            event_type=event.event_type,
            confirmation_basis=discovery.confirmation_basis,
            official_host=discovery.official_host,
        )
        return event, lineage

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("discovery clock must return a timezone-aware datetime")
        return value.astimezone(UTC)


__all__ = [
    "DEFAULT_OFFICIAL_DISCOVERY_HOSTS",
    "DISCOVERY_CONFIRMED_EVENT_TYPE",
    "DISCOVERY_CONFIRMED_PREFIX",
    "DISCOVERY_HINT_EVENT_TYPE",
    "DISCOVERY_HINT_PREFIX",
    "TAVILY_SEARCH_ENDPOINT",
    "DiscoveryCollection",
    "DiscoveryConfirmationBasis",
    "DiscoveryLineage",
    "DiscoverySourcesExhausted",
    "MultiProviderDiscoverySource",
    "SearchAttemptFailure",
    "SearchProviderAccessDenied",
    "SearchProviderError",
    "SearchProviderPayloadError",
    "SearchProviderRateLimited",
    "SearchProviderTimeout",
    "SearchProviderTransportError",
    "SearchProviderUnavailable",
    "SearXNGSearchProvider",
    "TavilySearchProvider",
    "select_confirmed_discovery_events",
]
