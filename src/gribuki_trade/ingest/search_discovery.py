"""通过 Tavily 与自托管 SearXNG 进行低信任搜索发现。

搜索响应只会作为 ``discovery`` 事件持久化。它们只是后续抓取原始来源并
核验的线索，绝不是官方证据。此处不提供 Brave 适配器，因为 Brave 默认
接口条款不允许将搜索结果用于通用持久化存储。
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

# 与宏观证据选择器使用同一套保守注册域/官方主体规则。搜索提供方只是发现路由，
# Tavily 与 SearXNG 同时返回一条链接并不能制造第二个发布者。
_MULTI_LABEL_PUBLIC_SUFFIXES = frozenset(
    {
        "ac.cn",
        "com.cn",
        "edu.cn",
        "gov.cn",
        "net.cn",
        "org.cn",
        "com.hk",
        "com.tw",
        "co.jp",
        "co.uk",
        "org.uk",
    }
)
_OFFICIAL_DOMAIN_BODIES = {
    "stats.gov.cn": "nbs",
    "pbc.gov.cn": "pboc",
    "federalreserve.gov": "fed",
    "csrc.gov.cn": "csrc",
    "mof.gov.cn": "mof",
    "ndrc.gov.cn": "ndrc",
    "safe.gov.cn": "safe",
    "sse.com.cn": "sse",
    "szse.cn": "szse",
}


class SearchProviderError(RuntimeError):
    """单次提供方及查询尝试的脱敏基础错误。"""

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
    """提供方返回 401/403；不会尝试绕过凭据验证。"""


class SearchProviderRateLimited(SearchProviderError):
    """提供方返回 429。"""

    def __init__(
        self,
        provider_id: str,
        *,
        retry_after_seconds: int | None,
    ) -> None:
        super().__init__(provider_id, "search provider rate limited", status_code=429)
        self.retry_after_seconds = retry_after_seconds


class SearchProviderTimeout(SearchProviderError):
    """注入的传输层未能在提供方超时期限内完成。"""


class SearchProviderTransportError(SearchProviderError):
    """请求在收到 HTTP 响应前失败。"""


class SearchProviderUnavailable(SearchProviderError):
    """提供方返回非成功状态或过大的响应。"""


class SearchProviderPayloadError(SearchProviderError):
    """成功响应不是有界的 JSON 搜索结果对象。"""


class DiscoverySourcesExhausted(SourceFetchError):
    """一次发现运行中的所有提供方与查询尝试均失败。"""


@dataclass(frozen=True, slots=True)
class SearchAttemptFailure:
    """不含查询文本、网址、请求头、正文或密钥的安全诊断信息。"""

    provider_id: str
    query_kind: DiscoveryQueryKind
    error_code: str
    status_code: int | None = None


class DiscoveryConfirmationBasis(StrEnum):
    NONE = "none"
    OFFICIAL_HOST = "official_host"
    INDEPENDENT_PUBLISHERS = "independent_publishers"


@dataclass(frozen=True, slots=True)
class DiscoveryLineage:
    """说明一个发现事件为何可以或不可以进入证据选择器。"""

    event_id: str
    canonical_url: str
    matched_urls: tuple[str, ...]
    provider_ids: tuple[str, ...]
    publisher_identities: tuple[str, ...]
    event_type: str
    confirmation_basis: DiscoveryConfirmationBasis
    official_host: str | None = None

    def __post_init__(self) -> None:
        if not self.matched_urls or self.canonical_url not in self.matched_urls:
            raise ValueError("discovery lineage must retain its canonical URL")
        if self.matched_urls != tuple(sorted(set(self.matched_urls))):
            raise ValueError("discovery lineage URLs must be sorted and unique")
        if not self.provider_ids or self.provider_ids != tuple(
            sorted(set(self.provider_ids))
        ):
            raise ValueError("discovery lineage provider IDs must be sorted and unique")
        if not self.publisher_identities or self.publisher_identities != tuple(
            sorted(set(self.publisher_identities))
        ):
            raise ValueError("publisher identities must be sorted and unique")
        confirmed = self.event_type == DISCOVERY_CONFIRMED_EVENT_TYPE
        if confirmed != (self.confirmation_basis is not DiscoveryConfirmationBasis.NONE):
            raise ValueError("event type and discovery confirmation basis disagree")
        if self.confirmation_basis is DiscoveryConfirmationBasis.OFFICIAL_HOST:
            if self.official_host is None:
                raise ValueError("official-host confirmation must retain the matched host")
        elif self.official_host is not None:
            raise ValueError("non-official confirmation cannot retain an official host")
        if (
            self.confirmation_basis
            is DiscoveryConfirmationBasis.INDEPENDENT_PUBLISHERS
            and len(self.publisher_identities) < 2
        ):
            raise ValueError("independent-publisher confirmation requires two publishers")

    @property
    def eligible_for_evidence(self) -> bool:
        return self.event_type == DISCOVERY_CONFIRMED_EVENT_TYPE

    @property
    def independent_publisher_count(self) -> int:
        return len(self.publisher_identities)


@dataclass(frozen=True, slots=True)
class DiscoveryCollection:
    batch: NewsBatch
    succeeded_provider_ids: tuple[str, ...]
    failures: tuple[SearchAttemptFailure, ...]
    lineage: tuple[DiscoveryLineage, ...]

    @property
    def confirmed_events(self) -> tuple[NormalizedEvent, ...]:
        """仅返回有资格进入未来宏观证据选择器的事件。"""

        return select_confirmed_discovery_events(self.batch.events)

    @property
    def hint_events(self) -> tuple[NormalizedEvent, ...]:
        """可展示但不得驱动可执行建议的线索。"""

        return tuple(
            event
            for event in self.batch.events
            if event.event_type == DISCOVERY_HINT_EVENT_TYPE
        )


def select_confirmed_discovery_events(
    events: Sequence[NormalizedEvent],
) -> tuple[NormalizedEvent, ...]:
    """按闭锁原则失败：实体标签绝不提升缺少独立发布域的搜索提示。"""

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
        # 自定义注入传输层可能抛出携带请求细节的提供方专用异常，必须在其
        # 进入日志之前规范化。
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
    """Tavily 官方 ``POST https://api.tavily.com/search`` 接口约定。"""

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
    """已配置 SearXNG 实例的官方 ``GET /search`` JSON 接口。"""

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


def _registrable_domain(hostname: str) -> str:
    labels = tuple(label for label in hostname.split(".") if label)
    if len(labels) <= 2:
        return ".".join(labels)
    suffix = ".".join(labels[-2:])
    if suffix in _MULTI_LABEL_PUBLIC_SUFFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return suffix


def _publisher_identity(url: str) -> str:
    """按规范化发布域返回稳定身份，不把搜索提供方 ID 当作发布者。"""

    host = (urlsplit(url).hostname or "").rstrip(".").casefold()
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError:
        return "publisher-unknown"
    if not host:
        return "publisher-unknown"
    domain = _registrable_domain(host)
    official_body = _OFFICIAL_DOMAIN_BODIES.get(domain)
    if official_body is not None:
        return f"official-body:{official_body}"
    return f"publisher-domain:{domain}"


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
    """拒绝将通用或短标题相同视为独立佐证。"""

    return (
        len(value) >= _MIN_TITLE_CONFIRMATION_CHARACTERS
        and value not in _GENERIC_TITLE_KEYS
    )


@dataclass(frozen=True, slots=True)
class _ResolvedDiscovery:
    hit: DiscoveryHit
    matched_urls: tuple[str, ...]
    provider_ids: tuple[str, ...]
    publisher_identities: tuple[str, ...]
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
        provider_ids = tuple(sorted({hit.provider_id for hit in cluster.hits}))
        matched_urls = tuple(sorted({hit.url for hit in cluster.hits}))
        publisher_identities = tuple(
            sorted({_publisher_identity(hit.url) for hit in cluster.hits})
        )
        ordered_hits = sorted(
            cluster.hits,
            key=lambda item: (item.url, _title_key(item.title), item.provider_id),
        )
        official_hit = next(
            (
                hit
                for hit in ordered_hits
                if _matched_official_host(hit.url, official_hosts) is not None
            ),
            None,
        )
        representative = official_hit or ordered_hits[0]
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
        elif len(publisher_identities) >= 2:
            basis = DiscoveryConfirmationBasis.INDEPENDENT_PUBLISHERS
        else:
            basis = DiscoveryConfirmationBasis.NONE
        resolved.append(
            _ResolvedDiscovery(
                hit=representative,
                matched_urls=matched_urls,
                provider_ids=provider_ids,
                publisher_identities=publisher_identities,
                confirmation_basis=basis,
                official_host=official_host,
            )
        )
    return tuple(resolved)


class MultiProviderDiscoverySource:
    """以 ``NewsSource`` 形式提供、并发且故障隔离的搜索发现。"""

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
                    # 绝不保留提供方异常文本，其中可能包含带凭据的请求头、
                    # 网址或原始响应正文。
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
        publishers = ",".join(discovery.publisher_identities)
        prefix = DISCOVERY_CONFIRMED_PREFIX if confirmed else DISCOVERY_HINT_PREFIX
        summary_body = discovery.hit.summary or "（搜索结果未提供摘要）"
        summary = f"{prefix}[providers={providers};publishers={publishers}] {summary_body}"
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
            publisher_identities=discovery.publisher_identities,
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
