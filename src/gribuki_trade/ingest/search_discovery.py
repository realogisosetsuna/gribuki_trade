"""通过 Tavily 与自托管 SearXNG 进行低信任搜索发现。

搜索响应只会作为 ``discovery`` 事件持久化。它们只是后续抓取原始来源并
核验的线索，绝不是官方证据。此处不提供 Brave 适配器，因为 Brave 默认
接口条款不允许将搜索结果用于通用持久化存储。
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from gribuki_trade.domain.events import (
    DISCOVERY_CONFIRMED_EVENT_TYPE,
    DISCOVERY_HINT_EVENT_TYPE,
    NormalizedEvent,
    SourceTier,
)
from gribuki_trade.ingest.http import (
    SourceBackoffActive,
    SourceFetchError,
)
from gribuki_trade.ports.news import (
    DiscoveryHit,
    DiscoveryQuery,
    DiscoveryQueryKind,
    FetchCursor,
    NewsBatch,
    NewsSearchProvider,
)

from . import search_discovery_policy as _policy
from .search_providers import (
    TAVILY_SEARCH_ENDPOINT,
    SearchProviderAccessDenied,
    SearchProviderError,
    SearchProviderPayloadError,
    SearchProviderRateLimited,
    SearchProviderTimeout,
    SearchProviderTransportError,
    SearchProviderUnavailable,
    SearXNGSearchProvider,
    TavilySearchProvider,
)

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

_PROVIDER_ID = _policy._PROVIDER_ID

class DiscoverySourcesExhausted(SourceFetchError):
    """一次发现运行中的所有提供方与查询尝试均失败。"""


@dataclass(frozen=True, slots=True)
class SearchAttemptFailure:
    """不含查询文本、网址、请求头、正文或密钥的安全诊断信息。"""

    provider_id: str
    query_kind: DiscoveryQueryKind
    error_code: str
    status_code: int | None = None


DiscoveryConfirmationBasis = _policy.DiscoveryConfirmationBasis

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


# 兼容历史私有导入；纯发现策略实际位于 search_discovery_policy。
_DiscoveryCluster = _policy._DiscoveryCluster
_ResolvedDiscovery = _policy._ResolvedDiscovery
_cluster_hits = _policy._cluster_hits
_official_hosts = _policy._official_hosts
_provider_id = _policy._provider_id
_publisher_identity = _policy._publisher_identity
_safe_hit = _policy._safe_hit
_title_is_specific_enough = _policy._title_is_specific_enough
_title_key = _policy._title_key
_matched_official_host = _policy._matched_official_host
_registrable_domain = _policy._registrable_domain


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
