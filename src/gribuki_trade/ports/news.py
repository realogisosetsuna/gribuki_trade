"""Ports for public news and announcement collection."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from gribuki_trade.domain.events import NormalizedEvent, RawDocument, SourcePolicy


@dataclass(frozen=True, slots=True)
class NewsHttpRequest:
    method: str
    url: str = field(repr=False)
    headers: dict[str, str] = field(default_factory=dict, repr=False)
    timeout_seconds: float = 15.0
    # Kept last so existing positional construction remains compatible.  The
    # body is intentionally excluded from repr because authenticated API
    # payloads can contain queries or provider-specific secrets.
    body: bytes | None = field(default=None, repr=False)

    def __repr__(self) -> str:
        safe_url = self.url.partition("?")[0]
        return (
            f"NewsHttpRequest(method={self.method!r}, url={safe_url!r}, "
            f"header_names={sorted(self.headers)!r}, timeout_seconds={self.timeout_seconds!r})"
        )


@dataclass(frozen=True, slots=True)
class NewsHttpResponse:
    status_code: int
    headers: dict[str, str] = field(default_factory=dict, repr=False)
    body: bytes = field(default=b"", repr=False)

    def __repr__(self) -> str:
        return f"NewsHttpResponse(status_code={self.status_code}, body=<{len(self.body)} bytes>)"


class NewsHttpTransport(Protocol):
    async def send(self, request: NewsHttpRequest) -> NewsHttpResponse: ...


@dataclass(frozen=True, slots=True)
class FetchCursor:
    """Conditional-request state that can be persisted by the scheduler."""

    etag: str | None = None
    last_modified: str | None = None
    content_sha256: str | None = None
    first_seen_at: datetime | None = None
    consecutive_failures: int = 0
    next_allowed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DocumentFetch:
    document: RawDocument | None
    cursor: FetchCursor
    not_modified: bool = False


class PublicDocumentFetcher(Protocol):
    async def fetch(
        self,
        policy: SourcePolicy,
        url: str,
        cursor: FetchCursor | None = None,
    ) -> DocumentFetch: ...


@dataclass(frozen=True, slots=True)
class NewsBatch:
    source_id: str
    cursor: FetchCursor
    documents: tuple[RawDocument, ...] = ()
    events: tuple[NormalizedEvent, ...] = ()
    not_modified: bool = False


class NewsSource(Protocol):
    async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch: ...


class DiscoveryQueryKind(StrEnum):
    """Why a search is being made; results remain unverified discovery leads."""

    STOCK = "stock"
    INDUSTRY = "industry"
    MACRO = "macro"


class DiscoveryTimeRange(StrEnum):
    """Portable time ranges supported by both Tavily and SearXNG."""

    DAY = "day"
    MONTH = "month"
    YEAR = "year"


@dataclass(frozen=True, slots=True)
class DiscoveryQuery:
    """One bounded search request with entities carried into evidence metadata."""

    text: str = field(repr=False)
    kind: DiscoveryQueryKind
    entities: tuple[str, ...]
    time_range: DiscoveryTimeRange | None = DiscoveryTimeRange.DAY
    max_results: int = 8

    def __post_init__(self) -> None:
        text = " ".join(self.text.split())
        if not text:
            raise ValueError("discovery query text must not be empty")
        if len(text) > 512:
            raise ValueError("discovery query text must not exceed 512 characters")
        if not 1 <= self.max_results <= 20:
            raise ValueError("max_results must be between 1 and 20")
        entities = tuple(dict.fromkeys(item.strip() for item in self.entities if item.strip()))
        if not entities:
            raise ValueError("discovery queries must identify at least one entity")
        object.__setattr__(self, "text", text)
        object.__setattr__(self, "entities", entities)

    @classmethod
    def for_stock(
        cls,
        *,
        symbol: str,
        name: str,
        industry: str | None = None,
        aliases: tuple[str, ...] = (),
        time_range: DiscoveryTimeRange | None = DiscoveryTimeRange.DAY,
        max_results: int = 8,
    ) -> DiscoveryQuery:
        """Build an A-share company/ETF news-and-announcement discovery query."""

        symbol_value = symbol.strip().upper()
        name_value = name.strip()
        if not symbol_value or not name_value:
            raise ValueError("stock symbol and name must not be empty")
        context = f" {industry.strip()}" if industry and industry.strip() else ""
        text = f'"{name_value}" {symbol_value}{context} 公告 新闻 业绩 资金'
        entities = (symbol_value, name_value, *(aliases or ()))
        if industry and industry.strip():
            entities += (industry.strip(),)
        return cls(text, DiscoveryQueryKind.STOCK, entities, time_range, max_results)

    @classmethod
    def for_industry(
        cls,
        industry: str,
        *,
        market: str = "A股",
        related_entities: tuple[str, ...] = (),
        time_range: DiscoveryTimeRange | None = DiscoveryTimeRange.DAY,
        max_results: int = 8,
    ) -> DiscoveryQuery:
        """Build a supply/demand, policy and constituent-industry query."""

        industry_value = industry.strip()
        market_value = market.strip()
        if not industry_value or not market_value:
            raise ValueError("industry and market must not be empty")
        text = f'"{industry_value}" {market_value} 行业 政策 供需 价格 公司 新闻'
        return cls(
            text,
            DiscoveryQueryKind.INDUSTRY,
            (industry_value, market_value, *related_entities),
            time_range,
            max_results,
        )

    @classmethod
    def for_macro(
        cls,
        topic: str,
        *,
        market: str = "A股",
        related_entities: tuple[str, ...] = (),
        time_range: DiscoveryTimeRange | None = DiscoveryTimeRange.DAY,
        max_results: int = 8,
    ) -> DiscoveryQuery:
        """Build a macro-policy and cross-market lead-discovery query."""

        topic_value = topic.strip()
        market_value = market.strip()
        if not topic_value or not market_value:
            raise ValueError("macro topic and market must not be empty")
        text = f'"{topic_value}" {market_value} 宏观 政策 利率 汇率 商品 市场影响'
        return cls(
            text,
            DiscoveryQueryKind.MACRO,
            (topic_value, market_value, *related_entities),
            time_range,
            max_results,
        )


@dataclass(frozen=True, slots=True)
class DiscoveryHit:
    """Provider-returned search lead, not an assertion that the result is true."""

    provider_id: str
    url: str
    title: str
    summary: str
    published_at: datetime | None
    entities: tuple[str, ...]


class NewsSearchProvider(Protocol):
    @property
    def provider_id(self) -> str: ...

    async def search(self, query: DiscoveryQuery) -> tuple[DiscoveryHit, ...]: ...
