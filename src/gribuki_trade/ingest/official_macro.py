"""Default official macro and regulatory news sources.

Only public list/feed documents are fetched.  Linked article bodies are not
followed by these sources, and every redirect remains inside the source's
explicit host allowlist.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from html.parser import HTMLParser
from zoneinfo import ZoneInfo

from gribuki_trade.domain.events import NormalizedEvent, RawDocument, SourcePolicy, SourceTier
from gribuki_trade.ingest.html import HtmlListConfig, HtmlListSource
from gribuki_trade.ingest.http import HttpxNewsTransport, PublicHttpFetcher
from gribuki_trade.ingest.rss import RssFeedSource
from gribuki_trade.pipeline.normalize import (
    canonicalize_url,
    normalise_text,
    parse_published_datetime,
)
from gribuki_trade.ports.news import FetchCursor, NewsBatch, NewsSource, PublicDocumentFetcher

NBS_DATA_RELEASE_URL = "https://www.stats.gov.cn/sj/zxfb/"
PBOC_OPEN_MARKET_URL = (
    "https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125431/125475/index.html"
)
FED_MONETARY_RSS_URL = "https://www.federalreserve.gov/feeds/press_monetary.xml"
CSRC_POLICY_INTERPRETATION_URL = (
    "https://www.csrc.gov.cn/csrc/c100039/common_list.shtml"
)

NBS_SOURCE_ID = "official.nbs.data_release"
PBOC_SOURCE_ID = "official.pboc.open_market"
FED_SOURCE_ID = "official.fed.monetary_press"
CSRC_SOURCE_ID = "official.csrc.policy_interpretation"

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_VOID_ELEMENTS = {
    "area",
    "base",
    "br",
    "col",
    "embed",
    "hr",
    "img",
    "input",
    "link",
    "meta",
    "param",
    "source",
    "track",
    "wbr",
}


def _official_policy(
    source_id: str,
    host: str,
    *,
    content_types: tuple[str, ...],
) -> SourcePolicy:
    return SourcePolicy(
        source_id=source_id,
        allowed_hosts=frozenset({host}),
        source_tier=SourceTier.OFFICIAL,
        allowed_schemes=frozenset({"https"}),
        allow_subdomains=False,
        allowed_content_types=content_types,
        max_response_bytes=2_000_000,
        timeout_seconds=15.0,
        max_attempts=1,
        max_redirects=2,
    )


@dataclass(slots=True)
class _PBOCItem:
    font_depth: int
    href: str | None = None
    title_attribute: str | None = None
    title_parts: list[str] = field(default_factory=list)
    time_parts: list[str] = field(default_factory=list)
    link_depth: int | None = None
    time_depth: int | None = None


def _classes(attributes: dict[str, str | None]) -> frozenset[str]:
    return frozenset((attributes.get("class") or "").split())


class _PBOCListParser(HTMLParser):
    """Parse the live PBOC ``font.newslist_style + span.hui12`` rows."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.depth = 0
        self.current: _PBOCItem | None = None
        self.items: list[_PBOCItem] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        self.depth += 1
        attributes = dict(attrs)
        classes = _classes(attributes)
        if tag == "font" and "newslist_style" in classes:
            self.current = _PBOCItem(font_depth=self.depth)
        item = self.current
        if item is not None:
            if tag == "a" and item.href is None and self.depth > item.font_depth:
                item.href = attributes.get("href")
                item.title_attribute = attributes.get("title")
                item.link_depth = self.depth
            elif tag == "span" and "hui12" in classes and item.time_depth is None:
                item.time_depth = self.depth
        if tag in _VOID_ELEMENTS:
            self.depth = max(0, self.depth - 1)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in _VOID_ELEMENTS:
            self.handle_starttag(tag, attrs)
            return
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_data(self, data: str) -> None:
        item = self.current
        if item is None:
            return
        if item.link_depth is not None and self.depth >= item.link_depth:
            item.title_parts.append(data)
        if item.time_depth is not None and self.depth >= item.time_depth:
            item.time_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        item = self.current
        if item is not None:
            if item.link_depth == self.depth and tag == "a":
                item.link_depth = None
            if item.time_depth == self.depth and tag == "span":
                item.time_depth = None
                self.items.append(item)
                self.current = None
        self.depth = max(0, self.depth - 1)


def _decode(document: RawDocument) -> str:
    for encoding in (document.encoding, "utf-8-sig", "utf-8", "gb18030"):
        if not encoding:
            continue
        try:
            return document.content.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue
    return document.content.decode("utf-8", errors="replace")


def parse_pboc_open_market_list(
    document: RawDocument,
    policy: SourcePolicy,
    *,
    max_items: int = 100,
) -> tuple[NormalizedEvent, ...]:
    """Normalize the PBOC open-market notice list without fetching articles."""

    if max_items < 1:
        raise ValueError("max_items must be positive")
    parser = _PBOCListParser()
    parser.feed(_decode(document))
    events: list[NormalizedEvent] = []
    for item in parser.items:
        if len(events) >= max_items:
            break
        title = normalise_text(item.title_attribute or " ".join(item.title_parts))
        if not title or not item.href:
            continue
        published = parse_published_datetime(
            " ".join(item.time_parts),
            default_timezone=_SHANGHAI,
        )
        if published is None:
            continue
        try:
            link = canonicalize_url(item.href, base_url=document.canonical_url)
            policy.validate_url(link)
        except ValueError:
            continue
        events.append(
            NormalizedEvent(
                source_id=policy.source_id,
                canonical_url=link,
                title=title,
                summary="",
                event_type="policy",
                source_tier=policy.source_tier,
                first_seen_at=document.first_seen_at,
                retrieved_at=document.retrieved_at,
                available_at=max(document.first_seen_at, published),
                published_at=published,
                raw_document_id=document.document_id,
            )
        )
    return tuple(events)


class PBOCOpenMarketSource:
    """News source for the PBOC's public open-market operation notices."""

    def __init__(
        self,
        endpoint: str,
        policy: SourcePolicy,
        fetcher: PublicDocumentFetcher,
    ) -> None:
        policy.validate_url(endpoint)
        self._endpoint = endpoint
        self._policy = policy
        self._fetcher = fetcher

    async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch:
        fetched = await self._fetcher.fetch(self._policy, self._endpoint, cursor)
        if fetched.document is None:
            return NewsBatch(
                source_id=self._policy.source_id,
                cursor=fetched.cursor,
                not_modified=fetched.not_modified,
            )
        events = parse_pboc_open_market_list(fetched.document, self._policy)
        return NewsBatch(
            source_id=self._policy.source_id,
            cursor=fetched.cursor,
            documents=(fetched.document,),
            events=events,
        )


def build_default_official_macro_sources() -> dict[str, NewsSource]:
    """Build always-on official macro/regulatory list sources for close research."""

    fetcher = PublicHttpFetcher(HttpxNewsTransport())
    nbs_policy = _official_policy(
        NBS_SOURCE_ID,
        "www.stats.gov.cn",
        content_types=("text/html",),
    )
    pboc_policy = _official_policy(
        PBOC_SOURCE_ID,
        "www.pbc.gov.cn",
        content_types=("text/html",),
    )
    fed_policy = _official_policy(
        FED_SOURCE_ID,
        "www.federalreserve.gov",
        content_types=("application/rss+xml", "application/xml", "text/xml"),
    )
    csrc_policy = _official_policy(
        CSRC_SOURCE_ID,
        "www.csrc.gov.cn",
        content_types=("text/html",),
    )

    sources: dict[str, NewsSource] = {
        NBS_SOURCE_ID: HtmlListSource(
            NBS_DATA_RELEASE_URL,
            nbs_policy,
            fetcher,
            HtmlListConfig(
                item_tag="li",
                link_tag="a",
                link_classes=frozenset({"fl", "pc_1600"}),
                time_tag="span",
                event_type="macro",
                source_timezone=_SHANGHAI,
                require_published_at=True,
            ),
        ),
        PBOC_SOURCE_ID: PBOCOpenMarketSource(
            PBOC_OPEN_MARKET_URL,
            pboc_policy,
            fetcher,
        ),
        FED_SOURCE_ID: RssFeedSource(
            FED_MONETARY_RSS_URL,
            fed_policy,
            fetcher,
            event_type="macro_global",
        ),
        CSRC_SOURCE_ID: HtmlListSource(
            CSRC_POLICY_INTERPRETATION_URL,
            csrc_policy,
            fetcher,
            HtmlListConfig(
                item_tag="li",
                link_tag="a",
                time_tag="span",
                time_classes=frozenset({"date"}),
                event_type="regulatory",
                source_timezone=_SHANGHAI,
                require_published_at=True,
            ),
        ),
    }
    return sources

