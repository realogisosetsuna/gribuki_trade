"""Configurable, dependency-free HTML list adapter.

It extracts only list metadata (title, link, timestamp and short summary).  It
does not follow item links, execute scripts, solve challenges, or access pages
behind a login/paywall.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, tzinfo
from html.parser import HTMLParser

from gribuki_trade.domain.events import NormalizedEvent, RawDocument, SourcePolicy
from gribuki_trade.pipeline.normalize import (
    canonicalize_url,
    normalise_text,
    parse_published_datetime,
)
from gribuki_trade.ports.news import FetchCursor, NewsBatch, PublicDocumentFetcher

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


@dataclass(frozen=True, slots=True)
class HtmlListConfig:
    item_tag: str = "article"
    item_classes: frozenset[str] = frozenset()
    link_tag: str = "a"
    link_classes: frozenset[str] = frozenset()
    time_tag: str | None = "time"
    time_classes: frozenset[str] = frozenset()
    summary_tag: str | None = None
    summary_classes: frozenset[str] = frozenset()
    time_attribute: str = "datetime"
    max_items: int = 200
    event_type: str = "news"
    source_timezone: tzinfo = UTC
    require_published_at: bool = False

    def __post_init__(self) -> None:
        if not self.item_tag.strip() or not self.link_tag.strip():
            raise ValueError("item_tag and link_tag must not be empty")
        if self.max_items < 1:
            raise ValueError("max_items must be positive")


@dataclass(slots=True)
class _Item:
    depth: int
    href: str | None = None
    title_attribute: str | None = None
    title_parts: list[str] = field(default_factory=list)
    summary_parts: list[str] = field(default_factory=list)
    time_parts: list[str] = field(default_factory=list)
    time_value: str | None = None
    link_depth: int | None = None
    summary_depth: int | None = None
    time_depth: int | None = None


def _classes(attributes: dict[str, str | None]) -> frozenset[str]:
    return frozenset((attributes.get("class") or "").split())


def _matches(
    tag: str,
    actual_classes: frozenset[str],
    expected_tag: str,
    expected: frozenset[str],
) -> bool:
    return tag == expected_tag.lower() and expected <= actual_classes


class _ListParser(HTMLParser):
    def __init__(self, config: HtmlListConfig) -> None:
        super().__init__(convert_charrefs=True)
        self.config = config
        self.depth = 0
        self.current: _Item | None = None
        self.items: list[_Item] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        self.depth += 1
        attributes = dict(attrs)
        classes = _classes(attributes)
        if self.current is None and _matches(
            tag, classes, self.config.item_tag, self.config.item_classes
        ):
            self.current = _Item(depth=self.depth)
        item = self.current
        if item is None:
            return
        if item.href is None and _matches(
            tag, classes, self.config.link_tag, self.config.link_classes
        ):
            item.href = attributes.get("href")
            item.link_depth = self.depth
            if attributes.get("title"):
                item.title_attribute = attributes["title"]
        if (
            self.config.summary_tag is not None
            and item.summary_depth is None
            and _matches(
                tag,
                classes,
                self.config.summary_tag,
                self.config.summary_classes,
            )
        ):
            item.summary_depth = self.depth
        if (
            self.config.time_tag is not None
            and item.time_depth is None
            and _matches(tag, classes, self.config.time_tag, self.config.time_classes)
        ):
            item.time_depth = self.depth
            item.time_value = attributes.get(self.config.time_attribute)
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
        if item.summary_depth is not None and self.depth >= item.summary_depth:
            item.summary_parts.append(data)
        if item.time_depth is not None and self.depth >= item.time_depth:
            item.time_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        item = self.current
        if item is not None:
            if item.link_depth == self.depth:
                item.link_depth = None
            if item.summary_depth == self.depth:
                item.summary_depth = None
            if item.time_depth == self.depth:
                item.time_depth = None
            if item.depth == self.depth and tag.lower() == self.config.item_tag.lower():
                self.items.append(item)
                self.current = None
        self.depth = max(0, self.depth - 1)


def _decode(document: RawDocument) -> str:
    for encoding in (document.encoding, "utf-8", "gb18030"):
        if not encoding:
            continue
        try:
            return document.content.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue
    return document.content.decode("utf-8", errors="replace")


def parse_html_list(
    document: RawDocument,
    policy: SourcePolicy,
    config: HtmlListConfig,
) -> tuple[NormalizedEvent, ...]:
    parser = _ListParser(config)
    parser.feed(_decode(document))
    events: list[NormalizedEvent] = []
    for item in parser.items:
        if len(events) >= config.max_items:
            break
        title = normalise_text(item.title_attribute or " ".join(item.title_parts))
        if not title or not item.href:
            continue
        try:
            link = canonicalize_url(item.href, base_url=document.canonical_url)
        except ValueError:
            continue
        published = parse_published_datetime(
            item.time_value or " ".join(item.time_parts),
            default_timezone=config.source_timezone,
        )
        if config.require_published_at and published is None:
            continue
        summary = normalise_text(
            " ".join(item.summary_parts),
            limit=policy.max_summary_characters,
        )
        available = max(
            document.first_seen_at,
            published if published is not None else document.first_seen_at,
        )
        events.append(
            NormalizedEvent(
                source_id=policy.source_id,
                canonical_url=link,
                title=title,
                summary=summary,
                event_type=config.event_type,
                source_tier=policy.source_tier,
                first_seen_at=document.first_seen_at,
                retrieved_at=document.retrieved_at,
                available_at=available,
                published_at=published,
                raw_document_id=document.document_id,
            )
        )
    return tuple(events)


class HtmlListSource:
    def __init__(
        self,
        endpoint: str,
        policy: SourcePolicy,
        fetcher: PublicDocumentFetcher,
        config: HtmlListConfig,
    ) -> None:
        policy.validate_url(endpoint)
        self._endpoint = endpoint
        self._policy = policy
        self._fetcher = fetcher
        self._config = config

    async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch:
        fetched = await self._fetcher.fetch(self._policy, self._endpoint, cursor)
        if fetched.document is None:
            return NewsBatch(
                source_id=self._policy.source_id,
                cursor=fetched.cursor,
                not_modified=fetched.not_modified,
            )
        events = parse_html_list(fetched.document, self._policy, self._config)
        return NewsBatch(
            source_id=self._policy.source_id,
            cursor=fetched.cursor,
            documents=(fetched.document,),
            events=events,
        )
