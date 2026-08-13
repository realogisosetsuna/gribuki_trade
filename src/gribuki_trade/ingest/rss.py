"""RSS 2.0 and Atom list adapter; linked articles are not fetched."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import UTC

from gribuki_trade.domain.events import NormalizedEvent, RawDocument, SourcePolicy
from gribuki_trade.pipeline.normalize import (
    canonicalize_url,
    normalise_text,
    parse_published_datetime,
)
from gribuki_trade.ports.news import (
    FetchCursor,
    NewsBatch,
    PublicDocumentFetcher,
)


class FeedParseError(ValueError):
    """The source returned content that is not a safe RSS/Atom document."""


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _children(element: ET.Element, *names: str) -> list[ET.Element]:
    accepted = set(names)
    return [child for child in element if _local_name(child.tag) in accepted]


def _text(element: ET.Element, *names: str) -> str | None:
    for child in _children(element, *names):
        value = "".join(child.itertext()).strip()
        if value:
            return value
    return None


def _link(element: ET.Element) -> str | None:
    for child in _children(element, "link"):
        relation = child.attrib.get("rel", "alternate").lower()
        href = child.attrib.get("href")
        if href and relation in {"alternate", ""}:
            return href.strip()
        value = "".join(child.itertext()).strip()
        if value:
            return value
    return None


def parse_feed(
    document: RawDocument,
    policy: SourcePolicy,
    *,
    event_type: str,
) -> tuple[NormalizedEvent, ...]:
    upper_prefix = document.content[:4_096].upper()
    if b"<!DOCTYPE" in upper_prefix or b"<!ENTITY" in upper_prefix:
        raise FeedParseError("DTD/entity declarations are not accepted")
    try:
        root = ET.fromstring(document.content)
    except ET.ParseError as error:
        raise FeedParseError("source returned malformed XML") from error
    item_names = {"item", "entry"}
    items = [element for element in root.iter() if _local_name(element.tag) in item_names]
    results: list[NormalizedEvent] = []
    for item in items:
        title = normalise_text(_text(item, "title") or "")
        raw_link = _link(item)
        if not title or not raw_link:
            continue
        try:
            link = canonicalize_url(raw_link, base_url=document.canonical_url)
        except ValueError:
            continue
        published = parse_published_datetime(
            _text(item, "pubdate", "published", "updated", "date"),
            default_timezone=UTC,
        )
        external_id = _text(item, "guid", "id")
        summary = normalise_text(
            _text(item, "description", "summary") or "",
            limit=policy.max_summary_characters,
        )
        available = max(
            document.first_seen_at,
            published if published is not None else document.first_seen_at,
        )
        results.append(
            NormalizedEvent(
                source_id=policy.source_id,
                canonical_url=link,
                title=title,
                summary=summary,
                event_type=event_type,
                source_tier=policy.source_tier,
                published_at=published,
                first_seen_at=document.first_seen_at,
                retrieved_at=document.retrieved_at,
                available_at=available,
                external_id=external_id,
                raw_document_id=document.document_id,
            )
        )
    return tuple(results)


class RssFeedSource:
    def __init__(
        self,
        endpoint: str,
        policy: SourcePolicy,
        fetcher: PublicDocumentFetcher,
        *,
        event_type: str = "news",
    ) -> None:
        policy.validate_url(endpoint)
        self._endpoint = endpoint
        self._policy = policy
        self._fetcher = fetcher
        self._event_type = event_type

    async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch:
        fetched = await self._fetcher.fetch(self._policy, self._endpoint, cursor)
        if fetched.document is None:
            return NewsBatch(
                source_id=self._policy.source_id,
                cursor=fetched.cursor,
                not_modified=fetched.not_modified,
            )
        events = parse_feed(fetched.document, self._policy, event_type=self._event_type)
        return NewsBatch(
            source_id=self._policy.source_id,
            cursor=fetched.cursor,
            documents=(fetched.document,),
            events=events,
        )
