import asyncio
from datetime import UTC, datetime
from functools import wraps
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.domain.events import RawDocument, SourcePolicy, SourceTier
from gribuki_trade.ingest.html import HtmlListConfig, HtmlListSource, parse_html_list
from gribuki_trade.ingest.rss import FeedParseError, RssFeedSource, parse_feed
from gribuki_trade.ports.news import DocumentFetch, FetchCursor

NOW = datetime(2026, 8, 13, 3, 0, tzinfo=UTC)


def async_test(function):
    @wraps(function)
    def run(*args, **kwargs):
        return asyncio.run(function(*args, **kwargs))

    return run


def policy() -> SourcePolicy:
    return SourcePolicy(
        source_id="exchange",
        allowed_hosts=frozenset({"feed.example.com"}),
        source_tier=SourceTier.OFFICIAL,
    )


def document(content: bytes, content_type: str, *, encoding: str | None = "utf-8") -> RawDocument:
    return RawDocument(
        source_id="exchange",
        canonical_url="https://feed.example.com/list",
        content_type=content_type,
        content=content,
        first_seen_at=NOW,
        retrieved_at=NOW,
        available_at=NOW,
        encoding=encoding,
    )


class FakeFetcher:
    def __init__(self, result: DocumentFetch) -> None:
        self.result = result

    async def fetch(
        self,
        source_policy: SourcePolicy,
        url: str,
        cursor: FetchCursor | None = None,
    ) -> DocumentFetch:
        del source_policy, url, cursor
        return self.result


def test_rss_and_atom_metadata_become_point_in_time_events() -> None:
    rss = b"""<?xml version="1.0"?>
    <rss><channel><item>
      <guid>announcement-1</guid>
      <title>  Major &amp; announcement </title>
      <link>/a/1?utm_source=feed&amp;b=2&amp;a=1</link>
      <pubDate>Thu, 13 Aug 2026 10:30:00 +0800</pubDate>
      <description><![CDATA[<p>Short <b>evidence</b>.</p><script>ignore()</script>]]></description>
    </item></channel></rss>"""

    events = parse_feed(document(rss, "application/rss+xml"), policy(), event_type="announcement")

    assert len(events) == 1
    event = events[0]
    assert event.title == "Major & announcement"
    assert event.summary == "Short evidence."
    assert event.canonical_url == "https://feed.example.com/a/1?a=1&b=2"
    assert event.published_at == datetime(2026, 8, 13, 2, 30, tzinfo=UTC)
    assert event.available_at == NOW
    assert event.external_id == "announcement-1"
    assert event.raw_document_id is not None

    atom = b"""<feed xmlns="http://www.w3.org/2005/Atom"><entry>
      <id>tag:example,2026:2</id><title>Atom title</title>
      <link rel="alternate" href="https://feed.example.com/a/2" />
      <updated>2026-08-13T03:05:00Z</updated><summary>Summary</summary>
    </entry></feed>"""
    atom_event = parse_feed(document(atom, "application/atom+xml"), policy(), event_type="news")[0]
    assert atom_event.published_at == datetime(2026, 8, 13, 3, 5, tzinfo=UTC)
    assert atom_event.available_at == datetime(2026, 8, 13, 3, 5, tzinfo=UTC)


def test_rss_rejects_dtd_and_external_entity_declarations() -> None:
    malicious = b'<!DOCTYPE rss [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><rss />'
    with pytest.raises(FeedParseError, match="DTD"):
        parse_feed(document(malicious, "application/xml"), policy(), event_type="news")


def test_html_list_adapter_extracts_configured_metadata_without_following_links() -> None:
    markup = """
    <html><head><meta charset="gb18030"></head><body>
      <ul><li class="news important">
        <a class="headline" href="/news/1?utm_medium=x">政策消息</a>
        <time class="published" datetime="2026-08-13 10:45:00"></time>
        <p class="summary">简短 <b>摘要</b><img src="x"></p>
      </li></ul>
    </body></html>
    """.encode("gb18030")
    config = HtmlListConfig(
        item_tag="li",
        item_classes=frozenset({"news"}),
        link_classes=frozenset({"headline"}),
        time_classes=frozenset({"published"}),
        summary_tag="p",
        summary_classes=frozenset({"summary"}),
        event_type="policy",
        source_timezone=ZoneInfo("Asia/Shanghai"),
    )

    event = parse_html_list(document(markup, "text/html", encoding="gb18030"), policy(), config)[0]

    assert event.title == "政策消息"
    assert event.summary == "简短 摘要"
    assert event.canonical_url == "https://feed.example.com/news/1"
    assert event.published_at == datetime(2026, 8, 13, 2, 45, tzinfo=UTC)
    assert event.event_type == "policy"


@async_test
async def test_sources_return_empty_batch_for_not_modified_response() -> None:
    fetched = DocumentFetch(None, FetchCursor(etag="x"), not_modified=True)

    rss = await RssFeedSource(
        "https://feed.example.com/rss",
        policy(),
        FakeFetcher(fetched),
    ).collect()
    html = await HtmlListSource(
        "https://feed.example.com/list",
        policy(),
        FakeFetcher(fetched),
        HtmlListConfig(),
    ).collect()

    assert rss.not_modified and not rss.events and not rss.documents
    assert html.not_modified and not html.events and not html.documents
