from __future__ import annotations

from datetime import UTC, datetime

from gribuki_trade.ingest.search_discovery_policy import (
    DiscoveryConfirmationBasis,
    _cluster_hits,
    _official_hosts,
    _publisher_identity,
    _safe_hit,
)
from gribuki_trade.ports.news import DiscoveryHit

NOW = datetime(2026, 8, 13, 9, 30, tzinfo=UTC)


def _hit(provider: str, url: str, title: str = "央行发布新的政策工具") -> DiscoveryHit:
    return DiscoveryHit(provider, url, title, "摘要", NOW, ("USDCNY",))


def test_safe_hit_normalizes_urls_and_discards_future_or_empty_entities() -> None:
    safe = _safe_hit(
        _hit(" Tavily ", "https://example.com/item?utm_source=search"),
        now=NOW,
    )
    assert safe is not None
    assert safe.provider_id == "tavily"
    assert safe.url == "https://example.com/item"
    assert _safe_hit(_hit("tavily", "https://example.com/item", title=""), now=NOW) is None


def test_cluster_confirms_official_host_and_merges_entities() -> None:
    resolved = _cluster_hits(
        (
            _hit("tavily", "https://www.pbc.gov.cn/item"),
            _hit("searxng", "https://media.example/item"),
        ),
        now=NOW,
        official_hosts=_official_hosts(("pbc.gov.cn",)),
    )

    assert len(resolved) == 1
    assert resolved[0].confirmation_basis is DiscoveryConfirmationBasis.OFFICIAL_HOST
    assert resolved[0].official_host == "pbc.gov.cn"
    assert resolved[0].publisher_identities == (
        "official-body:pboc",
        "publisher-domain:media.example",
    )


def test_publisher_identity_uses_registrable_domain() -> None:
    assert _publisher_identity("https://sub.news.example.co.uk/story") == (
        "publisher-domain:example.co.uk"
    )
