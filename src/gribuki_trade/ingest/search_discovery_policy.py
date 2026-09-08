"""搜索发现结果的纯校验、聚类与发布者确认策略。

本模块不执行网络请求，也不访问存储。它把多个搜索提供方返回的线索规范化，
再按链接、标题和发布域聚类，供发现源 facade 生成可追溯的事件。
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from urllib.parse import urlsplit

from gribuki_trade.pipeline.normalize import canonicalize_url, normalise_text
from gribuki_trade.ports.news import DiscoveryHit


class DiscoveryConfirmationBasis(StrEnum):
    """搜索发现晋级的保守确认依据。"""

    NONE = "none"
    OFFICIAL_HOST = "official_host"
    INDEPENDENT_PUBLISHERS = "independent_publishers"

_PROVIDER_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,47}$")
_TITLE_KEY = re.compile(r"[\W_]+", re.UNICODE)
_GENERIC_TITLE_KEYS = frozenset(
    {"详情", "首页", "新闻", "公告", "最新消息", "市场动态", "untitled"}
)
_MIN_TITLE_CONFIRMATION_CHARACTERS = 6
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


def _public_host(host: str) -> bool:
    if host == "localhost" or host.endswith((".localhost", ".local")):
        return False
    try:
        return ipaddress.ip_address(host).is_global
    except ValueError:
        return True


def _provider_id(value: str) -> str:
    provider_id = value.strip().lower()
    if not _PROVIDER_ID.fullmatch(provider_id):
        raise ValueError("provider_id must be a safe lowercase identifier")
    return provider_id


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


__all__ = [
    "_DiscoveryCluster",
    "_ResolvedDiscovery",
    "DiscoveryConfirmationBasis",
    "_cluster_hits",
    "_official_hosts",
    "_provider_id",
    "_publisher_identity",
    "_safe_hit",
]
