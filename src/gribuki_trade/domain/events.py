"""Point-in-time domain types for public information sources.

The ingestion layer keeps the time at which information became observable
separate from the timestamp asserted by its publisher.  That distinction is
required to replay a strategy without accidentally using information that was
not available at the decision time.
"""

from __future__ import annotations

import hashlib
import ipaddress
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from urllib.parse import urlsplit

_SOURCE_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")

# Search discovery has an explicit two-stage lineage.  A hint is displayable
# but never actionable evidence.  "Confirmed" means the discovery lead met a
# promotion rule (official host or independent-provider agreement), not that
# the underlying claim has been proven true.
DISCOVERY_HINT_EVENT_TYPE = "discovery_hint"
DISCOVERY_CONFIRMED_EVENT_TYPE = "discovery_confirmed"


class SourceTier(StrEnum):
    """Trust tier used by recommendation policy; it is not a truth score."""

    OFFICIAL = "OFFICIAL"
    LICENSED = "LICENSED"
    PUBLIC_MEDIA = "PUBLIC_MEDIA"
    SOCIAL = "SOCIAL"


class ContentRetention(StrEnum):
    """Whether a fetched response body may be retained in the raw archive."""

    FULL_DOCUMENT = "FULL_DOCUMENT"
    METADATA_ONLY = "METADATA_ONLY"


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _normalise_host(host: str) -> str:
    value = host.strip().rstrip(".").lower()
    if not value:
        raise ValueError("allowed_hosts must not contain empty hosts")
    try:
        return value.encode("idna").decode("ascii")
    except UnicodeError as error:
        raise ValueError(f"invalid host: {host!r}") from error


def _is_public_host(host: str) -> bool:
    if host == "localhost" or host.endswith(".localhost") or host.endswith(".local"):
        return False
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return True
    return address.is_global


@dataclass(frozen=True, slots=True)
class SourcePolicy:
    """Fail-closed network and storage policy for one source.

    A source must have an explicit host allowlist.  Redirects are checked
    against the same policy, which prevents an upstream page from redirecting
    the collector to localhost or an unrelated host.
    """

    source_id: str
    allowed_hosts: frozenset[str]
    source_tier: SourceTier = SourceTier.PUBLIC_MEDIA
    allowed_schemes: frozenset[str] = frozenset({"https"})
    allow_subdomains: bool = False
    allowed_content_types: tuple[str, ...] = (
        "application/atom+xml",
        "application/rss+xml",
        "application/xml",
        "text/html",
        "text/xml",
    )
    retention: ContentRetention = ContentRetention.FULL_DOCUMENT
    max_response_bytes: int = 2_000_000
    timeout_seconds: float = 15.0
    max_attempts: int = 3
    backoff_base_seconds: float = 1.0
    backoff_cap_seconds: float = 60.0
    max_redirects: int = 3
    max_summary_characters: int = 1_000

    def __post_init__(self) -> None:
        source_id = self.source_id.strip().lower()
        if not _SOURCE_ID.fullmatch(source_id):
            raise ValueError("source_id must be a safe lowercase identifier")
        hosts = frozenset(_normalise_host(host) for host in self.allowed_hosts)
        if not hosts:
            raise ValueError("allowed_hosts must not be empty")
        if any(not _is_public_host(host) for host in hosts):
            raise ValueError("allowed_hosts must contain only public hosts")
        schemes = frozenset(scheme.strip().lower() for scheme in self.allowed_schemes)
        if not schemes or not schemes <= {"http", "https"}:
            raise ValueError("allowed_schemes must contain only http and/or https")
        content_types = tuple(
            content_type.partition(";")[0].strip().lower()
            for content_type in self.allowed_content_types
        )
        if not all(content_types):
            raise ValueError("allowed_content_types must not contain empty values")
        if self.max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be positive")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if self.backoff_base_seconds <= 0 or self.backoff_cap_seconds <= 0:
            raise ValueError("backoff values must be positive")
        if self.backoff_base_seconds > self.backoff_cap_seconds:
            raise ValueError("backoff_base_seconds must not exceed its cap")
        if self.max_redirects < 0:
            raise ValueError("max_redirects must not be negative")
        if self.max_summary_characters < 1:
            raise ValueError("max_summary_characters must be positive")
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "allowed_hosts", hosts)
        object.__setattr__(self, "allowed_schemes", schemes)
        object.__setattr__(self, "allowed_content_types", content_types)

    def validate_url(self, url: str) -> None:
        """Raise before I/O when a URL is outside this source's boundary."""

        parts = urlsplit(url)
        scheme = parts.scheme.lower()
        host = (parts.hostname or "").rstrip(".").lower()
        try:
            host = host.encode("idna").decode("ascii")
        except UnicodeError as error:
            raise ValueError("URL host is invalid") from error
        if scheme not in self.allowed_schemes:
            raise ValueError("URL scheme is not allowed by source policy")
        if not host or not _is_public_host(host):
            raise ValueError("URL must use a public host")
        if parts.username is not None or parts.password is not None:
            raise ValueError("URL user information is not allowed")
        exact = host in self.allowed_hosts
        child = self.allow_subdomains and any(
            host.endswith(f".{item}") for item in self.allowed_hosts
        )
        if not (exact or child):
            raise ValueError("URL host is not allowed by source policy")

    def accepts_content_type(self, value: str | None) -> bool:
        if value is None:
            return False
        media_type = value.partition(";")[0].strip().lower()
        return media_type in self.allowed_content_types


@dataclass(frozen=True, slots=True)
class RawDocument:
    """One immutable HTTP representation and its observation timestamps."""

    source_id: str
    canonical_url: str
    content_type: str
    content: bytes = field(repr=False)
    first_seen_at: datetime
    retrieved_at: datetime
    available_at: datetime
    published_at: datetime | None = None
    etag: str | None = None
    last_modified: str | None = None
    encoding: str | None = None
    content_sha256: str = ""

    def __post_init__(self) -> None:
        source_id = self.source_id.strip().lower()
        if not _SOURCE_ID.fullmatch(source_id):
            raise ValueError("source_id must be a safe lowercase identifier")
        if not self.canonical_url.strip():
            raise ValueError("canonical_url must not be empty")
        if not self.content_type.strip():
            raise ValueError("content_type must not be empty")
        first_seen = _utc(self.first_seen_at, "first_seen_at")
        retrieved = _utc(self.retrieved_at, "retrieved_at")
        available = _utc(self.available_at, "available_at")
        published = (
            None if self.published_at is None else _utc(self.published_at, "published_at")
        )
        if retrieved < first_seen:
            raise ValueError("retrieved_at must not precede first_seen_at")
        if available < first_seen:
            raise ValueError("available_at must not precede first_seen_at")
        if published is not None and available < published:
            raise ValueError("available_at must not precede published_at")
        digest = self.content_sha256 or hashlib.sha256(self.content).hexdigest()
        if not _SHA256.fullmatch(digest):
            raise ValueError("content_sha256 must be a lowercase SHA-256 hex digest")
        if digest != hashlib.sha256(self.content).hexdigest():
            raise ValueError("content_sha256 does not match content")
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "content_type", self.content_type.partition(";")[0].lower())
        object.__setattr__(self, "first_seen_at", first_seen)
        object.__setattr__(self, "retrieved_at", retrieved)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "published_at", published)
        object.__setattr__(self, "content_sha256", digest)

    @property
    def document_id(self) -> str:
        material = f"{self.source_id}\0{self.canonical_url}\0{self.content_sha256}".encode()
        return hashlib.sha256(material).hexdigest()


@dataclass(frozen=True, slots=True)
class NormalizedEvent:
    """A small, source-linked event suitable for strategy evidence packs."""

    source_id: str
    canonical_url: str
    title: str
    summary: str
    event_type: str
    source_tier: SourceTier
    first_seen_at: datetime
    retrieved_at: datetime
    available_at: datetime
    published_at: datetime | None = None
    external_id: str | None = None
    raw_document_id: str | None = None
    entities: tuple[str, ...] = ()
    content_sha256: str = ""
    event_id: str = ""
    revision_id: str = ""
    revision_number: int = 1
    supersedes_revision_id: str | None = None

    def __post_init__(self) -> None:
        source_id = self.source_id.strip().lower()
        title = " ".join(self.title.split())
        summary = " ".join(self.summary.split())
        event_type = self.event_type.strip().lower()
        if not _SOURCE_ID.fullmatch(source_id):
            raise ValueError("source_id must be a safe lowercase identifier")
        if not self.canonical_url.strip():
            raise ValueError("canonical_url must not be empty")
        if not title:
            raise ValueError("title must not be empty")
        if not event_type:
            raise ValueError("event_type must not be empty")
        if self.revision_number < 1:
            raise ValueError("revision_number must be positive")
        first_seen = _utc(self.first_seen_at, "first_seen_at")
        retrieved = _utc(self.retrieved_at, "retrieved_at")
        available = _utc(self.available_at, "available_at")
        published = (
            None if self.published_at is None else _utc(self.published_at, "published_at")
        )
        if retrieved < first_seen:
            raise ValueError("retrieved_at must not precede first_seen_at")
        if available < first_seen:
            raise ValueError("available_at must not precede first_seen_at")
        if published is not None and available < published:
            raise ValueError("available_at must not precede published_at")

        content_material = f"{title}\0{summary}\0{published.isoformat() if published else ''}"
        expected_content_digest = hashlib.sha256(content_material.encode()).hexdigest()
        content_digest = self.content_sha256 or expected_content_digest
        if not _SHA256.fullmatch(content_digest):
            raise ValueError("content_sha256 must be a lowercase SHA-256 hex digest")
        if content_digest != expected_content_digest:
            raise ValueError("content_sha256 does not match normalized event content")
        identity = (self.external_id or self.canonical_url).strip()
        event_id = self.event_id or hashlib.sha256(f"{source_id}\0{identity}".encode()).hexdigest()
        revision_id = self.revision_id or hashlib.sha256(
            f"{event_id}\0{content_digest}".encode()
        ).hexdigest()
        if not _SHA256.fullmatch(event_id) or not _SHA256.fullmatch(revision_id):
            raise ValueError("event_id and revision_id must be lowercase SHA-256 digests")
        if self.raw_document_id is not None and not _SHA256.fullmatch(self.raw_document_id):
            raise ValueError("raw_document_id must be a lowercase SHA-256 digest")
        entities = tuple(dict.fromkeys(item.strip() for item in self.entities if item.strip()))

        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "summary", summary)
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(self, "first_seen_at", first_seen)
        object.__setattr__(self, "retrieved_at", retrieved)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "published_at", published)
        object.__setattr__(self, "content_sha256", content_digest)
        object.__setattr__(self, "event_id", event_id)
        object.__setattr__(self, "revision_id", revision_id)
        object.__setattr__(self, "entities", entities)
