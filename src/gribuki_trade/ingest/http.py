"""带条件请求与退避策略、且失败关闭的公共 HTTP 采集器。"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx

from gribuki_trade.domain.events import RawDocument, SourcePolicy
from gribuki_trade.pipeline.normalize import canonicalize_url
from gribuki_trade.ports.news import (
    DocumentFetch,
    FetchCursor,
    NewsHttpRequest,
    NewsHttpResponse,
    NewsHttpTransport,
)


class NewsTransportError(ConnectionError):
    """有意移除网址与正文细节的网络传输故障。"""


class SourceFetchError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        cursor: FetchCursor,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.cursor = cursor
        self.status_code = status_code


class SourceAccessDenied(SourceFetchError):
    """收到 401/403 响应；采集器不会尝试规避。"""


class SourceRateLimited(SourceFetchError):
    """收到 429 响应；调度器必须等待至 ``next_allowed_at``。"""


class SourceBackoffActive(SourceFetchError):
    """调用方在数据源退避期结束前尝试输入输出操作。"""


class SourceUnavailable(SourceFetchError):
    """瞬时故障重试次数已耗尽，或响应不可接受。"""


class HttpxNewsTransport:
    """禁用重定向、以便逐跳检查策略的 HTTPX 传输层。"""

    async def send(self, request: NewsHttpRequest) -> NewsHttpResponse:
        request_url, request_headers, extensions = _pinned_httpx_request(request)
        try:
            async with httpx.AsyncClient(
                follow_redirects=False,
                trust_env=False,
            ) as client:
                response = await client.request(
                    request.method,
                    request_url,
                    headers=request_headers,
                    content=request.body,
                    timeout=request.timeout_seconds,
                    extensions=extensions,
                )
        except httpx.HTTPError:
            raise NewsTransportError("public source request failed") from None
        return NewsHttpResponse(
            status_code=response.status_code,
            headers=dict(response.headers),
            body=response.content,
        )


def _pinned_httpx_request(
    request: NewsHttpRequest,
) -> tuple[str, dict[str, str], dict[str, object]]:
    if request.resolved_ip is None or request.server_hostname is None:
        return request.url, dict(request.headers), {}
    try:
        parsed = urlsplit(request.url)
        hostname = parsed.hostname
        port = parsed.port
        address = ipaddress.ip_address(request.resolved_ip)
    except ValueError:
        raise NewsTransportError("public source pinned endpoint is invalid") from None
    expected_hostname = request.server_hostname.rstrip(".").casefold()
    if hostname is None or hostname.rstrip(".").casefold() != expected_hostname:
        raise NewsTransportError("public source pinned endpoint is invalid")
    if not address.is_global or parsed.username is not None or parsed.password is not None:
        raise NewsTransportError("public source pinned endpoint is invalid")

    address_text = f"[{address.compressed}]" if address.version == 6 else address.compressed
    authority = address_text if port is None else f"{address_text}:{port}"
    pinned_url = urlunsplit((parsed.scheme, authority, parsed.path, parsed.query, ""))
    host_header = expected_hostname
    default_port = 443 if parsed.scheme == "https" else 80
    if port is not None and port != default_port:
        host_header = f"{host_header}:{port}"
    headers = {key: value for key, value in request.headers.items() if key.casefold() != "host"}
    headers["Host"] = host_header
    return pinned_url, headers, {"sni_hostname": expected_hostname}


HostResolver = Callable[[str, int], Awaitable[Sequence[str]]]


async def _system_resolver(hostname: str, port: int) -> Sequence[str]:
    results = await asyncio.get_running_loop().getaddrinfo(
        hostname,
        port,
        family=socket.AF_UNSPEC,
        type=socket.SOCK_STREAM,
        proto=socket.IPPROTO_TCP,
    )
    return tuple(str(result[4][0]) for result in results)


def _public_resolution(addresses: Sequence[str]) -> frozenset[str]:
    if not addresses:
        raise ValueError("source hostname returned no addresses")
    normalized: set[str] = set()
    for value in addresses:
        if not isinstance(value, str) or "%" in value:
            raise ValueError("source hostname returned an invalid address")
        try:
            address = ipaddress.ip_address(value)
        except ValueError:
            raise ValueError("source hostname returned an invalid address") from None
        if not address.is_global:
            raise ValueError("source hostname resolved to a non-public address")
        normalized.add(address.compressed)
    return frozenset(normalized)


def _url_endpoint(url: str) -> tuple[str, int]:
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        raise ValueError("source URL endpoint is invalid") from None
    if hostname is None:
        raise ValueError("source URL endpoint is invalid")
    if port is None:
        port = 443 if parsed.scheme == "https" else 80
    return hostname.rstrip(".").casefold(), port


def _header(headers: dict[str, str], name: str) -> str | None:
    expected = name.lower()
    return next((value for key, value in headers.items() if key.lower() == expected), None)


def _charset(content_type: str | None) -> str | None:
    if content_type is None:
        return None
    for item in content_type.split(";")[1:]:
        key, separator, value = item.partition("=")
        if separator and key.strip().lower() == "charset":
            return value.strip().strip("\"'") or None
    return None


def _retry_after(value: str | None, now: datetime) -> datetime | None:
    if value is None:
        return None
    try:
        seconds = max(0, int(value.strip()))
        return now + timedelta(seconds=seconds)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=UTC)
        return max(now, parsed.astimezone(UTC))


class PublicHttpFetcher:
    def __init__(
        self,
        transport: NewsHttpTransport,
        *,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        resolver: HostResolver | None = None,
        user_agent: str = "gribuki-trade/0.1 public-information-collector",
    ) -> None:
        self._transport = transport
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleeper = sleeper
        self._resolver = resolver or _system_resolver
        self._user_agent = user_agent

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("fetcher clock must return a timezone-aware datetime")
        return value.astimezone(UTC)

    @staticmethod
    def _delay(policy: SourcePolicy, failure_number: int) -> float:
        exponent = max(0, failure_number - 1)
        multiplier = float(2**exponent)
        return min(policy.backoff_cap_seconds, policy.backoff_base_seconds * multiplier)

    def _failed_cursor(
        self,
        policy: SourcePolicy,
        previous: FetchCursor,
        now: datetime,
        *,
        retry_at: datetime | None = None,
    ) -> FetchCursor:
        failures = previous.consecutive_failures + 1
        next_allowed = retry_at or now + timedelta(seconds=self._delay(policy, failures))
        return FetchCursor(
            etag=previous.etag,
            last_modified=previous.last_modified,
            content_sha256=previous.content_sha256,
            first_seen_at=previous.first_seen_at,
            consecutive_failures=failures,
            next_allowed_at=next_allowed,
        )

    async def fetch(
        self,
        policy: SourcePolicy,
        url: str,
        cursor: FetchCursor | None = None,
    ) -> DocumentFetch:
        state = cursor or FetchCursor()
        now = self._now()
        if state.next_allowed_at is not None:
            next_allowed = state.next_allowed_at.astimezone(UTC)
            if now < next_allowed:
                raise SourceBackoffActive("source backoff is active", cursor=state)

        current_url = canonicalize_url(url)
        policy.validate_url(current_url)
        headers = {
            "Accept": ", ".join(policy.allowed_content_types),
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": self._user_agent,
        }
        if state.etag:
            headers["If-None-Match"] = state.etag
        if state.last_modified:
            headers["If-Modified-Since"] = state.last_modified

        redirects = 0
        resolved_hosts: dict[tuple[str, int], frozenset[str]] = {}
        for attempt in range(1, policy.max_attempts + 1):
            endpoint = _url_endpoint(current_url)
            try:
                resolved = _public_resolution(await self._resolver(*endpoint))
            except (OSError, TimeoutError, ValueError):
                failed = self._failed_cursor(policy, state, self._now())
                raise SourceUnavailable(
                    "source hostname resolution was rejected",
                    cursor=failed,
                ) from None
            previous_resolution = resolved_hosts.get(endpoint)
            if previous_resolution is not None and previous_resolution != resolved:
                failed = self._failed_cursor(policy, state, self._now())
                raise SourceUnavailable(
                    "source DNS rebinding was rejected",
                    cursor=failed,
                )
            resolved_hosts[endpoint] = resolved
            selected_address = sorted(resolved)[0]
            request = NewsHttpRequest(
                "GET",
                current_url,
                headers,
                policy.timeout_seconds,
                resolved_ip=selected_address,
                server_hostname=endpoint[0],
            )
            try:
                response = await self._transport.send(request)
            except (NewsTransportError, TimeoutError, OSError):
                if attempt < policy.max_attempts:
                    await self._sleeper(self._delay(policy, attempt))
                    continue
                failed = self._failed_cursor(policy, state, self._now())
                raise SourceUnavailable("public source retries exhausted", cursor=failed) from None

            now = self._now()
            status = response.status_code
            if status in {301, 302, 303, 307, 308}:
                location = _header(response.headers, "Location")
                if not location or redirects >= policy.max_redirects:
                    failed = self._failed_cursor(policy, state, now)
                    raise SourceUnavailable(
                        "public source returned an unacceptable redirect",
                        cursor=failed,
                        status_code=status,
                    )
                redirected = canonicalize_url(urljoin(current_url, location))
                policy.validate_url(redirected)
                current_url = redirected
                redirects += 1
                continue

            if status == 304:
                return DocumentFetch(
                    document=None,
                    cursor=FetchCursor(
                        etag=state.etag,
                        last_modified=state.last_modified,
                        content_sha256=state.content_sha256,
                        first_seen_at=state.first_seen_at,
                    ),
                    not_modified=True,
                )
            if status in {401, 403}:
                failed = self._failed_cursor(policy, state, now)
                raise SourceAccessDenied(
                    "source denied access; collector stopped",
                    cursor=failed,
                    status_code=status,
                )
            if status == 429:
                retry_at = _retry_after(_header(response.headers, "Retry-After"), now)
                failed = self._failed_cursor(policy, state, now, retry_at=retry_at)
                raise SourceRateLimited(
                    "source rate limited the collector",
                    cursor=failed,
                    status_code=status,
                )
            if status >= 500:
                if attempt < policy.max_attempts:
                    await self._sleeper(self._delay(policy, attempt))
                    continue
                failed = self._failed_cursor(policy, state, now)
                raise SourceUnavailable(
                    "public source retries exhausted",
                    cursor=failed,
                    status_code=status,
                )
            if status != 200:
                failed = self._failed_cursor(policy, state, now)
                raise SourceUnavailable(
                    "public source returned a non-success response",
                    cursor=failed,
                    status_code=status,
                )

            content_type = _header(response.headers, "Content-Type")
            if not policy.accepts_content_type(content_type):
                failed = self._failed_cursor(policy, state, now)
                raise SourceUnavailable("source content type is not allowed", cursor=failed)
            content_length = _header(response.headers, "Content-Length")
            if content_length is not None:
                try:
                    if int(content_length) > policy.max_response_bytes:
                        raise SourceUnavailable(
                            "source response exceeds configured size",
                            cursor=self._failed_cursor(policy, state, now),
                        )
                except ValueError:
                    pass
            if len(response.body) > policy.max_response_bytes:
                raise SourceUnavailable(
                    "source response exceeds configured size",
                    cursor=self._failed_cursor(policy, state, now),
                )
            digest = hashlib.sha256(response.body).hexdigest()
            first_seen = (
                state.first_seen_at
                if state.content_sha256 == digest and state.first_seen_at is not None
                else now
            )
            document = RawDocument(
                source_id=policy.source_id,
                canonical_url=current_url,
                content_type=(content_type or "").partition(";")[0],
                content=response.body,
                first_seen_at=first_seen,
                retrieved_at=now,
                available_at=now,
                etag=_header(response.headers, "ETag"),
                last_modified=_header(response.headers, "Last-Modified"),
                encoding=_charset(content_type),
                content_sha256=digest,
            )
            return DocumentFetch(
                document=document,
                cursor=FetchCursor(
                    etag=document.etag,
                    last_modified=document.last_modified,
                    content_sha256=digest,
                    first_seen_at=first_seen,
                ),
            )

        failed = self._failed_cursor(policy, state, self._now())
        raise SourceUnavailable("public source request could not complete", cursor=failed)
