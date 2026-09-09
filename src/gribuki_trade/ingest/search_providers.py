"""Tavily 与 SearXNG 搜索提供方的 HTTP/JSON 边界。"""

from __future__ import annotations

import asyncio
import ipaddress
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from urllib.parse import urlencode, urlsplit, urlunsplit

from gribuki_trade.ingest.http import NewsTransportError
from gribuki_trade.pipeline.normalize import normalise_text, parse_published_datetime
from gribuki_trade.ports.news import (
    DiscoveryHit,
    DiscoveryQuery,
    NewsHttpRequest,
    NewsHttpResponse,
    NewsHttpTransport,
)

from . import search_discovery_policy as _policy

TAVILY_SEARCH_ENDPOINT = "https://api.tavily.com/search"
_PROVIDER_ID = _policy._PROVIDER_ID

class SearchProviderError(RuntimeError):
    """单次提供方及查询尝试的脱敏基础错误。"""

    def __init__(
        self,
        provider_id: str,
        message: str,
        *,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.provider_id = provider_id
        self.status_code = status_code


class SearchProviderAccessDenied(SearchProviderError):
    """提供方返回 401/403；不会尝试绕过凭据验证。"""


class SearchProviderRateLimited(SearchProviderError):
    """提供方返回 429。"""

    def __init__(
        self,
        provider_id: str,
        *,
        retry_after_seconds: int | None,
    ) -> None:
        super().__init__(provider_id, "search provider rate limited", status_code=429)
        self.retry_after_seconds = retry_after_seconds


class SearchProviderTimeout(SearchProviderError):
    """注入的传输层未能在提供方超时期限内完成。"""


class SearchProviderTransportError(SearchProviderError):
    """请求在收到 HTTP 响应前失败。"""


class SearchProviderUnavailable(SearchProviderError):
    """提供方返回非成功状态或过大的响应。"""


class SearchProviderPayloadError(SearchProviderError):
    """成功响应不是有界的 JSON 搜索结果对象。"""


def _header(headers: Mapping[str, str], name: str) -> str | None:
    expected = name.casefold()
    return next(
        (value for key, value in headers.items() if key.casefold() == expected),
        None,
    )


def _public_host(host: str) -> bool:
    if host == "localhost" or host.endswith((".localhost", ".local")):
        return False
    try:
        return ipaddress.ip_address(host).is_global
    except ValueError:
        return True


def _https_endpoint(value: str, *, exact_host: str | None = None) -> str:
    parts = urlsplit(value.strip())
    host = (parts.hostname or "").rstrip(".").lower()
    try:
        host = host.encode("idna").decode("ascii")
        port = parts.port
    except (UnicodeError, ValueError) as error:
        raise ValueError("search endpoint is invalid") from error
    if parts.scheme.lower() != "https":
        raise ValueError("search endpoint must use HTTPS")
    if not host or not _public_host(host):
        raise ValueError("search endpoint must use a public host")
    if parts.username is not None or parts.password is not None:
        raise ValueError("search endpoint must not contain credentials")
    if exact_host is not None and host != exact_host:
        raise ValueError("search endpoint host is not the official provider host")
    if parts.query or parts.fragment:
        raise ValueError("search endpoint must not contain a query or fragment")
    include_port = port is not None and port != 443
    netloc = f"{host}:{port}" if include_port else host
    return urlunsplit(("https", netloc, parts.path or "/", "", ""))


def _provider_id(value: str) -> str:
    provider_id = value.strip().lower()
    if not _PROVIDER_ID.fullmatch(provider_id):
        raise ValueError("provider_id must be a safe lowercase identifier")
    return provider_id


def _retry_after_seconds(response: NewsHttpResponse) -> int | None:
    value = _header(response.headers, "Retry-After")
    if value is None:
        return None
    try:
        return max(0, int(value.strip()))
    except ValueError:
        return None


async def _request_json(
    *,
    provider_id: str,
    transport: NewsHttpTransport,
    request: NewsHttpRequest,
    max_response_bytes: int,
) -> dict[str, object]:
    try:
        response = await asyncio.wait_for(
            transport.send(request),
            timeout=request.timeout_seconds,
        )
    except TimeoutError:
        raise SearchProviderTimeout(
            provider_id,
            "search provider request timed out",
        ) from None
    except (NewsTransportError, OSError):
        raise SearchProviderTransportError(
            provider_id,
            "search provider transport failed",
        ) from None
    except Exception:
        # 自定义注入传输层可能抛出携带请求细节的提供方专用异常，必须在其
        # 进入日志之前规范化。
        raise SearchProviderTransportError(
            provider_id,
            "search provider transport failed",
        ) from None

    if response.status_code in {401, 403}:
        raise SearchProviderAccessDenied(
            provider_id,
            "search provider denied access",
            status_code=response.status_code,
        )
    if response.status_code == 429:
        raise SearchProviderRateLimited(
            provider_id,
            retry_after_seconds=_retry_after_seconds(response),
        )
    if response.status_code != 200:
        raise SearchProviderUnavailable(
            provider_id,
            "search provider returned a non-success response",
            status_code=response.status_code,
        )
    if len(response.body) > max_response_bytes:
        raise SearchProviderUnavailable(
            provider_id,
            "search provider response exceeded its configured limit",
            status_code=response.status_code,
        )
    content_type = _header(response.headers, "Content-Type")
    if content_type is None or content_type.partition(";")[0].strip().lower() != "application/json":
        raise SearchProviderPayloadError(
            provider_id,
            "search provider did not return application/json",
            status_code=response.status_code,
        )
    try:
        payload = json.loads(response.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise SearchProviderPayloadError(
            provider_id,
            "search provider returned invalid JSON",
            status_code=response.status_code,
        ) from None
    if not isinstance(payload, dict):
        raise SearchProviderPayloadError(
            provider_id,
            "search provider JSON root must be an object",
            status_code=response.status_code,
        )
    return {str(key): value for key, value in payload.items()}


def _result_items(
    payload: Mapping[str, object],
    *,
    provider_id: str,
) -> Sequence[object]:
    results = payload.get("results")
    if not isinstance(results, list):
        raise SearchProviderPayloadError(
            provider_id,
            "search provider results must be an array",
            status_code=200,
        )
    return results


def _published(item: Mapping[str, object]) -> datetime | None:
    for field_name in ("published_date", "publishedDate", "published_at", "date"):
        value = item.get(field_name)
        if isinstance(value, str):
            parsed = parse_published_datetime(value)
            if parsed is not None:
                return parsed
    return None


def _parse_hit(
    item: object,
    *,
    provider_id: str,
    entities: tuple[str, ...],
    summary_fields: tuple[str, ...],
) -> DiscoveryHit | None:
    if not isinstance(item, dict):
        return None
    values = {str(key): value for key, value in item.items()}
    url = values.get("url")
    title = values.get("title")
    if not isinstance(url, str) or not isinstance(title, str):
        return None
    title_text = normalise_text(title, limit=300)
    if not title_text:
        return None
    summary = ""
    for field_name in summary_fields:
        value = values.get(field_name)
        if isinstance(value, str) and value.strip():
            summary = normalise_text(value, limit=1_000)
            break
    return DiscoveryHit(
        provider_id=provider_id,
        url=url.strip(),
        title=title_text,
        summary=summary,
        published_at=_published(values),
        entities=entities,
    )


class TavilySearchProvider:
    """Tavily 官方 ``POST https://api.tavily.com/search`` 接口约定。"""

    provider_id = "tavily"

    def __init__(
        self,
        transport: NewsHttpTransport,
        *,
        api_key: str,
        timeout_seconds: float = 12.0,
        max_response_bytes: int = 2_000_000,
        topic: str = "news",
    ) -> None:
        key = api_key.strip()
        if not key or "\r" in key or "\n" in key:
            raise ValueError("Tavily API key must not be empty or contain newlines")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_response_bytes < 1:
            raise ValueError("max_response_bytes must be positive")
        if topic not in {"news", "finance"}:
            raise ValueError("Tavily topic must be news or finance")
        self._transport = transport
        self._api_key = key
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes
        self._topic = topic
        self._endpoint = _https_endpoint(
            TAVILY_SEARCH_ENDPOINT,
            exact_host="api.tavily.com",
        )

    def __repr__(self) -> str:
        return (
            "TavilySearchProvider(provider_id='tavily', "
            f"timeout_seconds={self._timeout_seconds!r}, topic={self._topic!r})"
        )

    async def search(self, query: DiscoveryQuery) -> tuple[DiscoveryHit, ...]:
        document: dict[str, object] = {
            "query": query.text,
            "topic": self._topic,
            "search_depth": "basic",
            "max_results": query.max_results,
            "include_answer": False,
            "include_raw_content": False,
            "include_images": False,
        }
        if query.time_range is not None:
            document["time_range"] = query.time_range.value
        body = json.dumps(
            document,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        request = NewsHttpRequest(
            method="POST",
            url=self._endpoint,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            timeout_seconds=self._timeout_seconds,
            body=body,
        )
        payload = await _request_json(
            provider_id=self.provider_id,
            transport=self._transport,
            request=request,
            max_response_bytes=self._max_response_bytes,
        )
        hits = (
            _parse_hit(
                item,
                provider_id=self.provider_id,
                entities=query.entities,
                summary_fields=("content", "snippet"),
            )
            for item in _result_items(payload, provider_id=self.provider_id)
        )
        return tuple(item for item in hits if item is not None)[: query.max_results]


class SearXNGSearchProvider:
    """已配置 SearXNG 实例的官方 ``GET /search`` JSON 接口。"""

    provider_id = "searxng"

    def __init__(
        self,
        transport: NewsHttpTransport,
        *,
        base_url: str,
        bearer_token: str | None = None,
        timeout_seconds: float = 12.0,
        max_response_bytes: int = 2_000_000,
        categories: str = "news",
        language: str = "zh-CN",
        safesearch: int = 1,
    ) -> None:
        endpoint = _https_endpoint(base_url)
        parts = urlsplit(endpoint)
        path = parts.path.rstrip("/")
        if not path.endswith("/search"):
            path = f"{path}/search" if path else "/search"
        self._endpoint = urlunsplit(("https", parts.netloc, path, "", ""))
        token = None if bearer_token is None else bearer_token.strip()
        if token is not None and (not token or "\r" in token or "\n" in token):
            raise ValueError("SearXNG bearer token must not be empty or contain newlines")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_response_bytes < 1:
            raise ValueError("max_response_bytes must be positive")
        if not categories.strip() or not language.strip():
            raise ValueError("SearXNG categories and language must not be empty")
        if safesearch not in {0, 1, 2}:
            raise ValueError("SearXNG safesearch must be 0, 1, or 2")
        self._transport = transport
        self._bearer_token = token
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes
        self._categories = categories.strip()
        self._language = language.strip()
        self._safesearch = safesearch

    def __repr__(self) -> str:
        return (
            "SearXNGSearchProvider(provider_id='searxng', "
            f"endpoint={self._endpoint!r}, timeout_seconds={self._timeout_seconds!r})"
        )

    async def search(self, query: DiscoveryQuery) -> tuple[DiscoveryHit, ...]:
        parameters: list[tuple[str, str]] = [
            ("q", query.text),
            ("format", "json"),
            ("categories", self._categories),
            ("language", self._language),
            ("safesearch", str(self._safesearch)),
            ("pageno", "1"),
        ]
        if query.time_range is not None:
            parameters.append(("time_range", query.time_range.value))
        headers = {"Accept": "application/json"}
        if self._bearer_token is not None:
            headers["Authorization"] = f"Bearer {self._bearer_token}"
        request = NewsHttpRequest(
            method="GET",
            url=f"{self._endpoint}?{urlencode(parameters)}",
            headers=headers,
            timeout_seconds=self._timeout_seconds,
        )
        payload = await _request_json(
            provider_id=self.provider_id,
            transport=self._transport,
            request=request,
            max_response_bytes=self._max_response_bytes,
        )
        hits = (
            _parse_hit(
                item,
                provider_id=self.provider_id,
                entities=query.entities,
                summary_fields=("content", "snippet"),
            )
            for item in _result_items(payload, provider_id=self.provider_id)
        )
        return tuple(item for item in hits if item is not None)[: query.max_results]

__all__ = [
    "TAVILY_SEARCH_ENDPOINT",
    "SearchProviderAccessDenied",
    "SearchProviderError",
    "SearchProviderPayloadError",
    "SearchProviderRateLimited",
    "SearchProviderTimeout",
    "SearchProviderTransportError",
    "SearchProviderUnavailable",
    "SearXNGSearchProvider",
    "TavilySearchProvider",
]
