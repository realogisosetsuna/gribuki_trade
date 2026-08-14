"""盘后外部端点的安全配置表示。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from urllib.parse import urlsplit


class PostCloseSearxngURLValidationError(ValueError):
    """SearXNG URL 不适合用于盘后配置。"""


@dataclass(frozen=True, slots=True)
class ValidatedPostCloseSearxngURL:
    """运行时 URL 及已脱敏路径的持久配置记录。"""

    runtime_url: str
    manifest_document: dict[str, str]


def validate_post_close_searxng_url(
    value: str | None,
) -> ValidatedPostCloseSearxngURL | None:
    """验证 SearXNG 基础 URL，但不在持久状态中保留其路径。

    所提供的 URL 只在进程内请求路径中保留。持久配置仅接收规范化 origin 与精确路径的
    SHA-256 承诺值，因此私有部署前缀不会通过 manifest 或由其派生的命令输出泄露。
    """

    if value is None:
        return None
    runtime_url = value.strip()
    if not runtime_url:
        raise PostCloseSearxngURLValidationError("SearXNG URL is empty")
    try:
        parts = urlsplit(runtime_url)
        port = parts.port
    except ValueError as error:
        raise PostCloseSearxngURLValidationError("SearXNG URL is invalid") from error

    scheme = parts.scheme.lower()
    host = (parts.hostname or "").rstrip(".").lower()
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError as error:
        raise PostCloseSearxngURLValidationError("SearXNG URL host is invalid") from error
    if scheme not in {"http", "https"} or not host:
        raise PostCloseSearxngURLValidationError("SearXNG URL must use HTTP(S) with a host")
    if parts.username is not None or parts.password is not None:
        raise PostCloseSearxngURLValidationError("SearXNG URL must not include user information")
    if parts.query or "?" in runtime_url or parts.fragment or "#" in runtime_url:
        raise PostCloseSearxngURLValidationError("SearXNG URL must not include query or fragment")
    if port is not None and not 1 <= port <= 65535:
        raise PostCloseSearxngURLValidationError("SearXNG URL port is invalid")
    if parts.netloc.endswith(":"):
        raise PostCloseSearxngURLValidationError("SearXNG URL port is invalid")

    origin_host = f"[{host}]" if ":" in host else host
    include_port = port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    )
    origin = f"{scheme}://{origin_host}{f':{port}' if include_port else ''}"
    return ValidatedPostCloseSearxngURL(
        runtime_url=runtime_url,
        manifest_document={
            "origin": origin,
            "path_sha256": hashlib.sha256(parts.path.encode("utf-8")).hexdigest(),
        },
    )
