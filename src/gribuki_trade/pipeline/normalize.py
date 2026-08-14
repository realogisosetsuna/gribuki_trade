"""供数据源适配器共享的小型确定性规范化工具。"""

from __future__ import annotations

import html
import ipaddress
import posixpath
import re
from datetime import UTC, datetime, tzinfo
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

_TRACKING_PARAMETERS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "spm",
}


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._ignored_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag.lower() in {"script", "style", "template"}:
            self._ignored_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "template"} and self._ignored_depth:
            self._ignored_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._ignored_depth:
            self.parts.append(data)


def normalise_text(value: str, *, limit: int | None = None) -> str:
    """移除标记并合并 Unicode 空白字符，不解释其语义。"""

    parser = _TextExtractor()
    try:
        parser.feed(value)
        text = " ".join(html.unescape(" ".join(parser.parts)).split())
    except Exception:
        text = " ".join(html.unescape(value).split())
    text = re.sub(r"\s+([,.;:!?，。；：！？）】])", r"\1", text)
    if limit is not None and len(text) > limit:
        return text[: max(0, limit - 1)].rstrip() + "…"
    return text


def _public_host(host: str) -> bool:
    if host == "localhost" or host.endswith(".localhost") or host.endswith(".local"):
        return False
    try:
        return ipaddress.ip_address(host).is_global
    except ValueError:
        return True


def canonicalize_url(url: str, *, base_url: str | None = None) -> str:
    """规范化公开证据 URL，并移除常见跟踪字段。"""

    absolute = urljoin(base_url, url) if base_url is not None else url
    parts = urlsplit(absolute.strip())
    scheme = parts.scheme.lower()
    host = (parts.hostname or "").rstrip(".").lower()
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError as error:
        raise ValueError("URL host is invalid") from error
    if scheme not in {"http", "https"} or not host or not _public_host(host):
        raise ValueError("only public HTTP(S) URLs are supported")
    if parts.username is not None or parts.password is not None:
        raise ValueError("URL user information is not allowed")
    if parts.port is not None and not 1 <= parts.port <= 65535:
        raise ValueError("URL port is invalid")
    include_port = parts.port is not None and not (
        (scheme == "http" and parts.port == 80) or (scheme == "https" and parts.port == 443)
    )
    netloc = f"{host}:{parts.port}" if include_port else host
    path = re.sub(r"/{2,}", "/", parts.path or "/")
    if path not in {"", "/"}:
        trailing_slash = path.endswith("/")
        path = posixpath.normpath(path)
        if trailing_slash and not path.endswith("/"):
            path += "/"
    query_items = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith("utm_") and key.lower() not in _TRACKING_PARAMETERS
    ]
    return urlunsplit((scheme, netloc, path, urlencode(sorted(query_items)), ""))


def parse_published_datetime(
    value: str | None,
    *,
    default_timezone: tzinfo = UTC,
) -> datetime | None:
    """解析 RFC 2822、ISO-8601 及常见的发布时间戳。"""

    if value is None or not value.strip():
        return None
    raw = " ".join(value.strip().split())
    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError, OverflowError):
        parsed = None
    if parsed is None:
        iso_value = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
        try:
            parsed = datetime.fromisoformat(iso_value)
        except ValueError:
            parsed = None
    if parsed is None:
        for pattern in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d",
        ):
            try:
                parsed = datetime.strptime(raw, pattern)
                break
            except ValueError:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=default_timezone)
    return parsed.astimezone(UTC)
