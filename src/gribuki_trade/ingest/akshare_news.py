"""通过 AKShare 获取、供研究使用的新闻线索。

AKShare 封装了若干公开财经新闻页面，但不提供稳定的新闻服务等级承诺。
因此本适配器将每条记录都视为线索：记录时点观测与来源，校验数据结构，
绝不把提供方文本当作指令，也不跟随文章链接。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from importlib import import_module
from typing import Any
from zoneinfo import ZoneInfo

from gribuki_trade.domain.events import NormalizedEvent, RawDocument, SourceTier
from gribuki_trade.pipeline.normalize import (
    canonicalize_url,
    normalise_text,
    parse_published_datetime,
)
from gribuki_trade.ports.news import FetchCursor, NewsBatch

SHANGHAI = ZoneInfo("Asia/Shanghai")


class AKShareNewsError(RuntimeError):
    """提供方调用或其载荷未通过校验。"""


class AKShareNewsTimeoutError(AKShareNewsError):
    """调用方可见的超时时间已耗尽。"""


class AKShareNewsFeed(StrEnum):
    INDIVIDUAL_EASTMONEY = "individual_eastmoney"
    GLOBAL_EASTMONEY = "global_eastmoney"
    GLOBAL_CAILIANPRESS = "global_cailianpress"
    GLOBAL_SINA = "global_sina"
    GLOBAL_10JQKA = "global_10jqka"


@dataclass(frozen=True, slots=True)
class AKShareNewsConfig:
    feed: AKShareNewsFeed
    symbol: str | None = None
    entity_aliases: tuple[str, ...] = ()
    source_tier: SourceTier = SourceTier.PUBLIC_MEDIA
    timeout_seconds: float = 20.0
    max_attempts: int = 3
    retry_backoff_seconds: float = 0.5
    max_events: int = 200
    max_summary_characters: int = 1_000

    def __post_init__(self) -> None:
        if self.feed is AKShareNewsFeed.INDIVIDUAL_EASTMONEY:
            _stock_code(self.symbol or "")
        elif self.symbol is not None:
            raise ValueError("symbol is supported only by the individual news feed")
        aliases = tuple(dict.fromkeys(alias.strip() for alias in self.entity_aliases))
        if any(not alias for alias in aliases):
            raise ValueError("entity aliases must not be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if self.retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds cannot be negative")
        if self.max_events < 1 or self.max_summary_characters < 1:
            raise ValueError("event and summary limits must be positive")
        object.__setattr__(self, "entity_aliases", aliases)

    @property
    def source_id(self) -> str:
        suffix = (
            f".{_stock_code(self.symbol or '')}"
            if self.feed is AKShareNewsFeed.INDIVIDUAL_EASTMONEY
            else ""
        )
        return f"akshare.{self.feed.value}{suffix}"


class AKShareNewsSource:
    """采集一张 AKShare 新闻表，且不阻塞 asyncio 调度器。"""

    def __init__(
        self,
        config: AKShareNewsConfig,
        client: Any | None = None,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._config = config
        self._client = client
        self._clock = clock
        self._sleep = sleep

    async def collect(self, cursor: FetchCursor | None = None) -> NewsBatch:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._collect_sync, cursor or FetchCursor()),
                timeout=self._config.timeout_seconds,
            )
        except TimeoutError as error:
            raise AKShareNewsTimeoutError(
                f"AKShare news collection exceeded {self._config.timeout_seconds:g}s"
            ) from error

    def _collect_sync(self, cursor: FetchCursor) -> NewsBatch:
        observed_at = _aware_utc(self._clock())
        records = self._provider_records()
        canonical_records = json.dumps(
            records,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
        digest = hashlib.sha256(canonical_records).hexdigest()
        if cursor.content_sha256 == digest:
            return NewsBatch(
                source_id=self._config.source_id,
                cursor=FetchCursor(
                    content_sha256=digest,
                    first_seen_at=cursor.first_seen_at,
                ),
                not_modified=True,
            )

        endpoint = _feed_endpoint(self._config)
        document = RawDocument(
            source_id=self._config.source_id,
            canonical_url=endpoint,
            content_type="application/json",
            content=canonical_records,
            first_seen_at=observed_at,
            retrieved_at=observed_at,
            available_at=observed_at,
            encoding="utf-8",
            content_sha256=digest,
        )
        events = tuple(
            event
            for record in records[: self._config.max_events]
            if (event := self._normalise_record(record, document)) is not None
        )
        return NewsBatch(
            source_id=self._config.source_id,
            cursor=FetchCursor(content_sha256=digest, first_seen_at=observed_at),
            documents=(document,),
            events=events,
        )

    def _provider_records(self) -> list[Mapping[str, Any]]:
        client = self._client or _import_akshare()
        operation, kwargs = _provider_call(self._config)
        method = getattr(client, operation, None)
        if not callable(method):
            raise AKShareNewsError(f"AKShare has no callable {operation}")
        last_error: Exception | None = None
        for attempt in range(1, self._config.max_attempts + 1):
            try:
                frame = method(**kwargs)
                records = frame.to_dict(orient="records")
                if not isinstance(records, list):
                    raise TypeError("to_dict did not return a list")
                return [_string_key_mapping(item) for item in records]
            except Exception as error:
                last_error = error
                if attempt < self._config.max_attempts:
                    self._sleep(
                        self._config.retry_backoff_seconds * (2 ** (attempt - 1))
                    )
        raise AKShareNewsError(
            f"AKShare {operation} failed after {self._config.max_attempts} attempts"
        ) from last_error

    def _normalise_record(
        self,
        row: Mapping[str, Any],
        document: RawDocument,
    ) -> NormalizedEvent | None:
        fields = _extract_fields(self._config.feed, row)
        title = normalise_text(fields.title)
        summary = normalise_text(
            fields.summary,
            limit=self._config.max_summary_characters,
        )
        if not title:
            title = summary[:120]
        if not title:
            return None
        if (
            self._config.feed is AKShareNewsFeed.INDIVIDUAL_EASTMONEY
            and not _matches_individual_entity(
                _stock_code(self._config.symbol or ""),
                self._config.entity_aliases,
                title,
                summary,
            )
        ):
            return None
        published = parse_published_datetime(
            fields.published_at,
            default_timezone=SHANGHAI,
        )
        # 发布方给出的未来时间戳不是当前可用证据；保留该记录，但将首次
        # 观测时间作为保守的可用时间。
        if published is not None and published > document.first_seen_at:
            published = None
        try:
            url = canonicalize_url(fields.url or document.canonical_url)
        except ValueError:
            url = document.canonical_url
        identity = "\0".join(
            (
                self._config.source_id,
                url,
                title,
                published.isoformat() if published is not None else "",
            )
        )
        external_id = hashlib.sha256(identity.encode()).hexdigest()
        return NormalizedEvent(
            source_id=self._config.source_id,
            canonical_url=url,
            title=title,
            summary=summary,
            event_type=(
                "company_news"
                if self._config.feed is AKShareNewsFeed.INDIVIDUAL_EASTMONEY
                else "market_news"
            ),
            source_tier=self._config.source_tier,
            first_seen_at=document.first_seen_at,
            retrieved_at=document.retrieved_at,
            available_at=document.first_seen_at,
            published_at=published,
            external_id=external_id,
            raw_document_id=document.document_id,
            entities=(
                (_stock_code(self._config.symbol or ""),)
                if self._config.feed is AKShareNewsFeed.INDIVIDUAL_EASTMONEY
                else ()
            ),
        )


@dataclass(frozen=True, slots=True)
class _NewsFields:
    title: str
    summary: str
    published_at: str | None
    url: str | None


def _extract_fields(feed: AKShareNewsFeed, row: Mapping[str, Any]) -> _NewsFields:
    if feed is AKShareNewsFeed.INDIVIDUAL_EASTMONEY:
        return _NewsFields(
            _required_text(row, "新闻标题"),
            _optional_text(row.get("新闻内容")),
            _optional_text(row.get("发布时间")),
            _optional_text(row.get("新闻链接")),
        )
    if feed is AKShareNewsFeed.GLOBAL_EASTMONEY:
        return _NewsFields(
            _required_text(row, "标题"),
            _optional_text(row.get("摘要")),
            _optional_text(row.get("发布时间")),
            _optional_text(row.get("链接")),
        )
    if feed is AKShareNewsFeed.GLOBAL_10JQKA:
        return _NewsFields(
            _required_text(row, "标题"),
            _optional_text(row.get("内容")),
            _optional_text(row.get("发布时间")),
            _optional_text(row.get("链接")),
        )
    if feed is AKShareNewsFeed.GLOBAL_CAILIANPRESS:
        published = " ".join(
            item
            for item in (
                _optional_text(row.get("发布日期")),
                _optional_text(row.get("发布时间")),
            )
            if item
        )
        return _NewsFields(
            _required_text(row, "标题"),
            _optional_text(row.get("内容")),
            published or None,
            None,
        )
    if feed is AKShareNewsFeed.GLOBAL_SINA:
        content = _required_text(row, "内容")
        return _NewsFields(
            content[:120],
            content,
            _optional_text(row.get("时间")),
            None,
        )
    raise AssertionError(f"unsupported feed: {feed}")


def _provider_call(config: AKShareNewsConfig) -> tuple[str, dict[str, str]]:
    if config.feed is AKShareNewsFeed.INDIVIDUAL_EASTMONEY:
        return (
            "stock_news_em",
            {"symbol": _stock_code(config.symbol or "")},
        )
    mapping: dict[AKShareNewsFeed, tuple[str, dict[str, str]]] = {
        AKShareNewsFeed.GLOBAL_EASTMONEY: ("stock_info_global_em", {}),
        AKShareNewsFeed.GLOBAL_CAILIANPRESS: (
            "stock_info_global_cls",
            {"symbol": "全部"},
        ),
        AKShareNewsFeed.GLOBAL_SINA: ("stock_info_global_sina", {}),
        AKShareNewsFeed.GLOBAL_10JQKA: ("stock_info_global_ths", {}),
    }
    try:
        return mapping[config.feed]
    except KeyError as error:  # pragma: no cover - enum exhaustiveness guard
        raise AssertionError(f"unsupported feed: {config.feed}") from error


def _feed_endpoint(config: AKShareNewsConfig) -> str:
    if config.feed is AKShareNewsFeed.INDIVIDUAL_EASTMONEY:
        return (
            "https://so.eastmoney.com/news/s?keyword="
            f"{_stock_code(config.symbol or '')}"
        )
    endpoints = {
        AKShareNewsFeed.GLOBAL_EASTMONEY: "https://kuaixun.eastmoney.com/7_24.html",
        AKShareNewsFeed.GLOBAL_CAILIANPRESS: "https://www.cls.cn/telegraph",
        AKShareNewsFeed.GLOBAL_SINA: "https://finance.sina.com.cn/7x24",
        AKShareNewsFeed.GLOBAL_10JQKA: "https://news.10jqka.com.cn/realtimenews.html",
    }
    try:
        return endpoints[config.feed]
    except KeyError as error:  # pragma: no cover - enum exhaustiveness guard
        raise AssertionError(f"unsupported feed: {config.feed}") from error


def _stock_code(value: str) -> str:
    code = value.strip().upper().split(".", maxsplit=1)[0]
    if len(code) != 6 or not code.isdigit():
        raise ValueError("symbol must look like 600000.SH or 000001.SZ")
    return code


def _matches_individual_entity(
    code: str,
    aliases: tuple[str, ...],
    title: str,
    summary: str,
) -> bool:
    text = f"{title}\n{summary}"
    code_pattern = re.compile(
        rf"(?<![0-9]){re.escape(code)}\.(?:SH|SZ)(?![0-9A-Z])",
        flags=re.IGNORECASE,
    )
    return bool(code_pattern.search(text)) or any(alias in text for alias in aliases)


def _required_text(row: Mapping[str, Any], name: str) -> str:
    value = _optional_text(row.get(name))
    if not value:
        raise AKShareNewsError(f"AKShare news row is missing {name!r}")
    return value


def _optional_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.casefold() in {"nan", "nat", "none"} else text


def _string_key_mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise TypeError("AKShare news row must be a string-keyed mapping")
    return value


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _import_akshare() -> Any:
    try:
        return import_module("akshare")
    except ImportError as error:  # pragma: no cover - broken installation only
        raise AKShareNewsError("akshare is not installed") from error
