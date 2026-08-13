"""CNINFO company-disclosure leads exposed through AKShare."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
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


class AKShareDisclosureError(RuntimeError):
    """The CNINFO wrapper failed or returned an invalid table."""


class AKShareDisclosureTimeoutError(AKShareDisclosureError):
    """The caller-visible timeout elapsed."""


@dataclass(frozen=True, slots=True)
class AKShareDisclosureConfig:
    symbol: str
    start_date: date
    end_date: date
    category: str = ""
    keyword: str = ""
    timeout_seconds: float = 30.0
    max_attempts: int = 3
    retry_backoff_seconds: float = 0.5
    max_events: int = 500

    def __post_init__(self) -> None:
        code = _stock_code(self.symbol)
        if self.start_date > self.end_date:
            raise ValueError("start_date must not be after end_date")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if self.retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds cannot be negative")
        if self.max_events < 1:
            raise ValueError("max_events must be positive")
        object.__setattr__(self, "symbol", code)
        object.__setattr__(self, "category", self.category.strip())
        object.__setattr__(self, "keyword", self.keyword.strip())

    @property
    def source_id(self) -> str:
        return f"akshare.cninfo.{self.symbol}"


class AKShareDisclosureSource:
    """Collect a bounded CNINFO disclosure index without following attachments."""

    _COLUMNS = frozenset({"代码", "简称", "公告标题", "公告时间", "公告链接"})

    def __init__(
        self,
        config: AKShareDisclosureConfig,
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
            raise AKShareDisclosureTimeoutError(
                "AKShare CNINFO disclosure collection timed out"
            ) from error

    def _collect_sync(self, cursor: FetchCursor) -> NewsBatch:
        observed_at = _aware_utc(self._clock())
        records = self._provider_records()
        content = json.dumps(
            records,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
        digest = hashlib.sha256(content).hexdigest()
        if cursor.content_sha256 == digest:
            return NewsBatch(
                source_id=self._config.source_id,
                cursor=FetchCursor(
                    content_sha256=digest,
                    first_seen_at=cursor.first_seen_at,
                ),
                not_modified=True,
            )

        index_url = (
            "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch"
            "?url=disclosure/list/search"
        )
        document = RawDocument(
            source_id=self._config.source_id,
            canonical_url=index_url,
            content_type="application/json",
            content=content,
            first_seen_at=observed_at,
            retrieved_at=observed_at,
            available_at=observed_at,
            encoding="utf-8",
            content_sha256=digest,
        )
        events = tuple(
            self._normalize_record(record, document)
            for record in records[: self._config.max_events]
        )
        return NewsBatch(
            source_id=self._config.source_id,
            cursor=FetchCursor(content_sha256=digest, first_seen_at=observed_at),
            documents=(document,),
            events=events,
        )

    def _provider_records(self) -> list[Mapping[str, Any]]:
        client = self._client or _import_akshare()
        method = getattr(client, "stock_zh_a_disclosure_report_cninfo", None)
        if not callable(method):
            raise AKShareDisclosureError(
                "AKShare has no callable stock_zh_a_disclosure_report_cninfo"
            )
        last_error: Exception | None = None
        for attempt in range(1, self._config.max_attempts + 1):
            try:
                frame = method(
                    symbol=self._config.symbol,
                    market="沪深京",
                    keyword=self._config.keyword,
                    category=self._config.category,
                    start_date=self._config.start_date.strftime("%Y%m%d"),
                    end_date=self._config.end_date.strftime("%Y%m%d"),
                )
                columns = frozenset(str(column) for column in frame.columns)
                missing = self._COLUMNS - columns
                if missing:
                    raise AKShareDisclosureError(
                        f"CNINFO table is missing columns: {sorted(missing)}"
                    )
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
        raise AKShareDisclosureError(
            "AKShare CNINFO query failed after configured attempts"
        ) from last_error

    def _normalize_record(
        self,
        row: Mapping[str, Any],
        document: RawDocument,
    ) -> NormalizedEvent:
        code = _required_text(row, "代码")
        if code != self._config.symbol:
            raise AKShareDisclosureError("CNINFO row returned a different stock code")
        title = normalise_text(_required_text(row, "公告标题"))
        name = normalise_text(_required_text(row, "简称"))
        published = parse_published_datetime(
            _required_text(row, "公告时间"),
            default_timezone=SHANGHAI,
        )
        if published is not None and published > document.first_seen_at:
            published = None
        try:
            url = canonicalize_url(_required_text(row, "公告链接"))
        except ValueError as error:
            raise AKShareDisclosureError("CNINFO row contains an invalid URL") from error
        return NormalizedEvent(
            source_id=self._config.source_id,
            canonical_url=url,
            title=title,
            summary=f"{name}（{code}）信息披露公告",
            event_type="company_disclosure",
            source_tier=SourceTier.OFFICIAL,
            first_seen_at=document.first_seen_at,
            retrieved_at=document.retrieved_at,
            available_at=document.first_seen_at,
            published_at=published,
            external_id=url,
            raw_document_id=document.document_id,
            entities=(code,),
        )


def _stock_code(value: str) -> str:
    code = value.strip().upper().split(".", maxsplit=1)[0]
    if len(code) != 6 or not code.isdigit():
        raise ValueError("symbol must look like 600000.SH or 000001.SZ")
    return code


def _required_text(row: Mapping[str, Any], name: str) -> str:
    value = row.get(name)
    text = "" if value is None else str(value).strip()
    if not text or text.casefold() in {"nan", "nat", "none"}:
        raise AKShareDisclosureError(f"CNINFO row is missing {name!r}")
    return text


def _string_key_mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise TypeError("CNINFO row must be a string-keyed mapping")
    return value


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _import_akshare() -> Any:
    try:
        return import_module("akshare")
    except ImportError as error:  # pragma: no cover - broken installation only
        raise AKShareDisclosureError("akshare is not installed") from error
