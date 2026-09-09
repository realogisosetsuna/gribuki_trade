"""按时点重放不可变的本地 A 股日线证据。

该适配器刻意比缓存更严格：它只打开 ``archive_daily_bar_evidence`` 写入的
精确来源/代码/已完成交易日 URL，绝不会把旧交易日重新标记为当前数据。返回
任何行情柱之前，都会再次验证所选原始文档和第一版载荷。
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from functools import partial
from pathlib import Path
from typing import Any

from gribuki_trade.domain.events import RawDocument
from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.ports.market_data import MarketDataUnavailableError
from gribuki_trade.storage.research.market_evidence import daily_bar_evidence_canonical_url
from gribuki_trade.storage.research.raw_store import BodyNotRetainedError, FileRawDocumentStore

DEFAULT_ARCHIVED_DAILY_SOURCE_IDS = (
    "baostock.daily",
    "akshare.daily",
    "mixed.tail_stitch.daily",
    "other.research.daily",
)

_PAYLOAD_KEYS = frozenset(
    {
        "bars",
        "latest_completed_session",
        "provider",
        "schema_version",
        "symbol",
        "target_session",
    }
)
_BAR_KEYS = frozenset(
    {
        "adjustment",
        "amount",
        "close",
        "high",
        "is_st",
        "is_trading",
        "low",
        "open",
        "previous_close",
        "symbol",
        "trade_date",
        "turnover_percent",
        "volume",
    }
)
_RAW_METADATA_KEYS = frozenset(
    {
        "available_at",
        "body_retained",
        "canonical_url",
        "content_sha256",
        "content_type",
        "document_id",
        "encoding",
        "etag",
        "first_seen_at",
        "last_modified",
        "published_at",
        "retrieved_at",
        "schema_version",
        "source_id",
    }
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ArchivedDailyEvidenceError(MarketDataUnavailableError):
    """保留日线证据重放失败的基类。"""


class ArchivedDailyEvidenceUnavailableError(ArchivedDailyEvidenceError):
    """没有在指定时点可用且满足精确请求的保留正文。"""


class ArchivedDailyEvidenceSchemaError(ArchivedDailyEvidenceError):
    """候选归档未通过身份、完整性或架构验证。"""


class ArchivedDailyEvidenceUnsupportedAdjustmentError(ArchivedDailyEvidenceError):
    """不可变证据归档仅包含原始价格。"""


@dataclass(frozen=True, slots=True)
class ArchivedDailyBarSnapshot:
    """保留不可变来源身份的已验证重放结果。"""

    bars: tuple[DailyBar, ...]
    source_id: str
    canonical_url: str
    document_id: str
    content_sha256: str
    first_seen_at: datetime
    available_at: datetime
    latest_completed_session: date
    target_session: date


def load_archived_daily_bar_evidence(
    root: Path,
    *,
    symbol: str,
    start: date,
    latest_completed_session: date,
    as_of: datetime,
    source_id: str = "baostock.daily",
    minimum_bars: int = 1,
) -> ArchivedDailyBarSnapshot:
    """加载在 ``as_of`` 时点可见的最新精确归档修订版。

    URL 内嵌 ``latest_completed_session``。因此，截止日期更早的归档甚至不会
    成为候选，也不能被当作所请求已完成交易日的数据。
    """

    if start > latest_completed_session:
        raise ValueError("start must be on or before latest_completed_session")
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    if minimum_bars < 1:
        raise ValueError("minimum_bars must be positive")

    canonical_symbol = symbol.strip().upper()
    canonical_source_id = source_id.strip().lower()
    canonical_url = daily_bar_evidence_canonical_url(
        symbol=canonical_symbol,
        latest_completed_session=latest_completed_session,
        provider_id=canonical_source_id,
    )
    cutoff = as_of.astimezone(UTC)
    resolved_root = root.resolve()
    store = FileRawDocumentStore(resolved_root)
    try:
        revision_ids = store.revisions(
            source_id=canonical_source_id,
            canonical_url=canonical_url,
        )
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive revision metadata is corrupt"
        ) from exc
    if not revision_ids:
        raise ArchivedDailyEvidenceUnavailableError(
            "no archive exists for the exact source, symbol, and completed session"
        )

    candidates: list[tuple[datetime, str, dict[str, Any]]] = []
    for document_id in revision_ids:
        try:
            metadata = _read_revision_metadata(resolved_root, document_id)
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ArchivedDailyEvidenceSchemaError(
                "daily archive revision metadata validation failed"
            ) from exc
        if (
            metadata["source_id"] != canonical_source_id
            or metadata["canonical_url"] != canonical_url
        ):
            raise ArchivedDailyEvidenceSchemaError(
                "daily archive revision metadata identity mismatch"
            )
        first_seen_at = _metadata_datetime(metadata["first_seen_at"], "first_seen_at")
        available_at = _metadata_datetime(metadata["available_at"], "available_at")
        if first_seen_at <= cutoff and available_at <= cutoff:
            candidates.append((first_seen_at, document_id, metadata))

    if not candidates:
        raise ArchivedDailyEvidenceUnavailableError(
            "all exact archive revisions were first seen or available after as_of"
        )

    _, selected_id, selected_metadata = max(
        candidates,
        key=lambda item: (item[0], item[1]),
    )
    if selected_metadata["body_retained"] is not True:
        raise ArchivedDailyEvidenceUnavailableError(
            "latest point-in-time eligible archive body was not retained"
        )
    try:
        selected = store.load(selected_id)
    except BodyNotRetainedError as exc:
        raise ArchivedDailyEvidenceUnavailableError(
            "latest point-in-time eligible archive body was not retained"
        ) from exc
    except FileNotFoundError as exc:
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive metadata refers to a missing retained body"
        ) from exc
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive document integrity validation failed"
        ) from exc
    if (
        selected.document_id != selected_metadata["document_id"]
        or selected.content_sha256 != selected_metadata["content_sha256"]
        or selected.first_seen_at
        != _metadata_datetime(selected_metadata["first_seen_at"], "first_seen_at")
        or selected.available_at
        != _metadata_datetime(selected_metadata["available_at"], "available_at")
    ):
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive document does not match selected revision metadata"
        )
    return _decode_snapshot(
        selected,
        expected_source_id=canonical_source_id,
        expected_url=canonical_url,
        expected_symbol=canonical_symbol,
        start=start,
        latest_completed_session=latest_completed_session,
        minimum_bars=minimum_bars,
    )


class ArchivedHistoricalDailyAdapter:
    """基于已审计快照的只读同步/异步历史数据适配器。

    ``source_ids`` 是有序允许列表，而非模糊查找。每个来源都通过精确的规范
    URL 查询；若多个来源都有合格证据，则最新的 ``first_seen_at`` 胜出，并以
    允许列表顺序作为确定性的最终平局裁决。候选损坏时关闭失败，而不会静默
    回退到另一份正文。
    """

    def __init__(
        self,
        root: Path,
        *,
        as_of: datetime,
        source_ids: Iterable[str] = DEFAULT_ARCHIVED_DAILY_SOURCE_IDS,
        minimum_bars: int = 1,
    ) -> None:
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        if minimum_bars < 1:
            raise ValueError("minimum_bars must be positive")
        canonical_sources = tuple(
            dict.fromkeys(source_id.strip().lower() for source_id in source_ids)
        )
        if not canonical_sources or any(not item for item in canonical_sources):
            raise ValueError("source_ids must contain at least one non-blank source")
        # 在保留构造函数状态前验证来源标识符。
        for source_id in canonical_sources:
            daily_bar_evidence_canonical_url(
                symbol="VALIDATION",
                latest_completed_session=date(2000, 1, 1),
                provider_id=source_id,
            )
        self._root = root.resolve()
        self._as_of = as_of.astimezone(UTC)
        self._source_ids = canonical_sources
        self._minimum_bars = minimum_bars

    def fetch_daily_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> tuple[DailyBar, ...]:
        return self.fetch_daily_bars_with_archive(
            symbol,
            start,
            end,
            adjustment=adjustment,
        ).bars

    def fetch_daily_bars_with_archive(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> ArchivedDailyBarSnapshot:
        if adjustment is not PriceAdjustment.NONE:
            raise ArchivedDailyEvidenceUnsupportedAdjustmentError(
                "archived daily evidence supports only NONE adjustment"
            )
        if start > end:
            raise ValueError("start must be on or before end")

        matches: list[tuple[datetime, int, str, ArchivedDailyBarSnapshot]] = []
        failures: list[str] = []
        for priority, source_id in enumerate(self._source_ids):
            try:
                snapshot = load_archived_daily_bar_evidence(
                    self._root,
                    symbol=symbol,
                    start=start,
                    latest_completed_session=end,
                    as_of=self._as_of,
                    source_id=source_id,
                    minimum_bars=self._minimum_bars,
                )
            except ArchivedDailyEvidenceUnavailableError as exc:
                failures.append(f"{source_id}={exc}")
                continue
            matches.append(
                (snapshot.first_seen_at, -priority, snapshot.document_id, snapshot)
            )
        if not matches:
            details = "; ".join(failures)
            raise ArchivedDailyEvidenceUnavailableError(
                f"no eligible archived daily evidence: {details}"
            )
        return max(matches, key=lambda item: (item[0], item[1], item[2]))[3]

    async def fetch_daily_bars_async(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> tuple[DailyBar, ...]:
        snapshot = await self.fetch_daily_bars_async_with_archive(
            symbol,
            start,
            end,
            adjustment=adjustment,
        )
        return snapshot.bars

    async def fetch_daily_bars_async_with_archive(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> ArchivedDailyBarSnapshot:
        return await asyncio.to_thread(
            partial(
                self.fetch_daily_bars_with_archive,
                symbol,
                start,
                end,
                adjustment=adjustment,
            )
        )


def _decode_snapshot(
    document: RawDocument,
    *,
    expected_source_id: str,
    expected_url: str,
    expected_symbol: str,
    start: date,
    latest_completed_session: date,
    minimum_bars: int,
) -> ArchivedDailyBarSnapshot:
    if document.source_id != expected_source_id or document.canonical_url != expected_url:
        raise ArchivedDailyEvidenceSchemaError("daily archive document identity mismatch")
    if document.content_type != "application/json":
        raise ArchivedDailyEvidenceSchemaError("daily archive content type is not JSON")
    if document.encoding not in {None, "utf-8", "UTF-8"}:
        raise ArchivedDailyEvidenceSchemaError("daily archive encoding is not UTF-8")

    try:
        payload = json.loads(
            document.content.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive body is not strict UTF-8 JSON"
        ) from exc
    if not isinstance(payload, dict) or frozenset(payload) != _PAYLOAD_KEYS:
        raise ArchivedDailyEvidenceSchemaError("daily archive top-level schema mismatch")
    if type(payload["schema_version"]) is not int or payload["schema_version"] != 1:
        raise ArchivedDailyEvidenceSchemaError("daily archive schema_version must equal 1")
    if payload["symbol"] != expected_symbol:
        raise ArchivedDailyEvidenceSchemaError("daily archive payload symbol mismatch")
    if not isinstance(payload["provider"], str) or not payload["provider"].strip():
        raise ArchivedDailyEvidenceSchemaError("daily archive provider must be non-blank")

    payload_latest = _strict_date(
        payload["latest_completed_session"],
        field="latest_completed_session",
    )
    if payload_latest != latest_completed_session:
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive latest_completed_session does not match the request"
        )
    target_session = _strict_date(payload["target_session"], field="target_session")
    if target_session <= payload_latest:
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive target_session must follow latest_completed_session"
        )

    raw_bars = payload["bars"]
    if not isinstance(raw_bars, list) or not raw_bars:
        raise ArchivedDailyEvidenceSchemaError("daily archive bars must be a non-empty list")
    bars = tuple(_decode_bar(item, expected_symbol=expected_symbol) for item in raw_bars)
    dates = tuple(bar.trade_date for bar in bars)
    if any(current <= previous for previous, current in zip(dates, dates[1:], strict=False)):
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive trade dates must be unique and strictly ascending"
        )
    if bars[-1].trade_date != latest_completed_session:
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive final bar is not the required completed session"
        )
    if any(bar.trade_date > latest_completed_session for bar in bars):
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive contains a bar after latest_completed_session"
        )

    selected_bars = tuple(bar for bar in bars if start <= bar.trade_date)
    if not selected_bars or selected_bars[0].trade_date < start:
        raise ArchivedDailyEvidenceSchemaError("daily archive window validation failed")
    if len(selected_bars) < minimum_bars:
        raise ArchivedDailyEvidenceUnavailableError(
            "archived daily coverage below requirement: "
            f"required={minimum_bars}, available={len(selected_bars)}"
        )
    return ArchivedDailyBarSnapshot(
        bars=selected_bars,
        source_id=document.source_id,
        canonical_url=document.canonical_url,
        document_id=document.document_id,
        content_sha256=document.content_sha256,
        first_seen_at=document.first_seen_at,
        available_at=document.available_at,
        latest_completed_session=payload_latest,
        target_session=target_session,
    )


def _decode_bar(value: object, *, expected_symbol: str) -> DailyBar:
    if not isinstance(value, dict) or frozenset(value) != _BAR_KEYS:
        raise ArchivedDailyEvidenceSchemaError("daily archive bar schema mismatch")
    if value["symbol"] != expected_symbol:
        raise ArchivedDailyEvidenceSchemaError("daily archive bar symbol mismatch")
    if value["adjustment"] != PriceAdjustment.NONE.value:
        raise ArchivedDailyEvidenceSchemaError("daily archive bar adjustment must be NONE")
    if type(value["volume"]) is not int or value["volume"] < 0:
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive bar volume must be a non-negative integer"
        )
    if type(value["is_trading"]) is not bool or type(value["is_st"]) is not bool:
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive bar flags must be booleans"
        )

    amount = _strict_decimal(value["amount"], field="amount", optional=False)
    assert amount is not None
    if amount < 0:
        raise ArchivedDailyEvidenceSchemaError("daily archive bar amount cannot be negative")
    turnover = _strict_decimal(
        value["turnover_percent"],
        field="turnover_percent",
        optional=True,
    )
    if turnover is not None and turnover < 0:
        raise ArchivedDailyEvidenceSchemaError(
            "daily archive turnover_percent cannot be negative"
        )
    return DailyBar(
        symbol=expected_symbol,
        trade_date=_strict_date(value["trade_date"], field="trade_date"),
        open=_strict_decimal(value["open"], field="open", optional=True),
        high=_strict_decimal(value["high"], field="high", optional=True),
        low=_strict_decimal(value["low"], field="low", optional=True),
        close=_strict_decimal(value["close"], field="close", optional=True),
        previous_close=_strict_decimal(
            value["previous_close"],
            field="previous_close",
            optional=True,
        ),
        volume=value["volume"],
        amount=amount,
        turnover_percent=turnover,
        is_trading=value["is_trading"],
        is_st=value["is_st"],
        adjustment=PriceAdjustment.NONE,
    )


def _strict_date(value: object, *, field: str) -> date:
    if not isinstance(value, str):
        raise ArchivedDailyEvidenceSchemaError(f"daily archive {field} must be a string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ArchivedDailyEvidenceSchemaError(
            f"daily archive {field} is not an ISO date"
        ) from exc
    if value != parsed.isoformat():
        raise ArchivedDailyEvidenceSchemaError(
            f"daily archive {field} is not a canonical ISO date"
        )
    return parsed


def _strict_decimal(
    value: object,
    *,
    field: str,
    optional: bool,
) -> Decimal | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value:
        raise ArchivedDailyEvidenceSchemaError(
            f"daily archive {field} must be a decimal string"
        )
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ArchivedDailyEvidenceSchemaError(
            f"daily archive {field} is not a valid decimal"
        ) from exc
    if not parsed.is_finite():
        raise ArchivedDailyEvidenceSchemaError(
            f"daily archive {field} must be finite"
        )
    return parsed


def _read_revision_metadata(root: Path, document_id: str) -> dict[str, Any]:
    if not _SHA256.fullmatch(document_id):
        raise ValueError("archive revision id is not a SHA-256 digest")
    path = root / "records" / document_id[:2] / f"{document_id}.json"
    metadata = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=_unique_object,
        parse_constant=_reject_json_constant,
    )
    if not isinstance(metadata, dict) or frozenset(metadata) != _RAW_METADATA_KEYS:
        raise ValueError("raw archive metadata schema mismatch")
    if type(metadata["schema_version"]) is not int or metadata["schema_version"] != 1:
        raise ValueError("raw archive metadata schema_version must equal 1")
    if metadata["document_id"] != document_id:
        raise ValueError("raw archive metadata document_id mismatch")
    if not isinstance(metadata["content_sha256"], str) or not _SHA256.fullmatch(
        metadata["content_sha256"]
    ):
        raise ValueError("raw archive metadata content_sha256 is invalid")
    if type(metadata["body_retained"]) is not bool:
        raise ValueError("raw archive metadata body_retained must be boolean")
    if not isinstance(metadata["source_id"], str) or not isinstance(
        metadata["canonical_url"], str
    ):
        raise ValueError("raw archive metadata identity fields must be strings")
    first_seen = _metadata_datetime(metadata["first_seen_at"], "first_seen_at")
    retrieved = _metadata_datetime(metadata["retrieved_at"], "retrieved_at")
    available = _metadata_datetime(metadata["available_at"], "available_at")
    if retrieved < first_seen or available < first_seen:
        raise ValueError("raw archive metadata observation times are inconsistent")
    published = metadata["published_at"]
    if published is not None:
        published_at = _metadata_datetime(published, "published_at")
        if available < published_at:
            raise ValueError("raw archive metadata predates publisher availability")
    return metadata


def _metadata_datetime(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"raw archive metadata {field} must be a string")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"raw archive metadata {field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise ValueError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant: {value}")
