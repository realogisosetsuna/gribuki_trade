"""Immutable evidence snapshots for deterministic A-share daily analysis."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from gribuki_trade.domain.events import RawDocument
from gribuki_trade.domain.market import DailyBar
from gribuki_trade.domain.recommendations import EvidenceReference
from gribuki_trade.storage.raw_store import FileRawDocumentStore, StoredRawDocument

SHANGHAI = ZoneInfo("Asia/Shanghai")
_ARCHIVE_COMPONENT = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,63}$")
_SOURCE_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


@dataclass(frozen=True, slots=True)
class ArchivedDailyBarEvidence:
    reference: EvidenceReference
    stored: StoredRawDocument


def daily_bar_evidence_canonical_url(
    *,
    symbol: str,
    latest_completed_session: date,
    provider_id: str = "baostock.daily",
) -> str:
    """Return the exact URL identity shared by archival writes and replay reads."""

    canonical_symbol = symbol.strip().upper()
    canonical_provider_id = provider_id.strip().lower()
    if not _ARCHIVE_COMPONENT.fullmatch(canonical_symbol):
        raise ValueError("symbol is not a safe archive component")
    if not _SOURCE_ID.fullmatch(canonical_provider_id):
        raise ValueError("provider_id must be a safe lowercase source identifier")
    provider_path = (
        "baostock" if canonical_provider_id == "baostock.daily" else canonical_provider_id
    )
    return (
        f"local://{provider_path}/daily/{canonical_symbol}/"
        f"{latest_completed_session.isoformat()}"
    )


def archive_daily_bar_evidence(
    root: Path,
    *,
    symbol: str,
    latest_completed_session: date,
    target_session: date,
    fetched_at: datetime,
    bars: tuple[DailyBar, ...],
    provider_id: str = "baostock.daily",
    provider_name: str = "BaoStock",
) -> ArchivedDailyBarEvidence:
    """Archive the exact unadjusted bars used by a close decision."""

    if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
        raise ValueError("fetched_at must be timezone-aware")
    if not bars:
        raise ValueError("at least one daily bar is required")
    if not provider_id.strip() or not provider_name.strip():
        raise ValueError("provider_id and provider_name must not be blank")
    canonical_symbol = symbol.strip().upper()
    canonical_provider_id = provider_id.strip().lower()
    canonical_url = daily_bar_evidence_canonical_url(
        symbol=canonical_symbol,
        latest_completed_session=latest_completed_session,
        provider_id=canonical_provider_id,
    )
    if any(bar.symbol.strip().upper() != canonical_symbol for bar in bars):
        raise ValueError("all daily bars must match symbol")
    payload = {
        "bars": [_bar_document(bar) for bar in bars],
        "latest_completed_session": latest_completed_session.isoformat(),
        "provider": provider_name,
        "schema_version": 1,
        "symbol": canonical_symbol,
        "target_session": target_session.isoformat(),
    }
    content = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    published_at = datetime.combine(
        latest_completed_session,
        time(15, 5),
        tzinfo=SHANGHAI,
    )
    document = RawDocument(
        source_id=canonical_provider_id,
        canonical_url=canonical_url,
        content_type="application/json",
        content=content,
        first_seen_at=fetched_at,
        retrieved_at=fetched_at,
        available_at=fetched_at,
        published_at=published_at,
        encoding="utf-8",
    )
    stored = FileRawDocumentStore(root).save(document)
    return ArchivedDailyBarEvidence(
        reference=EvidenceReference(
            evidence_id=document.document_id,
            title=(
                f"{provider_name} unadjusted daily bars for {canonical_symbol} "
                f"through {latest_completed_session.isoformat()}"
            ),
            canonical_url=f"local://market-evidence/{document.document_id}",
            published_at=published_at,
            first_seen_at=fetched_at,
            source_tier=2,
        ),
        stored=stored,
    )


def _bar_document(bar: DailyBar) -> dict[str, object]:
    return {
        "adjustment": bar.adjustment.value,
        "amount": str(bar.amount),
        "close": _optional_decimal(bar.close),
        "high": _optional_decimal(bar.high),
        "is_st": bar.is_st,
        "is_trading": bar.is_trading,
        "low": _optional_decimal(bar.low),
        "open": _optional_decimal(bar.open),
        "previous_close": _optional_decimal(bar.previous_close),
        "symbol": bar.symbol,
        "trade_date": bar.trade_date.isoformat(),
        "turnover_percent": _optional_decimal(bar.turnover_percent),
        "volume": bar.volume,
    }


def _optional_decimal(value: object | None) -> str | None:
    return None if value is None else str(value)
