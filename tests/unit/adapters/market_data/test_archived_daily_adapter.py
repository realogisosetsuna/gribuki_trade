from __future__ import annotations

import asyncio
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from gribuki_trade.adapters.archived_daily import (
    ArchivedDailyEvidenceSchemaError,
    ArchivedDailyEvidenceUnavailableError,
    ArchivedDailyEvidenceUnsupportedAdjustmentError,
    ArchivedHistoricalDailyAdapter,
    load_archived_daily_bar_evidence,
)
from gribuki_trade.domain.events import RawDocument
from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.ports.market_data import AsyncHistoricalDailyData, HistoricalDailyData
from gribuki_trade.storage import (
    FileRawDocumentStore,
    archive_daily_bar_evidence,
    daily_bar_evidence_canonical_url,
)

SESSION = date(2026, 8, 13)
TARGET = date(2026, 8, 14)
FIRST_SEEN = datetime(2026, 8, 13, 8, 30, tzinfo=UTC)


def _bar(trade_date: date, *, close: str = "4.18", symbol: str = "510300.SH") -> DailyBar:
    value = Decimal(close)
    return DailyBar(
        symbol=symbol,
        trade_date=trade_date,
        open=value - Decimal("0.03"),
        high=value + Decimal("0.02"),
        low=value - Decimal("0.05"),
        close=value,
        previous_close=value - Decimal("0.01"),
        volume=100_000,
        amount=Decimal("41700000"),
        turnover_percent=Decimal("1.2"),
        is_trading=True,
        is_st=False,
        adjustment=PriceAdjustment.NONE,
    )


def _bars(*, final_close: str = "4.18") -> tuple[DailyBar, ...]:
    return (
        _bar(date(2026, 8, 11), close="4.10"),
        _bar(date(2026, 8, 12), close="4.15"),
        _bar(SESSION, close=final_close),
    )


def _archive(
    root: Path,
    *,
    bars: tuple[DailyBar, ...] | None = None,
    fetched_at: datetime = FIRST_SEEN,
    session: date = SESSION,
    source_id: str = "baostock.daily",
) -> None:
    archive_daily_bar_evidence(
        root,
        symbol="510300.SH",
        latest_completed_session=session,
        target_session=TARGET,
        fetched_at=fetched_at,
        bars=bars or _bars(),
        provider_id=source_id,
        provider_name=source_id,
    )


def _save_payload_revision(
    root: Path,
    payload: dict[str, object],
    *,
    first_seen_at: datetime,
    retain_body: bool = True,
) -> None:
    content = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    document = RawDocument(
        source_id="baostock.daily",
        canonical_url=daily_bar_evidence_canonical_url(
            symbol="510300.SH",
            latest_completed_session=SESSION,
        ),
        content_type="application/json",
        content=content,
        first_seen_at=first_seen_at,
        retrieved_at=first_seen_at,
        available_at=first_seen_at,
        encoding="utf-8",
    )
    FileRawDocumentStore(root).save(
        document,
        retain_body=retain_body,
    )


def _payload(root: Path) -> dict[str, object]:
    _archive(root)
    store = FileRawDocumentStore(root)
    document_id = store.revisions(
        source_id="baostock.daily",
        canonical_url=daily_bar_evidence_canonical_url(
            symbol="510300.SH",
            latest_completed_session=SESSION,
        ),
    )[0]
    payload = json.loads(store.load(document_id).content)
    assert isinstance(payload, dict)
    return payload


def test_loads_latest_exact_revision_visible_at_as_of(tmp_path) -> None:
    _archive(tmp_path, bars=_bars(final_close="4.18"), fetched_at=FIRST_SEEN)
    later = datetime(2026, 8, 13, 9, 0, tzinfo=UTC)
    _archive(tmp_path, bars=_bars(final_close="4.28"), fetched_at=later)

    earlier_view = load_archived_daily_bar_evidence(
        tmp_path,
        symbol="510300.SH",
        start=date(2026, 8, 11),
        latest_completed_session=SESSION,
        as_of=datetime(2026, 8, 13, 8, 45, tzinfo=UTC),
        minimum_bars=3,
    )
    later_view = load_archived_daily_bar_evidence(
        tmp_path,
        symbol="510300.SH",
        start=date(2026, 8, 11),
        latest_completed_session=SESSION,
        as_of=datetime(2026, 8, 13, 9, 5, tzinfo=UTC),
        minimum_bars=3,
    )

    assert earlier_view.bars[-1].close == Decimal("4.18")
    assert earlier_view.first_seen_at == FIRST_SEEN
    assert later_view.bars[-1].close == Decimal("4.28")
    assert later_view.first_seen_at == later


def test_never_relabels_an_older_completed_session(tmp_path) -> None:
    older = date(2026, 8, 12)
    _archive(
        tmp_path,
        bars=(_bar(date(2026, 8, 11)), _bar(older)),
        session=older,
    )

    with pytest.raises(ArchivedDailyEvidenceUnavailableError, match="exact source"):
        load_archived_daily_bar_evidence(
            tmp_path,
            symbol="510300.SH",
            start=date(2026, 8, 11),
            latest_completed_session=SESSION,
            as_of=datetime(2026, 8, 13, 9, 0, tzinfo=UTC),
        )


def test_exact_source_identity_is_required(tmp_path) -> None:
    _archive(tmp_path, source_id="akshare.daily")

    with pytest.raises(ArchivedDailyEvidenceUnavailableError):
        load_archived_daily_bar_evidence(
            tmp_path,
            symbol="510300.SH",
            start=date(2026, 8, 11),
            latest_completed_session=SESSION,
            as_of=datetime(2026, 8, 13, 9, 0, tzinfo=UTC),
            source_id="baostock.daily",
        )
    restored = load_archived_daily_bar_evidence(
        tmp_path,
        symbol="510300.SH",
        start=date(2026, 8, 11),
        latest_completed_session=SESSION,
        as_of=datetime(2026, 8, 13, 9, 0, tzinfo=UTC),
        source_id="akshare.daily",
    )
    assert restored.source_id == "akshare.daily"


def test_validates_schema_version_symbol_adjustment_order_and_final_session(tmp_path) -> None:
    mutations: tuple[tuple[str, object], ...] = (
        ("schema_version", 2),
        ("symbol", "600000.SH"),
        ("latest_completed_session", "2026-08-12"),
    )
    for index, (key, value) in enumerate(mutations, start=1):
        case_root = tmp_path / f"top-{index}"
        payload = _payload(case_root)
        payload[key] = value
        _save_payload_revision(
            case_root,
            payload,
            first_seen_at=datetime(2026, 8, 13, 9, index, tzinfo=UTC),
        )
        with pytest.raises(ArchivedDailyEvidenceSchemaError):
            load_archived_daily_bar_evidence(
                case_root,
                symbol="510300.SH",
                start=date(2026, 8, 11),
                latest_completed_session=SESSION,
                as_of=datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
            )

    bar_mutations = ("symbol", "adjustment", "trade_date")
    for index, key in enumerate(bar_mutations, start=1):
        case_root = tmp_path / f"bar-{index}"
        payload = _payload(case_root)
        raw_bars = payload["bars"]
        assert isinstance(raw_bars, list)
        assert isinstance(raw_bars[-1], dict)
        if key == "symbol":
            raw_bars[-1][key] = "600000.SH"
        elif key == "adjustment":
            raw_bars[-1][key] = "FORWARD"
        else:
            raw_bars[-1][key] = "2026-08-12"
        _save_payload_revision(
            case_root,
            payload,
            first_seen_at=datetime(2026, 8, 13, 9, index, tzinfo=UTC),
        )
        with pytest.raises(ArchivedDailyEvidenceSchemaError):
            load_archived_daily_bar_evidence(
                case_root,
                symbol="510300.SH",
                start=date(2026, 8, 11),
                latest_completed_session=SESSION,
                as_of=datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
            )


def test_rejects_duplicate_dates_and_insufficient_requested_window(tmp_path) -> None:
    duplicate_root = tmp_path / "duplicate"
    duplicate = _payload(duplicate_root)
    raw_bars = duplicate["bars"]
    assert isinstance(raw_bars, list)
    assert isinstance(raw_bars[1], dict)
    raw_bars[1]["trade_date"] = "2026-08-11"
    _save_payload_revision(
        duplicate_root,
        duplicate,
        first_seen_at=datetime(2026, 8, 13, 9, 0, tzinfo=UTC),
    )
    with pytest.raises(ArchivedDailyEvidenceSchemaError, match="strictly ascending"):
        load_archived_daily_bar_evidence(
            duplicate_root,
            symbol="510300.SH",
            start=date(2026, 8, 11),
            latest_completed_session=SESSION,
            as_of=datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
        )

    coverage_root = tmp_path / "coverage"
    _archive(coverage_root)
    with pytest.raises(ArchivedDailyEvidenceUnavailableError, match="required=3"):
        load_archived_daily_bar_evidence(
            coverage_root,
            symbol="510300.SH",
            start=date(2026, 8, 12),
            latest_completed_session=SESSION,
            as_of=datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
            minimum_bars=3,
        )


def test_detects_tampered_content_addressed_body(tmp_path) -> None:
    archived = archive_daily_bar_evidence(
        tmp_path,
        symbol="510300.SH",
        latest_completed_session=SESSION,
        target_session=TARGET,
        fetched_at=FIRST_SEEN,
        bars=_bars(),
    )
    assert archived.stored.body_path is not None
    archived.stored.body_path.write_bytes(b"{}")

    with pytest.raises(ArchivedDailyEvidenceSchemaError, match="integrity"):
        load_archived_daily_bar_evidence(
            tmp_path,
            symbol="510300.SH",
            start=date(2026, 8, 11),
            latest_completed_session=SESSION,
            as_of=datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
        )


def test_metadata_only_revision_is_typed_unavailable(tmp_path) -> None:
    payload = {
        "bars": [],
        "latest_completed_session": SESSION.isoformat(),
        "provider": "BaoStock",
        "schema_version": 1,
        "symbol": "510300.SH",
        "target_session": TARGET.isoformat(),
    }
    _save_payload_revision(
        tmp_path,
        payload,
        first_seen_at=FIRST_SEEN,
        retain_body=False,
    )

    with pytest.raises(ArchivedDailyEvidenceUnavailableError, match="not retained"):
        load_archived_daily_bar_evidence(
            tmp_path,
            symbol="510300.SH",
            start=date(2026, 8, 11),
            latest_completed_session=SESSION,
            as_of=datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
        )


def test_adapter_selects_newest_exact_source_and_implements_sync_async_ports(
    tmp_path,
) -> None:
    _archive(tmp_path, source_id="baostock.daily", fetched_at=FIRST_SEEN)
    newer = datetime(2026, 8, 13, 9, 0, tzinfo=UTC)
    _archive(
        tmp_path,
        source_id="akshare.daily",
        fetched_at=newer,
        bars=_bars(final_close="4.29"),
    )
    adapter = ArchivedHistoricalDailyAdapter(
        tmp_path,
        as_of=datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
        minimum_bars=3,
    )

    assert isinstance(adapter, HistoricalDailyData)
    assert isinstance(adapter, AsyncHistoricalDailyData)
    sync_result = adapter.fetch_daily_bars_with_archive(
        "510300.SH",
        date(2026, 8, 11),
        SESSION,
    )
    async_bars = asyncio.run(
        adapter.fetch_daily_bars_async(
            "510300.SH",
            date(2026, 8, 11),
            SESSION,
        )
    )
    assert sync_result.source_id == "akshare.daily"
    assert sync_result.bars[-1].close == Decimal("4.29")
    assert async_bars == sync_result.bars


def test_adapter_rejects_adjusted_prices(tmp_path) -> None:
    _archive(tmp_path)
    adapter = ArchivedHistoricalDailyAdapter(
        tmp_path,
        as_of=datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(ArchivedDailyEvidenceUnsupportedAdjustmentError):
        adapter.fetch_daily_bars(
            "510300.SH",
            date(2026, 8, 11),
            SESSION,
            adjustment=PriceAdjustment.FORWARD,
        )


def test_invalid_newest_revision_fails_closed_instead_of_using_older_body(tmp_path) -> None:
    payload = _payload(tmp_path)
    payload["schema_version"] = 2
    _save_payload_revision(
        tmp_path,
        payload,
        first_seen_at=datetime(2026, 8, 13, 9, 0, tzinfo=UTC),
    )

    with pytest.raises(ArchivedDailyEvidenceSchemaError, match="schema_version"):
        load_archived_daily_bar_evidence(
            tmp_path,
            symbol="510300.SH",
            start=date(2026, 8, 11),
            latest_completed_session=SESSION,
            as_of=datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
        )
