from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.storage import FileRawDocumentStore, archive_daily_bar_evidence


def _bar(symbol: str = "510300.SH") -> DailyBar:
    return DailyBar(
        symbol=symbol,
        trade_date=date(2026, 8, 13),
        open=Decimal("4.10"),
        high=Decimal("4.20"),
        low=Decimal("4.05"),
        close=Decimal("4.18"),
        previous_close=Decimal("4.08"),
        volume=100_000,
        amount=Decimal("41700000"),
        turnover_percent=Decimal("1.2"),
        is_trading=True,
        is_st=False,
        adjustment=PriceAdjustment.NONE,
    )


def test_archives_exact_daily_input_idempotently(tmp_path) -> None:
    now = datetime(2026, 8, 13, 8, 30, tzinfo=UTC)
    first = archive_daily_bar_evidence(
        tmp_path,
        symbol="510300.SH",
        latest_completed_session=date(2026, 8, 13),
        target_session=date(2026, 8, 14),
        fetched_at=now,
        bars=(_bar(),),
    )
    second = archive_daily_bar_evidence(
        tmp_path,
        symbol="510300.SH",
        latest_completed_session=date(2026, 8, 13),
        target_session=date(2026, 8, 14),
        fetched_at=now,
        bars=(_bar(),),
    )

    assert first.stored.created is True
    assert second.stored.created is False
    assert first.reference.evidence_id == second.reference.evidence_id
    restored = FileRawDocumentStore(tmp_path).load(first.reference.evidence_id)
    assert restored.content == first.stored.body_path.read_bytes()
    assert b'"adjustment":"NONE"' in restored.content


def test_rejects_mixed_symbols(tmp_path) -> None:
    with pytest.raises(ValueError, match="match symbol"):
        archive_daily_bar_evidence(
            tmp_path,
            symbol="510300.SH",
            latest_completed_session=date(2026, 8, 13),
            target_session=date(2026, 8, 14),
            fetched_at=datetime(2026, 8, 13, 8, 30, tzinfo=UTC),
            bars=(_bar("600000.SH"),),
        )
