from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

import pandas as pd
import pytest

from gribuki_trade.domain.events import SourceTier
from gribuki_trade.ingest.akshare_disclosures import (
    AKShareDisclosureConfig,
    AKShareDisclosureError,
    AKShareDisclosureSource,
)

NOW = datetime(2026, 8, 13, 4, 0, tzinfo=UTC)


class FakeClient:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self.calls: list[dict[str, str]] = []

    def stock_zh_a_disclosure_report_cninfo(self, **kwargs: str) -> pd.DataFrame:
        self.calls.append(kwargs)
        return self.frame


def config() -> AKShareDisclosureConfig:
    return AKShareDisclosureConfig(
        symbol="600000.SH",
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 13),
    )


def frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "代码": "600000",
                "简称": "浦发银行",
                "公告标题": "浦发银行关于某事项的公告",
                "公告时间": "2026-08-12 18:30:00",
                "公告链接": "http://www.cninfo.com.cn/new/disclosure/detail?a=1",
            }
        ]
    )


def test_cninfo_table_becomes_official_point_in_time_evidence() -> None:
    client = FakeClient(frame())
    source = AKShareDisclosureSource(config(), client, clock=lambda: NOW)

    batch = asyncio.run(source.collect())

    assert batch.source_id == "akshare.cninfo.600000"
    assert batch.documents[0].content_type == "application/json"
    assert len(batch.events) == 1
    event = batch.events[0]
    assert event.source_tier is SourceTier.OFFICIAL
    assert event.entities == ("600000",)
    assert event.first_seen_at == NOW
    assert event.available_at == NOW
    assert event.raw_document_id == batch.documents[0].document_id
    assert client.calls == [
        {
            "symbol": "600000",
            "market": "沪深京",
            "keyword": "",
            "category": "",
            "start_date": "20260801",
            "end_date": "20260813",
        }
    ]


def test_exact_replay_uses_content_cursor() -> None:
    source = AKShareDisclosureSource(config(), FakeClient(frame()), clock=lambda: NOW)
    first = asyncio.run(source.collect())
    second = asyncio.run(source.collect(first.cursor))

    assert second.not_modified is True
    assert second.documents == ()
    assert second.events == ()


def test_schema_and_symbol_contamination_fail_closed() -> None:
    missing = frame().drop(columns=["公告链接"])
    with pytest.raises(AKShareDisclosureError, match="configured attempts"):
        asyncio.run(
            AKShareDisclosureSource(
                config(),
                FakeClient(missing),
                sleep=lambda _: None,
            ).collect()
        )

    wrong = frame().copy()
    wrong.loc[0, "代码"] = "000001"
    with pytest.raises(AKShareDisclosureError, match="different stock code"):
        asyncio.run(
            AKShareDisclosureSource(config(), FakeClient(wrong)).collect()
        )


def test_config_rejects_invalid_range_and_symbol() -> None:
    with pytest.raises(ValueError, match="symbol"):
        AKShareDisclosureConfig("bad", date(2026, 8, 1), date(2026, 8, 2))
    with pytest.raises(ValueError, match="start_date"):
        AKShareDisclosureConfig("600000", date(2026, 8, 2), date(2026, 8, 1))
