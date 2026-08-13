from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pandas as pd
import pytest

from gribuki_trade.domain.events import SourceTier
from gribuki_trade.ingest.akshare_news import (
    AKShareNewsConfig,
    AKShareNewsError,
    AKShareNewsFeed,
    AKShareNewsSource,
)

NOW = datetime(2026, 8, 13, 3, 0, tzinfo=UTC)


class FakeAKShare:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def stock_news_em(self, *, symbol: str) -> pd.DataFrame:
        self.calls.append(("stock_news_em", symbol))
        return pd.DataFrame(
            [
                {
                    "关键词": symbol,
                    "新闻标题": "无关公司回购600000股",
                    "新闻内容": "另一家公司回购600,000股，不是证券代码。",
                    "发布时间": "2026-08-13 10:50:00",
                    "文章来源": "测试媒体",
                    "新闻链接": "https://finance.eastmoney.com/a/noise.html",
                },
                {
                    "关键词": symbol,
                    "新闻标题": "<em>浦发银行</em>发布公告",
                    "新闻内容": (
                        "<p>600000.SH 简短事实摘要</p><script>ignore()</script>"
                    ),
                    "发布时间": "2026-08-13 10:55:00",
                    "文章来源": "测试媒体",
                    "新闻链接": "https://finance.eastmoney.com/a/1.html?utm_source=x",
                }
            ]
        )

    def stock_info_global_em(self) -> pd.DataFrame:
        self.calls.append(("stock_info_global_em", None))
        return pd.DataFrame(
            [
                {
                    "标题": "宏观快讯",
                    "摘要": "事件摘要",
                    "发布时间": "2026-08-13 10:30:00",
                    "链接": "https://finance.eastmoney.com/a/2.html",
                }
            ]
        )


def test_individual_news_becomes_traceable_lead_and_not_modified() -> None:
    client = FakeAKShare()
    source = AKShareNewsSource(
        AKShareNewsConfig(
            AKShareNewsFeed.INDIVIDUAL_EASTMONEY,
            symbol="600000.SH",
        ),
        client,
        clock=lambda: NOW,
    )

    first = asyncio.run(source.collect())
    second = asyncio.run(source.collect(first.cursor))

    assert first.source_id == "akshare.individual_eastmoney.600000"
    assert len(first.documents) == 1
    assert len(first.events) == 1
    event = first.events[0]
    assert event.title == "浦发银行 发布公告"
    assert event.summary == "600000.SH 简短事实摘要"
    assert event.published_at == datetime(2026, 8, 13, 2, 55, tzinfo=UTC)
    assert event.first_seen_at == NOW
    assert event.raw_document_id == first.documents[0].document_id
    assert event.entities == ("600000",)
    assert "utm_source" not in event.canonical_url
    assert event.source_tier is SourceTier.PUBLIC_MEDIA
    assert second.not_modified is True
    assert not second.events and not second.documents
    assert client.calls == [
        ("stock_news_em", "600000"),
        ("stock_news_em", "600000"),
    ]


def test_global_feed_preserves_publication_and_observation_time() -> None:
    client = FakeAKShare()
    source = AKShareNewsSource(
        AKShareNewsConfig(AKShareNewsFeed.GLOBAL_EASTMONEY),
        client,
        clock=lambda: NOW,
    )

    event = asyncio.run(source.collect()).events[0]

    assert event.event_type == "market_news"
    assert event.published_at == datetime(2026, 8, 13, 2, 30, tzinfo=UTC)
    assert event.available_at == NOW


def test_provider_failure_is_retried_and_sanitized() -> None:
    class Broken:
        def stock_info_global_em(self) -> pd.DataFrame:
            raise RuntimeError("response leaked secret-token")

    source = AKShareNewsSource(
        AKShareNewsConfig(
            AKShareNewsFeed.GLOBAL_EASTMONEY,
            max_attempts=2,
            retry_backoff_seconds=0,
        ),
        Broken(),
        clock=lambda: NOW,
        sleep=lambda _: None,
    )

    with pytest.raises(AKShareNewsError) as caught:
        asyncio.run(source.collect())
    assert "secret-token" not in str(caught.value)


def test_config_rejects_ambiguous_symbols_and_wrong_scope() -> None:
    with pytest.raises(ValueError):
        AKShareNewsConfig(AKShareNewsFeed.INDIVIDUAL_EASTMONEY, symbol="bad")
    with pytest.raises(ValueError):
        AKShareNewsConfig(AKShareNewsFeed.GLOBAL_SINA, symbol="600000")
