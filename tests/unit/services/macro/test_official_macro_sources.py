import asyncio
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import gribuki_trade.cli as cli
import gribuki_trade.ingest as ingest
import gribuki_trade.services as services
import gribuki_trade.storage as storage
from gribuki_trade.domain.events import RawDocument, SourcePolicy, SourceTier
from gribuki_trade.ingest.official_macro import (
    CSRC_POLICY_INTERPRETATION_URL,
    CSRC_SOURCE_ID,
    FED_MONETARY_RSS_URL,
    FED_SOURCE_ID,
    MOF_POLICY_RELEASE_URL,
    MOF_SOURCE_ID,
    NBS_DATA_RELEASE_URL,
    NBS_SOURCE_ID,
    NDRC_NORMATIVE_POLICY_URL,
    NDRC_SOURCE_ID,
    PBOC_OPEN_MARKET_URL,
    PBOC_SOURCE_ID,
    SAFE_FOREIGN_EXCHANGE_NEWS_URL,
    SAFE_SOURCE_ID,
    SSE_MARKET_NEWS_URL,
    SSE_SOURCE_ID,
    build_default_official_macro_sources,
)
from gribuki_trade.ports.news import DocumentFetch, FetchCursor

FIXTURES = Path(__file__).parents[3] / "fixtures" / "official_macro"
FIRST_SEEN = datetime(2026, 8, 14, 1, 0, tzinfo=UTC)


class FixtureFetcher:
    def __init__(self, document: RawDocument) -> None:
        self.document = document

    async def fetch(
        self,
        policy: SourcePolicy,
        url: str,
        cursor: FetchCursor | None = None,
    ) -> DocumentFetch:
        del policy, url, cursor
        return DocumentFetch(self.document, FetchCursor())


def _document(source_id: str, url: str, fixture: str, content_type: str) -> RawDocument:
    return RawDocument(
        source_id=source_id,
        canonical_url=url,
        content_type=content_type,
        content=(FIXTURES / fixture).read_bytes(),
        first_seen_at=FIRST_SEEN,
        retrieved_at=datetime(2026, 8, 14, 1, 1, tzinfo=UTC),
        available_at=FIRST_SEEN,
        encoding="utf-8",
    )


def test_default_official_sources_parse_frozen_live_structures_with_pit_metadata() -> None:
    sources = build_default_official_macro_sources()
    fixtures = {
        NBS_SOURCE_ID: _document(
            NBS_SOURCE_ID,
            NBS_DATA_RELEASE_URL,
            "nbs_data_release.html",
            "text/html",
        ),
        PBOC_SOURCE_ID: _document(
            PBOC_SOURCE_ID,
            PBOC_OPEN_MARKET_URL,
            "pboc_open_market.html",
            "text/html",
        ),
        FED_SOURCE_ID: _document(
            FED_SOURCE_ID,
            FED_MONETARY_RSS_URL,
            "fed_monetary.xml",
            "text/xml",
        ),
        CSRC_SOURCE_ID: _document(
            CSRC_SOURCE_ID,
            CSRC_POLICY_INTERPRETATION_URL,
            "csrc_policy_interpretation.html",
            "text/html",
        ),
        MOF_SOURCE_ID: _document(
            MOF_SOURCE_ID,
            MOF_POLICY_RELEASE_URL,
            "mof_policy_release.html",
            "text/html",
        ),
        NDRC_SOURCE_ID: _document(
            NDRC_SOURCE_ID,
            NDRC_NORMATIVE_POLICY_URL,
            "ndrc_normative_policy.html",
            "text/html",
        ),
        SAFE_SOURCE_ID: _document(
            SAFE_SOURCE_ID,
            SAFE_FOREIGN_EXCHANGE_NEWS_URL,
            "safe_foreign_exchange_news.html",
            "text/html",
        ),
        SSE_SOURCE_ID: _document(
            SSE_SOURCE_ID,
            SSE_MARKET_NEWS_URL,
            "sse_market_news.html",
            "text/html",
        ),
    }
    batches = {}
    for source_id, source in sources.items():
        source._fetcher = FixtureFetcher(fixtures[source_id])  # type: ignore[attr-defined]
        batches[source_id] = asyncio.run(source.collect())

    assert tuple(sources) == (
        NBS_SOURCE_ID,
        PBOC_SOURCE_ID,
        FED_SOURCE_ID,
        CSRC_SOURCE_ID,
        MOF_SOURCE_ID,
        NDRC_SOURCE_ID,
        SAFE_SOURCE_ID,
        SSE_SOURCE_ID,
    )
    nbs = batches[NBS_SOURCE_ID].events
    assert len(nbs) == 1
    assert nbs[0].title == "2026年7月份居民消费价格同比上涨0.5%"
    assert nbs[0].canonical_url == (
        "https://www.stats.gov.cn/sj/zxfb/202608/t20260809_1965008.html"
    )
    assert nbs[0].published_at == datetime(2026, 8, 8, 16, 0, tzinfo=UTC)
    assert nbs[0].event_type == "macro"

    pboc = batches[PBOC_SOURCE_ID].events
    assert len(pboc) == 2
    assert pboc[0].title == "公开市场业务交易公告 [2026]第156号"
    assert pboc[0].canonical_url.endswith("/2026081308553983010/index.html")
    assert pboc[0].published_at == datetime(2026, 8, 12, 16, 0, tzinfo=UTC)
    assert pboc[0].event_type == "policy"

    fed = batches[FED_SOURCE_ID].events
    assert len(fed) == 1
    assert fed[0].canonical_url.endswith("/monetary20260729a.htm")
    assert fed[0].published_at == datetime(2026, 7, 29, 18, 0, tzinfo=UTC)
    assert fed[0].event_type == "macro_global"

    csrc = batches[CSRC_SOURCE_ID].events
    assert len(csrc) == 1
    assert csrc[0].title == "中国证监会发布《衍生品交易监督管理办法（试行）》"
    assert csrc[0].canonical_url == (
        "https://www.csrc.gov.cn/csrc/c100028/c7632835/content.shtml"
    )
    assert csrc[0].published_at == datetime(2026, 5, 14, 16, 0, tzinfo=UTC)
    assert csrc[0].event_type == "regulatory"

    mof = batches[MOF_SOURCE_ID].events
    assert len(mof) == 1  # 站外链接必须在归一化阶段被丢弃。
    assert mof[0].canonical_url == (
        "https://kjs.mof.gov.cn/zhengcefabu/202608/t20260805_3994927.htm"
    )
    assert mof[0].published_at == datetime(2026, 8, 4, 16, 0, tzinfo=UTC)
    assert mof[0].event_type == "fiscal_policy"

    ndrc = batches[NDRC_SOURCE_ID].events
    assert len(ndrc) == 1
    assert ndrc[0].canonical_url.endswith("/ghxwj/202605/t20260519_1405299.html")
    assert ndrc[0].event_type == "industrial_policy"

    safe = batches[SAFE_SOURCE_ID].events
    assert len(safe) == 1
    assert safe[0].canonical_url == "https://www.safe.gov.cn/safe/2026/0814/27784.html"
    assert safe[0].event_type == "foreign_exchange_policy"

    sse = batches[SSE_SOURCE_ID].events
    assert len(sse) == 1
    assert sse[0].canonical_url.endswith("/c/c_20260724_10826669.shtml")
    assert sse[0].event_type == "exchange_regulatory"

    for batch in batches.values():
        for event in batch.events:
            assert event.source_tier is SourceTier.OFFICIAL
            assert event.first_seen_at == FIRST_SEEN
            assert event.available_at == FIRST_SEEN
            assert event.raw_document_id is not None

    for source in sources.values():
        policy = source._policy  # type: ignore[attr-defined]
        assert policy.timeout_seconds == 15.0
        assert policy.max_attempts == 1
        assert policy.allowed_schemes == frozenset({"https"})
        if policy.source_id == MOF_SOURCE_ID:
            assert policy.allow_subdomains
            assert policy.allowed_hosts == frozenset({"mof.gov.cn"})
        else:
            assert not policy.allow_subdomains


def test_close_news_collection_always_merges_official_sources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: list[tuple[str, ...]] = []

    class FakeSource:
        def __init__(self, config: object | None = None) -> None:
            self.config = config

    class FakeEventStore:
        def __init__(self, path: Path) -> None:
            self.path = path

        def __enter__(self) -> "FakeEventStore":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def latest(self, *, limit: int) -> tuple[object, ...]:
            assert limit == 2_000
            return ()

    class FakeRawStore:
        def __init__(self, path: Path) -> None:
            self.path = path

    class FakeCollectionService:
        def __init__(
            self,
            sources: dict[str, object],
            *,
            raw_store: object,
            event_store: object,
            max_concurrency: int,
        ) -> None:
            del raw_store, event_store
            assert max_concurrency == 1
            captured.append(tuple(sources))
            self.source_ids = tuple(sources)

        async def run_once(self) -> tuple[SimpleNamespace, ...]:
            return tuple(
                SimpleNamespace(
                    documents_saved=0,
                    error_code=None,
                    events_duplicate=0,
                    events_new=0,
                    events_revised=0,
                    source_id=source_id,
                    status=SimpleNamespace(value="SUCCESS"),
                )
                for source_id in self.source_ids
            )

    official = {
        NBS_SOURCE_ID: FakeSource(),
        PBOC_SOURCE_ID: FakeSource(),
        FED_SOURCE_ID: FakeSource(),
        CSRC_SOURCE_ID: FakeSource(),
        MOF_SOURCE_ID: FakeSource(),
        NDRC_SOURCE_ID: FakeSource(),
        SAFE_SOURCE_ID: FakeSource(),
        SSE_SOURCE_ID: FakeSource(),
    }
    monkeypatch.setattr(ingest, "AKShareNewsSource", FakeSource)
    monkeypatch.setattr(ingest, "build_default_official_macro_sources", lambda: official)
    monkeypatch.setattr(services, "NewsCollectionService", FakeCollectionService)
    monkeypatch.setattr(storage, "SQLiteEventStore", FakeEventStore)
    monkeypatch.setattr(storage, "FileRawDocumentStore", FakeRawStore)

    _, results = asyncio.run(
        cli._collect_close_research_news(
            "510300.SH",
            tmp_path,
            ("global_sina",),
            entity_aliases=("沪深300ETF华泰柏瑞",),
            lookback_start=date(2026, 7, 1),
            lookback_end=date(2026, 8, 13),
        )
    )

    assert captured == [
        (
            "akshare.global_sina",
            "akshare.individual_eastmoney.510300",
            NBS_SOURCE_ID,
            PBOC_SOURCE_ID,
            FED_SOURCE_ID,
            CSRC_SOURCE_ID,
            MOF_SOURCE_ID,
            NDRC_SOURCE_ID,
            SAFE_SOURCE_ID,
            SSE_SOURCE_ID,
        )
    ]
    assert [item["source_id"] for item in results][-8:] == list(official)


def test_close_news_collection_rejects_source_id_collisions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class FakeSource:
        def __init__(self, config: object | None = None) -> None:
            self.config = config

    monkeypatch.setattr(ingest, "AKShareNewsSource", FakeSource)
    monkeypatch.setattr(
        ingest,
        "build_default_official_macro_sources",
        lambda: {"akshare.global_sina": FakeSource()},
    )

    with pytest.raises(ValueError, match="duplicate news source IDs"):
        asyncio.run(
            cli._collect_close_research_news(
                "510300.SH",
                tmp_path,
                ("global_sina",),
                lookback_start=date(2026, 7, 1),
                lookback_end=date(2026, 8, 13),
            )
        )
