"""A 股新闻与公告命令处理器。

CLI facade 继续保留历史的 ``gribuki_trade.cli`` 名称，本模块负责一次性新闻
采集、持续新闻轮询和巨潮公告索引入库。运行时钩子通过 facade 延迟解析，测试
和嵌入式调用仍可替换时钟、JSON 与休眠对象。
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any


class _LazyCliFacade:
    """仅在 CLI facade 初始化完成后解析兼容钩子。"""

    def __getattr__(self, name: str) -> Any:
        from gribuki_trade import cli

        return getattr(cli, name)


_cli: Any = _LazyCliFacade()


async def _ashare_news(
    feed_value: str,
    symbol: str | None,
    limit: int,
    archive_dir: str,
) -> dict[str, object]:
    from gribuki_trade.ingest import (
        AKShareNewsConfig,
        AKShareNewsFeed,
        AKShareNewsSource,
    )
    from gribuki_trade.storage import FileRawDocumentStore

    feed = AKShareNewsFeed(feed_value)
    config = AKShareNewsConfig(feed=feed, symbol=symbol)
    batch = await AKShareNewsSource(config).collect()
    store = FileRawDocumentStore(_cli.Path(archive_dir))
    stored = [store.save(document) for document in batch.documents]
    events = sorted(batch.events, key=lambda item: item.available_at, reverse=True)
    return {
        "archive_created": sum(item.created for item in stored),
        "event_count": len(events),
        "feed": feed.value,
        "latest": [
            {
                "available_at": event.available_at.isoformat(),
                "canonical_url": event.canonical_url,
                "event_id": event.event_id,
                "first_seen_at": event.first_seen_at.isoformat(),
                "published_at": (
                    event.published_at.isoformat() if event.published_at is not None else None
                ),
                "source_id": event.source_id,
                "summary": event.summary,
                "title": event.title,
            }
            for event in events[:limit]
        ],
        "not_modified": batch.not_modified,
        "raw_document_count": len(batch.documents),
        "source_id": batch.source_id,
    }


async def _ashare_news_watch(
    feed_values: Sequence[str] | None,
    symbols: Sequence[str] | None,
    interval_seconds: float,
    cycles: int,
    runtime_dir: str,
) -> dict[str, object]:
    if not 0 < interval_seconds < float("inf"):
        raise ValueError("interval_seconds must be positive and finite")
    if cycles < 0:
        raise ValueError("cycles must be non-negative")

    from gribuki_trade.ingest import (
        AKShareNewsConfig,
        AKShareNewsFeed,
        AKShareNewsSource,
    )
    from gribuki_trade.services import (
        NewsCollectionService,
        SourceCollectionObservation,
        SourceRunStatus,
    )
    from gribuki_trade.storage import (
        FileRawDocumentStore,
        ProviderRun,
        ProviderRunStatus,
        SQLiteEventStore,
        SQLiteSourceHealthStore,
    )

    selected_feeds = tuple(feed_values or ("global_sina", "global_cailianpress"))
    root = _cli.Path(runtime_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    sources: dict[str, AKShareNewsSource] = {}
    for value in selected_feeds:
        config = AKShareNewsConfig(AKShareNewsFeed(value))
        sources[config.source_id] = AKShareNewsSource(config)
    for symbol in symbols or ():
        config = AKShareNewsConfig(
            AKShareNewsFeed.INDIVIDUAL_EASTMONEY,
            symbol=symbol,
        )
        sources[config.source_id] = AKShareNewsSource(config)
    if not sources:
        raise ValueError("at least one news feed or symbol is required")

    completed_cycles = 0
    latest: list[dict[str, object]] = []
    with (
        SQLiteEventStore(root / "events.sqlite3") as event_store,
        SQLiteSourceHealthStore(root / "source_health.sqlite3") as health_store,
    ):

        def observe(item: SourceCollectionObservation) -> None:
            result = item.result
            successful = result.status in {
                SourceRunStatus.SUCCESS,
                SourceRunStatus.NOT_MODIFIED,
            }
            health_store.append(
                ProviderRun(
                    run_id=_cli.uuid4().hex,
                    source_id=result.source_id,
                    operation="news_collect",
                    started_at=item.started_at,
                    finished_at=item.finished_at,
                    status=(ProviderRunStatus.SUCCESS if successful else ProviderRunStatus.FAILURE),
                    error_code=result.error_code,
                    item_count=(
                        result.events_new + result.events_revised + result.events_duplicate
                    ),
                    degraded=result.status is SourceRunStatus.BACKOFF,
                    stale=False,
                    latency_ms=item.latency_ms,
                    adapter_version="akshare-news:1",
                )
            )

        service = NewsCollectionService(
            sources,
            raw_store=FileRawDocumentStore(root / "raw"),
            event_store=event_store,
            observer=observe,
        )
        while cycles == 0 or completed_cycles < cycles:
            results = await service.run_once()
            completed_cycles += 1
            latest = [
                {
                    "documents_saved": item.documents_saved,
                    "error_code": item.error_code,
                    "events_duplicate": item.events_duplicate,
                    "events_new": item.events_new,
                    "events_revised": item.events_revised,
                    "source_id": item.source_id,
                    "status": item.status.value,
                }
                for item in results
            ]
            print(
                _cli.json.dumps(
                    {
                        "cycle": completed_cycles,
                        "sources": latest,
                        "timestamp": _cli.datetime.now(_cli.UTC).isoformat(),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                flush=True,
            )
            if cycles != 0 and completed_cycles >= cycles:
                break
            await _cli.asyncio.sleep(interval_seconds)
        retained_events = len(event_store.latest(limit=10_000))
    return {
        "completed_cycles": completed_cycles,
        "latest_sources": latest,
        "retained_latest_event_count": retained_events,
        "runtime_dir": str(root),
    }


async def _ashare_disclosures(
    symbol: str,
    lookback_days: int,
    category: str,
    runtime_dir: str,
) -> dict[str, object]:
    from gribuki_trade.ingest import (
        AKShareDisclosureConfig,
        AKShareDisclosureSource,
    )
    from gribuki_trade.services import NewsCollectionService
    from gribuki_trade.storage import FileRawDocumentStore, SQLiteEventStore

    if lookback_days < 1:
        raise ValueError("lookback_days must be positive")
    today = _cli.date.today()
    config = AKShareDisclosureConfig(
        symbol=symbol,
        start_date=today - _cli.timedelta(days=lookback_days),
        end_date=today,
        category=category,
    )
    root = _cli.Path(runtime_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    source = AKShareDisclosureSource(config)
    with SQLiteEventStore(root / "events.sqlite3") as event_store:
        service = NewsCollectionService(
            {config.source_id: source},
            raw_store=FileRawDocumentStore(root / "raw"),
            event_store=event_store,
            max_concurrency=1,
        )
        result = (await service.run_once())[0]
        latest = tuple(
            event for event in event_store.latest(limit=200) if event.source_id == config.source_id
        )
    return {
        "documents_saved": result.documents_saved,
        "error_code": result.error_code,
        "events_duplicate": result.events_duplicate,
        "events_new": result.events_new,
        "events_revised": result.events_revised,
        "latest": [
            {
                "available_at": event.available_at.isoformat(),
                "event_id": event.event_id,
                "published_at": (
                    None if event.published_at is None else event.published_at.isoformat()
                ),
                "title": event.title,
                "url": event.canonical_url,
            }
            for event in latest[:20]
        ],
        "runtime_dir": str(root),
        "source_id": config.source_id,
        "status": result.status.value,
        "symbol": symbol.strip().upper(),
    }


__all__ = ["_ashare_disclosures", "_ashare_news", "_ashare_news_watch"]
