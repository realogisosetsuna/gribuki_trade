"""A 股只读市场和研究注册表命令处理器。

这些处理器保持在 CLI facade 外部，运行时依赖仍通过 facade 解析，
以兼容现有命令入口及测试中的 monkeypatch。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from gribuki_trade import cli as _runtime_cli

if TYPE_CHECKING:
    from gribuki_trade.storage.research_runs import StoredResearchRun

_cli: Any = _runtime_cli


def _ashare_research_runs(
    action: str,
    run_db: str,
    run_id: str | None,
    run_type: str | None,
    limit: int,
) -> dict[str, object]:
    """读取已保留运行输出，且不创建原本不存在的注册表。"""

    from gribuki_trade.storage import SQLiteResearchRunStore

    if action not in {"list", "get"}:
        raise ValueError("unsupported research run action")
    if limit < 1:
        raise ValueError("limit must be positive")
    if action == "get" and (run_id is None or not run_id.strip()):
        raise ValueError("--run-id is required for get")
    if action == "list" and run_id is not None:
        raise ValueError("--run-id is only valid for get")
    if action == "get" and run_type is not None:
        raise ValueError("--run-type is only valid for list")

    path = _cli.Path(run_db).resolve()
    if not path.is_file():
        return {
            "ok": False,
            "action": action,
            "error_code": "RESEARCH_RUN_DATABASE_NOT_FOUND",
            "run_database": str(path),
            "run": None,
            "runs": [],
        }

    with SQLiteResearchRunStore(path) as store:
        if action == "list":
            runs = store.list_runs(run_type=run_type, limit=limit)
            return {
                "ok": True,
                "action": action,
                "run_database": str(path),
                "run_type": run_type,
                "count": len(runs),
                "runs": [_research_run_summary(item) for item in runs],
            }
        assert run_id is not None
        stored = store.get(run_id.strip())
        if stored is None:
            return {
                "ok": False,
                "action": action,
                "error_code": "RESEARCH_RUN_NOT_FOUND",
                "run_database": str(path),
                "run_id": run_id.strip(),
                "run": None,
            }
        return {
            "ok": True,
            "action": action,
            "run_database": str(path),
            "run": {
                **_research_run_summary(stored),
                "config": stored.config_document(),
                "payload": stored.payload_document(),
            },
        }


def _research_run_summary(item: StoredResearchRun) -> dict[str, object]:
    return {
        "run_id": item.run_id,
        "run_type": item.run_type,
        "logical_key": item.logical_key,
        "strategy_version": item.strategy_version,
        "status": item.status,
        "started_at": item.started_at.isoformat(),
        "completed_at": item.completed_at.isoformat(),
        "source_revisions": [
            {"source": source, "revision": revision} for source, revision in item.source_revisions
        ],
        "config_sha256": item.config_sha256,
        "payload_sha256": item.payload_sha256,
        "record_version": item.record_version,
    }


def _ashare_snapshot(symbol: str) -> dict[str, object]:
    from gribuki_trade.adapters import AKShareMarketDataAdapter

    snapshot = AKShareMarketDataAdapter().fetch_spot_snapshot(symbol)
    return {
        "amount": _cli._decimal_text(snapshot.amount),
        "degraded": snapshot.meta.degraded,
        "fetched_at": snapshot.meta.fetched_at.isoformat(),
        "freshness": snapshot.meta.freshness.value,
        "high": _cli._decimal_text(snapshot.high),
        "last": _cli._decimal_text(snapshot.last),
        "low": _cli._decimal_text(snapshot.low),
        "name": snapshot.name,
        "open": _cli._decimal_text(snapshot.open),
        "previous_close": _cli._decimal_text(snapshot.previous_close),
        "provider": snapshot.meta.provider,
        "semantics": snapshot.meta.semantics.value,
        "symbol": snapshot.symbol,
        "turnover_percent": _cli._decimal_text(snapshot.turnover_percent),
        "volume_lots": snapshot.volume_lots,
        "warnings": list(snapshot.meta.warnings),
    }


def _ashare_bars(
    symbol: str,
    interval_value: str,
    lookback_minutes: int,
) -> dict[str, object]:
    from gribuki_trade.adapters import AKShareMarketDataAdapter
    from gribuki_trade.ports.market_data import MinuteInterval

    interval = MinuteInterval.ONE_MINUTE if interval_value == "1m" else MinuteInterval.FIVE_MINUTES
    now = _cli.datetime.now(_cli.ZoneInfo("Asia/Shanghai"))
    bars = AKShareMarketDataAdapter().fetch_intraday_bars(
        symbol,
        now - _cli.timedelta(minutes=lookback_minutes),
        now,
        interval=interval,
        completed_only=True,
    )
    latest = bars[-1] if bars else None
    return {
        "bar_count": len(bars),
        "interval": interval.value,
        "latest": (
            None
            if latest is None
            else {
                "amount": _cli._decimal_text(latest.amount),
                "close": _cli._decimal_text(latest.close),
                "end_at": latest.end_at.isoformat(),
                "freshness": latest.meta.freshness.value,
                "high": _cli._decimal_text(latest.high),
                "low": _cli._decimal_text(latest.low),
                "open": _cli._decimal_text(latest.open),
                "semantics": latest.meta.semantics.value,
                "start_at": latest.start_at.isoformat(),
                "volume_lots": latest.volume_lots,
                "warnings": list(latest.meta.warnings),
            }
        ),
        "provider": "AKShare/Eastmoney",
        "symbol": symbol.upper(),
    }


def _ashare_daily(symbol: str, days: int) -> dict[str, object]:
    from gribuki_trade.adapters import BaoStockDailyAdapter

    end = _cli.date.today()
    start = end - _cli.timedelta(days=days)
    bars = BaoStockDailyAdapter().fetch_daily_bars(symbol, start, end)
    latest = bars[-1] if bars else None
    return {
        "bar_count": len(bars),
        "latest": (
            None
            if latest is None
            else {
                "amount": _cli._decimal_text(latest.amount),
                "close": _cli._decimal_text(latest.close),
                "is_st": latest.is_st,
                "is_trading": latest.is_trading,
                "trade_date": latest.trade_date.isoformat(),
                "volume": latest.volume,
            }
        ),
        "provider": "BaoStock",
        "symbol": symbol.upper(),
    }
