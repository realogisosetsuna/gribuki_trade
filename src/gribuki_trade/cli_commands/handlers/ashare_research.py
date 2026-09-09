"""A 股研究监控命令处理器。

该模块承载有界的多标的研究轮询和标的解析。单次研究执行仍由 CLI
facade 提供，以保留旧导入路径、测试替换点和 research_only 约束。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast


def _cli() -> Any:
    """延迟取得 CLI facade，避免命令处理器和 facade 互相初始化。"""

    import gribuki_trade.cli as cli

    return cli


async def _ashare_research_watch(
    symbols: Sequence[str] | None,
    watchlist_path: str,
    watchlist_all: bool,
    interval_value: str,
    lookback_minutes: int,
    interval_seconds: float,
    cycles: int,
    events_db: str,
    research_db: str,
    outbox_db: str,
    macro_enabled: bool,
    macro_provider: str,
    model: str | None,
    notify_target_kind: str | None,
    notify_target_id: str | None,
    candidate_store_path: str | None = None,
    candidates_only: bool = False,
) -> dict[str, object]:
    """运行有界研究周期，并按标的隔离失败。"""

    from gribuki_trade.services import ResearchWatchService

    if cycles < 1:
        raise ValueError("cycles must be positive")
    if not 0 <= interval_seconds < float("inf"):
        raise ValueError("interval_seconds must be non-negative and finite")
    if lookback_minutes < 1:
        raise ValueError("lookback_minutes must be positive")
    if (notify_target_kind is None) != (notify_target_id is None):
        raise ValueError("notification target kind and ID must be supplied together")
    if candidates_only and candidate_store_path is None:
        raise ValueError("--candidates-only requires --candidate-db")

    resolved_symbols = _resolve_dynamic_research_symbols(
        symbols=symbols,
        watchlist_path=watchlist_path,
        watchlist_all=watchlist_all,
        candidate_store_path=candidate_store_path,
        candidates_only=candidates_only,
        as_of=datetime.now(UTC),
    )
    cli = _cli()

    async def run_symbol(symbol: str) -> dict[str, object]:
        return cast(
            dict[str, object],
            await cli._ashare_research_once(
                symbol,
                interval_value,
                lookback_minutes,
                events_db,
                research_db,
                outbox_db,
                macro_enabled,
                macro_provider,
                model,
                notify_target_kind,
                notify_target_id,
            ),
        )

    service: ResearchWatchService[dict[str, object]] = ResearchWatchService(
        resolved_symbols,
        run_symbol,
    )
    statistics = await service.run(
        max_cycles=cycles,
        interval_seconds=interval_seconds,
    )
    return {
        "completed_at": statistics.completed_at.isoformat(),
        "cycles": [
            {
                "completed_at": cycle.completed_at.isoformat(),
                "cycle_number": cycle.cycle_number,
                "failed": cycle.failed,
                "started_at": cycle.started_at.isoformat(),
                "succeeded": cycle.succeeded,
                "symbols": [
                    {
                        "error_code": item.error_code,
                        "result": item.result,
                        "status": item.status.value,
                        "symbol": item.symbol,
                    }
                    for item in cycle.symbol_runs
                ],
            }
            for cycle in statistics.cycles
        ],
        "cycles_completed": statistics.cycles_completed,
        "failed": statistics.failed,
        "reached_cycle_limit": statistics.reached_cycle_limit,
        "started_at": statistics.started_at.isoformat(),
        "stop_requested": statistics.stop_requested,
        "succeeded": statistics.succeeded,
        "symbols": list(service.symbols),
        "symbols_attempted": statistics.symbols_attempted,
    }


def _resolve_ashare_research_symbols(
    symbols: Sequence[str] | None,
    *,
    watchlist_path: str,
    watchlist_all: bool,
) -> tuple[str, ...]:
    """从显式参数或研究观察列表解析基础标的。"""

    if symbols:
        return tuple(symbols)

    from gribuki_trade.watchlists import load_research_watchlist

    watchlist = load_research_watchlist(Path(watchlist_path).resolve())
    return watchlist.symbols if watchlist_all else watchlist.default_symbols


def _resolve_dynamic_research_symbols(
    *,
    symbols: Sequence[str] | None,
    watchlist_path: str,
    watchlist_all: bool,
    candidate_store_path: str | None,
    candidates_only: bool,
    as_of: datetime,
) -> tuple[str, ...]:
    """合并观察列表和 ACTIVE 候选，并去除重复标的。"""

    resolved: list[str] = []
    if not candidates_only:
        resolved.extend(
            _resolve_ashare_research_symbols(
                symbols,
                watchlist_path=watchlist_path,
                watchlist_all=watchlist_all,
            )
        )
    if candidate_store_path is not None:
        from gribuki_trade.services.research.candidate_universe import CandidateUniverseService
        from gribuki_trade.storage.research.candidate_store import SQLiteCandidateStore

        path = Path(candidate_store_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        with SQLiteCandidateStore(path) as store:
            candidates = CandidateUniverseService(store).tracking_candidates(
                as_of=as_of,
                include_cooling=False,
            )
        resolved.extend(item.symbol for item in candidates)
    unique = tuple(dict.fromkeys(resolved))
    if not unique:
        raise ValueError("no ACTIVE symbols are available for research monitoring")
    return unique


__all__ = [
    "_ashare_research_watch",
    "_resolve_ashare_research_symbols",
    "_resolve_dynamic_research_symbols",
]
