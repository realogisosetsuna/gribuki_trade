"""A 股只读市场和研究注册表命令处理器。

这些处理器保持在 CLI facade 外部，运行时依赖仍通过 facade 解析，
以兼容现有命令入口及测试中的 monkeypatch。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from gribuki_trade import cli as _runtime_cli

if TYPE_CHECKING:
    from gribuki_trade.storage.research.research_runs import StoredResearchRun

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


def _ashare_source_health(
    source_id: str | None,
    operation: str,
    days: int,
    runtime_dir: str,
) -> dict[str, object]:
    """在不访问网络的情况下汇总仅追加的采集遥测。"""

    from gribuki_trade.storage import SQLiteSourceHealthStore

    if days < 1:
        raise ValueError("days must be positive")
    if not operation.strip():
        raise ValueError("operation must not be empty")
    root = _cli.Path(runtime_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    now = _cli.datetime.now(_cli.UTC)
    start = now - _cli.timedelta(days=days)
    with SQLiteSourceHealthStore(root / "source_health.sqlite3") as health_store:
        if source_id is None:
            source_ids = sorted(
                {
                    item.source_id
                    for item in health_store.list_runs(limit=10_000)
                    if item.operation == operation
                }
            )
        else:
            source_ids = [source_id.strip().lower()]
        summaries = [
            health_store.summarize(
                item,
                window_start=start,
                window_end=now,
                operation=operation,
            )
            for item in source_ids
        ]
    return {
        "operation": operation,
        "runtime_dir": str(root),
        "window_end": now.isoformat(),
        "window_start": start.isoformat(),
        "sources": [
            {
                "degraded_rate": item.degraded_rate,
                "failed_runs": item.failed_runs,
                "latest_failure": (
                    None
                    if item.latest_failure is None
                    else item.latest_failure.finished_at.isoformat()
                ),
                "latest_success": (
                    None
                    if item.latest_success is None
                    else item.latest_success.finished_at.isoformat()
                ),
                "p50_latency_ms": item.p50_latency_ms,
                "p95_latency_ms": item.p95_latency_ms,
                "source_id": item.source_id,
                "stale_rate": item.stale_rate,
                "success_rate": item.success_rate,
                "successful_runs": item.successful_runs,
                "total_runs": item.total_runs,
            }
            for item in summaries
        ],
    }

def _ashare_watchlist(config_path: str) -> dict[str, object]:
    from gribuki_trade.watchlists import load_research_watchlist

    path = _cli.Path(config_path).resolve()
    watchlist = load_research_watchlist(path)
    default_set = frozenset(watchlist.default_symbols)
    return {
        "config": str(path),
        "default_symbol_count": len(watchlist.default_symbols),
        "instrument_count": len(watchlist.instruments),
        "instruments": [
            {
                "asset_type": item.asset_type.value,
                "board": item.board.value,
                "default_monitor": item.symbol in default_set,
                "industry": item.industry,
                "name": item.name,
                "risk_tags": list(item.risk_tags),
                "role": item.role,
                "size_tier": item.size_tier.value,
                "styles": list(item.styles),
                "symbol": item.symbol,
            }
            for item in watchlist.instruments
        ],
        "title": watchlist.title,
        "verified_on": watchlist.verified_on.isoformat(),
        "watchlist_id": watchlist.watchlist_id,
    }

def _ashare_screening_run_json(run: object) -> dict[str, object]:
    """序列化一次类型化筛选运行，且不泄露供应商对象。"""

    from gribuki_trade.services.ashare.research.ashare_screening import AShareScreeningRun

    if not isinstance(run, AShareScreeningRun):
        raise TypeError("run must be an AShareScreeningRun")

    hard_filter_reason_counts: dict[str, int] = {}
    for hard_exclusion in run.hard_filter.excluded:
        for hard_reason in hard_exclusion.reasons:
            hard_filter_reason_counts[hard_reason.value] = (
                hard_filter_reason_counts.get(hard_reason.value, 0) + 1
            )

    factor_eligibility_reason_counts: dict[str, int] = {}
    for factor_exclusion in run.factor_ranking.factor_eligibility_exclusions:
        for factor_reason in factor_exclusion.reasons:
            factor_eligibility_reason_counts[factor_reason.value] = (
                factor_eligibility_reason_counts.get(factor_reason.value, 0) + 1
            )

    insufficient_reason_counts: dict[str, int] = {}
    for candidate in run.factor_ranking.insufficient_candidates:
        for degradation_reason in candidate.degradation_reasons:
            insufficient_reason_counts[degradation_reason] = (
                insufficient_reason_counts.get(degradation_reason, 0) + 1
            )

    factor_source: dict[str, object] | None = None
    if run.factor_source_id is not None:
        factor_source = {
            "feature_version": run.feature_version,
            "source_id": run.factor_source_id,
            "source_revision": run.factor_source_revision,
        }

    return {
        "as_of": run.as_of.isoformat(),
        "decision_at": run.decision_at.isoformat(),
        "eligible_count": run.eligible_count,
        "error_code": None,
        "exclusions": {
            "factor_budget_deferred": {
                "count": len(run.factor_budget_deferred),
                "reason": "FACTOR_BUDGET_DEFERRED",
            },
            "factor_eligibility": {
                "by_reason": dict(sorted(factor_eligibility_reason_counts.items())),
                "count": len(run.factor_ranking.factor_eligibility_exclusions),
            },
            "hard_filter": {
                "by_reason": dict(sorted(hard_filter_reason_counts.items())),
                "count": len(run.hard_filter.excluded),
            },
            "insufficient_candidates": {
                "by_reason": dict(sorted(insufficient_reason_counts.items())),
                "count": len(run.factor_ranking.insufficient_candidates),
            },
        },
        "factor_requested_count": run.factor_requested_count,
        "globally_unavailable_factors": [
            item.value for item in run.factor_ranking.globally_unavailable_factors
        ],
        "hard_filter_eligible_count": run.hard_filter_eligible_count,
        "ok": True,
        "ranked_count": run.ranked_count,
        "source": {
            "factors": factor_source,
            "universe": {
                "source_id": run.universe_source_id,
                "source_revision": run.universe_source_revision,
            },
        },
        "status": run.status.value,
        "strategy_version": run.strategy_version,
        "top_candidates": [
            {
                "board": candidate.board.value,
                "coverage": candidate.factor_weight_coverage,
                "data_status": candidate.data_status.value,
                "degradations": list(candidate.degradation_reasons),
                "factor_contributions": [
                    {
                        "contribution": factor.contribution,
                        "cross_section_observations": (factor.cross_section_observations),
                        "directional_score": factor.directional_score,
                        "factor": factor.factor_id.value,
                        "percentile_rank": factor.percentile_rank,
                        "raw_value": factor.raw_value,
                        "status": factor.status.value,
                        "weight": factor.configured_weight,
                        "winsorized_value": factor.winsorized_value,
                    }
                    for factor in candidate.factor_contributions
                ],
                "industry": candidate.industry,
                "name": candidate.name,
                "rank": candidate.rank,
                "score": candidate.composite_score,
                "symbol": candidate.symbol,
            }
            for candidate in run.top_candidates
        ],
        "universe_count": run.universe_count,
        "warnings": list(run.warnings),
    }

def _ashare_intraday_run_json(run: object) -> dict[str, object]:
    from gribuki_trade.services.ashare.research.ashare_surveillance import AShareSurveillanceRun

    if not isinstance(run, AShareSurveillanceRun):
        raise TypeError("run must be an AShareSurveillanceRun")
    exclusion_counts: dict[str, int] = {}
    for item in run.ranking.excluded:
        for reason in item.reasons:
            exclusion_counts[reason.value] = exclusion_counts.get(reason.value, 0) + 1
    return {
        "ok": True,
        "session_date": run.session_date.isoformat(),
        "requested_at": run.requested_at.isoformat(),
        "decision_at": run.decision_at.isoformat(),
        "status": run.status.value,
        "strategy_version": run.strategy_version,
        "source": {
            "source_id": run.source_id,
            "source_revision": run.source_revision,
        },
        "universe_count": run.universe_count,
        "eligible_count": run.ranking.eligible_count,
        "exclusions": dict(sorted(exclusion_counts.items())),
        "globally_unavailable_factors": [
            item.value for item in run.ranking.globally_unavailable_factors
        ],
        "candidates": [
            {
                "symbol": candidate.symbol,
                "name": candidate.name,
                "rank": candidate.rank,
                "candidate_class": candidate.candidate_class.value,
                "anomaly_score": candidate.anomaly_score,
                "factor_weight_coverage": candidate.factor_weight_coverage,
                "last_price": str(candidate.last_price),
                "change_percent": str(candidate.change_percent),
                "session_amount_cny": str(candidate.session_amount_cny),
                "reason_codes": list(candidate.reason_codes),
                "factors": [
                    {
                        "factor": factor.factor_id.value,
                        "raw_value": factor.raw_value,
                        "winsorized_value": factor.winsorized_value,
                        "percentile_rank": factor.percentile_rank,
                        "directional_score": factor.directional_score,
                        "weight": factor.configured_weight,
                        "contribution": factor.contribution,
                        "cross_section_observations": (factor.cross_section_observations),
                    }
                    for factor in candidate.factors
                ],
            }
            for candidate in run.ranking.candidates
        ],
        "warnings": list(run.warnings),
        "error_code": None,
    }
