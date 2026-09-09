"""实盘保护计划的应用级单轮行情跟踪。

这里刻意只提供一次有限轮询：未来的应用 runtime 可以周期调用它，但本模块
不会注册 Windows 任务、系统服务或常驻监听器。每轮仅消费提供方明确标记为
完整、且在观察时点已经可见的一分钟线；触发后只投递 NapCat 提醒，不下单。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from gribuki_trade.domain.live_records import LiveProtectionTracking, LiveWorkKind
from gribuki_trade.features.technical import TechnicalBar
from gribuki_trade.ports.market_data import (
    AsyncIntradayMarketData,
    IntradayBar,
    MarketDataUnavailableError,
    MinuteInterval,
)
from gribuki_trade.services.live.live_trade_orchestration import (
    LiveTradeOrchestrationService,
    LiveWorkRunSummary,
)
from gribuki_trade.storage.live_records.live_records import SQLiteLiveRecordStore


@dataclass(frozen=True, slots=True)
class LiveTrackingTargetFailure:
    """单个账户标的的稳定失败信息，不携带上游异常原文。"""

    account_id: str
    symbol: str
    error_code: str


@dataclass(frozen=True, slots=True)
class LiveTrackingCycleSummary:
    """一次跟踪轮询及本轮提醒入 outbox 的结果。"""

    target_count: int
    successful_targets: int
    fetched_bars: int
    barrier_observations: int
    queued_alerts: int
    failures: tuple[LiveTrackingTargetFailure, ...]
    delivery: LiveWorkRunSummary


@dataclass(frozen=True, slots=True)
class _TrackingTarget:
    account_id: str
    symbol: str
    start: datetime


class LiveMarketTrackingCycleService:
    """从真实分钟线端口执行一次可恢复的持仓保护观察。"""

    def __init__(
        self,
        *,
        live_store: SQLiteLiveRecordStore,
        market_data: AsyncIntradayMarketData,
        orchestration: LiveTradeOrchestrationService,
        initial_lookback: timedelta = timedelta(hours=6),
        maximum_concurrency: int = 4,
    ) -> None:
        if initial_lookback <= timedelta(0):
            raise ValueError("initial_lookback must be positive")
        if (
            isinstance(maximum_concurrency, bool)
            or not isinstance(maximum_concurrency, int)
            or maximum_concurrency < 1
        ):
            raise ValueError("maximum_concurrency must be a positive integer")
        self._store = live_store
        self._market = market_data
        self._orchestration = orchestration
        self._initial_lookback = initial_lookback
        self._maximum_concurrency = maximum_concurrency

    async def run_once(
        self,
        *,
        observed_at: datetime | None = None,
        delivery_limit: int = 100,
        protection_ids: frozenset[str] | None = None,
    ) -> LiveTrackingCycleSummary:
        """观察全部活动批次，并立即把已形成的提醒写入通用 outbox。"""

        moment = _aware_utc(observed_at or datetime.now(UTC))
        selected_protection_ids = _normalized_protection_ids(protection_ids)
        targets = self._targets(moment, protection_ids=selected_protection_ids)
        semaphore = asyncio.Semaphore(self._maximum_concurrency)

        async def fetch(target: _TrackingTarget) -> tuple[_TrackingTarget, object]:
            async with semaphore:
                try:
                    bars = await self._market.fetch_intraday_bars_async(
                        target.symbol,
                        target.start,
                        moment,
                        interval=MinuteInterval.ONE_MINUTE,
                        completed_only=True,
                    )
                except (MarketDataUnavailableError, OSError, TimeoutError):
                    return target, LiveTrackingTargetFailure(
                        target.account_id,
                        target.symbol,
                        "LIVE_TRACKING_MARKET_DATA_UNAVAILABLE",
                    )
                return target, tuple(bars)

        fetched = await asyncio.gather(*(fetch(target) for target in targets))
        failures: list[LiveTrackingTargetFailure] = []
        successful = 0
        fetched_bars = 0
        observations = 0
        queued: set[str] = set()
        for target, result in fetched:
            if isinstance(result, LiveTrackingTargetFailure):
                failures.append(result)
                continue
            try:
                bars = _completed_technical_bars(
                    result,
                    symbol=target.symbol,
                    observed_at=moment,
                )
            except (TypeError, ValueError):
                failures.append(
                    LiveTrackingTargetFailure(
                        target.account_id,
                        target.symbol,
                        "LIVE_TRACKING_MARKET_DATA_INVALID",
                    )
                )
                continue
            successful += 1
            fetched_bars += len(bars)
            for bar in bars:
                outcome = self._orchestration.observe_completed_bar(
                    account_id=target.account_id,
                    symbol=target.symbol,
                    bar=bar,
                    observed_at=moment,
                    protection_ids=selected_protection_ids,
                )
                observations += len(outcome.observations)
                queued.update(outcome.queued_alert_work_ids)

        delivery = await self._orchestration.process_due_work(
            now=moment,
            kinds=frozenset({LiveWorkKind.DELIVER_EXIT_ALERT}),
            work_ids=(
                None
                if selected_protection_ids is None
                else frozenset(queued)
            ),
            limit=delivery_limit,
        )
        return LiveTrackingCycleSummary(
            target_count=len(targets),
            successful_targets=successful,
            fetched_bars=fetched_bars,
            barrier_observations=observations,
            queued_alerts=len(queued),
            failures=tuple(failures),
            delivery=delivery,
        )

    def _targets(
        self,
        moment: datetime,
        *,
        protection_ids: frozenset[str] | None = None,
    ) -> tuple[_TrackingTarget, ...]:
        grouped: dict[tuple[str, str], list[LiveProtectionTracking]] = {}
        for account_id in self._store.account_ids():
            for tracking in self._store.tracking(account_id, active_only=True):
                if protection_ids is not None and tracking.protection_id not in protection_ids:
                    continue
                if not tracking.plan_ready:
                    continue
                grouped.setdefault((account_id, tracking.symbol), []).append(tracking)
        targets: list[_TrackingTarget] = []
        for (account_id, symbol), items in sorted(grouped.items()):
            observed = tuple(
                item.last_observed_bar_end
                for item in items
                if item.last_observed_bar_end is not None
            )
            start = (
                min(observed) - timedelta(minutes=1)
                if observed
                else moment - self._initial_lookback
            )
            targets.append(_TrackingTarget(account_id, symbol, start))
        return tuple(targets)


def _completed_technical_bars(
    values: object,
    *,
    symbol: str,
    observed_at: datetime,
) -> tuple[TechnicalBar, ...]:
    if not isinstance(values, tuple):
        raise TypeError("market data result must be a tuple")
    normalized = symbol.strip().upper()
    by_end: dict[datetime, IntradayBar] = {}
    fingerprints: dict[datetime, tuple[object, ...]] = {}
    for value in values:
        if not isinstance(value, IntradayBar):
            raise TypeError("market data result contains an invalid bar")
        fetched_at = _aware_utc(value.meta.fetched_at)
        end_at = _aware_utc(value.end_at)
        if value.symbol.strip().upper() != normalized:
            raise ValueError("market data symbol mismatch")
        if value.interval is not MinuteInterval.ONE_MINUTE:
            raise ValueError("market data interval mismatch")
        if not value.is_closed or end_at > observed_at or fetched_at > observed_at:
            continue
        fingerprint = (
            value.open,
            value.high,
            value.low,
            value.close,
            value.volume_lots,
            value.is_closed,
        )
        previous = fingerprints.get(end_at)
        if previous is not None and previous != fingerprint:
            raise ValueError("conflicting bars share the same end time")
        fingerprints[end_at] = fingerprint
        by_end[end_at] = value
    return tuple(
        TechnicalBar(
            end_time=end_at,
            available_at=_aware_utc(value.meta.fetched_at),
            open=value.open,
            high=value.high,
            low=value.low,
            close=value.close,
            volume=value.volume_lots,
            complete=True,
        )
        for end_at, value in sorted(by_end.items())
    )


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("timestamp must be datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _normalized_protection_ids(
    values: frozenset[str] | None,
) -> frozenset[str] | None:
    if values is None:
        return None
    normalized = frozenset(
        value.strip()
        for value in values
        if isinstance(value, str) and value.strip()
    )
    if len(normalized) != len(values):
        raise ValueError("protection_ids must contain only unique non-empty strings")
    return normalized


__all__ = [
    "LiveMarketTrackingCycleService",
    "LiveTrackingCycleSummary",
    "LiveTrackingTargetFailure",
]
