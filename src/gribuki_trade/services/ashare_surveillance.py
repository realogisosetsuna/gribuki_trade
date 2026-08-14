"""盘中全市场异常监控编排。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from zoneinfo import ZoneInfo

from gribuki_trade.features.ashare_surveillance import (
    AShareIntradayRanking,
    AShareIntradaySurveillanceConfig,
    rank_intraday_anomalies,
)
from gribuki_trade.ports.ashare_surveillance import (
    AShareIntradayUniverseSnapshot,
    AsyncAShareIntradayUniverseData,
    SurveillanceSourceQuality,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


class AShareSurveillanceRunStatus(StrEnum):
    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"
    NO_CANDIDATES = "NO_CANDIDATES"


class AShareMarketSessionError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"A-share intraday surveillance unavailable ({code})")


@dataclass(frozen=True, slots=True)
class AShareSurveillanceRun:
    session_date: date
    requested_at: datetime
    decision_at: datetime
    status: AShareSurveillanceRunStatus
    strategy_version: str
    source_id: str
    source_revision: str
    universe_count: int
    ranking: AShareIntradayRanking
    warnings: tuple[str, ...]


class AShareIntradaySurveillanceService:
    """拉取并评分一份当前交易日的新鲜标的全集快照。

    输出是候选发现结果，不依赖券商、订单或通知，也不能直接成为交易指令。
    """

    def __init__(
        self,
        data_source: AsyncAShareIntradayUniverseData,
        *,
        config: AShareIntradaySurveillanceConfig | None = None,
        minimum_universe_count: int = 4500,
        maximum_snapshot_age: timedelta = timedelta(minutes=3),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if minimum_universe_count < 1:
            raise ValueError("minimum_universe_count must be positive")
        if maximum_snapshot_age <= timedelta(0):
            raise ValueError("maximum_snapshot_age must be positive")
        self._data_source = data_source
        self._config = config or AShareIntradaySurveillanceConfig()
        self._minimum_universe_count = minimum_universe_count
        self._maximum_snapshot_age = maximum_snapshot_age
        self._clock = clock

    async def run_once(
        self,
        *,
        session_date: date,
        requested_at: datetime,
    ) -> AShareSurveillanceRun:
        requested_at = _aware_utc(requested_at, "requested_at")
        _validate_open_session(session_date, requested_at)
        snapshot = await self._data_source.fetch_intraday_universe(
            session_date=session_date,
            known_at=requested_at,
        )
        decision_at = _aware_utc(self._clock(), "clock")
        if decision_at < requested_at:
            raise AShareMarketSessionError("CLOCK_MOVED_BACKWARDS")
        _validate_snapshot(
            snapshot,
            session_date=session_date,
            decision_at=decision_at,
            minimum_universe_count=self._minimum_universe_count,
            maximum_snapshot_age=self._maximum_snapshot_age,
        )
        ranking = rank_intraday_anomalies(snapshot.records, config=self._config)
        warnings = list(snapshot.warnings)
        warnings.extend(
            f"GLOBAL_FACTOR_UNAVAILABLE:{factor.value}"
            for factor in ranking.globally_unavailable_factors
        )
        warnings.append(
            "candidate discovery only; current-session public snapshot is not a trade signal"
        )
        degraded = (
            snapshot.quality is SurveillanceSourceQuality.DEGRADED
            or bool(ranking.globally_unavailable_factors)
        )
        if not ranking.candidates:
            status = AShareSurveillanceRunStatus.NO_CANDIDATES
        elif degraded:
            status = AShareSurveillanceRunStatus.DEGRADED
        else:
            status = AShareSurveillanceRunStatus.COMPLETE
        return AShareSurveillanceRun(
            session_date=session_date,
            requested_at=requested_at,
            decision_at=decision_at,
            status=status,
            strategy_version=self._config.strategy_version,
            source_id=snapshot.source_id,
            source_revision=snapshot.source_revision,
            universe_count=len(snapshot.records),
            ranking=ranking,
            warnings=tuple(dict.fromkeys(warnings)),
        )


def _validate_open_session(session_date: date, requested_at: datetime) -> None:
    local = requested_at.astimezone(SHANGHAI)
    if local.date() != session_date:
        raise AShareMarketSessionError("SESSION_DATE_MISMATCH")
    current = local.timetz().replace(tzinfo=None)
    in_morning = time(9, 30) <= current <= time(11, 30)
    in_afternoon = time(13, 0) <= current < time(15, 0)
    if not (in_morning or in_afternoon):
        raise AShareMarketSessionError("MARKET_NOT_OPEN")


def _validate_snapshot(
    snapshot: AShareIntradayUniverseSnapshot,
    *,
    session_date: date,
    decision_at: datetime,
    minimum_universe_count: int,
    maximum_snapshot_age: timedelta,
) -> None:
    if snapshot.session_date != session_date:
        raise AShareMarketSessionError("SNAPSHOT_SESSION_MISMATCH")
    available_at = snapshot.available_at.astimezone(UTC)
    observed_at = snapshot.observed_at.astimezone(UTC)
    if available_at > decision_at or observed_at > decision_at:
        raise AShareMarketSessionError("SNAPSHOT_FROM_FUTURE")
    if decision_at - available_at > maximum_snapshot_age:
        raise AShareMarketSessionError("STALE_SNAPSHOT")
    if len(snapshot.records) < minimum_universe_count:
        raise AShareMarketSessionError("INCOMPLETE_UNIVERSE")


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)
