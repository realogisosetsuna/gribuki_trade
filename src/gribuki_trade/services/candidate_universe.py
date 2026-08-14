"""研究工作流的统一候选标的全集编排。

本服务接收手工选择与扫描器发现，应用明确的 TTL 策略，并公开受跟踪的标的集合。
它刻意不依赖订单、账户、券商或执行服务。
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol

from gribuki_trade.domain.candidates import (
    CandidateControlAction,
    CandidateControlEvent,
    CandidateObservation,
    CandidatePriority,
    CandidateRecord,
    CandidateSource,
    CandidateStatus,
)

if TYPE_CHECKING:
    from gribuki_trade.services.ashare_screening import AShareScreeningRun
    from gribuki_trade.services.ashare_surveillance import AShareSurveillanceRun


class CandidateRepository(Protocol):
    """候选标的全集服务所需的存储边界。"""

    def append_observation(self, item: CandidateObservation) -> bool: ...

    def append_control(self, item: CandidateControlEvent) -> bool: ...

    def get_candidate(
        self,
        symbol: str,
        *,
        as_of: datetime,
    ) -> CandidateRecord | None: ...

    def list_candidates(
        self,
        *,
        as_of: datetime,
        statuses: frozenset[CandidateStatus] | None = None,
        limit: int = 500,
    ) -> tuple[CandidateRecord, ...]: ...


class CandidateLifecycleError(RuntimeError):
    """请求的生命周期转换在该时点没有意义。"""


@dataclass(frozen=True, slots=True)
class CandidateDiscovery:
    """由单个扫描器或人工工作流提交、尚未持久化的发现。"""

    symbol: str
    source: CandidateSource
    source_run_id: str
    discovered_at: datetime
    reason_codes: tuple[str, ...]
    evidence_ids: tuple[str, ...] = ()
    observed_at: datetime | None = None
    expires_at: datetime | None = None
    priority: CandidatePriority | None = None


@dataclass(frozen=True, slots=True)
class CandidateMutation:
    """幂等变更的结果及由此产生的时点状态。"""

    candidate: CandidateRecord
    appended: bool


@dataclass(frozen=True, slots=True)
class CandidateUniversePolicy:
    """每种发现来源的初始显式 TTL 与优先级策略。"""

    close_screen_ttl: timedelta = timedelta(days=4)
    intraday_anomaly_ttl: timedelta = timedelta(hours=8)
    strategy_ttl: timedelta = timedelta(days=2)
    review_ttl: timedelta = timedelta(days=7)
    manual_ttl: timedelta | None = None
    default_cooling_period: timedelta = timedelta(hours=4)

    def __post_init__(self) -> None:
        values = (
            self.close_screen_ttl,
            self.intraday_anomaly_ttl,
            self.strategy_ttl,
            self.review_ttl,
        )
        if any(value <= timedelta(0) for value in values):
            raise ValueError("candidate TTL values must be positive")
        if self.manual_ttl is not None and self.manual_ttl <= timedelta(0):
            raise ValueError("manual_ttl must be positive when supplied")
        if self.default_cooling_period <= timedelta(0):
            raise ValueError("default_cooling_period must be positive")

    def ttl_for(self, source: CandidateSource) -> timedelta | None:
        return {
            CandidateSource.MANUAL: self.manual_ttl,
            CandidateSource.CLOSE_SCREEN: self.close_screen_ttl,
            CandidateSource.INTRADAY_ANOMALY: self.intraday_anomaly_ttl,
            CandidateSource.STRATEGY: self.strategy_ttl,
            CandidateSource.REVIEW: self.review_ttl,
        }[source]

    @staticmethod
    def priority_for(source: CandidateSource) -> CandidatePriority:
        return {
            CandidateSource.MANUAL: CandidatePriority.HIGH,
            CandidateSource.CLOSE_SCREEN: CandidatePriority.NORMAL,
            CandidateSource.INTRADAY_ANOMALY: CandidatePriority.HIGH,
            CandidateSource.STRATEGY: CandidatePriority.NORMAL,
            CandidateSource.REVIEW: CandidatePriority.HIGH,
        }[source]


class CandidateUniverseService:
    """合并候选发现，同时保留不可变来源信息。"""

    def __init__(
        self,
        repository: CandidateRepository,
        *,
        policy: CandidateUniversePolicy | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._repository = repository
        self._policy = policy or CandidateUniversePolicy()
        self._clock = clock

    @property
    def policy(self) -> CandidateUniversePolicy:
        return self._policy

    def upsert(self, discovery: CandidateDiscovery) -> CandidateMutation:
        """将一次来源运行的发现幂等加入合并标的全集。"""

        observed_at = discovery.observed_at or self._now()
        ttl = self._policy.ttl_for(discovery.source)
        expires_at = discovery.expires_at
        if expires_at is None and ttl is not None:
            expires_at = observed_at + ttl
        observation = CandidateObservation(
            symbol=discovery.symbol,
            source=discovery.source,
            source_run_id=discovery.source_run_id,
            discovered_at=discovery.discovered_at,
            observed_at=observed_at,
            expires_at=expires_at,
            priority=discovery.priority or self._policy.priority_for(discovery.source),
            reason_codes=discovery.reason_codes,
            evidence_ids=discovery.evidence_ids,
        )
        appended = self._repository.append_observation(observation)
        candidate = self._repository.get_candidate(
            observation.symbol,
            as_of=observation.observed_at,
        )
        if candidate is None:
            raise RuntimeError("persisted candidate observation was not visible")
        return CandidateMutation(candidate=candidate, appended=appended)

    def upsert_many(
        self,
        discoveries: Sequence[CandidateDiscovery],
    ) -> tuple[CandidateMutation, ...]:
        """按确定性输入顺序更新或插入调用方拥有的有界批次。"""

        return tuple(self.upsert(item) for item in discoveries)

    def ingest_close_screening(
        self,
        run: AShareScreeningRun,
        *,
        expires_at: datetime | None = None,
    ) -> tuple[CandidateMutation, ...]:
        """将一次确定性筛选的前 N 名提升为研究候选标的。"""

        run_id = _screening_run_id(run)
        discoveries: list[CandidateDiscovery] = []
        for candidate in run.top_candidates:
            if candidate.rank is None:
                raise ValueError("close-screen Top-N candidate must have a rank")
            reasons = (
                "CLOSE_SCREEN_TOP_N",
                f"CLOSE_SCREEN_RANK_{candidate.rank:04d}",
                f"SCREEN_DATA_{candidate.data_status.value}",
                *candidate.degradation_reasons,
            )
            discoveries.append(
                CandidateDiscovery(
                    symbol=candidate.symbol,
                    source=CandidateSource.CLOSE_SCREEN,
                    source_run_id=run_id,
                    discovered_at=run.decision_at,
                    observed_at=run.decision_at,
                    expires_at=expires_at,
                    priority=(
                        CandidatePriority.HIGH
                        if candidate.rank <= 5
                        else CandidatePriority.NORMAL
                    ),
                    reason_codes=reasons,
                )
            )
        return self.upsert_many(discoveries)

    def ingest_intraday_surveillance(
        self,
        run: AShareSurveillanceRun,
        *,
        expires_at: datetime | None = None,
    ) -> tuple[CandidateMutation, ...]:
        """将盘中异常排名提升为短期候选标的。"""

        run_id = _surveillance_run_id(run)
        discoveries = tuple(
            CandidateDiscovery(
                symbol=candidate.symbol,
                source=CandidateSource.INTRADAY_ANOMALY,
                source_run_id=run_id,
                discovered_at=run.decision_at,
                observed_at=run.decision_at,
                expires_at=expires_at,
                priority=(
                    CandidatePriority.URGENT
                    if candidate.rank <= 5
                    else CandidatePriority.HIGH
                ),
                reason_codes=(
                    "INTRADAY_ANOMALY_CANDIDATE",
                    f"INTRADAY_CLASS_{candidate.candidate_class.value}",
                    f"INTRADAY_RANK_{candidate.rank:04d}",
                    f"FACTOR_WEIGHT_COVERAGE_{candidate.factor_weight_coverage:.3f}",
                    *candidate.reason_codes,
                ),
            )
            for candidate in run.ranking.candidates
        )
        return self.upsert_many(discoveries)

    def cool(
        self,
        symbol: str,
        *,
        reason_code: str,
        at: datetime | None = None,
        until: datetime | None = None,
        operation_id: str | None = None,
    ) -> CandidateMutation:
        """暂时抑制主动监控且不丢失来源信息。"""

        occurred_at = at or self._now()
        candidate = self._required_candidate(symbol, as_of=occurred_at)
        if candidate.status in {CandidateStatus.REMOVED, CandidateStatus.EXPIRED}:
            raise CandidateLifecycleError(
                f"cannot cool a candidate in {candidate.status.value} state"
            )
        cooling_until = until or occurred_at + self._policy.default_cooling_period
        return self._control(
            symbol,
            action=CandidateControlAction.COOL,
            reason_code=reason_code,
            occurred_at=occurred_at,
            cooling_until=cooling_until,
            operation_id=operation_id,
        )

    def activate(
        self,
        symbol: str,
        *,
        reason_code: str,
        at: datetime | None = None,
        operation_id: str | None = None,
    ) -> CandidateMutation:
        """显式清除移除/冷却状态；已过期来源仍保持过期。"""

        occurred_at = at or self._now()
        self._required_candidate(symbol, as_of=occurred_at)
        return self._control(
            symbol,
            action=CandidateControlAction.ACTIVATE,
            reason_code=reason_code,
            occurred_at=occurred_at,
            cooling_until=None,
            operation_id=operation_id,
        )

    def remove(
        self,
        symbol: str,
        *,
        reason_code: str,
        at: datetime | None = None,
        operation_id: str | None = None,
    ) -> CandidateMutation:
        """显式为标的设置墓碑，直至后续明确激活。"""

        occurred_at = at or self._now()
        self._required_candidate(symbol, as_of=occurred_at)
        return self._control(
            symbol,
            action=CandidateControlAction.REMOVE,
            reason_code=reason_code,
            occurred_at=occurred_at,
            cooling_until=None,
            operation_id=operation_id,
        )

    def get(
        self,
        symbol: str,
        *,
        as_of: datetime | None = None,
    ) -> CandidateRecord | None:
        return self._repository.get_candidate(symbol, as_of=as_of or self._now())

    def tracking_candidates(
        self,
        *,
        as_of: datetime | None = None,
        include_cooling: bool = False,
        limit: int = 500,
    ) -> tuple[CandidateRecord, ...]:
        """返回调度输入；冷却记录只作为可选诊断信息。"""

        statuses = {CandidateStatus.ACTIVE}
        if include_cooling:
            statuses.add(CandidateStatus.COOLING)
        return self._repository.list_candidates(
            as_of=as_of or self._now(),
            statuses=frozenset(statuses),
            limit=limit,
        )

    def _control(
        self,
        symbol: str,
        *,
        action: CandidateControlAction,
        reason_code: str,
        occurred_at: datetime,
        cooling_until: datetime | None,
        operation_id: str | None,
    ) -> CandidateMutation:
        event = CandidateControlEvent(
            symbol=symbol,
            action=action,
            operation_id=operation_id
            or _control_operation_id(
                symbol=symbol,
                action=action,
                occurred_at=occurred_at,
                reason_code=reason_code,
                cooling_until=cooling_until,
            ),
            occurred_at=occurred_at,
            reason_code=reason_code,
            cooling_until=cooling_until,
        )
        appended = self._repository.append_control(event)
        candidate = self._repository.get_candidate(event.symbol, as_of=event.occurred_at)
        if candidate is None:
            raise RuntimeError("persisted candidate lifecycle event was not visible")
        return CandidateMutation(candidate=candidate, appended=appended)

    def _required_candidate(self, symbol: str, *, as_of: datetime) -> CandidateRecord:
        candidate = self._repository.get_candidate(symbol, as_of=as_of)
        if candidate is None:
            raise CandidateLifecycleError("candidate does not exist at the requested time")
        return candidate

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value.astimezone(UTC)


def _screening_run_id(run: AShareScreeningRun) -> str:
    material = "|".join(
        (
            run.strategy_version,
            run.as_of.isoformat(),
            run.decision_at.isoformat(),
            run.universe_source_id,
            run.universe_source_revision,
            run.factor_source_id or "none",
            run.factor_source_revision or "none",
        )
    ).encode()
    digest = hashlib.sha256(material).hexdigest()[:24]
    return f"close-{run.as_of.isoformat()}-{digest}"


def _surveillance_run_id(run: AShareSurveillanceRun) -> str:
    material = "|".join(
        (
            run.strategy_version,
            run.session_date.isoformat(),
            run.decision_at.isoformat(),
            run.source_id,
            run.source_revision,
        )
    ).encode()
    digest = hashlib.sha256(material).hexdigest()[:24]
    return f"intraday-{run.session_date.isoformat()}-{digest}"


def _control_operation_id(
    *,
    symbol: str,
    action: CandidateControlAction,
    occurred_at: datetime,
    reason_code: str,
    cooling_until: datetime | None,
) -> str:
    material = "|".join(
        (
            symbol.strip().upper(),
            action.value,
            occurred_at.isoformat(),
            reason_code.strip(),
            "none" if cooling_until is None else cooling_until.isoformat(),
        )
    ).encode()
    return f"{action.value}-{hashlib.sha256(material).hexdigest()[:32]}"
