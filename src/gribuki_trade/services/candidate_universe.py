"""Unified candidate-universe orchestration for research workflows.

This service accepts discoveries from manual selection and scanners, applies
explicit TTL policy, and exposes a tracked symbol set.  It has deliberately no
dependency on orders, accounts, brokers, or execution services.
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
    """Storage boundary needed by the candidate-universe service."""

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
    """The requested lifecycle transition is not meaningful at that time."""


@dataclass(frozen=True, slots=True)
class CandidateDiscovery:
    """Unpersisted discovery submitted by one scanner or human workflow."""

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
    """Result of an idempotent mutation and the resulting point-in-time state."""

    candidate: CandidateRecord
    appended: bool


@dataclass(frozen=True, slots=True)
class CandidateUniversePolicy:
    """Initial, explicit TTL and priority policy for every discovery source."""

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
    """Merge candidate discoveries while preserving immutable provenance."""

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
        """Idempotently add one source-run discovery to the merged universe."""

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
        """Upsert a bounded caller-owned batch in deterministic input order."""

        return tuple(self.upsert(item) for item in discoveries)

    def ingest_close_screening(
        self,
        run: AShareScreeningRun,
        *,
        expires_at: datetime | None = None,
    ) -> tuple[CandidateMutation, ...]:
        """Promote one deterministic screen's Top-N into research candidates."""

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
        """Promote an intraday anomaly ranking into short-lived candidates."""

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
        """Temporarily suppress active monitoring without losing provenance."""

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
        """Explicitly clear removal/cooling; expired provenance stays expired."""

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
        """Explicitly tombstone a symbol until a later explicit activation."""

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
        """Return scheduling inputs; cooling records are opt-in diagnostics."""

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
