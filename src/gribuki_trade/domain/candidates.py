"""Point-in-time candidate-universe domain models.

A candidate is a research input, never an order, allocation, or authorization to
trade.  The models deliberately retain every discovery provenance so that a
symbol found by several independent scanners is merged without losing lineage.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import IntEnum, StrEnum

_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/+\-]{0,127}$")


class CandidateSource(StrEnum):
    """Supported ways for a symbol to enter the research universe."""

    MANUAL = "manual"
    CLOSE_SCREEN = "close_screen"
    INTRADAY_ANOMALY = "intraday_anomaly"
    STRATEGY = "strategy"
    REVIEW = "review"


class CandidatePriority(IntEnum):
    """Coarse scheduling priority; it is not a position-size recommendation."""

    LOW = 10
    NORMAL = 20
    HIGH = 30
    URGENT = 40


class CandidateStatus(StrEnum):
    """Projected lifecycle state at a specific point in time."""

    ACTIVE = "active"
    COOLING = "cooling"
    EXPIRED = "expired"
    REMOVED = "removed"


class CandidateControlAction(StrEnum):
    """Explicit lifecycle actions retained in the append-only audit log."""

    COOL = "cool"
    ACTIVATE = "activate"
    REMOVE = "remove"


@dataclass(frozen=True, slots=True)
class CandidateObservation:
    """One immutable discovery made by one source run.

    ``discovered_at`` describes when the upstream signal says the candidate was
    discovered. ``observed_at`` is when this system first knew that signal and
    is therefore the timestamp used by point-in-time replay.
    """

    symbol: str
    source: CandidateSource
    source_run_id: str
    discovered_at: datetime
    observed_at: datetime
    expires_at: datetime | None
    priority: CandidatePriority = CandidatePriority.NORMAL
    reason_codes: tuple[str, ...] = ()
    evidence_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.source, CandidateSource):
            raise ValueError("source must be a CandidateSource")
        if not isinstance(self.priority, CandidatePriority):
            raise ValueError("priority must be a CandidatePriority")
        symbol = canonical_ashare_symbol(self.symbol)
        run_id = self.source_run_id.strip()
        if not _RUN_ID.fullmatch(run_id):
            raise ValueError("source_run_id must be a safe non-empty identifier")
        discovered_at = _utc(self.discovered_at, "discovered_at")
        observed_at = _utc(self.observed_at, "observed_at")
        expires_at = (
            None if self.expires_at is None else _utc(self.expires_at, "expires_at")
        )
        if discovered_at > observed_at:
            raise ValueError("discovered_at must not follow observed_at")
        if expires_at is not None and expires_at <= discovered_at:
            raise ValueError("expires_at must follow discovered_at")
        reasons = _tokens(self.reason_codes, "reason_codes", required=True)
        evidence = _tokens(self.evidence_ids, "evidence_ids", required=False)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "source_run_id", run_id)
        object.__setattr__(self, "discovered_at", discovered_at)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "evidence_ids", evidence)

    @property
    def observation_id(self) -> str:
        """Stable idempotency identity for one source-run/symbol pair."""

        material = (
            f"candidate-observation@1\0{self.symbol}\0{self.source.value}"
            f"\0{self.source_run_id}"
        ).encode()
        return hashlib.sha256(material).hexdigest()


@dataclass(frozen=True, slots=True)
class CandidateControlEvent:
    """One explicit cooling, activation, or removal instruction."""

    symbol: str
    action: CandidateControlAction
    operation_id: str
    occurred_at: datetime
    reason_code: str
    cooling_until: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.action, CandidateControlAction):
            raise ValueError("action must be a CandidateControlAction")
        symbol = canonical_ashare_symbol(self.symbol)
        operation_id = self.operation_id.strip()
        if not _RUN_ID.fullmatch(operation_id):
            raise ValueError("operation_id must be a safe non-empty identifier")
        occurred_at = _utc(self.occurred_at, "occurred_at")
        cooling_until = (
            None
            if self.cooling_until is None
            else _utc(self.cooling_until, "cooling_until")
        )
        reason_code = self.reason_code.strip()
        if not reason_code:
            raise ValueError("reason_code must not be empty")
        if self.action is CandidateControlAction.COOL:
            if cooling_until is None or cooling_until <= occurred_at:
                raise ValueError("cooling_until must follow a cooling event")
        elif cooling_until is not None:
            raise ValueError("cooling_until is only valid for a cooling event")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "operation_id", operation_id)
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "reason_code", reason_code)
        object.__setattr__(self, "cooling_until", cooling_until)

    @property
    def event_id(self) -> str:
        material = (
            f"candidate-control@1\0{self.symbol}\0{self.operation_id}"
        ).encode()
        return hashlib.sha256(material).hexdigest()


@dataclass(frozen=True, slots=True)
class CandidateProvenance:
    """Read model for one retained discovery event."""

    observation_id: str
    source: CandidateSource
    source_run_id: str
    discovered_at: datetime
    observed_at: datetime
    expires_at: datetime | None
    priority: CandidatePriority
    reason_codes: tuple[str, ...]
    evidence_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CandidateRecord:
    """Merged candidate state projected at ``as_of`` from immutable events."""

    symbol: str
    as_of: datetime
    status: CandidateStatus
    priority: CandidatePriority
    discovered_at: datetime
    first_observed_at: datetime
    last_observed_at: datetime
    expires_at: datetime | None
    cooling_until: datetime | None
    reason_codes: tuple[str, ...]
    evidence_ids: tuple[str, ...]
    sources: tuple[CandidateSource, ...]
    provenance: tuple[CandidateProvenance, ...]

    @property
    def is_trackable(self) -> bool:
        """Whether research monitoring may include this symbol right now."""

        return self.status is CandidateStatus.ACTIVE


@dataclass(frozen=True, slots=True)
class CandidateAuditRecord:
    """Public metadata for one immutable row in the candidate event log."""

    sequence: int
    event_id: str
    symbol: str
    event_type: str
    effective_at: datetime
    recorded_at: datetime
    payload_sha256: str


def canonical_ashare_symbol(symbol: str) -> str:
    """Return the canonical six-digit A-share/ETF symbol with exchange suffix."""

    value = symbol.strip().upper()
    if len(value) == 6 and value.isdigit():
        if value.startswith(("4", "8")):
            exchange = "BJ"
        elif value.startswith(("5", "6", "9")):
            exchange = "SH"
        else:
            exchange = "SZ"
        return f"{value}.{exchange}"
    if len(value) == 9 and value[6] == ".":
        code, exchange = value.split(".", maxsplit=1)
        if code.isdigit() and exchange in {"SH", "SZ", "BJ"}:
            return value
    raise ValueError("symbol must look like 600000.SH, 000001.SZ, or 430047.BJ")


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _tokens(
    values: tuple[str, ...],
    field_name: str,
    *,
    required: bool,
) -> tuple[str, ...]:
    normalized = tuple(sorted(item.strip() for item in values))
    if required and not normalized:
        raise ValueError(f"{field_name} must not be empty")
    if any(not item for item in normalized):
        raise ValueError(f"{field_name} must not contain blank values")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} must be unique")
    return normalized
