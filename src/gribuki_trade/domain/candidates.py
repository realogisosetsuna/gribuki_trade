"""具有时点约束的候选标的全集领域模型。

候选标的只是研究输入，绝不是订单、资金配置或交易授权。模型刻意保留每一条发现来源，
以便合并由多个独立扫描器发现的同一标的而不丢失血缘。
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import IntEnum, StrEnum

_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/+\-]{0,127}$")


class CandidateSource(StrEnum):
    """标的进入研究全集的受支持方式。"""

    MANUAL = "manual"
    CLOSE_SCREEN = "close_screen"
    INTRADAY_ANOMALY = "intraday_anomaly"
    STRATEGY = "strategy"
    REVIEW = "review"


class CandidatePriority(IntEnum):
    """粗粒度调度优先级；并非仓位规模建议。"""

    LOW = 10
    NORMAL = 20
    HIGH = 30
    URGENT = 40


class CandidateStatus(StrEnum):
    """在特定时点投影出的生命周期状态。"""

    ACTIVE = "active"
    COOLING = "cooling"
    EXPIRED = "expired"
    REMOVED = "removed"


class CandidateControlAction(StrEnum):
    """保留在仅追加审计日志中的显式生命周期操作。"""

    COOL = "cool"
    ACTIVATE = "activate"
    REMOVE = "remove"


@dataclass(frozen=True, slots=True)
class CandidateObservation:
    """一次来源运行产生的一条不可变发现。

    ``discovered_at`` 表示上游信号声称发现候选标的的时间；``observed_at`` 表示
    本系统首次知晓该信号的时间，因此是时点回放采用的时间戳。
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
        """一个来源运行/标的组合的稳定幂等标识。"""

        material = (
            f"candidate-observation@1\0{self.symbol}\0{self.source.value}"
            f"\0{self.source_run_id}"
        ).encode()
        return hashlib.sha256(material).hexdigest()


@dataclass(frozen=True, slots=True)
class CandidateControlEvent:
    """一条显式的冷却、激活或移除指令。"""

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
    """一条已保留发现事件的读取模型。"""

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
    """由不可变事件在 ``as_of`` 时点投影出的合并候选状态。"""

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
        """研究监控当前是否可以纳入该标的。"""

        return self.status is CandidateStatus.ACTIVE


@dataclass(frozen=True, slots=True)
class CandidateAuditRecord:
    """候选事件日志中一条不可变记录的公开元数据。"""

    sequence: int
    event_id: str
    symbol: str
    event_type: str
    effective_at: datetime
    recorded_at: datetime
    payload_sha256: str


def canonical_ashare_symbol(symbol: str) -> str:
    """返回带交易所后缀的规范六位 A 股/ETF 代码。"""

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
