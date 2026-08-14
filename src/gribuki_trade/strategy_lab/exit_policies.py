"""退出策略使用的可审计、仅限研究的参数与追踪契约。

本模块不提供在线调参器，也不对统计显著性作出承诺。有限搜索空间必须事先显式
登记，使每一组止损、止盈和最长持有期组合都能进入试验账本。
"""

from __future__ import annotations

import hashlib
import itertools
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum


@dataclass(frozen=True, slots=True)
class ExitPolicyParameters:
    policy_version: str
    atr_stop_multiple: Decimal
    structure_buffer_atr: Decimal
    reward_to_risk: Decimal
    maximum_holding_sessions: int
    trailing_atr_multiple: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "policy_version",
            _identifier(self.policy_version, "policy_version"),
        )
        for name in (
            "atr_stop_multiple",
            "structure_buffer_atr",
            "reward_to_risk",
        ):
            object.__setattr__(self, name, _positive_decimal(getattr(self, name), name))
        if self.trailing_atr_multiple is not None:
            object.__setattr__(
                self,
                "trailing_atr_multiple",
                _positive_decimal(self.trailing_atr_multiple, "trailing_atr_multiple"),
            )
        if (
            isinstance(self.maximum_holding_sessions, bool)
            or not isinstance(self.maximum_holding_sessions, int)
            or self.maximum_holding_sessions < 1
        ):
            raise ValueError("maximum_holding_sessions must be a positive integer")

    @property
    def fingerprint(self) -> str:
        return _sha256_document(_parameter_document(self))


@dataclass(frozen=True, slots=True)
class ExitPolicySearchSpace:
    """有限参数登记表；候选生成绝不会静默截断。"""

    search_space_id: str
    policy_version: str
    atr_stop_multiples: tuple[Decimal, ...]
    structure_buffer_atr_values: tuple[Decimal, ...]
    reward_to_risk_values: tuple[Decimal, ...]
    maximum_holding_sessions_values: tuple[int, ...]
    trailing_atr_multiples: tuple[Decimal | None, ...] = (None,)
    max_candidates: int = 10_000

    def __post_init__(self) -> None:
        for name in ("search_space_id", "policy_version"):
            object.__setattr__(self, name, _identifier(getattr(self, name), name))
        for name in (
            "atr_stop_multiples",
            "structure_buffer_atr_values",
            "reward_to_risk_values",
        ):
            values = getattr(self, name)
            _require_unique_nonempty(values, name)
            if any(
                not isinstance(value, Decimal) or not value.is_finite() or value <= 0
                for value in values
            ):
                raise ValueError(f"{name} must contain positive finite Decimals")
        _require_unique_nonempty(
            self.maximum_holding_sessions_values,
            "maximum_holding_sessions_values",
        )
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 1
            for value in self.maximum_holding_sessions_values
        ):
            raise ValueError("holding-session choices must be positive integers")
        _require_unique_nonempty(self.trailing_atr_multiples, "trailing_atr_multiples")
        if any(
            value is not None
            and (
                not isinstance(value, Decimal)
                or not value.is_finite()
                or value <= 0
            )
            for value in self.trailing_atr_multiples
        ):
            raise ValueError("trailing ATR choices must be None or positive Decimals")
        if (
            isinstance(self.max_candidates, bool)
            or not isinstance(self.max_candidates, int)
            or self.max_candidates < 1
        ):
            raise ValueError("max_candidates must be a positive integer")
        if self.candidate_count > self.max_candidates:
            raise ValueError("exit-policy search space exceeds max_candidates")

    @property
    def candidate_count(self) -> int:
        return (
            len(self.atr_stop_multiples)
            * len(self.structure_buffer_atr_values)
            * len(self.reward_to_risk_values)
            * len(self.maximum_holding_sessions_values)
            * len(self.trailing_atr_multiples)
        )

    @property
    def manifest_sha256(self) -> str:
        return _sha256_document(
            {
                "atr_stop_multiples": [str(value) for value in self.atr_stop_multiples],
                "max_candidates": self.max_candidates,
                "maximum_holding_sessions_values": list(
                    self.maximum_holding_sessions_values
                ),
                "policy_version": self.policy_version,
                "reward_to_risk_values": [
                    str(value) for value in self.reward_to_risk_values
                ],
                "search_space_id": self.search_space_id,
                "structure_buffer_atr_values": [
                    str(value) for value in self.structure_buffer_atr_values
                ],
                "trailing_atr_multiples": [
                    None if value is None else str(value)
                    for value in self.trailing_atr_multiples
                ],
            }
        )


def generate_exit_policy_candidates(
    search_space: ExitPolicySearchSpace,
) -> tuple[ExitPolicyParameters, ...]:
    """以稳定顺序生成每一组已登记参数组合。"""

    candidates = tuple(
        ExitPolicyParameters(
            policy_version=search_space.policy_version,
            atr_stop_multiple=atr,
            structure_buffer_atr=buffer,
            reward_to_risk=reward,
            maximum_holding_sessions=holding,
            trailing_atr_multiple=trailing,
        )
        for atr, buffer, reward, holding, trailing in itertools.product(
            search_space.atr_stop_multiples,
            search_space.structure_buffer_atr_values,
            search_space.reward_to_risk_values,
            search_space.maximum_holding_sessions_values,
            search_space.trailing_atr_multiples,
        )
    )
    if len(candidates) != search_space.candidate_count:
        raise AssertionError("exit-policy candidate enumeration is incomplete")
    return tuple(sorted(candidates, key=lambda candidate: candidate.fingerprint))


class ExitTraceBarrier(StrEnum):
    NONE = "NONE"
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"
    TIME = "TIME"


class ExitTraceExecution(StrEnum):
    NOT_REQUESTED = "NOT_REQUESTED"
    T1_BLOCKED = "T1_BLOCKED"
    FILLED = "FILLED"
    NO_FILL_LIMIT_LOCKED = "NO_FILL_LIMIT_LOCKED"
    NO_FILL_SUSPENDED = "NO_FILL_SUSPENDED"
    NO_FILL_OTHER = "NO_FILL_OTHER"


@dataclass(frozen=True, slots=True)
class ExitPolicyTrace:
    """一条时点正确的重放记录；门槛观察与成交结果彼此分离。"""

    trace_id: str
    observation_id: str
    symbol: str
    plan_id: str
    parameter_fingerprint: str
    decision_at: datetime
    entry_at: datetime
    entry_price: Decimal
    stop_price: Decimal
    take_profit_price: Decimal
    time_exit_at: datetime
    barrier: ExitTraceBarrier
    barrier_observed_at: datetime | None
    execution: ExitTraceExecution
    execution_at: datetime | None
    execution_price: Decimal | None
    both_price_barriers_touched: bool
    source_revisions: tuple[tuple[str, str], ...]
    warning_codes: tuple[str, ...] = ()
    research_only: bool = True

    def __post_init__(self) -> None:
        for name in ("trace_id", "observation_id", "plan_id"):
            object.__setattr__(self, name, _identifier(getattr(self, name), name))
        if not _SHA256.fullmatch(self.parameter_fingerprint):
            raise ValueError("parameter_fingerprint must be a lowercase SHA-256 digest")
        symbol = self.symbol.strip().upper()
        if (
            len(symbol) != 9
            or symbol[6:] not in {".SH", ".SZ", ".BJ"}
            or not symbol[:6].isdigit()
        ):
            raise ValueError("symbol must use canonical 600000.SH form")
        object.__setattr__(self, "symbol", symbol)
        decision_at = _aware_utc(self.decision_at, "decision_at")
        entry_at = _aware_utc(self.entry_at, "entry_at")
        time_exit_at = _aware_utc(self.time_exit_at, "time_exit_at")
        if entry_at < decision_at:
            raise ValueError("entry_at must not precede decision_at")
        if time_exit_at <= entry_at:
            raise ValueError("time_exit_at must be after entry_at")
        object.__setattr__(self, "decision_at", decision_at)
        object.__setattr__(self, "entry_at", entry_at)
        object.__setattr__(self, "time_exit_at", time_exit_at)
        for name in ("entry_price", "stop_price", "take_profit_price"):
            object.__setattr__(self, name, _positive_decimal(getattr(self, name), name))
        if not self.stop_price < self.take_profit_price:
            raise ValueError("stop_price must be below take_profit_price")
        if self.take_profit_price <= self.entry_price:
            raise ValueError("take_profit_price must be above entry_price")
        object.__setattr__(self, "barrier", ExitTraceBarrier(self.barrier))
        object.__setattr__(self, "execution", ExitTraceExecution(self.execution))
        if self.both_price_barriers_touched and self.barrier is not ExitTraceBarrier.STOP_LOSS:
            raise ValueError("ambiguous OHLC must use the conservative stop-first outcome")
        if (self.barrier is ExitTraceBarrier.NONE) != (self.barrier_observed_at is None):
            raise ValueError("barrier and barrier_observed_at must agree")
        if (
            self.barrier is ExitTraceBarrier.NONE
            and self.execution is not ExitTraceExecution.NOT_REQUESTED
        ):
            raise ValueError("execution outcomes require an observed exit barrier")
        if self.barrier_observed_at is not None:
            barrier_at = _aware_utc(self.barrier_observed_at, "barrier_observed_at")
            if barrier_at < entry_at:
                raise ValueError("barrier observation must not precede entry")
            object.__setattr__(self, "barrier_observed_at", barrier_at)
        has_execution = self.execution is ExitTraceExecution.FILLED
        if has_execution:
            if self.execution_at is None or self.execution_price is None:
                raise ValueError("FILLED requires an execution timestamp and price")
        elif self.execution_at is not None or self.execution_price is not None:
            raise ValueError("only FILLED may carry execution details")
        if self.execution_at is not None:
            execution_at = _aware_utc(self.execution_at, "execution_at")
            if self.barrier_observed_at is None or execution_at < self.barrier_observed_at:
                raise ValueError("execution must not precede its barrier observation")
            object.__setattr__(self, "execution_at", execution_at)
        if self.execution_price is not None:
            object.__setattr__(
                self,
                "execution_price",
                _positive_decimal(self.execution_price, "execution_price"),
            )
        if self.execution is ExitTraceExecution.T1_BLOCKED and (
            self.execution_at is not None or self.execution_price is not None
        ):
            raise ValueError("a T+1-blocked trace cannot claim an execution")
        revisions = tuple(sorted(self.source_revisions))
        if not revisions or len({name for name, _ in revisions}) != len(revisions):
            raise ValueError("source_revisions must be non-empty and uniquely named")
        for source_id, revision in revisions:
            _identifier(source_id, "source ID")
            _identifier(revision, "source revision")
        warnings = tuple(_identifier(value, "warning code") for value in self.warning_codes)
        if len(warnings) != len(set(warnings)):
            raise ValueError("warning_codes must be unique")
        if self.research_only is not True:
            raise ValueError("exit-policy traces are research only")
        object.__setattr__(self, "source_revisions", revisions)
        object.__setattr__(self, "warning_codes", warnings)


def _parameter_document(parameters: ExitPolicyParameters) -> dict[str, object]:
    return {
        "atr_stop_multiple": str(parameters.atr_stop_multiple),
        "maximum_holding_sessions": parameters.maximum_holding_sessions,
        "policy_version": parameters.policy_version,
        "reward_to_risk": str(parameters.reward_to_risk),
        "structure_buffer_atr": str(parameters.structure_buffer_atr),
        "trailing_atr_multiple": (
            None
            if parameters.trailing_atr_multiple is None
            else str(parameters.trailing_atr_multiple)
        ),
    }


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    normalized = value.strip()
    if _SAFE_IDENTIFIER.fullmatch(normalized) is None:
        raise ValueError(f"{name} must be a safe non-empty identifier")
    return normalized


def _positive_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return value


def _require_unique_nonempty(values: tuple[object, ...], name: str) -> None:
    if not values or len(values) != len(set(values)):
        raise ValueError(f"{name} must be non-empty and unique")


def _aware_utc(value: object, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _sha256_document(document: object) -> str:
    encoded = json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "ExitPolicyParameters",
    "ExitPolicySearchSpace",
    "ExitPolicyTrace",
    "ExitTraceBarrier",
    "ExitTraceExecution",
    "generate_exit_policy_candidates",
]
