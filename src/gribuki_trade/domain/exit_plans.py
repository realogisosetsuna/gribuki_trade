"""不可变保护退出计划及其只追加审计事件。

本模块中的对象有意与执行解耦。价格门槛只是决策触发器，并不能证明卖出能够
成交。尤其是当日买入的 A 股即使越过门槛，其数量仍可能受 T+1 限制而不可卖。
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class ExitPlanDepth(StrEnum):
    """生成一个计划版本时采用的分析深度。"""

    QUICK = "QUICK"
    DEEP = "DEEP"


class ExitPlanState(StrEnum):
    """一个不可变计划版本的证据状态。"""

    PROVISIONAL = "PROVISIONAL"
    CONFIRMED = "CONFIRMED"
    DEGRADED = "DEGRADED"


class ExitBarrierKind(StrEnum):
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"
    TIME = "TIME"


class ExitPlanEventType(StrEnum):
    """供后续编排与重放使用的稳定事件词汇。"""

    PLAN_CREATED = "PLAN_CREATED"
    PLAN_REPLACED = "PLAN_REPLACED"
    PLAN_ATTACHED_TO_FILL = "PLAN_ATTACHED_TO_FILL"
    DEEP_ANALYSIS_REQUESTED = "DEEP_ANALYSIS_REQUESTED"
    DEEP_ANALYSIS_FAILED = "DEEP_ANALYSIS_FAILED"
    BARRIER_OBSERVED = "BARRIER_OBSERVED"
    EXIT_SIGNAL_RAISED = "EXIT_SIGNAL_RAISED"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    PARTIAL_FILL_APPLIED = "PARTIAL_FILL_APPLIED"
    POSITION_CLOSED = "POSITION_CLOSED"


@dataclass(frozen=True, slots=True)
class ExitPlan:
    """一份不可变的多头持仓保护计划。

    ``entry_basis_price`` 是成交前风险定仓采用的最坏价格，通常取 BUY 限价而非
    信号价。后续版本会冻结该基准及 ``initial_risk_per_share``。DEEP 版本可以
    把止损上移至入场价之上以保护利润，但计划替换绝不能下移止损或延长时间门槛。
    """

    plan_id: str
    protection_id: str
    account_id: str
    symbol: str
    version: int
    depth: ExitPlanDepth
    state: ExitPlanState
    decision_at: datetime
    market_data_as_of: datetime
    time_exit_at: datetime
    entry_basis_price: Decimal
    stop_price: Decimal
    take_profit_price: Decimal
    initial_risk_per_share: Decimal
    reward_to_risk: Decimal
    price_tick: Decimal
    technical_invalidation_price: Decimal | None
    feature_snapshot_sha256: str
    policy_version: str
    strategy_version: str
    calibration_id: str
    reason_codes: tuple[str, ...]
    metrics: tuple[tuple[str, Decimal], ...]
    evidence_ids: tuple[str, ...] = ()
    supersedes_plan_id: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "plan_id",
            "protection_id",
            "account_id",
            "policy_version",
            "strategy_version",
            "calibration_id",
        ):
            object.__setattr__(self, name, _identifier(getattr(self, name), name))
        symbol = _symbol(self.symbol)
        object.__setattr__(self, "symbol", symbol)
        if isinstance(self.version, bool) or not isinstance(self.version, int):
            raise TypeError("version must be an integer")
        if self.version < 1:
            raise ValueError("version must be positive")
        object.__setattr__(self, "depth", ExitPlanDepth(self.depth))
        object.__setattr__(self, "state", ExitPlanState(self.state))

        decision_at = _aware_utc(self.decision_at, "decision_at")
        market_data_as_of = _aware_utc(self.market_data_as_of, "market_data_as_of")
        time_exit_at = _aware_utc(self.time_exit_at, "time_exit_at")
        if market_data_as_of > decision_at:
            raise ValueError("market_data_as_of must not exceed decision_at")
        if time_exit_at <= decision_at:
            raise ValueError("time_exit_at must be strictly after decision_at")
        object.__setattr__(self, "decision_at", decision_at)
        object.__setattr__(self, "market_data_as_of", market_data_as_of)
        object.__setattr__(self, "time_exit_at", time_exit_at)

        for name in (
            "entry_basis_price",
            "stop_price",
            "take_profit_price",
            "initial_risk_per_share",
            "reward_to_risk",
            "price_tick",
        ):
            object.__setattr__(self, name, _positive_decimal(getattr(self, name), name))
        if self.technical_invalidation_price is not None:
            object.__setattr__(
                self,
                "technical_invalidation_price",
                _positive_decimal(
                    self.technical_invalidation_price,
                    "technical_invalidation_price",
                ),
            )
        if self.stop_price >= self.take_profit_price:
            raise ValueError("stop_price must be below take_profit_price")
        if self.take_profit_price <= self.entry_basis_price:
            raise ValueError("take_profit_price must be above entry_basis_price")
        if self.initial_risk_per_share >= self.entry_basis_price:
            raise ValueError("initial_risk_per_share must be below entry_basis_price")
        for name in (
            "entry_basis_price",
            "stop_price",
            "take_profit_price",
        ):
            if not _is_tick_aligned(getattr(self, name), self.price_tick):
                raise ValueError(f"{name} must align to price_tick")
        if self.depth is ExitPlanDepth.QUICK:
            if self.state is not ExitPlanState.PROVISIONAL:
                raise ValueError("a QUICK plan must be PROVISIONAL")
            if self.supersedes_plan_id is not None or self.version != 1:
                raise ValueError("a QUICK plan must be the first unsuperseded version")
            if self.stop_price >= self.entry_basis_price:
                raise ValueError("a QUICK stop must be below its entry basis")
            if self.initial_risk_per_share != self.entry_basis_price - self.stop_price:
                raise ValueError("QUICK initial risk must equal entry basis minus stop")
        else:
            if self.state is ExitPlanState.PROVISIONAL:
                raise ValueError("a DEEP plan must be CONFIRMED or DEGRADED")
            if self.version < 2 or self.supersedes_plan_id is None:
                raise ValueError("a DEEP plan must supersede an earlier version")
            object.__setattr__(
                self,
                "supersedes_plan_id",
                _identifier(self.supersedes_plan_id, "supersedes_plan_id"),
            )

        planned_reward = self.take_profit_price - self.entry_basis_price
        configured_reward = self.initial_risk_per_share * self.reward_to_risk
        if abs(planned_reward - configured_reward) >= self.price_tick:
            raise ValueError("take-profit price conflicts with reward_to_risk")

        if not _SHA256.fullmatch(self.feature_snapshot_sha256):
            raise ValueError("feature_snapshot_sha256 must be a lowercase SHA-256 digest")
        reasons = _unique_identifiers(self.reason_codes, "reason_codes")
        if not reasons:
            raise ValueError("reason_codes must not be empty")
        evidence = _unique_identifiers(self.evidence_ids, "evidence_ids")
        normalized_metrics = tuple(sorted(self.metrics))
        if len({name for name, _ in normalized_metrics}) != len(normalized_metrics):
            raise ValueError("metrics must have unique names")
        for name, value in normalized_metrics:
            _identifier(name, "metric name")
            _finite_decimal(value, f"metric {name}")
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "evidence_ids", evidence)
        object.__setattr__(self, "metrics", normalized_metrics)


def validate_exit_plan_replacement(previous: ExitPlan, replacement: ExitPlan) -> None:
    """除非 ``replacement`` 是风险单调收紧的下一版本，否则封闭拒绝。"""

    identity_fields = ("protection_id", "account_id", "symbol", "entry_basis_price")
    if any(getattr(previous, name) != getattr(replacement, name) for name in identity_fields):
        raise ValueError("replacement must retain protection, account, symbol, and entry basis")
    if replacement.version != previous.version + 1:
        raise ValueError("replacement version must increment by exactly one")
    if replacement.supersedes_plan_id != previous.plan_id:
        raise ValueError("replacement must identify the exact superseded plan")
    if replacement.depth is not ExitPlanDepth.DEEP:
        raise ValueError("only a DEEP plan may replace an active plan")
    if replacement.decision_at < previous.decision_at:
        raise ValueError("replacement decision_at must not move backwards")
    if replacement.market_data_as_of < previous.market_data_as_of:
        raise ValueError("replacement market_data_as_of must not move backwards")
    if replacement.initial_risk_per_share != previous.initial_risk_per_share:
        raise ValueError("replacement must preserve initial risk per share")
    if replacement.price_tick != previous.price_tick:
        raise ValueError("replacement must preserve the price tick")
    if replacement.stop_price < previous.stop_price:
        raise ValueError("replacement must not loosen a long stop")
    if replacement.time_exit_at > previous.time_exit_at:
        raise ValueError("replacement must not extend the time barrier")


def exit_plan_id(
    *,
    protection_id: str,
    version: int,
    feature_snapshot_sha256: str,
    policy_version: str,
) -> str:
    """返回一个计划版本的确定性身份。"""

    protection_id = _identifier(protection_id, "protection_id")
    policy_version = _identifier(policy_version, "policy_version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ValueError("version must be a positive integer")
    if not _SHA256.fullmatch(feature_snapshot_sha256):
        raise ValueError("feature_snapshot_sha256 must be a lowercase SHA-256 digest")
    document = {
        "feature_snapshot_sha256": feature_snapshot_sha256,
        "policy_version": policy_version,
        "protection_id": protection_id,
        "version": version,
    }
    return "exit-plan-" + _sha256_document(document)[:40]


def exit_plan_document(plan: ExitPlan) -> dict[str, object]:
    """返回兼容规范 JSON 的无损审计表示。"""

    return {
        "account_id": plan.account_id,
        "calibration_id": plan.calibration_id,
        "decision_at": plan.decision_at.isoformat(),
        "depth": plan.depth.value,
        "entry_basis_price": str(plan.entry_basis_price),
        "evidence_ids": list(plan.evidence_ids),
        "feature_snapshot_sha256": plan.feature_snapshot_sha256,
        "initial_risk_per_share": str(plan.initial_risk_per_share),
        "market_data_as_of": plan.market_data_as_of.isoformat(),
        "metrics": [[name, str(value)] for name, value in plan.metrics],
        "plan_id": plan.plan_id,
        "policy_version": plan.policy_version,
        "price_tick": str(plan.price_tick),
        "protection_id": plan.protection_id,
        "reason_codes": list(plan.reason_codes),
        "reward_to_risk": str(plan.reward_to_risk),
        "state": plan.state.value,
        "stop_price": str(plan.stop_price),
        "strategy_version": plan.strategy_version,
        "supersedes_plan_id": plan.supersedes_plan_id,
        "symbol": plan.symbol,
        "take_profit_price": str(plan.take_profit_price),
        "technical_invalidation_price": (
            None
            if plan.technical_invalidation_price is None
            else str(plan.technical_invalidation_price)
        ),
        "time_exit_at": plan.time_exit_at.isoformat(),
        "version": plan.version,
    }


@dataclass(frozen=True, slots=True)
class NewExitPlanEvent:
    """一条尚未持久化的退出计划审计事件。"""

    protection_id: str
    account_id: str
    symbol: str
    event_type: ExitPlanEventType
    occurred_at: datetime
    known_at: datetime
    idempotency_key: str
    payload_json: str
    plan_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "protection_id",
            _identifier(self.protection_id, "protection_id"),
        )
        object.__setattr__(self, "account_id", _identifier(self.account_id, "account_id"))
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        object.__setattr__(self, "event_type", ExitPlanEventType(self.event_type))
        occurred_at = _aware_utc(self.occurred_at, "occurred_at")
        known_at = _aware_utc(self.known_at, "known_at")
        if known_at < occurred_at:
            raise ValueError("known_at must not precede occurred_at")
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "known_at", known_at)
        object.__setattr__(
            self,
            "idempotency_key",
            _identifier(self.idempotency_key, "idempotency_key"),
        )
        if self.plan_id is not None:
            object.__setattr__(self, "plan_id", _identifier(self.plan_id, "plan_id"))
        canonical = _canonical_object_json(self.payload_json, "payload_json")
        object.__setattr__(self, "payload_json", canonical)

    @classmethod
    def create(
        cls,
        *,
        protection_id: str,
        account_id: str,
        symbol: str,
        event_type: ExitPlanEventType,
        occurred_at: datetime,
        known_at: datetime,
        idempotency_key: str,
        payload: Mapping[str, object],
        plan_id: str | None = None,
    ) -> NewExitPlanEvent:
        return cls(
            protection_id=protection_id,
            account_id=account_id,
            symbol=symbol,
            event_type=event_type,
            occurred_at=occurred_at,
            known_at=known_at,
            idempotency_key=idempotency_key,
            payload_json=json.dumps(
                _normalize_json(payload),
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ),
            plan_id=plan_id,
        )

    @property
    def event_id(self) -> str:
        return "exit-event-" + hashlib.sha256(
            f"{self.protection_id}\0{self.idempotency_key}".encode()
        ).hexdigest()[:40]


@dataclass(frozen=True, slots=True)
class ExitPlanEvent:
    """一条带有流内防篡改哈希链接的已存储事件。"""

    sequence: int
    event_id: str
    protection_id: str
    account_id: str
    symbol: str
    event_type: ExitPlanEventType
    occurred_at: datetime
    known_at: datetime
    idempotency_key: str
    payload_json: str
    payload_sha256: str
    previous_hash: str | None
    event_hash: str
    plan_id: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int):
            raise TypeError("sequence must be an integer")
        if self.sequence < 1:
            raise ValueError("sequence must be positive")
        # 对所有语义字段和规范 JSON 复用命令校验逻辑。
        command = NewExitPlanEvent(
            protection_id=self.protection_id,
            account_id=self.account_id,
            symbol=self.symbol,
            event_type=self.event_type,
            occurred_at=self.occurred_at,
            known_at=self.known_at,
            idempotency_key=self.idempotency_key,
            payload_json=self.payload_json,
            plan_id=self.plan_id,
        )
        object.__setattr__(self, "event_id", _identifier(self.event_id, "event_id"))
        for name in (
            "protection_id",
            "account_id",
            "symbol",
            "event_type",
            "occurred_at",
            "known_at",
            "idempotency_key",
            "payload_json",
            "plan_id",
        ):
            object.__setattr__(self, name, getattr(command, name))
        if not _SHA256.fullmatch(self.payload_sha256):
            raise ValueError("payload_sha256 must be a lowercase SHA-256 digest")
        if self.previous_hash is not None and not _SHA256.fullmatch(self.previous_hash):
            raise ValueError("previous_hash must be a lowercase SHA-256 digest")
        if not _SHA256.fullmatch(self.event_hash):
            raise ValueError("event_hash must be a lowercase SHA-256 digest")

    @property
    def payload(self) -> dict[str, Any]:
        value = json.loads(self.payload_json)
        if not isinstance(value, dict):  # pragma: no cover - 构造器不变量
            raise TypeError("event payload must be an object")
        return value


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    normalized = value.strip()
    if _SAFE_IDENTIFIER.fullmatch(normalized) is None:
        raise ValueError(f"{name} must be a safe non-empty identifier")
    return normalized


def _symbol(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("symbol must be a string")
    normalized = value.strip().upper()
    if (
        len(normalized) != 9
        or normalized[6:] not in {".SH", ".SZ", ".BJ"}
        or not normalized[:6].isdigit()
    ):
        raise ValueError("symbol must use canonical 600000.SH form")
    return normalized


def _unique_identifiers(values: tuple[str, ...], name: str) -> tuple[str, ...]:
    normalized = tuple(_identifier(value, name) for value in values)
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{name} must be unique")
    return normalized


def _finite_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def _positive_decimal(value: object, name: str) -> Decimal:
    result = _finite_decimal(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _aware_utc(value: object, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _is_tick_aligned(value: Decimal, tick: Decimal) -> bool:
    return value % tick == 0


def _canonical_object_json(value: str, name: str) -> str:
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(f"{name} must be valid JSON") from error
    if not isinstance(decoded, dict):
        raise ValueError(f"{name} must encode an object")
    return json.dumps(
        _normalize_json(decoded),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _normalize_json(value: object) -> object:
    # ``StrEnum`` 同时也是 ``str``；优先处理它，确保规范载荷保存稳定的线协议值，
    # 而不是枚举实例。
    if isinstance(value, StrEnum):
        return value.value
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("event payload decimals must be finite")
        return str(value)
    if isinstance(value, datetime):
        return _aware_utc(value, "payload datetime").isoformat()
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("event payload object keys must be strings")
            normalized[key] = _normalize_json(item)
        return normalized
    if isinstance(value, (tuple, list)):
        return [_normalize_json(item) for item in value]
    raise TypeError(f"unsupported event payload value: {type(value).__name__}")


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
    "ExitBarrierKind",
    "ExitPlan",
    "ExitPlanDepth",
    "ExitPlanEvent",
    "ExitPlanEventType",
    "ExitPlanState",
    "NewExitPlanEvent",
    "exit_plan_document",
    "exit_plan_id",
    "validate_exit_plan_replacement",
]
