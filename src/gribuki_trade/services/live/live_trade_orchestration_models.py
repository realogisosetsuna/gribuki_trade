"""实盘保护编排使用的错误、输入和工作结果模型。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

from gribuki_trade.domain.live_records import (
    ConfirmedLiveFill,
)
from gribuki_trade.features.deep_exit_planning import DeepExitTimeframe, DeepSemanticAssessment
from gribuki_trade.features.exit_planning import QuickExitPlanConfig
from gribuki_trade.features.technical import TechnicalBar
from gribuki_trade.services.exit.exit_plan_lifecycle import ExitBarrierObservation


class LiveProtectionInputError(RuntimeError):
    """保护分析输入暂不可用或永久无效。"""

    def __init__(self, code: str, *, retryable: bool) -> None:
        normalized = "_".join(str(code).strip().upper().split())
        self.code = normalized or "UNKNOWN"
        self.retryable = retryable
        super().__init__(f"live protection input failed ({self.code})")


@dataclass(frozen=True, slots=True)
class LiveProtectionInputs:
    """同一证据时点冻结的 QUICK 与 DEEP 输入。"""

    decision_at: datetime
    bars: tuple[TechnicalBar, ...]
    technical_invalidation_price: Decimal
    time_exit_at: datetime
    strategy_version: str
    deep_timeframes: tuple[DeepExitTimeframe, ...]
    baseline_assessment: DeepSemanticAssessment | None = None
    adversarial_assessment: DeepSemanticAssessment | None = None
    quick_config: QuickExitPlanConfig | None = None

    def __post_init__(self) -> None:
        decision = _aware_utc(self.decision_at)
        time_exit = _aware_utc(self.time_exit_at)
        if not self.bars:
            raise ValueError("QUICK planning bars must not be empty")
        if not self.deep_timeframes:
            raise ValueError("DEEP planning timeframes must not be empty")
        if time_exit <= decision:
            raise ValueError("time exit must follow the decision")
        if (
            not isinstance(self.technical_invalidation_price, Decimal)
            or not self.technical_invalidation_price.is_finite()
            or self.technical_invalidation_price <= 0
        ):
            raise ValueError("technical invalidation price must be positive")
        if not self.strategy_version.strip():
            raise ValueError("strategy_version must not be empty")
        if (self.baseline_assessment is None) is not (self.adversarial_assessment is None):
            raise ValueError("both DEEP semantic assessments must be present or absent")
        object.__setattr__(self, "decision_at", decision)
        object.__setattr__(self, "time_exit_at", time_exit)


class LiveProtectionInputProvider(Protocol):
    """冻结行情和双轨 LLM 证据的生产端口。"""

    async def prepare(
        self,
        fill: ConfirmedLiveFill,
        *,
        requested_at: datetime,
    ) -> LiveProtectionInputs: ...

    async def assess_deep(
        self,
        fill: ConfirmedLiveFill,
        *,
        inputs: LiveProtectionInputs,
    ) -> tuple[DeepSemanticAssessment, DeepSemanticAssessment]: ...


@dataclass(frozen=True, slots=True)
class LiveWorkRunSummary:
    """一次有限工作轮询的可审计结果。"""

    claimed: int
    completed: int
    retried: int
    dead: int
    completed_work_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class LiveTrackingObservation:
    """一个账户/标的在一根完整 K 线上的全部批次观察结果。"""

    account_id: str
    symbol: str
    observations: tuple[ExitBarrierObservation, ...]
    queued_alert_work_ids: tuple[str, ...]


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)
