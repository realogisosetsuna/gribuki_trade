"""退出计划生命周期的异常、存储协议和结果模型。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from gribuki_trade.domain.exit_plans import (
    ExitBarrierKind,
    ExitPlan,
    ExitPlanEvent,
    NewExitPlanEvent,
)
from gribuki_trade.features.deep_exit_planning import (
    DeepExitPlanResult,
)
from gribuki_trade.features.exit_planning import QuickExitPlanResult


class ExitPlanLifecycleError(RuntimeError):
    """退出计划编排失败的基类。"""


class ExitPlanNotFoundError(ExitPlanLifecycleError):
    """请求的保护流中没有持久计划。"""


class ExitPlanLifecycleConflictError(ExitPlanLifecycleError):
    """已存储的生命周期状态与请求命令冲突。"""


class ExitPlanEventStore(Protocol):
    """本应用服务所需的最小只追加存储契约。"""

    def append(
        self,
        event: NewExitPlanEvent,
        *,
        expected_sequence: int,
    ) -> tuple[ExitPlanEvent, bool]: ...

    def events(self, protection_id: str) -> tuple[ExitPlanEvent, ...]: ...

    def event_by_idempotency_key(
        self,
        protection_id: str,
        idempotency_key: str,
    ) -> ExitPlanEvent | None: ...

    def protection_ids(self, account_id: str, symbol: str) -> tuple[str, ...]: ...


@dataclass(frozen=True, slots=True)
class QuickExitPlanCreation:
    """确定性 QUICK 规划及持久创建的结果。"""

    result: QuickExitPlanResult
    event: ExitPlanEvent
    created: bool


@dataclass(frozen=True, slots=True)
class PostFillExitPlanEvents:
    """成交后附着事件及一次性 DEEP 分析请求。"""

    active_plan: ExitPlan
    attachment_event: ExitPlanEvent
    deep_request_event: ExitPlanEvent
    attachment_created: bool
    deep_request_created: bool


@dataclass(frozen=True, slots=True)
class DeepExitPlanApplication:
    """校验并尝试不可变 DEEP 替换的结果。"""

    active_plan: ExitPlan
    event: ExitPlanEvent
    applied: bool
    failure_reason: str | None


@dataclass(frozen=True, slots=True)
class DeepExitPlanBuildApplication:
    """DEEP 生成器的完整结果及其持久化替换结果。"""

    result: DeepExitPlanResult
    application: DeepExitPlanApplication


@dataclass(frozen=True, slots=True)
class ExitBarrierObservation:
    """一次完整 K 线评估，不包含任何订单副作用。"""

    active_plan: ExitPlan
    selected_barrier: ExitBarrierKind | None
    crossed_barriers: tuple[ExitBarrierKind, ...]
    barrier_event: ExitPlanEvent | None
    signal_event: ExitPlanEvent | None
    sellable_quantity: int
    execution_handoff_required: bool
    suppression_reason: str | None

    @property
    def order_created(self) -> bool:
        """始终为 false：本服务不依赖券商或订单存储。"""

        return False
