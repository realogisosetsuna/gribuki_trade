"""不连接券商的不可变保护退出计划生命周期编排。

本服务有意止步于可审计的退出信号。PAPER 和实盘观察流程都可以使用它，
但不会因此获得创建券商订单的权限。执行编排器只有在完成自身的持仓、价格带、
交易时段和券商校验后，才可以消费返回的执行交接信息。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

from gribuki_trade.domain.exit_plans import (
    ExitBarrierKind,
    ExitPlan,
    ExitPlanDepth,
    ExitPlanEvent,
    ExitPlanEventType,
    ExitPlanState,
    NewExitPlanEvent,
    exit_plan_document,
    validate_exit_plan_replacement,
)
from gribuki_trade.features.deep_exit_planning import (
    DeepExitPlanConfig,
    DeepExitPlanResult,
    DeepExitTimeframe,
    DeepSemanticAssessment,
    build_deep_exit_plan,
)
from gribuki_trade.features.exit_planning import (
    QuickExitPlanConfig,
    QuickExitPlanResult,
    build_quick_exit_plan,
)
from gribuki_trade.features.technical import TechnicalBar

_SCHEMA_VERSION = 1


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


class ExitPlanLifecycleService:
    """在不提交订单的前提下持久化并重放保护退出决策。"""

    def __init__(self, store: ExitPlanEventStore) -> None:
        self._store = store

    def create_quick_plan(
        self,
        *,
        account_id: str,
        protection_id: str,
        symbol: str,
        bars: tuple[TechnicalBar, ...],
        decision_at: datetime,
        time_exit_at: datetime,
        worst_entry_price: Decimal,
        technical_invalidation_price: Decimal,
        strategy_version: str,
        config: QuickExitPlanConfig | None = None,
        known_at: datetime | None = None,
    ) -> QuickExitPlanCreation:
        """构建 QUICK 计划，并仅追加一次其完整规范文档。"""

        result = build_quick_exit_plan(
            account_id=account_id,
            protection_id=protection_id,
            symbol=symbol,
            bars=bars,
            decision_at=decision_at,
            time_exit_at=time_exit_at,
            worst_entry_price=worst_entry_price,
            technical_invalidation_price=technical_invalidation_price,
            strategy_version=strategy_version,
            config=config,
        )
        plan = result.plan
        current_events = self._store.events(plan.protection_id)
        if current_events:
            current = _active_plan(current_events)
            if current != plan:
                raise ExitPlanLifecycleConflictError(
                    "protection stream already contains a different active plan"
                )
        event = NewExitPlanEvent.create(
            protection_id=plan.protection_id,
            account_id=plan.account_id,
            symbol=plan.symbol,
            event_type=ExitPlanEventType.PLAN_CREATED,
            occurred_at=plan.decision_at,
            known_at=known_at or plan.decision_at,
            idempotency_key=f"plan-created:v{plan.version}:{plan.plan_id}",
            plan_id=plan.plan_id,
            payload={
                "plan": exit_plan_document(plan),
                "schema_version": _SCHEMA_VERSION,
            },
        )
        stored, created = self._append_semantically_idempotent(event)
        return QuickExitPlanCreation(result=result, event=stored, created=created)

    def active_plan(self, protection_id: str) -> ExitPlan:
        """重放并返回当前不可变计划版本。"""

        events = self._store.events(protection_id)
        if not events:
            raise ExitPlanNotFoundError(f"unknown exit protection: {protection_id!r}")
        return _active_plan(events)

    def history(self, protection_id: str) -> tuple[ExitPlanEvent, ...]:
        """返回已经完整校验过的保护流，供上层恢复编排使用。"""

        return self._store.events(protection_id)

    def latest_attached_protection(
        self,
        account_id: str,
        symbol: str,
    ) -> str | None:
        """返回最近一个已有成交附着、且未明确关闭的保护流。"""

        for protection_id in self._store.protection_ids(account_id, symbol):
            events = self._store.events(protection_id)
            if any(event.event_type is ExitPlanEventType.POSITION_CLOSED for event in events):
                continue
            if any(event.event_type is ExitPlanEventType.PLAN_ATTACHED_TO_FILL for event in events):
                _active_plan(events)
                return protection_id
        return None

    def attach_fill_and_request_deep(
        self,
        protection_id: str,
        *,
        fill_id: str,
        filled_quantity: int,
        fill_price: Decimal,
        filled_at: datetime,
        known_at: datetime,
    ) -> PostFillExitPlanEvents:
        """幂等附着真实或模拟成交，并请求一次 DEEP 复核。"""

        if (
            isinstance(filled_quantity, bool)
            or not isinstance(filled_quantity, int)
            or filled_quantity <= 0
        ):
            raise ValueError("filled_quantity must be a positive integer")
        fill_price = _positive_decimal(fill_price, "fill_price")
        filled_at = _aware_utc(filled_at, "filled_at")
        known_at = _aware_utc(known_at, "known_at")
        if known_at < filled_at:
            raise ValueError("known_at must not precede filled_at")
        plan = self.active_plan(protection_id)
        attachment = NewExitPlanEvent.create(
            protection_id=plan.protection_id,
            account_id=plan.account_id,
            symbol=plan.symbol,
            event_type=ExitPlanEventType.PLAN_ATTACHED_TO_FILL,
            occurred_at=filled_at,
            known_at=known_at,
            idempotency_key=f"plan-attached-to-fill:{fill_id}",
            plan_id=plan.plan_id,
            payload={
                "fill_id": fill_id,
                "fill_price": fill_price,
                "filled_at": filled_at,
                "filled_quantity": filled_quantity,
                "plan_id": plan.plan_id,
                "schema_version": _SCHEMA_VERSION,
            },
        )
        attachment_event, attachment_created = self._append_semantically_idempotent(attachment)

        request_key = f"deep-analysis-requested:v{plan.version}"
        existing_request = self._store.event_by_idempotency_key(
            plan.protection_id,
            request_key,
        )
        if existing_request is not None:
            if (
                existing_request.event_type is not ExitPlanEventType.DEEP_ANALYSIS_REQUESTED
                or existing_request.plan_id != plan.plan_id
            ):
                raise ExitPlanLifecycleConflictError(
                    "DEEP-analysis idempotency key is bound to another event"
                )
            request_event = existing_request
            request_created = False
        else:
            request = NewExitPlanEvent.create(
                protection_id=plan.protection_id,
                account_id=plan.account_id,
                symbol=plan.symbol,
                event_type=ExitPlanEventType.DEEP_ANALYSIS_REQUESTED,
                occurred_at=filled_at,
                known_at=known_at,
                idempotency_key=request_key,
                plan_id=plan.plan_id,
                payload={
                    "requested_after_fill_id": fill_id,
                    "source_plan_id": plan.plan_id,
                    "source_plan_version": plan.version,
                    "schema_version": _SCHEMA_VERSION,
                },
            )
            request_event, request_created = self._append_semantically_idempotent(request)
        return PostFillExitPlanEvents(
            active_plan=plan,
            attachment_event=attachment_event,
            deep_request_event=request_event,
            attachment_created=attachment_created,
            deep_request_created=request_created,
        )

    def apply_deep_replacement(
        self,
        replacement: ExitPlan,
        *,
        known_at: datetime,
        record_validation_failure: bool = True,
    ) -> DeepExitPlanApplication:
        """仅应用风险单调收紧的 DEEP 计划，否则保留当前活动计划。"""

        known_at = _aware_utc(known_at, "known_at")
        existing = self._store.event_by_idempotency_key(
            replacement.protection_id,
            f"plan-replaced:{replacement.plan_id}",
        )
        if existing is not None:
            stored_plan = _plan_from_event(existing)
            if stored_plan != replacement:
                raise ExitPlanLifecycleConflictError(
                    "replacement idempotency key is bound to another plan"
                )
            return DeepExitPlanApplication(
                active_plan=stored_plan,
                event=existing,
                applied=False,
                failure_reason=None,
            )

        events = self._store.events(replacement.protection_id)
        if not events:
            raise ExitPlanNotFoundError(f"unknown exit protection: {replacement.protection_id!r}")
        active = _active_plan(events)
        if not any(event.event_type is ExitPlanEventType.PLAN_ATTACHED_TO_FILL for event in events):
            raise ExitPlanLifecycleConflictError(
                "a DEEP replacement requires a durable fill attachment"
            )
        if not any(
            event.event_type is ExitPlanEventType.DEEP_ANALYSIS_REQUESTED
            and event.plan_id == active.plan_id
            for event in events
        ):
            raise ExitPlanLifecycleConflictError(
                "a DEEP replacement requires a durable post-fill analysis request"
            )
        try:
            validate_exit_plan_replacement(active, replacement)
        except ValueError as error:
            if not record_validation_failure:
                raise
            failure = self._deep_failure_event(
                active,
                attempt_id=f"replacement-{replacement.plan_id}",
                reason_code="DEEP_REPLACEMENT_REJECTED",
                failed_at=replacement.decision_at,
                known_at=known_at,
                details={
                    "candidate_plan": exit_plan_document(replacement),
                    "validation_error": str(error),
                },
            )
            stored_failure, _ = self._append_semantically_idempotent(failure)
            return DeepExitPlanApplication(
                active_plan=active,
                event=stored_failure,
                applied=False,
                failure_reason=str(error),
            )

        replacement_event = NewExitPlanEvent.create(
            protection_id=active.protection_id,
            account_id=active.account_id,
            symbol=active.symbol,
            event_type=ExitPlanEventType.PLAN_REPLACED,
            occurred_at=replacement.decision_at,
            known_at=known_at,
            idempotency_key=f"plan-replaced:{replacement.plan_id}",
            plan_id=replacement.plan_id,
            payload={
                "plan": exit_plan_document(replacement),
                "schema_version": _SCHEMA_VERSION,
                "superseded_plan_id": active.plan_id,
            },
        )
        stored, applied = self._append_semantically_idempotent(replacement_event)
        return DeepExitPlanApplication(
            active_plan=replacement,
            event=stored,
            applied=applied,
            failure_reason=None,
        )

    def build_and_apply_deep(
        self,
        protection_id: str,
        *,
        timeframes: tuple[DeepExitTimeframe, ...],
        decision_at: datetime,
        known_at: datetime,
        baseline_assessment: DeepSemanticAssessment | None = None,
        adversarial_assessment: DeepSemanticAssessment | None = None,
        config: DeepExitPlanConfig | None = None,
    ) -> DeepExitPlanBuildApplication:
        """在成交请求之后生成并原子追加一个可重放的 DEEP 版本。"""

        active = self.active_plan(protection_id)
        result = build_deep_exit_plan(
            active,
            timeframes=timeframes,
            decision_at=decision_at,
            baseline_assessment=baseline_assessment,
            adversarial_assessment=adversarial_assessment,
            config=config,
        )
        application = self.apply_deep_replacement(
            result.plan,
            known_at=known_at,
        )
        return DeepExitPlanBuildApplication(result=result, application=application)

    def record_deep_analysis_failure(
        self,
        protection_id: str,
        *,
        attempt_id: str,
        reason_code: str,
        failed_at: datetime,
        known_at: datetime,
    ) -> ExitPlanEvent:
        """记录上游 DEEP 分析失败，但不修改计划。"""

        active = self.active_plan(protection_id)
        events = self._store.events(protection_id)
        if not any(
            event.event_type is ExitPlanEventType.DEEP_ANALYSIS_REQUESTED
            and event.plan_id == active.plan_id
            for event in events
        ):
            raise ExitPlanLifecycleConflictError(
                "a DEEP failure requires a durable analysis request"
            )
        event = self._deep_failure_event(
            active,
            attempt_id=attempt_id,
            reason_code=reason_code,
            failed_at=failed_at,
            known_at=known_at,
            details={},
        )
        stored, _ = self._append_semantically_idempotent(event)
        return stored

    def observe_completed_bar(
        self,
        protection_id: str,
        *,
        bar: TechnicalBar,
        observed_at: datetime,
        sellable_quantity: int,
    ) -> ExitBarrierObservation:
        """记录止损、目标或时间门槛穿越；对歧义 K 线按止损优先解析。

        ``sellable_quantity`` 为零是 A 股当日买入受 T+1 限制的正常情况：两条
        审计事件仍会追加，但结果会明确抑制任何执行交接。即使股份可卖，本方法
        也绝不会创建订单。
        """

        if (
            isinstance(sellable_quantity, bool)
            or not isinstance(sellable_quantity, int)
            or sellable_quantity < 0
        ):
            raise ValueError("sellable_quantity must be a non-negative integer")
        if not bar.complete:
            raise ValueError("exit barriers require a completed bar")
        bar_end = _aware_utc(bar.end_time, "bar.end_time")
        available_at = _aware_utc(bar.available_at, "bar.available_at")
        observed_at = _aware_utc(observed_at, "observed_at")
        if observed_at < available_at:
            raise ValueError("observed_at must not precede bar availability")
        plan = self.active_plan(protection_id)
        events = self._store.events(protection_id)
        # 计划可以在买入前创建用于定仓，但只有确切成交已经持久化后，
        # 才允许把行情解释成持仓退出信号。
        if not any(event.event_type is ExitPlanEventType.PLAN_ATTACHED_TO_FILL for event in events):
            raise ExitPlanLifecycleConflictError(
                "exit barriers cannot be observed before a durable fill attachment"
            )

        crossed: list[ExitBarrierKind] = []
        if bar.low <= plan.stop_price:
            crossed.append(ExitBarrierKind.STOP_LOSS)
        if bar.high >= plan.take_profit_price:
            crossed.append(ExitBarrierKind.TAKE_PROFIT)
        if bar_end >= plan.time_exit_at:
            crossed.append(ExitBarrierKind.TIME)
        if not crossed:
            return ExitBarrierObservation(
                active_plan=plan,
                selected_barrier=None,
                crossed_barriers=(),
                barrier_event=None,
                signal_event=None,
                sellable_quantity=sellable_quantity,
                execution_handoff_required=False,
                suppression_reason=None,
            )

        # 完整 OHLC K 线无法提供可信的线内先后顺序。采用悲观的止损优先规则，
        # 避免乐观地选择止盈目标。
        selected = crossed[0]
        trigger_price = {
            ExitBarrierKind.STOP_LOSS: plan.stop_price,
            ExitBarrierKind.TAKE_PROFIT: plan.take_profit_price,
            ExitBarrierKind.TIME: bar.close,
        }[selected]
        stamp = bar_end.strftime("%Y%m%dT%H%M%S%fZ")
        payload = {
            "bar": {
                "available_at": available_at,
                "close": bar.close,
                "end_time": bar_end,
                "high": bar.high,
                "low": bar.low,
                "open": bar.open,
                "volume": bar.volume,
            },
            "crossed_barriers": [kind.value for kind in crossed],
            "plan_id": plan.plan_id,
            "resolution_policy": "STOP_FIRST_THEN_TARGET_THEN_TIME",
            "schema_version": _SCHEMA_VERSION,
            "selected_barrier": selected.value,
            "sellable_quantity": sellable_quantity,
            "trigger_price": trigger_price,
        }
        barrier = NewExitPlanEvent.create(
            protection_id=plan.protection_id,
            account_id=plan.account_id,
            symbol=plan.symbol,
            event_type=ExitPlanEventType.BARRIER_OBSERVED,
            occurred_at=bar_end,
            known_at=observed_at,
            idempotency_key=f"barrier-observed:v{plan.version}:{stamp}",
            plan_id=plan.plan_id,
            payload=payload,
        )
        barrier_event, _ = self._append_semantically_idempotent(barrier)

        t1_blocked = sellable_quantity == 0
        signal = NewExitPlanEvent.create(
            protection_id=plan.protection_id,
            account_id=plan.account_id,
            symbol=plan.symbol,
            event_type=ExitPlanEventType.EXIT_SIGNAL_RAISED,
            occurred_at=bar_end,
            known_at=observed_at,
            idempotency_key=f"exit-signal:v{plan.version}:{stamp}",
            plan_id=plan.plan_id,
            payload={
                **payload,
                "execution_handoff_required": not t1_blocked,
                "order_created": False,
                "suppression_reason": "T1_NO_SELLABLE_QUANTITY" if t1_blocked else None,
            },
        )
        signal_event, _ = self._append_semantically_idempotent(signal)
        return ExitBarrierObservation(
            active_plan=plan,
            selected_barrier=selected,
            crossed_barriers=tuple(crossed),
            barrier_event=barrier_event,
            signal_event=signal_event,
            sellable_quantity=sellable_quantity,
            execution_handoff_required=not t1_blocked,
            suppression_reason="T1_NO_SELLABLE_QUANTITY" if t1_blocked else None,
        )

    def record_position_closed(
        self,
        protection_id: str,
        *,
        fill_id: str,
        closed_quantity: int,
        fill_price: Decimal,
        closed_at: datetime,
        known_at: datetime,
    ) -> ExitPlanEvent:
        """记录用户已在券商完成的最终卖出；本方法本身绝不创建订单。"""

        if (
            isinstance(closed_quantity, bool)
            or not isinstance(closed_quantity, int)
            or closed_quantity <= 0
        ):
            raise ValueError("closed_quantity must be a positive integer")
        fill_price = _positive_decimal(fill_price, "fill_price")
        closed_at = _aware_utc(closed_at, "closed_at")
        known_at = _aware_utc(known_at, "known_at")
        if known_at < closed_at:
            raise ValueError("known_at must not precede closed_at")
        active = self.active_plan(protection_id)
        events = self._store.events(protection_id)
        if not any(event.event_type is ExitPlanEventType.PLAN_ATTACHED_TO_FILL for event in events):
            raise ExitPlanLifecycleConflictError(
                "position closure requires a durable fill attachment"
            )
        event = NewExitPlanEvent.create(
            protection_id=active.protection_id,
            account_id=active.account_id,
            symbol=active.symbol,
            event_type=ExitPlanEventType.POSITION_CLOSED,
            occurred_at=closed_at,
            known_at=known_at,
            idempotency_key=f"position-closed:{fill_id}",
            plan_id=active.plan_id,
            payload={
                "closed_quantity": closed_quantity,
                "fill_id": fill_id,
                "fill_price": fill_price,
                "order_created": False,
                "schema_version": _SCHEMA_VERSION,
                "source": "USER_CONFIRMED_EXTERNAL_FILL",
            },
        )
        stored, _ = self._append_semantically_idempotent(event)
        return stored

    def _deep_failure_event(
        self,
        active: ExitPlan,
        *,
        attempt_id: str,
        reason_code: str,
        failed_at: datetime,
        known_at: datetime,
        details: Mapping[str, object],
    ) -> NewExitPlanEvent:
        return NewExitPlanEvent.create(
            protection_id=active.protection_id,
            account_id=active.account_id,
            symbol=active.symbol,
            event_type=ExitPlanEventType.DEEP_ANALYSIS_FAILED,
            occurred_at=failed_at,
            known_at=known_at,
            idempotency_key=f"deep-analysis-failed:{attempt_id}",
            plan_id=active.plan_id,
            payload={
                **details,
                "active_plan_id": active.plan_id,
                "reason_code": reason_code,
                "schema_version": _SCHEMA_VERSION,
            },
        )

    def _append_semantically_idempotent(
        self,
        event: NewExitPlanEvent,
    ) -> tuple[ExitPlanEvent, bool]:
        existing = self._store.event_by_idempotency_key(
            event.protection_id,
            event.idempotency_key,
        )
        if existing is not None:
            # ``known_at`` 是首次观察的审计时间戳。安全重放可以稍后发生，
            # 但不能修改任何语义内容。
            if not (
                existing.account_id == event.account_id
                and existing.symbol == event.symbol
                and existing.event_type is event.event_type
                and existing.occurred_at == event.occurred_at
                and existing.plan_id == event.plan_id
                and existing.payload_json == event.payload_json
            ):
                raise ExitPlanLifecycleConflictError(
                    "idempotency key is bound to different lifecycle content"
                )
            return existing, False
        expected_sequence = len(self._store.events(event.protection_id))
        return self._store.append(event, expected_sequence=expected_sequence)


def _active_plan(events: tuple[ExitPlanEvent, ...]) -> ExitPlan:
    active: ExitPlan | None = None
    for event in events:
        if event.event_type is ExitPlanEventType.PLAN_CREATED:
            candidate = _plan_from_event(event)
            if active is not None or candidate.depth is not ExitPlanDepth.QUICK:
                raise ExitPlanLifecycleConflictError("invalid PLAN_CREATED history")
            active = candidate
        elif event.event_type is ExitPlanEventType.PLAN_REPLACED:
            if active is None:
                raise ExitPlanLifecycleConflictError("PLAN_REPLACED precedes PLAN_CREATED")
            candidate = _plan_from_event(event)
            try:
                validate_exit_plan_replacement(active, candidate)
            except ValueError as error:
                raise ExitPlanLifecycleConflictError(
                    "stored exit-plan replacement violates monotonic risk"
                ) from error
            active = candidate
    if active is None:
        raise ExitPlanNotFoundError("protection stream has no plan creation event")
    return active


def _plan_from_event(event: ExitPlanEvent) -> ExitPlan:
    raw = event.payload.get("plan")
    if not isinstance(raw, Mapping):
        raise ExitPlanLifecycleConflictError("plan event lacks a plan document")
    try:
        plan = _plan_from_document(raw)
    except (KeyError, TypeError, ValueError) as error:
        raise ExitPlanLifecycleConflictError("stored plan document is invalid") from error
    if (
        event.plan_id != plan.plan_id
        or event.protection_id != plan.protection_id
        or event.account_id != plan.account_id
        or event.symbol != plan.symbol
    ):
        raise ExitPlanLifecycleConflictError("plan event identity does not match its document")
    return plan


def _plan_from_document(document: Mapping[object, object]) -> ExitPlan:
    metrics_value = document["metrics"]
    if not isinstance(metrics_value, list):
        raise TypeError("metrics must be a list")
    metrics: list[tuple[str, Decimal]] = []
    for item in metrics_value:
        if not isinstance(item, list) or len(item) != 2:
            raise TypeError("metric entries must be name/value pairs")
        metrics.append((_text(item[0], "metric name"), Decimal(_text(item[1], "metric"))))
    invalidation_value = document["technical_invalidation_price"]
    return ExitPlan(
        plan_id=_text(document["plan_id"], "plan_id"),
        protection_id=_text(document["protection_id"], "protection_id"),
        account_id=_text(document["account_id"], "account_id"),
        symbol=_text(document["symbol"], "symbol"),
        version=_integer(document["version"], "version"),
        depth=ExitPlanDepth(_text(document["depth"], "depth")),
        state=ExitPlanState(_text(document["state"], "state")),
        decision_at=datetime.fromisoformat(_text(document["decision_at"], "decision_at")),
        market_data_as_of=datetime.fromisoformat(
            _text(document["market_data_as_of"], "market_data_as_of")
        ),
        time_exit_at=datetime.fromisoformat(_text(document["time_exit_at"], "time_exit_at")),
        entry_basis_price=Decimal(_text(document["entry_basis_price"], "entry_basis_price")),
        stop_price=Decimal(_text(document["stop_price"], "stop_price")),
        take_profit_price=Decimal(_text(document["take_profit_price"], "take_profit_price")),
        initial_risk_per_share=Decimal(
            _text(document["initial_risk_per_share"], "initial_risk_per_share")
        ),
        reward_to_risk=Decimal(_text(document["reward_to_risk"], "reward_to_risk")),
        price_tick=Decimal(_text(document["price_tick"], "price_tick")),
        technical_invalidation_price=(
            None
            if invalidation_value is None
            else Decimal(_text(invalidation_value, "technical_invalidation_price"))
        ),
        feature_snapshot_sha256=_text(
            document["feature_snapshot_sha256"],
            "feature_snapshot_sha256",
        ),
        policy_version=_text(document["policy_version"], "policy_version"),
        strategy_version=_text(document["strategy_version"], "strategy_version"),
        calibration_id=_text(document["calibration_id"], "calibration_id"),
        reason_codes=_text_tuple(document["reason_codes"], "reason_codes"),
        metrics=tuple(metrics),
        evidence_ids=_text_tuple(document["evidence_ids"], "evidence_ids"),
        supersedes_plan_id=(
            None
            if document["supersedes_plan_id"] is None
            else _text(document["supersedes_plan_id"], "supersedes_plan_id")
        ),
    )


def _text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    return value


def _text_tuple(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be a list")
    return tuple(_text(item, name) for item in value)


def _integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _positive_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return value


def _aware_utc(value: object, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


__all__ = [
    "DeepExitPlanBuildApplication",
    "DeepExitPlanApplication",
    "ExitBarrierObservation",
    "ExitPlanEventStore",
    "ExitPlanLifecycleConflictError",
    "ExitPlanLifecycleError",
    "ExitPlanLifecycleService",
    "ExitPlanNotFoundError",
    "PostFillExitPlanEvents",
    "QuickExitPlanCreation",
]
