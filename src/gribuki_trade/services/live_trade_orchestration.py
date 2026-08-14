"""实盘买后保护、持续观察与 NapCat 提醒的应用级编排。

该模块没有券商适配器。它只能处理用户已经确认的成交、读取完整 K 线、形成
退出信号并写入出站消息队列。工作采用租约和幂等键，应用崩溃后可由下一次
``process_due_work`` 调用继续；不需要、也不会安装操作系统服务。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol, cast

from gribuki_trade.domain.exit_plans import ExitPlanDepth
from gribuki_trade.domain.live_records import (
    ConfirmedLiveFill,
    LiveProtectionTracking,
    LiveWorkItem,
    LiveWorkKind,
)
from gribuki_trade.domain.orders import Side
from gribuki_trade.domain.paper_trading import PaperFillFees, PaperInstrumentType
from gribuki_trade.features.deep_exit_planning import (
    DeepExitTimeframe,
    DeepSemanticAssessment,
)
from gribuki_trade.features.exit_planning import QuickExitPlanConfig
from gribuki_trade.features.technical import TechnicalBar
from gribuki_trade.ports.notifier import (
    NotificationTargetKind,
    OutboundNotification,
)
from gribuki_trade.reporting.contracts import ReportKind, render_stable_text_report
from gribuki_trade.services.exit_plan_lifecycle import (
    ExitBarrierObservation,
    ExitPlanLifecycleError,
    ExitPlanLifecycleService,
    ExitPlanNotFoundError,
)
from gribuki_trade.storage.live_records import (
    LiveRecordStateError,
    SQLiteLiveRecordStore,
)
from gribuki_trade.storage.outbox import SQLiteOutbox


class LiveProtectionInputError(RuntimeError):
    """保护分析输入暂不可用或永久无效。"""

    def __init__(self, code: str, *, retryable: bool) -> None:
        self.code = _stable_code(code)
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
        if (self.baseline_assessment is None) is not (
            self.adversarial_assessment is None
        ):
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


class LiveTradeOrchestrationService:
    """连接实盘账本、退出生命周期和通用 OneBot outbox。"""

    def __init__(
        self,
        *,
        live_store: SQLiteLiveRecordStore,
        exit_lifecycle: ExitPlanLifecycleService,
        protection_inputs: LiveProtectionInputProvider,
        outbox: SQLiteOutbox,
        notification_target_kind: NotificationTargetKind,
        notification_target_id: str,
        maximum_work_attempts: int = 5,
    ) -> None:
        if not notification_target_id.strip():
            raise ValueError("notification target id must not be empty")
        if maximum_work_attempts < 1:
            raise ValueError("maximum_work_attempts must be positive")
        self._live_store = live_store
        self._exit = exit_lifecycle
        self._inputs = protection_inputs
        self._outbox = outbox
        self._target_kind = NotificationTargetKind(notification_target_kind)
        self._target_id = notification_target_id.strip()
        self._maximum_attempts = maximum_work_attempts

    async def process_due_work(
        self,
        *,
        now: datetime | None = None,
        kinds: frozenset[LiveWorkKind] | None = None,
        work_ids: frozenset[str] | None = None,
        limit: int = 10,
        work_timeout_seconds: float | None = None,
    ) -> LiveWorkRunSummary:
        """有限领取一批任务；调用方可在未来应用 runtime 中周期调用。"""

        moment = _aware_utc(now or datetime.now(UTC))
        if work_timeout_seconds is not None and (
            not 0 < work_timeout_seconds < float("inf")
        ):
            raise ValueError("work_timeout_seconds must be positive and finite")
        claimed = self._live_store.claim_due_work(
            now=moment,
            kinds=kinds,
            work_ids=work_ids,
            limit=limit,
        )
        completed: list[str] = []
        retried = 0
        dead = 0
        for work in claimed:
            try:
                if work.kind is LiveWorkKind.BUILD_PROTECTION:
                    operation = self._build_protection(work, now=moment)
                    result = (
                        await operation
                        if work_timeout_seconds is None
                        else await asyncio.wait_for(operation, timeout=work_timeout_seconds)
                    )
                elif work.kind is LiveWorkKind.BUILD_DEEP_PROTECTION:
                    operation = self._build_deep_protection(work, now=moment)
                    result = (
                        await operation
                        if work_timeout_seconds is None
                        else await asyncio.wait_for(operation, timeout=work_timeout_seconds)
                    )
                elif work.kind is LiveWorkKind.CLOSE_PROTECTION:
                    result = self._close_protection(work, now=moment)
                elif work.kind is LiveWorkKind.DELIVER_EXIT_ALERT:
                    result = self._deliver_exit_alert(work, now=moment)
                else:  # pragma: no cover - 枚举与数据库 CHECK 的完整性边界
                    raise LiveProtectionInputError("UNKNOWN_LIVE_WORK_KIND", retryable=False)
            except LiveProtectionInputError as error:
                failed = self._fail_claimed_work(
                    work,
                    failed_at=moment,
                    error_code=error.code,
                    retryable=error.retryable,
                )
                if failed is None:
                    retried += 1
                elif failed.status.value == "DEAD":
                    dead += 1
                else:
                    retried += 1
                continue
            except LiveRecordStateError as error:
                if error.code == "WORK_LEASE_FENCED":
                    retried += 1
                    continue
                raise
            except (OSError, TimeoutError):
                failed_at = max(moment, datetime.now(UTC))
                failed = self._fail_claimed_work(
                    work,
                    failed_at=failed_at,
                    error_code="TRANSIENT_DEPENDENCY_FAILURE",
                    retryable=True,
                )
                if failed is None:
                    retried += 1
                elif failed.status.value == "DEAD":
                    dead += 1
                else:
                    retried += 1
                continue
            except (ExitPlanLifecycleError, ValueError, ArithmeticError):
                failed = self._fail_claimed_work(
                    work,
                    failed_at=moment,
                    error_code="PROTECTION_WORK_INVALID",
                    retryable=False,
                )
                if failed is None:
                    retried += 1
                elif failed.status.value == "DEAD":
                    dead += 1
                continue
            completed.append(result.work_id)
        return LiveWorkRunSummary(
            claimed=len(claimed),
            completed=len(completed),
            retried=retried,
            dead=dead,
            completed_work_ids=tuple(completed),
        )

    def _fail_claimed_work(
        self,
        work: LiveWorkItem,
        *,
        failed_at: datetime,
        error_code: str,
        retryable: bool,
    ) -> LiveWorkItem | None:
        """失败释放也服从代际栅栏；接管后旧 worker 不得改写新 owner 状态。"""

        try:
            return self._live_store.fail_work(
                work.work_id,
                lease_attempt=work.attempts,
                failed_at=failed_at,
                error_code=error_code,
                retryable=retryable,
                maximum_attempts=self._maximum_attempts,
            )
        except LiveRecordStateError as error:
            if error.code == "WORK_LEASE_FENCED":
                return None
            raise

    async def _build_protection(
        self,
        work: LiveWorkItem,
        *,
        now: datetime,
    ) -> LiveWorkItem:
        if work.protection_id is None:
            raise LiveProtectionInputError("PROTECTION_ID_MISSING", retryable=False)
        tracking = self._tracking_by_id(work.account_id, work.protection_id)
        if tracking.remaining_quantity == 0:
            return self._live_store.complete_work(
                work.work_id,
                lease_attempt=work.attempts,
                completed_at=now,
                result_code="POSITION_ALREADY_CLOSED",
            )
        fill = _fill_from_work(work)
        if fill.side is not Side.BUY:
            raise LiveProtectionInputError("PROTECTION_FILL_IS_NOT_BUY", retryable=False)
        try:
            existing = self._exit.active_plan(work.protection_id)
        except ExitPlanNotFoundError:
            existing = None
        if existing is not None and (
            existing.account_id != fill.account_id or existing.symbol != fill.symbol
        ):
            raise LiveProtectionInputError("PROTECTION_STREAM_IDENTITY_MISMATCH", retryable=False)
        inputs = await self._inputs.prepare(fill, requested_at=now)
        known_at = max(now, inputs.decision_at, datetime.now(UTC))
        self._live_store.renew_work_lease(
            work.work_id,
            lease_attempt=work.attempts,
            renewed_at=known_at,
            lease_for=timedelta(minutes=15),
        )
        if inputs.technical_invalidation_price >= fill.price:
            raise LiveProtectionInputError("INVALIDATION_NOT_BELOW_FILL", retryable=False)

        # 逻辑保护流只保存 QUICK；DEEP 会写入按租约代际隔离的候选物理流。
        if existing is None:
            quick = self._exit.create_quick_plan(
                account_id=fill.account_id,
                protection_id=work.protection_id,
                symbol=fill.symbol,
                bars=inputs.bars,
                decision_at=inputs.decision_at,
                time_exit_at=inputs.time_exit_at,
                worst_entry_price=fill.price,
                technical_invalidation_price=inputs.technical_invalidation_price,
                strategy_version=inputs.strategy_version,
                config=inputs.quick_config,
                known_at=known_at,
            )
            if not quick.result.plan.plan_id:
                raise LiveProtectionInputError("QUICK_PLAN_INVALID", retryable=False)
        elif existing.depth not in {ExitPlanDepth.QUICK, ExitPlanDepth.DEEP}:
            raise LiveProtectionInputError("PROTECTION_PLAN_DEPTH_INVALID", retryable=False)
        self._exit.attach_fill_and_request_deep(
            work.protection_id,
            fill_id=fill.command_id,
            filled_quantity=fill.quantity,
            fill_price=fill.price,
            filled_at=fill.executed_at,
            known_at=known_at,
        )
        current = self._exit.active_plan(work.protection_id)
        completed, _deep_work = self._live_store.complete_quick_protection_work(
            work.work_id,
            lease_attempt=work.attempts,
            completed_at=known_at,
            plan_id=current.plan_id,
            plan_stream_id=work.protection_id,
        )
        return completed

    async def _build_deep_protection(
        self,
        work: LiveWorkItem,
        *,
        now: datetime,
    ) -> LiveWorkItem:
        """在代际隔离候选流生成 DEEP，再由 live 库租约栅栏决定是否激活。"""

        if work.protection_id is None:
            raise LiveProtectionInputError("PROTECTION_ID_MISSING", retryable=False)
        tracking = self._tracking_by_id(work.account_id, work.protection_id)
        if tracking.remaining_quantity == 0:
            return self._live_store.complete_work(
                work.work_id,
                lease_attempt=work.attempts,
                completed_at=now,
                result_code="POSITION_ALREADY_CLOSED",
            )
        if not tracking.plan_ready or tracking.plan_stream_id is None:
            raise LiveProtectionInputError("QUICK_PLAN_NOT_READY", retryable=True)
        fill = _fill_from_work(work)
        if fill.side is not Side.BUY:
            raise LiveProtectionInputError("PROTECTION_FILL_IS_NOT_BUY", retryable=False)
        active = self._exit.active_plan(tracking.plan_stream_id)
        if active.depth is ExitPlanDepth.DEEP:
            return self._live_store.complete_protection_work(
                work.work_id,
                lease_attempt=work.attempts,
                completed_at=now,
                result_code="DEEP_PLAN_ALREADY_READY",
                plan_id=active.plan_id,
                plan_stream_id=tracking.plan_stream_id,
            )

        inputs = await self._inputs.prepare(fill, requested_at=now)
        known_at = max(now, inputs.decision_at, datetime.now(UTC))
        self._live_store.renew_work_lease(
            work.work_id,
            lease_attempt=work.attempts,
            renewed_at=known_at,
            lease_for=timedelta(minutes=15),
        )
        if inputs.technical_invalidation_price >= fill.price:
            raise LiveProtectionInputError("INVALIDATION_NOT_BELOW_FILL", retryable=False)
        baseline = inputs.baseline_assessment
        adversarial = inputs.adversarial_assessment
        if baseline is None or adversarial is None:
            baseline, adversarial = await self._inputs.assess_deep(fill, inputs=inputs)
        known_at = max(known_at, datetime.now(UTC))
        self._live_store.renew_work_lease(
            work.work_id,
            lease_attempt=work.attempts,
            renewed_at=known_at,
            lease_for=timedelta(minutes=15),
        )
        candidate_stream_id = _deep_candidate_stream_id(work)
        try:
            candidate = self._exit.active_plan(candidate_stream_id)
        except ExitPlanNotFoundError:
            candidate = self._exit.create_quick_plan(
                account_id=fill.account_id,
                protection_id=candidate_stream_id,
                symbol=fill.symbol,
                bars=inputs.bars,
                decision_at=inputs.decision_at,
                time_exit_at=inputs.time_exit_at,
                worst_entry_price=fill.price,
                technical_invalidation_price=inputs.technical_invalidation_price,
                strategy_version=inputs.strategy_version,
                config=inputs.quick_config,
                known_at=known_at,
            ).result.plan
            self._exit.attach_fill_and_request_deep(
                candidate_stream_id,
                fill_id=fill.command_id,
                filled_quantity=fill.quantity,
                fill_price=fill.price,
                filled_at=fill.executed_at,
                known_at=known_at,
            )
        if candidate.depth is ExitPlanDepth.DEEP:
            active_candidate = candidate
            applied = True
        else:
            deep = self._exit.build_and_apply_deep(
                candidate_stream_id,
                timeframes=inputs.deep_timeframes,
                decision_at=inputs.decision_at,
                known_at=known_at,
                baseline_assessment=baseline,
                adversarial_assessment=adversarial,
            )
            active_candidate = deep.application.active_plan
            applied = deep.application.applied
        if active_candidate.depth is not ExitPlanDepth.DEEP:
            return self._live_store.complete_protection_work(
                work.work_id,
                lease_attempt=work.attempts,
                completed_at=known_at,
                result_code="QUICK_PLAN_RETAINED",
                plan_id=active.plan_id,
                plan_stream_id=tracking.plan_stream_id,
            )
        result_code = "DEEP_PLAN_READY" if applied else "DEEP_PLAN_ALREADY_READY"
        return self._live_store.complete_protection_work(
            work.work_id,
            lease_attempt=work.attempts,
            completed_at=known_at,
            result_code=result_code,
            plan_id=active_candidate.plan_id,
            plan_stream_id=candidate_stream_id,
        )

    def _close_protection(self, work: LiveWorkItem, *, now: datetime) -> LiveWorkItem:
        """把用户已确认的最终卖出同步到退出流，不产生任何执行副作用。"""

        if work.protection_id is None:
            raise LiveProtectionInputError("PROTECTION_ID_MISSING", retryable=False)
        tracking = self._tracking_by_id(work.account_id, work.protection_id)
        if tracking.remaining_quantity != 0:
            raise LiveProtectionInputError("PROTECTION_STILL_HAS_POSITION", retryable=False)
        if not tracking.plan_ready or tracking.plan_stream_id is None:
            return self._live_store.complete_work(
                work.work_id,
                lease_attempt=work.attempts,
                completed_at=now,
                result_code="NO_PLAN_TO_CLOSE",
            )
        document = _json_object(work.payload_json)
        fill = _fill_from_document(_mapping(document, "sell_fill"))
        allocated_quantity = _integer(document, "allocated_quantity")
        self._exit.record_position_closed(
            tracking.plan_stream_id,
            fill_id=fill.command_id,
            closed_quantity=allocated_quantity,
            fill_price=fill.price,
            closed_at=fill.executed_at,
            known_at=max(now, fill.executed_at),
        )
        return self._live_store.complete_work(
            work.work_id,
            lease_attempt=work.attempts,
            completed_at=max(now, fill.executed_at),
            result_code="PROTECTION_CLOSED",
        )

    def observe_completed_bar(
        self,
        *,
        account_id: str,
        symbol: str,
        bar: TechnicalBar,
        observed_at: datetime | None = None,
        protection_ids: frozenset[str] | None = None,
    ) -> LiveTrackingObservation:
        """观察所有仍有数量的批次；触发时只排队提醒，绝不创建卖单。"""

        moment = _aware_utc(observed_at or datetime.now(UTC))
        if not bar.complete:
            raise ValueError("live tracking requires a completed bar")
        selected_protection_ids = _normalized_work_ids(
            protection_ids,
            field_name="protection_ids",
        )
        observations: list[ExitBarrierObservation] = []
        queued: list[str] = []
        for tracking in self._live_store.tracking(
            account_id,
            symbol=symbol,
            active_only=True,
        ):
            if (
                selected_protection_ids is not None
                and tracking.protection_id not in selected_protection_ids
            ):
                continue
            if not tracking.plan_ready or tracking.plan_stream_id is None:
                continue
            if (
                tracking.last_observed_bar_end is not None
                and bar.end_time <= tracking.last_observed_bar_end
            ):
                continue
            sellable = self._live_store.sellable_quantity(
                tracking.protection_id,
                as_of=moment,
            )
            observation = self._exit.observe_completed_bar(
                tracking.plan_stream_id,
                bar=bar,
                observed_at=moment,
                sellable_quantity=sellable,
            )
            observations.append(observation)
            if observation.selected_barrier is None:
                self._live_store.record_observed_bar(
                    tracking.protection_id,
                    bar_end=bar.end_time,
                )
                continue
            text = _exit_alert_text(tracking, observation, bar=bar, observed_at=moment)
            try:
                work, _ = self._live_store.queue_exit_alert(
                    protection_id=tracking.protection_id,
                    command_id=tracking.buy_command_id,
                    plan_stream_id=tracking.plan_stream_id,
                    bar_end=bar.end_time,
                    observed_at=moment,
                    payload={
                        "channel": "onebot",
                        "target_id": self._target_id,
                        "target_kind": self._target_kind.value,
                        "text": text,
                    },
                )
            except LiveRecordStateError as error:
                if error.code in {
                    "PROTECTION_PLAN_CHANGED",
                    "PROTECTION_PLAN_NOT_READY",
                    "PROTECTION_POSITION_CLOSED",
                }:
                    continue
                raise
            queued.append(work.work_id)
        return LiveTrackingObservation(
            account_id=account_id.strip(),
            symbol=symbol.strip().upper(),
            observations=tuple(observations),
            queued_alert_work_ids=tuple(queued),
        )

    def _deliver_exit_alert(self, work: LiveWorkItem, *, now: datetime) -> LiveWorkItem:
        document = _json_object(work.payload_json)
        try:
            target_kind = NotificationTargetKind(_text(document, "target_kind"))
        except ValueError:
            raise LiveProtectionInputError("ALERT_TARGET_KIND_INVALID", retryable=False) from None
        target_id = _text(document, "target_id")
        text = _text(document, "text")
        if target_kind is not self._target_kind or target_id != self._target_id:
            raise LiveProtectionInputError("ALERT_TARGET_CHANGED", retryable=False)
        self._outbox.enqueue(
            OutboundNotification(
                idempotency_key=work.work_id,
                channel="onebot",
                target_kind=target_kind,
                target_id=target_id,
                text=text,
                created_at=work.created_at,
            )
        )
        return self._live_store.complete_work(
            work.work_id,
            lease_attempt=work.attempts,
            completed_at=now,
            result_code="EXIT_ALERT_ENQUEUED",
        )

    def _tracking_by_id(
        self,
        account_id: str,
        protection_id: str,
    ) -> LiveProtectionTracking:
        match = next(
            (
                item
                for item in self._live_store.tracking(account_id, active_only=False)
                if item.protection_id == protection_id
            ),
            None,
        )
        if match is None:
            raise LiveProtectionInputError("PROTECTION_TRACKING_NOT_FOUND", retryable=False)
        return match


def _exit_alert_text(
    tracking: LiveProtectionTracking,
    observation: ExitBarrierObservation,
    *,
    bar: TechnicalBar,
    observed_at: datetime,
) -> str:
    """按盘中告警契约生成卖出时机提醒，不暗示券商已成交。"""

    barrier_names = {
        "STOP_LOSS": "止损门",
        "TAKE_PROFIT": "止盈门",
        "TIME": "时间门",
    }
    assert observation.selected_barrier is not None
    selected = barrier_names[observation.selected_barrier.value]
    execution = (
        f"当前记录可卖 {observation.sellable_quantity} 股，请用户自行在券商评估并执行；"
        "系统未创建、也无权创建券商卖单。"
        if observation.execution_handoff_required
        else "当前记录数量受 A 股 T+1 限制，暂不可卖；系统未创建券商卖单。"
    )
    plan = observation.active_plan
    metrics = dict(plan.metrics)
    baseline_score = metrics.get("baseline_semantic_score")
    adversarial_score = metrics.get("adversarial_semantic_score")
    selected_track = (
        "结构化对抗分析器"
        if "ADVERSARIAL_RESULT_PREFERRED" in plan.reason_codes
        else "不可用（当前计划未记录生产采用轨道）"
    )
    return render_stable_text_report(
        ReportKind.INTRADAY_ALERT,
        title="实盘持仓卖出时机提醒",
        sections={
            "发生了什么": (
                f"{tracking.symbol} 的 {selected} 已在完整 K 线上触发；"
                f"保护流 {tracking.protection_id}。"
            ),
            "执行结果": execution,
            "关键价格": (
                f"本线收盘 {bar.close}，最低 {bar.low}，最高 {bar.high}；"
                f"止损 {plan.stop_price}，止盈 {plan.take_profit_price}。"
            ),
            "双轨复核": (
                "原单分析器加权评分："
                + (
                    str(baseline_score)
                    if baseline_score is not None
                    else "不可用（当前计划未记录原单分析器评分）"
                )
                + "；对抗分析器加权评分："
                + (
                    str(adversarial_score)
                    if adversarial_score is not None
                    else "不可用（当前计划未记录对抗分析器评分）"
                )
                + f"；生产采用：{selected_track}。"
            ),
            "证据时点": (
                f"K 线结束 {bar.end_time.astimezone(UTC).isoformat()}；"
                f"系统观察 {observed_at.astimezone(UTC).isoformat()}。"
            ),
        },
    )


def _fill_from_work(work: LiveWorkItem) -> ConfirmedLiveFill:
    document = _json_object(work.payload_json)
    return _fill_from_document(_mapping(document, "fill"))


def _deep_candidate_stream_id(work: LiveWorkItem) -> str:
    """把每次租约领取映射为独立、不可被旧代际覆盖的物理计划流。"""

    if work.protection_id is None:
        raise LiveProtectionInputError("PROTECTION_ID_MISSING", retryable=False)
    material = f"{work.protection_id}\0{work.work_id}\0{work.attempts}".encode()
    return "live-deep-candidate-" + hashlib.sha256(material).hexdigest()[:40]


def _fill_from_document(fill: Mapping[str, object]) -> ConfirmedLiveFill:
    fees = _mapping(fill, "fees")
    return ConfirmedLiveFill(
        command_id=_text(fill, "command_id"),
        account_id=_text(fill, "account_id"),
        side=Side(_text(fill, "side")),
        symbol=_text(fill, "symbol"),
        quantity=_integer(fill, "quantity"),
        price=Decimal(_text(fill, "price")),
        instrument_type=PaperInstrumentType(_text(fill, "instrument_type")),
        executed_at=datetime.fromisoformat(_text(fill, "executed_at")),
        fees=PaperFillFees(
            commission=Decimal(_text(fees, "commission")),
            transfer_fee=Decimal(_text(fees, "transfer_fee")),
            stamp_tax=Decimal(_text(fees, "stamp_tax")),
        ),
        external_order_id=_optional_text(fill.get("external_order_id")),
        external_fill_id=_optional_text(fill.get("external_fill_id")),
    )


def _json_object(payload: str) -> dict[str, object]:
    try:
        value = json.loads(payload)
    except (TypeError, ValueError):
        raise LiveProtectionInputError("WORK_PAYLOAD_INVALID", retryable=False) from None
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise LiveProtectionInputError("WORK_PAYLOAD_INVALID", retryable=False)
    return cast(dict[str, object], value)


def _mapping(document: Mapping[str, object], name: str) -> dict[str, object]:
    value = document.get(name)
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise LiveProtectionInputError("WORK_PAYLOAD_INVALID", retryable=False)
    return cast(dict[str, object], value)


def _text(document: Mapping[str, object], name: str) -> str:
    value = document.get(name)
    if not isinstance(value, str) or not value:
        raise LiveProtectionInputError("WORK_PAYLOAD_INVALID", retryable=False)
    return value


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise LiveProtectionInputError("WORK_PAYLOAD_INVALID", retryable=False)
    return value


def _integer(document: Mapping[str, object], name: str) -> int:
    value = document.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise LiveProtectionInputError("WORK_PAYLOAD_INVALID", retryable=False)
    return value


def _stable_code(value: str) -> str:
    normalized = str(value).strip().upper()
    if not normalized or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for character in normalized
    ):
        raise ValueError("code must be a stable uppercase identifier")
    return normalized


def _normalized_work_ids(
    values: frozenset[str] | None,
    *,
    field_name: str,
) -> frozenset[str] | None:
    if values is None:
        return None
    normalized = frozenset(
        value.strip()
        for value in values
        if isinstance(value, str) and value.strip()
    )
    if len(normalized) != len(values):
        raise ValueError(f"{field_name} must contain only unique non-empty strings")
    return normalized


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("timestamp must be datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


__all__ = [
    "LiveProtectionInputError",
    "LiveProtectionInputProvider",
    "LiveProtectionInputs",
    "LiveTrackingObservation",
    "LiveTradeOrchestrationService",
    "LiveWorkRunSummary",
]
