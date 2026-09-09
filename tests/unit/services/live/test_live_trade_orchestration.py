"""实盘保护 saga、持续观察与 NapCat outbox 的端到端测试。"""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from gribuki_trade.domain.exit_plans import ExitPlanDepth, ExitPlanEventType
from gribuki_trade.domain.live_records import LiveWorkKind
from gribuki_trade.features.deep_exit_planning import (
    DeepExitTimeframe,
    DeepSemanticAssessment,
    aggregate_completed_bars,
)
from gribuki_trade.features.technical import TechnicalBar
from gribuki_trade.ports.market_data import (
    FreshnessStatus,
    IntradayBar,
    MarketDataMeta,
    MinuteInterval,
    SourceSemantics,
)
from gribuki_trade.ports.notifier import NotificationTargetKind
from gribuki_trade.reporting.contracts import ReportKind, validate_text_report_contract
from gribuki_trade.services.exit.exit_plan_lifecycle import ExitPlanLifecycleService
from gribuki_trade.services.live.live_market_tracking import LiveMarketTrackingCycleService
from gribuki_trade.services.live.live_trade_orchestration import (
    LiveProtectionInputError,
    LiveProtectionInputs,
    LiveTradeOrchestrationService,
    LiveWorkRunSummary,
)
from gribuki_trade.services.live.live_trade_records import (
    LiveTradeRecordService,
    parse_onebot_private_message,
)
from gribuki_trade.storage.execution.exit_plans import SQLiteExitPlanStore
from gribuki_trade.storage.execution.outbox import SQLiteOutbox
from gribuki_trade.storage.live_records.live_records import (
    LiveRecordStateError,
    SQLiteLiveRecordStore,
)

_NOW = datetime(2026, 8, 14, 6, 30, 30, tzinfo=UTC)
_SENDER = "123456"


def _bars(count: int = 450) -> tuple[TechnicalBar, ...]:
    first = _NOW.replace(second=0, microsecond=0) - timedelta(minutes=count)
    output: list[TechnicalBar] = []
    for index in range(count):
        end = first + timedelta(minutes=index + 1)
        drift = Decimal(index % 30) * Decimal("0.002")
        close = Decimal("10.00") + drift
        output.append(
            TechnicalBar(
                end_time=end,
                available_at=end + timedelta(seconds=3),
                open=close - Decimal("0.01"),
                high=close + Decimal("0.08"),
                low=close - Decimal("0.08"),
                close=close,
                volume=1000 + index,
            )
        )
    return tuple(output)


class _Inputs:
    def __init__(self, *, fail_once: bool = False) -> None:
        self.calls = 0
        self._fail_once = fail_once
        self._semantic: tuple[DeepSemanticAssessment, DeepSemanticAssessment] | None = None

    async def prepare(self, _fill, *, requested_at: datetime) -> LiveProtectionInputs:
        self.calls += 1
        if self._fail_once and self.calls == 1:
            raise LiveProtectionInputError("MARKET_DATA_TEMPORARY", retryable=True)
        bars = _bars()
        frames = (
            DeepExitTimeframe(
                "1m",
                aggregate_completed_bars(bars, interval_minutes=1),
                Decimal("0.2"),
                timedelta(minutes=5),
            ),
            DeepExitTimeframe(
                "5m",
                aggregate_completed_bars(bars, interval_minutes=5),
                Decimal("0.3"),
                timedelta(minutes=10),
            ),
            DeepExitTimeframe(
                "15m",
                aggregate_completed_bars(bars, interval_minutes=15),
                Decimal("0.5"),
                timedelta(minutes=20),
            ),
        )
        baseline = DeepSemanticAssessment(
            assessment_id="baseline-live",
            system="baseline",
            score=Decimal("0.3"),
            confidence=Decimal("0.8"),
            market_data_as_of=_NOW - timedelta(seconds=5),
            evidence_ids=("baseline-evidence",),
        )
        adversarial = DeepSemanticAssessment(
            assessment_id="adversarial-live",
            system="adversarial",
            score=Decimal("0.7"),
            confidence=Decimal("0.9"),
            market_data_as_of=_NOW - timedelta(seconds=5),
            evidence_ids=("adversarial-evidence",),
        )
        self._semantic = (baseline, adversarial)
        return LiveProtectionInputs(
            decision_at=_NOW,
            bars=bars,
            technical_invalidation_price=Decimal("9.70"),
            time_exit_at=_NOW + timedelta(days=7),
            strategy_version="live-test@1",
            deep_timeframes=frames,
            baseline_assessment=baseline,
            adversarial_assessment=adversarial,
        )

    async def assess_deep(
        self,
        _fill,
        *,
        inputs: LiveProtectionInputs,
    ) -> tuple[DeepSemanticAssessment, DeepSemanticAssessment]:
        assert inputs.deep_timeframes
        assert self._semantic is not None
        return self._semantic


class _BlockingFailingDeepInputs(_Inputs):
    def __init__(self) -> None:
        super().__init__()
        self.deep_started = asyncio.Event()
        self.release_deep = asyncio.Event()

    async def prepare(self, fill, *, requested_at: datetime) -> LiveProtectionInputs:
        prepared = await super().prepare(fill, requested_at=requested_at)
        return LiveProtectionInputs(
            decision_at=prepared.decision_at,
            bars=prepared.bars,
            technical_invalidation_price=prepared.technical_invalidation_price,
            time_exit_at=prepared.time_exit_at,
            strategy_version=prepared.strategy_version,
            deep_timeframes=prepared.deep_timeframes,
            quick_config=prepared.quick_config,
        )

    async def assess_deep(
        self,
        _fill,
        *,
        inputs: LiveProtectionInputs,
    ) -> tuple[DeepSemanticAssessment, DeepSemanticAssessment]:
        assert inputs.baseline_assessment is None
        self.deep_started.set()
        await self.release_deep.wait()
        raise LiveProtectionInputError("SLOW_DEEP_UNAVAILABLE", retryable=True)


def _message(message_id: int, text: str, *, occurred_at: datetime = _NOW):
    return parse_onebot_private_message(
        {
            "post_type": "message",
            "message_type": "private",
            "sub_type": "friend",
            "self_id": 654321,
            "user_id": int(_SENDER),
            "message_id": message_id,
            "time": int(occurred_at.timestamp()),
            "raw_message": text,
        }
    )


def _confirmed_buy(
    store: SQLiteLiveRecordStore,
    *,
    command_id: str = "buy-live-1",
    external_order_id: str = "broker-live-1",
    external_fill_id: str = "broker-execution-live-1",
    message_offset: int = 0,
):
    service = LiveTradeRecordService(
        store,
        allowed_sender_ids=frozenset({_SENDER}),
        execution_session_validator=lambda _executed_at: True,
    )
    proposal = (
        f"GT-LIVE/1|command_id={command_id}|account=live-main|side=BUY"
        "|symbol=600000.SH|quantity=100|price=10.20|instrument=STOCK"
        "|executed_at=2026-08-14T14:29:00+08:00"
        "|commission=5.00|transfer_fee=0.10|stamp_tax=0.00"
        f"|external_order_id={external_order_id}"
        f"|external_fill_id={external_fill_id}"
    )
    proposed = service.ingest(_message(1 + message_offset, proposal), received_at=_NOW)
    return service.ingest(
        _message(
            2 + message_offset,
            f"GT-LIVE-CONFIRM/1|command_id={command_id}|fingerprint={proposed.fingerprint}",
        ),
        received_at=_NOW,
    )


def test_process_due_work_can_fence_claim_to_exact_confirmed_buy(
    tmp_path: Path,
) -> None:
    with (
        SQLiteLiveRecordStore(tmp_path / "live.sqlite3") as live,
        SQLiteExitPlanStore(tmp_path / "exit.sqlite3") as exits,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
    ):
        first = _confirmed_buy(live)
        second = _confirmed_buy(
            live,
            command_id="buy-live-2",
            external_order_id="broker-live-2",
            external_fill_id="broker-execution-live-2",
            message_offset=10,
        )
        assert first.protection_work_id is not None
        assert second.protection_work_id is not None
        service = LiveTradeOrchestrationService(
            live_store=live,
            exit_lifecycle=ExitPlanLifecycleService(exits),
            protection_inputs=_Inputs(),
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )

        summary = asyncio.run(
            service.process_due_work(
                now=_NOW,
                kinds=frozenset({LiveWorkKind.BUILD_PROTECTION}),
                work_ids=frozenset({second.protection_work_id}),
                limit=1,
            )
        )

        assert summary.claimed == 1
        assert summary.completed_work_ids == (second.protection_work_id,)
        states = {item.work_id: item.status.value for item in live.work_items("live-main")}
        assert states[first.protection_work_id] == "PENDING"
        assert states[second.protection_work_id] == "COMPLETED"


def _deep_due_at(store: SQLiteLiveRecordStore) -> datetime:
    return next(
        item.available_at
        for item in store.work_items("live-main", include_terminal=True)
        if item.kind is LiveWorkKind.BUILD_DEEP_PROTECTION
    )


def _active_tracking_plan(
    exits: SQLiteExitPlanStore,
    tracking,
):
    assert tracking.plan_stream_id is not None
    return ExitPlanLifecycleService(exits).active_plan(tracking.plan_stream_id)


def test_buy_work_builds_quick_deep_then_barrier_queues_napcat(
    tmp_path: Path,
) -> None:
    with (
        SQLiteLiveRecordStore(tmp_path / "live.sqlite3") as live,
        SQLiteExitPlanStore(tmp_path / "exit.sqlite3") as exits,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
    ):
        confirmed = _confirmed_buy(live)
        assert confirmed.protection_id is not None
        inputs = _Inputs()
        service = LiveTradeOrchestrationService(
            live_store=live,
            exit_lifecycle=ExitPlanLifecycleService(exits),
            protection_inputs=inputs,
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )

        built = asyncio.run(service.process_due_work(now=_NOW))
        assert built.claimed == built.completed == 1
        deep = asyncio.run(
            service.process_due_work(
                now=_deep_due_at(live),
                kinds=frozenset({LiveWorkKind.BUILD_DEEP_PROTECTION}),
            )
        )
        assert deep.claimed == deep.completed == 1
        tracking = live.tracking("live-main")[0]
        assert tracking.plan_ready is True
        active = _active_tracking_plan(exits, tracking)
        assert active.depth.value == "DEEP"
        assert active.state.value == "CONFIRMED"
        assert outbox.list_items() == ()

        observed = datetime(2026, 8, 17, 2, 1, tzinfo=UTC)
        with pytest.raises(LiveRecordStateError) as captured:
            live.queue_exit_alert(
                protection_id=tracking.protection_id,
                command_id=tracking.buy_command_id,
                plan_stream_id=confirmed.protection_id,
                bar_end=observed - timedelta(minutes=1),
                observed_at=observed,
                payload={"text": "stale QUICK alert"},
            )
        assert captured.value.code == "PROTECTION_PLAN_CHANGED"
        stop_bar = TechnicalBar(
            end_time=observed - timedelta(seconds=10),
            available_at=observed - timedelta(seconds=5),
            open=active.stop_price + Decimal("0.05"),
            high=active.stop_price + Decimal("0.10"),
            low=active.stop_price - Decimal("0.01"),
            close=active.stop_price,
            volume=1000,
        )
        observation = service.observe_completed_bar(
            account_id="live-main",
            symbol="600000.SH",
            bar=stop_bar,
            observed_at=observed,
        )
        assert len(observation.queued_alert_work_ids) == 1
        assert observation.observations[0].order_created is False
        assert outbox.list_items() == ()

        delivered = asyncio.run(service.process_due_work(now=observed))
        assert delivered.completed == 1
        notification = outbox.list_items()[0].notification
        validate_text_report_contract(ReportKind.INTRADAY_ALERT, notification.text)
        assert "系统未创建、也无权创建券商卖单" in notification.text
        assert "原单分析器加权评分：0.24" in notification.text
        assert "对抗分析器加权评分：0.63" in notification.text
        assert "生产采用：结构化对抗分析器" in notification.text


def test_failed_protection_work_retries_and_recovers_without_duplicate_plan(
    tmp_path: Path,
) -> None:
    with (
        SQLiteLiveRecordStore(tmp_path / "live.sqlite3") as live,
        SQLiteExitPlanStore(tmp_path / "exit.sqlite3") as exits,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
    ):
        confirmed = _confirmed_buy(live)
        inputs = _Inputs(fail_once=True)
        service = LiveTradeOrchestrationService(
            live_store=live,
            exit_lifecycle=ExitPlanLifecycleService(exits),
            protection_inputs=inputs,
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )

        first = asyncio.run(service.process_due_work(now=_NOW))
        assert first.retried == 1
        assert live.tracking("live-main")[0].plan_ready is False
        second = asyncio.run(service.process_due_work(now=_NOW + timedelta(seconds=30)))
        assert second.claimed == 0
        recovered = asyncio.run(service.process_due_work(now=_NOW + timedelta(minutes=1)))
        assert recovered.completed == 1
        assert inputs.calls == 2
        assert live.tracking("live-main")[0].plan_ready is True
        assert confirmed.protection_id is not None
        history = exits.events(confirmed.protection_id)
        assert len({event.idempotency_key for event in history}) == len(history)


def test_quick_plan_is_trackable_while_deep_is_slow_and_survives_deep_failure(
    tmp_path: Path,
) -> None:
    with (
        SQLiteLiveRecordStore(tmp_path / "live.sqlite3") as live,
        SQLiteExitPlanStore(tmp_path / "exit.sqlite3") as exits,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
    ):
        confirmed = _confirmed_buy(live)
        inputs = _BlockingFailingDeepInputs()
        service = LiveTradeOrchestrationService(
            live_store=live,
            exit_lifecycle=ExitPlanLifecycleService(exits),
            protection_inputs=inputs,
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )

        async def exercise() -> None:
            quick = await service.process_due_work(
                now=_NOW,
                kinds=frozenset({LiveWorkKind.BUILD_PROTECTION}),
            )
            assert quick.completed == 1
            build_task = asyncio.create_task(
                service.process_due_work(
                    now=_deep_due_at(live),
                    kinds=frozenset({LiveWorkKind.BUILD_DEEP_PROTECTION}),
                )
            )
            await asyncio.wait_for(inputs.deep_started.wait(), timeout=1)

            tracking = live.tracking("live-main")[0]
            assert tracking.plan_ready is True
            active = _active_tracking_plan(exits, tracking)
            assert active.depth.value == "QUICK"
            observed = datetime(2026, 8, 17, 2, 1, tzinfo=UTC)
            queued: list[str] = []
            for offset in (20, 10):
                stop_bar = TechnicalBar(
                    end_time=observed - timedelta(seconds=offset),
                    available_at=observed - timedelta(seconds=offset - 5),
                    open=active.stop_price + Decimal("0.05"),
                    high=active.stop_price + Decimal("0.10"),
                    low=active.stop_price - Decimal("0.01"),
                    close=active.stop_price,
                    volume=1000,
                )
                observation = service.observe_completed_bar(
                    account_id="live-main",
                    symbol="600000.SH",
                    bar=stop_bar,
                    observed_at=observed,
                )
                assert len(observation.observations) == 1
                assert observation.observations[0].order_created is False
                queued.extend(observation.queued_alert_work_ids)
            assert len(queued) == 2
            assert live.tracking("live-main")[0].last_observed_bar_end == (
                observed - timedelta(seconds=10)
            )

            inputs.release_deep.set()
            summary = await build_task
            assert summary.retried == 1

        asyncio.run(exercise())

        assert confirmed.protection_id is not None
        tracking = live.tracking("live-main")[0]
        assert tracking.plan_ready is True
        assert _active_tracking_plan(
            exits,
            live.tracking("live-main")[0],
        ).depth.value == "QUICK"
        build_work = next(
            item
            for item in live.work_items("live-main", include_terminal=True)
            if item.kind is LiveWorkKind.BUILD_DEEP_PROTECTION
        )
        assert build_work.status.value == "RETRY"
        delivered = asyncio.run(
            service.process_due_work(
                now=datetime(2026, 8, 17, 2, 1, tzinfo=UTC),
                kinds=frozenset({LiveWorkKind.DELIVER_EXIT_ALERT}),
            )
        )
        assert delivered.completed == 2
        quick_text = outbox.list_items()[0].notification.text
        assert "原单分析器加权评分：不可用" in quick_text
        assert "对抗分析器加权评分：不可用" in quick_text
        assert "生产采用：不可用" in quick_text


def test_crash_after_deep_commit_recovers_without_creating_a_new_plan_version(
    tmp_path: Path,
    monkeypatch,
) -> None:
    with (
        SQLiteLiveRecordStore(tmp_path / "live.sqlite3") as live,
        SQLiteExitPlanStore(tmp_path / "exit.sqlite3") as exits,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
    ):
        confirmed = _confirmed_buy(live)
        service = LiveTradeOrchestrationService(
            live_store=live,
            exit_lifecycle=ExitPlanLifecycleService(exits),
            protection_inputs=_Inputs(),
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )
        original = live.complete_protection_work
        calls = 0

        def fail_after_exit_commit(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError("simulated cross-database crash")
            return original(*args, **kwargs)

        monkeypatch.setattr(live, "complete_protection_work", fail_after_exit_commit)
        assert asyncio.run(
            service.process_due_work(
                now=_NOW,
                kinds=frozenset({LiveWorkKind.BUILD_PROTECTION}),
            )
        ).completed == 1
        failed = asyncio.run(
            service.process_due_work(
                now=_deep_due_at(live),
                kinds=frozenset({LiveWorkKind.BUILD_DEEP_PROTECTION}),
            )
        )
        assert failed.retried == 1
        assert confirmed.protection_id is not None
        assert _active_tracking_plan(exits, live.tracking("live-main")[0]).version == 1

        recovered = asyncio.run(
            service.process_due_work(
                now=max(_deep_due_at(live), datetime.now(UTC)) + timedelta(minutes=1),
                kinds=frozenset({LiveWorkKind.BUILD_DEEP_PROTECTION}),
            )
        )
        assert recovered.completed == 1
        tracking = live.tracking("live-main")[0]
        active = _active_tracking_plan(exits, tracking)
        assert active.version == 2
        replacements = [
            event
            for event in exits.events(tracking.plan_stream_id or "")
            if event.event_type is ExitPlanEventType.PLAN_REPLACED
        ]
        assert len(replacements) == 1


def test_attempt_takeover_fences_stale_cross_database_deep_candidate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    live_path = tmp_path / "live.sqlite3"
    exit_path = tmp_path / "exit.sqlite3"
    with (
        SQLiteLiveRecordStore(live_path) as old_live,
        SQLiteLiveRecordStore(live_path) as new_live,
        SQLiteExitPlanStore(exit_path) as old_exits,
        SQLiteExitPlanStore(exit_path) as new_exits,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
    ):
        _confirmed_buy(old_live)
        old_lifecycle = ExitPlanLifecycleService(old_exits)
        old_service = LiveTradeOrchestrationService(
            live_store=old_live,
            exit_lifecycle=old_lifecycle,
            protection_inputs=_Inputs(),
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )
        assert asyncio.run(
            old_service.process_due_work(
                now=_NOW,
                kinds=frozenset({LiveWorkKind.BUILD_PROTECTION}),
            )
        ).completed == 1

        paused_after_renewal = threading.Event()
        release_old_worker = threading.Event()
        original_build = old_lifecycle.build_and_apply_deep

        def paused_build(*args, **kwargs):
            paused_after_renewal.set()
            assert release_old_worker.wait(timeout=5)
            return original_build(*args, **kwargs)

        monkeypatch.setattr(old_lifecycle, "build_and_apply_deep", paused_build)
        old_result: dict[str, object] = {}

        def run_old_worker() -> None:
            old_result["summary"] = asyncio.run(
                old_service.process_due_work(
                    now=_deep_due_at(old_live),
                    kinds=frozenset({LiveWorkKind.BUILD_DEEP_PROTECTION}),
                )
            )

        old_thread = threading.Thread(target=run_old_worker, daemon=True)
        old_thread.start()
        assert paused_after_renewal.wait(timeout=5)
        attempt_one = next(
            item
            for item in new_live.work_items("live-main", include_terminal=True)
            if item.kind is LiveWorkKind.BUILD_DEEP_PROTECTION
        )
        assert attempt_one.attempts == 1
        assert attempt_one.lease_until is not None

        new_service = LiveTradeOrchestrationService(
            live_store=new_live,
            exit_lifecycle=ExitPlanLifecycleService(new_exits),
            protection_inputs=_Inputs(),
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )
        taken_over = asyncio.run(
            new_service.process_due_work(
                now=attempt_one.lease_until + timedelta(seconds=1),
                kinds=frozenset({LiveWorkKind.BUILD_DEEP_PROTECTION}),
            )
        )
        assert taken_over.completed == 1
        winning_stream = new_live.tracking("live-main")[0].plan_stream_id
        assert winning_stream is not None

        release_old_worker.set()
        old_thread.join(timeout=5)
        assert not old_thread.is_alive()
        stale_summary = old_result["summary"]
        assert isinstance(stale_summary, LiveWorkRunSummary)
        assert stale_summary.retried == 1
        tracking = new_live.tracking("live-main")[0]
        assert tracking.plan_stream_id == winning_stream
        assert _active_tracking_plan(new_exits, tracking).depth is ExitPlanDepth.DEEP
        deep_streams = tuple(
            stream_id
            for stream_id in new_exits.protection_ids("live-main", "600000.SH")
            if ExitPlanLifecycleService(new_exits).active_plan(stream_id).depth
            is ExitPlanDepth.DEEP
        )
        assert len(deep_streams) == 2
        assert winning_stream in deep_streams
        stale_stream = next(item for item in deep_streams if item != winning_stream)
        assert stale_stream != tracking.plan_stream_id
        final_work = next(
            item
            for item in new_live.work_items("live-main", include_terminal=True)
            if item.kind is LiveWorkKind.BUILD_DEEP_PROTECTION
        )
        assert final_work.attempts == 2
        assert final_work.status.value == "COMPLETED"


def test_deep_timeout_is_durable_and_restart_retries_from_quick(
    tmp_path: Path,
) -> None:
    live_path = tmp_path / "live.sqlite3"
    exit_path = tmp_path / "exit.sqlite3"
    outbox_path = tmp_path / "outbox.sqlite3"
    blocking = _BlockingFailingDeepInputs()
    with (
        SQLiteLiveRecordStore(live_path) as live,
        SQLiteExitPlanStore(exit_path) as exits,
        SQLiteOutbox(outbox_path) as outbox,
    ):
        _confirmed_buy(live)
        service = LiveTradeOrchestrationService(
            live_store=live,
            exit_lifecycle=ExitPlanLifecycleService(exits),
            protection_inputs=blocking,
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )
        assert asyncio.run(
            service.process_due_work(
                now=_NOW,
                kinds=frozenset({LiveWorkKind.BUILD_PROTECTION}),
            )
        ).completed == 1
        timed_out = asyncio.run(
            service.process_due_work(
                now=_deep_due_at(live),
                kinds=frozenset({LiveWorkKind.BUILD_DEEP_PROTECTION}),
                work_timeout_seconds=0.01,
            )
        )
        assert timed_out.retried == 1
        tracking = live.tracking("live-main")[0]
        assert _active_tracking_plan(exits, tracking).depth is ExitPlanDepth.QUICK
        retry = next(
            item
            for item in live.work_items("live-main", include_terminal=True)
            if item.kind is LiveWorkKind.BUILD_DEEP_PROTECTION
        )
        assert retry.status.value == "RETRY"
        retry_at = retry.available_at

    with (
        SQLiteLiveRecordStore(live_path) as live,
        SQLiteExitPlanStore(exit_path) as exits,
        SQLiteOutbox(outbox_path) as outbox,
    ):
        recovered_service = LiveTradeOrchestrationService(
            live_store=live,
            exit_lifecycle=ExitPlanLifecycleService(exits),
            protection_inputs=_Inputs(),
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )
        recovered = asyncio.run(
            recovered_service.process_due_work(
                now=retry_at,
                kinds=frozenset({LiveWorkKind.BUILD_DEEP_PROTECTION}),
            )
        )
        assert recovered.completed == 1
        tracking = live.tracking("live-main")[0]
        assert _active_tracking_plan(exits, tracking).depth is ExitPlanDepth.DEEP


def test_confirmed_full_sell_atomically_closes_tracking_and_exit_stream(
    tmp_path: Path,
) -> None:
    with (
        SQLiteLiveRecordStore(tmp_path / "live.sqlite3") as live,
        SQLiteExitPlanStore(tmp_path / "exit.sqlite3") as exits,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
    ):
        confirmed = _confirmed_buy(live)
        service = LiveTradeOrchestrationService(
            live_store=live,
            exit_lifecycle=ExitPlanLifecycleService(exits),
            protection_inputs=_Inputs(),
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )
        assert asyncio.run(service.process_due_work(now=_NOW)).completed == 1
        assert asyncio.run(
            service.process_due_work(
                now=_deep_due_at(live),
                kinds=frozenset({LiveWorkKind.BUILD_DEEP_PROTECTION}),
            )
        ).completed == 1
        sell_at = datetime(2026, 8, 17, 2, 1, tzinfo=UTC)
        record = LiveTradeRecordService(
            live,
            allowed_sender_ids=frozenset({_SENDER}),
            execution_session_validator=lambda _executed_at: True,
        )
        proposal = (
            "GT-LIVE/1|command_id=sell-live-1|account=live-main|side=SELL"
            "|symbol=600000.SH|quantity=100|price=10.50|instrument=STOCK"
            "|executed_at=2026-08-17T10:00:00+08:00"
            "|commission=5.00|transfer_fee=0.10|stamp_tax=0.53"
            "|external_order_id=broker-live-sell-1"
            "|external_fill_id=broker-execution-live-sell-1"
        )
        pending = record.ingest(
            _message(3, proposal, occurred_at=sell_at),
            received_at=sell_at,
        )
        sold = record.ingest(
            _message(
                4,
                f"GT-LIVE-CONFIRM/1|command_id=sell-live-1|fingerprint={pending.fingerprint}",
                occurred_at=sell_at,
            ),
            received_at=sell_at,
        )
        assert sold.analysis_required is False
        assert record.snapshot("live-main").positions[0].quantity == 0
        assert live.tracking("live-main", active_only=False)[0].remaining_quantity == 0
        closed = asyncio.run(
            service.process_due_work(
                now=sell_at,
                kinds=frozenset({LiveWorkKind.CLOSE_PROTECTION}),
            )
        )
        assert closed.completed == 1
        assert confirmed.protection_id is not None
        tracking = live.tracking("live-main", active_only=False)[0]
        assert tracking.plan_stream_id is not None
        assert (
            exits.events(tracking.plan_stream_id)[-1].event_type
            is ExitPlanEventType.POSITION_CLOSED
        )


def test_full_sell_fences_inflight_deep_candidate_before_pointer_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    live_path = tmp_path / "live.sqlite3"
    with (
        SQLiteLiveRecordStore(live_path) as worker_live,
        SQLiteLiveRecordStore(live_path) as seller_live,
        SQLiteExitPlanStore(tmp_path / "exit.sqlite3") as exits,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
    ):
        confirmed = _confirmed_buy(worker_live)
        service = LiveTradeOrchestrationService(
            live_store=worker_live,
            exit_lifecycle=ExitPlanLifecycleService(exits),
            protection_inputs=_Inputs(),
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )
        quick = asyncio.run(
            service.process_due_work(
                now=_NOW,
                kinds=frozenset({LiveWorkKind.BUILD_PROTECTION}),
            )
        )
        assert quick.completed == 1
        before_sell = worker_live.tracking("live-main")[0]
        quick_stream_id = before_sell.plan_stream_id
        assert quick_stream_id is not None
        assert _active_tracking_plan(exits, before_sell).depth is ExitPlanDepth.QUICK

        candidate_finished = threading.Event()
        release_old_worker = threading.Event()
        original_complete = worker_live.complete_protection_work

        def pause_before_live_pointer_switch(*args, **kwargs):
            candidate_finished.set()
            assert release_old_worker.wait(timeout=5)
            return original_complete(*args, **kwargs)

        monkeypatch.setattr(
            worker_live,
            "complete_protection_work",
            pause_before_live_pointer_switch,
        )
        worker_result: dict[str, LiveWorkRunSummary] = {}

        def run_deep_worker() -> None:
            worker_result["summary"] = asyncio.run(
                service.process_due_work(
                    now=_deep_due_at(worker_live),
                    kinds=frozenset({LiveWorkKind.BUILD_DEEP_PROTECTION}),
                )
            )

        worker_thread = threading.Thread(target=run_deep_worker, daemon=True)
        worker_thread.start()
        assert candidate_finished.wait(timeout=5)

        sell_at = datetime(2026, 8, 17, 2, 1, tzinfo=UTC)
        record = LiveTradeRecordService(
            seller_live,
            allowed_sender_ids=frozenset({_SENDER}),
            execution_session_validator=lambda _executed_at: True,
        )
        sell_text = (
            "GT-LIVE/1|command_id=sell-during-deep|account=live-main|side=SELL"
            "|symbol=600000.SH|quantity=100|price=10.50|instrument=STOCK"
            "|executed_at=2026-08-17T10:00:00+08:00"
            "|commission=5.00|transfer_fee=0.10|stamp_tax=0.53"
            "|external_order_id=broker-order-sell-during-deep"
            "|external_fill_id=broker-execution-sell-during-deep"
        )
        pending = record.ingest(
            _message(30, sell_text, occurred_at=sell_at),
            received_at=sell_at,
        )
        record.ingest(
            _message(
                31,
                "GT-LIVE-CONFIRM/1|command_id=sell-during-deep|"
                f"fingerprint={pending.fingerprint}",
                occurred_at=sell_at,
            ),
            received_at=sell_at,
        )

        fenced = next(
            item
            for item in seller_live.work_items("live-main", include_terminal=True)
            if item.kind is LiveWorkKind.BUILD_DEEP_PROTECTION
        )
        assert fenced.status.value == "COMPLETED"
        assert fenced.result_code == "POSITION_CLOSED_WORK_FENCED"
        closed_tracking = seller_live.tracking("live-main", active_only=False)[0]
        assert closed_tracking.remaining_quantity == 0
        assert closed_tracking.plan_stream_id == quick_stream_id
        with pytest.raises(LiveRecordStateError) as captured:
            seller_live.queue_exit_alert(
                protection_id=closed_tracking.protection_id,
                command_id=closed_tracking.buy_command_id,
                plan_stream_id=quick_stream_id,
                bar_end=sell_at,
                observed_at=sell_at,
                payload={"text": "must not be queued"},
            )
        assert captured.value.code == "PROTECTION_POSITION_CLOSED"

        release_old_worker.set()
        worker_thread.join(timeout=5)
        assert not worker_thread.is_alive()
        assert worker_result["summary"].completed == 1
        after_worker = seller_live.tracking("live-main", active_only=False)[0]
        assert after_worker.plan_stream_id == quick_stream_id
        assert confirmed.protection_id == after_worker.protection_id
        candidate_streams = tuple(
            stream_id
            for stream_id in exits.protection_ids("live-main", "600000.SH")
            if stream_id != quick_stream_id
        )
        assert len(candidate_streams) == 1
        candidate_stream_id = candidate_streams[0]
        assert ExitPlanLifecycleService(exits).active_plan(
            candidate_stream_id
        ).depth is ExitPlanDepth.DEEP
        assert candidate_stream_id != after_worker.plan_stream_id

        closed = asyncio.run(
            service.process_due_work(
                now=sell_at,
                kinds=frozenset({LiveWorkKind.CLOSE_PROTECTION}),
            )
        )
        assert closed.completed == 1
        assert exits.events(quick_stream_id)[-1].event_type is ExitPlanEventType.POSITION_CLOSED
        assert (
            exits.events(candidate_stream_id)[-1].event_type
            is not ExitPlanEventType.POSITION_CLOSED
        )


def test_application_tracking_cycle_fetches_completed_bar_and_delivers_alert(
    tmp_path: Path,
) -> None:
    with (
        SQLiteLiveRecordStore(tmp_path / "live.sqlite3") as live,
        SQLiteExitPlanStore(tmp_path / "exit.sqlite3") as exits,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
    ):
        _confirmed_buy(live)
        orchestration = LiveTradeOrchestrationService(
            live_store=live,
            exit_lifecycle=ExitPlanLifecycleService(exits),
            protection_inputs=_Inputs(),
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )
        assert asyncio.run(orchestration.process_due_work(now=_NOW)).completed == 1
        tracking = live.tracking("live-main")[0]
        active = _active_tracking_plan(exits, tracking)
        observed = datetime(2026, 8, 17, 2, 1, tzinfo=UTC)

        class _TrackingMarket:
            async def fetch_trade_prints_async(self, _symbol):
                return ()

            async def fetch_intraday_bars_async(
                self,
                symbol,
                _start,
                _end,
                *,
                interval=MinuteInterval.ONE_MINUTE,
                completed_only=True,
            ):
                assert interval is MinuteInterval.ONE_MINUTE
                assert completed_only is True
                end = observed - timedelta(seconds=10)
                return (
                    IntradayBar(
                        symbol=symbol,
                        start_at=end - timedelta(minutes=1),
                        end_at=end,
                        interval=MinuteInterval.ONE_MINUTE,
                        open=active.stop_price + Decimal("0.05"),
                        high=active.stop_price + Decimal("0.10"),
                        low=active.stop_price - Decimal("0.01"),
                        close=active.stop_price,
                        volume_lots=1000,
                        amount=active.stop_price * 1000,
                        vwap=active.stop_price,
                        is_closed=True,
                        meta=MarketDataMeta(
                            provider="tracking-test",
                            semantics=SourceSemantics.AGGREGATED_MINUTE_BAR,
                            fetched_at=observed - timedelta(seconds=5),
                            provider_timestamp=end,
                            freshness=FreshnessStatus.CURRENT,
                        ),
                    ),
                )

        cycle = LiveMarketTrackingCycleService(
            live_store=live,
            market_data=_TrackingMarket(),
            orchestration=orchestration,
        )
        result = asyncio.run(cycle.run_once(observed_at=observed))

        assert result.target_count == result.successful_targets == 1
        assert result.fetched_bars == result.barrier_observations == 1
        assert result.queued_alerts == result.delivery.completed == 1
        assert result.failures == ()
        item = outbox.list_items()[0]
        validate_text_report_contract(ReportKind.INTRADAY_ALERT, item.notification.text)


def test_alert_outbox_boundary_recovers_without_duplicate_notification(
    tmp_path: Path,
    monkeypatch,
) -> None:
    with (
        SQLiteLiveRecordStore(tmp_path / "live.sqlite3") as live,
        SQLiteExitPlanStore(tmp_path / "exit.sqlite3") as exits,
        SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox,
    ):
        _confirmed_buy(live)
        service = LiveTradeOrchestrationService(
            live_store=live,
            exit_lifecycle=ExitPlanLifecycleService(exits),
            protection_inputs=_Inputs(),
            outbox=outbox,
            notification_target_kind=NotificationTargetKind.PRIVATE,
            notification_target_id=_SENDER,
        )
        assert asyncio.run(service.process_due_work(now=_NOW)).completed == 1
        tracking = live.tracking("live-main")[0]
        active = _active_tracking_plan(exits, tracking)
        observed = datetime(2026, 8, 17, 2, 1, tzinfo=UTC)
        bar = TechnicalBar(
            end_time=observed - timedelta(seconds=10),
            available_at=observed - timedelta(seconds=5),
            open=active.stop_price + Decimal("0.05"),
            high=active.stop_price + Decimal("0.10"),
            low=active.stop_price - Decimal("0.01"),
            close=active.stop_price,
            volume=1000,
        )
        queued = service.observe_completed_bar(
            account_id="live-main",
            symbol="600000.SH",
            bar=bar,
            observed_at=observed,
        )
        alert_work_id = queued.queued_alert_work_ids[0]
        original = live.complete_work
        attempts = 0

        def fail_after_outbox_commit(work_id, **kwargs):
            nonlocal attempts
            attempts += 1
            if work_id == alert_work_id and attempts == 1:
                raise OSError("simulated outbox boundary crash")
            return original(work_id, **kwargs)

        monkeypatch.setattr(live, "complete_work", fail_after_outbox_commit)
        failed = asyncio.run(
            service.process_due_work(
                now=observed,
                kinds=frozenset({LiveWorkKind.DELIVER_EXIT_ALERT}),
            )
        )
        assert failed.retried == 1
        assert len(outbox.list_items()) == 1

        recovered = asyncio.run(
            service.process_due_work(
                now=observed + timedelta(minutes=1),
                kinds=frozenset({LiveWorkKind.DELIVER_EXIT_ALERT}),
            )
        )
        assert recovered.completed == 1
        assert len(outbox.list_items()) == 1
        assert outbox.list_items()[0].notification.idempotency_key == alert_work_id
