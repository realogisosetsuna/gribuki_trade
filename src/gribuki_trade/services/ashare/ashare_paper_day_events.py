"""A 股 PAPER 运行器的事件持久化、sidecar 和通知发布边界。

本模块只负责把已经构造好的 PAPER 事件写入窄存储接口，并维护 status.json、
JSONL 修复和通知 outbox；交易日历、行情、撮合和风险编排留在运行器 facade。
"""

from __future__ import annotations

import json
import os
import time as time_module
from collections.abc import Callable, Mapping
from contextlib import suppress
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol

from gribuki_trade.domain.paper_day import (
    NewPaperDayEvent,
    PaperDayEvent,
    PaperDayPhase,
    PaperDayRunManifest,
    PaperDaySeverity,
    paper_day_target_hash,
)
from gribuki_trade.ports.notifier import NotificationTargetKind, OutboundNotification
from gribuki_trade.services.ashare import ashare_paper_day_notifications as _notifications
from gribuki_trade.services.ashare.ashare_paper_day_serialization import (
    _atomic_write_text,
    _aware_utc,
    _event_jsonl,
)
from gribuki_trade.services.notification_dispatch import (
    NotificationDispatchService,
    NotificationDispatchServiceError,
)
from gribuki_trade.storage.outbox import SQLiteOutbox
from gribuki_trade.storage.report_artifact_outbox import ReportArtifactStatus

_contractualize_paper_notification = _notifications._contractualize_paper_notification
_paper_notification_kind = _notifications._paper_notification_kind


def _append_and_sync_text(path: Path, text: str) -> None:
    """保留 runner facade 的历史 sidecar 注入钩子，支持故障恢复测试。"""

    from gribuki_trade.services.ashare import ashare_paper_day

    ashare_paper_day._append_and_sync_text(path, text)


class PaperDayStore(Protocol):
    """运行器及其测试替身使用的窄存储接口。"""

    def append_event(
        self,
        event: NewPaperDayEvent,
        *,
        owner_id: str,
        lease_checked_at: datetime | None = None,
    ) -> tuple[PaperDayEvent, bool]: ...

    def events(self, run_id: str) -> tuple[PaperDayEvent, ...]: ...

    def event_by_key(self, run_id: str, event_key: str) -> PaperDayEvent | None: ...

    def acquire_lease(
        self,
        run_id: str,
        owner_id: str,
        *,
        now: datetime,
        lease_for: timedelta,
    ) -> None: ...

    def renew_lease(
        self,
        run_id: str,
        owner_id: str,
        *,
        now: datetime,
        lease_for: timedelta,
    ) -> None: ...

    def release_lease(self, run_id: str, owner_id: str) -> bool: ...


class PaperDayEventPublisher:
    """先写日志，再幂等入队并立即派发文本。"""

    _SIDECAR_RETRY_DELAYS_SECONDS = (0.0, 0.01, 0.025, 0.05)

    def __init__(
        self,
        *,
        manifest: PaperDayRunManifest,
        store: PaperDayStore,
        outbox: SQLiteOutbox,
        dispatcher: NotificationDispatchService,
        target_kind: NotificationTargetKind,
        target_id: str,
        owner_id: str,
        clock: Callable[[], datetime],
        status_path: Path,
    ) -> None:
        self._manifest = manifest
        self._store = store
        self._outbox = outbox
        self._dispatcher = dispatcher
        self._target_kind = target_kind
        self._target_id = target_id
        if self._manifest.target_hash != paper_day_target_hash(
            channel="onebot",
            target_kind=target_kind.value,
            target_id=target_id,
        ):
            raise ValueError("notification target conflicts with the manifest")
        self._owner_id = owner_id
        self._clock = clock
        self._status_path = status_path
        self._delivery_projection = self._retained_delivery_projection()
        retained = self._store.events(self._manifest.run_id)
        self._events_by_key = {item.event_key: item for item in retained}
        self._event_count = len(retained)
        self._latest_event = retained[-1] if retained else None
        self._jsonl_repair_pending = not self._rebuild_jsonl(retained)

    async def emit(
        self,
        *,
        event_key: str,
        event_type: str,
        phase: PaperDayPhase,
        severity: PaperDaySeverity,
        payload: Mapping[str, object],
        notification_text: str | None = None,
        symbol: str | None = None,
        correlation_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> PaperDayEvent:
        now = _aware_utc(self._clock(), "clock")
        existing = self._events_by_key.get(event_key)
        if existing is not None:
            if existing.notification_required:
                retained = existing.payload.get("notification_text")
                if not isinstance(retained, str) or not retained:
                    raise RuntimeError("stored notification event has no text")
                self._ensure_notification(existing, retained)
            self._write_status(existing, created=False)
            return existing
        material = dict(payload)
        contracted_notification = notification_text
        if notification_text is not None:
            if not notification_text.strip():
                raise ValueError("notification_text must not be blank")
            report_kind = _paper_notification_kind(event_type)
            contracted_notification = _contractualize_paper_notification(
                kind=report_kind,
                event_type=event_type,
                raw_text=notification_text,
                payload=material,
                symbol=symbol,
                evidence_at=occurred_at or now,
            )
            if len(contracted_notification) > 7_900:
                raise ValueError("contractual PAPER notification exceeds OneBot limit")
            material["notification_text"] = contracted_notification
            material["report_kind"] = report_kind.value
            material["report_contract"] = "user-readable-report-contract@1"
        event, created = self._store.append_event(
            NewPaperDayEvent(
                run_id=self._manifest.run_id,
                event_key=event_key,
                event_type=event_type,
                phase=phase,
                severity=severity,
                occurred_at=occurred_at or now,
                known_at=now,
                notification_required=contracted_notification is not None,
                payload=material,
                symbol=symbol,
                correlation_id=correlation_id,
            ),
            owner_id=self._owner_id,
            lease_checked_at=now,
        )
        self._events_by_key[event.event_key] = event
        if created:
            self._event_count += 1
        if contracted_notification is not None:
            self._ensure_notification(event, contracted_notification)
            with suppress(NotificationDispatchServiceError):
                await self._dispatcher.dispatch_once(limit=50)
        self._write_status(event, created=created)
        return event

    def reconcile_notifications(self) -> int:
        """修复日志追加与 outbox 入队之间的崩溃边界。"""

        repaired = 0
        for event in sorted(
            self._events_by_key.values(),
            key=lambda item: item.sequence,
        ):
            if not event.notification_required:
                continue
            text = event.payload.get("notification_text")
            if not isinstance(text, str) or not text:
                raise RuntimeError("notification-required event has no retained text")
            key = self.notification_key(event)
            if self._outbox.get_by_key(key) is None:
                self._ensure_notification(event, text)
                repaired += 1
        return repaired

    async def dispatch_once(self) -> None:
        with suppress(NotificationDispatchServiceError):
            await self._dispatcher.dispatch_once(limit=50)

    def write_heartbeat(self) -> None:
        """刷新只读存活旁路文件，不触碰 SQLite。"""

        if self._latest_event is not None:
            self._write_status(self._latest_event, created=False)

    def write_delivery_projection(
        self,
        *,
        artifact_delivery_status: str,
        artifact_delivery_complete: bool,
        daily_review_delivery_complete: bool,
        notification_required: int,
        notification_sent: int,
        notification_gaps: int,
        text_notification_required: int,
        text_notification_sent: int,
        text_notification_gaps: int,
    ) -> None:
        """把最终双交付状态写入只读 sidecar，供 ``status`` 精确展示。"""

        self._delivery_projection = {
            "artifact_delivery_status": artifact_delivery_status,
            "artifact_delivery_complete": artifact_delivery_complete,
            "daily_review_delivery_complete": daily_review_delivery_complete,
            "notification_required": notification_required,
            "notification_sent": notification_sent,
            "notification_gaps": notification_gaps,
            "text_notification_required": text_notification_required,
            "text_notification_sent": text_notification_sent,
            "text_notification_gaps": text_notification_gaps,
        }
        if self._latest_event is not None:
            self._write_status(self._latest_event, created=False)

    def _ensure_notification(self, event: PaperDayEvent, text: str) -> None:
        self._outbox.enqueue(
            OutboundNotification(
                idempotency_key=self.notification_key(event),
                channel="onebot",
                target_kind=self._target_kind,
                target_id=self._target_id,
                text=text,
                created_at=event.known_at,
            )
        )

    def notification_key(self, event: PaperDayEvent) -> str:
        """返回一条日志事件的确定性 outbox 身份。"""

        return (
            f"paper-day:{self._manifest.run_id}:{event.event_id}:{self._manifest.target_hash[:12]}"
        )

    def _write_status(self, latest: PaperDayEvent, *, created: bool) -> None:
        self._latest_event = latest
        document = {
            "event_count": self._event_count,
            "latest_event": latest.event_type,
            "latest_known_at": latest.known_at.isoformat(),
            "phase": latest.phase.value,
            "process_heartbeat_at": _aware_utc(
                self._clock(),
                "clock",
            ).isoformat(),
            "process_id": os.getpid(),
            "run_id": self._manifest.run_id,
            "session_date": self._manifest.session_date.isoformat(),
            **self._delivery_projection,
        }
        status_text = json.dumps(document, ensure_ascii=False, sort_keys=True) + "\n"
        self._try_sidecar_io(lambda: _atomic_write_text(self._status_path, status_text))
        if self._jsonl_repair_pending:
            # SQLite 日志是权威来源。重建投影，避免先前部分或失败的追加造成
            # 序列缺口或重复。
            retained = self._store.events(self._manifest.run_id)
            self._jsonl_repair_pending = not self._rebuild_jsonl(retained)
            return
        if created:
            appended = self._try_sidecar_io(
                lambda: _append_and_sync_text(
                    self._status_path.with_name("session.log.jsonl"),
                    _event_jsonl(latest),
                )
            )
            self._jsonl_repair_pending = not appended

    def _retained_delivery_projection(self) -> dict[str, object]:
        """恢复同一 run 已写 sidecar 的交付投影，避免心跳覆盖终态。"""

        try:
            retained = json.loads(self._status_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return {}
        if not isinstance(retained, dict) or retained.get("run_id") != self._manifest.run_id:
            return {}
        output: dict[str, object] = {}
        bool_fields = {
            "artifact_delivery_complete",
            "daily_review_delivery_complete",
        }
        count_fields = {
            "notification_required",
            "notification_sent",
            "notification_gaps",
            "text_notification_required",
            "text_notification_sent",
            "text_notification_gaps",
        }
        status = retained.get("artifact_delivery_status")
        if isinstance(status, str) and status in {
            ReportArtifactStatus.PENDING.value,
            ReportArtifactStatus.SENT.value,
            ReportArtifactStatus.AMBIGUOUS.value,
            "NOT_CONFIGURED",
        }:
            output["artifact_delivery_status"] = status
        for field in bool_fields:
            value = retained.get(field)
            if isinstance(value, bool):
                output[field] = value
        for field in count_fields:
            value = retained.get(field)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                output[field] = value
        return output

    def _rebuild_jsonl(self, events: tuple[PaperDayEvent, ...]) -> bool:
        """依据权威日志修复任何不完整的旁路文件追加。"""

        projection = "".join(_event_jsonl(item) for item in events)
        return self._try_sidecar_io(
            lambda: _atomic_write_text(
                self._status_path.with_name("session.log.jsonl"),
                projection,
            )
        )

    @classmethod
    def _try_sidecar_io(cls, operation: Callable[[], None]) -> bool:
        """针对非权威旁路文件的尽力有界重试。

        Windows 读取方可能短暂阻止追加或原子替换。这类冲突绝不能把已提交的
        日志事件变成交易日中止；后续写入会改为依据 SQLite 重建 JSONL。
        """

        for attempt, delay in enumerate(cls._SIDECAR_RETRY_DELAYS_SECONDS):
            if delay:
                time_module.sleep(delay)
            try:
                operation()
            except OSError:
                if attempt + 1 == len(cls._SIDECAR_RETRY_DELAYS_SECONDS):
                    return False
            else:
                return True
        return False


