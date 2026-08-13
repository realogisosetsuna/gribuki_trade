import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from gribuki_trade.ports.notifier import (
    DeliveryReceipt,
    NotificationTargetKind,
    OutboundNotification,
)
from gribuki_trade.services.notification_dispatch import (
    NotificationDispatchService,
    NotificationDispatchServiceError,
)
from gribuki_trade.storage import OutboxStatus, SQLiteOutbox

NOW = datetime(2020, 8, 13, 4, 0, tzinfo=UTC)


def make_notification(number: int) -> OutboundNotification:
    return OutboundNotification(
        idempotency_key=f"signal:{number}",
        channel="test",
        target_kind=NotificationTargetKind.PRIVATE,
        target_id="12345",
        text=f"alert {number}",
        created_at=NOW,
    )


class RecordingNotifier:
    channel = "test"

    def __init__(self, on_send: Any = None) -> None:
        self.delivered_keys: list[str] = []
        self._on_send = on_send

    async def send(self, notification: OutboundNotification) -> DeliveryReceipt:
        await asyncio.sleep(0)
        self.delivered_keys.append(notification.idempotency_key)
        if self._on_send is not None:
            self._on_send()
        return DeliveryReceipt(
            channel=self.channel,
            provider_message_id=f"provider:{notification.idempotency_key}",
        )


def test_dispatch_once_serializes_concurrent_callers_without_duplicates(tmp_path) -> None:
    async def scenario() -> None:
        notifier = RecordingNotifier()
        with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
            outbox.enqueue(make_notification(1))
            service = NotificationDispatchService(outbox, {"test": notifier})

            summaries = await asyncio.gather(service.dispatch_once(), service.dispatch_once())

            assert sum(item.claimed for item in summaries) == 1
            assert sum(item.sent for item in summaries) == 1
            assert notifier.delivered_keys == ["signal:1"]
            stored = outbox.get_by_key("signal:1")
            assert stored is not None
            assert stored.status is OutboxStatus.SENT

    asyncio.run(scenario())


def test_finite_polling_returns_aggregate_structured_statistics(tmp_path) -> None:
    async def scenario() -> None:
        notifier = RecordingNotifier()
        with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
            outbox.enqueue(make_notification(1))
            outbox.enqueue(make_notification(2))
            service = NotificationDispatchService(outbox, {"test": notifier})

            statistics = await service.poll(max_cycles=3, poll_interval=0, limit=1)

            assert statistics.cycles_completed == 3
            assert statistics.claimed == 2
            assert statistics.sent == 2
            assert statistics.retry_scheduled == 0
            assert statistics.dead == 0
            assert statistics.expired == 0
            assert statistics.stop_requested is False
            assert statistics.reached_cycle_limit is True
            assert notifier.delivered_keys == ["signal:1", "signal:2"]

    asyncio.run(scenario())


def test_stop_request_prevents_the_next_polling_cycle(tmp_path) -> None:
    async def scenario() -> None:
        holder: dict[str, NotificationDispatchService] = {}
        notifier = RecordingNotifier(lambda: holder["service"].request_stop())
        with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
            outbox.enqueue(make_notification(1))
            outbox.enqueue(make_notification(2))
            service = NotificationDispatchService(outbox, {"test": notifier})
            holder["service"] = service

            statistics = await service.poll(max_cycles=10, poll_interval=60, limit=1)

            assert statistics.cycles_completed == 1
            assert statistics.sent == 1
            assert statistics.stop_requested is True
            assert statistics.reached_cycle_limit is False
            assert notifier.delivered_keys == ["signal:1"]
            assert len(outbox.list_items(status=OutboxStatus.PENDING)) == 1

    asyncio.run(scenario())


def test_reset_stop_explicitly_allows_a_later_finite_run(tmp_path) -> None:
    async def scenario() -> None:
        notifier = RecordingNotifier()
        with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
            outbox.enqueue(make_notification(1))
            service = NotificationDispatchService(outbox, {"test": notifier})
            service.request_stop()

            stopped = await service.poll(max_cycles=2, poll_interval=0)
            assert stopped.cycles_completed == 0

            service.reset_stop()
            resumed = await service.poll(max_cycles=1, poll_interval=0)
            assert resumed.sent == 1

    asyncio.run(scenario())


def test_stop_interrupts_a_long_poll_wait(tmp_path) -> None:
    async def scenario() -> None:
        with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
            service = NotificationDispatchService(outbox, {})
            task = asyncio.create_task(service.poll(max_cycles=10, poll_interval=60))
            await asyncio.sleep(0)
            service.request_stop()

            statistics = await asyncio.wait_for(task, timeout=1)
            assert statistics.cycles_completed == 1
            assert statistics.stop_requested is True
            assert statistics.reached_cycle_limit is False

    asyncio.run(scenario())


def test_internal_exception_is_not_retained_or_exposed(tmp_path) -> None:
    class ExplodingDispatcher:
        async def run_once(self, **_kwargs: object) -> None:
            raise RuntimeError("token=top-secret; message=private-alert")

    async def scenario() -> None:
        with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
            service = NotificationDispatchService(
                outbox,
                {},
                dispatcher=ExplodingDispatcher(),  # type: ignore[arg-type]
            )
            with pytest.raises(NotificationDispatchServiceError) as caught:
                await service.dispatch_once()

            rendered = f"{caught.value!s} {caught.value!r}"
            assert "top-secret" not in rendered
            assert "private-alert" not in rendered
            assert caught.value.__context__ is None
            assert caught.value.__cause__ is None

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "arguments",
    [
        {"max_cycles": 0},
        {"max_cycles": 1, "poll_interval": -0.1},
        {"max_cycles": 1, "limit": 0},
        {"max_cycles": 1, "lease_for": timedelta(0)},
    ],
)
def test_poll_rejects_unbounded_or_invalid_configuration(tmp_path, arguments) -> None:
    async def scenario() -> None:
        with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
            service = NotificationDispatchService(outbox, {})
            with pytest.raises(ValueError):
                await service.poll(**arguments)

    asyncio.run(scenario())
