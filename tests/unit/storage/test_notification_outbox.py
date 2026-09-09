import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from gribuki_trade.adapters.notifiers import OneBotError
from gribuki_trade.ports.notifier import (
    DeliveryReceipt,
    NotificationTargetKind,
    OutboundNotification,
)
from gribuki_trade.storage import OutboxDispatcher, OutboxStatus, SQLiteOutbox

NOW = datetime(2026, 8, 13, 2, 0, tzinfo=UTC)


def notification(
    key: str = "signal:1",
    *,
    text: str = "研究提醒",
    expires_at: datetime | None = None,
    target_id: str = "12345",
    channel: str = "onebot",
) -> OutboundNotification:
    return OutboundNotification(
        idempotency_key=key,
        channel=channel,
        target_kind=NotificationTargetKind.PRIVATE,
        target_id=target_id,
        text=text,
        created_at=NOW,
        expires_at=expires_at,
    )


def test_enqueue_is_idempotent_but_rejects_key_collision(tmp_path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        first = outbox.enqueue(notification())
        duplicate = outbox.enqueue(notification())

        assert duplicate.id == first.id
        assert len(outbox.list_items()) == 1
        assert "研究提醒" not in repr(first)

        with pytest.raises(ValueError, match="different payload"):
            outbox.enqueue(notification(text="different"))


def test_claim_and_mark_sent_are_transactional(tmp_path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        enqueued = outbox.enqueue(notification())
        claimed = outbox.claim_due(now=NOW, lease_for=timedelta(seconds=10))

        assert [item.id for item in claimed] == [enqueued.id]
        assert claimed[0].status is OutboxStatus.IN_FLIGHT
        assert claimed[0].attempt_count == 1
        assert outbox.claim_due(now=NOW + timedelta(seconds=5)) == ()

        sent = outbox.mark_sent(enqueued.id, provider_message_id="message-7", now=NOW)
        assert sent.status is OutboxStatus.SENT
        assert sent.provider_message_id == "message-7"
        assert outbox.claim_due(now=NOW + timedelta(hours=1)) == ()


def test_claim_scope_never_leases_another_notification_target(tmp_path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        wanted = outbox.enqueue(notification("signal:wanted", target_id="12345"))
        other = outbox.enqueue(notification("signal:other", target_id="67890"))

        claimed = outbox.claim_due(
            now=NOW,
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="12345",
        )

        assert [item.id for item in claimed] == [wanted.id]
        stored_other = outbox.get_by_key("signal:other")
        assert stored_other is not None
        assert stored_other.id == other.id
        assert stored_other.status is OutboxStatus.PENDING


def test_claim_scope_can_also_isolate_notification_channels(tmp_path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        wanted = outbox.enqueue(notification("signal:wanted"))
        outbox.enqueue(notification("signal:email", channel="email"))

        claimed = outbox.claim_due(
            now=NOW,
            target_kind=NotificationTargetKind.PRIVATE,
            target_id="12345",
            channels=("onebot",),
        )

        assert [item.id for item in claimed] == [wanted.id]
        other = outbox.get_by_key("signal:email")
        assert other is not None
        assert other.status is OutboxStatus.PENDING


@pytest.mark.parametrize(
    ("target_kind", "target_id"),
    [
        (NotificationTargetKind.PRIVATE, None),
        (None, "12345"),
        (NotificationTargetKind.PRIVATE, ""),
    ],
)
def test_claim_scope_requires_a_complete_nonblank_target(
    tmp_path,
    target_kind,
    target_id,
) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox, pytest.raises(ValueError):
        outbox.claim_due(
            now=NOW,
            target_kind=target_kind,
            target_id=target_id,
        )


def test_expired_lease_is_reclaimed_without_losing_the_item(tmp_path) -> None:
    with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
        outbox.enqueue(notification())
        first = outbox.claim_due(now=NOW, lease_for=timedelta(seconds=10))[0]
        second = outbox.claim_due(now=NOW + timedelta(seconds=11))[0]

        assert second.id == first.id
        assert second.attempt_count == 2
        assert second.last_error_code == "lease_expired"


def test_retry_uses_exponential_backoff_and_nonretryable_is_dead(tmp_path) -> None:
    with SQLiteOutbox(
        tmp_path / "outbox.sqlite3",
        base_backoff=timedelta(seconds=5),
        max_backoff=timedelta(seconds=20),
    ) as outbox:
        outbox.enqueue(notification())
        first = outbox.claim_due(now=NOW)[0]
        retry = outbox.mark_failed(
            first.id,
            "server_error",
            retryable=True,
            now=NOW,
        )
        assert retry.status is OutboxStatus.RETRY
        assert retry.next_attempt_at == NOW + timedelta(seconds=5)
        assert outbox.claim_due(now=NOW + timedelta(seconds=4)) == ()

        second = outbox.claim_due(now=NOW + timedelta(seconds=5))[0]
        dead = outbox.mark_failed(
            second.id,
            "authentication_rejected",
            retryable=False,
            now=NOW + timedelta(seconds=5),
        )
        assert dead.status is OutboxStatus.DEAD
        assert dead.last_error_code == "authentication_rejected"


def test_ttl_prevents_a_retry_after_the_signal_is_stale(tmp_path) -> None:
    with SQLiteOutbox(
        tmp_path / "outbox.sqlite3",
        base_backoff=timedelta(seconds=5),
    ) as outbox:
        outbox.enqueue(notification(expires_at=NOW + timedelta(seconds=4)))
        item = outbox.claim_due(now=NOW)[0]
        expired = outbox.mark_failed(item.id, "server_error", retryable=True, now=NOW)

        assert expired.status is OutboxStatus.EXPIRED
        assert outbox.claim_due(now=NOW + timedelta(seconds=5)) == ()


def test_max_attempts_moves_repeated_failure_to_dead_letter(tmp_path) -> None:
    with SQLiteOutbox(
        tmp_path / "outbox.sqlite3",
        max_attempts=2,
        base_backoff=timedelta(seconds=1),
    ) as outbox:
        outbox.enqueue(notification())
        first = outbox.claim_due(now=NOW)[0]
        outbox.mark_failed(first.id, "server_error", retryable=True, now=NOW)
        second = outbox.claim_due(now=NOW + timedelta(seconds=1))[0]
        result = outbox.mark_failed(
            second.id,
            "server_error",
            retryable=True,
            now=NOW + timedelta(seconds=1),
        )

        assert result.status is OutboxStatus.DEAD
        assert result.attempt_count == 2


def test_dispatcher_persists_retry_then_receipt(tmp_path) -> None:
    class FlakyNotifier:
        channel = "onebot"

        def __init__(self) -> None:
            self.calls = 0

        async def send(self, _notification: OutboundNotification) -> DeliveryReceipt:
            self.calls += 1
            if self.calls == 1:
                raise OneBotError("server_error", retryable=True)
            return DeliveryReceipt(channel=self.channel, provider_message_id="qq-99")

    async def scenario() -> None:
        current = NOW
        notifier = FlakyNotifier()
        with SQLiteOutbox(
            tmp_path / "outbox.sqlite3",
            base_backoff=timedelta(seconds=5),
        ) as outbox:
            outbox.enqueue(notification())
            dispatcher = OutboxDispatcher(outbox, {"onebot": notifier}, clock=lambda: current)

            first = await dispatcher.run_once()
            assert first.retry_scheduled == 1
            assert first.sent == 0

            current += timedelta(seconds=5)
            second = await dispatcher.run_once()
            assert second.sent == 1
            stored = outbox.get_by_key("signal:1")
            assert stored is not None
            assert stored.status is OutboxStatus.SENT
            assert stored.provider_message_id == "qq-99"

    asyncio.run(scenario())


def test_missing_notifier_is_a_permanent_delivery_failure(tmp_path) -> None:
    async def scenario() -> None:
        with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
            outbox.enqueue(notification())
            result = await OutboxDispatcher(outbox, {}, clock=lambda: NOW).run_once()
            assert result.dead == 1
            stored = outbox.get_by_key("signal:1")
            assert stored is not None
            assert stored.status is OutboxStatus.DEAD
            assert stored.last_error_code == "notifier_not_configured"

    asyncio.run(scenario())


def test_dispatcher_does_not_start_delivery_after_ttl(tmp_path) -> None:
    class MustNotRunNotifier:
        channel = "onebot"

        def __init__(self) -> None:
            self.calls = 0

        async def send(self, _notification: OutboundNotification) -> DeliveryReceipt:
            self.calls += 1
            return DeliveryReceipt(channel=self.channel)

    async def scenario() -> None:
        times = iter((NOW, NOW + timedelta(seconds=2)))
        notifier = MustNotRunNotifier()
        with SQLiteOutbox(tmp_path / "outbox.sqlite3") as outbox:
            outbox.enqueue(notification(expires_at=NOW + timedelta(seconds=1)))
            dispatcher = OutboxDispatcher(outbox, {"onebot": notifier}, clock=lambda: next(times))
            result = await dispatcher.run_once()

            assert result.expired == 1
            assert notifier.calls == 0

    asyncio.run(scenario())
