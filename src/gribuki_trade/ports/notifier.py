"""Stable outbound-notification port.

Notifications are deliberately one-way.  Implementations may deliver messages
to an external service, but this port has no concept of inbound commands and no
method that can reach the broker or strategy layers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol


class NotificationTargetKind(StrEnum):
    """Kinds of destinations supported by the notification boundary."""

    PRIVATE = "private"
    GROUP = "group"


@dataclass(frozen=True, slots=True)
class OutboundNotification:
    """A text-only message ready for delivery.

    ``target_id`` and ``text`` are redacted from the generated representation:
    QQ identifiers and research alerts should not leak into logs by accident.
    """

    idempotency_key: str
    channel: str
    target_kind: NotificationTargetKind
    target_id: str = field(repr=False)
    text: str = field(repr=False)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.idempotency_key.strip():
            raise ValueError("idempotency_key must not be empty")
        if len(self.idempotency_key) > 200:
            raise ValueError("idempotency_key must not exceed 200 characters")
        if not self.channel.strip():
            raise ValueError("channel must not be empty")
        if not self.target_id.strip():
            raise ValueError("target_id must not be empty")
        if not self.text:
            raise ValueError("notification text must not be empty")
        _require_aware(self.created_at, "created_at")
        if self.expires_at is not None:
            _require_aware(self.expires_at, "expires_at")
            if self.expires_at <= self.created_at:
                raise ValueError("expires_at must be later than created_at")


@dataclass(frozen=True, slots=True)
class DeliveryReceipt:
    """Sanitized result returned by a notifier implementation."""

    channel: str
    provider_message_id: str | None = None
    delivered_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class NotificationDeliveryError(RuntimeError):
    """A delivery failure with a stable, log-safe classification."""

    def __init__(self, code: str, *, retryable: bool) -> None:
        super().__init__(f"notification delivery failed ({code})")
        self.code = code
        self.retryable = retryable


class Notifier(Protocol):
    """One-way outbound delivery boundary."""

    @property
    def channel(self) -> str: ...

    async def send(self, notification: OutboundNotification) -> DeliveryReceipt: ...


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
