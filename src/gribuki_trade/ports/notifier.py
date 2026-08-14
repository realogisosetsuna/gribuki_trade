"""稳定的出站通知端口。

通知刻意设计为单向。实现可以向外部服务交付消息，但此端口没有入站命令概念，
也不提供任何可触及券商层或策略层的方法。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol


class NotificationTargetKind(StrEnum):
    """通知边界支持的目标类型。"""

    PRIVATE = "private"
    GROUP = "group"


@dataclass(frozen=True, slots=True)
class OutboundNotification:
    """已准备交付的纯文本消息。

    生成的表示会隐藏 ``target_id`` 和 ``text``：QQ 标识符与研究提醒不应意外泄漏到日志中。
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
    """通知器实现返回的已脱敏结果。"""

    channel: str
    provider_message_id: str | None = None
    delivered_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class NotificationDeliveryError(RuntimeError):
    """具有稳定且日志安全分类的交付失败。"""

    def __init__(self, code: str, *, retryable: bool) -> None:
        super().__init__(f"notification delivery failed ({code})")
        self.code = code
        self.retryable = retryable


class Notifier(Protocol):
    """单向出站交付边界。"""

    @property
    def channel(self) -> str: ...

    async def send(self, notification: OutboundNotification) -> DeliveryReceipt: ...


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
