"""出站通知适配器。"""

from .onebot import (
    NAPCAT_ACCESS_TOKEN_SECRET,
    OneBotConfig,
    OneBotError,
    OneBotFileUploadReceipt,
    OneBotNotifier,
    OneBotTargetNotAllowedError,
)

__all__ = [
    "NAPCAT_ACCESS_TOKEN_SECRET",
    "OneBotConfig",
    "OneBotError",
    "OneBotFileUploadReceipt",
    "OneBotNotifier",
    "OneBotTargetNotAllowedError",
]
