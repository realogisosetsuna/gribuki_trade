"""实盘账本错误类型。

错误类型独立于 SQLite facade，使事件完整性校验和旧账本解析可以在没有
连接或事务的情况下复用，同时保留原有 ``live_records`` 导入路径。
"""

from __future__ import annotations

from gribuki_trade.storage.live_records.live_record_codec import _error_code


class LiveRecordStoreError(RuntimeError):
    """实盘账本持久化失败的基类。"""


class LiveRecordConflictError(LiveRecordStoreError):
    """幂等键、命令号或外部成交事实发生冲突。"""


class LiveRecordIntegrityError(LiveRecordStoreError):
    """只追加哈希链或事务投影不再自洽。"""


class LiveRecordStateError(LiveRecordStoreError):
    """命令状态、发送者、指纹或持仓不允许本次状态迁移。"""

    def __init__(self, code: str) -> None:
        self.code = _error_code(code)
        super().__init__(f"live-record state transition rejected ({self.code})")


__all__ = [
    "LiveRecordConflictError",
    "LiveRecordIntegrityError",
    "LiveRecordStateError",
    "LiveRecordStoreError",
]
