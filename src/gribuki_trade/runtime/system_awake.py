"""供长时间市场会话使用的进程级系统唤醒守卫。

阻塞式交易会话工作进程运行时，此守卫会阻止系统自动休眠。它不会保持显示器唤醒，
并会在退出上下文时恢复正常的 Windows 策略。其他平台使用有明确说明的空操作，
使研究核心在 Windows 之外仍可导入。
"""

from __future__ import annotations

import ctypes
import os
from types import TracebackType

_ES_CONTINUOUS = 0x80000000
_ES_SYSTEM_REQUIRED = 0x00000001


class SystemAwakeError(RuntimeError):
    """Windows 拒绝进程级执行状态请求时抛出。"""


class SystemAwakeGuard:
    """在一个阻塞式工作进程的生命周期内保持主机唤醒。"""

    def __init__(self) -> None:
        self._active = False

    def __enter__(self) -> SystemAwakeGuard:
        if os.name != "nt":
            return self
        result = _set_thread_execution_state(
            _ES_CONTINUOUS | _ES_SYSTEM_REQUIRED,
        )
        if result == 0:
            raise SystemAwakeError("Windows system-awake request failed")
        self._active = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        if not self._active:
            return
        self._active = False
        if _set_thread_execution_state(_ES_CONTINUOUS) == 0:
            raise SystemAwakeError("Windows system-awake policy restore failed")


def _set_thread_execution_state(flags: int) -> int:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    function = kernel32.SetThreadExecutionState
    function.argtypes = [ctypes.c_uint]
    function.restype = ctypes.c_uint
    return int(function(flags))
