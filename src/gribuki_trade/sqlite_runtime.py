"""共享 WAL 部署使用的 SQLite 运行时安全策略。

SQLite 在 2026 年披露了一个低概率的 WAL 重置损坏竞态。该问题要求多个连接同时访问
同一个 WAL 数据库，因此受影响运行时上的单存储本地流程仍可使用。长期运行或多进程部署
必须先调用 :func:`require_safe_shared_wal`，才能允许共享数据库路径。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Final

SQLITE_WAL_RESET_RUNTIME_UNSAFE: Final = "SQLITE_WAL_RESET_RUNTIME_UNSAFE"
SQLITE_WAL_RESET_GUIDANCE_URL: Final = (
    "https://www.sqlite.org/wal.html#walresetbug"
)
SQLITE_WAL_RESET_FIXED_RELEASES: Final = ("3.44.6", "3.50.7", ">=3.51.3")


class SQLiteSharedWALUnsafeError(RuntimeError):
    """当前运行时未获准让多个连接共享同一 WAL 数据库。"""

    error_code = SQLITE_WAL_RESET_RUNTIME_UNSAFE


@dataclass(frozen=True, slots=True)
class SQLiteRuntimeStatus:
    version: str
    version_info: tuple[int, int, int]
    shared_wal_safe: bool
    error_code: str | None
    fixed_releases: tuple[str, ...]
    guidance_url: str


def sqlite_shared_wal_is_safe(version: tuple[int, int, int]) -> bool:
    """返回 *version* 是否包含官方 WAL 重置修复。

    SQLite 在 3.51.3 修复当前发布线，并分别在 3.50.7 与 3.44.6 将修复回移到仍维护的
    3.50 和 3.44 发布线。其他较旧分支按保守原则视为不安全。
    """

    if len(version) != 3 or any(
        isinstance(part, bool) or not isinstance(part, int) or part < 0
        for part in version
    ):
        raise ValueError("SQLite version must contain three non-negative integers")
    if version >= (3, 51, 3):
        return True
    if version[:2] == (3, 50):
        return version >= (3, 50, 7)
    if version[:2] == (3, 44):
        return version >= (3, 44, 6)
    return False


def sqlite_runtime_status(
    version: tuple[int, int, int] | None = None,
) -> SQLiteRuntimeStatus:
    """描述当前或显式提供的 SQLite 运行时。"""

    selected = tuple(sqlite3.sqlite_version_info) if version is None else version
    if len(selected) != 3:
        raise ValueError("SQLite version must contain three components")
    normalized = (selected[0], selected[1], selected[2])
    safe = sqlite_shared_wal_is_safe(normalized)
    return SQLiteRuntimeStatus(
        version=".".join(str(part) for part in normalized),
        version_info=normalized,
        shared_wal_safe=safe,
        error_code=None if safe else SQLITE_WAL_RESET_RUNTIME_UNSAFE,
        fixed_releases=SQLITE_WAL_RESET_FIXED_RELEASES,
        guidance_url=SQLITE_WAL_RESET_GUIDANCE_URL,
    )


def require_safe_shared_wal(
    version: tuple[int, int, int] | None = None,
) -> SQLiteRuntimeStatus:
    """在同文件多连接 WAL 部署前执行失败关闭检查。"""

    status = sqlite_runtime_status(version)
    if not status.shared_wal_safe:
        raise SQLiteSharedWALUnsafeError(
            "SQLite runtime is not approved for shared WAL deployment; "
            "upgrade SQLite or keep one process and one Store instance per database path"
        )
    return status
