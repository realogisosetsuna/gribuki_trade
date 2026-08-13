"""SQLite runtime safety policy for shared WAL deployments.

SQLite disclosed a low-probability WAL-reset corruption race in 2026.  The
race requires multiple connections to the same WAL database, so single-store
local workflows remain usable on affected runtimes.  Long-running or
multi-process deployments must call :func:`require_safe_shared_wal` before
allowing a database path to be shared.
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
    """The runtime is not approved for multiple connections to one WAL DB."""

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
    """Return whether *version* contains an official WAL-reset fix.

    SQLite fixed the current release line in 3.51.3 and backported the fix to
    the maintained 3.50 and 3.44 lines in 3.50.7 and 3.44.6 respectively.
    Versions on other older branches are conservatively treated as unsafe.
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
    """Describe the current (or explicitly supplied) SQLite runtime."""

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
    """Fail closed before a same-file multi-connection WAL deployment."""

    status = sqlite_runtime_status(version)
    if not status.shared_wal_safe:
        raise SQLiteSharedWALUnsafeError(
            "SQLite runtime is not approved for shared WAL deployment; "
            "upgrade SQLite or keep one process and one Store instance per database path"
        )
    return status
