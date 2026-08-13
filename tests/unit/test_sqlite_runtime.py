from __future__ import annotations

import sqlite3

import pytest

from gribuki_trade.sqlite_runtime import (
    SQLITE_WAL_RESET_RUNTIME_UNSAFE,
    SQLiteSharedWALUnsafeError,
    require_safe_shared_wal,
    sqlite_runtime_status,
    sqlite_shared_wal_is_safe,
)


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ((3, 44, 5), False),
        ((3, 44, 6), True),
        ((3, 45, 3), False),
        ((3, 50, 4), False),
        ((3, 50, 7), True),
        ((3, 51, 2), False),
        ((3, 51, 3), True),
        ((3, 53, 0), True),
    ],
)
def test_sqlite_shared_wal_version_policy(
    version: tuple[int, int, int], expected: bool
) -> None:
    assert sqlite_shared_wal_is_safe(version) is expected


def test_runtime_status_describes_actual_library_without_opening_a_database() -> None:
    status = sqlite_runtime_status()

    assert status.version_info == tuple(sqlite3.sqlite_version_info)
    assert status.version == sqlite3.sqlite_version
    assert status.error_code == (
        None if status.shared_wal_safe else SQLITE_WAL_RESET_RUNTIME_UNSAFE
    )
    assert status.guidance_url.startswith("https://www.sqlite.org/")


def test_shared_wal_preflight_fails_closed_on_affected_runtime() -> None:
    with pytest.raises(SQLiteSharedWALUnsafeError) as caught:
        require_safe_shared_wal((3, 50, 4))

    assert caught.value.error_code == SQLITE_WAL_RESET_RUNTIME_UNSAFE
    assert "3.50.4" not in str(caught.value)


def test_shared_wal_preflight_accepts_patched_runtime() -> None:
    status = require_safe_shared_wal((3, 50, 7))

    assert status.shared_wal_safe is True
    assert status.error_code is None


@pytest.mark.parametrize("version", [(-1, 0, 0), (True, 50, 7)])
def test_invalid_version_is_rejected(version: tuple[int, int, int]) -> None:
    with pytest.raises(ValueError):
        sqlite_shared_wal_is_safe(version)
