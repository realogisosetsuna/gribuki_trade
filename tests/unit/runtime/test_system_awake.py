from __future__ import annotations

from unittest.mock import patch

import pytest

from gribuki_trade.runtime.system_awake import (
    SystemAwakeError,
    SystemAwakeGuard,
)


def test_non_windows_guard_is_a_noop() -> None:
    with (
        patch("gribuki_trade.runtime.system_awake.os.name", "posix"),
        SystemAwakeGuard(),
    ):
        pass


def test_windows_guard_requests_system_only_and_restores() -> None:
    calls: list[int] = []

    def fake(flags: int) -> int:
        calls.append(flags)
        return 1

    with (
        patch("gribuki_trade.runtime.system_awake.os.name", "nt"),
        patch(
            "gribuki_trade.runtime.system_awake._set_thread_execution_state",
            fake,
        ),
        SystemAwakeGuard(),
    ):
        pass

    assert calls == [0x80000001, 0x80000000]


def test_windows_guard_fails_closed_when_request_is_refused() -> None:
    with (
        patch("gribuki_trade.runtime.system_awake.os.name", "nt"),
        patch(
            "gribuki_trade.runtime.system_awake._set_thread_execution_state",
            return_value=0,
        ),
        pytest.raises(SystemAwakeError, match="request failed"),
        SystemAwakeGuard(),
    ):
        pass
