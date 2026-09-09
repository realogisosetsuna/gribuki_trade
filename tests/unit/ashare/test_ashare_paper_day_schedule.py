from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta

import pytest

from gribuki_trade.domain.paper_day import PaperDayPhase
from gribuki_trade.services.ashare.paper_day.ashare_paper_day_config import ASharePaperDayConfig
from gribuki_trade.services.ashare.paper_day.ashare_paper_day_schedule import (
    phase_at,
    scheduler_sleep_seconds,
    session_datetime,
)

SESSION = date(2026, 8, 14)


def test_session_datetime_uses_shanghai_wall_clock_and_returns_utc() -> None:
    assert session_datetime(SESSION, time(9, 30)) == datetime(
        2026,
        8,
        14,
        1,
        30,
        tzinfo=UTC,
    )


def test_phase_at_has_stable_boundaries_and_cross_day_states() -> None:
    config = ASharePaperDayConfig()

    assert phase_at(
        datetime(2026, 8, 13, 15, 59, tzinfo=UTC), SESSION, config
    ) is PaperDayPhase.BOOTSTRAP
    assert phase_at(
        session_datetime(SESSION, config.market_open), SESSION, config
    ) is PaperDayPhase.MORNING
    assert phase_at(
        session_datetime(SESSION, config.morning_end), SESSION, config
    ) is PaperDayPhase.LUNCH
    assert phase_at(
        session_datetime(SESSION, config.afternoon_start), SESSION, config
    ) is PaperDayPhase.AFTERNOON
    assert phase_at(
        session_datetime(SESSION, config.market_close), SESSION, config
    ) is PaperDayPhase.POST_CLOSE
    assert phase_at(
        session_datetime(SESSION, config.finalization_time), SESSION, config
    ) is PaperDayPhase.TERMINAL
    assert phase_at(
        datetime(2026, 8, 15, 0, 1, tzinfo=UTC), SESSION, config
    ) is PaperDayPhase.TERMINAL


def test_phase_at_rejects_naive_clock_values() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        phase_at(datetime(2026, 8, 14, 9, 30), SESSION, ASharePaperDayConfig())


def test_scheduler_sleep_seconds_is_bounded_and_never_busy_waits() -> None:
    now = datetime(2026, 8, 14, 1, 30, tzinfo=UTC)
    assert scheduler_sleep_seconds(now, now + timedelta(seconds=20), 1.0) == 1.0
    assert scheduler_sleep_seconds(now, now + timedelta(seconds=0.01), 1.0) == 0.05
    assert scheduler_sleep_seconds(now, now - timedelta(seconds=2), 1.0) == 0.05

    with pytest.raises(ValueError, match="positive"):
        scheduler_sleep_seconds(now, now, 0)
