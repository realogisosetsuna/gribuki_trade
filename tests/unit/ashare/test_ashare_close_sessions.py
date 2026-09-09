from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from typing import NoReturn
from zoneinfo import ZoneInfo

import pytest

from gribuki_trade.ports.market_data import TradeCalendarDay
from gribuki_trade.services.ashare.close.ashare_close_sessions import (
    AShareCloseSessionResolver,
    CloseAnalysisMode,
    InvalidCloseSessionOverrideError,
    MarketSessionNotClosedError,
    TradingCalendarCoverageError,
    TradingCalendarUnavailableError,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


class FakeTradingCalendar:
    def __init__(self, trading_dates: set[date]) -> None:
        self._trading_dates = trading_dates
        self.calls: list[tuple[date, date]] = []

    async def fetch_trade_calendar_async(
        self,
        start: date,
        end: date,
    ) -> tuple[TradeCalendarDay, ...]:
        self.calls.append((start, end))
        return tuple(
            TradeCalendarDay(
                calendar_date=start + timedelta(days=offset),
                is_trading_day=start + timedelta(days=offset) in self._trading_dates,
            )
            for offset in range((end - start).days + 1)
        )


class FailingTradingCalendar:
    async def fetch_trade_calendar_async(
        self,
        start: date,
        end: date,
    ) -> NoReturn:
        del start, end
        raise ConnectionError("provider credential and endpoint detail")


class IncompleteTradingCalendar:
    async def fetch_trade_calendar_async(
        self,
        start: date,
        end: date,
    ) -> tuple[TradeCalendarDay, ...]:
        del end
        return (TradeCalendarDay(start, True),)


def _local(day: int, hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 8, day, hour, minute, second, tzinfo=SHANGHAI)


def test_friday_after_close_targets_verified_monday_session() -> None:
    calendar = FakeTradingCalendar({date(2026, 8, 13), date(2026, 8, 14), date(2026, 8, 17)})
    resolver = AShareCloseSessionResolver(calendar)

    result = asyncio.run(resolver.resolve(_local(14, 15, 5)))

    assert result.as_of == _local(14, 15, 5)
    assert result.latest_completed_session == date(2026, 8, 14)
    assert result.next_session == date(2026, 8, 17)
    assert result.calendar_verified is True
    assert result.analysis_mode is CloseAnalysisMode.POST_CLOSE
    assert calendar.calls == [(date(2026, 6, 30), date(2026, 9, 28))]


def test_trading_day_preopen_uses_previous_session_and_targets_today() -> None:
    calendar = FakeTradingCalendar({date(2026, 8, 14), date(2026, 8, 17)})

    result = asyncio.run(
        AShareCloseSessionResolver(calendar).resolve(_local(17, 9, 29, 59))
    )

    assert result.latest_completed_session == date(2026, 8, 14)
    assert result.next_session == date(2026, 8, 17)
    assert result.analysis_mode is CloseAnalysisMode.PREOPEN


@pytest.mark.parametrize(
    "as_of",
    [
        _local(17, 9, 30),
        _local(17, 12, 0),
        _local(17, 15, 4, 59),
    ],
)
def test_open_or_not_yet_final_session_raises_stable_error(as_of: datetime) -> None:
    calendar = FakeTradingCalendar(
        {date(2026, 8, 14), date(2026, 8, 17), date(2026, 8, 18)}
    )

    with pytest.raises(MarketSessionNotClosedError) as captured:
        asyncio.run(AShareCloseSessionResolver(calendar).resolve(as_of))

    assert captured.value.code == "MARKET_SESSION_NOT_CLOSED"
    assert "provider" not in str(captured.value)


def test_weekend_uses_previous_and_next_real_sessions() -> None:
    calendar = FakeTradingCalendar({date(2026, 8, 14), date(2026, 8, 17)})

    result = asyncio.run(
        AShareCloseSessionResolver(calendar).resolve(_local(15, 16, 0))
    )

    assert result.latest_completed_session == date(2026, 8, 14)
    assert result.next_session == date(2026, 8, 17)
    assert result.analysis_mode is CloseAnalysisMode.POST_CLOSE


def test_exchange_holiday_is_not_inferred_from_weekday() -> None:
    calendar = FakeTradingCalendar({date(2026, 8, 14), date(2026, 8, 18)})

    result = asyncio.run(
        AShareCloseSessionResolver(calendar).resolve(_local(17, 16, 0))
    )

    assert result.latest_completed_session == date(2026, 8, 14)
    assert result.next_session == date(2026, 8, 18)


def test_explicit_pair_is_still_calendar_and_pit_verified() -> None:
    calendar = FakeTradingCalendar({date(2026, 8, 14), date(2026, 8, 18)})

    result = asyncio.run(
        AShareCloseSessionResolver(calendar).resolve(
            _local(17, 16, 0),
            latest_completed_session=date(2026, 8, 14),
            next_session=date(2026, 8, 18),
        )
    )

    assert result.latest_completed_session == date(2026, 8, 14)
    assert result.next_session == date(2026, 8, 18)
    assert result.calendar_verified is True


@pytest.mark.parametrize(
    ("latest", "target", "expected_code"),
    [
        (date(2026, 8, 15), date(2026, 8, 18), "LATEST_COMPLETED_SESSION_NOT_TRADING"),
        (date(2026, 8, 14), date(2026, 8, 17), "NEXT_SESSION_NOT_TRADING"),
        (date(2026, 8, 18), date(2026, 8, 14), "SESSION_ORDER_INVALID"),
        (date(2026, 8, 13), date(2026, 8, 18), "SESSIONS_NOT_ADJACENT"),
    ],
)
def test_explicit_pair_cannot_bypass_session_rules(
    latest: date,
    target: date,
    expected_code: str,
) -> None:
    calendar = FakeTradingCalendar(
        {date(2026, 8, 13), date(2026, 8, 14), date(2026, 8, 18)}
    )

    with pytest.raises(InvalidCloseSessionOverrideError) as captured:
        asyncio.run(
            AShareCloseSessionResolver(calendar).resolve(
                _local(17, 16, 0),
                latest_completed_session=latest,
                next_session=target,
            )
        )

    assert captured.value.code == expected_code


def test_explicit_target_must_not_have_opened() -> None:
    calendar = FakeTradingCalendar(
        {date(2026, 8, 13), date(2026, 8, 14), date(2026, 8, 17)}
    )

    with pytest.raises(InvalidCloseSessionOverrideError) as captured:
        asyncio.run(
            AShareCloseSessionResolver(calendar).resolve(
                _local(15, 16, 0),
                latest_completed_session=date(2026, 8, 13),
                next_session=date(2026, 8, 14),
            )
        )

    assert captured.value.code == "NEXT_SESSION_ALREADY_OPENED"


def test_missing_future_session_has_stable_coverage_failure() -> None:
    calendar = FakeTradingCalendar({date(2026, 8, 14)})

    with pytest.raises(TradingCalendarCoverageError) as captured:
        asyncio.run(AShareCloseSessionResolver(calendar).resolve(_local(14, 15, 5)))

    assert captured.value.code == "NEXT_TRADING_SESSION_MISSING"


def test_missing_previous_session_has_stable_coverage_failure() -> None:
    calendar = FakeTradingCalendar({date(2026, 8, 17)})

    with pytest.raises(TradingCalendarCoverageError) as captured:
        asyncio.run(AShareCloseSessionResolver(calendar).resolve(_local(17, 9, 29)))

    assert captured.value.code == "PREVIOUS_TRADING_SESSION_MISSING"


def test_no_trading_days_has_stable_coverage_failure() -> None:
    calendar = FakeTradingCalendar(set())

    with pytest.raises(TradingCalendarCoverageError) as captured:
        asyncio.run(AShareCloseSessionResolver(calendar).resolve(_local(15, 16, 0)))

    assert captured.value.code == "NO_TRADING_DAYS_IN_CALENDAR_RANGE"


@pytest.mark.parametrize("calendar", [FailingTradingCalendar(), IncompleteTradingCalendar()])
def test_provider_failure_or_invalid_payload_is_sanitized(
    calendar: FailingTradingCalendar | IncompleteTradingCalendar,
) -> None:
    with pytest.raises(TradingCalendarUnavailableError) as captured:
        asyncio.run(AShareCloseSessionResolver(calendar).resolve(_local(15, 16, 0)))

    assert captured.value.code == "TRADING_CALENDAR_UNAVAILABLE"
    assert "credential" not in str(captured.value)


def test_utc_input_is_normalized_to_shanghai_before_resolution() -> None:
    calendar = FakeTradingCalendar({date(2026, 8, 14), date(2026, 8, 17)})
    utc_now = datetime(2026, 8, 17, 1, 29, tzinfo=UTC)

    result = asyncio.run(AShareCloseSessionResolver(calendar).resolve(utc_now))

    assert result.as_of == _local(17, 9, 29)
    assert result.analysis_mode is CloseAnalysisMode.PREOPEN


def test_naive_now_is_rejected_before_calendar_access() -> None:
    calendar = FakeTradingCalendar(set())

    with pytest.raises(ValueError, match="timezone-aware"):
        asyncio.run(
            AShareCloseSessionResolver(calendar).resolve(datetime(2026, 8, 15, 16))
        )

    assert calendar.calls == []
