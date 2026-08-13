"""Resolve point-in-time A-share close-analysis sessions from a real calendar.

The resolver deliberately never infers exchange sessions from weekdays.  It
loads an inclusive natural-day calendar through :class:`AsyncTradingCalendar`,
validates the provider payload, and then applies Shanghai exchange clock
boundaries.  This keeps after-close research from accidentally targeting a
weekend, holiday, or a session that has already opened.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import StrEnum
from zoneinfo import ZoneInfo

from gribuki_trade.ports.market_data import AsyncTradingCalendar, TradeCalendarDay

SHANGHAI = ZoneInfo("Asia/Shanghai")


class CloseAnalysisMode(StrEnum):
    """Clock mode in which a next-session analysis is being prepared."""

    POST_CLOSE = "post_close"
    PREOPEN = "preopen"


class CloseSessionResolutionError(RuntimeError):
    """A stable, sanitized session-resolution failure."""

    def __init__(self, code: str) -> None:
        super().__init__(f"A-share close-session resolution failed ({code})")
        self.code = code


class MarketSessionNotClosedError(CloseSessionResolutionError):
    """The current Shanghai trading session has not produced a final bar yet."""

    def __init__(self) -> None:
        super().__init__("MARKET_SESSION_NOT_CLOSED")


class TradingCalendarUnavailableError(CloseSessionResolutionError):
    """The calendar provider failed or returned a structurally invalid payload."""

    def __init__(self) -> None:
        super().__init__("TRADING_CALENDAR_UNAVAILABLE")


class TradingCalendarCoverageError(CloseSessionResolutionError):
    """A valid calendar payload did not contain a required trading session."""

    def __init__(self, code: str) -> None:
        super().__init__(code)


class InvalidCloseSessionOverrideError(CloseSessionResolutionError):
    """A caller-supplied session violates calendar or point-in-time rules."""

    def __init__(self, code: str) -> None:
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class CloseSessionResolution:
    """Verified completed and target sessions for one close-analysis run."""

    as_of: datetime
    latest_completed_session: date
    next_session: date
    calendar_verified: bool
    analysis_mode: CloseAnalysisMode


class AShareCloseSessionResolver:
    """Resolve close-analysis boundaries using a provider's official calendar."""

    def __init__(
        self,
        calendar: AsyncTradingCalendar,
        *,
        lookback_days: int = 45,
        lookahead_days: int = 45,
        session_open: time = time(9, 30),
        completed_bar_available: time = time(15, 5),
    ) -> None:
        if lookback_days < 1:
            raise ValueError("lookback_days must be positive")
        if lookahead_days < 1:
            raise ValueError("lookahead_days must be positive")
        if session_open >= completed_bar_available:
            raise ValueError("session_open must precede completed_bar_available")
        self._calendar = calendar
        self._lookback_days = lookback_days
        self._lookahead_days = lookahead_days
        self._session_open = session_open
        self._completed_bar_available = completed_bar_available

    async def resolve(
        self,
        now: datetime,
        *,
        latest_completed_session: date | None = None,
        next_session: date | None = None,
    ) -> CloseSessionResolution:
        """Resolve and verify the completed/target session pair at ``now``.

        Overrides are useful for deterministic replay and operator checks, but
        are not trust inputs: both dates are still checked against the fetched
        exchange calendar, must be adjacent trading sessions, and must obey the
        current point-in-time boundary.
        """

        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        local_now = now.astimezone(SHANGHAI)
        today = local_now.date()

        anchors = tuple(
            day
            for day in (today, latest_completed_session, next_session)
            if day is not None
        )
        query_start = min(anchors) - timedelta(days=self._lookback_days)
        query_end = max(anchors) + timedelta(days=self._lookahead_days)
        days = await self._fetch_validated_calendar(query_start, query_end)
        trading_dates = tuple(
            day.calendar_date for day in days if day.is_trading_day
        )
        if not trading_dates:
            raise TradingCalendarCoverageError("NO_TRADING_DAYS_IN_CALENDAR_RANGE")

        today_is_trading = today in trading_dates
        local_time = local_now.time().replace(tzinfo=None)
        if (
            today_is_trading
            and self._session_open <= local_time < self._completed_bar_available
        ):
            raise MarketSessionNotClosedError()

        automatic_latest, automatic_next = self._automatic_pair(
            trading_dates,
            today=today,
            local_time=local_time,
            today_is_trading=today_is_trading,
        )
        resolved_latest = latest_completed_session or automatic_latest
        resolved_next = next_session or automatic_next
        self._validate_pair(
            trading_dates,
            latest_completed_session=resolved_latest,
            next_session=resolved_next,
            latest_available_session=automatic_latest,
            today=today,
            local_time=local_time,
        )

        mode = (
            CloseAnalysisMode.PREOPEN
            if resolved_next == today and local_time < self._session_open
            else CloseAnalysisMode.POST_CLOSE
        )
        return CloseSessionResolution(
            as_of=local_now,
            latest_completed_session=resolved_latest,
            next_session=resolved_next,
            calendar_verified=True,
            analysis_mode=mode,
        )

    async def _fetch_validated_calendar(
        self,
        start: date,
        end: date,
    ) -> tuple[TradeCalendarDay, ...]:
        try:
            supplied = tuple(
                await self._calendar.fetch_trade_calendar_async(start, end)
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise TradingCalendarUnavailableError() from None

        expected_dates = tuple(
            start + timedelta(days=offset)
            for offset in range((end - start).days + 1)
        )
        supplied_dates = tuple(day.calendar_date for day in supplied)
        if supplied_dates != expected_dates or any(
            type(day.is_trading_day) is not bool for day in supplied
        ):
            raise TradingCalendarUnavailableError()
        return supplied

    def _automatic_pair(
        self,
        trading_dates: tuple[date, ...],
        *,
        today: date,
        local_time: time,
        today_is_trading: bool,
    ) -> tuple[date, date]:
        if today_is_trading and local_time >= self._completed_bar_available:
            latest = today
        else:
            prior = tuple(day for day in trading_dates if day < today)
            if not prior:
                raise TradingCalendarCoverageError("PREVIOUS_TRADING_SESSION_MISSING")
            latest = prior[-1]

        following = tuple(day for day in trading_dates if day > latest)
        if not following:
            raise TradingCalendarCoverageError("NEXT_TRADING_SESSION_MISSING")
        return latest, following[0]

    def _validate_pair(
        self,
        trading_dates: tuple[date, ...],
        *,
        latest_completed_session: date,
        next_session: date,
        latest_available_session: date,
        today: date,
        local_time: time,
    ) -> None:
        if latest_completed_session not in trading_dates:
            raise InvalidCloseSessionOverrideError(
                "LATEST_COMPLETED_SESSION_NOT_TRADING"
            )
        if next_session not in trading_dates:
            raise InvalidCloseSessionOverrideError("NEXT_SESSION_NOT_TRADING")
        if latest_completed_session >= next_session:
            raise InvalidCloseSessionOverrideError("SESSION_ORDER_INVALID")
        if latest_completed_session > latest_available_session:
            raise InvalidCloseSessionOverrideError("LATEST_SESSION_NOT_COMPLETED")

        latest_index = trading_dates.index(latest_completed_session)
        if (
            latest_index + 1 >= len(trading_dates)
            or trading_dates[latest_index + 1] != next_session
        ):
            raise InvalidCloseSessionOverrideError("SESSIONS_NOT_ADJACENT")

        if next_session < today or (
            next_session == today and local_time >= self._session_open
        ):
            raise InvalidCloseSessionOverrideError("NEXT_SESSION_ALREADY_OPENED")
