"""匿名 BaoStock 历史数据适配器。

BaoStock 只用于研究、回测和模拟盘模式，并非获得许可的低延迟执行数据源。
依赖采用延迟导入，因此缺少这一可选外部服务时，领域包仍然可用。
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from datetime import date
from decimal import Decimal
from functools import partial
from typing import Any

from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.ports.market_data import (
    MarketDataTimeoutError,
    MarketDataUnavailableError,
    TradeCalendarDay,
)


class BaoStockError(MarketDataUnavailableError):
    """数据提供者登录、查询或载荷错误。"""


class BaoStockTimeoutError(MarketDataTimeoutError, BaoStockError):
    """异步 BaoStock 请求超过调用方可见的超时。"""


# BaoStock 的 Python SDK 维护进程全局登录状态。因此，完整的登录/查询/登出
# 会话必须在适配器实例之间，以及日线与交易日历端点之间串行执行。
_BAOSTOCK_SESSION_LOCK = threading.Lock()


class BaoStockDailyAdapter:
    _FIELDS = (
        "date,code,open,high,low,close,preclose,volume,amount,"
        "adjustflag,turn,tradestatus,isST"
    )
    _ADJUST_FLAGS = {
        PriceAdjustment.NONE: "3",
        PriceAdjustment.FORWARD: "2",
        PriceAdjustment.BACKWARD: "1",
    }

    def __init__(
        self,
        client: Any | None = None,
        *,
        timeout_seconds: float = 15.0,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 0.25,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds cannot be negative")
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._max_attempts = max_attempts
        self._retry_backoff_seconds = retry_backoff_seconds
        self._sleep = sleep

    def fetch_daily_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> tuple[DailyBar, ...]:
        """无需用户账户或 API 令牌即可获取日线。"""

        if start > end:
            raise ValueError("start must be on or before end")

        canonical_symbol, provider_symbol = _normalize_symbol(symbol)
        client = self._client or _import_baostock()
        last_error: BaoStockError | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                return self._fetch_once(
                    client,
                    canonical_symbol,
                    provider_symbol,
                    start,
                    end,
                    adjustment,
                )
            except BaoStockError as exc:
                last_error = exc
                if attempt < self._max_attempts:
                    self._sleep(self._retry_backoff_seconds * (2 ** (attempt - 1)))

        assert last_error is not None
        raise BaoStockError(
            f"BaoStock daily query failed after {self._max_attempts} attempts: {last_error}"
        ) from last_error

    async def fetch_daily_bars_async(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> tuple[DailyBar, ...]:
        """在 asyncio 图形界面/服务事件循环之外运行阻塞式 SDK。"""

        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    partial(
                        self.fetch_daily_bars,
                        symbol,
                        start,
                        end,
                        adjustment=adjustment,
                    )
                ),
                timeout=self._timeout_seconds,
            )
        except TimeoutError as exc:
            raise BaoStockTimeoutError(
                f"BaoStock daily query exceeded {self._timeout_seconds:g}s"
            ) from exc

    def fetch_trade_calendar(
        self,
        start: date,
        end: date,
    ) -> tuple[TradeCalendarDay, ...]:
        """获取并严格验证 BaoStock 的闭区间交易日历。"""

        if start > end:
            raise ValueError("start must be on or before end")

        client = self._client or _import_baostock()
        last_error: BaoStockError | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                return self._fetch_trade_calendar_once(client, start, end)
            except BaoStockError as exc:
                last_error = exc
                if attempt < self._max_attempts:
                    self._sleep(self._retry_backoff_seconds * (2 ** (attempt - 1)))

        assert last_error is not None
        raise BaoStockError(
            "BaoStock trade-calendar query failed after "
            f"{self._max_attempts} attempts: {last_error}"
        ) from last_error

    async def fetch_trade_calendar_async(
        self,
        start: date,
        end: date,
    ) -> tuple[TradeCalendarDay, ...]:
        """在 asyncio 循环之外运行阻塞式日历 SDK 调用。"""

        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self.fetch_trade_calendar, start, end),
                timeout=self._timeout_seconds,
            )
        except TimeoutError as exc:
            raise BaoStockTimeoutError(
                f"BaoStock trade-calendar query exceeded {self._timeout_seconds:g}s"
            ) from exc

    def _fetch_once(
        self,
        client: Any,
        canonical_symbol: str,
        provider_symbol: str,
        start: date,
        end: date,
        adjustment: PriceAdjustment,
    ) -> tuple[DailyBar, ...]:
        with _BAOSTOCK_SESSION_LOCK:
            return self._fetch_once_in_session(
                client,
                canonical_symbol,
                provider_symbol,
                start,
                end,
                adjustment,
            )

    def _fetch_once_in_session(
        self,
        client: Any,
        canonical_symbol: str,
        provider_symbol: str,
        start: date,
        end: date,
        adjustment: PriceAdjustment,
    ) -> tuple[DailyBar, ...]:
        logged_in = False
        try:
            try:
                login = client.login()
            except Exception as exc:
                raise BaoStockError("BaoStock login raised an exception") from exc
            if getattr(login, "error_code", None) != "0":
                message = getattr(login, "error_msg", "unknown login error")
                raise BaoStockError(f"BaoStock login failed: {message}")
            logged_in = True

            try:
                result = client.query_history_k_data_plus(
                    provider_symbol,
                    self._FIELDS,
                    start_date=start.isoformat(),
                    end_date=end.isoformat(),
                    frequency="d",
                    adjustflag=self._ADJUST_FLAGS[adjustment],
                )
            except Exception as exc:
                raise BaoStockError("BaoStock query raised an exception") from exc
            if getattr(result, "error_code", None) != "0":
                message = getattr(result, "error_msg", "unknown query error")
                raise BaoStockError(f"BaoStock query failed: {message}")

            bars: list[DailyBar] = []
            try:
                while result.next():
                    row = dict(zip(result.fields, result.get_row_data(), strict=True))
                    bars.append(_parse_daily_bar(canonical_symbol, row, adjustment))
            except BaoStockError:
                raise
            except Exception as exc:
                raise BaoStockError(
                    f"invalid BaoStock result payload for {canonical_symbol}"
                ) from exc
            return tuple(bars)
        finally:
            if logged_in:
        # 登出失败不得掩盖查询结果或错误；下一次重试会打开新的匿名提供者会话。
                with suppress(Exception):
                    client.logout()

    def _fetch_trade_calendar_once(
        self,
        client: Any,
        start: date,
        end: date,
    ) -> tuple[TradeCalendarDay, ...]:
        with _BAOSTOCK_SESSION_LOCK:
            logged_in = False
            try:
                try:
                    login = client.login()
                except Exception as exc:
                    raise BaoStockError("BaoStock login raised an exception") from exc
                if getattr(login, "error_code", None) != "0":
                    message = getattr(login, "error_msg", "unknown login error")
                    raise BaoStockError(f"BaoStock login failed: {message}")
                logged_in = True

                try:
                    result = client.query_trade_dates(
                        start_date=start.isoformat(),
                        end_date=end.isoformat(),
                    )
                except Exception as exc:
                    raise BaoStockError(
                        "BaoStock trade-calendar query raised an exception"
                    ) from exc
                if getattr(result, "error_code", None) != "0":
                    message = getattr(result, "error_msg", "unknown query error")
                    raise BaoStockError(
                        f"BaoStock trade-calendar query failed: {message}"
                    )

                days: list[TradeCalendarDay] = []
                try:
                    while result.next():
                        row = dict(
                            zip(result.fields, result.get_row_data(), strict=True)
                        )
                        days.append(_parse_trade_calendar_day(row))
                except BaoStockError:
                    raise
                except Exception as exc:
                    raise BaoStockError(
                        "invalid BaoStock trade-calendar result payload"
                    ) from exc
                _validate_trade_calendar(days, start=start, end=end)
                return tuple(days)
            finally:
                if logged_in:
                    with suppress(Exception):
                        client.logout()


def _import_baostock() -> Any:
    try:
        import baostock
    except ImportError as exc:  # pragma: no cover - exercised only in a broken install
        raise BaoStockError("baostock is not installed") from exc
    return baostock


def _normalize_symbol(symbol: str) -> tuple[str, str]:
    value = symbol.strip().upper()
    if "." not in value:
        if len(value) != 6 or not value.isdigit():
            raise ValueError(
                "symbol must look like 600000.SH, 000001.SZ, or 430047.BJ"
            )
        if value.startswith(("4", "8", "92")):
            suffix = "BJ"
        else:
            suffix = "SH" if value.startswith(("5", "6", "9")) else "SZ"
        value = f"{value}.{suffix}"

    code, exchange = value.split(".", maxsplit=1)
    if len(code) != 6 or not code.isdigit() or exchange not in {"SH", "SZ", "BJ"}:
        raise ValueError(
            "symbol must look like 600000.SH, 000001.SZ, or 430047.BJ"
        )
    return value, f"{exchange.lower()}.{code}"


def _parse_daily_bar(
    symbol: str,
    row: dict[str, str],
    adjustment: PriceAdjustment,
) -> DailyBar:
    try:
        return DailyBar(
            symbol=symbol,
            trade_date=date.fromisoformat(row["date"]),
            open=_optional_decimal(row["open"]),
            high=_optional_decimal(row["high"]),
            low=_optional_decimal(row["low"]),
            close=_optional_decimal(row["close"]),
            previous_close=_optional_decimal(row["preclose"]),
            volume=_integer_or_zero(row["volume"]),
            amount=_decimal_or_zero(row["amount"]),
            turnover_percent=_optional_decimal(row["turn"]),
            is_trading=row["tradestatus"] == "1",
            is_st=row["isST"] == "1",
            adjustment=adjustment,
        )
    except (KeyError, ValueError, ArithmeticError) as exc:
        raise BaoStockError(f"invalid BaoStock daily row for {symbol}: {row!r}") from exc


def _parse_trade_calendar_day(row: dict[str, str]) -> TradeCalendarDay:
    try:
        marker = row["is_trading_day"]
        if marker not in {"0", "1"}:
            raise ValueError("is_trading_day must be 0 or 1")
        return TradeCalendarDay(
            calendar_date=date.fromisoformat(row["calendar_date"]),
            is_trading_day=marker == "1",
        )
    except (KeyError, ValueError) as exc:
        raise BaoStockError(f"invalid BaoStock trade-calendar row: {row!r}") from exc


def _validate_trade_calendar(
    days: list[TradeCalendarDay],
    *,
    start: date,
    end: date,
) -> None:
    expected_count = (end - start).days + 1
    if len(days) != expected_count:
        raise BaoStockError(
            "BaoStock trade-calendar did not cover every requested natural day"
        )
    for offset, day in enumerate(days):
        expected = date.fromordinal(start.toordinal() + offset)
        if day.calendar_date != expected:
            raise BaoStockError(
                "BaoStock trade-calendar dates must be unique, ordered, and complete"
            )


def _optional_decimal(value: str) -> Decimal | None:
    return Decimal(value) if value else None


def _decimal_or_zero(value: str) -> Decimal:
    return Decimal(value) if value else Decimal(0)


def _integer_or_zero(value: str) -> int:
    return int(Decimal(value)) if value else 0
