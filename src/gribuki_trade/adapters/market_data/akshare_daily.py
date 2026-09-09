"""严格的不复权 A 股股票与 ETF 日线历史适配器。

AKShare 的东方财富股票函数与 ETF 函数具有不同端点语义。本模块把已经明确
分类的证券只路由到其中一个端点，拒绝复权价格，并且只将验证后的记录规范为
``DailyBar``。上游表格包含交易行情柱，而非自然日日历；因此缺失的停牌日绝不
会被静默向前填充。
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import date
from functools import partial
from typing import Any

from gribuki_trade.adapters.ashare.screening import (
    _SINA_HISTORY_LOCK,
    SINA_HISTORY_SOURCE_ID,
)
from gribuki_trade.adapters.market_data.akshare_daily_parsing import (
    _SINA_DAILY_FALLBACK_WARNINGS,
    AKShareDailyAssetType,
    AKShareDailyError,
    AKShareDailyNoDataError,
    AKShareDailyPayloadError,
    AKShareDailyTimeoutError,
    AKShareDailyUnsupportedAdjustmentError,
    _date_value,
    _frame_records,
    _infer_asset_type,
    _normalize_asset_types,
    _normalize_symbol,
    _parse_row,
    _resolve_columns,
    _validate_and_sort_bars,
    _validate_request,
)
from gribuki_trade.adapters.market_data.akshare_daily_router import (
    HistoricalDailyCoverageError,
    HistoricalDailyFallbackError,
    HistoricalDailyFallbackRouter,
    HistoricalDailyOverlapMismatchError,
    HistoricalDailyProvider,
    HistoricalDailyRouteResult,
    HistoricalDailySourceResult,
    HistoricalDailyTailStitchError,
)
from gribuki_trade.adapters.market_data.akshare_daily_stitch import (
    HistoricalDailyTailStitchDiagnostics,
    HistoricalDailyTailStitchPolicy,
)
from gribuki_trade.domain.market import DailyBar, PriceAdjustment

__all__ = (
    "AKShareDailyAssetType",
    "AKShareDailyError",
    "AKShareDailyNoDataError",
    "AKShareDailyPayloadError",
    "AKShareDailyTimeoutError",
    "AKShareDailyUnsupportedAdjustmentError",
    "AKShareHistoricalDailyAdapter",
    "HistoricalDailyCoverageError",
    "HistoricalDailyFallbackError",
    "HistoricalDailyFallbackRouter",
    "HistoricalDailyOverlapMismatchError",
    "HistoricalDailyProvider",
    "HistoricalDailyRouteResult",
    "HistoricalDailySourceResult",
    "HistoricalDailyTailStitchDiagnostics",
    "HistoricalDailyTailStitchError",
    "HistoricalDailyTailStitchPolicy",
)


class AKShareHistoricalDailyAdapter:
    """A 股股票和 ETF 的 AKShare/东方财富原始价格历史。

    首先从 ``asset_types`` 解析证券类型。回退代码族规则识别常见交易所交易
    基金族（沪市 ``5xxxxx`` 和深市 ``1xxxxx``）。生产调用方处理特殊产品时
    应传入明确映射，而不是依赖这一保守规则。
    """

    def __init__(
        self,
        client: Any | None = None,
        *,
        asset_types: Mapping[str, AKShareDailyAssetType | str] | None = None,
        timeout_seconds: float = 15.0,
        minimum_source_bars: int = 1,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if minimum_source_bars < 1:
            raise ValueError("minimum_source_bars must be positive")
        self._client = client
        self._asset_types = _normalize_asset_types(asset_types or {})
        self._timeout_seconds = timeout_seconds
        self._minimum_source_bars = minimum_source_bars

    def fetch_daily_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> tuple[DailyBar, ...]:
        """通过私有循环对各来源分别设置截止时间，获取严格窗口。"""

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.fetch_daily_bars_async(
                    symbol,
                    start,
                    end,
                    adjustment=adjustment,
                )
            )
        raise RuntimeError(
            "fetch_daily_bars cannot run inside an active event loop; "
            "await fetch_daily_bars_async instead"
        )

    def fetch_daily_bars_with_source(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> HistoricalDailySourceResult:
        """返回所选 AKShare 子来源的阻塞式诊断变体。"""

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.fetch_daily_bars_async_with_source(
                    symbol,
                    start,
                    end,
                    adjustment=adjustment,
                )
            )
        raise RuntimeError(
            "fetch_daily_bars_with_source cannot run inside an active event loop; "
            "await fetch_daily_bars_async_with_source instead"
        )

    async def fetch_daily_bars_async(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> tuple[DailyBar, ...]:
        return (
            await self.fetch_daily_bars_async_with_source(
                symbol,
                start,
                end,
                adjustment=adjustment,
            )
        ).bars

    async def fetch_daily_bars_async_with_source(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> HistoricalDailySourceResult:
        """在各自独立超时内尝试每个适用来源。"""

        canonical_symbol, code = _normalize_symbol(symbol)
        _validate_request(start, end, adjustment)
        asset_type = self._asset_types.get(
            canonical_symbol,
            _infer_asset_type(code),
        )
        client = self._client or _import_akshare()
        if asset_type is AKShareDailyAssetType.STOCK:
            failures: list[str] = []
            eastmoney_error: AKShareDailyError | None = None
            try:
                eastmoney_bars = await self._fetch_source_async(
                    client,
                    operation="stock_zh_a_hist",
                    kwargs={
                        "symbol": code,
                        "period": "daily",
                        "start_date": start.strftime("%Y%m%d"),
                        "end_date": end.strftime("%Y%m%d"),
                        "adjust": "",
                        "timeout": self._timeout_seconds,
                    },
                    canonical_symbol=canonical_symbol,
                    code=code,
                    asset_type=asset_type,
                    start=start,
                    end=end,
                    filter_to_window=False,
                    derive_adjacent_previous_close=False,
                )
                _require_minimum_source_bars(
                    eastmoney_bars,
                    minimum=self._minimum_source_bars,
                    operation="stock_zh_a_hist",
                )
                _require_exact_requested_end(
                    eastmoney_bars,
                    end=end,
                    operation="stock_zh_a_hist",
                )
                return HistoricalDailySourceResult(
                    bars=eastmoney_bars,
                    selected_source="AKShare/Eastmoney stock_zh_a_hist",
                )
            except AKShareDailyError as exc:
                eastmoney_error = exc
                failures.append(f"stock_zh_a_hist:{type(exc).__name__}")

            exchange = canonical_symbol.rsplit(".", maxsplit=1)[1].lower()
            if (
                exchange not in {"sh", "sz"}
                or not callable(getattr(client, "stock_zh_a_daily", None))
            ):
                assert eastmoney_error is not None
                raise eastmoney_error
            try:
                sina_bars = await self._fetch_source_async(
                    client,
                    operation="stock_zh_a_daily",
                    kwargs={
                        "symbol": f"{exchange}{code}",
                        "start_date": start.strftime("%Y%m%d"),
                        "end_date": end.strftime("%Y%m%d"),
                        "adjust": "",
                    },
                    canonical_symbol=canonical_symbol,
                    code=code,
                    asset_type=asset_type,
                    start=start,
                    end=end,
                    filter_to_window=True,
                    derive_adjacent_previous_close=True,
                    serialize_sina=True,
                )
                _require_minimum_source_bars(
                    sina_bars,
                    minimum=self._minimum_source_bars,
                    operation="stock_zh_a_daily",
                )
                _require_exact_requested_end(
                    sina_bars,
                    end=end,
                    operation="stock_zh_a_daily",
                )
                return HistoricalDailySourceResult(
                    bars=sina_bars,
                    selected_source=SINA_HISTORY_SOURCE_ID,
                    source_failures=tuple(failures),
                    warnings=_SINA_DAILY_FALLBACK_WARNINGS,
                )
            except AKShareDailyError as exc:
                failures.append(f"stock_zh_a_daily:{type(exc).__name__}")
                raise AKShareDailyError(
                    "all AKShare stock daily sources failed: " + ", ".join(failures)
                ) from exc

        etf_failures: list[str] = []
        etf_eastmoney_error: AKShareDailyError | None = None
        try:
            eastmoney_bars = await self._fetch_source_async(
                client,
                operation="fund_etf_hist_em",
                kwargs={
                    "symbol": code,
                    "period": "daily",
                    "start_date": start.strftime("%Y%m%d"),
                    "end_date": end.strftime("%Y%m%d"),
                    "adjust": "",
                },
                canonical_symbol=canonical_symbol,
                code=code,
                asset_type=asset_type,
                start=start,
                end=end,
                filter_to_window=False,
                derive_adjacent_previous_close=False,
            )
            _require_minimum_source_bars(
                eastmoney_bars,
                minimum=self._minimum_source_bars,
                operation="fund_etf_hist_em",
            )
            _require_exact_requested_end(
                eastmoney_bars,
                end=end,
                operation="fund_etf_hist_em",
            )
            return HistoricalDailySourceResult(
                bars=eastmoney_bars,
                selected_source="AKShare/Eastmoney fund_etf_hist_em",
            )
        except AKShareDailyError as exc:
            etf_eastmoney_error = exc
            etf_failures.append(f"fund_etf_hist_em:{type(exc).__name__}")

        if not callable(getattr(client, "fund_etf_hist_sina", None)):
            assert etf_eastmoney_error is not None
            raise etf_eastmoney_error
        exchange = canonical_symbol.rsplit(".", maxsplit=1)[1].lower()
        try:
            sina_bars = await self._fetch_source_async(
                client,
                operation="fund_etf_hist_sina",
                kwargs={"symbol": f"{exchange}{code}"},
                canonical_symbol=canonical_symbol,
                code=code,
                asset_type=asset_type,
                start=start,
                end=end,
                filter_to_window=True,
                derive_adjacent_previous_close=True,
                serialize_sina=True,
            )
            _require_minimum_source_bars(
                sina_bars,
                minimum=self._minimum_source_bars,
                operation="fund_etf_hist_sina",
            )
            _require_exact_requested_end(
                sina_bars,
                end=end,
                operation="fund_etf_hist_sina",
            )
            return HistoricalDailySourceResult(
                bars=sina_bars,
                selected_source="AKShare/Sina fund_etf_hist_sina",
                source_failures=tuple(etf_failures),
                warnings=_SINA_DAILY_FALLBACK_WARNINGS,
            )
        except AKShareDailyError as exc:
            etf_failures.append(f"fund_etf_hist_sina:{type(exc).__name__}")
            raise AKShareDailyError(
                "all AKShare ETF daily sources failed: " + ", ".join(etf_failures)
            ) from exc

    async def _fetch_source_async(
        self,
        client: Any,
        *,
        operation: str,
        kwargs: Mapping[str, object],
        canonical_symbol: str,
        code: str,
        asset_type: AKShareDailyAssetType,
        start: date,
        end: date,
        filter_to_window: bool,
        derive_adjacent_previous_close: bool,
        serialize_sina: bool = False,
    ) -> tuple[DailyBar, ...]:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    partial(
                        _fetch_source_once,
                        client,
                        operation=operation,
                        kwargs=kwargs,
                        canonical_symbol=canonical_symbol,
                        code=code,
                        asset_type=asset_type,
                        start=start,
                        end=end,
                        filter_to_window=filter_to_window,
                        derive_adjacent_previous_close=(
                            derive_adjacent_previous_close
                        ),
                        serialize_sina=serialize_sina,
                    )
                ),
                timeout=self._timeout_seconds,
            )
        except TimeoutError as exc:
            raise AKShareDailyTimeoutError(
                f"AKShare {operation} exceeded {self._timeout_seconds:g}s"
            ) from exc


def _import_akshare() -> Any:
    try:
        import akshare  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - declared project dependency
        raise AKShareDailyError("akshare is not installed") from exc
    return akshare


def _fetch_source_once(
    client: Any,
    *,
    operation: str,
    kwargs: Mapping[str, object],
    canonical_symbol: str,
    code: str,
    asset_type: AKShareDailyAssetType,
    start: date,
    end: date,
    filter_to_window: bool,
    derive_adjacent_previous_close: bool,
    serialize_sina: bool,
) -> tuple[DailyBar, ...]:
    method = getattr(client, operation, None)
    if method is None or not callable(method):
        raise AKShareDailyError(f"installed AKShare has no callable {operation}")
    try:
        if serialize_sina:
            with _SINA_HISTORY_LOCK:
                frame = method(**kwargs)
        else:
            frame = method(**kwargs)
    except (KeyboardInterrupt, SystemExit):
        raise
    except AKShareDailyError:
        raise
    except Exception as exc:
        raise AKShareDailyError(f"AKShare {operation} failed") from exc
    rows = _frame_records(frame, operation)
    columns = _resolve_columns(rows, operation)
    if filter_to_window:
        date_column = columns["date"]
        rows = [
            row
            for row in rows
            if start <= _date_value(row.get(date_column)) <= end
        ]
        if not rows:
            raise AKShareDailyNoDataError(
                f"AKShare {operation} returned no rows inside the requested window"
            )
    bars = tuple(
        _parse_row(
            row,
            columns,
            symbol=canonical_symbol,
            code=code,
            asset_type=asset_type,
        )
        for row in rows
    )
    return _validate_and_sort_bars(
        bars,
        start=start,
        end=end,
        operation=operation,
        filter_to_window=filter_to_window,
        derive_adjacent_previous_close=derive_adjacent_previous_close,
    )


def _require_minimum_source_bars(
    bars: tuple[DailyBar, ...],
    *,
    minimum: int,
    operation: str,
) -> None:
    if len(bars) < minimum:
        raise AKShareDailyNoDataError(
            f"AKShare {operation} returned {len(bars)} bars; required {minimum}"
        )


def _require_exact_requested_end(
    bars: tuple[DailyBar, ...],
    *,
    end: date,
    operation: str,
) -> None:
    latest = bars[-1].trade_date
    if latest != end:
        raise AKShareDailyNoDataError(
            f"AKShare {operation} latest session {latest.isoformat()} does not match "
            f"exact requested end {end.isoformat()}"
        )
