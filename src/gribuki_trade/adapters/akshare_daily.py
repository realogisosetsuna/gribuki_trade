"""严格的不复权 A 股股票与 ETF 日线历史适配器。

AKShare 的东方财富股票函数与 ETF 函数具有不同端点语义。本模块把已经明确
分类的证券只路由到其中一个端点，拒绝复权价格，并且只将验证后的记录规范为
``DailyBar``。上游表格包含交易行情柱，而非自然日日历；因此缺失的停牌日绝不
会被静默向前填充。
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from functools import partial
from typing import Any, Protocol

from gribuki_trade.adapters.ashare_screening import (
    _SINA_HISTORY_LOCK,
    SINA_HISTORY_SOURCE_ID,
)
from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.ports.market_data import (
    AsyncHistoricalDailyData,
    HistoricalDailyData,
    MarketDataTimeoutError,
    MarketDataUnavailableError,
)


class AKShareDailyError(MarketDataUnavailableError):
    """AKShare 历史日线数据的提供者/传输失败基类。"""


class AKShareDailyNoDataError(AKShareDailyError):
    """所选 AKShare 端点没有返回请求窗口内的记录。"""


class AKShareDailyPayloadError(AKShareDailyError):
    """所选端点返回了无效或有歧义的日线载荷。"""


class AKShareDailyTimeoutError(MarketDataTimeoutError, AKShareDailyError):
    """阻塞式 AKShare 来源超过异步调用方的时间限制。"""


class AKShareDailyUnsupportedAdjustmentError(AKShareDailyError):
    """只接受数据提供者的原始不复权价格。"""


class HistoricalDailyFallbackError(MarketDataUnavailableError):
    """主来源与回退来源均未提供可接受的历史。"""


class HistoricalDailyCoverageError(HistoricalDailyFallbackError):
    """两个来源返回的日线数量都少于配置要求。"""


class HistoricalDailyTailStitchError(HistoricalDailyFallbackError):
    """无法证明所请求的受控尾部拼接是安全的。"""


class HistoricalDailyOverlapMismatchError(HistoricalDailyTailStitchError):
    """候选来源之间近期共有日期或开高低收值不一致。"""


class AKShareDailyAssetType(StrEnum):
    STOCK = "stock"
    ETF = "etf"


class HistoricalDailyProvider(
    HistoricalDailyData,
    AsyncHistoricalDailyData,
    Protocol,
):
    """同时支持阻塞批处理和异步服务调用方的数据提供者。"""


@dataclass(frozen=True, slots=True)
class HistoricalDailyRouteResult:
    """供需要了解提供者选择的调用方使用的可选诊断结果。"""

    bars: tuple[DailyBar, ...]
    selected_source: str
    primary_count: int | None
    fallback_count: int | None
    primary_failure: str | None = None
    selected_source_failures: tuple[str, ...] = ()
    tail_stitch: HistoricalDailyTailStitchDiagnostics | None = None
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class HistoricalDailyTailStitchPolicy:
    """将延迟的长历史与新鲜尾部连接的明确选择加入策略。

    请求的 ``end`` 日期被视为 ``latest_completed_session``。只有主来源包含该
    精确日期时，才允许产生拼接结果。
    """

    minimum_overlap_sessions: int = 20

    def __post_init__(self) -> None:
        if self.minimum_overlap_sessions < 20:
            raise ValueError("minimum_overlap_sessions must be at least 20")


@dataclass(frozen=True, slots=True)
class HistoricalDailyTailStitchDiagnostics:
    """拼接结果的可审计证明与非阻塞单元诊断。"""

    base_source: str
    tail_source: str
    required_latest_session: date
    base_latest_session: date
    overlap_start_session: date
    overlap_end_session: date
    overlap_sessions_validated: int
    stitched_tail_sessions: int
    volume_mismatch_sessions: int
    amount_mismatch_sessions: int

    def __post_init__(self) -> None:
        if self.overlap_sessions_validated < 20:
            raise ValueError("overlap_sessions_validated must be at least 20")
        if self.stitched_tail_sessions < 1:
            raise ValueError("stitched_tail_sessions must be positive")
        if self.base_latest_session >= self.required_latest_session:
            raise ValueError("base source must lag required_latest_session")


@dataclass(frozen=True, slots=True)
class HistoricalDailySourceResult:
    """具有多个独立来源的数据提供者所返回的诊断结果。"""

    bars: tuple[DailyBar, ...]
    selected_source: str
    source_failures: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


_COLUMN_ALIASES: Mapping[str, tuple[str, ...]] = {
    "date": ("日期", "date", "trade_date"),
    "symbol": ("股票代码", "基金代码", "代码", "symbol", "code"),
    "open": ("开盘", "开盘价", "open"),
    "close": ("收盘", "收盘价", "close"),
    "high": ("最高", "最高价", "high"),
    "low": ("最低", "最低价", "low"),
    "volume": ("成交量", "volume"),
    "amount": ("成交额", "amount", "turnover"),
    "amplitude_percent": ("振幅", "amplitude", "amplitude_percent"),
    "change_percent": ("涨跌幅", "change_percent", "pct_change"),
    "change_amount": ("涨跌额", "change", "change_amount"),
    "turnover_percent": ("换手率", "turnover_percent"),
    "previous_close": ("昨收", "昨收价", "preclose", "previous_close"),
    "trading_status": ("交易状态", "交易状态码", "tradestatus", "is_trading"),
    "is_st": ("是否ST", "isST", "is_st"),
}
_SINA_STOCK_COLUMN_ALIASES: Mapping[str, tuple[str, ...]] = {
    "date": ("date",),
    "open": ("open",),
    "close": ("close",),
    "high": ("high",),
    "low": ("low",),
    "volume": ("volume",),
    "amount": ("amount",),
}
_REQUIRED_COLUMNS = frozenset(
    {"date", "open", "close", "high", "low", "volume", "amount"}
)
_TRADING_MARKERS = frozenset({"1", "true", "交易", "正常", "交易中"})
_SUSPENDED_MARKERS = frozenset({"0", "false", "停牌", "暂停交易"})
_TRUE_MARKERS = frozenset({"1", "true", "是", "st"})
_FALSE_MARKERS = frozenset({"0", "false", "否", "非st", "normal"})
_SINA_DAILY_FALLBACK_WARNINGS = (
    "SINA_FALLBACK_IS_UNADJUSTED_DAILY_HISTORY",
    "SINA_PREVIOUS_CLOSE_DERIVED_FROM_ADJACENT_RAW_CLOSES",
    "SINA_CORPORATE_ACTION_GUARD_HAS_NO_INDEPENDENT_REFERENCE_CLOSE",
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


class HistoricalDailyFallbackRouter:
    """优先采用一个完整来源，并提供选择加入的已证明尾部拼接策略。

    默认情况下，路由器绝不混合数据提供者。启用 ``tail_stitch_policy`` 后，
    只有精确的近期日期/开高低收一致性能证明边界安全时，才可将主来源中晚于
    较长回退历史的日期追加到尾部；绝不覆盖回退来源的重叠记录。
    """

    def __init__(
        self,
        primary: HistoricalDailyProvider,
        fallback: HistoricalDailyProvider,
        *,
        minimum_bars: int = 200,
        primary_name: str = "primary",
        fallback_name: str = "fallback",
        tail_stitch_policy: HistoricalDailyTailStitchPolicy | None = None,
    ) -> None:
        if minimum_bars < 1:
            raise ValueError("minimum_bars must be positive")
        if not primary_name.strip() or not fallback_name.strip():
            raise ValueError("provider names cannot be blank")
        self._primary = primary
        self._fallback = fallback
        self._minimum_bars = minimum_bars
        self._primary_name = primary_name.strip()
        self._fallback_name = fallback_name.strip()
        self._tail_stitch_policy = tail_stitch_policy

    def fetch_daily_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> tuple[DailyBar, ...]:
        return self.fetch_daily_bars_with_route(
            symbol,
            start,
            end,
            adjustment=adjustment,
        ).bars

    def fetch_daily_bars_with_route(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> HistoricalDailyRouteResult:
        primary_bars: tuple[DailyBar, ...] | None = None
        primary_failure: MarketDataUnavailableError | None = None
        primary_selected_source = self._primary_name
        primary_source_failures: tuple[str, ...] = ()
        primary_warnings: tuple[str, ...] = ()
        try:
            (
                primary_bars,
                primary_selected_source,
                primary_source_failures,
                primary_warnings,
            ) = _fetch_provider_sync(
                self._primary,
                configured_name=self._primary_name,
                symbol=symbol,
                start=start,
                end=end,
                adjustment=adjustment,
            )
        except MarketDataUnavailableError as exc:
            primary_failure = exc
        if (
            primary_bars is not None
            and len(primary_bars) >= self._minimum_bars
            and (
                self._tail_stitch_policy is None
                or primary_bars[-1].trade_date == end
            )
        ):
            return HistoricalDailyRouteResult(
                bars=primary_bars,
                selected_source=primary_selected_source,
                primary_count=len(primary_bars),
                fallback_count=None,
                selected_source_failures=primary_source_failures,
                warnings=primary_warnings,
            )
        try:
            (
                fallback_bars,
                fallback_selected_source,
                fallback_source_failures,
                fallback_warnings,
            ) = _fetch_provider_sync(
                self._fallback,
                configured_name=self._fallback_name,
                symbol=symbol,
                start=start,
                end=end,
                adjustment=adjustment,
            )
        except MarketDataUnavailableError as exc:
            reason = (
                type(primary_failure).__name__
                if primary_failure is not None
                else f"coverage={len(primary_bars or ())}"
            )
            raise HistoricalDailyFallbackError(
                f"{self._primary_name} was unacceptable ({reason}); "
                f"{self._fallback_name} failed ({type(exc).__name__})"
            ) from exc
        stitched = self._tail_stitch_if_required(
            primary_bars=primary_bars,
            primary_source=primary_selected_source,
            fallback_bars=fallback_bars,
            fallback_source=fallback_selected_source,
            required_latest_session=end,
        )
        if stitched is not None:
            stitched_bars, diagnostics = stitched
            if len(stitched_bars) < self._minimum_bars:
                raise HistoricalDailyCoverageError(
                    "stitched historical daily coverage below requirement: "
                    f"required={self._minimum_bars}, stitched={len(stitched_bars)}"
                )
            return HistoricalDailyRouteResult(
                bars=stitched_bars,
                selected_source=(
                    "MIXED/TAIL_STITCH base="
                    f"{fallback_selected_source} tail={primary_selected_source}"
                ),
                primary_count=len(primary_bars or ()),
                fallback_count=len(fallback_bars),
                selected_source_failures=(
                    *primary_source_failures,
                    *fallback_source_failures,
                ),
                tail_stitch=diagnostics,
                warnings=tuple(dict.fromkeys((*primary_warnings, *fallback_warnings))),
            )
        if len(fallback_bars) < self._minimum_bars:
            raise HistoricalDailyCoverageError(
                "historical daily coverage below requirement: "
                f"required={self._minimum_bars}, "
                f"{self._primary_name}={len(primary_bars or ())}, "
                f"{self._fallback_name}={len(fallback_bars)}"
            )
        return HistoricalDailyRouteResult(
            bars=fallback_bars,
            selected_source=fallback_selected_source,
            primary_count=None if primary_bars is None else len(primary_bars),
            fallback_count=len(fallback_bars),
            primary_failure=(
                None if primary_failure is None else type(primary_failure).__name__
            ),
            selected_source_failures=fallback_source_failures,
            warnings=fallback_warnings,
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
            await self.fetch_daily_bars_async_with_route(
                symbol,
                start,
                end,
                adjustment=adjustment,
            )
        ).bars

    async def fetch_daily_bars_async_with_route(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjustment: PriceAdjustment = PriceAdjustment.NONE,
    ) -> HistoricalDailyRouteResult:
        primary_bars: tuple[DailyBar, ...] | None = None
        primary_failure: MarketDataUnavailableError | None = None
        primary_selected_source = self._primary_name
        primary_source_failures: tuple[str, ...] = ()
        primary_warnings: tuple[str, ...] = ()
        try:
            (
                primary_bars,
                primary_selected_source,
                primary_source_failures,
                primary_warnings,
            ) = await _fetch_provider_async(
                self._primary,
                configured_name=self._primary_name,
                symbol=symbol,
                start=start,
                end=end,
                adjustment=adjustment,
            )
        except MarketDataUnavailableError as exc:
            primary_failure = exc
        if (
            primary_bars is not None
            and len(primary_bars) >= self._minimum_bars
            and (
                self._tail_stitch_policy is None
                or primary_bars[-1].trade_date == end
            )
        ):
            return HistoricalDailyRouteResult(
                bars=primary_bars,
                selected_source=primary_selected_source,
                primary_count=len(primary_bars),
                fallback_count=None,
                selected_source_failures=primary_source_failures,
                warnings=primary_warnings,
            )
        try:
            (
                fallback_bars,
                fallback_selected_source,
                fallback_source_failures,
                fallback_warnings,
            ) = await _fetch_provider_async(
                self._fallback,
                configured_name=self._fallback_name,
                symbol=symbol,
                start=start,
                end=end,
                adjustment=adjustment,
            )
        except MarketDataUnavailableError as exc:
            reason = (
                type(primary_failure).__name__
                if primary_failure is not None
                else f"coverage={len(primary_bars or ())}"
            )
            raise HistoricalDailyFallbackError(
                f"{self._primary_name} was unacceptable ({reason}); "
                f"{self._fallback_name} failed ({type(exc).__name__})"
            ) from exc
        stitched = self._tail_stitch_if_required(
            primary_bars=primary_bars,
            primary_source=primary_selected_source,
            fallback_bars=fallback_bars,
            fallback_source=fallback_selected_source,
            required_latest_session=end,
        )
        if stitched is not None:
            stitched_bars, diagnostics = stitched
            if len(stitched_bars) < self._minimum_bars:
                raise HistoricalDailyCoverageError(
                    "stitched historical daily coverage below requirement: "
                    f"required={self._minimum_bars}, stitched={len(stitched_bars)}"
                )
            return HistoricalDailyRouteResult(
                bars=stitched_bars,
                selected_source=(
                    "MIXED/TAIL_STITCH base="
                    f"{fallback_selected_source} tail={primary_selected_source}"
                ),
                primary_count=len(primary_bars or ()),
                fallback_count=len(fallback_bars),
                selected_source_failures=(
                    *primary_source_failures,
                    *fallback_source_failures,
                ),
                tail_stitch=diagnostics,
                warnings=tuple(dict.fromkeys((*primary_warnings, *fallback_warnings))),
            )
        if len(fallback_bars) < self._minimum_bars:
            raise HistoricalDailyCoverageError(
                "historical daily coverage below requirement: "
                f"required={self._minimum_bars}, "
                f"{self._primary_name}={len(primary_bars or ())}, "
                f"{self._fallback_name}={len(fallback_bars)}"
            )
        return HistoricalDailyRouteResult(
            bars=fallback_bars,
            selected_source=fallback_selected_source,
            primary_count=None if primary_bars is None else len(primary_bars),
            fallback_count=len(fallback_bars),
            primary_failure=(
                None if primary_failure is None else type(primary_failure).__name__
            ),
            selected_source_failures=fallback_source_failures,
            warnings=fallback_warnings,
        )

    def _tail_stitch_if_required(
        self,
        *,
        primary_bars: tuple[DailyBar, ...] | None,
        primary_source: str,
        fallback_bars: tuple[DailyBar, ...],
        fallback_source: str,
        required_latest_session: date,
    ) -> tuple[
        tuple[DailyBar, ...],
        HistoricalDailyTailStitchDiagnostics,
    ] | None:
        policy = self._tail_stitch_policy
        if policy is None:
            return None
        if not fallback_bars:
            raise HistoricalDailyTailStitchError(
                "tail stitch base source returned no bars"
            )
        fallback_latest = fallback_bars[-1].trade_date
        if fallback_latest >= required_latest_session:
            return None
        if primary_bars is None or not primary_bars:
            raise HistoricalDailyTailStitchError(
                "tail stitch required but fresh primary source was unavailable"
            )
        return _controlled_tail_stitch(
            base_bars=fallback_bars,
            base_source=fallback_source,
            tail_bars=primary_bars,
            tail_source=primary_source,
            required_latest_session=required_latest_session,
            minimum_overlap_sessions=policy.minimum_overlap_sessions,
        )


def _controlled_tail_stitch(
    *,
    base_bars: tuple[DailyBar, ...],
    base_source: str,
    tail_bars: tuple[DailyBar, ...],
    tail_source: str,
    required_latest_session: date,
    minimum_overlap_sessions: int,
) -> tuple[
    tuple[DailyBar, ...],
    HistoricalDailyTailStitchDiagnostics,
]:
    """只把经过严格证明的新鲜尾部连接到较长的延迟历史。"""

    _validate_stitch_input(base_bars, source=base_source)
    _validate_stitch_input(tail_bars, source=tail_source)
    if base_bars[0].symbol != tail_bars[0].symbol:
        raise HistoricalDailyTailStitchError(
            "tail stitch sources contain different symbols"
        )
    if base_bars[-1].trade_date >= required_latest_session:
        raise HistoricalDailyTailStitchError(
            "tail stitch base is not delayed relative to required latest session"
        )
    tail_by_date = {bar.trade_date: bar for bar in tail_bars}
    if required_latest_session not in tail_by_date:
        raise HistoricalDailyTailStitchError(
            "tail source does not contain required latest_completed_session "
            f"{required_latest_session.isoformat()}"
        )

    overlap_base = tuple(
        bar
        for bar in base_bars
        if bar.trade_date <= base_bars[-1].trade_date and bar.is_trading
    )[-minimum_overlap_sessions:]
    if len(overlap_base) < minimum_overlap_sessions:
        raise HistoricalDailyTailStitchError(
            "insufficient base history for tail-stitch overlap validation: "
            f"required={minimum_overlap_sessions}, available={len(overlap_base)}"
        )
    overlap_start = overlap_base[0].trade_date
    overlap_end = overlap_base[-1].trade_date
    overlap_tail = tuple(
        bar
        for bar in tail_bars
        if overlap_start <= bar.trade_date <= overlap_end
    )
    base_dates = tuple(bar.trade_date for bar in overlap_base)
    tail_dates = tuple(bar.trade_date for bar in overlap_tail)
    if tail_dates != base_dates:
        missing_from_tail = sorted(set(base_dates).difference(tail_dates))
        extra_in_tail = sorted(set(tail_dates).difference(base_dates))
        raise HistoricalDailyOverlapMismatchError(
            "tail-stitch overlap dates differ: "
            f"missing_from_tail={[item.isoformat() for item in missing_from_tail]}, "
            f"extra_in_tail={[item.isoformat() for item in extra_in_tail]}"
        )

    volume_mismatches = 0
    amount_mismatches = 0
    for base_bar, tail_bar in zip(overlap_base, overlap_tail, strict=True):
        if not tail_bar.is_trading:
            raise HistoricalDailyOverlapMismatchError(
                "tail-stitch common date is not trading in tail source: "
                f"{tail_bar.trade_date.isoformat()}"
            )
        base_ohlc = (base_bar.open, base_bar.high, base_bar.low, base_bar.close)
        tail_ohlc = (tail_bar.open, tail_bar.high, tail_bar.low, tail_bar.close)
        if base_ohlc != tail_ohlc:
            field_names = ("open", "high", "low", "close")
            differences = [
                f"{field}:base={base_value},tail={tail_value}"
                for field, base_value, tail_value in zip(
                    field_names,
                    base_ohlc,
                    tail_ohlc,
                    strict=True,
                )
                if base_value != tail_value
            ]
            raise HistoricalDailyOverlapMismatchError(
                "tail-stitch OHLC mismatch on "
                f"{base_bar.trade_date.isoformat()}: {', '.join(differences)}"
            )
        volume_mismatches += base_bar.volume != tail_bar.volume
        amount_mismatches += base_bar.amount != tail_bar.amount

    base_latest = base_bars[-1].trade_date
    fresh_tail = tuple(
        bar
        for bar in tail_bars
        if base_latest < bar.trade_date <= required_latest_session
    )
    if not fresh_tail or fresh_tail[-1].trade_date != required_latest_session:
        raise HistoricalDailyTailStitchError(
            "tail source cannot extend base through required latest session"
        )
    stitched = (*base_bars, *fresh_tail)
    stitched_dates = tuple(bar.trade_date for bar in stitched)
    if stitched_dates != tuple(sorted(stitched_dates)):
        raise HistoricalDailyTailStitchError(
            "stitched dates are not strictly ordered"
        )
    if len(stitched_dates) != len(set(stitched_dates)):
        raise HistoricalDailyTailStitchError(
            "stitched result contains duplicate dates"
        )
    return (
        stitched,
        HistoricalDailyTailStitchDiagnostics(
            base_source=base_source,
            tail_source=tail_source,
            required_latest_session=required_latest_session,
            base_latest_session=base_latest,
            overlap_start_session=overlap_start,
            overlap_end_session=overlap_end,
            overlap_sessions_validated=len(overlap_base),
            stitched_tail_sessions=len(fresh_tail),
            volume_mismatch_sessions=volume_mismatches,
            amount_mismatch_sessions=amount_mismatches,
        ),
    )


def _validate_stitch_input(
    bars: tuple[DailyBar, ...],
    *,
    source: str,
) -> None:
    if not bars:
        raise HistoricalDailyTailStitchError(
            f"tail stitch source {source} returned no bars"
        )
    dates = tuple(bar.trade_date for bar in bars)
    if dates != tuple(sorted(dates)) or len(dates) != len(set(dates)):
        raise HistoricalDailyTailStitchError(
            f"tail stitch source {source} dates must be ordered and unique"
        )
    symbol = bars[0].symbol
    if any(bar.symbol != symbol for bar in bars):
        raise HistoricalDailyTailStitchError(
            f"tail stitch source {source} contains mixed symbols"
        )
    if any(bar.adjustment is not PriceAdjustment.NONE for bar in bars):
        raise HistoricalDailyTailStitchError(
            f"tail stitch source {source} contains adjusted prices"
        )


def _fetch_provider_sync(
    provider: HistoricalDailyProvider,
    *,
    configured_name: str,
    symbol: str,
    start: date,
    end: date,
    adjustment: PriceAdjustment,
) -> tuple[tuple[DailyBar, ...], str, tuple[str, ...], tuple[str, ...]]:
    route_method = getattr(provider, "fetch_daily_bars_with_route", None)
    if callable(route_method):
        route = route_method(
            symbol,
            start,
            end,
            adjustment=adjustment,
        )
        if not isinstance(route, HistoricalDailyRouteResult):
            raise HistoricalDailyFallbackError(
                f"{configured_name} returned invalid route diagnostics"
            )
        return (
            route.bars,
            route.selected_source,
            route.selected_source_failures,
            route.warnings,
        )
    diagnostic_method = getattr(provider, "fetch_daily_bars_with_source", None)
    if callable(diagnostic_method):
        result = diagnostic_method(
            symbol,
            start,
            end,
            adjustment=adjustment,
        )
        if not isinstance(result, HistoricalDailySourceResult):
            raise HistoricalDailyFallbackError(
                f"{configured_name} returned invalid source diagnostics"
            )
        return result.bars, result.selected_source, result.source_failures, result.warnings
    archive_method = getattr(provider, "fetch_daily_bars_with_archive", None)
    if callable(archive_method):
        snapshot = archive_method(
            symbol,
            start,
            end,
            adjustment=adjustment,
        )
        return (
            tuple(snapshot.bars),
            f"LocalImmutableArchive/{snapshot.source_id}",
            (),
            (),
        )
    bars = tuple(
        provider.fetch_daily_bars(
            symbol,
            start,
            end,
            adjustment=adjustment,
        )
    )
    return bars, configured_name, (), ()


async def _fetch_provider_async(
    provider: HistoricalDailyProvider,
    *,
    configured_name: str,
    symbol: str,
    start: date,
    end: date,
    adjustment: PriceAdjustment,
) -> tuple[tuple[DailyBar, ...], str, tuple[str, ...], tuple[str, ...]]:
    route_method = getattr(provider, "fetch_daily_bars_async_with_route", None)
    if callable(route_method):
        route = await route_method(
            symbol,
            start,
            end,
            adjustment=adjustment,
        )
        if not isinstance(route, HistoricalDailyRouteResult):
            raise HistoricalDailyFallbackError(
                f"{configured_name} returned invalid async route diagnostics"
            )
        return (
            route.bars,
            route.selected_source,
            route.selected_source_failures,
            route.warnings,
        )
    diagnostic_method = getattr(provider, "fetch_daily_bars_async_with_source", None)
    if callable(diagnostic_method):
        result = await diagnostic_method(
            symbol,
            start,
            end,
            adjustment=adjustment,
        )
        if not isinstance(result, HistoricalDailySourceResult):
            raise HistoricalDailyFallbackError(
                f"{configured_name} returned invalid async source diagnostics"
            )
        return result.bars, result.selected_source, result.source_failures, result.warnings
    archive_method = getattr(provider, "fetch_daily_bars_async_with_archive", None)
    if callable(archive_method):
        snapshot = await archive_method(
            symbol,
            start,
            end,
            adjustment=adjustment,
        )
        return (
            tuple(snapshot.bars),
            f"LocalImmutableArchive/{snapshot.source_id}",
            (),
            (),
        )
    bars = tuple(
        await provider.fetch_daily_bars_async(
            symbol,
            start,
            end,
            adjustment=adjustment,
        )
    )
    return bars, configured_name, (), ()


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


def _normalize_asset_types(
    values: Mapping[str, AKShareDailyAssetType | str],
) -> dict[str, AKShareDailyAssetType]:
    output: dict[str, AKShareDailyAssetType] = {}
    for symbol, raw_type in values.items():
        canonical, _ = _normalize_symbol(symbol)
        try:
            asset_type = AKShareDailyAssetType(raw_type)
        except ValueError as exc:
            raise ValueError(f"unsupported asset type for {canonical}: {raw_type!r}") from exc
        output[canonical] = asset_type
    return output


def _normalize_symbol(symbol: str) -> tuple[str, str]:
    value = symbol.strip().upper()
    if "." not in value:
        if len(value) != 6 or not value.isdigit():
            raise ValueError(
                "symbol must look like 600000.SH, 000001.SZ, or 430047.BJ"
            )
        suffix = _infer_exchange(value)
        value = f"{value}.{suffix}"
    parts = value.split(".")
    if len(parts) != 2:
        raise ValueError(
            "symbol must look like 600000.SH, 000001.SZ, or 430047.BJ"
        )
    code, exchange = parts
    if len(code) != 6 or not code.isdigit() or exchange not in {"SH", "SZ", "BJ"}:
        raise ValueError(
            "symbol must look like 600000.SH, 000001.SZ, or 430047.BJ"
        )
    return value, code


def _infer_exchange(code: str) -> str:
    # 北交所股票使用旧式 4/8 系代码，当前编号方案还使用 92 系代码。应先检查
    # 92，再应用上海更宽泛的 9 系规则，避免无后缀的当前北交所代码被错误路由。
    if code.startswith(("4", "8", "92")):
        return "BJ"
    if code.startswith(("5", "6", "9")):
        return "SH"
    return "SZ"


def _infer_asset_type(code: str) -> AKShareDailyAssetType:
    if code.startswith(("1", "5")):
        return AKShareDailyAssetType.ETF
    return AKShareDailyAssetType.STOCK


def _validate_request(
    start: date,
    end: date,
    adjustment: PriceAdjustment,
) -> None:
    if start > end:
        raise ValueError("start must be on or before end")
    if adjustment is not PriceAdjustment.NONE:
        raise AKShareDailyUnsupportedAdjustmentError(
            "AKShare historical daily adapter accepts only PriceAdjustment.NONE"
        )


def _frame_records(frame: Any, operation: str) -> list[Mapping[str, Any]]:
    if frame is None or not hasattr(frame, "to_dict"):
        raise AKShareDailyPayloadError(f"AKShare {operation} did not return a DataFrame")
    try:
        records = frame.to_dict(orient="records")
    except (TypeError, ValueError, AttributeError) as exc:
        raise AKShareDailyPayloadError(
            f"AKShare {operation} returned an unreadable DataFrame"
        ) from exc
    if not isinstance(records, list) or any(not isinstance(row, Mapping) for row in records):
        raise AKShareDailyPayloadError(f"AKShare {operation} returned invalid records")
    if not records:
        raise AKShareDailyNoDataError(f"AKShare {operation} returned no rows")
    return records


def _resolve_columns(
    rows: Sequence[Mapping[str, Any]], operation: str
) -> dict[str, str]:
    provider_columns = {str(key) for row in rows for key in row}
    resolved: dict[str, str] = {}
    aliases_by_field = (
        _SINA_STOCK_COLUMN_ALIASES
        if operation == "stock_zh_a_daily"
        else _COLUMN_ALIASES
    )
    for canonical, aliases in aliases_by_field.items():
        matches = [alias for alias in aliases if alias in provider_columns]
        if len(matches) > 1:
            raise AKShareDailyPayloadError(
                f"AKShare {operation} has ambiguous columns for {canonical}: {matches}"
            )
        if matches:
            resolved[canonical] = matches[0]
    missing = sorted(_REQUIRED_COLUMNS.difference(resolved))
    if missing:
        raise AKShareDailyPayloadError(
            f"AKShare {operation} missing required columns: {', '.join(missing)}"
        )
    return resolved


def _parse_row(
    row: Mapping[str, Any],
    columns: Mapping[str, str],
    *,
    symbol: str,
    code: str,
    asset_type: AKShareDailyAssetType,
) -> DailyBar:
    try:
        trade_date = _date_value(row.get(columns["date"]))
        _validate_row_symbol(row, columns, code)
        is_trading = _trading_status(row, columns)
        open_price = _optional_decimal(row.get(columns["open"]), "open")
        close = _optional_decimal(row.get(columns["close"]), "close")
        high = _optional_decimal(row.get(columns["high"]), "high")
        low = _optional_decimal(row.get(columns["low"]), "low")
        volume = _volume(row.get(columns["volume"]), is_trading=is_trading)
        amount = _amount(row.get(columns["amount"]), is_trading=is_trading)
        turnover_percent = _optional_column_decimal(row, columns, "turnover_percent")
        change_amount = _optional_column_decimal(row, columns, "change_amount")
        previous_close = _optional_column_decimal(row, columns, "previous_close")
        amplitude = _optional_column_decimal(row, columns, "amplitude_percent")
        change_percent = _optional_column_decimal(row, columns, "change_percent")
        if is_trading:
            if any(value is None for value in (open_price, close, high, low)):
                raise AKShareDailyPayloadError("trading row has missing OHLC")
            assert open_price is not None
            assert close is not None
            assert high is not None
            assert low is not None
            _validate_ohlc(open_price, high, low, close)
        elif any(value is not None for value in (open_price, close, high, low)):
            raise AKShareDailyPayloadError("suspended row must not contain partial OHLC")
        if turnover_percent is not None and turnover_percent < 0:
            raise AKShareDailyPayloadError("turnover_percent cannot be negative")
        if amplitude is not None and amplitude < 0:
            raise AKShareDailyPayloadError("amplitude_percent cannot be negative")
        if change_percent is not None and not change_percent.is_finite():
            raise AKShareDailyPayloadError("change_percent must be finite")
        if previous_close is None and close is not None and change_amount is not None:
            previous_close = close - change_amount
        if previous_close is not None and previous_close <= 0:
            raise AKShareDailyPayloadError("previous_close must be positive")
        is_st = (
            False
            if asset_type is AKShareDailyAssetType.ETF
            else _stock_st_status(row, columns)
        )
        return DailyBar(
            symbol=symbol,
            trade_date=trade_date,
            open=open_price,
            high=high,
            low=low,
            close=close,
            previous_close=previous_close,
            volume=volume,
            amount=amount,
            turnover_percent=turnover_percent,
            is_trading=is_trading,
            is_st=is_st,
            adjustment=PriceAdjustment.NONE,
        )
    except AKShareDailyPayloadError as exc:
        raise AKShareDailyPayloadError(
            f"invalid AKShare daily row for {symbol}: {exc}"
        ) from exc


def _validate_and_sort_bars(
    bars: tuple[DailyBar, ...],
    *,
    start: date,
    end: date,
    operation: str,
    filter_to_window: bool,
    derive_adjacent_previous_close: bool,
) -> tuple[DailyBar, ...]:
    if not bars:
        raise AKShareDailyNoDataError(f"AKShare {operation} returned no rows")
    if not filter_to_window and any(
        bar.trade_date < start or bar.trade_date > end for bar in bars
    ):
        raise AKShareDailyPayloadError(
            f"AKShare {operation} returned a date outside the requested window"
        )
    ordered = tuple(
        bar
        for bar in sorted(bars, key=lambda item: item.trade_date)
        if not filter_to_window or start <= bar.trade_date <= end
    )
    if not ordered:
        raise AKShareDailyNoDataError(
            f"AKShare {operation} returned no rows inside the requested window"
        )
    dates = [bar.trade_date for bar in ordered]
    if len(dates) != len(set(dates)):
        raise AKShareDailyPayloadError(
            f"AKShare {operation} returned duplicate trading dates"
        )
    if derive_adjacent_previous_close:
    # 只在窗口过滤后进行派生。首条可见记录不得使用早于调用方时点序列的数据
    # 提供者记录。
        derived: list[DailyBar] = []
        previous: DailyBar | None = None
        for bar in ordered:
            if (
                bar.previous_close is None
                and previous is not None
                and previous.close is not None
            ):
                bar = replace(bar, previous_close=previous.close)
            derived.append(bar)
            previous = bar
        return tuple(derived)
    return ordered


def _validate_row_symbol(
    row: Mapping[str, Any], columns: Mapping[str, str], expected_code: str
) -> None:
    symbol_column = columns.get("symbol")
    if symbol_column is None or _is_missing(row.get(symbol_column)):
        return
    raw = str(row.get(symbol_column)).strip().lower()
    normalized = raw.removeprefix("sh.").removeprefix("sz.")
    if normalized != expected_code:
        raise AKShareDailyPayloadError(
            f"row symbol {raw!r} does not match requested code {expected_code}"
        )


def _date_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "to_pydatetime"):
        try:
            converted = value.to_pydatetime()
        except (TypeError, ValueError, AttributeError) as exc:
            raise AKShareDailyPayloadError("date is invalid") from exc
        if isinstance(converted, datetime):
            return converted.date()
    if _is_missing(value):
        raise AKShareDailyPayloadError("date is missing")
    text = str(value).strip()
    try:
        if len(text) == 8:
            return datetime.strptime(text, "%Y%m%d").date()
        return date.fromisoformat(text)
    except ValueError as exc:
        raise AKShareDailyPayloadError(f"invalid date: {text!r}") from exc


def _trading_status(row: Mapping[str, Any], columns: Mapping[str, str]) -> bool:
    status_column = columns.get("trading_status")
    if status_column is None:
    # 东方财富日线历史端点只发出交易行情柱。停牌日期直接缺失，而不是以向前
    # 填充的记录表示。
        return True
    value = row.get(status_column)
    if _is_missing(value):
        raise AKShareDailyPayloadError("trading_status is missing")
    marker = str(value).strip().casefold()
    if marker in _TRADING_MARKERS:
        return True
    if marker in _SUSPENDED_MARKERS:
        return False
    raise AKShareDailyPayloadError(f"unsupported trading_status: {value!r}")


def _stock_st_status(row: Mapping[str, Any], columns: Mapping[str, str]) -> bool:
    st_column = columns.get("is_st")
    if st_column is None or _is_missing(row.get(st_column)):
    # stock_zh_a_hist 没有时点 ST 列。适配器不会从名称推断 ST；需要权威 ST
    # 历史的调用方应继续以 BaoStock 为首选来源。
        return False
    value = row.get(st_column)
    marker = str(value).strip().casefold()
    if marker in _TRUE_MARKERS:
        return True
    if marker in _FALSE_MARKERS:
        return False
    raise AKShareDailyPayloadError(f"unsupported is_st marker: {value!r}")


def _validate_ohlc(
    open_price: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
) -> None:
    if any(value <= 0 for value in (open_price, high, low, close)):
        raise AKShareDailyPayloadError("trading OHLC values must be positive")
    if high < max(open_price, close, low):
        raise AKShareDailyPayloadError("high is below another OHLC value")
    if low > min(open_price, close, high):
        raise AKShareDailyPayloadError("low is above another OHLC value")


def _optional_column_decimal(
    row: Mapping[str, Any], columns: Mapping[str, str], field: str
) -> Decimal | None:
    column = columns.get(field)
    if column is None:
        return None
    return _optional_decimal(row.get(column), field)


def _optional_decimal(value: Any, field: str) -> Decimal | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        raise AKShareDailyPayloadError(f"{field} cannot be boolean")
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise AKShareDailyPayloadError(f"{field} is not numeric") from exc
    if not result.is_finite():
        raise AKShareDailyPayloadError(f"{field} is not finite")
    return result


def _volume(value: Any, *, is_trading: bool) -> int:
    parsed = _optional_decimal(value, "volume")
    if parsed is None:
        if is_trading:
            raise AKShareDailyPayloadError("trading row has missing volume")
        return 0
    if parsed < 0 or parsed != parsed.to_integral_value():
        raise AKShareDailyPayloadError("volume must be a non-negative integer")
    if not is_trading and parsed != 0:
        raise AKShareDailyPayloadError("suspended row volume must be zero")
    return int(parsed)


def _amount(value: Any, *, is_trading: bool) -> Decimal:
    parsed = _optional_decimal(value, "amount")
    if parsed is None:
        if is_trading:
            raise AKShareDailyPayloadError("trading row has missing amount")
        return Decimal(0)
    if parsed < 0:
        raise AKShareDailyPayloadError("amount cannot be negative")
    if not is_trading and parsed != 0:
        raise AKShareDailyPayloadError("suspended row amount must be zero")
    return parsed


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().casefold() in {"", "-", "--", "nan", "none", "null"}
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Decimal):
        return not value.is_finite()
    return False
