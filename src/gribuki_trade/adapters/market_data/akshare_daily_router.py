"""A 股历史日线的来源回退、诊断和尾部拼接路由。

本模块只协调已经规范化的 ``DailyBar`` 来源；具体 AKShare 请求与供应商行解析
留在 ``akshare_daily.py``，尾部拼接算法继续由独立 stitch 模块负责。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol

from gribuki_trade.adapters.market_data.akshare_daily_stitch import (
    HistoricalDailyTailStitchDiagnostics,
    HistoricalDailyTailStitchPolicy,
)
from gribuki_trade.adapters.market_data.akshare_daily_stitch import (
    controlled_tail_stitch as _controlled_tail_stitch_impl,
)
from gribuki_trade.adapters.market_data.akshare_daily_stitch import (
    validate_stitch_input as _validate_stitch_input_impl,
)
from gribuki_trade.domain.market import DailyBar, PriceAdjustment
from gribuki_trade.ports.market_data import (
    AsyncHistoricalDailyData,
    HistoricalDailyData,
    MarketDataUnavailableError,
)


class HistoricalDailyFallbackError(MarketDataUnavailableError):
    """主来源与回退来源均未提供可接受的历史。"""


class HistoricalDailyCoverageError(HistoricalDailyFallbackError):
    """两个来源返回的日线数量都少于配置要求。"""


class HistoricalDailyTailStitchError(HistoricalDailyFallbackError):
    """无法证明所请求的受控尾部拼接是安全的。"""


class HistoricalDailyOverlapMismatchError(HistoricalDailyTailStitchError):
    """候选来源之间近期共有日期或开高低收值不一致。"""


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
class HistoricalDailySourceResult:
    """具有多个独立来源的数据提供者所返回的诊断结果。"""

    bars: tuple[DailyBar, ...]
    selected_source: str
    source_failures: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


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
) -> tuple[tuple[DailyBar, ...], HistoricalDailyTailStitchDiagnostics]:
    """适配器错误层的兼容包装；算法位于纯 stitch 模块。"""

    return _controlled_tail_stitch_impl(
        base_bars=base_bars,
        base_source=base_source,
        tail_bars=tail_bars,
        tail_source=tail_source,
        required_latest_session=required_latest_session,
        minimum_overlap_sessions=minimum_overlap_sessions,
        tail_error=HistoricalDailyTailStitchError,
        overlap_error=HistoricalDailyOverlapMismatchError,
    )


def _validate_stitch_input(
    bars: tuple[DailyBar, ...],
    *,
    source: str,
) -> None:
    """兼容历史私有 helper 名称，实际校验位于纯 stitch 模块。"""

    _validate_stitch_input_impl(
        bars,
        source=source,
        error=HistoricalDailyTailStitchError,
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


