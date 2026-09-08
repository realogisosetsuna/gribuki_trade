"""确定性三层 A 股筛选器的 AKShare 输入。

适配器刻意将两个昂贵边界分开：

* 一份当前交易日的完整市场报价快照，用于低成本过滤；
* 只为数量受限的第一层幸存者获取逐代码不复权日线历史。

公开网页端点不暴露权威载荷时间戳。因此同交易日收盘快照只能在上海时间
15:05 后使用，并将这一保守时刻记录为 ``available_at``。``observed_at`` 是实际
完成时间，绝不替换为调用方的 ``known_at``。实时端点无法重放历史股票池；
历史回测必须改为读取已归档修订版。
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from functools import partial
from typing import Any, TypeVar
from zoneinfo import ZoneInfo

from gribuki_trade.ports.ashare_screening import (
    AShareFactorRecord,
    AShareFactorSnapshot,
    AShareFactorValue,  # noqa: F401 - historical facade export
    AShareUniverseSnapshot,
    ScreeningFactorId,  # noqa: F401 - historical facade export
    ScreeningHistoryPolicy,
    ScreeningSourceQuality,
)

from . import screening_payload as _screening_payload
from .screening_factors import (
    CORPORATE_ACTION_TOLERANCE as _CORPORATE_ACTION_TOLERANCE,  # noqa: F401 - historical facade export
)
from .screening_factors import (
    MIN_CORPORATE_ACTION_COVERAGE as _MIN_CORPORATE_ACTION_COVERAGE,  # noqa: F401 - historical facade export
)
from .screening_factors import (
    average_amount_20 as _average_amount_20,
)
from .screening_factors import (
    calculate_factors as _calculate_factors,
)
from .screening_factors import (
    corporate_action_guard as _corporate_action_guard,
)
from .screening_factors import (
    empty_factor_values as _empty_factor_values,
)
from .screening_factors import (
    factor_values_with_average_amount as _factor_values_with_average_amount,
)
from .screening_factors import (
    max_drawdown_magnitude as _max_drawdown_magnitude,  # noqa: F401 - historical facade export
)
from .screening_factors import (
    mean as _mean,  # noqa: F401 - historical facade export
)
from .screening_factors import (
    returns as _returns,  # noqa: F401 - historical facade export
)
from .screening_payload import (
    _FEATURE_VERSION,
    _UNIVERSE_SOURCES,
    SINA_HISTORY_SOURCE_ID,
    AKShareScreeningDataError,
    AKShareScreeningPayloadError,
    AKShareScreeningPointInTimeError,
    AKShareScreeningSourcesExhaustedError,
    _canonical_symbol,
    _factor_batch_source_id,
    _factor_revision,
    _ListingMetadata,
    _parse_history,
    _parse_listing_metadata,
    _parse_sina_history,
    _parse_universe_rows,
    _provider_records,
    _record_uses_sina_fallback,
    _sina_provider_records,
    _sina_symbol,
    _universe_revision,
)

# 下游盘前/跨市场适配器仍从历史 facade 读取这些协议对象和进程锁。
AKSHARE_HISTORY_SOURCE_ID = _screening_payload.AKSHARE_HISTORY_SOURCE_ID
EASTMONEY_SCREENING_SOURCE_ID = _screening_payload.EASTMONEY_SCREENING_SOURCE_ID
TENCENT_SCREENING_SOURCE_ID = _screening_payload.TENCENT_SCREENING_SOURCE_ID
AKShareScreeningCoverageError = _screening_payload.AKShareScreeningCoverageError
_SINA_HISTORY_LOCK = _screening_payload._SINA_HISTORY_LOCK

SHANGHAI = ZoneInfo("Asia/Shanghai")


_CLOSE_READY_TIME = time(15, 5)
_DEFAULT_MINIMUM_UNIVERSE = 4500
_DEFAULT_HISTORY_SESSIONS = 201
_DEFAULT_MAX_FACTOR_SYMBOLS = 300
_DEFAULT_HISTORY_CONCURRENCY = 4
_DEFAULT_HISTORY_CALENDAR_DAYS = 430

# AKShare 的新浪日线解码器通过 py_mini_racer 内嵌 V8。在 Windows 上并发首次
# 初始化可能终止整个 Python 进程，因此回退调用必须跨适配器实例串行执行。
# 东方财富主调用仍保留配置的并发度。














_T = TypeVar("_T")


class AKShareAShareScreeningAdapter:
    """当前收盘股票池及有限不复权历史因子适配器。"""

    def __init__(
        self,
        client: Any | None = None,
        *,
        timeout_seconds: float = 35.0,
        history_timeout_seconds: float = 18.0,
        minimum_universe_count: int = _DEFAULT_MINIMUM_UNIVERSE,
        minimum_history_sessions: int = _DEFAULT_HISTORY_SESSIONS,
        max_factor_symbols: int = _DEFAULT_MAX_FACTOR_SYMBOLS,
        history_concurrency: int = _DEFAULT_HISTORY_CONCURRENCY,
        history_calendar_days: int = _DEFAULT_HISTORY_CALENDAR_DAYS,
        stale_after: timedelta = timedelta(hours=6),
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive and finite")
        if not math.isfinite(history_timeout_seconds) or history_timeout_seconds <= 0:
            raise ValueError("history_timeout_seconds must be positive and finite")
        if minimum_universe_count < 1:
            raise ValueError("minimum_universe_count must be positive")
        if minimum_history_sessions < 126:
            raise ValueError("minimum_history_sessions must be at least 126")
        if max_factor_symbols < 1:
            raise ValueError("max_factor_symbols must be positive")
        if history_concurrency < 1 or history_concurrency > 16:
            raise ValueError("history_concurrency must be between 1 and 16")
        if history_calendar_days < 200:
            raise ValueError("history_calendar_days must be at least 200")
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._history_timeout_seconds = history_timeout_seconds
        self._minimum_universe_count = minimum_universe_count
        self._minimum_history_sessions = minimum_history_sessions
        self._max_factor_symbols = max_factor_symbols
        self._history_concurrency = history_concurrency
        self._history_calendar_days = history_calendar_days
        self._stale_after = stale_after
        self._now = now or (lambda: datetime.now(tz=UTC))

    async def fetch_universe_snapshot(
        self,
        *,
        as_of: date,
        known_at: datetime,
    ) -> AShareUniverseSnapshot:
        """获取盘后全市场修订版，并提供独立回退。"""

        started_at, available_at = self._validate_live_request(as_of, known_at)
        client = self._client or _import_akshare()
        metadata, metadata_warnings = await self._fetch_listing_metadata(client, as_of)
        failures: list[str] = []
        for source_index, spec in enumerate(_UNIVERSE_SOURCES):
            try:
                rows = await _call_async(
                    partial(_provider_records, client, spec.operation),
                    timeout_seconds=self._timeout_seconds,
                    operation=spec.operation,
                )
                records = _parse_universe_rows(
                    rows,
                    spec=spec,
                    as_of=as_of,
                    metadata=metadata,
                    minimum_count=self._minimum_universe_count,
                )
                observed_at = _aware_now(self._now)
                if observed_at < started_at:
                    raise ValueError("now() moved backwards during universe fetch")
                stale = observed_at.astimezone(SHANGHAI) - available_at > self._stale_after
                warnings = [
                    "secondary public-web close snapshot; not an exchange feed",
                    "provider payload has no authoritative quote timestamp; "
                    "15:05 Asia/Shanghai is a conservative availability boundary",
                    "listing_days is measured in natural calendar days from the "
                    "official exchange listing date",
                    "current ST status is inferred from the provider security name; "
                    "it is not a historical ST-status source",
                    "live universe endpoint cannot be used for historical replay; "
                    "persist source_revision for point-in-time backtests",
                    *metadata_warnings,
                ]
                missing_listing_age = sum(
                    item.listing_days is None for item in records
                )
                if missing_listing_age:
                    warnings.append(
                        f"LISTING_METADATA_MISSING_RECORDS:{missing_listing_age}"
                    )
                if spec is _UNIVERSE_SOURCES[0]:
                    warnings.append(
                        "Eastmoney amount and total market capitalization normalized as CNY"
                    )
                else:
                    warnings.append(
                        "Tencent turnover converted from CNY 10,000 and zsz total market "
                        "capitalization converted from CNY 100 million"
                    )
                warnings.extend(f"prior source failed: {item}" for item in failures)
                if stale:
                    warnings.append("universe observation exceeded configured close-age limit")
                quality = (
                    ScreeningSourceQuality.DEGRADED
                    if (
                        source_index > 0
                        or stale
                        or metadata_warnings
                        or missing_listing_age
                    )
                    else ScreeningSourceQuality.COMPLETE
                )
                return AShareUniverseSnapshot(
                    as_of=as_of,
                    available_at=available_at,
                    observed_at=observed_at,
                    source_id=spec.source_id,
                    source_revision=_universe_revision(spec.source_id, as_of, records),
                    records=records,
                    quality=quality,
                    warnings=tuple(dict.fromkeys(warnings)),
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                failures.append(f"{spec.operation}:{type(exc).__name__}")
        raise AKShareScreeningSourcesExhaustedError(tuple(failures))

    async def fetch_factor_snapshot(
        self,
        symbols: Sequence[str],
        *,
        as_of: date,
        known_at: datetime,
    ) -> AShareFactorSnapshot:
        """在服务明确预算范围内获取不复权历史。"""

        started_at, available_at = self._validate_live_request(as_of, known_at)
        canonical = tuple(_canonical_symbol(item) for item in symbols)
        if len(canonical) != len(set(canonical)):
            raise ValueError("factor symbols must be unique")
        if len(canonical) > self._max_factor_symbols:
            raise ValueError(
                f"factor request exceeds adapter budget {self._max_factor_symbols}"
            )
        client = self._client or _import_akshare()
        semaphore = asyncio.Semaphore(self._history_concurrency)

        async def one(symbol: str) -> AShareFactorRecord:
            async with semaphore:
                return await self._factor_record(client, symbol, as_of)

        records = tuple(await asyncio.gather(*(one(symbol) for symbol in canonical)))
        observed_at = _aware_now(self._now)
        if observed_at < started_at:
            raise ValueError("now() moved backwards during factor fetch")
        failed = sum(bool(item.warnings) for item in records)
        sina_fallbacks = sum(_record_uses_sina_fallback(item) for item in records)
        factor_source_id = _factor_batch_source_id(records)
        warnings = [
            "unadjusted stock_zh_a_hist only; adjust='' was sent explicitly",
            "price factors are withheld when the raw previous-close corporate-action "
            "guard is incomplete or detects a discontinuity",
            f"bounded per-symbol history enrichment: requested={len(canonical)}, "
            f"concurrency={self._history_concurrency}, minimum_sessions="
            f"{self._minimum_history_sessions}",
        ]
        if failed:
            warnings.append(f"SYMBOL_FACTOR_DEGRADATIONS:{failed}")
        if sina_fallbacks:
            warnings.extend(
                (
                    f"SINA_HISTORY_FALLBACK_SYMBOLS:{sina_fallbacks}/{len(records)}",
                    "SINA_FALLBACK_IS_UNADJUSTED_DAILY_HISTORY",
                    "SINA_PREVIOUS_CLOSE_DERIVED_FROM_ADJACENT_RAW_CLOSES",
                    "SINA_CORPORATE_ACTION_GUARD_HAS_NO_INDEPENDENT_REFERENCE_CLOSE",
                )
            )
        quality = (
            ScreeningSourceQuality.DEGRADED
            if failed
            else ScreeningSourceQuality.COMPLETE
        )
        return AShareFactorSnapshot(
            as_of=as_of,
            available_at=available_at,
            observed_at=observed_at,
            source_id=factor_source_id,
            source_revision=_factor_revision(
                as_of,
                records,
                source_id=factor_source_id,
            ),
            feature_version=_FEATURE_VERSION,
            history_policy=(
                ScreeningHistoryPolicy.UNADJUSTED_WITH_CORPORATE_ACTION_GUARD
            ),
            records=records,
            quality=quality,
            warnings=tuple(warnings),
        )

    def _validate_live_request(
        self,
        as_of: date,
        known_at: datetime,
    ) -> tuple[datetime, datetime]:
        if known_at.tzinfo is None or known_at.utcoffset() is None:
            raise ValueError("known_at must be timezone-aware")
        started_at = _aware_now(self._now)
        local_observed = started_at.astimezone(SHANGHAI)
        local_known = known_at.astimezone(SHANGHAI)
        available_at = datetime.combine(as_of, _CLOSE_READY_TIME, tzinfo=SHANGHAI)
        if as_of != local_observed.date() or as_of != local_known.date():
            raise AKShareScreeningPointInTimeError(
                "live AKShare screening supports only the current Shanghai session; "
                "use an archived revision for historical replay"
            )
        if local_known < available_at:
            raise AKShareScreeningPointInTimeError(
                f"close screening data is unavailable before {available_at.isoformat()}"
            )
        return started_at, available_at

    async def _fetch_listing_metadata(
        self,
        client: Any,
        as_of: date,
    ) -> tuple[dict[str, _ListingMetadata], tuple[str, ...]]:
        calls: tuple[tuple[str, str, Mapping[str, object]], ...] = (
            ("SSE_MAIN", "stock_info_sh_name_code", {"symbol": "主板A股"}),
            ("SSE_STAR", "stock_info_sh_name_code", {"symbol": "科创板"}),
            ("SZSE", "stock_info_sz_name_code", {"symbol": "A股列表"}),
            ("BSE", "stock_info_bj_name_code", {}),
        )

        async def one(
            label: str,
            operation: str,
            kwargs: Mapping[str, object],
        ) -> tuple[str, tuple[Mapping[str, Any], ...] | None, str | None]:
            try:
                rows = await _call_async(
                    partial(_provider_records, client, operation, **kwargs),
                    timeout_seconds=self._timeout_seconds,
                    operation=operation,
                )
                return label, rows, None
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                return label, None, type(exc).__name__

        outputs = await asyncio.gather(*(one(*call) for call in calls))
        metadata: dict[str, _ListingMetadata] = {}
        warnings: list[str] = []
        for label, rows, failure in outputs:
            if rows is None:
                warnings.append(f"LISTING_METADATA_UNAVAILABLE:{label}:{failure}")
                continue
            try:
                parsed = _parse_listing_metadata(label, rows, as_of)
            except AKShareScreeningPayloadError:
                warnings.append(f"LISTING_METADATA_INVALID:{label}")
                continue
            for symbol, item in parsed.items():
                if symbol in metadata and metadata[symbol] != item:
                    warnings.append(f"LISTING_METADATA_CONFLICT:{symbol}")
                    metadata.pop(symbol, None)
                else:
                    metadata[symbol] = item
        return metadata, tuple(dict.fromkeys(warnings))

    async def _factor_record(
        self,
        client: Any,
        symbol: str,
        as_of: date,
    ) -> AShareFactorRecord:
        code = symbol.split(".", maxsplit=1)[0]
        start = as_of - timedelta(days=self._history_calendar_days)
        empty = _empty_factor_values()
        primary_failure: Exception | None = None
        fallback_warnings: tuple[str, ...]
        try:
            rows = await _call_async(
                partial(
                    _provider_records,
                    client,
                    "stock_zh_a_hist",
                    symbol=code,
                    period="daily",
                    start_date=start.strftime("%Y%m%d"),
                    end_date=as_of.strftime("%Y%m%d"),
                    adjust="",
                    timeout=self._history_timeout_seconds,
                ),
                timeout_seconds=self._history_timeout_seconds,
                operation=f"stock_zh_a_hist:{symbol}",
            )
            bars = _parse_history(rows, as_of=as_of)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            primary_failure = exc
            try:
                rows = await _call_async(
                    partial(
                        _sina_provider_records,
                        client,
                        symbol=_sina_symbol(symbol),
                        start_date=start.strftime("%Y%m%d"),
                        end_date=as_of.strftime("%Y%m%d"),
                        adjust="",
                    ),
                    timeout_seconds=self._history_timeout_seconds,
                    operation=f"stock_zh_a_daily:{symbol}",
                )
                bars = _parse_sina_history(rows, as_of=as_of)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as fallback_exc:
                return AShareFactorRecord(
                    symbol=symbol,
                    values=empty,
                    warnings=(
                        f"HISTORY_FETCH_FAILED:{type(primary_failure).__name__}",
                        (
                            "HISTORY_FALLBACK_FAILED:"
                            f"{type(fallback_exc).__name__}"
                        ),
                    ),
                )
            fallback_warnings = (
                f"HISTORY_PRIMARY_FAILED:{type(primary_failure).__name__}",
                f"HISTORY_SOURCE_FALLBACK:{SINA_HISTORY_SOURCE_ID}",
                "SINA_PREVIOUS_CLOSE_DERIVED_FROM_ADJACENT_RAW_CLOSES",
                "SINA_CORPORATE_ACTION_GUARD_HAS_NO_INDEPENDENT_REFERENCE_CLOSE",
            )
        else:
            fallback_warnings = ()
        if not bars or bars[-1].trade_date != as_of:
            return AShareFactorRecord(
                symbol=symbol,
                values=empty,
                warnings=(*fallback_warnings, "HISTORY_LATEST_SESSION_MISMATCH"),
            )
        average_amount = _average_amount_20(bars)
        if len(bars) < self._minimum_history_sessions:
            return AShareFactorRecord(
                symbol=symbol,
                values=_factor_values_with_average_amount(average_amount),
                warnings=(
                    *fallback_warnings,
                    f"INSUFFICIENT_HISTORY:{len(bars)}/{self._minimum_history_sessions}",
                ),
            )
        guard_warning = _corporate_action_guard(bars[-self._minimum_history_sessions :])
        if guard_warning is not None:
            return AShareFactorRecord(
                symbol=symbol,
                values=_factor_values_with_average_amount(average_amount),
                warnings=(*fallback_warnings, guard_warning),
            )
        return AShareFactorRecord(
            symbol=symbol,
            values=_calculate_factors(bars),
            warnings=fallback_warnings,
        )


async def _call_async(
    call: Callable[[], _T],
    *,
    timeout_seconds: float,
    operation: str,
) -> _T:
    try:
        return await asyncio.wait_for(asyncio.to_thread(call), timeout=timeout_seconds)
    except TimeoutError as exc:
        raise AKShareScreeningDataError(
            f"{operation} exceeded {timeout_seconds:g}s"
        ) from exc






















































def _import_akshare() -> Any:
    try:
        import akshare  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - packaging concern
        raise AKShareScreeningDataError("AKShare is not installed") from exc
    return akshare


def _aware_now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now() must return a timezone-aware datetime")
    return value
