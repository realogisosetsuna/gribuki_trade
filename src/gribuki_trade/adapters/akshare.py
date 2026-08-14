"""面向 A 股公开网页行情数据的韧性 AKShare 适配器。

AKShare 聚合公开网页端点。特别是 ``stock_intraday_em`` 只提供类似逐笔成交的
数据，没有交易所序列号、数据包恢复或完整性保证。因此本模块绝不会把它标记
为交易所逐笔数据源。
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from datetime import time as wall_time
from decimal import Decimal, InvalidOperation
from functools import partial
from typing import Any, TypeVar
from zoneinfo import ZoneInfo

import httpx

from gribuki_trade.ports.market_data import (
    FreshnessStatus,
    IntradayBar,
    MarketDataMeta,
    MarketDataTimeoutError,
    MarketDataUnavailableError,
    MarketSnapshot,
    MinuteInterval,
    SourceSemantics,
    TradeDirection,
    TradePrint,
)

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_T = TypeVar("_T")
_SpotCacheEntry = tuple[
    datetime, Mapping[str, Any], str, bool, tuple[str, ...]
]


class AKShareError(MarketDataUnavailableError):
    """数据提供者、传输和载荷失败的基类。"""


class AKShareNoDataError(AKShareError):
    """数据提供者没有为有效请求返回记录。"""


class AKSharePayloadError(AKShareError):
    """数据提供者返回的字段缺失或无效。"""


class AKShareTimeoutError(MarketDataTimeoutError, AKShareError):
    """异步适配器调用超过调用方可见的超时。"""


class AKShareMarketDataAdapter:
    """A 股现货、公开逐笔成交及分钟线适配器。

    同步方法便于批处理任务使用。图形界面/事件循环调用方应使用 ``*_async``
    变体；数据提供者调用会移出事件循环，并受 ``timeout_seconds`` 限制。Python
    无法安全终止阻塞的第三方线程，因此异步超时只限制调用方等待时间，被放弃
    的提供者调用仍可能在后台完成。
    """

    _SINGLE_SPOT_COLUMNS = frozenset({"item", "value"})
    _SINGLE_SPOT_ITEMS = frozenset(
        {"最新", "今开", "最高", "最低", "昨收", "总手", "金额", "换手"}
    )
    _TENCENT_SPOT_COLUMNS = frozenset(
        {"code", "name", "zxj", "zd", "volume", "turnover", "hsl"}
    )
    _TRADE_COLUMNS = frozenset({"时间", "成交价", "手数", "买卖盘性质"})
    _BAR_COLUMNS = frozenset(
        {"时间", "开盘", "收盘", "最高", "最低", "成交量", "成交额"}
    )
    _SINA_BAR_COLUMNS = frozenset(
        {"day", "open", "high", "low", "close", "volume", "amount"}
    )

    def __init__(
        self,
        client: Any | None = None,
        *,
        http_transport: httpx.AsyncBaseTransport | None = None,
        timeout_seconds: float = 10.0,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 0.25,
        snapshot_cache_ttl_seconds: float = 2.0,
        snapshot_stale_fallback_seconds: float = 300.0,
        single_snapshot_failure_cooldown_seconds: float = 60.0,
        intraday_stale_after_seconds: float = 180.0,
        now: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds cannot be negative")
        if snapshot_cache_ttl_seconds < 0:
            raise ValueError("snapshot_cache_ttl_seconds cannot be negative")
        if snapshot_stale_fallback_seconds < snapshot_cache_ttl_seconds:
            raise ValueError(
                "snapshot_stale_fallback_seconds must be >= snapshot_cache_ttl_seconds"
            )
        if single_snapshot_failure_cooldown_seconds < 0:
            raise ValueError("single_snapshot_failure_cooldown_seconds cannot be negative")
        if intraday_stale_after_seconds < 0:
            raise ValueError("intraday_stale_after_seconds cannot be negative")

        self._client = client
        self._http_transport = http_transport
        self._timeout_seconds = timeout_seconds
        self._max_attempts = max_attempts
        self._retry_backoff_seconds = retry_backoff_seconds
        self._snapshot_cache_ttl = timedelta(seconds=snapshot_cache_ttl_seconds)
        self._snapshot_stale_fallback = timedelta(
            seconds=snapshot_stale_fallback_seconds
        )
        self._single_snapshot_failure_cooldown = timedelta(
            seconds=single_snapshot_failure_cooldown_seconds
        )
        self._intraday_stale_after = timedelta(seconds=intraday_stale_after_seconds)
        self._now = now or (lambda: datetime.now(tz=SHANGHAI_TZ))
        self._sleep = sleep
        self._spot_cache: dict[str, _SpotCacheEntry] = {}
        self._tencent_spot_cache: (
            tuple[datetime, dict[str, Mapping[str, Any]]] | None
        ) = None
        self._single_spot_unavailable_until: datetime | None = None

    def fetch_spot_snapshot(self, symbol: str) -> MarketSnapshot:
        """优先获取单个代码，并以腾讯表格和过期缓存作为回退。

        首选 ``stock_bid_ask_em``，因为它只获取一个代码而非数千个。若该端点
        不可用，则使用独立托管的腾讯行情表作为已标记降级回退，并缓存以供多个
        代码复用。两个来源都不暴露权威报价时间戳。
        """

        canonical_symbol, code = _normalize_symbol(symbol)
        fetched_at = _aware_now(self._now)
        row, source_fetch_time, provider, degraded, warnings = self._load_spot_row(
            code, fetched_at
        )

        try:
            volume_lots = _non_negative_integer(row["成交量"], "成交量", missing_zero=True)
            amount = _non_negative_decimal(row["成交额"], "成交额", missing_zero=True)
            high = _optional_decimal(row["最高"], "最高")
            low = _optional_decimal(row["最低"], "最低")
            if high is not None and low is not None and high < low:
                raise AKSharePayloadError("spot high is below low")
            meta = MarketDataMeta(
                provider=provider,
                semantics=SourceSemantics.PUBLIC_WEB_QUOTE_SNAPSHOT,
                fetched_at=source_fetch_time,
                provider_timestamp=None,
                freshness=(
                    FreshnessStatus.STALE if degraded else FreshnessStatus.UNKNOWN
                ),
                degraded=degraded,
                warnings=(
                    "upstream snapshot has no authoritative quote timestamp",
                    "public web snapshot is not an exchange tick feed",
                    *warnings,
                ),
            )
            return MarketSnapshot(
                symbol=canonical_symbol,
                name=_optional_text(row["名称"]),
                last=_optional_decimal(row["最新价"], "最新价"),
                open=_optional_decimal(row["今开"], "今开"),
                high=high,
                low=low,
                previous_close=_optional_decimal(row["昨收"], "昨收"),
                volume_lots=volume_lots,
                amount=amount,
                turnover_percent=_optional_decimal(row["换手率"], "换手率"),
                meta=meta,
            )
        except KeyError as exc:
            raise AKSharePayloadError(
                f"AKShare spot row is missing {exc.args[0]!r} for {canonical_symbol}"
            ) from exc

    async def fetch_spot_snapshot_async(self, symbol: str) -> MarketSnapshot:
        return await self._run_async("spot snapshot", self.fetch_spot_snapshot, symbol)

    def fetch_trade_prints(self, symbol: str) -> tuple[TradePrint, ...]:
        """获取数据提供者的当前交易日公开逐笔成交表。"""

        canonical_symbol, code = _normalize_symbol(symbol)
        fetched_at = _aware_now(self._now)
        rows = self._provider_records("stock_intraday_em", symbol=code)
        _require_columns(rows, self._TRADE_COLUMNS, "stock_intraday_em")

        prints: list[TradePrint] = []
        for row in rows:
            try:
                occurred_at, inferred_date = _parse_trade_time(row["时间"], fetched_at)
                freshness, freshness_warnings = self._freshness(occurred_at, fetched_at)
                warnings = [
                    "public web time-and-sales has no exchange sequence or completeness guarantee"
                ]
                if inferred_date:
                    warnings.append("trade date inferred from collection date")
                warnings.extend(freshness_warnings)
                meta = MarketDataMeta(
                    provider="AKShare/Eastmoney",
                    semantics=SourceSemantics.PUBLIC_WEB_TIME_AND_SALES,
                    fetched_at=fetched_at,
                    provider_timestamp=occurred_at,
                    freshness=freshness,
                    warnings=tuple(warnings),
                )
                prints.append(
                    TradePrint(
                        symbol=canonical_symbol,
                        occurred_at=occurred_at,
                        price=_positive_decimal(row["成交价"], "成交价"),
                        volume_lots=_non_negative_integer(row["手数"], "手数"),
                        direction=_parse_direction(row["买卖盘性质"]),
                        exchange_sequence=None,
                        meta=meta,
                    )
                )
            except KeyError as exc:
                raise AKSharePayloadError(
                    f"AKShare trade row is missing {exc.args[0]!r}: {row!r}"
                ) from exc
            except AKSharePayloadError as exc:
                raise AKSharePayloadError(f"invalid AKShare trade row: {row!r}") from exc
        return tuple(sorted(prints, key=lambda item: item.occurred_at))

    async def fetch_trade_prints_async(self, symbol: str) -> tuple[TradePrint, ...]:
        return await self._run_async("time-and-sales", self.fetch_trade_prints, symbol)

    def fetch_intraday_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        interval: MinuteInterval = MinuteInterval.ONE_MINUTE,
        completed_only: bool = True,
    ) -> tuple[IntradayBar, ...]:
        """获取数据提供者聚合的 1 分钟或 5 分钟行情柱。

        默认移除当前可能仍在变化的行情柱。AKShare 的 1 分钟端点可能只暴露
        较短的近期历史；调用方如需持久盘中归档，必须自行保存行情柱。
        """

        canonical_symbol, code = _normalize_symbol(symbol)
        start_local = _to_shanghai(start, "start")
        end_local = _to_shanghai(end, "end")
        if start_local > end_local:
            raise ValueError("start must be on or before end")
        if interval not in {MinuteInterval.ONE_MINUTE, MinuteInterval.FIVE_MINUTES}:
            raise ValueError("only 1m and 5m bars are supported")

        primary_operation = _minute_primary_operation(code)
        primary_error: AKShareError | None = None
        try:
            rows = self._provider_records(
                primary_operation,
                symbol=code,
                start_date=start_local.strftime("%Y-%m-%d %H:%M:%S"),
                end_date=end_local.strftime("%Y-%m-%d %H:%M:%S"),
                period=str(interval.minutes),
                adjust="",
            )
            _require_columns(rows, self._BAR_COLUMNS, primary_operation)
            fetched_at = _aware_now(self._now)
            return self._parse_intraday_rows(
                rows,
                canonical_symbol=canonical_symbol,
                start=start_local,
                end=end_local,
                interval=interval,
                fetched_at=fetched_at,
                completed_only=completed_only,
                fallback=False,
                primary_provider=_minute_primary_provider(primary_operation),
            )
        except AKShareError as exc:
            primary_error = exc

        try:
            rows = self._provider_records(
                "stock_zh_a_minute",
                symbol=_sina_symbol(canonical_symbol),
                period=str(interval.minutes),
                adjust="",
            )
            _require_columns(rows, self._SINA_BAR_COLUMNS, "stock_zh_a_minute")
            fetched_at = _aware_now(self._now)
            return self._parse_intraday_rows(
                rows,
                canonical_symbol=canonical_symbol,
                start=start_local,
                end=end_local,
                interval=interval,
                fetched_at=fetched_at,
                completed_only=completed_only,
                fallback=True,
                primary_error=primary_error,
            )
        except AKShareError as fallback_error:
            raise AKShareError(
                "AKShare minute primary and Sina fallback both failed: "
                f"primary={primary_error}; fallback={fallback_error}"
            ) from fallback_error

    def _parse_intraday_rows(
        self,
        rows: list[Mapping[str, Any]],
        *,
        canonical_symbol: str,
        start: datetime,
        end: datetime,
        interval: MinuteInterval,
        fetched_at: datetime,
        completed_only: bool,
        fallback: bool,
        primary_error: AKShareError | None = None,
        primary_provider: str = "AKShare/Eastmoney stock_zh_a_hist_min_em",
    ) -> tuple[IntradayBar, ...]:
        bars: list[IntradayBar] = []
        interval_delta = timedelta(minutes=interval.minutes)
        for row in rows:
            try:
                if fallback:
                    bar_end = _parse_provider_datetime(row["day"])
                    bar_start = bar_end - interval_delta
                    open_value = row["open"]
                    high_value = row["high"]
                    low_value = row["low"]
                    close_value = row["close"]
                    amount_value = row["amount"]
                    volume_lots, volume_warnings = _sina_volume_lots(row["volume"])
                    vwap = None
                    provider = "AKShare/Sina stock_zh_a_minute fallback"
                    source_warnings = (
                        f"Eastmoney minute endpoint failed: {primary_error}",
                        "fallback used Sina minute history (maximum provider window: 1970 bars)",
                        "Sina timestamp is treated as bar end",
                        *volume_warnings,
                    )
                else:
                    bar_start = _parse_provider_datetime(row["时间"])
                    bar_end = bar_start + interval_delta
                    open_value = row["开盘"]
                    high_value = row["最高"]
                    low_value = row["最低"]
                    close_value = row["收盘"]
                    amount_value = row["成交额"]
                    volume_lots = _non_negative_integer(row["成交量"], "成交量")
                    vwap = _optional_decimal(row.get("均价"), "均价")
                    provider = primary_provider
                    source_warnings = (
                        "Eastmoney timestamp is treated as bar start",
                    )

            # 不同数据提供者实现的切片行为并不完全相同。只返回完整位于调用方
            # 所请求时点窗口内的行情柱。
                if bar_start < start or bar_end > end:
                    continue
                is_closed = bar_end <= fetched_at
                if completed_only and not is_closed:
                    continue

                open_price = _positive_decimal(open_value, "open")
                high = _positive_decimal(high_value, "high")
                low = _positive_decimal(low_value, "low")
                close = _positive_decimal(close_value, "close")
                if high < max(open_price, low, close) or low > min(
                    open_price, high, close
                ):
                    raise AKSharePayloadError("minute bar OHLC is inconsistent")
                freshness, freshness_warnings = self._freshness(bar_end, fetched_at)
                meta = MarketDataMeta(
                    provider=provider,
                    semantics=SourceSemantics.AGGREGATED_MINUTE_BAR,
                    fetched_at=fetched_at,
                    provider_timestamp=bar_end,
                    freshness=freshness,
                    degraded=fallback,
                    warnings=(
                        "provider-aggregated public-web bar; not exchange tick data",
                        *source_warnings,
                        *freshness_warnings,
                    ),
                )
                bars.append(
                    IntradayBar(
                        symbol=canonical_symbol,
                        start_at=bar_start,
                        end_at=bar_end,
                        interval=interval,
                        open=open_price,
                        high=high,
                        low=low,
                        close=close,
                        volume_lots=volume_lots,
                        amount=_non_negative_decimal(amount_value, "amount"),
                        vwap=vwap,
                        is_closed=is_closed,
                        meta=meta,
                    )
                )
            except KeyError as exc:
                raise AKSharePayloadError(
                    f"AKShare minute row is missing {exc.args[0]!r}: {row!r}"
                ) from exc
            except AKSharePayloadError as exc:
                raise AKSharePayloadError(f"invalid AKShare minute row: {row!r}") from exc
        return tuple(sorted(bars, key=lambda item: item.start_at))

    async def fetch_intraday_bars_async(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        interval: MinuteInterval = MinuteInterval.ONE_MINUTE,
        completed_only: bool = True,
    ) -> tuple[IntradayBar, ...]:
        canonical_symbol, code = _normalize_symbol(symbol)
        start_local = _to_shanghai(start, "start")
        end_local = _to_shanghai(end, "end")
        if start_local > end_local:
            raise ValueError("start must be on or before end")
        if interval not in {MinuteInterval.ONE_MINUTE, MinuteInterval.FIVE_MINUTES}:
            raise ValueError("only 1m and 5m bars are supported")

        primary_operation = _minute_primary_operation(code)
        primary_error: AKShareError | None = None
        try:
            rows = await self._eastmoney_records_async(
                primary_operation=primary_operation,
                canonical_symbol=canonical_symbol,
                code=code,
                interval=interval,
            )
            _require_columns(rows, self._BAR_COLUMNS, primary_operation)
            fetched_at = _aware_now(self._now)
            return self._parse_intraday_rows(
                rows,
                canonical_symbol=canonical_symbol,
                start=start_local,
                end=end_local,
                interval=interval,
                fetched_at=fetched_at,
                completed_only=completed_only,
                fallback=False,
                primary_provider=_minute_primary_provider(primary_operation),
            )
        except AKShareError as exc:
            primary_error = exc

        try:
            rows = await self._sina_records_async(
                _sina_symbol(canonical_symbol), str(interval.minutes)
            )
            _require_columns(rows, self._SINA_BAR_COLUMNS, "stock_zh_a_minute")
            fetched_at = _aware_now(self._now)
            return self._parse_intraday_rows(
                rows,
                canonical_symbol=canonical_symbol,
                start=start_local,
                end=end_local,
                interval=interval,
                fetched_at=fetched_at,
                completed_only=completed_only,
                fallback=True,
                primary_error=primary_error,
            )
        except AKShareError as fallback_error:
            raise AKShareError(
                "AKShare minute primary and Sina fallback both failed"
            ) from fallback_error

    def _load_spot_row(
        self, code: str, fetched_at: datetime
    ) -> tuple[Mapping[str, Any], datetime, str, bool, tuple[str, ...]]:
        cached = self._spot_cache.get(code)
        if (
            cached is not None
            and _non_negative_age(fetched_at, cached[0]) <= self._snapshot_cache_ttl
        ):
            return (
                cached[1],
                cached[0],
                cached[2],
                cached[3],
                (*cached[4], "served from short-lived per-symbol cache"),
            )

        primary_error: AKShareError | None = None
        cooldown_until = self._single_spot_unavailable_until
        if cooldown_until is not None and fetched_at < cooldown_until:
            primary_error = AKShareError(
                "stock_bid_ask_em temporarily skipped after recent provider failures"
            )
        else:
            try:
                row = self._load_single_spot_row(code)
                provider = "AKShare/Eastmoney stock_bid_ask_em"
                entry: _SpotCacheEntry = (fetched_at, row, provider, False, ())
                self._spot_cache[code] = entry
                self._single_spot_unavailable_until = None
                return row, fetched_at, provider, False, ()
            except AKShareError as exc:
                primary_error = exc
                self._single_spot_unavailable_until = (
                    fetched_at + self._single_snapshot_failure_cooldown
                )

        assert primary_error is not None
        try:
            rows, source_fetch_time = self._load_tencent_spot_rows(fetched_at)
            row, row_warnings = _normalize_tencent_spot_row(code, rows[code])
            warnings = (
                f"single-symbol Eastmoney endpoint failed: {primary_error}",
                "fallback used Tencent full-market board table",
                *row_warnings,
            )
            provider = "AKShare/Tencent stock_zh_a_spot_tx fallback"
            fallback_entry: _SpotCacheEntry = (
                source_fetch_time,
                row,
                provider,
                True,
                warnings,
            )
            self._spot_cache[code] = fallback_entry
            return row, source_fetch_time, provider, True, warnings
        except KeyError as exc:
            fallback_error: AKShareError = AKShareNoDataError(
                f"Tencent spot fallback returned no row for {code}"
            )
            fallback_error.__cause__ = exc
        except AKShareError as exc:
            fallback_error = exc

        if cached is not None:
            age = _non_negative_age(fetched_at, cached[0])
            if age <= self._snapshot_stale_fallback:
                return (
                    cached[1],
                    cached[0],
                    cached[2],
                    True,
                    (
                        *cached[4],
                        "all refresh paths failed; served cached snapshot "
                        f"age={age.total_seconds():.1f}s",
                    ),
                )
        raise AKShareError(
            "AKShare spot primary and Tencent fallback both failed: "
            f"primary={primary_error}; fallback={fallback_error}"
        ) from fallback_error

    def _load_single_spot_row(self, code: str) -> Mapping[str, Any]:
        records = self._provider_records("stock_bid_ask_em", symbol=code)
        _require_columns(records, self._SINGLE_SPOT_COLUMNS, "stock_bid_ask_em")
        values: dict[str, Any] = {}
        for record in records:
            item = _optional_text(record.get("item"))
            if item is not None:
                values[item] = record.get("value")
        missing = self._SINGLE_SPOT_ITEMS.difference(values)
        if missing:
            raise AKSharePayloadError(
                "AKShare stock_bid_ask_em missing items: "
                + ", ".join(sorted(missing))
            )
        return {
            "代码": code,
            "名称": None,
            "最新价": values["最新"],
            "今开": values["今开"],
            "最高": values["最高"],
            "最低": values["最低"],
            "昨收": values["昨收"],
            "成交量": values["总手"],
            "成交额": values["金额"],
            "换手率": values["换手"],
        }

    def _load_tencent_spot_rows(
        self, fetched_at: datetime
    ) -> tuple[dict[str, Mapping[str, Any]], datetime]:
        cache = self._tencent_spot_cache
        if (
            cache is not None
            and _non_negative_age(fetched_at, cache[0]) <= self._snapshot_cache_ttl
        ):
            return cache[1], cache[0]

        records = self._provider_records("stock_zh_a_spot_tx")
        _require_columns(records, self._TENCENT_SPOT_COLUMNS, "stock_zh_a_spot_tx")
        indexed: dict[str, Mapping[str, Any]] = {}
        for record in records:
            raw_code = str(record.get("code", "")).strip().lower()
            if not raw_code.startswith(("sh", "sz")):
        # 本适配器的规范代码契约当前只覆盖沪深市场；腾讯 A 股行情表还包含北交所
        # 记录。
                continue
            code = _normalize_tencent_code(record.get("code"))
            if code in indexed:
                raise AKSharePayloadError(f"duplicate Tencent spot code: {code}")
            indexed[code] = record
        self._tencent_spot_cache = (fetched_at, indexed)
        return indexed, fetched_at

    def _provider_records(self, operation: str, **kwargs: Any) -> list[Mapping[str, Any]]:
        client = self._client or _import_akshare()
        method = getattr(client, operation, None)
        if method is None:
            raise AKShareError(f"installed AKShare has no {operation}")

        last_error: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                frame = method(**kwargs)
                records = _frame_records(frame, operation)
                if not records:
                    raise AKShareNoDataError(f"AKShare {operation} returned no rows")
                return records
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                last_error = exc
                if attempt < self._max_attempts:
                    self._sleep(self._retry_backoff_seconds * (2 ** (attempt - 1)))

        assert last_error is not None
        if isinstance(last_error, AKShareError):
            raise last_error
        raise AKShareError(
            f"AKShare {operation} failed after {self._max_attempts} attempts"
        ) from last_error

    def _provider_records_once(
        self, operation: str, **kwargs: Any
    ) -> list[Mapping[str, Any]]:
        """仅调用一次数据提供者，执行具有独立时限的异步尝试。"""

        client = self._client or _import_akshare()
        method = getattr(client, operation, None)
        if method is None:
            raise AKShareError(f"installed AKShare has no {operation}")
        try:
            records = _frame_records(method(**kwargs), operation)
        except (KeyboardInterrupt, SystemExit):
            raise
        except AKShareError:
            raise
        except Exception as exc:
            raise AKShareError(f"AKShare {operation} failed") from exc
        if not records:
            raise AKShareNoDataError(f"AKShare {operation} returned no rows")
        return records

    async def _provider_records_async(
        self, operation: str, **kwargs: Any
    ) -> list[Mapping[str, Any]]:
        """分别限制每个上游，避免缓慢主来源跳过回退。

        Python 无法停止已在工作线程中运行的第三方 HTTP 调用。因此超时只限制
        当前调用方的等待。每次尝试只调用一次，可防止被放弃的工作线程在后台
        执行适配器的常规同步重试循环。
        """

        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    partial(self._provider_records_once, operation, **kwargs)
                ),
                timeout=self._timeout_seconds,
            )
        except TimeoutError as exc:
            raise AKShareTimeoutError(
                f"AKShare {operation} exceeded {self._timeout_seconds:g}s"
            ) from exc

    async def _eastmoney_records_async(
        self,
        *,
        primary_operation: str,
        canonical_symbol: str,
        code: str,
        interval: MinuteInterval,
    ) -> list[Mapping[str, Any]]:
        if self._client is not None:
            return await self._provider_records_async(
                primary_operation,
                symbol=code,
                start_date="1979-09-01 09:32:00",
                end_date="2222-01-01 09:32:00",
                period=str(interval.minutes),
                adjust="",
            )

        exchange = canonical_symbol.rsplit(".", maxsplit=1)[1]
        secid = f"{1 if exchange == 'SH' else 0}.{code}"
        if interval is MinuteInterval.ONE_MINUTE:
            url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
            params = {
                "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "ndays": "5",
                "iscr": "0",
                "secid": secid,
            }
            values = await self._eastmoney_json_values(url, params, "trends")
        else:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            params = {
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "klt": str(interval.minutes),
                "fqt": "0",
                "secid": secid,
                "beg": "0",
                "end": "20500000",
            }
            values = await self._eastmoney_json_values(url, params, "klines")
        has_vwap = interval is MinuteInterval.ONE_MINUTE
        return [_eastmoney_minute_record(value, has_vwap=has_vwap) for value in values]

    async def _eastmoney_json_values(
        self, url: str, params: Mapping[str, str], field: str
    ) -> list[str]:
        try:
            async with asyncio.timeout(self._timeout_seconds):
                async with httpx.AsyncClient(
                    timeout=_http_timeout(self._timeout_seconds),
                    follow_redirects=False,
                    transport=self._http_transport,
                    trust_env=False,
                ) as client:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                    payload = response.json()
        except (TimeoutError, httpx.TimeoutException) as exc:
            raise AKShareTimeoutError(
                f"AKShare Eastmoney minute endpoint exceeded {self._timeout_seconds:g}s"
            ) from exc
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            raise AKShareError("AKShare Eastmoney minute HTTP request failed") from exc
        try:
            values = payload["data"][field]
        except (KeyError, TypeError) as exc:
            raise AKSharePayloadError(
                "AKShare Eastmoney minute endpoint returned invalid data"
            ) from exc
        if not isinstance(values, list) or any(not isinstance(item, str) for item in values):
            raise AKSharePayloadError(
                "AKShare Eastmoney minute endpoint returned invalid records"
            )
        if not values:
            raise AKShareNoDataError(
                "AKShare Eastmoney minute endpoint returned no rows"
            )
        return values

    async def _sina_records_async(
        self, symbol: str, period: str
    ) -> list[Mapping[str, Any]]:
        if self._client is not None:
            return await self._provider_records_async(
                "stock_zh_a_minute",
                symbol=symbol,
                period=period,
                adjust="",
            )

        url = (
            "https://quotes.sina.cn/cn/api/jsonp_v2.php/=/"
            "CN_MarketDataService.getKLineData"
        )
        params = {
            "symbol": symbol,
            "scale": period,
            "ma": "no",
            "datalen": "1970",
        }
        try:
            async with asyncio.timeout(self._timeout_seconds):
                async with httpx.AsyncClient(
                    timeout=_http_timeout(self._timeout_seconds),
                    follow_redirects=False,
                    transport=self._http_transport,
                    trust_env=False,
                ) as client:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
        except (TimeoutError, httpx.TimeoutException) as exc:
            raise AKShareTimeoutError(
                f"AKShare stock_zh_a_minute exceeded {self._timeout_seconds:g}s"
            ) from exc
        except httpx.HTTPError as exc:
            raise AKShareError("AKShare stock_zh_a_minute HTTP request failed") from exc
        return _sina_jsonp_records(response.text)

    async def _run_async(
        self,
        label: str,
        function: Callable[..., _T],
        *args: Any,
        **kwargs: Any,
    ) -> _T:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(partial(function, *args, **kwargs)),
                timeout=self._timeout_seconds,
            )
        except TimeoutError as exc:
            raise AKShareTimeoutError(
                f"AKShare {label} exceeded {self._timeout_seconds:g}s"
            ) from exc

    def _freshness(
        self, provider_timestamp: datetime, fetched_at: datetime
    ) -> tuple[FreshnessStatus, tuple[str, ...]]:
        age = fetched_at - provider_timestamp
        if age < timedelta(minutes=-5):
            return FreshnessStatus.UNKNOWN, ("provider timestamp is ahead of collector clock",)
        if age < timedelta(0):
            age = timedelta(0)
        if age <= self._intraday_stale_after:
            return FreshnessStatus.CURRENT, ()
        return (
            FreshnessStatus.STALE,
            (f"provider record age={age.total_seconds():.1f}s",),
        )


def _import_akshare() -> Any:
    try:
        import akshare  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - broken optional installation
        raise AKShareError("akshare is not installed") from exc
    return akshare


def _http_timeout(total_seconds: float) -> httpx.Timeout:
    connect_seconds = min(total_seconds, 3.0)
    return httpx.Timeout(
        total_seconds,
        connect=connect_seconds,
        pool=connect_seconds,
    )


def _frame_records(frame: Any, operation: str) -> list[Mapping[str, Any]]:
    if frame is None or not hasattr(frame, "to_dict"):
        raise AKSharePayloadError(f"AKShare {operation} did not return a DataFrame")
    try:
        records = frame.to_dict(orient="records")
    except (TypeError, ValueError, AttributeError) as exc:
        raise AKSharePayloadError(
            f"AKShare {operation} returned an unreadable DataFrame"
        ) from exc
    if not isinstance(records, list) or any(not isinstance(row, Mapping) for row in records):
        raise AKSharePayloadError(f"AKShare {operation} returned invalid records")
    return records


def _sina_jsonp_records(payload: str) -> list[Mapping[str, Any]]:
    marker = "=("
    start = payload.find(marker)
    end = payload.rfind(");")
    if start < 0 or end <= start + len(marker):
        raise AKSharePayloadError("AKShare stock_zh_a_minute returned invalid JSONP")
    try:
        decoded = json.loads(payload[start + len(marker) : end])
    except (json.JSONDecodeError, TypeError) as exc:
        raise AKSharePayloadError(
            "AKShare stock_zh_a_minute returned invalid JSONP"
        ) from exc
    if not isinstance(decoded, list) or any(
        not isinstance(row, Mapping) for row in decoded
    ):
        raise AKSharePayloadError(
            "AKShare stock_zh_a_minute returned invalid records"
        )
    if not decoded:
        raise AKShareNoDataError("AKShare stock_zh_a_minute returned no rows")
    return decoded


def _eastmoney_minute_record(value: str, *, has_vwap: bool) -> Mapping[str, Any]:
    fields = value.split(",")
    if len(fields) < 7:
        raise AKSharePayloadError(
            "AKShare Eastmoney minute endpoint returned a truncated record"
        )
    return {
        "时间": fields[0],
        "开盘": fields[1],
        "收盘": fields[2],
        "最高": fields[3],
        "最低": fields[4],
        "成交量": fields[5],
        "成交额": fields[6],
        "均价": fields[7] if has_vwap and len(fields) > 7 else None,
    }


def _require_columns(
    rows: list[Mapping[str, Any]], required: frozenset[str], operation: str
) -> None:
    if not rows:
        raise AKShareNoDataError(f"AKShare {operation} returned no rows")
    missing = required.difference(rows[0])
    if missing:
        raise AKSharePayloadError(
            f"AKShare {operation} missing columns: {', '.join(sorted(missing))}"
        )


def _normalize_symbol(symbol: str) -> tuple[str, str]:
    value = symbol.strip().upper()
    if "." not in value:
        if len(value) != 6 or not value.isdigit():
            raise ValueError("symbol must look like 600000.SH or 000001.SZ")
        suffix = "SH" if value.startswith(("5", "6", "9")) else "SZ"
        value = f"{value}.{suffix}"
    code, exchange = value.split(".", maxsplit=1)
    if len(code) != 6 or not code.isdigit() or exchange not in {"SH", "SZ"}:
        raise ValueError("symbol must look like 600000.SH or 000001.SZ")
    return value, code


def _normalize_code(value: Any) -> str:
    if isinstance(value, bool):
        raise AKSharePayloadError(f"invalid AKShare stock code: {value!r}")
    if isinstance(value, int):
        text = str(value).zfill(6)
    elif isinstance(value, float) and math.isfinite(value) and value.is_integer():
        text = str(int(value)).zfill(6)
    else:
        text = str(value).strip().zfill(6)
    if len(text) != 6 or not text.isdigit():
        raise AKSharePayloadError(f"invalid AKShare stock code: {value!r}")
    return text


def _normalize_tencent_code(value: Any) -> str:
    text = str(value).strip().lower()
    if text.startswith(("sh", "sz")):
        text = text[2:]
    return _normalize_code(text)


def _sina_symbol(canonical_symbol: str) -> str:
    code, exchange = canonical_symbol.split(".", maxsplit=1)
    return f"{exchange.lower()}{code}"


def _minute_primary_operation(code: str) -> str:
    """为类似 ETF 的代码选择 AKShare 文档规定的分钟端点。

    沪市 ETF 通常以 ``5`` 开头，深市 ETF 通常以 ``159`` 开头。AKShare 股票
    分钟助手只根据首位是否为 ``6`` 推断市场，会把 510300 等代码错误映射到
    深圳。ETF 助手使用 AKShare 市场编号解析器，并生成本适配器使用的同一行情
    柱架构。
    """

    if code.startswith("5") or code.startswith("159"):
        return "fund_etf_hist_min_em"
    return "stock_zh_a_hist_min_em"


def _minute_primary_provider(operation: str) -> str:
    return f"AKShare/Eastmoney {operation}"


def _normalize_tencent_spot_row(
    code: str, record: Mapping[str, Any]
) -> tuple[Mapping[str, Any], tuple[str, ...]]:
    volume, volume_warning = _tencent_volume_lots(record.get("volume"))
    last = _optional_decimal(record.get("zxj"), "zxj")
    change = _optional_decimal(record.get("zd"), "zd")
    previous_close = None
    if last is not None and change is not None:
        previous_close = last - change
    warnings = (
        "Tencent fallback does not provide open/high/low in this schema",
        *(() if volume_warning is None else (volume_warning,)),
    )
    return (
        {
            "代码": code,
            "名称": record.get("name"),
            "最新价": last,
            "今开": None,
            "最高": None,
            "最低": None,
            "昨收": previous_close,
            "成交量": volume,
            "成交额": _tencent_turnover_yuan(record.get("turnover")),
            "换手率": record.get("hsl"),
        },
        warnings,
    )


def _tencent_volume_lots(value: Any) -> tuple[int, str | None]:
    """把腾讯显示的小数手数转换为端口契约要求的整数。"""

    number = _non_negative_decimal(value, "Tencent volume")
    whole_lots = int(number)
    remainder = number - whole_lots
    if remainder:
        return (
            whole_lots,
            f"Tencent fractional lot remainder {remainder} omitted by integer volume_lots contract",
        )
    return whole_lots, None


def _tencent_turnover_yuan(value: Any) -> Decimal:
    """腾讯行情表的 ``turnover`` 以人民币万元显示。"""

    return _non_negative_decimal(value, "Tencent turnover") * Decimal(10_000)


def _sina_volume_lots(value: Any) -> tuple[int, tuple[str, ...]]:
    """把新浪分钟成交量（股）转换为完整 A 股手数。"""

    shares = _non_negative_integer(value, "Sina volume")
    lots, odd_shares = divmod(shares, 100)
    if odd_shares:
        return (
            lots,
            (
                f"Sina volume converted from shares; odd-share remainder {odd_shares} "
                "omitted by integer volume_lots contract",
            ),
        )
    return lots, ("Sina volume converted from shares to 100-share lots",)


def _optional_text(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def _optional_decimal(value: Any, field: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip().replace(",", "")
    if text.lower() in {"", "-", "--", "nan", "none", "null"}:
        return None
    try:
        number = Decimal(text)
    except InvalidOperation as exc:
        raise AKSharePayloadError(f"{field} is not numeric: {value!r}") from exc
    if not number.is_finite():
        raise AKSharePayloadError(f"{field} is not finite: {value!r}")
    return number


def _positive_decimal(value: Any, field: str) -> Decimal:
    number = _optional_decimal(value, field)
    if number is None or number <= 0:
        raise AKSharePayloadError(f"{field} must be positive: {value!r}")
    return number


def _non_negative_decimal(value: Any, field: str, *, missing_zero: bool = False) -> Decimal:
    number = _optional_decimal(value, field)
    if number is None:
        if missing_zero:
            return Decimal(0)
        raise AKSharePayloadError(f"{field} is missing")
    if number < 0:
        raise AKSharePayloadError(f"{field} cannot be negative: {value!r}")
    return number


def _non_negative_integer(value: Any, field: str, *, missing_zero: bool = False) -> int:
    number = _optional_decimal(value, field)
    if number is None:
        if missing_zero:
            return 0
        raise AKSharePayloadError(f"{field} is missing")
    integer = int(number)
    if number != integer or integer < 0:
        raise AKSharePayloadError(f"{field} must be a non-negative integer: {value!r}")
    return integer


def _parse_direction(value: Any) -> TradeDirection:
    normalized = str(value).strip().upper()
    if normalized in {"买盘", "买", "B", "BUY"}:
        return TradeDirection.BUY
    if normalized in {"卖盘", "卖", "S", "SELL"}:
        return TradeDirection.SELL
    if normalized in {"中性盘", "中性", "N", "NEUTRAL"}:
        return TradeDirection.NEUTRAL
    return TradeDirection.UNKNOWN


def _parse_trade_time(value: Any, fetched_at: datetime) -> tuple[datetime, bool]:
    text = str(value).strip()
    for datetime_format in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(text, datetime_format).replace(tzinfo=SHANGHAI_TZ)
            return parsed, False
        except ValueError:
            pass
    try:
        parsed_time = wall_time.fromisoformat(text)
    except ValueError as exc:
        raise AKSharePayloadError(f"invalid trade time: {value!r}") from exc
    return datetime.combine(fetched_at.date(), parsed_time, tzinfo=SHANGHAI_TZ), True


def _parse_provider_datetime(value: Any) -> datetime:
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise AKSharePayloadError(f"invalid provider datetime: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


def _to_shanghai(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(SHANGHAI_TZ)


def _aware_now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now() must return a timezone-aware datetime")
    return value.astimezone(SHANGHAI_TZ)


def _non_negative_age(current: datetime, past: datetime) -> timedelta:
    return max(current - past, timedelta(0))
