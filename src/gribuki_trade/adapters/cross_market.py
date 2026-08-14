"""严格的多上游 AKShare 跨市场快照适配器。

``index_global_spot_em`` 是主要的全表观测。若其失败或缺少已配置序列，只有在
已安装 AKShare API 暴露经审计精确指数映射时，适配器才可使用独立来源的新浪
日收盘价。其他缺口一律保持缺失；绝不替换为名称相似的 ETF、期货或波动率
序列。
"""

from __future__ import annotations

import asyncio
import math
import threading
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from functools import partial
from queue import Empty, Queue
from typing import Any
from zoneinfo import ZoneInfo

from gribuki_trade.adapters.ashare_screening import _SINA_HISTORY_LOCK
from gribuki_trade.ports.cross_market import (
    CrossMarketDataError,
    CrossMarketDataTimeoutError,
    CrossMarketInstrumentSpec,
    CrossMarketMissingItem,
    CrossMarketPayloadError,
    CrossMarketQuote,
    CrossMarketSegment,
    CrossMarketSnapshot,
)

PROVIDER = "AKShare/Eastmoney index_global_spot_em"
SINA_DAILY_PROVIDER = "AKShare/Sina daily close fallback"
_PROVIDER_TIMEZONE = ZoneInfo("Asia/Shanghai")


DEFAULT_CROSS_MARKET_UNIVERSE: tuple[CrossMarketInstrumentSpec, ...] = (
    CrossMarketInstrumentSpec(
        "SHANGHAI_COMPOSITE",
        "上证综合指数（A股大盘）",
        CrossMarketSegment.A_SHARE,
        ("000001",),
        ("上证指数", "上证综合指数"),
        "Asia/Shanghai",
    ),
    CrossMarketInstrumentSpec(
        "CSI_300",
        "沪深300指数（沪深大盘宽基）",
        CrossMarketSegment.A_SHARE,
        ("000300",),
        ("沪深300", "沪深300指数"),
        "Asia/Shanghai",
    ),
    CrossMarketInstrumentSpec(
        "SHENZHEN_COMPONENT",
        "深证成份指数（深市大盘）",
        CrossMarketSegment.A_SHARE,
        ("399001",),
        ("深证成指", "深证成份指数"),
        "Asia/Shanghai",
    ),
    CrossMarketInstrumentSpec(
        "CHINEXT",
        "创业板指数（A股成长风格）",
        CrossMarketSegment.A_SHARE,
        ("399006",),
        ("创业板指", "创业板指数"),
        "Asia/Shanghai",
    ),
    CrossMarketInstrumentSpec(
        "HANG_SENG",
        "恒生指数（香港大盘）",
        CrossMarketSegment.HONG_KONG,
        ("HSI",),
        ("恒生指数",),
        "Asia/Hong_Kong",
    ),
    CrossMarketInstrumentSpec(
        "HANG_SENG_CHINA_ENTERPRISES",
        "恒生中国企业指数（香港中资股）",
        CrossMarketSegment.HONG_KONG,
        ("HSCEI",),
        ("国企指数", "恒生中国企业指数"),
        "Asia/Hong_Kong",
    ),
    CrossMarketInstrumentSpec(
        "NIKKEI_225",
        "日经225指数（日本）",
        CrossMarketSegment.ASIA_PACIFIC,
        ("N225",),
        ("日经225", "日经225指数"),
        "Asia/Tokyo",
    ),
    CrossMarketInstrumentSpec(
        "KOSPI",
        "韩国综合股价指数（韩国）",
        CrossMarketSegment.ASIA_PACIFIC,
        ("KS11",),
        ("韩国KOSPI", "韩国综合股价指数"),
        "Asia/Seoul",
    ),
    CrossMarketInstrumentSpec(
        "TAIWAN_WEIGHTED",
        "台湾加权指数（中国台湾）",
        CrossMarketSegment.ASIA_PACIFIC,
        ("TWII",),
        ("台湾加权", "台湾加权指数"),
        "Asia/Taipei",
    ),
    CrossMarketInstrumentSpec(
        "STRAITS_TIMES",
        "海峡时报指数（新加坡）",
        CrossMarketSegment.ASIA_PACIFIC,
        ("STI",),
        ("富时新加坡海峡时报", "海峡时报指数"),
        "Asia/Singapore",
    ),
    CrossMarketInstrumentSpec(
        "SENSEX",
        "孟买SENSEX指数（印度）",
        CrossMarketSegment.ASIA_PACIFIC,
        ("SENSEX",),
        ("印度孟买SENSEX", "孟买SENSEX"),
        "Asia/Kolkata",
    ),
    CrossMarketInstrumentSpec(
        "S_AND_P_500",
        "标普500指数（美国大盘）",
        CrossMarketSegment.UNITED_STATES,
        ("SPX",),
        ("标普500", "标普500指数"),
        "America/New_York",
    ),
    CrossMarketInstrumentSpec(
        "NASDAQ_100",
        "纳斯达克100指数（美国科技成长）",
        CrossMarketSegment.UNITED_STATES,
        ("NDX",),
        ("纳斯达克", "纳斯达克100", "纳斯达克100指数"),
        "America/New_York",
    ),
    CrossMarketInstrumentSpec(
        "DOW_JONES_INDUSTRIAL",
        "道琼斯工业平均指数（美国蓝筹）",
        CrossMarketSegment.UNITED_STATES,
        ("DJIA",),
        ("道琼斯", "道琼斯工业平均指数"),
        "America/New_York",
    ),
    CrossMarketInstrumentSpec(
        "US_DOLLAR_INDEX",
        "美元指数（跨资产定价因子）",
        CrossMarketSegment.DOLLAR_COMMODITY_RISK,
        ("UDI", "DXY"),
        ("美元指数",),
        "America/New_York",
    ),
    CrossMarketInstrumentSpec(
        "CRB_COMMODITY_INDEX",
        "路透CRB商品指数（综合商品）",
        CrossMarketSegment.DOLLAR_COMMODITY_RISK,
        ("CRB",),
        ("路透CRB商品指数", "CRB商品指数"),
        "America/New_York",
    ),
    CrossMarketInstrumentSpec(
        "BALTIC_DRY_INDEX",
        "波罗的海干散货指数（航运景气代理）",
        CrossMarketSegment.DOLLAR_COMMODITY_RISK,
        ("BDI",),
        ("波罗的海BDI指数", "波罗的海干散货指数"),
        "Europe/London",
        timedelta(hours=72),
    ),
    CrossMarketInstrumentSpec(
        "CBOE_VIX",
        "CBOE波动率指数（美股隐含波动率）",
        CrossMarketSegment.DOLLAR_COMMODITY_RISK,
        ("VIX",),
        ("VIX恐慌指数", "CBOE波动率指数", "标普500波动率指数"),
        "America/Chicago",
    ),
)


@dataclass(frozen=True, slots=True)
class _SinaDailyFallbackSpec:
    method_name: str
    symbol: str
    provider_name: str
    local_timezone: str
    regular_close: time
    completion_grace: timedelta = timedelta(minutes=30)


# 这里刻意只采用已安装 AKShare 1.18.x 接口暴露的映射。
# ``index_us_stock_sina`` 明确记录 .INX、.NDX 和 .DJI，因此这三项是精确指数
# 映射。该 API 与 ``index_global_hist_sina`` 都不暴露 DXY、CRB 或 VIX。东方
# 财富不可用时，这些证券必须保持缺失；名称相似的 ETF 或期货不是同一序列。
_SINA_DAILY_FALLBACKS: Mapping[str, _SinaDailyFallbackSpec] = {
    "SHANGHAI_COMPOSITE": _SinaDailyFallbackSpec(
        "stock_zh_index_daily",
        "sh000001",
        "上证指数",
        "Asia/Shanghai",
        time(15, 0),
    ),
    "CSI_300": _SinaDailyFallbackSpec(
        "stock_zh_index_daily",
        "sh000300",
        "沪深300",
        "Asia/Shanghai",
        time(15, 0),
    ),
    "SHENZHEN_COMPONENT": _SinaDailyFallbackSpec(
        "stock_zh_index_daily",
        "sz399001",
        "深证成指",
        "Asia/Shanghai",
        time(15, 0),
    ),
    "CHINEXT": _SinaDailyFallbackSpec(
        "stock_zh_index_daily",
        "sz399006",
        "创业板指",
        "Asia/Shanghai",
        time(15, 0),
    ),
    "HANG_SENG": _SinaDailyFallbackSpec(
        "stock_hk_index_daily_sina",
        "HSI",
        "恒生指数",
        "Asia/Hong_Kong",
        time(16, 10),
    ),
    "HANG_SENG_CHINA_ENTERPRISES": _SinaDailyFallbackSpec(
        "stock_hk_index_daily_sina",
        "HSCEI",
        "恒生中国企业指数",
        "Asia/Hong_Kong",
        time(16, 10),
    ),
    "NIKKEI_225": _SinaDailyFallbackSpec(
        "index_global_hist_sina",
        "日经225指数",
        "日经225指数",
        "Asia/Tokyo",
        time(15, 30),
    ),
    "KOSPI": _SinaDailyFallbackSpec(
        "index_global_hist_sina",
        "首尔综合指数",
        "韩国综合股价指数",
        "Asia/Seoul",
        time(15, 30),
    ),
    "TAIWAN_WEIGHTED": _SinaDailyFallbackSpec(
        "index_global_hist_sina",
        "中国台湾加权指数",
        "中国台湾加权指数",
        "Asia/Taipei",
        time(13, 30),
    ),
    "SENSEX": _SinaDailyFallbackSpec(
        "index_global_hist_sina",
        "印度孟买SENSEX指数",
        "印度孟买SENSEX指数",
        "Asia/Kolkata",
        time(15, 30),
    ),
    "S_AND_P_500": _SinaDailyFallbackSpec(
        "index_us_stock_sina",
        ".INX",
        "S&P 500 Index",
        "America/New_York",
        time(16, 0),
    ),
    "NASDAQ_100": _SinaDailyFallbackSpec(
        "index_us_stock_sina",
        ".NDX",
        "Nasdaq-100 Index",
        "America/New_York",
        time(16, 0),
    ),
    "DOW_JONES_INDUSTRIAL": _SinaDailyFallbackSpec(
        "index_us_stock_sina",
        ".DJI",
        "Dow Jones Industrial Average",
        "America/New_York",
        time(16, 0),
    ),
}


_COLUMN_ALIASES: Mapping[str, tuple[str, ...]] = {
    "code": ("代码", "code", "symbol"),
    "name": ("名称", "name"),
    "last": ("最新价", "last", "price"),
    "change_amount": ("涨跌额", "change", "change_amount"),
    "change_percent": ("涨跌幅", "change_percent", "pct_change"),
    "open": ("开盘价", "开盘", "open"),
    "high": ("最高价", "最高", "high"),
    "low": ("最低价", "最低", "low"),
    "previous_close": ("昨收价", "昨收", "previous_close", "pre_close"),
    "amplitude_percent": ("振幅", "amplitude", "amplitude_percent"),
    "quote_time": ("最新行情时间", "行情时间", "quote_time", "timestamp"),
}
_REQUIRED_COLUMNS = frozenset({"code", "name", "last", "change_percent", "quote_time"})

_DAILY_COLUMN_ALIASES: Mapping[str, tuple[str, ...]] = {
    "date": ("date", "日期"),
    "open": ("open", "开盘"),
    "high": ("high", "最高"),
    "low": ("low", "最低"),
    "close": ("close", "收盘"),
}
_DAILY_REQUIRED_COLUMNS = frozenset({"date", "close"})
_DEFAULT_SPEC_BY_ID: Mapping[str, CrossMarketInstrumentSpec] = {
    spec.instrument_id: spec for spec in DEFAULT_CROSS_MARKET_UNIVERSE
}


class AKShareCrossMarketAdapter:
    """获取主要全球报价及严格映射的日线回退。"""

    def __init__(
        self,
        client: Any | None = None,
        *,
        universe: Sequence[CrossMarketInstrumentSpec] = DEFAULT_CROSS_MARKET_UNIVERSE,
        timeout_seconds: float = 15.0,
        fallback_timeout_seconds: float | None = None,
        future_tolerance_seconds: float = 300.0,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        resolved_fallback_timeout = (
            timeout_seconds
            if fallback_timeout_seconds is None
            else fallback_timeout_seconds
        )
        if resolved_fallback_timeout <= 0:
            raise ValueError("fallback_timeout_seconds must be positive")
        if future_tolerance_seconds < 0:
            raise ValueError("future_tolerance_seconds cannot be negative")
        specs = tuple(universe)
        _validate_universe(specs)
        self._client = client
        self._universe = specs
        self._timeout_seconds = timeout_seconds
        self._fallback_timeout_seconds = resolved_fallback_timeout
        self._future_tolerance = timedelta(seconds=future_tolerance_seconds)
        self._now = now or (lambda: datetime.now(tz=UTC))

    def fetch_cross_market_snapshot(self) -> CrossMarketSnapshot:
        """获取主表，之后只使用经过审计的精确日线回退。"""

        fetched_at = _aware_now(self._now)
        primary_error: CrossMarketDataError | None = None
        primary_quotes: list[CrossMarketQuote] = []
        unresolved: list[CrossMarketInstrumentSpec]
        try:
            rows = self._provider_records_once()
            primary_quotes, unresolved = self._parse_primary_rows(rows, fetched_at)
        except CrossMarketDataError as exc:
            primary_error = exc
            unresolved = list(self._universe)

        fallback_quotes: list[CrossMarketQuote] = []
        missing: list[CrossMarketMissingItem] = []
        fallback_work: list[
            tuple[CrossMarketInstrumentSpec, _SinaDailyFallbackSpec]
        ] = []
        fallback_failures = 0
        for spec in unresolved:
            fallback = _audited_sina_fallback(spec)
            if fallback is None:
                missing.append(
                    _missing_item(
                        spec,
                        reason=(
                            "configured exact aliases were absent from the primary "
                            "snapshot and no independently audited Sina daily mapping "
                            "exists; no proxy was substituted"
                            if primary_error is None
                            else "primary snapshot was unavailable and no independently "
                            "audited Sina daily mapping exists; no proxy was substituted"
                        ),
                    )
                )
                continue
            fallback_work.append((spec, fallback))

        # AKShare 已审计的新浪回退通过 MiniRacer 解码 JavaScript。在 Windows 上
        # 同时运行多个运行时可能终止进程，因此保持股票池顺序，并串行地给每个
        # 来源分配自身截止时间。
        for spec, fallback in fallback_work:
            try:
                fallback_quotes.append(
                    self._fetch_sina_daily_quote(spec, fallback, fetched_at)
                )
            except CrossMarketDataError as exc:
                fallback_failures += 1
                missing.append(
                    _missing_item(
                        spec,
                        reason=_fallback_failure_reason(exc),
                    )
                )

        quotes = [*primary_quotes, *fallback_quotes]
        if not quotes and isinstance(
            primary_error, (CrossMarketDataTimeoutError, CrossMarketPayloadError)
        ):
                # 没有独立来源观测能挽救请求时，保留既有类型化失败契约。
            raise primary_error

        degraded = (
            primary_error is not None
            or bool(fallback_quotes)
            or bool(missing)
            or any(quote.degraded for quote in quotes)
        )
        warnings = [
            "single provider table; observations are not synchronized exchange ticks",
            "cross-market co-movement is observational and does not establish causality",
        ]
        if primary_error is not None:
            warnings.append(
                "primary Eastmoney snapshot unavailable "
                f"({_source_error_code(primary_error)}); independent Sina daily "
                "fallbacks were attempted where exact mappings exist"
            )
        if fallback_quotes:
            warnings.append(
                f"{len(fallback_quotes)} series use independent Sina daily closes; "
                "their timestamps are configured local session-close anchors"
            )
        if fallback_failures:
            warnings.append(
                f"{fallback_failures} of {len(fallback_work)} audited Sina daily "
                "fallback calls failed independently"
            )
        if missing:
            warnings.append(
                f"{len(missing)} configured series absent; no proxy values were fabricated"
            )
        provider = PROVIDER
        if fallback_quotes and primary_quotes:
            provider = f"{PROVIDER} + {SINA_DAILY_PROVIDER}"
        elif fallback_quotes:
            provider = SINA_DAILY_PROVIDER
        elif primary_error is not None:
            provider = f"{PROVIDER}; {SINA_DAILY_PROVIDER} unavailable"
        return CrossMarketSnapshot(
            fetched_at=fetched_at,
            provider=provider,
            quotes=tuple(quotes),
            missing=tuple(missing),
            degraded=degraded,
            warnings=tuple(warnings),
        )

    async def fetch_cross_market_snapshot_async(self) -> CrossMarketSnapshot:
        """限制总编排时间；每个上游调用也分别受限。"""

        fallback_count = sum(
            _audited_sina_fallback(spec) is not None for spec in self._universe
        )
        orchestration_timeout = (
            self._timeout_seconds
            + self._fallback_timeout_seconds * fallback_count
            + 1.0
        )
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(partial(self.fetch_cross_market_snapshot)),
                timeout=orchestration_timeout,
            )
        except TimeoutError as exc:
            raise CrossMarketDataTimeoutError(
                "cross-market primary/fallback orchestration exceeded "
                f"{orchestration_timeout:g}s"
            ) from exc

    def _parse_primary_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
        fetched_at: datetime,
    ) -> tuple[list[CrossMarketQuote], list[CrossMarketInstrumentSpec]]:
        columns = _resolve_columns(rows)
        indexed_codes, indexed_names = _index_rows(rows, columns)
        quotes: list[CrossMarketQuote] = []
        unresolved: list[CrossMarketInstrumentSpec] = []
        consumed_rows: set[int] = set()
        for spec in self._universe:
            match = _match_row(spec, indexed_codes, indexed_names)
            if match is None:
                unresolved.append(spec)
                continue
            row_index, basis = match
            if row_index in consumed_rows:
                raise CrossMarketPayloadError(
                    "one provider row matched more than one configured instrument"
                )
            consumed_rows.add(row_index)
            quotes.append(
                _parse_quote(
                    rows[row_index],
                    columns,
                    spec,
                    fetched_at,
                    self._future_tolerance,
                    basis,
                )
            )
        return quotes, unresolved

    def _provider_records_once(self) -> list[Mapping[str, Any]]:
        client = self._client or _import_akshare()
        method = getattr(client, "index_global_spot_em", None)
        if method is None or not callable(method):
            raise CrossMarketDataError(
                "installed AKShare has no callable index_global_spot_em"
            )
        try:
            frame = _run_with_timeout(
                method,
                self._timeout_seconds,
                "AKShare index_global_spot_em",
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except CrossMarketDataError:
            raise
        except Exception as exc:
            raise CrossMarketDataError("AKShare index_global_spot_em failed") from exc
        return _frame_records(frame)

    def _fetch_sina_daily_quote(
        self,
        spec: CrossMarketInstrumentSpec,
        fallback: _SinaDailyFallbackSpec,
        fetched_at: datetime,
    ) -> CrossMarketQuote:
        client = self._client or _import_akshare()
        method = getattr(client, fallback.method_name, None)
        if method is None or not callable(method):
            raise CrossMarketDataError(
                f"installed AKShare has no callable {fallback.method_name}"
            )
        label = f"AKShare {fallback.method_name}({fallback.symbol})"
        try:
            frame = _run_with_timeout(
                partial(
                    _run_sina_v8_call,
                    partial(method, symbol=fallback.symbol),
                ),
                self._fallback_timeout_seconds,
                label,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except CrossMarketDataError:
            raise
        except Exception as exc:
            raise CrossMarketDataError(f"{label} failed") from exc
        rows = _frame_records(frame, label=label)
        return _parse_sina_daily_quote(
            rows,
            spec,
            fallback,
            fetched_at,
            self._future_tolerance,
        )


def _import_akshare() -> Any:
    try:
        import akshare  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dependency declared by project
        raise CrossMarketDataError("akshare is not installed") from exc
    return akshare


def _run_with_timeout(
    call: Callable[[], Any],
    timeout_seconds: float,
    label: str,
) -> Any:
    """运行一次阻塞上游调用，并设置其自身非阻塞截止时间。"""

    result_queue: Queue[tuple[bool, Any]] = Queue(maxsize=1)

    def invoke() -> None:
        try:
            result_queue.put((True, call()))
        except BaseException as exc:
    # 异常会传递到调用方线程并在那里重新抛出。
            result_queue.put((False, exc))

    worker = threading.Thread(target=invoke, daemon=True, name="cross-market-source")
    worker.start()
    worker.join(timeout_seconds)
    if worker.is_alive():
        raise CrossMarketDataTimeoutError(
            f"{label} exceeded {timeout_seconds:g}s"
        )
    try:
        succeeded, value = result_queue.get_nowait()
    except Empty as exc:  # pragma: no cover - defensive thread invariant
        raise CrossMarketDataError(f"{label} ended without a result") from exc
    if succeeded:
        return value
    if isinstance(value, BaseException):
        raise value
    raise CrossMarketDataError(f"{label} returned an invalid thread result")


def _run_sina_v8_call(call: Callable[[], Any]) -> Any:
    """在共享进程锁内运行一次由 MiniRacer 支持的新浪调用。"""

    with _SINA_HISTORY_LOCK:
        return call()


def _frame_records(
    frame: Any,
    *,
    label: str = "AKShare index_global_spot_em",
) -> list[Mapping[str, Any]]:
    if frame is None or not hasattr(frame, "to_dict"):
        raise CrossMarketPayloadError(f"{label} did not return a DataFrame")
    try:
        records = frame.to_dict(orient="records")
    except (TypeError, ValueError, AttributeError) as exc:
        raise CrossMarketPayloadError(f"{label} returned an unreadable DataFrame") from exc
    if not isinstance(records, list) or any(not isinstance(row, Mapping) for row in records):
        raise CrossMarketPayloadError(f"{label} returned invalid records")
    if not records:
        raise CrossMarketPayloadError(f"{label} returned no rows")
    return records


def _audited_sina_fallback(
    spec: CrossMarketInstrumentSpec,
) -> _SinaDailyFallbackSpec | None:
    """仅当自定义规格仍表示已审计序列时返回回退。"""

    fallback = _SINA_DAILY_FALLBACKS.get(spec.instrument_id)
    canonical = _DEFAULT_SPEC_BY_ID.get(spec.instrument_id)
    if fallback is None or canonical is None:
        return None
    if (
        spec.segment != canonical.segment
        or spec.local_timezone != canonical.local_timezone
        or spec.local_timezone != fallback.local_timezone
    ):
        return None
    code_overlap = {
        _normalize_code(alias) for alias in spec.code_aliases
    }.intersection(_normalize_code(alias) for alias in canonical.code_aliases)
    name_overlap = {
        _normalize_name(alias) for alias in spec.name_aliases
    }.intersection(_normalize_name(alias) for alias in canonical.name_aliases)
    if not code_overlap and not name_overlap:
        return None
    return fallback


def _missing_item(
    spec: CrossMarketInstrumentSpec,
    *,
    reason: str,
) -> CrossMarketMissingItem:
    return CrossMarketMissingItem(
        instrument_id=spec.instrument_id,
        display_name=spec.display_name,
        segment=spec.segment,
        expected_codes=spec.code_aliases,
        expected_names=spec.name_aliases,
        reason=reason,
    )


def _source_error_code(error: CrossMarketDataError) -> str:
    if isinstance(error, CrossMarketDataTimeoutError):
        return "timeout"
    if isinstance(error, CrossMarketPayloadError):
        return "incompatible_payload"
    return "provider_error"


def _fallback_failure_reason(error: CrossMarketDataError) -> str:
    if isinstance(error, CrossMarketDataTimeoutError):
        return "independent audited Sina daily fallback timed out"
    if isinstance(error, CrossMarketPayloadError):
        return "independent audited Sina daily fallback returned invalid data"
    return "independent audited Sina daily fallback was unavailable"


def _parse_sina_daily_quote(
    rows: Sequence[Mapping[str, Any]],
    spec: CrossMarketInstrumentSpec,
    fallback: _SinaDailyFallbackSpec,
    fetched_at: datetime,
    future_tolerance: timedelta,
) -> CrossMarketQuote:
    columns = _resolve_daily_columns(rows)
    local_zone = ZoneInfo(fallback.local_timezone)
    collector_local = fetched_at.astimezone(local_zone)
    completed: dict[
        date, tuple[Mapping[str, Any], Decimal, datetime]
    ] = {}
    for row in rows:
        session_date = _required_session_date(row.get(columns["date"]))
        quote_time = datetime.combine(
            session_date,
            fallback.regular_close,
            tzinfo=local_zone,
        )
        if quote_time + fallback.completion_grace > collector_local:
            continue
        close = _required_decimal(row.get(columns["close"]), "close")
        if close <= 0:
            raise CrossMarketPayloadError(
                f"close must be positive for {spec.instrument_id}"
            )
        if session_date in completed:
            raise CrossMarketPayloadError(
                f"duplicate completed daily row for {spec.instrument_id}: "
                f"{session_date.isoformat()}"
            )
        completed[session_date] = (row, close, quote_time)

    ordered = sorted(completed.items(), key=lambda item: item[0])
    if len(ordered) < 2:
        raise CrossMarketPayloadError(
            f"Sina daily fallback has fewer than two completed sessions for "
            f"{spec.instrument_id}"
        )
    previous_date, (_, previous_close, _) = ordered[-2]
    latest_date, (latest_row, last, local_quote_time) = ordered[-1]
    open_price = _optional_daily_decimal(latest_row, columns, "open")
    high = _optional_daily_decimal(latest_row, columns, "high")
    low = _optional_daily_decimal(latest_row, columns, "low")
    for field, value in (("open", open_price), ("high", high), ("low", low)):
        if value is not None and value <= 0:
            raise CrossMarketPayloadError(
                f"{field} must be positive for {spec.instrument_id}"
            )
    if high is not None and low is not None and high < low:
        raise CrossMarketPayloadError(f"high is below low for {spec.instrument_id}")
    if high is not None and last > high:
        raise CrossMarketPayloadError(f"close is above high for {spec.instrument_id}")
    if low is not None and last < low:
        raise CrossMarketPayloadError(f"close is below low for {spec.instrument_id}")

    change_amount = last - previous_close
    change_percent = change_amount / previous_close * Decimal("100")
    amplitude = (
        (high - low) / previous_close * Decimal("100")
        if high is not None and low is not None
        else None
    )
    future_by = local_quote_time.astimezone(UTC) - fetched_at.astimezone(UTC)
    if future_by > future_tolerance:
        raise CrossMarketPayloadError(
            f"daily close anchor is {future_by.total_seconds():.1f}s in the future "
            f"for {spec.instrument_id}"
        )
    age = fetched_at.astimezone(UTC) - local_quote_time.astimezone(UTC)
    if age < timedelta(0):
        age = timedelta(0)
    stale = age > spec.stale_after
    warnings = [
        "degraded independent fallback after the primary snapshot was unavailable "
        "or absent",
        "Sina daily data supplies only a session date; local_quote_time is a "
        "configured regular-close anchor, not a provider timestamp",
        "change_percent was computed from two consecutive completed Sina daily "
        f"closes ({previous_date.isoformat()} to {latest_date.isoformat()})",
    ]
    missing_optional = [
        field
        for field, value in (("open", open_price), ("high", high), ("low", low))
        if value is None
    ]
    if missing_optional:
        warnings.append(
            "optional daily fields unavailable: " + ", ".join(missing_optional)
        )
    if stale:
        warnings.append(f"daily close age={age.total_seconds():.1f}s")

    return CrossMarketQuote(
        instrument_id=spec.instrument_id,
        display_name=spec.display_name,
        segment=spec.segment,
        provider_code=fallback.symbol,
        provider_name=fallback.provider_name,
        last=last,
        change_percent=change_percent,
        change_amount=change_amount,
        open=open_price,
        high=high,
        low=low,
        previous_close=previous_close,
        amplitude_percent=amplitude,
        local_quote_time=local_quote_time,
        local_timezone=fallback.local_timezone,
        fetched_at=fetched_at,
        provider=f"{SINA_DAILY_PROVIDER} ({fallback.method_name})",
        stale=stale,
        degraded=True,
        warnings=tuple(warnings),
    )


def _resolve_daily_columns(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    provider_columns = {str(key) for row in rows for key in row}
    resolved: dict[str, str] = {}
    for canonical, aliases in _DAILY_COLUMN_ALIASES.items():
        matches = [alias for alias in aliases if alias in provider_columns]
        if len(matches) > 1:
            raise CrossMarketPayloadError(
                f"ambiguous Sina daily columns for {canonical}: {matches}"
            )
        if matches:
            resolved[canonical] = matches[0]
    missing = sorted(_DAILY_REQUIRED_COLUMNS.difference(resolved))
    if missing:
        raise CrossMarketPayloadError(
            "Sina daily fallback missing required columns: " + ", ".join(missing)
        )
    return resolved


def _required_session_date(value: Any) -> date:
    if _is_missing(value) or str(value).strip().casefold() == "nat":
        raise CrossMarketPayloadError("date is missing")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "to_pydatetime"):
        try:
            candidate = value.to_pydatetime()
        except (TypeError, ValueError, AttributeError) as exc:
            raise CrossMarketPayloadError("date is invalid") from exc
        if isinstance(candidate, datetime):
            return candidate.date()
        if isinstance(candidate, date):
            return candidate
    text = str(value).strip()
    try:
        return date.fromisoformat(text)
    except ValueError:
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError as exc:
            raise CrossMarketPayloadError(f"date is not ISO-compatible: {text!r}") from exc


def _optional_daily_decimal(
    row: Mapping[str, Any],
    columns: Mapping[str, str],
    canonical: str,
) -> Decimal | None:
    provider_column = columns.get(canonical)
    if provider_column is None or _is_missing(row.get(provider_column)):
        return None
    return _required_decimal(row.get(provider_column), canonical)


def _resolve_columns(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    provider_columns = {str(key) for row in rows for key in row}
    resolved: dict[str, str] = {}
    for canonical, aliases in _COLUMN_ALIASES.items():
        matches = [alias for alias in aliases if alias in provider_columns]
        if len(matches) > 1:
            raise CrossMarketPayloadError(
                f"ambiguous provider columns for {canonical}: {matches}"
            )
        if matches:
            resolved[canonical] = matches[0]
    missing = sorted(_REQUIRED_COLUMNS.difference(resolved))
    if missing:
        raise CrossMarketPayloadError(
            "AKShare index_global_spot_em missing required columns: " + ", ".join(missing)
        )
    return resolved


def _index_rows(
    rows: Sequence[Mapping[str, Any]],
    columns: Mapping[str, str],
) -> tuple[dict[str, tuple[int, ...]], dict[str, tuple[int, ...]]]:
    code_lists: dict[str, list[int]] = {}
    name_lists: dict[str, list[int]] = {}
    for index, row in enumerate(rows):
        code = _optional_text(row.get(columns["code"]))
        name = _optional_text(row.get(columns["name"]))
        if code is not None:
            code_lists.setdefault(_normalize_code(code), []).append(index)
        if name is not None:
            name_lists.setdefault(_normalize_name(name), []).append(index)
    return (
        {key: tuple(value) for key, value in code_lists.items()},
        {key: tuple(value) for key, value in name_lists.items()},
    )


def _match_row(
    spec: CrossMarketInstrumentSpec,
    indexed_codes: Mapping[str, tuple[int, ...]],
    indexed_names: Mapping[str, tuple[int, ...]],
) -> tuple[int, str] | None:
    code_matches = {
        row_index
        for alias in spec.code_aliases
        for row_index in indexed_codes.get(_normalize_code(alias), ())
    }
    if len(code_matches) > 1:
        raise CrossMarketPayloadError(
            f"multiple provider rows match code aliases for {spec.instrument_id}"
        )
    if code_matches:
        return next(iter(code_matches)), "code"

    name_matches = {
        row_index
        for alias in spec.name_aliases
        for row_index in indexed_names.get(_normalize_name(alias), ())
    }
    if len(name_matches) > 1:
        raise CrossMarketPayloadError(
            f"multiple provider rows match name aliases for {spec.instrument_id}"
        )
    if name_matches:
        return next(iter(name_matches)), "name"
    return None


def _parse_quote(
    row: Mapping[str, Any],
    columns: Mapping[str, str],
    spec: CrossMarketInstrumentSpec,
    fetched_at: datetime,
    future_tolerance: timedelta,
    match_basis: str,
) -> CrossMarketQuote:
    try:
        code = _required_text(row.get(columns["code"]), "code")
        name = _required_text(row.get(columns["name"]), "name")
        last = _required_decimal(row.get(columns["last"]), "last")
        change_percent = _required_decimal(
            row.get(columns["change_percent"]), "change_percent"
        )
        provider_time, time_warning = _parse_quote_time(row.get(columns["quote_time"]))
        change_amount = _optional_column_decimal(row, columns, "change_amount")
        open_price = _optional_column_decimal(row, columns, "open")
        high = _optional_column_decimal(row, columns, "high")
        low = _optional_column_decimal(row, columns, "low")
        previous_close = _optional_column_decimal(row, columns, "previous_close")
        amplitude = _optional_column_decimal(row, columns, "amplitude_percent")
    except CrossMarketPayloadError as exc:
        raise CrossMarketPayloadError(
            f"invalid matched row for {spec.instrument_id}: {exc}"
        ) from exc

    if last <= 0:
        raise CrossMarketPayloadError(f"last must be positive for {spec.instrument_id}")
    for field, value in (
        ("open", open_price),
        ("high", high),
        ("low", low),
        ("previous_close", previous_close),
    ):
        if value is not None and value <= 0:
            raise CrossMarketPayloadError(
                f"{field} must be positive for {spec.instrument_id}"
            )
    if high is not None and low is not None and high < low:
        raise CrossMarketPayloadError(f"high is below low for {spec.instrument_id}")
    if amplitude is not None and amplitude < 0:
        raise CrossMarketPayloadError(
            f"amplitude_percent cannot be negative for {spec.instrument_id}"
        )

    warnings: list[str] = []
    if time_warning is not None:
        warnings.append(time_warning)
    if match_basis == "name":
        warnings.append("matched by configured exact provider-name alias")
    elif _normalize_name(name) not in {
        _normalize_name(alias) for alias in spec.name_aliases
    }:
        warnings.append("provider name differs from configured aliases; stable code matched")

    future_by = provider_time.astimezone(UTC) - fetched_at.astimezone(UTC)
    if future_by > future_tolerance:
        raise CrossMarketPayloadError(
            f"provider quote time is {future_by.total_seconds():.1f}s in the future "
            f"for {spec.instrument_id}"
        )
    clock_skew = future_by > timedelta(0)
    if clock_skew:
        warnings.append("provider quote time is slightly ahead of collector clock")
    age = fetched_at.astimezone(UTC) - provider_time.astimezone(UTC)
    if age < timedelta(0):
        age = timedelta(0)
    stale = age > spec.stale_after
    if stale:
        warnings.append(f"provider quote age={age.total_seconds():.1f}s")

    optional_values = {
        "change_amount": change_amount,
        "open": open_price,
        "high": high,
        "low": low,
        "previous_close": previous_close,
        "amplitude_percent": amplitude,
    }
    missing_optional = tuple(
        field for field, value in optional_values.items() if value is None
    )
    if missing_optional:
        warnings.append(
            "optional quote fields unavailable: " + ", ".join(missing_optional)
        )

    local_time = provider_time.astimezone(ZoneInfo(spec.local_timezone))
    return CrossMarketQuote(
        instrument_id=spec.instrument_id,
        display_name=spec.display_name,
        segment=spec.segment,
        provider_code=code,
        provider_name=name,
        last=last,
        change_percent=change_percent,
        change_amount=change_amount,
        open=open_price,
        high=high,
        low=low,
        previous_close=previous_close,
        amplitude_percent=amplitude,
        local_quote_time=local_time,
        local_timezone=spec.local_timezone,
        fetched_at=fetched_at,
        provider=PROVIDER,
        stale=stale,
        degraded=stale or clock_skew or match_basis == "name" or bool(missing_optional),
        warnings=tuple(warnings),
    )


def _parse_quote_time(value: Any) -> tuple[datetime, str | None]:
    if _is_missing(value):
        raise CrossMarketPayloadError("quote_time is missing")
    parsed: datetime
    if isinstance(value, datetime):
        parsed = value
    elif hasattr(value, "to_pydatetime"):
        try:
            candidate = value.to_pydatetime()
        except (TypeError, ValueError, AttributeError) as exc:
            raise CrossMarketPayloadError("quote_time is invalid") from exc
        if not isinstance(candidate, datetime):
            raise CrossMarketPayloadError("quote_time is not a datetime")
        parsed = candidate
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and not math.isfinite(value):
            raise CrossMarketPayloadError("quote_time is not finite")
        try:
            parsed = datetime.fromtimestamp(float(value), tz=UTC)
        except (OverflowError, OSError, ValueError) as exc:
            raise CrossMarketPayloadError("quote_time epoch is invalid") from exc
    else:
        text = str(value).strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CrossMarketPayloadError(
                f"quote_time is not ISO-compatible: {text!r}"
            ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return (
            parsed.replace(tzinfo=_PROVIDER_TIMEZONE),
            "provider emitted timezone-naive text; interpreted as Asia/Shanghai "
            "per AKShare interface contract",
        )
    return parsed, None


def _optional_column_decimal(
    row: Mapping[str, Any], columns: Mapping[str, str], canonical: str
) -> Decimal | None:
    provider_column = columns.get(canonical)
    if provider_column is None:
        return None
    value = row.get(provider_column)
    if _is_missing(value):
        return None
    return _required_decimal(value, canonical)


def _required_decimal(value: Any, field: str) -> Decimal:
    if _is_missing(value) or isinstance(value, bool):
        raise CrossMarketPayloadError(f"{field} is missing")
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise CrossMarketPayloadError(f"{field} is not numeric") from exc
    if not result.is_finite():
        raise CrossMarketPayloadError(f"{field} is not finite")
    return result


def _required_text(value: Any, field: str) -> str:
    result = _optional_text(value)
    if result is None:
        raise CrossMarketPayloadError(f"{field} is missing")
    return result


def _optional_text(value: Any) -> str | None:
    if _is_missing(value):
        return None
    result = str(value).strip()
    return result or None


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


def _normalize_code(value: str) -> str:
    return unicodedata.normalize("NFKC", value).strip().casefold()


def _normalize_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).strip().casefold()
    return " ".join(normalized.split())


def _aware_now(now: Callable[[], datetime]) -> datetime:
    value = now()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now() must return a timezone-aware datetime")
    return value


def _validate_universe(specs: Sequence[CrossMarketInstrumentSpec]) -> None:
    if not specs:
        raise ValueError("universe cannot be empty")
    ids = [spec.instrument_id for spec in specs]
    if len(ids) != len(set(ids)):
        raise ValueError("universe instrument_id values must be unique")
    seen_codes: dict[str, str] = {}
    seen_names: dict[str, str] = {}
    for spec in specs:
        for alias in spec.code_aliases:
            normalized = _normalize_code(alias)
            existing = seen_codes.setdefault(normalized, spec.instrument_id)
            if existing != spec.instrument_id:
                raise ValueError(
                    f"code alias {alias!r} overlaps {existing} and {spec.instrument_id}"
                )
        for alias in spec.name_aliases:
            normalized = _normalize_name(alias)
            existing = seen_names.setdefault(normalized, spec.instrument_id)
            if existing != spec.instrument_id:
                raise ValueError(
                    f"name alias {alias!r} overlaps {existing} and {spec.instrument_id}"
                )
