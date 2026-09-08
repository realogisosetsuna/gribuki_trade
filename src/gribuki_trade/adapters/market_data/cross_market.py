"""严格的多上游 AKShare 跨市场快照适配器。

``index_global_spot_em`` 是主要的全表观测。若其失败或缺少已配置序列，只有在
已安装 AKShare API 暴露经审计精确指数映射时，适配器才可使用独立来源的新浪
日收盘价。其他缺口一律保持缺失；绝不替换为名称相似的 ETF、期货或波动率
序列。
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, time, timedelta
from functools import partial
from queue import Empty, Queue
from typing import Any

from gribuki_trade.adapters.ashare.screening import _SINA_HISTORY_LOCK
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

from . import cross_market_payload as _payload

PROVIDER = _payload.PROVIDER
SINA_DAILY_PROVIDER = _payload.SINA_DAILY_PROVIDER
_PROVIDER_TIMEZONE = _payload._PROVIDER_TIMEZONE
_SinaDailyFallbackSpec = _payload._SinaDailyFallbackSpec
_frame_records = _payload._frame_records
_missing_item = _payload._missing_item
_source_error_code = _payload._source_error_code
_fallback_failure_reason = _payload._fallback_failure_reason
_parse_sina_daily_quote = _payload._parse_sina_daily_quote
_resolve_columns = _payload._resolve_columns
_index_rows = _payload._index_rows
_match_row = _payload._match_row
_parse_quote = _payload._parse_quote
_parse_quote_time = _payload._parse_quote_time
_required_session_date = _payload._required_session_date
_optional_daily_decimal = _payload._optional_daily_decimal
_optional_column_decimal = _payload._optional_column_decimal
_required_decimal = _payload._required_decimal
_required_text = _payload._required_text
_optional_text = _payload._optional_text
_is_missing = _payload._is_missing
_normalize_code = _payload._normalize_code
_normalize_name = _payload._normalize_name
_aware_now = _payload._aware_now
_validate_universe = _payload._validate_universe

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

_COLUMN_ALIASES = _payload._COLUMN_ALIASES
_REQUIRED_COLUMNS = _payload._REQUIRED_COLUMNS
_DAILY_COLUMN_ALIASES = _payload._DAILY_COLUMN_ALIASES
_DAILY_REQUIRED_COLUMNS = _payload._DAILY_REQUIRED_COLUMNS

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
            timeout_seconds if fallback_timeout_seconds is None else fallback_timeout_seconds
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
        fallback_work: list[tuple[CrossMarketInstrumentSpec, _SinaDailyFallbackSpec]] = []
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
                fallback_quotes.append(self._fetch_sina_daily_quote(spec, fallback, fetched_at))
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

        fallback_count = sum(_audited_sina_fallback(spec) is not None for spec in self._universe)
        orchestration_timeout = (
            self._timeout_seconds + self._fallback_timeout_seconds * fallback_count + 1.0
        )
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(partial(self.fetch_cross_market_snapshot)),
                timeout=orchestration_timeout,
            )
        except TimeoutError as exc:
            raise CrossMarketDataTimeoutError(
                f"cross-market primary/fallback orchestration exceeded {orchestration_timeout:g}s"
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
            raise CrossMarketDataError("installed AKShare has no callable index_global_spot_em")
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
            raise CrossMarketDataError(f"installed AKShare has no callable {fallback.method_name}")
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
        raise CrossMarketDataTimeoutError(f"{label} exceeded {timeout_seconds:g}s")
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
    code_overlap = {_normalize_code(alias) for alias in spec.code_aliases}.intersection(
        _normalize_code(alias) for alias in canonical.code_aliases
    )
    name_overlap = {_normalize_name(alias) for alias in spec.name_aliases}.intersection(
        _normalize_name(alias) for alias in canonical.name_aliases
    )
    if not code_overlap and not name_overlap:
        return None
    return fallback
