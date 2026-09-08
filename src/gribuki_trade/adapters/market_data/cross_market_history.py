"""供跨市场因子研究使用的独立 AKShare/新浪日线历史。

每个已配置市场都使用经过审计的精确指数端点和代码。各次调用具有独立截止时间
和失败状态；不可用的指数保持缺失，而不会被 ETF、期货或名称相似的代理替代。
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from functools import partial
from queue import Empty, Queue
from typing import Any
from zoneinfo import ZoneInfo

from gribuki_trade.adapters.ashare.screening import _SINA_HISTORY_LOCK
from gribuki_trade.ports.cross_market_history import (
    MINIMUM_CROSS_MARKET_HISTORY,
    CrossMarketHistoryDataError,
    CrossMarketHistoryEndpointError,
    CrossMarketHistoryFailureCode,
    CrossMarketHistoryMissingSeries,
    CrossMarketHistoryObservation,
    CrossMarketHistoryPayloadError,
    CrossMarketHistorySeries,
    CrossMarketHistorySnapshot,
    CrossMarketHistoryTimeoutError,
)


@dataclass(frozen=True, slots=True)
class CrossMarketHistorySpec:
    """一个精确 AKShare 方法/代码及其常规收盘时间语义。"""

    market_id: str
    display_name: str
    method_name: str
    symbol: str
    local_timezone: str
    regular_close: time

    def __post_init__(self) -> None:
        for field_name in ("market_id", "display_name", "method_name", "symbol"):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"{field_name} cannot be blank")
        ZoneInfo(self.local_timezone)

    @property
    def source(self) -> str:
        return f"AKShare/Sina {self.method_name}({self.symbol})"


DEFAULT_CROSS_MARKET_HISTORY_UNIVERSE: tuple[CrossMarketHistorySpec, ...] = (
    CrossMarketHistorySpec(
        "CSI_300",
        "沪深300指数",
        "stock_zh_index_daily",
        "sh000300",
        "Asia/Shanghai",
        time(15, 0),
    ),
    CrossMarketHistorySpec(
        "HANG_SENG",
        "恒生指数",
        "stock_hk_index_daily_sina",
        "HSI",
        "Asia/Hong_Kong",
        time(16, 10),
    ),
    CrossMarketHistorySpec(
        "HANG_SENG_CHINA_ENTERPRISES",
        "恒生中国企业指数",
        "stock_hk_index_daily_sina",
        "HSCEI",
        "Asia/Hong_Kong",
        time(16, 10),
    ),
    CrossMarketHistorySpec(
        "S_AND_P_500",
        "标普500指数",
        "index_us_stock_sina",
        ".INX",
        "America/New_York",
        time(16, 0),
    ),
    CrossMarketHistorySpec(
        "NASDAQ_100",
        "纳斯达克100指数",
        "index_us_stock_sina",
        ".NDX",
        "America/New_York",
        time(16, 0),
    ),
    CrossMarketHistorySpec(
        "DOW_JONES_INDUSTRIAL",
        "道琼斯工业平均指数",
        "index_us_stock_sina",
        ".DJI",
        "America/New_York",
        time(16, 0),
    ),
    CrossMarketHistorySpec(
        "NIKKEI_225",
        "日经225指数",
        "index_global_hist_sina",
        "日经225指数",
        "Asia/Tokyo",
        time(15, 30),
    ),
    CrossMarketHistorySpec(
        "KOSPI",
        "韩国综合股价指数",
        "index_global_hist_sina",
        "首尔综合指数",
        "Asia/Seoul",
        time(15, 30),
    ),
    CrossMarketHistorySpec(
        "TAIWAN_WEIGHTED",
        "中国台湾加权指数",
        "index_global_hist_sina",
        "中国台湾加权指数",
        "Asia/Taipei",
        time(13, 30),
    ),
)


class _InsufficientHistoryError(CrossMarketHistoryDataError):
    def __init__(self, *, visible: int, required: int) -> None:
        super().__init__(
            f"only {visible} point-in-time observations were visible; {required} required"
        )
        self.visible = visible
        self.required = required


class AKShareCrossMarketHistoryAdapter:
    """采集具有时点可见性和失败隔离的精确指数历史。"""

    def __init__(
        self,
        client: Any | None = None,
        *,
        universe: Sequence[CrossMarketHistorySpec] = DEFAULT_CROSS_MARKET_HISTORY_UNIVERSE,
        timeout_seconds: float = 15.0,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        specs = tuple(universe)
        _validate_universe(specs)
        self._client = client
        self._universe = specs
        self._timeout_seconds = timeout_seconds
        self._now = now or (lambda: datetime.now(tz=UTC))

    def fetch_cross_market_history(
        self,
        *,
        as_of: datetime,
        minimum_observations: int = MINIMUM_CROSS_MARKET_HISTORY,
    ) -> CrossMarketHistorySnapshot:
        """只返回已到配置交易日收盘锚点的收盘记录。"""

        _require_aware(as_of, "as_of")
        if minimum_observations < MINIMUM_CROSS_MARKET_HISTORY:
            raise ValueError(
                "minimum_observations must be at least "
                f"{MINIMUM_CROSS_MARKET_HISTORY}"
            )
        fetched_at = _aware_now(self._now)
        resolved: list[CrossMarketHistorySeries] = []
        missing: list[CrossMarketHistoryMissingSeries] = []
        # 新浪历史解码器内嵌 MiniRacer/V8。按确定顺序处理配置的股票池，而不是
        # 同时调用多个原生解码器；每个序列仍拥有自身截止时间和隔离失败结果。
        for spec in self._universe:
            try:
                resolved.append(
                    self._fetch_series(spec, as_of, minimum_observations)
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except CrossMarketHistoryDataError as exc:
                missing.append(_missing_from_error(spec, exc))
            except Exception:  # 隔离异常的数据提供者对象。
                missing.append(
                    _missing_from_error(
                        spec,
                        CrossMarketHistoryDataError(
                            f"{spec.source} failed unexpectedly"
                        )
                    )
                )

        warnings = [
            "daily closes use configured regular-session close anchors for PIT visibility",
            "the public endpoints provide no historical release vintages or revision log",
            "cross-market co-movement is observational and does not establish causality",
        ]
        if missing:
            warnings.append(
                f"{len(missing)} of {len(self._universe)} exact index histories are missing; "
                "no proxy series were substituted"
            )
        return CrossMarketHistorySnapshot(
            as_of=as_of,
            fetched_at=fetched_at,
            minimum_observations=minimum_observations,
            series=tuple(resolved),
            missing=tuple(missing),
            degraded=bool(missing),
            warnings=tuple(warnings),
        )

    async def fetch_cross_market_history_async(
        self,
        *,
        as_of: datetime,
        minimum_observations: int = MINIMUM_CROSS_MARKET_HISTORY,
    ) -> CrossMarketHistorySnapshot:
        """受所有独立串行序列截止时间限制的异步外观。"""

        orchestration_timeout = (
            self._timeout_seconds * max(1, len(self._universe)) + 1.0
        )
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    partial(
                        self.fetch_cross_market_history,
                        as_of=as_of,
                        minimum_observations=minimum_observations,
                    )
                ),
                timeout=orchestration_timeout,
            )
        except TimeoutError as exc:  # pragma: no cover - defensive outer deadline
            raise CrossMarketHistoryTimeoutError(
                "cross-market history orchestration exceeded "
                f"{orchestration_timeout:g}s"
            ) from exc

    def _fetch_series(
        self,
        spec: CrossMarketHistorySpec,
        as_of: datetime,
        minimum_observations: int,
    ) -> CrossMarketHistorySeries:
        client = self._client or _import_akshare()
        method = getattr(client, spec.method_name, None)
        if method is None or not callable(method):
            raise CrossMarketHistoryEndpointError(
                f"installed AKShare has no callable {spec.method_name}"
            )
        try:
            frame = _run_with_timeout(
                partial(
                    _run_sina_history_call,
                    partial(method, symbol=spec.symbol),
                ),
                self._timeout_seconds,
                spec.source,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except CrossMarketHistoryDataError:
            raise
        except Exception as exc:
            raise CrossMarketHistoryDataError(f"{spec.source} failed") from exc
        observations = _parse_history_frame(frame, spec, as_of)
        if len(observations) < minimum_observations:
            raise _InsufficientHistoryError(
                visible=len(observations),
                required=minimum_observations,
            )
        return CrossMarketHistorySeries(
            market_id=spec.market_id,
            display_name=spec.display_name,
            observations=observations,
        )


def _parse_history_frame(
    frame: Any,
    spec: CrossMarketHistorySpec,
    as_of: datetime,
) -> tuple[CrossMarketHistoryObservation, ...]:
    if frame is None or not hasattr(frame, "to_dict"):
        raise CrossMarketHistoryPayloadError(f"{spec.source} did not return a DataFrame")
    try:
        records = frame.to_dict(orient="records")
    except (TypeError, ValueError, AttributeError) as exc:
        raise CrossMarketHistoryPayloadError(
            f"{spec.source} returned an unreadable DataFrame"
        ) from exc
    if not isinstance(records, list) or not records:
        raise CrossMarketHistoryPayloadError(f"{spec.source} returned no rows")
    if any(not isinstance(row, Mapping) for row in records):
        raise CrossMarketHistoryPayloadError(f"{spec.source} returned invalid records")
    columns = _resolve_columns(records)
    local_zone = ZoneInfo(spec.local_timezone)
    by_date: dict[date, CrossMarketHistoryObservation] = {}
    for row in records:
        session_date = _parse_session_date(row.get(columns["date"]), spec.source)
        available_at = datetime.combine(
            session_date,
            spec.regular_close,
            tzinfo=local_zone,
        )
        # 尚不可见的记录不得影响时点请求，即使当前提供者载荷后来包含格式错误的
        # 修订也不例外。
        if available_at > as_of:
            continue
        close = _parse_close(row.get(columns["close"]), spec.source, session_date)
        if session_date in by_date:
            raise CrossMarketHistoryPayloadError(
                f"{spec.source} returned duplicate session date {session_date.isoformat()}"
            )
        by_date[session_date] = CrossMarketHistoryObservation(
            market_id=spec.market_id,
            session_date=session_date,
            close=close,
            available_at=available_at,
            source=spec.source,
            local_timezone=spec.local_timezone,
        )
    return tuple(by_date[item] for item in sorted(by_date))


def _resolve_columns(records: Sequence[Mapping[Any, Any]]) -> dict[str, Any]:
    aliases: Mapping[str, tuple[str, ...]] = {
        "date": ("date", "日期"),
        "close": ("close", "收盘", "收盘价"),
    }
    available: dict[str, Any] = {}
    for row in records:
        for key in row:
            available.setdefault(str(key).strip().casefold(), key)
    resolved: dict[str, Any] = {}
    for canonical, candidates in aliases.items():
        for candidate in candidates:
            actual = available.get(candidate.casefold())
            if actual is not None:
                resolved[canonical] = actual
                break
        if canonical not in resolved:
            raise CrossMarketHistoryPayloadError(
                f"historical payload is missing required {canonical} column"
            )
    return resolved


def _parse_session_date(value: Any, source: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text or text.casefold() in {"nat", "nan", "none"}:
        raise CrossMarketHistoryPayloadError(f"{source} contains a missing session date")
    normalized = text.replace("/", "-")
    if len(normalized) == 8 and normalized.isdigit():
        normalized = f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:]}"
    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError as exc:
        raise CrossMarketHistoryPayloadError(
            f"{source} contains invalid session date {text!r}"
        ) from exc


def _parse_close(value: Any, source: str, session_date: date) -> Decimal:
    text = str(value).strip().replace(",", "")
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise CrossMarketHistoryPayloadError(
            f"{source} close is not numeric for {session_date.isoformat()}"
        ) from exc
    if not parsed.is_finite() or parsed <= 0:
        raise CrossMarketHistoryPayloadError(
            f"{source} close must be positive and finite for {session_date.isoformat()}"
        )
    return parsed


def _run_with_timeout(call: Callable[[], Any], timeout_seconds: float, label: str) -> Any:
    result_queue: Queue[tuple[bool, Any]] = Queue(maxsize=1)

    def invoke() -> None:
        try:
            result_queue.put((True, call()))
        except BaseException as exc:
            result_queue.put((False, exc))

    worker = threading.Thread(
        target=invoke,
        daemon=True,
        name="cross-market-history-source",
    )
    worker.start()
    worker.join(timeout_seconds)
    if worker.is_alive():
        raise CrossMarketHistoryTimeoutError(f"{label} exceeded {timeout_seconds:g}s")
    try:
        succeeded, value = result_queue.get_nowait()
    except Empty as exc:  # pragma: no cover - defensive thread invariant
        raise CrossMarketHistoryDataError(f"{label} ended without a result") from exc
    if succeeded:
        return value
    if isinstance(value, BaseException):
        raise value
    raise CrossMarketHistoryDataError(f"{label} returned an invalid thread result")


def _run_sina_history_call(call: Callable[[], Any]) -> Any:
    """在进程级锁内运行实际由 MiniRacer 支持的方法。"""

    with _SINA_HISTORY_LOCK:
        return call()


def _missing_from_error(
    spec: CrossMarketHistorySpec,
    error: CrossMarketHistoryDataError,
) -> CrossMarketHistoryMissingSeries:
    if isinstance(error, _InsufficientHistoryError):
        failure_code = CrossMarketHistoryFailureCode.INSUFFICIENT_HISTORY
    elif isinstance(error, CrossMarketHistoryTimeoutError):
        failure_code = CrossMarketHistoryFailureCode.TIMEOUT
    elif isinstance(error, CrossMarketHistoryEndpointError):
        failure_code = CrossMarketHistoryFailureCode.ENDPOINT_UNAVAILABLE
    elif isinstance(error, CrossMarketHistoryPayloadError):
        failure_code = CrossMarketHistoryFailureCode.INVALID_PAYLOAD
    else:
        failure_code = CrossMarketHistoryFailureCode.UPSTREAM_ERROR
    return CrossMarketHistoryMissingSeries(
        market_id=spec.market_id,
        display_name=spec.display_name,
        expected_source=spec.source,
        failure_code=failure_code,
        reason=str(error),
    )


def _import_akshare() -> Any:
    try:
        import akshare  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - declared project dependency
        raise CrossMarketHistoryEndpointError("akshare is not installed") from exc
    return akshare


def _validate_universe(specs: Sequence[CrossMarketHistorySpec]) -> None:
    ids = [spec.market_id for spec in specs]
    if len(ids) != len(set(ids)):
        raise ValueError("cross-market history universe contains duplicate market_id values")


def _aware_now(now: Callable[[], datetime]) -> datetime:
    value = now()
    _require_aware(value, "now()")
    return value


def _require_aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
