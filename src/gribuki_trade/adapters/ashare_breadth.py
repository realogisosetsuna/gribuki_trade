"""当前交易日 A 股收盘市场宽度的软失败 AKShare 适配器。

先尝试东方财富，腾讯作为独立限时的回退。二者都是次级公开网页快照。适配器
刻意拒绝规模过小或交易所覆盖不完整的股票池，而不会从残缺页面发布看似精确
的市场宽度。
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
from queue import Empty, Queue
from typing import Any, TypeVar, cast
from zoneinfo import ZoneInfo

from gribuki_trade.ports.ashare_breadth import (
    AShareBreadthCoverageError,
    AShareBreadthDataError,
    AShareBreadthEndpointError,
    AShareBreadthMeta,
    AShareBreadthPayloadError,
    AShareBreadthSnapshot,
    AShareBreadthSourceFailure,
    AShareBreadthSourcesExhaustedError,
    AShareBreadthTimeoutError,
    AShareExchange,
    AShareExchangeCount,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")

EASTMONEY_BREADTH_SOURCE_ID = "AKShare/Eastmoney stock_zh_a_spot_em"
EASTMONEY_BREADTH_SOURCE_URL = (
    "https://quote.eastmoney.com/center/gridlist.html#hs_a_board"
)
TENCENT_BREADTH_SOURCE_ID = "AKShare/Tencent stock_zh_a_spot_tx"
TENCENT_BREADTH_SOURCE_URL = (
    "https://stockapp.finance.qq.com/mstats/#mod=list&id=hs_hsj&module=hs&type=hsj"
)

_MARKET_CLOSE = time(15, 0)
_DEFAULT_READY_DELAY = timedelta(minutes=5)
_DEFAULT_STALE_AFTER = timedelta(hours=6)
_DEFAULT_MINIMUM_ELIGIBLE_COUNT = 4500
_REQUIRED_EXCHANGES = tuple(AShareExchange)

_EASTMONEY_ALIASES: Mapping[str, tuple[str, ...]] = {
    "code": ("代码", "code"),
    "name": ("名称", "name"),
    "last": ("最新价", "last"),
    "change_percent": ("涨跌幅", "change_percent"),
    "amount": ("成交额", "amount"),
}
_TENCENT_ALIASES: Mapping[str, tuple[str, ...]] = {
    "code": ("code",),
    "name": ("name",),
    "last": ("zxj",),
    "change_percent": ("zdf",),
    "amount": ("turnover",),
    "stock_type": ("stock_type",),
}
_REQUIRED_FIELDS = frozenset({"code", "name", "last", "change_percent", "amount"})
_TENCENT_AMOUNT_MULTIPLIER = Decimal("10000")

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class _SourceSpec:
    source_id: str
    source_url: str
    endpoint: str
    aliases: Mapping[str, tuple[str, ...]]
    amount_multiplier: Decimal


@dataclass(frozen=True, slots=True)
class _EligibleQuote:
    symbol: str
    name: str
    exchange: AShareExchange
    last: Decimal
    change_percent: Decimal
    amount_cny: Decimal


_SOURCES = (
    _SourceSpec(
        source_id=EASTMONEY_BREADTH_SOURCE_ID,
        source_url=EASTMONEY_BREADTH_SOURCE_URL,
        endpoint="stock_zh_a_spot_em",
        aliases=_EASTMONEY_ALIASES,
        amount_multiplier=Decimal("1"),
    ),
    _SourceSpec(
        source_id=TENCENT_BREADTH_SOURCE_ID,
        source_url=TENCENT_BREADTH_SOURCE_URL,
        endpoint="stock_zh_a_spot_tx",
        aliases=_TENCENT_ALIASES,
        amount_multiplier=_TENCENT_AMOUNT_MULTIPLIER,
    ),
)


class AKShareAShareBreadthAdapter:
    """Collect and aggregate a same-day post-close沪深京 breadth snapshot."""

    def __init__(
        self,
        client: Any | None = None,
        *,
        timeout_seconds: float = 35.0,
        minimum_eligible_count: int = _DEFAULT_MINIMUM_ELIGIBLE_COUNT,
        ready_delay: timedelta = _DEFAULT_READY_DELAY,
        stale_after: timedelta = _DEFAULT_STALE_AFTER,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if timeout_seconds <= 0 or not math.isfinite(timeout_seconds):
            raise ValueError("timeout_seconds must be positive and finite")
        if minimum_eligible_count < 1:
            raise ValueError("minimum_eligible_count must be positive")
        if ready_delay < timedelta(0):
            raise ValueError("ready_delay cannot be negative")
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._minimum_eligible_count = minimum_eligible_count
        self._ready_delay = ready_delay
        self._stale_after = stale_after
        self._now = now or (lambda: datetime.now(tz=UTC))

    def fetch_close_breadth(self, session_date: date) -> AShareBreadthSnapshot:
        started_at = _aware_now(self._now)
        _validate_same_session_close(session_date, started_at, self._ready_delay)
        client = self._client or _import_akshare()
        failures: list[AShareBreadthSourceFailure] = []

        for source_index, spec in enumerate(_SOURCES):
            try:
                records = _provider_records(
                    client,
                    spec,
                    timeout_seconds=self._timeout_seconds,
                )
                fetched_at = _aware_now(self._now)
                return _build_snapshot(
                    records,
                    spec=spec,
                    session_date=session_date,
                    fetched_at=fetched_at,
                    minimum_eligible_count=self._minimum_eligible_count,
                    stale_after=self._stale_after,
                    fallback_used=source_index > 0,
                    previous_failures=tuple(failures),
                )
            except AShareBreadthDataError as exc:
                failures.append(_source_failure(spec.source_id, exc))
            except Exception:
                failures.append(
                    AShareBreadthSourceFailure(
                        source_id=spec.source_id,
                        failure_code="UPSTREAM_ERROR",
                        reason="provider call failed",
                    )
                )

        raise AShareBreadthSourcesExhaustedError(tuple(failures))

    async def fetch_close_breadth_async(
        self,
        session_date: date,
    ) -> AShareBreadthSnapshot:
        return await asyncio.to_thread(self.fetch_close_breadth, session_date)


def _provider_records(
    client: Any,
    spec: _SourceSpec,
    *,
    timeout_seconds: float,
) -> tuple[Mapping[str, Any], ...]:
    endpoint = getattr(client, spec.endpoint, None)
    if endpoint is None or not callable(endpoint):
        raise AShareBreadthEndpointError(f"{spec.source_id} endpoint is unavailable")
    payload = _call_with_timeout(
        cast(Callable[[], Any], endpoint),
        timeout_seconds=timeout_seconds,
        source_id=spec.source_id,
    )
    if hasattr(payload, "columns") and hasattr(payload, "to_dict"):
        columns = tuple(str(item) for item in payload.columns)
        if len(columns) != len(set(columns)):
            raise AShareBreadthPayloadError(f"{spec.source_id} has duplicate columns")
        try:
            raw_records = payload.to_dict(orient="records")
        except (TypeError, ValueError, AttributeError) as exc:
            raise AShareBreadthPayloadError(
                f"{spec.source_id} cannot convert tabular payload"
            ) from exc
    elif isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        raw_records = payload
    else:
        raise AShareBreadthPayloadError(f"{spec.source_id} payload is not tabular")
    if not raw_records:
        raise AShareBreadthPayloadError(f"{spec.source_id} returned no rows")
    if any(not isinstance(item, Mapping) for item in raw_records):
        raise AShareBreadthPayloadError(f"{spec.source_id} rows must be mappings")
    return tuple(cast(Mapping[str, Any], item) for item in raw_records)


def _call_with_timeout(
    call: Callable[[], _T],
    *,
    timeout_seconds: float,
    source_id: str,
) -> _T:
    output: Queue[tuple[bool, object]] = Queue(maxsize=1)

    def run() -> None:
        try:
            output.put((True, call()))
        except BaseException as exc:  # 将数据提供者失败传递回调用方线程。
            output.put((False, exc))

    worker = threading.Thread(target=run, daemon=True, name="ashare-breadth-provider")
    worker.start()
    try:
        succeeded, value = output.get(timeout=timeout_seconds)
    except Empty:
        raise AShareBreadthTimeoutError(
            f"{source_id} exceeded {timeout_seconds:g}s"
        ) from None
    if succeeded:
        return cast(_T, value)
    if isinstance(value, AShareBreadthDataError):
        raise value
    if isinstance(value, (KeyboardInterrupt, SystemExit)):
        raise value
    raise AShareBreadthDataError(f"{source_id} provider call failed") from None


def _build_snapshot(
    records: tuple[Mapping[str, Any], ...],
    *,
    spec: _SourceSpec,
    session_date: date,
    fetched_at: datetime,
    minimum_eligible_count: int,
    stale_after: timedelta,
    fallback_used: bool,
    previous_failures: tuple[AShareBreadthSourceFailure, ...],
) -> AShareBreadthSnapshot:
    columns = _resolve_columns(records, spec)
    quotes: dict[str, _EligibleQuote] = {}
    duplicate_count = 0
    excluded_non_equity_count = 0
    non_trading_count = 0

    for row_number, row in enumerate(records, start=1):
        raw_code = _required_text(row.get(columns["code"]), "code", spec, row_number)
        classified = _classify_code(raw_code, spec, row_number)
        if classified is None:
            excluded_non_equity_count += 1
            continue
        symbol, exchange = classified
        name = _required_text(row.get(columns["name"]), "name", spec, row_number)
        _validate_tencent_stock_type(row, columns, spec, row_number, exchange)

        market_values = tuple(
            row.get(columns[field]) for field in ("last", "change_percent", "amount")
        )
        missing = tuple(_is_missing(value) for value in market_values)
        if all(missing):
            non_trading_count += 1
            continue
        if any(missing):
            raise AShareBreadthPayloadError(
                f"{spec.source_id} row {row_number} has partially missing market fields"
            )
        last = _decimal(market_values[0], "last", spec, row_number)
        change_percent = _decimal(
            market_values[1], "change_percent", spec, row_number
        )
        amount = _decimal(market_values[2], "amount", spec, row_number)
        if last <= 0:
            raise AShareBreadthPayloadError(
                f"{spec.source_id} row {row_number} last must be positive"
            )
        if change_percent < Decimal("-100") or change_percent > Decimal("10000"):
            raise AShareBreadthPayloadError(
                f"{spec.source_id} row {row_number} change_percent is implausible"
            )
        if amount < 0:
            raise AShareBreadthPayloadError(
                f"{spec.source_id} row {row_number} amount cannot be negative"
            )
        quote = _EligibleQuote(
            symbol=symbol,
            name=name,
            exchange=exchange,
            last=last,
            change_percent=change_percent,
            amount_cny=amount * spec.amount_multiplier,
        )
        previous = quotes.get(symbol)
        if previous is None:
            quotes[symbol] = quote
        elif previous == quote:
            duplicate_count += 1
        else:
            raise AShareBreadthPayloadError(
                f"{spec.source_id} returned conflicting duplicate {symbol}"
            )

    eligible = tuple(quotes[symbol] for symbol in sorted(quotes))
    if len(eligible) < minimum_eligible_count:
        raise AShareBreadthCoverageError(
            f"{spec.source_id} eligible universe {len(eligible)} is below "
            f"minimum {minimum_eligible_count}"
        )
    exchange_counts = tuple(
        AShareExchangeCount(
            exchange=exchange,
            eligible_count=sum(item.exchange is exchange for item in eligible),
        )
        for exchange in _REQUIRED_EXCHANGES
    )
    missing_exchanges = tuple(
        item.exchange.value for item in exchange_counts if item.eligible_count == 0
    )
    if missing_exchanges:
        raise AShareBreadthCoverageError(
            f"{spec.source_id} omitted required exchanges: {', '.join(missing_exchanges)}"
        )

    changes = tuple(item.change_percent for item in eligible)
    advancing = tuple(item for item in eligible if item.change_percent > 0)
    declining_count = sum(item.change_percent < 0 for item in eligible)
    flat_count = len(eligible) - len(advancing) - declining_count
    total_amount = sum((item.amount_cny for item in eligible), Decimal("0"))
    advancing_amount = sum((item.amount_cny for item in advancing), Decimal("0"))
    sorted_changes = tuple(sorted(changes))
    midpoint = len(sorted_changes) // 2
    median = (
        sorted_changes[midpoint]
        if len(sorted_changes) % 2
        else (sorted_changes[midpoint - 1] + sorted_changes[midpoint]) / Decimal("2")
    )
    close_at = datetime.combine(session_date, _MARKET_CLOSE, tzinfo=SHANGHAI)
    age = fetched_at.astimezone(SHANGHAI) - close_at
    stale = age > stale_after
    warnings = [
        "secondary public-web research snapshot; not an exchange feed and not causal evidence",
        "provider payload has no authoritative quote timestamp; same-session close is caller-gated",
        "independent expected listing count unavailable; exact universe coverage is not reported",
        "limit-up/down counts omitted until board, ST and listing-age "
        "price-limit rules are audited",
    ]
    warnings.extend(
        f"prior source {failure.source_id} failed ({failure.failure_code})"
        for failure in previous_failures
    )
    if stale:
        warnings.append(f"snapshot fetched {age.total_seconds():.0f}s after market close")
    meta = AShareBreadthMeta(
        source_id=spec.source_id,
        source_url=spec.source_url,
        observed_at=fetched_at,
        available_at=fetched_at,
        fetched_at=fetched_at,
        stale=stale,
        degraded=stale or fallback_used,
        fallback_used=fallback_used,
        warnings=tuple(warnings),
    )
    return AShareBreadthSnapshot(
        session_date=session_date,
        received_count=len(records),
        eligible_count=len(eligible),
        minimum_eligible_count=minimum_eligible_count,
        expected_count=None,
        coverage_percent=None,
        duplicate_count=duplicate_count,
        excluded_non_equity_count=excluded_non_equity_count,
        non_trading_count=non_trading_count,
        included_exchanges=exchange_counts,
        advancing_count=len(advancing),
        declining_count=declining_count,
        flat_count=flat_count,
        advance_decline_ratio=(
            None
            if declining_count == 0
            else Decimal(len(advancing)) / Decimal(declining_count)
        ),
        advancing_amount_share_percent=(
            None if total_amount == 0 else advancing_amount / total_amount * Decimal("100")
        ),
        equal_weight_mean_change_percent=(
            sum(changes, Decimal("0")) / Decimal(len(changes))
        ),
        median_change_percent=median,
        total_amount_cny=total_amount,
        limit_up_count=None,
        limit_down_count=None,
        meta=meta,
    )


def _resolve_columns(
    records: tuple[Mapping[str, Any], ...],
    spec: _SourceSpec,
) -> dict[str, str]:
    available = {str(key) for row in records for key in row}
    resolved: dict[str, str] = {}
    for canonical, aliases in spec.aliases.items():
        matches = tuple(alias for alias in aliases if alias in available)
        if len(matches) > 1:
            raise AShareBreadthPayloadError(
                f"{spec.source_id} has ambiguous columns for {canonical}"
            )
        if matches:
            resolved[canonical] = matches[0]
        elif canonical in _REQUIRED_FIELDS:
            raise AShareBreadthPayloadError(
                f"{spec.source_id} omitted required column {canonical}"
            )
    return resolved


def _classify_code(
    value: str,
    spec: _SourceSpec,
    row_number: int,
) -> tuple[str, AShareExchange] | None:
    normalized = unicodedata.normalize("NFKC", value).strip().lower()
    prefix: str | None = None
    if len(normalized) == 8 and normalized[:2] in {"sh", "sz", "bj"}:
        prefix, code = normalized[:2], normalized[2:]
    else:
        code = normalized
    if len(code) != 6 or not code.isascii() or not code.isdigit():
        raise AShareBreadthPayloadError(
            f"{spec.source_id} row {row_number} code is not a six-digit security code"
        )

    exchange: AShareExchange | None
    suffix: str
    if code.startswith(("600", "601", "603", "605", "688", "689")):
        exchange, suffix = AShareExchange.SHANGHAI, "SH"
    elif code.startswith(("000", "001", "002", "003", "300", "301")):
        exchange, suffix = AShareExchange.SHENZHEN, "SZ"
    elif code.startswith(("4", "8", "92")):
        exchange, suffix = AShareExchange.BEIJING, "BJ"
    else:
        return None
    expected_prefix = {
        AShareExchange.SHANGHAI: "sh",
        AShareExchange.SHENZHEN: "sz",
        AShareExchange.BEIJING: "bj",
    }[exchange]
    if prefix is not None and prefix != expected_prefix:
        raise AShareBreadthPayloadError(
            f"{spec.source_id} row {row_number} code prefix conflicts with security code"
        )
    return f"{code}.{suffix}", exchange


def _validate_tencent_stock_type(
    row: Mapping[str, Any],
    columns: Mapping[str, str],
    spec: _SourceSpec,
    row_number: int,
    exchange: AShareExchange,
) -> None:
    column = columns.get("stock_type")
    if column is None:
        return
    value = _required_text(row.get(column), "stock_type", spec, row_number).upper()
    valid = (
        value == "GP"
        if exchange is AShareExchange.BEIJING
        else value == "GP-A" or value.startswith("GP-A-")
    )
    if not valid:
        raise AShareBreadthPayloadError(
            f"{spec.source_id} row {row_number} code/type classification conflicts"
        )


def _required_text(
    value: Any,
    field: str,
    spec: _SourceSpec,
    row_number: int,
) -> str:
    if _is_missing(value):
        raise AShareBreadthPayloadError(
            f"{spec.source_id} row {row_number} {field} is missing"
        )
    result = unicodedata.normalize("NFKC", str(value)).strip()
    if not result:
        raise AShareBreadthPayloadError(
            f"{spec.source_id} row {row_number} {field} is blank"
        )
    return result


def _decimal(
    value: Any,
    field: str,
    spec: _SourceSpec,
    row_number: int,
) -> Decimal:
    if isinstance(value, bool) or _is_missing(value):
        raise AShareBreadthPayloadError(
            f"{spec.source_id} row {row_number} {field} is missing"
        )
    try:
        result = Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError) as exc:
        raise AShareBreadthPayloadError(
            f"{spec.source_id} row {row_number} {field} is not numeric"
        ) from exc
    if not result.is_finite():
        raise AShareBreadthPayloadError(
            f"{spec.source_id} row {row_number} {field} is not finite"
        )
    return result


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


def _validate_same_session_close(
    session_date: date,
    now: datetime,
    ready_delay: timedelta,
) -> None:
    local_now = now.astimezone(SHANGHAI)
    if local_now.date() != session_date:
        raise AShareBreadthCoverageError(
            "public breadth snapshot has no session timestamp and can only be used "
            "for the current Shanghai calendar date"
        )
    ready_at = datetime.combine(session_date, _MARKET_CLOSE, tzinfo=SHANGHAI) + ready_delay
    if local_now < ready_at:
        raise AShareBreadthCoverageError(
            f"close breadth is unavailable before {ready_at.isoformat(timespec='minutes')}"
        )


def _source_failure(
    source_id: str,
    error: AShareBreadthDataError,
) -> AShareBreadthSourceFailure:
    if isinstance(error, AShareBreadthTimeoutError):
        code = "TIMEOUT"
    elif isinstance(error, AShareBreadthEndpointError):
        code = "ENDPOINT_UNAVAILABLE"
    elif isinstance(error, AShareBreadthCoverageError):
        code = "INSUFFICIENT_COVERAGE"
    elif isinstance(error, AShareBreadthPayloadError):
        code = "INVALID_PAYLOAD"
    else:
        code = "UPSTREAM_ERROR"
    return AShareBreadthSourceFailure(source_id=source_id, failure_code=code, reason=str(error))


def _import_akshare() -> Any:
    try:
        import akshare  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - packaging concern
        raise AShareBreadthEndpointError("AKShare is not installed") from exc
    return akshare


def _aware_now(now: Callable[[], datetime]) -> datetime:
    value = now()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now() must return a timezone-aware datetime")
    return value
