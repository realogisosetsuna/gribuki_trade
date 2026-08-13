"""AKShare inputs for the deterministic three-layer A-share screener.

The adapter deliberately keeps the two expensive boundaries separate:

* one current-session, complete-market quote snapshot for cheap filtering;
* unadjusted per-symbol daily histories only for the bounded L1 survivors.

Public-web endpoints do not expose an authoritative payload timestamp.  A
same-session close snapshot is therefore usable only after 15:05 Shanghai time
and records that conservative time as ``available_at``.  ``observed_at`` is the
actual completion time and is never replaced with the caller's ``known_at``.
The live endpoints cannot replay a historical universe; historical backtests
must read an archived revision instead.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from functools import partial
from statistics import stdev
from typing import Any, TypeVar, cast
from zoneinfo import ZoneInfo

from gribuki_trade.ports.ashare_screening import (
    AShareBoard,
    AShareFactorRecord,
    AShareFactorSnapshot,
    AShareFactorValue,
    AShareUniverseRecord,
    AShareUniverseSnapshot,
    ScreeningFactorId,
    ScreeningHistoryPolicy,
    ScreeningSourceQuality,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")

EASTMONEY_SCREENING_SOURCE_ID = "AKShare/Eastmoney stock_zh_a_spot_em"
TENCENT_SCREENING_SOURCE_ID = "AKShare/Tencent stock_zh_a_spot_tx"
AKSHARE_HISTORY_SOURCE_ID = "AKShare/Eastmoney stock_zh_a_hist adjust=NONE"

_CLOSE_READY_TIME = time(15, 5)
_DEFAULT_MINIMUM_UNIVERSE = 4500
_DEFAULT_HISTORY_SESSIONS = 201
_DEFAULT_MAX_FACTOR_SYMBOLS = 300
_DEFAULT_HISTORY_CONCURRENCY = 4
_DEFAULT_HISTORY_CALENDAR_DAYS = 430
_FEATURE_VERSION = "ashare-screening-raw-factors@1"
_CORPORATE_ACTION_TOLERANCE = Decimal("0.02")
_MIN_CORPORATE_ACTION_COVERAGE = Decimal("0.95")

_ST_NAME_PATTERN = re.compile(r"^(?:S\*ST|SST|\*ST|ST)|退", re.IGNORECASE)

_EASTMONEY_ALIASES: Mapping[str, tuple[str, ...]] = {
    "code": ("代码", "code"),
    "name": ("名称", "name"),
    "last": ("最新价", "last"),
    "amount": ("成交额", "amount"),
    "market_cap": ("总市值", "market_cap"),
}
_TENCENT_ALIASES: Mapping[str, tuple[str, ...]] = {
    "code": ("code",),
    "name": ("name",),
    "last": ("zxj",),
    "amount": ("turnover",),
    "market_cap": ("zsz",),
    "state": ("state",),
    "stock_type": ("stock_type",),
}
_HISTORY_ALIASES: Mapping[str, tuple[str, ...]] = {
    "date": ("日期", "date", "trade_date"),
    "open": ("开盘", "open"),
    "close": ("收盘", "close"),
    "high": ("最高", "high"),
    "low": ("最低", "low"),
    "volume": ("成交量", "volume"),
    "amount": ("成交额", "amount", "turnover"),
    "change": ("涨跌额", "change", "change_amount"),
    "previous_close": ("昨收", "preclose", "previous_close"),
}


class AKShareScreeningDataError(RuntimeError):
    """Base failure for the public screening inputs."""


class AKShareScreeningPointInTimeError(AKShareScreeningDataError):
    """A live endpoint was requested outside its defensible time boundary."""


class AKShareScreeningPayloadError(AKShareScreeningDataError):
    """A provider response did not meet the explicit schema or unit contract."""


class AKShareScreeningCoverageError(AKShareScreeningDataError):
    """A full-market provider returned an implausibly small universe."""


class AKShareScreeningSourcesExhaustedError(AKShareScreeningDataError):
    """Neither independent full-market source produced an acceptable snapshot."""

    def __init__(self, failures: tuple[str, ...]) -> None:
        self.failures = failures
        super().__init__("all A-share universe sources failed: " + "; ".join(failures))


@dataclass(frozen=True, slots=True)
class _SourceSpec:
    source_id: str
    operation: str
    aliases: Mapping[str, tuple[str, ...]]
    amount_multiplier: Decimal
    market_cap_multiplier: Decimal


@dataclass(frozen=True, slots=True)
class _ListingMetadata:
    listing_date: date
    industry: str | None


@dataclass(frozen=True, slots=True)
class _HistoryBar:
    trade_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    previous_close: Decimal | None
    volume: Decimal
    amount: Decimal


_UNIVERSE_SOURCES = (
    _SourceSpec(
        source_id=EASTMONEY_SCREENING_SOURCE_ID,
        operation="stock_zh_a_spot_em",
        aliases=_EASTMONEY_ALIASES,
        amount_multiplier=Decimal("1"),
        market_cap_multiplier=Decimal("1"),
    ),
    _SourceSpec(
        source_id=TENCENT_SCREENING_SOURCE_ID,
        operation="stock_zh_a_spot_tx",
        aliases=_TENCENT_ALIASES,
        # Tencent ``turnover`` is CNY 10,000 and ``zsz`` is CNY 100 million.
        amount_multiplier=Decimal("10000"),
        market_cap_multiplier=Decimal("100000000"),
    ),
)

_T = TypeVar("_T")


class AKShareAShareScreeningAdapter:
    """Current-close universe plus bounded unadjusted-history factor adapter."""

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
        """Fetch a post-close whole-market revision with an independent fallback."""

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
        """Fetch unadjusted histories for at most the service's explicit budget."""

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
        quality = (
            ScreeningSourceQuality.DEGRADED
            if failed
            else ScreeningSourceQuality.COMPLETE
        )
        return AShareFactorSnapshot(
            as_of=as_of,
            available_at=available_at,
            observed_at=observed_at,
            source_id=AKSHARE_HISTORY_SOURCE_ID,
            source_revision=_factor_revision(as_of, records),
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
            return AShareFactorRecord(
                symbol=symbol,
                values=empty,
                warnings=(f"HISTORY_FETCH_FAILED:{type(exc).__name__}",),
            )
        if not bars or bars[-1].trade_date != as_of:
            return AShareFactorRecord(
                symbol=symbol,
                values=empty,
                warnings=("HISTORY_LATEST_SESSION_MISMATCH",),
            )
        average_amount = _average_amount_20(bars)
        if len(bars) < self._minimum_history_sessions:
            return AShareFactorRecord(
                symbol=symbol,
                values=_factor_values_with_average_amount(average_amount),
                warnings=(
                    f"INSUFFICIENT_HISTORY:{len(bars)}/{self._minimum_history_sessions}",
                ),
            )
        guard_warning = _corporate_action_guard(bars[-self._minimum_history_sessions :])
        if guard_warning is not None:
            return AShareFactorRecord(
                symbol=symbol,
                values=_factor_values_with_average_amount(average_amount),
                warnings=(guard_warning,),
            )
        return AShareFactorRecord(
            symbol=symbol,
            values=_calculate_factors(bars),
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


def _provider_records(
    client: Any,
    operation: str,
    **kwargs: object,
) -> tuple[Mapping[str, Any], ...]:
    method = getattr(client, operation, None)
    if method is None or not callable(method):
        raise AKShareScreeningDataError(f"AKShare has no callable {operation}")
    try:
        payload = method(**kwargs)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as exc:
        raise AKShareScreeningDataError(f"AKShare {operation} failed") from exc
    if hasattr(payload, "columns") and hasattr(payload, "to_dict"):
        columns = tuple(str(item) for item in payload.columns)
        if len(columns) != len(set(columns)):
            raise AKShareScreeningPayloadError(f"{operation} has duplicate columns")
        try:
            raw = payload.to_dict(orient="records")
        except (TypeError, ValueError, AttributeError) as exc:
            raise AKShareScreeningPayloadError(
                f"{operation} returned unreadable tabular data"
            ) from exc
    elif isinstance(payload, Sequence) and not isinstance(
        payload, (str, bytes, bytearray)
    ):
        raw = payload
    else:
        raise AKShareScreeningPayloadError(f"{operation} payload is not tabular")
    if not raw or any(not isinstance(item, Mapping) for item in raw):
        raise AKShareScreeningPayloadError(f"{operation} returned no valid rows")
    return tuple(cast(Mapping[str, Any], item) for item in raw)


def _parse_universe_rows(
    rows: tuple[Mapping[str, Any], ...],
    *,
    spec: _SourceSpec,
    as_of: date,
    metadata: Mapping[str, _ListingMetadata],
    minimum_count: int,
) -> tuple[AShareUniverseRecord, ...]:
    columns = _resolve_columns(rows, spec.aliases, required=tuple(spec.aliases))
    indexed: dict[str, AShareUniverseRecord] = {}
    for row_number, row in enumerate(rows, start=1):
        code = _required_text(row.get(columns["code"]), "code", row_number)
        classified = _classify_symbol(code)
        if classified is None:
            continue
        symbol, board = classified
        name = _required_text(row.get(columns["name"]), "name", row_number)
        _validate_tencent_type(row, columns, board, row_number)
        state = _optional_text(row.get(columns["state"])) if "state" in columns else None
        suspended: bool | None = state == "S"
        last = _optional_decimal(row.get(columns["last"]), "last", row_number)
        amount = _optional_decimal(row.get(columns["amount"]), "amount", row_number)
        market_cap = _optional_decimal(
            row.get(columns["market_cap"]), "market_cap", row_number
        )
        if suspended:
            is_tradable = False
        elif last is None and amount is None:
            suspended = True
            is_tradable = False
        elif last is None or amount is None:
            raise AKShareScreeningPayloadError(
                f"row {row_number} has partially missing price/amount"
            )
        elif state is None and amount == 0:
            # Eastmoney has no explicit suspension column.  A zero-turnover
            # row with a retained reference price cannot prove whether orders
            # were accepted, so preserve the unknown state for the fail-closed
            # hard filter instead of guessing ``False``.
            suspended = None
            is_tradable = None
        else:
            is_tradable = True
        if amount is not None:
            amount *= spec.amount_multiplier
        if market_cap is not None:
            market_cap *= spec.market_cap_multiplier
        item = metadata.get(symbol)
        listing_days = None if item is None else (as_of - item.listing_date).days
        industry = None if item is None else item.industry
        record = AShareUniverseRecord(
            symbol=symbol,
            name=name,
            board=board,
            industry=industry,
            listing_days=listing_days,
            is_tradable=is_tradable,
            is_st=bool(_ST_NAME_PATTERN.search(_normalize_text(name))),
            is_suspended=suspended,
            last_price=last,
            session_amount_cny=amount,
            market_cap_cny=market_cap,
        )
        previous = indexed.get(symbol)
        if previous is None:
            indexed[symbol] = record
        elif previous != record:
            raise AKShareScreeningPayloadError(
                f"conflicting duplicate universe row for {symbol}"
            )
    records = tuple(indexed[symbol] for symbol in sorted(indexed))
    if len(records) < minimum_count:
        raise AKShareScreeningCoverageError(
            f"{spec.source_id} classified {len(records)} A shares; required {minimum_count}"
        )
    exchanges = {record.symbol[-2:] for record in records}
    if exchanges != {"SH", "SZ", "BJ"}:
        raise AKShareScreeningCoverageError(
            f"{spec.source_id} omitted an exchange: {sorted(exchanges)}"
        )
    return records


def _parse_listing_metadata(
    label: str,
    rows: tuple[Mapping[str, Any], ...],
    as_of: date,
) -> dict[str, _ListingMetadata]:
    aliases: Mapping[str, tuple[str, ...]]
    if label.startswith("SSE"):
        aliases = {
            "code": ("证券代码", "A股代码", "code"),
            "listing_date": ("上市日期", "A股上市日期", "listing_date"),
            "industry": ("所属行业", "industry"),
        }
        suffix = "SH"
    elif label == "SZSE":
        aliases = {
            "code": ("A股代码", "证券代码", "code"),
            "listing_date": ("A股上市日期", "上市日期", "listing_date"),
            "industry": ("所属行业", "industry"),
        }
        suffix = "SZ"
    else:
        aliases = {
            "code": ("证券代码", "code"),
            "listing_date": ("上市日期", "listing_date"),
            "industry": ("所属行业", "industry"),
        }
        suffix = "BJ"
    columns = _resolve_columns(rows, aliases, required=("code", "listing_date"))
    output: dict[str, _ListingMetadata] = {}
    for row_number, row in enumerate(rows, start=1):
        code = _normalize_code(row.get(columns["code"]))
        if code is None:
            continue
        symbol = f"{code}.{suffix}"
        classified = _classify_symbol(symbol)
        if classified is None or classified[0] != symbol:
            continue
        listing_date = _date_value(row.get(columns["listing_date"]), row_number)
        if listing_date > as_of:
            continue
        industry = (
            _optional_text(row.get(columns["industry"]))
            if "industry" in columns
            else None
        )
        item = _ListingMetadata(listing_date=listing_date, industry=industry)
        if symbol in output and output[symbol] != item:
            raise AKShareScreeningPayloadError(
                f"conflicting listing metadata for {symbol}"
            )
        output[symbol] = item
    if not output:
        raise AKShareScreeningPayloadError(f"{label} metadata returned no valid rows")
    return output


def _parse_history(
    rows: tuple[Mapping[str, Any], ...],
    *,
    as_of: date,
) -> tuple[_HistoryBar, ...]:
    columns = _resolve_columns(
        rows,
        _HISTORY_ALIASES,
        required=("date", "open", "close", "high", "low", "volume", "amount"),
    )
    bars: list[_HistoryBar] = []
    for row_number, row in enumerate(rows, start=1):
        trade_date = _date_value(row.get(columns["date"]), row_number)
        if trade_date > as_of:
            raise AKShareScreeningPayloadError("history includes a future date")
        open_price = _required_positive_decimal(row.get(columns["open"]), "open", row_number)
        close = _required_positive_decimal(row.get(columns["close"]), "close", row_number)
        high = _required_positive_decimal(row.get(columns["high"]), "high", row_number)
        low = _required_positive_decimal(row.get(columns["low"]), "low", row_number)
        if high < max(open_price, close, low) or low > min(open_price, close, high):
            raise AKShareScreeningPayloadError(f"row {row_number} has invalid OHLC")
        volume = _required_non_negative_decimal(
            row.get(columns["volume"]), "volume", row_number
        )
        amount = _required_non_negative_decimal(
            row.get(columns["amount"]), "amount", row_number
        )
        previous_close = (
            _optional_decimal(
                row.get(columns["previous_close"]), "previous_close", row_number
            )
            if "previous_close" in columns
            else None
        )
        if previous_close is None and "change" in columns:
            change = _optional_decimal(row.get(columns["change"]), "change", row_number)
            if change is not None:
                previous_close = close - change
        if previous_close is not None and previous_close <= 0:
            raise AKShareScreeningPayloadError("previous_close must be positive")
        bars.append(
            _HistoryBar(
                trade_date=trade_date,
                open=open_price,
                high=high,
                low=low,
                close=close,
                previous_close=previous_close,
                volume=volume,
                amount=amount,
            )
        )
    ordered = tuple(sorted(bars, key=lambda item: item.trade_date))
    dates = tuple(item.trade_date for item in ordered)
    if len(dates) != len(set(dates)):
        raise AKShareScreeningPayloadError("history contains duplicate dates")
    return ordered


def _calculate_factors(bars: tuple[_HistoryBar, ...]) -> tuple[AShareFactorValue, ...]:
    """Calculate raw, unstandardized factors from completed sessions.

    Momentum is a simple close-to-close return.  The 120-session variant ends
    five sessions before ``as_of``.  Trend is ``MA20 / MA60 - 1``; breakout
    position is ``latest close / prior-20-session high - 1``; volume ratio is
    latest volume divided by the prior-20 mean.  Volatility is the sample
    standard deviation of 60 simple returns annualized by ``sqrt(252)``.
    Drawdown is a positive loss magnitude over 60 closes.  Amihud is the
    20-session mean of ``abs(return) / amount_cny`` and is absent unless every
    amount is positive.  No missing result is replaced with a neutral value.
    """

    closes = tuple(item.close for item in bars)
    current = closes[-1]
    momentum_20 = current / closes[-21] - Decimal(1)
    momentum_60 = current / closes[-61] - Decimal(1)
    momentum_120_skip_5 = closes[-6] / closes[-126] - Decimal(1)
    ma20 = _mean(closes[-20:])
    ma60 = _mean(closes[-60:])
    prior20 = bars[-21:-1]
    prior_high = max(item.high for item in prior20)
    breakout_position = current / prior_high - Decimal(1)
    prior_volume = _mean(tuple(item.volume for item in prior20))
    volume_ratio = None if prior_volume == 0 else bars[-1].volume / prior_volume
    returns_60 = _returns(closes[-61:])
    volatility = Decimal(str(stdev(float(item) for item in returns_60))) * Decimal(
        str(math.sqrt(252))
    )
    max_drawdown = _max_drawdown_magnitude(closes[-60:])
    amihud_values = tuple(
        abs(current_close / previous_close - Decimal(1)) / bar.amount
        for previous_close, current_close, bar in zip(
            closes[-21:-1], closes[-20:], bars[-20:], strict=True
        )
        if bar.amount > 0
    )
    amihud = _mean(amihud_values) if len(amihud_values) == 20 else None
    average_amount = _average_amount_20(bars)
    values: Mapping[ScreeningFactorId, Decimal | None] = {
        ScreeningFactorId.MOMENTUM_20: momentum_20,
        ScreeningFactorId.MOMENTUM_60: momentum_60,
        ScreeningFactorId.MOMENTUM_120_SKIP_5: momentum_120_skip_5,
        ScreeningFactorId.TREND_MA20_OVER_MA60: ma20 / ma60 - Decimal(1),
        ScreeningFactorId.BREAKOUT_20_POSITION: breakout_position,
        ScreeningFactorId.VOLUME_RATIO_20: volume_ratio,
        ScreeningFactorId.ANNUALIZED_VOLATILITY_60: volatility,
        ScreeningFactorId.MAX_DRAWDOWN_60_MAGNITUDE: max_drawdown,
        ScreeningFactorId.AMIHUD_ILLIQUIDITY_20: amihud,
        ScreeningFactorId.AVERAGE_AMOUNT_20_CNY: average_amount,
    }
    return tuple(
        AShareFactorValue(
            factor_id=factor_id,
            value=None if value is None else float(value),
        )
        for factor_id, value in values.items()
    )


def _corporate_action_guard(bars: tuple[_HistoryBar, ...]) -> str | None:
    comparable = 0
    for previous, current in zip(bars, bars[1:], strict=False):
        if current.previous_close is None:
            continue
        comparable += 1
        discontinuity = abs(current.previous_close / previous.close - Decimal(1))
        if discontinuity > _CORPORATE_ACTION_TOLERANCE:
            return f"CORPORATE_ACTION_DISCONTINUITY:{current.trade_date.isoformat()}"
    possible = max(1, len(bars) - 1)
    coverage = Decimal(comparable) / Decimal(possible)
    if coverage < _MIN_CORPORATE_ACTION_COVERAGE:
        return f"CORPORATE_ACTION_GUARD_INCOMPLETE:{coverage:.3f}"
    return None


def _empty_factor_values() -> tuple[AShareFactorValue, ...]:
    return tuple(AShareFactorValue(factor_id=item, value=None) for item in ScreeningFactorId)


def _factor_values_with_average_amount(
    average_amount: Decimal | None,
) -> tuple[AShareFactorValue, ...]:
    return tuple(
        AShareFactorValue(
            factor_id=item,
            value=(
                float(average_amount)
                if item is ScreeningFactorId.AVERAGE_AMOUNT_20_CNY
                and average_amount is not None
                else None
            ),
        )
        for item in ScreeningFactorId
    )


def _average_amount_20(bars: tuple[_HistoryBar, ...]) -> Decimal | None:
    if len(bars) < 20:
        return None
    return _mean(tuple(item.amount for item in bars[-20:]))


def _returns(closes: tuple[Decimal, ...]) -> tuple[Decimal, ...]:
    return tuple(
        current / previous - Decimal(1)
        for previous, current in zip(closes, closes[1:], strict=False)
    )


def _max_drawdown_magnitude(closes: tuple[Decimal, ...]) -> Decimal:
    peak = closes[0]
    largest = Decimal(0)
    for close in closes:
        peak = max(peak, close)
        largest = max(largest, Decimal(1) - close / peak)
    return largest


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def _resolve_columns(
    rows: tuple[Mapping[str, Any], ...],
    aliases: Mapping[str, tuple[str, ...]],
    *,
    required: tuple[str, ...],
) -> dict[str, str]:
    available = {str(key) for row in rows for key in row}
    resolved: dict[str, str] = {}
    for canonical, candidates in aliases.items():
        matches = tuple(item for item in candidates if item in available)
        if len(matches) > 1:
            raise AKShareScreeningPayloadError(
                f"ambiguous columns for {canonical}: {matches}"
            )
        if matches:
            resolved[canonical] = matches[0]
        elif canonical in required:
            raise AKShareScreeningPayloadError(f"missing required column {canonical}")
    return resolved


def _classify_symbol(raw: str) -> tuple[str, AShareBoard] | None:
    normalized = _normalize_text(raw).lower()
    prefix: str | None = None
    if len(normalized) == 8 and normalized[:2] in {"sh", "sz", "bj"}:
        prefix, code = normalized[:2], normalized[2:]
    elif len(normalized) == 9 and normalized[6] == ".":
        code, suffix = normalized.split(".", maxsplit=1)
        prefix = suffix
    else:
        code = normalized
    if len(code) != 6 or not code.isascii() or not code.isdigit():
        raise AKShareScreeningPayloadError(f"invalid security code {raw!r}")
    if code.startswith(("688", "689")):
        board, expected = AShareBoard.STAR, "sh"
    elif code.startswith(("600", "601", "603", "605", "609")):
        board, expected = AShareBoard.SSE_MAIN, "sh"
    elif code.startswith(("300", "301")):
        board, expected = AShareBoard.CHINEXT, "sz"
    elif code.startswith(("000", "001", "002", "003")):
        board, expected = AShareBoard.SZSE_MAIN, "sz"
    elif code.startswith(("4", "8", "92")):
        board, expected = AShareBoard.BSE, "bj"
    else:
        return None
    if prefix is not None and prefix != expected:
        raise AKShareScreeningPayloadError(
            f"security code prefix conflicts with code family: {raw!r}"
        )
    return f"{code}.{expected.upper()}", board


def _canonical_symbol(value: str) -> str:
    classified = _classify_symbol(value)
    if classified is None:
        raise ValueError(f"unsupported A-share symbol: {value!r}")
    return classified[0]


def _validate_tencent_type(
    row: Mapping[str, Any],
    columns: Mapping[str, str],
    board: AShareBoard,
    row_number: int,
) -> None:
    column = columns.get("stock_type")
    if column is None:
        return
    stock_type = _required_text(row.get(column), "stock_type", row_number).upper()
    expected = {
        AShareBoard.SSE_MAIN: "GP-A",
        AShareBoard.SZSE_MAIN: "GP-A",
        AShareBoard.CHINEXT: "GP-A-CYB",
        AShareBoard.STAR: "GP-A-KCB",
        AShareBoard.BSE: "GP",
    }[board]
    if stock_type != expected:
        raise AKShareScreeningPayloadError(
            f"row {row_number} stock_type {stock_type!r} conflicts with board"
        )


def _normalize_code(value: Any) -> str | None:
    if _is_missing(value):
        return None
    text = _normalize_text(value)
    if text.endswith(".0"):
        text = text[:-2]
    text = text.zfill(6)
    return text if len(text) == 6 and text.isascii() and text.isdigit() else None


def _date_value(value: Any, row_number: int) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if _is_missing(value):
        raise AKShareScreeningPayloadError(f"row {row_number} date is missing")
    text = _normalize_text(value)
    for pattern in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            pass
    raise AKShareScreeningPayloadError(f"row {row_number} date is invalid")


def _required_text(value: Any, field: str, row_number: int) -> str:
    if _is_missing(value):
        raise AKShareScreeningPayloadError(f"row {row_number} {field} is missing")
    text = _normalize_text(value)
    if not text:
        raise AKShareScreeningPayloadError(f"row {row_number} {field} is blank")
    return text


def _optional_text(value: Any) -> str | None:
    return None if _is_missing(value) else _normalize_text(value)


def _optional_decimal(value: Any, field: str, row_number: int) -> Decimal | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        raise AKShareScreeningPayloadError(f"row {row_number} {field} is boolean")
    try:
        result = Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError) as exc:
        raise AKShareScreeningPayloadError(
            f"row {row_number} {field} is not numeric"
        ) from exc
    if not result.is_finite():
        raise AKShareScreeningPayloadError(f"row {row_number} {field} is not finite")
    return result


def _required_positive_decimal(value: Any, field: str, row_number: int) -> Decimal:
    result = _optional_decimal(value, field, row_number)
    if result is None or result <= 0:
        raise AKShareScreeningPayloadError(
            f"row {row_number} {field} must be positive"
        )
    return result


def _required_non_negative_decimal(value: Any, field: str, row_number: int) -> Decimal:
    result = _optional_decimal(value, field, row_number)
    if result is None or result < 0:
        raise AKShareScreeningPayloadError(
            f"row {row_number} {field} must be non-negative"
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


def _normalize_text(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value)).strip()


def _universe_revision(
    source_id: str,
    as_of: date,
    records: tuple[AShareUniverseRecord, ...],
) -> str:
    document = {
        "source_id": source_id,
        "as_of": as_of.isoformat(),
        "records": [
            {
                "symbol": item.symbol,
                "name": item.name,
                "board": item.board.value,
                "industry": item.industry,
                "listing_days": item.listing_days,
                "is_tradable": item.is_tradable,
                "is_st": item.is_st,
                "is_suspended": item.is_suspended,
                "last_price": _decimal_text(item.last_price),
                "session_amount_cny": _decimal_text(item.session_amount_cny),
                "market_cap_cny": _decimal_text(item.market_cap_cny),
            }
            for item in records
        ],
    }
    return _sha256_document(document)


def _factor_revision(as_of: date, records: tuple[AShareFactorRecord, ...]) -> str:
    document = {
        "source_id": AKSHARE_HISTORY_SOURCE_ID,
        "feature_version": _FEATURE_VERSION,
        "as_of": as_of.isoformat(),
        "records": [
            {
                "symbol": item.symbol,
                "values": [
                    {"factor_id": value.factor_id.value, "value": value.value}
                    for value in item.values
                ],
                "warnings": list(item.warnings),
            }
            for item in records
        ],
    }
    return _sha256_document(document)


def _sha256_document(document: object) -> str:
    payload = json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


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
