"""AKShare current-session whole-market surveillance adapter."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from functools import partial
from typing import Any, TypeVar, cast
from zoneinfo import ZoneInfo

from gribuki_trade.ports.ashare_screening import AShareBoard
from gribuki_trade.ports.ashare_surveillance import (
    AShareIntradayUniverseRecord,
    AShareIntradayUniverseSnapshot,
    AShareSurveillanceDataError,
    SurveillanceSourceQuality,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")

EASTMONEY_SURVEILLANCE_SOURCE_ID = "AKShare/Eastmoney stock_zh_a_spot_em"
TENCENT_SURVEILLANCE_SOURCE_ID = "AKShare/Tencent stock_zh_a_spot_tx"

_ST_PATTERN = re.compile(r"^(?:S\*ST|SST|\*ST|ST)|退", re.IGNORECASE)
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class _SourceSpec:
    source_id: str
    operation: str
    aliases: Mapping[str, tuple[str, ...]]
    amount_multiplier: Decimal


_SOURCES = (
    _SourceSpec(
        source_id=EASTMONEY_SURVEILLANCE_SOURCE_ID,
        operation="stock_zh_a_spot_em",
        aliases={
            "code": ("代码", "code"),
            "name": ("名称", "name"),
            "last": ("最新价", "last"),
            "previous_close": ("昨收", "previous_close"),
            "open": ("今开", "open"),
            "high": ("最高", "high"),
            "low": ("最低", "low"),
            "change_percent": ("涨跌幅", "change_percent"),
            "amount": ("成交额", "amount"),
            "turnover_rate": ("换手率", "turnover_rate"),
            "volume_ratio": ("量比", "volume_ratio"),
        },
        amount_multiplier=Decimal("1"),
    ),
    _SourceSpec(
        source_id=TENCENT_SURVEILLANCE_SOURCE_ID,
        operation="stock_zh_a_spot_tx",
        aliases={
            "code": ("code",),
            "name": ("name",),
            "last": ("zxj",),
            "absolute_change": ("zd",),
            "change_percent": ("zdf",),
            "amount": ("turnover",),
            "turnover_rate": ("hsl",),
            "state": ("state",),
            "stock_type": ("stock_type",),
        },
        amount_multiplier=Decimal("10000"),
    ),
)


class AKShareAShareSurveillanceAdapter:
    """Fetch one independently bounded public-web whole-market snapshot."""

    def __init__(
        self,
        client: Any | None = None,
        *,
        timeout_seconds: float = 35.0,
        minimum_universe_count: int = 4500,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive and finite")
        if minimum_universe_count < 1:
            raise ValueError("minimum_universe_count must be positive")
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._minimum_universe_count = minimum_universe_count
        self._now = now

    async def fetch_intraday_universe(
        self,
        *,
        session_date: date,
        known_at: datetime,
    ) -> AShareIntradayUniverseSnapshot:
        _validate_request(session_date, known_at)
        client = self._client or _import_akshare()
        failures: list[str] = []
        for source_index, spec in enumerate(_SOURCES):
            try:
                payload = await _call_async(
                    partial(_provider_records, client, spec.operation),
                    timeout_seconds=self._timeout_seconds,
                )
                records = _parse_records(
                    payload,
                    spec=spec,
                    minimum_universe_count=self._minimum_universe_count,
                )
                fetched_at = _aware_utc(self._now(), "now")
                if fetched_at.astimezone(SHANGHAI).date() != session_date:
                    raise AShareSurveillanceDataError("COLLECTOR_DATE_CHANGED")
                warnings = [
                    "secondary public-web snapshot; not exchange tick, L1, or executable quote",
                    "provider has no authoritative quote timestamp; "
                    "available_at is first observation",
                    "current-session volume and amount are incomplete and time-of-day dependent",
                ]
                warnings.extend(f"PRIOR_SOURCE_FAILED:{item}" for item in failures)
                if source_index > 0:
                    warnings.append(
                        "Tencent fallback omits open/high/low and volume-ratio fields"
                    )
                return AShareIntradayUniverseSnapshot(
                    session_date=session_date,
                    available_at=fetched_at,
                    observed_at=fetched_at,
                    source_id=spec.source_id,
                    source_revision=_revision(spec.source_id, fetched_at, records),
                    records=records,
                    quality=(
                        SurveillanceSourceQuality.DEGRADED
                        if source_index > 0
                        else SurveillanceSourceQuality.COMPLETE
                    ),
                    warnings=tuple(warnings),
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                failures.append(f"{spec.operation}:{_failure_code(exc)}")
        raise AShareSurveillanceDataError("ALL_SOURCES_FAILED")


def _parse_records(
    rows: tuple[Mapping[str, Any], ...],
    *,
    spec: _SourceSpec,
    minimum_universe_count: int,
) -> tuple[AShareIntradayUniverseRecord, ...]:
    columns = _resolve_columns(rows, spec)
    records: dict[str, AShareIntradayUniverseRecord] = {}
    for row in rows:
        classified = _classify_symbol(_text(row.get(columns["code"])))
        if classified is None:
            continue
        symbol, board = classified
        name = _text(row.get(columns["name"]))
        last = _optional_decimal(row.get(columns["last"]))
        amount = _optional_decimal(row.get(columns["amount"]))
        if amount is not None:
            amount *= spec.amount_multiplier
        change_percent = _optional_decimal(row.get(columns["change_percent"]))
        previous_close = _optional_from_column(row, columns, "previous_close")
        if previous_close is None and last is not None:
            absolute_change = _optional_from_column(row, columns, "absolute_change")
            if absolute_change is not None:
                previous_close = last - absolute_change
            elif change_percent is not None and change_percent != Decimal("-100"):
                denominator = Decimal("1") + change_percent / Decimal("100")
                if denominator > 0:
                    previous_close = last / denominator
        state = _optional_text(row.get(columns["state"])) if "state" in columns else None
        is_suspended = _suspension_status(last, amount, state)
        record = AShareIntradayUniverseRecord(
            symbol=symbol,
            name=name,
            board=board,
            is_st=bool(_ST_PATTERN.search(name)),
            is_suspended=is_suspended,
            last_price=last,
            previous_close=previous_close,
            open_price=_optional_from_column(row, columns, "open"),
            high_price=_optional_from_column(row, columns, "high"),
            low_price=_optional_from_column(row, columns, "low"),
            change_percent=change_percent,
            session_amount_cny=amount,
            turnover_rate_percent=_optional_from_column(
                row, columns, "turnover_rate"
            ),
            volume_ratio=_optional_from_column(row, columns, "volume_ratio"),
        )
        previous = records.get(symbol)
        if previous is not None and previous != record:
            raise AShareSurveillanceDataError("CONFLICTING_DUPLICATE_SYMBOL")
        records[symbol] = record
    if len(records) < minimum_universe_count:
        raise AShareSurveillanceDataError("INCOMPLETE_UNIVERSE")
    return tuple(records[symbol] for symbol in sorted(records))


def _resolve_columns(
    rows: tuple[Mapping[str, Any], ...], spec: _SourceSpec
) -> dict[str, str]:
    available = {str(key) for row in rows for key in row}
    resolved: dict[str, str] = {}
    required = {"code", "name", "last", "change_percent", "amount"}
    for canonical, aliases in spec.aliases.items():
        matches = tuple(alias for alias in aliases if alias in available)
        if len(matches) > 1:
            raise AShareSurveillanceDataError("AMBIGUOUS_SCHEMA")
        if matches:
            resolved[canonical] = matches[0]
        elif canonical in required:
            raise AShareSurveillanceDataError("MISSING_REQUIRED_FIELD")
    return resolved


def _provider_records(client: Any, operation: str) -> tuple[Mapping[str, Any], ...]:
    method = getattr(client, operation, None)
    if method is None or not callable(method):
        raise AShareSurveillanceDataError("ENDPOINT_UNAVAILABLE")
    payload = method()
    if payload is None or not hasattr(payload, "to_dict"):
        raise AShareSurveillanceDataError("INVALID_PAYLOAD")
    try:
        rows = payload.to_dict(orient="records")
    except (TypeError, ValueError, AttributeError):
        raise AShareSurveillanceDataError("INVALID_PAYLOAD") from None
    if not isinstance(rows, Sequence) or not rows:
        raise AShareSurveillanceDataError("EMPTY_PAYLOAD")
    if any(not isinstance(row, Mapping) for row in rows):
        raise AShareSurveillanceDataError("INVALID_PAYLOAD")
    return tuple(cast(Mapping[str, Any], row) for row in rows)


async def _call_async(
    call: Callable[[], _T], *, timeout_seconds: float
) -> _T:
    try:
        return await asyncio.wait_for(asyncio.to_thread(call), timeout=timeout_seconds)
    except TimeoutError:
        raise AShareSurveillanceDataError("SOURCE_TIMEOUT") from None


def _classify_symbol(code_value: str) -> tuple[str, AShareBoard] | None:
    normalized = code_value.lower()
    prefix = normalized[:2] if normalized[:2] in {"sh", "sz", "bj"} else None
    code = normalized[2:] if prefix is not None else normalized
    if len(code) != 6 or not code.isascii() or not code.isdigit():
        raise AShareSurveillanceDataError("INVALID_SYMBOL")
    if code.startswith(("600", "601", "603", "605")):
        board, exchange = AShareBoard.SSE_MAIN, "SH"
    elif code.startswith(("688", "689")):
        board, exchange = AShareBoard.STAR, "SH"
    elif code.startswith(("000", "001", "002", "003")):
        board, exchange = AShareBoard.SZSE_MAIN, "SZ"
    elif code.startswith(("300", "301")):
        board, exchange = AShareBoard.CHINEXT, "SZ"
    elif code.startswith(("4", "8", "92")):
        board, exchange = AShareBoard.BSE, "BJ"
    else:
        return None
    if prefix is not None and prefix.upper() != exchange:
        raise AShareSurveillanceDataError("SYMBOL_EXCHANGE_CONFLICT")
    return f"{code}.{exchange}", board


def _suspension_status(
    last: Decimal | None, amount: Decimal | None, state: str | None
) -> bool | None:
    if state is not None:
        normalized = state.strip().casefold()
        if normalized in {"正常", "交易", "active", "0", "1"}:
            return False
        if normalized in {"停牌", "suspended", "2", "3"}:
            return True
    if last is None:
        return True if amount in {None, Decimal("0")} else None
    return False if last > 0 and amount is not None else None


def _optional_from_column(
    row: Mapping[str, Any], columns: Mapping[str, str], canonical: str
) -> Decimal | None:
    column = columns.get(canonical)
    return None if column is None else _optional_decimal(row.get(column))


def _text(value: object) -> str:
    result = unicodedata.normalize("NFKC", str(value)).strip()
    if not result or result.casefold() in {"none", "nan"}:
        raise AShareSurveillanceDataError("MISSING_TEXT_FIELD")
    return result


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    result = unicodedata.normalize("NFKC", str(value)).strip()
    return result or None


def _optional_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip().replace(",", "")
    if text.casefold() in {"", "-", "--", "nan", "none", "null"}:
        return None
    try:
        result = Decimal(text)
    except (InvalidOperation, ValueError):
        raise AShareSurveillanceDataError("NON_NUMERIC_FIELD") from None
    if not result.is_finite():
        raise AShareSurveillanceDataError("NON_FINITE_FIELD")
    return result


def _validate_request(session_date: date, known_at: datetime) -> None:
    known = _aware_utc(known_at, "known_at").astimezone(SHANGHAI)
    if known.date() != session_date:
        raise AShareSurveillanceDataError("SESSION_DATE_MISMATCH")
    current = known.timetz().replace(tzinfo=None)
    if not (time(9, 30) <= current <= time(11, 30) or time(13) <= current < time(15)):
        raise AShareSurveillanceDataError("MARKET_NOT_OPEN")


def _revision(
    source_id: str,
    fetched_at: datetime,
    records: tuple[AShareIntradayUniverseRecord, ...],
) -> str:
    payload = {
        "source_id": source_id,
        "fetched_at": fetched_at.isoformat(),
        "records": [
            {
                "symbol": item.symbol,
                "last": _decimal_text(item.last_price),
                "change": _decimal_text(item.change_percent),
                "amount": _decimal_text(item.session_amount_cny),
            }
            for item in records
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value.normalize())


def _failure_code(error: Exception) -> str:
    if isinstance(error, AShareSurveillanceDataError):
        return error.code
    return "UPSTREAM_ERROR"


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _import_akshare() -> Any:
    try:
        import akshare  # type: ignore[import-untyped]
    except ImportError:
        raise AShareSurveillanceDataError("AKSHARE_NOT_INSTALLED") from None
    return akshare
