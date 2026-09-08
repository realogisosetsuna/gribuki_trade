"""AKShare 当前交易日全市场监控适配器。"""

from __future__ import annotations

import asyncio
import concurrent.futures
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
from typing import Any, TypeVar, cast
from zoneinfo import ZoneInfo

import httpx

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
TENCENT_ENRICHED_SURVEILLANCE_SOURCE_ID = (
    "AKShare/Tencent stock_zh_a_spot_tx + Tencent qt.gtimg.cn bulk quote"
)

_ST_PATTERN = re.compile(r"^(?:S\*ST|SST|\*ST|ST)|退", re.IGNORECASE)
_TENCENT_QUOTE_LINE = re.compile(r'^v_([a-z]{2}\d{6})="(.*)"$')
_TENCENT_QUOTE_CHUNK_SIZE = 100
_TENCENT_QUOTE_WORKERS = 6
_TENCENT_PRICE_DIVERGENCE_LIMIT = Decimal("0.015")
_TENCENT_AMOUNT_LAG_LIMIT = Decimal("0.05")
_TENCENT_AMOUNT_ROUNDING_CNY = Decimal("100000")
_TENCENT_AMOUNT_LEAD_LIMIT = Decimal("0.50")
_TENCENT_AMOUNT_LEAD_FLOOR_CNY = Decimal("5000000")
_TENCENT_CHANGE_PERCENT_TOLERANCE = Decimal("0.05")
_TENCENT_QUOTE_MAX_AGE = timedelta(minutes=3)
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
            "volume_ratio": ("lb",),
            "state": ("state",),
            "stock_type": ("stock_type",),
        },
        amount_multiplier=Decimal("10000"),
    ),
)


class AKShareAShareSurveillanceAdapter:
    """获取一份具有独立时限的公开网页全市场快照。"""

    def __init__(
        self,
        client: Any | None = None,
        *,
        timeout_seconds: float = 35.0,
        minimum_universe_count: int = 4500,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        tencent_quote_fetcher: (
            Callable[[tuple[str, ...]], tuple[Mapping[str, Any], ...]] | None
        ) = None,
    ) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive and finite")
        if minimum_universe_count < 1:
            raise ValueError("minimum_universe_count must be positive")
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._minimum_universe_count = minimum_universe_count
        self._now = now
        self._tencent_quote_fetcher: (
            Callable[[tuple[str, ...]], tuple[Mapping[str, Any], ...]] | None
        )
        # 提供 AKShare 客户端是适配器的测试/离线接缝。不得通过实时 HTTP 静默
        # 绕过该接缝；调用方需要演练补充数据时，可注入确定性报价获取器。
        if tencent_quote_fetcher is not None:
            self._tencent_quote_fetcher = tencent_quote_fetcher
        elif client is None:
            request_timeout = min(max(timeout_seconds / 3, 1.0), 10.0)
            self._tencent_quote_fetcher = partial(
                _tencent_bulk_quote_records,
                request_timeout_seconds=request_timeout,
            )
        else:
            self._tencent_quote_fetcher = None

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
                if source_index > 0:
                    enriched = await self._try_enrich_tencent(
                        records=records,
                        session_date=session_date,
                        failures=failures,
                    )
                    if enriched is not None:
                        return enriched
                    warnings.append(
                        "Tencent board-only fallback omits open/high/low fields"
                    )
                warnings.extend(f"PRIOR_SOURCE_FAILED:{item}" for item in failures)
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

    async def _try_enrich_tencent(
        self,
        *,
        records: tuple[AShareIntradayUniverseRecord, ...],
        session_date: date,
        failures: list[str],
    ) -> AShareIntradayUniverseSnapshot | None:
        fetcher = self._tencent_quote_fetcher
        if fetcher is None:
            return None
        provider_codes = tuple(_tencent_provider_code(item.symbol) for item in records)
        try:
            quote_rows = await _call_async(
                partial(fetcher, provider_codes),
                timeout_seconds=self._timeout_seconds,
            )
            completed_at = _aware_utc(self._now(), "now")
            if completed_at.astimezone(SHANGHAI).date() != session_date:
                raise AShareSurveillanceDataError("COLLECTOR_DATE_CHANGED")
            enriched = _compose_tencent_enrichment(
                board_records=records,
                quote_rows=quote_rows,
                session_date=session_date,
                fetched_at=completed_at,
                minimum_universe_count=self._minimum_universe_count,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            failures.append(f"tencent_bulk_quote:{_failure_code(exc)}")
            return None
        warnings = (
            "secondary public-web snapshot; not exchange tick, L1, or executable quote",
            "Tencent board and bulk-quote endpoints share one publisher and are not "
            "independent market-data venues",
            "Tencent provider timestamps were validated to the requested session; "
            "each retained quote is no older than three minutes; available_at is first "
            "complete collection",
            "strict board/quote inner join required exact symbol and name, non-regressing "
            "and plausibly bounded session amount, internally consistent change, and at "
            "most 1.5% last-price divergence",
            "current-session volume and amount are incomplete and time-of-day dependent",
            *(f"PRIOR_SOURCE_FAILED:{item}" for item in failures),
        )
        return AShareIntradayUniverseSnapshot(
            session_date=session_date,
            available_at=completed_at,
            observed_at=completed_at,
            source_id=TENCENT_ENRICHED_SURVEILLANCE_SOURCE_ID,
            source_revision=_revision(
                TENCENT_ENRICHED_SURVEILLANCE_SOURCE_ID,
                completed_at,
                enriched,
            ),
            records=enriched,
            quality=SurveillanceSourceQuality.COMPLETE,
            warnings=warnings,
        )


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


def _compose_tencent_enrichment(
    *,
    board_records: tuple[AShareIntradayUniverseRecord, ...],
    quote_rows: tuple[Mapping[str, Any], ...],
    session_date: date,
    fetched_at: datetime,
    minimum_universe_count: int,
) -> tuple[AShareIntradayUniverseRecord, ...]:
    board_by_symbol = {item.symbol: item for item in board_records}
    quote_by_symbol: dict[str, AShareIntradayUniverseRecord] = {}
    for row in quote_rows:
        try:
            provider_code = _text(row.get("provider_code"))
            classified = _classify_symbol(provider_code)
            if classified is None:
                continue
            symbol, board = classified
            if symbol not in board_by_symbol:
                raise AShareSurveillanceDataError("UNEXPECTED_QUOTE_SYMBOL")
            _tencent_provider_at(
                row.get("provider_timestamp"),
                session_date=session_date,
                fetched_at=fetched_at,
            )
            name = _text(row.get("name"))
            last = _required_decimal(row.get("last"), "last")
            previous_close = _required_decimal(
                row.get("previous_close"), "previous_close"
            )
            open_price = _required_decimal(row.get("open"), "open")
            high = _required_decimal(row.get("high"), "high")
            low = _required_decimal(row.get("low"), "low")
            change_percent = _required_decimal(
                row.get("change_percent"), "change_percent"
            )
            amount = _required_decimal(row.get("amount_cny"), "amount_cny")
            turnover_rate = _required_decimal(
                row.get("turnover_rate"), "turnover_rate"
            )
            volume_ratio = _required_decimal(row.get("volume_ratio"), "volume_ratio")
            if amount < 0 or turnover_rate < 0:
                raise AShareSurveillanceDataError("NEGATIVE_QUOTE_FIELD")
            if previous_close <= 0:
                raise AShareSurveillanceDataError("INVALID_PREVIOUS_CLOSE")
            expected_change = (last / previous_close - Decimal("1")) * Decimal("100")
            if abs(expected_change - change_percent) > _TENCENT_CHANGE_PERCENT_TOLERANCE:
                raise AShareSurveillanceDataError("INCONSISTENT_CHANGE_PERCENT")
            suspended = (
                amount == 0 and open_price == 0 and high == 0 and low == 0
            )
            if suspended:
                volume_ratio_value = None if volume_ratio < 0 else volume_ratio
            else:
                if min(last, previous_close, open_price, high, low) <= 0:
                    raise AShareSurveillanceDataError("INVALID_QUOTE_OHLC")
                if low > min(open_price, last) or high < max(open_price, last):
                    raise AShareSurveillanceDataError("INCONSISTENT_QUOTE_OHLC")
                if volume_ratio < 0:
                    raise AShareSurveillanceDataError("NEGATIVE_QUOTE_FIELD")
                volume_ratio_value = volume_ratio
            record = AShareIntradayUniverseRecord(
                symbol=symbol,
                name=name,
                board=board,
                is_st=bool(_ST_PATTERN.search(name)),
                is_suspended=suspended,
                last_price=last,
                previous_close=previous_close,
                open_price=open_price,
                high_price=high,
                low_price=low,
                change_percent=change_percent,
                session_amount_cny=amount,
                turnover_rate_percent=turnover_rate,
                volume_ratio=volume_ratio_value,
            )
        except AShareSurveillanceDataError:
        # 拒绝无效报价行，而不是用中性值填充因子。下面的全市场阈值会让大范围
        # 数据损坏成为致命错误。
            continue
        previous = quote_by_symbol.get(symbol)
        if previous is not None and previous != record:
            raise AShareSurveillanceDataError("CONFLICTING_DUPLICATE_QUOTE")
        quote_by_symbol[symbol] = record

    enriched: list[AShareIntradayUniverseRecord] = []
    for symbol, board_record in sorted(board_by_symbol.items()):
        quote = quote_by_symbol.get(symbol)
        if quote is None or quote.name != board_record.name:
            continue
        if quote.board is not board_record.board:
            continue
        board_last = board_record.last_price
        quote_last = quote.last_price
        if board_last is None or quote_last is None or board_last <= 0 or quote_last <= 0:
            continue
        divergence = abs(quote_last / board_last - Decimal("1"))
        if divergence > _TENCENT_PRICE_DIVERGENCE_LIMIT:
            continue
        board_amount = board_record.session_amount_cny
        quote_amount = quote.session_amount_cny
        if board_amount is None or quote_amount is None:
            continue
        allowed_lag = max(
            _TENCENT_AMOUNT_ROUNDING_CNY,
            board_amount * _TENCENT_AMOUNT_LAG_LIMIT,
        )
        if quote_amount + allowed_lag < board_amount:
            continue
        allowed_lead = max(
            _TENCENT_AMOUNT_LEAD_FLOOR_CNY,
            board_amount * _TENCENT_AMOUNT_LEAD_LIMIT,
        )
        if quote_amount > board_amount + allowed_lead:
            continue
        enriched.append(quote)
    if len(enriched) < minimum_universe_count:
        raise AShareSurveillanceDataError("INCOMPLETE_ENRICHED_UNIVERSE")
    return tuple(enriched)


def _tencent_provider_at(
    value: object,
    *,
    session_date: date,
    fetched_at: datetime,
) -> datetime:
    timestamp = _text(value)
    try:
        local = datetime.strptime(timestamp, "%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI)
    except ValueError:
        raise AShareSurveillanceDataError("INVALID_PROVIDER_TIMESTAMP") from None
    if local.date() != session_date:
        raise AShareSurveillanceDataError("STALE_PROVIDER_TIMESTAMP")
    result = local.astimezone(UTC)
    if result > fetched_at + timedelta(seconds=15):
        raise AShareSurveillanceDataError("PROVIDER_TIMESTAMP_FROM_FUTURE")
    if fetched_at - result > _TENCENT_QUOTE_MAX_AGE:
        raise AShareSurveillanceDataError("STALE_PROVIDER_TIMESTAMP")
    return result


def _required_decimal(value: object, field_name: str) -> Decimal:
    result = _optional_decimal(value)
    if result is None:
        raise AShareSurveillanceDataError(f"MISSING_{field_name.upper()}")
    return result


def _tencent_provider_code(symbol: str) -> str:
    code, exchange = symbol.split(".", maxsplit=1)
    return f"{exchange.lower()}{code}"


def _tencent_bulk_quote_records(
    provider_codes: tuple[str, ...],
    *,
    request_timeout_seconds: float,
) -> tuple[Mapping[str, Any], ...]:
    if not provider_codes or len(provider_codes) != len(set(provider_codes)):
        raise AShareSurveillanceDataError("INVALID_QUOTE_REQUEST_UNIVERSE")
    chunks = tuple(
        provider_codes[index : index + _TENCENT_QUOTE_CHUNK_SIZE]
        for index in range(0, len(provider_codes), _TENCENT_QUOTE_CHUNK_SIZE)
    )
    headers = {
        "Referer": "https://gu.qq.com/",
        "User-Agent": "Mozilla/5.0 (compatible; gribuki-trade/0.1)",
    }
    timeout = httpx.Timeout(request_timeout_seconds)
    rows: list[Mapping[str, Any]] = []
    with (
        httpx.Client(headers=headers, timeout=timeout, follow_redirects=True) as client,
        concurrent.futures.ThreadPoolExecutor(
            max_workers=min(_TENCENT_QUOTE_WORKERS, len(chunks))
        ) as executor,
    ):
        futures = tuple(
            executor.submit(_fetch_tencent_quote_chunk, client, chunk)
            for chunk in chunks
        )
        for future in futures:
            rows.extend(future.result())
    returned = tuple(_text(row.get("provider_code")) for row in rows)
    if len(returned) != len(set(returned)):
        raise AShareSurveillanceDataError("DUPLICATE_QUOTE_SYMBOL")
    if set(returned) != set(provider_codes):
        raise AShareSurveillanceDataError("INCOMPLETE_QUOTE_UNIVERSE")
    return tuple(rows)


def _fetch_tencent_quote_chunk(
    client: httpx.Client,
    provider_codes: tuple[str, ...],
) -> tuple[Mapping[str, Any], ...]:
    response: httpx.Response | None = None
    for _attempt in range(2):
        try:
            response = client.get(
                "https://qt.gtimg.cn/q=" + ",".join(provider_codes)
            )
            response.raise_for_status()
            break
        except httpx.HTTPError:
            response = None
    if response is None:
        raise AShareSurveillanceDataError("QUOTE_HTTP_FAILED")
    try:
        body = response.content.decode("gbk")
    except UnicodeDecodeError:
        raise AShareSurveillanceDataError("QUOTE_DECODE_FAILED") from None
    rows: list[Mapping[str, Any]] = []
    for raw_line in body.split(";"):
        line = raw_line.strip()
        if not line:
            continue
        match = _TENCENT_QUOTE_LINE.fullmatch(line)
        if match is None:
            raise AShareSurveillanceDataError("INVALID_QUOTE_PAYLOAD")
        provider_code = match.group(1)
        fields = match.group(2).split("~")
        if len(fields) <= 49:
            raise AShareSurveillanceDataError("TRUNCATED_QUOTE_PAYLOAD")
        trade_triplet = fields[35].split("/")
        if len(trade_triplet) != 3:
            raise AShareSurveillanceDataError("INVALID_QUOTE_TRADE_FIELD")
        if fields[2] != provider_code[2:]:
            raise AShareSurveillanceDataError("QUOTE_SYMBOL_CONFLICT")
        rows.append(
            {
                "provider_code": provider_code,
                "name": fields[1],
                "last": fields[3],
                "previous_close": fields[4],
                "open": fields[5],
                "provider_timestamp": fields[30],
                "change_percent": fields[32],
                "high": fields[33],
                "low": fields[34],
                "amount_cny": trade_triplet[2],
                "turnover_rate": fields[38],
                "volume_ratio": fields[49],
            }
        )
    if {str(row["provider_code"]) for row in rows} != set(provider_codes):
        raise AShareSurveillanceDataError("INCOMPLETE_QUOTE_CHUNK")
    return tuple(rows)


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
