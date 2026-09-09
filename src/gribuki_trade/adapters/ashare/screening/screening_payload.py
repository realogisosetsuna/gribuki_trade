"""AKShare 三层筛选的提供者载荷解析与确定性校验。

本模块只处理已经返回到进程内的提供者数据：把表格或记录序列规范化为
领域记录，验证代码、日期、价格、单位和股票池覆盖范围，并生成可重放的
修订摘要。网络调用、超时、调度和降级选择仍由 ``screening`` 适配器负责。

保留了历史私有函数名，供兼容 facade 和盘前筛选适配器迁移期间继续使用。
这些函数没有 AKShare 客户端状态，也不会触发网络请求（新浪 V8 解码器的
进程级串行锁是唯一的并发边界）。
"""

from __future__ import annotations

import hashlib
import math
import re
import threading
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from gribuki_trade.ports.ashare_screening import (
    AShareBoard,
    AShareFactorRecord,
    AShareUniverseRecord,
)

from .screening_factors import HistoryBar as _HistoryBar

EASTMONEY_SCREENING_SOURCE_ID = "AKShare/Eastmoney stock_zh_a_spot_em"
TENCENT_SCREENING_SOURCE_ID = "AKShare/Tencent stock_zh_a_spot_tx"
AKSHARE_HISTORY_SOURCE_ID = "AKShare/Eastmoney stock_zh_a_hist adjust=NONE"
SINA_HISTORY_SOURCE_ID = "AKShare/Sina stock_zh_a_daily adjust=NONE"
_FEATURE_VERSION = "ashare-screening-raw-factors@1"


class AKShareScreeningDataError(RuntimeError):
    """公开筛选输入失败的基类。"""


class AKShareScreeningPointInTimeError(AKShareScreeningDataError):
    """在可辩护时间边界之外请求了实时端点。"""


class AKShareScreeningPayloadError(AKShareScreeningDataError):
    """数据提供者响应不符合明确的架构或单位契约。"""


class AKShareScreeningCoverageError(AKShareScreeningDataError):
    """全市场数据提供者返回的股票池小得不可信。"""


class AKShareScreeningSourcesExhaustedError(AKShareScreeningDataError):
    """两个独立全市场来源均未生成可接受快照。"""

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
        # 腾讯 turnover 以人民币万元计，zsz 以人民币亿元计。
        amount_multiplier=Decimal("10000"),
        market_cap_multiplier=Decimal("100000000"),
    ),
)

_SINA_HISTORY_LOCK = threading.Lock()
_ST_NAME_PATTERN = re.compile(r"^(?:S\*ST|SST|\*ST|ST)|退", re.IGNORECASE)


def _provider_records(
    client: Any,
    operation: str,
    **kwargs: object,
) -> tuple[Mapping[str, Any], ...]:
    """将 DataFrame 或记录序列转换成可验证的映射元组。"""

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


def _sina_provider_records(client: Any, **kwargs: object) -> tuple[Mapping[str, Any], ...]:
    """在整个进程中逐个调用由 V8 支持的新浪解码器。"""

    with _SINA_HISTORY_LOCK:
        return _provider_records(client, "stock_zh_a_daily", **kwargs)


def _parse_universe_rows(
    rows: tuple[Mapping[str, Any], ...],
    *,
    spec: _SourceSpec,
    as_of: date,
    metadata: Mapping[str, _ListingMetadata],
    minimum_count: int,
) -> tuple[AShareUniverseRecord, ...]:
    """解析全市场快照并执行跨交易所覆盖与单位检查。"""

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
            # 东方财富没有明确停牌列。成交额为零只能证明状态未知，不能猜测可交易。
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
    """解析交易所上市元数据，并拒绝同一代码的冲突记录。"""

    if label.startswith("SSE"):
        aliases: Mapping[str, tuple[str, ...]] = {
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
    """解析不复权 OHLCV 历史并检查时间、价格和重复日期。"""

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


def _parse_sina_history(
    rows: tuple[Mapping[str, Any], ...],
    *,
    as_of: date,
) -> tuple[_HistoryBar, ...]:
    """解析新浪不复权日线，并从相邻原始收盘价派生前收盘。"""

    projected = tuple(
        {
            "date": item.get("date"),
            "open": item.get("open"),
            "high": item.get("high"),
            "low": item.get("low"),
            "close": item.get("close"),
            "volume": item.get("volume"),
            "amount": item.get("amount"),
        }
        for item in rows
    )
    parsed = _parse_history(projected, as_of=as_of)
    output: list[_HistoryBar] = []
    previous: _HistoryBar | None = None
    for item in parsed:
        output.append(
            _HistoryBar(
                trade_date=item.trade_date,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                previous_close=None if previous is None else previous.close,
                volume=item.volume,
                amount=item.amount,
            )
        )
        previous = item
    return tuple(output)


def _resolve_columns(
    rows: tuple[Mapping[str, Any], ...],
    aliases: Mapping[str, tuple[str, ...]],
    *,
    required: tuple[str, ...],
) -> dict[str, str]:
    """按白名单别名解析列，并拒绝同一语义的歧义列。"""

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
    """按代码前缀归类交易所板块，并检查显式交易所前缀。"""

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


def _sina_symbol(symbol: str) -> str:
    """把一个规范沪深代码映射到新浪命名空间。"""

    code, exchange = symbol.split(".", maxsplit=1)
    if exchange not in {"SH", "SZ"}:
        raise AKShareScreeningDataError(
            "Sina stock_zh_a_daily fallback supports Shanghai/Shenzhen only"
        )
    return f"{exchange.lower()}{code}"


def _record_uses_sina_fallback(record: AShareFactorRecord) -> bool:
    marker = f"HISTORY_SOURCE_FALLBACK:{SINA_HISTORY_SOURCE_ID}"
    return marker in record.warnings


def _factor_batch_source_id(records: tuple[AShareFactorRecord, ...]) -> str:
    if any(_record_uses_sina_fallback(item) for item in records):
        return f"{AKSHARE_HISTORY_SOURCE_ID}; fallback={SINA_HISTORY_SOURCE_ID}"
    return AKSHARE_HISTORY_SOURCE_ID


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


def _factor_revision(
    as_of: date,
    records: tuple[AShareFactorRecord, ...],
    *,
    source_id: str = AKSHARE_HISTORY_SOURCE_ID,
) -> str:
    document = {
        "source_id": source_id,
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
    import json

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

