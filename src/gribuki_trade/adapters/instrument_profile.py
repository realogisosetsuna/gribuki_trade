"""动态发现 A 股的当前时点证券元数据。

此处使用的 AKShare 端点是实时公开网页快照，不提供历史修订或权威提供者时间戳。
因此适配器只接受接近采集器时钟的请求，记录实际观测日期，并对历史重放关闭
失败。已存储档案可在之后重放；历史回测期间不得查询本适配器来替代已归档的
证券主数据。
"""

from __future__ import annotations

import asyncio
import math
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from functools import partial
from typing import Any, TypeVar, cast
from zoneinfo import ZoneInfo

from gribuki_trade.domain.instruments import ResearchInstrumentProfile

AKSHARE_STOCK_PROFILE_SOURCE_ID = "AKShare/Eastmoney stock_individual_info_em"
AKSHARE_ETF_PROFILE_SOURCE_ID = "AKShare/Eastmoney fund_etf_spot_em"

SHANGHAI = ZoneInfo("Asia/Shanghai")
_DEFAULT_POINT_IN_TIME_TOLERANCE = timedelta(minutes=5)


class InstrumentProfileFailureCode(StrEnum):
    """实时证券档案边界上的稳定机器可读失败。"""

    AKSHARE_NOT_INSTALLED = "AKSHARE_NOT_INSTALLED"
    LIVE_PROFILE_HISTORICAL_UNSUPPORTED = "LIVE_PROFILE_HISTORICAL_UNSUPPORTED"
    LIVE_PROFILE_CUTOFF_STALE = "LIVE_PROFILE_CUTOFF_STALE"
    COLLECTOR_CLOCK_MOVED_BACKWARD = "COLLECTOR_CLOCK_MOVED_BACKWARD"
    COLLECTOR_DATE_CHANGED = "COLLECTOR_DATE_CHANGED"
    PROFILE_TIMEOUT = "PROFILE_TIMEOUT"
    PROFILE_UPSTREAM_ERROR = "PROFILE_UPSTREAM_ERROR"
    PROFILE_ENDPOINT_UNAVAILABLE = "PROFILE_ENDPOINT_UNAVAILABLE"
    PROFILE_PAYLOAD_INVALID = "PROFILE_PAYLOAD_INVALID"
    PROFILE_PAYLOAD_EMPTY = "PROFILE_PAYLOAD_EMPTY"
    PROFILE_SCHEMA_MISMATCH = "PROFILE_SCHEMA_MISMATCH"
    BLANK_PROFILE_ITEM = "BLANK_PROFILE_ITEM"
    DUPLICATE_PROFILE_ITEM = "DUPLICATE_PROFILE_ITEM"
    STOCK_CODE_MISSING = "STOCK_CODE_MISSING"
    STOCK_CODE_INVALID = "STOCK_CODE_INVALID"
    STOCK_NAME_MISSING = "STOCK_NAME_MISSING"
    STOCK_INDUSTRY_MISSING = "STOCK_INDUSTRY_MISSING"
    SYMBOL_MISMATCH = "SYMBOL_MISMATCH"
    INVALID_MARKET_CAP = "INVALID_MARKET_CAP"
    INVALID_LISTING_DATE = "INVALID_LISTING_DATE"
    FUTURE_LISTING_DATE = "FUTURE_LISTING_DATE"
    ETF_PROFILE_NOT_FOUND = "ETF_PROFILE_NOT_FOUND"
    DUPLICATE_ETF_PROFILE = "DUPLICATE_ETF_PROFILE"
    ETF_NAME_MISSING = "ETF_NAME_MISSING"


class InstrumentProfileDataError(RuntimeError):
    """无法在不猜测的情况下生成实时证券档案。"""

    def __init__(self, failure_code: InstrumentProfileFailureCode) -> None:
        self.failure_code = failure_code
        # 为兼容命令行和错误文档，将 ``code`` 保持为普通字符串。
        self.code = failure_code.value
        super().__init__(f"instrument profile data failed ({self.code})")


class InstrumentProfilePointInTimeError(InstrumentProfileDataError):
    """实时端点被要求表示一个不可取得的历史状态。"""


_T = TypeVar("_T")


class AKShareInstrumentProfileAdapter:
    """解析当前描述性元数据，但不创建订单输入。

    ``known_at`` 是实时采集截止时点，而不是任意查询时点。较小容差用于接纳在
    调用本适配器前刚刚捕获的运行时间戳和普通采集器时钟偏移。历史消费者必须
    使用先前已持久化的档案修订版。
    """

    def __init__(
        self,
        client: Any | None = None,
        *,
        timeout_seconds: float = 20.0,
        point_in_time_tolerance: timedelta = _DEFAULT_POINT_IN_TIME_TOLERANCE,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive and finite")
        if point_in_time_tolerance <= timedelta(0):
            raise ValueError("point_in_time_tolerance must be positive")
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._point_in_time_tolerance = point_in_time_tolerance
        self._now = now

    async def fetch(
        self,
        symbol: str,
        *,
        known_at: datetime,
    ) -> ResearchInstrumentProfile:
        """获取一份实时股票/ETF 档案并保留其实际观测日。"""

        canonical, asset_type = _classify_symbol(symbol)
        known = _aware_utc(known_at, "known_at")
        started_at = _aware_utc(self._now(), "now")
        _validate_live_cutoff(
            known_at=known,
            started_at=started_at,
            tolerance=self._point_in_time_tolerance,
        )
        client = self._client or _import_akshare()
        if asset_type == "etf":
            rows = await _call_async(
                partial(_provider_records, client, "fund_etf_spot_em"),
                timeout_seconds=self._timeout_seconds,
            )
        else:
            rows = await _call_async(
                partial(
                    _provider_records,
                    client,
                    "stock_individual_info_em",
                    symbol=canonical[:6],
                ),
                timeout_seconds=self._timeout_seconds,
            )
        observed_at = _aware_utc(self._now(), "now")
        _validate_completion(
            known_at=known,
            started_at=started_at,
            observed_at=observed_at,
            tolerance=self._point_in_time_tolerance,
        )
        if asset_type == "etf":
            return _etf_profile(canonical, rows, observed_at)
        return _stock_profile(canonical, rows, observed_at)


def _stock_profile(
    symbol: str,
    rows: tuple[Mapping[str, Any], ...],
    observed_at: datetime,
) -> ResearchInstrumentProfile:
    values = _vertical_values(rows)
    raw_code = _first_value(values, "股票代码", "代码")
    if _is_missing(raw_code):
        raise InstrumentProfileDataError(InstrumentProfileFailureCode.STOCK_CODE_MISSING)
    code = _security_code(raw_code)
    if code is None:
        raise InstrumentProfileDataError(InstrumentProfileFailureCode.STOCK_CODE_INVALID)
    if code != symbol[:6]:
        raise InstrumentProfileDataError(InstrumentProfileFailureCode.SYMBOL_MISMATCH)
    name = _required_first_text(
        values,
        InstrumentProfileFailureCode.STOCK_NAME_MISSING,
        "股票简称",
        "简称",
        "名称",
    )
    industry = _required_first_text(
        values,
        InstrumentProfileFailureCode.STOCK_INDUSTRY_MISSING,
        "行业",
    )
    market_cap = _first_decimal(values, "总市值")
    verified_on = observed_at.astimezone(SHANGHAI).date()
    listing_date = _first_date(values, "上市时间", "上市日期")
    if listing_date is not None and listing_date > verified_on:
        raise InstrumentProfileDataError(InstrumentProfileFailureCode.FUTURE_LISTING_DATE)
    background = [
        f"证券简称：{name}",
        f"当前行业分类：{industry}",
        f"当前证券画像首次观察时间：{observed_at.isoformat()}",
        "当前公开网页没有历史修订或权威发布时间，禁止据此回填历史证券画像",
    ]
    if market_cap is not None:
        background.append(f"当前公开总市值：{format(market_cap, 'f')}元")
    if listing_date is not None:
        background.append(f"公开上市日期：{listing_date.isoformat()}")
    return ResearchInstrumentProfile(
        symbol=symbol,
        name=name,
        market="A股",
        asset_type="stock",
        exchange=_exchange_name(symbol),
        board=_stock_board(symbol),
        size_tier=_size_tier(market_cap),
        industry=industry,
        styles=("动态全市场候选", "个股"),
        research_role="全市场筛选后的深度研究候选",
        risk_tags=("当前证券画像", "历史分类未归档"),
        source_id=AKSHARE_STOCK_PROFILE_SOURCE_ID,
        verified_on=verified_on,
        background_facts=tuple(background),
    )


def _etf_profile(
    symbol: str,
    rows: tuple[Mapping[str, Any], ...],
    observed_at: datetime,
) -> ResearchInstrumentProfile:
    code_key = _resolve_column(rows, ("代码", "code"))
    name_key = _resolve_column(rows, ("名称", "name"))
    matching = tuple(
        row for row in rows if _security_code(row.get(code_key)) == symbol[:6]
    )
    if len(matching) != 1:
        code = (
            InstrumentProfileFailureCode.ETF_PROFILE_NOT_FOUND
            if not matching
            else InstrumentProfileFailureCode.DUPLICATE_ETF_PROFILE
        )
        raise InstrumentProfileDataError(code)
    name = _normalized_text(matching[0].get(name_key))
    if name is None:
        raise InstrumentProfileDataError(InstrumentProfileFailureCode.ETF_NAME_MISSING)
    verified_on = observed_at.astimezone(SHANGHAI).date()
    return ResearchInstrumentProfile(
        symbol=symbol,
        name=name,
        market="A股",
        asset_type="etf",
        exchange=_exchange_name(symbol),
        board="sse_etf" if symbol.endswith(".SH") else "szse_etf",
        size_tier="cross_size",
        industry="交易型开放式基金（具体跟踪标的未核验）",
        styles=("动态全市场候选", "ETF"),
        research_role="全市场筛选后的ETF深度研究候选",
        risk_tags=("当前证券画像", "跟踪指数与基金档案未由该数据源提供"),
        source_id=AKSHARE_ETF_PROFILE_SOURCE_ID,
        verified_on=verified_on,
        background_facts=(
            f"基金简称：{name}",
            f"当前证券画像首次观察时间：{observed_at.isoformat()}",
            "当前公开快照仅确认代码与名称；未提供的跟踪指数、基金公司和费率未作推断",
            "当前公开网页没有历史修订或权威发布时间，禁止据此回填历史证券画像",
        ),
    )


def _vertical_values(rows: tuple[Mapping[str, Any], ...]) -> dict[str, object]:
    item_key = _resolve_column(rows, ("item", "项目", "指标"))
    value_key = _resolve_column(rows, ("value", "值", "数值"))
    result: dict[str, object] = {}
    for row in rows:
        key = _normalized_text(row.get(item_key))
        if key is None:
            raise InstrumentProfileDataError(InstrumentProfileFailureCode.BLANK_PROFILE_ITEM)
        if key in result:
            raise InstrumentProfileDataError(
                InstrumentProfileFailureCode.DUPLICATE_PROFILE_ITEM
            )
        result[key] = row.get(value_key)
    return result


def _first_value(values: Mapping[str, object], *keys: str) -> object | None:
    for key in keys:
        if key in values and not _is_missing(values[key]):
            return values[key]
    return None


def _first_text(values: Mapping[str, object], *keys: str) -> str | None:
    return _normalized_text(_first_value(values, *keys))


def _required_first_text(
    values: Mapping[str, object],
    failure_code: InstrumentProfileFailureCode,
    *keys: str,
) -> str:
    value = _first_text(values, *keys)
    if value is None:
        raise InstrumentProfileDataError(failure_code)
    return value


def _first_decimal(values: Mapping[str, object], *keys: str) -> Decimal | None:
    raw = _first_value(values, *keys)
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise InstrumentProfileDataError(InstrumentProfileFailureCode.INVALID_MARKET_CAP)
    try:
        value = Decimal(str(raw).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        raise InstrumentProfileDataError(
            InstrumentProfileFailureCode.INVALID_MARKET_CAP
        ) from None
    if not value.is_finite() or value < 0:
        raise InstrumentProfileDataError(InstrumentProfileFailureCode.INVALID_MARKET_CAP)
    return value


def _first_date(values: Mapping[str, object], *keys: str) -> date | None:
    raw = _first_value(values, *keys)
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    if isinstance(raw, bool):
        raise InstrumentProfileDataError(InstrumentProfileFailureCode.INVALID_LISTING_DATE)
    text = unicodedata.normalize("NFKC", str(raw)).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    for pattern in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            pass
    raise InstrumentProfileDataError(InstrumentProfileFailureCode.INVALID_LISTING_DATE)


def _size_tier(market_cap: Decimal | None) -> str:
    if market_cap is None:
        return "unclassified"
    if market_cap >= Decimal("200000000000"):
        return "mega"
    if market_cap >= Decimal("50000000000"):
        return "large"
    if market_cap >= Decimal("10000000000"):
        return "mid"
    return "small"


def _stock_board(symbol: str) -> str:
    code = symbol[:6]
    if symbol.endswith(".BJ"):
        return "bse"
    if code.startswith(("688", "689")):
        return "star"
    if code.startswith(("300", "301")):
        return "chinext"
    return "sse_main" if symbol.endswith(".SH") else "szse_main"


def _exchange_name(symbol: str) -> str:
    return {"SH": "sse", "SZ": "szse", "BJ": "bse"}[symbol[-2:]]


def _classify_symbol(symbol: str) -> tuple[str, str]:
    value = symbol.strip().upper()
    if len(value) == 6 and value.isascii() and value.isdigit():
        exchange = _expected_exchange(value)
        if exchange is None:
            raise ValueError(f"unsupported A-share stock/ETF code: {symbol!r}")
        value = f"{value}.{exchange}"
    if (
        len(value) != 9
        or value[6] != "."
        or not value[:6].isascii()
        or not value[:6].isdigit()
        or value[7:] not in {"SH", "SZ", "BJ"}
    ):
        raise ValueError("symbol must look like 600000.SH, 000001.SZ, or 430047.BJ")
    expected = _expected_exchange(value[:6])
    if expected is None:
        raise ValueError(f"unsupported A-share stock/ETF code: {symbol!r}")
    if value[7:] != expected:
        raise ValueError(f"symbol exchange conflicts with code family: {symbol!r}")
    return value, "etf" if _is_etf(value) else "stock"


def _expected_exchange(code: str) -> str | None:
    if code.startswith(("4", "8", "92")):
        return "BJ"
    if code.startswith(("5", "600", "601", "603", "605", "609", "688", "689")):
        return "SH"
    if code.startswith(("000", "001", "002", "003", "15", "16", "300", "301")):
        return "SZ"
    return None


def _is_etf(symbol: str) -> bool:
    code, exchange = symbol.split(".", maxsplit=1)
    return (exchange == "SH" and code.startswith("5")) or (
        exchange == "SZ" and code.startswith(("15", "16"))
    )


def _security_code(value: object) -> str | None:
    if _is_missing(value) or isinstance(value, bool):
        return None
    text = unicodedata.normalize("NFKC", str(value)).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    text = text.zfill(6)
    return text if len(text) == 6 and text.isascii() and text.isdigit() else None


def _resolve_column(
    rows: tuple[Mapping[str, Any], ...], aliases: tuple[str, ...]
) -> str:
    available = {str(key) for row in rows for key in row}
    matches = tuple(alias for alias in aliases if alias in available)
    if len(matches) != 1:
        raise InstrumentProfileDataError(
            InstrumentProfileFailureCode.PROFILE_SCHEMA_MISMATCH
        )
    return matches[0]


def _provider_records(
    client: Any, operation: str, **kwargs: object
) -> tuple[Mapping[str, Any], ...]:
    method = getattr(client, operation, None)
    if method is None or not callable(method):
        raise InstrumentProfileDataError(
            InstrumentProfileFailureCode.PROFILE_ENDPOINT_UNAVAILABLE
        )
    payload = method(**kwargs)
    if payload is None or not hasattr(payload, "to_dict"):
        raise InstrumentProfileDataError(
            InstrumentProfileFailureCode.PROFILE_PAYLOAD_INVALID
        )
    try:
        rows = payload.to_dict(orient="records")
    except (TypeError, ValueError, AttributeError):
        raise InstrumentProfileDataError(
            InstrumentProfileFailureCode.PROFILE_PAYLOAD_INVALID
        ) from None
    if not isinstance(rows, Sequence) or not rows:
        raise InstrumentProfileDataError(
            InstrumentProfileFailureCode.PROFILE_PAYLOAD_EMPTY
        )
    if any(not isinstance(row, Mapping) for row in rows):
        raise InstrumentProfileDataError(
            InstrumentProfileFailureCode.PROFILE_PAYLOAD_INVALID
        )
    return tuple(cast(Mapping[str, Any], row) for row in rows)


async def _call_async(call: Callable[[], _T], *, timeout_seconds: float) -> _T:
    try:
        return await asyncio.wait_for(asyncio.to_thread(call), timeout=timeout_seconds)
    except TimeoutError:
        raise InstrumentProfileDataError(
            InstrumentProfileFailureCode.PROFILE_TIMEOUT
        ) from None
    except InstrumentProfileDataError:
        raise
    except Exception:
        raise InstrumentProfileDataError(
            InstrumentProfileFailureCode.PROFILE_UPSTREAM_ERROR
        ) from None


def _validate_live_cutoff(
    *,
    known_at: datetime,
    started_at: datetime,
    tolerance: timedelta,
) -> None:
    if known_at.astimezone(SHANGHAI).date() != started_at.astimezone(SHANGHAI).date():
        raise InstrumentProfilePointInTimeError(
            InstrumentProfileFailureCode.LIVE_PROFILE_HISTORICAL_UNSUPPORTED
        )
    if abs(started_at - known_at) > tolerance:
        raise InstrumentProfilePointInTimeError(
            InstrumentProfileFailureCode.LIVE_PROFILE_CUTOFF_STALE
        )


def _validate_completion(
    *,
    known_at: datetime,
    started_at: datetime,
    observed_at: datetime,
    tolerance: timedelta,
) -> None:
    if observed_at < started_at:
        raise InstrumentProfilePointInTimeError(
            InstrumentProfileFailureCode.COLLECTOR_CLOCK_MOVED_BACKWARD
        )
    if observed_at.astimezone(SHANGHAI).date() != started_at.astimezone(SHANGHAI).date():
        raise InstrumentProfilePointInTimeError(
            InstrumentProfileFailureCode.COLLECTOR_DATE_CHANGED
        )
    if observed_at - known_at > tolerance:
        raise InstrumentProfilePointInTimeError(
            InstrumentProfileFailureCode.LIVE_PROFILE_CUTOFF_STALE
        )


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().casefold() in {
            "",
            "-",
            "--",
            "nan",
            "nat",
            "none",
            "null",
            "<na>",
        }
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Decimal):
        return not value.is_finite()
    # 覆盖 pandas.NA/NaT 等提供者哨兵值，同时不把 pandas 导入生产适配器。
    try:
        text = unicodedata.normalize("NFKC", str(value)).strip().casefold()
    except Exception:
        return False
    return text in {"", "-", "--", "nan", "nat", "none", "null", "<na>"}


def _normalized_text(value: object) -> str | None:
    if _is_missing(value) or isinstance(value, bool):
        return None
    text = unicodedata.normalize("NFKC", str(value)).strip()
    return text or None


def _aware_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _import_akshare() -> Any:
    try:
        import akshare  # type: ignore[import-untyped]
    except ImportError:
        raise InstrumentProfileDataError(
            InstrumentProfileFailureCode.AKSHARE_NOT_INSTALLED
        ) from None
    return akshare


__all__ = [
    "AKSHARE_ETF_PROFILE_SOURCE_ID",
    "AKSHARE_STOCK_PROFILE_SOURCE_ID",
    "AKShareInstrumentProfileAdapter",
    "InstrumentProfileDataError",
    "InstrumentProfileFailureCode",
    "InstrumentProfilePointInTimeError",
]
