"""面向不可执行 A 股研究上下文的严格 AKShare 适配器。

本模块中的端点刻意与现有跨市场报价适配器分离，避免 ETF 表、ChinaMoney
中间价、中国债券收益率曲线或 CFFEX CSV 变慢时污染主报价快照的失败域。
"""

from __future__ import annotations

import asyncio
import math
import threading
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from functools import partial
from queue import Empty, Queue
from typing import Any, TypeVar, cast
from zoneinfo import ZoneInfo

from gribuki_trade.ports.ashare_context import (
    AShareContextDataError,
    AShareContextEndpointError,
    AShareContextFailureCode,
    AShareContextMeta,
    AShareContextMissingSource,
    AShareContextNoDataError,
    AShareContextPayloadError,
    AShareContextTimeoutError,
    ETFContextSnapshot,
    GovernmentBondYieldCurve,
    GovernmentBondYieldPoint,
    IFContractDailyObservation,
    IFDailyContextSnapshot,
    LiquidityContextSnapshot,
    RepoFixingFamily,
    RepoFixingObservation,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")

ETF_SOURCE_ID = "AKShare/Eastmoney fund_etf_spot_em"
ETF_SOURCE_URL = "https://quote.eastmoney.com/center/gridlist.html#fund_etf"
FR_SOURCE_ID = "AKShare/ChinaMoney repo_rate_query(FR)"
FDR_SOURCE_ID = "AKShare/ChinaMoney repo_rate_query(FDR)"
REPO_SOURCE_URL = "https://www.chinamoney.com.cn/chinese/bkfrr/"
GOVERNMENT_CURVE_SOURCE_ID = "AKShare/ChinaBond bond_china_yield"
GOVERNMENT_CURVE_SOURCE_URL = "https://yield.chinabond.com.cn/"
CFFEX_IF_SOURCE_ID = "AKShare/CFFEX futures_hist_daily_cffex"
CFFEX_IF_SOURCE_URL = "http://www.cffex.com.cn/cn/rtj.html"

_GOVERNMENT_CURVE_NAME = "中债国债收益率曲线"
_REPO_SYMBOLS: Mapping[RepoFixingFamily, str] = {
    RepoFixingFamily.FR: "回购定盘利率",
    RepoFixingFamily.FDR: "银银间回购定盘利率",
}
_REPO_CONTEXT_IDS: Mapping[RepoFixingFamily, str] = {
    RepoFixingFamily.FR: "CHINAMONEY_FR_FIXING",
    RepoFixingFamily.FDR: "CHINAMONEY_FDR_FIXING",
}
_REPO_DISPLAY_NAMES: Mapping[RepoFixingFamily, str] = {
    RepoFixingFamily.FR: "回购定盘利率（FR001/FR007/FR014）",
    RepoFixingFamily.FDR: "银银间回购定盘利率（FDR001/FDR007/FDR014）",
}
_TENOR_COLUMNS: tuple[tuple[str, Decimal], ...] = (
    ("3月", Decimal("0.25")),
    ("6月", Decimal("0.5")),
    ("1年", Decimal("1")),
    ("3年", Decimal("3")),
    ("5年", Decimal("5")),
    ("7年", Decimal("7")),
    ("10年", Decimal("10")),
    ("30年", Decimal("30")),
)

_ETF_ALIASES: Mapping[str, tuple[str, ...]] = {
    "code": ("代码", "code"),
    "name": ("名称", "name"),
    "last": ("最新价", "last"),
    "iopv": ("IOPV实时估值", "iopv"),
    "discount": ("基金折价率", "discount_rate_percent"),
    "turnover": ("换手率", "turnover_percent"),
    "shares": ("最新份额", "shares_outstanding"),
    "amount": ("成交额", "amount_cny"),
    "main_flow": ("主力净流入-净额", "main_net_inflow_cny"),
    "main_flow_percent": ("主力净流入-净占比", "main_net_inflow_percent"),
    "bid1": ("买一", "bid1"),
    "ask1": ("卖一", "ask1"),
    "data_date": ("数据日期", "data_date"),
    "update_time": ("更新时间", "update_time"),
}
_ETF_REQUIRED = frozenset({"code", "name", "last"})

_IF_ALIASES: Mapping[str, tuple[str, ...]] = {
    "symbol": ("symbol", "合约代码"),
    "date": ("date", "日期"),
    "open": ("open", "今开盘"),
    "high": ("high", "最高价"),
    "low": ("low", "最低价"),
    "close": ("close", "今收盘"),
    "volume": ("volume", "成交量"),
    "open_interest": ("open_interest", "持仓量"),
    "turnover": ("turnover", "成交额"),
    "settle": ("settle", "今结算"),
    "pre_settle": ("pre_settle", "前结算"),
    "variety": ("variety", "品种"),
}
_IF_REQUIRED = frozenset(_IF_ALIASES)

_T = TypeVar("_T")


class AKShareETFContextAdapter:
    """从 AKShare 的东方财富全量 ETF 表中采集一条精确记录。"""

    def __init__(
        self,
        client: Any | None = None,
        *,
        timeout_seconds: float = 30.0,
        stale_after: timedelta = timedelta(hours=36),
        future_tolerance: timedelta = timedelta(minutes=5),
        now: Callable[[], datetime] | None = None,
    ) -> None:
        _validate_timeout(timeout_seconds)
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        if future_tolerance < timedelta(0):
            raise ValueError("future_tolerance cannot be negative")
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._stale_after = stale_after
        self._future_tolerance = future_tolerance
        self._now = now or (lambda: datetime.now(tz=UTC))

    def fetch_etf_context(self, symbol: str) -> ETFContextSnapshot:
        canonical_symbol, code = _normalize_etf_symbol(symbol)
        client = self._client or _import_akshare()
        records = _provider_records(
            client,
            "fund_etf_spot_em",
            timeout_seconds=self._timeout_seconds,
            label=ETF_SOURCE_ID,
        )
        columns = _resolve_columns(records, _ETF_ALIASES, _ETF_REQUIRED, ETF_SOURCE_ID)
        matches = [
            row for row in records if _provider_security_code(row.get(columns["code"])) == code
        ]
        if not matches:
            raise AShareContextNoDataError(
                f"{ETF_SOURCE_ID} returned no exact row for {canonical_symbol}"
            )
        if len(matches) != 1:
            raise AShareContextPayloadError(
                f"{ETF_SOURCE_ID} returned duplicate rows for {canonical_symbol}"
            )

        fetched_at = _aware_now(self._now)
        return _parse_etf_row(
            matches[0],
            columns,
            symbol=canonical_symbol,
            fetched_at=fetched_at,
            stale_after=self._stale_after,
            future_tolerance=self._future_tolerance,
        )

    async def fetch_etf_context_async(self, symbol: str) -> ETFContextSnapshot:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self.fetch_etf_context, symbol),
                timeout=self._timeout_seconds + 1.0,
            )
        except TimeoutError as exc:
            raise AShareContextTimeoutError(
                f"{ETF_SOURCE_ID} orchestration exceeded {self._timeout_seconds + 1:g}s"
            ) from exc


class AKShareLiquidityContextAdapter:
    """采集 FR、FDR 及精确的中国债券信息网国债收益率曲线。

    每个上游调用均有独立时限，失败时按缺失来源保留。FR007 和 FDR007 是定盘
    系列；本适配器绝不会把其中任何一个重新标记为成交量加权的银行间 DR007。
    """

    def __init__(
        self,
        client: Any | None = None,
        *,
        timeout_seconds: float = 15.0,
        repo_stale_after_days: int = 4,
        curve_stale_after_days: int = 7,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        _validate_timeout(timeout_seconds)
        if repo_stale_after_days < 1 or curve_stale_after_days < 1:
            raise ValueError("stale-after days must be positive")
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._repo_stale_after_days = repo_stale_after_days
        self._curve_stale_after_days = curve_stale_after_days
        self._now = now or (lambda: datetime.now(tz=UTC))

    def fetch_liquidity_context(
        self,
        *,
        start_date: date,
        end_date: date,
    ) -> LiquidityContextSnapshot:
        if start_date > end_date:
            raise ValueError("start_date cannot follow end_date")
        if (end_date - start_date).days >= 366:
            raise ValueError("ChinaBond query interval must be shorter than one year")
        collection_started = _aware_now(self._now)
        if end_date > collection_started.astimezone(SHANGHAI).date():
            raise ValueError("end_date cannot be after the collection date")
        client = self._client or _import_akshare()

        frames: dict[str, tuple[Mapping[str, Any], ...]] = {}
        source_errors: dict[str, AShareContextDataError] = {}
        for family in RepoFixingFamily:
            context_id = _REPO_CONTEXT_IDS[family]
            try:
                frames[context_id] = _provider_records(
                    client,
                    "repo_rate_query",
                    kwargs={"symbol": _REPO_SYMBOLS[family]},
                    timeout_seconds=self._timeout_seconds,
                    label=_repo_source_id(family),
                )
            except AShareContextDataError as exc:
                source_errors[context_id] = exc

        try:
            frames["CHINABOND_GOVERNMENT_YIELD_CURVE"] = _provider_records(
                client,
                "bond_china_yield",
                kwargs={
                    "start_date": start_date.strftime("%Y%m%d"),
                    "end_date": end_date.strftime("%Y%m%d"),
                },
                timeout_seconds=self._timeout_seconds,
                label=GOVERNMENT_CURVE_SOURCE_ID,
            )
        except AShareContextDataError as exc:
            source_errors["CHINABOND_GOVERNMENT_YIELD_CURVE"] = exc

        fetched_at = _aware_now(self._now)
        fixings: list[RepoFixingObservation] = []
        curve: GovernmentBondYieldCurve | None = None
        missing: list[AShareContextMissingSource] = []

        for family in RepoFixingFamily:
            context_id = _REPO_CONTEXT_IDS[family]
            error = source_errors.get(context_id)
            if error is None:
                try:
                    fixings.append(
                        _parse_repo_fixing(
                            frames[context_id],
                            family=family,
                            start_date=start_date,
                            end_date=end_date,
                            fetched_at=fetched_at,
                            stale_after_days=self._repo_stale_after_days,
                        )
                    )
                except AShareContextDataError as exc:
                    error = exc
            if error is not None:
                missing.append(
                    _missing_source(
                        context_id,
                        _REPO_DISPLAY_NAMES[family],
                        _repo_source_id(family),
                        error,
                    )
                )

        curve_error = source_errors.get("CHINABOND_GOVERNMENT_YIELD_CURVE")
        if curve_error is None:
            try:
                curve = _parse_government_curve(
                    frames["CHINABOND_GOVERNMENT_YIELD_CURVE"],
                    start_date=start_date,
                    end_date=end_date,
                    fetched_at=fetched_at,
                    stale_after_days=self._curve_stale_after_days,
                )
            except AShareContextDataError as exc:
                curve_error = exc
        if curve_error is not None:
            missing.append(
                _missing_source(
                    "CHINABOND_GOVERNMENT_YIELD_CURVE",
                    _GOVERNMENT_CURVE_NAME,
                    GOVERNMENT_CURVE_SOURCE_ID,
                    curve_error,
                )
            )

        fixings.sort(key=lambda item: item.family.value)
        degraded = (
            bool(missing)
            or any(item.meta.degraded for item in fixings)
            or (curve is not None and curve.meta.degraded)
        )
        return LiquidityContextSnapshot(
            fetched_at=fetched_at,
            cutoff_date=end_date,
            repo_fixings=tuple(fixings),
            government_curve=curve,
            missing=tuple(missing),
            degraded=degraded,
            warnings=(
                "FR and FDR are fixing rates and must not be relabelled as DR007",
                "date-only series use collection time as their first observed/available time",
                "context values are observational and do not establish equity-market causality",
            ),
        )

    async def fetch_liquidity_context_async(
        self,
        *,
        start_date: date,
        end_date: date,
    ) -> LiquidityContextSnapshot:
        orchestration_timeout = self._timeout_seconds * 3 + 1.0
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    partial(
                        self.fetch_liquidity_context,
                        start_date=start_date,
                        end_date=end_date,
                    )
                ),
                timeout=orchestration_timeout,
            )
        except TimeoutError as exc:  # pragma: no cover - defensive outer bound
            raise AShareContextTimeoutError(
                f"liquidity context orchestration exceeded {orchestration_timeout:g}s"
            ) from exc


class AKShareIFContextAdapter:
    """采集某交易日的中金所 IF 官方原始日合约。"""

    def __init__(
        self,
        client: Any | None = None,
        *,
        timeout_seconds: float = 15.0,
        stale_after_days: int = 4,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        _validate_timeout(timeout_seconds)
        if stale_after_days < 1:
            raise ValueError("stale_after_days must be positive")
        self._client = client
        self._timeout_seconds = timeout_seconds
        self._stale_after_days = stale_after_days
        self._now = now or (lambda: datetime.now(tz=UTC))

    def fetch_if_daily_context(self, session_date: date) -> IFDailyContextSnapshot:
        collection_started = _aware_now(self._now)
        if session_date > collection_started.astimezone(SHANGHAI).date():
            raise ValueError("session_date cannot be after the collection date")
        client = self._client or _import_akshare()
        records = _provider_records(
            client,
            "futures_hist_daily_cffex",
            kwargs={"date": session_date.strftime("%Y%m%d")},
            timeout_seconds=self._timeout_seconds,
            label=CFFEX_IF_SOURCE_ID,
        )
        fetched_at = _aware_now(self._now)
        contracts = _parse_if_contracts(
            records,
            session_date=session_date,
            fetched_at=fetched_at,
            stale_after_days=self._stale_after_days,
        )
        degraded = any(item.meta.degraded for item in contracts)
        return IFDailyContextSnapshot(
            session_date=session_date,
            fetched_at=fetched_at,
            contracts=contracts,
            degraded=degraded,
            warnings=(
                "raw IF futures rows do not contain an aligned CSI 300 spot value",
                "basis must be calculated later from a point-in-time aligned spot observation",
                "turnover_reported preserves the provider value and is not normalized to CNY",
            ),
        )

    async def fetch_if_daily_context_async(
        self,
        session_date: date,
    ) -> IFDailyContextSnapshot:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self.fetch_if_daily_context, session_date),
                timeout=self._timeout_seconds + 1.0,
            )
        except TimeoutError as exc:
            raise AShareContextTimeoutError(
                f"{CFFEX_IF_SOURCE_ID} orchestration exceeded {self._timeout_seconds + 1:g}s"
            ) from exc


def _parse_etf_row(
    row: Mapping[str, Any],
    columns: Mapping[str, str],
    *,
    symbol: str,
    fetched_at: datetime,
    stale_after: timedelta,
    future_tolerance: timedelta,
) -> ETFContextSnapshot:
    warnings = [
        "public web ETF snapshot is not an exchange-sequenced feed",
        "order-size flow labels are vendor classifications, not participant identity",
    ]
    update_value = _optional_row_value(row, columns, "update_time")
    if _is_missing(update_value):
        observed_at = fetched_at
        warnings.append("provider update timestamp missing; observation time equals fetch time")
    else:
        observed_at = _parse_datetime(update_value, SHANGHAI, "ETF update_time")
        if observed_at > fetched_at + future_tolerance:
            raise AShareContextPayloadError("ETF update_time is implausibly after fetched_at")
        if observed_at > fetched_at:
            observed_at = fetched_at
            warnings.append("small provider clock lead clamped to fetched_at")

    data_value = _optional_row_value(row, columns, "data_date")
    if _is_missing(data_value):
        data_date = observed_at.astimezone(SHANGHAI).date()
        warnings.append("provider data date missing; inferred from observation time")
    else:
        data_date = _parse_date(data_value, "ETF data_date")
    if data_date > fetched_at.astimezone(SHANGHAI).date():
        raise AShareContextPayloadError("ETF data_date is after fetched_at")

    optional_fields = {
        "iopv": _optional_decimal(_optional_row_value(row, columns, "iopv"), "ETF IOPV"),
        "discount_rate_percent": _optional_decimal(
            _optional_row_value(row, columns, "discount"), "ETF discount rate"
        ),
        "turnover_percent": _optional_decimal(
            _optional_row_value(row, columns, "turnover"), "ETF turnover"
        ),
        "shares_outstanding": _optional_decimal(
            _optional_row_value(row, columns, "shares"), "ETF shares"
        ),
        "amount_cny": _optional_decimal(_optional_row_value(row, columns, "amount"), "ETF amount"),
        "main_net_inflow_cny": _optional_decimal(
            _optional_row_value(row, columns, "main_flow"), "ETF main flow"
        ),
        "main_net_inflow_percent": _optional_decimal(
            _optional_row_value(row, columns, "main_flow_percent"),
            "ETF main flow percent",
        ),
        "bid1": _optional_decimal(_optional_row_value(row, columns, "bid1"), "ETF bid1"),
        "ask1": _optional_decimal(_optional_row_value(row, columns, "ask1"), "ETF ask1"),
    }
    missing_metrics = tuple(name for name, value in optional_fields.items() if value is None)
    if missing_metrics:
        warnings.append("missing optional ETF fields: " + ", ".join(missing_metrics))
    stale = fetched_at - observed_at > stale_after
    degraded = stale or bool(missing_metrics)
    if stale:
        warnings.append("ETF observation exceeded configured freshness window")
    meta = AShareContextMeta(
        source_id=ETF_SOURCE_ID,
        source_url=ETF_SOURCE_URL,
        observed_at=observed_at,
        available_at=fetched_at,
        fetched_at=fetched_at,
        stale=stale,
        degraded=degraded,
        warnings=tuple(warnings),
    )
    return ETFContextSnapshot(
        symbol=symbol,
        name=_required_text(row.get(columns["name"]), "ETF name"),
        data_date=data_date,
        last=_required_positive_decimal(row.get(columns["last"]), "ETF last"),
        meta=meta,
        **optional_fields,
    )


def _parse_repo_fixing(
    records: Sequence[Mapping[str, Any]],
    *,
    family: RepoFixingFamily,
    start_date: date,
    end_date: date,
    fetched_at: datetime,
    stale_after_days: int,
) -> RepoFixingObservation:
    prefix = family.value
    aliases: Mapping[str, tuple[str, ...]] = {
        "date": ("date", "日期"),
        "overnight": (f"{prefix}001",),
        "seven_day": (f"{prefix}007",),
        "fourteen_day": (f"{prefix}014",),
    }
    columns = _resolve_columns(
        records,
        aliases,
        frozenset(aliases),
        _repo_source_id(family),
    )
    eligible: list[tuple[date, Mapping[str, Any]]] = []
    for row in records:
        session_date = _parse_date(row.get(columns["date"]), f"{prefix} date")
        if start_date <= session_date <= end_date:
            eligible.append((session_date, row))
    if not eligible:
        raise AShareContextNoDataError(
            f"{_repo_source_id(family)} returned no row in requested interval"
        )
    latest_date = max(item[0] for item in eligible)
    latest_rows = [row for session_date, row in eligible if session_date == latest_date]
    if len(latest_rows) != 1:
        raise AShareContextPayloadError(
            f"{_repo_source_id(family)} returned duplicate latest-date rows"
        )
    row = latest_rows[0]
    stale = (fetched_at.astimezone(SHANGHAI).date() - latest_date).days > stale_after_days
    warnings = [
        "provider exposes a fixing date but no exact publication timestamp",
        "observed_at and available_at therefore equal this collector's fetch time",
        f"{prefix}007 is a fixing and is not DR007",
    ]
    if stale:
        warnings.append("repo fixing exceeded configured freshness window")
    meta = _date_only_meta(
        source_id=_repo_source_id(family),
        source_url=REPO_SOURCE_URL,
        fetched_at=fetched_at,
        stale=stale,
        warnings=tuple(warnings),
    )
    return RepoFixingObservation(
        family=family,
        session_date=latest_date,
        overnight_percent=_required_non_negative_decimal(
            row.get(columns["overnight"]), f"{prefix}001"
        ),
        seven_day_percent=_required_non_negative_decimal(
            row.get(columns["seven_day"]), f"{prefix}007"
        ),
        fourteen_day_percent=_required_non_negative_decimal(
            row.get(columns["fourteen_day"]), f"{prefix}014"
        ),
        meta=meta,
    )


def _parse_government_curve(
    records: Sequence[Mapping[str, Any]],
    *,
    start_date: date,
    end_date: date,
    fetched_at: datetime,
    stale_after_days: int,
) -> GovernmentBondYieldCurve:
    aliases: dict[str, tuple[str, ...]] = {
        "curve_name": ("曲线名称", "curve_name"),
        "date": ("日期", "date"),
    }
    aliases.update({column: (column,) for column, _ in _TENOR_COLUMNS})
    columns = _resolve_columns(
        records,
        aliases,
        frozenset({"curve_name", "date", "1年", "10年"}),
        GOVERNMENT_CURVE_SOURCE_ID,
    )
    eligible: list[tuple[date, Mapping[str, Any]]] = []
    for row in records:
        if _required_text(row.get(columns["curve_name"]), "curve_name") != _GOVERNMENT_CURVE_NAME:
            continue
        session_date = _parse_date(row.get(columns["date"]), "government curve date")
        if start_date <= session_date <= end_date:
            eligible.append((session_date, row))
    if not eligible:
        raise AShareContextNoDataError(
            "bond_china_yield returned no exact government curve in requested interval"
        )
    latest_date = max(item[0] for item in eligible)
    latest_rows = [row for session_date, row in eligible if session_date == latest_date]
    if len(latest_rows) != 1:
        raise AShareContextPayloadError(
            "bond_china_yield returned duplicate government curves for latest date"
        )
    row = latest_rows[0]
    points: list[GovernmentBondYieldPoint] = []
    for column, tenor in _TENOR_COLUMNS:
        actual = columns.get(column)
        if actual is None:
            continue
        value = _optional_decimal(row.get(actual), f"government curve {column}")
        if value is not None:
            points.append(GovernmentBondYieldPoint(tenor_years=tenor, yield_percent=value))
    if len(points) < 4:
        raise AShareContextPayloadError(
            "government curve contains fewer than four valid tenor points"
        )
    stale = (fetched_at.astimezone(SHANGHAI).date() - latest_date).days > stale_after_days
    warnings = [
        "provider exposes a curve date but no exact publication timestamp",
        "observed_at and available_at therefore equal this collector's fetch time",
    ]
    if stale:
        warnings.append("government curve exceeded configured freshness window")
    return GovernmentBondYieldCurve(
        curve_name=_GOVERNMENT_CURVE_NAME,
        session_date=latest_date,
        points=tuple(points),
        meta=_date_only_meta(
            source_id=GOVERNMENT_CURVE_SOURCE_ID,
            source_url=GOVERNMENT_CURVE_SOURCE_URL,
            fetched_at=fetched_at,
            stale=stale,
            warnings=tuple(warnings),
        ),
    )


def _parse_if_contracts(
    records: Sequence[Mapping[str, Any]],
    *,
    session_date: date,
    fetched_at: datetime,
    stale_after_days: int,
) -> tuple[IFContractDailyObservation, ...]:
    columns = _resolve_columns(records, _IF_ALIASES, _IF_REQUIRED, CFFEX_IF_SOURCE_ID)
    matching: list[Mapping[str, Any]] = []
    for row in records:
        symbol = _required_text(row.get(columns["symbol"]), "IF symbol").upper()
        variety = _required_text(row.get(columns["variety"]), "IF variety").upper()
        row_date = _parse_date(row.get(columns["date"]), "IF date")
        if variety == "IF" and symbol.startswith("IF") and row_date == session_date:
            matching.append(row)
    if not matching:
        raise AShareContextNoDataError(
            f"{CFFEX_IF_SOURCE_ID} returned no IF contracts for {session_date.isoformat()}"
        )
    stale = (fetched_at.astimezone(SHANGHAI).date() - session_date).days > stale_after_days
    warnings = [
        "CFFEX daily rows expose a session date but no publication timestamp",
        "observed_at and available_at therefore equal this collector's fetch time",
        "reported turnover is preserved without an assumed currency multiplier",
    ]
    if stale:
        warnings.append("IF daily row exceeded configured freshness window")
    meta = _date_only_meta(
        source_id=CFFEX_IF_SOURCE_ID,
        source_url=CFFEX_IF_SOURCE_URL,
        fetched_at=fetched_at,
        stale=stale,
        warnings=tuple(warnings),
    )
    contracts: list[IFContractDailyObservation] = []
    for row in matching:
        contracts.append(
            IFContractDailyObservation(
                symbol=_required_text(row.get(columns["symbol"]), "IF symbol").upper(),
                session_date=session_date,
                open=_required_positive_decimal(row.get(columns["open"]), "IF open"),
                high=_required_positive_decimal(row.get(columns["high"]), "IF high"),
                low=_required_positive_decimal(row.get(columns["low"]), "IF low"),
                close=_required_positive_decimal(row.get(columns["close"]), "IF close"),
                settle=_required_positive_decimal(row.get(columns["settle"]), "IF settle"),
                previous_settle=_required_positive_decimal(
                    row.get(columns["pre_settle"]), "IF previous settle"
                ),
                volume=_required_non_negative_integer(row.get(columns["volume"]), "IF volume"),
                open_interest=_required_non_negative_integer(
                    row.get(columns["open_interest"]), "IF open interest"
                ),
                turnover_reported=_required_non_negative_decimal(
                    row.get(columns["turnover"]), "IF reported turnover"
                ),
                meta=meta,
            )
        )
    contracts.sort(key=lambda item: item.symbol)
    symbols = tuple(item.symbol for item in contracts)
    if len(symbols) != len(set(symbols)):
        raise AShareContextPayloadError("CFFEX returned duplicate IF contract symbols")
    return tuple(contracts)


def _provider_records(
    client: Any,
    method_name: str,
    *,
    kwargs: Mapping[str, object] | None = None,
    timeout_seconds: float,
    label: str,
) -> tuple[Mapping[str, Any], ...]:
    method = getattr(client, method_name, None)
    if method is None or not callable(method):
        raise AShareContextEndpointError(f"installed provider has no callable {method_name}")
    try:
        frame = _run_with_timeout(
            partial(method, **dict(kwargs or {})),
            timeout_seconds,
            label,
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except AShareContextDataError:
        raise
    except Exception as exc:
        raise AShareContextDataError(f"{label} failed") from exc
    if frame is None or not hasattr(frame, "to_dict"):
        raise AShareContextPayloadError(f"{label} did not return a DataFrame")
    try:
        records = frame.to_dict(orient="records")
    except (TypeError, ValueError, AttributeError) as exc:
        raise AShareContextPayloadError(f"{label} returned an unreadable DataFrame") from exc
    if not isinstance(records, list) or any(not isinstance(row, Mapping) for row in records):
        raise AShareContextPayloadError(f"{label} returned invalid records")
    if not records:
        raise AShareContextNoDataError(f"{label} returned no rows")
    return tuple(records)


def _run_with_timeout(
    call: Callable[[], _T],
    timeout_seconds: float,
    label: str,
) -> _T:
    result_queue: Queue[tuple[bool, Any]] = Queue(maxsize=1)

    def invoke() -> None:
        try:
            result_queue.put((True, call()))
        except BaseException as exc:
            result_queue.put((False, exc))

    worker = threading.Thread(target=invoke, daemon=True, name="ashare-context-source")
    worker.start()
    worker.join(timeout_seconds)
    if worker.is_alive():
        raise AShareContextTimeoutError(f"{label} exceeded {timeout_seconds:g}s")
    try:
        succeeded, value = result_queue.get_nowait()
    except Empty as exc:  # pragma: no cover - defensive thread invariant
        raise AShareContextDataError(f"{label} ended without a result") from exc
    if succeeded:
        return cast(_T, value)
    if isinstance(value, BaseException):
        raise value
    raise AShareContextDataError(f"{label} returned an invalid thread result")


def _resolve_columns(
    records: Sequence[Mapping[str, Any]],
    aliases: Mapping[str, tuple[str, ...]],
    required: frozenset[str],
    label: str,
) -> dict[str, str]:
    available = {str(key): str(key) for row in records for key in row}
    resolved: dict[str, str] = {}
    for canonical, candidates in aliases.items():
        matches = [candidate for candidate in candidates if candidate in available]
        if len(matches) > 1:
            raise AShareContextPayloadError(
                f"{label} has ambiguous columns for {canonical}: {matches}"
            )
        if matches:
            resolved[canonical] = available[matches[0]]
    missing = sorted(required.difference(resolved))
    if missing:
        raise AShareContextPayloadError(f"{label} missing required columns: {', '.join(missing)}")
    return resolved


def _missing_source(
    context_id: str,
    display_name: str,
    source: str,
    error: AShareContextDataError,
) -> AShareContextMissingSource:
    if isinstance(error, AShareContextTimeoutError):
        failure_code = AShareContextFailureCode.TIMEOUT
    elif isinstance(error, AShareContextEndpointError):
        failure_code = AShareContextFailureCode.ENDPOINT_UNAVAILABLE
    elif isinstance(error, AShareContextPayloadError):
        failure_code = AShareContextFailureCode.INVALID_PAYLOAD
    elif isinstance(error, AShareContextNoDataError):
        failure_code = AShareContextFailureCode.NO_DATA
    else:
        failure_code = AShareContextFailureCode.UPSTREAM_ERROR
    return AShareContextMissingSource(
        context_id=context_id,
        display_name=display_name,
        expected_source=source,
        failure_code=failure_code,
        reason=" ".join(str(error).split()),
    )


def _date_only_meta(
    *,
    source_id: str,
    source_url: str,
    fetched_at: datetime,
    stale: bool,
    warnings: tuple[str, ...],
) -> AShareContextMeta:
    return AShareContextMeta(
        source_id=source_id,
        source_url=source_url,
        observed_at=fetched_at,
        available_at=fetched_at,
        fetched_at=fetched_at,
        stale=stale,
        degraded=stale,
        warnings=warnings,
    )


def _repo_source_id(family: RepoFixingFamily) -> str:
    return FR_SOURCE_ID if family is RepoFixingFamily.FR else FDR_SOURCE_ID


def _optional_row_value(
    row: Mapping[str, Any],
    columns: Mapping[str, str],
    canonical: str,
) -> Any:
    actual = columns.get(canonical)
    return None if actual is None else row.get(actual)


def _normalize_etf_symbol(symbol: str) -> tuple[str, str]:
    value = symbol.strip().upper()
    if len(value) == 6 and value.isdigit():
        suffix = "SH" if value.startswith("5") else "SZ"
        value = f"{value}.{suffix}"
    if len(value) != 9 or value[6] != ".":
        raise ValueError("ETF symbol must look like 510300.SH or 159915.SZ")
    code, exchange = value.split(".", maxsplit=1)
    if len(code) != 6 or not code.isdigit() or exchange not in {"SH", "SZ"}:
        raise ValueError("ETF symbol must look like 510300.SH or 159915.SZ")
    if (exchange == "SH") != code.startswith("5"):
        raise ValueError("ETF symbol exchange is inconsistent with public A-share ETF code")
    return value, code


def _provider_security_code(value: Any) -> str:
    if _is_missing(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6) if text.isdigit() else text


def _required_text(value: Any, field: str) -> str:
    if _is_missing(value):
        raise AShareContextPayloadError(f"{field} is missing")
    text = " ".join(str(value).split())
    if not text:
        raise AShareContextPayloadError(f"{field} is blank")
    return text


def _optional_decimal(value: Any, field: str) -> Decimal | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        raise AShareContextPayloadError(f"{field} cannot be boolean")
    try:
        result = Decimal(str(value).strip().replace(",", ""))
    except (InvalidOperation, ValueError) as exc:
        raise AShareContextPayloadError(f"{field} is not numeric") from exc
    if not result.is_finite():
        raise AShareContextPayloadError(f"{field} is not finite")
    return result


def _required_positive_decimal(value: Any, field: str) -> Decimal:
    result = _optional_decimal(value, field)
    if result is None or result <= 0:
        raise AShareContextPayloadError(f"{field} must be positive")
    return result


def _required_non_negative_decimal(value: Any, field: str) -> Decimal:
    result = _optional_decimal(value, field)
    if result is None or result < 0:
        raise AShareContextPayloadError(f"{field} must be non-negative")
    return result


def _required_non_negative_integer(value: Any, field: str) -> int:
    result = _required_non_negative_decimal(value, field)
    if result != result.to_integral_value():
        raise AShareContextPayloadError(f"{field} must be an integer")
    return int(result)


def _parse_date(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "to_pydatetime"):
        try:
            converted = value.to_pydatetime()
        except (TypeError, ValueError, AttributeError) as exc:
            raise AShareContextPayloadError(f"{field} is invalid") from exc
        if isinstance(converted, datetime):
            return converted.date()
        if isinstance(converted, date):
            return converted
    if _is_missing(value):
        raise AShareContextPayloadError(f"{field} is missing")
    text = str(value).strip().replace("/", "-")
    if len(text) == 8 and text.isdigit():
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise AShareContextPayloadError(f"{field} is not an ISO-compatible date") from exc


def _parse_datetime(value: Any, default_zone: ZoneInfo, field: str) -> datetime:
    if hasattr(value, "to_pydatetime"):
        try:
            value = value.to_pydatetime()
        except (TypeError, ValueError, AttributeError) as exc:
            raise AShareContextPayloadError(f"{field} is invalid") from exc
    if isinstance(value, datetime):
        result = value
    else:
        if _is_missing(value):
            raise AShareContextPayloadError(f"{field} is missing")
        try:
            result = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise AShareContextPayloadError(f"{field} is not ISO-compatible") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        result = result.replace(tzinfo=default_zone)
    return result


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().casefold() in {"", "-", "--", "nan", "nat", "none", "null"}
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Decimal):
        return not value.is_finite()
    return False


def _aware_now(now: Callable[[], datetime]) -> datetime:
    value = now()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now() must return a timezone-aware datetime")
    return value


def _validate_timeout(value: float) -> None:
    if value <= 0:
        raise ValueError("timeout_seconds must be positive")


def _import_akshare() -> Any:
    try:
        import akshare  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - declared dependency
        raise AShareContextEndpointError("akshare is not installed") from exc
    return akshare
