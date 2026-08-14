"""中国官方外汇与货币市场定盘数据的只读契约。

本模块中的观测是研究输入，而非可执行报价。每个值都带有显式单位、报价约定、计划
发布时间和 HTTP 来源，使调用方不能静默混淆百分数与小数利率，也不能混淆 SAFE 的
``100 USD`` 展示单位与 ``USD/CNY``。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, runtime_checkable


class OfficialRatesDataError(RuntimeError):
    """官方利率适配器抛出的基础失败。"""


class SafeCentralParityError(OfficialRatesDataError):
    """SAFE 中间价数据源的基础失败。"""


class ShiborDataError(OfficialRatesDataError):
    """官方 Shibor 数据源的基础失败。"""


class SafeCentralParityTimeoutError(TimeoutError, SafeCentralParityError):
    """SAFE 查询超过已配置的期限。"""


class ShiborTimeoutError(TimeoutError, ShiborDataError):
    """官方 Shibor 查询超过已配置的期限。"""


class SafeCentralParityTransportError(ConnectionError, SafeCentralParityError):
    """无法访问 SAFE HTTPS 端点。"""


class ShiborTransportError(ConnectionError, ShiborDataError):
    """无法访问官方 Shibor HTTPS 端点。"""


class SafeCentralParityHTTPStatusError(SafeCentralParityError):
    """SAFE 返回了不可接受的 HTTP 状态。"""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"SAFE central-parity source returned HTTP {status_code}")


class ShiborHTTPStatusError(ShiborDataError):
    """官方 Shibor 端点返回了不可接受的 HTTP 状态。"""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"official Shibor source returned HTTP {status_code}")


class SafeCentralParitySchemaError(SafeCentralParityError):
    """无法无歧义地解析 SAFE HTML。"""


class ShiborSchemaError(ShiborDataError):
    """无法无歧义地解析官方 Shibor JSON。"""


class SafeCentralParityNoDataError(SafeCentralParityError):
    """请求时点没有可见的 SAFE 观测。"""


class ShiborNoDataError(ShiborDataError):
    """请求时点没有可见的 Shibor 观测。"""


class FXQuoteConvention(StrEnum):
    """标准化货币对报价的方向。"""

    QUOTE_CURRENCY_PER_BASE_CURRENCY = "QUOTE_CURRENCY_PER_BASE_CURRENCY"


class InterestRateUnit(StrEnum):
    """官方 Shibor 发布使用的显式单位。"""

    PERCENT_PER_ANNUM = "PERCENT_PER_ANNUM"


class ShiborTenor(StrEnum):
    OVERNIGHT = "O/N"
    ONE_WEEK = "1W"
    TWO_WEEK = "2W"
    ONE_MONTH = "1M"
    THREE_MONTH = "3M"
    SIX_MONTH = "6M"
    NINE_MONTH = "9M"
    ONE_YEAR = "1Y"


@dataclass(frozen=True, slots=True)
class OfficialRateSourceMeta:
    """一个官方文档的来源与发布时间。"""

    source_id: str
    source_url: str
    available_at: datetime
    fetched_at: datetime
    stale: bool
    content_sha256: str
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.source_id.strip() or not self.source_url.strip():
            raise ValueError("official-rate source identifiers cannot be blank")
        _require_aware(self.available_at, "available_at")
        _require_aware(self.fetched_at, "fetched_at")
        if self.available_at > self.fetched_at:
            raise ValueError("available_at cannot follow fetched_at")
        if len(self.content_sha256) != 64:
            raise ValueError("content_sha256 must be a SHA-256 hex digest")
        try:
            int(self.content_sha256, 16)
        except ValueError as exc:
            raise ValueError("content_sha256 must be a SHA-256 hex digest") from exc
        if any(not warning.strip() for warning in self.warnings):
            raise ValueError("official-rate warnings cannot be blank")


@dataclass(frozen=True, slots=True)
class USDCNYCentralParityObservation:
    """一条 SAFE USD/CNY 中间价，标准化为每一美元对应的人民币数值。

    SAFE 将此序列展示为 ``100 USD = X CNY``。精确来源值与标准化市场约定均会保留并
    交叉校验。
    """

    session_date: date
    base_currency: str
    quote_currency: str
    quote_convention: FXQuoteConvention
    cny_per_usd: Decimal
    source_base_amount_usd: Decimal
    source_quote_amount_cny: Decimal
    observed_at: datetime
    available_at: datetime

    def __post_init__(self) -> None:
        if self.base_currency != "USD" or self.quote_currency != "CNY":
            raise ValueError("central-parity currencies must be USD/CNY")
        if (
            self.quote_convention
            is not FXQuoteConvention.QUOTE_CURRENCY_PER_BASE_CURRENCY
        ):
            raise ValueError("USD/CNY must be quoted as CNY per USD")
        for field_name in (
            "cny_per_usd",
            "source_base_amount_usd",
            "source_quote_amount_cny",
        ):
            value = getattr(self, field_name)
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{field_name} must be positive and finite")
        if self.source_base_amount_usd != Decimal("100"):
            raise ValueError("SAFE USD source amount must be exactly 100 USD")
        if self.cny_per_usd != (
            self.source_quote_amount_cny / self.source_base_amount_usd
        ):
            raise ValueError("normalized USD/CNY does not match SAFE's source quote")
        _validate_observation_timing(
            self.session_date,
            self.observed_at,
            self.available_at,
            label="SAFE USD/CNY",
        )


@dataclass(frozen=True, slots=True)
class SafeCentralParityHistory:
    """在 ``as_of`` 可见的严格有序 SAFE USD/CNY 观测。"""

    as_of: datetime
    start_date: date
    end_date: date
    observations: tuple[USDCNYCentralParityObservation, ...]
    meta: OfficialRateSourceMeta

    def __post_init__(self) -> None:
        _validate_history(
            self.as_of,
            self.start_date,
            self.end_date,
            tuple(item.session_date for item in self.observations),
            tuple(item.available_at for item in self.observations),
            self.meta,
            label="SAFE central-parity",
        )


@dataclass(frozen=True, slots=True)
class ShiborRate:
    """一个以年化百分点表示的期限值（并非小数比例）。"""

    tenor: ShiborTenor
    value_percent: Decimal

    def __post_init__(self) -> None:
        if not self.value_percent.is_finite() or not (
            Decimal("0") <= self.value_percent <= Decimal("100")
        ):
            raise ValueError("Shibor value_percent must be finite and between 0 and 100")


@dataclass(frozen=True, slots=True)
class ShiborDailyObservation:
    """一条包含八个期限的官方 Shibor 定盘记录。

    官方发布将 Shibor 定义为简单、无担保的批发市场报价利率，以年化百分数报价，
    采用 ACT/360 与 T+0 结算。
    """

    session_date: date
    rates: tuple[ShiborRate, ...]
    unit: InterestRateUnit
    day_count: str
    settlement: str
    observed_at: datetime
    available_at: datetime

    def __post_init__(self) -> None:
        expected = tuple(ShiborTenor)
        actual = tuple(rate.tenor for rate in self.rates)
        if actual != expected:
            raise ValueError("Shibor rates must contain all eight tenors in official order")
        if self.unit is not InterestRateUnit.PERCENT_PER_ANNUM:
            raise ValueError("Shibor unit must be annual percentage points")
        if self.day_count != "ACT/360" or self.settlement != "T+0":
            raise ValueError("Shibor convention must be ACT/360 and T+0")
        _validate_observation_timing(
            self.session_date,
            self.observed_at,
            self.available_at,
            label="Shibor",
        )

    def rate(self, tenor: ShiborTenor) -> Decimal:
        """返回一个年化百分点值，不改变单位。"""

        return next(item.value_percent for item in self.rates if item.tenor is tenor)


@dataclass(frozen=True, slots=True)
class ShiborHistory:
    """在 ``as_of`` 可见的严格有序官方 Shibor 定盘数据。"""

    as_of: datetime
    start_date: date
    end_date: date
    observations: tuple[ShiborDailyObservation, ...]
    meta: OfficialRateSourceMeta

    def __post_init__(self) -> None:
        _validate_history(
            self.as_of,
            self.start_date,
            self.end_date,
            tuple(item.session_date for item in self.observations),
            tuple(item.available_at for item in self.observations),
            self.meta,
            label="Shibor",
        )


@runtime_checkable
class AsyncSafeCentralParityData(Protocol):
    async def fetch_usd_cny_history(
        self,
        *,
        start_date: date,
        end_date: date,
        as_of: datetime,
    ) -> SafeCentralParityHistory: ...


@runtime_checkable
class AsyncShiborData(Protocol):
    async def fetch_shibor_history(
        self,
        *,
        start_date: date,
        end_date: date,
        as_of: datetime,
    ) -> ShiborHistory: ...


def content_sha256(body: bytes) -> str:
    """返回供来源测试与证据构建器使用的稳定摘要。"""

    return hashlib.sha256(body).hexdigest()


def _validate_history(
    as_of: datetime,
    start_date: date,
    end_date: date,
    session_dates: tuple[date, ...],
    available_times: tuple[datetime, ...],
    meta: OfficialRateSourceMeta,
    *,
    label: str,
) -> None:
    _require_aware(as_of, "as_of")
    if start_date > end_date:
        raise ValueError("start_date cannot follow end_date")
    if not session_dates:
        raise ValueError(f"{label} history cannot be empty")
    if session_dates != tuple(sorted(session_dates)):
        raise ValueError(f"{label} dates must be strictly ordered")
    if len(session_dates) != len(set(session_dates)):
        raise ValueError(f"{label} dates must be unique")
    if any(not (start_date <= item <= end_date) for item in session_dates):
        raise ValueError(f"{label} observation is outside the requested range")
    if any(item > as_of for item in available_times):
        raise ValueError(f"{label} history contains data unavailable at as_of")
    if meta.available_at != available_times[-1]:
        raise ValueError(f"{label} metadata must describe the latest observation")


def _validate_observation_timing(
    session_date: date,
    observed_at: datetime,
    available_at: datetime,
    *,
    label: str,
) -> None:
    _require_aware(observed_at, f"{label} observed_at")
    _require_aware(available_at, f"{label} available_at")
    if observed_at > available_at:
        raise ValueError(f"{label} observed_at cannot follow available_at")
    if session_date != observed_at.date():
        raise ValueError(f"{label} session_date must match observed_at local date")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
