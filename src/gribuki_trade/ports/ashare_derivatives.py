"""上交所 ETF 与期权官方研究数据的时点契约。

交易所页面公开的是日终观测，而非可执行报价。具体而言，ETF ``STAT_DATE`` 是结算后
份额统计日期，期权风险数值由该交易日收盘数据计算。官方端点不发布机器可读的发布时间戳，
因此 ``available_at`` 刻意表示采集器首次看见的时间。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol, runtime_checkable


class SSEOfficialDataError(RuntimeError):
    """两个只读上交所官方来源的基础错误。"""


class SSEOptionRiskDataError(SSEOfficialDataError):
    """上交所期权风险来源的基础错误。"""


class SSEOptionRiskTimeoutError(TimeoutError, SSEOptionRiskDataError):
    """上交所期权风险请求超过截止时间。"""


class SSEOptionRiskTransportError(ConnectionError, SSEOptionRiskDataError):
    """无法访问上交所期权风险端点。"""


class SSEOptionRiskHTTPStatusError(SSEOptionRiskDataError):
    """上交所期权风险端点返回不可接受的 HTTP 状态。"""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"official SSE option-risk source returned HTTP {status_code}")


class SSEOptionRiskSchemaError(SSEOptionRiskDataError):
    """期权风险载荷无法无歧义地解释。"""


class SSEOptionRiskNoDataError(SSEOptionRiskDataError):
    """没有可用的合格已完成期权风险交易日。"""


class SSEOptionRiskNotVisibleError(SSEOptionRiskDataError):
    """已拉取期权风险文档首次可见时间晚于 ``as_of``。"""


class SSEETFShareDataError(SSEOfficialDataError):
    """上交所结算后 ETF 份额来源的基础错误。"""


class SSEETFShareTimeoutError(TimeoutError, SSEETFShareDataError):
    """上交所 ETF 份额请求超过截止时间。"""


class SSEETFShareTransportError(ConnectionError, SSEETFShareDataError):
    """无法访问上交所 ETF 份额端点。"""


class SSEETFShareHTTPStatusError(SSEETFShareDataError):
    """上交所 ETF 份额端点返回不可接受的 HTTP 状态。"""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"official SSE ETF-share source returned HTTP {status_code}")


class SSEETFShareSchemaError(SSEETFShareDataError):
    """ETF 份额载荷无法无歧义地解释。"""


class SSEETFShareNoDataError(SSEETFShareDataError):
    """没有可用的合格结算后 ETF 份额观测。"""


class SSEETFShareNotVisibleError(SSEETFShareDataError):
    """已拉取 ETF 份额文档首次可见时间晚于 ``as_of``。"""


@dataclass(frozen=True, slots=True)
class SSEOfficialSourceMeta:
    """一条上交所官方观测的来源与首次可见时间。"""

    source_id: str
    source_url: str
    requested_date: date
    observed_date: date
    available_at: datetime
    fetched_at: datetime
    exact_date_match: bool
    latest_available_fallback: bool
    content_sha256: str
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.source_id.strip() or not self.source_url.strip():
            raise ValueError("official SSE source identifiers cannot be blank")
        _require_aware(self.available_at, "available_at")
        _require_aware(self.fetched_at, "fetched_at")
        if self.available_at > self.fetched_at:
            raise ValueError("available_at cannot follow fetched_at")
        if self.observed_date > self.requested_date:
            raise ValueError("observed_date cannot follow requested_date")
        if self.exact_date_match != (self.observed_date == self.requested_date):
            raise ValueError("exact_date_match does not match observed_date")
        if self.latest_available_fallback == self.exact_date_match:
            raise ValueError("exact and latest-available flags must be mutually exclusive")
        if len(self.content_sha256) != 64:
            raise ValueError("content_sha256 must be a SHA-256 hex digest")
        try:
            int(self.content_sha256, 16)
        except ValueError as exc:
            raise ValueError("content_sha256 must be a SHA-256 hex digest") from exc
        if any(not warning.strip() for warning in self.warnings):
            raise ValueError("official SSE source warnings cannot be blank")


@dataclass(frozen=True, slots=True)
class SSEOptionRiskContract:
    """一条保留来源小数精度的上交所官方收盘风险记录。"""

    security_id: str
    contract_id: str
    contract_symbol: str
    contract_type: str
    delta: Decimal
    theta: Decimal
    gamma: Decimal
    vega: Decimal
    rho: Decimal
    implied_volatility: Decimal
    raw_fields: tuple[tuple[str, str], ...] = field(repr=False)

    def __post_init__(self) -> None:
        if any(
            not value.strip()
            for value in (
                self.security_id,
                self.contract_id,
                self.contract_symbol,
                self.contract_type,
            )
        ):
            raise ValueError("option contract identifiers cannot be blank")
        values = (
            self.delta,
            self.theta,
            self.gamma,
            self.vega,
            self.rho,
            self.implied_volatility,
        )
        if any(not value.is_finite() for value in values):
            raise ValueError("option risk values must be finite")
        if self.gamma < 0 or self.vega < 0 or self.implied_volatility < 0:
            raise ValueError("Gamma, Vega, and implied volatility cannot be negative")
        if self.contract_type == "认购" and not Decimal("0") <= self.delta <= Decimal("1"):
            raise ValueError("call Delta must be between zero and one")
        if self.contract_type == "认沽" and not Decimal("-1") <= self.delta <= Decimal("0"):
            raise ValueError("put Delta must be between minus one and zero")
        if self.contract_type not in {"认购", "认沽"}:
            raise ValueError("contract_type must be 认购 or 认沽")
        names = tuple(name for name, _ in self.raw_fields)
        empty_raw_field = any(
            not name or not value for name, value in self.raw_fields
        )
        if len(names) != len(set(names)) or empty_raw_field:
            raise ValueError("raw option fields must be non-empty and unique")


@dataclass(frozen=True, slots=True)
class SSEOptionRiskSnapshot:
    """一个上交所 ETF 标的在某日期的全部官方收盘风险记录。"""

    underlying_symbol: str
    underlying_name: str
    contracts: tuple[SSEOptionRiskContract, ...]
    meta: SSEOfficialSourceMeta

    def __post_init__(self) -> None:
        if not self.underlying_symbol.strip() or not self.underlying_name.strip():
            raise ValueError("option underlying identifiers cannot be blank")
        if not self.contracts:
            raise ValueError("option-risk snapshot cannot be empty")
        security_ids = tuple(item.security_id for item in self.contracts)
        contract_ids = tuple(item.contract_id for item in self.contracts)
        if len(security_ids) != len(set(security_ids)):
            raise ValueError("option-risk snapshot has duplicate security IDs")
        if len(contract_ids) != len(set(contract_ids)):
            raise ValueError("option-risk snapshot has duplicate contract IDs")


@dataclass(frozen=True, slots=True)
class SSEETFShareObservation:
    """一条上交所官方结算后 ETF 总份额观测。

    ``total_shares_ten_thousands`` 是上交所原样发布的字段；``total_shares`` 是精确
    单位换算，并非净值或资产管理规模估计。
    """

    symbol: str
    name: str
    expanded_name: str | None
    etf_type: str | None
    total_shares_ten_thousands: Decimal
    total_shares: Decimal
    raw_total_shares: str
    meta: SSEOfficialSourceMeta

    def __post_init__(self) -> None:
        if not self.symbol.strip() or not self.name.strip() or not self.raw_total_shares.strip():
            raise ValueError("ETF share identifiers and raw value cannot be blank")
        if self.expanded_name is not None and not self.expanded_name.strip():
            raise ValueError("expanded ETF name cannot be blank")
        if self.etf_type is not None and not self.etf_type.strip():
            raise ValueError("ETF type cannot be blank")
        if not self.total_shares_ten_thousands.is_finite() or self.total_shares_ten_thousands < 0:
            raise ValueError("ETF total shares in ten-thousands must be non-negative and finite")
        if self.total_shares != self.total_shares_ten_thousands * Decimal("10000"):
            raise ValueError("ETF total_shares unit conversion is inconsistent")


@runtime_checkable
class AsyncSSEOptionRiskData(Protocol):
    async def fetch_option_risk(
        self,
        underlying_symbol: str,
        requested_date: date,
        *,
        as_of: datetime | None = None,
        allow_latest_available: bool = True,
    ) -> SSEOptionRiskSnapshot: ...


@runtime_checkable
class AsyncSSEETFShareData(Protocol):
    async def fetch_etf_shares(
        self,
        symbol: str,
        requested_date: date,
        *,
        as_of: datetime | None = None,
        allow_latest_available: bool = True,
    ) -> SSEETFShareObservation: ...


def digest_official_payload(body: bytes) -> str:
    """返回来源元数据使用的精确文档摘要。"""

    return hashlib.sha256(body).hexdigest()


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
