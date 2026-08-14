"""面向全 A 股筛选漏斗的时点端口。

契约将低成本标的全集快照与成本较高的历史因子增强分离，因此筛选服务可以先硬过滤
整个市场，再为通过的标的请求历史。

``available_at`` 是两个批次契约的一部分。供应商必须返回在 ``known_at`` 时点可知的
版本；较新的下载时间戳不能证明历史值在更早决策时点已经可用。
"""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, runtime_checkable


class AShareBoard(StrEnum):
    """筛选领域支持的 A 股上市板块。"""

    SSE_MAIN = "SSE_MAIN"
    SZSE_MAIN = "SZSE_MAIN"
    CHINEXT = "CHINEXT"
    STAR = "STAR"
    BSE = "BSE"


class ScreeningSourceQuality(StrEnum):
    """来源是否未回退且满足首选语义。"""

    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"


class ScreeningHistoryPolicy(StrEnum):
    """构建历史因子所用的公司行动策略。

    刻意表示 ``CURRENTLY_ADJUSTED``，使适配器能够报告上游端点的返回内容。历史时点
    运行会拒绝该策略，因为未来公司行动可能重写过去数值。
    """

    UNADJUSTED_WITH_CORPORATE_ACTION_GUARD = (
        "UNADJUSTED_WITH_CORPORATE_ACTION_GUARD"
    )
    POINT_IN_TIME_ADJUSTED = "POINT_IN_TIME_ADJUSTED"
    CURRENTLY_ADJUSTED = "CURRENTLY_ADJUSTED"


class ScreeningFactorId(StrEnum):
    """第一版横截面评分器接受的稳定原始因子。"""

    MOMENTUM_20 = "MOMENTUM_20"
    MOMENTUM_60 = "MOMENTUM_60"
    MOMENTUM_120_SKIP_5 = "MOMENTUM_120_SKIP_5"
    TREND_MA20_OVER_MA60 = "TREND_MA20_OVER_MA60"
    BREAKOUT_20_POSITION = "BREAKOUT_20_POSITION"
    VOLUME_RATIO_20 = "VOLUME_RATIO_20"
    ANNUALIZED_VOLATILITY_60 = "ANNUALIZED_VOLATILITY_60"
    MAX_DRAWDOWN_60_MAGNITUDE = "MAX_DRAWDOWN_60_MAGNITUDE"
    AMIHUD_ILLIQUIDITY_20 = "AMIHUD_ILLIQUIDITY_20"
    AVERAGE_AMOUNT_20_CNY = "AVERAGE_AMOUNT_20_CNY"


@dataclass(frozen=True, slots=True)
class AShareUniverseRecord:
    """硬过滤层使用的低成本交易日结束快照字段。

    状态字段刻意允许为空。``False`` 与“供应商未说明”是不同状态；对后者，硬过滤按
    失败关闭处理。数值范围检查属于过滤器，使排除项保留可读原因，而不会在传输解析时消失。
    """

    symbol: str
    name: str
    board: AShareBoard
    industry: str | None
    listing_days: int | None
    is_tradable: bool | None
    is_st: bool | None
    is_suspended: bool | None
    last_price: Decimal | None
    session_amount_cny: Decimal | None
    market_cap_cny: Decimal | None

    def __post_init__(self) -> None:
        _validate_symbol_board(self.symbol, self.board)
        if not self.name.strip():
            raise ValueError("name must not be empty")
        if self.industry is not None and not self.industry.strip():
            raise ValueError("industry must be non-empty when present")
        if self.listing_days is not None and self.listing_days < 0:
            raise ValueError("listing_days must not be negative")
        for field_name in (
            "last_price",
            "session_amount_cny",
            "market_cap_cny",
        ):
            value = getattr(self, field_name)
            if value is not None and not value.is_finite():
                raise ValueError(f"{field_name} must be finite when present")


@dataclass(frozen=True, slots=True)
class AShareUniverseSnapshot:
    """在精确时点可知的一份全市场快照版本。"""

    as_of: date
    available_at: datetime
    observed_at: datetime
    source_id: str
    source_revision: str
    records: tuple[AShareUniverseRecord, ...]
    quality: ScreeningSourceQuality = ScreeningSourceQuality.COMPLETE
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _validate_batch_metadata(
            available_at=self.available_at,
            observed_at=self.observed_at,
            source_id=self.source_id,
            source_revision=self.source_revision,
            warnings=self.warnings,
        )


@dataclass(frozen=True, slots=True)
class AShareFactorValue:
    """一个原始因子；``None`` 表示缺失且绝不以中性值填充。"""

    factor_id: ScreeningFactorId
    value: float | None

    def __post_init__(self) -> None:
        # 保持可表示供应商的非有限值，使特征层能够公开可审计的 INVALID_FACTOR 降级，
        # 而不是崩溃或静默删除标的。
        if self.value is not None and not isinstance(self.value, (int, float)):
            raise TypeError("factor value must be numeric or None")


@dataclass(frozen=True, slots=True)
class AShareFactorRecord:
    """单个标的仅计算至 ``as_of`` 时点的历史因子。"""

    symbol: str
    values: tuple[AShareFactorValue, ...]
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)
        identifiers = tuple(item.factor_id for item in self.values)
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("factor IDs must be unique within a symbol")
        _validate_warnings(self.warnings)

    def value_for(self, factor_id: ScreeningFactorId) -> float | None:
        """返回原始值且不制造默认值。"""

        return next(
            (item.value for item in self.values if item.factor_id is factor_id),
            None,
        )


@dataclass(frozen=True, slots=True)
class AShareFactorSnapshot:
    """仅面向硬过滤通过者的高成本因子批次。"""

    as_of: date
    available_at: datetime
    observed_at: datetime
    source_id: str
    source_revision: str
    feature_version: str
    history_policy: ScreeningHistoryPolicy
    records: tuple[AShareFactorRecord, ...]
    quality: ScreeningSourceQuality = ScreeningSourceQuality.COMPLETE
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _validate_batch_metadata(
            available_at=self.available_at,
            observed_at=self.observed_at,
            source_id=self.source_id,
            source_revision=self.source_revision,
            warnings=self.warnings,
        )
        if not self.feature_version.strip():
            raise ValueError("feature_version must not be empty")


@runtime_checkable
class AsyncAShareScreeningData(Protocol):
    """全市场漏斗的两阶段时点输入。"""

    async def fetch_universe_snapshot(
        self,
        *,
        as_of: date,
        known_at: datetime,
    ) -> AShareUniverseSnapshot: ...

    async def fetch_factor_snapshot(
        self,
        symbols: Sequence[str],
        *,
        as_of: date,
        known_at: datetime,
    ) -> AShareFactorSnapshot: ...


_SYMBOL_PATTERN = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")


def _validate_symbol(symbol: str) -> None:
    if _SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise ValueError("symbol must look like 600000.SH, 000001.SZ, or 430047.BJ")


def _validate_symbol_board(symbol: str, board: AShareBoard) -> None:
    _validate_symbol(symbol)
    exchange = symbol[-2:]
    expected_exchange = {
        AShareBoard.SSE_MAIN: "SH",
        AShareBoard.STAR: "SH",
        AShareBoard.SZSE_MAIN: "SZ",
        AShareBoard.CHINEXT: "SZ",
        AShareBoard.BSE: "BJ",
    }[board]
    if exchange != expected_exchange:
        raise ValueError("symbol exchange is inconsistent with board")


def _validate_batch_metadata(
    *,
    available_at: datetime,
    observed_at: datetime,
    source_id: str,
    source_revision: str,
    warnings: tuple[str, ...],
) -> None:
    if available_at.tzinfo is None or available_at.utcoffset() is None:
        raise ValueError("available_at must be timezone-aware")
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    if observed_at < available_at:
        raise ValueError("observed_at must not precede available_at")
    if not source_id.strip() or not source_revision.strip():
        raise ValueError("source_id and source_revision must not be empty")
    _validate_warnings(warnings)


def _validate_warnings(warnings: tuple[str, ...]) -> None:
    if any(not item.strip() for item in warnings):
        raise ValueError("warnings must contain non-empty text")
    if len(warnings) != len(set(warnings)):
        raise ValueError("warnings must be unique")


def is_finite_factor_value(value: float | None) -> bool:
    """供需要评分器有限值规则的适配器使用的公共辅助函数。"""

    return value is not None and math.isfinite(value)
