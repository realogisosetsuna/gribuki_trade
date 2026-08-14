"""与研究记录一同保留且独立于券商的标的元数据。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, slots=True)
class ResearchInstrumentProfile:
    """单个研究标的具有时点约束的描述性元数据。

    档案刻意只作描述：不能识别账户，也不能授权订单。在推荐记录中保留快照，
    可确保观察列表分类日后调整时旧报告仍可复现。
    """

    symbol: str
    name: str
    market: str
    asset_type: str
    exchange: str
    board: str
    size_tier: str
    industry: str
    styles: tuple[str, ...]
    research_role: str
    risk_tags: tuple[str, ...]
    source_id: str
    verified_on: date
    background_facts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field_name in (
            "symbol",
            "name",
            "market",
            "asset_type",
            "exchange",
            "board",
            "size_tier",
            "industry",
            "research_role",
            "source_id",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"instrument profile {field_name} must not be empty")
            if value != value.strip():
                raise ValueError(
                    f"instrument profile {field_name} must not have surrounding whitespace"
                )
        if self.symbol != self.symbol.upper():
            raise ValueError("instrument profile symbol must be uppercase")
        _validate_tags(self.styles, "styles")
        _validate_tags(self.risk_tags, "risk_tags")
        if self.background_facts:
            _validate_tags(self.background_facts, "background_facts")


def _validate_tags(values: tuple[str, ...], field_name: str) -> None:
    if not values:
        raise ValueError(f"instrument profile {field_name} must not be empty")
    if any(not value.strip() or value != value.strip() for value in values):
        raise ValueError(
            f"instrument profile {field_name} must contain normalized non-empty text"
        )
    if len(values) != len(set(values)):
        raise ValueError(f"instrument profile {field_name} values must be unique")
