from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from gribuki_trade.domain.instruments import ResearchInstrumentProfile


def _profile() -> ResearchInstrumentProfile:
    return ResearchInstrumentProfile(
        symbol="510300.SH",
        name="沪深300ETF",
        market="A股",
        asset_type="etf",
        exchange="sse",
        board="sse_etf",
        size_tier="cross_size",
        industry="宽基指数",
        styles=("宽基", "大盘"),
        research_role="A股大盘宽基与组合基准",
        risk_tags=("市场风险", "跟踪误差"),
        source_id="ashare-first-broad-coverage-v1",
        verified_on=date(2026, 8, 13),
        background_facts=("基金管理人：华泰柏瑞基金",),
    )


def test_research_instrument_profile_is_an_immutable_complete_snapshot() -> None:
    profile = _profile()

    assert profile.name == "沪深300ETF"
    assert profile.exchange == "sse"
    assert profile.styles == ("宽基", "大盘")
    assert profile.background_facts == ("基金管理人：华泰柏瑞基金",)
    with pytest.raises(AttributeError):
        profile.name = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"name": " "}, "name must not be empty"),
        ({"symbol": "510300.sh"}, "symbol must be uppercase"),
        ({"styles": ()}, "styles must not be empty"),
        ({"risk_tags": ("市场风险", "市场风险")}, "risk_tags values must be unique"),
        ({"background_facts": (" ",)}, "background_facts must contain"),
    ],
)
def test_research_instrument_profile_rejects_ambiguous_metadata(
    changes: dict[str, object],
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        replace(_profile(), **changes)
