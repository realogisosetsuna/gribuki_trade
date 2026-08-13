from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from gribuki_trade.watchlists import (
    WatchlistAssetType,
    WatchlistBoard,
    WatchlistConfigError,
    WatchlistExchange,
    WatchlistSizeTier,
    load_research_watchlist,
)

REPOSITORY_ROOT = Path(__file__).parents[2]
WATCHLIST_PATH = REPOSITORY_ROOT / "config" / "ashare_research_watchlist.toml"


def test_first_ashare_watchlist_loads_with_expected_coverage() -> None:
    watchlist = load_research_watchlist(WATCHLIST_PATH)

    assert watchlist.schema_version == 1
    assert watchlist.watchlist_id == "ashare-first-broad-coverage-v1"
    assert watchlist.verified_on == date(2026, 8, 13)
    assert len(watchlist.instruments) == 45
    assert len(watchlist.stocks) == 36
    assert len(watchlist.etfs) == 9
    assert len(watchlist.symbols) == len(set(watchlist.symbols))
    assert len(watchlist.default_symbols) == 18
    assert set(watchlist.default_symbols) <= set(watchlist.symbols)
    assert {item.board for item in watchlist.instruments} == set(WatchlistBoard)
    assert {item.size_tier for item in watchlist.instruments} == set(WatchlistSizeTier)

    by_symbol = {item.symbol: item for item in watchlist.instruments}
    assert by_symbol["600519.SH"].name == "贵州茅台"
    assert by_symbol["300750.SZ"].board is WatchlistBoard.CHINEXT
    assert by_symbol["688981.SH"].board is WatchlistBoard.STAR
    assert by_symbol["688116.SH"].size_tier is WatchlistSizeTier.SMALL
    assert by_symbol["510300.SH"].asset_type is WatchlistAssetType.ETF
    assert by_symbol["510300.SH"].exchange is WatchlistExchange.SSE
    assert by_symbol["159915.SZ"].board is WatchlistBoard.SZSE_ETF
    assert by_symbol["159915.SZ"].exchange is WatchlistExchange.SZSE

    profile = watchlist.instrument_profile("510300")
    assert profile is not None
    assert profile.symbol == "510300.SH"
    assert profile.name == "华泰柏瑞沪深300交易型开放式指数证券投资基金"
    assert profile.market == "A股"
    assert profile.asset_type == "etf"
    assert profile.exchange == "sse"
    assert profile.board == "sse_etf"
    assert profile.industry == "宽基指数"
    assert profile.styles
    assert profile.research_role
    assert profile.risk_tags
    assert any("基金管理人" in item for item in profile.background_facts)
    assert any("2012-05-28" in item for item in profile.background_facts)
    assert profile.source_id == watchlist.watchlist_id
    assert profile.verified_on == watchlist.verified_on


def test_watchlist_profile_lookup_returns_none_for_valid_unknown_symbol() -> None:
    watchlist = load_research_watchlist(WATCHLIST_PATH)

    assert watchlist.find_instrument("600001.SH") is None
    assert watchlist.instrument_profile("600001") is None


def test_every_instrument_carries_research_role_and_risk_metadata() -> None:
    watchlist = load_research_watchlist(WATCHLIST_PATH)

    assert all(item.industry for item in watchlist.instruments)
    assert all(item.styles for item in watchlist.instruments)
    assert all(item.role for item in watchlist.instruments)
    assert all(item.risk_tags for item in watchlist.instruments)
    industries = {item.industry for item in watchlist.instruments}
    assert {
        "银行",
        "高端白酒",
        "工业自动化",
        "创新药",
        "石油石化",
        "水力发电",
        "集装箱航运",
        "晶圆代工",
        "宽基指数",
    } <= industries


def test_duplicate_symbol_is_rejected(tmp_path: Path) -> None:
    instrument = _instrument()
    path = tmp_path / "duplicate.toml"
    path.write_text(_metadata() + instrument + instrument, encoding="utf-8")

    with pytest.raises(WatchlistConfigError, match="unique"):
        load_research_watchlist(path)


@pytest.mark.parametrize(
    "replacement",
    [
        'symbol = "600000.SZ"',
        'board = "chinext"',
        'asset_type = "etf"',
    ],
)
def test_symbol_asset_type_and_board_must_be_consistent(
    tmp_path: Path,
    replacement: str,
) -> None:
    instrument = _instrument()
    if replacement.startswith("symbol"):
        instrument = instrument.replace('symbol = "600000.SH"', replacement)
    elif replacement.startswith("board"):
        instrument = instrument.replace('board = "sse_main"', replacement)
    else:
        instrument = instrument.replace('asset_type = "stock"', replacement)
    path = tmp_path / "invalid.toml"
    path.write_text(_metadata() + instrument, encoding="utf-8")

    with pytest.raises(WatchlistConfigError):
        load_research_watchlist(path)


def test_empty_or_duplicate_metadata_tags_are_rejected(tmp_path: Path) -> None:
    instrument = _instrument().replace('styles = ["价值"]', 'styles = ["价值", "价值"]')
    path = tmp_path / "invalid-tags.toml"
    path.write_text(_metadata() + instrument, encoding="utf-8")

    with pytest.raises(WatchlistConfigError, match="styles values must be unique"):
        load_research_watchlist(path)


def _metadata() -> str:
    return """
[watchlist]
schema_version = 1
id = "test"
title = "Test"
purpose = "Research only"
verified_on = 2026-08-13
verification_sources = ["public listing"]
selection_rules = ["coverage"]
default_symbols = ["600000.SH"]
"""


def _instrument() -> str:
    return """
[[instrument]]
symbol = "600000.SH"
name = "浦发银行"
asset_type = "stock"
board = "sse_main"
size_tier = "large"
industry = "银行"
styles = ["价值"]
role = "银行研究样本"
risk_tags = ["资产质量"]
"""
