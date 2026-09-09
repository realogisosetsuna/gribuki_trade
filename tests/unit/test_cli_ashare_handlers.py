from __future__ import annotations

from pathlib import Path

import pytest

import gribuki_trade.cli as cli
from gribuki_trade.cli_commands.handlers import ashare


def test_ashare_read_only_handlers_remain_facade_compatible(tmp_path: Path) -> None:
    """只读 A 股处理器从 facade 导出时仍指向同一个实现。"""

    assert cli._ashare_source_health is ashare._ashare_source_health
    assert cli._ashare_watchlist is ashare._ashare_watchlist
    assert cli._ashare_screening_run_json is ashare._ashare_screening_run_json
    assert cli._ashare_intraday_run_json is ashare._ashare_intraday_run_json

    health = ashare._ashare_source_health(None, "news_collect", 1, str(tmp_path))
    assert health["operation"] == "news_collect"
    assert health["sources"] == []


@pytest.mark.parametrize(
    "projector",
    [ashare._ashare_screening_run_json, ashare._ashare_intraday_run_json],
)
def test_ashare_result_projectors_reject_untyped_values(projector: object) -> None:
    """结果投影必须接收对应的类型化运行对象，避免静默输出错误数据。"""

    with pytest.raises(TypeError, match="run must be"):
        projector(object())  # type: ignore[operator]
