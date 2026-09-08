"""NapCat 生命周期模块的边界与兼容导出测试。"""

from __future__ import annotations

import pytest

from gribuki_trade.gui import integrations, napcat_process


def test_integration_facade_reexports_process_boundary() -> None:
    """历史 integrations 导入路径与新生命周期模块保持同一类型。"""

    assert integrations.NapCatLaunchCommand is napcat_process.NapCatLaunchCommand
    assert integrations.ProcessSnapshot is napcat_process.ProcessSnapshot
    assert integrations.NapCatProcessControl is napcat_process.NapCatProcessControl
    assert integrations.QtNapCatProcessController is napcat_process.QtNapCatProcessController
    assert integrations.resolve_napcat_launch is napcat_process.resolve_napcat_launch


def test_launch_resolution_fails_closed_outside_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(napcat_process.os, "name", "posix")

    with pytest.raises(ValueError, match="仅支持 Windows"):
        napcat_process.resolve_napcat_launch("/tmp/napcat")
