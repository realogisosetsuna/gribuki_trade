"""桌面外壳的无头冒烟测试。"""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from collections.abc import Callable  # noqa: E402
from typing import TypeVar  # noqa: E402

from PySide6.QtWidgets import QApplication, QLabel, QTableWidget  # noqa: E402

from gribuki_trade.gui import TradingMainWindow, create_application  # noqa: E402
from gribuki_trade.gui.integrations import (  # noqa: E402
    DeepSeekHealth,
    IntegrationDependencies,
    NapCatHealth,
    NapCatLaunchCommand,
    ProcessSnapshot,
)

_T = TypeVar("_T")


class _InlineExecutor:
    def submit(
        self,
        operation: Callable[[], _T],
        on_success: Callable[[_T], None],
        on_failure: Callable[[str], None],
    ) -> None:
        try:
            result = operation()
        except Exception:
            on_failure("后台操作失败；详细信息已隐藏。")
        else:
            on_success(result)


class _Preferences:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get(self, name: str, default: str) -> str:
        return self.values.get(name, default)

    def set(self, name: str, value: str) -> None:
        self.values[name] = value


class _Gateway:
    def check_napcat(self, _base_url: str) -> NapCatHealth:
        return NapCatHealth(True, True, True, "NapCat.OneBot", "OneBot 可达，QQ 已登录。")

    def check_deepseek(self, selected_model: str) -> DeepSeekHealth:
        return DeepSeekHealth(
            True,
            True,
            selected_model,
            True,
            (selected_model,),
            "API 可用，所选模型可用。",
        )

    def save_deepseek_key(self, _api_key: str) -> None:
        return None


class _Process:
    def __init__(self) -> None:
        self._listener: Callable[[ProcessSnapshot], None] | None = None

    def set_listener(self, listener: Callable[[ProcessSnapshot], None]) -> None:
        self._listener = listener
        listener(self.snapshot())

    def snapshot(self) -> ProcessSnapshot:
        return ProcessSnapshot(False, False, False, "本窗口当前未持有 NapCat 进程。")

    def start(self, _command: NapCatLaunchCommand) -> bool:
        return True

    def stop_owned(self) -> bool:
        return False


def _integration_dependencies() -> IntegrationDependencies:
    return IntegrationDependencies(
        gateway=_Gateway(),
        preferences=_Preferences(),
        executor=_InlineExecutor(),
        process=_Process(),
        open_browser=lambda _url: True,
    )


def test_main_window_can_be_created_and_closed_offscreen() -> None:
    app = create_application(["gribuki-trade-test"])
    assert isinstance(app, QApplication)

    window = TradingMainWindow(integration_dependencies=_integration_dependencies())
    window.show()
    app.processEvents()

    assert window.windowTitle() == "Gribuki Trade · PAPER 工作台"
    assert window.workspace_tabs.count() == 8
    assert [window.workspace_tabs.tabText(index) for index in range(8)] == [
        "总览",
        "K线",
        "资讯 / 建议",
        "策略参数",
        "回测报告",
        "订单 / 成交",
        "风控 / 日志",
        "集成管理",
    ]
    assert "PAPER" in window.statusBar().currentMessage()

    window.close()
    window.deleteLater()
    app.processEvents()


def test_research_tab_starts_without_fabricated_recommendations() -> None:
    app = create_application(["gribuki-trade-research-test"])
    window = TradingMainWindow(integration_dependencies=_integration_dependencies())

    sources = window.findChild(QTableWidget, "researchSourceStatus")
    evidence = window.findChild(QTableWidget, "researchEvidenceTable")
    recommendations = window.findChild(QTableWidget, "researchRecommendationTable")
    napcat_status = window.findChild(QLabel, "napcatStatus")

    assert sources is not None
    assert [sources.item(row, 2).text() for row in range(sources.rowCount())] == [
        "尚未运行",
        "尚未运行",
        "尚未运行",
    ]
    assert evidence is not None
    assert evidence.item(0, 1).text() == "尚未运行"
    assert recommendations is not None
    assert [recommendations.item(row, 2).text() for row in range(2)] == [
        "尚未运行",
        "尚未运行",
    ]
    assert napcat_status is not None
    app.processEvents()
    assert napcat_status.text().startswith("已配置")
    assert "QQ 已登录" in napcat_status.text()

    window.close()
    window.deleteLater()
    app.processEvents()
