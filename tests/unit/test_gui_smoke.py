"""Headless smoke tests for the desktop shell."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QLabel, QTableWidget  # noqa: E402

from gribuki_trade.gui import TradingMainWindow, create_application  # noqa: E402


def test_main_window_can_be_created_and_closed_offscreen() -> None:
    app = create_application(["gribuki-trade-test"])
    assert isinstance(app, QApplication)

    window = TradingMainWindow()
    window.show()
    app.processEvents()

    assert window.windowTitle() == "Gribuki Trade · PAPER 工作台"
    assert window.workspace_tabs.count() == 7
    assert [window.workspace_tabs.tabText(index) for index in range(7)] == [
        "总览",
        "K线",
        "资讯 / 建议",
        "策略参数",
        "回测报告",
        "订单 / 成交",
        "风控 / 日志",
    ]
    assert "PAPER" in window.statusBar().currentMessage()

    window.close()
    window.deleteLater()
    app.processEvents()


def test_research_tab_starts_without_fabricated_recommendations() -> None:
    app = create_application(["gribuki-trade-research-test"])
    window = TradingMainWindow()

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
    assert napcat_status.text().startswith("未配置")

    window.close()
    window.deleteLater()
    app.processEvents()
