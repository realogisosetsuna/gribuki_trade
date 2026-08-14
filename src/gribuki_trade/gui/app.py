"""Qt 应用程序启动辅助工具。"""

from __future__ import annotations

import sys
from collections.abc import Sequence

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

from .main_window import TradingMainWindow


def create_application(argv: Sequence[str] | None = None) -> QApplication:
    """返回进程的 QApplication，必要时创建它。"""

    existing = QApplication.instance()
    if isinstance(existing, QApplication):
        return existing

    QApplication.setHighDpiScaleFactorRoundingPolicy(
        Qt.HighDpiScaleFactorRoundingPolicy.PassThrough
    )
    app = QApplication(list(argv) if argv is not None else sys.argv)
    app.setApplicationName("Gribuki Trade")
    app.setOrganizationName("Gribuki")
    app.setApplicationDisplayName("Gribuki Trade · PAPER 工作台")
    return app


def run(argv: Sequence[str] | None = None) -> int:
    """启动桌面工作台。"""

    app = create_application(argv)
    window = TradingMainWindow()
    window.show()
    return app.exec()
