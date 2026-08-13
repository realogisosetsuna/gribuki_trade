"""Qt application bootstrap helpers."""

from __future__ import annotations

import sys
from collections.abc import Sequence

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

from .main_window import TradingMainWindow


def create_application(argv: Sequence[str] | None = None) -> QApplication:
    """Return the process QApplication, creating it when necessary."""

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
    """Start the desktop workstation."""

    app = create_application(argv)
    window = TradingMainWindow()
    window.show()
    return app.exec()
