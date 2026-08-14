"""互斥的运行时交易模式。"""

from enum import StrEnum


class TradingMode(StrEnum):
    """执行路径可触及真实券商的程度。"""

    PAPER = "PAPER"
    SHADOW = "SHADOW"
    LIVE = "LIVE"
