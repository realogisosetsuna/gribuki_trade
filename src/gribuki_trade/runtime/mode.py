"""Mutually exclusive runtime trading modes."""

from enum import StrEnum


class TradingMode(StrEnum):
    """How far an execution path may reach toward a real broker."""

    PAPER = "PAPER"
    SHADOW = "SHADOW"
    LIVE = "LIVE"
