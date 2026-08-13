"""Port for optional macro/event language-model analysis."""

from __future__ import annotations

from typing import Protocol

from gribuki_trade.analysis.schemas import MacroAnalysis, MacroAnalysisRequest


class MacroAnalyzer(Protocol):
    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis: ...

