"""可选宏观与事件语言模型分析端口。"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from gribuki_trade.analysis.schemas import MacroAnalysis, MacroAnalysisRequest

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class AnalyzerAuditIdentity:
    """当前所用精确模型与提示词契约的非敏感身份信息。

    此值可安全保留在审计日志中，并刻意不携带凭据或请求与响应正文。
    """

    provider_id: str
    requested_model: str
    adapter_version: str
    prompt_version: str
    prompt_schema_sha256: str

    def __post_init__(self) -> None:
        for name in (
            "provider_id",
            "requested_model",
            "adapter_version",
            "prompt_version",
        ):
            value = getattr(self, name)
            if not value.strip() or value != value.strip():
                raise ValueError(f"{name} must be normalized and non-empty")
        if not _SHA256.fullmatch(self.prompt_schema_sha256):
            raise ValueError("prompt_schema_sha256 must be a lowercase SHA-256 digest")

    @property
    def manifest_sha256(self) -> str:
        document = {
            "adapter_version": self.adapter_version,
            "prompt_schema_sha256": self.prompt_schema_sha256,
            "prompt_version": self.prompt_version,
            "provider_id": self.provider_id,
            "requested_model": self.requested_model,
        }
        encoded = json.dumps(
            document,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class MacroAnalyzer(Protocol):
    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis: ...


@runtime_checkable
class AuditableMacroAnalyzer(MacroAnalyzer, Protocol):
    """供具有稳定提示词契约的适配器实现的可选扩展。"""

    @property
    def audit_identity(self) -> AnalyzerAuditIdentity: ...


@dataclass(frozen=True, slots=True)
class AnalyzerTokenUsage:
    """Provider 回传的 token 用量；缺失字段保留为 ``None``，不得伪造为零。"""

    input_tokens: int | None = None
    output_tokens: int | None = None
    reasoning_tokens: int | None = None
    total_tokens: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "input_tokens",
            "output_tokens",
            "reasoning_tokens",
            "total_tokens",
        ):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"{name} must be a non-negative integer or None")

    def audit_document(self) -> dict[str, int | None]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "reasoning_tokens": self.reasoning_tokens,
            "total_tokens": self.total_tokens,
        }


@dataclass(frozen=True, slots=True)
class TracedMacroAnalysis:
    analysis: MacroAnalysis
    usage: AnalyzerTokenUsage


@runtime_checkable
class UsageReportingMacroAnalyzer(MacroAnalyzer, Protocol):
    """在同一次调用中返回结果与 provider token 用量，避免并发串号。"""

    async def analyze_with_usage(
        self,
        request: MacroAnalysisRequest,
    ) -> TracedMacroAnalysis: ...


@dataclass(frozen=True, slots=True)
class DualTrackMacroAnalysis:
    """一次语义分析的单分析器与对抗分析器完整对照结果。

    ``selected_analysis`` 是生产决策真正采用的结果；其余字段只用于复核、
    报告和审计，调用方不得根据“更乐观”的分支自行改写已选结果。
    """

    selected_analysis: MacroAnalysis
    baseline_analysis: MacroAnalysis
    adversarial_analysis: MacroAnalysis
    selected_track: str
    failure_code: str | None
    audit_document: Mapping[str, object]
    audit_record_sha256: str | None = None


@runtime_checkable
class DualTrackMacroAnalyzer(MacroAnalyzer, Protocol):
    """可同时执行并返回单分析器与对抗分析器结果的生产扩展端口。"""

    async def analyze_dual(
        self,
        request: MacroAnalysisRequest,
    ) -> DualTrackMacroAnalysis: ...
