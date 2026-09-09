"""对抗宏观分析的协议、配置与不可变结果模型。

本模块只定义角色协议、调用预算和审计结果值对象；网络调用、轮次编排
与失败策略仍由 ``adversarial_macro`` 服务门面负责。
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Final

from gribuki_trade.analysis.schemas import (
    MacroAnalysis,
    MacroAnalysisRequest,
)
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
from gribuki_trade.services.adversarial_macro_serialization import (
    _analysis_document,
    _evidence_pack_sha256,
    _identity_document,
    _request_sha256,
)

_FAILURE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,79}$")
_ADAPTER_VERSION: Final = "adversarial-macro-wrapper@1"
_AGGREGATION_VERSION: Final = "conservative-median@1"
_PEER_ENVELOPE_VERSION: Final = "untrusted-peer-arguments@1"


class AdversarialMacroDepth(StrEnum):
    """命名的延迟/深度档位；任何档位都不允许无限轮次。"""

    FAST = "FAST"
    STANDARD = "STANDARD"
    DEEP = "DEEP"


class AdversarialMacroRole(StrEnum):
    """职责刻意分离、用于发现不同类型错误的分析角色。"""

    CATALYST_ADVOCATE = "CATALYST_ADVOCATE"
    RISK_CHALLENGER = "RISK_CHALLENGER"
    EVIDENCE_AUDITOR = "EVIDENCE_AUDITOR"
    MARKET_REGIME_ANALYST = "MARKET_REGIME_ANALYST"
    EXECUTION_RISK_AUDITOR = "EXECUTION_RISK_AUDITOR"


class AdversarialFeatureMode(StrEnum):
    """兼容包装器的显式发布状态。"""

    BASELINE = "BASELINE"
    SHADOW = "SHADOW"
    ENFORCE = "ENFORCE"


class AdversarialTermination(StrEnum):
    """稳定且可安全进入审计记录的 case 终止原因。"""

    MAX_ROUNDS_REACHED = "MAX_ROUNDS_REACHED"
    STABLE_CONSENSUS = "STABLE_CONSENSUS"
    CRITICAL_FAILURE = "CRITICAL_FAILURE"
    SESSION_BUDGET_EXHAUSTED = "SESSION_BUDGET_EXHAUSTED"
    CASE_DEADLINE_EXCEEDED = "CASE_DEADLINE_EXCEEDED"


_ROLE_CHARTERS: Final[Mapping[AdversarialMacroRole, tuple[str, ...]]] = {
    AdversarialMacroRole.CATALYST_ADVOCATE: (
        "Construct the strongest evidence-backed case that the supplied event and market context "
        "supports the deterministic technical setup.",
        "State contrary evidence and at least one concrete falsification condition; do not invent "
        "prices, indicators, orders, or facts.",
    ),
    AdversarialMacroRole.RISK_CHALLENGER: (
        "Challenge the proposed setup and search for contradictory evidence, stale context, "
        "causal overreach, crowding, liquidity, gap, price-limit, and T+1 risk.",
        "Use specific evidence and at least one falsification condition; generic risk disclaimers "
        "are not a valid answer.",
    ),
    AdversarialMacroRole.EVIDENCE_AUDITOR: (
        "Audit source independence, chronology, relevance, contradictions, and the boundary "
        "between observations and inferences.",
        "Do not count another role's statement as evidence and do not make unsupported numerical "
        "calculations; identify at least one condition that would falsify a material claim.",
    ),
    AdversarialMacroRole.MARKET_REGIME_ANALYST: (
        "Assess A-share liquidity, breadth, policy, sector, volatility, and cross-market regime "
        "only from supplied evidence.",
        "Label transmission mechanisms as hypotheses, surface counter-evidence, and provide a "
        "falsification condition.",
    ),
    AdversarialMacroRole.EXECUTION_RISK_AUDITOR: (
        "Audit whether the evidence changes execution risk under A-share lots, T+1, liquidity, "
        "price limits, gaps, and invalidation constraints.",
        "Never create or alter an order, price, quantity, or deterministic risk rule; provide a "
        "falsification condition for every directional conclusion.",
    ),
}


@dataclass(frozen=True, slots=True)
class AdversarialMacroConfig:
    """不可变 case 协议，以及彼此独立的会话 provider 调用预算。"""

    depth: AdversarialMacroDepth
    roles: tuple[AdversarialMacroRole, ...]
    max_rounds: int
    per_role_timeout: timedelta
    case_timeout: timedelta
    maximum_calls_per_session: int | None = None
    material_disagreement_threshold: Decimal = Decimal("0.60")
    early_stop_on_stable_consensus: bool = True
    protocol_version: str = "adversarial-macro@1"

    def __post_init__(self) -> None:
        if not self.roles or len(self.roles) != len(set(self.roles)):
            raise ValueError("roles must be non-empty and unique")
        required = {
            AdversarialMacroRole.CATALYST_ADVOCATE,
            AdversarialMacroRole.RISK_CHALLENGER,
        }
        if not required.issubset(self.roles):
            raise ValueError("adversarial analysis requires advocate and challenger roles")
        if isinstance(self.max_rounds, bool) or not isinstance(self.max_rounds, int):
            raise TypeError("max_rounds must be an integer")
        if not 1 <= self.max_rounds <= 3:
            raise ValueError("max_rounds must be between one and three")
        if self.per_role_timeout <= timedelta(0):
            raise ValueError("per_role_timeout must be positive")
        if self.case_timeout <= timedelta(0):
            raise ValueError("case_timeout must be positive")
        if self.case_timeout < self.per_role_timeout:
            raise ValueError("case_timeout must not be shorter than per_role_timeout")
        if self.maximum_calls_per_session is not None and (
            isinstance(self.maximum_calls_per_session, bool)
            or not isinstance(self.maximum_calls_per_session, int)
            or self.maximum_calls_per_session < 1
        ):
            raise ValueError("maximum_calls_per_session must be positive or None")
        if not self.material_disagreement_threshold.is_finite() or not (
            Decimal("0") <= self.material_disagreement_threshold <= Decimal("2")
        ):
            raise ValueError("material_disagreement_threshold must be in [0, 2]")
        if not isinstance(self.early_stop_on_stable_consensus, bool):
            raise TypeError("early_stop_on_stable_consensus must be bool")
        if (
            not self.protocol_version.strip()
            or self.protocol_version != self.protocol_version.strip()
        ):
            raise ValueError("protocol_version must be normalized and non-empty")

    @classmethod
    def for_depth(
        cls,
        depth: AdversarialMacroDepth,
        *,
        maximum_calls_per_session: int | None = None,
    ) -> AdversarialMacroConfig:
        """返回已复核默认值；会话预算为 ``None`` 也不会解除轮次上限。"""

        if depth is AdversarialMacroDepth.FAST:
            return cls(
                depth=depth,
                roles=(
                    AdversarialMacroRole.CATALYST_ADVOCATE,
                    AdversarialMacroRole.RISK_CHALLENGER,
                ),
                max_rounds=1,
                # 盘中双角色与单分析器并行；单角色允许 15 秒，总 case 仍严格有界。
                per_role_timeout=timedelta(seconds=15),
                case_timeout=timedelta(seconds=24),
                maximum_calls_per_session=maximum_calls_per_session,
            )
        if depth is AdversarialMacroDepth.STANDARD:
            return cls(
                depth=depth,
                roles=(
                    AdversarialMacroRole.CATALYST_ADVOCATE,
                    AdversarialMacroRole.RISK_CHALLENGER,
                    AdversarialMacroRole.EVIDENCE_AUDITOR,
                ),
                max_rounds=2,
                per_role_timeout=timedelta(seconds=60),
                case_timeout=timedelta(seconds=130),
                maximum_calls_per_session=maximum_calls_per_session,
            )
        return cls(
            depth=depth,
            roles=(
                AdversarialMacroRole.CATALYST_ADVOCATE,
                AdversarialMacroRole.RISK_CHALLENGER,
                AdversarialMacroRole.EVIDENCE_AUDITOR,
                AdversarialMacroRole.MARKET_REGIME_ANALYST,
                AdversarialMacroRole.EXECUTION_RISK_AUDITOR,
            ),
            max_rounds=3,
            per_role_timeout=timedelta(seconds=180),
            case_timeout=timedelta(seconds=570),
            maximum_calls_per_session=maximum_calls_per_session,
        )

    def audit_document(self) -> dict[str, object]:
        return {
            "depth": self.depth.value,
            "early_stop_on_stable_consensus": self.early_stop_on_stable_consensus,
            "material_disagreement_threshold": str(self.material_disagreement_threshold),
            "max_rounds": self.max_rounds,
            "maximum_calls_per_session": self.maximum_calls_per_session,
            "case_timeout_seconds": self.case_timeout.total_seconds(),
            "per_role_timeout_seconds": self.per_role_timeout.total_seconds(),
            "protocol_version": self.protocol_version,
            "roles": [role.value for role in self.roles],
        }


@dataclass(frozen=True, slots=True)
class AdversarialRoleOpinion:
    """某个有界轮次中通过本地校验的一条角色响应。"""

    role: AdversarialMacroRole
    round_number: int
    role_request_sha256: str
    analysis: MacroAnalysis
    started_at: datetime
    completed_at: datetime
    latency_ms: int
    provider_model: str
    prompt_contract_sha256: str
    evidence_pack_sha256: str
    usage: Mapping[str, int | None]
    termination_reason: str


@dataclass(frozen=True, slots=True)
class AdversarialRound:
    round_number: int
    opinions: tuple[AdversarialRoleOpinion, ...]


@dataclass(frozen=True, slots=True)
class AdversarialMacroRun:
    """供影子评估、报告和审计存储保留的完整结果。"""

    request: MacroAnalysisRequest
    analysis: MacroAnalysis
    rounds: tuple[AdversarialRound, ...]
    audit_identity: AnalyzerAuditIdentity
    termination: AdversarialTermination
    calls_started: int
    failure_code: str | None = None
    failed_role_calls: tuple[Mapping[str, object], ...] = ()
    protocol_document: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        if self.failure_code is not None and not _FAILURE_CODE.fullmatch(self.failure_code):
            raise ValueError("failure_code must be a stable uppercase code")
        if self.calls_started < 0:
            raise ValueError("calls_started must be non-negative")
        self.analysis.validate_against(self.request)

    def audit_document(self) -> dict[str, object]:
        """生成可持久化审计文档，不包含密钥、思维链或 provider 原始正文。"""

        return {
            "schema_version": "adversarial-macro-audit@2",
            "analysis_id": self.request.analysis_id,
            "symbol": self.request.symbol,
            "as_of": self.request.as_of.isoformat(),
            "request_sha256": _request_sha256(self.request),
            "evidence_pack_sha256": _evidence_pack_sha256(self.request),
            "evidence": [
                {
                    "evidence_id": item.evidence_id,
                    "content_hash": item.content_hash,
                    "publisher": item.publisher,
                    "source_tier": item.source_tier,
                    "published_at": item.published_at.isoformat(),
                    "first_seen_at": item.first_seen_at.isoformat(),
                }
                for item in self.request.evidence
            ],
            "identity": _identity_document(self.audit_identity),
            "prompt_protocol": (
                {} if self.protocol_document is None else dict(self.protocol_document)
            ),
            "termination": self.termination.value,
            "failure_code": self.failure_code,
            "calls_started": self.calls_started,
            "selected_analysis": _analysis_document(self.analysis),
            "failed_role_calls": [dict(item) for item in self.failed_role_calls],
            "rounds": [
                {
                    "round_number": item.round_number,
                    "roles": [
                        {
                            "role": opinion.role.value,
                            "role_request_sha256": opinion.role_request_sha256,
                            "prompt_contract_sha256": opinion.prompt_contract_sha256,
                            "evidence_pack_sha256": opinion.evidence_pack_sha256,
                            "started_at": opinion.started_at.isoformat(),
                            "completed_at": opinion.completed_at.isoformat(),
                            "latency_ms": opinion.latency_ms,
                            "provider_model": opinion.provider_model,
                            "usage": dict(opinion.usage),
                            "termination_reason": opinion.termination_reason,
                            "analysis": _analysis_document(opinion.analysis),
                        }
                        for opinion in item.opinions
                    ],
                }
                for item in self.rounds
            ],
        }


@dataclass(frozen=True, slots=True)
class AdversarialShadowRecord:
    """不改变决策的安全对照记录；不含 provider 异常正文。"""

    analysis_id: str
    baseline_analysis: MacroAnalysis
    adversarial_analysis: MacroAnalysis
    adversarial_failure_code: str | None
    baseline_identity_sha256: str
    adversarial_identity_sha256: str


ShadowObserver = Callable[[AdversarialShadowRecord], None]
DualTrackAuditSink = Callable[[Mapping[str, object]], str | None]
