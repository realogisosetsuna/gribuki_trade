"""A 股 PAPER-day 的 LLM 运行清单与盘前上下文纯契约。

本模块只负责冻结可审计的 LLM 身份、证据快照和盘前宏观上下文，
以及校验重启时运行清单是否兼容。它不访问网络、SQLite、调度器或
交易状态；运行器 facade 仅重新导出这些历史名称。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol, cast

from gribuki_trade.analysis.schemas import MacroAnalysisDecision
from gribuki_trade.domain.paper_day import paper_day_canonical_json
from gribuki_trade.features.ashare_surveillance import IntradayCandidate
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
from gribuki_trade.services.ashare.intraday.ashare_intraday_llm import (
    IntradayLLMCoordinator,
    intraday_llm_document_sha256,
)
from gribuki_trade.services.ashare.paper_day import ashare_paper_day_llm_payloads as _payloads
from gribuki_trade.services.ashare.paper_day.ashare_paper_day_serialization import _aware_utc
from gribuki_trade.services.ashare.research.ashare_surveillance import AShareSurveillanceRun
from gribuki_trade.services.macro.macro_research import MacroResearchPlan

_llm_analyzer_identity_document = _payloads._llm_analyzer_identity_document


@dataclass(frozen=True, slots=True)
class PaperDayLLMPreopenContext:
    """由外部准备、在开盘前冻结的非秘密宏观基线。"""

    context_id: str
    evidence_as_of: datetime
    known_at: datetime
    valid_until: datetime
    analysis_id: str
    decision: MacroAnalysisDecision
    macro_impact: Decimal
    evidence_pack_sha256: str
    request_sha256: str
    plan_manifest_sha256: str
    analyzer_identity: AnalyzerAuditIdentity
    response_model: str
    baseline_decision: MacroAnalysisDecision | None = None
    baseline_macro_impact: Decimal | None = None
    baseline_model: str | None = None
    adversarial_decision: MacroAnalysisDecision | None = None
    adversarial_macro_impact: Decimal | None = None
    adversarial_model: str | None = None
    selected_track: str | None = None
    dual_audit_record_sha256: str | None = None

    def __post_init__(self) -> None:
        evidence_as_of = _aware_utc(self.evidence_as_of, "evidence_as_of")
        known_at = _aware_utc(self.known_at, "known_at")
        valid_until = _aware_utc(self.valid_until, "valid_until")
        if not self.context_id.strip() or not self.analysis_id.strip():
            raise ValueError("preopen context and analysis IDs must not be empty")
        if not evidence_as_of <= known_at < valid_until:
            raise ValueError("preopen LLM context timestamps violate PIT chronology")
        if not Decimal("-1") <= self.macro_impact <= Decimal("1"):
            raise ValueError("preopen macro_impact must be in [-1, 1]")
        for name in (
            "evidence_pack_sha256",
            "request_sha256",
            "plan_manifest_sha256",
        ):
            value = getattr(self, name)
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise ValueError(f"{name} must be a lowercase SHA-256 digest")
        if not self.response_model.strip():
            raise ValueError("response_model must not be empty")
        dual_values = (
            self.baseline_decision,
            self.baseline_macro_impact,
            self.baseline_model,
            self.adversarial_decision,
            self.adversarial_macro_impact,
            self.adversarial_model,
            self.selected_track,
            self.dual_audit_record_sha256,
        )
        if any(value is not None for value in dual_values):
            if any(value is None for value in dual_values):
                raise ValueError("preopen dual-track context must be complete or absent")
            assert self.baseline_macro_impact is not None
            assert self.adversarial_macro_impact is not None
            if not Decimal("-1") <= self.baseline_macro_impact <= Decimal("1"):
                raise ValueError("preopen baseline macro_impact must be in [-1, 1]")
            if not Decimal("-1") <= self.adversarial_macro_impact <= Decimal("1"):
                raise ValueError("preopen adversarial macro_impact must be in [-1, 1]")
            assert self.baseline_model is not None
            assert self.adversarial_model is not None
            if not self.baseline_model.strip() or not self.adversarial_model.strip():
                raise ValueError("preopen dual-track models must not be empty")
            if self.selected_track != "ADVERSARIAL":
                raise ValueError("preopen production selection must use adversarial track")
            if self.response_model != self.adversarial_model:
                raise ValueError("preopen selected response model must match adversarial track")
            if self.adversarial_model != self.analyzer_identity.requested_model:
                raise ValueError("preopen adversarial model identity mismatch")
            assert self.dual_audit_record_sha256 is not None
            if len(self.dual_audit_record_sha256) != 64 or any(
                character not in "0123456789abcdef"
                for character in self.dual_audit_record_sha256
            ):
                raise ValueError(
                    "dual_audit_record_sha256 must be a lowercase SHA-256 digest"
                )
        object.__setattr__(self, "evidence_as_of", evidence_as_of)
        object.__setattr__(self, "known_at", known_at)
        object.__setattr__(self, "valid_until", valid_until)

    @property
    def has_dual_track(self) -> bool:
        return self.selected_track is not None

    def audit_document(self) -> dict[str, object]:
        document: dict[str, object] = {
            "analysis_id": self.analysis_id,
            "analyzer_identity": _llm_analyzer_identity_document(self.analyzer_identity),
            "context_id": self.context_id,
            "decision": self.decision.value,
            "evidence_as_of": self.evidence_as_of,
            "evidence_pack_sha256": self.evidence_pack_sha256,
            "known_at": self.known_at,
            "macro_impact": self.macro_impact,
            "plan_manifest_sha256": self.plan_manifest_sha256,
            "request_sha256": self.request_sha256,
            "response_model": self.response_model,
            "valid_until": self.valid_until,
        }
        if self.has_dual_track:
            assert self.baseline_decision is not None
            assert self.baseline_macro_impact is not None
            assert self.baseline_model is not None
            assert self.adversarial_decision is not None
            assert self.adversarial_macro_impact is not None
            assert self.adversarial_model is not None
            assert self.selected_track is not None
            assert self.dual_audit_record_sha256 is not None
            document["dual_track"] = {
                "adversarial": {
                    "decision": self.adversarial_decision.value,
                    "macro_impact": self.adversarial_macro_impact,
                    "model": self.adversarial_model,
                },
                "audit_record_sha256": self.dual_audit_record_sha256,
                "baseline": {
                    "decision": self.baseline_decision.value,
                    "macro_impact": self.baseline_macro_impact,
                    "model": self.baseline_model,
                },
                "selected_track": self.selected_track,
            }
        return document


class PaperDayIntradayLLMPlanFactory(Protocol):
    """根据已保留的 PIT 证据准备冻结计划，不执行 I/O。"""

    @property
    def available(self) -> bool:
        """当前是否能够精确重放已保留快照。"""

    def audit_document(self) -> Mapping[str, object]:
        """返回绑定到运行清单中的不可变快照身份。"""

    def prepare_candidate_review(
        self,
        *,
        candidate: IntradayCandidate,
        scan: AShareSurveillanceRun,
        preopen_context: PaperDayLLMPreopenContext,
        requested_at: datetime,
    ) -> MacroResearchPlan | None: ...


def intraday_llm_evidence_manifest_document(
    factory: PaperDayIntradayLLMPlanFactory,
) -> dict[str, object]:
    """冻结证据快照身份，但不纳入运行时可用性。"""

    available = _llm_plan_factory_available(factory)
    if not isinstance(available, bool):
        raise TypeError("LLM evidence factory availability must be bool")
    audit = _llm_plan_factory_audit_document(factory)
    if not audit:
        raise ValueError("LLM evidence snapshot audit document must not be empty")
    has_snapshot_identity = any(
        isinstance(audit.get(name), str) and bool(cast(str, audit[name]).strip())
        for name in ("snapshot_id", "snapshot_sha256")
    )
    if not has_snapshot_identity:
        raise ValueError("LLM evidence snapshot audit requires a snapshot identity")
    if "evidence_as_of" not in audit and "as_of" not in audit:
        raise ValueError("LLM evidence snapshot audit requires an evidence as_of")
    normalized = json.loads(paper_day_canonical_json(audit))
    if not isinstance(normalized, dict):  # pragma: no cover - 规范化不变量
        raise TypeError("LLM evidence snapshot audit must be an object")
    result = cast(dict[str, object], normalized)
    intraday_llm_document_sha256(result)
    return result


def _llm_plan_factory_available(
    factory: PaperDayIntradayLLMPlanFactory,
) -> bool:
    value = getattr(factory, "available", None)
    if value is None:
        value = getattr(getattr(factory, "snapshot", None), "available", None)
    if not isinstance(value, bool):
        raise TypeError("LLM evidence factory availability must be bool")
    return value


def _llm_plan_factory_audit_document(
    factory: PaperDayIntradayLLMPlanFactory,
) -> dict[str, object]:
    audit = getattr(factory, "audit_document", None)
    if not callable(audit):
        audit = getattr(getattr(factory, "snapshot", None), "audit_document", None)
    if not callable(audit):
        raise TypeError("LLM evidence factory must expose audit_document")
    value = audit()
    if not isinstance(value, Mapping):
        raise TypeError("LLM evidence snapshot audit must be an object")
    return dict(value)


def intraday_llm_manifest_document(
    coordinator: IntradayLLMCoordinator | None,
) -> dict[str, object]:
    """返回创建运行清单所需的精确非秘密 LLM 策略绑定。"""

    if coordinator is None:
        return {
            "enabled": False,
            "required_for_buy": False,
            "schema_version": 1,
        }
    identity = coordinator.analyzer_identity
    if identity is None:
        raise ValueError("intraday LLM coordinator has no auditable analyzer identity")
    config = coordinator.config
    return {
        "analyzer_identity": _llm_analyzer_identity_document(identity),
        "coordinator_config_sha256": config.manifest_sha256,
        "decision_boundary": (
            "ENTER_USES_SYNCHRONOUS_JOURNALED_DUAL_GATE;"
            "REDUCE_USES_POST_FILL_DEEP_DUAL_SCORE_WITHOUT_NETWORK_WAIT"
        ),
        "enabled": config.enabled,
        "policy_version": config.policy_version,
        "preopen_context_contract": {
            "audit_record_sha256_required": True,
            "legacy_journal_policy": "RESTORE_WITHOUT_FABRICATION",
            "persisted_track_fields": [
                "decision",
                "macro_impact",
                "model",
            ],
            "production_failure_policy": "FAIL_CLOSED",
            "production_selected_track": "ADVERSARIAL",
        },
        "required_for_buy": config.required_for_buy,
        "maximum_reviews_per_session": config.maximum_reviews_per_session,
        "review_top_n": config.review_top_n,
        "review_ttl_seconds": int(config.review_ttl.total_seconds()),
        "schema_version": 2,
    }


def intraday_llm_manifest_compatible(
    retained: object,
    current: Mapping[str, object],
) -> bool:
    """接受精确当前清单，或升级前不含盘前双轨声明的旧清单。"""

    if not isinstance(retained, Mapping):
        return False
    retained_document = dict(retained)
    current_document = dict(current)
    if retained_document == current_document:
        return True
    if (
        retained_document.get("schema_version") != 1
        or current_document.get("schema_version") != 2
        or current_document.get("enabled") is not True
    ):
        return False
    current_document.pop("preopen_context_contract", None)
    current_document["schema_version"] = 1
    return retained_document == current_document




__all__ = [
    "PaperDayIntradayLLMPlanFactory",
    "PaperDayLLMPreopenContext",
    "intraday_llm_evidence_manifest_document",
    "intraday_llm_manifest_compatible",
    "intraday_llm_manifest_document",
]
