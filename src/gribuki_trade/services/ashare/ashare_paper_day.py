"""在单进程中编排一个真实化且不连接券商的 A 股 PAPER 交易日。

运行器有意分离研究、模拟执行和通知边界：

* 由交易日历验证的上一交易时段筛选结果作为盘前观察列表种子；
* 当前交易时段的全市场监看持续维护并交叉验证该列表；
* 只有完整的一分钟 K 线能够形成技术优势；
* 入场只能交给信号后的下一根完整分钟线，绝不能使用信号线本身；
* 现有只追加 PAPER 账本始终是资金与持仓的唯一事实来源；
* 卖出信号会被保留并通知，但本日内运行器绝不提交卖单；
* 每个外部可见动作都从一条哈希链日内日志事件开始。

这仍是基于公共网页数据的 PAPER 模拟，无法复现交易所队列优先级、逐笔路径、
隐藏流动性或券商延迟。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time as time_module
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Protocol, cast

from gribuki_trade.analysis.schemas import (
    MacroAnalysisDecision,
)
from gribuki_trade.domain.exit_plans import (
    ExitPlan,
    ExitPlanDepth,
    ExitPlanEventType,
    exit_plan_document,
)
from gribuki_trade.domain.paper_day import (
    NewPaperDayEvent,
    PaperDayEvent,
    PaperDayPhase,
    PaperDayRunManifest,
    PaperDaySeverity,
    paper_day_canonical_json,
    paper_day_target_hash,
)
from gribuki_trade.domain.paper_trading import (
    ASharePaperFill,
    PaperAccountSnapshot,
    PaperInstrumentType,
)
from gribuki_trade.domain.recommendations import (
    RecommendationDecision,
    RecommendationHorizon,
)
from gribuki_trade.features.ashare_surveillance import (
    IntradayCandidate,
    IntradayCandidateClass,
)
from gribuki_trade.features.deep_exit_planning import (
    DeepExitPlanConfig,
    DeepExitTimeframe,
    DeepSemanticAssessment,
)
from gribuki_trade.features.exit_planning import QuickExitPlanConfig
from gribuki_trade.features.technical import (
    TechnicalBar,
    TechnicalSignal,
    TechnicalSignalConfig,
    build_technical_signal,
)
from gribuki_trade.ports.ashare_screening import AShareBoard
from gribuki_trade.ports.llm_analyzer import AnalyzerAuditIdentity
from gribuki_trade.ports.market_data import (
    AsyncIntradayMarketData,
    FreshnessStatus,
    IntradayBar,
    MinuteInterval,
)
from gribuki_trade.ports.notifier import (
    NotificationTargetKind,
    OutboundNotification,
)
from gribuki_trade.reporting.contracts import (
    ReportKind,
    humanize_internal_code,
    report_contract,
    validate_markdown_report_contract,
)
from gribuki_trade.services.ashare import ashare_paper_day_documents as _paper_day_documents
from gribuki_trade.services.ashare import ashare_paper_day_llm_payloads as _paper_day_llm_payloads
from gribuki_trade.services.ashare import ashare_paper_day_notifications as _paper_day_notifications
from gribuki_trade.services.ashare import ashare_paper_day_projection as _paper_day_projection
from gribuki_trade.services.ashare.ashare_intraday_llm import (
    IntradayLLMCoordinator,
    IntradayLLMGateAction,
    IntradayLLMGateOutcome,
    IntradayLLMGateReason,
    IntradayLLMReview,
    IntradayLLMScheduleStatus,
    intraday_llm_document_sha256,
    intraday_llm_review_document,
)
from gribuki_trade.services.ashare.ashare_intraday_paper import (
    IntradayOrderQuantityRule,
    IntradayPaperMatchOutcome,
    IntradayPaperMatchReason,
    IntradayPaperMatchStatus,
    IntradayPaperOrder,
    IntradayPaperRiskConfig,
    IntradayPaperRiskOutcome,
    IntradayPaperRiskStatus,
    IntradayPriceAcceptance,
    IntradaySellQuantityPlan,
    IntradaySellQuantityStatus,
    build_intraday_buy_order,
    build_intraday_sell_price_acceptance,
    build_intraday_sell_quantity_plan,
    intraday_buy_order_price_acceptance,
    intraday_order_quantity_rule,
    match_intraday_buy_order,
)
from gribuki_trade.services.ashare.ashare_paper import (
    ASharePaperTradingService,
    PaperAccountNotFoundError,
)
from gribuki_trade.services.ashare.ashare_paper_day_config import (
    _TENCENT_ENRICHED_SURVEILLANCE_SOURCE_ID,
    PAPER_RISK_POLICY_CHANGE_CONFIRMATION,
    ASharePaperDayConfig,
    _entry_policy_document,
    _risk_policy_manifest_binding,
    _runner_config_document,
)
from gribuki_trade.services.ashare.ashare_paper_day_schedule import (
    SHANGHAI,
    scheduler_sleep_seconds,
    session_datetime,
)
from gribuki_trade.services.ashare.ashare_paper_day_schedule import (
    phase_at as _phase_at,
)
from gribuki_trade.services.ashare.ashare_paper_day_serialization import (
    _append_and_sync_text,
    _atomic_write_text,
    _aware_utc,
    _bar_document,
    _bar_revision,
    _calendar_time_exit,
    _decimal_display,
    _document_sha256,
    _event_jsonl,
    _exit_barrier_display,
    _exit_barriers_for_bar,
    _exit_protection_id,
    _next_utc_minute,
    _optional_datetime,
    _optional_positive_decimal,
    _paper_deep_timeframes,
    _technical_bar_document,
    _technical_bars_from_document,
)
from gribuki_trade.services.ashare.ashare_preopen_screening import (
    ASharePreopenScreeningRun,
    ASharePreopenScreeningService,
)
from gribuki_trade.services.ashare.ashare_surveillance import (
    AShareIntradaySurveillanceService,
    AShareSurveillanceRun,
)
from gribuki_trade.services.exit_plan_lifecycle import (
    ExitPlanLifecycleConflictError,
    ExitPlanLifecycleError,
    ExitPlanLifecycleService,
)
from gribuki_trade.services.macro_research import MacroResearchPlan
from gribuki_trade.services.notification_dispatch import (
    NotificationDispatchService,
    NotificationDispatchServiceError,
)
from gribuki_trade.storage.exit_plans import SQLiteExitPlanStore
from gribuki_trade.storage.outbox import OutboxStatus, SQLiteOutbox
from gribuki_trade.storage.report_artifact_outbox import (
    ReportArtifactOutboxError,
    ReportArtifactRecord,
    ReportArtifactStatus,
    SQLiteReportArtifactOutbox,
)

_contractualize_paper_notification = _paper_day_notifications._contractualize_paper_notification
_notification_scalar = _paper_day_notifications._notification_scalar
_paper_notification_kind = _paper_day_notifications._paper_notification_kind
_paper_notification_price_text = _paper_day_notifications._paper_notification_price_text
_paper_notification_title_and_detail = _paper_day_notifications._paper_notification_title_and_detail
_paper_report_artifact_key = _paper_day_notifications._paper_report_artifact_key
_PAPER_HEALTH_EVENTS = _paper_day_notifications._PAPER_HEALTH_EVENTS

# 观察列表、候选、委托和成交文档由无副作用模块实现；这里保留历史名称，
# 让旧的恢复代码和外部测试继续从 runner facade 访问同一份契约。
PaperDayWatchEntry = _paper_day_documents.PaperDayWatchEntry
_board_from_symbol = _paper_day_documents.board_from_symbol
_candidate_document = _paper_day_documents.candidate_document
_candidate_from_document = _paper_day_documents.candidate_from_document
_candidate_previous_close = _paper_day_documents.candidate_previous_close
_fill_document = _paper_day_documents.fill_document
_fill_from_document = _paper_day_documents.fill_from_document
_order_document = _paper_day_documents.order_document
_order_from_document = _paper_day_documents.order_from_document
_strict_positive_int = _paper_day_documents.strict_positive_int
_watch_entry_document = _paper_day_documents.watch_entry_document
_watch_entry_from_document = _paper_day_documents.watch_entry_from_document
_watch_entry_from_intraday = _paper_day_documents.watch_entry_from_intraday
_watch_entry_from_screen = _paper_day_documents.watch_entry_from_screen

# 只有这个精确来源可以交叉验证降级的新浪分钟线，因为其 COMPLETE 契约会在
# 候选排名前严格联结腾讯行情板块与腾讯批量报价端点，并逐标的校验 OHLC、
# 价格、金额和因子。不得改成子字符串判断：普通腾讯板块回退仍为 DEGRADED，
# 不具备执行授权。
PAPER_REPORT_ARTIFACT_RECOVERY_CONFIRMATION = "PAPER_REPORT_ARTIFACT_RECOVERY"
PAPER_REPORT_ARTIFACT_MARK_SENT = "MARK_SENT_AFTER_PROVIDER_VERIFICATION"
PAPER_REPORT_ARTIFACT_RESEND = "RESEND_AFTER_PROVIDER_NON_RECEIPT_VERIFICATION"
_REMOVE_POSITION_COUNT_CAP = "REMOVE_POSITION_COUNT_CAP"
_ADD_PRICE_ACCEPTANCE_BOUNDS = "ADD_PRICE_ACCEPTANCE_BOUNDS"
_ADD_BOARD_QUANTITY_RULES = "ADD_BOARD_QUANTITY_RULES"


class PaperDayAbortRecoveryRequiredError(RuntimeError):
    """只有操作员显式操作才可越过终止中止状态。

    此异常有意保持稳定且不包含提供方异常文本。因此，调用方可把它映射为
    CLI/API 错误，而不会泄露带认证信息的 URL 或响应片段。
    """

    def __init__(self) -> None:
        super().__init__("PAPER day abort requires explicit operator recovery")


class PaperDayRiskPolicyChangeError(RuntimeError):
    """运行时风险策略迁移在修改任何日志前失败。"""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"PAPER day risk-policy change unavailable ({code})")


class PaperDayStore(Protocol):
    """运行器及其测试替身使用的窄存储接口。"""

    def append_event(
        self,
        event: NewPaperDayEvent,
        *,
        owner_id: str,
        lease_checked_at: datetime | None = None,
    ) -> tuple[PaperDayEvent, bool]: ...

    def events(self, run_id: str) -> tuple[PaperDayEvent, ...]: ...

    def event_by_key(self, run_id: str, event_key: str) -> PaperDayEvent | None: ...

    def acquire_lease(
        self,
        run_id: str,
        owner_id: str,
        *,
        now: datetime,
        lease_for: timedelta,
    ) -> None: ...

    def renew_lease(
        self,
        run_id: str,
        owner_id: str,
        *,
        now: datetime,
        lease_for: timedelta,
    ) -> None: ...

    def release_lease(self, run_id: str, owner_id: str) -> bool: ...


class PaperDayArtifactNotifier(Protocol):
    """可选的最终文件上传能力，不暴露入站命令路径。"""

    async def upload_private_file(self, target_id: str, artifact: Path) -> object: ...

    async def upload_group_file(self, target_id: str, artifact: Path) -> object: ...


class PaperDayDeepExitAssessmentProvider(Protocol):
    """为成交后的 DEEP 计划返回单分析器与对抗系统的冻结结论。"""

    async def assess(
        self,
        *,
        plan: ExitPlan,
        timeframes: tuple[DeepExitTimeframe, ...],
        decision_at: datetime,
    ) -> tuple[DeepSemanticAssessment | None, DeepSemanticAssessment | None]: ...


# LLM 审计载荷兼容别名：历史调用方仍从 paper_day facade 访问私有 helper。
_llm_analyzer_identity_document = _paper_day_llm_payloads._llm_analyzer_identity_document
_llm_analyzer_identity_from_document = _paper_day_llm_payloads._llm_analyzer_identity_from_document
_llm_preopen_context_from_document = _paper_day_llm_payloads._llm_preopen_context_from_document
_llm_macro_analysis_from_document = _paper_day_llm_payloads._llm_macro_analysis_from_document
_llm_review_from_document = _paper_day_llm_payloads._llm_review_from_document
_llm_gate_document = _paper_day_llm_payloads.llm_gate_document
_llm_preopen_dual_text = _paper_day_llm_payloads._llm_preopen_dual_text
_llm_gate_dual_document = _paper_day_llm_payloads.llm_gate_dual_document
_llm_gate_dual_text = _paper_day_llm_payloads.llm_gate_dual_text
_llm_object = _paper_day_llm_payloads._llm_object
_llm_object_list = _paper_day_llm_payloads._llm_object_list
_llm_string = _paper_day_llm_payloads._llm_string
_llm_string_tuple = _paper_day_llm_payloads._llm_string_tuple
_llm_datetime = _paper_day_llm_payloads._llm_datetime
_llm_date = _paper_day_llm_payloads._llm_date
_llm_decimal = _paper_day_llm_payloads._llm_decimal
_llm_int = _paper_day_llm_payloads._llm_int


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


@dataclass(frozen=True, slots=True)
class _PreopenRecoverySeed:
    """从不可变旁路文件加载的已校验降级 L1 种子。"""

    available_at: datetime
    source_id: str
    method: str
    sha256: str
    raw_universe_count: int
    eligible_count: int
    entries: tuple[PaperDayWatchEntry, ...]


@dataclass(frozen=True, slots=True)
class PaperDayResult:
    run_id: str
    session_date: date
    completed: bool
    event_count: int
    notification_required: int
    notification_sent: int
    notification_gaps: int
    final_snapshot: PaperAccountSnapshot
    report_path: Path
    artifact_delivery_status: str = "NOT_CONFIGURED"
    artifact_delivery_complete: bool = False
    daily_review_delivery_complete: bool = False
    text_notification_required: int = 0
    text_notification_sent: int = 0
    text_notification_gaps: int = 0


@dataclass(frozen=True, slots=True)
class _RiskPolicyMigration:
    """等待追加日志、且已完整校验的旧策略到新策略迁移。"""

    previous_policy: Mapping[str, object]
    new_policy: Mapping[str, object]
    reason_codes: tuple[str, ...]
    policy_diff: tuple[Mapping[str, object], ...]
    baseline_source: str
    source_event: PaperDayEvent | None


class PaperDayEventPublisher:
    """先写日志，再幂等入队并立即派发文本。"""

    _SIDECAR_RETRY_DELAYS_SECONDS = (0.0, 0.01, 0.025, 0.05)

    def __init__(
        self,
        *,
        manifest: PaperDayRunManifest,
        store: PaperDayStore,
        outbox: SQLiteOutbox,
        dispatcher: NotificationDispatchService,
        target_kind: NotificationTargetKind,
        target_id: str,
        owner_id: str,
        clock: Callable[[], datetime],
        status_path: Path,
    ) -> None:
        self._manifest = manifest
        self._store = store
        self._outbox = outbox
        self._dispatcher = dispatcher
        self._target_kind = target_kind
        self._target_id = target_id
        if self._manifest.target_hash != paper_day_target_hash(
            channel="onebot",
            target_kind=target_kind.value,
            target_id=target_id,
        ):
            raise ValueError("notification target conflicts with the manifest")
        self._owner_id = owner_id
        self._clock = clock
        self._status_path = status_path
        self._delivery_projection = self._retained_delivery_projection()
        retained = self._store.events(self._manifest.run_id)
        self._events_by_key = {item.event_key: item for item in retained}
        self._event_count = len(retained)
        self._latest_event = retained[-1] if retained else None
        self._jsonl_repair_pending = not self._rebuild_jsonl(retained)

    async def emit(
        self,
        *,
        event_key: str,
        event_type: str,
        phase: PaperDayPhase,
        severity: PaperDaySeverity,
        payload: Mapping[str, object],
        notification_text: str | None = None,
        symbol: str | None = None,
        correlation_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> PaperDayEvent:
        now = _aware_utc(self._clock(), "clock")
        existing = self._events_by_key.get(event_key)
        if existing is not None:
            if existing.notification_required:
                retained = existing.payload.get("notification_text")
                if not isinstance(retained, str) or not retained:
                    raise RuntimeError("stored notification event has no text")
                self._ensure_notification(existing, retained)
            self._write_status(existing, created=False)
            return existing
        material = dict(payload)
        contracted_notification = notification_text
        if notification_text is not None:
            if not notification_text.strip():
                raise ValueError("notification_text must not be blank")
            report_kind = _paper_notification_kind(event_type)
            contracted_notification = _contractualize_paper_notification(
                kind=report_kind,
                event_type=event_type,
                raw_text=notification_text,
                payload=material,
                symbol=symbol,
                evidence_at=occurred_at or now,
            )
            if len(contracted_notification) > 7_900:
                raise ValueError("contractual PAPER notification exceeds OneBot limit")
            material["notification_text"] = contracted_notification
            material["report_kind"] = report_kind.value
            material["report_contract"] = "user-readable-report-contract@1"
        event, created = self._store.append_event(
            NewPaperDayEvent(
                run_id=self._manifest.run_id,
                event_key=event_key,
                event_type=event_type,
                phase=phase,
                severity=severity,
                occurred_at=occurred_at or now,
                known_at=now,
                notification_required=contracted_notification is not None,
                payload=material,
                symbol=symbol,
                correlation_id=correlation_id,
            ),
            owner_id=self._owner_id,
            lease_checked_at=now,
        )
        self._events_by_key[event.event_key] = event
        if created:
            self._event_count += 1
        if contracted_notification is not None:
            self._ensure_notification(event, contracted_notification)
            with suppress(NotificationDispatchServiceError):
                await self._dispatcher.dispatch_once(limit=50)
        self._write_status(event, created=created)
        return event

    def reconcile_notifications(self) -> int:
        """修复日志追加与 outbox 入队之间的崩溃边界。"""

        repaired = 0
        for event in sorted(
            self._events_by_key.values(),
            key=lambda item: item.sequence,
        ):
            if not event.notification_required:
                continue
            text = event.payload.get("notification_text")
            if not isinstance(text, str) or not text:
                raise RuntimeError("notification-required event has no retained text")
            key = self.notification_key(event)
            if self._outbox.get_by_key(key) is None:
                self._ensure_notification(event, text)
                repaired += 1
        return repaired

    async def dispatch_once(self) -> None:
        with suppress(NotificationDispatchServiceError):
            await self._dispatcher.dispatch_once(limit=50)

    def write_heartbeat(self) -> None:
        """刷新只读存活旁路文件，不触碰 SQLite。"""

        if self._latest_event is not None:
            self._write_status(self._latest_event, created=False)

    def write_delivery_projection(
        self,
        *,
        artifact_delivery_status: str,
        artifact_delivery_complete: bool,
        daily_review_delivery_complete: bool,
        notification_required: int,
        notification_sent: int,
        notification_gaps: int,
        text_notification_required: int,
        text_notification_sent: int,
        text_notification_gaps: int,
    ) -> None:
        """把最终双交付状态写入只读 sidecar，供 ``status`` 精确展示。"""

        self._delivery_projection = {
            "artifact_delivery_status": artifact_delivery_status,
            "artifact_delivery_complete": artifact_delivery_complete,
            "daily_review_delivery_complete": daily_review_delivery_complete,
            "notification_required": notification_required,
            "notification_sent": notification_sent,
            "notification_gaps": notification_gaps,
            "text_notification_required": text_notification_required,
            "text_notification_sent": text_notification_sent,
            "text_notification_gaps": text_notification_gaps,
        }
        if self._latest_event is not None:
            self._write_status(self._latest_event, created=False)

    def _ensure_notification(self, event: PaperDayEvent, text: str) -> None:
        self._outbox.enqueue(
            OutboundNotification(
                idempotency_key=self.notification_key(event),
                channel="onebot",
                target_kind=self._target_kind,
                target_id=self._target_id,
                text=text,
                created_at=event.known_at,
            )
        )

    def notification_key(self, event: PaperDayEvent) -> str:
        """返回一条日志事件的确定性 outbox 身份。"""

        return (
            f"paper-day:{self._manifest.run_id}:{event.event_id}:{self._manifest.target_hash[:12]}"
        )

    def _write_status(self, latest: PaperDayEvent, *, created: bool) -> None:
        self._latest_event = latest
        document = {
            "event_count": self._event_count,
            "latest_event": latest.event_type,
            "latest_known_at": latest.known_at.isoformat(),
            "phase": latest.phase.value,
            "process_heartbeat_at": _aware_utc(
                self._clock(),
                "clock",
            ).isoformat(),
            "process_id": os.getpid(),
            "run_id": self._manifest.run_id,
            "session_date": self._manifest.session_date.isoformat(),
            **self._delivery_projection,
        }
        status_text = json.dumps(document, ensure_ascii=False, sort_keys=True) + "\n"
        self._try_sidecar_io(lambda: _atomic_write_text(self._status_path, status_text))
        if self._jsonl_repair_pending:
            # SQLite 日志是权威来源。重建投影，避免先前部分或失败的追加造成
            # 序列缺口或重复。
            retained = self._store.events(self._manifest.run_id)
            self._jsonl_repair_pending = not self._rebuild_jsonl(retained)
            return
        if created:
            appended = self._try_sidecar_io(
                lambda: _append_and_sync_text(
                    self._status_path.with_name("session.log.jsonl"),
                    _event_jsonl(latest),
                )
            )
            self._jsonl_repair_pending = not appended

    def _retained_delivery_projection(self) -> dict[str, object]:
        """恢复同一 run 已写 sidecar 的交付投影，避免心跳覆盖终态。"""

        try:
            retained = json.loads(self._status_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return {}
        if not isinstance(retained, dict) or retained.get("run_id") != self._manifest.run_id:
            return {}
        output: dict[str, object] = {}
        bool_fields = {
            "artifact_delivery_complete",
            "daily_review_delivery_complete",
        }
        count_fields = {
            "notification_required",
            "notification_sent",
            "notification_gaps",
            "text_notification_required",
            "text_notification_sent",
            "text_notification_gaps",
        }
        status = retained.get("artifact_delivery_status")
        if isinstance(status, str) and status in {
            ReportArtifactStatus.PENDING.value,
            ReportArtifactStatus.SENT.value,
            ReportArtifactStatus.AMBIGUOUS.value,
            "NOT_CONFIGURED",
        }:
            output["artifact_delivery_status"] = status
        for field in bool_fields:
            value = retained.get(field)
            if isinstance(value, bool):
                output[field] = value
        for field in count_fields:
            value = retained.get(field)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                output[field] = value
        return output

    def _rebuild_jsonl(self, events: tuple[PaperDayEvent, ...]) -> bool:
        """依据权威日志修复任何不完整的旁路文件追加。"""

        projection = "".join(_event_jsonl(item) for item in events)
        return self._try_sidecar_io(
            lambda: _atomic_write_text(
                self._status_path.with_name("session.log.jsonl"),
                projection,
            )
        )

    @classmethod
    def _try_sidecar_io(cls, operation: Callable[[], None]) -> bool:
        """针对非权威旁路文件的尽力有界重试。

        Windows 读取方可能短暂阻止追加或原子替换。这类冲突绝不能把已提交的
        日志事件变成交易日中止；后续写入会改为依据 SQLite 重建 JSONL。
        """

        for attempt, delay in enumerate(cls._SIDECAR_RETRY_DELAYS_SECONDS):
            if delay:
                time_module.sleep(delay)
            try:
                operation()
            except OSError:
                if attempt + 1 == len(cls._SIDECAR_RETRY_DELAYS_SECONDS):
                    return False
            else:
                return True
        return False


class ASharePaperDayRunner:
    """在单进程中运行一个已验证的上海交易时段。"""

    _DEEP_SEMANTIC_RETRY_INTERVAL = timedelta(minutes=5)

    def __init__(
        self,
        *,
        manifest: PaperDayRunManifest,
        latest_completed_session: date,
        owner_id: str,
        store: PaperDayStore,
        publisher: PaperDayEventPublisher,
        preopen_screening: ASharePreopenScreeningService,
        surveillance: AShareIntradaySurveillanceService,
        market_data: AsyncIntradayMarketData,
        paper: ASharePaperTradingService,
        outbox: SQLiteOutbox,
        report_dir: Path,
        config: ASharePaperDayConfig | None = None,
        technical_config: TechnicalSignalConfig | None = None,
        risk_config: IntradayPaperRiskConfig | None = None,
        intraday_llm: IntradayLLMCoordinator | None = None,
        intraday_llm_plan_factory: PaperDayIntradayLLMPlanFactory | None = None,
        llm_preopen_context: PaperDayLLMPreopenContext | None = None,
        exit_plan_lifecycle: ExitPlanLifecycleService | None = None,
        quick_exit_config: QuickExitPlanConfig | None = None,
        deep_exit_config: DeepExitPlanConfig | None = None,
        deep_exit_assessment_provider: PaperDayDeepExitAssessmentProvider | None = None,
        artifact_notifier: PaperDayArtifactNotifier | None = None,
        artifact_target_kind: NotificationTargetKind | None = None,
        artifact_target_id: str | None = None,
        artifact_outbox: SQLiteReportArtifactOutbox | None = None,
        report_artifact_recovery_action: str | None = None,
        report_artifact_recovery_confirmation: str | None = None,
        report_artifact_provider_identifier: str | None = None,
        preopen_recovery_path: Path | None = None,
        recover_after_abort: bool = False,
        risk_policy_change_confirmation: str | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._manifest = manifest
        if latest_completed_session >= manifest.session_date:
            raise ValueError("latest_completed_session must precede the PAPER session")
        self._latest_completed_session = latest_completed_session
        self._owner_id = owner_id
        self._store = store
        self._publisher = publisher
        self._preopen = preopen_screening
        self._surveillance = surveillance
        self._market_data = market_data
        self._paper = paper
        self._outbox = outbox
        self._report_dir = report_dir.resolve()
        self._report_dir.mkdir(parents=True, exist_ok=True)
        self._config = config or ASharePaperDayConfig(initial_cash=manifest.initial_cash)
        if self._config.initial_cash != manifest.initial_cash:
            raise ValueError("runner initial cash conflicts with the manifest")
        self._technical_config = technical_config or TechnicalSignalConfig()
        self._risk_config = risk_config or IntradayPaperRiskConfig(
            initial_equity=manifest.initial_cash
        )
        if self._risk_config.initial_equity != manifest.initial_cash:
            raise ValueError("risk initial equity conflicts with the manifest")
        self._quick_exit_config = quick_exit_config or QuickExitPlanConfig(
            price_tick=self._risk_config.stock_price_quantum,
        )
        self._deep_exit_config = deep_exit_config or DeepExitPlanConfig(
            price_tick=self._risk_config.stock_price_quantum,
        )
        if self._quick_exit_config.price_tick != self._risk_config.stock_price_quantum:
            raise ValueError("QUICK exit tick conflicts with PAPER stock tick")
        if self._deep_exit_config.price_tick != self._risk_config.stock_price_quantum:
            raise ValueError("DEEP exit tick conflicts with PAPER stock tick")
        self._owned_exit_plan_store: SQLiteExitPlanStore | None = None
        self._exit_plan_lifecycle: ExitPlanLifecycleService | None
        if self._config.exit_plan_enabled:
            if exit_plan_lifecycle is None:
                # 真实 CLI 的 report_dir 位于 <runtime>/<交易日>/reports；把退出账本
                # 放在日期目录的共同父目录，可让跨日持仓继续沿用同一保护流。
                if (
                    self._report_dir.name == "reports"
                    and self._report_dir.parent.name == self._manifest.session_date.isoformat()
                ):
                    exit_store_path = self._report_dir.parent.parent / "exit-plans.sqlite3"
                else:
                    exit_store_path = self._report_dir / "exit-plans.sqlite3"
                self._owned_exit_plan_store = SQLiteExitPlanStore(exit_store_path)
                exit_plan_lifecycle = ExitPlanLifecycleService(self._owned_exit_plan_store)
            self._exit_plan_lifecycle = exit_plan_lifecycle
        else:
            if exit_plan_lifecycle is not None or deep_exit_assessment_provider is not None:
                raise ValueError("exit-plan dependencies require enabled exit planning")
            self._exit_plan_lifecycle = None
        self._deep_exit_assessment_provider = deep_exit_assessment_provider
        self._intraday_llm = intraday_llm
        self._intraday_llm_plan_factory = intraday_llm_plan_factory
        self._llm_preopen_context = llm_preopen_context
        self._llm_manifest = intraday_llm_manifest_document(intraday_llm)
        self._llm_evidence_runtime_manifest = (
            None
            if intraday_llm_plan_factory is None
            else intraday_llm_evidence_manifest_document(intraday_llm_plan_factory)
        )
        self._llm_evidence_available = (
            False
            if intraday_llm_plan_factory is None
            else _llm_plan_factory_available(intraday_llm_plan_factory)
        )
        self._llm_evidence_manifest = self._llm_evidence_runtime_manifest
        if self._config.intraday_llm_enabled:
            if intraday_llm is None or intraday_llm_plan_factory is None:
                raise ValueError("enabled intraday LLM requires coordinator and plan factory")
            if not intraday_llm.config.enabled:
                raise ValueError("runner LLM is enabled but coordinator is disabled")
            if intraday_llm.config.required_for_buy != self._config.intraday_llm_required_for_buy:
                raise ValueError("runner and coordinator LLM buy policy mismatch")
            retained_manifest = self._manifest.config.get("intraday_llm_policy")
            if not intraday_llm_manifest_compatible(
                retained_manifest,
                self._llm_manifest,
            ):
                raise ValueError("run manifest does not bind the intraday LLM policy")
            retained_evidence = self._manifest.config.get("intraday_llm_evidence_snapshot")
            if not isinstance(retained_evidence, dict):
                raise ValueError("run manifest does not bind the intraday LLM evidence snapshot")
            if (
                self._llm_evidence_available
                and retained_evidence != self._llm_evidence_runtime_manifest
            ):
                raise ValueError("available LLM evidence does not match the run manifest")
            # 重启后，以留存身份为权威。如果无法重放完全一致的快照，则保留该身份、
            # 发出稳定失败结果，并让 BUY 继续失败关闭，而不阻断 SELL。
            self._llm_evidence_manifest = dict(retained_evidence)
            identity = intraday_llm.analyzer_identity
            if (
                llm_preopen_context is not None
                and llm_preopen_context.analyzer_identity != identity
            ):
                raise ValueError("preopen context analyzer identity mismatch")
        elif any(
            value is not None
            for value in (
                intraday_llm,
                intraday_llm_plan_factory,
                llm_preopen_context,
            )
        ):
            raise ValueError("intraday LLM dependencies require enabled runner config")
        else:
            retained_manifest = self._manifest.config.get("intraday_llm_policy")
            if retained_manifest is not None and not intraday_llm_manifest_compatible(
                retained_manifest,
                self._llm_manifest,
            ):
                raise ValueError("run manifest conflicts with disabled intraday LLM")
        self._artifact_notifier = artifact_notifier
        self._artifact_target_kind = artifact_target_kind
        self._artifact_target_id = artifact_target_id
        if (artifact_target_kind is None) != (artifact_target_id is None):
            raise ValueError("artifact target kind and ID must be supplied together")
        if artifact_notifier is not None and artifact_target_kind is None:
            raise ValueError("artifact notifier requires an exact target")
        if artifact_outbox is not None and artifact_target_kind is None:
            raise ValueError("artifact outbox requires an exact target")
        if (
            artifact_target_kind is not None
            and artifact_target_id is not None
            and self._manifest.target_hash
            != paper_day_target_hash(
                channel="onebot",
                target_kind=artifact_target_kind.value,
                target_id=artifact_target_id,
            )
        ):
            raise ValueError("artifact target conflicts with the manifest")
        self._owned_artifact_outbox: SQLiteReportArtifactOutbox | None = None
        self._artifact_outbox = artifact_outbox
        if self._artifact_outbox is None and artifact_target_kind is not None:
            self._owned_artifact_outbox = SQLiteReportArtifactOutbox(
                self._report_dir.parent / "report-artifacts.sqlite3"
            )
            self._artifact_outbox = self._owned_artifact_outbox
        allowed_recovery_actions = {
            PAPER_REPORT_ARTIFACT_MARK_SENT,
            PAPER_REPORT_ARTIFACT_RESEND,
        }
        if report_artifact_recovery_action not in {None, *allowed_recovery_actions}:
            raise ValueError("unsupported report-artifact recovery action")
        if report_artifact_recovery_action is None:
            if report_artifact_recovery_confirmation is not None:
                raise ValueError("report-artifact recovery confirmation has no action")
            if report_artifact_provider_identifier is not None:
                raise ValueError("report-artifact provider identifier has no action")
        else:
            if report_artifact_recovery_confirmation != (
                PAPER_REPORT_ARTIFACT_RECOVERY_CONFIRMATION
            ):
                raise ValueError("report-artifact recovery requires explicit confirmation")
            if artifact_target_kind is None:
                raise ValueError("report-artifact recovery requires an exact target")
            if (
                report_artifact_recovery_action == PAPER_REPORT_ARTIFACT_MARK_SENT
                and (
                    not isinstance(report_artifact_provider_identifier, str)
                    or not report_artifact_provider_identifier.strip()
                    or report_artifact_provider_identifier
                    != report_artifact_provider_identifier.strip()
                    or len(report_artifact_provider_identifier) > 256
                    or any(
                        character in report_artifact_provider_identifier
                        for character in "\r\n\x00"
                    )
                )
            ):
                raise ValueError("mark-sent recovery requires a provider identifier")
            if (
                report_artifact_recovery_action == PAPER_REPORT_ARTIFACT_RESEND
                and report_artifact_provider_identifier is not None
            ):
                raise ValueError("resend recovery must not supply a provider identifier")
        self._report_artifact_recovery_action = report_artifact_recovery_action
        self._report_artifact_provider_identifier = report_artifact_provider_identifier
        self._preopen_recovery_path = (
            None if preopen_recovery_path is None else preopen_recovery_path.resolve()
        )
        if not isinstance(recover_after_abort, bool):
            raise TypeError("recover_after_abort must be bool")
        self._recover_after_abort = recover_after_abort
        if risk_policy_change_confirmation not in {
            None,
            PAPER_RISK_POLICY_CHANGE_CONFIRMATION,
        }:
            raise ValueError("unsupported risk-policy change confirmation")
        self._risk_policy_change_confirmation = risk_policy_change_confirmation
        self._clock = clock
        self._sleep = sleep
        self._watchlist: dict[str, PaperDayWatchEntry] = {}
        self._initial: tuple[PaperDayWatchEntry, ...] = ()
        self._latest_candidates: dict[str, IntradayCandidate] = {}
        self._latest_scan_at: datetime | None = None
        self._latest_scan_source: str | None = None
        self._latest_scan_status: str | None = None
        self._pending: dict[str, IntradayPaperOrder] = {}
        self._exit_protection_by_symbol: dict[str, str] = {}
        self._deep_followup_retry_tasks: dict[str, asyncio.Task[None]] = {}
        self._entered_symbols: set[str] = set()
        self._last_technical_state: dict[str, str] = {}
        self._last_processed_bar: dict[str, datetime] = {}
        self._last_symbol_fetch: dict[str, datetime] = {}
        self._last_prices: dict[str, Decimal] = {}
        self._session_previous_closes: dict[str, Decimal] = {}
        self._last_source_state: dict[str, str] = {}
        self._latest_llm_context_by_symbol: dict[str, str] = {}
        self._llm_restored_reviews = 0
        self._llm_restore_rejections = 0
        self._llm_preopen_restore_conflict = False
        self._llm_service_state = "UNKNOWN"
        self._last_lease_renewed_at: datetime | None = None
        self._lease_task: asyncio.Task[None] | None = None
        self._partial_session = False

    async def run(self) -> PaperDayResult:
        """阻塞直至盘后收尾完成，或向上抛出失败关闭错误。"""

        await self._renew_lease(force=True)
        dispatcher_task = asyncio.create_task(
            self._dispatch_loop(),
            name="paper-day-notification-dispatch",
        )
        parent_task = asyncio.current_task()
        if parent_task is None:  # pragma: no cover - asyncio task contract
            raise RuntimeError("PAPER-day runner requires an asyncio task")
        lease_task = asyncio.create_task(
            self._lease_loop(parent_task),
            name="paper-day-lease-heartbeat",
        )
        self._lease_task = lease_task
        try:
            retained_at_start = self._store.events(self._manifest.run_id)
            startup_at = self._now()
            if retained_at_start and startup_at > self._at(self._config.market_open):
                # 开盘后出现进程边界会形成不可观测区间，即使之后恢复了所有持久化事实
                # 也不例外。因此保守标记覆盖范围，不宣称完整交易时段从未中断。
                self._partial_session = True
            migration = _requested_risk_policy_migration(
                events=retained_at_start,
                manifest=self._manifest,
                new_policy=self._risk_config.audit_document(),
                confirmation=self._risk_policy_change_confirmation,
            )
            abort = _terminal_abort_requiring_recovery(retained_at_start)
            if abort is not None and not self._recover_after_abort:
                # 此分支不得产生任何事件：DAY_ABORTED 仍是不可变日志头，操作员会收到
                # 稳定拒绝结果。尤其不能让通用异常处理器把被拒的恢复再写成第二个
                # DAY_ABORTED 事件。
                raise PaperDayAbortRecoveryRequiredError()
            self._restore_state()
            if migration is not None:
                _validate_risk_policy_migration_state(
                    pending=self._pending,
                    events=retained_at_start,
                )
                validated_position_count, validated_fill_count = (
                    self._validate_risk_policy_migration_ledger(retained_at_start)
                )
                await self._record_operator_risk_policy_change(
                    migration=migration,
                    changed_at=startup_at,
                    validated_fill_count=validated_fill_count,
                    validated_position_count=validated_position_count,
                )
            if abort is not None:
                await self._record_abort_recovery_authorized(
                    aborted=abort,
                    recovered_at=startup_at,
                )
            resume_boundary = (
                self._store.events(self._manifest.run_id)
                if migration is not None or abort is not None
                else retained_at_start
            )
            self._publisher.reconcile_notifications()
            account = self._open_or_resume_account()
            self._restore_existing_exit_protections(account)
            await self._recover_incomplete_fills()
            await self._recover_exit_plan_followups()
            completed = await self._completed_result_if_available()
            if completed is not None:
                return completed
            if (
                self._store.event_by_key(
                    self._manifest.run_id,
                    "day-completed",
                )
                is not None
            ):
                return await self._finalize()
            now = self._now()
            if resume_boundary:
                await self._record_runner_resume(
                    retained_at_start=resume_boundary,
                    resumed_at=now,
                )
            if self._intraday_llm is None:
                await self._publisher.emit(
                    event_key="llm-intraday-operator-disabled",
                    event_type="LLM_INTRADAY_OPERATOR_DISABLED",
                    phase=_phase_at(now, self._manifest.session_date, self._config),
                    severity=PaperDaySeverity.NOTICE,
                    payload={
                        "manifest_binding": self._llm_manifest,
                        "manifest_binding_sha256": _document_sha256(self._llm_manifest),
                        "new_buy_policy": "BLOCKED_MONITOR_ONLY",
                        "operator_opt_out": True,
                        "reduce_policy": "TECHNICAL_PLUS_EXISTING_DEEP_REVIEW_NO_VETO",
                    },
                )
            await self._publisher.emit(
                event_key="preflight-passed",
                event_type="PREFLIGHT_PASSED",
                phase=PaperDayPhase.BOOTSTRAP,
                severity=PaperDaySeverity.NOTICE,
                payload={
                    "account_id": self._manifest.account_id,
                    "calendar_verified": True,
                    "latest_completed_session": (self._latest_completed_session.isoformat()),
                    "notification_preflight": self._manifest.config.get(
                        "notification_preflight",
                        "CALLER_ENFORCED",
                    ),
                    "paper_ledger_verified": True,
                    "real_broker_enabled": False,
                    "session_date": self._manifest.session_date.isoformat(),
                },
                notification_text=(
                    "【A股模拟盘｜开盘前预检通过】\n"
                    f"交易日：{self._manifest.session_date.isoformat()}；"
                    f"上一完成交易日：{self._latest_completed_session.isoformat()}\n"
                    "PAPER 账本、交易日历和 QQ 出站通道已验证；真实券商接口未启用。"
                ),
            )
            await self._publisher.emit(
                event_key="day-started",
                event_type="DAY_STARTED",
                phase=PaperDayPhase.BOOTSTRAP,
                severity=PaperDaySeverity.NOTICE,
                payload={
                    "account_id": self._manifest.account_id,
                    "cash": account.cash,
                    "initial_cash": self._manifest.initial_cash,
                    "execution_mode": "PAPER_ONLY_NO_BROKER",
                    "sqlite_mode": "ONE_PROCESS_ONE_STORE_PER_DATABASE",
                },
                notification_text=(
                    "【A股模拟盘｜全天盯盘启动】\n"
                    f"交易日：{self._manifest.session_date.isoformat()}\n"
                    f"初始资金：{self._manifest.initial_cash:.2f} 元\n"
                    "模式：仅本地 PAPER，不连接任何真实券商；"
                    "买入信号只使用信号可知后才开始的完整分钟区间撮合。"
                ),
            )
            await self._initialize_intraday_llm()
            await self._run_preopen_phase(now)
            await self._wait_until(self._at(self._config.market_open))
            await self._publisher.emit(
                event_key="market-open",
                event_type="MARKET_OPENED",
                phase=PaperDayPhase.OPEN_AUCTION,
                severity=PaperDaySeverity.NOTICE,
                payload={"watchlist_count": len(self._watchlist)},
                notification_text=(
                    "【A股模拟盘｜开盘监控开始】\n"
                    f"当前关注：{len(self._watchlist)} 只；全市场异动每 "
                    f"{int(self._config.surveillance_interval.total_seconds() // 60)} "
                    "分钟维护一次。"
                ),
            )
            await self._run_open_segment(
                phase=PaperDayPhase.MORNING,
                start_at=self._at(self._config.market_open),
                end_at=self._at(self._config.morning_end),
            )
            await self._wait_until(
                self._at(self._config.morning_end) + self._config.boundary_bar_publication_grace
            )
            await self._monitor_cycle(
                phase=PaperDayPhase.MORNING,
                decision_at=self._now(),
            )
            await self._expire_pending_orders(
                phase=PaperDayPhase.LUNCH,
                event_type="ORDER_EXPIRED_AT_RECESS",
                reason="MORNING_SESSION_ENDED",
            )
            await self._publisher.emit(
                event_key="lunch-pause",
                event_type="LUNCH_PAUSE",
                phase=PaperDayPhase.LUNCH,
                severity=PaperDaySeverity.INFO,
                payload=self._account_summary_document(),
                notification_text=self._account_summary_text("午间"),
            )
            await self._wait_until(self._at(self._config.afternoon_start))
            await self._publisher.emit(
                event_key="afternoon-resume",
                event_type="AFTERNOON_RESUME",
                phase=PaperDayPhase.AFTERNOON,
                severity=PaperDaySeverity.INFO,
                payload={"watchlist_count": len(self._watchlist)},
                notification_text="【A股模拟盘｜午后监控恢复】",
            )
            await self._run_open_segment(
                phase=PaperDayPhase.AFTERNOON,
                start_at=self._at(self._config.afternoon_start),
                end_at=self._at(self._config.market_close),
            )
            await self._wait_until(
                self._at(self._config.market_close) + self._config.boundary_bar_publication_grace
            )
            await self._monitor_cycle(
                phase=PaperDayPhase.CLOSING,
                decision_at=self._now(),
            )
            await self._wait_until(self._at(self._config.finalization_time))
            return await self._finalize()
        except (PaperDayAbortRecoveryRequiredError, PaperDayRiskPolicyChangeError):
            raise
        except asyncio.CancelledError:
            if (
                lease_task.done()
                and not lease_task.cancelled()
                and (heartbeat_error := lease_task.exception()) is not None
            ):
                raise RuntimeError("PAPER-day lease heartbeat failed") from heartbeat_error
            raise
        except Exception:
            now = self._now()
            # 向上抛出前尝试写入稳定终止事件；不持久化原始异常文本，因为提供方错误
            # 可能包含带认证信息的 URL 或载荷片段。
            with suppress(Exception):
                await self._publisher.emit(
                    event_key=f"day-aborted:{now.strftime('%H%M%S')}",
                    event_type="DAY_ABORTED",
                    phase=PaperDayPhase.TERMINAL,
                    severity=PaperDaySeverity.CRITICAL,
                    payload={"error_code": "UNEXPECTED_PAPER_DAY_FAILURE"},
                    notification_text=(
                        "【A股模拟盘｜异常终止】\n"
                        "稳定错误码：UNEXPECTED_PAPER_DAY_FAILURE；真实订单从未启用。"
                    ),
                )
            raise
        finally:
            if self._intraday_llm is not None:
                with suppress(Exception):
                    await self._intraday_llm.close()
            deep_retry_tasks = tuple(self._deep_followup_retry_tasks.values())
            for task in deep_retry_tasks:
                task.cancel()
            if deep_retry_tasks:
                await asyncio.gather(*deep_retry_tasks, return_exceptions=True)
            self._deep_followup_retry_tasks.clear()
            dispatcher_task.cancel()
            lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await dispatcher_task
            with suppress(asyncio.CancelledError):
                await lease_task
            self._lease_task = None
            with suppress(Exception):
                self._store.release_lease(
                    self._manifest.run_id,
                    self._owner_id,
                )
            if self._owned_exit_plan_store is not None:
                with suppress(Exception):
                    self._owned_exit_plan_store.close()
            if self._owned_artifact_outbox is not None:
                with suppress(Exception):
                    self._owned_artifact_outbox.close()

    async def _record_operator_risk_policy_change(
        self,
        *,
        migration: _RiskPolicyMigration,
        changed_at: datetime,
        validated_fill_count: int,
        validated_position_count: int,
    ) -> None:
        """在恢复任何活动前，追加一次性的操作员边界。"""

        previous_sha256 = _document_sha256(migration.previous_policy)
        new_sha256 = _document_sha256(migration.new_policy)
        source_event = migration.source_event
        await self._publisher.emit(
            event_key=(f"operator-risk-policy-changed:{previous_sha256[:16]}:{new_sha256[:16]}"),
            event_type="OPERATOR_RISK_POLICY_CHANGED",
            phase=_phase_at(changed_at, self._manifest.session_date, self._config),
            severity=PaperDaySeverity.WARNING,
            payload={
                "account_id": self._manifest.account_id,
                "authorization_code": PAPER_RISK_POLICY_CHANGE_CONFIRMATION,
                "baseline_source": migration.baseline_source,
                "existing_fills_recomputed": False,
                "existing_positions_recomputed": False,
                "incomplete_fill_count": 0,
                "initial_cash": self._manifest.initial_cash,
                "initial_equity": self._manifest.initial_cash,
                "manifest_config_sha256": self._manifest.config_sha256,
                "new_risk_policy": dict(migration.new_policy),
                "new_risk_policy_sha256": new_sha256,
                "old_risk_policy": dict(migration.previous_policy),
                "old_risk_policy_sha256": previous_sha256,
                "operator_authorized": True,
                "pending_order_count": 0,
                "policy_diff": list(migration.policy_diff),
                "reason_codes": migration.reason_codes,
                # 保持通用字段为最新值，使后续每次重启都能发现新基线，
                # 无需为事件类型添加特殊分支。
                "risk_policy": dict(migration.new_policy),
                "risk_policy_sha256": new_sha256,
                "run_id": self._manifest.run_id,
                "session_date": self._manifest.session_date,
                "source_event_hash": (None if source_event is None else source_event.event_hash),
                "source_event_sequence": (None if source_event is None else source_event.sequence),
                "source_event_type": (None if source_event is None else source_event.event_type),
                "validated_fill_count": validated_fill_count,
                "validated_initial_cash": self._manifest.initial_cash,
                "validated_position_count": validated_position_count,
                "notification_target_hash": self._manifest.target_hash,
            },
            notification_text=(
                "【A股模拟盘｜运行中风险策略变更已获人工确认】\n"
                "变更：取消固定持仓个数上限；新增买卖可接受价格区间、"
                "破位不成交规则及分板块申报数量规则。\n"
                "安全边界：当前无待撮合委托；既有成交、持仓和资金账本不重算；"
                "现金储备、总敞口、单股权重、止损风险与整手约束继续有效。\n"
                f"旧策略：{previous_sha256[:12]}；新策略：{new_sha256[:12]}。"
            ),
            correlation_id=(None if source_event is None else source_event.event_id),
        )

    def _validate_risk_policy_migration_ledger(
        self,
        events: tuple[PaperDayEvent, ...],
    ) -> tuple[int, int]:
        """证明现有五持仓账本与当日日志一致。"""

        try:
            snapshot = self._paper.snapshot(self._manifest.account_id)
            ledger_fills = self._paper.fills(self._manifest.account_id)
        except PaperAccountNotFoundError:
            raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_LEDGER_MISSING") from None
        if snapshot.account_id != self._manifest.account_id:
            raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_ACCOUNT_MISMATCH")
        if snapshot.session_date != self._manifest.session_date:
            raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_SESSION_MISMATCH")
        active_positions = tuple(item for item in snapshot.positions if item.quantity > 0)
        if len(active_positions) != 5:
            raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_POSITION_COUNT_MISMATCH")
        reconstructed_initial_cash = snapshot.cash - sum(
            (item.cash_change for item in ledger_fills),
            start=Decimal("0"),
        )
        if reconstructed_initial_cash != self._manifest.initial_cash:
            raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_INITIAL_CASH_MISMATCH")
        journal_fill_events = tuple(item for item in events if item.event_type == "FILL_APPLIED")
        journal_fill_ids = {
            item.correlation_id for item in journal_fill_events if item.correlation_id is not None
        }
        ledger_fill_ids = {item.fill.fill_id for item in ledger_fills}
        if (
            len(journal_fill_ids) != len(journal_fill_events)
            or len(ledger_fill_ids) != len(ledger_fills)
            or journal_fill_ids != ledger_fill_ids
        ):
            raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_FILL_LEDGER_MISMATCH")
        return len(active_positions), len(ledger_fill_ids)

    async def _record_abort_recovery_authorized(
        self,
        *,
        aborted: PaperDayEvent,
        recovered_at: datetime,
    ) -> None:
        """在任何恢复写入前，追加经操作员授权的边界。

        既有事件、委托、成交和账本状态绝不重写。由终止事件哈希派生键值，
        使完全相同的重试保持幂等，而之后发生的不同中止仍需重新授权。
        """

        if aborted.event_type != "DAY_ABORTED" or aborted.phase is not PaperDayPhase.TERMINAL:
            raise ValueError("abort recovery requires a terminal DAY_ABORTED event")
        policy = _entry_policy_document(self._config)
        risk_policy = self._risk_config.audit_document()
        runner_config = _runner_config_document(self._config, self._risk_config)
        pending_order_ids = tuple(sorted(item.order_id for item in self._pending.values()))
        await self._publisher.emit(
            event_key=f"runner-recovery-after-abort:{aborted.event_hash[:24]}",
            event_type="RUNNER_RECOVERY_AFTER_ABORT",
            phase=_phase_at(recovered_at, self._manifest.session_date, self._config),
            severity=PaperDaySeverity.WARNING,
            payload={
                "aborted_error_code": aborted.payload.get("error_code"),
                "aborted_event_hash": aborted.event_hash,
                "aborted_event_sequence": aborted.sequence,
                "aborted_known_at": aborted.known_at,
                "entry_policy_sha256": _document_sha256(policy),
                "ledger_recovery_policy": "REUSE_EXISTING_LEDGER_IDEMPOTENT_FILL_IDS",
                "manifest_config_sha256": self._manifest.config_sha256,
                "operator_authorized": True,
                "order_recovery_policy": "RESTORE_ONLY_JOURNAL_OPEN_ORDERS",
                "pending_order_ids": pending_order_ids,
                "recovered_at": recovered_at,
                "restored_candidate_count": len(self._latest_candidates),
                "restored_entered_symbol_count": len(self._entered_symbols),
                "restored_last_processed_bar_count": len(self._last_processed_bar),
                "restored_position_state": "VALIDATE_FROM_LEDGER_AFTER_AUTHORIZATION",
                "restored_watchlist_count": len(self._watchlist),
                "session_coverage_after_recovery": (
                    "PARTIAL_SESSION" if self._partial_session else "FULL_SESSION"
                ),
                "resume_scope": "SAME_RUN_APPEND_ONLY",
                "run_id": self._manifest.run_id,
                "risk_policy": risk_policy,
                "risk_policy_manifest_binding": _risk_policy_manifest_binding(
                    self._manifest,
                    risk_policy,
                ),
                "risk_policy_sha256": _document_sha256(risk_policy),
                "runner_config_sha256": _document_sha256(runner_config),
            },
            notification_text=(
                "【A股模拟盘｜异常终止后恢复已获人工授权】\n"
                f"同一 Run：{self._manifest.run_id[-12:]}；"
                f"从事件 #{aborted.sequence} 之后继续。\n"
                f"已从日志恢复待撮合委托 {len(pending_order_ids)} 个；"
                "持仓随后从原账本校验，既有事件、成交和资金账本不改写。\n"
                + (
                    "持仓数量：不设硬个数上限；"
                    if self._risk_config.maximum_positions is None
                    else (f"持仓数量熔断器：{self._risk_config.maximum_positions} 个；")
                )
                + "现金储备、总敞口、单股权重、止损风险和整手约束保持启用。"
            ),
            correlation_id=aborted.event_id,
        )

    async def _record_runner_resume(
        self,
        *,
        retained_at_start: tuple[PaperDayEvent, ...],
        resumed_at: datetime,
    ) -> None:
        """记录热重启后使用的精确状态与入场策略。

        不可变清单仍是运行身份；在交易时段内修改它会创建第二次运行，而不是恢复
        首次运行。因此，运行时策略会作为单独计算哈希的审计事实记录。其事件键由
        进程启动时观察到的日志头派生，使重复恢复保持幂等，同时保留每个真实重启边界。
        """

        previous = retained_at_start[-1]
        policy = _entry_policy_document(self._config)
        risk_policy = self._risk_config.audit_document()
        runner_config = _runner_config_document(self._config, self._risk_config)
        snapshot = self._paper.snapshot(self._manifest.account_id)
        positions = tuple(item.symbol for item in snapshot.positions if item.quantity > 0)
        await self._publisher.emit(
            event_key=f"runner-resumed:{previous.event_hash[:24]}",
            event_type="RUNNER_RESUMED",
            phase=_phase_at(resumed_at, self._manifest.session_date, self._config),
            severity=PaperDaySeverity.NOTICE,
            payload={
                "entry_policy": policy,
                "entry_policy_sha256": _document_sha256(policy),
                "manifest_config_sha256": self._manifest.config_sha256,
                "previous_event_count": len(retained_at_start),
                "previous_event_hash": previous.event_hash,
                "previous_event_sequence": previous.sequence,
                "previous_event_type": previous.event_type,
                "restored_candidate_count": len(self._latest_candidates),
                "restored_entered_symbol_count": len(self._entered_symbols),
                "restored_last_processed_bar_count": len(self._last_processed_bar),
                "restored_last_price_count": len(self._last_prices),
                "restored_pending_order_count": len(self._pending),
                "restored_position_count": len(positions),
                "restored_position_symbols": positions,
                "session_coverage_after_resume": (
                    "PARTIAL_SESSION" if self._partial_session else "FULL_SESSION"
                ),
                "restored_scan_at": self._latest_scan_at,
                "restored_scan_source": self._latest_scan_source,
                "restored_scan_status": self._latest_scan_status,
                "restored_watchlist_count": len(self._watchlist),
                "resumed_at": resumed_at,
                "risk_policy": risk_policy,
                "risk_policy_manifest_binding": _risk_policy_manifest_binding(
                    self._manifest,
                    risk_policy,
                ),
                "risk_policy_sha256": _document_sha256(risk_policy),
                "runner_config_sha256": _document_sha256(runner_config),
            },
        )

    async def _initialize_intraday_llm(self) -> None:
        if self._intraday_llm is None:
            return
        now = self._now()
        identity = self._intraday_llm.analyzer_identity
        if identity is None:  # pragma: no cover - constructor invariant
            raise RuntimeError("enabled intraday LLM lost its analyzer identity")
        maximum_reviews = self._intraday_llm.config.maximum_reviews_per_session
        await self._publisher.emit(
            event_key="llm-intraday-policy-configured",
            event_type="LLM_INTRADAY_POLICY_CONFIGURED",
            phase=_phase_at(now, self._manifest.session_date, self._config),
            severity=PaperDaySeverity.NOTICE,
            payload={
                "manifest_binding": self._llm_manifest,
                "manifest_binding_sha256": _document_sha256(self._llm_manifest),
                "maximum_reviews_per_session": maximum_reviews,
                "model_prompt_identity_sha256": identity.manifest_sha256,
                "network_wait_on_buy_path": False,
                "reduce_policy": "NOT_APPLICABLE_SELL_NEVER_BLOCKED",
                "session_review_budget": (
                    "UNLIMITED" if maximum_reviews is None else maximum_reviews
                ),
            },
            notification_text=(
                "【A股模拟盘｜盘中 LLM 复核策略已启用】\n"
                f"模型：{identity.requested_model}；"
                f"Prompt 合约：{identity.prompt_schema_sha256[:12]}。\n"
                "模型仅能确认、否决或降级技术买入；买入触发路径不联网等待；"
                "卖出信号永不受 LLM 阻断。"
            ),
        )
        evidence_manifest = self._llm_evidence_manifest
        if evidence_manifest is None:  # pragma: no cover - constructor invariant
            raise RuntimeError("enabled intraday LLM lost its evidence binding")
        evidence_hash = intraday_llm_document_sha256(evidence_manifest)
        runtime_evidence_hash = (
            None
            if self._llm_evidence_runtime_manifest is None
            else intraday_llm_document_sha256(self._llm_evidence_runtime_manifest)
        )
        if self._llm_evidence_available:
            await self._publisher.emit(
                event_key=f"llm-evidence-snapshot-bound:{evidence_hash[:24]}",
                event_type="LLM_EVIDENCE_SNAPSHOT_BOUND",
                phase=_phase_at(now, self._manifest.session_date, self._config),
                severity=PaperDaySeverity.INFO,
                payload={
                    "evidence_snapshot_binding": evidence_manifest,
                    "evidence_snapshot_binding_sha256": evidence_hash,
                    "replay_available": True,
                },
            )
        else:
            self._llm_preopen_context = None
            await self._publisher.emit(
                event_key=f"llm-evidence-snapshot-failed:{evidence_hash[:24]}",
                event_type="LLM_EVIDENCE_SNAPSHOT_FAILED",
                phase=_phase_at(now, self._manifest.session_date, self._config),
                severity=PaperDaySeverity.WARNING,
                payload={
                    "error_code": "LLM_EVIDENCE_SNAPSHOT_UNAVAILABLE",
                    "evidence_snapshot_binding": evidence_manifest,
                    "evidence_snapshot_binding_sha256": evidence_hash,
                    "replay_available": False,
                    "required_for_buy": self._config.intraday_llm_required_for_buy,
                    "runtime_snapshot_audit_sha256": runtime_evidence_hash,
                },
                notification_text=(
                    "【A股模拟盘｜LLM 证据快照不可重放】\n"
                    "稳定原因码：LLM_EVIDENCE_SNAPSHOT_UNAVAILABLE。\n"
                    + (
                        "新买入按强制复核策略失败关闭；行情监控和卖出信号记录继续运行。"
                        if self._config.intraday_llm_required_for_buy
                        else "盘中模型复核将旁路；行情监控和卖出信号记录继续运行。"
                    )
                ),
            )
        if self._llm_restored_reviews or self._llm_restore_rejections:
            restore_identity = _document_sha256(
                {
                    "rejected": self._llm_restore_rejections,
                    "restored": self._llm_restored_reviews,
                    "symbols": sorted(self._latest_llm_context_by_symbol),
                }
            )
            await self._publisher.emit(
                event_key=f"llm-cache-restored:{restore_identity[:24]}",
                event_type="LLM_REVIEW_CACHE_RESTORED",
                phase=_phase_at(now, self._manifest.session_date, self._config),
                severity=(
                    PaperDaySeverity.INFO
                    if self._llm_restore_rejections == 0
                    else PaperDaySeverity.WARNING
                ),
                payload={
                    "rejected_review_count": self._llm_restore_rejections,
                    "restored_review_count": self._llm_restored_reviews,
                    "restored_symbol_count": len(self._latest_llm_context_by_symbol),
                },
            )

        context = self._llm_preopen_context
        failure_code: str | None = None
        if self._llm_preopen_restore_conflict:
            failure_code = "LLM_PREOPEN_CONTEXT_RESTORE_CONFLICT"
        elif context is None:
            failure_code = "LLM_PREOPEN_CONTEXT_UNAVAILABLE"
        elif context.analyzer_identity != identity:
            failure_code = "LLM_PREOPEN_CONTEXT_IDENTITY_MISMATCH"
        elif context.response_model != identity.requested_model:
            failure_code = "LLM_PREOPEN_CONTEXT_MODEL_MISMATCH"
        elif context.decision is MacroAnalysisDecision.ABSTAIN:
            failure_code = "LLM_PREOPEN_CONTEXT_ABSTAINED"
        elif context.known_at > now:
            failure_code = "LLM_PREOPEN_CONTEXT_FROM_FUTURE"
        elif context.valid_until < self._at(self._config.market_close):
            failure_code = "LLM_PREOPEN_CONTEXT_EXPIRES_BEFORE_CLOSE"
        if failure_code is not None:
            self._llm_preopen_context = None
            await self._publisher.emit(
                event_key=f"llm-preopen-failed:{failure_code.lower()}",
                event_type="LLM_PREOPEN_CONTEXT_FAILED",
                phase=_phase_at(now, self._manifest.session_date, self._config),
                severity=PaperDaySeverity.WARNING,
                payload={
                    "error_code": failure_code,
                    "required_for_buy": self._config.intraday_llm_required_for_buy,
                },
                notification_text=(
                    "【A股模拟盘｜盘前 LLM 宏观基线不可用】\n"
                    f"稳定原因码：{failure_code}。\n"
                    + (
                        "新买入将失败关闭；行情监控和卖出信号记录继续运行。"
                        if self._config.intraday_llm_required_for_buy
                        else "盘中模型复核不作为强制买入条件；卖出信号照常记录。"
                    )
                ),
            )
            return
        assert context is not None
        await self._publisher.emit(
            event_key=f"llm-preopen-frozen:{context.context_id}",
            event_type="LLM_PREOPEN_CONTEXT_FROZEN",
            phase=_phase_at(now, self._manifest.session_date, self._config),
            severity=PaperDaySeverity.NOTICE,
            payload={"preopen_context": context.audit_document()},
            notification_text=(
                "【A股模拟盘｜盘前 LLM 宏观基线已冻结】\n"
                f"生产结论：{humanize_internal_code(context.decision.value)}；"
                f"宏观影响：{context.macro_impact}；"
                f"有效至：{context.valid_until.astimezone(SHANGHAI):%H:%M}。\n"
                f"{_llm_preopen_dual_text(context)}\n"
                "盘中候选复核只能使用该已留痕基线及当时可知证据。"
            ),
            occurred_at=context.known_at,
        )

    async def _schedule_intraday_llm_reviews(
        self,
        *,
        phase: PaperDayPhase,
        scan: AShareSurveillanceRun,
        candidates: tuple[IntradayCandidate, ...],
    ) -> None:
        coordinator = self._intraday_llm
        factory = self._intraday_llm_plan_factory
        if coordinator is None or factory is None:
            return
        if scan.status.value != "COMPLETE":
            await self._publisher.emit(
                event_key=f"llm-review-scan-skipped:{scan.source_revision}",
                event_type="LLM_REVIEW_BATCH_SKIPPED",
                phase=phase,
                severity=PaperDaySeverity.WARNING,
                payload={
                    "error_code": "LLM_REVIEW_REQUIRES_COMPLETE_SCAN",
                    "scan_status": scan.status.value,
                    "source_revision": scan.source_revision,
                },
            )
            return
        preopen = self._llm_preopen_context if self._llm_evidence_available else None
        ordered = sorted(
            candidates,
            key=lambda item: (
                0 if item.candidate_class is IntradayCandidateClass.MOMENTUM_EXPANSION else 1,
                -item.anomaly_score,
                -item.factor_weight_coverage,
                item.symbol,
            ),
        )[: coordinator.config.review_top_n]
        requested_at = self._now()
        outcomes: list[dict[str, object]] = []
        for candidate in ordered:
            unavailable_context_id = (
                "llm-unavailable-"
                + _document_sha256(
                    {
                        "scan_revision": scan.source_revision,
                        "symbol": candidate.symbol,
                    }
                )[:40]
            )
            if preopen is None:
                self._latest_llm_context_by_symbol[candidate.symbol] = unavailable_context_id
                outcomes.append(
                    {
                        "error_code": (
                            "LLM_PREOPEN_CONTEXT_UNAVAILABLE"
                            if self._llm_evidence_available
                            else "LLM_EVIDENCE_SNAPSHOT_UNAVAILABLE"
                        ),
                        "status": "NOT_SCHEDULED",
                        "symbol": candidate.symbol,
                    }
                )
                continue
            try:
                plan = factory.prepare_candidate_review(
                    candidate=candidate,
                    scan=scan,
                    preopen_context=preopen,
                    requested_at=requested_at,
                )
                if plan is None:
                    raise ValueError("plan factory abstained")
                scope_sha256 = intraday_llm_document_sha256(
                    {
                        "candidate": _candidate_document(candidate),
                        "scan_revision": scan.source_revision,
                    }
                )
                context = coordinator.create_context(
                    session_date=self._manifest.session_date,
                    preopen_context_id=preopen.context_id,
                    scan_revision=scan.source_revision,
                    candidate_scope_sha256=scope_sha256,
                    requested_at=requested_at,
                    plan=plan,
                )
                schedule = coordinator.schedule(context)
            except Exception:
                self._latest_llm_context_by_symbol[candidate.symbol] = unavailable_context_id
                outcomes.append(
                    {
                        "error_code": "LLM_REVIEW_PREPARE_FAILED",
                        "status": "NOT_SCHEDULED",
                        "symbol": candidate.symbol,
                    }
                )
                continue
            self._latest_llm_context_by_symbol[candidate.symbol] = context.context_id
            outcomes.append(
                {
                    "context_id": context.context_id,
                    "review_id": schedule.review_id,
                    "status": schedule.status.value,
                    "symbol": candidate.symbol,
                }
            )
        await self._publisher.emit(
            event_key=f"llm-review-batch:{scan.source_revision}",
            event_type="LLM_REVIEW_BATCH_SCHEDULED",
            phase=phase,
            severity=PaperDaySeverity.INFO,
            payload={
                "candidate_count": len(ordered),
                "outcomes": outcomes,
                "preopen_context_id": None if preopen is None else preopen.context_id,
                "source_revision": scan.source_revision,
            },
        )

    async def _drain_intraday_llm_reviews(self, *, phase: PaperDayPhase) -> None:
        coordinator = self._intraday_llm
        if coordinator is None:
            return
        reviews = coordinator.drain_completed()
        for review in reviews:
            review_document = intraday_llm_review_document(review)
            event_type = (
                "LLM_CANDIDATE_REVIEW_FAILED"
                if review.failure_code is not None
                else "LLM_CANDIDATE_REVIEW_COMPLETED"
            )
            event = await self._publisher.emit(
                event_key=f"llm-review-completed:{review.review_id}",
                event_type=event_type,
                phase=phase,
                severity=(
                    PaperDaySeverity.WARNING
                    if review.failure_code is not None
                    else PaperDaySeverity.INFO
                ),
                payload={
                    "cache_acceptance_policy": "JOURNAL_EVENT_BEFORE_TRADABLE_CACHE",
                    "review": review_document,
                },
                symbol=review.symbol,
                correlation_id=review.review_id,
                occurred_at=review.completed_at,
            )
            if event.known_at >= review.expires_at:
                await self._publisher.emit(
                    event_key=f"llm-review-cache-expired:{review.review_id}",
                    event_type="LLM_CANDIDATE_REVIEW_CACHE_REJECTED",
                    phase=phase,
                    severity=PaperDaySeverity.WARNING,
                    payload={
                        "error_code": "LLM_REVIEW_EXPIRED_BEFORE_JOURNAL_ACCEPTANCE",
                        "review_id": review.review_id,
                    },
                    symbol=review.symbol,
                    correlation_id=review.review_id,
                )
                continue
            try:
                coordinator.accept_journaled(
                    review.review_id,
                    accepted_at=event.known_at,
                    journal_event_id=event.event_id,
                    journal_event_sha256=event.event_hash,
                )
            except (KeyError, TypeError, ValueError):
                await self._publisher.emit(
                    event_key=f"llm-review-cache-rejected:{review.review_id}",
                    event_type="LLM_CANDIDATE_REVIEW_CACHE_REJECTED",
                    phase=phase,
                    severity=PaperDaySeverity.WARNING,
                    payload={
                        "error_code": "LLM_REVIEW_CACHE_IDENTITY_REJECTED",
                        "review_id": review.review_id,
                    },
                    symbol=review.symbol,
                    correlation_id=review.review_id,
                )
        await self._record_llm_service_state(reviews=reviews, phase=phase)

    async def _record_llm_service_state(
        self,
        *,
        reviews: tuple[IntradayLLMReview, ...],
        phase: PaperDayPhase,
    ) -> None:
        """每次排空仅发出一个聚合健康状态变化，不按失败候选逐个发出。"""

        if not reviews:
            return
        failed = tuple(item for item in reviews if item.failure_code is not None)
        next_state = "DEGRADED" if failed else "HEALTHY"
        previous_state = self._llm_service_state
        if next_state == previous_state:
            return
        self._llm_service_state = next_state
        failure_counts: dict[str, int] = {}
        for review in failed:
            assert review.failure_code is not None
            failure_counts[review.failure_code] = failure_counts.get(review.failure_code, 0) + 1
        transition_id = _document_sha256(
            {
                "next_state": next_state,
                "previous_state": previous_state,
                "review_ids": sorted(item.review_id for item in reviews),
            }
        )
        notification_text = None
        if next_state == "DEGRADED":
            notification_text = (
                "【A股模拟盘｜盘中 LLM 服务降级】\n"
                f"本批复核成功 {len(reviews) - len(failed)}，失败 {len(failed)}；"
                f"稳定失败码：{', '.join(sorted(failure_counts)) or 'UNKNOWN'}。\n"
                "不会逐候选刷屏；强制复核的买入继续按对应结果失败关闭，卖出信号不受影响。"
            )
        elif previous_state == "DEGRADED":
            notification_text = (
                "【A股模拟盘｜盘中 LLM 服务已恢复】\n"
                f"本批 {len(reviews)} 个候选复核全部完成；服务状态恢复为 HEALTHY。\n"
                "买入仍只读取已先写入日志的有效缓存，卖出信号不受影响。"
            )
        await self._publisher.emit(
            event_key=f"llm-service-state:{transition_id[:32]}",
            event_type="LLM_SERVICE_STATE_CHANGED",
            phase=phase,
            severity=(
                PaperDaySeverity.WARNING if next_state == "DEGRADED" else PaperDaySeverity.INFO
            ),
            payload={
                "failed_review_count": len(failed),
                "failure_code_counts": failure_counts,
                "previous_state": previous_state,
                "review_count": len(reviews),
                "state": next_state,
                "successful_review_count": len(reviews) - len(failed),
            },
            notification_text=notification_text,
        )

    async def _run_preopen_phase(self, now: datetime) -> None:
        completed = self._store.event_by_key(
            self._manifest.run_id,
            "preopen-screen-completed",
        )
        if completed is not None:
            # 传输成功时，如果所有历史因子请求均失败，仍可能没有可排名候选。
            # 重启后的运行器可以通过已恢复健康或具备回退能力的适配器重试，
            # 但只能在原盘前决策窗口仍开放时执行。首次结果保持不可变，恢复另记事件。
            recovered = self._store.event_by_key(
                self._manifest.run_id,
                "preopen-screen-recovered",
            )
            local_now = now.astimezone(SHANGHAI)
            if (
                recovered is None
                and _paper_day_event_has_empty_candidates(completed)
                and local_now.time() <= self._config.latest_preopen_start
            ):
                await self._recover_empty_preopen_screen(started=now)
            return
        if any(
            self._store.event_by_key(self._manifest.run_id, key) is not None
            for key in (
                "preopen-screen-failed",
                "preopen-missed",
                "preopen-start-window-missed",
            )
        ):
            return
        market_open = self._at(self._config.market_open)
        if now >= market_open:
            self._partial_session = True
            await self._publisher.emit(
                event_key="preopen-missed",
                event_type="PREOPEN_SCREEN_MISSED",
                phase=PaperDayPhase.PREOPEN,
                severity=PaperDaySeverity.WARNING,
                payload={"started_at": now},
                notification_text=(
                    "【A股模拟盘｜盘前筛选缺口】\n"
                    "进程在开盘后启动，将如实标记 PARTIAL_SESSION，并以盘中全市场扫描建立名单。"
                ),
            )
            return
        await self._wait_until(self._at(self._config.preopen_screen_time))
        started = self._now()
        if started.astimezone(SHANGHAI).time() > self._config.latest_preopen_start:
            self._partial_session = True
            await self._publisher.emit(
                event_key="preopen-start-window-missed",
                event_type="PREOPEN_SCREEN_MISSED",
                phase=PaperDayPhase.PREOPEN,
                severity=PaperDaySeverity.WARNING,
                payload={
                    "error_code": "PREOPEN_START_WINDOW_MISSED",
                    "started_at": started,
                },
                notification_text=(
                    "【A股模拟盘｜盘前筛选时间窗已错过】\n"
                    "不会倒填或伪造昨收名单；开盘后由当日全市场扫描建立关注名单。"
                ),
            )
            return
        await self._publisher.emit(
            event_key="preopen-screen-started",
            event_type="PREOPEN_SCREEN_STARTED",
            phase=PaperDayPhase.PREOPEN,
            severity=PaperDaySeverity.INFO,
            payload={"as_of": self._preopen_as_of()},
        )
        await self._complete_original_preopen_screen(started=started)

    async def _complete_original_preopen_screen(self, *, started: datetime) -> None:
        """记录启动事件后，完成正常的首次盘前运行。"""

        try:
            result = await self._preopen.run(
                as_of=self._preopen_as_of(),
                requested_at=started,
            )
        except Exception:
            await self._publisher.emit(
                event_key="preopen-screen-failed",
                event_type="PREOPEN_SCREEN_FAILED",
                phase=PaperDayPhase.PREOPEN,
                severity=PaperDaySeverity.ERROR,
                payload={"error_code": "PREOPEN_SCREEN_UNAVAILABLE"},
                notification_text=(
                    "[A-share PAPER | pre-open screen failed]\n"
                    "No data was invented; intraday whole-market surveillance "
                    "will build the watchlist."
                ),
            )
            return
        self._apply_initial_screen(result)
        candidates = [
            {
                "symbol": item.symbol,
                "name": item.name,
                "board": item.board.value,
                "rank": item.rank,
                "score": item.composite_score,
            }
            for item in result.top_candidates
        ]
        lines = [
            f"{item.rank}. {item.name} ({item.symbol}); score={item.score:.3f}"
            for item in self._initial[:10]
        ]
        await self._publisher.emit(
            event_key="preopen-screen-completed",
            event_type="PREOPEN_SCREEN_COMPLETED",
            phase=PaperDayPhase.PREOPEN,
            severity=PaperDaySeverity.NOTICE,
            payload={
                "as_of": result.as_of,
                "candidate_count": len(candidates),
                "candidates": candidates,
                "decision_at": result.decision_at,
                "factor_source_id": result.factor_source_id,
                "factor_source_revision": result.factor_source_revision,
                "universe_source_id": result.universe_source_id,
                "universe_source_revision": result.universe_source_revision,
                "warnings": result.warnings,
            },
            notification_text=(
                "[A-share PAPER | initial pre-open watchlist]\n"
                f"As of {result.as_of.isoformat()}: {len(candidates)} candidates.\n"
                + "\n".join(lines)
                + "\nResearch seed only; intraday anomaly and completed minute "
                "bars must corroborate it after open."
            ),
        )

    async def _recover_empty_preopen_screen(self, *, started: datetime) -> None:
        """重试空筛选结果，然后回退到经审计、仅含 L1 的种子。

        两条恢复路径均不授予执行权限。常规入场门禁仍要求当前时段最新且 COMPLETE 的
        全市场扫描、一个 MOMENTUM_EXPANSION 候选，以及一项已完成分钟 K 线信号。
        """

        await self._publisher.emit(
            event_key="preopen-screen-recovery-started",
            event_type="PREOPEN_SCREEN_RECOVERY_STARTED",
            phase=PaperDayPhase.PREOPEN,
            severity=PaperDaySeverity.WARNING,
            payload={
                "as_of": self._preopen_as_of(),
                "reason": "ORIGINAL_SCREEN_RETURNED_ZERO_CANDIDATES",
            },
        )
        result: ASharePreopenScreeningRun | None
        retry_error = False
        try:
            result = await self._preopen.run(
                as_of=self._preopen_as_of(),
                requested_at=started,
            )
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except Exception:
            result = None
            retry_error = True

        if result is not None and result.top_candidates:
            entries = tuple(
                replace(
                    _watch_entry_from_screen(item),
                    source="PREOPEN_SCREEN_RECOVERY",
                )
                for item in result.top_candidates[: self._config.maximum_watchlist_size]
            )
            self._initial = entries
            self._watchlist = {item.symbol: item for item in entries}
            await self._emit_preopen_recovered(
                entries=entries,
                recovery_method="FULL_MULTIFACTOR_RETRY",
                source_id=result.factor_source_id or result.universe_source_id,
                source_revision=(result.factor_source_revision or result.universe_source_revision),
                available_at=result.decision_at,
                warnings=result.warnings,
                evidence={
                    "as_of": result.as_of,
                    "factor_requested_count": result.factor_requested_count,
                    "hard_filter_eligible_count": result.hard_filter_eligible_count,
                    "ranked_count": result.ranked_count,
                    "universe_count": result.universe_count,
                    "universe_source_id": result.universe_source_id,
                    "universe_source_revision": result.universe_source_revision,
                    "factor_source_id": result.factor_source_id,
                    "factor_source_revision": result.factor_source_revision,
                },
            )
            return

        seed = self._load_preopen_recovery_seed()
        if seed is not None:
            self._initial = seed.entries
            self._watchlist = {item.symbol: item for item in seed.entries}
            await self._emit_preopen_recovered(
                entries=seed.entries,
                recovery_method=seed.method,
                source_id=seed.source_id,
                source_revision=seed.sha256,
                available_at=seed.available_at,
                warnings=(
                    "PREOPEN_DEGRADED_RESEARCH_SEED_ONLY",
                    "NOT_A_FULL_MULTIFACTOR_SCREEN",
                    "MUST_NOT_AUTHORIZE_ENTRY",
                    (
                        "CURRENT_SESSION_COMPLETE_SURVEILLANCE_AND_COMPLETED_"
                        "MINUTE_CORROBORATION_REQUIRED"
                    ),
                ),
                evidence={
                    "raw_universe_count": seed.raw_universe_count,
                    "eligible_reliable_l1_count": seed.eligible_count,
                    "retry_error": retry_error,
                    "retry_returned_zero_candidates": (
                        result is not None and not result.top_candidates
                    ),
                    "sidecar_sha256": seed.sha256,
                },
            )
            return

        await self._publisher.emit(
            event_key="preopen-screen-recovery-unavailable",
            event_type="PREOPEN_SCREEN_RECOVERY_UNAVAILABLE",
            phase=PaperDayPhase.PREOPEN,
            severity=PaperDaySeverity.ERROR,
            payload={
                "error_code": "NO_VALID_RECOVERY_CANDIDATES",
                "multifactor_retry_failed": retry_error,
                "multifactor_retry_returned_zero": (
                    result is not None and not result.top_candidates
                ),
            },
            notification_text=(
                "[A-share PAPER | pre-open recovery unavailable]\n"
                "The multifactor retry and audited L1 seed both produced no valid "
                "candidate. Intraday whole-market surveillance will build the list."
            ),
        )

    async def _emit_preopen_recovered(
        self,
        *,
        entries: tuple[PaperDayWatchEntry, ...],
        recovery_method: str,
        source_id: str,
        source_revision: str,
        available_at: datetime,
        warnings: tuple[str, ...],
        evidence: Mapping[str, object],
    ) -> None:
        candidates = [_watch_entry_document(item) for item in entries]
        lines = [f"{item.rank}. {item.name} ({item.symbol})" for item in entries[:10]]
        await self._publisher.emit(
            event_key="preopen-screen-recovered",
            event_type="PREOPEN_SCREEN_RECOVERED",
            phase=PaperDayPhase.PREOPEN,
            severity=PaperDaySeverity.NOTICE,
            payload={
                "as_of": self._preopen_as_of(),
                "available_at": available_at,
                "candidate_count": len(candidates),
                "candidates": candidates,
                "execution_authority": False,
                "recovery_method": recovery_method,
                "source_id": source_id,
                "source_revision": source_revision,
                "warnings": list(warnings),
                **dict(evidence),
            },
            notification_text=(
                "[A-share PAPER | recovered pre-open watchlist]\n"
                f"Candidates: {len(entries)}; method: {recovery_method}.\n"
                + "\n".join(lines)
                + "\nResearch seed only; entry still requires a fresh COMPLETE "
                "intraday scan and completed minute-bar confirmation."
            ),
        )

    def _load_preopen_recovery_seed(self) -> _PreopenRecoverySeed | None:
        path = self._preopen_recovery_path
        if path is None:
            return None
        digest_path = path.with_name(path.name + ".sha256")
        try:
            if (
                path.is_symlink()
                or digest_path.is_symlink()
                or not path.is_file()
                or not digest_path.is_file()
                or path.stat().st_size > 1_000_000
            ):
                return None
            content = path.read_bytes()
            expected = digest_path.read_text(encoding="ascii").strip().lower()
            actual = hashlib.sha256(content).hexdigest()
            if len(expected) != 64 or any(ch not in "0123456789abcdef" for ch in expected):
                return None
            if actual != expected:
                return None
            document = json.loads(content.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return None
        if not isinstance(document, dict):
            return None
        required_policy = {
            "schema_version": "ashare-paper-preopen-recovery@1",
            "session_date": self._manifest.session_date.isoformat(),
            "verified_latest_completed_session": self._preopen_as_of().isoformat(),
            "quality": "DEGRADED",
            "method": "RELIABLE_L1_HARD_FILTER_THEN_SESSION_AMOUNT_RANK_ONLY",
            "execution_authority": False,
        }
        if any(document.get(key) != value for key, value in required_policy.items()):
            return None
        try:
            available_at = datetime.fromisoformat(str(document["available_at"]))
            if available_at.tzinfo is None or available_at.utcoffset() is None:
                return None
            local_available = available_at.astimezone(SHANGHAI)
            if (
                local_available.date() != self._manifest.session_date
                or local_available.time() > self._config.latest_preopen_start
                or available_at.astimezone(UTC) > self._now()
            ):
                return None
            raw_count = _strict_positive_int(document.get("raw_universe_count"))
            eligible_count = _strict_positive_int(document.get("eligible_reliable_l1_count"))
            candidate_count = _strict_positive_int(document.get("candidate_count"))
            if raw_count < 4500 or eligible_count < candidate_count:
                return None
            source_id = document.get("source_id")
            values = document.get("candidates")
            if not isinstance(source_id, str) or not source_id.startswith(
                "AKShare/Tencent stock_zh_a_spot_tx/"
            ):
                return None
            if not isinstance(values, list) or len(values) != candidate_count:
                return None
            if candidate_count > self._config.maximum_watchlist_size:
                return None
            entries: list[PaperDayWatchEntry] = []
            for expected_rank, value in enumerate(values, start=1):
                if not isinstance(value, dict) or value.get("rank") != expected_rank:
                    return None
                item = _watch_entry_from_document(value, "PREOPEN_L1_RECOVERY")
                if item is None or item.board is AShareBoard.BSE:
                    return None
                if item.board is not _board_from_symbol(item.symbol):
                    return None
                for field in ("last_price", "session_amount_cny", "market_cap_cny"):
                    number = Decimal(str(value.get(field)))
                    if not number.is_finite() or number <= 0:
                        return None
                entries.append(replace(item, source="PREOPEN_L1_RECOVERY", score=0.0))
            symbols = tuple(item.symbol for item in entries)
            if len(symbols) != len(set(symbols)):
                return None
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return None
        return _PreopenRecoverySeed(
            available_at=available_at.astimezone(UTC),
            source_id=source_id,
            method=str(document["method"]),
            sha256=actual,
            raw_universe_count=raw_count,
            eligible_count=eligible_count,
            entries=tuple(entries),
        )

    def _preopen_as_of(self) -> date:
        return self._latest_completed_session

    def _apply_initial_screen(self, result: ASharePreopenScreeningRun) -> None:
        entries = tuple(
            _watch_entry_from_screen(item)
            for item in result.top_candidates[: self._config.maximum_watchlist_size]
        )
        self._initial = entries
        self._watchlist = {item.symbol: item for item in entries}

    async def _run_open_segment(
        self,
        *,
        phase: PaperDayPhase,
        start_at: datetime,
        end_at: datetime,
    ) -> None:
        now = self._now()
        if now < start_at:
            await self._wait_until(start_at)
            now = self._now()
        if now >= end_at:
            self._partial_session = True
            return
        first_scan = start_at + self._config.first_intraday_scan_delay
        next_scan = max(first_scan, now)
        next_monitor = now
        next_health = now + self._config.health_notification_interval
        while (now := self._now()) < end_at:
            self._raise_if_background_failed()
            await self._renew_lease()
            await self._drain_intraday_llm_reviews(phase=phase)
            if now >= next_scan:
                await self._run_surveillance_scan(phase=phase, requested_at=now)
                next_scan = self._now() + self._config.surveillance_interval
            if now >= next_monitor:
                await self._monitor_cycle(phase=phase, decision_at=now)
                next_monitor = self._now() + self._config.primary_monitor_interval
            if now >= next_health:
                await self._publisher.emit(
                    event_key=f"health:{phase.value}:{now.strftime('%H%M')}",
                    event_type="HEALTH_CHECKPOINT",
                    phase=phase,
                    severity=PaperDaySeverity.INFO,
                    payload={
                        **self._account_summary_document(),
                        "pending_orders": len(self._pending),
                        "watchlist_count": len(self._watchlist),
                    },
                    notification_text=self._account_summary_text("定时状态"),
                )
                next_health = now + self._config.health_notification_interval
            next_due = min(next_scan, next_monitor, next_health, end_at)
            await self._sleep(
                min(
                    self._config.scheduler_tick_seconds,
                    max(0.05, (next_due - self._now()).total_seconds()),
                )
            )

    async def _run_surveillance_scan(
        self,
        *,
        phase: PaperDayPhase,
        requested_at: datetime,
    ) -> None:
        try:
            run = await self._surveillance.run_once(
                session_date=self._manifest.session_date,
                requested_at=requested_at,
            )
        except Exception:
            await self._source_transition(
                component="whole-market-surveillance",
                state="FAILED",
                phase=phase,
                error_code="SURVEILLANCE_UNAVAILABLE",
            )
            return
        await self._source_transition(
            component="whole-market-surveillance",
            state="DEGRADED" if run.status.value == "DEGRADED" else "HEALTHY",
            phase=phase,
            error_code=None,
        )
        supported_candidates = tuple(
            item
            for item in run.ranking.candidates
            if _board_from_symbol(item.symbol) is not AShareBoard.BSE
        )
        self._latest_candidates = {item.symbol: item for item in supported_candidates}
        for item in supported_candidates:
            previous_close = _candidate_previous_close(item)
            if previous_close is not None:
                self._session_previous_closes[item.symbol] = previous_close
        self._latest_scan_at = run.decision_at
        self._latest_scan_source = run.source_id
        self._latest_scan_status = run.status.value
        candidates = [_candidate_document(item) for item in supported_candidates]
        await self._publisher.emit(
            event_key=f"scan:{run.source_revision}",
            event_type="SURVEILLANCE_SCAN_COMPLETED",
            phase=phase,
            severity=PaperDaySeverity.INFO,
            payload={
                "candidate_count": len(candidates),
                "candidates": candidates,
                "decision_at": run.decision_at,
                "source_id": run.source_id,
                "source_revision": run.source_revision,
                "status": run.status.value,
                "universe_count": run.universe_count,
                "warnings": run.warnings,
                "unsupported_bse_candidates_excluded": (
                    len(run.ranking.candidates) - len(supported_candidates)
                ),
            },
        )
        await self._maintain_watchlist(phase=phase, scan=run)
        await self._schedule_intraday_llm_reviews(
            phase=phase,
            scan=run,
            candidates=supported_candidates,
        )

    async def _maintain_watchlist(
        self,
        *,
        phase: PaperDayPhase,
        scan: AShareSurveillanceRun,
    ) -> None:
        wanted: dict[str, PaperDayWatchEntry] = {}
        snapshot = self._paper.snapshot(self._manifest.account_id)
        # 已持仓和待处理标的永不移出名单。
        for position in snapshot.positions:
            if position.quantity > 0:
                wanted[position.symbol] = self._watchlist.get(
                    position.symbol,
                    PaperDayWatchEntry(
                        position.symbol,
                        position.symbol,
                        _board_from_symbol(position.symbol),
                        "PAPER_POSITION",
                        0,
                        1.0,
                    ),
                )
        for order in self._pending.values():
            wanted[order.symbol] = self._watchlist.get(
                order.symbol,
                PaperDayWatchEntry(
                    order.symbol,
                    order.symbol,
                    order.board,
                    "PENDING_ORDER",
                    0,
                    1.0,
                ),
            )
        for item in self._initial[: self._config.initial_core_size]:
            wanted.setdefault(item.symbol, item)
        dynamic_candidates = tuple(
            item
            for item in scan.ranking.candidates
            if _board_from_symbol(item.symbol) is not AShareBoard.BSE
        )
        for candidate in dynamic_candidates[: self._config.dynamic_candidate_size]:
            wanted.setdefault(candidate.symbol, _watch_entry_from_intraday(candidate))
        for item in self._initial:
            if len(wanted) >= self._config.maximum_watchlist_size:
                break
            wanted.setdefault(item.symbol, item)
        if len(wanted) > self._config.maximum_watchlist_size:
            protected = {item.symbol for item in snapshot.positions if item.quantity > 0} | {
                item.symbol for item in self._pending.values()
            }
            ordered = sorted(
                wanted.values(),
                key=lambda item: (
                    0 if item.symbol in protected else 1,
                    0 if item.source == "INTRADAY_SCAN" else 1,
                    item.rank,
                    item.symbol,
                ),
            )
            wanted = {item.symbol: item for item in ordered[: self._config.maximum_watchlist_size]}
        old = set(self._watchlist)
        new = set(wanted)
        added = sorted(new - old)
        removed = sorted(old - new)
        self._watchlist = wanted
        if not added and not removed:
            return
        watchlist = [_watch_entry_document(item) for item in wanted.values()]
        await self._publisher.emit(
            event_key=(
                "watchlist:"
                + hashlib.sha256(
                    json.dumps(watchlist, sort_keys=True, ensure_ascii=False).encode("utf-8")
                ).hexdigest()[:24]
                + f":{scan.source_revision[:16]}"
            ),
            event_type="WATCHLIST_UPDATED",
            phase=phase,
            severity=PaperDaySeverity.NOTICE,
            payload={
                "added": added,
                "removed": removed,
                "watchlist": watchlist,
                "source_revision": scan.source_revision,
            },
            notification_text=(
                "【A股模拟盘｜关注名单更新】\n"
                f"新增：{', '.join(added) if added else '无'}\n"
                f"移除：{', '.join(removed) if removed else '无'}\n"
                f"当前共 {len(wanted)} 只；持仓及待撮合标的不会被移除。"
            ),
        )

    async def _monitor_cycle(
        self,
        *,
        phase: PaperDayPhase,
        decision_at: datetime,
    ) -> None:
        await self._drain_intraday_llm_reviews(phase=phase)
        snapshot = self._paper.snapshot(self._manifest.account_id)
        held = {item.symbol for item in snapshot.positions if item.quantity > 0}
        symbols = set(self._watchlist) | held | set(self._pending)
        if not symbols:
            return
        semaphore = asyncio.Semaphore(self._config.market_data_concurrency)

        due: list[tuple[str, MinuteInterval]] = []
        for symbol in sorted(symbols):
            interval = self._monitor_interval_for(symbol, held)
            last_attempt = self._last_symbol_fetch.get(symbol)
            if (
                interval is MinuteInterval.FIVE_MINUTES
                and last_attempt is not None
                and decision_at - last_attempt < self._config.secondary_monitor_interval
            ):
                continue
            self._last_symbol_fetch[symbol] = decision_at
            due.append((symbol, interval))
        if not due:
            return

        async def fetch(
            symbol: str,
            interval: MinuteInterval,
        ) -> tuple[str, tuple[IntradayBar, ...] | None]:
            try:
                async with semaphore:
                    bars = tuple(
                        await self._market_data.fetch_intraday_bars_async(
                            symbol,
                            decision_at - self._config.minute_history_lookback,
                            decision_at,
                            interval=interval,
                            completed_only=True,
                        )
                    )
            except Exception:
                return symbol, None
            return symbol, bars

        fetched = await asyncio.gather(*(fetch(symbol, interval) for symbol, interval in due))
        # 行情数据传输期间，后台模型调用可能完成。在任何信号查询本地可交易缓存前，
        # 先把这些不可变观测写入日志。
        await self._drain_intraday_llm_reviews(phase=phase)
        failures = tuple(symbol for symbol, bars in fetched if bars is None)
        if failures:
            await self._source_transition(
                component="symbol-minute-bars",
                state="DEGRADED",
                phase=phase,
                error_code="PARTIAL_MINUTE_DATA_FAILURE",
                detail={"failed_count": len(failures)},
            )
        else:
            await self._source_transition(
                component="symbol-minute-bars",
                state="HEALTHY",
                phase=phase,
                error_code=None,
            )
        for symbol, bars in fetched:
            if not bars:
                continue
            await self._process_symbol_bars(
                symbol=symbol,
                bars=bars,
                phase=phase,
                decision_at=self._now(),
                held=symbol in held,
            )

    def _monitor_interval_for(self, symbol: str, held: set[str]) -> MinuteInterval:
        primary = {item.symbol for item in self._initial[: self._config.initial_core_size]}
        candidate = self._latest_candidates.get(symbol)
        if (
            symbol in held
            or symbol in self._pending
            or symbol in primary
            or (
                candidate is not None
                and candidate.candidate_class is IntradayCandidateClass.MOMENTUM_EXPANSION
            )
        ):
            return MinuteInterval.ONE_MINUTE
        return MinuteInterval.FIVE_MINUTES

    async def _process_symbol_bars(
        self,
        *,
        symbol: str,
        bars: tuple[IntradayBar, ...],
        phase: PaperDayPhase,
        decision_at: datetime,
        held: bool,
    ) -> None:
        if not bars:
            return
        bars = tuple(sorted(bars, key=lambda item: item.end_at))
        latest = bars[-1]
        self._last_prices[symbol] = latest.close
        technical_bars = tuple(
            TechnicalBar(
                end_time=item.end_at,
                available_at=item.meta.fetched_at,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume_lots,
                complete=item.is_closed,
            )
            for item in bars
        )

        pending = self._pending.get(symbol)
        if pending is not None:
            match_bar = next(
                (
                    item
                    for item in bars
                    if item.interval is MinuteInterval.ONE_MINUTE
                    and item.start_at > pending.signal_bar_end
                    and item.start_at >= pending.created_at
                    and item.end_at > pending.signal_bar_end
                ),
                None,
            )
            if match_bar is not None:
                await self._match_pending_order(
                    order=pending,
                    bar=match_bar,
                    planning_bars=technical_bars,
                    phase=phase,
                )

        prior = self._last_processed_bar.get(symbol)
        if prior is not None and latest.end_at <= prior:
            return
        self._last_processed_bar[symbol] = latest.end_at
        if held:
            await self._observe_exit_plan_bar(
                symbol=symbol,
                bar=technical_bars[-1],
                phase=phase,
                observed_at=decision_at,
            )
        try:
            signal = build_technical_signal(
                symbol,
                technical_bars,
                decision_time=decision_at,
                horizon=RecommendationHorizon.SHORT_1_TO_5_DAYS,
                is_currently_held=held,
                config=self._technical_config,
            )
        except ValueError:
            await self._publisher.emit(
                event_key=f"technical-invalid:{symbol}:{latest.end_at.isoformat()}",
                event_type="TECHNICAL_SIGNAL_INVALID",
                phase=phase,
                severity=PaperDaySeverity.WARNING,
                payload={"error_code": "INVALID_MINUTE_BAR_SEQUENCE"},
                symbol=symbol,
            )
            return
        await self._record_technical_evaluation(
            signal=signal,
            latest=latest,
            phase=phase,
        )
        previous_state = self._last_technical_state.get(symbol)
        self._last_technical_state[symbol] = signal.decision.value
        if signal.decision is RecommendationDecision.REDUCE and held:
            notify = previous_state != RecommendationDecision.REDUCE.value
            await self._record_sell_signal(
                signal=signal,
                latest=latest,
                phase=phase,
                notify=notify,
            )
        elif signal.decision is RecommendationDecision.ENTER_CANDIDATE:
            await self._handle_buy_signal(
                signal=signal,
                latest=latest,
                planning_bars=technical_bars,
                phase=phase,
            )

    async def _record_technical_evaluation(
        self,
        *,
        signal: TechnicalSignal,
        latest: IntradayBar,
        phase: PaperDayPhase,
    ) -> None:
        await self._publisher.emit(
            event_key=(
                f"technical:{signal.symbol}:{latest.interval.value}:{latest.end_at.isoformat()}"
            ),
            event_type="TECHNICAL_SIGNAL_EVALUATED",
            phase=phase,
            severity=PaperDaySeverity.DEBUG,
            payload={
                "bar_end": latest.end_at,
                "bar_provider": latest.meta.provider,
                "bar_degraded": latest.meta.degraded,
                "bar_freshness": latest.meta.freshness.value,
                "decision": signal.decision.value,
                "invalidation_price": signal.invalidation_price,
                "interval": latest.interval.value,
                "metrics": {key: value for key, value in signal.metrics},
                "reason_codes": signal.reason_codes,
                "reference_price": signal.reference_price,
                "score": signal.score,
                "strategy_version": signal.strategy_version,
            },
            symbol=signal.symbol,
            occurred_at=latest.end_at,
        )

    async def _record_sell_signal(
        self,
        *,
        signal: TechnicalSignal,
        latest: IntradayBar,
        phase: PaperDayPhase,
        notify: bool,
    ) -> None:
        llm_policy = (
            None
            if self._intraday_llm is None
            else {
                "action": IntradayLLMGateAction.NOT_APPLICABLE.value,
                "reason_code": IntradayLLMGateReason.REDUCE_NOT_APPLICABLE.value,
                "sell_never_blocked": True,
            }
        )
        # REDUCE 不再被误写成“完全没有 LLM 分析”：入场 LLM 门对此信号不适用，
        # 但成交后的 DEEP 保护计划已经持久保存 baseline/对抗评分，卖出复核读取
        # 最近有效版本即可，绝不能在分钟线临界路径重新等待网络。
        deep_exit_llm_assessment = self._deep_exit_llm_assessment(signal.symbol)
        deep_exit_sell_review = _deep_exit_sell_review(
            technical_score=signal.score,
            assessment=deep_exit_llm_assessment,
        )
        snapshot = self._paper.snapshot(self._manifest.account_id)
        position = snapshot.position(signal.symbol)
        available = 0 if position is None else position.available_to_sell
        today_buy = 0 if position is None else position.today_buy
        watch_entry = self._watchlist.get(signal.symbol)
        board = _board_from_symbol(signal.symbol) if watch_entry is None else watch_entry.board
        sell_quantity_plan: IntradaySellQuantityPlan | None
        try:
            sell_quantity_plan = build_intraday_sell_quantity_plan(
                available_to_sell=available,
                board=board,
            )
        except ValueError:
            sell_quantity_plan = None
        previous_close = self._session_previous_closes.get(signal.symbol)
        if previous_close is None:
            previous_close = _candidate_previous_close(self._latest_candidates.get(signal.symbol))
        price_acceptance: IntradayPriceAcceptance | None = None
        price_acceptance_status = "AVAILABLE"
        if signal.reference_price is None or previous_close is None:
            price_acceptance_status = "REFERENCE_OR_PREVIOUS_CLOSE_MISSING"
        else:
            try:
                price_acceptance = build_intraday_sell_price_acceptance(
                    reference_price=signal.reference_price,
                    previous_close=previous_close,
                    board=board,
                    instrument_type=(
                        PaperInstrumentType.STOCK if position is None else position.instrument_type
                    ),
                    config=self._risk_config,
                )
            except ValueError:
                price_acceptance_status = "PRICE_CORRIDOR_INVALID"
        event_key = f"sell-signal:{signal.symbol}:{latest.end_at.isoformat()}"
        await self._publisher.emit(
            event_key=f"sell-price-acceptance:{signal.symbol}:{latest.end_at.isoformat()}",
            event_type="SELL_PRICE_ACCEPTANCE_EVALUATED",
            phase=phase,
            severity=(
                PaperDaySeverity.NOTICE
                if price_acceptance is not None
                else PaperDaySeverity.WARNING
            ),
            payload={
                "bar_end": latest.end_at,
                "execution_mode": "RECORD_ONLY_NO_SELL_ORDER",
                "llm_gate": llm_policy,
                "deep_exit_llm_assessment": deep_exit_llm_assessment,
                "deep_exit_sell_review": deep_exit_sell_review,
                "current_execution_blockers": tuple(
                    item
                    for item in (
                        "NO_SELL_ORDER_BY_DAY_TEST_POLICY",
                        "T1_SELLABLE_QUANTITY_ZERO" if available <= 0 else None,
                        ("SELL_QUANTITY_RULE_UNAVAILABLE" if sell_quantity_plan is None else None),
                    )
                    if item is not None
                ),
                "future_non_execution_conditions": (
                    "NEXT_EXECUTION_PRICE_BELOW_SELL_LIMIT",
                    "NEXT_BAR_HIGH_BELOW_SELL_LIMIT",
                    "LOCKED_LIMIT_DOWN_QUEUE_UNMODELED",
                    "INSUFFICIENT_VERIFIED_VOLUME",
                    "SELL_QUANTITY_NOT_BOARD_VALID",
                    "PRICE_BAND_OR_REFERENCE_METADATA_INVALID",
                ),
                "price_acceptance": _price_acceptance_document(price_acceptance),
                "price_acceptance_status": price_acceptance_status,
                "previous_close": previous_close,
                "quantity_rule": _quantity_rule_document(
                    None if sell_quantity_plan is None else sell_quantity_plan.rule
                ),
                "reference_price": signal.reference_price,
                "sell_quantity_plan": _sell_quantity_plan_document(sell_quantity_plan),
            },
            symbol=signal.symbol,
            correlation_id=event_key,
            occurred_at=latest.end_at,
        )
        acceptance_text = (
            (
                f"未来可执行卖出价：{price_acceptance.acceptable_lower:.3f}–"
                f"{price_acceptance.acceptable_upper:.3f}；"
                f"最低接受价/卖出限价：{price_acceptance.limit_price:.3f}；"
                f"保守日价格带：{price_acceptance.exchange_lower:.3f}–"
                f"{price_acceptance.exchange_upper:.3f}。\n"
                "若下一执行时点跳空或持续低于最低接受价，则不得假设成交；"
                "分钟K不含实时盘口价格笼子，未来接实盘前必须重新校验。"
            )
            if price_acceptance is not None
            else (
                "卖出价格区间暂不可验证：缺少有效参考价、昨收或价格带元数据；"
                f"状态码：{price_acceptance_status}。"
            )
        )
        text = (
            "【A股模拟盘｜卖出信号已记录】\n"
            f"标的：{signal.symbol}\n"
            f"完成分钟线：{latest.end_at.astimezone(SHANGHAI):%H:%M}\n"
            f"可卖数量：{available}；今日买入：{today_buy}\n"
            f"{_sell_quantity_plan_text(sell_quantity_plan)}\n"
            f"{acceptance_text}\n"
            f"{_deep_exit_llm_assessment_text(deep_exit_llm_assessment)}\n"
            f"{_deep_exit_sell_review_text(deep_exit_sell_review)}\n"
            "本次全天测试按约定不提交卖单；T+1 状态与信号均已写入不可变日志。"
        )
        await self._publisher.emit(
            event_key=event_key,
            event_type="SELL_SIGNAL_TRIGGERED",
            phase=phase,
            severity=PaperDaySeverity.WARNING,
            payload={
                "available_to_sell": available,
                "bar_end": latest.end_at,
                "decision": signal.decision.value,
                "execution_action": "NO_SELL_ORDER",
                "llm_gate": llm_policy,
                "deep_exit_llm_assessment": deep_exit_llm_assessment,
                "deep_exit_sell_review": deep_exit_sell_review,
                "reason_codes": signal.reason_codes,
                "score": signal.score,
                "today_buy": today_buy,
                "price_acceptance": _price_acceptance_document(price_acceptance),
                "price_acceptance_status": price_acceptance_status,
                "previous_close": previous_close,
                "sell_quantity_plan": _sell_quantity_plan_document(sell_quantity_plan),
            },
            notification_text=text if notify else None,
            symbol=signal.symbol,
            correlation_id=event_key,
            occurred_at=latest.end_at,
        )
        await self._publisher.emit(
            event_key=f"sell-not-submitted:{signal.symbol}:{latest.end_at.isoformat()}",
            event_type="SELL_NOT_SUBMITTED_T1",
            phase=phase,
            severity=PaperDaySeverity.NOTICE,
            payload={
                "available_to_sell": available,
                "policy": "RECORD_ONLY_NO_SELL_ORDER",
                "sell_quantity_plan": _sell_quantity_plan_document(sell_quantity_plan),
                "today_buy": today_buy,
            },
            symbol=signal.symbol,
            correlation_id=event_key,
            occurred_at=latest.end_at,
        )

    def _deep_exit_llm_assessment(self, symbol: str) -> dict[str, object]:
        """读取最近有效 DEEP 语义评分；读取失败只降级展示，不阻塞保护信号。"""

        lifecycle = self._exit_plan_lifecycle
        protection_id = self._exit_protection_by_symbol.get(symbol)
        if lifecycle is None or protection_id is None:
            return {
                "available": False,
                "status": "EXIT_PLAN_NOT_AVAILABLE",
            }
        try:
            plan = lifecycle.active_plan(protection_id)
        except ExitPlanLifecycleError:
            return {
                "available": False,
                "protection_id": protection_id,
                "status": "EXIT_PLAN_REPLAY_FAILED",
            }
        return self._deep_exit_llm_assessment_document(plan)

    @staticmethod
    def _deep_exit_llm_assessment_document(plan: ExitPlan) -> dict[str, object]:
        """把一个确定的计划版本投影为双轨语义快照。

        屏障触发与 DEEP 后台替换可能非常接近，因此调用方应传入触发观察返回的
        ``active_plan``，而不是在通知前再次查询当前版本。QUICK 计划不会伪造语义
        评分；历史 DEEP 计划若没有基线/对抗指标，也会明确降级为不可用。
        """

        metrics = dict(plan.metrics)
        baseline_score = metrics.get("baseline_semantic_score")
        adversarial_score = metrics.get("adversarial_semantic_score")
        selected_score = metrics.get("selected_semantic_score")
        has_complete_dual_metrics = (
            isinstance(baseline_score, Decimal)
            and isinstance(adversarial_score, Decimal)
            and isinstance(selected_score, Decimal)
            and selected_score == adversarial_score
            and "ADVERSARIAL_RESULT_PREFERRED" in plan.reason_codes
        )
        available = plan.depth is ExitPlanDepth.DEEP and has_complete_dual_metrics
        selected_system = "ADVERSARIAL_LLM" if available else "UNAVAILABLE"
        return {
            "adversarial_score": adversarial_score,
            "adversarial_status": (
                "AVAILABLE"
                if isinstance(adversarial_score, Decimal)
                else "UNAVAILABLE"
            ),
            "available": available,
            "baseline_score": baseline_score,
            "baseline_status": (
                "AVAILABLE" if isinstance(baseline_score, Decimal) else "UNAVAILABLE"
            ),
            "evidence_ids": plan.evidence_ids,
            "plan_id": plan.plan_id,
            "plan_state": plan.state.value,
            "protection_id": plan.protection_id,
            "selected_score": selected_score if available else None,
            "selected_system": selected_system if available else "UNAVAILABLE",
            "status": (
                "DEEP_ASSESSMENT_AVAILABLE"
                if available
                else (
                    "QUICK_PLAN_RETAINED"
                    if plan.depth is ExitPlanDepth.QUICK
                    else "DEEP_LLM_METRICS_NOT_AVAILABLE"
                )
            ),
        }
    async def _handle_buy_signal(
        self,
        *,
        signal: TechnicalSignal,
        latest: IntradayBar,
        planning_bars: tuple[TechnicalBar, ...] = (),
        phase: PaperDayPhase,
    ) -> None:
        candidate = self._latest_candidates.get(signal.symbol)
        try:
            quantity_rule = intraday_order_quantity_rule(self._watchlist[signal.symbol].board)
        except ValueError:
            quantity_rule = None
        gate_reason = self._entry_gate_reason(
            symbol=signal.symbol,
            latest=latest,
            candidate=candidate,
        )
        previous_close = _candidate_previous_close(candidate)
        if previous_close is not None:
            self._session_previous_closes[signal.symbol] = previous_close
        risk: IntradayPaperRiskOutcome | None = None
        llm_gate: IntradayLLMGateOutcome | None = None
        quick_plan: ExitPlan | None = None
        protection_id: str | None = None
        if gate_reason is None:
            if latest.meta.degraded:
                if candidate is None:  # pragma: no cover - gate invariant
                    raise RuntimeError("entry gate accepted without a candidate")
                await self._record_degraded_minute_corroboration(
                    candidate=candidate,
                    latest=latest,
                    phase=phase,
                )
            if self._intraday_llm is None:
                gate_reason = "LLM_OPERATOR_DISABLED_NEW_BUY_BLOCKED"
                await self._publisher.emit(
                    event_key=(
                        f"buy-llm-gate-disabled:{signal.symbol}:"
                        f"{latest.end_at.isoformat()}"
                    ),
                    event_type="BUY_LLM_GATE_EVALUATED",
                    phase=phase,
                    severity=PaperDaySeverity.WARNING,
                    payload={
                        "approved": False,
                        "blocks_entry": True,
                        "reason": gate_reason,
                        "required_for_buy": True,
                        "status": "OPERATOR_DISABLED_MONITOR_ONLY",
                    },
                    symbol=signal.symbol,
                    correlation_id=(
                        f"llm-gate-disabled:{signal.symbol}:"
                        f"{latest.end_at.isoformat()}"
                    ),
                    occurred_at=latest.end_at,
                )
            else:
                expected_context_id = self._latest_llm_context_by_symbol.get(signal.symbol)
                llm_gate = self._intraday_llm.evaluate_buy(
                    signal,
                    expected_context_id=expected_context_id,
                    decision_at=self._now(),
                )
                required = self._config.intraday_llm_required_for_buy
                negative_decision = llm_gate.reason in {
                    IntradayLLMGateReason.NEGATIVE_VETO,
                    IntradayLLMGateReason.COMBINED_SCORE_BELOW_ENTRY_THRESHOLD,
                }
                llm_blocks_entry = not llm_gate.approved and (required or negative_decision)
                await self._publisher.emit(
                    event_key=(f"buy-llm-gate:{signal.symbol}:{latest.end_at.isoformat()}"),
                    event_type="BUY_LLM_GATE_EVALUATED",
                    phase=phase,
                    severity=(
                        PaperDaySeverity.INFO if not llm_blocks_entry else PaperDaySeverity.WARNING
                    ),
                    payload={
                        **_llm_gate_document(llm_gate),
                        "blocks_entry": llm_blocks_entry,
                        "expected_context_id": expected_context_id,
                        "required_for_buy": required,
                    },
                    symbol=signal.symbol,
                    correlation_id=(
                        llm_gate.review_id
                        or f"llm-gate:{signal.symbol}:{latest.end_at.isoformat()}"
                    ),
                    occurred_at=latest.end_at,
                )
                if llm_blocks_entry:
                    gate_reason = llm_gate.reason.value
        if gate_reason is None:
            snapshot = self._paper.snapshot(self._manifest.account_id)
            risk = build_intraday_buy_order(
                signal,
                snapshot,
                board=self._watchlist[signal.symbol].board,
                signal_bar_end=latest.end_at,
                previous_close=previous_close,
                created_at=self._now(),
                position_marks=self._last_prices,
                config=self._risk_config,
                fee_schedule=self._paper.fee_schedule,
            )
            if risk.status is not IntradayPaperRiskStatus.APPROVED:
                gate_reason = risk.reason.value
            elif self._exit_plan_lifecycle is not None:
                assert risk.order is not None
                assert signal.invalidation_price is not None
                protection_id = _exit_protection_id(
                    account_id=self._manifest.account_id,
                    symbol=signal.symbol,
                    signal_bar_end=latest.end_at,
                    strategy_version=signal.strategy_version,
                )
                try:
                    creation = self._exit_plan_lifecycle.create_quick_plan(
                        account_id=self._manifest.account_id,
                        protection_id=protection_id,
                        symbol=signal.symbol,
                        bars=planning_bars,
                        decision_at=self._now(),
                        time_exit_at=_calendar_time_exit(
                            self._manifest.session_date,
                            trading_sessions=(
                                self._config.exit_plan_trading_sessions
                            ),
                            holding_sessions=self._config.exit_plan_holding_weekdays,
                            market_close=self._config.market_close,
                        ),
                        worst_entry_price=risk.order.limit_price,
                        technical_invalidation_price=signal.invalidation_price,
                        strategy_version=signal.strategy_version,
                        config=self._quick_exit_config,
                        known_at=self._now(),
                    )
                except (
                    ArithmeticError,
                    ExitPlanLifecycleConflictError,
                    TypeError,
                    ValueError,
                ):
                    gate_reason = "QUICK_EXIT_PLAN_UNAVAILABLE"
                    await self._publisher.emit(
                        event_key=f"exit-plan-quick-failed:{protection_id}",
                        event_type="EXIT_PLAN_QUICK_FAILED",
                        phase=phase,
                        severity=PaperDaySeverity.WARNING,
                        payload={
                            "error_code": "QUICK_EXIT_PLAN_UNAVAILABLE",
                            "protection_id": protection_id,
                            "order_created": False,
                        },
                        symbol=signal.symbol,
                        correlation_id=protection_id,
                        occurred_at=latest.end_at,
                    )
                else:
                    quick_plan = creation.result.plan
                    self._exit_protection_by_symbol[signal.symbol] = protection_id
                    await self._publisher.emit(
                        event_key=f"exit-plan-quick-created:{quick_plan.plan_id}",
                        event_type="EXIT_PLAN_QUICK_CREATED",
                        phase=phase,
                        severity=PaperDaySeverity.NOTICE,
                        payload={
                            "created_new": creation.created,
                            "plan": exit_plan_document(quick_plan),
                            "protection_id": protection_id,
                            "pretrade_required": True,
                            "selection_policy": "LOOSEST_VALID_STOP",
                        },
                        symbol=signal.symbol,
                        correlation_id=protection_id,
                        occurred_at=latest.end_at,
                    )
                    crossed = _exit_barriers_for_bar(quick_plan, planning_bars[-1])
                    if crossed:
                        gate_reason = "QUICK_EXIT_CONDITION_ALREADY_MET"
                        await self._publisher.emit(
                            event_key=f"exit-plan-pretrade-rejected:{quick_plan.plan_id}",
                            event_type="EXIT_PLAN_PRETRADE_REJECTED",
                            phase=phase,
                            severity=PaperDaySeverity.WARNING,
                            payload={
                                "crossed_barriers": tuple(item.value for item in crossed),
                                "order_created": False,
                                "plan_id": quick_plan.plan_id,
                                "protection_id": protection_id,
                                "reason_code": "QUICK_EXIT_CONDITION_ALREADY_MET",
                            },
                            symbol=signal.symbol,
                            correlation_id=protection_id,
                            occurred_at=latest.end_at,
                        )
                    else:
                        # 第一次定仓仅用于确定最坏买入限价；QUICK 形成后必须用
                        # 真实保护止损重新定仓，较松止损只会减少而不会放大数量。
                        risk = build_intraday_buy_order(
                            signal,
                            snapshot,
                            board=self._watchlist[signal.symbol].board,
                            signal_bar_end=latest.end_at,
                            previous_close=previous_close,
                            created_at=self._now(),
                            position_marks=self._last_prices,
                            config=self._risk_config,
                            fee_schedule=self._paper.fee_schedule,
                            protective_stop_price=quick_plan.stop_price,
                        )
                        if risk.status is not IntradayPaperRiskStatus.APPROVED:
                            gate_reason = risk.reason.value
        signal_key = f"buy-signal:{signal.symbol}:{latest.end_at.isoformat()}"
        approved = (
            gate_reason is None
            and risk is not None
            and risk.status is IntradayPaperRiskStatus.APPROVED
        )
        price_acceptance = (
            intraday_buy_order_price_acceptance(risk.order, config=self._risk_config)
            if approved and risk is not None and risk.order is not None
            else None
        )
        if approved:
            assert price_acceptance is not None
            assert quantity_rule is not None
            entry_result_text = (
                "风险门通过：已创建只在信号可知后才开始的首个完整"
                "1分钟区间有效的 PAPER 限价单。\n"
                f"可接受买入价：{price_acceptance.acceptable_lower:.3f}–"
                f"{price_acceptance.acceptable_upper:.3f}；"
                f"保守日价格带：{price_acceptance.exchange_lower:.3f}–"
                f"{price_acceptance.exchange_upper:.3f}。\n"
                "若撮合分钟触及或跌破失效位，整笔 IOC 失效；不会把破位低价视作便宜买入。"
                "本次仅为分钟K PAPER，未验证实时盘口价格笼子。"
                f"数量规则：最少{quantity_rule.minimum_buy_quantity}股，"
                f"其后按{quantity_rule.buy_increment}股递增。"
            )
        else:
            entry_result_text = (
                f"未创建订单：{_entry_rejection_display(gate_reason)}（原因码：{gate_reason}）。"
            )
        await self._publisher.emit(
            event_key=signal_key,
            event_type="BUY_SIGNAL_TRIGGERED",
            phase=phase,
            severity=(PaperDaySeverity.NOTICE if approved else PaperDaySeverity.WARNING),
            payload={
                "anomaly_class": (None if candidate is None else candidate.candidate_class.value),
                "anomaly_score": None if candidate is None else candidate.anomaly_score,
                "bar_end": latest.end_at,
                "gate_reason": gate_reason,
                "invalidation_price": signal.invalidation_price,
                "reference_price": signal.reference_price,
                "risk_approved": approved,
                "score": signal.score,
                "llm_gate": (None if llm_gate is None else _llm_gate_document(llm_gate)),
                "price_acceptance": _price_acceptance_document(price_acceptance),
                "quantity_rule": _quantity_rule_document(quantity_rule),
                "exit_plan": (None if quick_plan is None else exit_plan_document(quick_plan)),
                "exit_protection_id": protection_id,
            },
            notification_text=(
                (
                    "【A股模拟盘｜可执行买入信号】\n"
                    if approved
                    else "【A股模拟盘｜技术候选未通过入场门】\n"
                )
                + f"标的：{signal.symbol}\n"
                f"信号价：{_decimal_display(signal.reference_price)}；"
                f"失效参考：{_decimal_display(signal.invalidation_price)}\n"
                + (
                    ""
                    if llm_gate is None
                    else (
                        f"LLM复核：{llm_gate.reason.value}；"
                        f"综合分：{_decimal_display(llm_gate.combined_score)}\n"
                        f"{_llm_gate_dual_text(llm_gate)}\n"
                    )
                )
                + entry_result_text
            ),
            symbol=signal.symbol,
            correlation_id=signal_key,
            occurred_at=latest.end_at,
        )
        if not approved or risk is None or risk.order is None:
            return
        assert price_acceptance is not None
        assert quantity_rule is not None
        order = replace(
            risk.order,
            # 仅在信号写入日志并通过 QQ 投递后的下一个自然分钟边界激活。
            # 即使通知 I/O 缓慢，任何已开始区间也不能成为执行证据。
            created_at=_next_utc_minute(self._now()),
        )
        self._pending[order.symbol] = order
        await self._publisher.emit(
            event_key=f"order-submitted:{order.order_id}",
            event_type="ORDER_SUBMITTED",
            phase=phase,
            severity=PaperDaySeverity.NOTICE,
            payload={
                "order": _order_document(order),
                "price_acceptance": _price_acceptance_document(price_acceptance),
                "quantity_rule": _quantity_rule_document(quantity_rule),
                "risk_budget": risk.risk_budget,
                "stop_distance": risk.stop_distance,
                "available_cash_budget": risk.available_cash_budget,
                "exit_plan": (None if quick_plan is None else exit_plan_document(quick_plan)),
                "exit_protection_id": protection_id,
                "risk_price_basis": "WORST_BUY_LIMIT_MINUS_QUICK_STOP",
            },
            notification_text=(
                "【A股模拟盘｜PAPER 委托已创建】\n"
                f"标的：{order.symbol}；数量：{order.quantity} 股；"
                f"限价：{order.limit_price:.3f}\n"
                f"数量规则：最少{quantity_rule.minimum_buy_quantity}股，"
                f"其后按{quantity_rule.buy_increment}股递增；"
                f"本单{order.quantity}股已通过校验\n"
                f"可接受买入价：{price_acceptance.acceptable_lower:.3f}–"
                f"{price_acceptance.acceptable_upper:.3f}；"
                f"保守日价格带：{price_acceptance.exchange_lower:.3f}–"
                f"{price_acceptance.exchange_upper:.3f}\n"
                "成交条件：仅信号可知后才开始的首个完整1分钟区间、最多参与该分钟成交量1%、不穿限价；"
                "撮合分钟触及失效位则不成交。分钟K不含实时盘口价格笼子，不可直接转为实盘委托。"
            ),
            symbol=order.symbol,
            correlation_id=order.order_id,
        )

    async def _record_degraded_minute_corroboration(
        self,
        *,
        candidate: IntradayCandidate,
        latest: IntradayBar,
        phase: PaperDayPhase,
    ) -> None:
        """在创建风险评估或委托前，持久化独立来源证据。"""

        policy = _entry_policy_document(self._config)
        divergence = abs(latest.close / candidate.last_price - Decimal("1"))
        await self._publisher.emit(
            event_key=(
                f"degraded-minute-corroborated:{candidate.symbol}:{latest.end_at.isoformat()}"
            ),
            event_type="DEGRADED_MINUTE_CORROBORATION_ACCEPTED",
            phase=phase,
            severity=PaperDaySeverity.NOTICE,
            payload={
                "candidate_class": candidate.candidate_class.value,
                "candidate_factor_weight_coverage": (candidate.factor_weight_coverage),
                "candidate_last_price": candidate.last_price,
                "entry_policy_sha256": _document_sha256(policy),
                "manifest_config_sha256": self._manifest.config_sha256,
                "minute_bar_close": latest.close,
                "minute_bar_end": latest.end_at,
                "minute_bar_provider": latest.meta.provider,
                "price_divergence": divergence,
                "price_tolerance": self._config.fallback_price_tolerance,
                "scan_at": self._latest_scan_at,
                "scan_source": self._latest_scan_source,
                "scan_status": self._latest_scan_status,
            },
            symbol=candidate.symbol,
            occurred_at=latest.end_at,
        )

    def _entry_gate_reason(
        self,
        *,
        symbol: str,
        latest: IntradayBar,
        candidate: IntradayCandidate | None,
    ) -> str | None:
        if symbol in self._entered_symbols:
            return "SYMBOL_ALREADY_ENTERED_TODAY"
        if symbol in self._pending:
            return "SYMBOL_ORDER_ALREADY_PENDING"
        if self._pending:
            return "PENDING_CAPACITY_RESERVED"
        snapshot = self._paper.snapshot(self._manifest.account_id)
        position = snapshot.position(symbol)
        if position is not None and position.quantity > 0:
            return "SYMBOL_ALREADY_HELD"
        if candidate is None:
            return "NO_CURRENT_SESSION_ANOMALY"
        if candidate.candidate_class is not IntradayCandidateClass.MOMENTUM_EXPANSION:
            return "ANOMALY_NOT_MOMENTUM_EXPANSION"
        if self._latest_scan_at is None:
            return "NO_CURRENT_SESSION_SCAN"
        if self._latest_scan_status != "COMPLETE":
            return "CURRENT_SESSION_SCAN_NOT_COMPLETE"
        if self._now() - self._latest_scan_at > self._config.anomaly_ttl:
            return "CURRENT_SESSION_ANOMALY_EXPIRED"
        if latest.meta.freshness is FreshnessStatus.STALE:
            return "MINUTE_DATA_STALE"
        if latest.meta.degraded:
            if "Sina" not in latest.meta.provider:
                return "UNAPPROVED_DEGRADED_MINUTE_PROVIDER"
            if self._latest_scan_source != _TENCENT_ENRICHED_SURVEILLANCE_SOURCE_ID:
                return "DEGRADED_SOURCE_NOT_INDEPENDENTLY_CORROBORATED"
            if candidate.last_price <= 0:
                return "CORROBORATION_PRICE_MISSING"
            divergence = abs(latest.close / candidate.last_price - Decimal("1"))
            if divergence > self._config.fallback_price_tolerance:
                return "CROSS_SOURCE_PRICE_DIVERGENCE"
        elif latest.meta.freshness is not FreshnessStatus.CURRENT:
            return "MINUTE_FRESHNESS_NOT_CURRENT"
        return None

    async def _match_pending_order(
        self,
        *,
        order: IntradayPaperOrder,
        bar: IntradayBar,
        planning_bars: tuple[TechnicalBar, ...],
        phase: PaperDayPhase,
    ) -> None:
        revision = _bar_revision(bar)
        # DEEP 仅需覆盖 21 根 15 分钟线；保留 450 根一分钟线给午休缺口和
        # 边界残段留余量，避免把五天全量分钟线复制进每笔成交事件。
        planning_bars = planning_bars[-450:]
        protection_id = self._exit_protection_by_symbol.get(order.symbol)
        active_exit_plan: ExitPlan | None = None
        if protection_id is not None and self._exit_plan_lifecycle is not None:
            with suppress(Exception):
                active_exit_plan = self._exit_plan_lifecycle.active_plan(protection_id)
        price_acceptance = intraday_buy_order_price_acceptance(
            order,
            config=self._risk_config,
        )
        quantity_rule = intraday_order_quantity_rule(order.board)
        if self._exit_plan_lifecycle is not None and active_exit_plan is None:
            # 升级前遗留的待撮合委托没有可验证 QUICK 时必须终止，不能让恢复
            # 路径因为缺少保护映射而绕开新的成交前风险边界。
            outcome = IntradayPaperMatchOutcome(
                status=IntradayPaperMatchStatus.NOT_FILLED_IOC,
                reason=IntradayPaperMatchReason.EXIT_PLAN_MISSING_BEFORE_FILL,
                order_id=order.order_id,
                requested_quantity=order.quantity,
                filled_quantity=0,
                cancelled_quantity=order.quantity,
                volume_capacity=0,
            )
        else:
            outcome = match_intraday_buy_order(
                order,
                bar,
                match_revision=revision,
                config=self._risk_config,
                protective_stop_price=(
                    None if active_exit_plan is None else active_exit_plan.stop_price
                ),
                take_profit_price=(
                    None if active_exit_plan is None else active_exit_plan.take_profit_price
                ),
                time_exit_at=(None if active_exit_plan is None else active_exit_plan.time_exit_at),
            )
        await self._publisher.emit(
            event_key=f"order-match:{order.order_id}:{revision[:24]}",
            event_type="ORDER_MATCH_EVALUATED",
            phase=phase,
            severity=(
                PaperDaySeverity.NOTICE if outcome.filled_quantity > 0 else PaperDaySeverity.WARNING
            ),
            payload={
                "bar": _bar_document(bar),
                "cancelled_quantity": outcome.cancelled_quantity,
                "fill": (None if outcome.fill is None else _fill_document(outcome.fill)),
                "fill_price": outcome.fill_price,
                "filled_quantity": outcome.filled_quantity,
                "match_revision": revision,
                "order_id": order.order_id,
                "price_acceptance": _price_acceptance_document(price_acceptance),
                "quantity_rule": _quantity_rule_document(quantity_rule),
                "reason": outcome.reason.value,
                "requested_quantity": outcome.requested_quantity,
                "status": outcome.status.value,
                "volume_capacity": outcome.volume_capacity,
                "exit_plan_id": (None if active_exit_plan is None else active_exit_plan.plan_id),
                "exit_protection_id": protection_id,
                "exit_plan_market_bars": (
                    []
                    if active_exit_plan is None
                    else [_technical_bar_document(item) for item in planning_bars]
                ),
            },
            symbol=order.symbol,
            correlation_id=order.order_id,
            occurred_at=bar.end_at,
        )
        self._pending.pop(order.symbol, None)
        if outcome.fill is None:
            await self._publisher.emit(
                event_key=f"order-terminal:{order.order_id}:{revision[:24]}",
                event_type="ORDER_EXPIRED_UNFILLED",
                phase=phase,
                severity=PaperDaySeverity.WARNING,
                payload={
                    "order_id": order.order_id,
                    "reason": outcome.reason.value,
                    "status": outcome.status.value,
                },
                notification_text=(
                    "【A股模拟盘｜PAPER 委托未成交】\n"
                    f"标的：{order.symbol}；原因：{_match_reason_display(outcome.reason.value)}"
                    f"（原因码：{outcome.reason.value}）\n"
                    f"有效买入价：{price_acceptance.acceptable_lower:.3f}–"
                    f"{price_acceptance.acceptable_upper:.3f}\n"
                    "单分钟 IOC 已终止，不会跨分钟偷偷补成交。"
                ),
                symbol=order.symbol,
                correlation_id=order.order_id,
            )
            return
        await self._apply_fill_saga(
            fill=outcome.fill,
            order=order,
            outcome=outcome,
            planning_bars=planning_bars,
            protection_id=protection_id,
            phase=phase,
        )

    async def _apply_fill_saga(
        self,
        *,
        fill: ASharePaperFill,
        order: IntradayPaperOrder,
        outcome: IntradayPaperMatchOutcome,
        planning_bars: tuple[TechnicalBar, ...] = (),
        protection_id: str | None = None,
        phase: PaperDayPhase,
    ) -> None:
        await self._publisher.emit(
            event_key=f"fill-started:{fill.fill_id}",
            event_type="FILL_STARTED",
            phase=phase,
            severity=PaperDaySeverity.NOTICE,
            payload={
                "fill": _fill_document(fill),
                "order_id": order.order_id,
                "status": outcome.status.value,
                "exit_protection_id": protection_id,
                "exit_plan_market_bars": [_technical_bar_document(item) for item in planning_bars],
            },
            symbol=fill.symbol,
            correlation_id=fill.fill_id,
            occurred_at=fill.executed_at,
        )
        receipt = self._paper.record_fill(fill, recorded_at=self._now())
        self._entered_symbols.add(fill.symbol)
        await self._publisher.emit(
            event_key=f"fill-applied:{fill.fill_id}",
            event_type="FILL_APPLIED",
            phase=phase,
            severity=PaperDaySeverity.NOTICE,
            payload={
                "applied_new": receipt.applied_new,
                "cash": receipt.snapshot.cash,
                "cash_change": receipt.applied_fill.cash_change,
                "commission": receipt.applied_fill.fees.commission,
                "fill_id": fill.fill_id,
                "price": fill.price,
                "quantity": fill.quantity,
                "stamp_tax": receipt.applied_fill.fees.stamp_tax,
                "transfer_fee": receipt.applied_fill.fees.transfer_fee,
                "exit_protection_id": protection_id,
            },
            notification_text=(
                "【A股模拟盘｜PAPER 买入成交】\n"
                f"标的：{fill.symbol}\n"
                f"成交：{fill.quantity} 股 @ {fill.price:.3f}\n"
                f"佣金：{receipt.applied_fill.fees.commission:.2f}；"
                f"过户费：{receipt.applied_fill.fees.transfer_fee:.2f}\n"
                f"剩余现金：{receipt.snapshot.cash:.2f} 元；今日买入股份 T+1 不可卖。"
            ),
            symbol=fill.symbol,
            correlation_id=fill.fill_id,
            occurred_at=fill.executed_at,
        )
        await self._ensure_exit_plan_post_fill(
            fill=fill,
            protection_id=protection_id,
            planning_bars=planning_bars,
            phase=phase,
        )

    async def _recover_incomplete_fills(self) -> int:
        events = self._store.events(self._manifest.run_id)
        applied = {item.correlation_id for item in events if item.event_type == "FILL_APPLIED"}
        started = {item.correlation_id for item in events if item.event_type == "FILL_STARTED"}
        # 在终态撮合评估之后、FILL_STARTED 之前崩溃时，不得重新获取或重新撮合 IOC。
        # 撮合事件中保留的不可变成交命令会确定性推进现有账本 saga。
        for event in events:
            if event.event_type != "ORDER_MATCH_EVALUATED":
                continue
            fill_document = event.payload.get("fill")
            if not isinstance(fill_document, dict):
                continue
            fill = _fill_from_document(fill_document)
            if fill.fill_id in applied or fill.fill_id in started:
                continue
            await self._publisher.emit(
                event_key=f"fill-started:{fill.fill_id}",
                event_type="FILL_STARTED",
                phase=event.phase,
                severity=PaperDaySeverity.NOTICE,
                payload={
                    "fill": fill_document,
                    "order_id": event.payload.get("order_id"),
                    "recovered_from_terminal_match": True,
                    "status": event.payload.get("status"),
                    "exit_protection_id": event.payload.get("exit_protection_id"),
                    "exit_plan_market_bars": event.payload.get(
                        "exit_plan_market_bars",
                        [],
                    ),
                },
                symbol=fill.symbol,
                correlation_id=fill.fill_id,
                occurred_at=fill.executed_at,
            )
            started.add(fill.fill_id)
        events = self._store.events(self._manifest.run_id)
        recovered = 0
        for event in events:
            if event.event_type != "FILL_STARTED" or event.correlation_id in applied:
                continue
            fill_document = event.payload.get("fill")
            if not isinstance(fill_document, dict):
                raise RuntimeError("FILL_STARTED has no immutable fill document")
            fill = _fill_from_document(fill_document)
            receipt = self._paper.record_fill(fill, recorded_at=self._now())
            self._entered_symbols.add(fill.symbol)
            await self._publisher.emit(
                event_key=f"fill-applied:{fill.fill_id}",
                event_type="FILL_APPLIED",
                phase=event.phase,
                severity=PaperDaySeverity.NOTICE,
                payload={
                    "applied_new": receipt.applied_new,
                    "cash": receipt.snapshot.cash,
                    "cash_change": receipt.applied_fill.cash_change,
                    "commission": receipt.applied_fill.fees.commission,
                    "fill_id": fill.fill_id,
                    "price": fill.price,
                    "quantity": fill.quantity,
                    "recovered": True,
                    "stamp_tax": receipt.applied_fill.fees.stamp_tax,
                    "transfer_fee": receipt.applied_fill.fees.transfer_fee,
                    "exit_protection_id": event.payload.get("exit_protection_id"),
                },
                notification_text=(
                    "【A股模拟盘｜崩溃恢复完成】\n"
                    f"成交 {fill.fill_id[-10:]} 已与资金账本幂等对齐，未重复扣款。"
                ),
                symbol=fill.symbol,
                correlation_id=fill.fill_id,
                occurred_at=fill.executed_at,
            )
            protection_id = event.payload.get("exit_protection_id")
            raw_bars = event.payload.get("exit_plan_market_bars")
            planning_bars = _technical_bars_from_document(raw_bars)
            await self._ensure_exit_plan_post_fill(
                fill=fill,
                protection_id=(protection_id if isinstance(protection_id, str) else None),
                planning_bars=planning_bars,
                phase=event.phase,
            )
            recovered += 1
        return recovered

    async def _recover_exit_plan_followups(self) -> int:
        """补齐已入账成交与退出计划之间可能被崩溃截断的边界。"""

        if self._exit_plan_lifecycle is None:
            return 0
        events = self._store.events(self._manifest.run_id)
        started = {
            item.correlation_id: item
            for item in events
            if item.event_type == "FILL_STARTED" and item.correlation_id is not None
        }
        recovered = 0
        for event in events:
            if event.event_type != "FILL_APPLIED" or event.correlation_id is None:
                continue
            source = started.get(event.correlation_id)
            if source is None:
                continue
            fill_document = source.payload.get("fill")
            if not isinstance(fill_document, dict):
                continue
            protection = source.payload.get("exit_protection_id")
            if not isinstance(protection, str):
                continue
            history = self._exit_plan_lifecycle.history(protection)
            attached = any(
                item.event_type is ExitPlanEventType.PLAN_ATTACHED_TO_FILL
                and item.payload.get("fill_id") == event.correlation_id
                for item in history
            )
            has_deep = any(item.event_type is ExitPlanEventType.PLAN_REPLACED for item in history)
            if attached and has_deep:
                self._exit_protection_by_symbol[event.symbol or ""] = protection
                continue
            await self._ensure_exit_plan_post_fill(
                fill=_fill_from_document(fill_document),
                protection_id=protection,
                planning_bars=_technical_bars_from_document(
                    source.payload.get("exit_plan_market_bars")
                ),
                phase=event.phase,
            )
            recovered += 1
        return recovered

    def _restore_existing_exit_protections(
        self,
        account: PaperAccountSnapshot,
    ) -> None:
        """把跨日 PAPER 账本中的现有持仓重新绑定到持久退出流。"""

        lifecycle = self._exit_plan_lifecycle
        if lifecycle is None:
            return
        for position in account.positions:
            if position.quantity <= 0 or position.symbol in self._exit_protection_by_symbol:
                continue
            protection_id = lifecycle.latest_attached_protection(
                account.account_id,
                position.symbol,
            )
            if protection_id is not None:
                self._exit_protection_by_symbol[position.symbol] = protection_id

    async def _ensure_exit_plan_post_fill(
        self,
        *,
        fill: ASharePaperFill,
        protection_id: str | None,
        planning_bars: tuple[TechnicalBar, ...],
        phase: PaperDayPhase,
    ) -> None:
        """成交后幂等附着 QUICK、请求并生成 DEEP；失败时保留 QUICK。"""

        lifecycle = self._exit_plan_lifecycle
        if lifecycle is None:
            return
        if protection_id is None:
            await self._publisher.emit(
                event_key=f"exit-plan-followup-missing:{fill.fill_id}",
                event_type="EXIT_PLAN_POST_FILL_FAILED",
                phase=phase,
                severity=PaperDaySeverity.CRITICAL,
                payload={
                    "error_code": "EXIT_PROTECTION_ID_MISSING",
                    "fill_id": fill.fill_id,
                    "position_monitoring_active": False,
                },
                symbol=fill.symbol,
                correlation_id=fill.fill_id,
                occurred_at=fill.executed_at,
            )
            return
        self._exit_protection_by_symbol[fill.symbol] = protection_id
        now = self._now()
        history = lifecycle.history(protection_id)
        attachment = next(
            (
                item
                for item in history
                if item.event_type is ExitPlanEventType.PLAN_ATTACHED_TO_FILL
                and item.payload.get("fill_id") == fill.fill_id
            ),
            None,
        )
        if attachment is None:
            attached = lifecycle.attach_fill_and_request_deep(
                protection_id,
                fill_id=fill.fill_id,
                filled_quantity=fill.quantity,
                fill_price=fill.price,
                filled_at=fill.executed_at,
                known_at=max(now, fill.executed_at),
            )
            attachment = attached.attachment_event
        await self._publisher.emit(
            event_key=f"exit-plan-fill-attached:{fill.fill_id}",
            event_type="EXIT_PLAN_ATTACHED_TO_FILL",
            phase=phase,
            severity=PaperDaySeverity.NOTICE,
            payload={
                "attachment_event_id": attachment.event_id,
                "fill_id": fill.fill_id,
                "plan_id": attachment.plan_id,
                "protection_id": protection_id,
            },
            symbol=fill.symbol,
            correlation_id=fill.fill_id,
            occurred_at=fill.executed_at,
        )
        active = lifecycle.active_plan(protection_id)
        if active.depth is ExitPlanDepth.DEEP:
            await self._emit_deep_exit_plan_projection(
                plan=active,
                fill=fill,
                phase=phase,
            )
            return
        if self._deep_exit_assessment_provider is None:
            await self._retain_quick_after_semantic_failure(
                fill=fill,
                protection_id=protection_id,
                planning_bars=planning_bars,
                phase=phase,
                attempt_id=f"provider-unavailable-{fill.fill_id}",
                reason_code="DEEP_DUAL_TRACK_PROVIDER_UNAVAILABLE_QUICK_RETAINED",
            )
            return
        try:
            timeframes = _paper_deep_timeframes(planning_bars)
            baseline: DeepSemanticAssessment | None = None
            adversarial: DeepSemanticAssessment | None = None
            try:
                baseline, adversarial = await self._deep_exit_assessment_provider.assess(
                        plan=active,
                        timeframes=timeframes,
                        decision_at=now,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._retain_quick_after_semantic_failure(
                    fill=fill,
                    protection_id=protection_id,
                    planning_bars=planning_bars,
                    phase=phase,
                    attempt_id=f"provider-exception-{fill.fill_id}",
                    reason_code="DEEP_SEMANTIC_PROVIDER_FAILED_QUICK_RETAINED",
                )
                return
            if baseline is None or adversarial is None:
                await self._retain_quick_after_semantic_failure(
                    fill=fill,
                    protection_id=protection_id,
                    planning_bars=planning_bars,
                    phase=phase,
                    attempt_id=f"provider-incomplete-{fill.fill_id}",
                    reason_code="DEEP_DUAL_TRACK_INCOMPLETE_QUICK_RETAINED",
                )
                return
            built = lifecycle.build_and_apply_deep(
                protection_id,
                timeframes=timeframes,
                decision_at=now,
                known_at=now,
                baseline_assessment=baseline,
                adversarial_assessment=adversarial,
                config=self._deep_exit_config,
            )
        except asyncio.CancelledError:
            raise
        except (ArithmeticError, ExitPlanLifecycleConflictError, TypeError, ValueError):
            failure = lifecycle.record_deep_analysis_failure(
                protection_id,
                attempt_id=f"generator-{fill.fill_id}",
                reason_code="DEEP_GENERATION_FAILED_QUICK_RETAINED",
                failed_at=fill.executed_at,
                known_at=max(now, fill.executed_at),
            )
            await self._publisher.emit(
                event_key=f"exit-plan-deep-failed:{fill.fill_id}",
                event_type="EXIT_PLAN_DEEP_FAILED",
                phase=phase,
                severity=PaperDaySeverity.WARNING,
                payload={
                    "active_quick_plan": exit_plan_document(lifecycle.active_plan(protection_id)),
                    "error_code": "DEEP_GENERATION_FAILED_QUICK_RETAINED",
                    "failure_event_id": failure.event_id,
                    "fill_id": fill.fill_id,
                    "protection_id": protection_id,
                },
                notification_text=(
                    "【A股模拟盘｜DEEP 退出计划降级】\n"
                    f"标的：{fill.symbol}\n"
                    "多时间框架复核暂不可用；成交前 QUICK 保护继续有效，监控不中断。"
                ),
                symbol=fill.symbol,
                correlation_id=fill.fill_id,
                occurred_at=fill.executed_at,
            )
            return
        await self._emit_deep_exit_plan_projection(
            plan=built.application.active_plan,
            fill=fill,
            phase=phase,
        )

    async def _retain_quick_after_semantic_failure(
        self,
        *,
        fill: ASharePaperFill,
        protection_id: str,
        planning_bars: tuple[TechnicalBar, ...],
        phase: PaperDayPhase,
        attempt_id: str,
        reason_code: str,
    ) -> None:
        """语义轨失败时保留 QUICK，并在当前 runner 内持久重试。"""

        lifecycle = self._exit_plan_lifecycle
        if lifecycle is None:  # pragma: no cover - 由调用路径保证
            return
        failure = lifecycle.record_deep_analysis_failure(
            protection_id,
            attempt_id=attempt_id,
            reason_code=reason_code,
            failed_at=fill.executed_at,
            known_at=max(self._now(), fill.executed_at),
        )
        active = lifecycle.active_plan(protection_id)
        await self._publisher.emit(
            event_key=(
                f"exit-plan-deep-semantic-failed:{fill.fill_id}:"
                f"{_document_sha256({'reason_code': reason_code})[:12]}"
            ),
            event_type="EXIT_PLAN_DEEP_FAILED",
            phase=phase,
            severity=PaperDaySeverity.WARNING,
            payload={
                "active_quick_plan": exit_plan_document(active),
                "error_code": reason_code,
                "failure_event_id": failure.event_id,
                "fill_id": fill.fill_id,
                "protection_id": protection_id,
                "retry_policy": "RUNNER_BACKGROUND_AND_RESTART_RECOVERY",
            },
            notification_text=(
                "【A股模拟盘｜DEEP 双轨复核降级】\n"
                f"标的：{fill.symbol}\n"
                "单分析器或对抗分析器本次未形成完整结果；成交前 QUICK 保护继续有效，"
                "当前进程会后台重试，重启后也会从持久请求恢复。"
            ),
            symbol=fill.symbol,
            correlation_id=fill.fill_id,
            occurred_at=fill.executed_at,
        )
        self._schedule_deep_followup_retry(
            fill=fill,
            protection_id=protection_id,
            planning_bars=planning_bars,
            phase=phase,
        )

    def _schedule_deep_followup_retry(
        self,
        *,
        fill: ASharePaperFill,
        protection_id: str,
        planning_bars: tuple[TechnicalBar, ...],
        phase: PaperDayPhase,
    ) -> None:
        existing = self._deep_followup_retry_tasks.get(protection_id)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(
            self._deep_followup_retry_loop(
                fill=fill,
                protection_id=protection_id,
                planning_bars=planning_bars,
                phase=phase,
            ),
            name=f"paper-day-deep-retry-{protection_id[-12:]}",
        )
        self._deep_followup_retry_tasks[protection_id] = task

        def discard(completed: asyncio.Task[None]) -> None:
            if self._deep_followup_retry_tasks.get(protection_id) is completed:
                self._deep_followup_retry_tasks.pop(protection_id, None)
            with suppress(asyncio.CancelledError, Exception):
                completed.exception()

        task.add_done_callback(discard)

    async def _deep_followup_retry_loop(
        self,
        *,
        fill: ASharePaperFill,
        protection_id: str,
        planning_bars: tuple[TechnicalBar, ...],
        phase: PaperDayPhase,
    ) -> None:
        """按有界间隔重试持久 DEEP 请求；每次模型 case 自身仍有 deadline。"""

        lifecycle = self._exit_plan_lifecycle
        if lifecycle is None:  # pragma: no cover - 由调度路径保证
            return
        while lifecycle.active_plan(protection_id).depth is ExitPlanDepth.QUICK:
            await asyncio.sleep(self._DEEP_SEMANTIC_RETRY_INTERVAL.total_seconds())
            await self._ensure_exit_plan_post_fill(
                fill=fill,
                protection_id=protection_id,
                planning_bars=planning_bars,
                phase=phase,
            )
            if lifecycle.active_plan(protection_id).depth is ExitPlanDepth.DEEP:
                return

    async def _emit_deep_exit_plan_projection(
        self,
        *,
        plan: ExitPlan,
        fill: ASharePaperFill,
        phase: PaperDayPhase,
    ) -> None:
        deep_exit_llm_assessment = self._deep_exit_llm_assessment(plan.symbol)
        await self._publisher.emit(
            event_key=f"exit-plan-deep-applied:{plan.plan_id}",
            event_type="EXIT_PLAN_DEEP_APPLIED",
            phase=phase,
            severity=PaperDaySeverity.NOTICE,
            payload={
                "fill_id": fill.fill_id,
                "deep_exit_llm_assessment": deep_exit_llm_assessment,
                "plan": exit_plan_document(plan),
                "protection_id": plan.protection_id,
                "semantic_policy": "ADVERSARIAL_PREFERRED_BASELINE_RETAINED",
                "hard_risk_policy": "LLM_CANNOT_LOOSEN_STOP_OR_EXTEND_TIME",
            },
            notification_text=(
                "【A股模拟盘｜DEEP 退出计划已生效】\n"
                f"标的：{plan.symbol}\n"
                f"确定止损：{plan.stop_price:.3f}；目标：{plan.take_profit_price:.3f}；"
                f"最晚复核：{plan.time_exit_at.astimezone(SHANGHAI):%Y-%m-%d %H:%M}\n"
                f"{_deep_exit_llm_assessment_text(deep_exit_llm_assessment)}\n"
                "价格门槛来自多时间框架完成线；LLM 不能放宽硬止损。"
            ),
            symbol=plan.symbol,
            correlation_id=fill.fill_id,
            occurred_at=fill.executed_at,
        )

    async def _observe_exit_plan_bar(
        self,
        *,
        symbol: str,
        bar: TechnicalBar,
        phase: PaperDayPhase,
        observed_at: datetime,
    ) -> None:
        lifecycle = self._exit_plan_lifecycle
        protection_id = self._exit_protection_by_symbol.get(symbol)
        if lifecycle is None or protection_id is None:
            return
        snapshot = self._paper.snapshot(self._manifest.account_id)
        position = snapshot.position(symbol)
        if position is None or position.quantity <= 0:
            return
        observation = lifecycle.observe_completed_bar(
            protection_id,
            bar=bar,
            observed_at=observed_at,
            sellable_quantity=position.available_to_sell,
        )
        if observation.selected_barrier is None:
            return
        selected = observation.selected_barrier
        plan = observation.active_plan
        deep_exit_llm_assessment = self._deep_exit_llm_assessment_document(plan)
        event_key = f"exit-plan-barrier:{plan.plan_id}:{bar.end_time.isoformat()}"
        payload = {
            "available_to_sell": position.available_to_sell,
            "crossed_barriers": tuple(item.value for item in observation.crossed_barriers),
            "deep_exit_llm_assessment": deep_exit_llm_assessment,
            "execution_action": "NO_SELL_ORDER",
            "execution_handoff_required": observation.execution_handoff_required,
            "order_created": False,
            "plan": exit_plan_document(plan),
            "protection_id": protection_id,
            "selected_barrier": selected.value,
            "suppression_reason": observation.suppression_reason,
        }
        await self._publisher.emit(
            event_key=event_key,
            event_type="EXIT_PLAN_BARRIER_TRIGGERED",
            phase=phase,
            severity=PaperDaySeverity.WARNING,
            payload=payload,
            notification_text=(
                "【A股模拟盘｜退出计划触发】\n"
                f"标的：{symbol}；门槛：{_exit_barrier_display(selected)}\n"
                f"计划止损：{plan.stop_price:.3f}；目标：{plan.take_profit_price:.3f}；"
                f"可卖：{position.available_to_sell}\n"
                f"{_deep_exit_llm_assessment_text(deep_exit_llm_assessment)}\n"
                "本轮按 PAPER 约束仅记录并推送，不创建真实或模拟卖单。"
            ),
            symbol=symbol,
            correlation_id=protection_id,
            occurred_at=bar.end_time,
        )

    def _restore_state(self) -> None:
        """从不可变当日日志重建易失的调度器状态。"""

        retained = self._store.events(self._manifest.run_id)
        if self._intraday_llm is not None:
            scheduled_llm_contexts: set[str] = set()
            for event in retained:
                if event.event_type != "LLM_REVIEW_BATCH_SCHEDULED":
                    continue
                outcomes = event.payload.get("outcomes")
                if not isinstance(outcomes, list):
                    continue
                for outcome in outcomes:
                    if not isinstance(outcome, dict) or outcome.get("status") != (
                        IntradayLLMScheduleStatus.SCHEDULED.value
                    ):
                        continue
                    context_id = outcome.get("context_id")
                    if isinstance(context_id, str) and context_id.strip():
                        scheduled_llm_contexts.add(context_id)
            self._intraday_llm.restore_session_budget(len(scheduled_llm_contexts))
        rejected_llm_reviews = {
            review_id
            for event in retained
            if event.event_type == "LLM_CANDIDATE_REVIEW_CACHE_REJECTED"
            if isinstance(
                review_id := event.payload.get("review_id"),
                str,
            )
        }
        for event in retained:
            payload = event.payload
            if event.event_type == "LLM_SERVICE_STATE_CHANGED" and self._intraday_llm is not None:
                state = payload.get("state")
                if state in {"UNKNOWN", "HEALTHY", "DEGRADED"}:
                    self._llm_service_state = cast(str, state)
            elif (
                event.event_type == "LLM_PREOPEN_CONTEXT_FROZEN" and self._intraday_llm is not None
            ):
                value = payload.get("preopen_context")
                try:
                    restored_preopen = _llm_preopen_context_from_document(value)
                except (KeyError, TypeError, ValueError):
                    self._llm_preopen_restore_conflict = True
                    self._llm_preopen_context = None
                else:
                    if (
                        self._llm_preopen_context is not None
                        and self._llm_preopen_context != restored_preopen
                    ):
                        self._llm_preopen_restore_conflict = True
                        self._llm_preopen_context = None
                    elif not self._llm_preopen_restore_conflict:
                        self._llm_preopen_context = restored_preopen
            elif (
                event.event_type == "LLM_PREOPEN_CONTEXT_FAILED" and self._intraday_llm is not None
            ):
                self._llm_preopen_context = None
            elif (
                event.event_type == "LLM_REVIEW_BATCH_SCHEDULED" and self._intraday_llm is not None
            ):
                outcomes = payload.get("outcomes")
                if isinstance(outcomes, list):
                    for value in outcomes:
                        if not isinstance(value, dict):
                            continue
                        symbol = value.get("symbol")
                        context_id = value.get("context_id")
                        if isinstance(symbol, str) and isinstance(context_id, str):
                            self._latest_llm_context_by_symbol[symbol] = context_id
            elif (
                event.event_type
                in {
                    "LLM_CANDIDATE_REVIEW_COMPLETED",
                    "LLM_CANDIDATE_REVIEW_FAILED",
                }
                and self._intraday_llm is not None
            ):
                try:
                    review = _llm_review_from_document(payload.get("review"))
                    if review.review_id in rejected_llm_reviews:
                        raise ValueError("review cache acceptance was rejected")
                    self._intraday_llm.restore_journaled(
                        review,
                        accepted_at=event.known_at,
                        journal_event_id=event.event_id,
                        journal_event_sha256=event.event_hash,
                    )
                except (ArithmeticError, KeyError, TypeError, ValueError):
                    self._llm_restore_rejections += 1
                else:
                    self._llm_restored_reviews += 1
                    self._latest_llm_context_by_symbol[review.symbol] = review.context_id
            elif event.event_type in {
                "PREOPEN_SCREEN_COMPLETED",
                "PREOPEN_SCREEN_RECOVERED",
            }:
                candidates = payload.get("candidates")
                if isinstance(candidates, list):
                    restored = tuple(
                        item
                        for item in (
                            _watch_entry_from_document(value, "PREOPEN_SCREEN")
                            for value in candidates
                        )
                        if item is not None
                    )
                    self._initial = restored
                    self._watchlist = {item.symbol: item for item in restored}
            elif event.event_type == "WATCHLIST_UPDATED":
                values = payload.get("watchlist")
                if isinstance(values, list):
                    restored = tuple(
                        item
                        for item in (
                            _watch_entry_from_document(value, "RESTORED") for value in values
                        )
                        if item is not None
                    )
                    self._watchlist = {item.symbol: item for item in restored}
            elif event.event_type == "SURVEILLANCE_SCAN_COMPLETED":
                candidates = payload.get("candidates")
                if isinstance(candidates, list):
                    self._latest_candidates = {
                        item.symbol: item
                        for item in (
                            candidate
                            for candidate in (
                                _candidate_from_document(value) for value in candidates
                            )
                            if candidate is not None
                        )
                    }
                    for item in self._latest_candidates.values():
                        previous_close = _candidate_previous_close(item)
                        if previous_close is not None:
                            self._session_previous_closes[item.symbol] = previous_close
                value = payload.get("decision_at")
                self._latest_scan_at = _optional_datetime(value)
                source = payload.get("source_id")
                self._latest_scan_source = source if isinstance(source, str) else None
                status = payload.get("status")
                self._latest_scan_status = status if isinstance(status, str) else None
            elif event.event_type == "ORDER_SUBMITTED":
                value = payload.get("order")
                if isinstance(value, dict):
                    order = _order_from_document(value)
                    self._pending[order.symbol] = order
                    self._session_previous_closes[order.symbol] = order.previous_close
                    protection_id = payload.get("exit_protection_id")
                    if isinstance(protection_id, str):
                        self._exit_protection_by_symbol[order.symbol] = protection_id
            elif event.event_type == "EXIT_PLAN_QUICK_CREATED" and event.symbol:
                protection_id = payload.get("protection_id")
                if isinstance(protection_id, str):
                    self._exit_protection_by_symbol[event.symbol] = protection_id
            elif event.event_type in {
                "ORDER_MATCH_EVALUATED",
                "ORDER_EXPIRED_UNFILLED",
                "ORDER_EXPIRED_AT_RECESS",
                "ORDER_EXPIRED_AT_CLOSE",
                "FILL_STARTED",
                "FILL_APPLIED",
            }:
                if event.symbol is not None:
                    self._pending.pop(event.symbol, None)
                if event.event_type == "FILL_APPLIED" and event.symbol is not None:
                    self._entered_symbols.add(event.symbol)
                    protection_id = payload.get("exit_protection_id")
                    if isinstance(protection_id, str):
                        self._exit_protection_by_symbol[event.symbol] = protection_id
            elif event.event_type == "TECHNICAL_SIGNAL_EVALUATED" and event.symbol:
                bar_end = _optional_datetime(payload.get("bar_end"))
                decision = payload.get("decision")
                reference_price = _optional_positive_decimal(payload.get("reference_price"))
                if bar_end is not None:
                    self._last_processed_bar[event.symbol] = bar_end
                if isinstance(decision, str):
                    self._last_technical_state[event.symbol] = decision
                if reference_price is not None:
                    self._last_prices[event.symbol] = reference_price
            elif event.event_type == "SOURCE_STATE_CHANGED":
                component = payload.get("component")
                state = payload.get("state")
                if isinstance(component, str) and isinstance(state, str):
                    self._last_source_state[component] = state

    def _open_or_resume_account(self) -> PaperAccountSnapshot:
        try:
            snapshot = self._paper.snapshot(self._manifest.account_id)
        except PaperAccountNotFoundError:
            return self._paper.open_account(
                self._manifest.account_id,
                initial_cash=self._manifest.initial_cash,
                session_date=self._manifest.session_date,
                opened_at=self._manifest.created_at,
            )
        if snapshot.session_date < self._manifest.session_date:
            return self._paper.rollover_session(
                self._manifest.account_id,
                target_session_date=self._manifest.session_date,
                occurred_at=max(self._now(), snapshot.updated_at),
            )
        if snapshot.session_date > self._manifest.session_date:
            raise RuntimeError("PAPER account is already beyond the requested session")
        return snapshot

    async def _renew_lease(self, *, force: bool = False) -> None:
        now = self._now()
        if force or self._last_lease_renewed_at is None:
            self._store.acquire_lease(
                self._manifest.run_id,
                self._owner_id,
                now=now,
                lease_for=self._config.lease_for,
            )
            self._last_lease_renewed_at = now
            return
        if now - self._last_lease_renewed_at >= self._config.lease_renew_interval:
            self._store.renew_lease(
                self._manifest.run_id,
                self._owner_id,
                now=now,
                lease_for=self._config.lease_for,
            )
            self._last_lease_renewed_at = now

    async def _wait_until(self, target: datetime) -> None:
        while (now := self._now()) < target:
            self._raise_if_background_failed()
            await self._renew_lease()
            await self._sleep(
                scheduler_sleep_seconds(
                    now,
                    target,
                    self._config.scheduler_tick_seconds,
                )
            )

    async def _dispatch_loop(self) -> None:
        while True:
            await self._publisher.dispatch_once()
            await self._sleep(1.0)

    async def _lease_loop(self, parent_task: asyncio.Task[object]) -> None:
        """当耗时的提供方调用让出事件循环时，独立续租。"""

        interval = max(
            1.0,
            self._config.lease_renew_interval.total_seconds() / 2,
        )
        try:
            while True:
                await self._sleep(interval)
                await self._renew_lease(force=False)
                self._publisher.write_heartbeat()
        except asyncio.CancelledError:
            raise
        except Exception:
            parent_task.cancel()
            raise

    def _raise_if_background_failed(self) -> None:
        """当安全关键心跳失败时，使前台循环失败。"""

        task = self._lease_task
        if task is None or not task.done():
            return
        if task.cancelled():
            raise RuntimeError("PAPER-day lease heartbeat stopped")
        error = task.exception()
        if error is not None:
            raise RuntimeError("PAPER-day lease heartbeat failed") from error
        raise RuntimeError("PAPER-day lease heartbeat ended unexpectedly")

    async def _source_transition(
        self,
        *,
        component: str,
        state: str,
        phase: PaperDayPhase,
        error_code: str | None,
        detail: Mapping[str, object] | None = None,
    ) -> None:
        previous = self._last_source_state.get(component)
        if previous == state:
            return
        self._last_source_state[component] = state
        now = self._now()
        payload: dict[str, object] = {
            "component": component,
            "error_code": error_code,
            "previous_state": previous,
            "state": state,
        }
        if detail is not None:
            payload["detail"] = dict(detail)
        await self._publisher.emit(
            event_key=f"source-state:{component}:{state}:{now.strftime('%H%M%S')}",
            event_type="SOURCE_STATE_CHANGED",
            phase=phase,
            severity=(PaperDaySeverity.INFO if state == "HEALTHY" else PaperDaySeverity.WARNING),
            payload=payload,
            notification_text=(
                "【A股模拟盘｜数据源状态变化】\n"
                f"组件：{component}\n状态：{previous or 'UNKNOWN'} → {state}"
                + ("" if error_code is None else f"\n错误码：{error_code}")
            ),
        )

    def _account_summary_document(self) -> dict[str, object]:
        snapshot = self._paper.snapshot(self._manifest.account_id)
        market_value = sum(
            (
                self._last_prices.get(item.symbol, item.average_cost) * item.quantity
                for item in snapshot.positions
            ),
            Decimal("0"),
        )
        return {
            "cash": snapshot.cash,
            "estimated_equity": snapshot.cash + market_value,
            "estimated_market_value": market_value,
            "positions": [
                {
                    "available_to_sell": item.available_to_sell,
                    "average_cost": item.average_cost,
                    "mark": self._last_prices.get(item.symbol),
                    "quantity": item.quantity,
                    "symbol": item.symbol,
                    "today_buy": item.today_buy,
                }
                for item in snapshot.positions
                if item.quantity > 0
            ],
        }

    def _account_summary_text(self, label: str) -> str:
        document = self._account_summary_document()
        positions = cast(list[dict[str, object]], document["positions"])
        return (
            f"【A股模拟盘｜{label}摘要】\n"
            f"现金：{Decimal(str(document['cash'])):.2f} 元\n"
            f"估算持仓市值：{Decimal(str(document['estimated_market_value'])):.2f} 元\n"
            f"估算权益：{Decimal(str(document['estimated_equity'])):.2f} 元\n"
            f"持仓数量：{len(positions)}；待撮合：{len(self._pending)}。"
        )

    async def _expire_pending_orders(
        self,
        *,
        phase: PaperDayPhase,
        event_type: str,
        reason: str,
    ) -> None:
        """终止 IOC 意图，不将其跨休市时段延续。"""

        for order in tuple(self._pending.values()):
            await self._publisher.emit(
                event_key=f"order-recess-expired:{order.order_id}",
                event_type=event_type,
                phase=phase,
                severity=PaperDaySeverity.WARNING,
                payload={
                    "order_id": order.order_id,
                    "reason": reason,
                    "remaining_quantity": order.quantity,
                },
                notification_text=(
                    "【A股模拟盘｜午休前撤销未成交委托】\n"
                    f"标的：{order.symbol}；数量：{order.quantity} 股；"
                    "单分钟 IOC 不跨午间休市延续。"
                ),
                symbol=order.symbol,
                correlation_id=order.order_id,
            )
        self._pending.clear()

    def _at(self, wall_time: time) -> datetime:
        return session_datetime(self._manifest.session_date, wall_time)

    def _now(self) -> datetime:
        return _aware_utc(self._clock(), "clock")

    async def _finalize(self) -> PaperDayResult:
        await self._drain_intraday_llm_reviews(phase=PaperDayPhase.CLOSING)
        for order in tuple(self._pending.values()):
            await self._publisher.emit(
                event_key=f"order-close-expired:{order.order_id}",
                event_type="ORDER_EXPIRED_AT_CLOSE",
                phase=PaperDayPhase.CLOSING,
                severity=PaperDaySeverity.WARNING,
                payload={"order_id": order.order_id, "remaining_quantity": order.quantity},
                notification_text=(
                    "【A股模拟盘｜收盘撤销未成交委托】\n"
                    f"标的：{order.symbol}；数量：{order.quantity} 股。"
                ),
                symbol=order.symbol,
                correlation_id=order.order_id,
            )
        self._pending.clear()
        await self._publisher.emit(
            event_key="market-closed",
            event_type="MARKET_CLOSED",
            phase=PaperDayPhase.CLOSING,
            severity=PaperDaySeverity.NOTICE,
            payload=self._account_summary_document(),
            notification_text=self._account_summary_text("收盘"),
        )
        self._publisher.reconcile_notifications()
        for _ in range(3):
            await self._publisher.dispatch_once()
            await self._sleep(0.2)
        required, sent, gaps = self._notification_counts()
        await self._publisher.emit(
            event_key="day-completed",
            event_type=("DAY_COMPLETED" if gaps == 0 else "DAY_COMPLETED_WITH_NOTIFICATION_GAPS"),
            phase=PaperDayPhase.TERMINAL,
            severity=(PaperDaySeverity.NOTICE if gaps == 0 else PaperDaySeverity.WARNING),
            payload={
                **self._account_summary_document(),
                "notification_required_before_summary": required,
                "notification_sent_before_summary": sent,
                "notification_gaps_before_summary": gaps,
                "partial_session": self._partial_session,
            },
            notification_text=(
                "【A股模拟盘｜全天监控完成】\n"
                f"交易日：{self._manifest.session_date.isoformat()}\n"
                f"会话完整性：{'PARTIAL_SESSION' if self._partial_session else 'FULL_SESSION'}\n"
                + self._account_summary_text("最终").split("\n", maxsplit=1)[1]
                + f"\n此前必推事件投递缺口：{gaps}。详细 Markdown 日志随后上传。"
            ),
        )
        for _ in range(3):
            await self._publisher.dispatch_once()
            await self._sleep(0.2)
        required, sent, gaps = self._notification_counts()
        self._report_dir.mkdir(parents=True, exist_ok=True)
        report_path = self._report_dir / (
            f"ashare-paper-day-{self._manifest.session_date.isoformat()}-"
            f"{self._manifest.run_id[-10:]}.md"
        )
        _atomic_write_text(report_path, self._render_report(required, sent, gaps))
        await self._publisher.emit(
            event_key="report-generated",
            event_type="REPORT_GENERATED",
            phase=PaperDayPhase.POST_CLOSE,
            severity=PaperDaySeverity.INFO,
            payload={
                "report_name": report_path.name,
                "sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
            },
        )
        artifact_status = await self._upload_report(report_path)
        return self._paper_day_result(
            report_path=report_path,
            artifact_status=artifact_status,
        )

    async def _completed_result_if_available(self) -> PaperDayResult | None:
        if (
            self._store.event_by_key(
                self._manifest.run_id,
                "day-completed",
            )
            is None
        ):
            return None
        if (
            self._store.event_by_key(
                self._manifest.run_id,
                "report-generated",
            )
            is None
        ):
            return None
        reports = tuple(
            sorted(
                self._report_dir.glob(
                    f"ashare-paper-day-{self._manifest.session_date.isoformat()}-"
                    f"{self._manifest.run_id[-10:]}.md"
                )
            )
        )
        if not reports:
            return None
        artifact_status = await self._upload_report(reports[-1])
        return self._paper_day_result(
            report_path=reports[-1],
            artifact_status=artifact_status,
        )

    def _paper_day_result(
        self,
        *,
        report_path: Path,
        artifact_status: str,
    ) -> PaperDayResult:
        text_required, text_sent, text_gaps = self._notification_counts()
        artifact_sent = artifact_status == ReportArtifactStatus.SENT.value
        artifact_gap = 0 if artifact_sent else 1
        result = PaperDayResult(
            run_id=self._manifest.run_id,
            session_date=self._manifest.session_date,
            completed=True,
            event_count=len(self._store.events(self._manifest.run_id)),
            notification_required=text_required + 1,
            notification_sent=text_sent + int(artifact_sent),
            notification_gaps=text_gaps + artifact_gap,
            final_snapshot=self._paper.snapshot(self._manifest.account_id),
            report_path=report_path,
            artifact_delivery_status=artifact_status,
            artifact_delivery_complete=artifact_sent,
            daily_review_delivery_complete=artifact_sent and text_gaps == 0,
            text_notification_required=text_required,
            text_notification_sent=text_sent,
            text_notification_gaps=text_gaps,
        )
        self._publisher.write_delivery_projection(
            artifact_delivery_status=result.artifact_delivery_status,
            artifact_delivery_complete=result.artifact_delivery_complete,
            daily_review_delivery_complete=result.daily_review_delivery_complete,
            notification_required=result.notification_required,
            notification_sent=result.notification_sent,
            notification_gaps=result.notification_gaps,
            text_notification_required=result.text_notification_required,
            text_notification_sent=result.text_notification_sent,
            text_notification_gaps=result.text_notification_gaps,
        )
        return result

    async def _upload_report(self, report_path: Path) -> str:
        """通过持久一次性交付边界上传日报，任何歧义都禁止自动重发。"""

        try:
            report_bytes = report_path.read_bytes()
            report_text = report_bytes.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
            validate_markdown_report_contract(ReportKind.DAILY_REVIEW, report_text)
        except (OSError, UnicodeError, ValueError):
            await self._emit_report_artifact_state(
                report_name=report_path.name,
                artifact_sha256=None,
                status=ReportArtifactStatus.AMBIGUOUS.value,
                error_code="REPORT_ARTIFACT_INVALID_OR_UNREADABLE",
            )
            return ReportArtifactStatus.AMBIGUOUS.value

        artifact_sha256 = hashlib.sha256(report_bytes).hexdigest()
        generated = self._store.event_by_key(self._manifest.run_id, "report-generated")
        if (
            generated is None
            or generated.payload.get("report_name") != report_path.name
            or generated.payload.get("sha256") != artifact_sha256
        ):
            await self._emit_report_artifact_state(
                report_name=report_path.name,
                artifact_sha256=artifact_sha256,
                status=ReportArtifactStatus.AMBIGUOUS.value,
                error_code="REPORT_ARTIFACT_GENERATION_BINDING_MISMATCH",
            )
            return ReportArtifactStatus.AMBIGUOUS.value

        if (
            self._artifact_notifier is None
            or self._artifact_target_kind is None
            or self._artifact_target_id is None
            or self._artifact_outbox is None
        ):
            await self._emit_report_artifact_state(
                report_name=report_path.name,
                artifact_sha256=artifact_sha256,
                status="NOT_CONFIGURED",
                error_code="ARTIFACT_NOTIFIER_NOT_CONFIGURED",
            )
            return "NOT_CONFIGURED"

        delivery_key = _paper_report_artifact_key(
            run_id=self._manifest.run_id,
            target_kind=self._artifact_target_kind,
            target_id=self._artifact_target_id,
            artifact_sha256=artifact_sha256,
        )
        try:
            existing_delivery = self._artifact_outbox.get(delivery_key)
            delivery = self._artifact_outbox.enqueue(
                idempotency_key=delivery_key,
                report_kind=ReportKind.DAILY_REVIEW.value,
                target_kind=self._artifact_target_kind,
                target_id=self._artifact_target_id,
                artifact_name=report_path.name,
                artifact_sha256=artifact_sha256,
                created_at=self._now(),
            )
            delivery = await self._migrate_report_artifact_journal(
                delivery=delivery,
                delivery_key=delivery_key,
                report_name=report_path.name,
                artifact_sha256=artifact_sha256,
                outbox_record_was_missing=existing_delivery is None,
            )
        except (ReportArtifactOutboxError, ValueError):
            await self._emit_report_artifact_state(
                report_name=report_path.name,
                artifact_sha256=artifact_sha256,
                status=ReportArtifactStatus.AMBIGUOUS.value,
                error_code="REPORT_ARTIFACT_OUTBOX_BINDING_FAILED",
            )
            return ReportArtifactStatus.AMBIGUOUS.value

        if delivery.status is ReportArtifactStatus.IN_FLIGHT:
            delivery = self._artifact_outbox.mark_ambiguous(delivery_key)
        if delivery.status is ReportArtifactStatus.AMBIGUOUS:
            delivery = await self._apply_report_artifact_recovery(
                delivery=delivery,
                delivery_key=delivery_key,
                artifact_sha256=artifact_sha256,
                report_name=report_path.name,
            )
            if delivery.status is ReportArtifactStatus.AMBIGUOUS:
                await self._emit_report_artifact_state(
                    report_name=report_path.name,
                    artifact_sha256=artifact_sha256,
                    status=ReportArtifactStatus.AMBIGUOUS.value,
                    error_code="REPORT_ARTIFACT_DELIVERY_AMBIGUOUS",
                )
                return ReportArtifactStatus.AMBIGUOUS.value
        if delivery.status is ReportArtifactStatus.SENT:
            await self._emit_report_artifact_state(
                report_name=report_path.name,
                artifact_sha256=artifact_sha256,
                status=ReportArtifactStatus.SENT.value,
                provider_identifier=delivery.provider_identifier,
            )
            return ReportArtifactStatus.SENT.value

        await self._emit_report_artifact_state(
            report_name=report_path.name,
            artifact_sha256=artifact_sha256,
            status=ReportArtifactStatus.PENDING.value,
        )
        try:
            delivery = self._artifact_outbox.claim(delivery_key, claimed_at=self._now())
        except ReportArtifactOutboxError:
            retained = self._artifact_outbox.get(delivery_key)
            if retained is not None and retained.status is ReportArtifactStatus.IN_FLIGHT:
                retained = self._artifact_outbox.mark_ambiguous(delivery_key)
            status = (
                ReportArtifactStatus.AMBIGUOUS.value
                if retained is None
                else retained.status.value
            )
            await self._emit_report_artifact_state(
                report_name=report_path.name,
                artifact_sha256=artifact_sha256,
                status=status,
                error_code="REPORT_ARTIFACT_CLAIM_FAILED",
            )
            return status
        if delivery.status is ReportArtifactStatus.SENT:
            await self._emit_report_artifact_state(
                report_name=report_path.name,
                artifact_sha256=artifact_sha256,
                status=ReportArtifactStatus.SENT.value,
                provider_identifier=delivery.provider_identifier,
            )
            return ReportArtifactStatus.SENT.value

        try:
            if hashlib.sha256(report_path.read_bytes()).hexdigest() != artifact_sha256:
                raise ReportArtifactOutboxError("REPORT_ARTIFACT_CHANGED_AFTER_CLAIM")
            receipt = (
                await self._artifact_notifier.upload_private_file(
                    self._artifact_target_id,
                    report_path,
                )
                if self._artifact_target_kind is NotificationTargetKind.PRIVATE
                else await self._artifact_notifier.upload_group_file(
                    self._artifact_target_id,
                    report_path,
                )
            )
            provider_identifier = getattr(receipt, "provider_file_id", None)
            if not isinstance(provider_identifier, str) or not provider_identifier.strip():
                raise ReportArtifactOutboxError("REPORT_ARTIFACT_PROVIDER_RECEIPT_MISSING")
            delivery = self._artifact_outbox.mark_sent(
                delivery_key,
                provider_identifier=provider_identifier,
                sent_at=self._now(),
            )
        except asyncio.CancelledError:
            with suppress(Exception):
                self._artifact_outbox.mark_ambiguous(delivery_key)
            raise
        except Exception:
            with suppress(Exception):
                self._artifact_outbox.mark_ambiguous(delivery_key)
            await self._emit_report_artifact_state(
                report_name=report_path.name,
                artifact_sha256=artifact_sha256,
                status=ReportArtifactStatus.AMBIGUOUS.value,
                error_code="REPORT_ARTIFACT_DELIVERY_AMBIGUOUS",
            )
            return ReportArtifactStatus.AMBIGUOUS.value

        await self._emit_report_artifact_state(
            report_name=report_path.name,
            artifact_sha256=artifact_sha256,
            status=ReportArtifactStatus.SENT.value,
            provider_identifier=delivery.provider_identifier,
        )
        return ReportArtifactStatus.SENT.value

    async def _migrate_report_artifact_journal(
        self,
        *,
        delivery: ReportArtifactRecord,
        delivery_key: str,
        report_name: str,
        artifact_sha256: str,
        outbox_record_was_missing: bool,
    ) -> ReportArtifactRecord:
        """把旧不可变上传回执绑定到新 outbox，绝不为迁移调用提供方。"""

        artifact_outbox = self._artifact_outbox
        if artifact_outbox is None:  # pragma: no cover - 由调用边界保证
            raise ReportArtifactOutboxError("REPORT_ARTIFACT_OUTBOX_NOT_CONFIGURED")
        if delivery.status is not ReportArtifactStatus.PENDING:
            return delivery
        sent_event = self._store.event_by_key(
            self._manifest.run_id,
            f"report-artifact-sent:{artifact_sha256}",
        )
        legacy = self._store.event_by_key(self._manifest.run_id, "report-upload-result")
        ambiguous_event = self._store.event_by_key(
            self._manifest.run_id,
            f"report-artifact-ambiguous:{artifact_sha256}",
        )
        pending_event = self._store.event_by_key(
            self._manifest.run_id,
            f"report-artifact-pending:{artifact_sha256}",
        )
        legacy_sent = (
            legacy is not None
            and legacy.event_type == "REPORT_UPLOADED"
            and legacy.payload.get("delivered") is True
            and legacy.payload.get("report_name") == report_name
        )
        if sent_event is not None or legacy_sent:
            source = sent_event if sent_event is not None else legacy
            assert source is not None
            claimed = artifact_outbox.claim(delivery_key, claimed_at=self._now())
            migrated = artifact_outbox.mark_sent(
                delivery_key,
                provider_identifier=f"journal-{source.event_id}",
                sent_at=source.known_at,
            )
            await self._publisher.emit(
                event_key=f"report-artifact-legacy-bound:{artifact_sha256}",
                event_type="REPORT_ARTIFACT_LEGACY_RECEIPT_BOUND",
                phase=PaperDayPhase.POST_CLOSE,
                severity=PaperDaySeverity.NOTICE,
                payload={
                    "artifact_sha256": artifact_sha256,
                    "previous_status": claimed.status.value,
                    "report_name": report_name,
                    "status": migrated.status.value,
                    "upload_attempted": False,
                },
            )
            return migrated
        if ambiguous_event is not None or (
            outbox_record_was_missing and pending_event is not None
        ):
            artifact_outbox.claim(delivery_key, claimed_at=self._now())
            migrated = artifact_outbox.mark_ambiguous(delivery_key)
            await self._publisher.emit(
                event_key=f"report-artifact-state-rebound:{artifact_sha256}",
                event_type="REPORT_ARTIFACT_JOURNAL_STATE_REBOUND",
                phase=PaperDayPhase.POST_CLOSE,
                severity=PaperDaySeverity.WARNING,
                payload={
                    "artifact_sha256": artifact_sha256,
                    "previous_journal_status": (
                        ReportArtifactStatus.AMBIGUOUS.value
                        if ambiguous_event is not None
                        else ReportArtifactStatus.PENDING.value
                    ),
                    "report_name": report_name,
                    "status": migrated.status.value,
                    "upload_attempted": False,
                },
            )
            return migrated
        if (
            legacy is not None
            and legacy.event_type == "REPORT_UPLOAD_FAILED"
            and legacy.payload.get("report_name") == report_name
        ):
            artifact_outbox.claim(delivery_key, claimed_at=self._now())
            migrated = artifact_outbox.mark_ambiguous(delivery_key)
            await self._publisher.emit(
                event_key=f"report-artifact-legacy-failure:{artifact_sha256}",
                event_type="REPORT_ARTIFACT_LEGACY_FAILURE_AMBIGUOUS",
                phase=PaperDayPhase.POST_CLOSE,
                severity=PaperDaySeverity.WARNING,
                payload={
                    "artifact_sha256": artifact_sha256,
                    "report_name": report_name,
                    "status": migrated.status.value,
                    "upload_attempted": False,
                },
            )
            return migrated
        return delivery

    async def _apply_report_artifact_recovery(
        self,
        *,
        delivery: ReportArtifactRecord,
        delivery_key: str,
        artifact_sha256: str,
        report_name: str,
    ) -> ReportArtifactRecord:
        """只执行已持久授权的歧义处理；没有授权时保持失败关闭。"""

        artifact_outbox = self._artifact_outbox
        if artifact_outbox is None:  # pragma: no cover - 由调用边界保证
            raise ReportArtifactOutboxError("REPORT_ARTIFACT_OUTBOX_NOT_CONFIGURED")
        authorizations = tuple(
            event
            for event in self._store.events(self._manifest.run_id)
            if event.event_type == "REPORT_ARTIFACT_RECOVERY_AUTHORIZED"
            and event.payload.get("artifact_sha256") == artifact_sha256
        )
        configured_action = self._report_artifact_recovery_action
        resend_consumed_key = f"report-artifact-recovery-used:{artifact_sha256}:resend"
        resend_consumed = self._store.event_by_key(
            self._manifest.run_id,
            resend_consumed_key,
        )
        authorization = None
        if configured_action is not None:
            authorization = next(
                (
                    event
                    for event in reversed(authorizations)
                    if event.payload.get("recovery_action") == configured_action
                ),
                None,
            )
            if authorization is None:
                # 未消费的旧授权不能被相反动作静默覆盖。已经消费过的一次性 RESEND
                # 可以由后续人工核验收件后的 MARK_SENT 收敛。
                has_unconsumed_conflict = any(
                    event.payload.get("recovery_action") == PAPER_REPORT_ARTIFACT_MARK_SENT
                    or (
                        event.payload.get("recovery_action") == PAPER_REPORT_ARTIFACT_RESEND
                        and resend_consumed is None
                    )
                    for event in authorizations
                )
                if has_unconsumed_conflict:
                    return delivery
        else:
            authorization = next(
                (
                    event
                    for event in reversed(authorizations)
                    if event.payload.get("recovery_action")
                    == PAPER_REPORT_ARTIFACT_MARK_SENT
                    or (
                        event.payload.get("recovery_action")
                        == PAPER_REPORT_ARTIFACT_RESEND
                        and resend_consumed is None
                    )
                ),
                None,
            )

        if authorization is not None:
            retained_action = authorization.payload.get("recovery_action")
            if not isinstance(retained_action, str):
                return delivery
            action = retained_action
            provider_identifier = authorization.payload.get("provider_identifier")
            if (
                configured_action == PAPER_REPORT_ARTIFACT_MARK_SENT
                and self._report_artifact_provider_identifier != provider_identifier
            ):
                return delivery
        elif configured_action is not None:
            action = configured_action
            provider_identifier = self._report_artifact_provider_identifier
            action_key = (
                "mark-sent"
                if action == PAPER_REPORT_ARTIFACT_MARK_SENT
                else "resend"
            )
            await self._publisher.emit(
                event_key=f"report-artifact-recovery:{artifact_sha256}:{action_key}",
                event_type="REPORT_ARTIFACT_RECOVERY_AUTHORIZED",
                phase=PaperDayPhase.POST_CLOSE,
                severity=PaperDaySeverity.WARNING,
                payload={
                    "artifact_sha256": artifact_sha256,
                    "operator_confirmed": True,
                    "provider_identifier": provider_identifier,
                    "recovery_action": action,
                    "report_name": report_name,
                },
            )
        else:
            return delivery

        if action == PAPER_REPORT_ARTIFACT_MARK_SENT:
            if not isinstance(provider_identifier, str) or not provider_identifier.strip():
                return delivery
            return artifact_outbox.resolve_ambiguous_as_sent(
                delivery_key,
                provider_identifier=provider_identifier,
                resolved_at=self._now(),
            )
        if action == PAPER_REPORT_ARTIFACT_RESEND:
            if resend_consumed is not None:
                return delivery
            requeued = artifact_outbox.requeue_ambiguous(delivery_key)
            # 重新入队后、调用提供方前消费一次性授权。若两次持久化之间崩溃，
            # 启动恢复会依据原歧义事件把 PENDING 重新隔离，绝不会自动上传；若
            # 消费事件写入后崩溃，则同样再次要求人工核验，不能复用旧授权。
            await self._publisher.emit(
                event_key=resend_consumed_key,
                event_type="REPORT_ARTIFACT_RECOVERY_CONSUMED",
                phase=PaperDayPhase.POST_CLOSE,
                severity=PaperDaySeverity.WARNING,
                payload={
                    "artifact_sha256": artifact_sha256,
                    "recovery_action": action,
                    "report_name": report_name,
                    "single_use": True,
                },
            )
            return requeued
        return delivery

    async def _emit_report_artifact_state(
        self,
        *,
        report_name: str,
        artifact_sha256: str | None,
        status: str,
        error_code: str | None = None,
        provider_identifier: str | None = None,
    ) -> None:
        stable_identity = artifact_sha256 or hashlib.sha256(report_name.encode("utf-8")).hexdigest()
        normalized_status = status.lower().replace("_", "-")
        event_type = {
            ReportArtifactStatus.PENDING.value: "REPORT_ARTIFACT_DELIVERY_PENDING",
            ReportArtifactStatus.SENT.value: "REPORT_ARTIFACT_DELIVERY_SENT",
            ReportArtifactStatus.AMBIGUOUS.value: "REPORT_ARTIFACT_DELIVERY_AMBIGUOUS",
            "NOT_CONFIGURED": "REPORT_ARTIFACT_DELIVERY_NOT_CONFIGURED",
        }.get(status, "REPORT_ARTIFACT_DELIVERY_STATE_CHANGED")
        await self._publisher.emit(
            event_key=f"report-artifact-{normalized_status}:{stable_identity}",
            event_type=event_type,
            phase=PaperDayPhase.POST_CLOSE,
            severity=(
                PaperDaySeverity.INFO
                if status == ReportArtifactStatus.SENT.value
                else PaperDaySeverity.WARNING
            ),
            payload={
                "artifact_sha256": artifact_sha256,
                "delivered": status == ReportArtifactStatus.SENT.value,
                "error_code": error_code,
                "provider_identifier_sha256": (
                    None
                    if provider_identifier is None
                    else hashlib.sha256(provider_identifier.encode("utf-8")).hexdigest()
                ),
                "report_kind": ReportKind.DAILY_REVIEW.value,
                "report_name": report_name,
                "status": status,
            },
        )
        text_required, text_sent, text_gaps = self._notification_counts()
        artifact_sent = status == ReportArtifactStatus.SENT.value
        self._publisher.write_delivery_projection(
            artifact_delivery_status=status,
            artifact_delivery_complete=artifact_sent,
            daily_review_delivery_complete=artifact_sent and text_gaps == 0,
            notification_required=text_required + 1,
            notification_sent=text_sent + int(artifact_sent),
            notification_gaps=text_gaps + (0 if artifact_sent else 1),
            text_notification_required=text_required,
            text_notification_sent=text_sent,
            text_notification_gaps=text_gaps,
        )

    def _notification_counts(self) -> tuple[int, int, int]:
        events = self._store.events(self._manifest.run_id)
        notification_events = tuple(item for item in events if item.notification_required)
        required = len(notification_events)
        statuses = tuple(
            item.status
            for event in notification_events
            if (item := self._outbox.get_by_key(self._publisher.notification_key(event)))
            is not None
        )
        sent = sum(status is OutboxStatus.SENT for status in statuses)
        return required, sent, max(0, required - sent)

    def _render_report(self, required: int, sent: int, gaps: int) -> str:
        events = self._store.events(self._manifest.run_id)
        snapshot = self._paper.snapshot(self._manifest.account_id)
        fills = self._paper.fills(self._manifest.account_id)
        rows = [
            "# A股模拟盘全天运行报告",
            "",
            f"> 报告类型：{report_contract(ReportKind.DAILY_REVIEW).chinese_name}",
            "> 权威边界：本报告是不可变交易事件与 PAPER 账本的可读投影。",
            "",
            "## 执行摘要",
            "",
            f"- 交易日：{self._manifest.session_date.isoformat()}",
            f"- Run ID：`{self._manifest.run_id}`",
            f"- 策略：`{self._config.strategy_version}`",
            f"- 会话覆盖：{'PARTIAL_SESSION' if self._partial_session else 'FULL_SESSION'}",
            f"- 初始资金：{self._manifest.initial_cash:.2f} 元",
            "- 执行边界：仅本地 PAPER；从未连接或调用真实券商下单接口",
            "- 撮合边界：完成分钟线产生信号，仅使用信号可知后才开始的"
            "首个完整 1 分钟区间做单次 IOC；非盘口仿真",
            "",
            "## 市场复盘",
            "",
            f"- 不可变事件总数：{len(events)}。",
            "- 全市场扫描、关注名单、技术信号及数据源状态均按事件可知时点留痕；"
            "详细证据见本报告末尾时间线。",
            "- 公开网页行情不等同于交易所可执行盘口，任何降级源均不得静默提高入场权限。",
            "",
            "## 操作复盘",
            "",
            "### 最终账户",
            "",
            f"- 现金：{snapshot.cash:.2f} 元",
            f"- 成交笔数：{len(fills)}",
            f"- 必推事件：{required}；已发送：{sent}；投递缺口：{gaps}",
            "",
            "| 标的 | 数量 | 当日买入 | 可卖 | 均价 | 最新留存价 |",
            "|---|---:|---:|---:|---:|---:|",
        ]
        for position in snapshot.positions:
            if position.quantity <= 0:
                continue
            mark = self._last_prices.get(position.symbol)
            rows.append(
                f"| {position.symbol} | {position.quantity} | {position.today_buy} | "
                f"{position.available_to_sell} | {position.average_cost:.3f} | "
                f"{_decimal_display(mark)} |"
            )
        rows.extend(("", "## 持仓深研", "", "### 持仓退出计划", ""))
        rows.extend(
            (
                "| 标的 | 深度 | 状态 | 确定止损 | 趋势目标 | 时间门槛 |",
                "|---|---|---|---:|---:|---|",
            )
        )
        planned_symbols: set[str] = set()
        if self._exit_plan_lifecycle is not None:
            for symbol, protection_id in sorted(self._exit_protection_by_symbol.items()):
                held_position = snapshot.position(symbol)
                if held_position is None or held_position.quantity <= 0:
                    continue
                try:
                    plan = self._exit_plan_lifecycle.active_plan(protection_id)
                except Exception:
                    continue
                planned_symbols.add(symbol)
                rows.append(
                    f"| {symbol} | {plan.depth.value} | {plan.state.value} | "
                    f"{plan.stop_price:.3f} | {plan.take_profit_price:.3f} | "
                    f"{plan.time_exit_at.astimezone(SHANGHAI):%Y-%m-%d %H:%M} |"
                )
        unplanned = tuple(
            item.symbol
            for item in snapshot.positions
            if item.quantity > 0 and item.symbol not in planned_symbols
        )
        if unplanned:
            rows.append("")
            rows.append(
                "未绑定退出计划的历史持仓：" + "、".join(unplanned) + "。"
                "这些标的不会被伪装为已持续监控。"
            )
        rows.extend(("", "## 成交与费用", ""))
        if not fills:
            rows.append("当日没有满足全部双门信号、风险和下一分钟撮合条件的成交。")
        else:
            rows.extend(
                (
                    "| 时间 | 标的 | 方向 | 数量 | 价格 | 佣金 | 过户费 | 印花税 |",
                    "|---|---|---|---:|---:|---:|---:|---:|",
                )
            )
            for item in fills:
                fill = item.fill
                rows.append(
                    f"| {fill.executed_at.astimezone(SHANGHAI):%H:%M:%S} | "
                    f"{fill.symbol} | {fill.side.value} | {fill.quantity} | "
                    f"{fill.price:.3f} | {item.fees.commission:.2f} | "
                    f"{item.fees.transfer_fee:.2f} | {item.fees.stamp_tax:.2f} |"
                )
        held_symbols = "、".join(
            item.symbol for item in snapshot.positions if item.quantity > 0
        ) or "无"
        rows.extend(
            (
                "",
                "## 次日基线",
                "",
                f"- 收盘留存持仓：{held_symbols}。",
                "- 下一交易日开盘前重新核验交易日历、停复牌、价格带、公告和最新退出计划；"
                "不得把今日信号直接复用为次日订单。",
                "- 逐标的 LLM 双轨深研由盘后编排独立生成；未生成时不得声称已经完成次日复核。",
            )
        )
        rows.extend(("", "## 不可变事件时间线", ""))
        for event in events:
            local = event.known_at.astimezone(SHANGHAI)
            symbol = "" if event.symbol is None else f" [{event.symbol}]"
            rows.append(f"### {event.sequence}. {local:%H:%M:%S} {event.event_type}{symbol}")
            rows.append("")
            rows.append(
                f"阶段 `{event.phase.value}`；级别 `{event.severity.value}`；"
                f"事件 `{event.event_id}`。"
            )
            rows.append("")
            rows.append("```json")
            rows.append(json.dumps(event.payload, ensure_ascii=False, sort_keys=True))
            rows.append("```")
            rows.append("")
        rows.extend(
            (
                "## 真实性与限制",
                "",
                "- 数据来自公开网页聚合源，不是交易所 tick/L1/L2 或可执行报价。",
                "- 盘前网页快照以今晨首次抓取时间作为 available_at，未倒填为昨日已知。",
                "- 降级分钟源只有在独立全市场快照价格交叉核对后才可通过入场数据门。",
                "- 同日卖出信号全部留存；本次按约定不提交任何卖单。",
                "- QQ outbox 是至少一次投递；极端崩溃窗口可能产生可识别的重复消息。",
                "",
            )
        )
        rendered = "\n".join(rows).rstrip() + "\n"
        validate_markdown_report_contract(ReportKind.DAILY_REVIEW, rendered)
        return rendered


def _paper_day_event_has_empty_candidates(event: PaperDayEvent) -> bool:
    """仅当结果明确为空且内部一致时返回 true。"""

    candidates = event.payload.get("candidates")
    count = event.payload.get("candidate_count")
    return (
        isinstance(candidates, list)
        and not candidates
        and isinstance(count, int)
        and not isinstance(count, bool)
        and count == 0
    )


def _terminal_abort_requiring_recovery(
    events: tuple[PaperDayEvent, ...],
) -> PaperDayEvent | None:
    """返回仅可由操作员跨越的日志头中止事件。

    先前中止后若已有持久化恢复边界，就属于历史事件，不应阻止常规崩溃重启。
    此处只有当前处于终态的日志头需要处理。
    """

    if not events:
        return None
    latest = events[-1]
    if latest.event_type != "DAY_ABORTED":
        return None
    if latest.phase is not PaperDayPhase.TERMINAL:
        raise RuntimeError("DAY_ABORTED journal head is not terminal")
    return latest


def _requested_risk_policy_migration(
    *,
    events: tuple[PaperDayEvent, ...],
    manifest: PaperDayRunManifest,
    new_policy: Mapping[str, object],
    confirmation: str | None,
) -> _RiskPolicyMigration | None:
    """验证精确且已显式授权的运行时策略变更。"""

    baseline = _latest_risk_policy_baseline(events, manifest)
    if baseline is None:
        reconstructed = _reconstruct_legacy_cli_risk_policy(manifest)
        if reconstructed is not None:
            baseline = (
                None,
                "LEGACY_CLI_DEFAULTS_RECONSTRUCTED",
                reconstructed,
            )
        else:
            # 不得把缺少可审计策略基线的留存运行视为通配状态。
            # 显式确认也不能让未知旧策略变得可以安全重解释。
            raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_BASELINE_MISSING")
    source_event, baseline_source, previous_policy = baseline
    if previous_policy == dict(new_policy):
        if confirmation is not None:
            if _matching_risk_policy_migration_exists(events, new_policy):
                return None
            raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_NOT_REQUIRED")
        return None
    if confirmation != PAPER_RISK_POLICY_CHANGE_CONFIRMATION:
        raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_CONFIRMATION_REQUIRED")
    reason_codes = _allowed_risk_policy_change_reasons(
        previous_policy,
        new_policy,
    )
    if reason_codes is None:
        raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_NOT_ALLOWED")
    return _RiskPolicyMigration(
        previous_policy=previous_policy,
        new_policy=dict(new_policy),
        reason_codes=reason_codes,
        policy_diff=_risk_policy_diff(previous_policy, new_policy),
        baseline_source=baseline_source,
        source_event=source_event,
    )


def _latest_risk_policy_baseline(
    events: tuple[PaperDayEvent, ...],
    manifest: PaperDayRunManifest,
) -> tuple[PaperDayEvent | None, str, dict[str, object]] | None:
    for event in reversed(events):
        value = event.payload.get("risk_policy")
        if isinstance(value, dict):
            return event, "JOURNAL_EVENT", dict(value)
    value = manifest.config.get("intraday_risk_policy")
    if isinstance(value, dict):
        return None, "RUN_MANIFEST", dict(value)
    return None


def _reconstruct_legacy_cli_risk_policy(
    manifest: PaperDayRunManifest,
) -> dict[str, object] | None:
    """仅重建旧版 CLI 中固定且不可配置的策略。"""

    config = manifest.config
    runner_config = ASharePaperDayConfig(initial_cash=manifest.initial_cash).audit_document()
    runtime_keys = {
        "calendar_provider",
        "calendar_verified",
        "latest_completed_session",
        "notification_channel",
        "notification_preflight_policy",
        "notification_preflight_required",
        "notification_target_kind",
    }
    latest_completed = config.get("latest_completed_session")
    target_kind = config.get("notification_target_kind")
    if (
        set(config) != set(runner_config) | runtime_keys
        or any(config.get(key) != value for key, value in runner_config.items())
        or config.get("calendar_provider") != "BaoStock"
        or config.get("calendar_verified") is not True
        or config.get("notification_channel") != "onebot"
        or config.get("notification_preflight_policy") != "GET_STATUS_GOOD_AND_ONLINE"
        or config.get("notification_preflight_required") is not True
        or target_kind not in {"private", "group"}
        or not isinstance(latest_completed, str)
    ):
        return None
    try:
        latest_completed_date = date.fromisoformat(latest_completed)
    except ValueError:
        return None
    if latest_completed_date >= manifest.session_date:
        return None
    return {
        "initial_equity": str(manifest.initial_cash),
        "cash_reserve_fraction": "0.20",
        "maximum_gross_fraction": "0.80",
        "maximum_positions": 5,
        "maximum_symbol_fraction": "0.20",
        "risk_per_trade_fraction": "0.0075",
        "minimum_stop_fraction": "0.015",
        "stock_lot_size": 100,
        "volume_participation_rate": "0.01",
        "buy_limit_markup": "0.001",
        "stock_slippage_rate": "0.0005",
        "etf_slippage_rate": "0.0002",
        "stock_price_quantum": "0.01",
        "etf_price_quantum": "0.001",
        "latest_entry_time": "14:55:00",
        "execution_policy": "NEXT_FULLY_POST_SIGNAL_1M_INTERVAL_IOC",
    }


def _matching_risk_policy_migration_exists(
    events: tuple[PaperDayEvent, ...],
    new_policy: Mapping[str, object],
) -> bool:
    expected_sha256 = _document_sha256(new_policy)
    expected_reasons = (
        _REMOVE_POSITION_COUNT_CAP,
        _ADD_PRICE_ACCEPTANCE_BOUNDS,
        _ADD_BOARD_QUANTITY_RULES,
    )
    for event in reversed(events):
        if event.event_type != "OPERATOR_RISK_POLICY_CHANGED":
            continue
        payload = event.payload
        reasons = payload.get("reason_codes")
        previous_policy = payload.get("old_risk_policy")
        retained_new_policy = payload.get("new_risk_policy")
        return (
            payload.get("operator_authorized") is True
            and payload.get("authorization_code") == PAPER_RISK_POLICY_CHANGE_CONFIRMATION
            and payload.get("new_risk_policy_sha256") == expected_sha256
            and isinstance(previous_policy, dict)
            and payload.get("old_risk_policy_sha256") == _document_sha256(previous_policy)
            and retained_new_policy == dict(new_policy)
            and payload.get("risk_policy") == dict(new_policy)
            and payload.get("risk_policy_sha256") == expected_sha256
            and isinstance(reasons, list)
            and all(isinstance(item, str) for item in reasons)
            and tuple(reasons) == expected_reasons
            and _allowed_risk_policy_change_reasons(
                previous_policy,
                new_policy,
            )
            == expected_reasons
        )
    return False


def _allowed_risk_policy_change_reasons(
    previous_policy: Mapping[str, object],
    new_policy: Mapping[str, object],
) -> tuple[str, ...] | None:
    """仅允许今日已复核的三项变更，拒绝其他所有差异。"""

    missing = object()
    previous = dict(previous_policy)
    current = dict(new_policy)

    previous_maximum = previous.pop("maximum_positions", missing)
    current_maximum = current.pop("maximum_positions", missing)
    previous_count_enabled = previous.pop(
        "position_count_limit_enabled",
        missing,
    )
    current_count_enabled = current.pop(
        "position_count_limit_enabled",
        missing,
    )
    if not (
        previous_maximum == 5
        and current_maximum is None
        and (previous_count_enabled is missing or previous_count_enabled is True)
        and current_count_enabled is False
    ):
        return None

    previous_sell_markdown = previous.pop("sell_limit_markdown", missing)
    current_sell_markdown = current.pop("sell_limit_markdown", missing)
    previous_price_policy = previous.pop("price_acceptance_policy", missing)
    current_price_policy = current.pop("price_acceptance_policy", missing)
    expected_price_policy = (
        IntradayPaperRiskConfig().audit_document().get("price_acceptance_policy")
    )
    if not (
        (previous_sell_markdown is missing or previous_sell_markdown == "0.001")
        and current_sell_markdown == "0.001"
        and previous_price_policy is missing
        and isinstance(current_price_policy, dict)
        and current_price_policy == expected_price_policy
    ):
        return None
    previous_quantity_policy = previous.pop("order_quantity_policy", missing)
    current_quantity_policy = current.pop("order_quantity_policy", missing)
    expected_quantity_policy = (
        IntradayPaperRiskConfig().audit_document().get("order_quantity_policy")
    )
    if not (
        previous_quantity_policy is missing
        and isinstance(current_quantity_policy, dict)
        and current_quantity_policy == expected_quantity_policy
    ):
        return None
    if previous != current:
        return None
    return (
        _REMOVE_POSITION_COUNT_CAP,
        _ADD_PRICE_ACCEPTANCE_BOUNDS,
        _ADD_BOARD_QUANTITY_RULES,
    )


def _risk_policy_diff(
    previous_policy: Mapping[str, object],
    new_policy: Mapping[str, object],
) -> tuple[Mapping[str, object], ...]:
    changes: list[Mapping[str, object]] = []
    for field in sorted(set(previous_policy) | set(new_policy)):
        old_present = field in previous_policy
        new_present = field in new_policy
        old_value = previous_policy.get(field)
        new_value = new_policy.get(field)
        if old_present == new_present and old_value == new_value:
            continue
        changes.append(
            {
                "field": field,
                "new_present": new_present,
                "new_value": new_value,
                "old_present": old_present,
                "old_value": old_value,
            }
        )
    return tuple(changes)


def _incomplete_fill_ids(events: tuple[PaperDayEvent, ...]) -> tuple[str, ...]:
    applied = {
        item.correlation_id
        for item in events
        if item.event_type == "FILL_APPLIED" and item.correlation_id is not None
    }
    started = {
        item.correlation_id
        for item in events
        if item.event_type == "FILL_STARTED" and item.correlation_id is not None
    }
    terminal_match_fills: set[str] = set()
    for event in events:
        if event.event_type != "ORDER_MATCH_EVALUATED":
            continue
        fill = event.payload.get("fill")
        fill_id = fill.get("fill_id") if isinstance(fill, dict) else None
        if isinstance(fill_id, str):
            terminal_match_fills.add(fill_id)
    return tuple(sorted((started | terminal_match_fills) - applied))


def _validate_risk_policy_migration_state(
    *,
    pending: Mapping[str, IntradayPaperOrder],
    events: tuple[PaperDayEvent, ...],
) -> None:
    """拒绝重解释任何未结委托或未完成的成交 saga。"""

    if pending:
        raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_PENDING_ORDER")
    if _incomplete_fill_ids(events):
        raise PaperDayRiskPolicyChangeError("RISK_POLICY_CHANGE_INCOMPLETE_FILL")


def _price_acceptance_document(
    value: IntradayPriceAcceptance | None,
) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "acceptable_lower_inclusive": value.acceptable_lower,
        "acceptable_upper_inclusive": value.acceptable_upper,
        "board": value.board.value,
        "exchange_lower": value.exchange_lower,
        "exchange_upper": value.exchange_upper,
        "exchange_band_regime": "CONSERVATIVE_STANDARD_NON_ST_BOARD_BAND",
        "continuous_auction_price_cage_status": ("NOT_L1_VERIFIED_PAPER_MINUTE_BAR_ONLY"),
        "real_broker_submission_allowed": False,
        "invalidation_boundary_exclusive": value.invalidation_price,
        "limit_price": value.limit_price,
        "policy_version": value.policy_version,
        "price_tick": value.price_tick,
        "reference_price": value.reference_price,
        "side": value.side.value,
    }


def _quantity_rule_document(
    value: IntradayOrderQuantityRule | None,
) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "board": value.board.value,
        "buy_increment": value.buy_increment,
        "limit_order_type": "LIMIT",
        "maximum_limit_order_quantity": value.maximum_limit_order_quantity,
        "minimum_buy_quantity": value.minimum_buy_quantity,
        "minimum_regular_sell_quantity": value.minimum_regular_sell_quantity,
        "paper_partial_fill_increment": value.paper_partial_fill_increment,
        "policy_version": value.policy_version,
        "sell_increment": value.sell_increment,
        "sell_residual_policy": value.sell_residual_policy,
    }


def _sell_quantity_plan_document(
    value: IntradaySellQuantityPlan | None,
) -> dict[str, object] | None:
    if value is None:
        return None
    order_sequence = [*value.regular_order_quantities]
    if value.residual_sell_all_quantity > 0:
        order_sequence.append(value.residual_sell_all_quantity)
    return {
        "available_to_sell": value.available_to_sell,
        "future_limit_order_sequence": order_sequence,
        "quantity_rule": _quantity_rule_document(value.rule),
        "regular_order_quantities": list(value.regular_order_quantities),
        "residual_component_quantity": value.residual_component_quantity,
        "residual_must_be_sold_all_once": value.residual_component_quantity > 0,
        "residual_sell_all_quantity": value.residual_sell_all_quantity,
        "status": value.status.value,
    }


def _sell_quantity_plan_text(value: IntradaySellQuantityPlan | None) -> str:
    if value is None:
        return "卖出数量规则：板块不受当前 PAPER 执行模型支持，未来不得创建卖单。"
    rule = value.rule
    if value.status is IntradaySellQuantityStatus.NO_SELLABLE_QUANTITY:
        return "卖出数量规则：当前无 T+1 可卖数量；今日买入部分不得当日卖出。"
    sequence = "、".join(
        str(item)
        for item in (
            *value.regular_order_quantities,
            *((value.residual_sell_all_quantity,) if value.residual_sell_all_quantity > 0 else ()),
        )
    )
    rule_text = (
        f"卖出数量规则：常规限价卖单最少{rule.minimum_regular_sell_quantity}股，"
        f"其后按{rule.sell_increment}股递增，单笔最多"
        f"{rule.maximum_limit_order_quantity}股；未来申报序列为{sequence}股。"
    )
    if value.residual_component_quantity == 0:
        return rule_text
    return (
        f"{rule_text} 末笔{value.residual_sell_all_quantity}股包含"
        f"{value.residual_component_quantity}股余股，必须作为届时全部余额一次性卖出，"
        "不得拆成多笔余股申报。"
    )


def _deep_exit_llm_assessment_text(value: Mapping[str, object]) -> str:
    """保持旧模块路径，委托给纯投影模块。"""
    return _paper_day_projection.deep_exit_llm_assessment_text(value)


def _deep_exit_sell_review(
    *,
    technical_score: Decimal,
    assessment: Mapping[str, object],
) -> dict[str, object]:
    """保持旧模块路径，委托给纯投影模块。"""
    return _paper_day_projection.deep_exit_sell_review(
        technical_score=technical_score, assessment=assessment
    )


def _deep_exit_sell_review_text(value: Mapping[str, object]) -> str:
    """保持旧模块路径，委托给纯投影模块。"""
    return _paper_day_projection.deep_exit_sell_review_text(value)


























_ENTRY_REJECTION_EXPLANATIONS: Mapping[str, str] = {
    "NO_CURRENT_SESSION_ANOMALY": (
        "最近一次全市场扫描没有把该标的列为当日异动候选；只有单股分钟技术形态，缺少横截面确认"
    ),
    "ANOMALY_NOT_MOMENTUM_EXPANSION": ("标的虽在异动名单中，但尚未达到动量扩张级别"),
    "NO_CURRENT_SESSION_SCAN": "尚无本交易日全市场扫描结果",
    "CURRENT_SESSION_SCAN_NOT_COMPLETE": "最近一次全市场扫描数据不完整或处于降级状态",
    "CURRENT_SESSION_ANOMALY_EXPIRED": "最近一次异动确认已经超过有效期",
    "SYMBOL_ALREADY_ENTERED_TODAY": "该标的今日已经模拟买入，不重复加仓",
    "SYMBOL_ORDER_ALREADY_PENDING": "该标的已有待撮合 PAPER 委托",
    "PENDING_CAPACITY_RESERVED": "已有另一笔待撮合委托占用保守资金容量",
    "SYMBOL_ALREADY_HELD": "账户已经持有该标的，今日策略禁止重复加仓",
    "MINUTE_DATA_STALE": "最新完成分钟线已经过期",
    "UNAPPROVED_DEGRADED_MINUTE_PROVIDER": "分钟行情来自未获准的降级数据源",
    "DEGRADED_SOURCE_NOT_INDEPENDENTLY_CORROBORATED": (
        "降级分钟行情没有得到独立全市场快照交叉确认"
    ),
    "CORROBORATION_PRICE_MISSING": "交叉确认快照缺少有效价格",
    "CROSS_SOURCE_PRICE_DIVERGENCE": "分钟行情与全市场快照价格偏差超过容许范围",
    "MINUTE_FRESHNESS_NOT_CURRENT": "分钟行情的新鲜度状态不是当前可用",
    "SIGNAL_NOT_ENTER_CANDIDATE": "技术结论不是可入场候选",
    "SIGNAL_PRICE_MISSING": "技术信号缺少有效参考价格",
    "INVALIDATION_PRICE_MISSING": "技术信号缺少失效参考价格",
    "INVALIDATION_NOT_BELOW_ENTRY": "失效参考价格没有低于拟入场价格",
    "SIGNAL_NOT_COMPLETED": "信号所用分钟线尚未完整收盘",
    "SIGNAL_SESSION_MISMATCH": "信号不属于当前交易日",
    "ACCOUNT_SESSION_MISMATCH": "PAPER 账户交易日尚未对齐",
    "ENTRY_CUTOFF_PASSED": "已超过当日允许新建买入委托的最晚时间",
    "BOARD_MISSING": "缺少可验证的上市板块信息",
    "BOARD_UNSUPPORTED": "当前盘中执行模型不支持该上市板块",
    "BOARD_SYMBOL_MISMATCH": "证券代码与上市板块不一致",
    "PREVIOUS_CLOSE_MISSING": "缺少可验证的上一交易日收盘价",
    "PRICE_OUTSIDE_DAILY_BAND": "信号价格超出当日价格限制范围",
    "LIMIT_OUTSIDE_DAILY_BAND": "拟定限价超出当日价格限制范围",
    "PROTECTIVE_STOP_INVALID": "成交前保护止损无效",
    "QUICK_EXIT_PLAN_UNAVAILABLE": "无法用当前时点数据形成成交前 QUICK 退出计划",
    "QUICK_EXIT_CONDITION_ALREADY_MET": "买入前已经触及 QUICK 退出条件，拒绝建立新仓",
    "MAX_POSITIONS_REACHED": "显式启用的持仓数量熔断器已达到上限",
    "GROSS_LIMIT_REACHED": "组合总敞口已达到上限",
    "SYMBOL_LIMIT_REACHED": "单一标的资金上限已达到",
    "CASH_RESERVE_BINDING": "执行后会侵占最低现金储备",
    "BELOW_ROUND_LOT": "按风险与资金约束计算的数量不足一手",
    "BELOW_BOARD_MINIMUM_BUY": ("按风险、资金、费用与板块申报规则测算后不足最低买入数量"),
    "LLM_PREOPEN_CONTEXT_UNAVAILABLE": "盘前宏观基线或原证据快照不可用",
    "LLM_REVIEW_NOT_READY": "候选复核尚未完成；买入路径不会等待模型或网络",
    "LLM_REVIEW_NOT_JOURNALED": "模型结果尚未先写入不可变日志，不能用于交易",
    "LLM_REVIEW_EXPIRED": "候选复核已超过冻结有效期",
    "LLM_REVIEW_INPUT_MISMATCH": "复核结果与当前候选或扫描证据作用域不一致",
    "LLM_REVIEW_KNOWN_AFTER_SIGNAL": "复核结果晚于技术信号可知，禁止回看使用",
    "LLM_MODEL_IDENTITY_MISMATCH": "请求模型、响应模型或提示词合约身份不一致",
    "LLM_REVIEW_FAILED": "候选模型复核失败",
    "LLM_REVIEW_ABSTAINED": "候选模型复核明确弃权",
    "LLM_EVIDENCE_INVALID": "复核证据覆盖不足或引用无效",
    "LLM_NEGATIVE_VETO": "宏观复核达到负面否决阈值",
    "LLM_COMBINED_SCORE_BELOW_ENTRY_THRESHOLD": "技术分与宏观分融合后低于 0.70 入场阈值",
}


def _entry_rejection_display(reason: str | None) -> str:
    if reason is None:
        return "入场门未通过，具体原因不可用"
    return _ENTRY_REJECTION_EXPLANATIONS.get(reason, "入场门未通过")


_MATCH_REASON_EXPLANATIONS: Mapping[str, str] = {
    "EXIT_CONDITION_MET_BEFORE_FILL": "撮合分钟已触及预登记退出条件，无法证明先买入后退出",
    "EXIT_PLAN_MISSING_BEFORE_FILL": "待撮合委托缺少可验证 QUICK 退出计划，已安全终止",
    "SIGNAL_INVALIDATED_BEFORE_FILL": "撮合分钟触及或跌破技术失效位，买入逻辑已失效",
    "LIMIT_NOT_TOUCHED": "撮合分钟没有触及买入限价",
    "BAR_OUTSIDE_DAILY_BAND": "分钟行情超出已验证的交易所日价格带",
    "LOCKED_LIMIT_UP_QUEUE_UNMODELED": "涨停板封死，分钟线无法证明买单排队能够成交",
    "VOLUME_CAPACITY_BELOW_LOT": "按成交量参与上限计算后低于该板块的 PAPER 撮合单位",
    "BAR_VOLUME_ZERO": "撮合分钟没有可验证成交量",
    "ORDER_EXPIRED": "委托已超过当日最晚入场时间",
    "FILL_OUTSIDE_ACCEPTABLE_RANGE": "拟成交价不在冻结的策略可接受区间内",
    "ORDER_QUANTITY_OUTSIDE_BOARD_RULES": ("委托数量不符合对应板块的最低数量、递增单位或单笔上限"),
}


def _match_reason_display(reason: str) -> str:
    return _MATCH_REASON_EXPLANATIONS.get(reason, "未满足单分钟 IOC 成交条件")







__all__ = [
    "ASharePaperDayConfig",
    "ASharePaperDayRunner",
    "PAPER_RISK_POLICY_CHANGE_CONFIRMATION",
    "PaperDayAbortRecoveryRequiredError",
    "PaperDayDeepExitAssessmentProvider",
    "PaperDayEventPublisher",
    "PaperDayIntradayLLMPlanFactory",
    "PaperDayLLMPreopenContext",
    "PaperDayRiskPolicyChangeError",
    "PaperDayResult",
    "PaperDayWatchEntry",
    "intraday_llm_evidence_manifest_document",
    "intraday_llm_manifest_compatible",
    "intraday_llm_manifest_document",
]
