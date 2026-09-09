"""有界、证据可追溯的生产级双轨语义分析。

本模块在同一份冻结证据上并行运行原单分析器与结构化对抗分析器。生产决策
优先采用对抗结果；对抗失败时失败关闭，跨轨结论发生实质冲突时明确降级为
``WATCH``。单分析器结果始终保留，供报告和审计对照，不能反向覆盖生产选择。

The first round is blind: every role sees the same immutable evidence pack and
none of the other role outputs.  Later rounds may receive only a bounded,
canonical JSON rendering of validated peer arguments.  Peer arguments are
explicitly labelled untrusted and can never become evidence references.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from threading import Lock
from time import monotonic

from gribuki_trade.analysis.schemas import (
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
)
from gribuki_trade.ports.llm_analyzer import (
    AnalyzerAuditIdentity,
    AuditableMacroAnalyzer,
    DualTrackMacroAnalysis,
    MacroAnalyzer,
    UsageReportingMacroAnalyzer,
)
from gribuki_trade.services import adversarial_macro_boundaries as _boundaries
from gribuki_trade.services import adversarial_macro_policy as _policy
from gribuki_trade.services import adversarial_macro_serialization as _serialization
from gribuki_trade.services.adversarial_macro_models import (
    _ADAPTER_VERSION,
    _AGGREGATION_VERSION,
    _PEER_ENVELOPE_VERSION,
    _ROLE_CHARTERS,
    AdversarialFeatureMode,
    AdversarialMacroConfig,
    AdversarialMacroDepth,
    AdversarialMacroRole,
    AdversarialMacroRun,
    AdversarialRoleOpinion,
    AdversarialRound,
    AdversarialShadowRecord,
    AdversarialTermination,
    DualTrackAuditSink,
    ShadowObserver,
)
from gribuki_trade.services.adversarial_macro_serialization import (
    _MAX_ROLE_CLAIMS,
    _analysis_document,
    _cross_track_conflict,
    _document_sha256,
    _evidence_pack_sha256,
    _identity_document,
    _prompt_contract_sha256,
    _request_sha256,
)

# facade 继续保留历史私有序列化辅助函数。
_MAX_CLAIM_CHARACTERS = _serialization._MAX_CLAIM_CHARACTERS
_MAX_INVALIDATION_CONDITIONS = _serialization._MAX_INVALIDATION_CONDITIONS
_median = _serialization._median
_single_line = _serialization._single_line
_unique_text = _serialization._unique_text

# 纯策略 helper 仍通过此门面暴露，兼容既有调用方。
_role_output_failure = _policy._role_output_failure
_peer_document = _policy._peer_document
_rounds_are_stable = _policy._rounds_are_stable
_round_signature = _policy._round_signature


def _aggregate_analysis(
    request: MacroAnalysisRequest,
    final_round: AdversarialRound,
    *,
    audit_identity: AnalyzerAuditIdentity,
    config: AdversarialMacroConfig,
) -> MacroAnalysis | None:
    return _policy._aggregate_analysis(
        request,
        final_round,
        audit_identity=audit_identity,
        config=config,
        directional_roles=frozenset(
            {
                AdversarialMacroRole.CATALYST_ADVOCATE.value,
                AdversarialMacroRole.RISK_CHALLENGER.value,
                AdversarialMacroRole.MARKET_REGIME_ANALYST.value,
            }
        ),
    )


_RoleCallResult = _boundaries._RoleCallResult
_SanitizedRoleCallFailure = _boundaries._SanitizedRoleCallFailure


class AdversarialMacroAnalyzer:
    """执行有界角色调用并返回一个保守的 ``MacroAnalysis``。"""

    def __init__(
        self,
        analyzer: MacroAnalyzer,
        *,
        config: AdversarialMacroConfig | None = None,
        analyzer_identity: AnalyzerAuditIdentity | None = None,
    ) -> None:
        self._analyzer = analyzer
        self._config = config or AdversarialMacroConfig.for_depth(AdversarialMacroDepth.FAST)
        discovered = (
            analyzer.audit_identity if isinstance(analyzer, AuditableMacroAnalyzer) else None
        )
        if (
            analyzer_identity is not None
            and discovered is not None
            and analyzer_identity != discovered
        ):
            raise ValueError("explicit analyzer identity does not match wrapped analyzer")
        resolved_identity = analyzer_identity or discovered
        if resolved_identity is None:
            raise ValueError("adversarial analysis requires an auditable wrapped analyzer")
        self._base_identity = resolved_identity
        self._audit_identity = _adversarial_audit_identity(
            self._base_identity,
            self._config,
        )
        self._budget_lock = Lock()
        self._calls_started = 0

    @property
    def config(self) -> AdversarialMacroConfig:
        return self._config

    @property
    def audit_identity(self) -> AnalyzerAuditIdentity:
        return self._audit_identity

    @property
    def calls_started(self) -> int:
        with self._budget_lock:
            return self._calls_started

    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        """实现现有 ``MacroAnalyzer`` 端口。"""

        return (await self.analyze_case(request)).analysis

    async def analyze_case(self, request: MacroAnalysisRequest) -> AdversarialMacroRun:
        """最多执行 ``max_rounds``；任何必要角色失败都失败关闭。"""

        rounds: list[AdversarialRound] = []
        previous_round: AdversarialRound | None = None
        case_calls_started = 0
        case_deadline = monotonic() + self._config.case_timeout.total_seconds()

        for round_number in range(1, self._config.max_rounds + 1):
            call_count = len(self._config.roles)
            if not self._reserve_calls(call_count):
                return self._failed_run(
                    request,
                    tuple(rounds),
                    failure_code="ADVERSARIAL_SESSION_BUDGET_EXHAUSTED",
                    termination=AdversarialTermination.SESSION_BUDGET_EXHAUSTED,
                    calls_started=case_calls_started,
                )
            case_calls_started += call_count

            requests = tuple(
                _role_request(
                    request,
                    role=role,
                    round_number=round_number,
                    previous_round=previous_round,
                    audit_identity=self._audit_identity,
                    protocol_version=self._config.protocol_version,
                )
                for role in self._config.roles
            )
            remaining = case_deadline - monotonic()
            if remaining <= 0:
                return self._failed_run(
                    request,
                    tuple(rounds),
                    failure_code="ADVERSARIAL_CASE_DEADLINE_EXCEEDED",
                    termination=AdversarialTermination.CASE_DEADLINE_EXCEEDED,
                    calls_started=case_calls_started,
                    failed_role_calls=_role_failure_documents(
                        self._config.roles,
                        requests,
                        (),
                        default_failure_code="ADVERSARIAL_CASE_DEADLINE_EXCEEDED",
                    ),
                )
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(
                        *(self._call_role(role_request) for role_request in requests),
                        return_exceptions=True,
                    ),
                    timeout=remaining,
                )
            except TimeoutError:
                return self._failed_run(
                    request,
                    tuple(rounds),
                    failure_code="ADVERSARIAL_CASE_DEADLINE_EXCEEDED",
                    termination=AdversarialTermination.CASE_DEADLINE_EXCEEDED,
                    calls_started=case_calls_started,
                    failed_role_calls=_role_failure_documents(
                        self._config.roles,
                        requests,
                        (),
                        default_failure_code="ADVERSARIAL_CASE_DEADLINE_EXCEEDED",
                    ),
                )

            opinions: list[AdversarialRoleOpinion] = []
            for role, role_request, result in zip(
                self._config.roles,
                requests,
                results,
                strict=True,
            ):
                if isinstance(result, asyncio.CancelledError):
                    raise result
                if isinstance(result, BaseException):
                    return self._failed_run(
                        request,
                        tuple(rounds),
                        failure_code="ADVERSARIAL_ROLE_CALL_FAILED",
                        termination=AdversarialTermination.CRITICAL_FAILURE,
                        calls_started=case_calls_started,
                        failed_role_calls=_role_failure_documents(
                            self._config.roles,
                            requests,
                            results,
                            default_failure_code="ADVERSARIAL_ROLE_CALL_FAILED",
                        ),
                    )
                validation_failure = _role_output_failure(
                    result.analysis,
                    role_request=role_request,
                    original_request=request,
                    base_identity=self._base_identity,
                )
                if validation_failure is not None:
                    return self._failed_run(
                        request,
                        tuple(rounds),
                        failure_code=validation_failure,
                        termination=AdversarialTermination.CRITICAL_FAILURE,
                        calls_started=case_calls_started,
                        failed_role_calls=_role_failure_documents(
                            self._config.roles,
                            requests,
                            results,
                            default_failure_code=validation_failure,
                            rejected_role=role,
                        ),
                    )
                opinions.append(
                    AdversarialRoleOpinion(
                        role=role,
                        round_number=round_number,
                        role_request_sha256=_request_sha256(role_request),
                        analysis=result.analysis,
                        started_at=result.started_at,
                        completed_at=result.completed_at,
                        latency_ms=result.latency_ms,
                        provider_model=result.analysis.model_version,
                        prompt_contract_sha256=_prompt_contract_sha256(role_request),
                        evidence_pack_sha256=_evidence_pack_sha256(role_request),
                        # 通用端口无法保证 provider 回传 token 用量；未知值必须
                        # 明确记录，不能用 0 冒充“没有消耗”。
                        usage=result.usage,
                        termination_reason="ROLE_COMPLETED",
                    )
                )

            current_round = AdversarialRound(round_number, tuple(opinions))
            rounds.append(current_round)
            if (
                previous_round is not None
                and self._config.early_stop_on_stable_consensus
                and _rounds_are_stable(previous_round, current_round)
            ):
                analysis = _aggregate_analysis(
                    request,
                    current_round,
                    audit_identity=self._audit_identity,
                    config=self._config,
                )
                if analysis is None:
                    return self._failed_run(
                        request,
                        tuple(rounds),
                        failure_code="ADVERSARIAL_AGGREGATION_FAILED",
                        termination=AdversarialTermination.CRITICAL_FAILURE,
                        calls_started=case_calls_started,
                    )
                return AdversarialMacroRun(
                    request=request,
                    analysis=analysis,
                    rounds=tuple(rounds),
                    audit_identity=self._audit_identity,
                    termination=AdversarialTermination.STABLE_CONSENSUS,
                    calls_started=case_calls_started,
                    protocol_document=_protocol_document(self._config),
                )
            previous_round = current_round

        assert rounds
        analysis = _aggregate_analysis(
            request,
            rounds[-1],
            audit_identity=self._audit_identity,
            config=self._config,
        )
        if analysis is None:
            return self._failed_run(
                request,
                tuple(rounds),
                failure_code="ADVERSARIAL_AGGREGATION_FAILED",
                termination=AdversarialTermination.CRITICAL_FAILURE,
                calls_started=case_calls_started,
            )
        return AdversarialMacroRun(
            request=request,
            analysis=analysis,
            rounds=tuple(rounds),
            audit_identity=self._audit_identity,
            termination=AdversarialTermination.MAX_ROUNDS_REACHED,
            calls_started=case_calls_started,
            protocol_document=_protocol_document(self._config),
        )

    async def _call_role(self, request: MacroAnalysisRequest) -> _RoleCallResult:
        started_at = datetime.now(UTC)
        started = monotonic()
        try:
            async with asyncio.timeout(self._config.per_role_timeout.total_seconds()):
                return await _call_analyzer_with_usage(self._analyzer, request)
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            code = "ADVERSARIAL_ROLE_TIMEOUT"
        except Exception:
            code = "ADVERSARIAL_ROLE_PROVIDER_FAILED"
        raise _SanitizedRoleCallFailure(
            failure_code=code,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            latency_ms=max(0, int(round((monotonic() - started) * 1_000))),
        ) from None

    def _reserve_calls(self, count: int) -> bool:
        with self._budget_lock:
            maximum = self._config.maximum_calls_per_session
            if maximum is not None and self._calls_started + count > maximum:
                return False
            self._calls_started += count
            return True

    def _failed_run(
        self,
        request: MacroAnalysisRequest,
        rounds: tuple[AdversarialRound, ...],
        *,
        failure_code: str,
        termination: AdversarialTermination,
        calls_started: int,
        failed_role_calls: tuple[Mapping[str, object], ...] = (),
    ) -> AdversarialMacroRun:
        return AdversarialMacroRun(
            request=request,
            analysis=_abstain_analysis(
                request,
                failure_code,
                model_version=self._audit_identity.requested_model,
            ),
            rounds=rounds,
            audit_identity=self._audit_identity,
            termination=termination,
            calls_started=calls_started,
            failure_code=failure_code,
            failed_role_calls=failed_role_calls,
            protocol_document=_protocol_document(self._config),
        )


class ProductionDualTrackMacroAnalyzer:
    """生产双轨入口：并行执行两个分支，并优先采用对抗结果。

    总会话 token/call 预算可以为 ``None``，但单角色、轮次和整个 case 的
    deadline 始终有界。对抗分支失败时返回 ``ABSTAIN``；跨轨实质冲突时把
    对抗分支的 ``PUBLISH`` 明确降级为 ``WATCH``。可选审计 sink 在返回前
    持久化完整对照记录，持久化失败同样失败关闭。
    """

    def __init__(
        self,
        baseline: MacroAnalyzer,
        adversarial: AdversarialMacroAnalyzer,
        *,
        case_timeout: timedelta | None = None,
        audit_sink: DualTrackAuditSink | None = None,
        baseline_identity: AnalyzerAuditIdentity | None = None,
        material_cross_track_disagreement: Decimal = Decimal("0.60"),
    ) -> None:
        discovered = (
            baseline.audit_identity if isinstance(baseline, AuditableMacroAnalyzer) else None
        )
        if (
            baseline_identity is not None
            and discovered is not None
            and baseline_identity != discovered
        ):
            raise ValueError("explicit baseline identity does not match baseline analyzer")
        resolved_identity = baseline_identity or discovered
        if resolved_identity is None:
            raise ValueError("production dual-track analysis requires an auditable baseline")
        resolved_timeout = case_timeout or adversarial.config.case_timeout
        if resolved_timeout <= timedelta(0):
            raise ValueError("case_timeout must be positive")
        if not material_cross_track_disagreement.is_finite() or not (
            Decimal("0") <= material_cross_track_disagreement <= Decimal("2")
        ):
            raise ValueError("material_cross_track_disagreement must be in [0, 2]")
        self._baseline = baseline
        self._baseline_identity = resolved_identity
        self._adversarial = adversarial
        self._case_timeout = resolved_timeout
        self._audit_sink = audit_sink
        self._material_cross_track_disagreement = material_cross_track_disagreement

    @property
    def audit_identity(self) -> AnalyzerAuditIdentity:
        """生产选择身份与对抗分支严格一致，供现有 PIT 清单继续校验。"""

        return self._adversarial.audit_identity

    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        return (await self.analyze_dual(request)).selected_analysis

    async def analyze_dual(
        self,
        request: MacroAnalysisRequest,
    ) -> DualTrackMacroAnalysis:
        started_at = datetime.now(UTC)
        started = monotonic()
        baseline_task = asyncio.create_task(
            _call_analyzer_with_usage(self._baseline, request),
            name=f"llm-baseline:{request.analysis_id[-12:]}",
        )
        adversarial_task = asyncio.create_task(
            self._adversarial.analyze_case(request),
            name=f"llm-adversarial:{request.analysis_id[-12:]}",
        )
        tasks = (baseline_task, adversarial_task)
        try:
            _done, pending = await asyncio.wait(
                tasks,
                timeout=self._case_timeout.total_seconds(),
            )
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        for pending_task in pending:
            pending_task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        baseline_failure: str | None = None
        baseline_call: _RoleCallResult | None = None
        if not baseline_task.done() or baseline_task.cancelled():
            baseline_failure = "DUAL_TRACK_BASELINE_DEADLINE_EXCEEDED"
            baseline_analysis = _abstain_analysis(
                request,
                baseline_failure,
                model_version=self._baseline_identity.requested_model,
            )
        else:
            try:
                baseline_call = baseline_task.result()
                baseline_analysis = baseline_call.analysis
                baseline_analysis.validate_against(request)
            except Exception:
                baseline_failure = "DUAL_TRACK_BASELINE_FAILED"
                baseline_analysis = _abstain_analysis(
                    request,
                    baseline_failure,
                    model_version=self._baseline_identity.requested_model,
                )

        adversarial_run: AdversarialMacroRun
        if not adversarial_task.done() or adversarial_task.cancelled():
            adversarial_run = self._adversarial._failed_run(  # noqa: SLF001
                request,
                (),
                failure_code="ADVERSARIAL_CASE_DEADLINE_EXCEEDED",
                termination=AdversarialTermination.CASE_DEADLINE_EXCEEDED,
                calls_started=0,
            )
        else:
            try:
                adversarial_run = adversarial_task.result()
            except Exception:
                adversarial_run = self._adversarial._failed_run(  # noqa: SLF001
                    request,
                    (),
                    failure_code="ADVERSARIAL_CASE_FAILED",
                    termination=AdversarialTermination.CRITICAL_FAILURE,
                    calls_started=0,
                )

        adversarial_analysis = adversarial_run.analysis
        selected = adversarial_analysis
        cross_track_conflict = _cross_track_conflict(
            baseline_analysis,
            adversarial_analysis,
            threshold=self._material_cross_track_disagreement,
        )
        if (
            adversarial_run.failure_code is None
            and cross_track_conflict
            and selected.decision is MacroAnalysisDecision.PUBLISH
        ):
            selected = replace(
                selected,
                decision=MacroAnalysisDecision.WATCH,
                regime=f"{selected.regime}；单分析器与对抗分析器存在实质分歧，已降级",
                uncertainties=tuple(
                    dict.fromkeys(("DUAL_TRACK_MATERIAL_CONFLICT", *selected.uncertainties))
                ),
            )
            selected.validate_against(request)

        failure_code = adversarial_run.failure_code
        completed_at = datetime.now(UTC)
        audit_document: dict[str, object] = {
            "schema_version": "dual-track-macro-audit@1",
            "analysis_id": request.analysis_id,
            "symbol": request.symbol,
            "as_of": request.as_of.isoformat(),
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "latency_ms": max(0, int(round((monotonic() - started) * 1_000))),
            "case_timeout_seconds": self._case_timeout.total_seconds(),
            "request_sha256": _request_sha256(request),
            "evidence_pack_sha256": _evidence_pack_sha256(request),
            "selected_track": "ADVERSARIAL",
            "cross_track_conflict": cross_track_conflict,
            "failure_code": failure_code,
            "baseline_failure_code": baseline_failure,
            "baseline_identity": _identity_document(self._baseline_identity),
            "baseline_call": (
                None
                if baseline_call is None
                else {
                    "started_at": baseline_call.started_at.isoformat(),
                    "completed_at": baseline_call.completed_at.isoformat(),
                    "latency_ms": baseline_call.latency_ms,
                    "model": baseline_call.analysis.model_version,
                    "prompt_contract_sha256": _prompt_contract_sha256(request),
                    "evidence_pack_sha256": _evidence_pack_sha256(request),
                    "usage": dict(baseline_call.usage),
                    "termination_reason": "BASELINE_COMPLETED",
                }
            ),
            "adversarial_identity": _identity_document(self._adversarial.audit_identity),
            "baseline_analysis": _analysis_document(baseline_analysis),
            "adversarial_analysis": _analysis_document(adversarial_analysis),
            "selected_analysis": _analysis_document(selected),
            "adversarial_run": adversarial_run.audit_document(),
            "termination": (
                "ADVERSARIAL_FAILED_CLOSED"
                if failure_code is not None
                else (
                    "ADVERSARIAL_SELECTED_WITH_CONFLICT_DOWNGRADE"
                    if cross_track_conflict
                    else "ADVERSARIAL_SELECTED"
                )
            ),
        }
        audit_record_sha256: str | None = None
        if self._audit_sink is not None:
            try:
                audit_record_sha256 = self._audit_sink(audit_document)
            except Exception:
                failure_code = "DUAL_TRACK_AUDIT_PERSIST_FAILED"
                selected = _abstain_analysis(
                    request,
                    failure_code,
                    model_version=self._adversarial.audit_identity.requested_model,
                )
                audit_document["failure_code"] = failure_code
                audit_document["termination"] = "AUDIT_PERSISTENCE_FAILED_CLOSED"
                audit_document["selected_analysis"] = _analysis_document(selected)
        return DualTrackMacroAnalysis(
            selected_analysis=selected,
            baseline_analysis=baseline_analysis,
            adversarial_analysis=adversarial_analysis,
            selected_track="ADVERSARIAL",
            failure_code=failure_code,
            audit_document=audit_document,
            audit_record_sha256=audit_record_sha256,
        )


class FeatureFlaggedAdversarialMacroAnalyzer:
    """用于渐进发布、默认关闭的 BASELINE/SHADOW/ENFORCE 入口。

    SHADOW 并行启动两个分支，但只等待 baseline；对抗任务通过完成回调异步
    送入 observer，因此慢 provider 不会增加 baseline 的返回延迟。
    """

    def __init__(
        self,
        baseline: MacroAnalyzer,
        adversarial: AdversarialMacroAnalyzer,
        *,
        mode: AdversarialFeatureMode = AdversarialFeatureMode.BASELINE,
        baseline_identity: AnalyzerAuditIdentity | None = None,
        shadow_observer: ShadowObserver | None = None,
    ) -> None:
        discovered = (
            baseline.audit_identity if isinstance(baseline, AuditableMacroAnalyzer) else None
        )
        if (
            baseline_identity is not None
            and discovered is not None
            and baseline_identity != discovered
        ):
            raise ValueError("explicit baseline identity does not match baseline analyzer")
        resolved_identity = baseline_identity or discovered
        if resolved_identity is None:
            raise ValueError("feature-flagged analysis requires an auditable baseline")
        self._baseline_identity = resolved_identity
        self._baseline = baseline
        self._adversarial = adversarial
        self._mode = mode
        self._shadow_observer = shadow_observer
        self._shadow_tasks: set[asyncio.Task[AdversarialMacroRun]] = set()

    @property
    def mode(self) -> AdversarialFeatureMode:
        return self._mode

    @property
    def audit_identity(self) -> AnalyzerAuditIdentity:
        if self._mode is AdversarialFeatureMode.ENFORCE:
            return self._adversarial.audit_identity
        return self._baseline_identity

    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        if self._mode is AdversarialFeatureMode.BASELINE:
            return await self._baseline.analyze(request)
        if self._mode is AdversarialFeatureMode.ENFORCE:
            return await self._adversarial.analyze(request)

        shadow_task = asyncio.create_task(
            self._adversarial.analyze_case(request),
            name=f"adversarial-shadow:{request.analysis_id[-12:]}",
        )
        try:
            baseline_analysis = await self._baseline.analyze(request)
        except BaseException:
            shadow_task.cancel()
            await asyncio.gather(shadow_task, return_exceptions=True)
            raise
        # 给纯内存/零延迟 provider 几次调度机会，以便测试与本地观察立即完成；
        # 每次只让出事件循环，不等待任何实际 I/O 或计时器。
        for _ in range(4):
            if shadow_task.done():
                break
            await asyncio.sleep(0)
        if shadow_task.done():
            self._publish_shadow(shadow_task, baseline_analysis)
        else:
            self._shadow_tasks.add(shadow_task)
            shadow_task.add_done_callback(
                lambda task: self._publish_shadow(task, baseline_analysis)
            )
        return baseline_analysis

    def _publish_shadow(
        self,
        task: asyncio.Task[AdversarialMacroRun],
        baseline_analysis: MacroAnalysis,
    ) -> None:
        self._shadow_tasks.discard(task)
        if task.cancelled():
            return
        with suppress(Exception):
            shadow_run = task.result()
            if self._shadow_observer is not None:
                self._shadow_observer(
                    AdversarialShadowRecord(
                        analysis_id=baseline_analysis.analysis_id,
                        baseline_analysis=baseline_analysis,
                        adversarial_analysis=shadow_run.analysis,
                        adversarial_failure_code=shadow_run.failure_code,
                        baseline_identity_sha256=(self._baseline_identity.manifest_sha256),
                        adversarial_identity_sha256=(shadow_run.audit_identity.manifest_sha256),
                    )
                )


def _role_request(
    request: MacroAnalysisRequest,
    *,
    role: AdversarialMacroRole,
    round_number: int,
    previous_round: AdversarialRound | None,
    audit_identity: AnalyzerAuditIdentity,
    protocol_version: str,
) -> MacroAnalysisRequest:
    """兼容旧 helper；角色请求协议位于 boundaries 模块。"""

    peer_document = _peer_document(previous_round, receiving_role=role)
    return _boundaries._role_request(
        request,
        role=role,
        role_charter=_ROLE_CHARTERS[role],
        round_number=round_number,
        peer_document=peer_document,
        previous_round_present=previous_round is not None,
        audit_identity=audit_identity,
        protocol_version=protocol_version,
    )


def _adversarial_audit_identity(
    base: AnalyzerAuditIdentity,
    config: AdversarialMacroConfig,
) -> AnalyzerAuditIdentity:
    prompt_contract = {
        **_protocol_document(config),
        "base_identity_sha256": base.manifest_sha256,
    }
    return AnalyzerAuditIdentity(
        provider_id=f"{base.provider_id}.adversarial",
        requested_model=(f"{base.requested_model}+adversarial-{config.depth.value.lower()}@1"),
        adapter_version=_ADAPTER_VERSION,
        prompt_version=f"{config.protocol_version}:{config.depth.value.lower()}",
        prompt_schema_sha256=_document_sha256(prompt_contract),
    )


def _protocol_document(config: AdversarialMacroConfig) -> dict[str, object]:
    return {
        "aggregation_version": _AGGREGATION_VERSION,
        "config": config.audit_document(),
        "peer_envelope_version": _PEER_ENVELOPE_VERSION,
        "role_charters": {role.value: list(_ROLE_CHARTERS[role]) for role in config.roles},
        "role_output_contract": {
            "claim_evidence_required": True,
            "falsification_condition_required": True,
            "maximum_claims": _MAX_ROLE_CLAIMS,
            "peer_arguments_are_evidence": False,
        },
    }


def _abstain_analysis(
    request: MacroAnalysisRequest,
    failure_code: str,
    *,
    model_version: str,
) -> MacroAnalysis:
    """兼容旧 helper；失败关闭分析位于 boundaries 模块。"""

    return _boundaries._abstain_analysis(request, failure_code, model_version=model_version)


async def _call_analyzer_with_usage(
    analyzer: MacroAnalyzer,
    request: MacroAnalysisRequest,
) -> _RoleCallResult:
    started_at = datetime.now(UTC)
    started = monotonic()
    if isinstance(analyzer, UsageReportingMacroAnalyzer):
        traced = await analyzer.analyze_with_usage(request)
        analysis = traced.analysis
        usage = traced.usage.audit_document()
    else:
        analysis = await analyzer.analyze(request)
        usage = {
            "input_tokens": None,
            "output_tokens": None,
            "reasoning_tokens": None,
            "total_tokens": None,
        }
    completed_at = datetime.now(UTC)
    return _RoleCallResult(
        analysis=analysis,
        started_at=started_at,
        completed_at=completed_at,
        latency_ms=max(0, int(round((monotonic() - started) * 1_000))),
        usage=usage,
    )


def _role_failure_documents(
    roles: Sequence[AdversarialMacroRole],
    requests: Sequence[MacroAnalysisRequest],
    results: Sequence[object],
    *,
    default_failure_code: str,
    rejected_role: AdversarialMacroRole | None = None,
) -> tuple[Mapping[str, object], ...]:
    """兼容旧 helper；失败调用审计文档位于 boundaries 模块。"""

    return _boundaries._role_failure_documents(
        roles,
        requests,
        results,
        default_failure_code=default_failure_code,
        rejected_role=rejected_role,
    )


__all__ = (
    "AdversarialFeatureMode",
    "AdversarialMacroAnalyzer",
    "AdversarialMacroConfig",
    "AdversarialMacroDepth",
    "AdversarialMacroRole",
    "AdversarialMacroRun",
    "AdversarialRoleOpinion",
    "AdversarialRound",
    "AdversarialShadowRecord",
    "AdversarialTermination",
    "FeatureFlaggedAdversarialMacroAnalyzer",
    "ProductionDualTrackMacroAnalyzer",
)
