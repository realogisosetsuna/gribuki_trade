"""盘中 LLM sidecar 事件的纯投影。

本模块只读取事件对象的稳定字段，构建可审计的双轨模型、
预开盘上下文和 DEEP 退出摘要；不访问网络或持久化。
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from typing import Any


@dataclass(frozen=True, slots=True)
class PaperDayLLMTrackComparison:
    """一条盘中复核中两条模型轨道的紧凑可读投影。"""

    symbol: str
    review_id: str
    selected_track: str
    audit_record_sha256: str | None
    baseline_decision: str
    baseline_macro_impact: Decimal | None
    baseline_regime: str
    adversarial_decision: str
    adversarial_macro_impact: Decimal | None
    adversarial_regime: str


@dataclass(frozen=True, slots=True)
class PaperDayPreopenLLMComparison:
    """盘前冻结事件中两条模型轨道的完整摘要。"""

    status: str
    selected_track: str | None
    audit_record_sha256: str | None
    baseline_decision: str | None
    baseline_macro_impact: Decimal | None
    baseline_model: str | None
    adversarial_decision: str | None
    adversarial_macro_impact: Decimal | None
    adversarial_model: str | None


@dataclass(frozen=True, slots=True)
class PaperDayDeepExitLLMComparison:
    """成交后 DEEP 退出计划所保留的双轨语义评分。"""

    sequence: int
    known_at: datetime
    symbol: str
    protection_id: str | None
    plan_id: str | None
    selected_system: str
    selected_score: Decimal | None
    baseline_score: Decimal | None
    adversarial_score: Decimal | None
    status: str


@dataclass(frozen=True, slots=True)
class PaperDayDeepExitSellReview:
    """技术 REDUCE 与持久 DEEP 评分合并后的卖出紧迫度复核。"""

    sequence: int
    known_at: datetime
    symbol: str
    action: str
    technical_score: Decimal | None
    selected_semantic_score: Decimal | None
    combined_exit_score: Decimal | None
    llm_can_veto: bool


@dataclass(frozen=True, slots=True)
class PaperDayLLMProjection:
    """仅从日志旁路文件推导的可审计盘中大模型活动。"""

    enabled: bool
    required_for_buy: bool | None
    preopen_status: str
    preopen_context_id: str | None
    preopen_failure_code: str | None
    preopen_dual_track: PaperDayPreopenLLMComparison | None
    evidence_snapshot_status: str
    evidence_snapshot_sha256: str | None
    evidence_snapshot_failure_code: str | None
    review_batch_count: int
    review_candidate_count: int
    schedule_status_counts: tuple[tuple[str, int], ...]
    reviews_completed: int
    reviews_failed: int
    cache_rejected: int
    cache_restored: int
    gate_evaluations: int
    gate_blocked: int
    gate_action_counts: tuple[tuple[str, int], ...]
    gate_reason_counts: tuple[tuple[str, int], ...]
    sell_not_applicable: int
    latency_p50_ms: int | None
    latency_p95_ms: int | None
    latency_max_ms: int | None
    requested_models: tuple[str, ...]
    response_models: tuple[str, ...]
    prompt_schema_sha256: tuple[str, ...]
    service_state: str
    service_state_transition_count: int
    service_degradation_count: int
    service_recovery_count: int
    dual_track_comparisons: tuple[PaperDayLLMTrackComparison, ...]
    deep_exit_comparisons: tuple[PaperDayDeepExitLLMComparison, ...]
    deep_exit_sell_reviews: tuple[PaperDayDeepExitSellReview, ...]

def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _optional_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    if not isinstance(value, (str, int, float, Decimal)):
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        return None
    return result if result.is_finite() else None


def _optional_object(value: object) -> dict[str, object] | None:
    return value if isinstance(value, dict) else None


def _last_event(events: Iterable[Any], event_types: set[str]) -> Any | None:
    retained = [event for event in events if event.event_type in event_types]
    return retained[-1] if retained else None


def _counter_items(counter: Counter[str]) -> tuple[tuple[str, int], ...]:
    return tuple(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def project_llm_sidecars(
    events: tuple[Any, ...],
) -> PaperDayLLMProjection:
    policy = _last_event(events, {"LLM_INTRADAY_POLICY_CONFIGURED"})
    operator_disabled = _last_event(events, {"LLM_INTRADAY_OPERATOR_DISABLED"})
    policy_source = policy or operator_disabled
    policy_binding = (
        None
        if policy_source is None
        else _optional_object(policy_source.payload.get("manifest_binding"))
    )
    enabled = policy is not None or any(
        event.event_type
        in {
            "BUY_LLM_GATE_EVALUATED",
            "LLM_CANDIDATE_REVIEW_COMPLETED",
            "LLM_CANDIDATE_REVIEW_FAILED",
            "LLM_PREOPEN_CONTEXT_FAILED",
            "LLM_PREOPEN_CONTEXT_FROZEN",
            "LLM_REVIEW_BATCH_SCHEDULED",
            "LLM_SERVICE_STATE_CHANGED",
        }
        for event in events
    )
    required_for_buy = (
        None
        if policy_binding is None
        else _optional_bool(policy_binding.get("required_for_buy"))
    )

    frozen = _last_event(events, {"LLM_PREOPEN_CONTEXT_FROZEN"})
    failed = _last_event(events, {"LLM_PREOPEN_CONTEXT_FAILED"})
    latest_preopen = max(
        (item for item in (frozen, failed) if item is not None),
        key=lambda item: item.sequence,
        default=None,
    )
    if latest_preopen is None:
        preopen_status = "PENDING" if enabled else "DISABLED"
        preopen_context_id = None
        preopen_failure_code = None
        preopen_dual_track = None
    elif latest_preopen.event_type == "LLM_PREOPEN_CONTEXT_FAILED":
        preopen_status = "FAILED"
        preopen_context_id = None
        preopen_dual_track = None
        preopen_failure_code = _optional_string(
            latest_preopen.payload.get("error_code")
        )
        if required_for_buy is None:
            required_for_buy = _optional_bool(
                latest_preopen.payload.get("required_for_buy")
            )
    else:
        preopen_status = "FROZEN"
        preopen = _optional_object(latest_preopen.payload.get("preopen_context"))
        preopen_context_id = (
            None if preopen is None else _optional_string(preopen.get("context_id"))
        )
        preopen_failure_code = None
        dual = None if preopen is None else _optional_object(preopen.get("dual_track"))
        if dual is None:
            preopen_dual_track = PaperDayPreopenLLMComparison(
                status="LEGACY_NOT_CARRIED",
                selected_track=None,
                audit_record_sha256=None,
                baseline_decision=None,
                baseline_macro_impact=None,
                baseline_model=None,
                adversarial_decision=None,
                adversarial_macro_impact=None,
                adversarial_model=None,
            )
        else:
            baseline = _optional_object(dual.get("baseline"))
            adversarial = _optional_object(dual.get("adversarial"))
            selected_track = _optional_string(dual.get("selected_track"))
            audit_record_sha256 = _optional_string(
                dual.get("audit_record_sha256")
            )
            baseline_decision = (
                None if baseline is None else _optional_string(baseline.get("decision"))
            )
            baseline_macro_impact = (
                None
                if baseline is None
                else _optional_decimal(baseline.get("macro_impact"))
            )
            baseline_model = (
                None if baseline is None else _optional_string(baseline.get("model"))
            )
            adversarial_decision = (
                None
                if adversarial is None
                else _optional_string(adversarial.get("decision"))
            )
            adversarial_macro_impact = (
                None
                if adversarial is None
                else _optional_decimal(adversarial.get("macro_impact"))
            )
            adversarial_model = (
                None
                if adversarial is None
                else _optional_string(adversarial.get("model"))
            )
            complete = all(
                item is not None
                for item in (
                    selected_track,
                    audit_record_sha256,
                    baseline_decision,
                    baseline_macro_impact,
                    baseline_model,
                    adversarial_decision,
                    adversarial_macro_impact,
                    adversarial_model,
                )
            )
            preopen_dual_track = PaperDayPreopenLLMComparison(
                status="COMPLETE" if complete else "INCOMPLETE",
                selected_track=selected_track,
                audit_record_sha256=audit_record_sha256,
                baseline_decision=baseline_decision,
                baseline_macro_impact=baseline_macro_impact,
                baseline_model=baseline_model,
                adversarial_decision=adversarial_decision,
                adversarial_macro_impact=adversarial_macro_impact,
                adversarial_model=adversarial_model,
            )

    evidence_bound = _last_event(events, {"LLM_EVIDENCE_SNAPSHOT_BOUND"})
    evidence_failed = _last_event(events, {"LLM_EVIDENCE_SNAPSHOT_FAILED"})
    latest_evidence = max(
        (item for item in (evidence_bound, evidence_failed) if item is not None),
        key=lambda item: item.sequence,
        default=None,
    )
    if latest_evidence is None:
        evidence_snapshot_status = "NOT_RECORDED" if enabled else "DISABLED"
        evidence_snapshot_sha256 = None
        evidence_snapshot_failure_code = None
    else:
        binding = _optional_object(
            latest_evidence.payload.get("evidence_snapshot_binding")
        )
        evidence_snapshot_sha256 = _optional_string(
            latest_evidence.payload.get("evidence_snapshot_binding_sha256")
        )
        if evidence_snapshot_sha256 is None and binding is not None:
            evidence_snapshot_sha256 = _optional_string(
                binding.get("audit_sha256")
            )
        if latest_evidence.event_type == "LLM_EVIDENCE_SNAPSHOT_FAILED":
            evidence_snapshot_status = "FAILED"
            evidence_snapshot_failure_code = _optional_string(
                latest_evidence.payload.get("error_code")
            )
        else:
            evidence_snapshot_status = "BOUND"
            evidence_snapshot_failure_code = None

    batches = tuple(
        event for event in events if event.event_type == "LLM_REVIEW_BATCH_SCHEDULED"
    )
    schedule_statuses: Counter[str] = Counter()
    review_candidate_count = 0
    for batch in batches:
        count = _optional_int(batch.payload.get("candidate_count"))
        outcomes = batch.payload.get("outcomes")
        if count is not None:
            review_candidate_count += count
        elif isinstance(outcomes, list):
            review_candidate_count += len(outcomes)
        if not isinstance(outcomes, list):
            continue
        for value in outcomes:
            item = _optional_object(value)
            if item is None:
                continue
            schedule_statuses[_optional_string(item.get("status")) or "UNKNOWN"] += 1

    completed = tuple(
        event
        for event in events
        if event.event_type == "LLM_CANDIDATE_REVIEW_COMPLETED"
    )
    failed_reviews = tuple(
        event
        for event in events
        if event.event_type == "LLM_CANDIDATE_REVIEW_FAILED"
    )
    review_events = (*completed, *failed_reviews)
    latencies: list[int] = []
    requested_models: set[str] = set()
    response_models: set[str] = set()
    prompt_hashes: set[str] = set()
    dual_track_comparisons: list[PaperDayLLMTrackComparison] = []
    if policy_binding is not None:
        identity = _optional_object(policy_binding.get("analyzer_identity"))
        if identity is not None:
            if (model := _optional_string(identity.get("requested_model"))) is not None:
                requested_models.add(model)
            if (prompt := _optional_string(identity.get("prompt_schema_sha256"))) is not None:
                prompt_hashes.add(prompt)
    for event in review_events:
        review = _optional_object(event.payload.get("review"))
        if review is None:
            continue
        latency = _optional_int(review.get("latency_ms"))
        if latency is not None and latency >= 0:
            latencies.append(latency)
        if (model := _optional_string(review.get("response_model"))) is not None:
            response_models.add(model)
        dual = _optional_object(review.get("dual_track"))
        if dual is not None:
            baseline = _optional_object(dual.get("baseline_analysis"))
            adversarial = _optional_object(dual.get("adversarial_analysis"))
            if baseline is not None and adversarial is not None:
                dual_track_comparisons.append(
                    PaperDayLLMTrackComparison(
                        symbol=_optional_string(review.get("symbol")) or "—",
                        review_id=_optional_string(review.get("review_id")) or "—",
                        selected_track=(
                            _optional_string(dual.get("selected_track")) or "—"
                        ),
                        audit_record_sha256=_optional_string(
                            dual.get("audit_record_sha256")
                        ),
                        baseline_decision=(
                            _optional_string(baseline.get("decision")) or "—"
                        ),
                        baseline_macro_impact=_optional_decimal(
                            baseline.get("macro_impact")
                        ),
                        baseline_regime=(
                            _optional_string(baseline.get("regime")) or "—"
                        ),
                        adversarial_decision=(
                            _optional_string(adversarial.get("decision")) or "—"
                        ),
                        adversarial_macro_impact=_optional_decimal(
                            adversarial.get("macro_impact")
                        ),
                        adversarial_regime=(
                            _optional_string(adversarial.get("regime")) or "—"
                        ),
                    )
                )
        identity = _optional_object(review.get("analyzer_identity"))
        if identity is not None:
            if (model := _optional_string(identity.get("requested_model"))) is not None:
                requested_models.add(model)
            if (prompt := _optional_string(identity.get("prompt_schema_sha256"))) is not None:
                prompt_hashes.add(prompt)

    deep_exit_comparisons: list[PaperDayDeepExitLLMComparison] = []
    for event in events:
        if event.event_type != "EXIT_PLAN_DEEP_APPLIED":
            continue
        assessment = _optional_object(
            event.payload.get("deep_exit_llm_assessment")
        )
        if assessment is None:
            continue
        plan = _optional_object(event.payload.get("plan"))
        deep_exit_comparisons.append(
            PaperDayDeepExitLLMComparison(
                sequence=event.sequence,
                known_at=event.known_at,
                symbol=(
                    event.symbol
                    or (None if plan is None else _optional_string(plan.get("symbol")))
                    or "—"
                ),
                protection_id=_optional_string(
                    assessment.get("protection_id")
                )
                or _optional_string(event.payload.get("protection_id")),
                plan_id=_optional_string(assessment.get("plan_id"))
                or (None if plan is None else _optional_string(plan.get("plan_id"))),
                selected_system=(
                    _optional_string(assessment.get("selected_system")) or "UNKNOWN"
                ),
                selected_score=_optional_decimal(assessment.get("selected_score")),
                baseline_score=_optional_decimal(assessment.get("baseline_score")),
                adversarial_score=_optional_decimal(
                    assessment.get("adversarial_score")
                ),
                status=_optional_string(assessment.get("status")) or "UNKNOWN",
            )
        )

    deep_exit_sell_reviews: list[PaperDayDeepExitSellReview] = []
    for event in events:
        if event.event_type != "SELL_SIGNAL_TRIGGERED":
            continue
        review = _optional_object(event.payload.get("deep_exit_sell_review"))
        if review is None:
            continue
        deep_exit_sell_reviews.append(
            PaperDayDeepExitSellReview(
                sequence=event.sequence,
                known_at=event.known_at,
                symbol=event.symbol or "—",
                action=_optional_string(review.get("action")) or "UNKNOWN",
                technical_score=_optional_decimal(review.get("technical_score")),
                selected_semantic_score=_optional_decimal(
                    review.get("selected_semantic_score")
                ),
                combined_exit_score=_optional_decimal(
                    review.get("combined_exit_score")
                ),
                llm_can_veto=_optional_bool(review.get("llm_can_veto")) is True,
            )
        )

    gates = tuple(
        event for event in events if event.event_type == "BUY_LLM_GATE_EVALUATED"
    )
    gate_actions = Counter(
        _optional_string(event.payload.get("action")) or "UNKNOWN" for event in gates
    )
    gate_reasons = Counter(
        _optional_string(event.payload.get("reason_code")) or "UNKNOWN"
        for event in gates
    )
    for event in gates:
        if required_for_buy is None:
            required_for_buy = _optional_bool(event.payload.get("required_for_buy"))
        if (model := _optional_string(event.payload.get("requested_model"))) is not None:
            requested_models.add(model)
        if (model := _optional_string(event.payload.get("response_model"))) is not None:
            response_models.add(model)
        if (prompt := _optional_string(event.payload.get("prompt_schema_sha256"))) is not None:
            prompt_hashes.add(prompt)

    sell_not_applicable = 0
    for event in events:
        if event.event_type != "SELL_SIGNAL_TRIGGERED":
            continue
        gate = _optional_object(event.payload.get("llm_gate"))
        if gate is not None and gate.get("action") == "NOT_APPLICABLE":
            sell_not_applicable += 1

    restored = sum(
        _optional_int(event.payload.get("restored_review_count")) or 0
        for event in events
        if event.event_type == "LLM_REVIEW_CACHE_RESTORED"
    )
    service_edges = tuple(
        event for event in events if event.event_type == "LLM_SERVICE_STATE_CHANGED"
    )
    service_state = (
        "DISABLED"
        if not enabled
        else (
            "UNKNOWN"
            if not service_edges
            else _optional_string(service_edges[-1].payload.get("state")) or "UNKNOWN"
        )
    )
    ordered_latencies = tuple(sorted(latencies))
    return PaperDayLLMProjection(
        enabled=enabled,
        required_for_buy=required_for_buy,
        preopen_status=preopen_status,
        preopen_context_id=preopen_context_id,
        preopen_failure_code=preopen_failure_code,
        preopen_dual_track=preopen_dual_track,
        evidence_snapshot_status=evidence_snapshot_status,
        evidence_snapshot_sha256=evidence_snapshot_sha256,
        evidence_snapshot_failure_code=evidence_snapshot_failure_code,
        review_batch_count=len(batches),
        review_candidate_count=review_candidate_count,
        schedule_status_counts=_counter_items(schedule_statuses),
        reviews_completed=len(completed),
        reviews_failed=len(failed_reviews),
        cache_rejected=sum(
            event.event_type == "LLM_CANDIDATE_REVIEW_CACHE_REJECTED"
            for event in events
        ),
        cache_restored=restored,
        gate_evaluations=len(gates),
        gate_blocked=sum(event.payload.get("blocks_entry") is True for event in gates),
        gate_action_counts=_counter_items(gate_actions),
        gate_reason_counts=_counter_items(gate_reasons),
        sell_not_applicable=sell_not_applicable,
        latency_p50_ms=_nearest_rank(ordered_latencies, Decimal("0.50")),
        latency_p95_ms=_nearest_rank(ordered_latencies, Decimal("0.95")),
        latency_max_ms=None if not ordered_latencies else ordered_latencies[-1],
        requested_models=tuple(sorted(requested_models)),
        response_models=tuple(sorted(response_models)),
        prompt_schema_sha256=tuple(sorted(prompt_hashes)),
        service_state=service_state,
        service_state_transition_count=len(service_edges),
        service_degradation_count=sum(
            event.payload.get("state") == "DEGRADED" for event in service_edges
        ),
        service_recovery_count=sum(
            event.payload.get("previous_state") == "DEGRADED"
            and event.payload.get("state") == "HEALTHY"
            for event in service_edges
        ),
        dual_track_comparisons=tuple(dual_track_comparisons),
        deep_exit_comparisons=tuple(deep_exit_comparisons),
        deep_exit_sell_reviews=tuple(deep_exit_sell_reviews),
    )


def _nearest_rank(values: tuple[int, ...], percentile: Decimal) -> int | None:
    if not values:
        return None
    index = max(
        0,
        int(
            (Decimal(len(values)) * percentile).to_integral_value(
                rounding=ROUND_CEILING
            )
        )
        - 1,
    )
    return values[index]


__all__ = [
    "PaperDayDeepExitLLMComparison",
    "PaperDayDeepExitSellReview",
    "PaperDayLLMProjection",
    "PaperDayLLMTrackComparison",
    "PaperDayPreopenLLMComparison",
    "project_llm_sidecars",
]
