"""A 股候选池与研究复核命令处理器。

这些命令只操作候选、研究与复核状态，不触发交易执行；运行时依赖通过
CLI facade 延迟解析，以保留历史 monkeypatch 和嵌入调用入口。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, cast


class _LazyCliFacade:
    """延迟解析 CLI facade，避免独立导入时的循环依赖。"""

    def __getattr__(self, name: str) -> Any:
        from gribuki_trade import cli

        return getattr(cli, name)


_cli: Any = _LazyCliFacade()

__all__ = [
    "_ashare_candidates",
    "_ashare_review",
    "_ashare_review_case_json",
    "_bounded_review_reasons",
    "_optional_cli_identifier",
]


def _ashare_candidates(
    action: str,
    symbol: str | None,
    reason: str,
    candidate_store_path: str,
    cooling_minutes: int,
    limit: int,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """管理研究候选事件存储，且不包含任何执行路径。"""

    from gribuki_trade.domain.candidates import CandidateSource
    from gribuki_trade.services.candidate_universe import (
        CandidateDiscovery,
        CandidateUniverseService,
    )
    from gribuki_trade.storage.candidate_store import SQLiteCandidateStore

    if action not in {"list", "add", "cool", "activate", "remove"}:
        raise ValueError("unsupported candidate action")
    if (action == "list") != (symbol is None):
        raise ValueError("--symbol is required for mutations and omitted for list")
    if not reason.strip():
        raise ValueError("reason must not be empty")
    if cooling_minutes < 1 or limit < 1:
        raise ValueError("cooling_minutes and limit must be positive")
    resolved_now = now or _cli.datetime.now(_cli.UTC)
    if resolved_now.tzinfo is None or resolved_now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    path = _cli.Path(candidate_store_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with SQLiteCandidateStore(path) as store:
        service = CandidateUniverseService(store, clock=lambda: resolved_now)
        if action == "list":
            candidates = service.tracking_candidates(as_of=resolved_now, limit=limit)
            mutation_appended: bool | None = None
        else:
            assert symbol is not None
            if action == "add":
                mutation = service.upsert(
                    CandidateDiscovery(
                        symbol=symbol,
                        source=CandidateSource.MANUAL,
                        source_run_id=(
                            f"manual-{resolved_now.astimezone(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
                        ),
                        discovered_at=resolved_now,
                        observed_at=resolved_now,
                        reason_codes=(reason.strip(),),
                    )
                )
            elif action == "cool":
                mutation = service.cool(
                    symbol,
                    reason_code=reason,
                    at=resolved_now,
                    until=resolved_now + _cli.timedelta(minutes=cooling_minutes),
                )
            elif action == "activate":
                mutation = service.activate(symbol, reason_code=reason, at=resolved_now)
            else:
                mutation = service.remove(symbol, reason_code=reason, at=resolved_now)
            candidates = (mutation.candidate,)
            mutation_appended = mutation.appended
    return {
        "ok": True,
        "action": action,
        "as_of": resolved_now.astimezone(UTC).isoformat(),
        "candidate_store": str(path),
        "mutation_appended": mutation_appended,
        "candidates": [
            {
                "symbol": item.symbol,
                "status": item.status.value,
                "priority": item.priority.name,
                "sources": [source.value for source in item.sources],
                "reason_codes": list(item.reason_codes),
                "evidence_ids": list(item.evidence_ids),
                "discovered_at": item.discovered_at.isoformat(),
                "first_observed_at": item.first_observed_at.isoformat(),
                "last_observed_at": item.last_observed_at.isoformat(),
                "expires_at": (None if item.expires_at is None else item.expires_at.isoformat()),
                "cooling_until": (
                    None if item.cooling_until is None else item.cooling_until.isoformat()
                ),
                "provenance_count": len(item.provenance),
            }
            for item in candidates
        ],
    }


def _ashare_review(
    action: str,
    review_db: str,
    research_db: str,
    candidate_db: str,
    recommendation_id: str | None,
    case_id: str | None,
    reasons: Sequence[str] | None,
    limit: int,
    confirmation: str | None,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """操作研究复核状态机，且不产生执行副作用。"""

    from gribuki_trade.domain.review_cases import RecommendationReviewCase, ReviewActor
    from gribuki_trade.services.recommendation_review import (
        RecommendationReviewService,
    )
    from gribuki_trade.storage.candidate_store import SQLiteCandidateStore
    from gribuki_trade.storage.research_store import SQLiteResearchStore
    from gribuki_trade.storage.review_case_store import SQLiteReviewCaseStore

    supported = {"open", "list", "get", "confirm", "reject", "cancel"}
    if action not in supported:
        raise ValueError("unsupported review action")
    if limit < 1 or limit > 500:
        raise ValueError("limit must be between 1 and 500")
    resolved_now = now or _cli.datetime.now(_cli.UTC)
    if resolved_now.tzinfo is None or resolved_now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    resolved_now = resolved_now.astimezone(UTC)
    recommendation_id = _optional_cli_identifier(recommendation_id, "recommendation_id")
    case_id = _optional_cli_identifier(case_id, "case_id")
    if action == "open":
        if recommendation_id is None or case_id is not None:
            raise ValueError("open requires only --recommendation-id")
    elif action == "get":
        if (recommendation_id is None) == (case_id is None):
            raise ValueError("get requires exactly one of --case-id/--recommendation-id")
    elif action in {"confirm", "reject", "cancel"}:
        if case_id is None or recommendation_id is not None:
            raise ValueError(f"{action} requires only --case-id")
    elif recommendation_id is not None or case_id is not None:
        raise ValueError("list does not accept recommendation or case identifiers")
    if action == "confirm" and confirmation != "RESEARCH_ONLY":
        raise ValueError("confirm requires --confirm RESEARCH_ONLY")
    if action != "confirm" and confirmation is not None:
        raise ValueError("--confirm is valid only for the confirm action")

    default_reasons = {
        "open": ("DETAILED_REVIEW_REQUIRED",),
        "confirm": ("LOCAL_RESEARCH_CONFIRMED",),
        "reject": ("LOCAL_RESEARCH_REJECTED",),
        "cancel": ("LOCAL_REVIEW_CANCELLED",),
    }
    reason_codes = _bounded_review_reasons(
        reasons,
        default=default_reasons.get(action, ()),
    )
    if action in {"list", "get"} and reason_codes:
        raise ValueError(f"{action} does not accept --reason")

    review_path = _cli.Path(review_db).resolve()
    review_path.parent.mkdir(parents=True, exist_ok=True)
    with SQLiteReviewCaseStore(review_path) as repository:
        service = RecommendationReviewService(repository, clock=lambda: resolved_now)
        appended: bool | None = None
        cases: tuple[RecommendationReviewCase, ...]
        if action == "open":
            assert recommendation_id is not None
            existing = service.get_for_recommendation(
                recommendation_id,
                as_of=resolved_now,
            )
            if existing is not None:
                cases = (existing,)
                appended = False
            else:
                research_path = _cli.Path(research_db).resolve()
                if not research_path.is_file():
                    raise ValueError("research database does not exist")
                with SQLiteResearchStore(research_path) as research_store:
                    recommendation = research_store.get_recommendation(recommendation_id)
                if recommendation is None:
                    raise ValueError("recommendation does not exist")
                candidate = None
                candidate_path = _cli.Path(candidate_db).resolve()
                if candidate_path.is_file():
                    with SQLiteCandidateStore(candidate_path) as candidate_store:
                        candidate = candidate_store.get_candidate(
                            recommendation.symbol,
                            as_of=resolved_now,
                        )
                mutation = service.open_case(
                    recommendation,
                    candidate=candidate,
                    actor=ReviewActor.LOCAL_USER,
                    reason_codes=reason_codes,
                    at=resolved_now,
                )
                cases = (mutation.case,)
                appended = mutation.appended
        elif action == "list":
            cases = repository.list_cases(as_of=resolved_now, limit=limit)
        elif action == "get":
            case = (
                service.get(case_id, as_of=resolved_now)
                if case_id is not None
                else service.get_for_recommendation(
                    cast(str, recommendation_id), as_of=resolved_now
                )
            )
            if case is None:
                raise ValueError("review case does not exist")
            cases = (case,)
        else:
            assert case_id is not None
            transition = getattr(service, action)
            mutation = transition(
                case_id,
                actor=ReviewActor.LOCAL_USER,
                reason_codes=reason_codes,
                at=resolved_now,
            )
            cases = (mutation.case,)
            appended = mutation.appended
    return {
        "ok": True,
        "action": action,
        "as_of": resolved_now.isoformat(),
        "review_store": str(review_path),
        "mutation_appended": appended,
        "research_only": True,
        "execution_authorized": False,
        "cases": [_ashare_review_case_json(item) for item in cases],
    }


def _optional_cli_identifier(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized or len(normalized) > 160:
        raise ValueError(f"{field_name} must be between 1 and 160 characters")
    if any(character.isspace() or ord(character) < 32 for character in normalized):
        raise ValueError(f"{field_name} must not contain whitespace or control characters")
    return normalized


def _bounded_review_reasons(
    reasons: Sequence[str] | None,
    *,
    default: tuple[str, ...],
) -> tuple[str, ...]:
    values = default if reasons is None else tuple(reasons)
    normalized = tuple(sorted({item.strip() for item in values}))
    if len(normalized) > 12:
        raise ValueError("at most 12 review reason codes are allowed")
    if any(
        not item or len(item) > 120 or any(ord(character) < 32 for character in item)
        for item in normalized
    ):
        raise ValueError("review reasons must be non-empty bounded single-line values")
    return normalized


def _ashare_review_case_json(item: object) -> dict[str, object]:
    from gribuki_trade.domain.review_cases import RecommendationReviewCase

    case = cast(RecommendationReviewCase, item)
    provenance = case.candidate_provenance
    resolution = case.resolution
    return {
        "case_id": case.case_id,
        "recommendation_id": case.recommendation_id,
        "symbol": case.symbol,
        "status": case.status.value,
        "recommendation_as_of": case.recommendation_as_of.isoformat(),
        "opened_at": case.opened_at.isoformat(),
        "expires_at": case.expires_at.isoformat(),
        "as_of": case.as_of.isoformat(),
        "opened_by": case.opened_by.value,
        "open_reason_codes": list(case.open_reason_codes),
        "evidence_count": len(case.evidence_ids),
        "candidate_provenance": (
            None
            if provenance is None
            else {
                "status": provenance.candidate_status.value,
                "priority": provenance.priority.name,
                "sources": [source.value for source in provenance.sources],
                "reason_codes": list(provenance.reason_codes),
                "observation_count": len(provenance.observation_ids),
                "source_run_count": len(provenance.source_run_ids),
                "evidence_count": len(provenance.evidence_ids),
                "candidate_as_of": provenance.candidate_as_of.isoformat(),
            }
        ),
        "research_confirmed": case.is_research_approved,
        "execution_authorized": False,
        "resolution": (
            None
            if resolution is None
            else {
                "status": resolution.status.value,
                "actor": resolution.actor.value,
                "reason_codes": list(resolution.reason_codes),
                "occurred_at": resolution.occurred_at.isoformat(),
                "operation_id": resolution.operation_id,
            }
        ),
    }
