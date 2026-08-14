"""不依赖券商的单次 A 股盘后编排。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from gribuki_trade.analysis.schemas import MacroAnalysis
from gribuki_trade.domain.candidates import CandidateRecord, CandidateStatus
from gribuki_trade.domain.paper_trading import (
    PaperAccountSnapshot,
    PaperInstrumentType,
    PaperPosition,
)
from gribuki_trade.domain.post_close import (
    PostCloseCandidate,
    PostCloseInstrumentResearch,
    PostCloseResearchStatus,
    PostCloseReview,
)
from gribuki_trade.features.close_analysis import CloseInstrumentType
from gribuki_trade.reporting.paper_day_summary import (
    PaperDayExecutiveProjection,
    project_paper_day_sidecars,
)
from gribuki_trade.reporting.post_close import write_post_close_review
from gribuki_trade.services.ashare_close_analysis import (
    AShareCloseAnalysisRequest,
    AShareCloseAnalysisService,
)
from gribuki_trade.services.ashare_close_sessions import (
    AShareCloseSessionResolver,
    CloseAnalysisMode,
    CloseSessionResolution,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


class PostCloseOrchestrationError(RuntimeError):
    """已脱敏且稳定的编排失败。"""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"A-share post-close orchestration failed ({code})")


class PaperAccountReader(Protocol):
    def snapshot(self, account_id: str) -> PaperAccountSnapshot: ...


class CandidateReader(Protocol):
    def list_candidates(
        self,
        *,
        as_of: datetime,
        statuses: frozenset[CandidateStatus] | None = None,
        limit: int = 500,
    ) -> tuple[CandidateRecord, ...]: ...


class HeldPositionCloseResearch(Protocol):
    async def research(
        self,
        position: PaperPosition,
        *,
        sessions: CloseSessionResolution,
    ) -> PostCloseInstrumentResearch: ...


class CloseSessionReader(Protocol):
    async def resolve(self, now: datetime) -> CloseSessionResolution: ...


@dataclass(frozen=True, slots=True)
class PostCloseOrchestrationRequest:
    session_root: Path
    account_id: str
    now: datetime

    def __post_init__(self) -> None:
        if self.now.tzinfo is None or self.now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if not self.account_id.strip():
            raise ValueError("account_id must not be blank")
        supplied_parts = self.session_root.parts
        resolved_parts = self.session_root.resolve(strict=False).parts
        if any(
            part.casefold() == "secrets" for part in (*supplied_parts, *resolved_parts)
        ):
            raise ValueError("session_root must not be inside a secrets directory")


@dataclass(frozen=True, slots=True)
class PostCloseOrchestrationResult:
    review: PostCloseReview
    artifact_path: Path


class ExistingCloseAnalysisResearch:
    """封装既有收盘分析服务且不发送通知的适配器。"""

    def __init__(
        self,
        service: AShareCloseAnalysisService,
        *,
        history_calendar_days: int = 540,
    ) -> None:
        if history_calendar_days < 180:
            raise ValueError("history_calendar_days must be at least 180")
        self._service = service
        self._history_calendar_days = history_calendar_days

    async def research(
        self,
        position: PaperPosition,
        *,
        sessions: CloseSessionResolution,
    ) -> PostCloseInstrumentResearch:
        instrument_type = (
            CloseInstrumentType.ETF
            if position.instrument_type is PaperInstrumentType.ETF
            else CloseInstrumentType.STOCK
        )
        run = await self._service.run_once(
            AShareCloseAnalysisRequest(
                symbol=position.symbol,
                history_start=sessions.latest_completed_session
                - timedelta(days=self._history_calendar_days),
                latest_completed_session=sessions.latest_completed_session,
                next_session=sessions.next_session,
                as_of=sessions.as_of,
                is_currently_held=True,
                instrument_type=instrument_type,
                calendar_verified=sessions.calendar_verified,
            )
        )
        baseline_macro = getattr(run, "baseline_macro", None)
        adversarial_macro = getattr(run, "adversarial_macro", None)
        recommendation_evidence = tuple(
            getattr(run.recommendation, "evidence", ())
        )
        return PostCloseInstrumentResearch(
            symbol=position.symbol,
            status=PostCloseResearchStatus.COMPLETED,
            decision=run.recommendation.decision.value,
            technical_score=run.recommendation.technical_score,
            reference_price=run.recommendation.reference_price,
            invalidation_price=run.recommendation.invalidation_price,
            reason_codes=run.recommendation.reason_codes,
            uncertainties=run.recommendation.uncertainties,
            daily_bar_count=run.daily_bar_count,
            technical_decision=getattr(
                getattr(getattr(run, "assessment", None), "decision", None),
                "value",
                None,
            ),
            combined_score=getattr(run.recommendation, "combined_score", None),
            macro_score=getattr(run.recommendation, "macro_score", None),
            macro_evidence_coverage=getattr(
                run.recommendation,
                "macro_evidence_coverage",
                None,
            ),
            macro_model=getattr(getattr(run, "macro", None), "model_version", None),
            macro_failure_code=getattr(run, "macro_failure_code", None),
            market_data_failure_code=getattr(run, "market_data_failure_code", None),
            macro_analysis_id=_macro_analysis_id(
                baseline_macro,
                adversarial_macro,
            ),
            macro_selected_track=getattr(run, "macro_selected_track", None),
            macro_audit_record_sha256=getattr(
                run,
                "macro_audit_record_sha256",
                None,
            ),
            baseline_macro_decision=_macro_decision(baseline_macro),
            baseline_macro_regime=_macro_text(baseline_macro, "regime"),
            baseline_macro_score=_macro_decimal(baseline_macro, "macro_impact"),
            baseline_macro_evidence_coverage=_macro_evidence_coverage(
                baseline_macro,
                recommendation_evidence,
            ),
            baseline_macro_model=_macro_text(baseline_macro, "model_version"),
            adversarial_macro_decision=_macro_decision(adversarial_macro),
            adversarial_macro_regime=_macro_text(adversarial_macro, "regime"),
            adversarial_macro_score=_macro_decimal(adversarial_macro, "macro_impact"),
            adversarial_macro_evidence_coverage=_macro_evidence_coverage(
                adversarial_macro,
                recommendation_evidence,
            ),
            adversarial_macro_model=_macro_text(adversarial_macro, "model_version"),
        )


class ASharePostCloseOrchestrator:
    """在不接受任何券商或通知器依赖的情况下构建一份本地报告。"""

    def __init__(
        self,
        *,
        session_resolver: AShareCloseSessionResolver | CloseSessionReader,
        paper_account: PaperAccountReader,
        close_research: HeldPositionCloseResearch,
        candidate_reader: CandidateReader | None = None,
        sidecar_loader: Callable[[Path], PaperDayExecutiveProjection] = (
            project_paper_day_sidecars
        ),
        report_writer: Callable[..., Path] = write_post_close_review,
        research_timeout_seconds: float = 120.0,
        candidate_limit: int = 500,
    ) -> None:
        if research_timeout_seconds <= 0:
            raise ValueError("research_timeout_seconds must be positive")
        if candidate_limit < 1:
            raise ValueError("candidate_limit must be positive")
        self._sessions = session_resolver
        self._paper_account = paper_account
        self._close_research = close_research
        self._candidate_reader = candidate_reader
        self._sidecar_loader = sidecar_loader
        self._report_writer = report_writer
        self._research_timeout_seconds = research_timeout_seconds
        self._candidate_limit = candidate_limit

    async def run_once(
        self,
        request: PostCloseOrchestrationRequest,
    ) -> PostCloseOrchestrationResult:
        local_now = request.now.astimezone(SHANGHAI)
        if local_now.time().replace(tzinfo=None) < time(15, 5):
            raise PostCloseOrchestrationError("BEFORE_COMPLETED_BAR_AVAILABLE")

        sessions = await self._sessions.resolve(request.now)
        if (
            sessions.analysis_mode is not CloseAnalysisMode.POST_CLOSE
            or sessions.latest_completed_session != local_now.date()
        ):
            raise PostCloseOrchestrationError("CURRENT_DATE_NOT_TRADING_SESSION")
        if not sessions.calendar_verified:
            raise PostCloseOrchestrationError("TRADING_CALENDAR_NOT_VERIFIED")

        paper_day = self._sidecar_loader(request.session_root)
        if paper_day.session_date != local_now.date():
            raise PostCloseOrchestrationError("PAPER_SIDECAR_SESSION_MISMATCH")
        if paper_day.lifecycle != "COMPLETED":
            raise PostCloseOrchestrationError("PAPER_DAY_NOT_COMPLETED")

        account = self._paper_account.snapshot(request.account_id)
        if account.account_id != request.account_id.strip():
            raise PostCloseOrchestrationError("PAPER_ACCOUNT_ID_MISMATCH")
        if account.session_date != local_now.date():
            raise PostCloseOrchestrationError("PAPER_LEDGER_SESSION_MISMATCH")
        self._validate_position_consistency(paper_day, account)

        candidates, candidate_warning = self._load_candidates(request.now)
        positions = tuple(
            sorted(
                (position for position in account.positions if position.quantity > 0),
                key=lambda item: item.symbol,
            )
        )
        research = await asyncio.gather(
            *(self._research_one(position, sessions=sessions) for position in positions)
        )
        warnings = () if candidate_warning is None else (candidate_warning,)
        review = PostCloseReview(
            generated_at=local_now,
            session_date=local_now.date(),
            next_session=sessions.next_session,
            account_id=account.account_id,
            paper_run_id=paper_day.run_id,
            paper_lifecycle=paper_day.lifecycle,
            paper_coverage=paper_day.coverage,
            cash=account.cash,
            last_ledger_sequence=account.last_sequence,
            held_symbols=tuple(position.symbol for position in positions),
            candidates=candidates,
            research=tuple(research),
            warnings=warnings,
        )
        artifact = self._report_writer(review, paper_day=paper_day, account=account)
        return PostCloseOrchestrationResult(review=review, artifact_path=artifact)

    async def _research_one(
        self,
        position: PaperPosition,
        *,
        sessions: CloseSessionResolution,
    ) -> PostCloseInstrumentResearch:
        try:
            result = await asyncio.wait_for(
                self._close_research.research(position, sessions=sessions),
                timeout=self._research_timeout_seconds,
            )
            if result.symbol != position.symbol:
                raise ValueError("research result symbol mismatch")
            return result
        except TimeoutError:
            return _failed_research(position.symbol, "CLOSE_RESEARCH_TIMEOUT")
        except Exception:
            return _failed_research(position.symbol, "CLOSE_RESEARCH_FAILED")

    def _load_candidates(
        self,
        as_of: datetime,
    ) -> tuple[tuple[PostCloseCandidate, ...], str | None]:
        if self._candidate_reader is None:
            return (), "CANDIDATE_STORE_NOT_CONFIGURED"
        try:
            values = self._candidate_reader.list_candidates(
                as_of=as_of,
                statuses=frozenset({CandidateStatus.ACTIVE}),
                limit=self._candidate_limit,
            )
        except Exception:
            return (), "CANDIDATE_STORE_READ_FAILED"
        candidates = tuple(
            sorted(
                (
                    PostCloseCandidate(
                        symbol=item.symbol,
                        status=item.status.value,
                        priority=int(item.priority),
                        reason_codes=item.reason_codes,
                    )
                    for item in values
                ),
                key=lambda item: (-item.priority, item.symbol),
            )
        )
        return candidates, None

    @staticmethod
    def _validate_position_consistency(
        paper_day: PaperDayExecutiveProjection,
        account: PaperAccountSnapshot,
    ) -> None:
        sidecar = {
            item.symbol: item.quantity for item in paper_day.positions if item.quantity > 0
        }
        ledger = {item.symbol: item.quantity for item in account.positions if item.quantity > 0}
        if sidecar != ledger:
            raise PostCloseOrchestrationError("PAPER_POSITION_PROJECTION_MISMATCH")


def _failed_research(symbol: str, code: str) -> PostCloseInstrumentResearch:
    return PostCloseInstrumentResearch(
        symbol=symbol,
        status=PostCloseResearchStatus.FAILED,
        failure_code=code,
    )


def _macro_analysis_id(
    baseline: MacroAnalysis | None,
    adversarial: MacroAnalysis | None,
) -> str | None:
    for analysis in (baseline, adversarial):
        if analysis is not None and analysis.analysis_id.strip():
            return analysis.analysis_id
    return None


def _macro_decision(analysis: MacroAnalysis | None) -> str | None:
    return None if analysis is None else analysis.decision.value


def _macro_text(analysis: MacroAnalysis | None, name: str) -> str | None:
    if analysis is None:
        return None
    value = getattr(analysis, name, None)
    return value.strip() if isinstance(value, str) and value.strip() else None


def _macro_decimal(analysis: MacroAnalysis | None, name: str) -> Decimal | None:
    if analysis is None:
        return None
    value = getattr(analysis, name, None)
    return value if isinstance(value, Decimal) and value.is_finite() else None


def _macro_evidence_coverage(
    analysis: MacroAnalysis | None,
    evidence: tuple[object, ...],
) -> Decimal | None:
    """复算单条轨道实际引用的冻结证据比例，避免复用另一轨道的融合值。"""

    if analysis is None:
        return None
    available = {
        evidence_id
        for item in evidence
        if isinstance((evidence_id := getattr(item, "evidence_id", None)), str)
        and evidence_id
    }
    referenced = {
        evidence_id
        for claim in analysis.claims
        for evidence_id in claim.evidence_ids
    }
    referenced.update(
        evidence_id
        for scenario in analysis.scenarios
        for evidence_id in scenario.evidence_ids
    )
    if not available:
        return Decimal("0")
    return Decimal(len(referenced & available)) / Decimal(len(available))
