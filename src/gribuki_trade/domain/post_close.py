"""一次只读 A 股盘后复盘的不可变值。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum


class PostCloseResearchStatus(StrEnum):
    """一次独立隔离的持仓研究调用结果。"""

    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class PostCloseCandidate:
    """为下一交易时段保留的候选知识。"""

    symbol: str
    status: str
    priority: int
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PostCloseInstrumentResearch:
    """一个持仓标的的脱敏收盘研究结果。"""

    symbol: str
    status: PostCloseResearchStatus
    decision: str | None = None
    technical_score: Decimal | None = None
    reference_price: Decimal | None = None
    invalidation_price: Decimal | None = None
    reason_codes: tuple[str, ...] = ()
    uncertainties: tuple[str, ...] = ()
    daily_bar_count: int | None = None
    failure_code: str | None = None
    technical_decision: str | None = None
    combined_score: Decimal | None = None
    macro_score: Decimal | None = None
    macro_evidence_coverage: Decimal | None = None
    macro_provider: str | None = None
    macro_model: str | None = None
    macro_failure_code: str | None = None
    market_data_failure_code: str | None = None
    macro_analysis_id: str | None = None
    macro_selected_track: str | None = None
    macro_audit_record_sha256: str | None = None
    baseline_macro_decision: str | None = None
    baseline_macro_regime: str | None = None
    baseline_macro_score: Decimal | None = None
    baseline_macro_evidence_coverage: Decimal | None = None
    baseline_macro_model: str | None = None
    adversarial_macro_decision: str | None = None
    adversarial_macro_regime: str | None = None
    adversarial_macro_score: Decimal | None = None
    adversarial_macro_evidence_coverage: Decimal | None = None
    adversarial_macro_model: str | None = None

    def __post_init__(self) -> None:
        if self.status is PostCloseResearchStatus.COMPLETED:
            if self.decision is None or self.failure_code is not None:
                raise ValueError("completed research requires a decision and no failure")
        elif self.failure_code is None:
            raise ValueError("failed research requires a stable failure code")
        for name in ("baseline_macro_score", "adversarial_macro_score"):
            value = getattr(self, name)
            if value is not None and (
                not value.is_finite() or not Decimal("-1") <= value <= Decimal("1")
            ):
                raise ValueError(f"{name} must be a finite value in [-1, 1]")
        for name in (
            "baseline_macro_evidence_coverage",
            "adversarial_macro_evidence_coverage",
        ):
            value = getattr(self, name)
            if value is not None and (
                not value.is_finite() or not Decimal("0") <= value <= Decimal("1")
            ):
                raise ValueError(f"{name} must be a finite value in [0, 1]")


@dataclass(frozen=True, slots=True)
class PostCloseReview:
    """盘后编排器生成的完整知识快照。"""

    generated_at: datetime
    session_date: date
    next_session: date
    account_id: str
    paper_run_id: str
    paper_lifecycle: str
    paper_coverage: str
    cash: Decimal
    last_ledger_sequence: int
    held_symbols: tuple[str, ...]
    candidates: tuple[PostCloseCandidate, ...]
    research: tuple[PostCloseInstrumentResearch, ...]
    warnings: tuple[str, ...] = ()
    execution_mode: str = "PAPER_READ_ONLY"
    delivery_mode: str = "LOCAL_ARTIFACT_ONLY"

    def __post_init__(self) -> None:
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("generated_at must be timezone-aware")
        if self.session_date >= self.next_session:
            raise ValueError("next_session must follow session_date")
        if self.execution_mode != "PAPER_READ_ONLY":
            raise ValueError("post-close review cannot enable broker execution")
        if self.delivery_mode != "LOCAL_ARTIFACT_ONLY":
            raise ValueError("post-close review cannot deliver notifications")
        symbols = tuple(item.symbol for item in self.research)
        if symbols != tuple(sorted(symbols)) or len(symbols) != len(set(symbols)):
            raise ValueError("research results must be unique and sorted")
