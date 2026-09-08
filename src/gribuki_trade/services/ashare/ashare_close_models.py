"""盘后分析的输入、捕获结果与运行结果模型。

这些对象只描述一次收盘分析的边界，不负责网络访问、模型调用、通知发送或持久化。
将它们从编排服务中独立出来后，调用方可以在不加载分析流程的情况下构造和校验
时点受限的请求，也让结果投影能够依赖稳定的数据契约。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

from gribuki_trade.analysis.schemas import MacroAnalysis
from gribuki_trade.domain.events import NormalizedEvent
from gribuki_trade.domain.instruments import ResearchInstrumentProfile
from gribuki_trade.domain.market import DailyBar
from gribuki_trade.domain.recommendations import (
    EvidenceReference,
    ResearchRecommendation,
)
from gribuki_trade.features.close_analysis import (
    CloseInstrumentType,
    CloseTechnicalAssessment,
)
from gribuki_trade.ports.ashare_breadth import AShareBreadthSnapshot
from gribuki_trade.ports.cross_market import CrossMarketSnapshot
from gribuki_trade.ports.cross_market_history import CrossMarketHistorySnapshot
from gribuki_trade.ports.notifier import OutboundNotification

if TYPE_CHECKING:
    from gribuki_trade.services.ashare.ashare_breadth_evidence import AShareBreadthEvidenceBundle
    from gribuki_trade.services.ashare.ashare_context_evidence import AShareContextEvidenceBundle
    from gribuki_trade.services.ashare.ashare_derivatives_evidence import (
        AShareDerivativesEvidenceBundle,
    )
    from gribuki_trade.services.global_risk_evidence import GlobalRiskEvidenceBundle
    from gribuki_trade.services.macro_research import EvidenceSelection
    from gribuki_trade.services.official_rates_evidence import OfficialRatesEvidenceBundle


def canonical_symbol(symbol: str) -> str:
    """将六位 A 股代码规范化为带交易所后缀的形式。"""

    value = symbol.strip().upper()
    if len(value) == 6 and value.isdigit():
        if value.startswith(("4", "8", "92")):
            exchange = "BJ"
        else:
            exchange = "SH" if value.startswith(("5", "6", "9")) else "SZ"
        return f"{value}.{exchange}"
    if len(value) == 9 and value[6] == ".":
        code, exchange = value.split(".", maxsplit=1)
        if code.isdigit() and exchange in {"SH", "SZ", "BJ"}:
            return value
    raise ValueError("symbol must look like 600000.SH, 000001.SZ, or 430047.BJ")


def require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class AShareCloseAnalysisRequest:
    """一次收盘研究请求及其冻结的点时证据。"""

    symbol: str
    history_start: date
    latest_completed_session: date
    next_session: date
    events: tuple[NormalizedEvent, ...] = ()
    market_evidence: EvidenceReference | None = None
    as_of: datetime | None = None
    is_currently_held: bool = False
    instrument_type: CloseInstrumentType = CloseInstrumentType.STOCK
    instrument_profile: ResearchInstrumentProfile | None = None
    ashare_context: AShareContextEvidenceBundle | None = None
    ashare_context_failure_codes: tuple[str, ...] = ()
    ashare_breadth: AShareBreadthEvidenceBundle | None = None
    ashare_breadth_snapshot: AShareBreadthSnapshot | None = None
    ashare_breadth_failure_codes: tuple[str, ...] = ()
    global_risk: GlobalRiskEvidenceBundle | None = None
    global_risk_failure_codes: tuple[str, ...] = ()
    official_rates: OfficialRatesEvidenceBundle | None = None
    official_rates_failure_codes: tuple[str, ...] = ()
    ashare_derivatives: AShareDerivativesEvidenceBundle | None = None
    ashare_derivatives_failure_codes: tuple[str, ...] = ()
    cross_market_snapshot: CrossMarketSnapshot | None = None
    cross_market_failure_code: str | None = None
    cross_market_history: CrossMarketHistorySnapshot | None = None
    cross_market_history_failure_code: str | None = None
    calendar_verified: bool = False

    def __post_init__(self) -> None:
        canonical_symbol(self.symbol)
        if self.history_start > self.latest_completed_session:
            raise ValueError("history_start must not follow latest_completed_session")
        if self.latest_completed_session >= self.next_session:
            raise ValueError("latest_completed_session must precede next_session")
        if self.as_of is not None:
            require_aware(self.as_of, "as_of")
            if self.market_evidence is not None and self.market_evidence.first_seen_at > self.as_of:
                raise ValueError("market evidence was first seen after as_of")
        if (
            self.instrument_profile is not None
            and self.instrument_profile.symbol != self.canonical_symbol
        ):
            raise ValueError("instrument profile symbol must match request symbol")
        _validate_failure_codes("A-share context", self.ashare_context_failure_codes)
        _validate_failure_codes("A-share breadth", self.ashare_breadth_failure_codes)
        _validate_failure_codes("global-risk", self.global_risk_failure_codes)
        _validate_failure_codes("official-rates", self.official_rates_failure_codes)
        _validate_failure_codes("A-share derivatives", self.ashare_derivatives_failure_codes)
        self._validate_evidence_cutoffs()
        if self.cross_market_snapshot is not None and self.cross_market_failure_code:
            raise ValueError("cross-market snapshot and failure code are mutually exclusive")
        _validate_optional_code("cross_market_failure_code", self.cross_market_failure_code)
        if self.cross_market_history is not None and self.cross_market_history_failure_code:
            raise ValueError("cross-market history and its failure code are mutually exclusive")
        _validate_optional_code(
            "cross_market_history_failure_code", self.cross_market_history_failure_code
        )
        if (
            self.as_of is not None
            and self.cross_market_snapshot is not None
            and self.cross_market_snapshot.fetched_at > self.as_of
        ):
            raise ValueError("cross-market snapshot was fetched after as_of")
        if self.as_of is not None and self.cross_market_history is not None:
            if self.cross_market_history.as_of > self.as_of:
                raise ValueError("cross-market history cutoff was after as_of")
            if self.cross_market_history.fetched_at > self.as_of:
                raise ValueError("cross-market history was fetched after as_of")

    def _validate_evidence_cutoffs(self) -> None:
        if self.as_of is None:
            return
        bundles = (
            (self.ashare_context, "A-share context evidence"),
            (self.ashare_breadth, "A-share breadth evidence"),
            (self.global_risk, "global-risk evidence"),
            (self.official_rates, "official-rate evidence"),
            (self.ashare_derivatives, "A-share derivatives evidence"),
        )
        for bundle, label in bundles:
            if bundle is not None and any(
                item.first_seen_at > self.as_of for item in bundle.items
            ):
                raise ValueError(f"{label} was first seen after as_of")
        if (
            self.ashare_breadth_snapshot is not None
            and self.ashare_breadth_snapshot.meta.available_at > self.as_of
        ):
            raise ValueError("A-share breadth snapshot was first seen after as_of")

    @property
    def canonical_symbol(self) -> str:
        return canonical_symbol(self.symbol)


def _validate_failure_codes(label: str, values: tuple[str, ...]) -> None:
    if any(not code.strip() for code in values):
        raise ValueError(f"{label} failure codes must not be blank")
    if len(values) != len(set(values)):
        raise ValueError(f"{label} failure codes must be unique")


def _validate_optional_code(name: str, value: str | None) -> None:
    if value is not None and not value.strip():
        raise ValueError(f"{name} must not be blank")


@dataclass(frozen=True, slots=True)
class AShareCloseMarketDataCollection:
    """在新闻/模型工作前捕获的一份不可变日线数据结果。"""

    bars: tuple[DailyBar, ...]
    fetched_at: datetime
    failure_code: str | None = None
    source_name: str | None = None

    def __post_init__(self) -> None:
        require_aware(self.fetched_at, "fetched_at")
        if self.failure_code is not None and self.bars:
            raise ValueError("failed market-data collection cannot contain bars")
        if self.source_name is not None and not self.source_name.strip():
            raise ValueError("source_name must not be blank")


@dataclass(frozen=True, slots=True)
class AShareCloseAnalysisRun:
    """一次收盘分析的确定性结果及其通知副作用状态。"""

    assessment: CloseTechnicalAssessment
    recommendation: ResearchRecommendation
    evidence_selection: EvidenceSelection
    macro: MacroAnalysis | None
    daily_bar_count: int
    calendar_verified: bool
    baseline_macro: MacroAnalysis | None = None
    adversarial_macro: MacroAnalysis | None = None
    macro_selected_track: str | None = None
    macro_dual_audit_document: Mapping[str, object] | None = None
    macro_audit_record_sha256: str | None = None
    market_data_failure_code: str | None = None
    macro_failure_code: str | None = None
    ashare_context_failure_codes: tuple[str, ...] = ()
    ashare_context_report_lines: tuple[str, ...] = ()
    ashare_breadth_failure_codes: tuple[str, ...] = ()
    ashare_breadth_report_lines: tuple[str, ...] = ()
    global_risk_failure_codes: tuple[str, ...] = ()
    global_risk_report_lines: tuple[str, ...] = ()
    official_rates_failure_codes: tuple[str, ...] = ()
    official_rates_report_lines: tuple[str, ...] = ()
    ashare_derivatives_failure_codes: tuple[str, ...] = ()
    ashare_derivatives_report_lines: tuple[str, ...] = ()
    cross_market_failure_code: str | None = None
    cross_market_history_failure_code: str | None = None
    cross_market_report_lines: tuple[str, ...] = ()
    cross_market_relation_report_lines: tuple[str, ...] = ()
    notification: OutboundNotification | None = None
    notifications: tuple[OutboundNotification, ...] = ()
    notification_enqueued: bool = False
