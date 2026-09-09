"""生产双轨 LLM 的统一构造入口。"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from gribuki_trade.analysis.schemas import (
    EvidenceItem,
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
)
from gribuki_trade.domain.exit_plans import ExitPlan
from gribuki_trade.features.deep_exit_planning import (
    DeepExitTimeframe,
    DeepSemanticAssessment,
)
from gribuki_trade.ports.llm_analyzer import (
    AnalyzerAuditIdentity,
    DualTrackMacroAnalysis,
    DualTrackMacroAnalyzer,
    MacroAnalyzer,
)
from gribuki_trade.services.adversarial_macro import (
    AdversarialMacroAnalyzer,
    AdversarialMacroConfig,
    AdversarialMacroDepth,
    ProductionDualTrackMacroAnalyzer,
)
from gribuki_trade.storage.adversarial_audit import SQLiteAdversarialAuditStore


class ProductionLLMProfile(StrEnum):
    INTRADAY = "INTRADAY"
    STANDARD = "STANDARD"
    DEEP = "DEEP"


class OwnedProductionDualTrackAnalyzer:
    """同时拥有分析器和 SQLite 审计连接，便于 CLI 明确管理生命周期。"""

    def __init__(
        self,
        analyzer: ProductionDualTrackMacroAnalyzer,
        store: SQLiteAdversarialAuditStore,
    ) -> None:
        self._analyzer = analyzer
        self._store = store

    @property
    def audit_identity(self) -> AnalyzerAuditIdentity:
        return self._analyzer.audit_identity

    async def analyze(self, request: MacroAnalysisRequest) -> MacroAnalysis:
        return await self._analyzer.analyze(request)

    async def analyze_dual(
        self,
        request: MacroAnalysisRequest,
    ) -> DualTrackMacroAnalysis:
        return await self._analyzer.analyze_dual(request)

    def close(self) -> None:
        self._store.close()

    def __enter__(self) -> OwnedProductionDualTrackAnalyzer:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class PaperDayDualTrackDeepExitAssessmentProvider:
    """把成交后多时间框架冻结证据送入同一生产双轨协议。

    LLM 只返回[-1, 1]语义评分；止损、止盈和期限仍由确定性 DEEP
    生成器映射，因而模型无法直接生成价格或放宽硬风险。
    """

    def __init__(self, analyzer: DualTrackMacroAnalyzer) -> None:
        self._analyzer = analyzer

    async def assess(
        self,
        *,
        plan: ExitPlan,
        timeframes: tuple[DeepExitTimeframe, ...],
        decision_at: datetime,
    ) -> tuple[DeepSemanticAssessment | None, DeepSemanticAssessment | None]:
        request = _deep_exit_request(plan, timeframes, decision_at)
        result = await self._analyzer.analyze_dual(request)
        market_data_as_of = max(frame.bars[-1].end_time for frame in timeframes)
        baseline = _deep_semantic_assessment(
            result.baseline_analysis,
            system="BASELINE_LLM",
            evidence_ids=tuple(item.evidence_id for item in request.evidence),
            market_data_as_of=market_data_as_of,
        )
        adversarial = (
            None
            if result.failure_code is not None
            else _deep_semantic_assessment(
                result.adversarial_analysis,
                system="ADVERSARIAL_LLM",
                evidence_ids=tuple(item.evidence_id for item in request.evidence),
                market_data_as_of=market_data_as_of,
            )
        )
        return baseline, adversarial


def build_production_dual_track_analyzer(
    baseline: MacroAnalyzer,
    *,
    audit_path: Path,
    profile: ProductionLLMProfile,
    maximum_calls_per_session: int | None = None,
) -> OwnedProductionDualTrackAnalyzer:
    """按场景建立有界对抗分析器；``None`` 仅表示取消会话总调用预算。"""

    depth = {
        ProductionLLMProfile.INTRADAY: AdversarialMacroDepth.FAST,
        ProductionLLMProfile.STANDARD: AdversarialMacroDepth.STANDARD,
        ProductionLLMProfile.DEEP: AdversarialMacroDepth.DEEP,
    }[profile]
    config = AdversarialMacroConfig.for_depth(
        depth,
        maximum_calls_per_session=maximum_calls_per_session,
    )
    adversarial = AdversarialMacroAnalyzer(baseline, config=config)
    store = SQLiteAdversarialAuditStore(audit_path)
    analyzer = ProductionDualTrackMacroAnalyzer(
        baseline,
        adversarial,
        case_timeout=config.case_timeout,
        audit_sink=store.append,
        material_cross_track_disagreement=Decimal("0.60"),
    )
    return OwnedProductionDualTrackAnalyzer(analyzer, store)


def recommended_intraday_review_timeout() -> timedelta:
    """外层协调器必须比 FAST case 多留少量落盘和事件循环余量。"""

    return timedelta(seconds=28)


def _deep_exit_request(
    plan: ExitPlan,
    timeframes: tuple[DeepExitTimeframe, ...],
    decision_at: datetime,
) -> MacroAnalysisRequest:
    if decision_at.tzinfo is None or decision_at.utcoffset() is None:
        raise ValueError("decision_at must be timezone-aware")
    decision_at = decision_at.astimezone(UTC)
    if not timeframes:
        raise ValueError("deep exit assessment requires at least one timeframe")
    evidence = tuple(_deep_timeframe_evidence(plan, frame) for frame in timeframes)
    material = {
        "plan_id": plan.plan_id,
        "version": plan.version,
        "decision_at": decision_at.isoformat(),
        "evidence": [item.content_hash for item in evidence],
    }
    analysis_id = "deep-exit-" + hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return MacroAnalysisRequest(
        analysis_id=analysis_id,
        symbol=plan.symbol,
        as_of=decision_at,
        horizon="POST_FILL_DEEP_EXIT_REVIEW",
        technical_summary=(
            "仅评估多头持仓趋势是否仍有延续空间以及风险是否应更快收紧。",
            "不得提出订单、仓位或价格；价格只由确定性退出计划映射。",
            f"当前保护止损={plan.stop_price}; 当前目标={plan.take_profit_price}; "
            f"原始每股风险={plan.initial_risk_per_share}",
        ),
        evidence=evidence,
    )


def _deep_timeframe_evidence(
    plan: ExitPlan,
    frame: DeepExitTimeframe,
) -> EvidenceItem:
    if not frame.bars:
        raise ValueError("deep exit timeframe bars must not be empty")
    last = frame.bars[-1]
    published_at = last.end_time.astimezone(UTC)
    first_seen_at = max(last.available_at.astimezone(UTC), published_at)
    bar_documents = [
        {
            "end": item.end_time.astimezone(UTC).isoformat(),
            "available": item.available_at.astimezone(UTC).isoformat(),
            "open": str(item.open),
            "high": str(item.high),
            "low": str(item.low),
            "close": str(item.close),
            "volume": str(item.volume),
            "complete": item.complete,
        }
        for item in frame.bars
    ]
    encoded = json.dumps(
        {
            "plan_id": plan.plan_id,
            "timeframe": frame.timeframe_id,
            "bars": bar_documents,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    closes = tuple(item.close for item in frame.bars)
    excerpt = (
        f"时间框架={frame.timeframe_id}; 完成线数量={len(frame.bars)}; "
        f"末线开高低收={last.open}/{last.high}/{last.low}/{last.close}; "
        f"区间收盘变化={closes[-1] - closes[0]}; "
        f"区间最高/最低={max(item.high for item in frame.bars)}/"
        f"{min(item.low for item in frame.bars)}。"
    )
    return EvidenceItem(
        evidence_id=f"deep-exit-{frame.timeframe_id}-{digest[:24]}",
        publisher="gribuki.deep_exit.technical",
        source_tier=0,
        published_at=published_at,
        first_seen_at=first_seen_at,
        title=f"{plan.symbol} {frame.timeframe_id} 完成线退出复核证据",
        excerpt=excerpt,
        canonical_url=f"urn:gribuki-trade:deep-exit:{digest}",
        content_hash=digest,
    )


def _deep_semantic_assessment(
    analysis: MacroAnalysis,
    *,
    system: str,
    evidence_ids: tuple[str, ...],
    market_data_as_of: datetime,
) -> DeepSemanticAssessment | None:
    if analysis.decision is MacroAnalysisDecision.ABSTAIN:
        return None
    score = (
        analysis.technical_alignment * Decimal("0.75")
        + analysis.macro_impact * Decimal("0.25")
    )
    confidence = {
        "HIGH": Decimal("0.75"),
        "MEDIUM": Decimal("0.50"),
        "LOW": Decimal("0.25"),
        "UNCALIBRATED": Decimal("0.25"),
    }.get(analysis.reported_confidence.strip().upper(), Decimal("0.25"))
    if analysis.decision is MacroAnalysisDecision.WATCH:
        confidence = min(confidence, Decimal("0.50"))
    assessment_id = hashlib.sha256(
        f"{analysis.analysis_id}|{system}|{analysis.model_version}".encode()
    ).hexdigest()
    return DeepSemanticAssessment(
        assessment_id=f"deep-semantic-{assessment_id}",
        system=system,
        score=max(Decimal("-1"), min(Decimal("1"), score)),
        confidence=confidence,
        market_data_as_of=market_data_as_of,
        evidence_ids=evidence_ids,
    )
