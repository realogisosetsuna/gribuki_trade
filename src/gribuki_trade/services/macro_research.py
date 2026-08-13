"""Point-in-time evidence selection and fail-closed macro analysis."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256

from gribuki_trade.analysis.schemas import (
    EvidenceItem,
    MacroAnalysis,
    MacroAnalysisDecision,
    MacroAnalysisRequest,
)
from gribuki_trade.domain.events import (
    DISCOVERY_CONFIRMED_EVENT_TYPE,
    DISCOVERY_HINT_EVENT_TYPE,
    NormalizedEvent,
    SourceTier,
)
from gribuki_trade.domain.recommendations import EvidenceReference
from gribuki_trade.ports.llm_analyzer import MacroAnalyzer

_INJECTION_MARKERS = (
    re.compile(r"\bignore\s+(?:all\s+)?previous\s+instructions?\b", re.IGNORECASE),
    re.compile(r"\bsystem\s+prompt\b", re.IGNORECASE),
    re.compile(r"\b(?:api|access|secret)[-_ ]?key\b", re.IGNORECASE),
    re.compile(r"忽略(?:此前|之前|以上).{0,12}(?:指令|提示|要求)"),
    re.compile(r"系统提示(?:词|内容)?"),
)

_TIER_NUMBER = {
    SourceTier.OFFICIAL: 0,
    SourceTier.LICENSED: 1,
    SourceTier.PUBLIC_MEDIA: 2,
    SourceTier.SOCIAL: 3,
}

_GENERAL_EVIDENCE_EVENT_TYPES = frozenset(
    {
        "macro",
        "market_news",
        "market_notice",
        "policy",
        "regulatory",
        DISCOVERY_CONFIRMED_EVENT_TYPE,
    }
)

# Entity-free media wires are useful only when they carry recognisable China
# market or macro context.  This intentionally favours precision: generic
# overseas company, crime, sport, and celebrity headlines must not fill an
# A-share EvidencePack merely because the source is recent.
_A_SHARE_MARKET_TERMS = (
    "a股",
    "上证",
    "深证",
    "沪指",
    "深成指",
    "创业板",
    "科创板",
    "沪深",
    "北交所",
    "上交所",
    "深交所",
    "证监会",
    "北向资金",
    "融资融券",
)

_CHINA_SYSTEMIC_TERMS = (
    "中国国务院",
    "国务院常务会议",
    "国务院办公厅",
    "中国人民银行",
    "人民银行",
    "国家统计局",
    "中国财政部",
    "国家发展改革委",
    "社融",
    "逆回购",
    "降准",
    "降息",
    "lpr",
    "mlf",
    "在岸人民币",
    "离岸人民币",
    "人民币兑美元",
    "外汇储备",
)

_UNAMBIGUOUS_CHINA_SYSTEMIC_TERMS = (
    "中国国务院",
    "国务院常务会议",
    "国务院办公厅",
    "中国人民银行",
    "中国财政部",
    "国家发展改革委",
    "社融",
    "逆回购",
    "降准",
    "在岸人民币",
    "离岸人民币",
    "人民币兑美元",
    "外汇储备",
)

_AMBIGUOUS_MACRO_TERMS = ("央行", "cpi", "ppi", "pmi", "gdp")

_GLOBAL_SYSTEMIC_TERMS = (
    "美联储",
    "美国国债收益率",
    "美国30年期国债",
    "美国10年期国债",
    "美元指数",
    "纳斯达克",
    "道琼斯",
    "标普500",
    "vix",
    "恒生指数",
    "恒生国企",
    "恒生科技",
    "港股",
    "富时中国a50",
    "离岸人民币",
    "美元兑离岸人民币",
    "美国2年期国债",
    "美国就业",
    "美国非农",
    "美国通胀",
    "国际原油",
    "布伦特原油",
    "纽约黄金",
    "伦敦铜",
    "铁矿石",
    "波罗的海干散货",
)

_COMPANY_STORY_MARKERS = (
    "公告称",
    "董事会",
    "股东",
    "净利润",
    "营业收入",
    "营收",
    "增资",
    "股份回购",
    "回购股份",
    "拟回购",
    "中标",
    "签订合同",
    "天眼查",
    "法定代表人",
    "投资项目",
    "股价异动",
    "协议转让",
    "业绩预告",
    "预亏",
    "预增",
    "预减",
    "实控人",
    "停牌",
    "复牌",
)

_TECHNICAL_ONLY_ADDITIONAL_PUBLISHERS = frozenset({"baostock.daily"})

_FOREIGN_CONTEXT_TERMS = (
    "美国",
    "美联储",
    "美股",
    "美元",
    "加拿大",
    "欧洲",
    "欧元",
    "英国",
    "日本",
    "韩国",
    "印度",
    "俄罗斯",
    "乌克兰",
    "以色列",
    "巴西",
    "澳大利亚",
    "波兰",
    "纳斯达克",
    "道琼斯",
    "标普",
)


@dataclass(frozen=True, slots=True)
class MacroEvidenceConfig:
    """Bounds applied before untrusted source text reaches a model."""

    max_age: timedelta = timedelta(days=14)
    max_events: int = 24
    max_events_per_source: int = 6
    max_excerpt_characters: int = 1_200

    def __post_init__(self) -> None:
        if self.max_age <= timedelta(0):
            raise ValueError("max_age must be positive")
        if self.max_events < 1 or self.max_events_per_source < 1:
            raise ValueError("event limits must be positive")
        if self.max_excerpt_characters < 100:
            raise ValueError("max_excerpt_characters must be at least 100")


@dataclass(frozen=True, slots=True)
class EvidenceSelection:
    items: tuple[EvidenceItem, ...]
    references: tuple[EvidenceReference, ...]
    future_rejected: int = 0
    stale_rejected: int = 0
    irrelevant_rejected: int = 0
    injection_rejected: int = 0
    duplicate_rejected: int = 0
    source_limited: int = 0


@dataclass(frozen=True, slots=True)
class MacroResearchRun:
    request: MacroAnalysisRequest
    analysis: MacroAnalysis
    selection: EvidenceSelection
    failure_code: str | None = None


class MacroResearchService:
    """Select bounded evidence and invoke a model without tools of its own."""

    def __init__(
        self,
        analyzer: MacroAnalyzer,
        *,
        config: MacroEvidenceConfig | None = None,
    ) -> None:
        self._analyzer = analyzer
        self._config = config or MacroEvidenceConfig()

    async def analyze(
        self,
        *,
        symbol: str,
        as_of: datetime,
        horizon: str,
        technical_summary: Sequence[str],
        events: Sequence[NormalizedEvent],
        additional_evidence: Sequence[EvidenceItem] = (),
    ) -> MacroResearchRun:
        _require_aware(as_of, "as_of")
        selection = select_macro_evidence(
            symbol,
            as_of,
            events,
            config=self._config,
        )
        combined_evidence = tuple(additional_evidence) + selection.items
        identifiers = [item.evidence_id for item in combined_evidence]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("additional evidence IDs must not collide")
        if any(item.first_seen_at > as_of for item in additional_evidence):
            raise ValueError("additional evidence was not available at as_of")
        request = MacroAnalysisRequest(
            analysis_id=_analysis_id(symbol, as_of, horizon, combined_evidence),
            symbol=_canonical_symbol(symbol),
            as_of=as_of,
            horizon=horizon,
            technical_summary=tuple(_single_line(item) for item in technical_summary),
            evidence=combined_evidence,
        )
        macro_capable_additional = any(
            item.publisher not in _TECHNICAL_ONLY_ADDITIONAL_PUBLISHERS
            for item in additional_evidence
        )
        if not selection.items and not macro_capable_additional:
            return MacroResearchRun(
                request=request,
                analysis=_abstain_analysis(request, "NO_ELIGIBLE_EVIDENCE"),
                selection=selection,
                failure_code="NO_ELIGIBLE_EVIDENCE",
            )
        try:
            analysis = await self._analyzer.analyze(request)
            analysis.validate_against(request)
        except Exception as exc:
            # Upstream errors can include request IDs or response fragments.
            # Keep only an explicitly structured stable code in the research record.
            failure_code = _sanitized_analyzer_failure_code(exc)
            return MacroResearchRun(
                request=request,
                analysis=_abstain_analysis(request, failure_code),
                selection=selection,
                failure_code=failure_code,
            )
        return MacroResearchRun(
            request=request,
            analysis=analysis,
            selection=selection,
        )


def _sanitized_analyzer_failure_code(error: Exception) -> str:
    code = getattr(error, "error_code", None)
    if not isinstance(code, str):
        return "ANALYZER_FAILED"
    normalized = code.strip().upper()
    if not normalized.startswith("DEEPSEEK_"):
        return "ANALYZER_FAILED"
    if not normalized.replace("_", "").isalnum() or len(normalized) > 80:
        return "ANALYZER_FAILED"
    return normalized


def select_macro_evidence(
    symbol: str,
    as_of: datetime,
    events: Sequence[NormalizedEvent],
    *,
    config: MacroEvidenceConfig | None = None,
) -> EvidenceSelection:
    """Choose relevant, visible, diverse event revisions for one decision time."""

    _require_aware(as_of, "as_of")
    resolved = config or MacroEvidenceConfig()
    canonical = _canonical_symbol(symbol)
    code = canonical.split(".", maxsplit=1)[0]
    counters: Counter[str] = Counter()
    candidates: list[NormalizedEvent] = []
    seen_titles: set[str] = set()

    ordered = sorted(
        events,
        key=lambda event: (
            _evidence_scope_rank(event, canonical, code),
            _TIER_NUMBER[event.source_tier],
            -event.available_at.timestamp(),
            event.revision_id,
        ),
    )
    for event in ordered:
        if event.available_at > as_of or event.first_seen_at > as_of:
            counters["future"] += 1
            continue
        published_at = event.published_at or event.first_seen_at
        if published_at > as_of:
            counters["future"] += 1
            continue
        if as_of - published_at > resolved.max_age:
            counters["stale"] += 1
            continue
        if _evidence_scope_rank(event, canonical, code) >= 99:
            counters["irrelevant"] += 1
            continue
        untrusted_text = f"{event.title}\n{event.summary}"
        if any(marker.search(untrusted_text) for marker in _INJECTION_MARKERS):
            counters["injection"] += 1
            continue
        title_key = _single_line(event.title).casefold()
        if title_key in seen_titles:
            counters["duplicate"] += 1
            continue
        if counters[f"source:{event.source_id}"] >= resolved.max_events_per_source:
            counters["source_limited"] += 1
            continue
        seen_titles.add(title_key)
        counters[f"source:{event.source_id}"] += 1
        candidates.append(event)
        if len(candidates) >= resolved.max_events:
            break

    items: list[EvidenceItem] = []
    references: list[EvidenceReference] = []
    for event in candidates:
        published = event.published_at or event.first_seen_at
        excerpt = _single_line(f"{event.title}。{event.summary}")
        item = EvidenceItem(
            evidence_id=event.revision_id,
            publisher=event.source_id,
            source_tier=_TIER_NUMBER[event.source_tier],
            published_at=published,
            first_seen_at=event.first_seen_at,
            title=event.title,
            excerpt=excerpt[: resolved.max_excerpt_characters],
            canonical_url=event.canonical_url,
            content_hash=event.content_sha256,
        )
        items.append(item)
        references.append(
            EvidenceReference(
                evidence_id=item.evidence_id,
                title=item.title,
                canonical_url=item.canonical_url,
                published_at=item.published_at,
                first_seen_at=item.first_seen_at,
                source_tier=item.source_tier,
            )
        )
    return EvidenceSelection(
        items=tuple(items),
        references=tuple(references),
        future_rejected=counters["future"],
        stale_rejected=counters["stale"],
        irrelevant_rejected=counters["irrelevant"],
        injection_rejected=counters["injection"],
        duplicate_rejected=counters["duplicate"],
        source_limited=counters["source_limited"],
    )


def _evidence_scope_rank(
    event: NormalizedEvent,
    canonical_symbol: str,
    code: str,
) -> int:
    """Return a deterministic relevance tier; 99 means reject.

    Explicit entity metadata is authoritative.  An entity-free official macro
    event may provide broad context, while an entity-free public-media event
    needs a recognisable China-market term.  Direct company evidence always
    sorts ahead of broad context regardless of source tier.
    """

    # Search hints may be displayed and archived, but entity tags must never
    # bypass corroboration and turn one provider's snippet into actionable
    # macro evidence. Confirmed discovery still passes through all upstream
    # PIT, freshness, injection, dedupe and per-source-cap checks.
    if event.event_type == DISCOVERY_HINT_EVENT_TYPE:
        return 99
    if event.entities:
        return 0 if code in event.entities or canonical_symbol in event.entities else 99
    title = event.title.casefold()
    text = f"{event.title}\n{event.summary}".casefold()
    if code.casefold() in text or canonical_symbol.casefold() in text:
        return 0
    if event.event_type not in _GENERAL_EVIDENCE_EVENT_TYPES:
        return 99
    if event.source_tier in {SourceTier.OFFICIAL, SourceTier.LICENSED}:
        return 1
    if event.source_tier is SourceTier.PUBLIC_MEDIA:
        company_story = any(marker in title for marker in _COMPANY_STORY_MARKERS)
        if company_story:
            return 99
        # Broad-market headlines are useful for an index/ETF. Requiring the
        # market term in the title prevents an unrelated company announcement
        # mentioning "A shares" in its boilerplate from entering the pack.
        if any(term in title for term in _A_SHARE_MARKET_TERMS):
            return 2
        has_foreign_context = any(term in text for term in _FOREIGN_CONTEXT_TERMS)
        has_unambiguous_china_context = any(
            term in text for term in _UNAMBIGUOUS_CHINA_SYSTEMIC_TERMS
        )
        if any(term in text for term in _CHINA_SYSTEMIC_TERMS) and (
            not has_foreign_context or has_unambiguous_china_context
        ):
            return 2
        has_ambiguous_macro_term = any(
            term in text for term in _AMBIGUOUS_MACRO_TERMS
        )
        has_china_context = "中国" in text or any(
            term in text for term in _UNAMBIGUOUS_CHINA_SYSTEMIC_TERMS
        )
        if has_ambiguous_macro_term and has_china_context and not has_foreign_context:
            return 2
        if any(term in text for term in _GLOBAL_SYSTEMIC_TERMS):
            return 3
    return 99


def _analysis_id(
    symbol: str,
    as_of: datetime,
    horizon: str,
    evidence: Sequence[EvidenceItem],
) -> str:
    material = "|".join(
        (
            _canonical_symbol(symbol),
            as_of.isoformat(),
            horizon,
            *(item.evidence_id for item in evidence),
        )
    )
    return sha256(material.encode()).hexdigest()


def _abstain_analysis(request: MacroAnalysisRequest, reason: str) -> MacroAnalysis:
    return MacroAnalysis(
        analysis_id=request.analysis_id,
        as_of=request.as_of,
        decision=MacroAnalysisDecision.ABSTAIN,
        regime="unknown",
        technical_alignment=Decimal("0"),
        macro_impact=Decimal("0"),
        scenarios=(),
        claims=(),
        uncertainties=(reason,),
        data_gaps=("MACRO_ANALYSIS_UNAVAILABLE",),
        invalidation_conditions=(),
        reported_confidence="UNCALIBRATED",
        refusal_reason=reason,
        model_version="local-fail-closed@1",
    )


def _canonical_symbol(symbol: str) -> str:
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


def _single_line(value: str) -> str:
    return " ".join(value.split())


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
