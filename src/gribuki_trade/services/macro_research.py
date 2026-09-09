"""具有时点约束的证据选择与失败关闭宏观分析。"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from urllib.parse import urlsplit

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
from gribuki_trade.ports.llm_analyzer import (
    AnalyzerAuditIdentity,
    AuditableMacroAnalyzer,
    DualTrackMacroAnalyzer,
    MacroAnalyzer,
)
from gribuki_trade.services.macro_models import (
    EvidenceCorroboration,
    EvidenceSelection,
    MacroEvidenceConfig,
    MacroResearchRun,
)

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

_MULTI_LABEL_PUBLIC_SUFFIXES = frozenset(
    {
        "ac.cn",
        "com.cn",
        "edu.cn",
        "gov.cn",
        "net.cn",
        "org.cn",
        "com.hk",
        "com.tw",
        "co.jp",
        "co.uk",
        "org.uk",
    }
)
_KNOWN_PUBLIC_PUBLISHERS = {
    "eastmoney": "eastmoney.com",
    "cailianpress": "cls.cn",
    "sina": "sina.com.cn",
    "10jqka": "10jqka.com.cn",
}
_OFFICIAL_DOMAIN_BODIES = {
    "stats.gov.cn": "nbs",
    "pbc.gov.cn": "pboc",
    "federalreserve.gov": "fed",
    "csrc.gov.cn": "csrc",
    "mof.gov.cn": "mof",
    "ndrc.gov.cn": "ndrc",
    "safe.gov.cn": "safe",
    "sse.com.cn": "sse",
    "szse.cn": "szse",
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

# 无实体媒体稿只有在包含可识别的中国市场或宏观上下文时才有价值。此处刻意偏向精确率：
# 不能仅因来源较新，就让一般海外公司、犯罪、体育或名人标题填充 A 股 EvidencePack。
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
class MacroResearchPlan:
    """在调用任何供应商前准备、已冻结且受哈希约束的输入。"""

    request: MacroAnalysisRequest
    selection: EvidenceSelection
    references: tuple[EvidenceReference, ...]
    request_sha256: str
    evidence_pack_sha256: str
    manifest_sha256: str
    analyzer_identity: AnalyzerAuditIdentity | None
    eligible_for_analysis: bool

    def __post_init__(self) -> None:
        expected_analysis_id = _analysis_id(
            self.request.symbol,
            self.request.as_of,
            self.request.horizon,
            self.request.technical_summary,
            self.request.evidence,
        )
        if self.request.analysis_id != expected_analysis_id:
            raise ValueError("analysis_id does not match complete macro request input")
        expected_evidence = _evidence_pack_sha256(self.request.evidence)
        if self.evidence_pack_sha256 != expected_evidence:
            raise ValueError("evidence_pack_sha256 does not match request evidence")
        expected_request = _request_sha256(self.request)
        if self.request_sha256 != expected_request:
            raise ValueError("request_sha256 does not match macro request")
        expected_manifest = _macro_manifest_sha256(
            request_sha256=self.request_sha256,
            evidence_pack_sha256=self.evidence_pack_sha256,
            analyzer_identity=self.analyzer_identity,
            eligible_for_analysis=self.eligible_for_analysis,
        )
        if self.manifest_sha256 != expected_manifest:
            raise ValueError("manifest_sha256 does not match macro research plan")
        request_ids = tuple(item.evidence_id for item in self.request.evidence)
        reference_ids = tuple(item.evidence_id for item in self.references)
        if request_ids != reference_ids:
            raise ValueError("plan references must exactly match request evidence order")


class MacroResearchService:
    """选择有界证据，并调用本身不具备工具的模型。"""

    def __init__(
        self,
        analyzer: MacroAnalyzer,
        *,
        config: MacroEvidenceConfig | None = None,
        analyzer_identity: AnalyzerAuditIdentity | None = None,
    ) -> None:
        self._analyzer = analyzer
        self._config = config or MacroEvidenceConfig()
        discovered_identity = (
            analyzer.audit_identity if isinstance(analyzer, AuditableMacroAnalyzer) else None
        )
        if (
            analyzer_identity is not None
            and discovered_identity is not None
            and analyzer_identity != discovered_identity
        ):
            raise ValueError("explicit analyzer identity does not match adapter identity")
        self._analyzer_identity = analyzer_identity or discovered_identity

    @property
    def analyzer_identity(self) -> AnalyzerAuditIdentity | None:
        return self._analyzer_identity

    def prepare(
        self,
        *,
        symbol: str,
        as_of: datetime,
        horizon: str,
        technical_summary: Sequence[str],
        events: Sequence[NormalizedEvent],
        additional_evidence: Sequence[EvidenceItem] = (),
    ) -> MacroResearchPlan:
        """在不执行网络 I/O 的情况下冻结一个时点请求及其完整血缘。"""

        _require_aware(as_of, "as_of")
        canonical_symbol = _canonical_symbol(symbol)
        normalized_summary = tuple(_single_line(item) for item in technical_summary)
        selection = select_macro_evidence(
            canonical_symbol,
            as_of,
            events,
            config=self._config,
        )
        additional = tuple(additional_evidence)
        combined_evidence = additional + selection.items
        identifiers = [item.evidence_id for item in combined_evidence]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("additional evidence IDs must not collide")
        for item in additional:
            _require_aware(item.published_at, "additional evidence published_at")
            _require_aware(item.first_seen_at, "additional evidence first_seen_at")
            if item.first_seen_at > as_of or item.published_at > as_of:
                raise ValueError("additional evidence was not available at as_of")
        evidence_pack_sha256 = _evidence_pack_sha256(combined_evidence)
        analysis_id = _analysis_id(
            canonical_symbol,
            as_of,
            horizon,
            normalized_summary,
            combined_evidence,
        )
        request = MacroAnalysisRequest(
            analysis_id=analysis_id,
            symbol=canonical_symbol,
            as_of=as_of,
            horizon=horizon,
            technical_summary=normalized_summary,
            evidence=combined_evidence,
        )
        references = tuple(_evidence_reference(item) for item in combined_evidence)
        macro_capable_additional = any(
            item.publisher not in _TECHNICAL_ONLY_ADDITIONAL_PUBLISHERS for item in additional
        )
        eligible = bool(selection.items or macro_capable_additional)
        request_sha256 = _request_sha256(request)
        manifest_sha256 = _macro_manifest_sha256(
            request_sha256=request_sha256,
            evidence_pack_sha256=evidence_pack_sha256,
            analyzer_identity=self._analyzer_identity,
            eligible_for_analysis=eligible,
        )
        return MacroResearchPlan(
            request=request,
            selection=selection,
            references=references,
            request_sha256=request_sha256,
            evidence_pack_sha256=evidence_pack_sha256,
            manifest_sha256=manifest_sha256,
            analyzer_identity=self._analyzer_identity,
            eligible_for_analysis=eligible,
        )

    async def execute(self, plan: MacroResearchPlan) -> MacroResearchRun:
        """只执行为该分析器身份准备的精确计划。"""

        if plan.analyzer_identity != self._analyzer_identity:
            raise ValueError("macro research plan analyzer identity mismatch")
        request = plan.request
        selection = plan.selection
        if not plan.eligible_for_analysis:
            return MacroResearchRun(
                request=request,
                analysis=_abstain_analysis(request, "NO_ELIGIBLE_EVIDENCE"),
                selection=selection,
                plan=plan,
                failure_code="NO_ELIGIBLE_EVIDENCE",
            )
        try:
            dual = None
            if isinstance(self._analyzer, DualTrackMacroAnalyzer):
                dual = await self._analyzer.analyze_dual(request)
                analysis = dual.selected_analysis
            else:
                analysis = await self._analyzer.analyze(request)
            analysis.validate_against(request)
        except Exception as exc:
            # 上游错误可能包含请求标识或响应片段；研究记录中只保留显式结构化的稳定代码。
            failure_code = _sanitized_analyzer_failure_code(exc)
            return MacroResearchRun(
                request=request,
                analysis=_abstain_analysis(request, failure_code),
                selection=selection,
                plan=plan,
                failure_code=failure_code,
            )
        return MacroResearchRun(
            request=request,
            analysis=analysis,
            selection=selection,
            plan=plan,
            failure_code=None if dual is None else dual.failure_code,
            baseline_analysis=None if dual is None else dual.baseline_analysis,
            adversarial_analysis=(None if dual is None else dual.adversarial_analysis),
            selected_track=None if dual is None else dual.selected_track,
            dual_audit_document=None if dual is None else dual.audit_document,
            dual_audit_record_sha256=(None if dual is None else dual.audit_record_sha256),
        )

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
        plan = self.prepare(
            symbol=symbol,
            as_of=as_of,
            horizon=horizon,
            technical_summary=technical_summary,
            events=events,
            additional_evidence=additional_evidence,
        )
        return await self.execute(plan)


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
    """为单个决策时点选择相关、可见且多样的事件版本。"""

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
    corroboration: list[EvidenceCorroboration] = []
    corroboration_pool = tuple(
        event
        for event in events
        if _eligible_for_corroboration(
            event,
            canonical_symbol=canonical,
            code=code,
            as_of=as_of,
            max_age=resolved.max_age,
        )
    )
    for event in candidates:
        published = event.published_at or event.first_seen_at
        evidence_status = _corroboration_status(event, corroboration_pool)
        if evidence_status.status == "AUTHORITATIVE_SOURCE":
            status_prefix = ""
        elif evidence_status.status == "CORROBORATED_INDEPENDENT_SOURCES":
            status_prefix = (
                "[证据状态：已由独立来源交叉印证；独立发布方="
                + ",".join(evidence_status.independent_publishers)
                + "] "
            )
        else:
            status_prefix = (
                "[证据状态：单一公共媒体且未经独立印证；只能作为线索，不得当作已证实事实] "
            )
        excerpt = _single_line(f"{status_prefix}{event.title}。{event.summary}")
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
        corroboration.append(replace(evidence_status, evidence_id=item.evidence_id))
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
        corroboration=tuple(corroboration),
        uncorroborated_public_media=sum(
            item.status == "UNCORROBORATED_PUBLIC_MEDIA" for item in corroboration
        ),
    )


def _eligible_for_corroboration(
    event: NormalizedEvent,
    *,
    canonical_symbol: str,
    code: str,
    as_of: datetime,
    max_age: timedelta,
) -> bool:
    published = event.published_at or event.first_seen_at
    if (
        event.available_at > as_of
        or event.first_seen_at > as_of
        or published > as_of
        or as_of - published > max_age
        or _evidence_scope_rank(event, canonical_symbol, code) >= 99
    ):
        return False
    return not any(
        marker.search(f"{event.title}\n{event.summary}") for marker in _INJECTION_MARKERS
    )


def _corroboration_status(
    event: NormalizedEvent,
    pool: Sequence[NormalizedEvent],
) -> EvidenceCorroboration:
    if event.source_tier in {SourceTier.OFFICIAL, SourceTier.LICENSED}:
        return EvidenceCorroboration(
            evidence_id=event.revision_id,
            status="AUTHORITATIVE_SOURCE",
            independent_publishers=(_publisher_identity(event),),
        )
    publisher_identity = _publisher_identity(event)
    publishers = {publisher_identity}
    for candidate in pool:
        candidate_identity = _publisher_identity(candidate)
        if candidate.revision_id == event.revision_id or candidate_identity == publisher_identity:
            continue
        published = event.published_at or event.first_seen_at
        candidate_published = candidate.published_at or candidate.first_seen_at
        if abs(candidate_published - published) > timedelta(days=3):
            continue
        if _headline_similarity(event, candidate) >= Decimal("0.55"):
            publishers.add(candidate_identity)
    ordered_publishers = tuple(sorted(publishers))
    return EvidenceCorroboration(
        evidence_id=event.revision_id,
        status=(
            "CORROBORATED_INDEPENDENT_SOURCES"
            if len(ordered_publishers) >= 2
            else "UNCORROBORATED_PUBLIC_MEDIA"
        ),
        independent_publishers=ordered_publishers,
    )


def _publisher_identity(event: NormalizedEvent) -> str:
    """返回保守的发布者身份，绝不把采集路由 ID 当作发布者。"""

    normalized_source = event.source_id.strip().casefold()
    if event.source_tier is SourceTier.OFFICIAL and normalized_source.startswith("official."):
        components = normalized_source.split(".")
        if len(components) >= 2 and components[1]:
            return f"official-body:{components[1]}"

    try:
        hostname = urlsplit(event.canonical_url).hostname
    except ValueError:
        hostname = None
    if hostname:
        normalized_host = hostname.rstrip(".").casefold()
        try:
            normalized_host = normalized_host.encode("idna").decode("ascii")
        except UnicodeError:
            normalized_host = ""
        if normalized_host:
            domain = _registrable_domain(normalized_host)
            official_body = _OFFICIAL_DOMAIN_BODIES.get(domain)
            if official_body is not None:
                return f"official-body:{official_body}"
            prefix = (
                "official-domain"
                if event.source_tier is SourceTier.OFFICIAL
                else "publisher-domain"
            )
            return f"{prefix}:{domain}"

    for marker, domain in _KNOWN_PUBLIC_PUBLISHERS.items():
        if marker in normalized_source:
            return f"publisher-domain:{domain}"
    # 缺少发布者元数据不能证明来源独立；统一为未知身份可失败关闭，避免把两个适配器
    # ID 错当作两家媒体。
    return "publisher-unknown"


def _registrable_domain(hostname: str) -> str:
    labels = tuple(label for label in hostname.split(".") if label)
    if len(labels) <= 2:
        return ".".join(labels)
    suffix = ".".join(labels[-2:])
    if suffix in _MULTI_LABEL_PUBLIC_SUFFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return suffix


def _headline_similarity(left: NormalizedEvent, right: NormalizedEvent) -> Decimal:
    """用标题字符二元组做保守近似，避免同源转载仅凭摘要字面相同升级。"""

    left_parts = _character_bigrams(left.title)
    right_parts = _character_bigrams(right.title)
    if not left_parts or not right_parts:
        return Decimal("0")
    overlap = len(left_parts & right_parts)
    union = len(left_parts | right_parts)
    return Decimal(overlap) / Decimal(union)


def _character_bigrams(value: str) -> set[str]:
    normalized = "".join(character.casefold() for character in value if character.isalnum())
    return {normalized[index : index + 2] for index in range(max(0, len(normalized) - 1))}


def _evidence_scope_rank(
    event: NormalizedEvent,
    canonical_symbol: str,
    code: str,
) -> int:
    """返回确定性相关性层级；99 表示拒绝。

    显式实体元数据具有权威性。无实体的官方宏观事件可提供广泛上下文，而无实体的
    公共媒体事件必须包含可识别的中国市场术语。无论来源层级如何，直接公司证据
    始终排在广泛上下文之前。
    """

    # 搜索提示可以展示和归档，但实体标签绝不能绕过交叉印证，把单一供应商摘要变为可行动
    # 宏观证据。确认后的发现仍须通过全部上游时点、新鲜度、注入、去重与单来源上限检查。
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
        # 广泛市场标题对指数/ETF 有用。要求标题含市场术语，可防止仅在样板文字中提到
        # “A 股”的无关公司公告进入证据包。
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
        has_ambiguous_macro_term = any(term in text for term in _AMBIGUOUS_MACRO_TERMS)
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
    technical_summary: Sequence[str],
    evidence: Sequence[EvidenceItem],
) -> str:
    document = {
        "as_of": as_of.astimezone(UTC).isoformat(),
        "evidence": [_evidence_document(item) for item in evidence],
        "horizon": horizon,
        "schema_version": 2,
        "symbol": _canonical_symbol(symbol),
        "technical_summary": list(technical_summary),
    }
    return _document_sha256(document)


def _evidence_reference(item: EvidenceItem) -> EvidenceReference:
    return EvidenceReference(
        evidence_id=item.evidence_id,
        title=item.title,
        canonical_url=item.canonical_url,
        published_at=item.published_at,
        first_seen_at=item.first_seen_at,
        source_tier=item.source_tier,
    )


def _evidence_document(item: EvidenceItem) -> dict[str, object]:
    _require_aware(item.published_at, "evidence published_at")
    _require_aware(item.first_seen_at, "evidence first_seen_at")
    return {
        "canonical_url": item.canonical_url,
        "content_hash": item.content_hash,
        "evidence_id": item.evidence_id,
        "excerpt": item.excerpt,
        "first_seen_at": item.first_seen_at.astimezone(UTC).isoformat(),
        "published_at": item.published_at.astimezone(UTC).isoformat(),
        "publisher": item.publisher,
        "source_tier": item.source_tier,
        "title": item.title,
    }


def _evidence_pack_sha256(evidence: Sequence[EvidenceItem]) -> str:
    return _document_sha256(
        {
            "evidence": [_evidence_document(item) for item in evidence],
            "schema_version": 1,
        }
    )


def _request_sha256(request: MacroAnalysisRequest) -> str:
    _require_aware(request.as_of, "request as_of")
    return _document_sha256(
        {
            "analysis_id": request.analysis_id,
            "as_of": request.as_of.astimezone(UTC).isoformat(),
            "evidence": [_evidence_document(item) for item in request.evidence],
            "horizon": request.horizon,
            "schema_version": 1,
            "symbol": request.symbol,
            "technical_summary": list(request.technical_summary),
        }
    )


def _macro_manifest_sha256(
    *,
    request_sha256: str,
    evidence_pack_sha256: str,
    analyzer_identity: AnalyzerAuditIdentity | None,
    eligible_for_analysis: bool,
) -> str:
    identity_document: dict[str, object] | None = None
    if analyzer_identity is not None:
        identity_document = {
            "adapter_version": analyzer_identity.adapter_version,
            "identity_manifest_sha256": analyzer_identity.manifest_sha256,
            "prompt_schema_sha256": analyzer_identity.prompt_schema_sha256,
            "prompt_version": analyzer_identity.prompt_version,
            "provider_id": analyzer_identity.provider_id,
            "requested_model": analyzer_identity.requested_model,
        }
    return _document_sha256(
        {
            "analyzer_identity": identity_document,
            "eligible_for_analysis": eligible_for_analysis,
            "evidence_pack_sha256": evidence_pack_sha256,
            "request_sha256": request_sha256,
            "schema_version": 1,
        }
    )


def _document_sha256(document: dict[str, object]) -> str:
    encoded = json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return sha256(encoded.encode("utf-8")).hexdigest()


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
