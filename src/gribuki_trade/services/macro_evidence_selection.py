"""宏观证据选择、相关性过滤与来源交叉印证策略。"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from urllib.parse import urlsplit

from gribuki_trade.analysis.schemas import EvidenceItem
from gribuki_trade.domain.events import (
    DISCOVERY_CONFIRMED_EVENT_TYPE,
    DISCOVERY_HINT_EVENT_TYPE,
    NormalizedEvent,
    SourceTier,
)
from gribuki_trade.domain.recommendations import EvidenceReference
from gribuki_trade.services.macro_models import (
    EvidenceCorroboration,
    EvidenceSelection,
    MacroEvidenceConfig,
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

