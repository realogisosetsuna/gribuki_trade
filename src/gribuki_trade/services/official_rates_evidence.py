"""将外汇管理局与 Shibor 官方观测转换为有界证据。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime

from gribuki_trade.analysis.schemas import EvidenceItem
from gribuki_trade.domain.recommendations import EvidenceReference
from gribuki_trade.ports.official_rates import (
    SafeCentralParityHistory,
    ShiborHistory,
)


@dataclass(frozen=True, slots=True)
class OfficialRatesEvidenceBundle:
    items: tuple[EvidenceItem, ...]
    references: tuple[EvidenceReference, ...]
    report_lines: tuple[str, ...]

    def __post_init__(self) -> None:
        if tuple(item.evidence_id for item in self.items) != tuple(
            item.evidence_id for item in self.references
        ):
            raise ValueError("official-rate evidence items and references must align")


def build_official_rates_evidence(
    safe: SafeCentralParityHistory | None,
    shibor: ShiborHistory | None,
    *,
    as_of: datetime,
) -> OfficialRatesEvidenceBundle:
    """在不改变来源单位的情况下构建官方数值证据。"""

    _require_aware(as_of)
    items: list[EvidenceItem] = []
    references: list[EvidenceReference] = []
    lines: list[str] = []
    if safe is not None:
        latest = safe.observations[-1]
        if safe.meta.fetched_at > as_of or latest.available_at > as_of:
            raise ValueError("SAFE evidence is unavailable at as_of")
        title = f"SAFE人民币汇率中间价｜{latest.session_date.isoformat()}"
        excerpt = (
            f"SAFE官方美元兑人民币中间价：1美元={latest.cny_per_usd:.4f}元人民币；"
            f"原始口径为100美元={latest.source_quote_amount_cny}元人民币；"
            "不是在岸即期收盘价或离岸USD/CNH。"
        )
        item, reference = _evidence(
            source_id=safe.meta.source_id,
            source_url=safe.meta.source_url,
            title=title,
            excerpt=excerpt,
            published_at=latest.available_at,
            first_seen_at=safe.meta.fetched_at,
        )
        items.append(item)
        references.append(reference)
        lines.append(
            f"SAFE美元兑人民币中间价（{latest.session_date.isoformat()}）："
            f"1美元={latest.cny_per_usd:.3f}元人民币；发布时间="
            f"{latest.available_at.isoformat(timespec='seconds')}；状态="
            f"{'陈旧' if safe.meta.stale else '正常'}。"
        )
    if shibor is not None:
        latest_shibor = shibor.observations[-1]
        if shibor.meta.fetched_at > as_of or latest_shibor.available_at > as_of:
            raise ValueError("Shibor evidence is unavailable at as_of")
        values = "，".join(
            f"{rate.tenor.value}={rate.value_percent:.3f}%"
            for rate in latest_shibor.rates
        )
        title = f"官方Shibor定盘｜{latest_shibor.session_date.isoformat()}"
        excerpt = (
            f"官方Shibor（年化百分数、ACT/360、T+0）：{values}；"
            "这是无担保同业拆借报价定盘，不是回购利率。"
        )
        item, reference = _evidence(
            source_id=shibor.meta.source_id,
            source_url=shibor.meta.source_url,
            title=title,
            excerpt=excerpt,
            published_at=latest_shibor.available_at,
            first_seen_at=shibor.meta.fetched_at,
        )
        items.append(item)
        references.append(reference)
        lines.append(
            f"官方Shibor（{latest_shibor.session_date.isoformat()}）：{values}；"
            "单位=年化百分数，日计数=ACT/360，结算=T+0；"
            f"状态={'陈旧' if shibor.meta.stale else '正常'}。"
        )
    return OfficialRatesEvidenceBundle(tuple(items), tuple(references), tuple(lines))


def _evidence(
    *,
    source_id: str,
    source_url: str,
    title: str,
    excerpt: str,
    published_at: datetime,
    first_seen_at: datetime,
) -> tuple[EvidenceItem, EvidenceReference]:
    payload = json.dumps(
        {
            "excerpt": excerpt,
            "first_seen_at": first_seen_at.astimezone(UTC).isoformat(),
            "published_at": published_at.astimezone(UTC).isoformat(),
            "source_id": source_id,
            "source_url": source_url,
            "title": title,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    content_hash = hashlib.sha256(payload).hexdigest()
    evidence_id = hashlib.sha256(
        f"gribuki.official_rates.v1\0{content_hash}".encode()
    ).hexdigest()
    item = EvidenceItem(
        evidence_id=evidence_id,
        publisher=source_id,
        source_tier=1,
        published_at=published_at,
        first_seen_at=first_seen_at,
        title=title,
        excerpt=excerpt,
        canonical_url=source_url,
        content_hash=content_hash,
    )
    return item, EvidenceReference(
        evidence_id=evidence_id,
        title=title,
        canonical_url=source_url,
        published_at=published_at,
        first_seen_at=first_seen_at,
        source_tier=1,
    )


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
